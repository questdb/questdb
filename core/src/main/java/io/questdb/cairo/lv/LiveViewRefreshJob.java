package io.questdb.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Job that processes WAL commit notifications and refreshes live view instances.
 * On each refresh, compiles the live view's SELECT query, runs the cursor,
 * and copies all results into the InMemoryTable.
 */
public class LiveViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    private final CairoEngine engine;
    private final SqlExecutionContextImpl executionContext;
    private final ObjList<LiveViewInstance> viewInstanceSink = new ObjList<>();

    public LiveViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
        this.executionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
    }

    @Override
    public void close() {
        executionContext.close();
    }

    @Override
    public boolean run(int workerId, @NotNull Job.RunStatus runStatus) {
        return processNotifications();
    }

    private boolean processNotifications() {
        boolean didWork = false;
        ConcurrentLinkedQueue<LiveViewRefreshTask> queue = engine.getLiveViewTaskQueue();
        LiveViewRefreshTask polled;
        while ((polled = queue.poll()) != null) {
            refreshViewsForBaseTable(polled.baseTableToken, polled.seqTxn);
            didWork = true;
        }
        return didWork;
    }

    private void refreshInstance(LiveViewInstance instance) {
        String selectSql = instance.getDefinition().getViewSql();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CompiledQuery cq = compiler.compile(selectSql, executionContext);
            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(executionContext)) {
                    instance.refresh(cursor);
                }
            }
        } catch (SqlException e) {
            LOG.error().$("live view refresh failed [view=").$(instance.getDefinition().getViewName())
                    .$(", error=").$(e.getFlyweightMessage())
                    .I$();
        }
    }

    private void refreshViewsForBaseTable(TableToken baseTableToken, long seqTxn) {
        LiveViewRegistry registry = engine.getLiveViewRegistry();
        registry.getViewsForBaseTable(baseTableToken.getTableName(), viewInstanceSink);

        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (seqTxn > instance.getLastProcessedSeqTxn()) {
                refreshInstance(instance);
                instance.setLastProcessedSeqTxn(seqTxn);
            }
        }
    }
}
