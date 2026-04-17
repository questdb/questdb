/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentLinkedQueue;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

/**
 * Job that processes WAL commit notifications and refreshes live view instances.
 * <p>
 * On the first refresh (bootstrap), compiles the live view's SELECT query, caches
 * the factory on the instance, and populates the InMemoryTable via the full factory
 * cursor. Window function state accumulates during bootstrap and carries over into
 * subsequent incremental refreshes.
 * <p>
 * Incremental refreshes walk the {@link TransactionLogCursor} from
 * {@code lastProcessedSeqTxn + 1}, open each WAL segment's event file to check
 * the transaction type, and for DATA events read the raw rows through
 * {@link WalSegmentPageFrameCursor} wrapped in the factory's incremental cursor.
 * Non-DATA events (TRUNCATE, SQL, schema changes) trigger a full recompute that
 * clears the table, resets window functions, and re-bootstraps.
 */
public class LiveViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final IntList columnIndexes = new IntList();
    private final IntList columnSizeShifts = new IntList();
    private final CairoEngine engine;
    private final SqlExecutionContextImpl executionContext;
    private final PageFrameMemoryPool memoryPool = new PageFrameMemoryPool(0);
    private final ObjList<LiveViewInstance> viewInstanceSink = new ObjList<>();
    private final WalEventReader walEventReader;
    private final StringSink walNameSink = new StringSink();
    private final Path walPath = new Path();

    public LiveViewRefreshJob(CairoEngine engine) {
        this.engine = engine;
        this.executionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
        this.walEventReader = new WalEventReader(engine.getConfiguration());
    }

    @Override
    public void close() {
        executionContext.close();
        Misc.free(walEventReader);
        Misc.free(walPath);
        Misc.free(addressCache);
        Misc.free(memoryPool);
    }

    @Override
    public boolean run(int workerId, @NotNull Job.RunStatus runStatus) {
        return processNotifications();
    }

    /**
     * Populates the InMemoryTable from scratch using the compiled factory's base cursor
     * and the bootstrap window cursor. Window functions are reset to zero and then
     * re-accumulated during iteration. After this call, window state reflects
     * all rows currently in the base table.
     */
    private void bootstrap(LiveViewInstance instance) throws SqlException {
        WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        RecordCursorFactory baseFactory = windowFactory.getBaseFactory();
        windowFactory.resetWindowFunctions();
        RecordCursor baseCursor = baseFactory.getCursor(executionContext);
        try {
            RecordCursor windowCursor = windowFactory.getBootstrapCursor(baseCursor, executionContext);
            try {
                instance.refresh(windowCursor);
            } finally {
                windowCursor.close();
            }
        } catch (Throwable t) {
            // baseCursor may not have been adopted by windowCursor yet
            Misc.free(baseCursor);
            throw t;
        }
    }

    private void buildColumnMappings(RecordMetadata baseMetadata) {
        columnIndexes.clear();
        columnSizeShifts.clear();
        for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
            int writerIndex = baseMetadata.getWriterIndex(i);
            columnIndexes.add(writerIndex);
            int type = baseMetadata.getColumnType(i);
            if (ColumnType.isVarSize(type)) {
                columnSizeShifts.add(0);
            } else {
                columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(type)));
            }
        }
    }

    private RecordCursorFactory ensureCompiledFactory(LiveViewInstance instance) throws SqlException {
        RecordCursorFactory factory = instance.getCompiledFactory();
        if (factory == null) {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile(instance.getDefinition().getViewSql(), executionContext);
                factory = cq.getRecordCursorFactory();
            }
            instance.setCompiledFactory(factory);
        }
        return factory;
    }

    private WindowRecordCursorFactory getWindowFactory(LiveViewInstance instance) throws SqlException {
        RecordCursorFactory factory = ensureCompiledFactory(instance);
        return unwrapWindowFactory(factory);
    }

    /**
     * Walks the {@link TransactionLogCursor} from {@code fromSeqTxn} through
     * {@code toSeqTxn}. For each DATA transaction, reads the corresponding WAL segment
     * rows and appends them to the InMemoryTable via the factory's incremental cursor.
     * Falls back to a full recompute on any non-DATA event or when WAL segment data
     * is unavailable (e.g. clean symbol files were never hardlinked by the WAL writer).
     */
    private void incrementalRefresh(
            LiveViewInstance instance,
            long fromSeqTxn,
            long toSeqTxn
    ) throws SqlException {
        try {
            incrementalRefresh0(instance, fromSeqTxn, toSeqTxn);
        } catch (CairoException e) {
            LOG.info().$("WAL segment data unavailable, falling back to full recompute [error=").$(e.getMessage()).I$();
            fullRecompute(instance);
        }
    }

    private void incrementalRefresh0(
            LiveViewInstance instance,
            long fromSeqTxn,
            long toSeqTxn
    ) throws SqlException {
        WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        RecordCursorFactory baseFactory = windowFactory.getBaseFactory();
        TableToken baseToken = instance.getDefinition().getBaseTableToken();
        RecordMetadata baseMetadata = baseFactory.getMetadata();

        buildColumnMappings(baseMetadata);

        WalSegmentPageFrameCursor frameCursor = new WalSegmentPageFrameCursor(
                engine.getConfiguration(), columnIndexes, columnSizeShifts
        );
        WalSegmentRecordCursor walRecordCursor = new WalSegmentRecordCursor(addressCache, memoryPool);
        try {
            try (TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn)) {
                while (txnCursor.hasNext()) {
                    long txn = txnCursor.getTxn();
                    if (txn > toSeqTxn) {
                        break;
                    }
                    int walId = txnCursor.getWalId();
                    int segmentId = txnCursor.getSegmentId();
                    int segmentTxn = txnCursor.getSegmentTxn();

                    if (walId <= 0) {
                        fullRecompute(instance);
                        return;
                    }

                    walPath.of(engine.getConfiguration().getDbRoot())
                            .concat(baseToken)
                            .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                    WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, walEventReader, segmentTxn, txn);

                    if (!WalTxnType.isDataType(eventCursor.getType())) {
                        fullRecompute(instance);
                        return;
                    }

                    WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    long startRow = dataInfo.getStartRowID();
                    long endRow = dataInfo.getEndRowID();
                    if (endRow <= startRow) {
                        continue;
                    }

                    walNameSink.clear();
                    walNameSink.put(WAL_NAME_BASE).put(walId);
                    frameCursor.of(baseToken, walNameSink, segmentId, endRow, startRow, endRow, baseMetadata);
                    walRecordCursor.of(frameCursor, baseMetadata);

                    RecordCursor windowCursor = windowFactory.getIncrementalCursor(walRecordCursor, executionContext);
                    try {
                        instance.appendIncremental(windowCursor);
                    } finally {
                        windowCursor.close();
                    }
                }
            }
        } finally {
            Misc.free(frameCursor);
        }
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

    private void refreshInstance(LiveViewInstance instance, long seqTxn) {
        if (!instance.tryLockForRefresh()) {
            return;
        }
        try {
            if (instance.isDropped() || instance.isInvalid()) {
                return;
            }
            try {
                long lastSeqTxn = instance.getLastProcessedSeqTxn();
                if (lastSeqTxn < 0) {
                    bootstrap(instance);
                } else {
                    incrementalRefresh(instance, lastSeqTxn, seqTxn);
                }
                instance.setLastProcessedSeqTxn(seqTxn);
            } catch (Throwable t) {
                LOG.error().$("live view refresh failed [view=").$(instance.getDefinition().getViewName())
                        .$(", error=").$(t.getMessage())
                        .I$();
            }
        } finally {
            instance.unlockAfterRefresh();
            instance.tryCloseIfDropped();
        }
    }

    private void refreshViewsForBaseTable(TableToken baseTableToken, long seqTxn) {
        LiveViewRegistry registry = engine.getLiveViewRegistry();
        registry.getViewsForBaseTable(baseTableToken.getTableName(), viewInstanceSink);

        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            if (seqTxn > instance.getLastProcessedSeqTxn()) {
                refreshInstance(instance, seqTxn);
            }
        }
    }

    /**
     * Resets window state and re-bootstraps from the base table. Called when
     * a non-DATA WAL event (TRUNCATE, SQL, schema change) makes incremental
     * refresh impossible.
     */
    private void fullRecompute(LiveViewInstance instance) throws SqlException {
        bootstrap(instance);
    }

    private static WindowRecordCursorFactory unwrapWindowFactory(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (f instanceof WindowRecordCursorFactory wf) {
                return wf;
            }
            if (f instanceof QueryProgress) {
                f = f.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }
}
