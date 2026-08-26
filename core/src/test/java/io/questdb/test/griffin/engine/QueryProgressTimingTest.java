package io.questdb.test.griffin.engine;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryProgressTimingTest extends AbstractCairoTest {

    @Before
    public void setup() {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
    }

    @Test
    public void testNoRowsLeavesFirstRowUnset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_empty (x LONG)");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            try (
                    RecordCursorFactory factory = select("tab_empty");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertFalse(cursor.hasNext());
            }
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(-1, trace.firstRowNanos);
            Assert.assertEquals(0, trace.waitNanos);
        });
    }

    @Test
    public void testPageFrameNextCleansUpWhenPartitionOpenThrows() throws Exception {
        // Mutation caught: removing the catch/close0(th)/rethrow path from
        // QueryProgress.RegisteredPageFrameCursor.next(long) leaves this query registered.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_page_frame_error AS (SELECT x, timestamp_sequence(0, 1) ts FROM long_sequence(1)) TIMESTAMP(ts) PARTITION BY DAY");
            engine.releaseAllWriters();
            final QueryRegistry registry = engine.getQueryRegistry();
            final AtomicLong queryId = new AtomicLong(-1);
            registry.setListener((query, id, context) -> {
                if ("tab_page_frame_error".contentEquals(query)) {
                    queryId.set(id);
                }
            });
            try (RecordCursorFactory factory = select("tab_page_frame_error")) {
                Assert.assertTrue(factory instanceof QueryProgress);
                final PageFrameCursor cursor = factory.getPageFrameCursor(
                        sqlExecutionContext,
                        PartitionFrameCursorFactory.ORDER_ASC
                );
                final long id = queryId.get();
                Assert.assertTrue(id >= 0);
                Assert.assertNotNull(registry.getEntry(id));

                final TableToken tableToken = engine.verifyTableName("tab_page_frame_error");
                try (Path path = new Path().of(configuration.getDbRoot()).concat(tableToken).concat("1970-01-01")) {
                    Assert.assertTrue(Files.rmdir(path, true));
                }

                try {
                    cursor.next();
                    Assert.fail();
                } catch (CairoException ignored) {
                }
                // Deliberately do not close cursor: next() must have closed it on the throw path.
                Assert.assertNull(registry.getEntry(id));
            } finally {
                registry.setListener(null);
            }
        });
    }

    @Test
    public void testPageFrameNextStampsFirstRow() throws Exception {
        // Mutation caught: removing the first non-null frame stamp from
        // QueryProgress.RegisteredPageFrameCursor.next(long) leaves ttfr at -1.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_page_frame AS (SELECT x FROM long_sequence(1))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            currentMicros = 1_000;
            try (
                    RecordCursorFactory factory = select("tab_page_frame");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)
            ) {
                Assert.assertTrue(factory instanceof QueryProgress);
                currentMicros = 1_500;
                Assert.assertNotNull(cursor.next());
                currentMicros = 2_000;
            }
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(1_000_000L, trace.executionNanos);
            Assert.assertEquals(500_000L, trace.firstRowNanos);
        });
    }

    @Test
    public void testSuspendIsIdempotent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_idem AS (SELECT x FROM long_sequence(1))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            currentMicros = 1_000;
            try (
                    RecordCursorFactory factory = select("tab_idem");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                cursor.resumeTimer(); // resume with no suspend: no-op
                currentMicros = 1_100;
                cursor.suspendTimer();
                currentMicros = 1_200;
                cursor.suspendTimer(); // second suspend: no-op, interval keeps running
                currentMicros = 1_400;
                cursor.resumeTimer();  // wait = 1400 - 1100 = 300us
                cursor.resumeTimer();  // second resume: no-op
                while (cursor.hasNext()) {
                }
                currentMicros = 2_000;
            }
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(300_000L, trace.waitNanos);
        });
    }

    @Test
    public void testTerminalSuspensionIsCountedOnClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_term AS (SELECT x FROM long_sequence(2))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            currentMicros = 1_000;
            try (
                    RecordCursorFactory factory = select("tab_term");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                currentMicros = 1_500;
                cursor.suspendTimer();
                currentMicros = 2_000;
                // close while suspended: implicit resume must count 500us
            }
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(500_000L, trace.waitNanos);
            Assert.assertEquals(1_000_000L, trace.executionNanos);
        });
    }

    @Test
    public void testWaitAndFirstRowAccounting() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(3))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            currentMicros = 1_000;
            try (
                    RecordCursorFactory factory = select("tab");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                currentMicros = 1_500;
                Assert.assertTrue(cursor.hasNext()); // first row at 1500 -> ttfr 500us
                currentMicros = 1_600;
                cursor.suspendTimer();
                currentMicros = 1_900;
                cursor.resumeTimer();                // wait 300us
                while (cursor.hasNext()) {
                }
                currentMicros = 2_000;
            }                                        // wall = 1000us
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(1_000_000L, trace.executionNanos);
            Assert.assertEquals(300_000L, trace.waitNanos);
            Assert.assertEquals(500_000L, trace.firstRowNanos);
        });
    }

    private static void drain(ConcurrentQueue<QueryTrace> queue) {
        final QueryTrace trace = new QueryTrace();
        while (queue.tryDequeue(trace)) {
        }
    }
}
