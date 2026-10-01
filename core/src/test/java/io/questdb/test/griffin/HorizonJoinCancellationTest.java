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

package io.questdb.test.griffin;

import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class HorizonJoinCancellationTest extends AbstractCairoTest {
    @Test
    public void testAllFactoriesAndLookupPaths() throws Exception {
        for (boolean isParallel : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                AtomicLong millis = new AtomicLong(1000);
                TestUtils.execute(null, (engine, _, context) -> {
                    createTables(engine, context);
                    for (boolean isGrouped : new boolean[]{false, true}) {
                        for (int slavePosition = 0; slavePosition < 3; slavePosition++) {
                            for (int route = 0; route < 3; route++) {
                                assertCancellation(engine, context, millis, isParallel, isGrouped, slavePosition, route, null, false, false);
                            }
                        }
                    }
                    assertQuery("SELECT count(q.ts) n FROM t HORIZON JOIN q q LIST (0s) AS h")
                            .withEngine(engine).withContext(context).noRandomAccess().expectSize().returns("n\n1\n");
                }, testConfiguration(isParallel, millis), LOG);
            });
        }
    }

    @Test
    public void testTimeoutAndReuse() throws Exception {
        for (boolean isParallel : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                AtomicLong millis = new AtomicLong(1000);
                TestUtils.execute(null, (engine, _, context) -> {
                    createTables(engine, context);
                    assertCancellation(engine, context, millis, isParallel, false, 0, 0, null, false, true);
                }, testConfiguration(isParallel, millis), LOG);
            });
        }
    }

    @Test
    public void testWorkerBreakerAndCleanupLegacyAndFiberHost() throws Exception {
        for (WorkerPoolMode mode : WorkerPoolMode.values()) {
            assertMemoryLeak(() -> {
                AtomicLong millis = new AtomicLong(1000);
                TestUtils.execute(new TestWorkerPool(1, mode), (engine, _, context) -> {
                    createTables(engine, context);
                    for (boolean hasMasterFilter : new boolean[]{false, true}) {
                        assertCancellation(engine, context, millis, true, false, 0, 0, mode, hasMasterFilter, false);
                    }
                }, testConfiguration(true, millis), LOG);
            });
        }
    }

    private void assertCancellation(
            CairoEngine engine,
            SqlExecutionContext context,
            AtomicLong millis,
            boolean isParallel,
            boolean isGrouped,
            int slavePosition,
            int route,
            WorkerPoolMode workerMode,
            boolean hasMasterFilter,
            boolean isTimeout
    ) throws Exception {
        String sql = query(isGrouped, slavePosition, route, hasMasterFilter);
        String factoryName = (isParallel ? "Async" : "") + (slavePosition > 0 ? "Multi" : "")
                + "HorizonJoin" + (isGrouped ? "" : "NotKeyed") + "RecordCursorFactory";
        String method = switch (route) {
            case 0 -> "backwardScanForFilterMatch";
            case 1 -> "backwardScanForKeyMatch";
            default -> "forwardScanToPosition";
        };
        int signalAt = route == 2 ? 129 : 1;
        SqlExecutionCircuitBreaker originalBreaker = context.getCircuitBreaker();
        Thread ownerThread = Thread.currentThread();
        context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        Callback callback = new Callback(engine, context, sql, factoryName, method, signalAt, ownerThread, workerMode, hasMasterFilter, isTimeout, millis);
        TestLatchedCounterFunctionFactory.reset(callback);
        try (OwnerBreaker breaker = new OwnerBreaker(engine, engine.getConfiguration().getCircuitBreakerConfiguration(), ownerThread)) {
            ((SqlExecutionContextImpl) context).with(breaker);
            try (RecordCursorFactory factory = engine.select(sql, context)) {
                RecordCursorFactory baseFactory = factory.getBaseFactory();
                Assert.assertEquals(factoryName, baseFactory.getClass().getSimpleName());
                PerWorkerLocks locks = workerMode == null ? null : ((AsyncHorizonJoinNotKeyedRecordCursorFactory) baseFactory).getAtom().getPerWorkerLocks();
                CountDownLatch acquired = new CountDownLatch(1);
                if (locks != null) {
                    locks.setTestAcquireLatch(acquired);
                }
                breaker.resetTimer();
                try (RecordCursor cursor = factory.getCursor(context)) {
                    cursor.hasNext();
                    Assert.fail("expected interruption: " + factoryName + "/" + method);
                } catch (CairoException e) {
                    Assert.assertEquals(e.getFlyweightMessage().toString(), isTimeout ? SqlExecutionCircuitBreaker.STATE_TIMEOUT : SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                }
                Assert.assertTrue("signal not reached: " + sql, callback.hasSignalled);
                int visits = TestLatchedCounterFunctionFactory.getCount();
                Assert.assertTrue("post-signal visits=" + (visits - signalAt), visits - signalAt <= 64);
                Assert.assertTrue("exhausted scan: " + visits, visits < 256);
                Assert.assertNotNull(callback.tracker);
                Assert.assertEquals(0, callback.tracker.getUsed());
                Assert.assertNull(context.getMemoryTracker());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                if (locks != null) {
                    Assert.assertEquals(0, acquired.getCount());
                    Assert.assertEquals(0, locks.getAcquiredSlotCount());
                    locks.setTestAcquireLatch(null);
                }
                callback.isArmed = false;
                millis.set(1000);
                breaker.resetTimer();
                String expected = (isGrouped ? "k\t" : "") + "n" + (slavePosition > 0 ? "\tn2" : "") + "\n"
                        + (isGrouped ? "1\t" : "") + "0" + (slavePosition > 0 ? "\t" + (route == 2 ? 2 : 1) : "") + "\n";
                new QueryAssertion(engine, factory).withContext(context).inferRandomAccess().expectSize().returns(expected);
                Assert.assertEquals(0, callback.tracker.getUsed());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                if (locks != null) {
                    Assert.assertEquals(0, locks.getAcquiredSlotCount());
                }
            }
        } finally {
            ((SqlExecutionContextImpl) context).with(originalBreaker);
            TestLatchedCounterFunctionFactory.reset(null);
        }
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext context) throws Exception {
        engine.execute("CREATE TABLE q AS (SELECT timestamp_sequence(0, 1) ts, 1 k, 1 price FROM long_sequence(256)) TIMESTAMP(ts)", context);
        engine.execute("CREATE TABLE t (ts TIMESTAMP, k INT) TIMESTAMP(ts)", context);
        engine.execute("INSERT INTO t VALUES (256, 1)", context);
        engine.execute("CREATE TABLE tf (ts TIMESTAMP, k INT) TIMESTAMP(ts)", context);
        engine.execute("INSERT INTO tf VALUES (127, 1), (256, 1)", context);
    }

    private static String query(boolean isGrouped, int slavePosition, int route, boolean hasMasterFilter) {
        String filtered = " HORIZON JOIN q q ON (" + (route == 0 ? "" : "t.k = q.k AND (")
                + "q.price < 0 OR test_latched_counter()" + (route == 0 ? "" : ")") + ")";
        String unfiltered = " HORIZON JOIN q q2";
        return "SELECT " + (isGrouped ? "t.k, " : "") + "count(q.ts) n" + (slavePosition > 0 ? ", count(q2.ts) n2" : "")
                + " FROM " + (route == 2 ? "tf" : "t") + " t"
                + (slavePosition == 2 ? unfiltered + filtered : filtered + (slavePosition == 1 ? unfiltered : ""))
                + " LIST (0s) AS h" + (hasMasterFilter ? " WHERE t.k > 0" : "");
    }

    private static DefaultTestCairoConfiguration testConfiguration(boolean isParallel, AtomicLong millis) throws Exception {
        SqlExecutionCircuitBreakerConfiguration breakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public boolean checkConnection() {
                return false;
            }

            @Override
            public int getCircuitBreakerThrottle() {
                return 2048;
            }

            @Override
            public MillisecondClock getClock() {
                return millis::get;
            }

            @Override
            public long getQueryTimeout() {
                return 100;
            }
        };
        FactoryProvider provider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        return new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
            @Override
            public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                return breakerConfiguration;
            }

            @Override
            public FactoryProvider getFactoryProvider() {
                return provider;
            }

            @Override
            public long getSqlHorizonJoinBwdScanAbsoluteThreshold() {
                return 0;
            }

            @Override
            public boolean isSqlParallelHorizonJoinEnabled() {
                return isParallel;
            }
        };
    }

    private static class Callback implements TestLatchedCounterFunctionFactory.Callback {
        private final SqlExecutionContext context;
        private final CairoEngine engine;
        private final String factoryName;
        private final boolean hasMasterFilter;
        private final boolean isTimeout;
        private final String method;
        private final AtomicLong millis;
        private final Thread ownerThread;
        private final int signalAt;
        private final String sql;
        private final WorkerPoolMode workerMode;
        private volatile boolean hasSignalled;
        private volatile boolean isArmed = true;
        private volatile MemoryTracker tracker;

        Callback(CairoEngine engine, SqlExecutionContext context, String sql, String factoryName, String method, int signalAt, Thread ownerThread, WorkerPoolMode workerMode, boolean hasMasterFilter, boolean isTimeout, AtomicLong millis) {
            this.engine = engine;
            this.context = context;
            this.sql = sql;
            this.factoryName = factoryName;
            this.method = method;
            this.signalAt = signalAt;
            this.ownerThread = ownerThread;
            this.workerMode = workerMode;
            this.hasMasterFilter = hasMasterFilter;
            this.isTimeout = isTimeout;
            this.millis = millis;
        }

        @Override
        public boolean onGet(Record record, int count) {
            tracker = context.getMemoryTracker();
            if (isArmed && count == signalAt) {
                Assert.assertTrue(method, StackWalker.getInstance().walk(stream -> stream.anyMatch(frame -> frame.getMethodName().equals(method))));
                if (workerMode != null) {
                    Assert.assertNotSame(ownerThread, Thread.currentThread());
                    Assert.assertEquals(workerMode == WorkerPoolMode.FIBER_HOST, Fiber.current() != null);
                    String reducer = hasMasterFilter ? "filterAndReduce" : "reduce";
                    Assert.assertTrue(reducer, StackWalker.getInstance().walk(stream -> stream.anyMatch(frame -> frame.getMethodName().equals(reducer) && frame.getClassName().endsWith(factoryName))));
                }
                hasSignalled = true;
                if (isTimeout) {
                    millis.set(2000);
                } else {
                    LongList ids = new LongList();
                    engine.getQueryRegistry().getEntryIds(ids);
                    for (int i = 0; i < ids.size(); i++) {
                        long id = ids.getQuick(i);
                        var entry = engine.getQueryRegistry().getEntry(id);
                        if (entry != null && entry.getQuery().toString().equals(sql)) {
                            Assert.assertTrue(engine.getQueryRegistry().cancel(id, context));
                            return false;
                        }
                    }
                    Assert.fail("active query not found");
                }
            }
            return false;
        }
    }

    private static class OwnerBreaker extends NetworkSqlExecutionCircuitBreaker {
        private final Thread ownerThread;

        OwnerBreaker(CairoEngine engine, SqlExecutionCircuitBreakerConfiguration configuration, Thread ownerThread) {
            super(engine, configuration);
            this.ownerThread = ownerThread;
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
            Assert.assertSame(ownerThread, Thread.currentThread());
            super.statefulThrowExceptionIfTripped();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            Assert.assertSame(ownerThread, Thread.currentThread());
            super.statefulThrowExceptionIfTrippedNoThrottle();
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            Assert.assertSame(ownerThread, Thread.currentThread());
            super.statefulThrowExceptionIfTrippedTimeThrottled();
        }
    }
}
