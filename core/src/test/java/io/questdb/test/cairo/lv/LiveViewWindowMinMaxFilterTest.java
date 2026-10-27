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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.AsyncWindowMinMaxFilterRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code Async Window Min/Max Filter} over a live view: its base reads the view's disk rows and its
 * pinned in-memory slot as page frames. The output must be the serial plan's, and a refresh that
 * lands while the query runs must not tear it: every pass reads the one page frame cursor, which
 * pins the slot for its life.
 */
public class LiveViewWindowMinMaxFilterTest extends AbstractLiveViewTest {
    private static final long CYCLE_2_START = 1_700_000_000_000_000L + 5_000_000L;
    private static final long DATA_START = 1_700_000_000_000_000L;

    @Test
    public void testLiveViewBase() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                createSeamSplitLv(job, "");
                for (String query : new String[]{min(""), max("")}) {
                    final String expected = run(query, false);
                    Assert.assertTrue(query, expected.split("\n").length > 2);
                    Assert.assertEquals(query, expected, run(query, true));
                }
            }
        });
    }

    @Test
    public void testLiveViewBaseRefreshedMidQuery() throws Exception {
        // a refresh lands from inside the query, at each consultation of the circuit breaker in
        // turn: the output is the serial plan's before the refresh or after it, never a mix
        int fireAt = 1;
        int midQuery = 0;
        for (; fireAt <= 64; fireAt++) {
            final int fa = fireAt;
            final int[] outcome = {0};
            assertMemoryLeak(() -> {
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    // each round has its own view
                    final String n = String.valueOf(fa);
                    createSeamSplitLv(job, n);
                    final String query = min(n);
                    final String before = run(query, false);
                    final CommittingCircuitBreaker cb = new CommittingCircuitBreaker(engine, fa, () -> {
                        // a lower minimum for A, near ties for B, and a new key
                        execute("INSERT INTO base" + n + " (ts, k, p) VALUES " +
                                "(" + (CYCLE_2_START + 1_000_001) + ", 'A', -5.0), " +
                                "(" + (CYCLE_2_START + 1_000_002) + ", 'B', -0.99999999994), " +
                                "(" + (CYCLE_2_START + 1_000_003) + ", 'B', -1.0), " +
                                "(" + (CYCLE_2_START + 1_000_004) + ", 'N', 3.0)");
                        drainWalQueue();
                        setCurrentMicros(1_000_000L);
                        drainJob(job);
                        drainWalQueue();
                    });
                    final String actual;
                    try {
                        withCircuitBreaker(cb);
                        sqlExecutionContext.setParallelWindowMinMaxRewriteEnabled(true);
                        try (RecordCursorFactory factory = select(query)) {
                            Assert.assertNotNull(find(factory));
                            cb.arm();
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                actual = print(cursor, factory);
                            }
                        }
                    } finally {
                        withCircuitBreaker(circuitBreaker);
                        Misc.free(cb);
                    }
                    final String after = run(query, false);
                    if (!actual.equals(before) && !actual.equals(after)) {
                        Assert.fail("refresh at breaker call " + fa + ": the output matches no snapshot\nbefore:\n" + before + "after:\n" + after + "actual:\n" + actual);
                    }
                    outcome[0] = !cb.fired ? 0 : actual.equals(before) && !before.equals(after) ? 2 : 1;
                    execute("DROP LIVE VIEW lv" + n);
                    execute("DROP TABLE base" + n);
                    drainWalQueue();
                }
            });
            if (outcome[0] == 0) {
                break;
            }
            if (outcome[0] == 2) {
                midQuery++;
            }
        }
        Assert.assertTrue("refreshes that landed mid-query: " + midQuery + " of " + (fireAt - 1), midQuery > 0);
    }

    private static AsyncWindowMinMaxFilterRecordCursorFactory find(RecordCursorFactory factory) {
        while (factory != null) {
            if (factory instanceof AsyncWindowMinMaxFilterRecordCursorFactory minMax) {
                return minMax;
            }
            factory = factory.getBaseFactory();
        }
        return null;
    }

    private static String max(String n) {
        return "select ts, k, p, mx from (select ts, k, p, max(p) over (partition by k) mx from lv" + n + ") where p = mx";
    }

    private static String min(String n) {
        return "select ts, k, p, mn from (select ts, k, p, min(p) over (partition by k) mn from lv" + n + ") where p = mn";
    }

    private static String print(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        return sink.toString();
    }

    private static String run(String query, boolean rewrite) throws Exception {
        sqlExecutionContext.setParallelWindowMinMaxRewriteEnabled(rewrite);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertEquals(query, rewrite, find(factory) != null);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final String first = print(cursor, factory);
                cursor.toTop();
                Assert.assertEquals(query, first, print(cursor, factory));
                return first;
            }
        }
    }

    private static void withCircuitBreaker(SqlExecutionCircuitBreaker cb) {
        ((SqlExecutionContextImpl) sqlExecutionContext).with(
                sqlExecutionContext.getSecurityContext(),
                sqlExecutionContext.getBindVariableService(),
                sqlExecutionContext.getRandom(),
                sqlExecutionContext.getRequestFd(),
                cb
        );
    }

    // A view whose published slot holds only the second cycle's rows while disk holds both, so
    // that the base's frames cross the seam; see LiveViewInMemReadTest.createSeamSplitLv().
    private void createSeamSplitLv(LiveViewRefreshJob job, String n) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        execute("CREATE TABLE base" + n + " (ts TIMESTAMP, k SYMBOL, p DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv" + n + " FLUSH EVERY 100ms IN MEMORY 1s START FROM NOW AS " +
                "SELECT ts, k, p, count(*) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base" + n);
        execute("INSERT INTO base" + n + " SELECT (" + DATA_START + " + x)::timestamp, rnd_symbol('A', 'B', 'C'), (x * 7 % 13)::double FROM long_sequence(300)");
        drainWalQueue();
        setCurrentMicros(250_000L);
        drainJob(job);
        // B's near ties sit in the slot: its partition replays
        execute("INSERT INTO base" + n + " SELECT (" + CYCLE_2_START + " + x)::timestamp, rnd_symbol('A', 'B', 'C'), (x * 5 % 11)::double FROM long_sequence(200)");
        execute("INSERT INTO base" + n + " (ts, k, p) VALUES (" + (CYCLE_2_START + 500) + ", 'B', 0.00000000006), (" + (CYCLE_2_START + 501) + ", 'B', 0.0)");
        drainWalQueue();
        setCurrentMicros(500_000L);
        drainJob(job);
        drainWalQueue();
    }

    /**
     * Refreshes the view from inside the query: at the {@code fireAt}-th consultation of the
     * circuit breaker after {@link #arm()}.
     */
    private static class CommittingCircuitBreaker extends NetworkSqlExecutionCircuitBreaker {
        private final AtomicInteger calls = new AtomicInteger(Integer.MIN_VALUE);
        private final Commit commit;
        private final int fireAt;
        private volatile boolean fired;

        CommittingCircuitBreaker(CairoEngine engine, int fireAt, Commit commit) {
            super(engine, new DefaultSqlExecutionCircuitBreakerConfiguration());
            this.fireAt = fireAt;
            this.commit = commit;
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            super.statefulThrowExceptionIfTrippedNoThrottle();
            tick();
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            tick();
        }

        void arm() {
            calls.set(0);
        }

        private void tick() {
            if (calls.incrementAndGet() == fireAt) {
                try {
                    commit.run();
                    fired = true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @FunctionalInterface
    private interface Commit {
        void run() throws Exception;
    }
}
