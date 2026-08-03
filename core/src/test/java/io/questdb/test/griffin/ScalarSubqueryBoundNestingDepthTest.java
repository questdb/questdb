/*******************************************************************************
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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Resolving a {@code monotonic(ts) >= (SELECT ...)} bound compiles the sub-query speculatively,
 * because only the generated factory can say whether pruning is legal. Every decline discards that
 * compile and leaves the predicate as a residual row filter, which compiles the same sub-query
 * again - so without a cap each nesting level doubles compile time, T(k) = 2*T(k+1) = O(2^D), and
 * nothing in code generation tests the circuit breaker, making the burn uncancellable.
 *
 * <p>{@code WhereClauseParser.MAX_SPECULATIVE_SCALAR_BOUND_DEPTH} stops the speculation past a
 * small depth, which restores T(k) = T(k+1) for the tail. These tests pin the resulting bound and
 * verify that declining to prune never changes results.
 */
public class ScalarSubqueryBoundNestingDepthTest extends AbstractCairoTest {

    private static String nestedMonotonic(int depth) {
        StringBuilder sb = new StringBuilder("SELECT i, ts FROM t WHERE dateadd('h', 1, ts) >= ");
        StringBuilder tail = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("(SELECT min(ts) FROM t WHERE dateadd('h', 1, ts) >= ");
            tail.append(')');
        }
        sb.append("'2020-06-02T00:00:00.000000Z'").append(tail);
        return sb.toString();
    }

    // ts = (subquery) OR <sibling that declines extraction>, nested
    private static String nestedOr(int depth) {
        return "SELECT i, ts FROM t WHERE ts = " + orBound(depth) + " OR ts = rnd_int()";
    }

    private static String orBound(int depth) {
        StringBuilder sb = new StringBuilder();
        StringBuilder tail = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("(SELECT max(ts) FROM t WHERE ts = ");
            tail.append(" OR ts = rnd_int())");
        }
        return sb.append("'2020-06-01T00:00:00.000000Z'").append(tail).toString();
    }

    // ts BETWEEN (subquery) AND <boundary that fails to translate>, nested
    private static String nestedBetween(int depth) {
        return "SELECT i, ts FROM t WHERE ts BETWEEN " + betweenBound(depth) + " AND dateadd('h', 1, ts)";
    }

    private static String betweenBound(int depth) {
        StringBuilder sb = new StringBuilder();
        StringBuilder tail = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("(SELECT max(ts) FROM t WHERE ts BETWEEN ");
            tail.append(" AND dateadd('h', 1, ts))");
        }
        return sb.append("'2020-06-01T00:00:00.000000Z'").append(tail).toString();
    }

    private void createTable() throws Exception {
        execute("CREATE TABLE t (i INT, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES (1, '2020-06-01T00:00:00.000000Z', '2020-06-01T00:00:00.000000Z')," +
                " (2, '2020-06-02T00:00:00.000000Z', '2020-06-02T00:00:00.000000Z')," +
                " (3, '2020-06-03T00:00:00.000000Z', '2020-06-03T00:00:00.000000Z')");
    }

    /**
     * A deeply nested chain must stay cheap to compile. Before the cap this grew as 2^D: ~4 s at
     * depth 14 and ~76 s at depth 17 on a developer laptop, with no way to cancel it.
     */
    @Test
    public void testDeepNestingCompilesInBoundedTime() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // warm up so the measurement excludes first-compile/class-loading noise
            for (int i = 0; i < 3; i++) {
                Misc.free(select(nestedMonotonic(2)));
            }
            final long start = System.nanoTime();
            try (RecordCursorFactory f = select(nestedMonotonic(20))) {
                Assert.assertNotNull(f);
            }
            final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            // generous: the point is bounded-vs-exponential, not a precise timing pin
            Assert.assertTrue("depth-20 compile took " + elapsedMs + "ms, expected bounded", elapsedMs < 5_000);
        });
    }

    /**
     * Declining to prune must never change the answer, at any depth - above or below the cap.
     * Each level takes min(ts) of the rows at or after the previous bound, so the bound is stable
     * at 2020-06-02 and every depth returns the same two rows.
     */
    @Test
    public void testResultsAreIdenticalAcrossTheCap() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String expected = "i\tts\n" +
                    "2\t2020-06-02T00:00:00.000000Z\n" +
                    "3\t2020-06-03T00:00:00.000000Z\n";
            // depths straddling MAX_SPECULATIVE_SCALAR_BOUND_DEPTH (4): below, at, and well above
            for (int depth : new int[]{1, 2, 3, 4, 5, 6, 9}) {
                assertQuery(nestedMonotonic(depth))
                        .timestamp("ts")
                        .returns(expected);
            }
        });
    }

    /**
     * A declined bound hands its compiled sub-query to the residual filter instead of freeing it.
     * The residual owns and evaluates that function itself - nothing is frozen - so results must be
     * identical to generating it a second time, and the hand-off must not leak the open factory.
     * <p>
     * {@code bounds} carries two rows, so the bound is not a provably stable single-row scan and the
     * stability gate declines, which is the path that parks the compile. Repeating the query proves
     * the slot is re-armed per compilation rather than serving a stale factory.
     */
    @Test
    public void testDeclinedBoundReusesCompileWithoutChangingResults() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE bounds (lo TIMESTAMP)");
            execute("INSERT INTO bounds VALUES ('2020-06-02T00:00:00.000000Z')");

            final String expected = "i\tts\n" +
                    "2\t2020-06-02T00:00:00.000000Z\n" +
                    "3\t2020-06-03T00:00:00.000000Z\n";
            // ORDER BY ... LIMIT 1 over a plain table is not a provably stable scan, so the stability
            // gate declines and the compiled bound is parked for the residual rather than freed
            final String query = "SELECT i, ts FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT lo FROM bounds ORDER BY lo DESC LIMIT 1)";

            // compiled repeatedly: the slot must be re-armed per compilation, never serve a stale factory
            for (int i = 0; i < 3; i++) {
                assertQuery(query).timestamp("ts").returns(expected);
            }

            // same predicate over a non-designated column never takes the monotonic path at all,
            // so it is an independent oracle for the reused-compile answer
            assertQuery("SELECT i, ts FROM t WHERE dateadd('h', 1, ts2) >= " +
                    "(SELECT lo FROM bounds ORDER BY lo DESC LIMIT 1)")
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    /**
     * The OR channel speculatively compiles a sub-query too, and its compile is discarded whenever
     * any sibling disjunct turns out not to be extractable - here {@code ts = rnd_int()}, which
     * passes the structural pre-screen but is not a timestamp - so the whole OR extraction rolls
     * back and the predicate stays a residual filter. Uncharged, that doubled per nesting level:
     * ~1.1 s at depth 11 and unusable beyond.
     */
    @Test
    public void testOrChannelCompilesInBoundedTime() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int i = 0; i < 3; i++) {
                Misc.free(select(nestedOr(2)));
            }
            final long start = System.nanoTime();
            try (RecordCursorFactory f = select(nestedOr(20))) {
                Assert.assertNotNull(f);
            }
            final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            Assert.assertTrue("depth-20 OR compile took " + elapsedMs + "ms, expected bounded", elapsedMs < 5_000);
        });
    }

    /**
     * Same for the BETWEEN channel, whose sub-query boundary compile is discarded when the OTHER
     * boundary fails to translate - here a column expression, which is neither constant nor
     * runtime-constant. Uncharged, ~2.6 s at depth 13.
     */
    @Test
    public void testBetweenChannelCompilesInBoundedTime() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int i = 0; i < 3; i++) {
                Misc.free(select(nestedBetween(2)));
            }
            final long start = System.nanoTime();
            try (RecordCursorFactory f = select(nestedBetween(20))) {
                Assert.assertNotNull(f);
            }
            final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            Assert.assertTrue("depth-20 BETWEEN compile took " + elapsedMs + "ms, expected bounded", elapsedMs < 5_000);
        });
    }

    /**
     * Declining to translate, and reusing the parked compile, must not change what the BETWEEN
     * returns. {@code max(ts)} is 2020-06-03, so only that row falls in [max(ts), ts + 1h].
     */
    @Test
    public void testBetweenChannelResultsUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String expected = "i\tts\n3\t2020-06-03T00:00:00.000000Z\n";
            for (int depth : new int[]{1, 2, 4, 5, 8}) {
                assertQuery("SELECT i, ts FROM t WHERE ts BETWEEN " + betweenBound(depth) + " AND dateadd('h', 1, ts)")
                        .timestamp("ts")
                        .returns(expected);
            }
        });
    }

    /**
     * Ordinary sub-select nesting must not consume the speculation budget: the bound below sits at
     * scalar-bound depth 0 no matter how many plain derived tables wrap it, so pruning is retained.
     */
    @Test
    public void testPlainSubSelectNestingKeepsPruning() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            String inner = "SELECT i, ts FROM t WHERE dateadd('h', 1, ts) >= (SELECT min(ts) FROM t)";
            for (int i = 0; i < 8; i++) {
                inner = "SELECT i, ts FROM (" + inner + ")";
            }
            assertQuery(inner)
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("i\tts\n" +
                            "1\t2020-06-01T00:00:00.000000Z\n" +
                            "2\t2020-06-02T00:00:00.000000Z\n" +
                            "3\t2020-06-03T00:00:00.000000Z\n");
        });
    }
}
