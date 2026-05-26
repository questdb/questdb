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

package io.questdb.test.cairo.fuzz;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.engine.window.StreamingLeadEquivalence;
import org.junit.Test;

import java.util.Random;

/**
 * Fuzz and edge-case equivalence tests for the streaming-LEAD path. Each test generates input
 * data and/or queries deterministically from a seed and asserts that the CACHED path (flag off,
 * source of truth) and the STREAMING path (flag on, Phase 6.1 dispatch) produce identical row
 * multisets. Failures include the seed in the assertion message for reproducibility.
 * <p>
 * Categories:
 * <ul>
 *     <li>Edge cases — empty table, single row, single partition, large lookahead, partition cap.</li>
 *     <li>Data types — INT, DOUBLE, FLOAT, DATE, TIMESTAMP, DECIMAL through LEAD/LAG.</li>
 *     <li>Re-execution — same factory run repeatedly to catch toTop / partition-map clear bugs.</li>
 *     <li>Random fuzz — randomised row counts, partition cardinalities, lookahead values, and
 *         OVER ORDER BY directions, exercising both single-function and mixed-function shapes.</li>
 * </ul>
 */
public class StreamingLeadFuzzTest extends AbstractCairoTest {

    private static final String[] LEAD_LAG_FN = {"lag", "lead"};
    private static final String[] ORDER_DIR = {"asc", "desc"};

    @Test
    public void testEdgeCaseEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            // No rows inserted. Both paths should emit just a header.
            String[] queries = {
                    "select x, ts, lead(x, 1) over () as lx from t",
                    "select x, sym, lag(x, 1) over (partition by sym order by ts desc) as lx from t",
                    "select x, ts, lag(x, 1) over (order by ts desc) as l, lead(x, 1) over (order by ts desc) as ld from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "empty-table");
            }
        });
    }

    @Test
    public void testEdgeCaseSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (42, 'A', 1000)");

            String[] queries = {
                    "select x, ts, lead(x, 1) over () as lx from t",
                    "select x, ts, lead(x, 7) over () as lx from t",
                    "select x, sym, lag(x, 1) over (partition by sym order by ts desc) as lx from t",
                    "select x, ts, lag(x, 1) over (order by ts desc) as l, lead(x, 1) over (order by ts desc) as ld from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "single-row");
            }
        });
    }

    @Test
    public void testEdgeCaseSinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select x, 'ONLY' as sym, (1000_000L * x)::timestamp from long_sequence(100)"
            );

            String[] queries = {
                    "select x, sym, lead(x, 1) over (partition by sym) as lx from t",
                    "select x, sym, lag(x, 1) over (partition by sym order by ts desc) as lx from t",
                    "select x, sym, lead(x, 5) over (partition by sym order by ts asc) as lx from t",
                    "select x, sym, " +
                            "lag(x, 1) over (partition by sym order by ts desc) as l, " +
                            "lead(x, 1) over (partition by sym order by ts desc) as ld from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "single-partition");
            }
        });
    }

    @Test
    public void testEdgeCaseManyPartitions() throws Exception {
        // 500 distinct symbols, each appearing 4 times. Exercises the per-partition ring buffer
        // map at moderate scale without tripping the default partition cap.
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select x, ('S' || (x % 500))::symbol as sym, (1000_000L * x)::timestamp from long_sequence(2000)"
            );

            String[] queries = {
                    "select x, sym, lead(x, 1) over (partition by sym) as lx from t",
                    "select x, sym, lag(x, 1) over (partition by sym order by ts desc) as lx from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "many-partitions=500");
            }
        });
    }

    @Test
    public void testEdgeCaseLargeLookahead() throws Exception {
        // Lookahead 30 with a single LEAD function -> ringCapacity 31. Single-function so
        // ringCap * leadCount = 31 <= 64. Validates the pending-bit-mask layout near its upper
        // bound (each slot consumes one bit).
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select x, 'A' as sym, (1000_000L * x)::timestamp from long_sequence(50)"
            );

            // Single LEAD with various large lookaheads.
            for (int k : new int[]{2, 5, 10, 15, 20, 30}) {
                String sql = "select x, ts, lead(x, " + k + ") over () as lx from t";
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql,
                        "large-lookahead=" + k);
            }

            // Single LAG with various lookaheads + DESC ordering (normalises to LEAD ASC).
            for (int k : new int[]{2, 5, 10, 20}) {
                String sql = "select x, ts, lag(x, " + k + ") over (order by ts desc) as lx from t";
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql,
                        "large-lookahead-desc=" + k);
            }
        });
    }

    @Test
    public void testEdgeCaseMixedLookaheads() throws Exception {
        // Mixed lookaheads in the same query: lead(x,2) + lead(x,5). ringCap=6, leadCount=2,
        // product=12 <= 64. Validates per-slot bit layout when each slot has multiple LEAD bits.
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select x, 'A' as sym, (1000_000L * x)::timestamp from long_sequence(30)"
            );

            String[] queries = {
                    "select x, ts, lead(x, 2) over () as l2, lead(x, 5) over () as l5 from t",
                    "select x, ts, lead(x, 1) over () as l1, lead(x, 7) over () as l7 from t",
                    "select x, ts, " +
                            "lag(x, 1) over (order by ts desc) as a, " +
                            "lag(x, 3) over (order by ts desc) as b from t",
                    "select x, sym, " +
                            "lead(x, 2) over (partition by sym) as l2, " +
                            "lead(x, 5) over (partition by sym) as l5 from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "mixed-lookahead");
            }
        });
    }

    @Test
    public void testEdgeCaseNullsInInput() throws Exception {
        // Source data contains NULL x values. LEAD/LAG must propagate NULLs through to the output
        // identically in both paths (this is RESPECT NULLS — the default).
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select " +
                            "case when x % 4 = 0 then cast(null as long) else x end, " +
                            "case when x % 2 = 0 then 'A' else 'B' end, " +
                            "(1000_000L * x)::timestamp " +
                            "from long_sequence(40)"
            );

            String[] queries = {
                    "select x, ts, lead(x, 1) over () as lx from t",
                    "select x, sym, lag(x, 1) over (partition by sym order by ts desc) as lx from t",
                    "select x, ts, lead(x, 3) over (order by ts asc) as lx from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "null-input");
            }
        });
    }

    @Test
    public void testFuzzAllLeadSingleFunction() throws Exception {
        // Random rowCount, partition cardinality, lookahead, sometimes PARTITION BY, sometimes
        // ORDER BY ts (matching or opposing scan direction). Single LEAD function -> always
        // dispatches to streaming under the cost-model heuristic.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < 100; iter++) {
                long seed = 0xC0FFEE_5EEDL + iter;
                Random r = new Random(seed);
                try {
                    runFuzzCase(r, seed, true, false);
                } catch (Throwable t) {
                    throw new AssertionError("fuzz failure all-LEAD seed=0x" + Long.toHexString(seed), t);
                }
            }
        });
    }

    @Test
    public void testFuzzAllLagDescNormalised() throws Exception {
        // Random single LAG with OVER ORDER BY ts DESC -> normalises to LEAD ASC. Validates the
        // Phase 4 path under random shapes/sizes.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < 100; iter++) {
                long seed = 0xDEADBEEF_5EEDL + iter;
                Random r = new Random(seed);
                try {
                    runFuzzCase(r, seed, false, true);
                } catch (Throwable t) {
                    throw new AssertionError("fuzz failure LAG-DESC seed=0x" + Long.toHexString(seed), t);
                }
            }
        });
    }

    @Test
    public void testFuzzDualLeadDifferentLookaheads() throws Exception {
        // Specifically targets the multi-LEAD + tail-backfill shape (Q9 family) that caught the
        // flush-overwrite bug. Two LEAD functions with random different lookaheads, single
        // partition or partitioned, random row counts including small tables where the last few
        // rows depend on tail backfills not being overwritten by flush defaults.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < 80; iter++) {
                long seed = 0xD0A1_F00DL + iter;
                Random r = new Random(seed);
                try {
                    String table = "tdual_" + Long.toHexString(seed);
                    int rows = r.nextInt(80);
                    int partitions = 1 + r.nextInt(5);
                    int k1 = 1 + r.nextInt(4);
                    int k2 = 1 + r.nextInt(7);
                    if (k1 == k2) {
                        k2 = k1 + 1;
                    }
                    // Ensure ringCap * leadCount <= 64. ringCap = max(k1,k2)+1, leadCount=2.
                    int ringCap = Math.max(k1, k2) + 1;
                    if (ringCap * 2 > 64) {
                        continue;
                    }
                    boolean partitioned = r.nextBoolean();
                    String overClause = partitioned ? "(partition by sym)" : "()";
                    execute("create table " + table
                            + " (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
                    if (rows > 0) {
                        execute(buildInsertLong(table, rows, partitions, r.nextBoolean()));
                    }
                    String sql = "select x, sym, "
                            + "lead(x, " + k1 + ") over " + overClause + " as l_a, "
                            + "lead(x, " + k2 + ") over " + overClause + " as l_b "
                            + "from " + table;
                    String ctxMsg = "DUAL-LEAD seed=0x" + Long.toHexString(seed)
                            + " rows=" + rows + " partitions=" + partitions
                            + " k1=" + k1 + " k2=" + k2 + " partitioned=" + partitioned;
                    StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, ctxMsg);
                } catch (Throwable t) {
                    throw new AssertionError("fuzz failure DUAL-LEAD seed=0x" + Long.toHexString(seed), t);
                }
            }
        });
    }

    @Test
    public void testFuzzBackwardBaseScan() throws Exception {
        // Outer ORDER BY ts DESC reverses the base scan direction. Phase 4 normalisation flips its
        // mismatch detection so OVER ORDER BY ts ASC (which would otherwise match forward scan)
        // now opposes BACKWARD scan and normalises to LAG. Random over-order direction toggles
        // between dismissed and normalised cases.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < 50; iter++) {
                long seed = 0xBACC0001_5EEDL + iter;
                Random r = new Random(seed);
                try {
                    String table = "tback_" + Long.toHexString(seed);
                    int rows = r.nextInt(100);
                    int partitions = 1 + r.nextInt(4);
                    int lookahead = 1 + r.nextInt(5);
                    String dir = ORDER_DIR[r.nextInt(ORDER_DIR.length)];
                    String fn = LEAD_LAG_FN[r.nextInt(LEAD_LAG_FN.length)];
                    boolean partitioned = r.nextBoolean();
                    String overInner = partitioned
                            ? "(partition by sym order by ts " + dir + ")"
                            : "(order by ts " + dir + ")";
                    execute("create table " + table
                            + " (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
                    if (rows > 0) {
                        execute(buildInsertLong(table, rows, partitions, r.nextBoolean()));
                    }
                    String sql = "select x, sym, " + fn + "(x, " + lookahead + ") over " + overInner
                            + " as v from " + table + " order by ts desc";
                    String ctxMsg = "BACKWARD-SCAN seed=0x" + Long.toHexString(seed)
                            + " rows=" + rows + " fn=" + fn + " lookahead=" + lookahead
                            + " dir=" + dir + " partitioned=" + partitioned;
                    StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, ctxMsg);
                } catch (Throwable t) {
                    throw new AssertionError("fuzz failure BACKWARD-SCAN seed=0x" + Long.toHexString(seed), t);
                }
            }
        });
    }

    @Test
    public void testFuzzMixedLagLeadDesc() throws Exception {
        // Random mixed LAG+LEAD with OVER ORDER BY ts DESC -> normalises both, streams.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < 80; iter++) {
                long seed = 0xBADF00D_5EEDL + iter;
                Random r = new Random(seed);
                try {
                    String table = "tmix_" + iter;
                    int rows = 3 + r.nextInt(120);
                    int partitions = 1 + r.nextInt(8);
                    boolean nulls = r.nextBoolean();
                    String createSql = "create table " + table
                            + " (x long, sym symbol, ts timestamp) timestamp(ts) partition by day";
                    String insertSql = buildInsertLong(table, rows, partitions, nulls);
                    int lookahead = 1 + r.nextInt(4);
                    boolean partition = r.nextBoolean();
                    String overClause = partition
                            ? "(partition by sym order by ts desc)"
                            : "(order by ts desc)";
                    String sql = "select x, sym, "
                            + "lag(x, " + lookahead + ") over " + overClause + " as l, "
                            + "lead(x, " + lookahead + ") over " + overClause + " as ld "
                            + "from " + table;
                    execute(createSql);
                    execute(insertSql);
                    String ctxMsg = "MIXED-LAG-LEAD-DESC seed=0x" + Long.toHexString(seed)
                            + " rows=" + rows + " partitions=" + partitions
                            + " lookahead=" + lookahead + " partition=" + partition + " nulls=" + nulls;
                    StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, ctxMsg);
                } catch (Throwable t) {
                    throw new AssertionError("fuzz failure MIXED-LAG-LEAD-DESC seed=0x" + Long.toHexString(seed), t);
                }
            }
        });
    }

    @Test
    public void testFuzzReExecution() throws Exception {
        // Re-execute the same factory multiple times in succession. Catches partition-map clear
        // bugs and mem-arena reset bugs that would only show after the first invocation. Compares
        // each iteration's output against cached's deterministic output.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < 10; iter++) {
                long seed = 0xFEEDBEEF_5EEDL + iter;
                Random r = new Random(seed);
                String table = "trex_" + iter;
                int rows = 20 + r.nextInt(80);
                int partitions = 1 + r.nextInt(4);
                execute("create table " + table
                        + " (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
                execute(buildInsertLong(table, rows, partitions, false));
                String sql = "select x, sym, lead(x, " + (1 + r.nextInt(3))
                        + ") over (partition by sym) as lx from " + table;
                // Run 3 times under STREAMING; all three must equal cached.
                for (int rep = 0; rep < 3; rep++) {
                    String ctxMsg = "RE-EXEC seed=0x" + Long.toHexString(seed) + " rep=" + rep;
                    StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, ctxMsg);
                }
            }
        });
    }

    @Test
    public void testTypeDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (d date, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select " +
                            "cast(x * 100 as date) as d, " +
                            "case when x % 3 = 0 then 'A' else 'B' end as sym, " +
                            "(1000_000L * x)::timestamp as ts " +
                            "from long_sequence(30)"
            );
            String[] queries = {
                    "select d, ts, lead(d, 1) over () as ld from t",
                    "select d, sym, lag(d, 1) over (partition by sym order by ts desc) as ld from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "type-date");
            }
        });
    }

    @Test
    public void testTypeDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (d double, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select " +
                            "x * 1.5 as d, " +
                            "case when x % 3 = 0 then 'A' else 'B' end as sym, " +
                            "(1000_000L * x)::timestamp as ts " +
                            "from long_sequence(40)"
            );
            String[] queries = {
                    "select d, ts, lead(d, 1) over () as ld from t",
                    "select d, sym, lag(d, 1) over (partition by sym order by ts desc) as ld from t",
                    "select d, ts, lead(d, 3) over (order by ts asc) as ld from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "type-double");
            }
        });
    }

    @Test
    public void testTypeFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (f float, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select " +
                            "(x * 0.25)::float as f, " +
                            "case when x % 3 = 0 then 'A' else 'B' end as sym, " +
                            "(1000_000L * x)::timestamp as ts " +
                            "from long_sequence(30)"
            );
            String[] queries = {
                    "select f, ts, lead(f, 1) over () as ld from t",
                    "select f, sym, lag(f, 1) over (partition by sym order by ts desc) as ld from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "type-float");
            }
        });
    }

    @Test
    public void testTypeInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (i int, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select " +
                            "x::int as i, " +
                            "case when x % 3 = 0 then 'A' else 'B' end as sym, " +
                            "(1000_000L * x)::timestamp as ts " +
                            "from long_sequence(40)"
            );
            String[] queries = {
                    "select i, ts, lead(i, 1) over () as ld from t",
                    "select i, sym, lag(i, 1) over (partition by sym order by ts desc) as ld from t",
                    "select i, ts, lead(i, 2) over (order by ts asc) as ld from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "type-int");
            }
        });
    }

    @Test
    public void testTypeTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t select " +
                            "case when x % 3 = 0 then 'A' else 'B' end as sym, " +
                            "(1000_000L * x)::timestamp as ts " +
                            "from long_sequence(30)"
            );
            String[] queries = {
                    "select ts, lead(ts, 1) over () as lts from t",
                    "select sym, ts, lag(ts, 1) over (partition by sym order by ts desc) as lts from t",
            };
            for (String sql : queries) {
                StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, "type-timestamp");
            }
        });
    }

    private static String buildInsertLong(String table, int rows, int partitions, boolean withNulls) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(table).append(" select ");
        if (withNulls) {
            sb.append("case when x % 5 = 0 then cast(null as long) else x end, ");
        } else {
            sb.append("x, ");
        }
        sb.append("('S' || (x % ").append(partitions).append("))::symbol as sym, ");
        sb.append("(1000_000L * x)::timestamp as ts ");
        sb.append("from long_sequence(").append(rows).append(")");
        return sb.toString();
    }

    /**
     * Generates a random query against a fresh randomly-seeded table and asserts CACHED and
     * STREAMING produce equivalent outputs. The {@code allLead} flag picks LEAD only (always
     * streams via the all-LEAD branch); {@code lagDescOnly} picks LAG with OVER ORDER BY ts DESC
     * (streams via Phase 4 normalisation).
     */
    private void runFuzzCase(Random r, long seed, boolean allLead, boolean lagDescOnly) throws Exception {
        String table = "tfz_" + Long.toHexString(seed);
        int rows = r.nextInt(150);                // 0..149, includes empty
        int partitions = 1 + r.nextInt(7);        // 1..7
        boolean withNulls = r.nextBoolean();
        boolean partitioned = r.nextBoolean();
        int lookahead = 1 + r.nextInt(8);         // 1..8

        execute("create table " + table
                + " (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
        if (rows > 0) {
            execute(buildInsertLong(table, rows, partitions, withNulls));
        }

        String fn;
        String dir;
        boolean withOrderBy;
        if (allLead) {
            fn = "lead";
            dir = ORDER_DIR[r.nextInt(ORDER_DIR.length)];
            withOrderBy = r.nextBoolean();
        } else if (lagDescOnly) {
            fn = "lag";
            dir = "desc";
            withOrderBy = true;
        } else {
            fn = LEAD_LAG_FN[r.nextInt(LEAD_LAG_FN.length)];
            dir = ORDER_DIR[r.nextInt(ORDER_DIR.length)];
            withOrderBy = r.nextBoolean();
        }

        StringBuilder over = new StringBuilder("(");
        boolean haveContent = false;
        if (partitioned) {
            over.append("partition by sym");
            haveContent = true;
        }
        if (withOrderBy) {
            if (haveContent) {
                over.append(' ');
            }
            over.append("order by ts ").append(dir);
        }
        over.append(')');

        String sql = "select x, sym, ts, " + fn + "(x, " + lookahead + ") over " + over + " as v from " + table;
        String ctxMsg = "FUZZ seed=0x" + Long.toHexString(seed)
                + " rows=" + rows + " partitions=" + partitions
                + " partitioned=" + partitioned + " orderBy=" + withOrderBy
                + " dir=" + dir + " fn=" + fn + " lookahead=" + lookahead
                + " nulls=" + withNulls;
        StreamingLeadEquivalence.assertEquivalent(engine, sqlExecutionContext, sql, ctxMsg);
    }
}
