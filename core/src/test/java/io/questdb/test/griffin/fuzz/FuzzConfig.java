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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.Rnd;

/**
 * Lightweight knobs driving table and query budgets. Query count can be
 * overridden via {@code -Dquestdb.fuzz.queries=N} without touching the
 * test itself, so a developer can crank it up when hunting issues.
 */
public final class FuzzConfig {
    public static final String DIFF_JIT_PROP = "questdb.fuzz.diff.jit";
    public static final String DIFF_SHADOW_PROP = "questdb.fuzz.diff.shadow";
    public static final String DUMP_PROP = "questdb.fuzz.dump";
    public static final String FAULTS_PROP = "questdb.fuzz.faults";
    public static final String FAULT_PARALLEL_PROP = "questdb.fuzz.fault.parallel";
    public static final String FAULT_PCT_PROP = "questdb.fuzz.fault.pct";
    public static final String HORIZON_JOIN_PROP = "questdb.fuzz.horizonjoin";
    public static final String LATEST_ON_PROP = "questdb.fuzz.lateston";
    public static final String QUERIES_PROP = "questdb.fuzz.queries";
    public static final String VERIFY_CURSOR_PROP = "questdb.fuzz.verify.cursor";
    public static final String WINDOW_JOIN_PROP = "questdb.fuzz.windowjoin";
    public static final String WINDOW_PROP = "questdb.fuzz.window";
    // Queries per run when nothing overrides it, i.e. what CI executes. Sized so that every query
    // shape the run can draw clears MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR (QueryFuzzTest) and the
    // "this generator has stopped compiling" guard actually holds it. Measured queries per shape,
    // on one seed that drew a posting-indexed SYMBOL:
    //
    //   budget | GROUP_BY SAMPLE_BY SIMPLE WINDOW LATEST_ON POSTING HORIZON_JOIN TEMPORAL_JOIN WINDOW_JOIN
    //      100 |       16        31     11      7         7       0            3             3           6
    //     1000 |      187       178    124    134        43      75           42            39          34
    //
    // At 100 only SAMPLE_BY reached the floor of 25, so the guard was dormant for the other eight
    // shapes - every join shape among them. At 1000 all nine clear it on this seed, for ~2s more
    // (1.7s -> 3.8s), which is noise next to the build it rides on.
    //
    // Read POSTING's 0 -> 75 as conditional on the seed: PostingClause generates only when the
    // run's random schema draw put a posting-indexed SYMBOL on some table, which
    // FuzzTableFactory.assignIndexes decides per SYMBOL column. On a run that drew none - 7 of the
    // 40 measured - no budget lifts POSTING off zero and it reports 0/0 whatever the budget.
    // QueryFuzzTest checks that precondition before it asserts a shape generated anything, so those
    // runs stay green instead of failing a working generator.
    private static final int DEFAULT_NUM_QUERIES = 1_000;

    private final boolean isDiffJitEnabled;
    private final boolean isDiffShadowEnabled;
    private final boolean isFaultInjectionEnabled;
    private final boolean isHorizonJoinEnabled;
    private final boolean isLatestOnEnabled;
    private final boolean isParallelFaultEnabled;
    private final boolean isVerifyCursorEnabled;
    private final boolean isWindowEnabled;
    private final boolean isWindowJoinEnabled;
    private final String dumpPath;
    private final int faultProbabilityPct;
    private final int maxColumnsPerTable;
    private final int minColumnsPerTable;
    private final int numQueries;
    private final int numTables;
    private final int rowsPerTable;
    private final long stepMicros;
    private final String tsStart;

    public FuzzConfig(Rnd rnd) {
        // At least two: QueryGenerator gates every join shape (TEMPORAL, HORIZON, WINDOW JOIN) on
        // tables.size() >= 2, so a single-table run generated none of them at all - whatever the
        // query budget - and one run in three drew exactly one table. No shape needs a lone table
        // (the others just pick one at random), so the floor costs nothing and keeps the join
        // generators in every run.
        this.numTables = 2 + rnd.nextInt(2);
        this.rowsPerTable = 60 + rnd.nextInt(90);
        // Wide enough that one table takes a real slice off the shuffled type
        // deck FuzzTableFactory deals from, so a run covers a good part of the
        // type registry even when it builds a single table.
        this.minColumnsPerTable = 5;
        this.maxColumnsPerTable = 12;
        // 30 minutes: rowsPerTable * 30min covers 30..75 hours, so 2-4 DAY partitions.
        this.stepMicros = 30L * 60L * 1_000_000L;
        this.tsStart = "2024-01-01";
        this.numQueries = Integer.getInteger(QUERIES_PROP, DEFAULT_NUM_QUERIES);
        this.dumpPath = System.getProperty(DUMP_PROP);
        this.isDiffJitEnabled = Boolean.parseBoolean(System.getProperty(DIFF_JIT_PROP, "true"));
        this.isDiffShadowEnabled = Boolean.parseBoolean(System.getProperty(DIFF_SHADOW_PROP, "true"));
        this.isVerifyCursorEnabled = Boolean.parseBoolean(System.getProperty(VERIFY_CURSOR_PROP, "true"));
        this.isFaultInjectionEnabled = Boolean.parseBoolean(System.getProperty(FAULTS_PROP, "true"));
        this.faultProbabilityPct = Integer.getInteger(FAULT_PCT_PROP, 15);
        // On by default, like fault injection: parallel fault injection runs
        // fault-injected queries with parallel SQL execution enabled so the
        // parallel filter / GROUP BY / top-K reduce error paths get exercised by
        // the crash-and-recover oracle. All three fault types run in parallel: the
        // query loop halts the writer pool, so no background job competes (FUNCTION
        // is data-scoped, FILE is scoped to the query execution, and MALLOC's
        // process-global RSS ceiling can only be tripped by the query's own
        // allocations; see the runFuzz fault branch). Pass
        // -Dquestdb.fuzz.fault.parallel=false to run every fault serially.
        this.isParallelFaultEnabled = Boolean.parseBoolean(System.getProperty(FAULT_PARALLEL_PROP, "true"));
        // On by default, like fault injection: window-function shapes still
        // surface unfixed window-function defects, so the run goes red on the
        // seeds that hit them until those are fixed. Pass
        // -Dquestdb.fuzz.window=false to exercise the rest of the corpus.
        this.isWindowEnabled = Boolean.parseBoolean(System.getProperty(WINDOW_PROP, "true"));
        // HORIZON JOIN and WINDOW JOIN shapes share the 15% join band with the
        // temporal joins (see QueryGenerator). On by default; pass
        // -Dquestdb.fuzz.horizonjoin=false / -Dquestdb.fuzz.windowjoin=false to
        // drop either kind and give the band back to the remaining join shapes.
        this.isHorizonJoinEnabled = Boolean.parseBoolean(System.getProperty(HORIZON_JOIN_PROP, "true"));
        this.isWindowJoinEnabled = Boolean.parseBoolean(System.getProperty(WINDOW_JOIN_PROP, "true"));
        // LATEST ON shapes (latest row per PARTITION BY key) carve a band out of
        // the SIMPLE range (see QueryGenerator). On by default, like window. Pass
        // -Dquestdb.fuzz.lateston=false to drop them and give the band back to
        // SIMPLE.
        this.isLatestOnEnabled = Boolean.parseBoolean(System.getProperty(LATEST_ON_PROP, "true"));
    }

    public String getDumpPath() {
        return dumpPath;
    }

    public int getFaultProbabilityPct() {
        return faultProbabilityPct;
    }

    public int getMaxColumnsPerTable() {
        return maxColumnsPerTable;
    }

    public int getMinColumnsPerTable() {
        return minColumnsPerTable;
    }

    public int getNumQueries() {
        return numQueries;
    }

    public int getNumTables() {
        return numTables;
    }

    public int getRowsPerTable() {
        return rowsPerTable;
    }

    public long getStepMicros() {
        return stepMicros;
    }

    public String getTsStart() {
        return tsStart;
    }

    public boolean isDiffJitEnabled() {
        return isDiffJitEnabled;
    }

    public boolean isDiffShadowEnabled() {
        return isDiffShadowEnabled;
    }

    public boolean isFaultInjectionEnabled() {
        return isFaultInjectionEnabled;
    }

    public boolean isHorizonJoinEnabled() {
        return isHorizonJoinEnabled;
    }

    public boolean isLatestOnEnabled() {
        return isLatestOnEnabled;
    }

    public boolean isParallelFaultEnabled() {
        return isParallelFaultEnabled;
    }

    public boolean isVerifyCursorEnabled() {
        return isVerifyCursorEnabled;
    }

    public boolean isWindowEnabled() {
        return isWindowEnabled;
    }

    public boolean isWindowJoinEnabled() {
        return isWindowJoinEnabled;
    }
}
