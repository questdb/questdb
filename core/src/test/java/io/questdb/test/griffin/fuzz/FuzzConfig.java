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
import io.questdb.test.griffin.HashJoinBuildMode;
import io.questdb.test.griffin.HashJoinPayloadLayout;

/**
 * Lightweight knobs driving table and query budgets. Query count can be
 * overridden via {@code -Dquestdb.fuzz.queries=N} without touching the
 * test itself, so a developer can crank it up when hunting issues.
 */
public final class FuzzConfig {
    public static final String DIFF_FUSED_PROP = "questdb.fuzz.diff.fused";
    public static final String DIFF_JIT_PROP = "questdb.fuzz.diff.jit";
    public static final String DIFF_MAP_PAGE_PROP = "questdb.fuzz.diff.mappage";
    public static final String DIFF_PARALLEL_JOIN_PROP = "questdb.fuzz.diff.parallel.join";
    public static final String DIFF_SHADOW_PROP = "questdb.fuzz.diff.shadow";
    public static final String DUMP_PROP = "questdb.fuzz.dump";
    public static final String EQUI_JOIN_PROP = "questdb.fuzz.equijoin";
    public static final String FAULTS_PROP = "questdb.fuzz.faults";
    public static final String FAULT_PARALLEL_PROP = "questdb.fuzz.fault.parallel";
    public static final String FAULT_PCT_PROP = "questdb.fuzz.fault.pct";
    public static final String GROUPING_SETS_PROP = "questdb.fuzz.groupingsets";
    public static final String HASH_JOIN_BUILD_PROP = "questdb.fuzz.hashjoin.build";
    public static final String HASH_JOIN_PAYLOAD_PROP = "questdb.fuzz.hashjoin.payload";
    public static final String HASH_JOIN_PROP = "questdb.fuzz.hashjoin";
    public static final String HORIZON_JOIN_PROP = "questdb.fuzz.horizonjoin";
    public static final String LATEST_ON_PROP = "questdb.fuzz.lateston";
    public static final String PARALLEL_JOIN_CAP_PROP = "questdb.fuzz.parallel.join.cap";
    public static final String PARALLEL_JOIN_DISPATCH_PROP = "questdb.fuzz.parallel.join.dispatch";
    public static final String PARALLEL_JOIN_FRAMES_PROP = "questdb.fuzz.parallel.join.frames";
    public static final String PARALLEL_JOIN_RING_PROP = "questdb.fuzz.parallel.join.ring";
    public static final String PARQUET_ROW_GROUP_PROP = "questdb.fuzz.parquet.row.group";
    public static final String QUERIES_PROP = "questdb.fuzz.queries";
    public static final String STORAGE_PROP = "questdb.fuzz.storage";
    public static final String VERIFY_CURSOR_PROP = "questdb.fuzz.verify.cursor";
    public static final String WINDOW_JOIN_PROP = "questdb.fuzz.windowjoin";
    public static final String WINDOW_PROP = "questdb.fuzz.window";
    // Queries per run when nothing overrides it, i.e. what CI executes. Sized so that every query
    // shape the run can draw clears MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR (QueryFuzzTest) and the
    // "this generator has stopped compiling" guard actually holds it. Measured queries per shape,
    // on one seed that drew a posting-indexed SYMBOL:
    //
    //   budget | GROUP_BY SAMPLE_BY SIMPLE WINDOW LATEST_ON POSTING HORIZON_JOIN TEMPORAL_JOIN WINDOW_JOIN HASH_JOIN_GROUP_BY
    //      100 |        9        28     14      8         6       3            3             6           3                  6
    //     1000 |      120       207    131    102        49      56           43            37          44                 52
    //
    // At 100 only SAMPLE_BY reached the floor of 25, so the guard was dormant for the other nine
    // shapes - every join shape among them. At 1000 all ten clear it on this seed, for ~3s more
    // (1.9s -> 4.6s), which is noise next to the build it rides on. The same budget also gives the
    // fused on/off axis in QueryRunner enough completed comparisons for its own floor: 30 to 41
    // over ten measured default runs, against a sample floor of 25.
    //
    // Read POSTING's figures as conditional on the seed: PostingClause generates only when the
    // run's random schema draw put a posting-indexed SYMBOL on some table, which
    // FuzzTableFactory.assignIndexes decides per SYMBOL column. On a run that drew none - 7 of the
    // 40 measured - no budget lifts POSTING off zero and it reports 0/0 whatever the budget.
    // QueryFuzzTest checks that precondition before it asserts a shape generated anything, so those
    // runs stay green instead of failing a working generator.
    private static final int DEFAULT_NUM_QUERIES = 1_000;

    private final boolean isDiffFusedEnabled;
    private final boolean isDiffJitEnabled;
    private final boolean isDiffMapPageEnabled;
    private final boolean isDiffParallelJoinEnabled;
    private final boolean isDiffShadowEnabled;
    private final boolean isEquiJoinEnabled;
    private final boolean isFaultInjectionEnabled;
    private final boolean isGroupingSetsEnabled;
    private final boolean isHashJoinEnabled;
    private final boolean isHorizonJoinEnabled;
    private final boolean isLatestOnEnabled;
    private final boolean isParallelFaultEnabled;
    private final boolean isStorageAxesEnabled;
    private final boolean isVerifyCursorEnabled;
    private final boolean isWindowEnabled;
    private final boolean isWindowJoinEnabled;
    private final String dumpPath;
    private final int faultProbabilityPct;
    private final HashJoinBuildMode hashJoinBuildMode;
    // Null leaves the fused hash join GROUP BY's payload copy rule to the defaults.
    private final HashJoinPayloadLayout hashJoinPayloadLayout;
    private final int maxColumnsPerTable;
    private final int minColumnsPerTable;
    private final int numQueries;
    private final int numTables;
    // The parallel hash join's cap of tuples per probe task, or 0 to leave it at its default.
    private final long parallelJoinTaskOutputMaxRows;
    // The dispatch limit that the equi-join queries run with, or -1 to leave it at its default.
    private final int parallelJoinDispatchLimit;
    // The most rows of a page frame that the equi-join queries run with, or 0 to leave the default.
    private final int parallelJoinFrameMaxRows;
    // The parallel hash join's tuple ring budget, in bytes, or 0 to leave it at its default.
    private final long parallelJoinRingMaxSize;
    // The rows of a Parquet row group of the run's tables, or 0 to leave the default.
    private final int parquetRowGroupSize;
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
        // 30 minutes: rowsPerTable * 30min covers 30..75 hours, so 2-4 DAY partitions. A run's
        // later tables hold a row or two more (see getTableRows()).
        this.stepMicros = 30L * 60L * 1_000_000L;
        this.tsStart = "2024-01-01";
        this.numQueries = Integer.getInteger(QUERIES_PROP, DEFAULT_NUM_QUERIES);
        this.dumpPath = System.getProperty(DUMP_PROP);
        this.isDiffJitEnabled = Boolean.parseBoolean(System.getProperty(DIFF_JIT_PROP, "true"));
        this.isDiffShadowEnabled = Boolean.parseBoolean(System.getProperty(DIFF_SHADOW_PROP, "true"));
        // Runs every equi-join GROUP BY query with the fused hash join GROUP BY on and off and
        // compares the results (see QueryRunner). Pass -Dquestdb.fuzz.diff.fused=false to drop
        // the comparison and keep the shape.
        this.isDiffFusedEnabled = Boolean.parseBoolean(System.getProperty(DIFF_FUSED_PROP, "true"));
        // Reruns every differential query with a map page of a few hundred bytes at most and
        // compares the results (see QueryRunner). Pass -Dquestdb.fuzz.diff.mappage=false
        // to drop the comparison.
        this.isDiffMapPageEnabled = Boolean.parseBoolean(System.getProperty(DIFF_MAP_PAGE_PROP, "true"));
        // Runs every row-producing equi-join query with the parallel hash join on and off and
        // compares the results in order (see QueryRunner). Pass
        // -Dquestdb.fuzz.diff.parallel.join=false to drop the comparison and keep the shape.
        this.isDiffParallelJoinEnabled = Boolean.parseBoolean(System.getProperty(DIFF_PARALLEL_JOIN_PROP, "true"));
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
        // Equi-join GROUP BY shapes, the ones the fused hash join GROUP BY replaces, carve a band
        // out of the GROUP BY range (see QueryGenerator). On by default; pass
        // -Dquestdb.fuzz.hashjoin=false to drop them, for example while investigating a failure,
        // and give the band back to GROUP BY.
        this.isHashJoinEnabled = Boolean.parseBoolean(System.getProperty(HASH_JOIN_PROP, "true"));
        // Row-producing INNER and LEFT equi-joins, alone or in stars, the ones the parallel hash
        // join runs, carve a band out of the SIMPLE range (see QueryGenerator). On by default; pass
        // -Dquestdb.fuzz.equijoin=false to drop them and give the band back to SIMPLE.
        this.isEquiJoinEnabled = Boolean.parseBoolean(System.getProperty(EQUI_JOIN_PROP, "true"));
        // ROLLUP, CUBE and GROUPING SETS shapes also carve a band out of the GROUP BY range (see
        // QueryGenerator), and QueryRunner checks each against its UNION ALL rewrite. On by
        // default; pass -Dquestdb.fuzz.groupingsets=false to drop them.
        this.isGroupingSetsEnabled = Boolean.parseBoolean(System.getProperty(GROUPING_SETS_PROP, "true"));
        // The fuzz tables hold nearly the same rows (see getTableRows()), so the default rule copies
        // a build's payload unless a probe interval leaves the probe under half the build. Pass
        // -Dquestdb.fuzz.hashjoin.payload=row_ids or =copied to force every fused query onto one
        // layout; the fused on/off axis then checks that one.
        final String payloadLayout = System.getProperty(HASH_JOIN_PAYLOAD_PROP);
        this.hashJoinPayloadLayout = payloadLayout != null ? HashJoinPayloadLayout.valueOf(payloadLayout.toUpperCase()) : null;
        // The fuzz tables are far below the row count from which a fused build runs on the workers,
        // so a run forces the build mode: -Dquestdb.fuzz.hashjoin.build=serial or =parallel, or,
        // without the property, a mode that the seed picks without drawing from it, so that a seed
        // replays the same queries whichever mode it picks.
        final String buildMode = System.getProperty(HASH_JOIN_BUILD_PROP);
        this.hashJoinBuildMode = buildMode != null
                ? HashJoinBuildMode.valueOf(buildMode.toUpperCase())
                : ((rnd.getSeed0() ^ rnd.getSeed1()) & 1) == 0 ? HashJoinBuildMode.SERIAL : HashJoinBuildMode.PARALLEL;
        // The fuzz tables' frames hold far fewer rows than the parallel hash join's default cap of
        // tuples per probe task, so a frame's task never stops at the cap and leaves the rest of the
        // frame to a continuation. A run therefore lowers the cap to one tuple, which leaves every
        // frame whose pairs outnumber its rows to continuations, or keeps the default:
        // -Dquestdb.fuzz.parallel.join.cap=N sets it, or, without the property, the seed picks one of
        // the two without drawing from it, as it picks the build mode.
        final String cap = System.getProperty(PARALLEL_JOIN_CAP_PROP);
        this.parallelJoinTaskOutputMaxRows = cap != null
                ? Long.parseLong(cap)
                : (((rnd.getSeed0() ^ rnd.getSeed1()) >>> 1) & 1) == 0 ? 0 : 1;
        // The fuzz tables hold 60 to 150 rows over 2 to 4 partitions, so with the default frame size a
        // probe has fewer frames than the parallel hash join's tuple ring has entries, its window never
        // moves, and no frame shares an entry. A run therefore puts the equi-join queries under frame
        // pressure, or not, with three knobs that the seed picks without drawing from it, as it picks
        // the cap, and that -D properties set:
        // - frames of 1 to 8 rows, -Dquestdb.fuzz.parallel.join.frames=N, 0 for the default, which
        //   the run's equi-join queries, and every axis that compares them, run with;
        // - a tuple ring budget, -Dquestdb.fuzz.parallel.join.ring=BYTES, 0 for the default: 1 byte,
        //   below one frame's tuples, leaves one entry, and 192 bytes, four frames of two rows of a
        //   single join, leaves a window that a frame of duplicate matches narrows;
        // - a dispatch limit of 0 to 2, -Dquestdb.fuzz.parallel.join.dispatch=N, -1 for the default,
        //   which the run's equi-join queries compile with.
        // Every axis compares runs under the same knobs, so the in-order oracle holds. A Parquet row
        // group of a few rows, -Dquestdb.fuzz.parquet.row.group=N, makes the Parquet partitions of the
        // run's tables hold many row groups. It applies to every shape, and the seed does not pick it:
        // row groups of two rows still meet a defect outside the equi-join shapes, tracked on its own,
        // so a run that wants the axis sets it, to 4 or more. QueryFuzzTest#testParallelJoinStorageAxes
        // pins it at 4.
        final long seedBits = rnd.getSeed0() ^ rnd.getSeed1();
        this.parallelJoinFrameMaxRows = Integer.getInteger(
                PARALLEL_JOIN_FRAMES_PROP,
                ((seedBits >>> 2) & 1) == 0 ? 0 : 1 + (int) ((seedBits >>> 3) & 7)
        );
        final int ringPick = (int) ((seedBits >>> 6) & 3);
        this.parallelJoinRingMaxSize = Long.getLong(PARALLEL_JOIN_RING_PROP, ringPick == 1 ? 1 : ringPick == 2 ? 192 : 0);
        this.parallelJoinDispatchLimit = Integer.getInteger(PARALLEL_JOIN_DISPATCH_PROP, (int) ((seedBits >>> 8) & 3) - 1);
        this.parquetRowGroupSize = Integer.getInteger(PARQUET_ROW_GROUP_PROP, 0);
        // The run's primary tables take storage shapes that their shadows do not: non-WAL and
        // non-partitioned tables, column tops and a dropped column (see FuzzTableFactory). On by
        // default; pass -Dquestdb.fuzz.storage=false to keep every table a partitioned WAL table
        // that holds every column from its first row. The shapes come from a generator of their
        // own, so the property changes no query a seed generates.
        this.isStorageAxesEnabled = Boolean.parseBoolean(System.getProperty(STORAGE_PROP, "true"));
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

    public HashJoinBuildMode getHashJoinBuildMode() {
        return hashJoinBuildMode;
    }

    public HashJoinPayloadLayout getHashJoinPayloadLayout() {
        return hashJoinPayloadLayout;
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

    /**
     * The parallel hash join's cap of tuples per probe task for the run, or 0 to leave the default.
     */
    public long getParallelJoinTaskOutputMaxRows() {
        return parallelJoinTaskOutputMaxRows;
    }

    /**
     * The dispatch limit that the run's equi-join queries compile with, or -1 to leave the default.
     */
    public int getParallelJoinDispatchLimit() {
        return parallelJoinDispatchLimit;
    }

    /**
     * The most rows of a page frame that the run's equi-join queries run with, or 0 to leave the
     * default.
     */
    public int getParallelJoinFrameMaxRows() {
        return parallelJoinFrameMaxRows;
    }

    /**
     * The parallel hash join's tuple ring budget for the run, in bytes, or 0 to leave the default.
     */
    public long getParallelJoinRingMaxSize() {
        return parallelJoinRingMaxSize;
    }

    /**
     * The rows of a Parquet row group of the run's tables, or 0 to leave the default.
     */
    public int getParquetRowGroupSize() {
        return parquetRowGroupSize;
    }

    public int getRowsPerTable() {
        return rowsPerTable;
    }

    /**
     * The rows of the run's table of the given index: {@link #getRowsPerTable()} plus the index, so
     * that no two tables of a run start out the same size. An INNER join of two unfiltered tables
     * hashes the smaller one and streams the other, whichever of the two the query names first, so
     * the queries over one pair of tables decide that swap both ways, and a few rows apart, the
     * margin at which a wrong table size would swap a join that should not swap. A table can still
     * gain up to three rows from a composite partition (see FuzzTableFactory), which may make two
     * tables the same size, the swap's other edge.
     */
    public int getTableRows(int tableIndex) {
        return rowsPerTable + tableIndex;
    }

    public long getStepMicros() {
        return stepMicros;
    }

    public String getTsStart() {
        return tsStart;
    }

    public boolean isDiffFusedEnabled() {
        return isDiffFusedEnabled;
    }

    public boolean isDiffJitEnabled() {
        return isDiffJitEnabled;
    }

    public boolean isDiffMapPageEnabled() {
        return isDiffMapPageEnabled;
    }

    public boolean isDiffParallelJoinEnabled() {
        return isDiffParallelJoinEnabled;
    }

    public boolean isDiffShadowEnabled() {
        return isDiffShadowEnabled;
    }

    public boolean isEquiJoinEnabled() {
        return isEquiJoinEnabled;
    }

    public boolean isFaultInjectionEnabled() {
        return isFaultInjectionEnabled;
    }

    public boolean isGroupingSetsEnabled() {
        return isGroupingSetsEnabled;
    }

    public boolean isHashJoinEnabled() {
        return isHashJoinEnabled;
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

    public boolean isStorageAxesEnabled() {
        return isStorageAxesEnabled;
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
