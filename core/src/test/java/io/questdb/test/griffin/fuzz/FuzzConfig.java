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
 * test itself, so CI can run a small budget and developers can crank it
 * up when hunting issues.
 */
public final class FuzzConfig {
    public static final String DIFF_JIT_PROP = "questdb.fuzz.diff.jit";
    public static final String DIFF_SHADOW_PROP = "questdb.fuzz.diff.shadow";
    public static final String DUMP_PROP = "questdb.fuzz.dump";
    public static final String FAULTS_PROP = "questdb.fuzz.faults";
    public static final String FAULT_PARALLEL_PROP = "questdb.fuzz.fault.parallel";
    public static final String FAULT_PCT_PROP = "questdb.fuzz.fault.pct";
    public static final String QUERIES_PROP = "questdb.fuzz.queries";
    public static final String VERIFY_CURSOR_PROP = "questdb.fuzz.verify.cursor";
    public static final String WINDOW_PROP = "questdb.fuzz.window";

    private final boolean isDiffJitEnabled;
    private final boolean isDiffShadowEnabled;
    private final boolean isFaultInjectionEnabled;
    private final boolean isParallelFaultEnabled;
    private final boolean isVerifyCursorEnabled;
    private final boolean isWindowEnabled;
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
        this.numTables = 1 + rnd.nextInt(3);
        this.rowsPerTable = 60 + rnd.nextInt(90);
        this.minColumnsPerTable = 3;
        this.maxColumnsPerTable = 10;
        // 30 minutes: rowsPerTable * 30min covers 30..75 hours, so 2-4 DAY partitions.
        this.stepMicros = 30L * 60L * 1_000_000L;
        this.tsStart = "2024-01-01";
        this.numQueries = Integer.getInteger(QUERIES_PROP, 100);
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

    public boolean isParallelFaultEnabled() {
        return isParallelFaultEnabled;
    }

    public boolean isVerifyCursorEnabled() {
        return isVerifyCursorEnabled;
    }

    public boolean isWindowEnabled() {
        return isWindowEnabled;
    }
}
