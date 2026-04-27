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
    public static final String DUMP_PROP = "questdb.fuzz.dump";
    public static final String QUERIES_PROP = "questdb.fuzz.queries";

    private final boolean isDiffJitEnabled;
    private final String dumpPath;
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
    }

    public String getDumpPath() {
        return dumpPath;
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
}
