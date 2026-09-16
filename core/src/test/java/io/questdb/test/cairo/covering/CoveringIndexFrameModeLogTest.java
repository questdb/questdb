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

package io.questdb.test.cairo.covering;

import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The per-execution frame mode must be observable WITHOUT a {@code @TestOnly} hook.
 * <p>
 * {@code EXPLAIN} prints {@code frames: per-key (unordered)} from the plan-stable PERMISSION and
 * keeps doing so deliberately -- a plan that changed with the data would be worse than one that
 * is blind to it -- so it cannot answer "which mode did my execution actually take, and why".
 * Until this log record existed the only answers were
 * {@code CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting()} and its merged twin,
 * both {@code @TestOnly} statics: a user watching a query quietly take the slower path had
 * nothing to look at, and a test asserting the plan string passed under three mutations that
 * broke per-key mode outright.
 * <p>
 * Both arms assert the record, and both assert the NUMBERS in it, not just the mode word. The
 * numbers are the whole point: {@code rowsPerPair} against {@code crossover} is why the density
 * gate decided what it decided, and a record that named the mode without them would leave a user
 * exactly where they started.
 */
public class CoveringIndexFrameModeLogTest extends AbstractCoveringIndexQueryTest {

    private static final int KEYS = 4;
    private static final String QUERY =
            "SELECT param_id, avg(value) a, count() c FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC','KCAS','CALT') ORDER BY param_id";
    private LogCapture capture;

    @After
    @Override
    public void tearDown() throws Exception {
        if (capture != null) {
            capture.stop();
        }
        super.tearDown();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        capture = new LogCapture();
        capture.start();
    }

    @Test(timeout = 300_000)
    public void testDenseOpenLogsPerKeyWithTheNumbersBehindIt() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            // 40 partitions x 4 keys x 200 rows per pair: 6.2x the crossover.
            insertUniformBlock(40, 200);
            printSql(QUERY, sink);
            capture.drain();
            capture.assertLoggedRE(
                    "covering scan frame mode \\[table=telemetry, mode=per-key, reason=density, keys=4, "
                            + "partitionsUpper=40, framesUpper=\\d+, frameCeiling=\\d+, rowsPerPair=200, crossover=32]"
            );
        });
    }

    @Test(timeout = 300_000)
    public void testSparseOpenLogsMergedWithTheNumbersBehindIt() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            // 40 partitions x 4 keys x 5 rows per pair: well under the crossover.
            insertUniformBlock(40, 5);
            printSql(QUERY, sink);
            capture.drain();
            capture.assertLoggedRE(
                    "covering scan frame mode \\[table=telemetry, mode=merged, reason=density, keys=4, "
                            + "partitionsUpper=40, framesUpper=\\d+, frameCeiling=\\d+, rowsPerPair=5, crossover=32]"
            );
        });
    }

    private void createTelemetryTable() throws Exception {
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
    }

    private void insertUniformBlock(int partitions, int rowsPerPair) throws Exception {
        final int rowsPerPartition = KEYS * rowsPerPair;
        final long rows = (long) rowsPerPartition * partitions;
        execute("INSERT INTO telemetry SELECT" +
                " ((((x - 1) / " + rowsPerPartition + ") * 86400000000L)" +
                "   + (((x - 1) % " + rowsPerPartition + ") * 1000L))::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " x::double" +
                " FROM long_sequence(" + rows + ")");
    }
}
