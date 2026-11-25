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

import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The per-execution frame mode is written to the DEBUG log, one record per execution, and never
 * at INFO.
 * <p>
 * {@code EXPLAIN} prints {@code frames: per-key (unordered)} from the plan-stable PERMISSION and
 * keeps doing so deliberately -- a plan that changed with the data would be worse than one that
 * is blind to it -- so it cannot answer "which mode did my execution actually take, and why". A
 * user answers it from the debug record. Tests do not see debug records (debug is on only with
 * {@code -Debug} or {@code QDB_DEBUG}), so they read the same text from
 * {@code CoveringIndexRecordCursorFactory.getLastFrameModeForTesting()}, which formats the last
 * decision through the formatter the record uses. Each arm resets that snapshot before its query
 * and asserts the query wrote it, so a decision left by an earlier test cannot satisfy the arm,
 * and asserts that no INFO record was written.
 * <p>
 * Both arms assert the record, and both assert the NUMBERS in it, not just the mode word. The
 * numbers are the whole point: {@code rowsPerPair} against {@code crossover} is why the density
 * gate decided what it decided, and a record that named the mode without them would leave a user
 * exactly where they started.
 * <p>
 * The {@code crossover} the record carries is per-CONSUMER, not a global constant: it is
 * {@code PER_KEY_MIN_ROWS_PER_PAIR_BASE} times the passes the consumer makes over each frame.
 * {@link #QUERY} carries two aggregate values on the vectorized group by, which dispatches one
 * task per (frame, aggregate) pair, so the number these arms expect is {@code base x 2}. It is
 * DERIVED here rather than written out, so that moving the base moves this expectation with it
 * instead of turning these two arms red for a retune they should not care about -- while a change
 * to the DERIVATION, which they should care about, still fails them.
 */
public class CoveringIndexFrameModeLogTest extends AbstractCoveringIndexQueryTest {

    private static final int KEYS = 4;
    // avg(value) and count(): two vector aggregate functions, hence two passes per frame.
    private static final int QUERY_FRAME_PASSES = 2;
    private static final int CROSSOVER =
            CoveringIndexRecordCursorFactory.getPerKeyMinRowsPerPairBaseForTesting() * QUERY_FRAME_PASSES;
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
            // 40 partitions x 4 keys x 1000 rows per pair, against a crossover of base x 2.
            insertUniformBlock(40, 1000);
            assertFrameModeRecord(
                    "covering scan frame mode \\[table=telemetry, mode=per-key, reason=density, keys=4, "
                            + "partitionsUpper=40, framesUpper=\\d+, frameCeiling=\\d+, rowsPerPair=1000, crossover="
                            + CROSSOVER + "]"
            );
        });
    }

    @Test(timeout = 300_000)
    public void testSparseOpenLogsMergedWithTheNumbersBehindIt() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryTable();
            // 40 partitions x 4 keys x 5 rows per pair: well under the crossover.
            insertUniformBlock(40, 5);
            assertFrameModeRecord(
                    "covering scan frame mode \\[table=telemetry, mode=merged, reason=density, keys=4, "
                            + "partitionsUpper=40, framesUpper=\\d+, frameCeiling=\\d+, rowsPerPair=5, crossover="
                            + CROSSOVER + "]"
            );
        });
    }

    // runs QUERY on a reset snapshot: it must write the record, matching regex, and log nothing at INFO
    private void assertFrameModeRecord(String regex) throws Exception {
        CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
        Assert.assertNull(CoveringIndexRecordCursorFactory.getLastFrameModeForTesting());
        printSql(QUERY, sink);
        final String record = CoveringIndexRecordCursorFactory.getLastFrameModeForTesting();
        Assert.assertNotNull("the query wrote no frame mode record", record);
        Assert.assertTrue(record, record.matches(regex));
        capture.drain();
        capture.assertNotLogged("covering scan frame mode");
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
