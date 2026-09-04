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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import org.junit.Test;

/**
 * Aggregates and {@code SAMPLE BY} over a FILTERED composite scan.
 * <p>
 * The sibling-cell defect was a wrong ROW SET, and a wrong row set silently becomes a wrong NUMBER once
 * an aggregate sits on top — which is how it reaches a dashboard rather than a stack trace. These
 * consumers also reach the interval cursors by different routes than a plain row scan does
 * ({@code count()} on a plain table goes through {@code calculateSize()}, aggregates and SAMPLE BY pull
 * frames), so they are not merely a re-run of the row-scan tests.
 * <p>
 * Data is the shape that breaks the cursors: a cell straddling the window without matching, siblings
 * holding the rows that do. Every result is compared against a plain twin fed identical rows, so the
 * assertions state the contract rather than hand-computed numbers.
 */
public class CompositeIntervalAggregateTest extends AbstractCompositeTwinTest {

    @Test
    public void testScalarAggregatesOverFilteredScan() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            final String where = " WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'";
            assertTwinQuery("SELECT count() FROM %s" + where);
            assertTwinQuery("SELECT count(px), sum(px), min(px), max(px), avg(px) FROM %s" + where);
            assertTwinQuery("SELECT min(ts), max(ts) FROM %s" + where);
            assertTwinQuery("SELECT count_distinct(exch) FROM %s" + where);
        });
    }

    @Test
    public void testKeyedAggregateOverFilteredScan() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            final String where = " WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'";
            assertTwinQuery("SELECT exch, count(), sum(px) FROM %s" + where + " ORDER BY exch");
        });
    }

    @Test
    public void testSampleByOverFilteredScan() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            final String where = " WHERE ts >= '2023-01-02T00:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z'";
            assertTwinQuery("SELECT ts, count(), sum(px) FROM %s" + where + " SAMPLE BY 1h ORDER BY ts");
            assertTwinQuery("SELECT ts, exch, count() FROM %s" + where + " SAMPLE BY 2h ORDER BY ts, exch");
        });
    }

    /**
     * A point filter under an aggregate — the interval shape most cells of a day fail to match, and the
     * one where a dropped row turns straight into a wrong count.
     */
    @Test
    public void testAggregateOverPointFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            for (String t : new String[]{"02:30", "03:30", "03:45", "01:00", "05:00"}) {
                final String where = " WHERE ts = '2023-01-02T" + t + ":00.000000Z'";
                assertTwinQuery("SELECT count(), sum(px) FROM %s" + where);
                assertTwinQuery("SELECT exch, count() FROM %s" + where + " ORDER BY exch");
            }
        });
    }

    /**
     * Aggregates over MANY cells where only a few hold rows in the window -- the fuzz-found shape, under
     * an aggregate rather than a row scan.
     */
    @Test
    public void testAggregatesOverManyCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            final StringBuilder rows = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                rows.append("('2023-01-02T01:00:00.000000Z','E").append(i).append("',1.0),")
                        .append("('2023-01-02T05:00:00.000000Z','E").append(i).append("',5.0),");
            }
            rows.append("('2023-01-02T03:00:00.000000Z','X1',31.0),")
                    .append("('2023-01-02T03:00:00.000000Z','X2',32.0)");
            insertIntoBoth(rows.toString());
            drainWalQueue();

            final String where = " WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'";
            assertTwinQuery("SELECT count(), sum(px), min(px), max(px) FROM %s" + where);
            assertTwinQuery("SELECT exch, count() FROM %s" + where + " ORDER BY exch");
            assertTwinQuery("SELECT ts, count() FROM %s" + where + " SAMPLE BY 1h ORDER BY ts");
        });
    }

    /**
     * Runs {@code template} against the plain twin and the composite subject and requires identical
     * output. {@code %s} is the table name.
     */
    private void assertTwinQuery(String template) throws SqlException {
        assertSqlCursors(String.format(template, "p"), String.format(template, "c"));
    }


    /**
     * E0 straddles 02:00-04:00 without a row inside it; E1 and E2 hold the rows that are.
     */
    private void fillSiblingShape() throws SqlException {
        insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-02T05:00:00.000000Z','E0',5.0),"
                + "('2023-01-02T02:30:00.000000Z','E1',2.5),"
                + "('2023-01-02T03:30:00.000000Z','E2',3.5),"
                + "('2023-01-02T03:45:00.000000Z','E1',3.75)");
        drainWalQueue();
    }

}
