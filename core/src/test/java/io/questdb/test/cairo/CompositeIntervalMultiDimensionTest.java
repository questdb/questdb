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
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Interval scans over composite tables with TWO dimensions, and with a clustering key.
 * <p>
 * Another gap in the tests written alongside the interval fixes: they all use a single dimension. Two
 * dimensions multiply the cells per day (one per dimension TUPLE, not per value), which is the state the
 * defect was originally — and wrongly — believed to require. Getting that belief wrong once already sent
 * an investigation to the wrong code, so the multi-dimension case deserves its own coverage rather than
 * an assumption that one dimension generalises.
 * <p>
 * {@code ORDER BY} clustering is included for the same reason: it changes how rows are laid out inside a
 * cell, which is what the interval cursors binary-search over.
 * <p>
 * All shapes are the discriminating ones and are compared against a plain twin, forward and backward.
 */
public class CompositeIntervalMultiDimensionTest extends AbstractCairoTest {

    /**
     * Two dimensions: the straddling cell and the matching cell differ in the SECOND dimension only, so
     * they are distinct cells whose tuples share a component.
     */
    @Test
    public void testTwoDimensionsStraddlingCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwoDimensionTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','EX','S0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','EX','S0',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','EX','S1',2.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts < '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * Two dimensions where the cells differ in the FIRST component instead.
     */
    @Test
    public void testTwoDimensionsFirstComponentDiffers() throws Exception {
        assertMemoryLeak(() -> {
            createTwoDimensionTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','EX0','S',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','EX0','S',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','EX1','S',2.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts < '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * Backward, two dimensions: the cell met first going backwards lies entirely below the window.
     */
    @Test
    public void testTwoDimensionsCellBelowIntervalBackward() throws Exception {
        assertMemoryLeak(() -> {
            createTwoDimensionTwins();
            insertIntoBoth("('2023-01-02T05:00:00.000000Z','EX','S0',5.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','EX','S1',1.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts >= '2023-01-02T04:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z'");
            assertTwinEqual(" WHERE ts = '2023-01-02T05:00:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * A full grid of dimension tuples across two days, with windows each tuple is variously inside,
     * straddling, above and below.
     */
    @Test
    public void testTwoDimensionGridAcrossDays() throws Exception {
        assertMemoryLeak(() -> {
            createTwoDimensionTwins();
            final StringBuilder rows = new StringBuilder();
            boolean firstRow = true;
            for (String day : new String[]{"2023-01-01", "2023-01-02"}) {
                for (int hour = 1; hour <= 5; hour += 2) {
                    for (String exch : new String[]{"EX0", "EX1"}) {
                        for (String sym : new String[]{"S0", "S1"}) {
                            if (!firstRow) {
                                rows.append(',');
                            }
                            firstRow = false;
                            rows.append("('").append(day).append("T0").append(hour).append(":00:00.000000Z','")
                                    .append(exch).append("','").append(sym).append("',").append(hour).append(".0)");
                        }
                    }
                }
            }
            insertIntoBoth(rows.toString());
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T03:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-01T18:00:00.000000Z' AND ts < '2023-01-02T06:00:00.000000Z'");
            assertTwinEqual(" WHERE exch = 'EX1' AND ts >= '2023-01-01T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * A CLUSTERED composite table ({@code ORDER BY sym}): clustering changes the row order inside each
     * cell, which is what the interval cursors binary-search over when computing a frame's bounds.
     */
    @Test
    public void testClusteredCompositeStraddlingCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch ORDER BY sym WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0','B',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0','A',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1','B',2.0),"
                    + "('2023-01-02T02:30:00.000000Z','E1','A',2.5)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts <= '2023-01-02T02:45:00.000000Z'");
            assertTwinEqual("");
        });
    }

    private void assertTwinEqual(String where) throws SqlException {
        final String orderAsc = " ORDER BY ts, exch, sym, px";
        assertSqlCursors("SELECT * FROM p" + where + orderAsc, "SELECT * FROM c" + where + orderAsc);
        assertSqlCursors("SELECT count() FROM p" + where, "SELECT count() FROM c" + where);
        assertSqlCursors("SELECT ts FROM p" + where + " ORDER BY ts DESC",
                "SELECT ts FROM c" + where + " ORDER BY ts DESC");
    }

    private void createTwoDimensionTwins() throws SqlException {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY, exch, sym LAYOUT PLAIN WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertIntoBoth(String values) throws SqlException {
        execute("INSERT INTO c VALUES " + values);
        execute("INSERT INTO p VALUES " + values);
    }
}
