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
import io.questdb.test.tools.TestUtils;
import io.questdb.std.str.StringSink;
import org.junit.Test;

/**
 * The sibling-cell interval shapes under the HIVE directory layout.
 * <p>
 * This file exists because of a gap in the tests written alongside the interval fixes: every one of them
 * declares {@code LAYOUT PLAIN}, while HIVE is the DEFAULT layout. The two differ in how a cell's
 * directory is built — {@code exch=BTC} versus {@code BTC} — and that path building is not idle
 * bystander code: rendering the {@code name=} prefix from the wrong column index was a real defect fixed
 * separately in this same branch. A suite that never exercises the default layout is not covering what
 * most tables will actually use.
 * <p>
 * The shapes are deliberately the discriminating ones — a cell straddling the window without matching,
 * and a cell wholly below it visited first by a backward scan — so this is a genuine second execution of
 * the fixed code paths rather than a cosmetic layout smoke test.
 */
public class CompositeIntervalHiveLayoutTest extends AbstractCompositeTwinTest {

    /**
     * Forward: cell E0 straddles the point interval without matching it; sibling E1 holds the row.
     */
    @Test
    public void testStraddlingCellHiveLayout() throws Exception {
        assertMemoryLeak(() -> {
            createHiveTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            assertHiveCellDirectories();
            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts < '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * Backward: cell E1 (higher cellKey, so met first going backwards) sits entirely below the window;
     * cell E0 holds the matching row.
     */
    @Test
    public void testCellBelowIntervalHiveLayout() throws Exception {
        assertMemoryLeak(() -> {
            createHiveTwins();
            insertIntoBoth("('2023-01-02T05:00:00.000000Z','E0',5.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E1',1.0)");
            drainWalQueue();

            assertHiveCellDirectories();
            assertTwinEqual(" WHERE ts >= '2023-01-02T04:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z'");
            assertTwinEqual(" WHERE ts = '2023-01-02T05:00:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * A NULL dimension value under HIVE, which names the cell directory differently again.
     */
    @Test
    public void testNullCellHiveLayout() throws Exception {
        assertMemoryLeak(() -> {
            createHiveTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T02:00:00.000000Z',null,2.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts < '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * Many cells, only later ones matching -- the fuzz-found shape, under HIVE.
     */
    @Test
    public void testManyCellsHiveLayout() throws Exception {
        assertMemoryLeak(() -> {
            createHiveTwins();
            final StringBuilder rows = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                rows.append("('2023-01-02T01:00:00.000000Z','E").append(i).append("',1.0),")
                        .append("('2023-01-02T05:00:00.000000Z','E").append(i).append("',5.0),");
            }
            rows.append("('2023-01-02T03:00:00.000000Z','X1',31.0),")
                    .append("('2023-01-02T03:00:00.000000Z','X2',32.0)");
            insertIntoBoth(rows.toString());
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T03:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * Confirms the table really is using HIVE naming, so a future default change cannot turn this file
     * into a duplicate of the PLAIN-layout suites without anyone noticing.
     */
    private void assertHiveCellDirectories() throws SqlException {
        final StringSink partitions = new StringSink();
        printSql("SELECT name FROM table_partitions('c')", partitions);
        TestUtils.assertContains(partitions, "exch=");
    }


    /**
     * No LAYOUT clause at all: the composite table takes the DEFAULT layout, which is the point of this
     * file.
     */
    private void createHiveTwins() throws SqlException {
        createTwins("ts TIMESTAMP, exch SYMBOL, px DOUBLE", "PARTITION BY DAY, exch");
    }

}
