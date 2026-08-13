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

import org.junit.Test;

/**
 * Cell PRUNING (a dimension predicate) combined with interval scanning.
 * <p>
 * {@code isCellAllowed} is the third way these loops can skip a cell, alongside the two the
 * sibling-cell fixes touched. It sits immediately beside them and shares their state: a pruned cell
 * advances {@code partitionLo} and resets the residual limit WITHOUT consuming the interval — the same
 * move the sibling-cell branches now make. Pruning was not changed by those fixes, but nothing covered
 * the two interacting, and an interaction bug here looks exactly like the defect that was just fixed:
 * silently missing rows.
 * <p>
 * The interesting arrangement is a pruned cell sitting where the interval-retiring decision is made, so
 * these tests deliberately prune the cell that would otherwise straddle the window, and prune cells at
 * the start, middle and end of a day's cell run. Forward and backward, against a plain twin.
 */
public class CompositeIntervalCellPruningTest extends AbstractCompositeTwinTest {

    /**
     * The straddling cell is the one PRUNED OUT. The matching row lives in a cell that the scan only
     * reaches if pruning advanced without consuming the interval.
     */
    @Test
    public void testPrunedStraddlingCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            // exch = 'E1' prunes E0 -- the cell that straddles 02:00 without matching it
            assertTwinEqual(" WHERE exch = 'E1' AND ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE exch = 'E1' AND ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T03:00:00.000000Z'");
            // and the complement: prune the MATCHING cell instead, leaving only the straddler
            assertTwinEqual(" WHERE exch = 'E0' AND ts = '2023-01-02T02:00:00.000000Z'");
        });
    }

    /**
     * Pruning that removes cells at the START, MIDDLE and END of a day's cell run, each with an interval
     * that some remaining cell matches.
     */
    @Test
    public void testPruningAtEachPositionInTheCellRun() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T03:00:00.000000Z','E2',3.0),"
                    + "('2023-01-02T04:00:00.000000Z','E3',4.0)");
            drainWalQueue();

            final String window = " AND ts >= '2023-01-02T00:00:00.000000Z' AND ts <= '2023-01-02T05:00:00.000000Z'";
            assertTwinEqual(" WHERE exch != 'E0'" + window); // first pruned
            assertTwinEqual(" WHERE exch != 'E1'" + window); // middle pruned
            assertTwinEqual(" WHERE exch != 'E3'" + window); // last pruned
            assertTwinEqual(" WHERE exch IN ('E1','E3')" + window);
            assertTwinEqual(" WHERE exch IN ('E0','E3')" + window);
        });
    }

    /**
     * Pruning down to a SINGLE cell whose rows lie entirely outside the window -- so the one cell the
     * scan is allowed to look at cannot match, and the answer must be empty rather than wrong.
     */
    @Test
    public void testPruningToACellOutsideTheWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T05:00:00.000000Z','E1',5.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE exch = 'E0' AND ts >= '2023-01-02T04:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z'");
            assertTwinEqual(" WHERE exch = 'E1' AND ts >= '2023-01-02T00:00:00.000000Z' AND ts <= '2023-01-02T02:00:00.000000Z'");
        });
    }

    /**
     * Pruning where the surviving cell is the one a BACKWARD scan meets last -- so the backward walk
     * must pass over pruned cells without retiring the interval on the way.
     */
    @Test
    public void testPrunedCellsBeforeTheMatchBackward() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T02:00:00.000000Z','KEEP',2.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','SKIP1',1.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:30:00.000000Z','SKIP2',1.5)");
            drainWalQueue();

            assertTwinEqual(" WHERE exch = 'KEEP' AND ts >= '2023-01-02T00:00:00.000000Z' AND ts <= '2023-01-02T03:00:00.000000Z'");
            assertTwinEqual(" WHERE exch != 'SKIP1' AND ts >= '2023-01-02T00:00:00.000000Z' AND ts <= '2023-01-02T03:00:00.000000Z'");
        });
    }

    /**
     * Pruning with MULTIPLE intervals, so all three skip mechanisms (pruned cell, interval-retiring exit,
     * sibling advance) are live in the same scan.
     */
    @Test
    public void testPruningWithMultipleIntervals() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T04:00:00.000000Z','E2',4.0),"
                    + "('2023-01-02T05:00:00.000000Z','E1',5.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE exch IN ('E1','E2') AND ("
                    + "(ts >= '2023-01-02T01:30:00.000000Z' AND ts <= '2023-01-02T02:30:00.000000Z')"
                    + " OR (ts >= '2023-01-02T03:30:00.000000Z' AND ts <= '2023-01-02T04:30:00.000000Z'))");
            assertTwinEqual(" WHERE exch = 'E0' AND ("
                    + "(ts >= '2023-01-02T00:30:00.000000Z' AND ts <= '2023-01-02T01:30:00.000000Z')"
                    + " OR (ts >= '2023-01-02T04:30:00.000000Z' AND ts <= '2023-01-02T05:30:00.000000Z'))");
        });
    }



}
