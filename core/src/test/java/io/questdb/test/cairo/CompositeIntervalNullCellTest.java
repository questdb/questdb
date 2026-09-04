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
 * Interval scans over the NULL-dimension cell.
 * <p>
 * A NULL dimension value is a legitimate cell of its own ({@code %NULL} on disk), not an absence — and
 * NULL handling has already produced one composite defect in this feature, a WAL-apply hang on a NULL
 * identity dimension. The interval cursors treat every cell the same way, so the NULL cell is subject to
 * the same sibling-cell hazards as any other: it can be the cell that straddles a window without
 * matching, or the one holding the only matching row.
 * <p>
 * Nothing covered that combination. These tests put the NULL cell on both sides of the hazard, forward
 * and backward, against a plain twin.
 */
public class CompositeIntervalNullCellTest extends AbstractCompositeTwinTest {

    /**
     * The NULL cell holds the only row inside the window, and a non-NULL cell straddles it without
     * matching. If the scan retires the interval at the straddling cell, the NULL cell's row is lost.
     */
    @Test
    public void testNullCellHoldsTheOnlyMatch() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T02:00:00.000000Z',null,2.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts <= '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts <= '2023-01-02T02:30:00.000000Z' AND exch IS NULL");
            assertTwinEqual("");
        });
    }

    /**
     * The mirror: the NULL cell is the one that straddles the window without matching, and a non-NULL
     * sibling holds the row.
     */
    @Test
    public void testNullCellStraddlesWithoutMatching() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z',null,1.0),"
                    + "('2023-01-02T03:00:00.000000Z',null,3.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts <= '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * The NULL cell created in a LATER commit, so it takes a high cellKey and is the first cell a
     * BACKWARD scan meets — with its rows entirely below the queried window.
     */
    @Test
    public void testNullCellFirstInBackwardOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T05:00:00.000000Z','E0',5.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z',null,1.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts >= '2023-01-02T04:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z'");
            assertTwinEqual(" WHERE ts = '2023-01-02T05:00:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * A whole day whose only cell is the NULL one, plus a neighbouring day with ordinary cells, queried
     * across the boundary. The NULL cell being the ONLY cell of its day makes it both the first and last
     * cell that day, which is the degenerate end of the sibling logic.
     */
    @Test
    public void testDayWithOnlyTheNullCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T20:00:00.000000Z',null,1.0),"
                    + "('2023-01-01T22:00:00.000000Z',null,2.0),"
                    + "('2023-01-02T02:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T04:00:00.000000Z','E1',4.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts >= '2023-01-01T18:00:00.000000Z' AND ts < '2023-01-02T06:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-01T21:00:00.000000Z' AND ts <= '2023-01-02T03:00:00.000000Z'");
            assertTwinEqual(" WHERE ts = '2023-01-01T22:00:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * NULL and non-NULL cells interleaved across several days, with windows that each cell is variously
     * inside, straddling, above and below.
     */
    @Test
    public void testMixedNullAndNonNullAcrossDays() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            final StringBuilder rows = new StringBuilder();
            boolean first = true;
            for (String day : new String[]{"2023-01-01", "2023-01-02"}) {
                for (int hour = 1; hour <= 5; hour += 2) {
                    for (String exch : new String[]{"'E0'", "null", "'E1'"}) {
                        if (!first) {
                            rows.append(',');
                        }
                        first = false;
                        rows.append("('").append(day).append("T0").append(hour).append(":00:00.000000Z',")
                                .append(exch).append(',').append(hour).append(".0)");
                    }
                }
            }
            insertIntoBoth(rows.toString());
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T03:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-01T04:00:00.000000Z' AND ts <= '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE exch IS NULL AND ts >= '2023-01-01T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'");
            assertTwinEqual("");
        });
    }


}
