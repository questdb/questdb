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
 * {@code LIMIT} over a composite interval scan, forward and backward.
 * <p>
 * The sibling-cell fixes live inside {@code next(long skipTarget)}, and {@code skipTarget} is the LIMIT
 * pushdown: the engine tells the cursor how many rows it may skip before it needs to start producing.
 * Skipping and the sibling-cell advance both move {@code partitionLo}, so they interact, and nothing
 * covered that combination for a composite table.
 * <p>
 * Data is the shape that actually stresses the fix — cell E0 straddles the interval without matching it
 * while sibling E1 holds the matching rows — so a LIMIT that stopped early at the wrong cell shows up
 * as a short or empty result rather than merely a reordered one.
 * <p>
 * Every case is compared against a plain twin fed identical rows. Backward cases use a single sort key
 * so the backward cursor is genuinely selected, and project {@code ts} so tied timestamps cannot make
 * the comparison flap.
 */
public class CompositeIntervalLimitTest extends AbstractCompositeTwinTest {

    @Test
    public void testLimitForwardOverSiblingCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            for (String limit : new String[]{" LIMIT 1", " LIMIT 2", " LIMIT 3", " LIMIT 100", " LIMIT 0"}) {
                assertTwinEqual(" WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'",
                        " ORDER BY ts", limit);
            }
        });
    }

    @Test
    public void testLimitBackwardOverSiblingCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            for (String limit : new String[]{" LIMIT 1", " LIMIT 2", " LIMIT 3", " LIMIT 100"}) {
                assertTwinEqualTimestampsOnly(
                        " WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T04:00:00.000000Z'",
                        " ORDER BY ts DESC", limit);
            }
        });
    }

    /**
     * {@code LIMIT -n} asks for the LAST n rows, which drives the cursor from the other end.
     */
    @Test
    public void testNegativeLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            for (String limit : new String[]{" LIMIT -1", " LIMIT -2", " LIMIT -100"}) {
                assertTwinEqual(" WHERE ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T05:00:00.000000Z'",
                        " ORDER BY ts", limit);
            }
        });
    }

    /**
     * {@code LIMIT lo,hi} — a non-zero starting offset is exactly what makes the engine pass a non-zero
     * skipTarget, the parameter the changed methods take.
     */
    @Test
    public void testLimitWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            fillSiblingShape();
            for (String limit : new String[]{" LIMIT 1,3", " LIMIT 2,4", " LIMIT 0,2", " LIMIT 3,100"}) {
                assertTwinEqual(" WHERE ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T05:00:00.000000Z'",
                        " ORDER BY ts", limit);
                assertTwinEqualTimestampsOnly(
                        " WHERE ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T05:00:00.000000Z'",
                        " ORDER BY ts DESC", limit);
            }
        });
    }

    /**
     * A wider table: many cells, most holding no row inside the queried window, with LIMIT stopping the
     * scan part-way. If a wrongly-retired interval left rows unvisited, a small LIMIT is the most likely
     * thing to mask it — so this checks small limits specifically.
     */
    @Test
    public void testSmallLimitsOverManyCells() throws Exception {
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
            for (String limit : new String[]{" LIMIT 1", " LIMIT 2", " LIMIT 5"}) {
                assertTwinEqual(where, " ORDER BY ts, exch, px", limit);
                assertTwinEqualTimestampsOnly(where, " ORDER BY ts DESC", limit);
            }
        });
    }

    private void assertTwinEqual(String where, String order, String limit) throws SqlException {
        assertSqlCursors("SELECT * FROM p" + where + order + limit, "SELECT * FROM c" + where + order + limit);
    }

    private void assertTwinEqualTimestampsOnly(String where, String order, String limit) throws SqlException {
        assertSqlCursors("SELECT ts FROM p" + where + order + limit, "SELECT ts FROM c" + where + order + limit);
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
