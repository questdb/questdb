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
 * Interval scans against a REUSED reader — the coverage shape that hid a wrong-answer bug once already.
 * <p>
 * A composite table served a stale scan after a cell was extended, and 328 tests missed it because they
 * all write-then-read or call {@code releaseInactive()} between commits, which throws the reader away.
 * The bug needed a reader POOLED ACROSS the commits. That lesson was applied to full scans; interval
 * scans never got the same treatment, and they resolve partitions through a different path
 * ({@code cullPartitions} plus the interval cursors) that has its own view of which cells exist.
 * <p>
 * So these tests deliberately do NOT call {@code engine.releaseInactive()} between commits: each query
 * runs against a reader that has already served an earlier query on the same table. A cell created or
 * extended after that first query must still be visible to the interval scan.
 * <p>
 * Both directions, and each interval query is checked against the plain twin rather than a hand-computed
 * number.
 */
public class CompositeIntervalReaderReloadTest extends AbstractCompositeTwinTest {

    /**
     * A brand-new CELL appears inside a window the reader has already scanned. The new cell changes the
     * partition array's shape (an extra entry at the same day), which is what the interval cursors walk.
     */
    @Test
    public void testNewCellVisibleToIntervalScanOnReusedReader() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T02:00:00.000000Z','E0',1.0)");
            drainWalQueue();

            final String where = " WHERE ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T05:00:00.000000Z'";
            assertTwinEqual(where);

            // second commit introduces a NEW cell inside the same window -- no releaseInactive
            insertIntoBoth("('2023-01-02T03:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            assertTwinEqual(where);

            // and a third, so the reader reloads more than once
            insertIntoBoth("('2023-01-02T04:00:00.000000Z','E2',3.0)");
            drainWalQueue();
            assertTwinEqual(where);
        });
    }

    /**
     * An EXISTING cell is extended with rows inside a window already scanned — the exact operation
     * behind the original stale-scan defect, now through the interval path.
     */
    @Test
    public void testExtendedCellVisibleToIntervalScanOnReusedReader() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T02:00:00.000000Z','E0',1.0),('2023-01-02T02:10:00.000000Z','E1',2.0)");
            drainWalQueue();

            final String where = " WHERE ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T05:00:00.000000Z'";
            assertTwinEqual(where);

            insertIntoBoth("('2023-01-02T03:00:00.000000Z','E0',3.0),('2023-01-02T03:10:00.000000Z','E1',4.0)");
            drainWalQueue();
            assertTwinEqual(where);

            insertIntoBoth("('2023-01-02T04:00:00.000000Z','E0',5.0)");
            drainWalQueue();
            assertTwinEqual(where);
        });
    }

    /**
     * The sibling-cell shape, built up ACROSS commits on a reused reader: the first query sees only a
     * cell that straddles the window without matching it, and the row that matches arrives afterwards in
     * a different cell.
     */
    @Test
    public void testSiblingCellArrivingLaterOnReusedReader() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),('2023-01-02T03:00:00.000000Z','E0',3.0)");
            drainWalQueue();

            final String point = " WHERE ts = '2023-01-02T02:00:00.000000Z'";
            assertTwinEqual(point); // empty on both, and the reader is now warm

            insertIntoBoth("('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            assertTwinEqual(point); // must now find the new cell's row
        });
    }

    /**
     * A new cell in a NEW day, arriving after the window has been scanned — this moves the interval's
     * high boundary onto a different partition, which is resolved by {@code cullPartitions} rather than
     * by the cursors themselves.
     */
    @Test
    public void testNewDayVisibleToIntervalScanOnReusedReader() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T02:00:00.000000Z','E0',1.0),('2023-01-02T04:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            final String where = " WHERE ts >= '2023-01-02T00:00:00.000000Z' AND ts < '2023-01-04T00:00:00.000000Z'";
            assertTwinEqual(where);

            insertIntoBoth("('2023-01-03T02:00:00.000000Z','E0',3.0),('2023-01-03T03:00:00.000000Z','E2',4.0)");
            drainWalQueue();
            assertTwinEqual(where);
        });
    }


}
