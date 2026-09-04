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

import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Sub-project 9A rests on ONE fact about the reader: a day's cells are CONTIGUOUS in partition-index
 * order, so a day is a run {@code [runLo, runHi)} of partitions sharing one partition timestamp.
 * <p>
 * Both interval cursors already depend on it -- {@code hasSameDaySiblingAhead}/{@code Below} only ever
 * look at {@code ±1} -- but nothing asserted it. This does, directly against a reader rather than
 * through a query, at the edges every branch of the new walk touches: the first run, the last run, a
 * single-cell run, a start in the MIDDLE of a run, and a run that would extend past a culled bound.
 * <p>
 * If this test ever fails, the 9A design is invalid rather than the test being stale: the cell-major
 * inner walk assumes it can find a day's every cell by scanning adjacent indices.
 */
public class CompositeDayRunUnitTest extends AbstractCairoTest {

    @Test
    public void testRunBoundsOverMixedCellCounts() throws Exception {
        assertMemoryLeak(() -> {
            // day 1: 1 cell, day 2: 3 cells, day 3: 2 cells
            execute("create table c (ts timestamp, exch symbol, px double)"
                    + " timestamp(ts) partition by day, exch layout plain wal");
            execute("insert into c values"
                    + " ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + " ('2023-01-02T01:00:00.000000Z','E0',2.0),"
                    + " ('2023-01-02T02:00:00.000000Z','E1',3.0),"
                    + " ('2023-01-02T03:00:00.000000Z','E2',4.0),"
                    + " ('2023-01-03T01:00:00.000000Z','E0',5.0),"
                    + " ('2023-01-03T02:00:00.000000Z','E1',6.0)");
            drainWalQueue();

            try (TableReader reader = getReader("c")) {
                Assert.assertEquals(6, reader.getPartitionCount());

                // forward: one past the last index of the run containing `from`
                assertForwardRun(reader, 0, 6, 1);   // day 1, single cell
                assertForwardRun(reader, 1, 6, 4);   // day 2, three cells
                assertForwardRun(reader, 2, 6, 4);   // starting MID-run still ends at 4
                assertForwardRun(reader, 3, 6, 4);   // last cell of the run
                assertForwardRun(reader, 4, 6, 6);   // day 3, two cells, ends at the bound
                assertForwardRun(reader, 1, 3, 3);   // a culled hi bound must clamp the run

                // backward: first index of the run containing `from`
                assertBackwardRun(reader, 5, 0, 4);  // day 3 starts at 4
                assertBackwardRun(reader, 4, 0, 4);
                assertBackwardRun(reader, 3, 0, 1);  // day 2 starts at 1
                assertBackwardRun(reader, 2, 0, 1);  // starting MID-run still starts at 1
                assertBackwardRun(reader, 0, 0, 0);  // day 1, single cell
                assertBackwardRun(reader, 3, 2, 2);  // a culled lo bound must clamp the run
            }
        });
    }

    /**
     * A plain table's every run is exactly one partition. That is what makes the cell-major inner walk
     * a no-op there, and therefore what keeps plain byte-identical -- so it is asserted, not assumed.
     */
    @Test
    public void testPlainTableRunsAreAlwaysSingleCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
            execute("insert into p values"
                    + " ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + " ('2023-01-02T01:00:00.000000Z','E0',2.0),"
                    + " ('2023-01-02T02:00:00.000000Z','E1',3.0),"
                    + " ('2023-01-03T01:00:00.000000Z','E0',4.0)");
            drainWalQueue();

            try (TableReader reader = getReader("p")) {
                final int n = reader.getPartitionCount();
                Assert.assertEquals(3, n);
                for (int i = 0; i < n; i++) {
                    assertForwardRun(reader, i, n, i + 1);
                    assertBackwardRun(reader, i, 0, i);
                }
            }
        });
    }

    private static void assertBackwardRun(TableReader reader, int from, int loBound, int expected) {
        final long ts = reader.getPartitionTimestampByIndex(from);
        int start = from;
        while (start > loBound && reader.getPartitionTimestampByIndex(start - 1) == ts) {
            start--;
        }
        Assert.assertEquals("backward run start from " + from + " (loBound " + loBound + ")", expected, start);
    }

    private static void assertForwardRun(TableReader reader, int from, int hiBound, int expected) {
        final long ts = reader.getPartitionTimestampByIndex(from);
        int end = from + 1;
        while (end < hiBound && reader.getPartitionTimestampByIndex(end) == ts) {
            end++;
        }
        Assert.assertEquals("forward run end from " + from + " (hiBound " + hiBound + ")", expected, end);
    }
}
