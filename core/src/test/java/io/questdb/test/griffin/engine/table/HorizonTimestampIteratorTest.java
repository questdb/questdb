/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonTimestampIterator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Compares every timestamp that {@link HorizonTimestampIterator} emits with a brute-force
 * merge of the master rows and offsets. Each case runs the iterator twice, so the second
 * pass starts over the ring buffer that the first pass grew.
 */
public class HorizonTimestampIteratorTest extends AbstractCairoTest {
    private static final long HOUR = 3_600_000_000L;
    private static final Log LOG = LogFactory.getLog(HorizonTimestampIteratorTest.class);
    private static final long MILLI = 1_000L;
    private static final long SECOND = 1_000_000L;

    @Test
    public void testEmptyMaster() throws Exception {
        assertMemoryLeak(() -> {
            createMaster();
            assertIteratorMatchesBruteForce(new long[]{-SECOND, 0, SECOND});
            assertIteratorMatchesBruteForce(new long[]{SECOND});
        });
    }

    @Test
    public void testFuzzMatchesBruteForce() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            createMaster();
            for (int iteration = 0; iteration < 20; iteration++) {
                execute("TRUNCATE TABLE master");
                final int rowCount = rnd.nextInt(20_000);
                final int gapMode = rnd.nextInt(4);
                try (TableWriter writer = getWriter("master")) {
                    long ts = 0;
                    for (int i = 0; i < rowCount; i++) {
                        ts += switch (gapMode) {
                            // duplicate timestamps
                            case 0 -> rnd.nextInt(3) * SECOND;
                            // bursts with rare long gaps
                            case 1 -> rnd.nextInt(50) == 0 ? rnd.nextLong(HOUR) : rnd.nextInt(10_000);
                            // sparse, then dense: the window grows after it has wrapped around the ring
                            case 2 -> i < rowCount / 2 ? 10 * SECOND : 10 * MILLI;
                            default -> 1 + rnd.nextInt(1_000) * MILLI;
                        };
                        writer.newRow(ts).append();
                    }
                    writer.commit();
                }

                final int offsetCount = 1 + rnd.nextInt(12);
                final long span = switch (rnd.nextInt(3)) {
                    case 0 -> 10 * SECOND;
                    case 1 -> HOUR;
                    // beyond the time range of most master tables
                    default -> 24 * HOUR;
                };
                final long[] offsets = new long[offsetCount];
                for (int i = 0; i < offsetCount; i++) {
                    offsets[i] = rnd.nextLong(2 * span) - span;
                }
                Arrays.sort(offsets);
                // offsets must be strictly increasing, as RANGE and LIST produce them
                for (int i = 1; i < offsetCount; i++) {
                    if (offsets[i] <= offsets[i - 1]) {
                        offsets[i] = offsets[i - 1] + 1;
                    }
                }
                LOG.info().$("iteration=").$(iteration)
                        .$(", rows=").$(rowCount)
                        .$(", gapMode=").$(gapMode)
                        .$(", offsets=").$(offsetCount)
                        .$(", span=").$(span)
                        .$();
                assertIteratorMatchesBruteForce(offsets);
            }
        });
    }

    @Test
    public void testSingleOffset() throws Exception {
        assertMemoryLeak(() -> {
            createMaster();
            insertMasterRows(1_000, 0, SECOND);
            assertIteratorMatchesBruteForce(new long[]{-5 * SECOND});
        });
    }

    @Test
    public void testSpanBeyondMasterTimeRange() throws Exception {
        // The master rows cover 1,000 seconds, and the largest offset is 12 hours. The window
        // holds every master row until the 12h stream starts, then drains one row at a time.
        assertMemoryLeak(() -> {
            createMaster();
            insertMasterRows(20_000, 0, 50 * MILLI);
            final long[] offsets = new long[13];
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = i * HOUR;
            }
            assertIteratorMatchesBruteForce(offsets);
        });
    }

    @Test
    public void testWindowGrowsWhileWrapped() throws Exception {
        // 2,000 sparse rows keep the window at about 7 rows, so its head wraps around the
        // 64-slot ring many times. 10,000 dense rows then grow the window to about 6,000 rows,
        // and the ring doubles while the window wraps around its end.
        assertMemoryLeak(() -> {
            createMaster();
            final long denseStart = insertMasterRows(2_000, 0, 10 * SECOND);
            insertMasterRows(10_000, denseStart, 10 * MILLI);
            assertIteratorMatchesBruteForce(new long[]{0, 30 * SECOND, 60 * SECOND});
        });
    }

    private static void assertIteratorMatchesBruteForce(long[] offsets) throws Exception {
        try (
                RecordCursorFactory factory = select("master");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                HorizonTimestampIterator iterator = new HorizonTimestampIterator(offsets)
        ) {
            final int timestampIndex = factory.getMetadata().getTimestampIndex();
            final LongList rowIdList = new LongList();
            final LongList timestampList = new LongList();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                rowIdList.add(record.getRowId());
                timestampList.add(record.getTimestamp(timestampIndex));
            }
            final int rowCount = rowIdList.size();
            final long[] rowIds = new long[rowCount];
            for (int i = 0; i < rowCount; i++) {
                rowIds[i] = rowIdList.getQuick(i);
                if (i > 0) {
                    // the lookup below relies on ascending rowIds
                    Assert.assertTrue(rowIds[i] > rowIds[i - 1]);
                }
            }

            for (int pass = 0; pass < 2; pass++) {
                cursor.toTop();
                iterator.of(cursor, cursor.getRecordB(), timestampIndex);
                final int[] lastIndexByOffset = new int[offsets.length];
                Arrays.fill(lastIndexByOffset, -1);
                final int[] countByOffset = new int[offsets.length];
                long prevHorizonTs = Long.MIN_VALUE;
                while (iterator.next()) {
                    final int offsetIndex = iterator.getOffsetIndex();
                    final int index = Arrays.binarySearch(rowIds, iterator.getMasterRowId());
                    Assert.assertTrue("unknown rowId: " + iterator.getMasterRowId(), index >= 0);
                    // each offset stream visits every master row once, in master order
                    Assert.assertEquals(lastIndexByOffset[offsetIndex] + 1, index);
                    lastIndexByOffset[offsetIndex] = index;
                    countByOffset[offsetIndex]++;
                    final long horizonTs = iterator.getHorizonTimestamp();
                    Assert.assertEquals(timestampList.getQuick(index) + offsets[offsetIndex], horizonTs);
                    Assert.assertTrue("horizon timestamps must not decrease", horizonTs >= prevHorizonTs);
                    prevHorizonTs = horizonTs;
                }
                for (int k = 0; k < offsets.length; k++) {
                    Assert.assertEquals("pass " + pass + ", offset index " + k, rowCount, countByOffset[k]);
                }
            }
        }
    }

    private static void createMaster() throws Exception {
        execute("CREATE TABLE master (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
    }

    private static long insertMasterRows(int rowCount, long startTs, long gap) {
        long ts = startTs;
        try (TableWriter writer = getWriter("master")) {
            for (int i = 0; i < rowCount; i++) {
                writer.newRow(ts).append();
                ts += gap;
            }
            writer.commit();
        }
        return ts;
    }
}
