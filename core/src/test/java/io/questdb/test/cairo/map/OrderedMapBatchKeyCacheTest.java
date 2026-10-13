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


package io.questdb.test.cairo.map;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Differential test of OrderedMap's batch key cache: a batch probe of more rows than the cache's trial
 * (which takes the cache) writes the same entries, and leaves the same map, as single-row probes (which
 * never take it), for fixed-size keys of 4, 12, 24 and 40 bytes, few or many distinct keys, a
 * cardinality change mid-batch, and slot collisions.
 */
public class OrderedMapBatchKeyCacheTest {
    private static final int ROWS = 20_000;

    @Test
    public void testFewKeys() throws Exception {
        for (int longs = 0; longs <= 4; longs++) {
            assertBatchMatchesSingleRows(longs, 50, 0, false, 1);
            assertBatchMatchesSingleRows(longs, 50, 0, true, 2);
        }
    }

    @Test
    public void testManyKeys() throws Exception {
        for (int longs = 0; longs <= 4; longs++) {
            assertBatchMatchesSingleRows(longs, 100_000, 0, false, 3);
            assertBatchMatchesSingleRows(longs, 100_000, 0, true, 4);
        }
    }

    @Test
    public void testCardinalityChangesMidBatch() throws Exception {
        // a few keys first (the cache passes its trial), then a run of unique keys, then few again
        for (int longs = 0; longs <= 2; longs++) {
            assertBatchMatchesSingleRows(longs, 8, 1, false, 5);
            assertBatchMatchesSingleRows(longs, 8, 1, true, 6);
        }
    }

    @Test
    public void testOneKey() throws Exception {
        assertBatchMatchesSingleRows(0, 1, 0, true, 7);
        assertBatchMatchesSingleRows(1, 1, 0, false, 8);
    }

    private static void assertBatchMatchesSingleRows(int longs, int distinct, int shape, boolean filtered, long seed) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(seed, seed * 31);
            final int[] ints = new int[ROWS];
            final long[][] ls = new long[longs][ROWS];
            for (int i = 0; i < ROWS; i++) {
                final int k;
                if (shape == 1 && i >= ROWS / 3 && i < 2 * ROWS / 3) {
                    k = 1_000_000 + i; // unique
                } else {
                    k = rnd.nextInt(distinct);
                }
                // keys that differ only in the long parts, or only in the int part
                ints[i] = longs == 0 ? k * 7 - 3 : k & 3;
                for (int j = 0; j < longs; j++) {
                    ls[j][i] = longs == 0 ? 0 : (k >> 2) * 1_000_003L + j;
                }
            }
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            for (int j = 0; j < longs; j++) {
                keyTypes.add(ColumnType.LONG);
            }
            final SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
            final int batch = 2048;
            try (
                    OrderedMap expectedMap = new OrderedMap(1024, keyTypes, valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    OrderedMap actualMap = new OrderedMap(1024, keyTypes, valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    DirectLongList rowIds = new DirectLongList(ROWS, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList expected = new DirectLongList(ROWS, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList actual = new DirectLongList(ROWS, MemoryTag.NATIVE_DEFAULT)
            ) {
                for (int i = 0; i < ROWS; i++) {
                    if (!filtered || rnd.nextInt(5) != 0) {
                        rowIds.add(i);
                    }
                }
                final long n = filtered ? rowIds.size() : ROWS;
                expected.setPos(n);
                actual.setPos(n);
                final ArrayRecord record = new ArrayRecord(ints, ls);
                final RecordSink sink = new ArraySink(longs);
                for (long lo = 0; lo < n; lo += batch) {
                    final long hi = Math.min(n, lo + batch);
                    long actualBase;
                    actualMap.reserveCapacity(hi - lo);
                    if (filtered) {
                        actualBase = actualMap.probeBatchFiltered(record, sink, rowIds.getAddress(), lo, hi, actual.getAddress() + (lo << 3));
                    } else {
                        // the unfiltered probe takes frame rows lo..hi
                        actualBase = actualMap.probeBatch(record, sink, lo, hi, actual.getAddress() + (lo << 3));
                    }
                    long expectedBase = 0;
                    for (long p = lo; p < hi; p++) {
                        expectedMap.reserveCapacity(1);
                        expectedBase = filtered
                                ? expectedMap.probeBatchFiltered(record, sink, rowIds.getAddress(), p, p + 1, expected.getAddress() + (p << 3))
                                : expectedMap.probeBatch(record, sink, p, p + 1, expected.getAddress() + (p << 3));
                    }
                    for (long p = lo; p < hi; p++) {
                        final long e = expected.get(p);
                        final long a = actual.get(p);
                        final String msg = "longs=" + longs + ", distinct=" + distinct + ", filtered=" + filtered + ", row " + p;
                        Assert.assertEquals(msg, Map.decodeBatchRowIndex(e), Map.decodeBatchRowIndex(a));
                        Assert.assertEquals(msg, Map.isNewBatchEntry(e), Map.isNewBatchEntry(a));
                        Assert.assertEquals(msg, Map.decodeBatchOffset(e), Map.decodeBatchOffset(a));
                        Assert.assertNotNull(actualMap.valueAt(actualBase + Map.decodeBatchOffset(a)));
                        Assert.assertNotNull(expectedMap.valueAt(expectedBase + Map.decodeBatchOffset(e)));
                    }
                    Assert.assertEquals(expectedMap.size(), actualMap.size());
                }
            }
        });
    }

    private static class ArrayRecord extends PageFrameMemoryRecord {
        private final int[] ints;
        private final long[][] longs;

        ArrayRecord(int[] ints, long[][] longs) {
            this.ints = ints;
            this.longs = longs;
        }

        @Override
        public int getInt(int columnIndex) {
            return ints[(int) rowIndex];
        }

        @Override
        public long getLong(int columnIndex) {
            return longs[columnIndex - 1][(int) rowIndex];
        }
    }

    private static class ArraySink implements RecordSink {
        private final int longs;

        ArraySink(int longs) {
            this.longs = longs;
        }

        @Override
        public void copy(Record r, RecordSinkSPI w) {
            w.putInt(r.getInt(0));
            for (int j = 0; j < longs; j++) {
                w.putLong(r.getLong(j + 1));
            }
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    }
}
