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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.std.BinarySequence;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class PostingCoverStreamingTest extends AbstractCairoTest {
    private static final int BATCH_ROWS = 4096;
    private static final int BINARY_SLOT = 3 * Long.BYTES;
    private static final int FIXED_WORDS = 5;
    private static final int KEY_COUNT = 64;
    private static final int ROW_COUNT = 262_144;

    @Test
    public void testBorrowedBatches() throws Exception {
        assertMemoryLeak(() -> checkBatches(SealBudget.UNLIMITED));
    }

    @Test
    public void testLowMemorySeal() throws Exception {
        assertMemoryLeak(() -> checkBatches(SealBudget.LIMITED));
    }

    private static void checkBatches(SealBudget budget) {
        long fixed = 0;
        long binary = 0;
        long aux = 0;
        try {
            fixed = Unsafe.malloc((long) FIXED_WORDS * BATCH_ROWS * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            binary = Unsafe.malloc((long) BATCH_ROWS * BINARY_SLOT, MemoryTag.NATIVE_DEFAULT);
            aux = Unsafe.malloc((BATCH_ROWS + 1L) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int rootLen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "stream", COLUMN_NAME_TXN_NONE)) {
                    writer.enableCoverStreaming();
                    LongList sizes = new LongList();
                    sizes.add((long) BATCH_ROWS * Long.BYTES);
                    sizes.add(4L * BATCH_ROWS * Long.BYTES);
                    sizes.add((long) BATCH_ROWS * BINARY_SLOT);
                    LongList auxSizes = new LongList();
                    auxSizes.add(0);
                    auxSizes.add(0);
                    auxSizes.add((BATCH_ROWS + 1L) * Long.BYTES);
                    for (int first = 0; first < ROW_COUNT; first += BATCH_ROWS) {
                        fillBatch(fixed, binary, aux, first);
                        writer.configureCovering(
                                new long[]{fixed, fixed + (long) BATCH_ROWS * Long.BYTES, binary},
                                new long[]{0, 0, aux}, new long[]{first, first, first},
                                new int[]{3, 5, -1}, new int[]{1, 2, 3},
                                new int[]{ColumnType.LONG, ColumnType.LONG256, ColumnType.BINARY}, 3, -1
                        );
                        writer.setCoveredColumnAddrSizes(sizes, auxSizes);
                        for (int i = 0; i < BATCH_ROWS; i++) {
                            writer.add((first + i) % KEY_COUNT, first + i);
                        }
                        writer.commit();
                        writer.releaseCoveredColumnReadMappings();
                        // Destroy each batch before sealing or decoding the next one.
                        Unsafe.setMemory(fixed, (long) FIXED_WORDS * BATCH_ROWS * Long.BYTES, (byte) 0xa5);
                        Unsafe.setMemory(binary, (long) BATCH_ROWS * BINARY_SLOT, (byte) 0xa5);
                        Unsafe.setMemory(aux, (BATCH_ROWS + 1L) * Long.BYTES, (byte) 0xa5);
                        if (first + BATCH_ROWS == ROW_COUNT / 2) {
                            writer.seal();
                        }
                    }
                    // Merge previously compressed covers with newer raw generations.
                    long savedLimit = Unsafe.getRssMemLimit();
                    try {
                        if (budget == SealBudget.LIMITED) {
                            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 4L * 1024 * 1024);
                        }
                        writer.seal();
                        Assert.assertEquals(budget == SealBudget.LIMITED, writer.isLastSealStreamingForTesting());
                        Assert.assertEquals(1, writer.getGenCount());
                    } finally {
                        Unsafe.setRssMemLimit(savedLimit);
                    }
                }
                GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("sym", ColumnType.SYMBOL, IndexType.NONE, 0, true, null, 0, false));
                metadata.add(new TableColumnMetadata("a", ColumnType.LONG, IndexType.NONE, 0, false, null, 1, false));
                metadata.add(new TableColumnMetadata("b", ColumnType.LONG256, IndexType.NONE, 0, false, null, 2, false));
                metadata.add(new TableColumnMetadata("bin", ColumnType.BINARY, IndexType.NONE, 0, false, null, 3, false));
                try (ColumnVersionReader versions = new ColumnVersionReader();
                     PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path.trimTo(rootLen),
                             "stream", COLUMN_NAME_TXN_NONE, 0, 0, metadata, versions, 0)) {
                    for (int key = 0; key < KEY_COUNT; key++) {
                        try (CoveringRowCursor cursor = (CoveringRowCursor) reader.getCursor(key, 0, Long.MAX_VALUE, new int[]{0, 1, 2})) {
                            for (long row = key; row < ROW_COUNT; row += KEY_COUNT) {
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertEquals(row, cursor.next());
                                Assert.assertEquals(row % 11 == 0 ? Long.MIN_VALUE : row, cursor.getCoveredLong(0));
                                Assert.assertEquals(row, cursor.getCoveredLong256_0(1));
                                Assert.assertEquals(~row, cursor.getCoveredLong256_1(1));
                                Assert.assertEquals(row * 7, cursor.getCoveredLong256_2(1));
                                Assert.assertEquals(~(row * 7), cursor.getCoveredLong256_3(1));
                                BinarySequence value = cursor.getCoveredBin(2);
                                if (row % 17 == 0) {
                                    Assert.assertNull(value);
                                } else {
                                    int len = row % 19 == 0 ? 0 : 2 * Long.BYTES;
                                    Assert.assertNotNull(value);
                                    Assert.assertEquals(len, value.length());
                                    for (int i = 0; i < len; i++) {
                                        Assert.assertEquals(i < Long.BYTES ? (byte) (row >>> (i * Byte.SIZE)) : 0, value.byteAt(i));
                                    }
                                }
                            }
                            Assert.assertFalse(cursor.hasNext());
                        }
                    }
                }
            }
        } finally {
            if (aux != 0) {
                Unsafe.free(aux, (BATCH_ROWS + 1L) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            if (binary != 0) {
                Unsafe.free(binary, (long) BATCH_ROWS * BINARY_SLOT, MemoryTag.NATIVE_DEFAULT);
            }
            if (fixed != 0) {
                Unsafe.free(fixed, (long) FIXED_WORDS * BATCH_ROWS * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static void fillBatch(long fixed, long binary, long aux, int first) {
        for (int i = 0; i < BATCH_ROWS; i++) {
            long row = first + i;
            Unsafe.putLong(fixed + (long) i * Long.BYTES, row % 11 == 0 ? Long.MIN_VALUE : row);
            long wide = fixed + ((long) BATCH_ROWS + 4L * i) * Long.BYTES;
            Unsafe.putLong(wide, row);
            Unsafe.putLong(wide + Long.BYTES, ~row);
            Unsafe.putLong(wide + 2 * Long.BYTES, row * 7);
            Unsafe.putLong(wide + 3 * Long.BYTES, ~(row * 7));
            Unsafe.putLong(aux + (long) i * Long.BYTES, (long) i * BINARY_SLOT);
            long value = binary + (long) i * BINARY_SLOT;
            Unsafe.putLong(value, row % 17 == 0 ? -1 : row % 19 == 0 ? 0 : 2 * Long.BYTES);
            Unsafe.putLong(value + Long.BYTES, row);
            Unsafe.putLong(value + 2 * Long.BYTES, 0);
        }
        Unsafe.putLong(aux + (long) BATCH_ROWS * Long.BYTES, (long) BATCH_ROWS * BINARY_SLOT);
    }

    private enum SealBudget {
        LIMITED, UNLIMITED
    }
}
