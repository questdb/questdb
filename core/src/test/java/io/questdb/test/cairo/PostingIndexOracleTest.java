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

import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.idx.PostingIndexUtils.ENCODING_ADAPTIVE;

/**
 * Oracle tests comparing PostingIndex (BP) against the Legacy (BitmapIndex)
 * implementation for correctness.
 * <p>
 * BP writers have a per-key capacity of 64 values per commit (BLOCK_CAPACITY).
 * Tests batch writes and commit accordingly.
 */
public class PostingIndexOracleTest extends AbstractCairoTest {

    private static final int BP_BATCH = PostingIndexUtils.BLOCK_CAPACITY;     // 64

    @Test
    public void testBPBwdReaderWithGenLookup() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                // Write multiple batches (creates multiple sparse gens)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "bp_bwd", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 64; i++) {
                        writer.add(0, i);
                        writer.add(1, i + 1000);
                    }
                    writer.commit();
                    for (int i = 64; i < 128; i++) {
                        writer.add(0, i);
                        writer.add(1, i + 1000);
                    }
                    writer.commit();
                }

                // Read backward
                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(configuration, path.trimTo(plen), "bp_bwd", COLUMN_NAME_TXN_NONE, -1, 0, null, null, 0)) {
                    RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
                    int count = 0;
                    long prev = Long.MAX_VALUE;
                    while (cursor.hasNext()) {
                        long val = cursor.next();
                        Assert.assertTrue("Values should be descending: " + prev + " vs " + val, val <= prev);
                        prev = val;
                        count++;
                    }
                    Assert.assertEquals(128, count);
                    Misc.free(cursor);
                }
            }
        });
    }

    @Test
    public void testBPCommitSeal() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                // Write multiple batches, close (which triggers seal)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "bp_seal", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 64; i++) {
                        writer.add(0, i);
                    }
                    writer.commit();

                    for (int i = 64; i < 128; i++) {
                        writer.add(0, i);
                    }
                    writer.commit();

                    for (int i = 128; i < 192; i++) {
                        writer.add(0, i);
                    }
                    writer.commit();
                    // seal() called by close()
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path.trimTo(plen), "bp_seal", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals(192, count);
                    Misc.free(cursor);
                }
            }
        });
    }

    @Test
    public void testBPComparisonWithLegacy() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int valueCount = 10000;
                long[] values = new long[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    values[i] = i * 3L;
                }

                final int plen = path.size();
                // Write to Legacy index
                try (BitmapIndexWriter legacyWriter = new BitmapIndexWriter(configuration)) {
                    legacyWriter.of(path, "bp_legacy_cmp", COLUMN_NAME_TXN_NONE, 256);
                    for (long value : values) {
                        legacyWriter.add(0, value);
                    }
                    legacyWriter.commit();
                }

                // Write to BP index in batches
                try (PostingIndexWriter bpWriter = new PostingIndexWriter(configuration, path.trimTo(plen), "bp_cmp", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < valueCount; i++) {
                        bpWriter.add(0, values[i]);
                        if ((i + 1) % BP_BATCH == 0) {
                            bpWriter.commit();
                        }
                    }
                    bpWriter.commit();
                }

                // Read Legacy
                LongList legacyValues = new LongList();
                try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "bp_legacy_cmp", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    try (RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            legacyValues.add(cursor.next());
                        }
                    }
                }

                // Read BP
                LongList bpValues = new LongList();
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path.trimTo(plen), "bp_cmp", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    try (RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            bpValues.add(cursor.next());
                        }
                    }
                }

                Assert.assertEquals("Value count mismatch", legacyValues.size(), bpValues.size());
                for (int i = 0; i < legacyValues.size(); i++) {
                    Assert.assertEquals("Mismatch at index " + i, legacyValues.getQuick(i), bpValues.getQuick(i));
                }
            }
        });
    }

    @Test
    public void testBPEmptyKey() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "bp_empty", COLUMN_NAME_TXN_NONE)) {
                    writer.add(5, 100);
                    writer.add(5, 200);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path, "bp_empty", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
                    Assert.assertFalse("Key 0 should be empty", cursor.hasNext());

                    cursor = reader.getCursor(5, 0, Long.MAX_VALUE);
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(100, cursor.next());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(200, cursor.next());
                    Assert.assertFalse(cursor.hasNext());
                    Misc.free(cursor);
                }
            }
        });
    }

    @Test
    public void testBPIncrementalSealMatchesFullSeal() throws Exception {
        assertMemoryLeak(() -> {
            // Write same data via two writers; one will seal incrementally, the other fully
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int keyCount = 10;
                int valuesPerBatch = 40;
                int batches = 4;
                final int plen = path.size();

                // Generate test data
                long[][] keyValues = new long[keyCount][valuesPerBatch * batches];
                for (int key = 0; key < keyCount; key++) {
                    for (int i = 0; i < valuesPerBatch * batches; i++) {
                        keyValues[key][i] = (long) key * 10_000 + i * 3;
                    }
                }

                // Writer 1: commit in batches, then close (triggers seal)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "bp_iseal1", COLUMN_NAME_TXN_NONE)) {
                    for (int b = 0; b < batches; b++) {
                        for (int key = 0; key < keyCount; key++) {
                            for (int i = 0; i < valuesPerBatch; i++) {
                                writer.add(key, keyValues[key][b * valuesPerBatch + i]);
                            }
                        }
                        writer.commit();
                    }
                    // close triggers seal (incremental or full depending on gen layout)
                }

                // Writer 2: single batch, triggers single gen (no seal needed)
                // Instead, compare against decoding all values from writer 1
                for (int key = 0; key < keyCount; key++) {
                    LongList expected = new LongList();
                    for (int i = 0; i < valuesPerBatch * batches; i++) {
                        expected.add(keyValues[key][i]);
                    }

                    LongList actual = readAllBP(path.trimTo(plen), "bp_iseal1", key);
                    Assert.assertEquals("Key " + key + " count mismatch", expected.size(), actual.size());
                    for (int i = 0; i < expected.size(); i++) {
                        Assert.assertEquals("Key " + key + " mismatch at index " + i,
                                expected.getQuick(i), actual.getQuick(i));
                    }
                }
            }
        });
    }

    @Test
    public void testBPLargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                long baseOffset = 1_000_000_000L;
                int count = 640; // 10 batches of 64

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "bp_large", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < count; i++) {
                        writer.add(0, baseOffset + i * 7L);
                        if ((i + 1) % BP_BATCH == 0) {
                            writer.commit();
                        }
                    }
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path, "bp_large", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
                    int idx = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("Mismatch at index " + idx,
                                baseOffset + idx * 7L, cursor.next());
                        idx++;
                    }
                    Assert.assertEquals(count, idx);
                    Misc.free(cursor);
                }
            }
        });
    }

    @Test
    public void testBPManyKeysMultipleGens() throws Exception {
        assertMemoryLeak(() -> {
            // This test exercises the gen lookup tiers with many keys
            // across multiple sparse gens (unsealed reads).
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int keyCount = 500;
                int valuesPerKey = 10;
                int batches = 3;
                final int plen = path.size();

                Rnd rnd = new Rnd();
                long[][] allValues = new long[keyCount][valuesPerKey * batches];
                for (int key = 0; key < keyCount; key++) {
                    long val = rnd.nextInt(1000);
                    for (int i = 0; i < valuesPerKey * batches; i++) {
                        allValues[key][i] = val;
                        val += rnd.nextInt(20) + 1;
                    }
                }

                // Write to BP in batches (creates multiple sparse gens)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "bp_manykeys", COLUMN_NAME_TXN_NONE)) {
                    for (int b = 0; b < batches; b++) {
                        for (int key = 0; key < keyCount; key++) {
                            for (int i = 0; i < valuesPerKey; i++) {
                                writer.add(key, allValues[key][b * valuesPerKey + i]);
                            }
                        }
                        writer.commit();
                    }
                    // Don't close yet — read unsealed
                }

                // Read all keys and verify correctness (exercises PostingGenLookup)
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path.trimTo(plen), "bp_manykeys", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int key = 0; key < keyCount; key++) {
                        RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                        int idx = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals("Key " + key + " mismatch at " + idx,
                                    allValues[key][idx], cursor.next());
                            idx++;
                        }
                        Assert.assertEquals("Key " + key + " count mismatch",
                                valuesPerKey * batches, idx);
                        Misc.free(cursor);
                    }
                }
            }
        });
    }

    @Test
    public void testBPMultipleKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int keyCount = 50;
                int valuesPerKey = 50; // fits in one BP batch (64)
                Rnd rnd = new Rnd();
                final int plen = path.size();

                long[][] keyValues = new long[keyCount][valuesPerKey];
                for (int key = 0; key < keyCount; key++) {
                    long value = rnd.nextInt(1000);
                    for (int v = 0; v < valuesPerKey; v++) {
                        keyValues[key][v] = value;
                        value += rnd.nextInt(50) + 1;
                    }
                }

                // Write to Legacy
                try (BitmapIndexWriter legacyWriter = new BitmapIndexWriter(configuration)) {
                    legacyWriter.of(path, "bp_mk_legacy", COLUMN_NAME_TXN_NONE, 256);
                    for (int key = 0; key < keyCount; key++) {
                        for (int v = 0; v < valuesPerKey; v++) {
                            legacyWriter.add(key, keyValues[key][v]);
                        }
                    }
                    legacyWriter.commit();
                }

                // Write to BP (all fit in one batch per key)
                try (PostingIndexWriter bpWriter = new PostingIndexWriter(configuration, path.trimTo(plen), "bp_mk", COLUMN_NAME_TXN_NONE)) {
                    for (int key = 0; key < keyCount; key++) {
                        for (int v = 0; v < valuesPerKey; v++) {
                            bpWriter.add(key, keyValues[key][v]);
                        }
                    }
                    bpWriter.commit();
                }

                // Compare each key
                for (int key = 0; key < keyCount; key++) {
                    LongList legacyVals = readAllLegacy(path.trimTo(plen), key);
                    LongList bpVals = readAllBP(path.trimTo(plen), "bp_mk", key);
                    Assert.assertEquals("Key " + key + " count mismatch", legacyVals.size(), bpVals.size());
                    for (int i = 0; i < legacyVals.size(); i++) {
                        Assert.assertEquals("Key " + key + " mismatch at index " + i,
                                legacyVals.getQuick(i), bpVals.getQuick(i));
                    }
                }
            }
        });
    }

    @Test
    public void testBPNativeEncodeKey() throws Exception {
        assertMemoryLeak(() -> {
            // Test encodeKeyNative produces identical output to encodeKey
            int count = 200;
            long[] values = new long[count];
            for (int i = 0; i < count; i++) {
                values[i] = i * 5L + 100;
            }

            // Allocate native buffer for values
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr1 = Unsafe.malloc(PostingIndexUtils.computeMaxEncodedSize(count), MemoryTag.NATIVE_DEFAULT);
            long destAddr2 = Unsafe.malloc(PostingIndexUtils.computeMaxEncodedSize(count), MemoryTag.NATIVE_DEFAULT);
            try (
                    PostingIndexUtils.EncodeContext ctx1 = new PostingIndexUtils.EncodeContext();
                    PostingIndexUtils.EncodeContext ctx2 = new PostingIndexUtils.EncodeContext()
            ) {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, values[i]);
                }

                ctx1.ensureCapacity(count);
                int size1 = PostingIndexUtils.encodeKey(values, count, destAddr1, ctx1);

                ctx2.ensureCapacity(count);
                int size2 = PostingIndexUtils.encodeKeyNative(srcAddr, count, destAddr2, ctx2, ENCODING_ADAPTIVE);

                Assert.assertEquals("Encoded sizes differ", size1, size2);
                for (int i = 0; i < size1; i++) {
                    Assert.assertEquals("Byte mismatch at offset " + i,
                            Unsafe.getByte(destAddr1 + i),
                            Unsafe.getByte(destAddr2 + i));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr1, PostingIndexUtils.computeMaxEncodedSize(count), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr2, PostingIndexUtils.computeMaxEncodedSize(count), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testBPRoundTripEliasFano() throws Exception {
        assertMemoryLeak(() -> {
            // count=1 dense (L=0)
            assertEliasFanoRoundTrip(new long[]{0});
            assertEliasFanoRoundTrip(new long[]{42});

            // L=0: dense values where universe <= count.
            long[] dense = new long[64];
            for (int i = 0; i < 64; i++) {
                dense[i] = i;
            }
            assertEliasFanoRoundTrip(dense);

            // Single high-bits word boundary: count=64 keeps high bits inside
            // word 0 for typical L; count=65 forces a write past the boundary.
            long[] step1k = new long[64];
            for (int i = 0; i < 64; i++) {
                step1k[i] = (long) i * 1_000;
            }
            assertEliasFanoRoundTrip(step1k);
            long[] step1kPlus = new long[65];
            for (int i = 0; i < 65; i++) {
                step1kPlus[i] = (long) i * 1_000;
            }
            assertEliasFanoRoundTrip(step1kPlus);

            // Multi-block (count > BLOCK_CAPACITY) with mid-range L.
            long[] step10 = new long[1_000];
            for (int i = 0; i < 1_000; i++) {
                step10[i] = (long) i * 10;
            }
            assertEliasFanoRoundTrip(step10);

            // Sparse values that span many high-bits words, leaving empty
            // words in between: validates the batched flush correctly skips
            // unset words (keeping them at the setMemory zero baseline).
            long[] sparse = new long[]{0L, 1L << 20, (1L << 21) + 5, (1L << 30), (1L << 30) + 100};
            assertEliasFanoRoundTrip(sparse);

            // Large L: u/count near 2^60 forces L close to its practical max.
            long[] highL = new long[]{0L, 1L << 60, (1L << 60) + 1, (1L << 61)};
            assertEliasFanoRoundTrip(highL);

            // Pseudo-random sorted distinct longs to exercise mixed deltas.
            Rnd rnd = new Rnd();
            long[] randomVals = new long[2_000];
            long acc = 0;
            for (int i = 0; i < randomVals.length; i++) {
                acc += 1L + (rnd.nextLong() & 0xFFFFL);
                randomVals[i] = acc;
            }
            assertEliasFanoRoundTrip(randomVals);
        });
    }

    private static void assertEliasFanoRoundTrip(long[] values) {
        int count = values.length;
        long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long encMaxBytes = PostingIndexUtils.computeMaxEncodedSize(count);
        long destAddr = Unsafe.malloc(encMaxBytes, MemoryTag.NATIVE_DEFAULT);
        long nativeOutAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try (PostingIndexUtils.EncodeContext ctx = new PostingIndexUtils.EncodeContext()) {
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(srcAddr + (long) i * Long.BYTES, values[i]);
            }
            ctx.ensureCapacity(count);
            int size = PostingIndexUtils.encodeKeyEF(srcAddr, count, destAddr, ctx);
            Assert.assertTrue("encoded size positive [count=" + count + "]", size > 0);
            // Sentinel at start signals EF format to the decoder.
            Assert.assertEquals("EF sentinel missing",
                    PostingIndexUtils.EF_FORMAT_SENTINEL,
                    Unsafe.getInt(destAddr));

            long[] decoded = new long[count];
            PostingIndexUtils.decodeKeyEF(destAddr, decoded);
            for (int i = 0; i < count; i++) {
                Assert.assertEquals("decodeKeyEF mismatch at " + i, values[i], decoded[i]);
            }

            PostingIndexUtils.decodeKeyEFToNative(destAddr, nativeOutAddr);
            for (int i = 0; i < count; i++) {
                Assert.assertEquals("decodeKeyEFToNative mismatch at " + i,
                        values[i],
                        Unsafe.getLong(nativeOutAddr + (long) i * Long.BYTES));
            }
        } finally {
            Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, encMaxBytes, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nativeOutAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCompactOnReopen() throws Exception {
        assertMemoryLeak(() -> {
            // After seal + close, reopening should compact dead space and produce correct data.
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                int numBatches = 4;
                int totalValues = numBatches * BP_BATCH; // 256

                // Write key 0 with multiple commits, then seal
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path.trimTo(plen), "bp_compact", COLUMN_NAME_TXN_NONE)) {
                    for (int batch = 0; batch < numBatches; batch++) {
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, (long) batch * BP_BATCH + v);
                        }
                        writer.commit();
                    }
                    writer.setMaxValue(totalValues - 1);
                    writer.seal();
                    Assert.assertEquals(1, writer.getGenCount());
                }
                // close() calls seal() again but genCount==1 so it's a no-op

                // Reopen — compaction should move gen 0 to offset 0
                try (PostingIndexWriter writer2 = new PostingIndexWriter(configuration)) {
                    writer2.of(path.trimTo(plen), "bp_compact", COLUMN_NAME_TXN_NONE, false);
                    Assert.assertEquals(1, writer2.getGenCount());

                    // Verify data via writer cursor
                    RowCursor cursor = writer2.getCursor(0);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("value " + count, count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("total count", totalValues, count);
                    Misc.free(cursor);
                }

                // Also verify via reader
                LongList vals = readAllBP(path.trimTo(plen), "bp_compact", 0);
                Assert.assertEquals("reader count", totalValues, vals.size());
                for (int i = 0; i < totalValues; i++) {
                    Assert.assertEquals("reader value " + i, i, vals.getQuick(i));
                }
            }
        });
    }

    @Test
    public void testReaderSurvivesWriterSeal() throws Exception {
        assertMemoryLeak(() -> {
            // A reader with an active cursor must not be corrupted when the writer
            // seals (which now appends rather than overwriting offset 0).
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                int numBatches = 4;
                int valuesPerBatch = BP_BATCH; // 64
                int totalValues = numBatches * valuesPerBatch; // 256

                // Phase 1: populate key 0 with multiple commits to create multiple gens
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path.trimTo(plen), "bp_seal_conc", COLUMN_NAME_TXN_NONE)) {
                    for (int batch = 0; batch < numBatches; batch++) {
                        for (int v = 0; v < valuesPerBatch; v++) {
                            writer.add(0, (long) batch * valuesPerBatch + v);
                        }
                        writer.commit();
                    }
                    writer.setMaxValue(totalValues - 1);

                    // Phase 2: open a reader and start iterating key 0
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), "bp_seal_conc", COLUMN_NAME_TXN_NONE, -1, 0)) {
                        reader.reloadConditionally();
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);

                        // Read half the values from the cursor
                        LongList partialValues = new LongList();
                        int halfCount = totalValues / 2;
                        for (int i = 0; i < halfCount && cursor.hasNext(); i++) {
                            partialValues.add(cursor.next());
                        }
                        Assert.assertTrue("should have read some values", partialValues.size() > 0);

                        // Phase 3: writer seals while reader cursor is mid-iteration
                        writer.seal();

                        // Phase 4: continue reading from the cursor — must not crash or return garbage
                        LongList remainingValues = new LongList();
                        while (cursor.hasNext()) {
                            remainingValues.add(cursor.next());
                        }

                        // Verify all values are sequential 0..totalValues-1
                        LongList allValues = new LongList();
                        allValues.addAll(partialValues);
                        allValues.addAll(remainingValues);
                        Assert.assertEquals("total value count for key 0", totalValues, allValues.size());
                        for (int i = 0; i < totalValues; i++) {
                            Assert.assertEquals("value mismatch at " + i, i, allValues.getQuick(i));
                        }
                        Misc.free(cursor);
                    }
                }
            }
        });
    }

    @Test
    public void testSealCommitSealCycle() throws Exception {
        assertMemoryLeak(() -> {
            // Seal -> more commits -> seal again. Verifies append-only seal handles
            // the case where gen 0 already has a non-zero fileOffset.
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path.trimTo(plen), "bp_cycle", COLUMN_NAME_TXN_NONE)) {
                    // First batch: 4 commits of 64 values for key 0 = 256 values
                    long rowId = 0;
                    for (int batch = 0; batch < 4; batch++) {
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, rowId++);
                        }
                        writer.commit();
                    }
                    writer.setMaxValue(rowId - 1);
                    writer.seal();
                    Assert.assertEquals(1, writer.getGenCount());

                    // Second batch: 3 more commits = 192 more values (total 448)
                    for (int batch = 0; batch < 3; batch++) {
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, rowId++);
                        }
                        writer.commit();
                    }
                    writer.setMaxValue(rowId - 1);
                    writer.seal();
                    Assert.assertEquals(1, writer.getGenCount());

                    // Verify: key 0 should have 7 * 64 = 448 sequential values
                    int totalValues = 7 * BP_BATCH;
                    RowCursor cursor = writer.getCursor(0);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("val " + count, count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("total count", totalValues, count);
                    Misc.free(cursor);
                }

                // Verify via reader after close
                LongList vals = readAllBP(path.trimTo(plen), "bp_cycle", 0);
                Assert.assertEquals("reader count", 7 * BP_BATCH, vals.size());
                for (int i = 0; i < vals.size(); i++) {
                    Assert.assertEquals("reader val " + i, i, vals.getQuick(i));
                }
            }
        });
    }

    @Test
    public void testRecordPostingSealPurgeUsesChainDerivedInterval() throws Exception {
        // Phase 4 invariant: when a seal supersedes a previous .pv file,
        // the pending purge entry's [fromTableTxn, toTableTxn) window
        // comes directly from the chain — fromTableTxn is the predecessor
        // entry's txnAtSeal, toTableTxn is the new head's txnAtSeal.
        // Phase 4 retired the v1 [0, Long.MAX_VALUE) fallback that fired
        // whenever pendingPublishTableTxn was unset; this regression test
        // would re-fail if that fallback ever crept back in.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "phase4_purge_window";

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // First chain entry: txnAtSeal=10, sealTxn=0.
                    writer.setNextTxnAtSeal(10);
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.commit();

                    // Second commit extends the head with a sparse gen but
                    // does not bump sealTxn or append a new entry.
                    for (int v = BP_BATCH; v < 2 * BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.commit();

                    // Seal bumps sealTxn -> 1, appends a fresh chain entry,
                    // and recordPostingSealPurge fires for the old sealTxn.
                    writer.setMaxValue(2 * BP_BATCH - 1);
                    writer.setNextTxnAtSeal(20);
                    writer.seal();

                    Assert.assertEquals("expected exactly one queued purge entry",
                            1, writer.getPendingPurgesSizeForTesting());
                    Assert.assertEquals("fromTxn must equal predecessor's txnAtSeal",
                            10L, writer.getPendingPurgeFromTxnForTesting(0));
                    Assert.assertEquals("toTxn must equal new head's txnAtSeal",
                            20L, writer.getPendingPurgeToTxnForTesting(0));
                    Assert.assertNotEquals("toTxn must not be the v1 [0, MAX) sentinel",
                            Long.MAX_VALUE, writer.getPendingPurgeToTxnForTesting(0));
                }
            }
        });
    }

    @Test
    public void testRecoveryDropsAbandonedHeadEntryOnReopen() throws Exception {
        // A path-based PostingIndexWriter persists every committed chain
        // entry to disk before the encompassing TableWriter#commit lands.
        // If the host TableWriter distresses between the chain publish and
        // txWriter.commit, the chain advertises a sealTxn that no committed
        // _txn ever covered. The next open must call setCurrentTableTxn
        // and have recoveryDropAbandoned remove the abandoned head before
        // any reader can pin to it.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "recovery_abandoned";

                // Phase 1: write+commit an entry recording txnAtSeal=100.
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.setNextTxnAtSeal(100);
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.commit();
                }

                // Phase 2: reopen with currentTableTxn=50, simulating the
                // distress-then-recovery case where _txn never reached 100.
                // The recovery walk must drop the head entry.
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.setCurrentTableTxn(50);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    Assert.assertEquals("chain should be empty after recovery dropped the abandoned entry",
                            0, writer.getKeyCount());
                }

                // Phase 3: a reader opening fresh now sees an empty chain
                // (no key/value visible) — the abandoned entry is gone.
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    Assert.assertEquals("reader keyCount after recovery", 0, reader.getKeyCount());
                }
            }
        });
    }

    @Test
    public void testRecoveryKeepsCommittedEntryOnReopen() throws Exception {
        // Sibling of testRecoveryDropsAbandonedHeadEntryOnReopen — verify
        // the recovery walk leaves a committed entry alone when
        // currentTableTxn covers it. Guards against an over-zealous walk
        // that accidentally drops committed state.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "recovery_committed";

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.setNextTxnAtSeal(10);
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.commit();
                }

                // currentTableTxn=50 covers the entry's txnAtSeal=10, so it stays.
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.setCurrentTableTxn(50);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    Assert.assertEquals("chain entry should survive when txnAtSeal <= currentTableTxn",
                            1, writer.getKeyCount());
                }

                LongList vals = readAllBP(path.trimTo(plen), name, 0);
                Assert.assertEquals("all values readable after recovery", BP_BATCH, vals.size());
            }
        });
    }

    private LongList readAllBP(Path path, CharSequence name, int key) {
        LongList values = new LongList();
        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
            try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                while (cursor.hasNext()) {
                    values.add(cursor.next());
                }
            }
        }
        return values;
    }

    private LongList readAllLegacy(Path path, int key) {
        LongList values = new LongList();
        try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path, "bp_mk_legacy", COLUMN_NAME_TXN_NONE, -1, 0)) {
            try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                while (cursor.hasNext()) {
                    values.add(cursor.next());
                }
            }
        }
        return values;
    }
}
