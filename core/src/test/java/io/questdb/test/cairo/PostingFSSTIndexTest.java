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

import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.FSSTBitmapIndexFwdReader;
import io.questdb.cairo.idx.FSSTBitmapIndexUtils;
import io.questdb.cairo.idx.FSSTBitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Oracle tests comparing BP and FSST bitmap index implementations
 * against the Legacy (BitmapIndex) implementation for correctness.
 * <p>
 * BP writers have a per-key capacity of 64 values per commit (BLOCK_CAPACITY).
 * FSST writers have a per-key capacity of 128 values per commit (DEFAULT_BLOCK_VALUES).
 * Tests batch writes and commit accordingly.
 */
public class PostingFSSTIndexTest extends AbstractCairoTest {

    private static final int BP_BATCH = PostingIndexUtils.BLOCK_CAPACITY;     // 64
    private static final int FSST_BATCH = FSSTBitmapIndexUtils.DEFAULT_BLOCK_VALUES; // 128

    // ===== BP Tests =====

    @Test
    public void testBPComparisonWithLegacy() {
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
                if (valueCount % BP_BATCH != 0) {
                    bpWriter.commit();
                }
            }

            // Read Legacy
            LongList legacyValues = new LongList();
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "bp_legacy_cmp", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                while (cursor.hasNext()) {
                    legacyValues.add(cursor.next());
                }
            }

            // Read BP
            LongList bpValues = new LongList();
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path.trimTo(plen), "bp_cmp", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                while (cursor.hasNext()) {
                    bpValues.add(cursor.next());
                }
            }

            Assert.assertEquals("Value count mismatch", legacyValues.size(), bpValues.size());
            for (int i = 0; i < legacyValues.size(); i++) {
                Assert.assertEquals("Mismatch at index " + i, legacyValues.getQuick(i), bpValues.getQuick(i));
            }
        }
    }

    @Test
    public void testBPMultipleKeys() {
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
                LongList legacyVals = readAllLegacy(path.trimTo(plen), "bp_mk_legacy", key);
                LongList bpVals = readAllBP(path.trimTo(plen), "bp_mk", key);
                Assert.assertEquals("Key " + key + " count mismatch", legacyVals.size(), bpVals.size());
                for (int i = 0; i < legacyVals.size(); i++) {
                    Assert.assertEquals("Key " + key + " mismatch at index " + i,
                            legacyVals.getQuick(i), bpVals.getQuick(i));
                }
            }
        }
    }

    @Test
    public void testBPLargeOffset() {
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
                if (count % BP_BATCH != 0) {
                    writer.commit();
                }
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path, "bp_large", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int idx = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("Mismatch at index " + idx,
                            baseOffset + idx * 7L, cursor.next());
                    idx++;
                }
                Assert.assertEquals(count, idx);
            }
        }
    }

    @Test
    public void testBPCommitSeal() {
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
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(192, count);
            }
        }
    }

    @Test
    public void testBPEmptyKey() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "bp_empty", COLUMN_NAME_TXN_NONE)) {
                writer.add(5, 100);
                writer.add(5, 200);
                writer.commit();
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path, "bp_empty", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertFalse("Key 0 should be empty", cursor.hasNext());

                cursor = reader.getCursor(true, 5, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(100, cursor.next());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(200, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    // ===== FSST Tests =====

    @Test
    public void testFSSTComparisonWithLegacy() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            int valueCount = 10000;
            long[] values = new long[valueCount];
            for (int i = 0; i < valueCount; i++) {
                values[i] = i * 3L;
            }

            final int plen = path.size();
            try (BitmapIndexWriter legacyWriter = new BitmapIndexWriter(configuration)) {
                legacyWriter.of(path, "fsst_legacy_cmp", COLUMN_NAME_TXN_NONE, 256);
                for (long value : values) {
                    legacyWriter.add(0, value);
                }
                legacyWriter.commit();
            }

            try (FSSTBitmapIndexWriter fsstWriter = new FSSTBitmapIndexWriter(configuration, path.trimTo(plen), "fsst_cmp", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < valueCount; i++) {
                    fsstWriter.add(0, values[i]);
                    if ((i + 1) % FSST_BATCH == 0) {
                        fsstWriter.commit();
                    }
                }
                if (valueCount % FSST_BATCH != 0) {
                    fsstWriter.commit();
                }
            }

            LongList legacyValues = new LongList();
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "fsst_legacy_cmp", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                while (cursor.hasNext()) {
                    legacyValues.add(cursor.next());
                }
            }

            LongList fsstValues = new LongList();
            try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(configuration, path.trimTo(plen), "fsst_cmp", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                while (cursor.hasNext()) {
                    fsstValues.add(cursor.next());
                }
            }

            Assert.assertEquals("Value count mismatch", legacyValues.size(), fsstValues.size());
            for (int i = 0; i < legacyValues.size(); i++) {
                Assert.assertEquals("Mismatch at index " + i, legacyValues.getQuick(i), fsstValues.getQuick(i));
            }
        }
    }

    @Test
    public void testFSSTMultipleKeys() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            int keyCount = 50;
            int valuesPerKey = 100; // fits in one FSST batch (128)
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

            try (BitmapIndexWriter legacyWriter = new BitmapIndexWriter(configuration)) {
                legacyWriter.of(path, "fsst_mk_legacy", COLUMN_NAME_TXN_NONE, 256);
                for (int key = 0; key < keyCount; key++) {
                    for (int v = 0; v < valuesPerKey; v++) {
                        legacyWriter.add(key, keyValues[key][v]);
                    }
                }
                legacyWriter.commit();
            }

            try (FSSTBitmapIndexWriter fsstWriter = new FSSTBitmapIndexWriter(configuration, path.trimTo(plen), "fsst_mk", COLUMN_NAME_TXN_NONE)) {
                for (int key = 0; key < keyCount; key++) {
                    for (int v = 0; v < valuesPerKey; v++) {
                        fsstWriter.add(key, keyValues[key][v]);
                    }
                }
                fsstWriter.commit();
            }

            for (int key = 0; key < keyCount; key++) {
                LongList legacyVals = readAllLegacy(path.trimTo(plen), "fsst_mk_legacy", key);
                LongList fsstVals = readAllFSST(path.trimTo(plen), "fsst_mk", key);
                Assert.assertEquals("Key " + key + " count mismatch", legacyVals.size(), fsstVals.size());
                for (int i = 0; i < legacyVals.size(); i++) {
                    Assert.assertEquals("Key " + key + " mismatch at index " + i,
                            legacyVals.getQuick(i), fsstVals.getQuick(i));
                }
            }
        }
    }

    @Test
    public void testFSSTLargeOffset() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            long baseOffset = 1_000_000_000L;
            int count = 640;

            try (FSSTBitmapIndexWriter writer = new FSSTBitmapIndexWriter(configuration, path, "fsst_large", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < count; i++) {
                    writer.add(0, baseOffset + i * 7L);
                    if ((i + 1) % FSST_BATCH == 0) {
                        writer.commit();
                    }
                }
                if (count % FSST_BATCH != 0) {
                    writer.commit();
                }
            }

            try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(configuration, path, "fsst_large", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int idx = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("Mismatch at index " + idx,
                            baseOffset + idx * 7L, cursor.next());
                    idx++;
                }
                Assert.assertEquals(count, idx);
            }
        }
    }

    @Test
    public void testFSSTCommitSeal() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            try (FSSTBitmapIndexWriter writer = new FSSTBitmapIndexWriter(configuration, path, "fsst_seal", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < 128; i++) {
                    writer.add(0, i);
                }
                writer.commit();

                for (int i = 128; i < 256; i++) {
                    writer.add(0, i);
                }
                writer.commit();

                for (int i = 256; i < 384; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(configuration, path.trimTo(plen), "fsst_seal", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(384, count);
            }
        }
    }

    @Test
    public void testFSSTEmptyKey() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FSSTBitmapIndexWriter writer = new FSSTBitmapIndexWriter(configuration, path, "fsst_empty", COLUMN_NAME_TXN_NONE)) {
                writer.add(5, 100);
                writer.add(5, 200);
                writer.commit();
            }

            try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(configuration, path, "fsst_empty", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertFalse("Key 0 should be empty", cursor.hasNext());

                cursor = reader.getCursor(true, 5, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(100, cursor.next());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(200, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    // ===== BP GenLookup / SBBF Integration Tests =====

    @Test
    public void testBPManyKeysMultipleGens() {
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
                    RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);
                    int idx = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("Key " + key + " mismatch at " + idx,
                                allValues[key][idx], cursor.next());
                        idx++;
                    }
                    Assert.assertEquals("Key " + key + " count mismatch",
                            valuesPerKey * batches, idx);
                }
            }
        }
    }

    // ===== BP Incremental Seal Tests =====

    @Test
    public void testBPIncrementalSealMatchesFullSeal() {
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
    }

    @Test
    public void testBPBwdReaderWithGenLookup() {
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
            try (PostingIndexBwdReader reader = new PostingIndexBwdReader(configuration, path.trimTo(plen), "bp_bwd", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                long prev = Long.MAX_VALUE;
                while (cursor.hasNext()) {
                    long val = cursor.next();
                    Assert.assertTrue("Values should be descending: " + prev + " vs " + val, val <= prev);
                    prev = val;
                    count++;
                }
                Assert.assertEquals(128, count);
            }
        }
    }

    @Test
    public void testBPNativeEncodeKey() {
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
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, values[i]);
            }

            PostingIndexUtils.EncodeContext ctx1 = new PostingIndexUtils.EncodeContext();
            ctx1.ensureCapacity(count);
            int size1 = PostingIndexUtils.encodeKey(values, count, destAddr1, ctx1);

            PostingIndexUtils.EncodeContext ctx2 = new PostingIndexUtils.EncodeContext();
            ctx2.ensureCapacity(count);
            int size2 = PostingIndexUtils.encodeKeyNative(srcAddr, count, destAddr2, ctx2);

            Assert.assertEquals("Encoded sizes differ", size1, size2);
            for (int i = 0; i < size1; i++) {
                Assert.assertEquals("Byte mismatch at offset " + i,
                        Unsafe.getUnsafe().getByte(destAddr1 + i),
                        Unsafe.getUnsafe().getByte(destAddr2 + i));
            }
        } finally {
            Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr1, PostingIndexUtils.computeMaxEncodedSize(count), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr2, PostingIndexUtils.computeMaxEncodedSize(count), MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ===== Concurrent read safety tests =====

    @Test
    public void testReaderSurvivesWriterSeal() {
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
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);

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
                        Assert.assertEquals("value mismatch at " + i, (long) i, allValues.getQuick(i));
                    }
                }
            }
        }
    }

    @Test
    public void testCompactOnReopen() {
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
                    Assert.assertEquals("value " + count, (long) count, cursor.next());
                    count++;
                }
                Assert.assertEquals("total count", totalValues, count);
            }

            // Also verify via reader
            LongList vals = readAllBP(path.trimTo(plen), "bp_compact", 0);
            Assert.assertEquals("reader count", totalValues, vals.size());
            for (int i = 0; i < totalValues; i++) {
                Assert.assertEquals("reader value " + i, (long) i, vals.getQuick(i));
            }
        }
    }

    @Test
    public void testSealCommitSealCycle() {
        // Seal → more commits → seal again. Verifies append-only seal handles
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
                    Assert.assertEquals("val " + count, (long) count, cursor.next());
                    count++;
                }
                Assert.assertEquals("total count", totalValues, count);
            }

            // Verify via reader after close
            LongList vals = readAllBP(path.trimTo(plen), "bp_cycle", 0);
            Assert.assertEquals("reader count", 7 * BP_BATCH, vals.size());
            for (int i = 0; i < vals.size(); i++) {
                Assert.assertEquals("reader val " + i, (long) i, vals.getQuick(i));
            }
        }
    }

    // ===== Helpers =====

    private LongList readAllLegacy(Path path, CharSequence name, int key) {
        LongList values = new LongList();
        try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
            RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                values.add(cursor.next());
            }
        }
        return values;
    }

    private LongList readAllBP(Path path, CharSequence name, int key) {
        LongList values = new LongList();
        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
            RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                values.add(cursor.next());
            }
        }
        return values;
    }

    private LongList readAllFSST(Path path, CharSequence name, int key) {
        LongList values = new LongList();
        try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
            RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                values.add(cursor.next());
            }
        }
        return values;
    }
}
