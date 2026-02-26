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

import io.questdb.cairo.idx.BPBitmapIndexFwdReader;
import io.questdb.cairo.idx.BPBitmapIndexUtils;
import io.questdb.cairo.idx.BPBitmapIndexWriter;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.FSSTBitmapIndexFwdReader;
import io.questdb.cairo.idx.FSSTBitmapIndexUtils;
import io.questdb.cairo.idx.FSSTBitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
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
public class BPFSSTBitmapIndexTest extends AbstractCairoTest {

    private static final int BP_BATCH = BPBitmapIndexUtils.BLOCK_CAPACITY;     // 64
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
            try (BPBitmapIndexWriter bpWriter = new BPBitmapIndexWriter(configuration, path.trimTo(plen), "bp_cmp", COLUMN_NAME_TXN_NONE)) {
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
            try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(configuration, path.trimTo(plen), "bp_cmp", COLUMN_NAME_TXN_NONE, -1, 0)) {
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
            try (BPBitmapIndexWriter bpWriter = new BPBitmapIndexWriter(configuration, path.trimTo(plen), "bp_mk", COLUMN_NAME_TXN_NONE)) {
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

            try (BPBitmapIndexWriter writer = new BPBitmapIndexWriter(configuration, path, "bp_large", COLUMN_NAME_TXN_NONE)) {
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

            try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(configuration, path, "bp_large", COLUMN_NAME_TXN_NONE, -1, 0)) {
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
            try (BPBitmapIndexWriter writer = new BPBitmapIndexWriter(configuration, path, "bp_seal", COLUMN_NAME_TXN_NONE)) {
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

            try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(configuration, path.trimTo(plen), "bp_seal", COLUMN_NAME_TXN_NONE, -1, 0)) {
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
            try (BPBitmapIndexWriter writer = new BPBitmapIndexWriter(configuration, path, "bp_empty", COLUMN_NAME_TXN_NONE)) {
                writer.add(5, 100);
                writer.add(5, 200);
                writer.commit();
            }

            try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(configuration, path, "bp_empty", COLUMN_NAME_TXN_NONE, -1, 0)) {
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
        try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
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
