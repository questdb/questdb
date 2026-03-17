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

import io.questdb.cairo.idx.PostingsIndexBwdReader;
import io.questdb.cairo.idx.PostingsIndexFwdReader;
import io.questdb.cairo.idx.PostingsIndexUtils;
import io.questdb.cairo.idx.PostingsIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Aggressive fuzz and stress tests for PostingsIndex concurrent read safety,
 * append-only seal, and compaction. Exercises edge cases, randomized workloads,
 * and multi-threaded reader/writer concurrency.
 */
public class PostingsIndexStressTest extends AbstractCairoTest {

    private static final int BP_BATCH = PostingsIndexUtils.BLOCK_CAPACITY; // 64

    // ===================================================================
    // Fuzz: randomized workloads verified against in-memory oracle
    // ===================================================================

    @Test
    public void testFuzzRandomKeysAndCommitPattern() {
        // Randomized key count, values per key, commit frequency, with oracle.
        for (long seed = 0; seed < 20; seed++) {
            Rnd rnd = new Rnd(seed, seed * 31 + 17);
            int keyCount = rnd.nextInt(200) + 1;
            int totalAdds = rnd.nextInt(5000) + 500;
            int commitEvery = rnd.nextInt(100) + 10;

            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "fuzz_rnd_" + seed;

                // Oracle: track expected values per key
                ObjList<LongList> oracle = new ObjList<>();
                for (int k = 0; k < keyCount; k++) {
                    oracle.add(new LongList());
                }

                // Monotonic per-key row IDs
                long[] nextRowId = new long[keyCount];
                for (int k = 0; k < keyCount; k++) {
                    nextRowId[k] = rnd.nextLong(1_000_000);
                }

                try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int sinceCommit = 0;
                    long maxVal = -1;
                    for (int i = 0; i < totalAdds; i++) {
                        int key = rnd.nextInt(keyCount);
                        long val = nextRowId[key];
                        nextRowId[key] += rnd.nextInt(100) + 1;
                        writer.add(key, val);
                        oracle.getQuick(key).add(val);
                        if (val > maxVal) {
                            maxVal = val;
                        }
                        sinceCommit++;
                        if (sinceCommit >= commitEvery) {
                            writer.setMaxValue(maxVal);
                            writer.commit();
                            sinceCommit = 0;
                        }
                    }
                    if (sinceCommit > 0) {
                        writer.setMaxValue(maxVal);
                        writer.commit();
                    }
                } // close triggers seal

                // Verify forward reader
                try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int k = 0; k < keyCount; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = 0;
                        while (cursor.hasNext()) {
                            Assert.assertTrue("seed=" + seed + " key=" + k + " extra values",
                                    idx < expected.size());
                            Assert.assertEquals("seed=" + seed + " key=" + k + " idx=" + idx,
                                    expected.getQuick(idx), cursor.next());
                            idx++;
                        }
                        Assert.assertEquals("seed=" + seed + " key=" + k + " count",
                                expected.size(), idx);
                    }
                }

                // Verify backward reader
                try (PostingsIndexBwdReader reader = new PostingsIndexBwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int k = 0; k < keyCount; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = expected.size() - 1;
                        while (cursor.hasNext()) {
                            Assert.assertTrue("seed=" + seed + " key=" + k + " bwd extra", idx >= 0);
                            Assert.assertEquals("seed=" + seed + " key=" + k + " bwd idx=" + idx,
                                    expected.getQuick(idx), cursor.next());
                            idx--;
                        }
                        Assert.assertEquals("seed=" + seed + " key=" + k + " bwd count",
                                -1, idx);
                    }
                }
            }
        }
    }

    @Test
    public void testFuzzRandomSealTiming() {
        // Random seal points during write, then verify correctness.
        for (long seed = 0; seed < 15; seed++) {
            Rnd rnd = new Rnd(seed * 7, seed * 13 + 3);
            int keyCount = rnd.nextInt(50) + 1;
            int totalAdds = rnd.nextInt(3000) + 500;
            int commitEvery = rnd.nextInt(60) + 5;
            int sealEvery = rnd.nextInt(500) + 100;

            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "fuzz_seal_" + seed;

                ObjList<LongList> oracle = new ObjList<>();
                for (int k = 0; k < keyCount; k++) {
                    oracle.add(new LongList());
                }
                long[] nextRowId = new long[keyCount];

                try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int sinceCommit = 0;
                    int sinceSeal = 0;
                    long maxVal = -1;
                    for (int i = 0; i < totalAdds; i++) {
                        int key = rnd.nextInt(keyCount);
                        long val = nextRowId[key];
                        nextRowId[key] += rnd.nextInt(50) + 1;
                        writer.add(key, val);
                        oracle.getQuick(key).add(val);
                        if (val > maxVal) {
                            maxVal = val;
                        }
                        sinceCommit++;
                        sinceSeal++;
                        if (sinceCommit >= commitEvery) {
                            writer.setMaxValue(maxVal);
                            writer.commit();
                            sinceCommit = 0;
                        }
                        if (sinceSeal >= sealEvery && sinceCommit == 0) {
                            writer.seal();
                            sinceSeal = 0;
                        }
                    }
                    if (sinceCommit > 0) {
                        writer.setMaxValue(maxVal);
                        writer.commit();
                    }
                }

                // Verify via reader
                try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int k = 0; k < keyCount; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals("seed=" + seed + " key=" + k + " idx=" + idx,
                                    expected.getQuick(idx), cursor.next());
                            idx++;
                        }
                        Assert.assertEquals("seed=" + seed + " key=" + k + " count",
                                expected.size(), idx);
                    }
                }
            }
        }
    }

    @Test
    public void testFuzzMinMaxRangeQuery() {
        // Write data, then query with random min/max ranges and verify bounds.
        Rnd rnd = new Rnd(42, 42);
        int keyCount = 20;
        int valuesPerKey = 300;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < keyCount; k++) {
                oracle.add(new LongList());
            }

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "fuzz_range", COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                for (int batch = 0; batch < valuesPerKey / BP_BATCH + 1; batch++) {
                    for (int key = 0; key < keyCount; key++) {
                        int count = Math.min(BP_BATCH, valuesPerKey - batch * BP_BATCH);
                        if (count <= 0) continue;
                        for (int v = 0; v < count; v++) {
                            long val = (long) key * 100_000 + batch * BP_BATCH + v;
                            writer.add(key, val);
                            oracle.getQuick(key).add(val);
                            if (val > maxVal) {
                                maxVal = val;
                            }
                        }
                    }
                    writer.setMaxValue(maxVal);
                    writer.commit();
                }
            }

            // Query with random ranges
            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "fuzz_range", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int trial = 0; trial < 200; trial++) {
                    int key = rnd.nextInt(keyCount);
                    LongList expected = oracle.getQuick(key);
                    if (expected.size() == 0) continue;

                    long lo = expected.getQuick(rnd.nextInt(expected.size()));
                    long hi = expected.getQuick(rnd.nextInt(expected.size()));
                    if (lo > hi) {
                        long tmp = lo;
                        lo = hi;
                        hi = tmp;
                    }

                    RowCursor cursor = reader.getCursor(false, key, lo, hi);
                    long prev = Long.MIN_VALUE;
                    int count = 0;
                    while (cursor.hasNext()) {
                        long val = cursor.next();
                        Assert.assertTrue("trial=" + trial + " val=" + val + " < lo=" + lo, val >= lo);
                        Assert.assertTrue("trial=" + trial + " val=" + val + " > hi=" + hi, val <= hi);
                        Assert.assertTrue("trial=" + trial + " not ascending: " + prev + " -> " + val, val >= prev);
                        prev = val;
                        count++;
                    }

                    // Cross-check count with oracle
                    int expectedCount = 0;
                    for (int i = 0; i < expected.size(); i++) {
                        long v = expected.getQuick(i);
                        if (v >= lo && v <= hi) expectedCount++;
                    }
                    Assert.assertEquals("trial=" + trial + " key=" + key + " range [" + lo + "," + hi + "]",
                            expectedCount, count);
                }
            }
        }
    }

    // ===================================================================
    // Stress: repeated seal/compact/reopen cycles
    // ===================================================================

    @Test
    public void testStressRepeatedSealCompactCycles() {
        // Many cycles of: write → seal → reopen (compact) → write → seal → ...
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int cycles = 10;
            long rowId = 0;

            for (int cycle = 0; cycle < cycles; cycle++) {
                try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration)) {
                    if (cycle == 0) {
                        writer.of(path.trimTo(plen), "stress_cycle", COLUMN_NAME_TXN_NONE, true);
                    } else {
                        writer.of(path.trimTo(plen), "stress_cycle", COLUMN_NAME_TXN_NONE, false);
                    }

                    // Write 3 batches per cycle
                    for (int batch = 0; batch < 3; batch++) {
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, rowId++);
                        }
                        writer.setMaxValue(rowId - 1);
                        writer.commit();
                    }
                    writer.seal();
                } // close: no-op seal (already sealed), truncates

                // Verify data after each cycle via reader
                try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                        configuration, path.trimTo(plen), "stress_cycle", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    long expectedTotal = (long) (cycle + 1) * 3 * BP_BATCH;
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("cycle=" + cycle + " val " + count,
                                (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("cycle=" + cycle + " count", expectedTotal, count);
                }
            }
        }
    }

    @Test
    public void testStressManyKeys() {
        // High cardinality: 1000 keys, each with a few values, triggers
        // gen lookup tier transitions (per-key → SBBF → binary search).
        Rnd rnd = new Rnd(99, 99);
        int keyCount = 1000;
        int valuesPerKey = 20;
        int batches = 5;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < keyCount; k++) {
                oracle.add(new LongList());
            }
            long[] nextVal = new long[keyCount];
            for (int k = 0; k < keyCount; k++) {
                nextVal[k] = rnd.nextLong(10_000_000);
            }

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "stress_many", COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                for (int b = 0; b < batches; b++) {
                    for (int k = 0; k < keyCount; k++) {
                        int count = valuesPerKey / batches;
                        for (int v = 0; v < count; v++) {
                            long val = nextVal[k];
                            nextVal[k] += rnd.nextInt(100) + 1;
                            writer.add(k, val);
                            oracle.getQuick(k).add(val);
                            if (val > maxVal) maxVal = val;
                        }
                    }
                    writer.setMaxValue(maxVal);
                    writer.commit();
                }
            }

            // Verify all keys (exercises PostingsGenLookup tier logic)
            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "stress_many", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int k = 0; k < keyCount; k++) {
                    LongList expected = oracle.getQuick(k);
                    RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                    int idx = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("key=" + k + " idx=" + idx,
                                expected.getQuick(idx), cursor.next());
                        idx++;
                    }
                    Assert.assertEquals("key=" + k + " count",
                            expected.size(), idx);
                }
            }
        }
    }

    @Test
    public void testStressHotKey() {
        // Single hot key receiving thousands of values, triggering many spills
        // and potential auto-seals (genCount > MAX_GEN_COUNT).
        int totalValues = 20_000;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "stress_hot", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < totalValues; i++) {
                    writer.add(0, i);
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }
                if (totalValues % BP_BATCH != 0) {
                    writer.setMaxValue(totalValues - 1);
                    writer.commit();
                }
            }

            // Verify forward
            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "stress_hot", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(totalValues, count);
            }

            // Verify backward
            try (PostingsIndexBwdReader reader = new PostingsIndexBwdReader(
                    configuration, path.trimTo(plen), "stress_hot", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(totalValues - 1 - count, cursor.next());
                    count++;
                }
                Assert.assertEquals(totalValues, count);
            }
        }
    }

    // ===================================================================
    // Edge cases
    // ===================================================================

    @Test
    public void testEdgeSingleValuePerKey() {
        int keyCount = 500;
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "edge_single", COLUMN_NAME_TXN_NONE)) {
                for (int k = 0; k < keyCount; k++) {
                    writer.add(k, k * 1000L);
                }
                writer.setMaxValue((keyCount - 1) * 1000L);
                writer.commit();
            }

            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_single", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int k = 0; k < keyCount; k++) {
                    RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                    Assert.assertTrue("key " + k + " should have value", cursor.hasNext());
                    Assert.assertEquals(k * 1000L, cursor.next());
                    Assert.assertFalse("key " + k + " should have only one value", cursor.hasNext());
                }
            }
        }
    }

    @Test
    public void testEdgeLargeGapsBetweenValues() {
        // Large gaps stress the bitpacking (wide deltas → many bits).
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int count = 200;

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "edge_gaps", COLUMN_NAME_TXN_NONE)) {
                long val = 0;
                for (int i = 0; i < count; i++) {
                    writer.add(0, val);
                    val += 1_000_000_000L; // 1 billion gap
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(val - 1_000_000_000L);
                        writer.commit();
                    }
                }
                if (count % BP_BATCH != 0) {
                    writer.setMaxValue(val - 1_000_000_000L);
                    writer.commit();
                }
            }

            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_gaps", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                long expected = 0;
                int idx = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("idx " + idx, expected, cursor.next());
                    expected += 1_000_000_000L;
                    idx++;
                }
                Assert.assertEquals(count, idx);
            }
        }
    }

    @Test
    public void testEdgeConstantDeltas() {
        // All deltas are identical → bitWidth should be 0 (constant FoR).
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int count = 320; // 5 blocks of 64

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "edge_const", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < count; i++) {
                    writer.add(0, i * 7L); // constant delta of 7
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i * 7L);
                        writer.commit();
                    }
                }
                if (count % BP_BATCH != 0) {
                    writer.setMaxValue((count - 1) * 7L);
                    writer.commit();
                }
            }

            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_const", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int idx = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(idx * 7L, cursor.next());
                    idx++;
                }
                Assert.assertEquals(count, idx);
            }
        }
    }

    @Test
    public void testEdgeEmptyIndex() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "edge_empty", COLUMN_NAME_TXN_NONE)) {
                writer.commit(); // commit with no data
            }

            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_empty", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testEdgeSealEmptyIndex() {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "edge_seal_empty", COLUMN_NAME_TXN_NONE)) {
                writer.seal(); // seal with no data — should be a no-op
                Assert.assertEquals(0, writer.getGenCount());
            }
        }
    }

    @Test
    public void testEdgeSealSingleGen() {
        // Seal with only one generation — should be a no-op.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "edge_seal_1gen", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < BP_BATCH; i++) {
                    writer.add(0, i);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();
                Assert.assertEquals(1, writer.getGenCount());
                writer.seal();
                Assert.assertEquals(1, writer.getGenCount()); // unchanged
            }
        }
    }

    @Test
    public void testEdgeExactBlockBoundary() {
        // Values count is exact multiple of BLOCK_CAPACITY across multiple gens.
        int blocks = 10;
        int totalValues = blocks * BP_BATCH; // 640

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "edge_boundary", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < totalValues; i++) {
                    writer.add(0, i);
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }
            }

            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_boundary", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(totalValues, count);
            }
        }
    }

    // ===================================================================
    // Concurrent reader/writer stress tests
    // ===================================================================

    @Test
    public void testConcurrentReadersWhileWriterCommits() throws Exception {
        // Writer commits in a loop while multiple reader threads continuously
        // read and verify data integrity.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_rw";
        final int numReaders = 4;
        final int writerCommits = 50;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicInteger committedBatches = new AtomicInteger(0);
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                // Seed initial data so readers have something to read
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();
                committedBatches.set(1);

                // Start reader threads — each creates its own Path (Path is not thread-safe)
                Thread[] readers = new Thread[numReaders];
                for (int r = 0; r < numReaders; r++) {
                    final int readerId = r;
                    readers[r] = new Thread(() -> {
                        try (Path rPath = new Path().of(dbRoot);
                             PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            while (writerDone.getCount() > 0 || committedBatches.get() <= writerCommits) {
                                reader.reloadConditionally();
                                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                long prev = -1;
                                int count = 0;
                                while (cursor.hasNext()) {
                                    long val = cursor.next();
                                    if (val <= prev) {
                                        throw new AssertionError(
                                                "reader " + readerId + ": non-ascending " + prev + " -> " + val);
                                    }
                                    prev = val;
                                    count++;
                                }
                                // Count must be a multiple of BP_BATCH (each commit adds exactly one batch)
                                if (count % BP_BATCH != 0) {
                                    throw new AssertionError(
                                            "reader " + readerId + ": partial batch visible, count=" + count);
                                }
                                if (count == 0) {
                                    throw new AssertionError(
                                            "reader " + readerId + ": zero values visible");
                                }
                                Thread.yield();
                            }
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    readers[r].setDaemon(true);
                    readers[r].start();
                }

                // Writer: commit more batches
                for (int batch = 1; batch < writerCommits; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                    committedBatches.incrementAndGet();
                }
                writerDone.countDown();

                for (Thread t : readers) {
                    t.join(10_000);
                    if (t.isAlive()) t.interrupt();
                }

                if (error.get() != null) {
                    throw new AssertionError("Concurrent reader failed", error.get());
                }
            }
        }
    }

    @Test
    public void testConcurrentReadersWhileWriterSeals() throws Exception {
        // Writer builds up gens then seals while readers are iterating.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_seal";
        final int numReaders = 4;
        final int totalBatches = 20;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CyclicBarrier barrier = new CyclicBarrier(numReaders + 1);
        final CountDownLatch done = new CountDownLatch(numReaders);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                // Write initial data: 10 batches (10 sparse gens)
                for (int batch = 0; batch < 10; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                }

                // Start reader threads
                Thread[] readers = new Thread[numReaders];
                for (int r = 0; r < numReaders; r++) {
                    final int readerId = r;
                    readers[r] = new Thread(() -> {
                        try (Path rPath = new Path().of(dbRoot);
                             PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            reader.reloadConditionally();
                            barrier.await();

                            RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                            long prev = -1;
                            int count = 0;
                            while (cursor.hasNext()) {
                                long val = cursor.next();
                                if (val <= prev) {
                                    throw new AssertionError(
                                            "reader " + readerId + ": non-ascending " + prev + " -> " + val
                                                    + " at position " + count);
                                }
                                if (val != count) {
                                    throw new AssertionError(
                                            "reader " + readerId + ": expected " + count + " got " + val);
                                }
                                prev = val;
                                count++;
                                if (count % 10 == 0) Thread.yield();
                            }
                            if (count != 10 * BP_BATCH) {
                                throw new AssertionError(
                                        "reader " + readerId + ": expected " + (10 * BP_BATCH) + " got " + count);
                            }
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        } finally {
                            done.countDown();
                        }
                    });
                    readers[r].setDaemon(true);
                    readers[r].start();
                }

                barrier.await();
                writer.seal();

                for (int batch = 10; batch < totalBatches; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                }

                done.await();

                if (error.get() != null) {
                    throw new AssertionError("Concurrent reader failed during seal", error.get());
                }

                writer.seal();
            }

            try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                    configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(totalBatches * BP_BATCH, count);
            }
        }
    }

    @Test
    public void testConcurrentReaderReloadWhileWriterCommits() throws Exception {
        // Reader repeatedly reloads and reads while writer commits — exercises
        // the seq/seqCheck atomicity handshake under contention.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_reload";
        final int writerBatches = 100;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();

                Thread readerThread = new Thread(() -> {
                    try (Path rPath = new Path().of(dbRoot);
                         PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                                 configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        int iterations = 0;
                        while (writerDone.getCount() > 0 || iterations < writerBatches) {
                            reader.reloadConditionally();
                            RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                            long prev = -1;
                            int count = 0;
                            while (cursor.hasNext()) {
                                long val = cursor.next();
                                if (val <= prev) {
                                    throw new AssertionError("non-ascending: " + prev + " -> " + val);
                                }
                                if (val != count) {
                                    throw new AssertionError("expected " + count + " got " + val);
                                }
                                prev = val;
                                count++;
                            }
                            if (count % BP_BATCH != 0) {
                                throw new AssertionError("partial batch: count=" + count);
                            }
                            iterations++;
                        }
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                });
                readerThread.setDaemon(true);
                readerThread.start();

                for (int batch = 1; batch < writerBatches; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                }
                writerDone.countDown();

                readerThread.join(30_000);
                if (readerThread.isAlive()) readerThread.interrupt();

                if (error.get() != null) {
                    throw new AssertionError("Reader reload stress failed", error.get());
                }
            }
        }
    }

    // ===================================================================
    // Multi-key concurrent stress
    // ===================================================================

    @Test
    public void testConcurrentMultiKeyReadersWhileWriting() throws Exception {
        // Multiple keys, each reader thread reads a different key while writer
        // commits across all keys.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_mk";
        final int keyCount = 8;
        final int writerBatches = 30;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                for (int k = 0; k < keyCount; k++) {
                    for (int v = 0; v < BP_BATCH; v++) {
                        long val = (long) k * 1_000_000 + v;
                        writer.add(k, val);
                        if (val > maxVal) maxVal = val;
                    }
                }
                writer.setMaxValue(maxVal);
                writer.commit();

                Thread[] readers = new Thread[keyCount];
                for (int k = 0; k < keyCount; k++) {
                    final int key = k;
                    readers[k] = new Thread(() -> {
                        try (Path rPath = new Path().of(dbRoot);
                             PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            while (writerDone.getCount() > 0) {
                                reader.reloadConditionally();
                                RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);
                                long prev = -1;
                                int count = 0;
                                while (cursor.hasNext()) {
                                    long val = cursor.next();
                                    long expected = (long) key * 1_000_000 + count;
                                    if (val != expected) {
                                        throw new AssertionError(
                                                "key=" + key + " pos=" + count + " expected=" + expected + " got=" + val);
                                    }
                                    if (val <= prev) {
                                        throw new AssertionError(
                                                "key=" + key + " non-ascending: " + prev + " -> " + val);
                                    }
                                    prev = val;
                                    count++;
                                }
                                if (count % BP_BATCH != 0) {
                                    throw new AssertionError(
                                            "key=" + key + " partial batch: count=" + count);
                                }
                                Thread.yield();
                            }
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    readers[k].setDaemon(true);
                    readers[k].start();
                }

                for (int batch = 1; batch < writerBatches; batch++) {
                    long batchMax = -1;
                    for (int k = 0; k < keyCount; k++) {
                        for (int v = 0; v < BP_BATCH; v++) {
                            long val = (long) k * 1_000_000 + batch * BP_BATCH + v;
                            writer.add(k, val);
                            if (val > batchMax) batchMax = val;
                        }
                    }
                    writer.setMaxValue(batchMax);
                    writer.commit();
                }
                writerDone.countDown();

                for (Thread t : readers) {
                    t.join(10_000);
                    if (t.isAlive()) t.interrupt();
                }

                if (error.get() != null) {
                    throw new AssertionError("Multi-key concurrent reader failed", error.get());
                }
            }
        }
    }

    // ===================================================================
    // Compact safety under concurrent readers
    // ===================================================================

    @Test
    public void testCompactWhileReaderHoldsOldSnapshot() {
        // Reader opens before seal, holds cursor. Writer seals (append-only),
        // closes (triggers compaction on reopen). Old reader must still
        // see consistent data.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int totalValues = 4 * BP_BATCH; // 256

            // Phase 1: write, commit
            try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration, path, "compact_snap", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < totalValues; i++) {
                    writer.add(0, i);
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }

                // Phase 2: open reader, start cursor
                try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                        configuration, path.trimTo(plen), "compact_snap", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    reader.reloadConditionally();
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);

                    // Read a few values
                    LongList partial = new LongList();
                    for (int i = 0; i < BP_BATCH && cursor.hasNext(); i++) {
                        partial.add(cursor.next());
                    }

                    // Phase 3: seal (append-only)
                    writer.seal();

                    // Phase 4: continue cursor — old data still valid
                    LongList rest = new LongList();
                    while (cursor.hasNext()) {
                        rest.add(cursor.next());
                    }

                    int totalRead = partial.size() + rest.size();
                    Assert.assertEquals("total count", totalValues, totalRead);
                    for (int i = 0; i < totalRead; i++) {
                        long val = i < partial.size() ? partial.getQuick(i) : rest.getQuick(i - partial.size());
                        Assert.assertEquals("value " + i, (long) i, val);
                    }
                }
            }

            // Phase 5: reopen (triggers compact), verify
            try (PostingsIndexWriter writer2 = new PostingsIndexWriter(configuration)) {
                writer2.of(path.trimTo(plen), "compact_snap", COLUMN_NAME_TXN_NONE, false);

                RowCursor cursor = writer2.getCursor(0);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(totalValues, count);
            }
        }
    }

    @Test
    public void testMultipleSealCompactCyclesWithReaderVerification() {
        // Repeated: write → seal → close → reopen (compact) → read → verify
        // Each cycle adds more data on top of compacted base.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int cycles = 8;
            long rowId = 0;

            for (int cycle = 0; cycle < cycles; cycle++) {
                // Write phase
                try (PostingsIndexWriter writer = new PostingsIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), "multi_compact", COLUMN_NAME_TXN_NONE, cycle == 0);
                    for (int batch = 0; batch < 2; batch++) {
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, rowId++);
                        }
                        writer.setMaxValue(rowId - 1);
                        writer.commit();
                    }
                } // close triggers seal + truncate

                // Read phase: verify everything from value 0 to rowId-1
                try (PostingsIndexFwdReader reader = new PostingsIndexFwdReader(
                        configuration, path.trimTo(plen), "multi_compact", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("cycle=" + cycle + " val " + count,
                                (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("cycle=" + cycle + " count",
                            rowId, count);
                }

                // Backward read verification
                try (PostingsIndexBwdReader reader = new PostingsIndexBwdReader(
                        configuration, path.trimTo(plen), "multi_compact", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    long expected = rowId - 1;
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("cycle=" + cycle + " bwd val " + count,
                                expected, cursor.next());
                        expected--;
                        count++;
                    }
                    Assert.assertEquals("cycle=" + cycle + " bwd count",
                            rowId, count);
                }
            }
        }
    }
}
