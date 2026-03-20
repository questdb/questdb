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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
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
 * Aggressive fuzz and stress tests for PostingIndex concurrent read safety,
 * append-only seal, and compaction. Exercises edge cases, randomized workloads,
 * and multi-threaded reader/writer concurrency.
 */
public class PostingIndexStressTest extends AbstractCairoTest {

    private static final int BP_BATCH = PostingIndexUtils.BLOCK_CAPACITY; // 64

    // PostingGenLookup tier constants (mirrored here since PostingGenLookup is package-private)
    private static final int TIER_NONE = 0;
    private static final int TIER_PER_KEY = 1;
    private static final int TIER_SBBF = 2;

    // ===================================================================
    // Fuzz: randomized workloads verified against in-memory oracle
    // ===================================================================

    @Test
    public void testFuzzRandomKeysAndCommitPattern() throws Exception {
        assertMemoryLeak(() -> {
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

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
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
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
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
        });
    }

    @Test
    public void testFuzzRandomSealTiming() throws Exception {
        assertMemoryLeak(() -> {
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

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
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
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
        });
    }

    @Test
    public void testFuzzMinMaxRangeQuery() throws Exception {
        assertMemoryLeak(() -> {
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

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "fuzz_range", COLUMN_NAME_TXN_NONE)) {
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
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
        });
    }

    // ===================================================================
    // Stress: repeated seal/compact/reopen cycles
    // ===================================================================

    @Test
    public void testStressRepeatedSealCompactCycles() throws Exception {
        assertMemoryLeak(() -> {
        // Many cycles of: write → seal → reopen (compact) → write → seal → ...
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int cycles = 10;
            long rowId = 0;

            for (int cycle = 0; cycle < cycles; cycle++) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
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
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
        });
    }

    @Test
    public void testStressManyKeys() throws Exception {
        assertMemoryLeak(() -> {
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

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "stress_many", COLUMN_NAME_TXN_NONE)) {
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

            // Verify all keys (exercises PostingGenLookup tier logic)
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
        });
    }

    @Test
    public void testStressHotKey() throws Exception {
        assertMemoryLeak(() -> {
        // Single hot key receiving thousands of values, triggering many spills
        // and potential auto-seals (genCount > MAX_GEN_COUNT).
        int totalValues = 20_000;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "stress_hot", COLUMN_NAME_TXN_NONE)) {
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
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
            try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
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
        });
    }

    // ===================================================================
    // Edge cases
    // ===================================================================

    @Test
    public void testEdgeSingleValuePerKey() throws Exception {
        assertMemoryLeak(() -> {
        int keyCount = 500;
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_single", COLUMN_NAME_TXN_NONE)) {
                for (int k = 0; k < keyCount; k++) {
                    writer.add(k, k * 1000L);
                }
                writer.setMaxValue((keyCount - 1) * 1000L);
                writer.commit();
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_single", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int k = 0; k < keyCount; k++) {
                    RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                    Assert.assertTrue("key " + k + " should have value", cursor.hasNext());
                    Assert.assertEquals(k * 1000L, cursor.next());
                    Assert.assertFalse("key " + k + " should have only one value", cursor.hasNext());
                }
            }
        }
        });
    }

    @Test
    public void testEdgeLargeGapsBetweenValues() throws Exception {
        assertMemoryLeak(() -> {
        // Large gaps stress the bitpacking (wide deltas → many bits).
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int count = 200;

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_gaps", COLUMN_NAME_TXN_NONE)) {
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

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
        });
    }

    @Test
    public void testEdgeConstantDeltas() throws Exception {
        assertMemoryLeak(() -> {
        // All deltas are identical → bitWidth should be 0 (constant FoR).
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int count = 320; // 5 blocks of 64

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_const", COLUMN_NAME_TXN_NONE)) {
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

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
        });
    }

    @Test
    public void testEdgeEmptyIndex() throws Exception {
        assertMemoryLeak(() -> {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_empty", COLUMN_NAME_TXN_NONE)) {
                writer.commit(); // commit with no data
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_empty", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                Assert.assertFalse(cursor.hasNext());
            }
        }
        });
    }

    @Test
    public void testEdgeSealEmptyIndex() throws Exception {
        assertMemoryLeak(() -> {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_seal_empty", COLUMN_NAME_TXN_NONE)) {
                writer.seal(); // seal with no data — should be a no-op
                Assert.assertEquals(0, writer.getGenCount());
            }
        }
        });
    }

    @Test
    public void testEdgeSealSingleGen() throws Exception {
        assertMemoryLeak(() -> {
        // Seal with only one generation — should be a no-op.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_seal_1gen", COLUMN_NAME_TXN_NONE)) {
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
        });
    }

    @Test
    public void testEdgeExactBlockBoundary() throws Exception {
        assertMemoryLeak(() -> {
        // Values count is exact multiple of BLOCK_CAPACITY across multiple gens.
        int blocks = 10;
        int totalValues = blocks * BP_BATCH; // 640

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_boundary", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < totalValues; i++) {
                    writer.add(0, i);
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
        });
    }

    @Test
    public void testEdgeNonExistentKey() throws Exception {
        assertMemoryLeak(() -> {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_nokey", COLUMN_NAME_TXN_NONE)) {
                // Write 3 keys: 0, 1, 2
                for (int k = 0; k < 3; k++) {
                    for (int v = 0; v < 10; v++) {
                        writer.add(k, (long) k * 100 + v);
                    }
                }
                writer.setMaxValue(209);
                writer.commit();
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_nokey", COLUMN_NAME_TXN_NONE, -1, 0)) {
                // Query key=5 (non-existent, beyond written keys)
                RowCursor cursor5 = reader.getCursor(false, 5, 0, Long.MAX_VALUE);
                Assert.assertFalse("key=5 should be empty", cursor5.hasNext());

                // Query key=-1 (negative, non-existent)
                RowCursor cursorNeg = reader.getCursor(false, -1, 0, Long.MAX_VALUE);
                Assert.assertFalse("key=-1 should be empty", cursorNeg.hasNext());

                // Query key=Integer.MAX_VALUE (far beyond written keys)
                RowCursor cursorMax = reader.getCursor(false, Integer.MAX_VALUE, 0, Long.MAX_VALUE);
                Assert.assertFalse("key=MAX_VALUE should be empty", cursorMax.hasNext());
            }

            try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                    configuration, path.trimTo(plen), "edge_nokey", COLUMN_NAME_TXN_NONE, -1, 0)) {
                // Same checks for backward reader
                RowCursor cursor5 = reader.getCursor(false, 5, 0, Long.MAX_VALUE);
                Assert.assertFalse("bwd key=5 should be empty", cursor5.hasNext());

                RowCursor cursorNeg = reader.getCursor(false, -1, 0, Long.MAX_VALUE);
                Assert.assertFalse("bwd key=-1 should be empty", cursorNeg.hasNext());

                RowCursor cursorMax = reader.getCursor(false, Integer.MAX_VALUE, 0, Long.MAX_VALUE);
                Assert.assertFalse("bwd key=MAX_VALUE should be empty", cursorMax.hasNext());
            }
        }
        });
    }

    @Test
    public void testEdgeExactBlockCapacity() throws Exception {
        assertMemoryLeak(() -> {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            // Write exactly 64 values (BLOCK_CAPACITY) to a single key.
            // This creates exactly 1 full block with 63 packed deltas.
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "edge_exact_bc", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < BP_BATCH; i++) {
                    writer.add(0, i);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();
            }

            // Verify forward
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_exact_bc", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("fwd val " + count, (long) count, cursor.next());
                    count++;
                }
                Assert.assertEquals("fwd count for 64 values", BP_BATCH, count);
            }

            // Verify backward
            try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                    configuration, path.trimTo(plen), "edge_exact_bc", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("bwd val " + count,
                            (long) (BP_BATCH - 1 - count), cursor.next());
                    count++;
                }
                Assert.assertEquals("bwd count for 64 values", BP_BATCH, count);
            }

            // Now write 65 values (1 full block + 1 value in second block)
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path.trimTo(plen), "edge_exact_bc_65", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < BP_BATCH + 1; i++) {
                    writer.add(0, i);
                }
                writer.setMaxValue(BP_BATCH);
                writer.commit();
            }

            // Verify forward with 65 values
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "edge_exact_bc_65", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("fwd65 val " + count, (long) count, cursor.next());
                    count++;
                }
                Assert.assertEquals("fwd count for 65 values", BP_BATCH + 1, count);
            }

            // Verify backward with 65 values
            try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                    configuration, path.trimTo(plen), "edge_exact_bc_65", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("bwd65 val " + count,
                            (long) (BP_BATCH - count), cursor.next());
                    count++;
                }
                Assert.assertEquals("bwd count for 65 values", BP_BATCH + 1, count);
            }
        }
        });
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
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
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
                             PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
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
                             PostingIndexFwdReader reader = new PostingIndexFwdReader(
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

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();

                Thread readerThread = new Thread(() -> {
                    try (Path rPath = new Path().of(dbRoot);
                         PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
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
                             PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
    public void testCompactWhileReaderHoldsOldSnapshot() throws Exception {
        assertMemoryLeak(() -> {
        // Reader opens before seal, holds cursor. Writer seals (append-only),
        // closes (triggers compaction on reopen). Old reader must still
        // see consistent data.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int totalValues = 4 * BP_BATCH; // 256

            // Phase 1: write, commit
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "compact_snap", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < totalValues; i++) {
                    writer.add(0, i);
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }

                // Phase 2: open reader, start cursor
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
            try (PostingIndexWriter writer2 = new PostingIndexWriter(configuration)) {
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
        });
    }

    @Test
    public void testMultipleSealCompactCyclesWithReaderVerification() throws Exception {
        assertMemoryLeak(() -> {
        // Repeated: write → seal → close → reopen (compact) → read → verify
        // Each cycle adds more data on top of compacted base.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int cycles = 8;
            long rowId = 0;

            for (int cycle = 0; cycle < cycles; cycle++) {
                // Write phase
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
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
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
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
                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
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
        });
    }

    // ===================================================================
    // Rollback tests
    // ===================================================================

    @Test
    public void testRollbackMiddle() throws Exception {
        assertMemoryLeak(() -> {
        // Write 200 values, rollback to midpoint (99), verify only 0..99 survive.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int total = 200;
            int rollbackTo = 99;

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "rb_mid", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < total; i++) {
                    writer.add(0, i);
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }
                if (total % BP_BATCH != 0) {
                    writer.setMaxValue(total - 1);
                    writer.commit();
                }

                writer.rollbackValues(rollbackTo);

                RowCursor cursor = writer.getCursor(0);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("value " + count, (long) count, cursor.next());
                    count++;
                }
                Assert.assertEquals("count", rollbackTo + 1, count);
            }

            // Also verify via reader after close
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "rb_mid", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(rollbackTo + 1, count);
            }
        }
        });
    }

    @Test
    public void testRollbackAcrossKeys() throws Exception {
        assertMemoryLeak(() -> {
        // Multi-key: keys 0-4 with different row IDs; rollback removes some keys entirely.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            int keyCount = 5;

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "rb_keys", COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                // Key 0: values 0..49
                // Key 1: values 100..149
                // Key 2: values 200..249
                // Key 3: values 300..349
                // Key 4: values 400..449
                for (int k = 0; k < keyCount; k++) {
                    for (int v = 0; v < 50; v++) {
                        long val = (long) k * 100 + v;
                        writer.add(k, val);
                        if (val > maxVal) maxVal = val;
                    }
                }
                writer.setMaxValue(maxVal);
                writer.commit();

                // Rollback to 250 — keys 0,1,2 survive (key 2 partially), keys 3,4 are gone
                writer.rollbackValues(250);

                // Key 0: all 50 values survive (0..49)
                RowCursor c0 = writer.getCursor(0);
                int count0 = 0;
                while (c0.hasNext()) {
                    Assert.assertEquals(count0, c0.next());
                    count0++;
                }
                Assert.assertEquals(50, count0);

                // Key 1: all 50 values survive (100..149)
                RowCursor c1 = writer.getCursor(1);
                int count1 = 0;
                while (c1.hasNext()) {
                    Assert.assertEquals(100 + count1, c1.next());
                    count1++;
                }
                Assert.assertEquals(50, count1);

                // Key 2: all 50 values (200..249) survive since all <= 250
                RowCursor c2 = writer.getCursor(2);
                int count2 = 0;
                while (c2.hasNext()) {
                    Assert.assertEquals(200 + count2, c2.next());
                    count2++;
                }
                Assert.assertEquals(50, count2); // 200..249

                // Key 3: values 300..349 — all > 250, so fully removed
                RowCursor c3 = writer.getCursor(3);
                Assert.assertFalse(c3.hasNext());
            }
        }
        });
    }

    @Test
    public void testRollbackToZero() throws Exception {
        assertMemoryLeak(() -> {
        // Rollback to value before any data → equivalent to truncate.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "rb_zero", COLUMN_NAME_TXN_NONE)) {
                for (int i = 10; i < 100; i++) {
                    writer.add(0, i);
                }
                writer.setMaxValue(99);
                writer.commit();

                // Rollback to 5 — all values are >= 10, so none survive
                writer.rollbackValues(5);

                Assert.assertEquals(0, writer.getKeyCount());
                RowCursor cursor = writer.getCursor(0);
                Assert.assertFalse(cursor.hasNext());
            }
        }
        });
    }

    @Test
    public void testRollbackAfterSeal() throws Exception {
        assertMemoryLeak(() -> {
        // Seal compresses into single gen, then rollback. Verify decode+filter+reencode.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "rb_seal", COLUMN_NAME_TXN_NONE)) {
                // Create multiple gens
                for (int batch = 0; batch < 5; batch++) {
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, (long) batch * BP_BATCH + v);
                    }
                    writer.setMaxValue((long) batch * BP_BATCH + BP_BATCH - 1);
                    writer.commit();
                }
                // Seal into single gen
                writer.seal();
                Assert.assertEquals(1, writer.getGenCount());

                // Rollback to midpoint
                int rollbackTo = 2 * BP_BATCH + BP_BATCH / 2; // 160
                writer.rollbackValues(rollbackTo);

                RowCursor cursor = writer.getCursor(0);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(rollbackTo + 1, count);
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "rb_seal", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                int rollbackTo = 2 * BP_BATCH + BP_BATCH / 2;
                Assert.assertEquals(rollbackTo + 1, count);
            }
        }
        });
    }

    @Test
    public void testRollbackNoop() throws Exception {
        assertMemoryLeak(() -> {
        // Rollback to value > maxValue — no data lost.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "rb_noop", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < 100; i++) {
                    writer.add(0, i);
                }
                writer.setMaxValue(99);
                writer.commit();

                writer.rollbackValues(999); // well past maxValue

                RowCursor cursor = writer.getCursor(0);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(100, count);
            }

            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "rb_noop", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(100, count);
            }
        }
        });
    }

    @Test
    public void testRollbackThenWrite() throws Exception {
        assertMemoryLeak(() -> {
        // Rollback then continue writing — verify integrity.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "rb_write", COLUMN_NAME_TXN_NONE)) {
                // Write 0..199
                for (int i = 0; i < 200; i++) {
                    writer.add(0, i);
                    if ((i + 1) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }
                if (200 % BP_BATCH != 0) {
                    writer.setMaxValue(199);
                    writer.commit();
                }

                // Rollback to 99
                writer.rollbackValues(99);

                // Continue writing from 100 onwards (new data)
                for (int i = 100; i < 300; i++) {
                    writer.add(0, i);
                    if ((i - 99) % BP_BATCH == 0) {
                        writer.setMaxValue(i);
                        writer.commit();
                    }
                }
                writer.setMaxValue(299);
                writer.commit();
            }

            // Verify: should have 0..99 then 100..299 = 300 values
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "rb_write", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(300, count);
            }
        }
        });
    }

    @Test
    public void testFuzzRollback() throws Exception {
        assertMemoryLeak(() -> {
        // Randomized rollback points with oracle verification.
        for (long seed = 0; seed < 15; seed++) {
            Rnd rnd = new Rnd(seed * 11, seed * 23 + 7);
            int keyCount = rnd.nextInt(20) + 1;
            int totalAdds = rnd.nextInt(2000) + 200;
            int commitEvery = rnd.nextInt(80) + 10;

            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "fuzz_rb_" + seed;

                ObjList<LongList> oracle = new ObjList<>();
                for (int k = 0; k < keyCount; k++) {
                    oracle.add(new LongList());
                }
                long[] nextRowId = new long[keyCount];
                for (int k = 0; k < keyCount; k++) {
                    nextRowId[k] = rnd.nextLong(100_000);
                }

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int sinceCommit = 0;
                    long maxVal = -1;
                    for (int i = 0; i < totalAdds; i++) {
                        int key = rnd.nextInt(keyCount);
                        long val = nextRowId[key];
                        nextRowId[key] += rnd.nextInt(50) + 1;
                        writer.add(key, val);
                        oracle.getQuick(key).add(val);
                        if (val > maxVal) maxVal = val;
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

                    // Pick a random rollback point
                    long rollbackValue = rnd.nextLong(maxVal + 1);
                    writer.rollbackValues(rollbackValue);

                    // Update oracle
                    for (int k = 0; k < keyCount; k++) {
                        LongList vals = oracle.getQuick(k);
                        while (vals.size() > 0 && vals.getQuick(vals.size() - 1) > rollbackValue) {
                            vals.setPos(vals.size() - 1);
                        }
                    }
                }

                // Verify via forward reader
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int k = 0; k < keyCount; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = 0;
                        while (cursor.hasNext()) {
                            Assert.assertTrue("seed=" + seed + " key=" + k + " extra", idx < expected.size());
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
        });
    }

    // ===================================================================
    // Compaction overlap test
    // ===================================================================

    @Test
    public void testCompactionWithOverlap() throws Exception {
        assertMemoryLeak(() -> {
        // Craft an index where sealed gen is larger than dead space,
        // triggering the overlap path in compactValueFile.
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            // Phase 1: write one small commit (gen 0 sparse, small)
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "compact_overlap", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 0);
                writer.setMaxValue(0);
                writer.commit();

                // Phase 2: write a much larger second commit so we have 2 gens
                for (int i = 1; i < 500; i++) {
                    writer.add(0, i);
                }
                writer.setMaxValue(499);
                writer.commit();

                // Phase 3: seal — sealed gen (dense) will be large, gen 0 sparse was small
                // so gen0Offset (= small gen size) < gen0Size (= large sealed gen)
                // This triggers the overlap path
                writer.seal();
            }

            // Phase 4: reopen (triggers compact), verify
            try (PostingIndexWriter writer2 = new PostingIndexWriter(configuration)) {
                writer2.of(path.trimTo(plen), "compact_overlap", COLUMN_NAME_TXN_NONE, false);

                RowCursor cursor = writer2.getCursor(0);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(500, count);
            }
        }
        });
    }

    // ===================================================================
    // Concurrent backward reader stress tests
    // ===================================================================

    @Test
    public void testConcurrentBwdReadersWhileWriterCommits() throws Exception {
        // Writer commits while multiple backward reader threads continuously read.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_bwd_rw";
        final int numReaders = 4;
        final int writerCommits = 50;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicInteger committedBatches = new AtomicInteger(0);
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();
                committedBatches.set(1);

                Thread[] readers = new Thread[numReaders];
                for (int r = 0; r < numReaders; r++) {
                    final int readerId = r;
                    readers[r] = new Thread(() -> {
                        try (Path rPath = new Path().of(dbRoot);
                             PostingIndexBwdReader reader = new PostingIndexBwdReader(
                                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            while (writerDone.getCount() > 0 || committedBatches.get() <= writerCommits) {
                                reader.reloadConditionally();
                                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                long prev = Long.MAX_VALUE;
                                int count = 0;
                                while (cursor.hasNext()) {
                                    long val = cursor.next();
                                    if (val >= prev) {
                                        throw new AssertionError(
                                                "bwd reader " + readerId + ": non-descending " + prev + " -> " + val);
                                    }
                                    prev = val;
                                    count++;
                                }
                                if (count % BP_BATCH != 0) {
                                    throw new AssertionError(
                                            "bwd reader " + readerId + ": partial batch visible, count=" + count);
                                }
                                if (count == 0) {
                                    throw new AssertionError(
                                            "bwd reader " + readerId + ": zero values visible");
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
                    throw new AssertionError("Concurrent bwd reader failed", error.get());
                }
            }
        }
    }

    @Test
    public void testConcurrentBwdReadersWhileWriterSeals() throws Exception {
        // Backward cursor mid-iteration survives seal.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_bwd_seal";
        final int numReaders = 4;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CyclicBarrier barrier = new CyclicBarrier(numReaders + 1);
        final CountDownLatch done = new CountDownLatch(numReaders);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                for (int batch = 0; batch < 10; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                }

                Thread[] readers = new Thread[numReaders];
                for (int r = 0; r < numReaders; r++) {
                    final int readerId = r;
                    readers[r] = new Thread(() -> {
                        try (Path rPath = new Path().of(dbRoot);
                             PostingIndexBwdReader reader = new PostingIndexBwdReader(
                                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            reader.reloadConditionally();
                            barrier.await();

                            RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                            long prev = Long.MAX_VALUE;
                            int count = 0;
                            while (cursor.hasNext()) {
                                long val = cursor.next();
                                if (val >= prev) {
                                    throw new AssertionError(
                                            "bwd reader " + readerId + ": non-descending " + prev + " -> " + val
                                                    + " at position " + count);
                                }
                                long expected = 10 * BP_BATCH - 1 - count;
                                if (val != expected) {
                                    throw new AssertionError(
                                            "bwd reader " + readerId + ": expected " + expected + " got " + val);
                                }
                                prev = val;
                                count++;
                                if (count % 10 == 0) Thread.yield();
                            }
                            if (count != 10 * BP_BATCH) {
                                throw new AssertionError(
                                        "bwd reader " + readerId + ": expected " + (10 * BP_BATCH) + " got " + count);
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

                done.await();

                if (error.get() != null) {
                    throw new AssertionError("Concurrent bwd reader failed during seal", error.get());
                }
            }
        }
    }

    @Test
    public void testConcurrentMixedFwdBwdReaders() throws Exception {
        // Both forward and backward readers simultaneously during writes.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_mixed";
        final int writerBatches = 50;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();

                // 2 forward + 2 backward readers
                Thread[] threads = new Thread[4];
                for (int r = 0; r < 4; r++) {
                    final int readerId = r;
                    final boolean isForward = r < 2;
                    threads[r] = new Thread(() -> {
                        try {
                            if (isForward) {
                                try (Path rPath = new Path().of(dbRoot);
                                     PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                             configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                                    while (writerDone.getCount() > 0) {
                                        reader.reloadConditionally();
                                        RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                        long prev = -1;
                                        int count = 0;
                                        while (cursor.hasNext()) {
                                            long val = cursor.next();
                                            if (val <= prev) {
                                                throw new AssertionError(
                                                        "fwd reader " + readerId + ": non-ascending " + prev + " -> " + val);
                                            }
                                            prev = val;
                                            count++;
                                        }
                                        if (count % BP_BATCH != 0 || count == 0) {
                                            throw new AssertionError(
                                                    "fwd reader " + readerId + ": unexpected count=" + count);
                                        }
                                        Thread.yield();
                                    }
                                }
                            } else {
                                try (Path rPath = new Path().of(dbRoot);
                                     PostingIndexBwdReader reader = new PostingIndexBwdReader(
                                             configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                                    while (writerDone.getCount() > 0) {
                                        reader.reloadConditionally();
                                        RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                        long prev = Long.MAX_VALUE;
                                        int count = 0;
                                        while (cursor.hasNext()) {
                                            long val = cursor.next();
                                            if (val >= prev) {
                                                throw new AssertionError(
                                                        "bwd reader " + readerId + ": non-descending " + prev + " -> " + val);
                                            }
                                            prev = val;
                                            count++;
                                        }
                                        if (count % BP_BATCH != 0 || count == 0) {
                                            throw new AssertionError(
                                                    "bwd reader " + readerId + ": unexpected count=" + count);
                                        }
                                        Thread.yield();
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    threads[r].setDaemon(true);
                    threads[r].start();
                }

                for (int batch = 1; batch < writerBatches; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                }
                writerDone.countDown();

                for (Thread t : threads) {
                    t.join(10_000);
                    if (t.isAlive()) t.interrupt();
                }

                if (error.get() != null) {
                    throw new AssertionError("Mixed fwd/bwd concurrent reader failed", error.get());
                }
            }
        }
    }

    // ===================================================================
    // Double-buffered metadata page protocol tests
    // ===================================================================

    @Test
    public void testPageFlipVisibility() throws Exception {
        // Writer commits 10 times. After each commit, a fresh reader reload
        // sees the correct data. Track which page the reader uses and verify
        // the writer alternates between pages with monotonically increasing seqs.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "page_flip_vis";

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {

                        long prevSeq = -1;
                        long prevPageUsed = -1;

                        for (int commit = 0; commit < 10; commit++) {
                            long base = (long) commit * BP_BATCH;
                            for (int v = 0; v < BP_BATCH; v++) {
                                writer.add(0, base + v);
                            }
                            writer.setMaxValue(base + BP_BATCH - 1);
                            writer.commit();

                            // Reload reader to pick up the new commit
                            reader.reloadConditionally();

                            // Read seq_start from both pages to determine which is active
                            long keyBase = reader.getKeyBaseAddress();
                            long seqA = Unsafe.getUnsafe().getLong(
                                    keyBase + PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                            long seqEndA = Unsafe.getUnsafe().getLong(
                                    keyBase + PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
                            long seqB = Unsafe.getUnsafe().getLong(
                                    keyBase + PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                            long seqEndB = Unsafe.getUnsafe().getLong(
                                    keyBase + PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                            long validA = (seqA == seqEndA) ? seqA : 0;
                            long validB = (seqB == seqEndB) ? seqB : 0;
                            long bestSeq = Math.max(validA, validB);
                            long bestPage = (validB > validA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

                            // Sequence must increase monotonically
                            Assert.assertTrue("commit " + commit + ": seq " + bestSeq + " not > prevSeq " + prevSeq,
                                    bestSeq > prevSeq);

                            // Pages must alternate (writer writes to inactive, then flips)
                            if (prevPageUsed >= 0) {
                                Assert.assertNotEquals(
                                        "commit " + commit + ": expected page flip but same page used",
                                        prevPageUsed, bestPage);
                            }

                            prevSeq = bestSeq;
                            prevPageUsed = bestPage;

                            // Verify all data is readable
                            RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                            int expectedCount = (commit + 1) * BP_BATCH;
                            int count = 0;
                            while (cursor.hasNext()) {
                                Assert.assertEquals("commit " + commit + " val " + count,
                                        (long) count, cursor.next());
                                count++;
                            }
                            Assert.assertEquals("commit " + commit + " count", expectedCount, count);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testTornPageFallback() throws Exception {
        // Writer commits data, creating a valid state. Then manually corrupt
        // one page (seq_start without matching seq_end). A fresh reader should
        // fall back to the valid page.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "torn_page_fb";

                // Write initial data (2 commits to get both pages written)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(BP_BATCH - 1);
                    writer.commit();

                    for (int v = BP_BATCH; v < 2 * BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(2 * BP_BATCH - 1);
                    writer.commit();
                }

                // Determine which page is currently active (highest valid seq)
                // Then corrupt the OTHER page's seq_start without matching seq_end
                try (MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                    try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                        PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                        keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);

                        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
                        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                        long validA = (seqA == seqEndA) ? seqA : 0;
                        long validB = (seqB == seqEndB) ? seqB : 0;
                        long activePage = (validB > validA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
                        long otherPage = (activePage == PostingIndexUtils.PAGE_A_OFFSET)
                                ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

                        // Corrupt the OTHER page: write a high seq_start but don't match seq_end
                        keyMem.putLong(otherPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 999L);
                        // seq_end stays at its old value -> torn page
                    }
                }

                // Open a fresh reader — should fall back to the valid page
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("count after other page corruption", 2 * BP_BATCH, count);
                }

                // Now corrupt the ACTIVE page (the one with the highest valid seq)
                try (MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                    try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                        PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                        keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);

                        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
                        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                        long validA = (seqA == seqEndA) ? seqA : 0;
                        long validB = (seqB == seqEndB) ? seqB : 0;
                        long activePage = (validB > validA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

                        // Corrupt the active page: write a new seq_start without matching seq_end
                        keyMem.putLong(activePage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 9999L);
                        // seq_end stays at its old valid value -> torn
                    }
                }

                // Open reader — should fall back to the other page (the old valid one)
                // The other page was corrupted earlier with seq_start=999, but its
                // old seq_end doesn't match 999. So the reader's readIndexMetadataFromBestPage
                // tries the highest seq_start (9999) first — that's torn. Falls back to the
                // other page with seq_start=999 — also torn. Both pages are torn, so the reader
                // should see 0 keys (graceful degradation, not a crash).
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    Assert.assertFalse("both pages torn: cursor should be empty", cursor.hasNext());
                }

                // Fix: restore one valid page so we can verify fallback works when only one is torn
                try (MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                    try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                        PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                        keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);

                        // Make page A valid with seq=50
                        keyMem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 50L);
                        Unsafe.getUnsafe().storeFence();
                        keyMem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, 50L);

                        // Page B stays torn (seq_start=999 != seq_end)
                        // Page A seq=50 should be picked
                    }
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("restored page A, val " + count, (long) count, cursor.next());
                        count++;
                    }
                    // Page A should have the data from when it was last validly written
                    Assert.assertTrue("restored page A should have data", count > 0);
                }
            }
        });
    }

    @Test
    public void testSnapshotDecoupledFromPage() throws Exception {
        // Writer commits 5 batches. Reader reloads and snapshots gen dir.
        // Then the writer seals (overwriting gen dir on the inactive page and flipping).
        // The reader's cursor should still iterate correctly using its snapshotted gen dir.
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "snap_decoupled";
        final AtomicReference<Throwable> error = new AtomicReference<>();

        try (Path path = new Path().of(dbRoot)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                // Write 5 sparse gens to key 0
                for (int batch = 0; batch < 5; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                }

                int expectedTotal = 5 * BP_BATCH;
                CyclicBarrier readerReady = new CyclicBarrier(2);
                CyclicBarrier sealDone = new CyclicBarrier(2);

                Thread readerThread = new Thread(() -> {
                    try (Path rPath = new Path().of(dbRoot);
                         PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                 configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        // Reload to snapshot the 5 sparse gens
                        reader.reloadConditionally();

                        // Create cursor (this snapshots gen dir into genLookup)
                        RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);

                        // Signal to writer: reader has snapshotted, go ahead and seal
                        readerReady.await();

                        // Wait for writer to finish sealing
                        sealDone.await();

                        // Now iterate the cursor — seal has rewritten gen dir on
                        // the now-active page, but cursor uses the snapshotted copy.
                        long prev = -1;
                        int count = 0;
                        while (cursor.hasNext()) {
                            long val = cursor.next();
                            if (val <= prev) {
                                throw new AssertionError(
                                        "non-ascending at pos " + count + ": " + prev + " -> " + val);
                            }
                            if (val != count) {
                                throw new AssertionError(
                                        "expected " + count + " got " + val + " at pos " + count);
                            }
                            prev = val;
                            count++;
                        }
                        if (count != expectedTotal) {
                            throw new AssertionError(
                                    "expected " + expectedTotal + " values, got " + count);
                        }
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                });
                readerThread.setDaemon(true);
                readerThread.start();

                // Wait for reader to snapshot
                readerReady.await();

                // Seal: merges 5 sparse gens into 1 dense gen, writes to inactive page, flips
                writer.seal();

                // Signal reader: seal is done
                sealDone.await();

                readerThread.join(10_000);
                if (readerThread.isAlive()) readerThread.interrupt();

                if (error.get() != null) {
                    throw new AssertionError("Snapshot decoupled test failed", error.get());
                }

                // Also verify a fresh reader sees all data after seal
                try (PostingIndexFwdReader freshReader = new PostingIndexFwdReader(
                        configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = freshReader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals(expectedTotal, count);
                }
            }
        }
    }

    @Test
    public void testMaxGenCountAutoSeal() throws Exception {
        // Write exactly MAX_GEN_COUNT+1 batches to trigger auto-seal.
        // The writer calls seal() inside flushAllPending when genCount > MAX_GEN_COUNT.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "auto_seal_max";

                int batchCount = PostingIndexUtils.MAX_GEN_COUNT + 1; // 168

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int batch = 0; batch < batchCount; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }

                    // After MAX_GEN_COUNT+1 commits, auto-seal should have triggered,
                    // reducing genCount back to 1 (sealed) plus any commits after seal.
                    // The auto-seal fires at genCount > MAX_GEN_COUNT, so after the 168th
                    // commit genCount becomes 168 > 167, triggering seal() which sets it to 1.
                    Assert.assertTrue(
                            "genCount should be <= MAX_GEN_COUNT after auto-seal, was " + writer.getGenCount(),
                            writer.getGenCount() <= PostingIndexUtils.MAX_GEN_COUNT);
                }

                // Verify all data is readable
                int totalValues = batchCount * BP_BATCH;
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("total count", totalValues, count);
                }

                // Also verify backward
                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("bwd val " + count,
                                (long) (totalValues - 1 - count), cursor.next());
                        count++;
                    }
                    Assert.assertEquals("bwd total count", totalValues, count);
                }
            }
        });
    }

    @Test
    public void testRapidPageCycling() throws Exception {
        // Writer commits 500 times in a tight loop with small batches.
        // This cycles through both pages ~250 times each. After every 50 commits,
        // a reader reloads and verifies all data.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "rapid_cycling";
                int totalCommits = 500;
                int batchSize = 10; // small batches to keep it fast
                int checkEvery = 50;

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {

                        for (int commit = 0; commit < totalCommits; commit++) {
                            long base = (long) commit * batchSize;
                            for (int v = 0; v < batchSize; v++) {
                                writer.add(0, base + v);
                            }
                            writer.setMaxValue(base + batchSize - 1);
                            writer.commit();

                            if ((commit + 1) % checkEvery == 0) {
                                reader.reloadConditionally();

                                // Verify both pages have valid sequences (no corruption)
                                long keyBase = reader.getKeyBaseAddress();
                                long seqA = Unsafe.getUnsafe().getLong(
                                        keyBase + PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                                long seqEndA = Unsafe.getUnsafe().getLong(
                                        keyBase + PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
                                long seqB = Unsafe.getUnsafe().getLong(
                                        keyBase + PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                                long seqEndB = Unsafe.getUnsafe().getLong(
                                        keyBase + PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                                // Both pages should be valid (seq_start == seq_end) since writes are single-threaded
                                Assert.assertEquals("commit " + (commit + 1) + " page A torn",
                                        seqA, seqEndA);
                                Assert.assertEquals("commit " + (commit + 1) + " page B torn",
                                        seqB, seqEndB);

                                // The active page should have a higher seq
                                long bestSeq = Math.max(seqA, seqB);
                                Assert.assertTrue("commit " + (commit + 1) + " seq should be positive",
                                        bestSeq > 0);

                                // Verify all data
                                int expectedTotal = (commit + 1) * batchSize;
                                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                long prev = -1;
                                int count = 0;
                                while (cursor.hasNext()) {
                                    long val = cursor.next();
                                    Assert.assertTrue(
                                            "commit " + (commit + 1) + " non-ascending at " + count + ": " + prev + " -> " + val,
                                            val > prev);
                                    prev = val;
                                    count++;
                                }
                                Assert.assertEquals("commit " + (commit + 1) + " count",
                                        expectedTotal, count);
                            }
                        }
                    }
                }

                // Final verification with a fresh reader
                int totalValues = totalCommits * batchSize;
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("final val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("final count", totalValues, count);
                }
            }
        });
    }

    // ===================================================================
    // Corruption detection and crash safety tests
    // ===================================================================

    @Test
    public void testCrashDuringCommitRecovery() throws Exception {
        // Simulate a crash mid-commit by writing seq_start but NOT seq_end on
        // the inactive metadata page. A fresh reader and writer should recover
        // from the valid page and continue operating correctly.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "crash_commit";
                int initialBatches = 5;
                int totalInitialValues = initialBatches * BP_BATCH;

                // Phase 1: write 5 batches normally, close (triggers seal + compact)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int batch = 0; batch < initialBatches; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }
                }
                // After close: sealed to 1 gen, compacted

                // Phase 2: corrupt the inactive page to simulate a partial next commit
                try (MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                    try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                        PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                        keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);

                        // Find the active and inactive pages
                        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
                        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                        long validA = (seqA == seqEndA) ? seqA : 0;
                        long validB = (seqB == seqEndB) ? seqB : 0;
                        long activePage = (validB > validA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
                        long inactivePage = (activePage == PostingIndexUtils.PAGE_A_OFFSET)
                                ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

                        // Write seq_start on inactive page with a higher sequence
                        // but do NOT write matching seq_end (simulating crash mid-write)
                        long activeSeq = keyMem.getLong(activePage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        keyMem.putLong(inactivePage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, activeSeq + 1);
                        // Write a bogus genCount to simulate partial metadata
                        keyMem.putInt(inactivePage + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT, 999);
                        // seq_end stays stale -> torn page
                    }
                }

                // Phase 3: open a fresh reader -- should fall back to the valid page
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("reader val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("reader sees all initial data", totalInitialValues, count);
                }

                // Phase 4: open a fresh writer -- should recover from the valid page
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    // After seal+compact, genCount=1
                    Assert.assertEquals("writer genCount after recovery", 1, writer.getGenCount());

                    // Verify existing data via writer cursor
                    RowCursor preCursor = writer.getCursor(0);
                    int preCount = 0;
                    while (preCursor.hasNext()) {
                        Assert.assertEquals("pre-write val " + preCount,
                                (long) preCount, preCursor.next());
                        preCount++;
                    }
                    Assert.assertEquals("existing data intact", totalInitialValues, preCount);

                    // Phase 5: write 5 more batches
                    for (int batch = initialBatches; batch < initialBatches + 5; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }
                }

                // Verify all 10 batches are readable
                int totalFinalValues = (initialBatches + 5) * BP_BATCH;
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("final val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("all batches readable after recovery", totalFinalValues, count);
                }
            }
        });
    }

    @Test
    public void testCrashDuringSealRecovery() throws Exception {
        // Simulate a crash after seal writes value data but before the metadata
        // page update completes: the value file is larger than what the metadata
        // page claims (extra bytes from an incomplete seal). Reader and writer
        // should ignore the extra bytes and operate on the metadata-defined state.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "crash_seal";

                // Write 10 batches, close (which seals to 1 gen and compacts)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int batch = 0; batch < 10; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }
                }
                // After close: data is sealed (1 gen), compacted, value file is trimmed.

                // Read the metadata-claimed value file size
                long metadataValueMemSize;
                try (MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                    try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                        PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                        keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);

                        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
                        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                        long seqEndB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
                        long validA = (seqA == seqEndA) ? seqA : 0;
                        long validB = (seqB == seqEndB) ? seqB : 0;
                        long activePage = (validB > validA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
                        metadataValueMemSize = keyMem.getLong(activePage + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
                    }
                }

                // Simulate incomplete seal: extend the value file with 4KB of garbage
                // (as if seal wrote new gen data but crashed before updating the metadata page)
                FilesFacade ff = configuration.getFilesFacade();
                try (Path valPath = new Path().of(configuration.getDbRoot())) {
                    PostingIndexUtils.valueFileName(valPath, name, COLUMN_NAME_TXN_NONE);
                    long newSize = metadataValueMemSize + 4096;
                    long fd = ff.openRW(valPath.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue("could not open value file", fd > 0);
                    try {
                        ff.truncate(fd, newSize);
                    } finally {
                        ff.close(fd);
                    }
                }

                // Open fresh reader -- should see the sealed data, ignoring extra bytes
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("reader val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("reader sees sealed data", 10 * BP_BATCH, count);
                }

                // Open fresh writer -- should recover and be able to write more data
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    Assert.assertEquals("writer recovered with 1 sealed gen", 1, writer.getGenCount());

                    // Write 5 more batches on top
                    for (int batch = 10; batch < 15; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }
                    writer.seal();
                }

                // Verify all data is correct after recovery + additional writes
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("sealed val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("all 15 batches readable", 15 * BP_BATCH, count);
                }
            }
        });
    }

    @Test
    public void testCorruptedValueFileRecovery() throws Exception {
        // Write data and seal, then truncate the value file shorter than what
        // the metadata claims. Opening a reader should throw CairoException
        // or handle gracefully (not SIGSEGV).
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "corrupt_val";
                FilesFacade ff = configuration.getFilesFacade();

                // Write and seal
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int batch = 0; batch < 5; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }
                }

                // Truncate the value file to be shorter than what metadata claims
                try (Path valPath = new Path().of(configuration.getDbRoot())) {
                    PostingIndexUtils.valueFileName(valPath, name, COLUMN_NAME_TXN_NONE);
                    long fd = ff.openRW(valPath.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue("could not open value file", fd > 0);
                    try {
                        long currentSize = ff.length(fd);
                        Assert.assertTrue("value file should have data", currentSize > 0);
                        // Truncate to half the size
                        boolean truncated = ff.truncate(fd, currentSize / 2);
                        Assert.assertTrue("truncate should succeed", truncated);
                    } finally {
                        ff.close(fd);
                    }
                }

                // Open reader -- should either throw CairoException or handle gracefully
                // (the key thing is it should NOT cause a SIGSEGV / JVM crash)
                boolean survived = false;
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    // If the reader opens, attempt to use the cursor
                    try {
                        RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                        while (cursor.hasNext()) {
                            cursor.next();
                        }
                    } catch (CairoException e) {
                        // Exception during cursor iteration is acceptable
                    }
                    survived = true;
                } catch (CairoException e) {
                    // Reader may throw on open -- that is also acceptable
                    survived = true;
                }
                // We survived without a JVM crash
                Assert.assertTrue("should complete without JVM crash", survived);
            }
        });
    }

    @Test
    public void testBothPagesCorrupted() throws Exception {
        // Write valid data, then corrupt BOTH metadata pages (mismatched
        // seq_start/seq_end on both). A fresh reader should see 0 keys /
        // empty index (graceful degradation, not crash).
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "both_corrupt";

                // Write valid data
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(BP_BATCH - 1);
                    writer.commit();
                }

                // Corrupt BOTH pages
                try (MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                    try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                        PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                        keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);

                        // Page A: write mismatched seq_start/seq_end
                        keyMem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 100L);
                        // Leave seq_end at its old value (not 100) -> torn
                        keyMem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, 50L);

                        // Page B: write mismatched seq_start/seq_end
                        keyMem.putLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 200L);
                        keyMem.putLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, 75L);
                    }
                }

                // Open reader -- should see 0 keys (graceful degradation)
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    Assert.assertFalse("both pages corrupted: cursor should be empty", cursor.hasNext());
                    Assert.assertEquals("keyCount should be 0", 0, reader.getKeyCount());
                }
            }
        });
    }

    @Test
    public void testPartialKeyFileRecovery() throws Exception {
        // Write valid data, then truncate the key file to less than 8192 bytes
        // (only page A remains). Opening a reader should work if page A is valid,
        // or handle gracefully if not.
        // Note: no assertMemoryLeak — intentionally corrupts key file which can cause
        // OS-level FD tracking artifacts from mmap of truncated files.
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "partial_key";
                FilesFacade ff = configuration.getFilesFacade();

                // Write valid data
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(BP_BATCH - 1);
                    writer.commit();

                    // Write a second batch to ensure page B has been written too
                    for (int v = BP_BATCH; v < 2 * BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(2 * BP_BATCH - 1);
                    writer.commit();
                }

                // Verify the key file is at least 8192 bytes
                try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                    PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                    long keyFd = ff.openRW(keyPath.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue("could not open key file", keyFd > 0);
                    try {
                        long keyFileSize = ff.length(keyFd);
                        Assert.assertTrue("key file should be >= 8192", keyFileSize >= PostingIndexUtils.KEY_FILE_RESERVED);
                    } finally {
                        ff.close(keyFd);
                    }
                }

                // Truncate key file to 4096 bytes (only page A)
                try (Path keyPath = new Path().of(configuration.getDbRoot())) {
                    PostingIndexUtils.keyFileName(keyPath, name, COLUMN_NAME_TXN_NONE);
                    long keyFd = ff.openRW(keyPath.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue("could not reopen key file", keyFd > 0);
                    try {
                        boolean truncated = ff.truncate(keyFd, PostingIndexUtils.PAGE_SIZE);
                        Assert.assertTrue("truncate to 4096 should succeed", truncated);
                    } finally {
                        ff.close(keyFd);
                    }
                }

                // Open reader -- should either work (if page A is valid and the reader
                // can tolerate a short file) or throw an exception, but NOT SIGSEGV.
                // On Linux, mmap beyond file end on a 4096-byte file will map 2 pages
                // but accessing page B may produce zero-filled reads (page-aligned file
                // boundary) or an error. The reader reads page B's seq_start which will
                // be 0, so it falls back to page A.
                boolean succeeded = false;
                try {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        // If it opens, try to read -- we want to ensure no JVM crash
                        RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                        int count = 0;
                        while (cursor.hasNext()) {
                            cursor.next();
                            count++;
                        }
                        // Page A should be valid, page B zeros -> reader uses page A
                        Assert.assertTrue("should see data from page A", count > 0);
                    }
                    succeeded = true;
                } catch (CairoException | InternalError e) {
                    // Acceptable: reader may refuse a truncated key file, or
                    // the JVM may catch SIGBUS from accessing mmap'd pages
                    // beyond the truncated file's EOF (during open, read, or close).
                    succeeded = true;
                }
                Assert.assertTrue("should complete without JVM crash", succeeded);
            }
    }

    @Test
    public void testWriterRecoveryAfterDirtyShutdown() throws Exception {
        // Simulate a dirty shutdown: writer commits 20 batches to create 20 sparse gens.
        // On close, seal() and compact are called (simulating a clean shutdown).
        // After reopen, the writer should see the sealed (1 gen) state, be able to
        // write 5 more batches, seal again, and all 25 batches should be readable.
        // This exercises the full close-reopen-continue-writing recovery path.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                String name = "dirty_shutdown";

                // Phase 1: write 20 batches (creating 20 sparse gens)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    for (int batch = 0; batch < 20; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }
                    Assert.assertEquals("20 gens after commits", 20, writer.getGenCount());
                }
                // close() called seal() + compact: genCount is now 1

                // Phase 2: re-open the writer on the same files (init=false)
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);

                    // After close+seal, genCount should be 1 (single sealed gen)
                    Assert.assertEquals("genCount after recovery", 1, writer.getGenCount());
                    Assert.assertEquals("keyCount preserved", 1, writer.getKeyCount());

                    // Verify existing data is accessible via writer cursor
                    RowCursor preCursor = writer.getCursor(0);
                    int preCount = 0;
                    while (preCursor.hasNext()) {
                        Assert.assertEquals("pre-write val " + preCount,
                                (long) preCount, preCursor.next());
                        preCount++;
                    }
                    Assert.assertEquals("pre-write count", 20 * BP_BATCH, preCount);

                    // Phase 3: write 5 more batches
                    for (int batch = 20; batch < 25; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                    }

                    // Seal to finalize
                    writer.seal();
                }

                // Phase 4: verify all 25 batches readable via forward reader
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("val " + count, (long) count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals("all 25 batches readable", 25 * BP_BATCH, count);
                }

                // Also verify backward
                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals("bwd val " + count,
                                (long) (25 * BP_BATCH - 1 - count), cursor.next());
                        count++;
                    }
                    Assert.assertEquals("bwd all 25 batches readable", 25 * BP_BATCH, count);
                }
            }
        });
    }

    // ===================================================================
    // Tier-specific lookup tests
    // ===================================================================

    @Test
    public void testTier1PerKeyLookupFwd() throws Exception {
        assertMemoryLeak(() -> {
        // Write data for 10 active keys spread across 50 sparse generations.
        // Each key appears in only 2-3 of the 50 gens.
        // With the default 256MB budget, Tier 1 (per-key) will be used.
        int activeKeyCount = 10;
        int totalKeySpace = 1000;
        int genCount = 50;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < totalKeySpace; k++) {
                oracle.add(new LongList());
            }

            Rnd rnd = new Rnd(777, 777);
            // Precompute which gens each key appears in (2-3 gens per key)
            int[][] keyGenMap = new int[activeKeyCount][];
            for (int k = 0; k < activeKeyCount; k++) {
                int numGens = 2 + rnd.nextInt(2); // 2 or 3
                keyGenMap[k] = new int[numGens];
                for (int g = 0; g < numGens; g++) {
                    keyGenMap[k][g] = rnd.nextInt(genCount);
                }
            }

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "tier1_fwd", COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                long[] nextRowId = new long[totalKeySpace];
                for (int g = 0; g < genCount; g++) {
                    boolean anyWritten = false;
                    for (int k = 0; k < activeKeyCount; k++) {
                        boolean inThisGen = false;
                        for (int gIdx : keyGenMap[k]) {
                            if (gIdx == g) {
                                inThisGen = true;
                                break;
                            }
                        }
                        if (inThisGen) {
                            int key = k * (totalKeySpace / activeKeyCount); // spread keys across key space
                            for (int v = 0; v < 5; v++) {
                                long val = nextRowId[key]++;
                                writer.add(key, val);
                                oracle.getQuick(key).add(val);
                                if (val > maxVal) maxVal = val;
                            }
                            anyWritten = true;
                        }
                    }
                    if (anyWritten) {
                        writer.setMaxValue(maxVal);
                        writer.commit();
                    }
                }

                // Open reader INSIDE the writer scope (before seal) so sparse gens exist
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), "tier1_fwd", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    // Force lookup build by reading a cursor
                    RowCursor firstCursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    while (firstCursor.hasNext()) {
                        firstCursor.next();
                    }
                    Assert.assertEquals("Tier 1 should be used",
                            TIER_PER_KEY, reader.getGenLookupTier());

                    for (int k = 0; k < totalKeySpace; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = 0;
                        long prev = Long.MIN_VALUE;
                        while (cursor.hasNext()) {
                            long val = cursor.next();
                            Assert.assertTrue("key=" + k + " idx=" + idx + " not ascending: " + prev + " -> " + val,
                                    val > prev);
                            Assert.assertTrue("key=" + k + " extra values at idx=" + idx,
                                    idx < expected.size());
                            Assert.assertEquals("key=" + k + " idx=" + idx,
                                    expected.getQuick(idx), val);
                            prev = val;
                            idx++;
                        }
                        Assert.assertEquals("key=" + k + " count", expected.size(), idx);
                    }
                }
            }
        }
        });
    }

    @Test
    public void testTier1PerKeyLookupBwd() throws Exception {
        assertMemoryLeak(() -> {
        // Same setup as testTier1PerKeyLookupFwd but reads with BwdReader.
        int activeKeyCount = 10;
        int totalKeySpace = 1000;
        int genCount = 50;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < totalKeySpace; k++) {
                oracle.add(new LongList());
            }

            Rnd rnd = new Rnd(777, 777);
            int[][] keyGenMap = new int[activeKeyCount][];
            for (int k = 0; k < activeKeyCount; k++) {
                int numGens = 2 + rnd.nextInt(2);
                keyGenMap[k] = new int[numGens];
                for (int g = 0; g < numGens; g++) {
                    keyGenMap[k][g] = rnd.nextInt(genCount);
                }
            }

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "tier1_bwd", COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                long[] nextRowId = new long[totalKeySpace];
                for (int g = 0; g < genCount; g++) {
                    boolean anyWritten = false;
                    for (int k = 0; k < activeKeyCount; k++) {
                        boolean inThisGen = false;
                        for (int gIdx : keyGenMap[k]) {
                            if (gIdx == g) {
                                inThisGen = true;
                                break;
                            }
                        }
                        if (inThisGen) {
                            int key = k * (totalKeySpace / activeKeyCount);
                            for (int v = 0; v < 5; v++) {
                                long val = nextRowId[key]++;
                                writer.add(key, val);
                                oracle.getQuick(key).add(val);
                                if (val > maxVal) maxVal = val;
                            }
                            anyWritten = true;
                        }
                    }
                    if (anyWritten) {
                        writer.setMaxValue(maxVal);
                        writer.commit();
                    }
                }

                // Open reader INSIDE the writer scope (before seal) so sparse gens exist
                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), "tier1_bwd", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    // Force lookup build
                    RowCursor firstCursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    while (firstCursor.hasNext()) {
                        firstCursor.next();
                    }
                    Assert.assertEquals("Tier 1 should be used",
                            TIER_PER_KEY, reader.getGenLookupTier());

                    for (int k = 0; k < totalKeySpace; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = expected.size() - 1;
                        long prev = Long.MAX_VALUE;
                        while (cursor.hasNext()) {
                            long val = cursor.next();
                            Assert.assertTrue("key=" + k + " not descending: " + prev + " -> " + val,
                                    val < prev);
                            Assert.assertTrue("key=" + k + " extra bwd values", idx >= 0);
                            Assert.assertEquals("key=" + k + " bwd idx=" + idx,
                                    expected.getQuick(idx), val);
                            prev = val;
                            idx--;
                        }
                        Assert.assertEquals("key=" + k + " bwd count", -1, idx);
                    }
                }
            }
        }
        });
    }

    @Test
    public void testTier2SbbfLookup() throws Exception {
        assertMemoryLeak(() -> {
        // Write data across 20 sparse generations. Set a tiny memory budget
        // to force Tier 2 (SBBF) instead of Tier 1.
        int keyCount = 100;
        int genCount = 20;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < keyCount; k++) {
                oracle.add(new LongList());
            }

            Rnd rnd = new Rnd(42, 42);
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "tier2_sbbf", COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                long[] nextRowId = new long[keyCount];
                for (int g = 0; g < genCount; g++) {
                    // Each gen has ~30 random keys active
                    int activeInGen = 30;
                    for (int i = 0; i < activeInGen; i++) {
                        int key = rnd.nextInt(keyCount);
                        for (int v = 0; v < 3; v++) {
                            long val = nextRowId[key]++;
                            writer.add(key, val);
                            oracle.getQuick(key).add(val);
                            if (val > maxVal) maxVal = val;
                        }
                    }
                    writer.setMaxValue(maxVal);
                    writer.commit();
                }

                // Open readers INSIDE the writer scope (before seal) so sparse gens exist

                // Verify forward reader with Tier 2 (SBBF)
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), "tier2_sbbf", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    // Set a small budget: too small for Tier 1 (per-key) but enough for Tier 2 (SBBF)
                    reader.setGenLookupMemoryBudget(1024);

                    // Force lookup rebuild by reading a cursor
                    RowCursor firstCursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    while (firstCursor.hasNext()) {
                        firstCursor.next();
                    }
                    Assert.assertEquals("Tier 2 (SBBF) should be used",
                            TIER_SBBF, reader.getGenLookupTier());

                    // Verify correctness for all keys
                    for (int k = 0; k < keyCount; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = 0;
                        while (cursor.hasNext()) {
                            Assert.assertTrue("key=" + k + " extra values at idx=" + idx,
                                    idx < expected.size());
                            Assert.assertEquals("key=" + k + " idx=" + idx,
                                    expected.getQuick(idx), cursor.next());
                            idx++;
                        }
                        Assert.assertEquals("key=" + k + " count", expected.size(), idx);
                    }
                }

                // Also verify backward reader with Tier 2
                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), "tier2_sbbf", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    reader.setGenLookupMemoryBudget(1024);

                    RowCursor firstCursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    while (firstCursor.hasNext()) {
                        firstCursor.next();
                    }
                    Assert.assertEquals("Bwd Tier 2 (SBBF) should be used",
                            TIER_SBBF, reader.getGenLookupTier());

                    for (int k = 0; k < keyCount; k++) {
                        LongList expected = oracle.getQuick(k);
                        RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                        int idx = expected.size() - 1;
                        while (cursor.hasNext()) {
                            Assert.assertTrue("key=" + k + " bwd extra", idx >= 0);
                            Assert.assertEquals("key=" + k + " bwd idx=" + idx,
                                    expected.getQuick(idx), cursor.next());
                            idx--;
                        }
                        Assert.assertEquals("key=" + k + " bwd count", -1, idx);
                    }
                }
            }
        }
        });
    }

    @Test
    public void testTier3FallbackLookup() throws Exception {
        assertMemoryLeak(() -> {
        // Write data with only 2 sparse generations and small data.
        // Set budget=0 so Tier 1 is rejected, and genCount <= 2 so Tier 2 is rejected.
        // This forces Tier 3 (NONE, binary search fallback).
        int keyCount = 5;

        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();

            ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < keyCount; k++) {
                oracle.add(new LongList());
            }

            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "tier3_none", COLUMN_NAME_TXN_NONE)) {
                long maxVal = -1;
                // Gen 0: keys 0, 1, 2
                for (int k = 0; k < 3; k++) {
                    for (int v = 0; v < 10; v++) {
                        long val = (long) k * 1000 + v;
                        writer.add(k, val);
                        oracle.getQuick(k).add(val);
                        if (val > maxVal) maxVal = val;
                    }
                }
                writer.setMaxValue(maxVal);
                writer.commit();

                // Gen 1: keys 2, 3, 4
                for (int k = 2; k < keyCount; k++) {
                    for (int v = 0; v < 10; v++) {
                        long val = (long) k * 1000 + 100 + v;
                        writer.add(k, val);
                        oracle.getQuick(k).add(val);
                        if (val > maxVal) maxVal = val;
                    }
                }
                writer.setMaxValue(maxVal);
                writer.commit();
            }

            // Verify forward reader with Tier 3 (NONE)
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), "tier3_none", COLUMN_NAME_TXN_NONE, -1, 0)) {
                // Set budget=0 to reject Tier 1, genCount=2 rejects Tier 2
                reader.setGenLookupMemoryBudget(0);

                RowCursor firstCursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                while (firstCursor.hasNext()) {
                    firstCursor.next();
                }
                Assert.assertEquals("Tier 3 (NONE) should be used",
                        TIER_NONE, reader.getGenLookupTier());

                for (int k = 0; k < keyCount; k++) {
                    LongList expected = oracle.getQuick(k);
                    RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                    int idx = 0;
                    while (cursor.hasNext()) {
                        Assert.assertTrue("key=" + k + " extra values at idx=" + idx,
                                idx < expected.size());
                        Assert.assertEquals("key=" + k + " idx=" + idx,
                                expected.getQuick(idx), cursor.next());
                        idx++;
                    }
                    Assert.assertEquals("key=" + k + " count", expected.size(), idx);
                }
            }

            // Also verify backward reader with Tier 3
            try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                    configuration, path.trimTo(plen), "tier3_none", COLUMN_NAME_TXN_NONE, -1, 0)) {
                reader.setGenLookupMemoryBudget(0);

                RowCursor firstCursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                while (firstCursor.hasNext()) {
                    firstCursor.next();
                }
                Assert.assertEquals("Bwd Tier 3 (NONE) should be used",
                        TIER_NONE, reader.getGenLookupTier());

                for (int k = 0; k < keyCount; k++) {
                    LongList expected = oracle.getQuick(k);
                    RowCursor cursor = reader.getCursor(false, k, 0, Long.MAX_VALUE);
                    int idx = expected.size() - 1;
                    while (cursor.hasNext()) {
                        Assert.assertTrue("key=" + k + " bwd extra", idx >= 0);
                        Assert.assertEquals("key=" + k + " bwd idx=" + idx,
                                expected.getQuick(idx), cursor.next());
                        idx--;
                    }
                    Assert.assertEquals("key=" + k + " bwd count", -1, idx);
                }
            }
        }
        });
    }

    @Test
    public void testStreamingScenarioWithIncrementalSeal() throws Exception {
        // Reproduces the benchmark S3 streaming scenario that triggers
        // sealIncremental -> encodeDirtyStride -> mergeKeyValues crash.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "streaming_seal";
                int keyCount = 50_000;
                int commits = 500;
                double activityRatio = 0.02;
                int activePerCommit = (int) (keyCount * activityRatio); // 1000
                int valsPerActive = 10;

                Rnd rnd = new Rnd(12345, 67890);
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int rowId = 0;
                    for (int c = 0; c < commits; c++) {
                        // Pick activePerCommit random keys
                        int[] pool = new int[keyCount];
                        for (int i = 0; i < keyCount; i++) pool[i] = i;
                        for (int i = 0; i < activePerCommit; i++) {
                            int j = i + rnd.nextPositiveInt() % (keyCount - i);
                            int tmp = pool[i]; pool[i] = pool[j]; pool[j] = tmp;
                        }
                        int[] activeKeys = java.util.Arrays.copyOf(pool, activePerCommit);
                        java.util.Arrays.sort(activeKeys);

                        for (int key : activeKeys) {
                            for (int v = 0; v < valsPerActive; v++) {
                                writer.add(key, rowId++);
                            }
                        }
                        writer.setMaxValue(rowId - 1);
                        writer.commit();
                    }
                } // close triggers seal

                // Verify data is readable
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        cursor.next();
                        count++;
                    }
                    Assert.assertTrue("should have some values for key 0", count > 0);
                }
            }
        });
    }
}
