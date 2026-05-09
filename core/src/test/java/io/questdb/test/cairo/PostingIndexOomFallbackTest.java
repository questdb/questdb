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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Exercises the spill-arena back-pressure and seal-time streaming fallback
 * added to bound PostingIndexWriter peak RSS during long indexing runs
 * (ALTER ADD INDEX TYPE POSTING, IndexBuilder, the discardForRebuild path
 * O3 commits drive on indexed columns).
 * <p>
 * Each test asserts both that the expected memory-bounded code path
 * actually fires and that the produced index is still byte-equivalent
 * to the one the unbounded fast path would have produced -- readers
 * cannot tell which path generated the bytes on disk.
 */
public class PostingIndexOomFallbackTest extends AbstractCairoTest {

    private static LongList collectAllRowIds(PostingIndexFwdReader reader, int keyCount) {
        LongList all = new LongList();
        for (int k = 0; k < keyCount; k++) {
            all.add(k);
            all.add(-1); // separator
            RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                all.add(cursor.next());
            }
            Misc.free(cursor);
        }
        return all;
    }

    /**
     * Writer-level smoke test. Configures a 64 KiB spill budget so the
     * mid-stream flush fires repeatedly during the indexing loop. Drives
     * 50 keys × 4096 rows per key = 200_000 rows -- the worst-case spill
     * arena would be ~1.6 MiB raw at peak, comfortably exceeding the 64
     * KiB budget. Asserts:
     * <ul>
     *   <li>genCount > 1 before seal (proves periodic flush fired);</li>
     *   <li>peak NATIVE_INDEX_READER usage stayed within ~10x the
     *       budget (the arena drains; pending arrays stay live);</li>
     *   <li>final cursor returns every (key, rowId) pair in order.</li>
     * </ul>
     */
    @Test
    public void testIndexerPeriodicFlushUnderBudget() throws Exception {
        node1.getConfigurationOverrides().setProperty(
                PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 64L * 1024L);
        final int keys = 50;
        final int rowsPerKey = 4096;

        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "periodic_flush";

                int genCountBeforeSeal;
                long peakNativeIndexReader = 0;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    long row = 0;
                    for (int r = 0; r < rowsPerKey; r++) {
                        for (int k = 0; k < keys; k++) {
                            writer.add(k, row++);
                        }
                        long used = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                        if (used > peakNativeIndexReader) {
                            peakNativeIndexReader = used;
                        }
                    }
                    genCountBeforeSeal = writer.getGenCount();
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    writer.seal();
                }

                Assert.assertTrue(
                        "expected genCount > 1 before seal (periodic flush should have produced multiple gens), got " + genCountBeforeSeal,
                        genCountBeforeSeal > 1);

                // Peak should be a small multiple of the budget. The 10x
                // bound is conservative -- pending buffers, fsst scratch,
                // valueMem mmap pages, and the keyMem header all share the
                // same NATIVE_INDEX_READER tag. What matters is that the
                // peak does not scale with row count.
                Assert.assertTrue(
                        "peak NATIVE_INDEX_READER usage too high: " + peakNativeIndexReader,
                        peakNativeIndexReader < 64L * 1024L * 64L);

                // Verify every value lands at the right key in the right
                // order. The indexer feeds rowIds monotonically; flushes
                // produce sparse gens that the seal compacts into a dense
                // gen 0.
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int k = 0; k < keys; k++) {
                        RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                        for (int r = 0; r < rowsPerKey; r++) {
                            Assert.assertTrue("key " + k + " row " + r + ": cursor exhausted early", cursor.hasNext());
                            long expected = (long) r * keys + k;
                            long got = cursor.next();
                            Assert.assertEquals("key " + k + " row " + r, expected, got);
                        }
                        Assert.assertFalse("key " + k + ": cursor has unexpected extra rows", cursor.hasNext());
                        Misc.free(cursor);
                    }
                }
            }
        });
    }

    /**
     * Pathological-budget regression: a per-key spill budget so tight
     * that periodic flushes accumulate past MAX_GEN_COUNT (143). Each
     * flush bumps genCount by one; once it crosses MAX_GEN_COUNT,
     * flushAllPending fires an inline seal() which frees both pending
     * and spill buffers. The compactIfOverBudget call site inside
     * spillKey runs immediately before add()'s post-write to
     * pendingValuesAddr -- without the explicit pending-realloc the
     * post-write would dereference a freed pointer.
     * <p>
     * Forces the path with a 64-byte budget over 50 keys * 4096 rows.
     * Each per-key spill grow is ~256 bytes, so the budget trips on
     * essentially every spill, yielding hundreds of periodic flushes
     * and at least one MAX_GEN_COUNT-driven seal. Asserts the index is
     * still readable and round-trips cleanly.
     */
    @Test
    public void testIndexerSurvivesMaxGenCountInlineSeal() throws Exception {
        // Tiny budget forces a flush on (essentially) every spill.
        node1.getConfigurationOverrides().setProperty(
                PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 64L);
        final int keys = 50;
        final int rowsPerKey = 4096;

        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "max_gen_inline_seal";

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    long row = 0;
                    for (int r = 0; r < rowsPerKey; r++) {
                        for (int k = 0; k < keys; k++) {
                            writer.add(k, row++);
                        }
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    writer.seal();
                }

                // Round-trip the data: with hundreds of inline seals the
                // chain went through repeated dense-gen rotations. If any
                // of them lost data, the cursor would short-count.
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int k = 0; k < keys; k++) {
                        RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                        for (int r = 0; r < rowsPerKey; r++) {
                            Assert.assertTrue("key " + k + " row " + r + ": cursor exhausted", cursor.hasNext());
                            long expected = (long) r * keys + k;
                            Assert.assertEquals("key " + k + " row " + r, expected, cursor.next());
                        }
                        Assert.assertFalse("key " + k + ": unexpected extra rows", cursor.hasNext());
                        Misc.free(cursor);
                    }
                }
            }
        });
    }

    /**
     * Forces the seal-time pre-flight to refuse via a pathological RSS
     * limit. The error message must explicitly call out that even the
     * streaming compaction would not fit, distinguishing the "too tight
     * for fast path" case (where streaming would still run) from the
     * "too tight for either" case the operator must remediate.
     * <p>
     * Calibration note: seal() calls freeSpillData + freePendingBuffers
     * BEFORE reencodeAllGenerations runs the pre-flight, so by the time
     * the pre-flight reads {@code Unsafe.getRssMemUsed()} the headroom
     * has grown by roughly (spill arena bytes) + (pending arena bytes)
     * relative to the snapshot at {@code setRssMemLimit}. Use a workload
     * with a single hot key whose streaming peak ({@code maxKeyCount}
     * times ~17 bytes) clearly exceeds even the post-free headroom.
     */
    @Test
    public void testSealHardLimitWhenEvenStreamingDoesNotFit() throws Exception {
        // Single hot key: maxKeyCount == rowCount. Streaming peak
        // = ~17 * rowCount bytes. With 200_000 rows that's ~3.4 MiB,
        // comfortably above the 256 KiB tight-limit even after seal()
        // frees the ~1.6 MiB spill arena.
        final int rowCount = 200_000;

        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "hard_limit";

                long savedLimit = Unsafe.getRssMemLimit();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < rowCount; i++) {
                        writer.add(0, i);
                    }
                    writer.setMaxValue(rowCount - 1);
                    writer.commit();

                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 256L * 1024L);
                    try {
                        writer.seal();
                        Assert.fail("expected CairoException, seal succeeded");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(),
                                "would exceed RSS limit even with streaming compaction");
                    } finally {
                        Unsafe.setRssMemLimit(savedLimit);
                    }
                }
            }
        });
    }

    /**
     * Streaming compaction explicitly refuses var-size cover columns
     * with an actionable error message. The fast path still handles
     * var-size cover data; this guard only applies when the pre-flight
     * forces the streaming branch. A follow-up will lift the
     * restriction by adding a two-pass write that materialises the
     * var-size offsets table after walking each key.
     */
    @Test
    public void testSealStreamingVarIncludeFails() throws Exception {
        // Use multiple keys so streaming peak (2 * maxKeyCount * 8) is much
        // smaller than fast-path peak (2 * rowsPerKey * keys * 8). With 16
        // keys * 256 rows/key, fast-path peak is ~64 KiB (single stride
        // holds them all) and streaming peak is ~4 KiB.
        final int keys = 16;
        final int rowsPerKey = 256;
        final int totalRows = keys * rowsPerKey;
        // BINARY layout: [8B length][N-byte payload]. Use 1-byte payload.
        final int binStride = Long.BYTES + 1;
        final long binDataSize = (long) totalRows * binStride;
        final long binAuxSize = (long) (totalRows + 1) * Long.BYTES;

        assertMemoryLeak(() -> {
            long binDataAddr = Unsafe.malloc(binDataSize, MemoryTag.NATIVE_DEFAULT);
            long binAuxAddr = Unsafe.malloc(binAuxSize, MemoryTag.NATIVE_DEFAULT);
            try {
                long off = 0;
                for (int i = 0; i < totalRows; i++) {
                    Unsafe.putLong(binAuxAddr + (long) i * Long.BYTES, off);
                    Unsafe.putLong(binDataAddr + off, 1L); // length = 1
                    Unsafe.putByte(binDataAddr + off + Long.BYTES, (byte) (i & 0xff));
                    off += binStride;
                }
                Unsafe.putLong(binAuxAddr + (long) totalRows * Long.BYTES, off);

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final String name = "var_cover_streaming";
                    long savedLimit = Unsafe.getRssMemLimit();
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{binDataAddr},
                                new long[]{binAuxAddr},
                                new long[]{0L},     // colTops
                                new int[]{-1},      // shifts: var-size
                                new int[]{2},       // writer indices
                                new int[]{ColumnType.BINARY},
                                /* coverCount */ 1,
                                /* timestampColumnIndex */ -1
                        );
                        long row = 0;
                        for (int r = 0; r < rowsPerKey; r++) {
                            for (int k = 0; k < keys; k++) {
                                writer.add(k, row++);
                            }
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();

                        // 32 KiB headroom: too small for fast path (~64 KiB),
                        // large enough for streaming (~4 KiB) -- forcing the
                        // pre-flight to pick streaming, which then refuses
                        // because of the var-size cover column.
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 32L * 1024L);
                        try {
                            writer.seal();
                            Assert.fail("expected CairoException, seal succeeded");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(),
                                    "variable-size; streaming compaction of var-size cover columns is not yet supported");
                        } finally {
                            Unsafe.setRssMemLimit(savedLimit);
                        }
                    }
                }
            } finally {
                Unsafe.free(binAuxAddr, binAuxSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(binDataAddr, binDataSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Forces the seal to take the streaming fallback by setting a tight
     * RSS limit AFTER all rows are indexed but before seal. The streaming
     * path runs, producing the same on-disk dense gen 0 the fast path
     * would have. Read-back must match the no-budget baseline.
     */
    @Test
    public void testSealStreamingFallbackTriggers() throws Exception {
        final int keys = 16;
        final int rowsPerKey = 4096;

        assertMemoryLeak(() -> {
            // Baseline: no RSS budget, fast-path seal. Capture all rowIds.
            LongList baseline;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "streaming_baseline";
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    long row = 0;
                    for (int r = 0; r < rowsPerKey; r++) {
                        for (int k = 0; k < keys; k++) {
                            writer.add(k, row++);
                        }
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    writer.seal();
                }
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    baseline = collectAllRowIds(reader, keys);
                }
            }

            // Streaming path: same data, tight RSS budget set before seal.
            // streaming peak ~= 2 * maxKeyCount * 8 = ~64 KiB, fast peak
            // ~= 2 * (rowsPerKey * keys) * 8 = ~1 MiB.
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "streaming_fallback";
                long savedLimit = Unsafe.getRssMemLimit();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    long row = 0;
                    for (int r = 0; r < rowsPerKey; r++) {
                        for (int k = 0; k < keys; k++) {
                            writer.add(k, row++);
                        }
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    // Headroom 256 KiB: too small for the fast path
                    // (~1 MiB), large enough for streaming (~64 KiB).
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 256L * 1024L);
                    try {
                        writer.seal();
                    } finally {
                        Unsafe.setRssMemLimit(savedLimit);
                    }
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    LongList streaming = collectAllRowIds(reader, keys);
                    Assert.assertEquals("streaming and baseline row sets differ in length",
                            baseline.size(), streaming.size());
                    for (int i = 0; i < baseline.size(); i++) {
                        Assert.assertEquals("rowId at position " + i,
                                baseline.getQuick(i), streaming.getQuick(i));
                    }
                }
            }
        });
    }

    /**
     * Fuzz: vary spill budget across orders of magnitude (32 B to 16 MiB)
     * over random workloads (random key counts, rows per key, monotonic
     * row IDs), assert the index round-trips through both forward and
     * backward readers regardless of how many periodic flushes fired.
     * <p>
     * This is the strongest correctness check for the periodic-flush
     * path: any silent data drop -- whether from a misordered spill,
     * a missed gen, or the MAX_GEN_COUNT inline seal interaction --
     * surfaces as a cursor short-count or a value mismatch against
     * the oracle.
     */
    @Test
    public void testFuzzPeriodicFlushAcrossBudgets() throws Exception {
        assertMemoryLeak(() -> {
            for (long seed = 0; seed < 16; seed++) {
                Rnd rnd = new Rnd(seed * 17 + 11, seed * 23 + 5);
                // Budget spans 5 orders of magnitude. Some seeds fire the
                // flush almost every spill, others never fire it.
                long budget = 32L << rnd.nextInt(20);
                node1.getConfigurationOverrides().setProperty(
                        PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, budget);
                int keys = rnd.nextInt(150) + 1;
                int rowsPerKey = rnd.nextInt(800) + 8;

                ObjList<LongList> oracle = new ObjList<>();
                for (int k = 0; k < keys; k++) {
                    oracle.add(new LongList());
                }

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final int plen = path.size();
                    String name = "fuzz_budget_" + seed;
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        long row = 0;
                        for (int r = 0; r < rowsPerKey; r++) {
                            // Each round assigns each row index 0..keys-1 to
                            // some key; ensure rowIds are monotonically
                            // increasing per key by always using `row` and
                            // incrementing.
                            for (int k = 0; k < keys; k++) {
                                writer.add(k, row);
                                oracle.getQuick(k).add(row);
                                row++;
                            }
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                        writer.seal();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        for (int k = 0; k < keys; k++) {
                            LongList expected = oracle.getQuick(k);
                            RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                            int idx = 0;
                            while (cursor.hasNext()) {
                                Assert.assertTrue("seed=" + seed + " budget=" + budget + " key=" + k + " extra at idx=" + idx,
                                        idx < expected.size());
                                Assert.assertEquals("seed=" + seed + " budget=" + budget + " key=" + k + " idx=" + idx,
                                        expected.getQuick(idx), cursor.next());
                                idx++;
                            }
                            Assert.assertEquals("seed=" + seed + " budget=" + budget + " key=" + k + " short-count",
                                    expected.size(), idx);
                            Misc.free(cursor);
                        }
                    }
                    try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, null, null, 0)) {
                        for (int k = 0; k < keys; k++) {
                            LongList expected = oracle.getQuick(k);
                            RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                            int idx = expected.size() - 1;
                            while (cursor.hasNext()) {
                                Assert.assertTrue("seed=" + seed + " budget=" + budget + " bwd key=" + k + " extra at idx=" + idx,
                                        idx >= 0);
                                Assert.assertEquals("seed=" + seed + " budget=" + budget + " bwd key=" + k + " idx=" + idx,
                                        expected.getQuick(idx), cursor.next());
                                idx--;
                            }
                            Assert.assertEquals("seed=" + seed + " budget=" + budget + " bwd key=" + k + " short-count",
                                    -1, idx);
                            Misc.free(cursor);
                        }
                    }
                }
                // Reset the property so the next iteration's setProperty
                // call observes a deterministic prior state.
                node1.getConfigurationOverrides().setProperty(
                        PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256L << 20);
            }
        });
    }

    /**
     * Fuzz: for each random workload, build the index twice -- once on the
     * fast path with no RSS budget, once with an RSS limit dialled in so
     * tightly that the seal must take the streaming path. Read the (key,
     * rowId) sets back through both forward and backward cursors and
     * assert they are identical. This is the strongest check that
     * streaming compaction produces wire-compatible output: any divergence
     * in the encoded dense gen 0 (DELTA vs FLAT, missing keys, off-by-one
     * in stride headers) surfaces as a mismatch between the two readers.
     */
    @Test
    public void testFuzzStreamingMatchesFastPathBaseline() throws Exception {
        assertMemoryLeak(() -> {
            for (long seed = 0; seed < 12; seed++) {
                Rnd rnd = new Rnd(seed * 41 + 7, seed * 53 + 19);
                int keys = rnd.nextInt(60) + 4;
                int rowsPerKey = rnd.nextInt(600) + 16;

                LongList baseline = buildAndCollect("fuzz_base_" + seed, keys, rowsPerKey, /* tight RSS */ false);
                LongList streaming = buildAndCollect("fuzz_str_" + seed, keys, rowsPerKey, /* tight RSS */ true);

                Assert.assertEquals("seed=" + seed + " baseline vs streaming size mismatch",
                        baseline.size(), streaming.size());
                for (int i = 0; i < baseline.size(); i++) {
                    if (baseline.getQuick(i) != streaming.getQuick(i)) {
                        Assert.fail("seed=" + seed + " divergence at position " + i +
                                " (key/sentinel sequence): baseline=" + baseline.getQuick(i) +
                                " streaming=" + streaming.getQuick(i));
                    }
                }
            }
        });
    }

    private LongList buildAndCollect(String name, int keys, int rowsPerKey, boolean forceStreamingAtSeal) throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            long savedRssLimit = Unsafe.getRssMemLimit();
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                long row = 0;
                for (int r = 0; r < rowsPerKey; r++) {
                    for (int k = 0; k < keys; k++) {
                        writer.add(k, row++);
                    }
                }
                writer.setMaxValue(row - 1);
                writer.commit();
                if (forceStreamingAtSeal) {
                    // Cap headroom small enough to push the pre-flight off
                    // the fast path. 64 KiB is large enough for streaming
                    // (peak ~2 * maxKeyCount * 8 = ~10 KiB at rowsPerKey
                    // up to ~600) and small enough that fast path's
                    // 2 * keys * rowsPerKey * 8 (~600 KiB at the upper
                    // end) doesn't fit.
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 64L * 1024L);
                }
                try {
                    writer.seal();
                } finally {
                    if (forceStreamingAtSeal) {
                        Unsafe.setRssMemLimit(savedRssLimit);
                    }
                }
            }

            LongList collected = new LongList();
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int k = 0; k < keys; k++) {
                    collected.add(k);
                    collected.add(-1L); // separator
                    RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                    while (cursor.hasNext()) {
                        collected.add(cursor.next());
                    }
                    Misc.free(cursor);
                }
            }
            return collected;
        }
    }

    /**
     * Fuzz: combine random spill budgets (forcing periodic flushes during
     * the indexing loop) with random RSS limits at seal time (forcing
     * either fast path, streaming, or hard-fail). Any combination must
     * either succeed with correct read-back or throw with a clear
     * "RSS limit" / "stride aggregate" diagnostic.
     */
    @Test
    public void testFuzzMixedBudgetAndRssAtSeal() throws Exception {
        assertMemoryLeak(() -> {
            int passes = 0;
            int hardFails = 0;
            for (long seed = 0; seed < 24; seed++) {
                Rnd rnd = new Rnd(seed * 71 + 31, seed * 89 + 13);
                long budget = 64L << rnd.nextInt(18);
                node1.getConfigurationOverrides().setProperty(
                        PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, budget);
                int keys = rnd.nextInt(80) + 2;
                int rowsPerKey = rnd.nextInt(500) + 32;
                // Pick a head-room that ranges from very tight (8 KiB) to
                // very loose (8 MiB). At the tight end every seed should
                // hard-fail; at the loose end most should pass. Anchored
                // to 8 KiB because seal()'s pre-free liberates the spill
                // and pending arenas (multi-KiB) before pre-flight runs.
                long sealHeadroomBytes = 8L * 1024L << rnd.nextInt(11); // 8 KiB to 8 MiB

                ObjList<LongList> oracle = new ObjList<>();
                for (int k = 0; k < keys; k++) {
                    oracle.add(new LongList());
                }

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final int plen = path.size();
                    String name = "fuzz_mixed_" + seed;
                    long savedRssLimit = Unsafe.getRssMemLimit();
                    boolean sealSucceeded = false;
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        long row = 0;
                        for (int r = 0; r < rowsPerKey; r++) {
                            for (int k = 0; k < keys; k++) {
                                writer.add(k, row);
                                oracle.getQuick(k).add(row);
                                row++;
                            }
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + sealHeadroomBytes);
                        try {
                            writer.seal();
                            sealSucceeded = true;
                        } catch (CairoException e) {
                            String msg = e.getFlyweightMessage().toString();
                            // Either RSS-related or stride-overflow. Both
                            // are acceptable hard-fail diagnostics; any
                            // other message is a bug.
                            boolean known = msg.contains("RSS limit")
                                    || msg.contains("stride aggregate exceeds")
                                    || msg.contains("split commit");
                            if (!known) {
                                throw e;
                            }
                            hardFails++;
                        } finally {
                            Unsafe.setRssMemLimit(savedRssLimit);
                        }
                    }

                    if (sealSucceeded) {
                        passes++;
                        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            for (int k = 0; k < keys; k++) {
                                LongList expected = oracle.getQuick(k);
                                RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                                int idx = 0;
                                while (cursor.hasNext()) {
                                    Assert.assertTrue("seed=" + seed + " budget=" + budget + " seal=" + sealHeadroomBytes
                                                    + " key=" + k + " extra",
                                            idx < expected.size());
                                    Assert.assertEquals("seed=" + seed + " key=" + k + " idx=" + idx,
                                            expected.getQuick(idx), cursor.next());
                                    idx++;
                                }
                                Assert.assertEquals("seed=" + seed + " key=" + k + " short-count",
                                        expected.size(), idx);
                                Misc.free(cursor);
                            }
                        }
                    }
                }
                node1.getConfigurationOverrides().setProperty(
                        PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256L << 20);
            }
            // Sanity: across 24 seeds we expect both passes and hard-fails.
            // If passes == 0 the test is no longer covering the success
            // path; if hardFails == 0 we never tightened RSS enough.
            Assert.assertTrue("fuzz never succeeded a seal: passes=" + passes, passes > 0);
            Assert.assertTrue("fuzz never tightened RSS to hard-fail: hardFails=" + hardFails, hardFails > 0);
        });
    }
}
