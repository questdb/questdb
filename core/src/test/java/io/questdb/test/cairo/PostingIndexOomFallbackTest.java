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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.FSSTNative;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
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

    private static final Log LOG = LogFactory.getLog(PostingIndexOomFallbackTest.class);

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
     * that periodic flushes accumulate past MAX_GEN_COUNT. Each
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
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int iterations = 8 + rnd.nextInt(16); // 8..23 iterations per CI run
        assertMemoryLeak(() -> {
            for (int i = 0; i < iterations; i++) {
                // Budget spans 5 orders of magnitude. Some iterations fire
                // the flush almost every spill, others never fire it.
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
                    String name = "fuzz_budget_" + i;
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
                                Assert.assertTrue("iter=" + i + " budget=" + budget + " key=" + k + " extra at idx=" + idx,
                                        idx < expected.size());
                                Assert.assertEquals("iter=" + i + " budget=" + budget + " key=" + k + " idx=" + idx,
                                        expected.getQuick(idx), cursor.next());
                                idx++;
                            }
                            Assert.assertEquals("iter=" + i + " budget=" + budget + " key=" + k + " short-count",
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
                                Assert.assertTrue("iter=" + i + " budget=" + budget + " bwd key=" + k + " extra at idx=" + idx,
                                        idx >= 0);
                                Assert.assertEquals("iter=" + i + " budget=" + budget + " bwd key=" + k + " idx=" + idx,
                                        expected.getQuick(idx), cursor.next());
                                idx--;
                            }
                            Assert.assertEquals("iter=" + i + " budget=" + budget + " bwd key=" + k + " short-count",
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
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int iterations = 8 + rnd.nextInt(8); // 8..15 iterations per CI run
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < iterations; iter++) {
                int keys = rnd.nextInt(60) + 4;
                int rowsPerKey = rnd.nextInt(600) + 16;

                LongList baseline = buildAndCollect("fuzz_base_" + iter, keys, rowsPerKey, /* tight RSS */ false);
                LongList streaming = buildAndCollect("fuzz_str_" + iter, keys, rowsPerKey, /* tight RSS */ true);

                Assert.assertEquals("iter=" + iter + " baseline vs streaming size mismatch",
                        baseline.size(), streaming.size());
                for (int i = 0; i < baseline.size(); i++) {
                    if (baseline.getQuick(i) != streaming.getQuick(i)) {
                        Assert.fail("iter=" + iter + " divergence at position " + i +
                                " (key/sentinel sequence): baseline=" + baseline.getQuick(i) +
                                " streaming=" + streaming.getQuick(i));
                    }
                }
            }
        });
    }

    private LongList buildAndCollect(String name, int keys, int rowsPerKey, boolean forceStreamingAtSeal) {
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
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // Force at least one tight-RSS iteration and one loose-RSS one
        // so the passes/hardFails sanity assertions stay meaningful
        // regardless of which seeds the random clock picks.
        final int iterations = 16 + rnd.nextInt(16); // 16..31 iterations per CI run
        final int[] passes = {0};
        final int[] hardFails = {0};
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < iterations; iter++) {
                long budget = 64L << rnd.nextInt(18);
                node1.getConfigurationOverrides().setProperty(
                        PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, budget);
                // iter 0 pins a shape guaranteed to hard-fail (1 hot key
                // with 100K rows -> streaming peak >> 8 KiB headroom even
                // after seal's pre-free), iter 1 pins one guaranteed to
                // pass (4 keys * 32 rows fits trivially in 8 MiB headroom).
                // The remaining iterations use random workloads + random
                // RSS limits to fuzz the boundary.
                int keys;
                int rowsPerKey;
                long sealHeadroomBytes;
                if (iter == 0) {
                    keys = 1;
                    rowsPerKey = 100_000;
                    sealHeadroomBytes = 8L * 1024L; // tight -> hard-fail
                } else if (iter == 1) {
                    keys = 4;
                    rowsPerKey = 32;
                    sealHeadroomBytes = 8L * 1024L * 1024L; // loose -> pass
                } else {
                    keys = rnd.nextInt(80) + 2;
                    rowsPerKey = rnd.nextInt(500) + 32;
                    sealHeadroomBytes = 8L * 1024L << rnd.nextInt(11);
                }

                ObjList<LongList> oracle = new ObjList<>();
                for (int k = 0; k < keys; k++) {
                    oracle.add(new LongList());
                }

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final int plen = path.size();
                    String name = "fuzz_mixed_" + iter;
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
                            hardFails[0]++;
                        } finally {
                            Unsafe.setRssMemLimit(savedRssLimit);
                        }
                    }

                    if (sealSucceeded) {
                        passes[0]++;
                        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            for (int k = 0; k < keys; k++) {
                                LongList expected = oracle.getQuick(k);
                                RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                                int idx = 0;
                                while (cursor.hasNext()) {
                                    Assert.assertTrue("iter=" + iter + " budget=" + budget + " seal=" + sealHeadroomBytes
                                                    + " key=" + k + " extra",
                                            idx < expected.size());
                                    Assert.assertEquals("iter=" + iter + " key=" + k + " idx=" + idx,
                                            expected.getQuick(idx), cursor.next());
                                    idx++;
                                }
                                Assert.assertEquals("iter=" + iter + " key=" + k + " short-count",
                                        expected.size(), idx);
                                Misc.free(cursor);
                            }
                        }
                    }
                }
                node1.getConfigurationOverrides().setProperty(
                        PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256L << 20);
            }
            // Sanity: iter 0 forces tight RSS, iter 1 forces loose, so we
            // are guaranteed at least one of each.
            Assert.assertTrue("fuzz never succeeded a seal: passes=" + passes[0], passes[0] > 0);
            Assert.assertTrue("fuzz never tightened RSS to hard-fail: hardFails=" + hardFails[0], hardFails[0] > 0);
        });
    }

    /**
     * Fuzz: streaming compaction with a fixed-size DOUBLE INCLUDE
     * column. Drives random workloads, builds the index twice (fast
     * path baseline + streaming-forced), reads back covered values
     * via the covering cursor, and asserts byte-identical results.
     * <p>
     * Exercises {@link io.questdb.cairo.idx.PostingIndexWriter}'s
     * streaming sidecar writer (writeSidecarsPerColumnStreaming +
     * writeSidecarFixedStreamingForColumn) -- the path that decodes
     * each key from the freshly-sealed dense gen 0 via
     * decodeDenseGenSingleKey, then ALP-compresses one key at a
     * time. Any divergence from the per-stride fast-path output
     * surfaces here as a covered-value mismatch.
     */
    @Test
    public void testFuzzStreamingFixedIncludeMatchesBaseline() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int iterations = 4 + rnd.nextInt(8); // 4..11 iterations per CI run
        final ColumnVersionReader emptyCvr = new ColumnVersionReader();
        try {
            assertMemoryLeak(() -> {
                for (int iter = 0; iter < iterations; iter++) {
                    int keys = rnd.nextInt(40) + 4;
                    int rowsPerKey = rnd.nextInt(400) + 32;

                    // Backing DOUBLE column: one value per total row.
                    int totalRows = keys * rowsPerKey;
                    long covAddr = Unsafe.malloc((long) totalRows * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (int i = 0; i < totalRows; i++) {
                            Unsafe.putDouble(covAddr + (long) i * Double.BYTES, 0.5 + i);
                        }

                        double[] baseline = collectCoveredDoubles(
                                "fuzz_cov_base_" + iter, keys, rowsPerKey, covAddr, /* tight RSS */ false, emptyCvr);
                        double[] streaming = collectCoveredDoubles(
                                "fuzz_cov_str_" + iter, keys, rowsPerKey, covAddr, /* tight RSS */ true, emptyCvr);

                        Assert.assertEquals("iter=" + iter + " covered length mismatch",
                                baseline.length, streaming.length);
                        for (int i = 0; i < baseline.length; i++) {
                            Assert.assertEquals("iter=" + iter + " covered value at i=" + i,
                                    baseline[i], streaming[i], 0.0);
                        }
                    } finally {
                        Unsafe.free(covAddr, (long) totalRows * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            });
        } finally {
            Misc.free(emptyCvr);
        }
    }

    private double[] collectCoveredDoubles(
            String name, int keys, int rowsPerKey, long covAddr,
            boolean forceStreamingAtSeal, ColumnVersionReader emptyCvr) {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            final int plen = path.size();
            long savedRssLimit = Unsafe.getRssMemLimit();
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                // shift = 3 -> valueSize = 8 (DOUBLE). writerIndex = 2;
                // colTop = 0; coverCount = 1; timestampColumnIndex = -1.
                writer.configureCovering(
                        new long[]{covAddr},
                        new long[]{0L},
                        new int[]{Long.numberOfTrailingZeros(Double.BYTES)},
                        new int[]{2},
                        new int[]{ColumnType.DOUBLE},
                        1
                );
                long row = 0;
                for (int r = 0; r < rowsPerKey; r++) {
                    for (int k = 0; k < keys; k++) {
                        writer.add(k, row++);
                    }
                }
                writer.setMaxValue(row - 1);
                writer.commit();
                if (forceStreamingAtSeal) {
                    // 256 KiB headroom: too small for fast-path peak
                    // (~2 * keys * rowsPerKey * 8 bytes = up to ~250 KiB
                    // for the bigger fuzz seeds, plus encodeCtx and
                    // sidecarBuf), large enough for streaming peak
                    // (~maxKeyCount * (8+8+9+9) = ~14 KiB at the upper
                    // end). seal() pre-frees spill+pending before the
                    // pre-flight runs, so effective headroom at
                    // pre-flight is meaningfully larger -- 256 KiB is
                    // calibrated to land between fast-path and
                    // streaming peaks even after the pre-free bonus.
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 256L * 1024L);
                }
                try {
                    writer.seal();
                } finally {
                    if (forceStreamingAtSeal) {
                        Unsafe.setRssMemLimit(savedRssLimit);
                    }
                }
            }

            RecordMetadata meta = coveringMetadata();
            double[] collected = new double[keys * rowsPerKey];
            int idx = 0;
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                    meta, emptyCvr, 0)) {
                for (int k = 0; k < keys; k++) {
                    try (RowCursor c = reader.getCursor(k, 0, Long.MAX_VALUE, new int[]{0})) {
                        Assert.assertTrue("expected CoveringRowCursor for " + name + " key=" + k,
                                c instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) c;
                        while (cc.hasNext()) {
                            cc.next();
                            collected[idx++] = cc.getCoveredDouble(0);
                        }
                    }
                }
            }
            Assert.assertEquals("collected length mismatch for " + name, collected.length, idx);
            return collected;
        }
    }

    private static RecordMetadata coveringBinaryMetadata() {
        GenericRecordMetadata m = new GenericRecordMetadata();
        for (int i = 0; i <= 2; i++) {
            int type = (i == 2) ? ColumnType.BINARY : ColumnType.LONG;
            m.add(new TableColumnMetadata("c" + i, type, IndexType.NONE, 0, false, null, i, false));
        }
        return m;
    }

    private static RecordMetadata coveringMetadata() {
        GenericRecordMetadata m = new GenericRecordMetadata();
        for (int i = 0; i <= 2; i++) {
            int type = (i == 2) ? ColumnType.DOUBLE : ColumnType.LONG;
            m.add(new TableColumnMetadata("c" + i, type, IndexType.NONE, 0, false, null, i, false));
        }
        return m;
    }

    /**
     * Tight-RSS seal with a var-size (BINARY) INCLUDE column. Streaming
     * FSST trains a symbol table from a stride sample (~64 KiB) and then
     * encodes the full stride in {@code FSST_BATCH_SIZE}-sized batches,
     * staging the compressed bytes inside the sidecar mmap. Anonymous-heap
     * scratch stays in the low MiBs regardless of stride raw bytes, so the
     * seal completes under an RSS budget that the old single-shot
     * trainAndCompressBlock allocation (cmpCap = 2 * rawDataLen) would
     * have blown.
     * <p>
     * The test verifies (a) the seal completes; (b) the cover cursor reads
     * every BINARY value back unchanged; (c) for the deliberately
     * redundant payload pattern, streaming actually compressed -- the
     * .pc* block ends up shorter than the uncompressed worst case.
     */
    @Test
    public void testSealVarIncludeFsstStreamingUnderRssPressure() throws Exception {
        final int keys = 16;
        final int rowsPerKey = 256;
        final int totalRows = keys * rowsPerKey;
        // Each row writes (8 + payloadBytes) into the sidecar. 256 bytes
        // per payload puts raw stride data over 1 MiB. The old single-shot
        // trainAndCompressBlock wanted cmpCap = 2 * rawDataLen + batch +
        // 2 * offsets arrays = ~2.5 MiB of anon-heap in one realloc; the
        // streaming path's anon-heap stays bounded by FSST_BATCH_SIZE
        // (sample, batch in/out arrays, ~1 MiB batch out buffer), so the
        // 8 MiB RSS headroom below is comfortable for streaming but would
        // not have fit the old single-shot allocation.
        final int payloadBytes = 256;
        final int binStride = Long.BYTES + payloadBytes;
        final long binDataSize = (long) totalRows * binStride;
        final long binAuxSize = (long) (totalRows + 1) * Long.BYTES;

        // Highly redundant payload: a short repeated phrase. FSST's symbol
        // table picks up the n-grams of the phrase, so 256-byte payloads
        // compress down to a handful of code bytes -- the compressed block
        // ends up roughly an order of magnitude smaller than the raw,
        // which is what we assert below.
        final byte[] phrase = "the_quick_brown_fox_jumps_over_lazy_dog_".getBytes();

        assertMemoryLeak(() -> {
            long binDataAddr = Unsafe.malloc(binDataSize, MemoryTag.NATIVE_DEFAULT);
            long binAuxAddr = Unsafe.malloc(binAuxSize, MemoryTag.NATIVE_DEFAULT);
            try {
                long off = 0;
                for (int i = 0; i < totalRows; i++) {
                    Unsafe.putLong(binAuxAddr + (long) i * Long.BYTES, off);
                    Unsafe.putLong(binDataAddr + off, payloadBytes);
                    for (int b = 0; b < payloadBytes; b++) {
                        Unsafe.putByte(binDataAddr + off + Long.BYTES + b,
                                phrase[b % phrase.length]);
                    }
                    off += binStride;
                }
                Unsafe.putLong(binAuxAddr + (long) totalRows * Long.BYTES, off);

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final int plen = path.size();
                    final String name = "var_cover_fsst_streaming";
                    final long savedLimit = Unsafe.getRssMemLimit();
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{binDataAddr},
                                new long[]{binAuxAddr},
                                new long[]{0L},     // colTops
                                new int[]{-1},      // shifts: var-size
                                new int[]{2},       // writer indices (cover col is at meta index 2)
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

                        // 8 MiB headroom: enough for the fast-path symbol
                        // encoding scratch (~70 KiB) plus the streaming
                        // FSST scratch (FSST_BATCH_OUT_CAP_MIN=1 MiB +
                        // sample/batch lens/ptrs + ~900 KiB JNI encoder),
                        // but FAR smaller than the old one-shot
                        // trainAndCompressBlock allocation
                        // (cmpCap = 2 * rawDataLen ~= 2.2 MiB at this
                        // workload, scaling unbounded with rawDataLen).
                        // The point: even a small headroom is enough
                        // because streaming bounds anon-heap to
                        // ~few-MiB regardless of stride raw bytes.
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 8L * 1024L * 1024L);
                        try {
                            writer.seal();
                        } finally {
                            Unsafe.setRssMemLimit(savedLimit);
                        }
                    }

                    // Spot-check the on-disk format: streaming FSST should
                    // have collapsed the redundant ASCII payload well below
                    // the raw block size (4096 values * 264 bytes + offsets
                    // = ~1.05 MiB). Anything close to 1 MiB means the
                    // streaming path silently fell back to uncompressed.
                    long pc0Size = findPc0Size(path.trimTo(plen), name);
                    Assert.assertTrue("expected streaming FSST compression to shrink the .pc0 well below the raw 1 MiB; latest .pc0 size=" + pc0Size,
                            pc0Size > 0 && pc0Size < 512L * 1024L);
                    // Block header must carry FSST_BLOCK_FLAG (compression won).
                    int header = readFirstStrideBlockHeader(path.trimTo(plen), name);
                    Assert.assertNotEquals("expected FSST_BLOCK_FLAG set in stride block; header=" + Integer.toHexString(header),
                            0, header & FSSTNative.FSST_BLOCK_FLAG);

                    // Read back the BINARY cover values via the covering
                    // cursor and verify each row matches what we wrote. If
                    // the on-disk block format is wrong the reader will
                    // surface a mismatch here.
                    RecordMetadata meta = coveringBinaryMetadata();
                    try (ColumnVersionReader emptyCvr = new ColumnVersionReader();
                         PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                 configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                                 meta, emptyCvr, 0)) {
                        for (int k = 0; k < keys; k++) {
                            try (RowCursor c = reader.getCursor(k, 0, Long.MAX_VALUE, new int[]{0})) {
                                Assert.assertTrue("expected CoveringRowCursor for key=" + k,
                                        c instanceof CoveringRowCursor);
                                CoveringRowCursor cc = (CoveringRowCursor) c;
                                int seen = 0;
                                while (cc.hasNext()) {
                                    long rowId = cc.next();
                                    Assert.assertEquals("BINARY length mismatch [key=" + k + ", rowId=" + rowId + "]",
                                            payloadBytes, cc.getCoveredBinLen(0));
                                    BinarySequence bin = cc.getCoveredBin(0);
                                    Assert.assertNotNull("cover BINARY should not be null [key=" + k + ", rowId=" + rowId + "]", bin);
                                    for (int b = 0; b < payloadBytes; b++) {
                                        byte expected = phrase[b % phrase.length];
                                        Assert.assertEquals(
                                                "BINARY byte mismatch [key=" + k + ", rowId=" + rowId + ", b=" + b + "]",
                                                expected, bin.byteAt(b));
                                    }
                                    seen++;
                                }
                                Assert.assertEquals("row count for key=" + k, rowsPerKey, seen);
                            }
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
     * Companion to {@link #testSealVarIncludeFsstStreamingUnderRssPressure}
     * that exercises the "compressed loses" branch of
     * tryFsstStreamingCompress: incompressible (random) payloads. The
     * streaming loop runs end-to-end, but compressedBlockSize ends up
     * &gt;= uncompressedBlockSize, so the writer rewinds to uncompressedEnd
     * and leaves the uncompressed block on disk. We verify (a) the seal
     * still completes; (b) the values read back unchanged; (c) the .pc0
     * file size is consistent with the uncompressed format -- i.e. the
     * rewind happened correctly and we did not double-write or corrupt
     * the offsets region.
     */
    @Test
    public void testSealVarIncludeFsstStreamingKeepsUncompressedWhenCompressionLoses() throws Exception {
        final int keys = 16;
        final int rowsPerKey = 256;
        final int totalRows = keys * rowsPerKey;
        final int payloadBytes = 256;
        final int binStride = Long.BYTES + payloadBytes;
        final long binDataSize = (long) totalRows * binStride;
        final long binAuxSize = (long) (totalRows + 1) * Long.BYTES;

        // Per-byte random payload. FSST trains a symbol table from a
        // sample, but with uniform random bytes the table cannot capture
        // useful n-grams; the encoded output is ~the same size as input
        // plus per-string overhead, so compressedBlockSize >= raw.
        final Rnd rnd = new Rnd(0xC0FFEEL, 0xDECAFBADL);
        final byte[][] payloads = new byte[totalRows][payloadBytes];
        for (int i = 0; i < totalRows; i++) {
            for (int b = 0; b < payloadBytes; b++) {
                payloads[i][b] = (byte) rnd.nextInt();
            }
        }

        assertMemoryLeak(() -> {
            long binDataAddr = Unsafe.malloc(binDataSize, MemoryTag.NATIVE_DEFAULT);
            long binAuxAddr = Unsafe.malloc(binAuxSize, MemoryTag.NATIVE_DEFAULT);
            try {
                long off = 0;
                for (int i = 0; i < totalRows; i++) {
                    Unsafe.putLong(binAuxAddr + (long) i * Long.BYTES, off);
                    Unsafe.putLong(binDataAddr + off, payloadBytes);
                    for (int b = 0; b < payloadBytes; b++) {
                        Unsafe.putByte(binDataAddr + off + Long.BYTES + b, payloads[i][b]);
                    }
                    off += binStride;
                }
                Unsafe.putLong(binAuxAddr + (long) totalRows * Long.BYTES, off);

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final int plen = path.size();
                    final String name = "var_cover_fsst_streaming_loses";
                    final long savedLimit = Unsafe.getRssMemLimit();
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{binDataAddr},
                                new long[]{binAuxAddr},
                                new long[]{0L},
                                new int[]{-1},
                                new int[]{2},
                                new int[]{ColumnType.BINARY},
                                1,
                                -1
                        );
                        long row = 0;
                        for (int r = 0; r < rowsPerKey; r++) {
                            for (int k = 0; k < keys; k++) {
                                writer.add(k, row++);
                            }
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 8L * 1024L * 1024L);
                        try {
                            writer.seal();
                        } finally {
                            Unsafe.setRssMemLimit(savedLimit);
                        }
                    }

                    // Compression lost: the .pc0 must be at least the
                    // uncompressed raw-block size. Anything smaller would
                    // mean we kept the compressed bytes; anything much
                    // larger would mean the staging area wasn't truncated.
                    long pc0Size = findPc0Size(path.trimTo(plen), name);
                    long rawBlockLowerBound = 4L + 4L * (totalRows + 1) + (long) totalRows * binStride;
                    Assert.assertTrue("expected uncompressed block to remain; pc0Size=" + pc0Size
                                    + " rawBlockLowerBound=" + rawBlockLowerBound,
                            pc0Size >= rawBlockLowerBound);
                    // Block header must NOT carry FSST_BLOCK_FLAG (compression lost on random bytes).
                    int header = readFirstStrideBlockHeader(path.trimTo(plen), name);
                    Assert.assertEquals("expected FSST_BLOCK_FLAG unset on the stride block; header=" + Integer.toHexString(header),
                            0, header & FSSTNative.FSST_BLOCK_FLAG);

                    // Read back via the cover cursor. If the rewind +
                    // staging truncate corrupted the offsets region, the
                    // cursor would surface random bytes here.
                    RecordMetadata meta = coveringBinaryMetadata();
                    try (ColumnVersionReader emptyCvr = new ColumnVersionReader();
                         PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                 configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                                 meta, emptyCvr, 0)) {
                        for (int k = 0; k < keys; k++) {
                            try (RowCursor c = reader.getCursor(k, 0, Long.MAX_VALUE, new int[]{0})) {
                                Assert.assertTrue("expected CoveringRowCursor for key=" + k,
                                        c instanceof CoveringRowCursor);
                                CoveringRowCursor cc = (CoveringRowCursor) c;
                                int seen = 0;
                                while (cc.hasNext()) {
                                    long rowId = cc.next();
                                    Assert.assertEquals("BINARY length mismatch [key=" + k + ", rowId=" + rowId + "]",
                                            payloadBytes, cc.getCoveredBinLen(0));
                                    BinarySequence bin = cc.getCoveredBin(0);
                                    Assert.assertNotNull("cover BINARY should not be null [key=" + k + ", rowId=" + rowId + "]", bin);
                                    for (int b = 0; b < payloadBytes; b++) {
                                        Assert.assertEquals(
                                                "BINARY byte mismatch [key=" + k + ", rowId=" + rowId + ", b=" + b + "]",
                                                payloads[(int) rowId][b], bin.byteAt(b));
                                    }
                                    seen++;
                                }
                                Assert.assertEquals("row count for key=" + k, rowsPerKey, seen);
                            }
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
     * Reader-side companion: forces the cover cursor to walk a large
     * FSST-compressed block under a tight RSS budget. The pre-streaming
     * decompressor sized its dst buffer to {@code 4 * totalCompressed}
     * for the whole block, so a sealed block whose compressed payload
     * exceeds the RSS budget could not be read at all. The chunked
     * decompressor sizes scratch to one FSST_DECODE_CHUNK_SIZE-wide
     * window, so the same read fits in a fraction of the headroom.
     * <p>
     * We assert (a) every value reads back byte-identical; (b) the
     * .pc0 stride block is FSST-compressed (sanity-check the writer
     * actually compressed); (c) the seal+read together stayed under
     * the configured RSS limit.
     */
    @Test
    public void testFsstReaderStreamsLargeCompressedBlock() throws Exception {
        final int keys = 32;
        final int rowsPerKey = 1024; // 32_768 values total -- 128 chunks at CHUNK_SIZE=256
        final int totalRows = keys * rowsPerKey;
        final int payloadBytes = 256;
        final int binStride = Long.BYTES + payloadBytes;
        final long binDataSize = (long) totalRows * binStride;
        final long binAuxSize = (long) (totalRows + 1) * Long.BYTES;

        // Same redundant phrase as the wins test -- ensures FSST achieves
        // strong compression so the block uses the FSST-compressed branch
        // on disk; the reader test is most meaningful against an
        // actually-compressed block.
        final byte[] phrase = "the_quick_brown_fox_jumps_over_lazy_dog_".getBytes();

        assertMemoryLeak(() -> {
            long binDataAddr = Unsafe.malloc(binDataSize, MemoryTag.NATIVE_DEFAULT);
            long binAuxAddr = Unsafe.malloc(binAuxSize, MemoryTag.NATIVE_DEFAULT);
            try {
                long off = 0;
                for (int i = 0; i < totalRows; i++) {
                    Unsafe.putLong(binAuxAddr + (long) i * Long.BYTES, off);
                    Unsafe.putLong(binDataAddr + off, payloadBytes);
                    for (int b = 0; b < payloadBytes; b++) {
                        Unsafe.putByte(binDataAddr + off + Long.BYTES + b,
                                phrase[(b + i) % phrase.length]);
                    }
                    off += binStride;
                }
                Unsafe.putLong(binAuxAddr + (long) totalRows * Long.BYTES, off);

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final int plen = path.size();
                    final String name = "var_cover_fsst_reader_streaming";
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{binDataAddr},
                                new long[]{binAuxAddr},
                                new long[]{0L},
                                new int[]{-1},
                                new int[]{2},
                                new int[]{ColumnType.BINARY},
                                1,
                                -1
                        );
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

                    // Sanity: the block must actually be FSST-compressed,
                    // otherwise the reader streaming path isn't exercised.
                    int header = readFirstStrideBlockHeader(path.trimTo(plen), name);
                    Assert.assertNotEquals("expected FSST_BLOCK_FLAG set; header=" + Integer.toHexString(header),
                            0, header & FSSTNative.FSST_BLOCK_FLAG);

                    // Walk every value through the cover cursor under an
                    // RSS budget that's far smaller than the old whole-block
                    // dstCap (4 * totalCompressed) would have wanted. For
                    // 32_768 values * 256 byte payloads the old path
                    // wanted ~10-40 MiB of dst scratch; streaming sizes
                    // to ~tens of KiB per chunk.
                    RecordMetadata meta = coveringBinaryMetadata();
                    final long savedLimit = Unsafe.getRssMemLimit();
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 2L * 1024L * 1024L);
                    try (ColumnVersionReader emptyCvr = new ColumnVersionReader();
                         PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                 configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                                 meta, emptyCvr, 0)) {
                        for (int k = 0; k < keys; k++) {
                            try (RowCursor c = reader.getCursor(k, 0, Long.MAX_VALUE, new int[]{0})) {
                                CoveringRowCursor cc = (CoveringRowCursor) c;
                                int seen = 0;
                                while (cc.hasNext()) {
                                    long rowId = cc.next();
                                    Assert.assertEquals(payloadBytes, cc.getCoveredBinLen(0));
                                    BinarySequence bin = cc.getCoveredBin(0);
                                    Assert.assertNotNull(bin);
                                    for (int b = 0; b < payloadBytes; b++) {
                                        byte expected = phrase[(b + (int) rowId) % phrase.length];
                                        Assert.assertEquals(
                                                "BINARY byte mismatch [key=" + k + ", rowId=" + rowId + ", b=" + b + "]",
                                                expected, bin.byteAt(b));
                                    }
                                    seen++;
                                }
                                Assert.assertEquals("row count for key=" + k, rowsPerKey, seen);
                            }
                        }
                    } finally {
                        Unsafe.setRssMemLimit(savedLimit);
                    }
                }
            } finally {
                Unsafe.free(binAuxAddr, binAuxSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(binDataAddr, binDataSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static java.io.File findLatestPc0(Path dir, String name) {
        // The writer produces one .pc0.0.<sealTxn> per published gen; the
        // pre-seal commit lands at sealTxn=0 with the uncompressed block,
        // and the seal-time file lands at the next sealTxn (=1 in this
        // test) with the FSST-compressed block. We pick the latest because
        // that's the one a fresh reader would open.
        final java.io.File d = new java.io.File(dir.toString());
        final java.io.File[] files = d.listFiles();
        if (files == null) return null;
        String prefix = name + ".pc0.";
        long bestSealTxn = -1L;
        java.io.File best = null;
        for (java.io.File f : files) {
            String n = f.getName();
            if (!n.startsWith(prefix)) continue;
            int lastDot = n.lastIndexOf('.');
            if (lastDot <= prefix.length() - 1) continue;
            try {
                long sealTxn = Long.parseLong(n.substring(lastDot + 1));
                if (sealTxn > bestSealTxn) {
                    bestSealTxn = sealTxn;
                    best = f;
                }
            } catch (NumberFormatException ignore) {
            }
        }
        return best;
    }

    private static long findPc0Size(Path dir, String name) {
        java.io.File f = findLatestPc0(dir, name);
        return f == null ? -1L : f.length();
    }

    /**
     * Reads the first 4-byte word of the first stride block in the .pc0
     * dense gen 0. Layout per writeSidecarForColumn:
     * <pre>
     *   [0, PC_HEADER_SIZE=1024): gen pointer table
     *   [1024, 1024 + (strideCount + 1) * 8): stride index (sentinel-terminated)
     *   [1024 + (strideCount + 1) * 8, ...): stride blocks
     * </pre>
     * The single-stride tests below have strideCount = 1, so the first
     * block starts at 1024 + 16 = 1040. Returns the {@code count | flags}
     * word so callers can inspect FSST_BLOCK_FLAG / LONG_OFFSETS_FLAG.
     */
    private static int readFirstStrideBlockHeader(Path dir, String name) throws java.io.IOException {
        java.io.File f = findLatestPc0(dir, name);
        Assert.assertNotNull("no .pc0 file found for " + name, f);
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, "r")) {
            raf.seek(1040L);
            int b0 = raf.read();
            int b1 = raf.read();
            int b2 = raf.read();
            int b3 = raf.read();
            return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
        }
    }
}
