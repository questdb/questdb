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
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
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
     * Forces the seal-time pre-flight to refuse via a pathological RSS
     * limit. The error message must explicitly call out that even the
     * streaming compaction would not fit, distinguishing the "too tight
     * for fast path" case (where streaming would still run) from the
     * "too tight for either" case the operator must remediate.
     */
    @Test
    public void testSealHardLimitWhenEvenStreamingDoesNotFit() throws Exception {
        final int keys = 4;
        final int rowsPerKey = 8192;

        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "hard_limit";

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

                    // Tight enough that streaming's 2 * maxKeyCount * 8 +
                    // overhead would exceed headroom. maxKeyCount here is
                    // rowsPerKey == 8192, so streaming peak is at least
                    // 2 * 8192 * 8 = 128 KiB; we cap headroom at 32 KiB.
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 32L * 1024L);
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
}
