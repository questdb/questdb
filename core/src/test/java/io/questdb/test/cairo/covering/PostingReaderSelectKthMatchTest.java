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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.idx.PostingGenLookup;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.lang.reflect.Field;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exact-equivalence (TDD oracle) tests for the O(genCount) covered-frame metadata
 * primitives on {@link io.questdb.cairo.idx.AbstractPostingIndexReader}:
 * {@code selectKthMatch}, {@code countMatchesClamped}, and {@code populateCacheForKey}.
 * <p>
 * The ground truth for {@code selectKthMatch} / {@code countMatchesClamped} is a full drain
 * of a real forward cursor over the same clamped range: the cursor's {@code next()} at
 * iteration position k must equal {@code selectKthMatch(key, lo, clamp, k)}, and the drained
 * row count must equal {@code countMatchesClamped(key, lo, clamp)} (both bail to the
 * {@code LONG_NULL} sentinel on the same genuinely-MIXED gens). The ground truth for
 * {@code populateCacheForKey} is the genLookup cache state a traverse-warmed reader holds for
 * the key — the metadata-only warm must produce a byte-identical cache.
 * <p>
 * Layouts covered: single-gen dense FLAT (many keys, few rows/key), single-gen EF (one key,
 * many gapped rows), multi-gen with sparse gens, a null-prefix (columnTop) case, and a
 * dirty-rows-past-entryMaxValue case (documented sentinel / fallback).
 */
public class PostingReaderSelectKthMatchTest extends AbstractCairoTest {

    private static final long CACHE_NOT_PRESENT = -1L;

    /**
     * Dirty rows past the chain entry's MAX_VALUE. Key 0 is encoded for rowids
     * 0,5,...,95 in a single dense gen, but {@code setMaxValue(49)} lowers the
     * entry's coverage to 49, marking 50..95 dirty. A cursor clamps to 49 and
     * emits 0,5,...,45. selectKthMatch over the lowered clamp (49) sees the gen's
     * real max (95) straddle the clamp — a MIXED gen — and returns the documented
     * sentinel for every k (never a wrong row id), so the caller falls back. With a
     * non-trimming clamp (95), the same select arithmetic returns the exact values.
     */
    @Test
    public void testDirtyRowsPastEntryMaxValueReturnsSentinel() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_dirty";
                final int plen = path.size();

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 0; rowId < 100; rowId++) {
                        writer.add((int) (rowId % 5), rowId);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                    // Lower MAX_VALUE in place: rows 50..99 become dirty.
                    writer.setMaxValue(49);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();

                    // Ground truth from the clamped cursor: 0,5,...,45.
                    LongList gt = drain(reader, 0, 0, Long.MAX_VALUE);
                    assertEquals(10, gt.size());
                    long clamp = entryMaxValue(reader);
                    assertEquals(49L, clamp);

                    // MIXED gen (real max 95 > clamp 49): sentinel for every k. columnTop is 0
                    // here, so the null bound is inert; pass the unclamped caller max for form.
                    for (int k = 0; k < gt.size(); k++) {
                        assertEquals("dirty/MIXED gen must yield the fallback sentinel at k=" + k,
                                Numbers.LONG_NULL, reader.selectKthMatch(0, 0, Long.MAX_VALUE, clamp, k));
                    }
                    // And past the (true) end too.
                    assertEquals(Numbers.LONG_NULL, reader.selectKthMatch(0, 0, Long.MAX_VALUE, clamp, 10));
                    // countMatchesClamped must agree on the MIXED classification: a
                    // gen the slack bound straddles is genuinely clipped here (real max
                    // 95 > clamp 49), so the exact coverage check bails to the sentinel.
                    assertEquals("dirty/MIXED gen must make countMatchesClamped sentinel",
                            Numbers.LONG_NULL, reader.countMatchesClamped(0, 0, Long.MAX_VALUE, clamp));

                    // Non-trimming clamp (>= gen max): the select arithmetic is exact for
                    // all 20 encoded rows 0,5,...,95. This isolates the select logic from
                    // the dirty-clamp fallback.
                    for (int k = 0; k < 20; k++) {
                        assertEquals("untrimmed select must be exact at k=" + k,
                                (long) (k * 5), reader.selectKthMatch(0, 0, 95, 95, k));
                    }
                    assertEquals(Numbers.LONG_NULL, reader.selectKthMatch(0, 0, 95, 95, 20));
                    // Clean (untrimmed) clamp: exact full count, no sentinel.
                    assertEquals("untrimmed countMatchesClamped must be the exact count",
                            20L, reader.countMatchesClamped(0, 0, 95, 95));
                }
            }
        });
    }

    /**
     * Null prefix where {@code entryMaxValue < columnTop} and the queried frame extends past
     * {@code entryMaxValue}: the cheap-path null bound MUST come from the UNCLAMPED caller max,
     * not the entryMaxValue-folded clamp. Key 0 has clean postings 0..10 (all within the lowered
     * clamp); key 1 has dirty rows 11..49 that {@code setMaxValue(10)} marks past the entry's
     * coverage, so {@code entryMaxValue == 10 < columnTop == 20}. Over a frame rowHi=50
     * (callerHiInclusive=49, clampedMax=min(49,10)=10), the real NullCursor emits
     * {@code nullCount = min(columnTop=20, callerMax+1=50) = 20} null rows 0..19, then key 0's 11
     * clean index postings 0..10 — 31 rows total. The cheap primitives must reproduce that EXACTLY:
     * the null prefix is bounded by the unclamped caller max (50 -> 20 nulls), only the gen walk by
     * the clamp (10). Before the fix the null prefix was bounded by clampedMax (=> only 11 nulls),
     * dropping rows 11..19 (under-count 22 vs 31) and returning WRONG row ids for k in the dropped
     * null band.
     */
    @Test
    public void testNullPrefixUnclampedWhenEntryMaxBelowColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_nullprefix_dirty";
                final int plen = path.size();
                final long columnTop = 20;
                final long callerHiInclusive = 49; // frame rowHi = 50
                final long loweredMax = 10;        // entryMaxValue after the dirty shrink

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Key 0: clean postings 0..10, entirely within the lowered clamp.
                    for (long row = 0; row <= loweredMax; row++) {
                        writer.add(0, row);
                    }
                    // Key 1: dirty rows 11..49, evicted by lowering MAX_VALUE below them.
                    for (long row = loweredMax + 1; row <= callerHiInclusive; row++) {
                        writer.add(1, row);
                    }
                    writer.setMaxValue(callerHiInclusive);
                    writer.commit();
                    // Lower MAX_VALUE in place: rows 11..49 become dirty; key 0's gen stays clean.
                    writer.setMaxValue(loweredMax);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, columnTop)) {
                    reader.reloadConditionally();
                    assertEquals("dirty shrink must leave entryMaxValue below columnTop",
                            loweredMax, entryMaxValue(reader));

                    // Ground truth: drain the REAL forward cursor over [0, 49]. NullCursor emits
                    // min(columnTop=20, 50)=20 nulls (0..19) then key 0's clean index rows 0..10.
                    LongList gt = drain(reader, 0, 0, callerHiInclusive);
                    assertEquals("cursor must emit 20 nulls + 11 index rows", 31, gt.size());
                    for (int k = 0; k < 20; k++) {
                        assertEquals("null prefix row " + k, (long) k, gt.getQuick(k));
                    }
                    for (int k = 20; k < 31; k++) {
                        assertEquals("index row at k=" + k, (long) (k - 20), gt.getQuick(k));
                    }

                    final long clampedMax = loweredMax;          // min(49, 10)
                    final long nullMaxValue = callerHiInclusive;  // unclamped caller max

                    // countMatchesClamped must equal the cursor's drained count.
                    assertEquals("countMatchesClamped must use unclamped null bound",
                            (long) gt.size(),
                            reader.countMatchesClamped(0, 0, nullMaxValue, clampedMax));

                    // selectKthMatch must match the cursor at every k: the full null prefix
                    // (esp. k=11..19, the previously-dropped nulls), the null->index boundary
                    // (k=19 last null, k=20 first index), and the index tail.
                    for (int k = 0; k < gt.size(); k++) {
                        assertEquals("selectKthMatch != cursor at k=" + k,
                                gt.getQuick(k),
                                reader.selectKthMatch(0, 0, nullMaxValue, clampedMax, k));
                    }
                    // One past the end is the sentinel.
                    assertEquals("k == N must be the sentinel",
                            Numbers.LONG_NULL,
                            reader.selectKthMatch(0, 0, nullMaxValue, clampedMax, gt.size()));
                }
            }
        });
    }

    /**
     * Single-gen EF layout: one key, many strictly-increasing gapped rowids across two
     * committed gens then sealed into one dense gen. The high-value-to-count ratio drives
     * the adaptive encoder to Elias-Fano. Asserts selectKthMatch == cursor ground truth at
     * k = 0, 1, interior, and N-1, including a chunk-boundary interior index.
     */
    @Test
    public void testSingleGenEfMatchesCursor() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_ef";
                final int plen = path.size();
                final int totalRows = 4_000;
                long[] rowIds = new long[totalRows];
                long pos = 0;
                for (int i = 0; i < totalRows; i++) {
                    pos += 1 + ((i * 0x9E3779B1L) & 0x7F); // gapped, strictly increasing
                    rowIds[i] = pos;
                }
                long maxRow = rowIds[totalRows - 1];
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    int half = totalRows / 2;
                    for (int i = 0; i < half; i++) {
                        writer.add(0, rowIds[i]);
                    }
                    writer.setMaxValue(rowIds[half - 1]);
                    writer.commit();
                    for (int i = half; i < totalRows; i++) {
                        writer.add(0, rowIds[i]);
                    }
                    writer.setMaxValue(maxRow);
                    writer.commit();
                    writer.seal();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();
                    assertSelectMatchesCursor(reader, 0, 0, Long.MAX_VALUE);
                }
            }
        });
    }

    /**
     * Single-gen dense FLAT layout: many keys, few rows per key (stride-wide FoR after seal).
     * Asserts selectKthMatch == cursor ground truth for several keys across strides.
     */
    @Test
    public void testSingleGenFlatMatchesCursor() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_flat";
                final int plen = path.size();
                final int keyCount = 300; // > DENSE_STRIDE (256): exercises multiple strides
                final int rowsPerKey = 3;
                final int totalRows = keyCount * rowsPerKey;
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int row = 0; row < totalRows; row++) {
                        writer.add(row % keyCount, row);
                    }
                    writer.setMaxValue(totalRows - 1);
                    writer.commit();
                    writer.seal();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();
                    // Probe keys in the first stride, the stride boundary, and the last stride.
                    for (int key : new int[]{0, 1, 7, 255, 256, 257, 299}) {
                        assertSelectMatchesCursor(reader, key, 0, Long.MAX_VALUE);
                    }
                }
            }
        });
    }

    /**
     * Single-gen DELTA layout: few keys, many consecutive rows per key (multi-block delta-FoR,
     * BLOCK_CAPACITY=64). Exercises the delta-blob select's block locate + in-block accumulate,
     * including indices past a block boundary.
     */
    @Test
    public void testSingleGenDeltaMatchesCursor() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_delta";
                final int plen = path.size();
                final int keyCount = 5;
                final int rowsPerKey = 200; // > 64 -> multiple delta blocks per key
                final int totalRows = keyCount * rowsPerKey;
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int row = 0; row < totalRows; row++) {
                        writer.add(row % keyCount, row);
                    }
                    writer.setMaxValue(totalRows - 1);
                    writer.commit();
                    writer.seal();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();
                    for (int key = 0; key < keyCount; key++) {
                        assertSelectMatchesCursor(reader, key, 0, Long.MAX_VALUE);
                    }
                }
            }
        });
    }

    /**
     * Multi-gen layout with sparse gens (no seal): a dense base gen touching every key, then
     * several sparse gens touching a subset. selectKthMatch must stitch the per-gen posting
     * lists in gen order exactly as the cursor does. A clamp at an intermediate gen boundary
     * must yield exact values for matches in the included gens and the sentinel past them.
     */
    @Test
    public void testMultiGenSparseMatchesCursorAndClampBoundary() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_multigen";
                final int plen = path.size();
                final int keyCount = 4;
                final int extraGens = 4;
                final int sparseKeyCount = 2;
                final int baseRows = keyCount * 3;
                final int extraRowsPerGen = sparseKeyCount * 3;

                long baseGenMaxRow;
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    int row = 0;
                    for (int j = 0; j < baseRows; j++) {
                        writer.add(j % keyCount, row++);
                    }
                    baseGenMaxRow = row - 1;
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    for (int g = 0; g < extraGens; g++) {
                        for (int j = 0; j < extraRowsPerGen; j++) {
                            writer.add(j % sparseKeyCount, row++);
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                    }
                    // No seal(): keep the multi-gen sparse head.
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();

                    // Full range: each cacheable sparse key spans the dense base gen plus
                    // every sparse gen, so its list crosses dense + sparse + (warmed) cache paths.
                    for (int key = 0; key < keyCount; key++) {
                        assertSelectMatchesCursor(reader, key, 0, Long.MAX_VALUE);
                    }

                    // Clamp exactly at the dense base gen's last row: only the base gen is
                    // fully covered, every later sparse gen has min > clamp (skipped). For key 0
                    // (present in base + every sparse gen) this trims to just the base-gen matches.
                    LongList baseOnly = drain(reader, 0, 0, baseGenMaxRow);
                    assertTrue("base gen must contribute matches for key 0", baseOnly.size() > 0);
                    for (int k = 0; k < baseOnly.size(); k++) {
                        assertEquals("clamp-at-gen-boundary mismatch at k=" + k,
                                baseOnly.getQuick(k), reader.selectKthMatch(0, 0, baseGenMaxRow, baseGenMaxRow, k));
                    }
                    assertEquals("past the clamped match set must be the sentinel",
                            Numbers.LONG_NULL, reader.selectKthMatch(0, 0, baseGenMaxRow, baseGenMaxRow, baseOnly.size()));
                }
            }
        });
    }

    /**
     * Null-prefix (columnTop) case: rows below columnTop are implicit NULLs the cursor emits
     * as synthetic contiguous row ids for key 0 BEFORE any index posting. selectKthMatch must
     * reproduce the prefix then the index tail, identical to the NullCursor drain.
     */
    @Test
    public void testNullPrefixMatchesCursor() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_nullprefix";
                final int plen = path.size();
                final long columnTop = 7; // rows 0..6 are implicit NULLs
                final int keyCount = 3;
                final int rowsPerKey = 5;
                // Encoded rowids start at columnTop (rows before it are not in the index).
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    long row = columnTop;
                    for (int i = 0; i < keyCount * rowsPerKey; i++) {
                        writer.add((int) (row % keyCount), row);
                        row++;
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    writer.seal();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, columnTop)) {
                    reader.reloadConditionally();
                    // Key 0 gets the null prefix [0..6] then its index postings.
                    assertSelectMatchesCursor(reader, 0, 0, Long.MAX_VALUE);
                    // A non-zero minValue that lands inside the null prefix.
                    assertSelectMatchesCursor(reader, 0, 3, Long.MAX_VALUE);
                    // A key without a null prefix (key != 0) is index-only.
                    assertSelectMatchesCursor(reader, 1, 0, Long.MAX_VALUE);
                }
            }
        });
    }

    /**
     * populateCacheForKey byte-identity: warm reader A via a full traverse (the only thing that
     * fires putCacheEntries today) and reader B via the metadata-only populateCacheForKey. The
     * cached entry list for the key must be byte-identical (same start/count, same packed
     * (gen,posInGen) entries, same ascending order). Then a cursor over the populate-warmed
     * reader must yield the same rows as one over the traverse-warmed reader.
     */
    @Test
    public void testPopulateCacheForKeyIsByteIdenticalToTraverse() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_cacheid";
                final int plen = path.size();
                final int keyCount = 4;
                final int extraGens = 5;
                final int sparseKeyCount = 3; // keys 0,1,2 appear in every sparse gen
                final int baseRows = keyCount * 3;
                final int extraRowsPerGen = sparseKeyCount * 3;

                writeMultiGenSparse(path, plen, name, keyCount, extraGens, sparseKeyCount, baseRows, extraRowsPerGen);

                for (int key = 0; key < sparseKeyCount; key++) {
                    // Reader A: traverse-warmed (drives a full cursor pass).
                    try (PostingIndexFwdReader a = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0);
                         PostingIndexFwdReader b = new PostingIndexFwdReader(
                                 configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                        a.reloadConditionally();
                        b.reloadConditionally();
                        PostingGenLookup la = genLookupOf(a);
                        PostingGenLookup lb = genLookupOf(b);

                        // Precondition: cache cold on both.
                        assertEquals(CACHE_NOT_PRESENT, la.cacheLookup(key));
                        assertEquals(CACHE_NOT_PRESENT, lb.cacheLookup(key));

                        // Warm A by traversal.
                        drain(a, key, 0, Long.MAX_VALUE);
                        // Warm B by the metadata-only primitive.
                        b.populateCacheForKey(key, Long.MAX_VALUE);

                        long slotA = la.cacheLookup(key);
                        long slotB = lb.cacheLookup(key);
                        assertNotEquals("traverse must warm key " + key, CACHE_NOT_PRESENT, slotA);
                        assertNotEquals("populateCacheForKey must warm key " + key, CACHE_NOT_PRESENT, slotB);

                        int countA = PostingGenLookup.unpackEntryCount(slotA);
                        int countB = PostingGenLookup.unpackEntryCount(slotB);
                        assertEquals("cached entry count must match for key " + key, countA, countB);
                        assertTrue("key " + key + " must have at least one sparse-gen cache entry", countA > 0);

                        int startA = PostingGenLookup.unpackEntryStart(slotA);
                        int startB = PostingGenLookup.unpackEntryStart(slotB);
                        long prevGen = Long.MIN_VALUE;
                        for (int i = 0; i < countA; i++) {
                            long ea = la.cacheEntryAt(startA + i);
                            long eb = lb.cacheEntryAt(startB + i);
                            assertEquals("cache entry " + i + " mismatch for key " + key, ea, eb);
                            // Canonical ascending gen order.
                            long gen = PostingGenLookup.unpackCacheGen(ea);
                            assertTrue("cache entries must be ascending by gen", gen > prevGen);
                            prevGen = gen;
                        }

                        // A cursor over the populate-warmed reader (cache replay) yields the same
                        // rows as one over the traverse-warmed reader.
                        LongList rowsA = drain(a, key, 0, Long.MAX_VALUE);
                        LongList rowsB = drain(b, key, 0, Long.MAX_VALUE);
                        assertEquals("row count mismatch for key " + key, rowsA.size(), rowsB.size());
                        for (int i = 0; i < rowsA.size(); i++) {
                            assertEquals("row mismatch for key " + key + " at " + i,
                                    rowsA.getQuick(i), rowsB.getQuick(i));
                        }
                    }
                }
            }
        });
    }

    /**
     * populateCacheForKey is gated on a multi-gen sparse layout. A single-gen dense (sealed)
     * index never caches, so the call must be a no-op and leave the key cold — matching the
     * traverse, whose single-gen-dense fast path also skips the cache.
     */
    @Test
    public void testPopulateCacheForKeyNoOpOnSingleGenDense() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_cache_noop";
                final int plen = path.size();
                final int keyCount = 8;
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int row = 0; row < keyCount * 4; row++) {
                        writer.add(row % keyCount, row);
                    }
                    writer.setMaxValue(keyCount * 4 - 1);
                    writer.commit();
                    writer.seal(); // collapse to a single dense gen
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();
                    PostingGenLookup lookup = genLookupOf(reader);
                    reader.populateCacheForKey(0, Long.MAX_VALUE);
                    assertEquals("single-gen-dense must not cache",
                            CACHE_NOT_PRESENT, lookup.cacheLookup(0));
                    // And selectKthMatch still works against the single dense gen.
                    assertSelectMatchesCursor(reader, 0, 0, Long.MAX_VALUE);
                }
            }
        });
    }

    /**
     * Regression for the populateCacheForKey cache-poisoning bug. For a key in a sparse gen's
     * [minKey, maxKey] range but ABSENT from it (a "hole"), the cursor's loadSparseGenByPrefixSum
     * records NO cache entry because start == prefixSum[k] == prefixSum[k+1] == end. The buggy
     * predicate counts[start] > 0 instead records a spurious entry whenever the gen's SBBF
     * false-positives the absent key (counts[start] is then the NEXT key's count, pointing at a
     * different key's postings). With many in-range holes across several gens an SBBF
     * false-positive is deterministic and effectively certain, so populateCacheForKey must stay
     * byte-identical to a full traverse for EVERY in-range key -- present and absent -- and an
     * absent key must warm to an EMPTY hit, never a spurious entry.
     */
    @Test
    public void testPopulateCacheForKeyMatchesTraverseForAbsentInRangeKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_cache_holes";
                final int plen = path.size();
                final int activeKeys = 128;              // even keys 0,2,...,254 present
                final int gens = 8;                      // 8 sparse gens -> an FP across some gen is certain
                final int maxKey = 2 * (activeKeys - 1); // 254; odd keys 1..253 are in-range holes
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    int row = 0;
                    for (int g = 0; g < gens; g++) {
                        for (int e = 0; e < activeKeys; e++) {
                            writer.add(2 * e, row++); // even key only -> odd keys are holes
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                    }
                    // No seal: keep the multi-gen sparse head so every gen carries an SBBF.
                }

                for (int key = 0; key <= maxKey; key++) {
                    try (PostingIndexFwdReader a = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0);
                         PostingIndexFwdReader b = new PostingIndexFwdReader(
                                 configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                        a.reloadConditionally();
                        b.reloadConditionally();
                        PostingGenLookup la = genLookupOf(a);
                        PostingGenLookup lb = genLookupOf(b);

                        drain(a, key, 0, Long.MAX_VALUE);            // traverse-warm (fires putCacheEntries)
                        b.populateCacheForKey(key, Long.MAX_VALUE);   // metadata-warm

                        long slotA = la.cacheLookup(key);
                        long slotB = lb.cacheLookup(key);
                        int cntA = PostingGenLookup.unpackEntryCount(slotA);
                        assertEquals("cache entry count mismatch for key " + key,
                                cntA, PostingGenLookup.unpackEntryCount(slotB));
                        int sA = PostingGenLookup.unpackEntryStart(slotA);
                        int sB = PostingGenLookup.unpackEntryStart(slotB);
                        for (int i = 0; i < cntA; i++) {
                            assertEquals("cache entry " + i + " mismatch for key " + key,
                                    la.cacheEntryAt(sA + i), lb.cacheEntryAt(sB + i));
                        }
                        if ((key & 1) == 1) {
                            // Absent (odd, in-range) key: must warm to an EMPTY hit, never a spurious entry.
                            assertEquals("absent in-range key " + key + " must cache zero entries", 0, cntA);
                        }
                    }
                }
            }
        });
    }

    /**
     * Regression for the single-sparse-gen cache-warm gap (the parallel-decode race fix). Before
     * the fix populateCacheForKey bailed when genCount <= 1, leaving a single SPARSE gen cold --
     * a worker's detached cursor would then write the shared cache concurrently. It must now warm
     * a single sparse gen byte-identically to the traverse, so workers replay (read-only).
     */
    @Test
    public void testPopulateCacheForKeyWarmsSingleSparseGen() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_single_sparse";
                final int plen = path.size();
                // ONE clearly-sparse gen: keys {0, 50, 100} over range [0,100] (single commit, no
                // seal) -> anySparseGen && genCount == 1, the exact case the old bail skipped.
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    int row = 0;
                    for (int rep = 0; rep < 4; rep++) {
                        writer.add(0, row++);
                        writer.add(50, row++);
                        writer.add(100, row++);
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                }
                try (PostingIndexFwdReader a = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0);
                     PostingIndexFwdReader b = new PostingIndexFwdReader(
                             configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    a.reloadConditionally();
                    b.reloadConditionally();
                    PostingGenLookup la = genLookupOf(a);
                    PostingGenLookup lb = genLookupOf(b);
                    assertTrue("layout must be a single sparse gen", lb.anySparseGen());

                    final int key = 50; // present in the single sparse gen
                    assertEquals(CACHE_NOT_PRESENT, lb.cacheLookup(key));
                    drain(a, key, 0, Long.MAX_VALUE);            // traverse warms
                    b.populateCacheForKey(key, Long.MAX_VALUE);   // must now ALSO warm (was a no-op)

                    long slotA = la.cacheLookup(key);
                    long slotB = lb.cacheLookup(key);
                    assertNotEquals("single sparse gen must now be warmed by populateCacheForKey",
                            CACHE_NOT_PRESENT, slotB);
                    int cnt = PostingGenLookup.unpackEntryCount(slotA);
                    assertTrue("present key in single sparse gen must cache an entry", cnt > 0);
                    assertEquals("populate-warm must be byte-identical to traverse-warm (count)",
                            cnt, PostingGenLookup.unpackEntryCount(slotB));
                    int sA = PostingGenLookup.unpackEntryStart(slotA);
                    int sB = PostingGenLookup.unpackEntryStart(slotB);
                    for (int i = 0; i < cnt; i++) {
                        assertEquals("cache entry " + i, la.cacheEntryAt(sA + i), lb.cacheEntryAt(sB + i));
                    }
                }
            }
        });
    }

    /**
     * selectKthMatch / countMatchesClamped edge cases: an absent key (out of every gen's range)
     * yields zero matches and the sentinel at k=0, and a single-row-per-key layout resolves k=0 to
     * the row and k==count to the sentinel -- whatever encoding the writer picks.
     */
    @Test
    public void testSelectKthMatchAbsentAndSingleRowEdges() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                // (a) Absent key: build keys 0..2, probe an out-of-range key.
                final String name = "skm_absent";
                writeMultiGenSparse(path, plen, name, 3, 4, 3, 9, 9);
                try (PostingIndexFwdReader r = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    r.reloadConditionally();
                    final int absent = 99;
                    assertEquals("absent key must have zero clamped matches",
                            0L, r.countMatchesClamped(absent, 0, Long.MAX_VALUE, Long.MAX_VALUE));
                    assertEquals("absent key selectKthMatch(0) must be the sentinel",
                            Numbers.LONG_NULL, r.selectKthMatch(absent, 0, Long.MAX_VALUE, Long.MAX_VALUE, 0));
                    assertSelectMatchesCursor(r, absent, 0, Long.MAX_VALUE);
                }
                // (b) Single row per key: keys 0,1,2 each at exactly one row.
                final String name2 = "skm_single_row";
                try (PostingIndexWriter w = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name2, COLUMN_NAME_TXN_NONE)) {
                    w.add(0, 0);
                    w.add(1, 1);
                    w.add(2, 2);
                    w.setMaxValue(2);
                    w.commit();
                }
                try (PostingIndexFwdReader r = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name2, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    r.reloadConditionally();
                    for (int key = 0; key < 3; key++) {
                        assertEquals("single-row key must count 1",
                                1L, r.countMatchesClamped(key, 0, Long.MAX_VALUE, Long.MAX_VALUE));
                        assertEquals("single-row key k=0 is its row",
                                (long) key, r.selectKthMatch(key, 0, Long.MAX_VALUE, Long.MAX_VALUE, 0));
                        assertEquals("single-row key k==count is the sentinel",
                                Numbers.LONG_NULL, r.selectKthMatch(key, 0, Long.MAX_VALUE, Long.MAX_VALUE, 1));
                        assertSelectMatchesCursor(r, key, 0, Long.MAX_VALUE);
                    }
                }
            }
        });
    }

    // ---- helpers ----

    /**
     * An early gen whose postings for the key are ENTIRELY below minValue must be skipped
     * by the cheap path (it contributes 0, exactly as the cursor skips it), NOT trip the
     * MIXED bail. Layout: key 0 has gen 0 at rows 0,2,..,98 (all &lt; 1000) and gen 1 at rows
     * 1000,1002,..,1098 (all &gt;= 1000). For any minValue in (98, 1000] the early gen is fully
     * below and the late gen is fully covered, so selectKthMatch/countMatchesClamped must
     * equal the cursor exactly (NON-sentinel). Before the fully-below optimization this
     * returned the LONG_NULL sentinel and forced the whole partition onto the O(rows) traverse.
     */
    @Test
    public void testEarlyGenFullyBelowMinValueUsesCheapPath() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_below_min";
                final int plen = path.size();

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Gen 0: rows 0..99, key = row % 2 -> key 0 at 0,2,..,98 (all < 1000).
                    for (long rowId = 0; rowId < 100; rowId++) {
                        writer.add((int) (rowId % 2), rowId);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                    // Gen 1: rows 1000..1099, key = row % 2 -> key 0 at 1000,1002,..,1098.
                    for (long rowId = 1000; rowId < 1100; rowId++) {
                        writer.add((int) (rowId % 2), rowId);
                    }
                    writer.setMaxValue(1099);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();
                    final long clamp = entryMaxValue(reader);

                    // Sweep minValues where the early gen (max 98) is fully below and the late
                    // gen (1000..1098, within clamp) is fully covered: 0 (no skip) plus
                    // interior values that skip only the early gen. Each must match the cursor
                    // exactly and NEVER produce the sentinel. (minValue > clamp is a separate,
                    // pre-existing empty-range bail and is not what this optimization touches.)
                    for (long minValue : new long[]{0, 200, 500, 999, 1000}) {
                        assertSelectMatchesCursor(reader, 0, minValue, Long.MAX_VALUE);
                    }

                    // Loud, explicit teeth: at minValue=500 the cheap path must SUCCEED with the
                    // exact count (50: rows 1000,1002,..,1098), not bail to the sentinel.
                    assertNotEquals("fully-below early gen must not force the sentinel",
                            Numbers.LONG_NULL, reader.countMatchesClamped(0, 500, Long.MAX_VALUE, clamp));
                    assertEquals("cheap-path count must equal the cursor's drained count",
                            50L, reader.countMatchesClamped(0, 500, Long.MAX_VALUE, clamp));
                    assertEquals("first match at minValue=500 must be row 1000",
                            1000L, reader.selectKthMatch(0, 500, Long.MAX_VALUE, clamp, 0));
                    assertEquals("last match at minValue=500 must be row 1098",
                            1098L, reader.selectKthMatch(0, 500, Long.MAX_VALUE, clamp, 49));
                }
            }
        });
    }

    /**
     * A gen that STRADDLES minValue (some postings below, some at/above) must still bail to
     * the sentinel — the full per-gen count would over-count the below-minValue postings the
     * cursor filters out. This guards the precision of the fully-below optimization: only a
     * gen entirely below minValue may be skipped, never a straddling one.
     */
    @Test
    public void testGenStraddlingMinValueStillBailsToSentinel() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "skm_straddle";
                final int plen = path.size();

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Single gen: key 0 at rows 0,2,..,98.
                    for (long rowId = 0; rowId < 100; rowId++) {
                        writer.add((int) (rowId % 2), rowId);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    reader.reloadConditionally();
                    final long clamp = entryMaxValue(reader);

                    // minValue=50 lands inside the gen (postings 0..48 below, 50..98 at/above):
                    // a genuine straddle -> the metadata count diverges from the cursor, so the
                    // cheap path MUST return the sentinel and let the caller traverse.
                    assertEquals("a gen straddling minValue must still yield the fallback sentinel",
                            Numbers.LONG_NULL, reader.countMatchesClamped(0, 50, Long.MAX_VALUE, clamp));
                    for (int k = 0; k < 25; k++) {
                        assertEquals("straddling gen must yield the sentinel for every k (k=" + k + ")",
                                Numbers.LONG_NULL, reader.selectKthMatch(0, 50, Long.MAX_VALUE, clamp, k));
                    }
                }
            }
        });
    }

    private static void assertSelectMatchesCursor(PostingIndexFwdReader reader, int key, long minValue, long callerMax) {
        LongList gt = drain(reader, key, minValue, callerMax);
        // The cursor clamps internally to min(callerMax, entryMaxValue). Mirror that
        // so selectKthMatch sees the identical inclusive upper bound.
        long entryMax;
        try {
            entryMax = entryMaxValue(reader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long clamp = entryMax >= 0 ? Math.min(callerMax == Long.MAX_VALUE ? Long.MAX_VALUE : callerMax, entryMax) : callerMax;
        // The null prefix is bounded by the UNCLAMPED caller max (columnTop only); only
        // the gen walk uses the entryMaxValue-folded clamp. For these clean layouts the
        // two coincide (entryMax >= callerMax), so the assertions hold either way.
        long nullMax = callerMax;

        int n = gt.size();
        // countMatchesClamped is selectKthMatch's sibling: over a fully-covered
        // (non-MIXED) range it must equal the cursor's drained count exactly. These
        // layouts are clean (the dirty-rows MIXED case is asserted separately), so the
        // sentinel must not appear here.
        assertEquals("countMatchesClamped != cursor count for key " + key,
                (long) n, reader.countMatchesClamped(key, minValue, nullMax, clamp));
        if (n == 0) {
            assertEquals("empty match set must yield the sentinel at k=0",
                    Numbers.LONG_NULL, reader.selectKthMatch(key, minValue, nullMax, clamp, 0));
            return;
        }
        // k = 0, 1, a spread of interior indices (incl. a chunk/block boundary), and N-1.
        for (int k : interiorProbes(n)) {
            assertEquals("selectKthMatch != cursor for key " + key + " at k=" + k,
                    gt.getQuick(k), reader.selectKthMatch(key, minValue, nullMax, clamp, k));
        }
        // One past the end is the sentinel.
        assertEquals("k == N must be the sentinel for key " + key,
                Numbers.LONG_NULL, reader.selectKthMatch(key, minValue, nullMax, clamp, n));
    }

    private static LongList drain(PostingIndexFwdReader reader, int key, long minValue, long maxValue) {
        LongList out = new LongList();
        try (RowCursor c = reader.getCursor(key, minValue, maxValue)) {
            while (c.hasNext()) {
                // next() returns the row id relative to minValue; restore the absolute id.
                out.add(c.next() + minValue);
            }
        }
        return out;
    }

    private static long entryMaxValue(PostingIndexFwdReader reader) throws Exception {
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("entryMaxValue");
        f.setAccessible(true);
        return f.getLong(reader);
    }

    private static PostingGenLookup genLookupOf(PostingIndexFwdReader reader) throws Exception {
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("genLookup");
        f.setAccessible(true);
        return (PostingGenLookup) f.get(reader);
    }

    private static int[] interiorProbes(int n) {
        // 0, 1, midpoint, the first block/chunk boundary (64), n/4, 3n/4, and n-1 — de-duplicated.
        LongList tmp = new LongList();
        addProbe(tmp, n, 0);
        addProbe(tmp, n, 1);
        addProbe(tmp, n, n / 2);
        addProbe(tmp, n, 63);
        addProbe(tmp, n, 64);
        addProbe(tmp, n, n / 4);
        addProbe(tmp, n, (3 * n) / 4);
        addProbe(tmp, n, n - 1);
        int[] out = new int[tmp.size()];
        for (int i = 0; i < tmp.size(); i++) {
            out[i] = (int) tmp.getQuick(i);
        }
        return out;
    }

    private static void addProbe(LongList acc, int n, int k) {
        if (k < 0 || k >= n) {
            return;
        }
        for (int i = 0; i < acc.size(); i++) {
            if (acc.getQuick(i) == k) {
                return;
            }
        }
        acc.add(k);
    }

    private static void writeMultiGenSparse(
            Path path, int plen, String name, int keyCount, int extraGens, int sparseKeyCount,
            int baseRows, int extraRowsPerGen
    ) {
        try (PostingIndexWriter writer = new PostingIndexWriter(
                configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
            int row = 0;
            for (int j = 0; j < baseRows; j++) {
                writer.add(j % keyCount, row++);
            }
            writer.setMaxValue(row - 1);
            writer.commit();
            for (int g = 0; g < extraGens; g++) {
                for (int j = 0; j < extraRowsPerGen; j++) {
                    writer.add(j % sparseKeyCount, row++);
                }
                writer.setMaxValue(row - 1);
                writer.commit();
            }
            // No seal(): keep the multi-gen sparse head.
        } finally {
            path.trimTo(plen);
        }
    }
}
