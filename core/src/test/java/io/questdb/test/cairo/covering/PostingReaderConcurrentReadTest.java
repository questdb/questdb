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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.PostingGenLookup;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.*;

public class PostingReaderConcurrentReadTest extends AbstractCairoTest {

    // Mirror of PostingGenLookup.CACHE_NOT_PRESENT (package-private there).
    private static final long CACHE_NOT_PRESENT = -1L;
    private static final ColumnVersionReader EMPTY_CVR = new ColumnVersionReader();

    /**
     * warmForKeys must drive a full cursor pass per key so the (per-reader,
     * idempotent) genLookup cache is populated, and it must pre-open the
     * required cover sidecars and pre-extend valueMem to its full published
     * size — all single-threaded, before any concurrent cursors run.
     * <p>
     * Oracle (a): a cursor opened on a WARMED reader yields exactly the same
     * (rowId, covered value) sequence as a cursor on a fresh COLD reader, i.e.
     * warming does not change results.
     * <p>
     * Oracle (b): the genLookup cache is empty for a key before warming and
     * populated after — proving warmForKeys is what filled it. The dataset uses
     * several committed generations so sparse gens (hence cacheable keys) exist.
     */
    @Test
    public void testWarmForKeysPopulatesCacheAndPreservesResults() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "warm_posting";
                final int plen = path.size();

                // Layout: gen 0 covers every key (becomes the dense base), then
                // several gens that touch only a SUBSET of keys. Those later gens
                // are encoded sparse, so anySparseGen == true and the subset keys
                // are cacheable. The head is left UNSEALED so the multi-gen sparse
                // layout is what the reader serves (seal() would re-encode the head
                // into a single dense gen and erase the sparse gens).
                final int keyCount = 4;
                final int extraGens = 4;          // sparse gens appended after the base
                final int sparseKeyCount = 2;     // keys 0 and 1 appear in every extra gen
                // rows: base gen has keyCount*3 rows; each extra gen has sparseKeyCount*3.
                final int baseRows = keyCount * 3;
                final int extraRowsPerGen = sparseKeyCount * 3;
                final int totalRows = baseRows + extraGens * extraRowsPerGen;

                // One covered LONG column whose value mirrors the row id, so the
                // oracle can check covered payload, not just row ids.
                final long colBytes = (long) totalRows * Long.BYTES;
                final long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},               // shift for LONG (8 bytes == 2^3)
                                new int[]{1},               // covered column writer index
                                new int[]{ColumnType.LONG},
                                1
                        );
                        int row = 0;
                        // Base (dense) gen: every key present.
                        for (int j = 0; j < baseRows; j++) {
                            writer.add(j % keyCount, row++);
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                        // Extra (sparse) gens: only the first sparseKeyCount keys.
                        for (int g = 0; g < extraGens; g++) {
                            for (int j = 0; j < extraRowsPerGen; j++) {
                                writer.add(j % sparseKeyCount, row++);
                            }
                            writer.setMaxValue(row - 1);
                            writer.commit();
                        }
                        // No seal(): keep the multi-gen sparse head.
                    }

                    final RecordMetadata md = coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG});
                    final int[] keys = new int[keyCount];
                    for (int k = 0; k < keyCount; k++) {
                        keys[k] = k;
                    }
                    final int[] required = {0};

                    // Capture the cold (un-warmed) expected sequence per key.
                    final LongList[] expectedRows = new LongList[keyCount];
                    final LongList[] expectedVals = new LongList[keyCount];
                    try (PostingIndexFwdReader cold = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        for (int k = 0; k < keyCount; k++) {
                            expectedRows[k] = new LongList();
                            expectedVals[k] = new LongList();
                            CoveringRowCursor cc = (CoveringRowCursor) cold.getCursor(k, 0, Long.MAX_VALUE, required);
                            while (cc.hasNext()) {
                                long r = cc.next();
                                expectedRows[k].add(r);
                                expectedVals[k].add(cc.getCoveredLong(0));
                            }
                            Misc.free(cc);
                        }
                    }

                    // The warmed reader: assert cache empty before, populated after.
                    try (PostingIndexFwdReader warm = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        // Force metadata so cacheLookup's anySparseGen flag is meaningful.
                        warm.reloadConditionally();
                        PostingGenLookup lookup = genLookupOf(warm);
                        for (int k = 0; k < keyCount; k++) {
                            assertEquals("cache must be cold before warm for key " + k,
                                    CACHE_NOT_PRESENT, cacheLookup(lookup, k));
                        }

                        warm.warmForKeys(keys, required);

                        // (b) warming populated the per-key cache (sparse gens exist).
                        int cachedKeys = 0;
                        for (int k = 0; k < keyCount; k++) {
                            if (cacheLookup(lookup, k) != CACHE_NOT_PRESENT) {
                                cachedKeys++;
                            }
                        }
                        assertTrue("warmForKeys must populate the genLookup cache for at least one key"
                                + " (anySparseGen layout); cachedKeys=" + cachedKeys, cachedKeys > 0);

                        // side effect: required sidecar pre-opened.
                        assertTrue("required cover sidecar must be mapped after warm",
                                sidecarMmapSize(warm, 0) > 0);

                        // side effect: valueMem pre-extended to its full published size.
                        assertEquals("valueMem must be pre-extended to valueMemSize",
                                valueMemSize(warm), valueMemSizeField(warm));
                        assertTrue("valueMem size must be non-zero after warm", valueMemSize(warm) > 0);

                        // (a) a cursor on the warmed reader yields identical results.
                        for (int k = 0; k < keyCount; k++) {
                            CoveringRowCursor cc = (CoveringRowCursor) warm.getCursor(k, 0, Long.MAX_VALUE, required);
                            int i = 0;
                            while (cc.hasNext()) {
                                long r = cc.next();
                                assertTrue("warmed cursor produced more rows than cold for key " + k,
                                        i < expectedRows[k].size());
                                assertEquals("row id mismatch key " + k + " idx " + i,
                                        expectedRows[k].getQuick(i), r);
                                assertEquals("covered value mismatch key " + k + " idx " + i,
                                        expectedVals[k].getQuick(i), cc.getCoveredLong(0));
                                i++;
                            }
                            assertEquals("warmed cursor produced fewer rows than cold for key " + k,
                                    expectedRows[k].size(), i);
                            Misc.free(cc);
                        }
                    }
                } finally {
                    Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * THE WARM INVARIANT for Task 7 (metadata-only single-key frame production).
     * Single-key frame production no longer materializes covered values; it only
     * TRAVERSES the covering cursor ({@code while (hasNext()) next();}) to natural
     * exhaustion. That traverse is the ONLY thing that fires the posting reader's
     * putCacheEntries, which warms the per-key genLookup cache so the async
     * workers' detached cursors run read-only under the freeze. This test proves
     * the traverse ALONE -- with NO covered value reads (no getCoveredLong, no
     * symbol fill, no buffer writes) -- still warms the cache: a metadata-only
     * production pass is sufficient. (If production had been replaced with a
     * size()-only count, the cursor would never advance gens to exhaustion and
     * the cache would stay cold -- the exact race this design avoids.)
     */
    @Test
    public void testTraverseOnlyWarmsCacheWithoutValueReads() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "warm_traverse_only";
                final int plen = path.size();

                // Same multi-gen sparse layout as testWarmForKeysPopulatesCache...:
                // a dense base gen touching every key, then several sparse gens
                // touching only the first sparseKeyCount keys. No seal(), so the
                // reader serves the multi-gen sparse head (anySparseGen == true and
                // the subset keys are cacheable).
                final int keyCount = 4;
                final int extraGens = 4;
                final int sparseKeyCount = 2; // keys 0 and 1 are the cacheable sparse keys
                final int baseRows = keyCount * 3;
                final int extraRowsPerGen = sparseKeyCount * 3;
                final int totalRows = baseRows + extraGens * extraRowsPerGen;

                final long colBytes = (long) totalRows * Long.BYTES;
                final long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{1},
                                new int[]{ColumnType.LONG},
                                1
                        );
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
                    }

                    final RecordMetadata md = coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG});
                    final int[] required = {0};

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        // Force metadata so cacheLookup's anySparseGen flag is meaningful.
                        reader.reloadConditionally();
                        final PostingGenLookup lookup = genLookupOf(reader);

                        // Precondition: this layout actually produces sparse gens,
                        // otherwise cacheLookup is permanently CACHE_NOT_PRESENT and
                        // the warm assertion below would be vacuous.
                        assertTrue("test layout must have sparse gens for the warm assertion to be meaningful",
                                anySparseGen(lookup));

                        // The sparse key 0 is COLD before the traverse.
                        assertEquals("cache must be cold for the sparse key before the production traverse",
                                CACHE_NOT_PRESENT, cacheLookup(lookup, 0));

                        // EXACTLY the new single-key production loop: open a covering
                        // cursor and traverse to natural exhaustion -- hasNext()/next()
                        // ONLY, no covered value reads. This is what fillFrameForKey
                        // now does (metadata-only). Drive it over the full row range.
                        long traversed = 0;
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(0, 0, Long.MAX_VALUE, required);
                        while (cc.hasNext()) {
                            cc.next(); // NO getCoveredLong(...) -- production no longer materializes values
                            traversed++;
                        }
                        Misc.free(cc);
                        assertTrue("the production traverse must visit key 0's rows", traversed > 0);

                        // The traverse-to-exhaustion fired putCacheEntries: key 0 is
                        // now WARM. This is the invariant Task 7's metadata-only
                        // production must preserve.
                        assertTrue("metadata-only traverse must WARM the genLookup cache for the resolved key"
                                        + " (cacheLookup hit after production)",
                                cacheLookup(lookup, 0) != CACHE_NOT_PRESENT);
                    }
                } finally {
                    Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Two DETACHED (non-pooled) cursors over the SAME warmed reader and SAME key,
     * driven from two threads that overlap their iteration via a CyclicBarrier.
     * Each detached cursor owns its private decode scratch and never touches the
     * reader's freeCursors pool, so the two passes must not cross-talk: each thread
     * sees the full, identical (rowId, coveredValue) sequence that a single-threaded
     * cold reader produces. Warming first is the precondition that makes the shared
     * state (genLookup, valueMem, sidecarMems) read-only for the duration.
     */
    @Test
    public void testTwoDetachedCursorsSameKeyConcurrent() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "detached_posting";
                final int plen = path.size();

                // Same multi-gen sparse layout as the warm test: a dense base gen
                // touching every key, then several sparse gens touching a subset.
                final int keyCount = 4;
                final int extraGens = 4;
                final int sparseKeyCount = 2;
                final int baseRows = keyCount * 3;
                final int extraRowsPerGen = sparseKeyCount * 3;
                final int totalRows = baseRows + extraGens * extraRowsPerGen;

                final long colBytes = (long) totalRows * Long.BYTES;
                final long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{1},
                                new int[]{ColumnType.LONG},
                                1
                        );
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
                    }

                    final RecordMetadata md = coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG});
                    final int[] keys = new int[keyCount];
                    for (int k = 0; k < keyCount; k++) {
                        keys[k] = k;
                    }
                    final int[] required = {0};

                    // Key 0 appears in the dense base AND every sparse gen, so its
                    // posting list spans the most code paths (dense + multiple sparse
                    // gens + cache replay) — the strongest target for cross-talk.
                    final int probeKey = 0;

                    // Cold single-threaded reference for the probe key.
                    final LongList expectedRows = new LongList();
                    final LongList expectedVals = new LongList();
                    try (PostingIndexFwdReader cold = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        CoveringRowCursor cc = (CoveringRowCursor) cold.getCursor(probeKey, 0, Long.MAX_VALUE, required);
                        while (cc.hasNext()) {
                            expectedRows.add(cc.next());
                            expectedVals.add(cc.getCoveredLong(0));
                        }
                        Misc.free(cc);
                    }
                    assertTrue("probe key must yield rows for a meaningful test", expectedRows.size() > 0);

                    try (PostingIndexFwdReader warm = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        warm.warmForKeys(keys, required);

                        // Baseline: warming drives a pooled cursor per key and closes it,
                        // so the pool legitimately holds some cursors now. The detached
                        // cursors below must leave this count unchanged (they never pool).
                        final int poolSizeAfterWarm = freeCursorsSize(warm);

                        final CyclicBarrier barrier = new CyclicBarrier(2);
                        final AtomicReference<Throwable> error = new AtomicReference<>();

                        final Runnable worker = () -> {
                            try {
                                // Overlap the two passes: both threads reach here,
                                // open their detached cursor, then iterate in lockstep
                                // start so decode runs concurrently on shared mmap.
                                barrier.await();
                                CoveringRowCursor cc = (CoveringRowCursor)
                                        warm.getDetachedCursor(probeKey, 0, Long.MAX_VALUE, required);
                                try {
                                    int i = 0;
                                    while (cc.hasNext()) {
                                        long r = cc.next();
                                        long v = cc.getCoveredLong(0);
                                        assertTrue("detached cursor produced more rows than cold (idx " + i + ")",
                                                i < expectedRows.size());
                                        assertEquals("row id mismatch at idx " + i,
                                                expectedRows.getQuick(i), r);
                                        assertEquals("covered value mismatch at idx " + i,
                                                expectedVals.getQuick(i), v);
                                        i++;
                                    }
                                    assertEquals("detached cursor produced fewer rows than cold",
                                            expectedRows.size(), i);
                                } finally {
                                    cc.close();
                                }
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        };

                        Thread t1 = new Thread(worker, "detached-worker-1");
                        Thread t2 = new Thread(worker, "detached-worker-2");
                        t1.start();
                        t2.start();
                        t1.join();
                        t2.join();

                        Throwable t = error.get();
                        if (t != null) {
                            throw new AssertionError("concurrent detached cursor failure", t);
                        }

                        // Detached cursors must NOT have re-pooled: the pool size is
                        // exactly what warming left it, with no contribution from the
                        // two detached cursors (which free their scratch directly).
                        assertEquals("detached cursors must not push to freeCursors",
                                poolSizeAfterWarm, freeCursorsSize(warm));
                    }
                } finally {
                    Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * CRITICAL parallel-decode race (single sparse gen). The old populateCacheForKey bailed when
     * genCount &lt;= 1, leaving a single SPARSE gen cold, so concurrent detached worker cursors
     * raced on the shared genLookup putCacheEntries. This reproduces the PRODUCTION path -- warm via
     * populateCacheForKey (NOT the test-only warmForKeys), freeze, then hammer the frozen reader
     * with many detached cursors across many iterations. The fix (a) warms the single sparse gen so
     * workers replay read-only, and (b) forbids detached cursors from writing the cache; the
     * frozen-write assert turns any regression into a loud, deterministic failure. Every pass must
     * equal the cold reference and the cursor pool must never be touched.
     */
    @Test
    public void testConcurrentDetachedCursorsSingleSparseGenProductionWarm() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "conc_single_sparse";
                final int plen = path.size();
                // ONE clearly-sparse gen: keys {0,50,100} over [0,100] (single commit, no seal).
                final int[] sparseKeys = {0, 50, 100};
                final int rowsPerKey = 40;
                final int totalRows = sparseKeys.length * rowsPerKey;
                final long colBytes = (long) totalRows * Long.BYTES;
                final long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0}, new int[]{3}, new int[]{1}, new int[]{ColumnType.LONG}, 1);
                        int row = 0;
                        for (int r = 0; r < rowsPerKey; r++) {
                            for (int sk : sparseKeys) {
                                writer.add(sk, row++);
                            }
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit(); // single commit -> single sparse gen
                    }

                    final RecordMetadata md = coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG});
                    final int[] required = {0};
                    final int probeKey = 50;

                    // Cold single-threaded reference for the probe key.
                    final LongList expRows = new LongList();
                    final LongList expVals = new LongList();
                    try (PostingIndexFwdReader cold = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        CoveringRowCursor cc = (CoveringRowCursor) cold.getCursor(probeKey, 0, Long.MAX_VALUE, required);
                        while (cc.hasNext()) {
                            expRows.add(cc.next());
                            expVals.add(cc.getCoveredLong(0));
                        }
                        Misc.free(cc);
                    }
                    assertTrue("probe key must yield rows", expRows.size() > 0);

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        reader.reloadConditionally();
                        // Cheap-path prep: open a pooled cursor to MAP the required covered sidecars
                        // (production's openOrContinueCoveringCursor maps them before workers run),
                        // reading one row to force the mapping but NOT draining it -- the cheap path is
                        // metadata-only and leaves gen-cache warming to populateCacheForKey, so the
                        // cache stays cold here. Then warm (the single-sparse-gen path under test) + freeze.
                        CoveringRowCursor prep = (CoveringRowCursor) reader.getCursor(probeKey, 0, Long.MAX_VALUE, required);
                        if (prep.hasNext()) {
                            prep.next();
                            prep.getCoveredLong(0);
                        }
                        Misc.free(prep);
                        reader.populateCacheForKey(probeKey, Long.MAX_VALUE);
                        reader.setFrozen(true);
                        final int poolAfterWarm = freeCursorsSize(reader);

                        final int threads = 6;
                        final int iterations = 300;
                        final AtomicReference<Throwable> error = new AtomicReference<>();
                        final CyclicBarrier start = new CyclicBarrier(threads);
                        final Thread[] ts = new Thread[threads];
                        for (int t = 0; t < threads; t++) {
                            ts[t] = new Thread(() -> {
                                try {
                                    start.await();
                                    for (int it = 0; it < iterations && !Thread.interrupted(); it++) {
                                        CoveringRowCursor cc = (CoveringRowCursor)
                                                reader.getDetachedCursor(probeKey, 0, Long.MAX_VALUE, required);
                                        try {
                                            int i = 0;
                                            while (cc.hasNext()) {
                                                long r = cc.next();
                                                long v = cc.getCoveredLong(0);
                                                assertTrue("more rows than cold at idx " + i, i < expRows.size());
                                                assertEquals("row id mismatch at idx " + i, expRows.getQuick(i), r);
                                                assertEquals("covered value mismatch at idx " + i, expVals.getQuick(i), v);
                                                i++;
                                            }
                                            assertEquals("fewer rows than cold", expRows.size(), i);
                                        } finally {
                                            cc.close();
                                        }
                                    }
                                } catch (Throwable e) {
                                    error.compareAndSet(null, e);
                                }
                            }, "sparse-worker-" + t);
                        }
                        for (Thread th : ts) {
                            th.start();
                        }
                        for (Thread th : ts) {
                            th.join();
                        }

                        Throwable e = error.get();
                        if (e != null) {
                            throw new AssertionError("concurrent single-sparse-gen detached decode failure", e);
                        }
                        assertEquals("detached cursors must not push to freeCursors", poolAfterWarm, freeCursorsSize(reader));
                        reader.setFrozen(false);
                    }
                } finally {
                    Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Freeze guard: while a reader is frozen, {@code reloadConditionally()} must be a
     * complete no-op so the value mmap (and everything keyed off it) stays put for
     * in-flight worker cursors holding raw page addresses. We:
     * <ol>
     *   <li>build a covering dataset and leave the writer OPEN,</li>
     *   <li>open a reader, {@code warmForKeys}, and snapshot the value mmap base
     *       address + size, the picked {@code chainSequence}, and the probe key's
     *       full (rowId, coveredValue) sequence,</li>
     *   <li>publish MORE generations through the still-open writer so an unfrozen
     *       reload WOULD grow / remap {@code valueMem} (same {@code .pv}, larger
     *       {@code valueMemSize} -&gt; the {@code changeSize} branch),</li>
     *   <li>{@code setFrozen(true)} then {@code reloadConditionally()} and assert
     *       NOTHING moved: same base, same size, same {@code chainSequence}, and the
     *       probe cursor still yields the pre-freeze snapshot,</li>
     *   <li>{@code setFrozen(false)} then {@code reloadConditionally()} and assert the
     *       suppression lifted: {@code chainSequence} advanced and the probe key now
     *       surfaces the extra rows the writer published while we were frozen.</li>
     * </ol>
     */
    @Test
    public void testFrozenReaderSuppressesReload() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "frozen_posting";
                final int plen = path.size();

                final int keyCount = 4;
                final int extraGens = 4;
                final int sparseKeyCount = 2;
                final int baseRows = keyCount * 3;
                final int extraRowsPerGen = sparseKeyCount * 3;
                // Gens published AFTER the reader warms (mirror layout: touch the
                // probe key so its visible row count strictly grows on a real reload).
                final int postGens = 3;
                final int postRowsPerGen = sparseKeyCount * 3;
                final int totalRows = baseRows + extraGens * extraRowsPerGen + postGens * postRowsPerGen;

                // Covered LONG column mirrors row id (+1000) so the oracle checks
                // covered payload, not just row ids.
                final long colBytes = (long) totalRows * Long.BYTES;
                final long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    final RecordMetadata md = coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG});
                    final int[] keys = new int[keyCount];
                    for (int k = 0; k < keyCount; k++) {
                        keys[k] = k;
                    }
                    final int[] required = {0};
                    // Key 0 is present in the base gen, every extra (pre-warm) sparse
                    // gen, AND every post-warm gen, so its visible row count is the
                    // sharpest signal of whether a reload was applied.
                    final int probeKey = 0;

                    // Keep the writer OPEN across the whole test so the post-warm
                    // commits publish into the same chain / same .pv the reader mapped.
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},               // shift for LONG (8 bytes == 2^3)
                                new int[]{1},               // covered column writer index
                                new int[]{ColumnType.LONG},
                                1
                        );
                        int row = 0;
                        // Base (dense) gen: every key present.
                        for (int j = 0; j < baseRows; j++) {
                            writer.add(j % keyCount, row++);
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                        // Pre-warm sparse gens: only the first sparseKeyCount keys.
                        for (int g = 0; g < extraGens; g++) {
                            for (int j = 0; j < extraRowsPerGen; j++) {
                                writer.add(j % sparseKeyCount, row++);
                            }
                            writer.setMaxValue(row - 1);
                            writer.commit();
                        }

                        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                            reader.warmForKeys(keys, required);

                            // Pre-freeze snapshot of the mmap and picker state.
                            final long baseAddrBefore = reader.getValueBaseAddress();   // == valueMem.addressOf(0)
                            final long sizeBefore = reader.getValueMemorySize();        // == valueMem.size()
                            final long chainSeqBefore = chainSequence(reader);
                            assertTrue("valueMem must be mapped after warm", sizeBefore > 0);

                            // Pre-freeze cursor result for the probe key.
                            final LongList preRows = new LongList();
                            final LongList preVals = new LongList();
                            CoveringRowCursor pre = (CoveringRowCursor) reader.getCursor(probeKey, 0, Long.MAX_VALUE, required);
                            while (pre.hasNext()) {
                                preRows.add(pre.next());
                                preVals.add(pre.getCoveredLong(0));
                            }
                            Misc.free(pre);
                            assertTrue("probe key must yield rows before freeze", preRows.size() > 0);

                            // Publish MORE gens through the still-open writer. Same .pv,
                            // larger valueMemSize -> an unfrozen reload would changeSize()
                            // (remap) the value mmap and advance chainSequence.
                            for (int g = 0; g < postGens; g++) {
                                for (int j = 0; j < postRowsPerGen; j++) {
                                    writer.add(j % sparseKeyCount, row++);
                                }
                                writer.setMaxValue(row - 1);
                                writer.commit();
                            }

                            // --- FROZEN: reloadConditionally must do nothing observable. ---
                            reader.setFrozen(true);
                            reader.reloadConditionally();

                            assertEquals("frozen reload must not remap valueMem (base address)",
                                    baseAddrBefore, reader.getValueBaseAddress());
                            assertEquals("frozen reload must not resize valueMem (size)",
                                    sizeBefore, reader.getValueMemorySize());
                            assertEquals("frozen reload must not advance the picked chainSequence",
                                    chainSeqBefore, chainSequence(reader));

                            CoveringRowCursor frozenCursor =
                                    (CoveringRowCursor) reader.getCursor(probeKey, 0, Long.MAX_VALUE, required);
                            int fi = 0;
                            while (frozenCursor.hasNext()) {
                                long r = frozenCursor.next();
                                long v = frozenCursor.getCoveredLong(0);
                                assertTrue("frozen cursor yielded more rows than the pre-freeze snapshot (idx " + fi + ")",
                                        fi < preRows.size());
                                assertEquals("frozen cursor row id mismatch at idx " + fi, preRows.getQuick(fi), r);
                                assertEquals("frozen cursor covered value mismatch at idx " + fi, preVals.getQuick(fi), v);
                                fi++;
                            }
                            assertEquals("frozen cursor must still yield exactly the pre-freeze snapshot",
                                    preRows.size(), fi);
                            Misc.free(frozenCursor);

                            // --- UNFROZEN: reload resumes and picks up the new chain state. ---
                            reader.setFrozen(false);
                            reader.reloadConditionally();

                            assertTrue("unfrozen reload must advance the picked chainSequence",
                                    chainSequence(reader) > chainSeqBefore);

                            final LongList postRows = new LongList();
                            CoveringRowCursor post = (CoveringRowCursor) reader.getCursor(probeKey, 0, Long.MAX_VALUE, required);
                            while (post.hasNext()) {
                                postRows.add(post.next());
                            }
                            Misc.free(post);
                            assertTrue("unfrozen reload must surface the gens published while frozen"
                                            + " (pre=" + preRows.size() + ", post=" + postRows.size() + ")",
                                    postRows.size() > preRows.size());
                        }
                    }
                } finally {
                    Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static boolean anySparseGen(PostingGenLookup lookup) throws Exception {
        // active.anySparseGen gates whether cacheLookup can ever return a hit; a
        // test layout must produce sparse gens for the warm assertion to bite.
        Field activeF = lookup.getClass().getDeclaredField("active");
        activeF.setAccessible(true);
        Object active = activeF.get(lookup);
        Field anyF = active.getClass().getDeclaredField("anySparseGen");
        anyF.setAccessible(true);
        return anyF.getBoolean(active);
    }

    private static long cacheLookup(PostingGenLookup lookup, int key) {
        // cacheLookup() is public; it short-circuits to CACHE_NOT_PRESENT when
        // the active snapshot has no sparse gens, otherwise returns the key's slot.
        return lookup.cacheLookup(key);
    }

    private static long chainSequence(PostingIndexFwdReader reader) throws Exception {
        // chainSequence is the private seqlock stamp of the last picked chain
        // header on the abstract base; it advances on every applied reload.
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("chainSequence");
        f.setAccessible(true);
        return f.getLong(reader);
    }

    /**
     * Concurrent-path coverage for the BACKWARD reader's {@code getDetachedCursor}, which is
     * provided for API symmetry but is not exercised by the (forward-only) covered-decode
     * pipeline. Two threads each open a detached BWD cursor over the same warmed reader and the
     * same key and iterate in lockstep; each must reproduce the cold backward cursor's full
     * (descending) (rowId, coveredValue) sequence, and neither may push to the shared cursor pool.
     */
    @Test
    public void testTwoDetachedBwdCursorsSameKeyConcurrent() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "detached_bwd_posting";
                final int plen = path.size();

                final int keyCount = 4;
                final int extraGens = 4;
                final int sparseKeyCount = 2;
                final int baseRows = keyCount * 3;
                final int extraRowsPerGen = sparseKeyCount * 3;
                final int totalRows = baseRows + extraGens * extraRowsPerGen;

                final long colBytes = (long) totalRows * Long.BYTES;
                final long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0}, new int[]{3}, new int[]{1}, new int[]{ColumnType.LONG}, 1);
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
                    }

                    final RecordMetadata md = coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG});
                    final int[] keys = new int[keyCount];
                    for (int k = 0; k < keyCount; k++) {
                        keys[k] = k;
                    }
                    final int[] required = {0};
                    final int probeKey = 0;

                    // Cold single-threaded BACKWARD reference (descending row ids).
                    final LongList expectedRows = new LongList();
                    final LongList expectedVals = new LongList();
                    try (PostingIndexBwdReader cold = new PostingIndexBwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        CoveringRowCursor cc = (CoveringRowCursor) cold.getCursor(probeKey, 0, Long.MAX_VALUE, required);
                        while (cc.hasNext()) {
                            expectedRows.add(cc.next());
                            expectedVals.add(cc.getCoveredLong(0));
                        }
                        Misc.free(cc);
                    }
                    assertTrue("probe key must yield rows for a meaningful test", expectedRows.size() > 0);

                    try (PostingIndexBwdReader warm = new PostingIndexBwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0, md, EMPTY_CVR, 0)) {
                        warm.warmForKeys(keys, required);
                        final int poolSizeAfterWarm = bwdFreeCursorsSize(warm);

                        final CyclicBarrier barrier = new CyclicBarrier(2);
                        final AtomicReference<Throwable> error = new AtomicReference<>();
                        final Runnable worker = () -> {
                            try {
                                barrier.await();
                                CoveringRowCursor cc = (CoveringRowCursor)
                                        warm.getDetachedCursor(probeKey, 0, Long.MAX_VALUE, required);
                                try {
                                    int i = 0;
                                    while (cc.hasNext()) {
                                        long r = cc.next();
                                        long v = cc.getCoveredLong(0);
                                        assertTrue("detached bwd cursor produced more rows than cold (idx " + i + ")",
                                                i < expectedRows.size());
                                        assertEquals("bwd row id mismatch at idx " + i, expectedRows.getQuick(i), r);
                                        assertEquals("bwd covered value mismatch at idx " + i, expectedVals.getQuick(i), v);
                                        i++;
                                    }
                                    assertEquals("detached bwd cursor produced fewer rows than cold",
                                            expectedRows.size(), i);
                                } finally {
                                    cc.close();
                                }
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        };

                        Thread t1 = new Thread(worker, "detached-bwd-1");
                        Thread t2 = new Thread(worker, "detached-bwd-2");
                        t1.start();
                        t2.start();
                        t1.join();
                        t2.join();

                        Throwable t = error.get();
                        if (t != null) {
                            throw new AssertionError("concurrent detached bwd cursor failure", t);
                        }
                        assertEquals("detached bwd cursors must not push to freeCursors",
                                poolSizeAfterWarm, bwdFreeCursorsSize(warm));
                    }
                } finally {
                    Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static int bwdFreeCursorsSize(PostingIndexBwdReader reader) throws Exception {
        Field f = reader.getClass().getDeclaredField("freeCursors");
        f.setAccessible(true);
        return ((ObjList<?>) f.get(reader)).size();
    }

    private static RecordMetadata coveringMetadata(int[] coveredIndices, int[] coveredTypes) {
        int maxIdx = -1;
        for (int idx : coveredIndices) {
            if (idx > maxIdx) maxIdx = idx;
        }
        GenericRecordMetadata m = new GenericRecordMetadata();
        for (int i = 0; i <= maxIdx; i++) {
            int type = ColumnType.LONG;
            for (int j = 0; j < coveredIndices.length; j++) {
                if (coveredIndices[j] == i) {
                    type = coveredTypes[j];
                    break;
                }
            }
            m.add(new TableColumnMetadata("c" + i, type, IndexType.NONE, 0, false, null, i, false));
        }
        return m;
    }

    private static int freeCursorsSize(PostingIndexFwdReader reader) throws Exception {
        // freeCursors lives on the concrete reader, not the abstract base.
        Field f = reader.getClass().getDeclaredField("freeCursors");
        f.setAccessible(true);
        return ((ObjList<?>) f.get(reader)).size();
    }

    private static PostingGenLookup genLookupOf(PostingIndexFwdReader reader) throws Exception {
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("genLookup");
        f.setAccessible(true);
        return (PostingGenLookup) f.get(reader);
    }

    @SuppressWarnings("unchecked")
    private static long sidecarMmapSize(PostingIndexFwdReader reader, int includeIdx) throws Exception {
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("sidecarMems");
        f.setAccessible(true);
        ObjList<MemoryMR> mems = (ObjList<MemoryMR>) f.get(reader);
        if (includeIdx >= mems.size()) {
            return -1;
        }
        MemoryMR mem = mems.getQuick(includeIdx);
        return mem == null ? -1 : mem.size();
    }

    private static long valueMemSize(PostingIndexFwdReader reader) throws Exception {
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("valueMem");
        f.setAccessible(true);
        MemoryMR mem = (MemoryMR) f.get(reader);
        return mem.size();
    }

    private static long valueMemSizeField(PostingIndexFwdReader reader) throws Exception {
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("valueMemSize");
        f.setAccessible(true);
        return f.getLong(reader);
    }
}
