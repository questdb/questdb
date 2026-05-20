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

import io.questdb.cairo.ParquetFileCache;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Lifecycle and correctness tests for the engine-shared {@link ParquetFileCache}.
 * Deterministic, fast-running. Randomised / long-running soak tests live in
 * {@code io.questdb.test.cairo.fuzz.ParquetFileCacheFuzzTest}.
 */
public class ParquetFileCacheTest extends AbstractCairoTest {

    @org.junit.Before
    public void setUp() {
        super.setUp();
        // read_parquet refuses absolute / unsanitised paths unless input root is
        // set; the test root is the natural choice and matches every other
        // parquet test harness.
        inputRoot = root;
    }

    @Test
    public void testAcquireReleaseSameRefcounting() throws Exception {
        // Verifies the basic refcount contract: two acquires of the same path
        // return the same entry; the second release does not free until both
        // refs are returned. After full drain, the cache holds the entry in
        // LRU (refcount=0, eligible for eviction) and evictAllReleased frees it.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id, x::timestamp ts from long_sequence(8))");
            final String rel = "fuzz/refcount.parquet";
            writeParquet(rel, "t");

            final ParquetFileCache cache = engine.getParquetFileCache();
            try (Path p = parquetPath(rel)) {
                final ParquetFileCache.Entry first = cache.acquire(p, configuration.getFilesFacade());
                final ParquetFileCache.Entry second = cache.acquire(p, configuration.getFilesFacade());
                Assert.assertSame("same path must reuse entry", first, second);
                Assert.assertEquals(1, cache.getEntryCount());
                final long bytesAfterAcquire = cache.getCurrentBytes();
                Assert.assertTrue("entry has mapped bytes", bytesAfterAcquire > 0);

                cache.release(first);
                Assert.assertEquals("partial release keeps entry alive", 1, cache.getEntryCount());
                cache.release(second);
                // Entry is now refcount=0; still in LRU pending eviction.
                Assert.assertEquals(1, cache.getEntryCount());
                Assert.assertEquals(bytesAfterAcquire, cache.getCurrentBytes());
            }
            final int freed = engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(1, freed);
            Assert.assertEquals(0, cache.getEntryCount());
            Assert.assertEquals(0, cache.getCurrentBytes());
        });
    }

    @Test
    public void testEvictionUnderEntryCap() throws Exception {
        // Configure the cache (by direct construction) to a tiny entry cap and
        // verify it never grows beyond it during a sequential acquire+release
        // walk over more files than the cap. Tests the LRU+budget interplay
        // independently of the engine's defaults.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id, x::timestamp ts from long_sequence(4))");
            final int fileCount = 8;
            final int cap = 3;
            for (int i = 0; i < fileCount; i++) {
                writeParquet("fuzz/cap/file_" + i + ".parquet", "t");
            }

            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(Long.MAX_VALUE, cap))) {
                int peak = 0;
                for (int i = 0; i < fileCount; i++) {
                    try (Path p = parquetPath("fuzz/cap/file_" + i + ".parquet")) {
                        final ParquetFileCache.Entry e = local.acquire(p, configuration.getFilesFacade());
                        peak = Math.max(peak, local.getEntryCount());
                        local.release(e);
                    }
                }
                Assert.assertTrue("entry count must respect cap [peak=" + peak + ", cap=" + cap + ']', peak <= cap);
                // Drain any refcount=0 stragglers so the test-level leak check
                // doesn't blame this cache - local.close() below also handles it.
            }
        });
    }

    @Test
    public void testEvictionUnderByteCap() throws Exception {
        // Byte cap is the other half of the LRU contract. After two large
        // files, a third acquire should force eviction of the oldest
        // refcount=0 entry to make room.
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_str(64,128,0) s from long_sequence(128))");
            final int fileCount = 5;
            for (int i = 0; i < fileCount; i++) {
                writeParquet("fuzz/bytecap/file_" + i + ".parquet", "t");
            }
            // Discover one file's mapped size so we can pick a byte cap that
            // permits ~2 entries but forces eviction at the third.
            final long oneFileSize;
            try (Path p = parquetPath("fuzz/bytecap/file_0.parquet")) {
                oneFileSize = configuration.getFilesFacade().length(p.$());
            }
            Assert.assertTrue(oneFileSize > 0);

            final long byteCap = oneFileSize * 2 + 1;
            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(byteCap, Integer.MAX_VALUE))) {
                for (int i = 0; i < fileCount; i++) {
                    try (Path p = parquetPath("fuzz/bytecap/file_" + i + ".parquet")) {
                        final ParquetFileCache.Entry e = local.acquire(p, configuration.getFilesFacade());
                        local.release(e);
                    }
                    Assert.assertTrue(
                            "currentBytes must stay <= byteCap [i=" + i + ", cur=" + local.getCurrentBytes() + ']',
                            local.getCurrentBytes() <= byteCap
                    );
                }
            }
        });
    }

    @Test
    public void testEvictionDoesNotTouchPinnedEntry() throws Exception {
        // Pin a single entry by holding its acquire, then walk many other
        // files. The cache must NOT evict the pinned entry; it must evict
        // refcount=0 entries instead and possibly stay over the byte cap
        // until the pin is released.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id, x::timestamp ts from long_sequence(8))");
            final int fileCount = 6;
            for (int i = 0; i < fileCount; i++) {
                writeParquet("fuzz/pin/file_" + i + ".parquet", "t");
            }

            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(Long.MAX_VALUE, 2))) {
                ParquetFileCache.Entry pinned;
                try (Path p = parquetPath("fuzz/pin/file_0.parquet")) {
                    pinned = local.acquire(p, configuration.getFilesFacade());
                }
                // Cap is 2; cycle through the rest. Pinned must always be
                // present in the cache because refcount > 0.
                for (int i = 1; i < fileCount; i++) {
                    try (Path p = parquetPath("fuzz/pin/file_" + i + ".parquet")) {
                        ParquetFileCache.Entry e = local.acquire(p, configuration.getFilesFacade());
                        local.release(e);
                    }
                    // Pinned entry's path must still be retrievable - we test
                    // this indirectly via re-acquiring it and asserting the
                    // returned entry is the same instance.
                    try (Path p = parquetPath("fuzz/pin/file_0.parquet")) {
                        ParquetFileCache.Entry refetch = local.acquire(p, configuration.getFilesFacade());
                        Assert.assertSame("pinned entry must survive eviction sweeps", pinned, refetch);
                        local.release(refetch);
                    }
                }
                local.release(pinned);
            }
        });
    }

    @Test
    public void testStatValidateInvalidatesOnRewrite() throws Exception {
        // Acquire+release a file; rewrite it with different content (different
        // row count and hence different file size); next acquire must NOT
        // return the stale entry. We verify this by comparing the decoder's
        // reported row count to the new file's row count.
        assertMemoryLeak(() -> {
            execute("create table small as (select x id from long_sequence(2))");
            execute("create table large as (select x id from long_sequence(64))");
            final String rel = "fuzz/rewrite/swap.parquet";
            writeParquet(rel, "small");

            final ParquetFileCache cache = engine.getParquetFileCache();
            try (Path p = parquetPath(rel)) {
                final FilesFacade ff = configuration.getFilesFacade();
                final ParquetFileCache.Entry first = cache.acquire(p, ff);
                final long firstRows = first.decoder.metadata().getRowCount();
                Assert.assertEquals(2, firstRows);
                cache.release(first);

                // Rewrite the parquet at the same path with different content.
                ff.remove(p.$());
                writeParquet(rel, "large");

                final ParquetFileCache.Entry second = cache.acquire(p, ff);
                Assert.assertEquals("rewritten file must decode fresh row count",
                        64, second.decoder.metadata().getRowCount());
                cache.release(second);
            }
            // Reset the cache so the test-level leak check sees a clean slate.
            engine.getParquetFileCache().evictAllReleased();
        });
    }

    @Test
    public void testInvalidateWhileHeldIsOrphanedAndFreed() throws Exception {
        // Holder H acquires file A. Another caller rewrites A (different size)
        // and acquires - this triggers invalidate while H still has a ref.
        // The old entry is orphaned (inCache=false). When H finally releases,
        // its resources are freed and the cache's byte counter is unchanged
        // (the orphan was already subtracted at invalidation).
        assertMemoryLeak(() -> {
            execute("create table small as (select x id from long_sequence(2))");
            execute("create table large as (select x id from long_sequence(64))");
            final String rel = "fuzz/orphan/swap.parquet";
            writeParquet(rel, "small");

            final ParquetFileCache cache = engine.getParquetFileCache();
            final FilesFacade ff = configuration.getFilesFacade();
            ParquetFileCache.Entry holder;
            try (Path p = parquetPath(rel)) {
                holder = cache.acquire(p, ff);
            }
            // Old bytes still readable through holder.
            Assert.assertEquals(2, holder.decoder.metadata().getRowCount());

            // Rewrite under the holder. Old fd / mmap stay alive via holder's
            // ref; freed lazily by Files / MmapCache when no ref remains.
            try (Path p = parquetPath(rel)) {
                ff.remove(p.$());
            }
            writeParquet(rel, "large");

            // Second acquire stat-validates, sees a size mismatch, invalidates
            // the old entry (orphaning it because holder still has refcount=1),
            // and opens a fresh one.
            ParquetFileCache.Entry second;
            try (Path p = parquetPath(rel)) {
                second = cache.acquire(p, ff);
            }
            Assert.assertNotSame("invalidation must produce a fresh entry", holder, second);
            Assert.assertEquals(64, second.decoder.metadata().getRowCount());

            // Holder still observes old metadata - the orphan kept its decoder.
            Assert.assertEquals(2, holder.decoder.metadata().getRowCount());

            // Release the holder first: it should free the orphaned resources
            // and NOT touch the cache's currentBytes counter (already debited
            // at invalidation time). Release the new entry second so the cache
            // returns to a clean state.
            cache.release(holder);
            cache.release(second);
            engine.getParquetFileCache().evictAllReleased();
        });
    }

    @Test
    public void testTinyCapsAcquireReleaseStillFunctional() throws Exception {
        // Pathological caps: 1 entry / 1 byte. Every acquire must evict the
        // previous entry; the cache should still serve correct data, just
        // with zero reuse. Catches off-by-one eviction bugs and division-
        // by-zero hazards in the cap arithmetic.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id from long_sequence(4))");
            for (int i = 0; i < 4; i++) {
                writeParquet("tiny/file_" + i + ".parquet", "t");
            }
            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(1L, 1))) {
                final FilesFacade ff = configuration.getFilesFacade();
                for (int i = 0; i < 4; i++) {
                    try (Path p = parquetPath("tiny/file_" + i + ".parquet")) {
                        ParquetFileCache.Entry e = local.acquire(p, ff);
                        Assert.assertEquals(4L, e.decoder.metadata().getRowCount());
                        local.release(e);
                    }
                }
            }
        });
    }

    @Test
    public void testCursorErrorMidIterationReleasesEntry() throws Exception {
        // Simulate an error mid-iteration via TableReferenceOutOfDateException
        // by rewriting the parquet file with a different schema between the
        // factory compile and the cursor open. The factory's try/catch around
        // cursor.of must close the cursor, which must release the cache entry.
        // We verify by counting refcount=0 entries in the cache - if the
        // cursor failed to release, evictAllReleased would not be able to
        // reclaim that file's space.
        assertMemoryLeak(() -> {
            execute("create table src_int as (select cast(x as int) id from long_sequence(2))");
            execute("create table src_long as (select x id from long_sequence(2))");
            final String rel = "err/swap.parquet";
            writeParquet(rel, "src_int");

            io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
            io.questdb.cairo.sql.RecordCursorFactory factory = null;
            try {
                factory = compiler.compile(
                        "select id from read_parquet('" + rel + "')",
                        sqlExecutionContext
                ).getRecordCursorFactory();
                // Burn the first cursor so the factory's schema lock is set.
                try (io.questdb.cairo.sql.RecordCursor c = factory.getCursor(sqlExecutionContext)) {
                    while (c.hasNext()) {
                        c.getRecord().getInt(0);
                    }
                }
                // Rewrite the file with a different schema.
                writeParquet(rel, "src_long");
                // Second open should fail; cursor close in the catch must release.
                try (io.questdb.cairo.sql.RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected schema mismatch");
                } catch (io.questdb.cairo.sql.TableReferenceOutOfDateException expected) {
                    // expected
                }
            } finally {
                io.questdb.std.Misc.free(factory);
                io.questdb.std.Misc.free(compiler);
            }
            // Verify everything reclaims.
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals("cursor must release its acquired entry on error path",
                    0, engine.getParquetFileCache().getEntryCount());
        });
    }

    @Test
    public void testRepeatedFactoryCloseReopen() throws Exception {
        // Compile a factory, use it, close it, compile a new factory at the
        // SAME path, use it. The cache should serve the file across factory
        // lifecycles without leaks. Catches the case where factory close
        // accidentally tears down state the cache depends on.
        assertMemoryLeak(() -> {
            execute("create table src as (select x id from long_sequence(16))");
            final String rel = "reopen/file.parquet";
            writeParquet(rel, "src");

            for (int cycle = 0; cycle < 50; cycle++) {
                try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                     io.questdb.cairo.sql.RecordCursorFactory factory =
                             compiler.compile(
                                     "select count(*) from read_parquet('" + rel + "')",
                                     sqlExecutionContext
                             ).getRecordCursorFactory();
                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(16L, cursor.getRecord().getLong(0));
                }
            }
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(0, engine.getParquetFileCache().getEntryCount());
        });
    }

    /**
     * Helper: build an absolute path inside the test root for a relative
     * parquet path. Caller closes.
     */
    private Path parquetPath(String relativePath) {
        final Path p = new Path();
        p.of(root).concat(relativePath);
        return p;
    }

    private void writeParquet(String relativePath, String tableName) {
        FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                Path dir = new Path();
                PartitionDescriptor desc = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root);
            int start = path.size();
            int slash = -1;
            for (int i = 0; i < relativePath.length(); i++) {
                char c = relativePath.charAt(i);
                if (c == '/' || c == java.io.File.separatorChar) {
                    slash = i;
                }
            }
            if (slash >= 0) {
                dir.of(root).concat(relativePath.substring(0, slash));
                Assert.assertEquals(0, ff.mkdirs(dir.slash(), 493));
            }
            path.trimTo(start).concat(relativePath);
            PartitionEncoder.populateFromTableReader(reader, desc, 0);
            PartitionEncoder.encode(desc, path);
            Assert.assertTrue(io.questdb.std.Files.exists(path.$()));
        }
    }

    /**
     * Stand-alone CairoConfiguration that exposes only the parquet-cache
     * knobs. Used to construct a local cache with stress-test caps so the
     * tests do not depend on engine defaults, and the local cache's close()
     * drains all entries regardless of refcount so the surrounding leak
     * check sees a clean state.
     */
    private static final class SmallCacheConfig extends io.questdb.cairo.DefaultCairoConfiguration {
        private final long maxBytes;
        private final int maxEntries;

        SmallCacheConfig(long maxBytes, int maxEntries) {
            super("/tmp/parquet-cache-test"); // root is unused by the cache
            this.maxBytes = maxBytes;
            this.maxEntries = maxEntries;
        }

        @Override
        public long getParquetCacheMaxBytes() {
            return maxBytes;
        }

        @Override
        public int getParquetCacheMaxEntries() {
            return maxEntries;
        }
    }
}
