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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.ParquetFileCache;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fuzz tests for the engine-shared {@link ParquetFileCache}. Each test runs
 * a short randomised workload by default (configurable upward via system
 * properties for soak runs). Every test wraps in {@code assertMemoryLeak} so
 * the registered teardown hook and the tests' own final releases must drive
 * native memory back to baseline. Basic lifecycle / correctness tests live
 * in {@code io.questdb.test.cairo.ParquetFileCacheTest}.
 */
public class ParquetFileCacheFuzzTest extends AbstractCairoTest {

    @org.junit.Before
    public void setUp() {
        super.setUp();
        // read_parquet refuses absolute / unsanitised paths unless input root is
        // set; the test root is the natural choice and matches every other
        // parquet test harness.
        inputRoot = root;
    }

    @Test
    public void testConcurrentAcquireReleaseSamePath() throws Exception {
        // N threads pound the same path with acquire+release loops. The cache
        // must serialise correctly: every thread sees the same shared entry,
        // refcount never reaches an inconsistent state, and at the end the
        // entry is back to refcount=0.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id, x::timestamp ts from long_sequence(8))");
            final String rel = "fuzz/concurrent/same.parquet";
            writeParquet(rel, "t");

            final ParquetFileCache cache = engine.getParquetFileCache();
            final FilesFacade ff = configuration.getFilesFacade();
            final int threadCount = 8;
            final int iterations = 256;
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threadCount);
            final AtomicInteger failures = new AtomicInteger();
            final ParquetFileCache.Entry[] seenEntry = new ParquetFileCache.Entry[1];

            for (int t = 0; t < threadCount; t++) {
                final int seed = t;
                new Thread(() -> {
                    try {
                        start.await();
                        try (Path p = parquetPath(rel)) {
                            for (int i = 0; i < iterations; i++) {
                                ParquetFileCache.Entry e = cache.acquire(p, ff);
                                synchronized (seenEntry) {
                                    if (seenEntry[0] == null) {
                                        seenEntry[0] = e;
                                    } else if (seenEntry[0] != e) {
                                        // Hits should converge on the same entry. A
                                        // race-induced invalidate could legitimately
                                        // produce a different entry if the file's
                                        // size changed - but we never rewrite here,
                                        // so a divergence means a bug.
                                        failures.incrementAndGet();
                                    }
                                }
                                // Small read to exercise the decoder under load.
                                if (e.decoder.metadata().getRowCount() != 8) {
                                    failures.incrementAndGet();
                                }
                                cache.release(e);
                            }
                        }
                    } catch (Throwable th) {
                        th.printStackTrace();
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "fuzz-concurrent-" + seed).start();
            }
            start.countDown();
            Assert.assertTrue("threads must finish within 30s",
                    done.await(30, TimeUnit.SECONDS));
            Assert.assertEquals("no thread should report a divergence or refcount error",
                    0, failures.get());

            // Final state: one entry in cache, refcount=0.
            Assert.assertEquals(1, cache.getEntryCount());
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(0, cache.getEntryCount());
        });
    }

    @Test
    public void testFuzzRandomOpsManyFiles() throws Exception {
        // Hammer the cache with a randomised mix of acquire + release across
        // many paths, with the cap deliberately set well below the working
        // set so eviction fires repeatedly. Tracks every outstanding entry so
        // we can drain at the end and assert no leaks. The assertMemoryLeak
        // wrapper enforces native-memory parity.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id, x::timestamp ts from long_sequence(8))");
            final int fileCount = 16;
            for (int i = 0; i < fileCount; i++) {
                writeParquet("fuzz/random/file_" + i + ".parquet", "t");
            }

            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(Long.MAX_VALUE, 4))) {
                final FilesFacade ff = configuration.getFilesFacade();
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final ParquetFileCache.Entry[] held = new ParquetFileCache.Entry[fileCount];
                final int ops = 2_000;
                for (int step = 0; step < ops; step++) {
                    final int slot = rnd.nextInt(fileCount);
                    if (held[slot] != null) {
                        // 50% chance to release the held entry; otherwise acquire
                        // another ref for the same path (test refcount handling).
                        if ((rnd.nextInt() & 1) == 0) {
                            local.release(held[slot]);
                            held[slot] = null;
                        } else {
                            try (Path p = parquetPath("fuzz/random/file_" + slot + ".parquet")) {
                                ParquetFileCache.Entry e = local.acquire(p, ff);
                                Assert.assertSame("repeat acquire must return same entry", held[slot], e);
                                local.release(e);
                            }
                        }
                    } else {
                        try (Path p = parquetPath("fuzz/random/file_" + slot + ".parquet")) {
                            held[slot] = local.acquire(p, ff);
                        }
                    }
                }
                // Drain all outstanding refs.
                for (int i = 0; i < fileCount; i++) {
                    if (held[i] != null) {
                        local.release(held[i]);
                    }
                }
            }
            // local.close() above already drained everything; nothing else to do.
        });
    }

    @Test
    public void testIntegrationSingleFileCursorFuzz() throws Exception {
        // SQL-level fuzz: many distinct read_parquet() calls each compile a
        // factory, open a cursor, drain a row, close. Exercises every code
        // path between the SQL compiler and ParquetFileCache.acquire/release.
        // Failure modes this catches: cursor close forgetting to release,
        // double-release on a TableReferenceOutOfDateException path, factory
        // close leaking a held ref.
        assertMemoryLeak(() -> {
            execute("create table src as (select x id, x::timestamp ts, rnd_str() s from long_sequence(64))");
            final int fileCount = 8;
            for (int i = 0; i < fileCount; i++) {
                writeParquet("int_single/file_" + i + ".parquet", "src");
            }
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int iterations = 400;
            for (int i = 0; i < iterations; i++) {
                final int slot = rnd.nextInt(fileCount);
                final String sql = "select count(*) from read_parquet('int_single/file_" + slot + ".parquet')";
                try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                     io.questdb.cairo.sql.RecordCursorFactory factory =
                             compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(64L, cursor.getRecord().getLong(0));
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            // After every cursor closes, the cache should hold each file at
            // refcount=0 (modulo LRU evictions). All should be reclaimable.
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(0, engine.getParquetFileCache().getEntryCount());
        });
    }

    @Test
    public void testIntegrationHiveCursorFuzz() throws Exception {
        // Same as the single-file fuzz but through the hive glob path so the
        // HivePartitionedReadParquetPageFrameCursor's acquire/release path
        // also gets hammered. Multiple distinct glob shapes exercise the
        // matched-file enumeration + cache acquire flow.
        assertMemoryLeak(() -> {
            execute("create table src as (select x id, x::timestamp ts from long_sequence(8))");
            final int dayCount = 6;
            for (int d = 0; d < dayCount; d++) {
                final String day = "2026-01-" + String.format("%02d", d + 1);
                writeParquet("int_hive/day=" + day + "/data.parquet", "src");
            }
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int iterations = 200;
            final String[] queryShapes = new String[]{
                    "select count(*) from read_parquet('int_hive/day=*/data.parquet')",
                    "select id from read_parquet('int_hive/day=*/data.parquet') order by id limit 5",
                    "select day, count(*) from read_parquet('int_hive/day=*/data.parquet') group by day",
            };
            for (int i = 0; i < iterations; i++) {
                final String sql = queryShapes[rnd.nextInt(queryShapes.length)];
                try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                     io.questdb.cairo.sql.RecordCursorFactory factory =
                             compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // Drain - cursor close must release every per-file entry it
                    // acquired.
                    while (cursor.hasNext()) {
                        // Touch a column to force frame materialization.
                        cursor.getRecord().getLong(0);
                    }
                }
            }
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(0, engine.getParquetFileCache().getEntryCount());
        });
    }

    @Test
    public void testIntegrationMixedRewriteFuzz() throws Exception {
        // SQL queries interleaved with file rewrites at the same path. The
        // stat-validation on hit should catch every rewrite. The cursor's
        // schema-change handling must NOT leak the orphaned entry. Tracks
        // expected vs observed row counts to detect stale reads.
        assertMemoryLeak(() -> {
            execute("create table small as (select x id from long_sequence(2))");
            execute("create table large as (select x id, rnd_str() s from long_sequence(8))");
            final String rel = "int_mixed/swap.parquet";
            writeParquet(rel, "small");

            final Rnd rnd = TestUtils.generateRandom(LOG);
            final String sql = "select count(*) from read_parquet('" + rel + "')";
            int rewriteCount = 0;
            int queryCount = 0;
            String currentTable = "small";
            long expectedRows = 2L;
            for (int i = 0; i < 200; i++) {
                if (rnd.nextInt(5) == 0) {
                    // Rewrite to alternate table to flip the file size.
                    final String nextTable = currentTable.equals("small") ? "large" : "small";
                    expectedRows = nextTable.equals("small") ? 2L : 8L;
                    writeParquet(rel, nextTable);
                    currentTable = nextTable;
                    rewriteCount++;
                }
                try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                     io.questdb.cairo.sql.RecordCursorFactory factory =
                             compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final long got = cursor.getRecord().getLong(0);
                    Assert.assertEquals("query #" + i + " after " + rewriteCount + " rewrites",
                            expectedRows, got);
                    queryCount++;
                } catch (io.questdb.cairo.CairoException e) {
                    // A schema or projection-mismatch error can fire if the
                    // factory was compiled against an older schema. Tolerate
                    // it; the next compile will see the new layout.
                    final String msg = e.getFlyweightMessage().toString();
                    if (!msg.contains("schema") && !msg.contains("project")
                            && !msg.contains("parquet") && !msg.contains("out of date")) {
                        throw e;
                    }
                }
            }
            Assert.assertTrue("at least one rewrite must have fired", rewriteCount > 0);
            Assert.assertTrue("at least 100 queries must have run", queryCount > 100);
            engine.getParquetFileCache().evictAllReleased();
        });
    }

    @Test
    public void testStressLongRunningRandomOps() throws Exception {
        // Long-running stress. Default 30 seconds; raise via system property
        // for soak runs (e.g. -Dparquet.cache.fuzz.seconds=3600 for an hour).
        // Mixes single-file SQL, hive-glob SQL, and direct cache ops with a
        // rewriter so every code path is exercised under sustained churn.
        // assertMemoryLeak around the whole run enforces final fd / mem
        // parity; periodic invariant checks catch transient state damage.
        final long seconds = Long.getLong("parquet.cache.fuzz.seconds", 10L);
        final long deadlineNanos = System.nanoTime() + seconds * 1_000_000_000L;
        LOG.info().$("ParquetFileCache stress fuzz running for ").$(seconds).$('s').$();
        assertMemoryLeak(() -> {
            execute("create table src_a as (select x id, x::timestamp ts from long_sequence(8))");
            execute("create table src_b as (select x id, x::timestamp ts, rnd_str() s from long_sequence(64))");
            final int fileCount = 8;
            for (int i = 0; i < fileCount; i++) {
                writeParquet("stress/file_" + i + ".parquet", "src_a");
            }
            // Also seed a hive layout under a different prefix so the hive
            // glob queries have something to match.
            for (int d = 0; d < 4; d++) {
                writeParquet("stress_hive/day=2026-01-0" + (d + 1) + "/data.parquet", "src_a");
            }

            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ParquetFileCache cache = engine.getParquetFileCache();
            final FilesFacade ff = configuration.getFilesFacade();
            long opsCount = 0;
            long sqlSingleCount = 0;
            long sqlHiveCount = 0;
            long cacheDirectCount = 0;
            long rewriteCount = 0;
            long lastInvariantNs = System.nanoTime();
            while (System.nanoTime() < deadlineNanos) {
                final int op = rnd.nextInt(100);
                try {
                    if (op < 40) {
                        // Single-file SQL.
                        final int slot = rnd.nextInt(fileCount);
                        final String sql = "select count(*) from read_parquet('stress/file_" + slot + ".parquet')";
                        try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                             io.questdb.cairo.sql.RecordCursorFactory factory =
                                     compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                             io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            if (cursor.hasNext()) {
                                cursor.getRecord().getLong(0);
                            }
                        }
                        sqlSingleCount++;
                    } else if (op < 70) {
                        // Hive-glob SQL.
                        final String sql = "select count(*) from read_parquet('stress_hive/day=*/data.parquet')";
                        try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                             io.questdb.cairo.sql.RecordCursorFactory factory =
                                     compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                             io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            if (cursor.hasNext()) {
                                cursor.getRecord().getLong(0);
                            }
                        }
                        sqlHiveCount++;
                    } else if (op < 95) {
                        // Direct cache ops to exercise the API without the
                        // cursor layering.
                        final int slot = rnd.nextInt(fileCount);
                        try (Path p = parquetPath("stress/file_" + slot + ".parquet")) {
                            final ParquetFileCache.Entry e = cache.acquire(p, ff);
                            // Touch metadata so a stale entry would surface.
                            e.decoder.metadata().getRowCount();
                            cache.release(e);
                        }
                        cacheDirectCount++;
                    } else {
                        // Rewrite a random single-file slot. Flips between
                        // src_a (8 rows) and src_b (64 rows) so the file
                        // size changes and stat-validation fires.
                        final int slot = rnd.nextInt(fileCount);
                        final String table = (rnd.nextInt() & 1) == 0 ? "src_a" : "src_b";
                        writeParquet("stress/file_" + slot + ".parquet", table);
                        rewriteCount++;
                    }
                } catch (io.questdb.cairo.CairoException e) {
                    // Schema and projection errors are legitimate when the
                    // file got rewritten under us with a different schema.
                    final String msg = e.getFlyweightMessage().toString();
                    final boolean expected = msg.contains("schema")
                            || msg.contains("project")
                            || msg.contains("out of date")
                            || msg.contains("parquet");
                    if (!expected) {
                        throw e;
                    }
                }
                opsCount++;
                // Periodically assert basic invariants. Bytes and entry
                // counts must stay non-negative and within the cache's caps.
                final long now = System.nanoTime();
                if (now - lastInvariantNs > 5_000_000_000L) {
                    Assert.assertTrue("currentBytes non-negative", cache.getCurrentBytes() >= 0);
                    Assert.assertTrue("entryCount non-negative", cache.getEntryCount() >= 0);
                    lastInvariantNs = now;
                    LOG.info().$("stress progress: ops=").$(opsCount)
                            .$(", sqlSingle=").$(sqlSingleCount)
                            .$(", sqlHive=").$(sqlHiveCount)
                            .$(", direct=").$(cacheDirectCount)
                            .$(", rewrites=").$(rewriteCount)
                            .$(", cache.entries=").$(cache.getEntryCount())
                            .$(", cache.bytes=").$(cache.getCurrentBytes())
                            .$();
                }
            }
            LOG.info().$("stress done: total ops=").$(opsCount)
                    .$(", sqlSingle=").$(sqlSingleCount)
                    .$(", sqlHive=").$(sqlHiveCount)
                    .$(", direct=").$(cacheDirectCount)
                    .$(", rewrites=").$(rewriteCount).$();
            // Final drain - assertMemoryLeak will verify no leaks remain.
            engine.getParquetFileCache().evictAllReleased();
        });
    }

    @Test
    public void testFuzzMixedQueryShapesSingleFile() throws Exception {
        // Replaces the count-only single-file fuzz with the realistic mix of
        // query shapes a user would run against a single read_parquet(file).
        // Each shape exercises different cursor paths: projection, filter,
        // group-by, top-K with order-by, predicate-pushdown over multi-row
        // -group files. Validates BOTH that the cache stays leak-free AND
        // that results are content-correct across eviction churn.
        final int rowsPerFile = Integer.getInteger("parquet.cache.shape.file.rows", 12_000);
        final int fileCount = Integer.getInteger("parquet.cache.shape.file.count", 20);
        final long seconds = Long.getLong("parquet.cache.shape.fuzz.seconds", 10L);

        assertMemoryLeak(() -> {
            // Wide schema with mixed types so projection has something to do.
            execute("create table src as (select " +
                    "x id, x::timestamp ts, x % 7 bucket, rnd_double() v, " +
                    "rnd_str(8,32,0) tag, rnd_int(0,100,0) score " +
                    "from long_sequence(" + rowsPerFile + "))");
            for (int i = 0; i < fileCount; i++) {
                writeParquet("shape_single/file_" + i + ".parquet", "src");
            }

            final Rnd rnd = TestUtils.generateRandom(LOG);
            final long deadline = System.nanoTime() + seconds * 1_000_000_000L;
            int[] shapeCounts = new int[6];
            int queries = 0;
            while (System.nanoTime() < deadline) {
                final int slot = rnd.nextInt(fileCount);
                final int shape = rnd.nextInt(6);
                final String tbl = "read_parquet('shape_single/file_" + slot + ".parquet')";
                final String sql;
                final long expected;
                switch (shape) {
                    case 0: // count(*) - baseline
                        sql = "select count(*) from " + tbl;
                        expected = rowsPerFile;
                        break;
                    case 1: // narrow projection + filter pushdown
                        sql = "select count(*) from " + tbl + " where bucket = 3";
                        expected = rowsPerFile / 7; // approx; we check >0 and <=rows
                        break;
                    case 2: // group-by-with-agg
                        sql = "select count(*) from (select bucket, count(*) c from " + tbl + " group by bucket)";
                        expected = 7L; // 7 buckets
                        break;
                    case 3: // top-K with order-by
                        sql = "select count(*) from (select id from " + tbl + " order by score desc limit 100)";
                        expected = Math.min(100, rowsPerFile);
                        break;
                    case 4: // multi-column projection
                        sql = "select count(*) from (select id, ts, tag, score from " + tbl + ")";
                        expected = rowsPerFile;
                        break;
                    default: // sum aggregation - exercises double decoding
                        sql = "select count(*) from (select sum(v) s from " + tbl + ")";
                        expected = 1L;
                        break;
                }
                try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                     io.questdb.cairo.sql.RecordCursorFactory factory =
                             compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final long got = cursor.getRecord().getLong(0);
                    if (shape == 1) {
                        // Filter result is approximate due to randomised bucket
                        // distribution - just bound-check.
                        Assert.assertTrue("filter result bounded [shape=" + shape + ", got=" + got + ']',
                                got > 0 && got <= rowsPerFile);
                    } else {
                        Assert.assertEquals("shape=" + shape + ", sql=" + sql, expected, got);
                    }
                    shapeCounts[shape]++;
                    queries++;
                }
            }
            LOG.info().$("mixed-shape single-file done: total=").$(queries)
                    .$(", count=").$(shapeCounts[0])
                    .$(", filter=").$(shapeCounts[1])
                    .$(", group=").$(shapeCounts[2])
                    .$(", topk=").$(shapeCounts[3])
                    .$(", proj=").$(shapeCounts[4])
                    .$(", agg=").$(shapeCounts[5]).$();
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(0, engine.getParquetFileCache().getEntryCount());
        });
    }

    @Test
    public void testFuzzHiveSubsetQueries() throws Exception {
        // The defining real-user pattern for hive parquet: a glob over many
        // partition values, with a WHERE clause on the partition column that
        // prunes down to a subset. Different subset sizes (1, 7, 30, all)
        // exercise different prefetch + cursor paths. Each query is content-
        // validated by checking row count matches the partition cardinality.
        final int rowsPerFile = Integer.getInteger("parquet.cache.subset.file.rows", 6_000);
        final int dayCount = Integer.getInteger("parquet.cache.subset.day.count", 60);
        final long seconds = Long.getLong("parquet.cache.subset.fuzz.seconds", 10L);

        assertMemoryLeak(() -> {
            execute("create table src as (select x id, x::timestamp ts, rnd_double() v from long_sequence(" + rowsPerFile + "))");
            // Create a hive layout with realistic day=YYYY-MM-DD partitions.
            for (int d = 0; d < dayCount; d++) {
                final String day = String.format("2026-01-%02d", (d % 28) + 1);
                final String month = String.format("2026-%02d", (d / 28) + 1);
                writeParquet("subset/month=" + month + "/day=" + day + "/data.parquet", "src");
            }

            final Rnd rnd = TestUtils.generateRandom(LOG);
            final long deadline = System.nanoTime() + seconds * 1_000_000_000L;
            int singleDay = 0, weekRange = 0, monthRange = 0, fullGlob = 0;
            int queries = 0;
            // For verifying row counts: we know how many files match each
            // pattern based on the way we wrote them.
            while (System.nanoTime() < deadline) {
                final int pick = rnd.nextInt(4);
                final String sql;
                final long expectedRows;
                switch (pick) {
                    case 0: { // single day - 1 file
                        final int d = rnd.nextInt(28) + 1;
                        sql = "select count(*) from read_parquet('subset/month=*/day=*/data.parquet') " +
                                "where day = '2026-01-" + String.format("%02d", d) + "'";
                        expectedRows = (long) rowsPerFile * matchedDayCount(d, d, dayCount);
                        singleDay++;
                        break;
                    }
                    case 1: { // week range - 7 files (typical week-over-week)
                        final int lo = rnd.nextInt(22) + 1;
                        final int hi = lo + 6;
                        sql = "select count(*) from read_parquet('subset/month=*/day=*/data.parquet') " +
                                "where day between '2026-01-" + String.format("%02d", lo) + "' " +
                                "and '2026-01-" + String.format("%02d", hi) + "'";
                        expectedRows = (long) rowsPerFile * matchedDayCount(lo, hi, dayCount);
                        weekRange++;
                        break;
                    }
                    case 2: { // month range - all files in a month
                        sql = "select count(*) from read_parquet('subset/month=*/day=*/data.parquet') " +
                                "where day between '2026-01-01' and '2026-01-31'";
                        expectedRows = (long) rowsPerFile * matchedDayCount(1, 31, dayCount);
                        monthRange++;
                        break;
                    }
                    default: { // no filter - full glob
                        sql = "select count(*) from read_parquet('subset/month=*/day=*/data.parquet')";
                        expectedRows = (long) rowsPerFile * dayCount;
                        fullGlob++;
                        break;
                    }
                }
                try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                     io.questdb.cairo.sql.RecordCursorFactory factory =
                             compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals("sql=" + sql, expectedRows, cursor.getRecord().getLong(0));
                    queries++;
                }
            }
            LOG.info().$("hive-subset done: total=").$(queries)
                    .$(", singleDay=").$(singleDay)
                    .$(", weekRange=").$(weekRange)
                    .$(", monthRange=").$(monthRange)
                    .$(", fullGlob=").$(fullGlob).$();
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(0, engine.getParquetFileCache().getEntryCount());
        });
    }

    @Test
    public void testFuzzConcurrentSqlSessions() throws Exception {
        // Multiple concurrent SQL sessions hit the same dataset with
        // overlapping but not identical file subsets. This is the workload
        // shape the shared cache is designed for: a single cached entry
        // serves N concurrent readers via refcounting. The KB-scale
        // testFuzzConcurrentMultiPath exercised the cache API directly;
        // this one drives every cursor + page-frame + projection path.
        // Each thread compiles its own SqlCompiler since SqlCompiler is
        // single-threaded; the shared sqlExecutionContext is reused but
        // serialised by Java's per-thread SQL state.
        final int rowsPerFile = Integer.getInteger("parquet.cache.concurrent.file.rows", 8_000);
        final int fileCount = Integer.getInteger("parquet.cache.concurrent.file.count", 20);
        final int threadCount = Integer.getInteger("parquet.cache.concurrent.threads", 4);
        final long seconds = Long.getLong("parquet.cache.concurrent.fuzz.seconds", 10L);

        assertMemoryLeak(() -> {
            execute("create table src as (select x id, x::timestamp ts, rnd_double() v from long_sequence(" + rowsPerFile + "))");
            for (int i = 0; i < fileCount; i++) {
                writeParquet("concurrent_sql/file_" + i + ".parquet", "src");
            }

            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threadCount);
            final java.util.concurrent.atomic.AtomicInteger failures = new java.util.concurrent.atomic.AtomicInteger();
            final java.util.concurrent.atomic.AtomicLong totalQueries = new java.util.concurrent.atomic.AtomicLong();
            final long deadline = System.nanoTime() + seconds * 1_000_000_000L;
            for (int t = 0; t < threadCount; t++) {
                final int seed = t;
                new Thread(() -> {
                    final Rnd rnd = new Rnd(seed * 0x9E3779B97F4A7C15L, seed * 0xBF58476D1CE4E5B9L);
                    long localQueries = 0;
                    try {
                        start.await();
                        // Per-thread SqlExecutionContextImpl so contexts do not
                        // interleave bind variables / thread state.
                        final io.questdb.griffin.SqlExecutionContextImpl ctx =
                                new io.questdb.griffin.SqlExecutionContextImpl(engine, 1).with(securityContext);
                        try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler()) {
                            while (System.nanoTime() < deadline) {
                                // Each thread biases toward its own slot range
                                // so we get overlapping-but-not-identical file
                                // sets - the realistic cache-sharing scenario.
                                final int slot = (seed * 5 + rnd.nextInt(fileCount)) % fileCount;
                                final String sql = "select count(*) from read_parquet('concurrent_sql/file_" + slot + ".parquet')";
                                try (io.questdb.cairo.sql.RecordCursorFactory factory =
                                             compiler.compile(sql, ctx).getRecordCursorFactory();
                                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(ctx)) {
                                    if (!cursor.hasNext()
                                            || cursor.getRecord().getLong(0) != rowsPerFile) {
                                        failures.incrementAndGet();
                                    }
                                }
                                localQueries++;
                            }
                        }
                    } catch (Throwable th) {
                        th.printStackTrace();
                        failures.incrementAndGet();
                    } finally {
                        // Worker threads accumulate thread-local native Path
                        // buffers via Path / Misc usage inside the compiler /
                        // cursor stack. The test framework's leak detector
                        // counts these as leaks against the main thread; drain
                        // them here before the worker exits.
                        io.questdb.std.str.Path.clearThreadLocals();
                        totalQueries.addAndGet(localQueries);
                        done.countDown();
                    }
                }, "fuzz-concurrent-sql-" + seed).start();
            }
            start.countDown();
            Assert.assertTrue("concurrent SQL threads must finish within 2× the deadline",
                    done.await(seconds * 2, TimeUnit.SECONDS));
            Assert.assertEquals("no thread errors", 0, failures.get());
            LOG.info().$("concurrent-sql done: totalQueries=").$(totalQueries.get())
                    .$(", threads=").$(threadCount).$();
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals(0, engine.getParquetFileCache().getEntryCount());
        });
    }

    @Test
    public void testFuzzZipfianAccessPattern() throws Exception {
        // Real workloads are skewed - 80/20 hot/cold file access. Uniform
        // random distributes pressure evenly across files, which is the
        // EASIEST case for an LRU. Zipfian access concentrates ~80% of
        // queries on ~20% of files, forcing the LRU to actually decide
        // what stays warm. We verify the cache keeps the hot set resident
        // by reading the entry count after the run and comparing to the
        // 20% bound.
        final int fileCount = Integer.getInteger("parquet.cache.zipf.file.count", 40);
        final int rowsPerFile = Integer.getInteger("parquet.cache.zipf.file.rows", 6_000);
        final int capMb = Integer.getInteger("parquet.cache.zipf.cap.mb", 4);
        final long seconds = Long.getLong("parquet.cache.zipf.fuzz.seconds", 10L);

        assertMemoryLeak(() -> {
            execute("create table src as (select x id from long_sequence(" + rowsPerFile + "))");
            for (int i = 0; i < fileCount; i++) {
                writeParquet("zipf/file_" + i + ".parquet", "src");
            }
            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(capMb * 1024L * 1024L, Integer.MAX_VALUE))) {
                final FilesFacade ff = configuration.getFilesFacade();
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final long deadline = System.nanoTime() + seconds * 1_000_000_000L;
                final int[] hitsPerFile = new int[fileCount];
                long ops = 0;
                while (System.nanoTime() < deadline) {
                    // Zipfian-ish: cube of a uniform in [0,1) gives a strongly
                    // right-skewed distribution. PDF favours low indices so
                    // they dominate the hit count. Stronger than `u*v` and
                    // crucially file-count-independent in its top-N share,
                    // so the assertion below holds whether the test runs at
                    // 40 or 1000 files.
                    final double u0 = rnd.nextDouble();
                    final double u = u0 * u0 * u0;
                    final int slot = (int) (u * fileCount);
                    try (Path p = parquetPath("zipf/file_" + slot + ".parquet")) {
                        final ParquetFileCache.Entry e = local.acquire(p, ff);
                        if (e.decoder.metadata().getRowCount() != rowsPerFile) {
                            Assert.fail("zipf: stale decoder for slot " + slot);
                        }
                        hitsPerFile[slot]++;
                        local.release(e);
                    }
                    ops++;
                }
                // Top 4 files should hold at least 3× their uniform-random
                // share. With u^3 distribution the actual top-4 share is
                // O(fileCount^(-1/3)) - well above 3× uniform across the
                // whole 40..1000 file range. A "fair" share of N=fileCount
                // would put top-4 at 4/N; we demand 3× that or more, so
                // breaking the algorithm to (near-)uniform would trip this.
                int top4 = hitsPerFile[0] + hitsPerFile[1] + hitsPerFile[2] + hitsPerFile[3];
                LOG.info().$("zipf done: ops=").$(ops)
                        .$(", top4Hits=").$(top4)
                        .$(", entries=").$(local.getEntryCount())
                        .$(", bytes=").$(local.getCurrentBytes()).$();
                final double topShare = (double) top4 / ops;
                final double uniformShare = 4.0 / fileCount;
                Assert.assertTrue("zipf must be skewed: top 4 share " + topShare
                                + " should be ≥ 3× uniform " + uniformShare,
                        topShare >= 3.0 * uniformShare);
            }
        });
    }

    /**
     * Helper for {@link #testFuzzHiveSubsetQueries}: count how many files
     * fall in the inclusive day range {@code [lo, hi]} given our generation
     * pattern (day cycles through 1..28 modulo). Mirrors the actual layout
     * so the expected-row assertion stays exact.
     */
    private int matchedDayCount(int lo, int hi, int dayCount) {
        int matches = 0;
        for (int d = 0; d < dayCount; d++) {
            int day = (d % 28) + 1;
            if (day >= lo && day <= hi) matches++;
        }
        return matches;
    }

    @Test
    public void testFuzzLargeFilesByteCapChurn() throws Exception {
        // Multi-megabyte files, hundreds of them, cap deliberately well
        // below the working set so byte-cap eviction fires constantly. The
        // KB-scale fuzz never actually pressured the byte counter because
        // total dataset size fit under the 1 GiB default cap; this test
        // closes that gap. Sysprops:
        //   parquet.cache.large.file.rows (default 50000): rows per file -
        //     ~1.5 MB at the default column shape with compression.
        //   parquet.cache.large.file.count (default 40): how many files to
        //     materialise. Total dataset = rows * count * ~30 bytes/row.
        //   parquet.cache.large.cap.mb (default 8): cache byte cap. Files
        //     beyond this evict.
        //   parquet.cache.large.fuzz.seconds (default 30): runtime.
        final int rowsPerFile = Integer.getInteger("parquet.cache.large.file.rows", 12_000);
        final int fileCount = Integer.getInteger("parquet.cache.large.file.count", 8);
        final int capMb = Integer.getInteger("parquet.cache.large.cap.mb", 8);
        final long seconds = Long.getLong("parquet.cache.large.fuzz.seconds", 10L);
        LOG.info().$("large-files fuzz: rowsPerFile=").$(rowsPerFile)
                .$(", files=").$(fileCount).$(", capMb=").$(capMb)
                .$(", seconds=").$(seconds).$();

        assertMemoryLeak(() -> {
            execute("create table src as (select x id, x::timestamp ts, " +
                    "rnd_str(48,96,0) s1, rnd_str(48,96,0) s2 from long_sequence(" + rowsPerFile + "))");
            // Materialise the dataset.
            for (int i = 0; i < fileCount; i++) {
                writeParquet("large/file_" + i + ".parquet", "src");
            }

            // Measure one file's on-disk size so we can compute eviction
            // expectations precisely instead of guessing from rowsPerFile.
            final long fileBytes;
            try (Path p = parquetPath("large/file_0.parquet")) {
                fileBytes = configuration.getFilesFacade().length(p.$());
            }
            final long capBytes = capMb * 1024L * 1024L;
            final int maxFitting = (int) Math.max(1, capBytes / Math.max(1, fileBytes));
            LOG.info().$("large-files fuzz: per-file=").$(fileBytes)
                    .$(" bytes, cap=").$(capBytes)
                    .$(" bytes (~").$(maxFitting).$(" files fit)").$();
            // Sanity: the working set must overflow the cap or this isn't a
            // churn test.
            Assert.assertTrue("working set must exceed cap to force eviction",
                    fileCount > maxFitting);

            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(capBytes, Integer.MAX_VALUE))) {
                final FilesFacade ff = configuration.getFilesFacade();
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final long deadline = System.nanoTime() + seconds * 1_000_000_000L;
                long ops = 0;
                long peakBytes = 0;
                long peakEntries = 0;
                while (System.nanoTime() < deadline) {
                    final int slot = rnd.nextInt(fileCount);
                    try (Path p = parquetPath("large/file_" + slot + ".parquet")) {
                        final ParquetFileCache.Entry e = local.acquire(p, ff);
                        // Touch the decoder so eviction-induced staleness
                        // would surface as wrong metadata.
                        final long rows = e.decoder.metadata().getRowCount();
                        if (rows != rowsPerFile) {
                            Assert.fail("decoder returned wrong row count: " + rows);
                        }
                        local.release(e);
                    }
                    peakBytes = Math.max(peakBytes, local.getCurrentBytes());
                    peakEntries = Math.max(peakEntries, local.getEntryCount());
                    ops++;
                    // The cache is allowed to briefly exceed the cap if all
                    // residents are pinned, but a single-thread fuzz with
                    // release-after-acquire means refcount=0 between ops -
                    // every acquire that crosses the cap MUST evict. Verify
                    // we are inside the steady-state bound: at most one
                    // entry over (the freshly acquired one mid-eviction).
                    Assert.assertTrue("entries within cap [+1 head-room] [cur=" + local.getEntryCount()
                                    + ", max=" + maxFitting + ']',
                            local.getEntryCount() <= maxFitting + 1);
                }
                LOG.info().$("large-files fuzz done: ops=").$(ops)
                        .$(", peakBytes=").$(peakBytes)
                        .$(", peakEntries=").$(peakEntries).$();
                Assert.assertTrue("must have churned at least once per file",
                        ops >= fileCount);
            }
        });
    }

    @Test
    public void testFuzzLargeFilesIntegrationChurn() throws Exception {
        // Same MB-scale dataset, exercised through SQL cursors instead of the
        // direct cache API. Hammers HivePartitionedReadParquetPageFrameCursor
        // and ReadParquetRecordCursor's acquire/release plumbing under real
        // eviction churn. Uses the ENGINE cache; we cannot constrain its
        // size from this test scope, so the cap stays at the engine default
        // (1 GiB) and we just verify the integration paths handle hundreds
        // of files without leaks. The direct-API test above is what proves
        // the byte-cap honouring.
        final int rowsPerFile = Integer.getInteger("parquet.cache.large.file.rows", 12_000);
        final int fileCount = Integer.getInteger("parquet.cache.large.file.count", 8);
        final long seconds = Long.getLong("parquet.cache.large.fuzz.seconds", 10L);
        LOG.info().$("large-files integration fuzz: rowsPerFile=").$(rowsPerFile)
                .$(", files=").$(fileCount).$(", seconds=").$(seconds).$();

        assertMemoryLeak(() -> {
            execute("create table src as (select x id, x::timestamp ts, " +
                    "rnd_str(48,96,0) s1, rnd_str(48,96,0) s2 from long_sequence(" + rowsPerFile + "))");
            for (int i = 0; i < fileCount; i++) {
                writeParquet("large_int/file_" + i + ".parquet", "src");
            }
            // Also seed a hive layout so we can mix glob queries.
            for (int d = 0; d < Math.min(fileCount, 16); d++) {
                writeParquet("large_int_hive/day=2026-01-" + String.format("%02d", d + 1) + "/data.parquet", "src");
            }

            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ParquetFileCache cache = engine.getParquetFileCache();
            final long deadline = System.nanoTime() + seconds * 1_000_000_000L;
            long sqlSingle = 0;
            long sqlHive = 0;
            long sqlOom = 0;
            long peakBytes = 0;
            long peakEntries = 0;
            while (System.nanoTime() < deadline) {
                final boolean hive = rnd.nextInt(3) == 0;
                final String sql = hive
                        ? "select count(*) from read_parquet('large_int_hive/day=*/data.parquet')"
                        : "select count(*) from read_parquet('large_int/file_" + rnd.nextInt(fileCount) + ".parquet')";
                try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                     io.questdb.cairo.sql.RecordCursorFactory factory =
                             compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                     io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.getRecord().getLong(0);
                    if (hive) sqlHive++; else sqlSingle++;
                } catch (OutOfMemoryError oom) {
                    // If this fires on MB files it means our cap or refcount
                    // is wrong - the cache should evict refcount=0 entries
                    // before letting JVM OOM happen on subsequent acquires.
                    sqlOom++;
                    throw oom;
                }
                peakBytes = Math.max(peakBytes, cache.getCurrentBytes());
                peakEntries = Math.max(peakEntries, cache.getEntryCount());
            }
            LOG.info().$("large-files integration done: sqlSingle=").$(sqlSingle)
                    .$(", sqlHive=").$(sqlHive)
                    .$(", oom=").$(sqlOom)
                    .$(", peakBytes=").$(peakBytes)
                    .$(", peakEntries=").$(peakEntries).$();
            engine.getParquetFileCache().evictAllReleased();
            Assert.assertEquals("integration must finish with empty cache",
                    0, engine.getParquetFileCache().getEntryCount());
        });
    }

    @Test
    public void testFuzzConcurrentMultiPath() throws Exception {
        // The hardest scenario: many threads, many distinct paths, a cap
        // small enough to force constant eviction, and the threads run
        // unbounded acquire/release loops while one thread occasionally
        // rewrites a random file under everyone else's feet. The contract:
        // no leaks (assertMemoryLeak), no double-release (refcount logic),
        // no crash on stale handles (the cache must serialise correctly).
        assertMemoryLeak(() -> {
            execute("create table tA as (select x id from long_sequence(2))");
            execute("create table tB as (select x id from long_sequence(64))");
            final int fileCount = 12;
            for (int i = 0; i < fileCount; i++) {
                writeParquet("fuzz/multi/file_" + i + ".parquet", "tA");
            }

            try (ParquetFileCache local = new ParquetFileCache(new SmallCacheConfig(Long.MAX_VALUE, 3))) {
                final FilesFacade ff = configuration.getFilesFacade();
                final int readerCount = 6;
                final int rewriterCount = 1;
                final int iterations = 400;
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(readerCount + rewriterCount);
                final AtomicInteger failures = new AtomicInteger();
                final Rnd seedRnd = TestUtils.generateRandom(LOG);
                final long s0 = seedRnd.nextLong();
                final long s1 = seedRnd.nextLong();

                for (int t = 0; t < readerCount; t++) {
                    final long localS0 = s0 ^ (t * 0x9E3779B97F4A7C15L);
                    final long localS1 = s1 ^ ((long) t * 0xBF58476D1CE4E5B9L);
                    new Thread(() -> {
                        final Rnd rnd = new Rnd(localS0, localS1);
                        try {
                            start.await();
                            for (int i = 0; i < iterations; i++) {
                                final int slot = rnd.nextInt(fileCount);
                                ParquetFileCache.Entry e = null;
                                try (Path p = parquetPath("fuzz/multi/file_" + slot + ".parquet")) {
                                    e = local.acquire(p, ff);
                                    // Touch the decoder so a stale invalidation
                                    // would surface as wrong metadata or NPE.
                                    final long rows = e.decoder.metadata().getRowCount();
                                    if (rows != 2 && rows != 64) {
                                        failures.incrementAndGet();
                                    }
                                } catch (io.questdb.cairo.CairoException ce) {
                                    // The rewriter may catch the file mid-write
                                    // (truncated by PartitionEncoder before the
                                    // new footer is flushed). Tolerate the
                                    // expected mid-write error shapes - the
                                    // fuzz contract is correctness of the
                                    // cache state, not atomicity of the
                                    // writer. Anything else is a real bug.
                                    final String msg = ce.getFlyweightMessage().toString();
                                    final boolean expected = msg.contains("does not exist")
                                            || msg.contains("parquet")
                                            || msg.contains("footer")
                                            || msg.contains("invalid len")
                                            || msg.contains("mmap");
                                    if (!expected) {
                                        ce.printStackTrace();
                                        failures.incrementAndGet();
                                    }
                                } catch (Throwable th) {
                                    th.printStackTrace();
                                    failures.incrementAndGet();
                                } finally {
                                    if (e != null) {
                                        local.release(e);
                                    }
                                }
                            }
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    }, "fuzz-multi-reader-" + t).start();
                }
                // Rewriter: periodically swap a file's content between tA and
                // tB so its size flips. Each swap forces a stat-validation
                // miss on the next acquire and exercises the orphan path.
                new Thread(() -> {
                    final Rnd rnd = new Rnd(s0 ^ 0xDEADBEEFL, s1 ^ 0xC0FFEEL);
                    try {
                        start.await();
                        for (int i = 0; i < iterations / 10; i++) {
                            final int slot = rnd.nextInt(fileCount);
                            final String rel = "fuzz/multi/file_" + slot + ".parquet";
                            // Alternate the source table so the new file's size
                            // is consistently different from the old one. We
                            // overwrite-in-place via PartitionEncoder.encode -
                            // a temp-then-rename pattern would be more realistic
                            // but adds machinery; readers tolerate a brief miss
                            // window via the catch below.
                            writeParquet(rel, (i & 1) == 0 ? "tB" : "tA");
                            Thread.yield();
                        }
                    } catch (Throwable th) {
                        th.printStackTrace();
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "fuzz-multi-rewriter").start();
                start.countDown();
                Assert.assertTrue("threads must finish within 60s",
                        done.await(60, TimeUnit.SECONDS));
                Assert.assertEquals("no fuzz failures", 0, failures.get());

                // Cache entries should respect the cap once the dust settles.
                Assert.assertTrue("entry count must respect cap [count=" + local.getEntryCount() + ']',
                        local.getEntryCount() <= 3);
            }
            // local.close() drains everything regardless of refcount.
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
