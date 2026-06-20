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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the per-query memory limit on the one path that drives both sides of the
 * shared {@code used} counter from parallel workers at once: a parallel GROUP BY over a
 * {@code CONVERT PARTITION TO PARQUET} partition with a wide VARCHAR column.
 * <p>
 * Worker threads decode the wide VARCHAR pages into the Rust-side {@code page_buffers}
 * ({@code QdbAllocator.charge_tracked}/{@code credit_tracked}) while the reduce phase
 * grows the per-worker GROUP BY maps through {@code Unsafe.malloc}/{@code free}
 * ({@code recordPerQueryMemAlloc}). Both charge and release the same per-query word
 * concurrently, so this is where a transient negative on the Java free path (the
 * {@code saturating_decrement} parity that {@code recordPerQueryMemAlloc} mirrors) or a
 * Java/Rust decrement race would surface - as the {@code -ea} balance assert firing or a
 * residual native allocation. The existing coverage misses this: {@code ParquetMemoryTrackerTest}
 * pins read_parquet() to the synchronous cursor, the {@code Parallel*MemoryTrackerTest}
 * suites carry no parquet, and {@code ParallelGroupByFuzzTest} sets no per-query limit.
 * <p>
 * Each query runs on a dedicated {@link WorkerPool} via {@link TestUtils#execute}, which
 * builds a fresh {@code CairoEngine} from the test configuration; the per-query limit is
 * read fresh by every test, so the breach test can tighten it in its own body. The wide
 * values are a unique prefix padded with a long run of 'a' so ZSTD stores the pages
 * compressed, which forces the decode to materialize them into {@code page_buffers}
 * rather than borrow them zero-copy from the mmap.
 */
public class ParallelParquetMemoryTrackerTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // All set before super.setUp() so they are baked into the configuration the
        // WorkerPool engine reads.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "true");
        // Low threshold so a high-cardinality GROUP BY shards during the reduce.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        // Small row groups so the converted partition holds several of them and the scan
        // fans out across the worker pool - multiple decodes charge the counter at once.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10_000);
        // ZSTD so the wide VARCHAR pages decompress into the Rust page_buffers on scan.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, "ZSTD");
        // Many small page frames so the scan dispatches widely across the workers.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
        // Generous limit: the bounded-key success case must never breach. Its value is
        // the balance check, not the limit - the per-query counter is maintained even for
        // unlimited queries, and a non-zero limit also exercises the concurrent limit read.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024 * 1024L);
        super.setUp();
    }

    @Test
    public void testParallelGroupByOverParquetBalancesSharedCounter() throws Exception {
        // Bounded-cardinality key (500 distinct wide values) keeps the GROUP BY maps small,
        // so the run stays under the generous limit. assertMemoryLeak around the repeated
        // parallel runs is the load-bearing check: a Java/Rust decrement race or a transient
        // negative on the shared counter would surface as the -ea assert or a leak.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (s VARCHAR, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        // 500 distinct ~256-byte values in the first partition; a tiny later
                        // partition seals it so CONVERT can run.
                        engine.execute(
                                "INSERT INTO tab SELECT rpad((x % 500)::varchar, 256, 'a'), (x * 1_000_000L)::timestamp, x FROM long_sequence(50_000)",
                                sqlExecutionContext
                        );
                        engine.execute("INSERT INTO tab VALUES ('z', '1970-01-02T00:00:00.000000Z', -1)", sqlExecutionContext);
                        engine.execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '1970-01-01'", sqlExecutionContext);
                        try (RecordCursorFactory factory = compiler.compile(
                                "SELECT s, count(*) c FROM tab WHERE ts < '1970-01-02' GROUP BY s",
                                sqlExecutionContext
                        ).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    long rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals("iteration " + i, 500, rows);
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelGroupByOverParquetFailsUnderTightLimit() throws Exception {
        // High-cardinality key (every row distinct) grows the per-worker maps without bound;
        // their keys are the wide VARCHAR values decoded from the parquet partition, so the
        // workers charge the counter from both the decode and the map growth until the reduce
        // crosses the tight limit and surfaces with isOutOfMemory() set. This exercises the
        // concurrent limit check on the shared word, which the single-threaded
        // ParquetMemoryTrackerTest cannot.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (s VARCHAR, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT rpad(x::varchar, 256, 'a'), (x * 1_000_000L)::timestamp, x FROM long_sequence(50_000)",
                                sqlExecutionContext
                        );
                        engine.execute("INSERT INTO tab VALUES ('z', '1970-01-02T00:00:00.000000Z', -1)", sqlExecutionContext);
                        engine.execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '1970-01-01'", sqlExecutionContext);
                        try (RecordCursorFactory factory = compiler.compile(
                                "SELECT s, count(*) c FROM tab WHERE ts < '1970-01-02' GROUP BY s",
                                sqlExecutionContext
                        ).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                    // drain until breach
                                }
                                Assert.fail("expected per-query memory breach");
                            } catch (CairoException e) {
                                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertInTree(RecordCursorFactory factory, Class<?> expected) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (expected.isInstance(f)) {
                return;
            }
            f = f.getBaseFactory();
        }
        Assert.fail("expected " + expected.getSimpleName() + " in the factory tree, but top was " + factory.getClass().getName());
    }
}
