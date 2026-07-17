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
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
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
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        // Selecting the adaptive strategy is necessary but not sufficient: its owner spins for only
        // 50us before stealing, which a worker often fails to wake up inside, so the acquire the
        // leak assertions rest on would still ride on the timing of the box. This gate makes the
        // owner wait for it instead.
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        // The join reducers navigate to the master frame while holding a slot, so they leak the
        // same way; over a parquet master that navigation is what breaches the limit.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, "true");
        // Low threshold so a high-cardinality GROUP BY shards during the reduce.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        // Small row groups so the converted partition holds several of them and the scan
        // fans out across the worker pool - multiple decodes charge the counter at once.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10_000);
        // ZSTD so the wide VARCHAR pages decompress into the Rust page_buffers on scan.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, "ZSTD");
        // Many small page frames so a native scan dispatches widely across the workers. The window
        // join resizes the master scan to the "small" frame sizes, so those have to be small too.
        // These bound native scans only: a parquet scan cuts on row-group boundaries, so over the
        // converted partition the row group size above is what decides the frame count. The breach
        // tests cap the reduce queues instead, which is what makes them dispatch off the owner.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
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
                            TestUtils.assertFactoryInTree(factory, AsyncGroupByRecordCursorFactory.class);
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
                            TestUtils.assertFactoryInTree(factory, AsyncGroupByRecordCursorFactory.class);
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

    @Test
    public void testParallelGroupByOverParquetReleasesWorkerSlotsOnBreach() throws Exception {
        // A reducer acquires a per-worker slot and only then navigates to the frame, which
        // decodes it and can breach the per-query limit. The acquire must therefore sit inside
        // the try that releases the slot. PerWorkerLocks has no reset, and the atom belongs to
        // the factory rather than the cursor, so a slot leaked on a failed reduce is gone for as
        // long as the factory stays in the SQL cache. Once every slot has leaked, each later
        // execution spins in acquireSlot waiting for a slot nobody will release, burning a core
        // until the circuit breaker trips. Re-executing the same cached factory after a breach is
        // what makes the leak observable; a single execution hides it, because the first error
        // cancels the frame sequence.
        //
        // The owner defers to a worker on the reducer's slot-acquire latch only from the
        // latch-gated steal branch, which it reaches only once a reduce queue is full. A parquet
        // scan cuts on row-group boundaries, so the 50k-row partition at a 10k row group yields
        // exactly five frames - far under the 32-deep ordered default and the 4096-deep unordered
        // one. Both queues would then swallow every frame, the owner would drain them itself
        // through the ungated gang-steal loop, and reducing the breaching frame on the owner path
        // takes no per-worker slot, so the acquire-count precondition below would ride on a race.
        // Cap both queues under the frame count. Every query below except the plain filter reduces
        // on UnorderedPageFrameSequence, which carries a queue of its own; the plain filter reduces
        // on the ordered one - hence both caps. On the ordered path the cap is a guarantee: the
        // owner cannot publish every frame unless a worker has consumed, and so acquired a slot
        // for, at least one. The unordered path frees the ring slot before its reducer acquires,
        // so there the cap forces the gate in practice rather than by construction.
        // CAIRO_SQL_PAGE_FRAME_MAX_ROWS does not help: it does not subdivide row groups.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
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
                        // One query per reducer that acquires a slot. Every one reads the wide VARCHAR
                        // column, so the breach lands in the parquet decode inside navigateTo.
                        // The filters test s, not v: with late materialization navigateTo decodes only
                        // the filter columns, so a filter on a narrow column would defer the wide
                        // decode past the acquire and never fault the code under test.
                        // Keyed, unfiltered and filtered:
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT s, count(*) c FROM tab WHERE ts < '1970-01-02' GROUP BY s");
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT s, count(*) c FROM tab WHERE ts < '1970-01-02' AND s != 'zzz' GROUP BY s");
                        // Not-keyed: count(s) alone is not batch-eligible, so it takes the row-by-row
                        // reducer; adding a batch-eligible max(v) switches it to the vectorized one,
                        // which still decodes s and so still breaches in the decode. The filtered
                        // variant takes the third reducer.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT count(s) c FROM tab WHERE ts < '1970-01-02'");
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT max(v) m, count(s) c FROM tab WHERE ts < '1970-01-02'");
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT count(s) c FROM tab WHERE ts < '1970-01-02' AND s != 'zzz'");
                        // The filtered parallel top-K reducer acquires a slot the same way.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT s FROM tab WHERE ts < '1970-01-02' AND s != 'zzz' ORDER BY s LIMIT 5");
                        // Dropping the filter routes to findTopK instead, the fourth reducer: the atom
                        // builds its locks whether or not a filter exists, so this one acquires and
                        // releases a slot of its own on a path the filtered variant never enters.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT s FROM tab ORDER BY s LIMIT 5");
                        // A plain parallel filter - no GROUP BY, no ORDER BY - takes
                        // AsyncFilteredRecordCursorFactory.filter(), the most common parallel query
                        // shape there is. It acquires a filter slot and only then navigates to the
                        // frame, so it leaks exactly like the reducers above. The filter must be
                        // non-thread-safe for the code generator to clone per-worker filters at all,
                        // which is what makes the atom build the locks. The acquire latch times out if a
                        // filter turns out to be thread-safe, so that plan change cannot pass silently.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT s, v FROM tab WHERE ts < '1970-01-02' AND s != 'zzz'");
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelJoinsOverParquetReleaseWorkerSlotsOnBreach() throws Exception {
        // The HORIZON JOIN and WINDOW JOIN reducers acquire a per-worker slot and only then navigate
        // to the master frame. Over a parquet master that navigation decodes the wide VARCHAR column
        // and can breach the per-query limit, so the acquire must sit inside the try that releases
        // the slot - the same defect, and the same fix, as the GROUP BY and top-K reducers above.
        //
        // A native master cannot fault these sites at all: navigateTo() makes no per-query-tracked
        // allocation there, so the breach lands later, inside the try, and the slot comes back on
        // its own. That is why every query below reads s (the wide VARCHAR) rather than v: with late
        // materialization navigateTo() decodes only the columns the query needs, so a projection or
        // filter over a narrow column would defer the wide decode past the acquire and never fault
        // the code under test.
        //
        // ParallelWindowJoinMemoryTrackerTest covers the window join's own sixteen reducers by name,
        // over both masters, for the same reason. The window-join row below overlaps its
        // FILTER_AND_AGGREGATE_PREVAILING row and is kept as the horizon joins' next-door neighbour.
        //
        // Both reduce queues are capped under the parquet master's frame count for the reason
        // spelled out in testParallelGroupByOverParquetReleasesWorkerSlotsOnBreach: without it the
        // owner never reaches the latch-gated steal branch and the acquire-count precondition
        // rides on a race. The horizon joins reduce on UnorderedPageFrameSequence, the window join
        // on the ordered one, so both caps are load-bearing here.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (s VARCHAR, sym SYMBOL, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT rpad(x::varchar, 256, 'a'), (x % 8)::symbol, (x * 1_000_000L)::timestamp, x FROM long_sequence(50_000)",
                                sqlExecutionContext
                        );
                        engine.execute("INSERT INTO tab VALUES ('z', '0', '1970-01-02T00:00:00.000000Z', -1)", sqlExecutionContext);
                        engine.execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '1970-01-01'", sqlExecutionContext);
                        // A small native slave: the leak is on the master side, so the slave only has
                        // to produce matches.
                        engine.execute(
                                "CREATE TABLE px (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO px SELECT (x * 100_000L)::timestamp, (x % 8)::symbol, x::double FROM long_sequence(10_000)",
                                sqlExecutionContext
                        );

                        // ts < '1970-01-02' is interval-extracted off the designated timestamp, so it
                        // keeps the scan on the parquet partition without becoming a master filter:
                        // these take the unfiltered reducers. Adding s != 'zzz' is a real master
                        // filter and routes to the filtered ones.
                        final String horizon = "RANGE FROM -2s TO 2s STEP 1s AS h";
                        // Keyed horizon join: AsyncHorizonJoinRecordCursorFactory, reduce and filterAndReduce.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT t.s, array_agg(p.price) FROM tab t HORIZON JOIN px p ON (t.sym = p.sym) "
                                        + horizon + " WHERE t.ts < '1970-01-02'");
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT t.s, array_agg(p.price) FROM tab t HORIZON JOIN px p ON (t.sym = p.sym) "
                                        + horizon + " WHERE t.ts < '1970-01-02' AND t.s != 'zzz'");
                        // Non-keyed horizon join: AsyncHorizonJoinNotKeyedRecordCursorFactory. Selecting
                        // t.s would make it the grouping key and route to the keyed factory instead, so
                        // the wide column has to enter through an aggregate to keep the master decoding
                        // it while the aggregation stays keyless.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT array_agg(p.price), count(t.s) FROM tab t HORIZON JOIN px p "
                                        + horizon + " WHERE t.ts < '1970-01-02'");
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT array_agg(p.price), count(t.s) FROM tab t HORIZON JOIN px p "
                                        + horizon + " WHERE t.ts < '1970-01-02' AND t.s != 'zzz'");
                        // A second HORIZON JOIN routes to the multi-slave factories, whose reducers
                        // take the slot before navigating to the frame just as the single-slave ones
                        // do. They are correct on master, but nothing asserted it: the only rows that
                        // reached them ran a 100-row master as a single page frame, so the owner
                        // reduced it alone, no worker ever acquired, and a zero held-slot count there
                        // held for the wrong reason. Here the master is the 50k-row parquet partition
                        // the rest of this test uses, so the frames fan out and a worker does acquire.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT t.s, array_agg(p.price), count(p2.price) FROM tab t "
                                        + "HORIZON JOIN px p ON (t.sym = p.sym) HORIZON JOIN px p2 "
                                        + horizon + " WHERE t.ts < '1970-01-02'");
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT array_agg(p.price), count(p2.price), count(t.s) FROM tab t "
                                        + "HORIZON JOIN px p ON (t.sym = p.sym) HORIZON JOIN px p2 "
                                        + horizon + " WHERE t.ts < '1970-01-02'");
                        // And their filtered reducers: t.s != 'zzz' is a real master filter, unlike the
                        // interval-extracted ts predicate above.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT t.s, array_agg(p.price), count(p2.price) FROM tab t "
                                        + "HORIZON JOIN px p ON (t.sym = p.sym) HORIZON JOIN px p2 "
                                        + horizon + " WHERE t.ts < '1970-01-02' AND t.s != 'zzz'");
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT array_agg(p.price), count(p2.price), count(t.s) FROM tab t "
                                        + "HORIZON JOIN px p ON (t.sym = p.sym) HORIZON JOIN px p2 "
                                        + horizon + " WHERE t.ts < '1970-01-02' AND t.s != 'zzz'");

                        // Window join: only the filtered reducers populate the frame while holding the
                        // slot, so only they can leak on the decode. The unfiltered ones populate
                        // before the acquire and leak on the temporary lists instead. Both families,
                        // and each of the sixteen reducers by name, are covered in
                        // ParallelWindowJoinMemoryTrackerTest.
                        TestUtils.assertNoSlotLeakOnBreach(compiler, sqlExecutionContext,
                                "SELECT t.ts, array_agg(p.price) FROM tab t WINDOW JOIN px p "
                                        + "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING "
                                        + "WHERE t.ts < '1970-01-02' AND t.s != 'zzz'");
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelQueryOverParquetRecoversAfterBreach() throws Exception {
        // Black-box counterpart of the slot-leak tests above: it reads no atom and no lock counter,
        // only the symptom a user would see. A query that breaches the per-query limit and is then
        // given enough memory has to return its rows again. It cannot once the reducers have leaked
        // their slots: the atom belongs to the factory, PerWorkerLocks has no reset, so after enough
        // failed executions every slot is held by nobody and the workers spin in acquireSlot until
        // the circuit breaker stops them. The query timeout below is what turns that into a failed
        // assertion rather than a test that hangs.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
        // Keep the timeout local to the breaker this test builds. Assigning the static
        // circuitBreakerConfiguration would outlive the test: staticOverrides.reset() only runs
        // @AfterClass, so every later test in the class would inherit the timeout below.
        final SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public long getQueryTimeout() {
                return 30_000;
            }
        };
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                        engine.execute(
                                "CREATE TABLE tab (s VARCHAR, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY",
                                context
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT rpad(x::varchar, 256, 'a'), (x * 1_000_000L)::timestamp, x FROM long_sequence(50_000)",
                                context
                        );
                        engine.execute("INSERT INTO tab VALUES ('z', '1970-01-02T00:00:00.000000Z', -1)", context);
                        engine.execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '1970-01-01'", context);

                        final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                                engine,
                                circuitBreakerConfiguration
                        );
                        try {
                            context.with(
                                    context.getSecurityContext(),
                                    context.getBindVariableService(),
                                    context.getRandom(),
                                    context.getRequestFd(),
                                    circuitBreaker
                            );
                            // The filtered non-keyed reducer, which acquires a slot and only then
                            // navigates to the frame, where the wide VARCHAR decode breaches. Same
                            // query as one of the rows in testParallelGroupByOverParquetReleasesWorkerSlotsOnBreach,
                            // approached from the outside.
                            final String query = "SELECT count(s) c FROM tab WHERE ts < '1970-01-02' AND s != 'zzz'";
                            try (RecordCursorFactory factory = compiler.compile(query, context).getRecordCursorFactory()) {
                                // Breach more often than there are slots in the pool, so a reducer that
                                // leaked one on every failed frame would have emptied it by the end.
                                for (int i = 0; i < 8; i++) {
                                    circuitBreaker.resetTimer();
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        //noinspection StatementWithEmptyBody
                                        while (cursor.hasNext()) {
                                            // drain until breach
                                        }
                                        Assert.fail("expected per-query memory breach on iteration " + i);
                                    } catch (CairoException e) {
                                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                                    }
                                }

                                // Same cached factory, now with room to finish. Every slot the failed
                                // executions took has to be back in the pool for this to complete.
                                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024 * 1024L);
                                circuitBreaker.resetTimer();
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    Assert.assertTrue("recovered query returned no rows", cursor.hasNext());
                                    Assert.assertEquals(50_000, cursor.getRecord().getLong(0));
                                    Assert.assertFalse(cursor.hasNext());
                                }
                            }
                        } finally {
                            Misc.free(circuitBreaker);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }
}
