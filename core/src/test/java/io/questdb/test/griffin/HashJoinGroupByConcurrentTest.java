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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

public class HashJoinGroupByConcurrentTest extends AbstractCairoTest {
    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 31);
        super.setUp();
    }

    @Test
    public void testConcurrentQueriesWithOwnerWorkStealing() throws Exception {
        assertConcurrentQueries(null, false);
    }

    @Test
    public void testConcurrentQueriesWithLegacyWorkers() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testConcurrentQueriesWithFiberWorkers() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.FIBER_HOST, false);
    }

    // The builds of every key shape run in rounds of about 40 tasks, one per page frame of up to 31
    // rows, then one per hash partition. Fiber workers batch round tasks by their rows, as they batch
    // frames, so they must see each task's own row count rather than a probe frame's.
    @Test
    public void testConcurrentQueriesWithOwnerWorkStealingAndParallelBuild() throws Exception {
        assertConcurrentQueries(null, true);
    }

    @Test
    public void testConcurrentQueriesWithLegacyWorkersAndParallelBuild() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.LEGACY, true);
    }

    @Test
    public void testConcurrentQueriesWithFiberWorkersAndParallelBuild() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.FIBER_HOST, true);
    }

    // A lone count over a build that repeats its keys: every worker reads the key row counts that the
    // owner left in the build, and no reducer visits a joined pair.
    @Test
    public void testConcurrentLoneCountsWithOwnerWorkStealing() throws Exception {
        assertConcurrentLoneCounts(null, false);
    }

    @Test
    public void testConcurrentLoneCountsWithLegacyWorkers() throws Exception {
        assertConcurrentLoneCounts(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testConcurrentLoneCountsWithFiberWorkers() throws Exception {
        assertConcurrentLoneCounts(WorkerPoolMode.FIBER_HOST, false);
    }

    @Test
    public void testConcurrentLoneCountsWithOwnerWorkStealingAndParallelBuild() throws Exception {
        assertConcurrentLoneCounts(null, true);
    }

    @Test
    public void testConcurrentLoneCountsWithLegacyWorkersAndParallelBuild() throws Exception {
        assertConcurrentLoneCounts(WorkerPoolMode.LEGACY, true);
    }

    @Test
    public void testConcurrentLoneCountsWithFiberWorkersAndParallelBuild() throws Exception {
        assertConcurrentLoneCounts(WorkerPoolMode.FIBER_HOST, true);
    }

    // The probe's key dictionary is about seventy times the build's, so each execution fills the
    // SYMBOL key translation cache from the build's few texts before it publishes a probe task,
    // and the workers only read the cache.
    @Test
    public void testSmallBuildDictionaryWithOwnerWorkStealing() throws Exception {
        assertSmallBuildDictionary(null, false);
    }

    @Test
    public void testSmallBuildDictionaryWithLegacyWorkers() throws Exception {
        assertSmallBuildDictionary(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testSmallBuildDictionaryWithFiberWorkers() throws Exception {
        assertSmallBuildDictionary(WorkerPoolMode.FIBER_HOST, false);
    }

    @Test
    public void testSmallBuildDictionaryWithOwnerWorkStealingAndParallelBuild() throws Exception {
        assertSmallBuildDictionary(null, true);
    }

    @Test
    public void testSmallBuildDictionaryWithLegacyWorkersAndParallelBuild() throws Exception {
        assertSmallBuildDictionary(WorkerPoolMode.LEGACY, true);
    }

    @Test
    public void testSmallBuildDictionaryWithFiberWorkersAndParallelBuild() throws Exception {
        assertSmallBuildDictionary(WorkerPoolMode.FIBER_HOST, true);
    }

    private void assertConcurrentLoneCounts(WorkerPoolMode mode, boolean isParallelBuild) throws Exception {
        if (isParallelBuild) {
            HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        }
        assertMemoryLeak(() -> {
            TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                // r holds two or three rows of each of 509 keys, and p, the smaller table and so the
                // build, two rows of each of 503. k holds the text of id; p interns the key texts in a
                // different order, so equal text has different symbol keys in r and p.
                db.execute("create table r as (select (x%509)::int id, ('k'||(x%509))::symbol k, (x%509)::long lk, "
                        + "('k'||(x%509))::varchar vk, x*0.25 d, timestamp_sequence('2020-01-01', 100_000_000) t"
                        + " from long_sequence(1200)) timestamp(t) partition by day", ignored);
                db.execute("create table p as (select ((x+250)%503)::int id, ('k'||((x+250)%503))::symbol k, ((x+250)%503)::long lk, "
                        + "('k'||((x+250)%503))::varchar vk, x*0.5 d, timestamp_sequence('2020-01-01', 100_000_000) t"
                        + " from long_sequence(1006)) timestamp(t) partition by day", ignored);
                // The two key shapes the INT layout reads, then the three that stage a key into the
                // build's map, each over whole probe frames, filtered probe frames and a filtered build.
                String[] keys = {"r.id=p.id", "r.k=p.k", "r.lk=p.lk", "r.id=p.id and r.vk=p.vk", "r.id=p.id and r.k=p.k"};
                String[] filters = {"", " where r.d > 100", " where p.d > 100"};
                String[] queries = new String[keys.length * filters.length];
                for (int key = 0; key < keys.length; key++) {
                    for (int filter = 0; filter < filters.length; filter++) {
                        queries[key * filters.length + filter] = "select count() from r join p on " + keys[key] + filters[filter];
                    }
                }
                for (int format = 0; format < 2; format++) {
                    if (format > 0) {
                        for (String table : new String[]{"r", "p"}) {
                            db.execute("alter table " + table + " convert partition to parquet where t < '2020-01-02'", ignored);
                        }
                    }
                    String[] expected = new String[queries.length];
                    for (int q = 0; q < queries.length; q++) {
                        try (SqlExecutionContextImpl context = context(db, 4)) {
                            context.setParallelHashJoinGroupByEnabled(false);
                            try (RecordCursorFactory baseline = db.select(queries[q], context)) {
                                expected[q] = result(baseline, context);
                            }
                        }
                    }
                    // Every query holds a build with its key rows counted at the same time. A cursor
                    // builds on its first read, so each owner stops inside that read, at its first
                    // breaker check that finds the counts, until every query got there. Reuse the
                    // same factories after cancellation; unrelated queries must remain successful.
                    CyclicBarrier acquired = new CyclicBarrier(queries.length);
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    Thread[] owners = new Thread[queries.length];
                    for (int q = 0; q < queries.length; q++) {
                        final int query = q;
                        owners[q] = new Thread(() -> {
                            AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(db);
                            try (SqlExecutionContextImpl context = context(db, 4)) {
                                context.with(AllowAllSecurityContext.INSTANCE, null, null, -1, breaker);
                                try (RecordCursorFactory factory = db.select(queries[query], context)) {
                                    AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                                    for (int run = 0; run < 4; run++) {
                                        boolean cancel = query % 3 == 0 && run == 1;
                                        // A cancelled run trips right after the meeting, so it gives
                                        // up a live, counted build while the others probe.
                                        context.with(HashJoinBuildCircuitBreaker.meetingOnceBuilt(db, fused.getAtom()::isCountedByKey, acquired, cancel));
                                        try (RecordCursor cursor = factory.getCursor(context)) {
                                            // The open cursor has built and counted nothing yet.
                                            Assert.assertNull(queries[query], fused.getAtom().getFrozenBuild());
                                            Assert.assertFalse(queries[query], fused.getAtom().isCountedByKey());
                                            StringSink sink = new StringSink();
                                            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
                                            Assert.assertFalse("cancelled query completed", cancel);
                                            Assert.assertEquals(queries[query], isParallelBuild, fused.getAtom().isBuiltInRounds());
                                            Assert.assertTrue(queries[query], fused.getAtom().isCountedByKey());
                                            Assert.assertEquals(queries[query], expected[query], sink.toString());
                                            cursor.toTop();
                                            Assert.assertTrue(cursor.hasNext());
                                        } catch (CairoException ex) {
                                            Assert.assertTrue(ex.getMessage(), cancel && ex.isCancellation());
                                        }
                                        Assert.assertFalse(fused.getAtom().isCountedByKey());
                                        Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                        Assert.assertNull(context.getMemoryTracker());
                                    }
                                }
                            } catch (Throwable th) {
                                failure.compareAndSet(null, th);
                                acquired.reset();
                            } finally {
                                Path.clearThreadLocals();
                            }
                        }, "fused-count-" + q);
                        owners[q].start();
                    }
                    TestUtils.joinThreads(owners);
                    if (failure.get() != null) {
                        throw new AssertionError("mode=" + mode + ", parallelBuild=" + isParallelBuild + ", format=" + format, failure.get());
                    }
                }
            }, configuration, LOG);
        });
    }

    private void assertConcurrentQueries(WorkerPoolMode mode, boolean isParallelBuild) throws Exception {
        if (isParallelBuild) {
            HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        }
        assertMemoryLeak(() -> {
            TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                // k holds the text of id. p writes the key texts in a different order, so equal text
                // has different symbol keys in r and p.
                db.execute("create table r as (select (x%509)::int id, ('k'||(x%509))::symbol k, (x%509)::long lk, "
                        + "('k'||(x%509))::varchar vk, x::int g, x*0.25 d, ('s'||(6-x%7))::symbol s, "
                        + "('s'||(x%5))::symbol s2, timestamp_sequence('2020-01-01', 100000000) t from long_sequence(1200)) timestamp(t) partition by day", ignored);
                db.execute("create table p as (select ((x+250)%503)::int id, ('k'||((x+250)%503))::symbol k, ((x+250)%503)::long lk, "
                        + "('k'||((x+250)%503))::varchar vk, x*0.5 d, ('s'||(x%7))::symbol s, "
                        + "('s'||(4-x%5))::symbol s2, timestamp_sequence('2020-01-01', 100000000) t from long_sequence(1006)) timestamp(t) partition by day", ignored);
                String[] intKeyQueries = {
                        "select r.s, p.s, r.s2, p.s2, count(*) n, count(r.s) rs, count(p.s) ps, sum(r.d) d"
                                + " from r left join p on r.id=p.id group by r.s,p.s,r.s2,p.s2 order by r.s,p.s,r.s2,p.s2",
                        "select r.g, p.s, r.s, count(*) n, count(p.s) ps, count(r.s) rs, avg(p.d) d"
                                + " from p right join r on r.id=p.id group by r.g,p.s,r.s order by r.g,p.s,r.s",
                        "select count(*) n, count(p.d) c, count(r.s) rs, count(p.s) ps, count(r.s2) rs2, count(p.s2) ps2,"
                                + " sum(length(r.s)::double) rl, avg(length(p.s)::double) pl, sum(r.d) s, avg(p.d) a"
                                + " from r left join p on r.id=p.id where p.d>8 or p.d is null"
                };
                // Each query runs once per key shape: the two the INT layout reads, then the three
                // that stage a key into the build's map. The last one puts a SYMBOL pair inside a
                // staged key, so every worker translates through one shared cache. The shape index
                // stays q % 3, so the sharding and cancellation expectations below keep addressing
                // the same query.
                String[] keys = {"r.id=p.id", "r.k=p.k", "r.lk=p.lk", "r.id=p.id and r.vk=p.vk",
                        "r.id=p.id and r.k=p.k"};
                String[] queries = new String[intKeyQueries.length * keys.length];
                for (int key = 0; key < keys.length; key++) {
                    for (int q = 0; q < intKeyQueries.length; q++) {
                        queries[key * intKeyQueries.length + q] = intKeyQueries[q].replace("r.id=p.id", keys[key]);
                    }
                }
                for (int format = 0; format < 3; format++) {
                    if (format > 0) {
                        for (String table : new String[]{"r", "p"}) {
                            db.execute("alter table " + table + " convert partition to parquet where "
                                    + (format == 1 ? "t < '2020-01-02'" : "t >= '2020-01-02'"), ignored);
                        }
                    }
                    for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                        String[] expected = new String[queries.length];
                        for (int q = 0; q < queries.length; q++) {
                            try (SqlExecutionContextImpl context = context(db, 4)) {
                                context.setParallelHashJoinGroupByEnabled(false);
                                try (RecordCursorFactory baseline = db.select(queries[q], context)) {
                                    expected[q] = result(baseline, context);
                                }
                            }
                        }
                        // Every query holds a live frozen build at the same time. A cursor builds
                        // on its first read, so each owner stops inside that read, at its first
                        // breaker check that finds the build, until every query got there. Reuse the
                        // same factories after cancellation; unrelated queries must remain successful.
                        CyclicBarrier acquired = new CyclicBarrier(queries.length);
                        AtomicReference<Throwable> failure = new AtomicReference<>();
                        Thread[] owners = new Thread[queries.length];
                        for (int q = 0; q < queries.length; q++) {
                            final int query = q;
                            owners[q] = new Thread(() -> {
                                AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(db);
                                try (SqlExecutionContextImpl context = context(db, 4)) {
                                    context.with(AllowAllSecurityContext.INSTANCE, null, null, -1, breaker);
                                    try (RecordCursorFactory factory = db.select(queries[query], context)) {
                                        AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                                        for (int run = 0; run < 4; run++) {
                                            boolean cancel = query % 3 == 0 && run == 1;
                                            // A cancelled run trips right after the meeting, so it
                                            // gives up a live build while the others probe.
                                            context.with(HashJoinBuildCircuitBreaker.meetingOnceBuilt(
                                                    db, () -> fused.getAtom().getFrozenBuild() != null, acquired, cancel));
                                            try (RecordCursor cursor = factory.getCursor(context)) {
                                                // The open cursor has built nothing yet.
                                                Assert.assertNull(queries[query], fused.getAtom().getFrozenBuild());
                                                StringSink sink = new StringSink();
                                                CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
                                                Assert.assertFalse("cancelled query completed", cancel);
                                                Assert.assertEquals(queries[query], isParallelBuild, fused.getAtom().isBuiltInRounds());
                                                Assert.assertEquals(queries[query], expected[query], sink.toString());
                                                Assert.assertEquals(query % 3 < 2 && threshold == 1, fused.getAtom().isSharded());
                                                cursor.toTop();
                                                Assert.assertTrue(cursor.hasNext());
                                                // Leave output partially consumed on close.
                                            } catch (CairoException ex) {
                                                Assert.assertTrue(ex.getMessage(), cancel && ex.isCancellation());
                                            }
                                            Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                            Assert.assertNull(context.getMemoryTracker());
                                        }
                                    }
                                } catch (Throwable th) {
                                    failure.compareAndSet(null, th);
                                    acquired.reset();
                                } finally {
                                    Path.clearThreadLocals();
                                }
                            }, "fused-query-" + q);
                            owners[q].start();
                        }
                        TestUtils.joinThreads(owners);
                        if (failure.get() != null) {
                            throw new AssertionError("mode=" + mode + ", parallelBuild=" + isParallelBuild + ", format=" + format
                                    + ", threshold=" + threshold, failure.get());
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    private void assertSmallBuildDictionary(WorkerPoolMode mode, boolean isParallelBuild) throws Exception {
        if (isParallelBuild) {
            HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        }
        assertMemoryLeak(() -> {
            TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                // r's keys hold 2,401 texts: the empty string and k1 to k2999 but those ending in
                // 0 or 5, and a NULL key in every tenth row. p's hold 36: z0 to z6, which r lacks,
                // the empty string and 28 texts of r, each on several rows, and NULL keys too.
                db.execute("""
                        CREATE TABLE r AS (
                            SELECT (CASE WHEN x % 10 = 0 THEN NULL WHEN x % 10 = 5 THEN '' ELSE 'k' || (x % 3_000) END)::SYMBOL k,
                                x * 0.25 d,
                                timestamp_sequence('2020-01-01', 100_000_000) t
                            FROM long_sequence(9_000)
                        ) TIMESTAMP(t) PARTITION BY DAY""", ignored);
                db.execute("""
                        CREATE TABLE p AS (
                            SELECT (CASE WHEN x % 10 = 0 THEN NULL WHEN x % 10 = 1 THEN 'z' || (x % 7)
                                WHEN x % 10 = 5 THEN '' ELSE 'k' || (x % 40) END)::SYMBOL k,
                                x * 0.5 d,
                                timestamp_sequence('2020-01-01', 1_000_000_000) t
                            FROM long_sequence(200)
                        ) TIMESTAMP(t) PARTITION BY DAY""", ignored);
                try (TableReader probe = db.getReader("r"); TableReader build = db.getReader("p")) {
                    Assert.assertEquals(2_401, probe.getSymbolMapReader(0).getSymbolCount());
                    Assert.assertEquals(36, build.getSymbolMapReader(0).getSymbolCount());
                }
                String[] queries = {
                        // Keyed: an INNER join over the build's repeated keys, and a LEFT join that
                        // keeps the probe rows whose key the build lacks.
                        "SELECT r.k, p.k pk, count(*) n, sum(p.d) pd, sum(r.d) rd FROM r JOIN p ON r.k = p.k ORDER BY r.k, pk",
                        "SELECT r.k, count(*) n, count(p.d) pn, sum(p.d) pd FROM r LEFT JOIN p ON r.k = p.k ORDER BY r.k",
                        // Scalar, over every probe row and over filtered ones.
                        "SELECT count(*) n, count(p.d) pn, sum(r.d) rd FROM r LEFT JOIN p ON r.k = p.k",
                        "SELECT count(*) n, count(p.d) pn, sum(r.d) rd FROM r LEFT JOIN p ON r.k = p.k WHERE r.d > 100",
                        // Lone counts, which take each key's row count from the build.
                        "SELECT count() FROM r JOIN p ON r.k = p.k",
                        "SELECT count() FROM r JOIN p ON r.k = p.k WHERE r.d > 100",
                };
                for (int format = 0; format < 2; format++) {
                    if (format > 0) {
                        for (String table : new String[]{"r", "p"}) {
                            db.execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'", ignored);
                        }
                    }
                    for (String query : queries) {
                        try (SqlExecutionContextImpl context = context(db, 4)) {
                            context.setParallelHashJoinGroupByEnabled(false);
                            String expected;
                            try (RecordCursorFactory baseline = db.select(query, context)) {
                                expected = result(baseline, context);
                            }
                            context.setParallelHashJoinGroupByEnabled(true);
                            try (RecordCursorFactory factory = db.select(query, context)) {
                                AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                                Assert.assertTrue(query, plan(factory, context).contains("symbolKeyJoin: true"));
                                // Every execution fills the cache anew, so the second runs on a
                                // reused translator.
                                for (int run = 0; run < 2; run++) {
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        StringSink sink = new StringSink();
                                        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
                                        Assert.assertEquals(query + ", format=" + format, expected, sink.toString());
                                        Assert.assertEquals(query, isParallelBuild, fused.getAtom().isBuiltInRounds());
                                    }
                                    Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                }
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }
}
