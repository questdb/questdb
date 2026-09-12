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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
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
        assertConcurrentQueries(null);
    }

    @Test
    public void testConcurrentQueriesWithLegacyWorkers() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testConcurrentQueriesWithFiberWorkers() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.FIBER_HOST);
    }

    private void assertConcurrentQueries(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> {
            TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                db.execute("create table r as (select (x%509)::int id, x::int g, x*0.25 d, "
                        + "timestamp_sequence('2020-01-01', 100000000) t from long_sequence(1200)) timestamp(t) partition by day", ignored);
                db.execute("create table p as (select (x%503)::int id, x*0.5 d, ('s'||(x%7))::symbol s "
                        + "from long_sequence(1006))", ignored);
                String[] queries = {
                        "select p.s, count(*) n, sum(r.d) d from r left join p on r.id=p.id group by p.s order by p.s",
                        "select r.g, count(*) n, avg(p.d) d from p right join r on r.id=p.id group by r.g order by r.g",
                        "select count(*) n, count(p.d) c, sum(r.d) s, avg(p.d) a from r left join p on r.id=p.id where p.d>8 or p.d is null"
                };
                for (boolean parquet : new boolean[]{false, true}) {
                    if (parquet) {
                        db.execute("alter table r convert partition to parquet where t < '2020-01-02'", ignored);
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
                        // Every query has a live frozen build before any starts probing. Reuse the
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
                                            breaker.reset();
                                            boolean cancel = query == 0 && run == 1;
                                            try (RecordCursor cursor = factory.getCursor(context)) {
                                                acquired.await(20, TimeUnit.SECONDS);
                                                if (cancel) {
                                                    breaker.cancel();
                                                }
                                                StringSink sink = new StringSink();
                                                CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
                                                Assert.assertFalse("cancelled query completed", cancel);
                                                Assert.assertEquals(queries[query], expected[query], sink.toString());
                                                Assert.assertEquals(query < 2 && threshold == 1, fused.getAtom().isSharded());
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
                            throw new AssertionError("mode=" + mode + ", parquet=" + parquet + ", threshold=" + threshold, failure.get());
                        }
                    }
                }
            }, configuration, LOG);
        });
    }
}
