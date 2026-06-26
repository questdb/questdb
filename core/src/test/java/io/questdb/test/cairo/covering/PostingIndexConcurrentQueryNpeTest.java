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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reproduces issue #7294: intermittent NullPointerException in
 * CoveringIndexRecordCursorFactory.getCursor under concurrent queries.
 * <p>
 * Root cause: {@link io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory}
 * keeps a <b>direct reference</b> to {@code IntrinsicModel.keyValueFuncs}, which is a
 * <b>pooled</b> {@link io.questdb.std.ObjList} owned by the compiler's
 * {@link io.questdb.griffin.WhereClauseParser} (an {@code ObjectPool<IntrinsicModel>}).
 * SqlCompilers are pooled and shared across threads/connections. When another thread
 * borrows the same pooled compiler and recompiles, {@code models.next()} calls
 * {@code IntrinsicModel.clear()} -> {@code keyValueFuncs.clear()}, which runs
 * {@code Arrays.fill(buffer, null)} <i>before</i> resetting {@code pos = 0}.
 * <p>
 * A concurrent {@code getCursor} on the still-cached factory calls
 * {@code Function.init(keyValueFuncs, ...)} which reads {@code size()} (stale, &gt; 0)
 * and then {@code getQuick(i)} (already nulled), producing exactly the reported:
 * <pre>
 * java.lang.NullPointerException: Cannot invoke
 *   "io.questdb.cairo.sql.Function.init(...)" because the return value of
 *   "io.questdb.std.ObjList.getQuick(int)" is null
 * </pre>
 * <p>
 * The query mirrors the issue: a window function over a UNION ALL whose first branch
 * is an IN-list scan served by the posting/covering index.
 */
public class PostingIndexConcurrentQueryNpeTest extends AbstractCairoTest {

    // Window function over a UNION ALL whose first branch is an IN-list covering-index scan.
    private static final String FAILING_QUERY = "SELECT ts, last_value(v) ignore nulls over (order by ts) AS lv\n" +
            "FROM (\n" +
            "  SELECT ts, 1 AS v\n" +
            "  FROM signals_boolean\n" +
            "  WHERE name IN ('signal.a','signal.b','signal.c','signal.d','signal.e')\n" +
            "    AND ts <= cast(1781184000000000 as timestamp)\n" +
            "  UNION ALL SELECT cast(1781042400000000 as timestamp), cast(null as int)\n" +
            ")";

    @Test(timeout = 120_000)
    public void testConcurrentWindowOverUnionAllWithCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            // Schema from the issue (TTL/DEDUP omitted -- irrelevant to the race; the
            // posting/covering index with an INCLUDE list is what matters).
            execute("CREATE TABLE signals_boolean (" +
                    "  ts TIMESTAMP," +
                    "  name SYMBOL INDEX TYPE POSTING INCLUDE (source, value, ts)," +
                    "  source SYMBOL," +
                    "  value BOOLEAN" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");

            // ~250 rows across the 5 signal.* keys used by the IN list.
            execute("INSERT INTO signals_boolean " +
                    "SELECT (1781000000000000 + x * 100000000)::timestamp, " +
                    "  'signal.' || CASE x % 5 WHEN 0 THEN 'a' WHEN 1 THEN 'b' WHEN 2 THEN 'c' WHEN 3 THEN 'd' ELSE 'e' END, " +
                    "  'src' || (x % 3), " +
                    "  x % 2 = 0 " +
                    "FROM long_sequence(250)");

            // Sanity: the query must actually be served by the covering index, otherwise
            // we would not be exercising the buggy code path.
            final String plan;
            try (RecordCursorFactory factory = select(FAILING_QUERY)) {
                planSink.clear();
                factory.toPlan(planSink);
                plan = planSink.getSink().toString();
            }
            Assert.assertTrue("expected a CoveringIndex plan, got:\n" + plan, plan.contains("CoveringIndex"));

            // Correctness guard: the factory now holds its own copy of keyValueFuncs, so the
            // multi-key IN-list covering scan must still return exactly the reference results.
            assertSqlCursors(
                    "SELECT /*+ no_covering */ ts, name, source, value FROM signals_boolean " +
                            "WHERE name IN ('signal.a','signal.c','signal.e') ORDER BY ts",
                    "SELECT ts, name, source, value FROM signals_boolean " +
                            "WHERE name IN ('signal.a','signal.c','signal.e') ORDER BY ts"
            );

            // A modest amount of concurrent compile/execute overlap is enough: on the
            // unfixed code this reproduces the NPE essentially every run. Threads borrow
            // a compiler, compile, RETURN it to the pool, then execute -- so a sibling
            // thread reuses the same pooled compiler and clears the keyValueFuncs list
            // this factory still references, racing getCursor().
            final int threads = 8;
            final int iterations = 15;
            final CyclicBarrier barrier = new CyclicBarrier(threads);
            final CountDownLatch done = new CountDownLatch(threads);
            final AtomicReference<Throwable> firstError = new AtomicReference<>();

            for (int t = 0; t < threads; t++) {
                new Thread(() -> {
                    try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)) {
                        ctx.with(AllowAllSecurityContext.INSTANCE);
                        barrier.await();
                        for (int i = 0; i < iterations && firstError.get() == null; i++) {
                            // Mirror PG-wire: compile, RETURN the compiler to the pool,
                            // then execute the cached factory. Another thread reuses the
                            // same pooled compiler (and its WhereClauseParser
                            // IntrinsicModel pool), clearing the keyValueFuncs list that
                            // this factory still references -- racing this getCursor().
                            final RecordCursorFactory factory;
                            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                                factory = compiler.compile(FAILING_QUERY, ctx).getRecordCursorFactory();
                            }
                            try (RecordCursorFactory f = factory; RecordCursor cursor = f.getCursor(ctx)) {
                                final Record record = cursor.getRecord();
                                while (cursor.hasNext()) {
                                    record.getTimestamp(0);
                                    record.getInt(1);
                                }
                            }
                        }
                    } catch (Throwable th) {
                        firstError.compareAndSet(null, th);
                    } finally {
                        done.countDown();
                    }
                }, "repro-7294-" + t).start();
            }

            done.await();
            final Throwable err = firstError.get();
            if (err != null) {
                // Before the fix this is the intermittent NPE from issue #7294:
                //   Function.init(...) <- ObjList.getQuick(int) returns null
                // raised in CoveringIndexRecordCursorFactory.getCursor.
                throw new AssertionError(
                        "concurrent window-over-UNION-ALL covering-index query must not fail (issue #7294): " + err,
                        err
                );
            }
        });
    }
}
