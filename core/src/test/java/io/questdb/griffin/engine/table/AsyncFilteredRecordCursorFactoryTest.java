/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.Metrics;
import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.O3Utils;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CustomisableRunnable;
import io.questdb.jit.JitUtil;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ANY;

public class AsyncFilteredRecordCursorFactoryTest extends AbstractGriffinTest {

    private static final int QUEUE_CAPACITY = 4;

    @BeforeClass
    public static void setUpStatic() {
        // Having a single shard is important for many tests in this suite. See resetTaskCapacities
        // method for more detail.
        pageFrameReduceShardCount = 1;
        // We intentionally use a small capacity for the reduce queue to exhibit various edge cases.
        pageFrameReduceQueueCapacity = QUEUE_CAPACITY;

        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testNoLimit() throws Exception {
        testNoLimit(true, SqlJitMode.JIT_MODE_DISABLED, io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory.class);
    }

    @Test
    public void testNoLimitJit() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        testNoLimit(true, SqlJitMode.JIT_MODE_ENABLED, io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.class);
    }

    @Test
    public void testNoLimitDisabledParallelFilterJit() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        // JIT should be ignored since it's only supported for parallel filters.
        testNoLimit(false, SqlJitMode.JIT_MODE_ENABLED, io.questdb.griffin.engine.table.FilteredRecordCursorFactory.class);
    }

    @Test
    public void testNoLimitDisabledParallelFilterNonJit() throws Exception {
        testNoLimit(false, SqlJitMode.JIT_MODE_DISABLED, io.questdb.griffin.engine.table.FilteredRecordCursorFactory.class);
    }

    @Test
    public void testPositiveLimit() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour", sqlExecutionContext);
            final String sql = "x where a > 0.345747032 and a < 0.34575 limit 5";
            try (RecordCursorFactory f = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory.class, f.getClass());
            }

            assertQuery(compiler,
                    "a\tt\n" +
                            "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                            "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                            "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                            "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n",
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    false
            );
        });
    }

    @Test
    public void testPositiveLimitGroupBy() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour", sqlExecutionContext);
            final String sql = "select sum(a) from x where a > 0.345747032 and a < 0.34575 limit 5";
            try (RecordCursorFactory f = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(io.questdb.griffin.engine.LimitRecordCursorFactory.class, f.getClass());
            }

            assertQuery(compiler,
                    "sum\n" +
                            "1.382992963766362\n",
                    sql,
                    null,
                    false,
                    sqlExecutionContext,
                    true
            );
        });
    }

    @Test
    public void testLimitBinVariable() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour", sqlExecutionContext);
            final String sql = "x where a > 0.345747032 and a < 0.34575 limit $1";
            try (RecordCursorFactory f = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory.class, f.getClass());
            }

            sqlExecutionContext.getBindVariableService().setLong(0, 3);
            assertQuery(compiler,
                    "a\tt\n" +
                            "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                            "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                            "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n",
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    false
            );

            // greater
            sqlExecutionContext.getBindVariableService().setLong(0, 5);
            assertQuery(compiler,
                    "a\tt\n" +
                            "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                            "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                            "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                            "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n",
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    false
            );

            // lower
            sqlExecutionContext.getBindVariableService().setLong(0, 2);
            assertQuery(compiler,
                    "a\tt\n" +
                            "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                            "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n",
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    false
            );

            // negative
            sqlExecutionContext.getBindVariableService().setLong(0, -2);
            assertQuery(compiler,
                    "a\tt\n" +
                            "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                            "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n",
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    true // cursor for negative limit accumulates row ids, so it supports size
            );

            resetTaskCapacities();
        });
    }

    @Test
    public void testNegativeLimit() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour", sqlExecutionContext);
            final String sql = "x where a > 0.345747032 and a < 0.34575 limit -5";
            try (RecordCursorFactory f = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory.class, f.getClass());
            }

            assertQuery(compiler,
                    "a\tt\n" +
                            "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                            "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                            "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                            "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n",
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    true
            );
        });
    }

    @Test
    public void testPageFrameSequenceJit() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        testPageFrameSequence(SqlJitMode.JIT_MODE_ENABLED, io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.class);
    }

    @Test
    public void testPageFrameSequenceNonJit() throws Exception {
        testPageFrameSequence(SqlJitMode.JIT_MODE_DISABLED, io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory.class);
    }

    @Test
    public void testFullQueueNoLimit() throws Exception {
        testFullQueue("x where a > 0.42");
    }

    @Test
    public void testFullQueuePositiveLimit() throws Exception {
        testFullQueue("x where a > 0.42 limit 3");
    }

    @Test
    public void testFullQueueNegativeLimit() throws Exception {
        testFullQueue("x where a > 0.42 limit -3");
    }

    private void testNoLimit(boolean enableParallelFilter, int jitMode, Class<?> expectedFactoryClass) throws Exception {
        AbstractCairoTest.enableParallelFilter = enableParallelFilter;
        withPool((engine, compiler, sqlExecutionContext) -> {
            sqlExecutionContext.setJitMode(jitMode);
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour", sqlExecutionContext);
            final String sql = "x where a > 0.345747032 and a < 0.34575";
            try (RecordCursorFactory f = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(expectedFactoryClass.getName(), f.getClass().getName());
            }

            assertQuery(
                    compiler,
                    "a\tt\n" +
                            "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                            "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                            "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                            "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n",
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    false
            );
        });
    }

    private void testPageFrameSequence(int jitMode, Class<?> expectedFactoryClass) throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            sqlExecutionContext.setJitMode(jitMode);

            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour", sqlExecutionContext);
            try (RecordCursorFactory f = compiler.compile("x where a > 0.34", sqlExecutionContext).getRecordCursorFactory()) {

                Assert.assertEquals(expectedFactoryClass.getName(), f.getClass().getName());
                SCSequence subSeq = new SCSequence();
                PageFrameSequence<?> frameSequence = f.execute(sqlExecutionContext, subSeq, ORDER_ANY);

                int frameCount = 0;
                while (frameCount < frameSequence.getFrameCount()) {
                    long cursor = frameSequence.next();
                    if (cursor < 0) {
                        continue;
                    }
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    PageFrameSequence<?> taskSequence = task.getFrameSequence();
                    Assert.assertEquals(frameSequence, taskSequence);
                    frameCount++;
                    frameSequence.collect(cursor, false);
                }
                frameSequence.await();
                Misc.free(frameSequence.getSymbolTableSource());
                frameSequence.clear();
            }
        });
    }

    private void testFullQueue(String query) throws Exception {
        final int pageFrameRows = 100;
        pageFrameMaxRows = pageFrameRows;

        withPool((engine, compiler, sqlExecutionContext) -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            compiler.compile("create table x as (" +
                    "  select rnd_double() a," +
                    "  timestamp_sequence(0, 100000) t from long_sequence(" + (10 * pageFrameRows * QUEUE_CAPACITY) + ")" +
                    ") timestamp(t) partition by hour", sqlExecutionContext);

            try (
                    RecordCursorFactory f1 = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
                    RecordCursorFactory f2 = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()
            ) {
                Assert.assertEquals(AsyncFilteredRecordCursorFactory.class.getName(), f1.getClass().getName());
                Assert.assertEquals(AsyncFilteredRecordCursorFactory.class.getName(), f2.getClass().getName());

                try (
                        RecordCursor c1 = f1.getCursor(sqlExecutionContext);
                        RecordCursor c2 = f2.getCursor(sqlExecutionContext)
                ) {
                    StringSink sink1 = new StringSink();
                    StringSink sink2 = new StringSink();

                    Record r1 = c1.getRecord();
                    Record r2 = c2.getRecord();

                    // We expect both cursors to be able to make progress even although only one of them
                    // occupies the reduce queue most of the time. The second one should be using a local task.
                    while (c1.hasNext()) {
                        printer.print(r1, f1.getMetadata(), sink1);
                        if (c2.hasNext()) {
                            printer.print(r2, f2.getMetadata(), sink2);
                        }
                    }

                    Assert.assertFalse(c1.hasNext());
                    Assert.assertFalse(c2.hasNext());

                    TestUtils.assertEquals(sink1, sink2);
                }
            }

            resetTaskCapacities();
        });
    }

    private void resetTaskCapacities() {
        // Some tests, e.g. testFullQueue, may lead to only a fraction of the page frames being
        // reduced and/or collected before the factory gets closed. When that happens, row id and
        // column lists' capacities in the reduce queue's tasks don't get reset to initial values,
        // so the memory leak check fails. As a workaround, we clean up the memory manually.
        final long maxPageFrameRows = configuration.getSqlPageFrameMaxRows();
        final RingQueue<PageFrameReduceTask> tasks = engine.getMessageBus().getPageFrameReduceQueue(0);
        for (int i = 0; i < tasks.getCycle(); i++) {
            PageFrameReduceTask task = tasks.get(i);
            Assert.assertTrue("Row id list capacity exceeds max page frame rows", task.getRows().getCapacity() <= maxPageFrameRows);
            task.resetCapacities();
        }
    }

    private void withPool(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> {
            WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1, -1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 4;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            }, Metrics.disabled());

            O3Utils.setupWorkerPool(
                    pool,
                    messageBus,
                    null
            );
            pool.start(null);

            try {
                runnable.run(engine, compiler, sqlExecutionContext);
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            } finally {
                pool.halt();
            }
        });
    }
}
