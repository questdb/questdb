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
import io.questdb.cairo.O3Utils;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CustomisableRunnable;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.Misc;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncFilteredRecordCursorFactoryTest extends AbstractGriffinTest {

    @BeforeClass
    public static void setUpStatic() {
        jitMode = SqlJitMode.JIT_MODE_DISABLED;
        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testNoLimit() throws Exception {
        withPool(
                (engine, compiler, sqlExecutionContext) -> {
                    compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(20000000)) timestamp(t) partition by hour", sqlExecutionContext);
                    try (RecordCursorFactory f = compiler.compile("x where a > 0.345747032 and a < 0.34575", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertEquals("io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory", f.getClass().getName());
                        try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                            TestUtils.printCursor(cursor, f.getMetadata(), true, sink, printer);
                            TestUtils.assertEquals(
                                    "a\tt\n" +
                                            "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                                            "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                                            "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                                            "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n" +
                                            "0.34574855654046466\t1970-01-04T06:05:54.600000Z\n" +
                                            "0.34574830477559426\t1970-01-04T12:59:33.600000Z\n" +
                                            "0.3457497123080201\t1970-01-05T06:46:13.500000Z\n" +
                                            "0.34574941967613215\t1970-01-05T07:47:38.800000Z\n" +
                                            "0.3457480594580782\t1970-01-05T19:01:15.800000Z\n" +
                                            "0.3457494098829942\t1970-01-06T00:06:00.600000Z\n" +
                                            "0.3457484320163944\t1970-01-06T03:18:23.100000Z\n" +
                                            "0.34574856130504406\t1970-01-06T14:26:12.800000Z\n" +
                                            "0.34574721620944815\t1970-01-08T04:16:51.100000Z\n" +
                                            "0.3457495577997822\t1970-01-08T09:52:24.400000Z\n" +
                                            "0.34574980549230017\t1970-01-09T03:17:07.900000Z\n" +
                                            "0.3457499122593962\t1970-01-09T09:38:49.700000Z\n" +
                                            "0.345748379382518\t1970-01-09T10:12:38.600000Z\n" +
                                            "0.34574988260758466\t1970-01-09T20:58:04.300000Z\n" +
                                            "0.34574772502590545\t1970-01-10T13:46:48.000000Z\n" +
                                            "0.3457490180783126\t1970-01-10T23:17:03.200000Z\n" +
                                            "0.34574937622456714\t1970-01-11T00:53:01.100000Z\n" +
                                            "0.34574928249994386\t1970-01-11T08:39:00.200000Z\n" +
                                            "0.3457483995171776\t1970-01-11T09:17:48.400000Z\n" +
                                            "0.3457475771956874\t1970-01-11T09:41:23.000000Z\n" +
                                            "0.34574817614215625\t1970-01-11T14:02:07.800000Z\n" +
                                            "0.3457472576037246\t1970-01-12T00:58:33.500000Z\n" +
                                            "0.3457482276864321\t1970-01-12T12:26:39.400000Z\n" +
                                            "0.34574890001620284\t1970-01-12T22:47:36.400000Z\n" +
                                            "0.3457476227177313\t1970-01-13T03:10:09.900000Z\n" +
                                            "0.34574804394134817\t1970-01-14T02:05:00.200000Z\n" +
                                            "0.34574786789754897\t1970-01-15T00:38:49.600000Z\n" +
                                            "0.34574867504872375\t1970-01-15T21:13:14.100000Z\n" +
                                            "0.3457487686033581\t1970-01-16T06:56:01.800000Z\n" +
                                            "0.3457487263044071\t1970-01-16T18:54:42.700000Z\n" +
                                            "0.345749316939169\t1970-01-17T00:03:58.700000Z\n" +
                                            "0.34574902450309153\t1970-01-17T03:19:53.700000Z\n" +
                                            "0.34574838536519026\t1970-01-17T08:27:11.700000Z\n" +
                                            "0.3457476000800782\t1970-01-17T22:37:39.500000Z\n" +
                                            "0.34574944904496097\t1970-01-18T05:36:08.200000Z\n" +
                                            "0.3457472405873696\t1970-01-18T10:33:56.400000Z\n" +
                                            "0.345749392052735\t1970-01-18T13:33:50.000000Z\n" +
                                            "0.34574744709985417\t1970-01-19T09:44:56.700000Z\n" +
                                            "0.3457488680180544\t1970-01-19T22:25:06.200000Z\n" +
                                            "0.34574722093256316\t1970-01-20T03:39:41.400000Z\n" +
                                            "0.34574822399187977\t1970-01-20T18:24:48.100000Z\n" +
                                            "0.3457479287069378\t1970-01-21T06:27:10.600000Z\n" +
                                            "0.34574735886284735\t1970-01-21T09:10:20.300000Z\n" +
                                            "0.34574914532219103\t1970-01-21T23:51:17.800000Z\n" +
                                            "0.34574735339779694\t1970-01-22T10:35:07.200000Z\n" +
                                            "0.34574730512933716\t1970-01-22T13:17:34.000000Z\n" +
                                            "0.34574939247010494\t1970-01-22T15:19:51.300000Z\n" +
                                            "0.34574977254639505\t1970-01-23T09:29:20.100000Z\n" +
                                            "0.3457470327991202\t1970-01-23T23:32:20.700000Z\n",
                                    sink
                            );
                        }
                    }
                }
        );
    }

    @Test
    public void testPositiveLimit() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(20000000)) timestamp(t) partition by hour", sqlExecutionContext);
            try (RecordCursorFactory f = compiler.compile("x where a > 0.345747032 and a < 0.34575 limit 5", sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals("io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory", f.getClass().getName());
                try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, f.getMetadata(), true, sink, printer);

                    TestUtils.assertEquals(
                            "a\tt\n" +
                                    "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                                    "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                                    "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                                    "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n" +
                                    "0.34574855654046466\t1970-01-04T06:05:54.600000Z\n",
                            sink
                    );
                }
            }
        });
    }

    @Test
    public void testPositiveLimitBinVariable() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(20000000)) timestamp(t) partition by hour", sqlExecutionContext);
            try (RecordCursorFactory f = compiler.compile("x where a > 0.345747032 and a < 0.34575 limit $1", sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals("io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory", f.getClass().getName());

                sqlExecutionContext.getBindVariableService().setLong(0, 6);

                try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, f.getMetadata(), true, sink, printer);

                    TestUtils.assertEquals(
                            "a\tt\n" +
                                    "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                                    "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                                    "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                                    "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n" +
                                    "0.34574855654046466\t1970-01-04T06:05:54.600000Z\n" +
                                    "0.34574830477559426\t1970-01-04T12:59:33.600000Z\n",
                            sink
                    );
                }

                // greater

                sqlExecutionContext.getBindVariableService().setLong(0, 10);

                try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, f.getMetadata(), true, sink, printer);

                    TestUtils.assertEquals(
                            "a\tt\n" +
                                    "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                                    "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n" +
                                    "0.34574784156471083\t1970-01-02T08:17:06.600000Z\n" +
                                    "0.34574958643398823\t1970-01-02T20:31:57.900000Z\n" +
                                    "0.34574855654046466\t1970-01-04T06:05:54.600000Z\n" +
                                    "0.34574830477559426\t1970-01-04T12:59:33.600000Z\n" +
                                    "0.3457497123080201\t1970-01-05T06:46:13.500000Z\n" +
                                    "0.34574941967613215\t1970-01-05T07:47:38.800000Z\n" +
                                    "0.3457480594580782\t1970-01-05T19:01:15.800000Z\n" +
                                    "0.3457494098829942\t1970-01-06T00:06:00.600000Z\n",
                            sink
                    );
                }

                // lower
                sqlExecutionContext.getBindVariableService().setLong(0, 2);

                try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, f.getMetadata(), true, sink, printer);

                    TestUtils.assertEquals(
                            "a\tt\n" +
                                    "0.34574819315105954\t1970-01-01T15:03:20.500000Z\n" +
                                    "0.34574734261660356\t1970-01-02T02:14:37.600000Z\n",
                            sink
                    );
                }
            }
        });
    }

    @Test
    public void testPositiveLimitGroupBy() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(20000000)) timestamp(t) partition by hour", sqlExecutionContext);
            try (RecordCursorFactory f = compiler.compile("select sum(a) from x where a > 0.345747032 and a < 0.34575 limit 5", sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals("io.questdb.griffin.engine.LimitRecordCursorFactory", f.getClass().getName());
                try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, f.getMetadata(), true, sink, printer);
                    TestUtils.assertEquals(
                            "sum\n" +
                                    "18.324669275833013\n",
                            sink
                    );
                }
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(20000000)) timestamp(t) partition by hour", sqlExecutionContext);
            try (RecordCursorFactory f = compiler.compile("x where a > 0.34", sqlExecutionContext).getRecordCursorFactory()) {

                Assert.assertEquals("io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory", f.getClass().getName());
                SCSequence subSeq = new SCSequence();
                PageFrameSequence<?> frameSequence = f.execute(sqlExecutionContext, subSeq);

                final RingQueue<PageFrameReduceTask> queue = frameSequence.getPageFrameReduceQueue();
                int frameCount = 0;

                while (true) {
                    long cursor = subSeq.nextBully();
                    PageFrameReduceTask task = queue.get(cursor);
                    if (task.getFrameSequence() == frameSequence) {
                        frameCount++;
                        task.collected();
                        if (frameCount == task.getFrameSequence().getFrameCount()) {
                            subSeq.done(cursor);
                            break;
                        }
                    }
                    subSeq.done(cursor);
                }
                frameSequence.await();
                Misc.free(frameSequence.getSymbolTableSource());
            }
        });
    }

    public void withPool(CustomisableRunnable runnable) throws Exception {
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
