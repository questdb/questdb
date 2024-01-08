/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class EmbeddedApiTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testConcurrentReadsAndCreateTableIfNotExists() throws Exception {
        final int N = 100;
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(temp.getRoot().getAbsolutePath());

        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                // Create table upfront, so that reader sees it
                try (
                        final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                        final SqlCompiler compiler = engine.getSqlCompiler()
                ) {
                    compiler.compile("create table if not exists abc (a int, b byte, ts timestamp) timestamp(ts)", ctx);
                }

                // Now start single reader and writer
                AtomicInteger errors = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(2);
                CountDownLatch latch = new CountDownLatch(2);
                Reader reader = new Reader(engine, errors, barrier, latch, N);
                reader.start();
                Writer writer = new Writer(engine, errors, barrier, latch, N);
                writer.start();

                latch.await();

                Assert.assertEquals(0, errors.get());
            }
        });
    }

    @Test
    public void testConcurrentSQLExec() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(temp.getRoot().getAbsolutePath());
        final Log log = LogFactory.getLog("testConcurrentSQLExec");

        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool workerPool = new TestWorkerPool(2);

            Rnd rnd = new Rnd();
            try (
                    final CairoEngine engine = new CairoEngine(configuration)
            ) {
                workerPool.assign(new GroupByVectorAggregateJob(engine.getMessageBus()));
                workerPool.start(log);
                try {
                    // number of cores is current thread + workers in the pool
                    try (
                            SqlCompiler compiler = engine.getSqlCompiler();
                            SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 2)
                    ) {

                        compiler.compile("create table abc (g double, ts timestamp) timestamp(ts) partition by DAY", ctx);

                        long timestamp = 0;
                        try (TableWriter writer = TestUtils.getWriter(engine, "abc")) {
                            for (int i = 0; i < 10_000_000; i++) {
                                TableWriter.Row row = writer.newRow(timestamp);
                                row.putDouble(0, rnd.nextDouble());
                                row.append();
                                timestamp += 1_000_000;
                            }
                            writer.commit();
                        }

                        try (RecordCursorFactory factory = compiler.compile("select sum(g) from abc", ctx).getRecordCursorFactory()) {
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                final Record ignored = cursor.getRecord();
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                    // access 'record' instance for field values
                                }
                            }
                        }
                    }
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testReadWrite() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(temp.getRoot().getAbsolutePath());

        TestUtils.assertMemoryLeak(() -> {
            // write part
            try (
                    final CairoEngine engine = new CairoEngine(configuration);
                    final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                    final SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                compiler.compile("create table abc (a int, b byte, c short, d long, e float, g double, h date, i symbol, j string, k boolean, ts timestamp) timestamp(ts)", ctx);
                try (TableWriter writer = TestUtils.getWriter(engine, "abc")) {
                    for (int i = 0; i < 10; i++) {
                        TableWriter.Row row = writer.newRow(Os.currentTimeMicros());
                        row.putInt(0, 123);
                        row.putByte(1, (byte) 1111);
                        row.putShort(2, (short) 222);
                        row.putLong(3, 333);
                        row.putFloat(4, 4.44f);
                        row.putDouble(5, 5.55);
                        row.putDate(6, System.currentTimeMillis());
                        row.putSym(7, "xyz");
                        row.putStr(8, "abc");
                        row.putBool(9, true);
                        row.append();
                    }
                    writer.commit();
                }

                try (RecordCursorFactory factory = compiler.compile("abc", ctx).getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        final Record ignore = cursor.getRecord();
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // access 'record' instance for field values
                        }
                    }
                }
            }
        });
    }

    private static class Reader extends Thread {

        private final CyclicBarrier barrier;
        private final CairoEngine engine;
        private final AtomicInteger errors;
        private final int iterations;
        private final CountDownLatch latch;

        private Reader(CairoEngine engine, AtomicInteger errors, CyclicBarrier barrier, CountDownLatch latch, int iterations) {
            this.engine = engine;
            this.errors = errors;
            this.barrier = barrier;
            this.latch = latch;
            this.iterations = iterations;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    try (
                            final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                            final SqlCompiler compiler = engine.getSqlCompiler();
                            final RecordCursorFactory factory = compiler.compile("abc", ctx).getRecordCursorFactory();
                            final RecordCursor cursor = factory.getCursor(ctx)
                    ) {
                        final Record ignore = cursor.getRecord();
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // access 'record' instance for field values
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                errors.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }
    }

    private static class Writer extends Thread {

        private final CyclicBarrier barrier;
        private final CairoEngine engine;
        private final AtomicInteger errors;
        private final int iterations;
        private final CountDownLatch latch;

        private Writer(CairoEngine engine, AtomicInteger errors, CyclicBarrier barrier, CountDownLatch latch, int iterations) {
            this.engine = engine;
            this.errors = errors;
            this.barrier = barrier;
            this.latch = latch;
            this.iterations = iterations;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    try (
                            final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                            final SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        compiler.compile("create table if not exists abc (a int, b byte, ts timestamp) timestamp(ts) partition by HOUR", ctx);
                        try (TableWriter writer = TestUtils.getWriter(engine, "abc")) {
                            for (int j = 0; j < 100; j++) {
                                TableWriter.Row row = writer.newRow(Os.currentTimeMicros());
                                row.putInt(0, j);
                                row.putByte(1, (byte) j);
                                row.putDate(2, System.currentTimeMillis());
                                row.append();
                            }
                            writer.commit();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                errors.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }
    }
}
