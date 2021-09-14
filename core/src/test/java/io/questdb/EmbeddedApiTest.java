/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EmbeddedApiTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testConcurrentSQLExec() throws Exception {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(temp.getRoot().getAbsolutePath());
        final Log log = LogFactory.getLog("testConcurrentSQLExec");

        TestUtils.assertMemoryLeak(() -> {
            WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{
                            -1, -1
                    };
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });


            Rnd rnd = new Rnd();
            try (
                    final CairoEngine engine = new CairoEngine(configuration);
                    final MessageBusImpl messageBus = new MessageBusImpl(configuration)
            ) {
                workerPool.assign(new GroupByJob(messageBus));
                workerPool.start(log);
                try {
                    // number of cores is current thread + workers in the pool
                    final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 2, messageBus);
                    try (SqlCompiler compiler = new SqlCompiler(engine)) {

                        compiler.compile("create table abc (g double, ts timestamp) timestamp(ts) partition by DAY", ctx);

                        long timestamp = 0;
                        try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), "abc", "testing")) {
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
        final CairoConfiguration configuration = new DefaultCairoConfiguration(temp.getRoot().getAbsolutePath());

        TestUtils.assertMemoryLeak(() -> {
            // the write part
            try (
                    final CairoEngine engine = new CairoEngine(configuration);
                    final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
                    final SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                compiler.compile("create table abc (a int, b byte, c short, d long, e float, g double, h date, i symbol, j string, k boolean, ts timestamp) timestamp(ts)", ctx);
                try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), "abc", "testing")) {
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
                        final Record record = cursor.getRecord();
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // access 'record' instance for field values
                        }
                    }
                }
            }
        });
    }
}
