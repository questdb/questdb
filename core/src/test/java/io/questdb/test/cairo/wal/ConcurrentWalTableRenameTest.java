/*******************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentWalTableRenameTest extends AbstractCairoTest {

    @Test
    public void testConcurrentSelectRename() throws Exception {
        assertMemoryLeak(() -> {
            int threadCount = 3;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);

            execute("create table t1 as (select x, rnd_symbol('a', 'b', 'c', null, 'd') s," +
                    "timestamp_sequence('2022-02-24T04', 100000000) ts " +
                    "from long_sequence(5)) timestamp(ts) partition by DAY WAL");

            execute("create table t2 as (select x, rnd_symbol('a', 'b', 'c', null, 'd') s," +
                    "timestamp_sequence('2022-02-24T04', 100000000) ts " +
                    "from long_sequence(5)) timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            ObjList<Thread> threads = new ObjList<>(threadCount + 2);
            for (int i = 1; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        StringSink sink = new StringSink();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    TestUtils.assertSql(
                                            engine,
                                            executionContext,
                                            "select t1.ts from t1 join t2 on t1.ts = t2.ts",
                                            sink,
                                            "ts\n" +
                                                    "2022-02-24T04:00:00.000000Z\n" +
                                                    "2022-02-24T04:01:40.000000Z\n" +
                                                    "2022-02-24T04:03:20.000000Z\n" +
                                                    "2022-02-24T04:05:00.000000Z\n" +
                                                    "2022-02-24T04:06:40.000000Z\n"
                                    );
                                } catch (SqlException | CairoException e) {
                                    if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")) {
                                        throw e;
                                    }
                                } catch (TableReferenceOutOfDateException e) {
                                    // ignore
                                }
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.getLast().start();
            }

            AtomicBoolean done = new AtomicBoolean();
            threads.add(new Thread(() -> {
                try {
                    barrier.await();
                    try (
                            SqlCompiler compiler = engine.getSqlCompiler();
                            SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                    ) {
                        while (!done.get()) {
                            try {
                                compiler.compile("rename table t1 to temp", executionContext);
                                compiler.compile("rename table t2 to t1", executionContext);
                                compiler.compile("rename table temp to t2", executionContext);
                            } catch (SqlException | CairoException e) {
                                if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")) {
                                    throw e;
                                }
                            } catch (TableReferenceOutOfDateException e) {
                                // ignore
                            }
                        }
                    }
                } catch (Throwable e) {
                    ref.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }));
            threads.getLast().start();

            for (int i = 0; i < threads.size() - 1; i++) {
                threads.getQuick(i).join();
            }

            done.set(true);
            threads.getLast().join();

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }
        });
    }
}
