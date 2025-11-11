/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.createSqlExecutionCtx;

public class InsertAsSelectTest extends AbstractCairoTest {
    @Test
    public void testInsertAsSelectStrToDecimal() throws SqlException {
        try {
            ColumnType.makeUtf16DefaultString();

            execute("create table append as (" +
                    "select" +
                    "  timestamp_sequence(518300000010L,100000L) ts," +
                    "  x::string as d" +
                    " from long_sequence(10)" +
                    ")"
            );

            execute("create table target (" +
                    "ts timestamp," +
                    "d decimal(15, 3)" +
                    ") timestamp (ts) partition by DAY WAL"
            );

            drainWalQueue();

            // insert as select
            execute("insert into target select * from append");
            drainWalQueue();

            // check
            assertSql("d\n" +
                            "1.000\n" +
                            "2.000\n" +
                            "3.000\n" +
                            "4.000\n" +
                            "5.000\n" +
                            "6.000\n" +
                            "7.000\n" +
                            "8.000\n" +
                            "9.000\n" +
                            "10.000\n",
                    "select d from target");
        } finally {
            ColumnType.resetStringToDefault();
        }
    }

    @Test
    public void testInsertAsSelectStringToVarChar() throws SqlException {
        try {
            ColumnType.makeUtf16DefaultString();

            execute("create table append as (" +
                    "select" +
                    "  timestamp_sequence(518300000010L,100000L) ts," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_varchar('ABC', 'CDE', null, 'XYZ') d," +
                    " rnd_char() t" +
                    " from long_sequence(1000)" +
                    ")"
            );

            execute("create table target (" +
                    "ts timestamp," +
                    "c varchar," +
                    "d string," +
                    "t varchar" +
                    ") timestamp (ts) partition by DAY BYPASS WAL"
            );

            drainWalQueue();

            // insert as select
            execute("insert into target select * from append");
            drainWalQueue();

            // check
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(
                        compiler,
                        sqlExecutionContext,
                        "append order by ts",
                        "target"
                );
            }
        } finally {
            ColumnType.resetStringToDefault();
        }
    }

    @Test
    public void testInsertAsSelectWithConcurrentSchemaChange() throws Exception {
        execute("create table source (ts timestamp) timestamp (ts) partition by DAY");
        execute("create table target (ts timestamp) timestamp (ts) partition by DAY");

        execute("insert into source select timestamp_sequence(0, 1000000L) from long_sequence(10)");
        drainWalQueue();

        final SOCountDownLatch startLatch = new SOCountDownLatch(2);
        final SOCountDownLatch doneLatch = new SOCountDownLatch(2);
        final AtomicReference<Exception> insertException = new AtomicReference<>();

        Thread insertThread = new Thread(() -> {
            try {
                startLatch.countDown();
                startLatch.await();

                for (int i = 0; i < 10; i++) {
                    execute("insert into target (ts) select ts from source");
                    drainWalQueue();
                    Os.sleep(10);
                }
            } catch (Exception e) {
                LOG.info().$("insert as select failed with:").$(e).$();
                insertException.set(e);
            } finally {
                doneLatch.countDown();
            }
        });

        Thread alterThread = new Thread(() -> {
            try (
                    final SqlExecutionContext sqlExecutionContext = createSqlExecutionCtx(engine, bindVariableService, 1)
            ) {
                startLatch.countDown();
                startLatch.await();


                for (int i = 0; i < 3; i++) {
                    try {
                        execute("alter table source add column new_col" + i + " int", sqlExecutionContext);
                        Os.sleep(10);
                    } catch (Throwable e) {
                        LOG.info().$("Alter retry ").$(i).$(" failed with: ").$(e).$();
                    }
                }
            } finally {
                doneLatch.countDown();
            }
        });

        insertThread.start();
        alterThread.start();
        doneLatch.await();
        Assert.assertNull(insertException.get());
        drainWalQueue();
        assertSql("count\n100\n", "select count(*) from target");
    }
}
