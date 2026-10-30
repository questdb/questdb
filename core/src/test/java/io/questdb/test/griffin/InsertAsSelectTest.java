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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.createSqlExecutionCtx;

public class InsertAsSelectTest extends AbstractCairoTest {
    @Test
    public void testInsertAsSelectLateralOverTableFunctionWithSubqueryArgument() throws Exception {
        assertMemoryLeak(() -> {
            final String query = """
                    SELECT s.generate_series g, l.c
                    FROM generate_series(1, CASE WHEN NOT (SELECT false) THEN 3 ELSE 4 END, 1) s
                    CROSS JOIN LATERAL (SELECT count(*) AS c FROM long_sequence(3) WHERE x < s.generate_series) l
                    """;
            final String expected = """
                    g\tc
                    1\t0
                    2\t1
                    3\t2
                    """;
            execute("CREATE TABLE dst (g LONG, c LONG)");
            execute("INSERT INTO dst " + query);
            assertQuery("dst").noLeakCheck().expectSize().returns(expected);
            execute("CREATE TABLE ctas AS (" + query + ")");
            assertQuery("ctas").noLeakCheck().expectSize().returns(expected);
        });
    }

    @Test
    public void testInsertAsSelectNumberIntoCharColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (b BYTE, s SHORT, l LONG, f FLOAT, d DOUBLE, dt DATE, ts TIMESTAMP)");
            execute("INSERT INTO src VALUES (65, 66, 67, 68.0, 69.0, 70, 71)");
            execute("CREATE TABLE dst (c CHAR)");
            execute("INSERT INTO dst SELECT b FROM src");
            execute("INSERT INTO dst SELECT s FROM src");
            assertQuery("SELECT c FROM dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            c
                            A
                            B
                            """);
            final String insert = "INSERT INTO dst SELECT ";
            assertQuery(insert + "l FROM src").fails(insert.length(), "inconvertible types: LONG -> CHAR [from=l, to=c]");
            assertQuery(insert + "f FROM src").fails(insert.length(), "inconvertible types: FLOAT -> CHAR [from=f, to=c]");
            assertQuery(insert + "d FROM src").fails(insert.length(), "inconvertible types: DOUBLE -> CHAR [from=d, to=c]");
            assertQuery(insert + "dt FROM src").fails(insert.length(), "inconvertible types: DATE -> CHAR [from=dt, to=c]");
            assertQuery(insert + "ts FROM src").fails(insert.length(), "inconvertible types: TIMESTAMP -> CHAR [from=ts, to=c]");
        });
    }

    @Test
    public void testInsertAsSelectStrToDecimal() throws Exception {
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
            assertQuery("select d from target")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            d
                            1.000
                            2.000
                            3.000
                            4.000
                            5.000
                            6.000
                            7.000
                            8.000
                            9.000
                            10.000
                            """);
        } finally {
            ColumnType.resetStringToDefault();
        }
    }

    @Test
    public void testInsertAsSelectStringToVarChar() throws Exception {
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
    public void testInsertAsSelectSymbolIntoTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (s SYMBOL)");
            execute("INSERT INTO src VALUES ('2024-01-01T00:00:00.000000Z'), (null), ('2024-01-02T03:04:05.000006Z')");
            execute("CREATE TABLE dst (ts TIMESTAMP)");
            execute("INSERT INTO dst SELECT s FROM src");
            assertQuery("SELECT ts FROM dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts
                            2024-01-01T00:00:00.000000Z

                            2024-01-02T03:04:05.000006Z
                            """);
        });
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
        assertQuery("select count(*) from target")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n100\n");
    }

    @Test
    public void testSelectAsInsertManyColumns() throws Exception {
        createTableManyCols("main");
        createTableManyCols("temp");

        execute("insert into main select * from temp");
    }

    @Test
    public void testSelectAsInsertManyColumnsWithUnion() throws Exception {
        createTableManyCols("main");
        createTableManyCols("temp");

        execute("insert into main select * from temp " +
                "except " +
                "select * from main");
    }

    private static void createTableManyCols(String name) {
        TableModel tm = new TableModel(configuration, name, PartitionBy.DAY);
        tm.col("index", ColumnType.SYMBOL);
        tm.timestamp();
        for (int i = 0; i < 6000; i++) {
            tm.col("a" + i, ColumnType.INT);
        }
        createTable(tm);
    }
}
