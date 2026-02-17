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

package io.questdb.test.griffin;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static io.questdb.griffin.CompiledQuery.TRUNCATE;

public class TruncateTest extends AbstractCairoTest {

    @Test
    public void testAddColumnTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " x " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );


            execute("alter table y add column new_x int", sqlExecutionContext);
            execute("truncate table y");

            execute("insert into y values('2022-02-24', 1, 2)");

            assertSql("timestamp\tx\tnew_x\n" +
                    "2022-02-24T00:00:00.000000Z\t1\t2\n", "select * from y");
        });
    }

    @Test
    public void testCachedFilterAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table tab as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b') symbol " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            String sql = "select * from tab where symbol != 'c' limit 6";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertCursor("timestamp\tsymbol\n" +
                                        "1970-01-01T00:00:00.000000Z\ta\n" +
                                        "1970-01-01T00:16:40.000000Z\ta\n" +
                                        "1970-01-01T00:33:20.000000Z\tb\n" +
                                        "1970-01-01T00:50:00.000000Z\tb\n" +
                                        "1970-01-01T01:06:40.000000Z\tb\n" +
                                        "1970-01-01T01:23:20.000000Z\tb\n",
                                true, true, true, cursor, factory.getMetadata(), false);
                    }

                    execute("truncate table tab");

                    drainWalQueue();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertCursor("timestamp\tsymbol\n", true, true, true, cursor, factory.getMetadata(), false);
                    }
                }
            }
        });
    }

    @Test
    public void testDropColumnTruncatePartitionByNone() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b',null) symbol1 " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            execute("alter table y drop column symbol1", sqlExecutionContext);
            execute("truncate table y");
            try (TableWriter w = getWriter("y")) {
                TableWriter.Row row = w.newRow(123);
                row.cancel();
            }

            execute("insert into y values(223)");
            assertSql(
                    "timestamp\n" +
                            "1970-01-01T00:00:00.000223Z\n",
                    "select * from y"
            );
        });
    }

    @Test
    public void testDropColumnWithCachedPlanSelectFull() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b',null) symbol1 " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            try (RecordCursorFactory factory = select("select * from y")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(
                            "timestamp\tsymbol1\n" +
                                    "1970-01-01T00:00:00.000000Z\ta\n" +
                                    "1970-01-01T00:16:40.000000Z\ta\n" +
                                    "1970-01-01T00:33:20.000000Z\tb\n" +
                                    "1970-01-01T00:50:00.000000Z\t\n" +
                                    "1970-01-01T01:06:40.000000Z\t\n" +
                                    "1970-01-01T01:23:20.000000Z\t\n" +
                                    "1970-01-01T01:40:00.000000Z\t\n" +
                                    "1970-01-01T01:56:40.000000Z\tb\n" +
                                    "1970-01-01T02:13:20.000000Z\ta\n" +
                                    "1970-01-01T02:30:00.000000Z\tb\n",
                            sink
                    );
                }

                execute("alter table y drop column symbol1", sqlExecutionContext);

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot be used because table schema has changed [table=y");
                }
            }
        });
    }

    @Test
    public void testDropTableWithCachedPlanLatestBy() throws Exception {
        testDropTableWithCachedPlan("select * from y latest on timestamp partition by symbol1");
    }

    @Test
    public void testDropTableWithCachedPlanSelectCount() throws Exception {
        testDropTableWithCachedPlan("select count() from y");
    }

    @Test
    public void testDropTableWithCachedPlanSelectFirst() throws Exception {
        testDropTableWithCachedPlan("select first(symbol1) from y");
    }

    @Test
    public void testDropTableWithCachedPlanSelectFirstSampleBy() throws Exception {
        testDropTableWithCachedPlan("select first(symbol1) from y sample by 1h");
    }

    @Test
    public void testDropTableWithCachedPlanSelectFull() throws Exception {
        testDropTableWithCachedPlan("select * from y");
    }

    @Test
    public void testExpectStatementEnd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                assertExceptionNoLeakCheck("truncate table x keep symbol maps bla-bla-bla");
            } catch (SqlException e) {
                Assert.assertEquals(34, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "unexpected token [bla]");
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("truncate x", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(9, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "TABLE expected");
            }
        });
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("truncate");
            } catch (SqlException e) {
                Assert.assertEquals(8, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "TABLE expected");
            }
        });
    }

    @Test
    public void testExpectTableName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("truncate table", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(14, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table name expected");
            }
        });
    }

    @Test
    public void testExpectTableName2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                assertExceptionNoLeakCheck("truncate table x,", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table name expected");
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testExpectTableName3() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("truncate table with");
            } catch (SqlException e) {
                Assert.assertEquals(15, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table name expected");
            }
        });
    }

    @Test
    public void testExpectTableName4() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                assertExceptionNoLeakCheck("truncate table x, keep");
            } catch (SqlException e) {
                Assert.assertEquals(22, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table name expected");
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testExpectTableName5() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                assertExceptionNoLeakCheck("truncate table x y z");
            } catch (SqlException e) {
                Assert.assertEquals(19, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "',' or 'keep' expected");
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testHappyPath() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    assertQuery(
                            "count\n" +
                                    "10\n",
                            "select count() from x",
                            null,
                            false,
                            true
                    );

                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        Assert.assertEquals(TRUNCATE, compiler.compile("truncate table x", sqlExecutionContext).getType());
                    }

                    assertQuery(
                            "count\n" +
                                    "0\n",
                            "select count() from x",
                            null,
                            false,
                            true
                    );
                }
        );
    }

    @Test
    public void testHappyPathKeepSymbolTables() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    assertQuery(
                            "count\n" +
                                    "10\n",
                            "select count() from x",
                            null,
                            false,
                            true
                    );

                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        Assert.assertEquals(TRUNCATE, compiler.compile("truncate table x keep symbol maps", sqlExecutionContext).getType());
                    }

                    assertQuery(
                            "count\n" +
                                    "0\n",
                            "select count() from x",
                            null,
                            false,
                            true
                    );
                }
        );
    }

    @Test
    public void testHappyPathKeepSymbolTablesAndSemicolon() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();
                    createY();

                    assertQuery("count\n10\n", "select count() from x", null, false, true);
                    assertQuery("count\n20\n", "select count() from y", null, false, true);

                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        Assert.assertEquals(TRUNCATE, compiler.compile("TRUNCATE TABLE x, y KEEP SYMBOL MAPS;", sqlExecutionContext).getType());
                    }

                    assertQuery("count\n0\n", "select count() from x", null, false, true);
                    assertQuery("count\n0\n", "select count() from y", null, false, true);
                }
        );
    }

    @Test
    public void testHappyPathTableNameKeep() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute(
                            "create table keep as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                                    " from long_sequence(3)" +
                                    ") timestamp (timestamp)"
                    );

                    assertQuery(
                            "count\n" +
                                    "3\n",
                            "select count() from keep",
                            null,
                            false,
                            true
                    );

                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        Assert.assertEquals(TRUNCATE, compiler.compile("TRUNCATE TABLE keep;", sqlExecutionContext).getType());
                    }

                    assertQuery(
                            "count\n" +
                                    "0\n",
                            "select count() from keep",
                            null,
                            false,
                            true
                    );
                }
        );
    }

    @Test
    public void testHappyPathWithSemicolon() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    assertQuery(
                            "count\n" +
                                    "10\n",
                            "select count() from x",
                            null,
                            false,
                            true
                    );

                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        Assert.assertEquals(TRUNCATE, compiler.compile("TRUNCATE TABLE x;", sqlExecutionContext).getType());
                    }

                    assertQuery(
                            "count\n" +
                                    "0\n",
                            "select count() from x",
                            null,
                            false,
                            true
                    );
                }
        );
    }

    @Test
    public void testKeepSymbolTablesExpectSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                assertExceptionNoLeakCheck("truncate table x keep bla-bla-bla");
            } catch (SqlException e) {
                Assert.assertEquals(22, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "SYMBOL expected");
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testKeepSymbolTablesExpectTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                assertExceptionNoLeakCheck("truncate table x keep symbol bla-bla-bla");
            } catch (SqlException e) {
                Assert.assertEquals(29, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "MAPS expected");
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testTableBusy() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            createY();

            assertQueryNoLeakCheck(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    false,
                    true
            );

            CyclicBarrier useBarrier = new CyclicBarrier(2);
            CyclicBarrier releaseBarrier = new CyclicBarrier(2);
            CountDownLatch haltLatch = new CountDownLatch(1);

            new Thread(() -> {
                // lock table and wait until main thread uses it
                try (TableWriter ignore = getWriter("y")) {
                    useBarrier.await();
                    releaseBarrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                haltLatch.countDown();
            }).start();

            useBarrier.await();
            try {
                assertExceptionNoLeakCheck("truncate table x,y");
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table 'y' could not be truncated: [-1]: table busy");
            }

            releaseBarrier.await();

            assertQueryNoLeakCheck(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    false,
                    true
            );

            Assert.assertTrue(haltLatch.await(1, TimeUnit.SECONDS));
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            createY();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    false,
                    true
            );

            try {
                assertExceptionNoLeakCheck("truncate table x, y,z");
            } catch (SqlException e) {
                Assert.assertEquals(20, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=z]");
            }

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTableOnly() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(TRUNCATE, compiler.compile("truncate table only x", sqlExecutionContext).getType());
            }

            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTruncateIfExistsExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(TRUNCATE, compiler.compile("truncate table if exists x", sqlExecutionContext).getType());
            }

            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTruncateIfExistsMultipleTables() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            createY();

            assertQuery("count\n10\n", "select count() from x", null, false, true);
            assertQuery("count\n20\n", "select count() from y", null, false, true);

            // Truncate existing and non-existing tables
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(TRUNCATE, compiler.compile("truncate table if exists x, nonexistent, y", sqlExecutionContext).getType());
            }

            assertQuery("count\n0\n", "select count() from x", null, false, true);
            assertQuery("count\n0\n", "select count() from y", null, false, true);
        });
    }

    @Test
    public void testTruncateIfExistsNonExistentTable() throws Exception {
        assertMemoryLeak(() -> {
            // Should not throw an error for non-existent table when IF EXISTS is used
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(TRUNCATE, compiler.compile("truncate table if exists nonexistent", sqlExecutionContext).getType());
            }
        });
    }

    @Test
    public void testTruncateIfExistsSyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile("truncate table if", sqlExecutionContext);
                Assert.fail("Expected SqlException for incomplete IF EXISTS syntax");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "expected EXISTS table-name");
            }
        });
    }

    @Test
    public void testTruncateIfExistsWithKeepSymbolMaps() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(TRUNCATE, compiler.compile("truncate table if exists x keep symbol maps", sqlExecutionContext).getType());
            }

            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTruncateOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            createX(1_000_000);

            assertQuery(
                    "count\n" +
                            "1000000\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            try (RecordCursorFactory factory = select("select * from x")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getInt(0);
                        record.getSymA(1);
                        record.getDouble(2);
                    }
                }
            }

            execute("truncate table 'x'");
        });
    }

    @Test
    public void testTruncateSymbolIndexRestoresCapacity() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b',null) symbol1 " +
                            " from long_sequence(10)" +
                            "), index(symbol1 capacity 512) timestamp (timestamp) partition By DAY"
            );
            TestUtils.assertIndexBlockCapacity(engine, "x", "symbol1");

            execute("truncate table x");
            execute("insert into x\n" +
                    "select timestamp_sequence(0, 1000000000) timestamp," +
                    " rnd_symbol('a','b',null) symbol1 " +
                    " from long_sequence(10)");
            TestUtils.assertIndexBlockCapacity(engine, "x", "symbol1");
        });
    }

    @Test
    public void testTruncateWithColumnTop() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute(
                            "create table testTruncateWithColumnTop as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " rnd_symbol('msft','ibm', 'googl') sym," +
                                    " timestamp_sequence(0, 100000000) k " +
                                    " from long_sequence(100)" +
                                    ") timestamp (k) partition by day"
                    );

                    execute("alter table testTruncateWithColumnTop add column column_with_top int");

                    execute(
                            "insert into testTruncateWithColumnTop " +
                                    "select" +
                                    " cast(x as int) i," +
                                    " rnd_symbol('msft','ibm', 'googl') sym," +
                                    " timestamp_sequence('1970-01-01T12', 10000) k," +
                                    " x as column_with_top " +
                                    " from long_sequence(1000)"
                    );

                    assertQuery(
                            "count\n" +
                                    "1100\n",
                            "select count() from testTruncateWithColumnTop",
                            null,
                            false,
                            true
                    );

                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        Assert.assertEquals(TRUNCATE, compiler.compile("truncate table testTruncateWithColumnTop", sqlExecutionContext).getType());
                    }

                    execute(
                            "insert into testTruncateWithColumnTop " +
                                    "select" +
                                    " cast(x as int) i," +
                                    " rnd_symbol('msft','ibm', 'googl') sym," +
                                    " timestamp_sequence('1970-01-01T12', 10000) k, " +
                                    " x as column_with_top " +
                                    " from long_sequence(1000)"
                    );

                    assertSql("column_with_top\n" +
                            "999\n" +
                            "1000\n", "select column_with_top from testTruncateWithColumnTop limit -2");
                }
        );
    }

    @Test
    public void testTruncateWithoutIfExistsFailsForNonExistentTable() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile("truncate table nonexistent", sqlExecutionContext);
                Assert.fail("Expected SqlException for non-existent table");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
            }
        });
    }

    @Test
    public void testTwoTables() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            createY();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    false,
                    true
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(TRUNCATE, compiler.compile("truncate table x, y", sqlExecutionContext).getType());
            }

            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from x",
                    null,
                    false,
                    true
            );

            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from y",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testUpdateThenTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " x " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            update("update y set x = 10");
            execute("truncate table y");

            execute("insert into y values('2022-02-24', 1)");

            assertSql("timestamp\tx\n" +
                    "2022-02-24T00:00:00.000000Z\t1\n", "select * from y");
        });
    }

    private void createX() throws SqlException {
        createX(10);
    }

    private void createX(long count) throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(" + count + ")" +
                        ") timestamp (timestamp)"
        );
    }

    private void createY() throws SqlException {
        execute(
                "create table y as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(20)" +
                        ") timestamp (timestamp)"
        );
    }

    private void testDropTableWithCachedPlan(String query) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b',null) symbol1 " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                execute("drop table y");
                execute(
                        "create table y as ( " +
                                " select " +
                                " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp " +
                                " ,rnd_str('a','b','c', 'd', 'e', 'f',null) symbol2" +
                                " ,rnd_str('a','b',null) symbol1" +
                                " from long_sequence(10)" +
                                ")"
                );

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot be used because table schema has changed [table=y");
                }
            }
        });
    }
}
