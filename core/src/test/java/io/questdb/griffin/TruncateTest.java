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

package io.questdb.griffin;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static io.questdb.griffin.CompiledQuery.TRUNCATE;

public class TruncateTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("truncate x", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(9, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'table' expected");
            }
        });
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("truncate", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(8, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'table' expected");
            }
        });
    }

    @Test
    public void testExpectTableName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("truncate table", sqlExecutionContext);
                Assert.fail();
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

                compiler.compile("truncate table x,", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table name expected");
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

                    Assert.assertEquals(TRUNCATE, compiler.compile("truncate table x", sqlExecutionContext).getType());

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

                    Assert.assertEquals(TRUNCATE, compiler.compile("truncate table x;", sqlExecutionContext).getType());

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
    public void testTableBusy() throws Exception {
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

            CyclicBarrier useBarrier = new CyclicBarrier(2);
            CyclicBarrier releaseBarrier = new CyclicBarrier(2);
            CountDownLatch haltLatch = new CountDownLatch(1);

            new Thread(() -> {
                // lock table and wait until main thread uses it
                try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
                    useBarrier.await();
                    releaseBarrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                haltLatch.countDown();

            }).start();

            useBarrier.await();
            try {
                Assert.assertNull(compiler.compile("truncate table x,y", sqlExecutionContext));
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table 'y' could not be truncated: [0]: table busy");
            }

            releaseBarrier.await();

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
                Assert.assertNull(compiler.compile("truncate table x, y,z", sqlExecutionContext));
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(20, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table 'z' does not");
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


            Assert.assertEquals(TRUNCATE, compiler.compile("truncate table only x", sqlExecutionContext).getType());

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

            try (RecordCursorFactory factory = compiler.compile("select * from x", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getInt(0);
                        record.getSym(1);
                        record.getDouble(2);
                    }
                }
            }

            compiler.compile("truncate table 'x'", sqlExecutionContext);
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

            Assert.assertEquals(TRUNCATE, compiler.compile("truncate table x, y", sqlExecutionContext).getType());

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
    public void testDropTableWithCachedPlanSelectFull() throws Exception {
        testDropTableWithCachedPlan("select * from y");
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
    public void testDropTableWithCachedPlanLatestBy() throws Exception {
        testDropTableWithCachedPlan("select * from y latest by symbol1");
    }

    private void testDropTableWithCachedPlan(String query) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b',null) symbol1 " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );

            try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
                }

                compiler.compile("drop table y", sqlExecutionContext);
                compiler.compile(
                        "create table y as ( " +
                                " select " +
                                " timestamp_sequence('1970-01-01T02:30:00.000000Z', 1000000000L) timestamp " +
                                " ,rnd_str('a','b','c', 'd', 'e', 'f',null) symbol2" +
                                " ,rnd_str('a','b',null) symbol1" +
                                " from long_sequence(10)" +
                                ")",
                        sqlExecutionContext
                );

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
                    Assert.fail();
                } catch (ReaderOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot be used because table schema has changed [table='y']");
                }
            }
        });
    }

    @Test
    public void testDropColumnWithCachedPlanSelectFull() throws Exception {
        testDropColumnWithCachedPlan("select * from y");
    }
    
    private void testDropColumnWithCachedPlan(String query) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b',null) symbol1 " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );

            try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
                }

                compiler.compile("alter table y drop column symbol1", sqlExecutionContext);

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
                    Assert.fail();
                } catch (ReaderOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot be used because table schema has changed [table='y']");
                }
            }
        });
    }
    
    private void createX() throws SqlException {
        createX(10);
    }

    private void createX(long count) throws SqlException {
        compiler.compile(
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
                        ") timestamp (timestamp)",
                sqlExecutionContext
        );
    }

    private void createY() throws SqlException {
        compiler.compile(
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
                        ") timestamp (timestamp)",
                sqlExecutionContext
        );
    }
}
