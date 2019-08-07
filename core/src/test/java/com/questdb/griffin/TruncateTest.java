/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.TableWriter;
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class TruncateTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("truncate x");
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
                compiler.compile("truncate");
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
                compiler.compile("truncate table");
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

                compiler.compile("truncate table x,");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table name expected");
            } finally {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
            }
        });
    }

    @Test
    public void testHappyPath() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        assertQuery(
                                "count\n" +
                                        "10\n",
                                "select count() from x",
                                null,
                                true
                        );

                        Assert.assertNull(compiler.compile("truncate table x"));

                        assertQuery(
                                "count\n",
                                "select count() from x",
                                null,
                                true
                        );

                        Assert.assertEquals(0, engine.getBusyWriterCount());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    } finally {
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                    }
                }
        );
    }

    @Test
    public void testTableBusy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();
            createY();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    true
            );

            CyclicBarrier useBarrier = new CyclicBarrier(2);
            CyclicBarrier releaseBarrier = new CyclicBarrier(2);
            CountDownLatch haltLatch = new CountDownLatch(1);

            new Thread(() -> {
                // lock table and wait until main thread uses it
                try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                    useBarrier.await();
                    releaseBarrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                haltLatch.countDown();

            }).start();

            useBarrier.await();
            try {
                Assert.assertNull(compiler.compile("truncate table x,y"));
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table 'y' is busy");
            }

            releaseBarrier.await();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    true
            );

            haltLatch.await(1, TimeUnit.SECONDS);

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testTruncateOpenReader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX(1_000_000);

            assertQuery(
                    "count\n" +
                            "1000000\n",
                    "select count() from x",
                    null,
                    true
            );

            try (RecordCursorFactory factory = compiler.compile("select * from x")) {
                try (RecordCursor cursor = factory.getCursor(DefaultSqlExecutionContext.INSTANCE)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getInt(0);
                        record.getSym(1);
                        record.getDouble(2);
                    }
                }
            }


            compiler.compile("truncate table 'x'");

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();
            createY();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    true
            );

            try {
                Assert.assertNull(compiler.compile("truncate table x, y,z"));
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
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    true
            );

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testTwoTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();
            createY();

            assertQuery(
                    "count\n" +
                            "10\n",
                    "select count() from x",
                    null,
                    true
            );

            assertQuery(
                    "count\n" +
                            "20\n",
                    "select count() from y",
                    null,
                    true
            );

            Assert.assertNull(compiler.compile("truncate table x, y"));

            assertQuery(
                    "count\n",
                    "select count() from x",
                    null,
                    true
            );

            assertQuery(
                    "count\n",
                    "select count() from y",
                    null,
                    true
            );


            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    private void createX() throws SqlException {
        createX(10);
    }

    private void createX(long count) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " to_int(x) i," +
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
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(" + count + ")" +
                        ") timestamp (timestamp)"
        );
    }

    private void createY() throws SqlException {
        compiler.compile(
                "create table y as (" +
                        "select" +
                        " to_int(x) i," +
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
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(20)" +
                        ") timestamp (timestamp)"
        );
    }
}
