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

import com.questdb.cairo.TableReader;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.TableWriter;
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AlterTableDropColumnTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBadSyntax() throws Exception {
        assertFailure("alter table x drop column l m", 28, "',' expected");
    }

    @Test
    public void testBusyTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch allHaltLatch = new CountDownLatch(1);
            try {
                createX();
                AtomicInteger errorCounter = new AtomicInteger();

                // start a thread that would lock table we
                // about to alter
                CyclicBarrier startBarrier = new CyclicBarrier(2);
                CountDownLatch haltLatch = new CountDownLatch(1);
                new Thread(() -> {
                    try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                        // make sure writer is locked before test begins
                        startBarrier.await();
                        // make sure we don't release writer until main test finishes
                        Assert.assertTrue(haltLatch.await(5, TimeUnit.SECONDS));
                    } catch (Throwable e) {
                        e.printStackTrace();
                        errorCounter.incrementAndGet();
                    } finally {
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();

                        allHaltLatch.countDown();
                    }
                }).start();

                startBarrier.await();
                try {
                    compiler.compile("alter table x drop column ik");
                    Assert.fail();
                } finally {
                    haltLatch.countDown();
                }
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table 'x' is busy");
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            allHaltLatch.await(2, TimeUnit.SECONDS);
        });
    }

    @Test
    public void testDropExpectColumnKeyword() throws Exception {
        assertFailure("alter table x drop", 18, "'column' expected");
    }

    @Test
    public void testDropExpectColumnName() throws Exception {
        assertFailure("alter table x drop column", 25, "column name expected");
    }

    @Test
    public void testDropTwoColumns() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x drop column e, m"));

                        String expected = "{\"columnCount\":14,\"columns\":[{\"index\":0,\"name\":\"i\",\"type\":\"INT\"},{\"index\":1,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":2,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":3,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":4,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":5,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":6,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":7,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":8,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":9,\"name\":\"ik\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":11,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":12,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":13,\"name\":\"n\",\"type\":\"STRING\"}],\"timestampIndex\":3}";

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            sink.clear();
                            reader.getMetadata().toJson(sink);
                            TestUtils.assertEquals(expected, sink);
                        }

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
    public void testExpectActionKeyword() throws Exception {
        assertFailure("alter table x", 13, "'add' or 'drop' expected");
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        assertFailure("alter x", 6, "'table' expected");
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        assertFailure("alter", 5, "'table' expected");
    }

    @Test
    public void testExpectTableName() throws Exception {
        assertFailure("alter table", 11, "table name expected");
    }

    @Test
    public void testInvalidColumn() throws Exception {
        assertFailure("alter table x drop column l, kk", 29, "Invalid column: kk");
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertFailure("alter table y", 12, "table 'y' does not");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                compiler.compile(sql);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    private void createX() throws SqlException {
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
                        " from long_sequence(10)" +
                        ") timestamp (timestamp)"
        );
    }
}
