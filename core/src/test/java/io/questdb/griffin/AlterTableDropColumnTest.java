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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.griffin.CompiledQuery.ALTER;

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
        assertMemoryLeak(() -> {
            CountDownLatch allHaltLatch = new CountDownLatch(1);
            try {
                createX();
                AtomicInteger errorCounter = new AtomicInteger();

                // start a thread that would lock table we
                // about to alter
                CyclicBarrier startBarrier = new CyclicBarrier(2);
                CountDownLatch haltLatch = new CountDownLatch(1);
                new Thread(() -> {
                    try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "testing")) {
                        // make sure writer is locked before test begins
                        startBarrier.await();
                        // make sure we don't release writer until main test finishes
                        Assert.assertTrue(haltLatch.await(5, TimeUnit.SECONDS));
                    } catch (Throwable e) {
                        e.printStackTrace();
                        errorCounter.incrementAndGet();
                    } finally {
                        engine.clear();
                        allHaltLatch.countDown();
                    }
                }).start();

                startBarrier.await();
                try {
                    compiler.compile("alter table x drop column ik", sqlExecutionContext);
                    Assert.fail();
                } finally {
                    haltLatch.countDown();
                }
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table 'x' could not be altered: [0]: table busy");
            }

            Assert.assertTrue(allHaltLatch.await(2, TimeUnit.SECONDS));
        });
    }

    @Test
    public void testDropExpectColumnKeyword() throws Exception {
        assertFailure("alter table x drop", 18, "'column' or 'partition' expected");
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

                        Assert.assertEquals(ALTER, compiler.compile("alter table x drop column e, m", sqlExecutionContext).getType());

                        String expected = "{\"columnCount\":14,\"columns\":[{\"index\":0,\"name\":\"i\",\"type\":\"INT\"},{\"index\":1,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":2,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":3,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":4,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":5,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":6,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":7,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":8,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":9,\"name\":\"ik\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":11,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":12,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":13,\"name\":\"n\",\"type\":\"STRING\"}],\"timestampIndex\":3}";

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION, TableUtils.ANY_TABLE_VERSION)) {
                            sink.clear();
                            reader.getMetadata().toJson(sink);
                            TestUtils.assertEquals(expected, sink);
                        }

                        Assert.assertEquals(0, engine.getBusyWriterCount());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    } finally {
                        engine.clear();
                    }
                }
        );
    }

    @Test
    public void testExpectActionKeyword() throws Exception {
        assertFailure("alter table x", 13, "'add', 'alter' or 'drop' expected");
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        assertFailure("alter x", 6, "'table' or 'system' expected");
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        assertFailure("alter", 5, "'table' or 'system' expected");
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
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
            engine.clear();
        });
    }

    private void createX() throws SqlException {
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
                        " from long_sequence(10)" +
                        ") timestamp (timestamp)",
                sqlExecutionContext
        );
    }
}
