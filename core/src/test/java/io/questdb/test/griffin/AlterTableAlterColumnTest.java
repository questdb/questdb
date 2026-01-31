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

import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AlterTableAlterColumnTest extends AbstractCairoTest {

    @Test
    public void testAddIndexColumnWithCapacity_capacityCanBeReadByWriter() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x alter column ik add index capacity 1024");

                    try (TableWriter writer = getWriter("x")) {
                        int blockCapacity = writer.getMetadata().getIndexValueBlockCapacity("ik");
                        Assert.assertEquals(1024, blockCapacity);
                    }
                }
        );
    }

    @Test
    public void testAddIndexColumns() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();
                    try (TableReader reader = getReader("x")) {
                        try {
                            reader.getBitmapIndexReader(0, reader.getMetadata().getColumnIndex("ik"), BitmapIndexReader.DIR_FORWARD);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }
                    }

                    execute("alter table x alter column ik add index");

                    try (TableReader reader = getReader("x")) {
                        Assert.assertNotNull(reader.getBitmapIndexReader(0, reader.getMetadata().getColumnIndex("ik"), BitmapIndexReader.DIR_FORWARD));
                    }
                }
        );
    }

    @Test
    public void testAlterExpectCapacityKeyword() throws Exception {
        assertFailure("alter table x alter column c add index a", 39, "'capacity' expected");
    }

    @Test
    public void testAlterExpectCapacityValue() throws Exception {
        assertFailure("alter table x alter column c add index capacity ", 48, "capacity value expected");
    }

    @Test
    public void testAlterExpectCapacityValueIsInteger() throws Exception {
        assertFailure("alter table x alter column c add index capacity qwe", 48, "positive integer literal expected as index capacity");
    }

    @Test
    public void testAlterExpectCapacityValueIsPositiveInteger() throws Exception {
        assertFailure("alter table x alter column c add index capacity -123", 48, "positive integer literal expected as index capacity");
    }

    @Test
    public void testAlterExpectColumnKeyword() throws Exception {
        assertFailure("alter table x alter", 19, "'column' expected");
    }

    @Test
    public void testAlterExpectColumnName() throws Exception {
        assertFailure("alter table x alter column", 26, "column name expected");
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
                    try (TableWriter ignore = getWriter("x")) {
                        // make sure writer is locked before test begins
                        startBarrier.await();
                        // make sure we don't release writer until main test finishes
                        Assert.assertTrue(haltLatch.await(5, TimeUnit.SECONDS));
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errorCounter.incrementAndGet();
                    } finally {
                        engine.clear();
                        allHaltLatch.countDown();
                    }
                }).start();

                startBarrier.await();
                try {
                    execute("alter table x alter column ik add index", sqlExecutionContext);
                    Assert.fail();
                } finally {
                    haltLatch.countDown();
                }
            } catch (EntryUnavailableException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table busy");
            }
            Assert.assertTrue(allHaltLatch.await(2, TimeUnit.SECONDS));
        });
    }

    @Test
    public void testExpectActionKeyword() throws Exception {
        assertFailure("alter table x", 13, SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR);
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        assertFailure("alter x", 6, "'table' or 'materialized' or 'view' expected");
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        assertFailure("alter", 5, "'table' or 'materialized' or 'view' expected");
    }

    @Test
    public void testExpectTableName() throws Exception {
        assertFailure("alter table", 11, "table name expected");
    }

    @Test
    public void testInvalidColumnName() throws Exception {
        assertFailure("alter table x alter column y add index", 27, "column 'y' does not exist in table 'x'");
    }

    @Test
    public void testInvalidTableName() throws Exception {
        assertFailure("alter table z alter column y add index", 12, "table does not exist [table=z]");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                select(sql);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), message);
                Assert.assertEquals(position, e.getPosition());
            }
        });
    }

    private void createX() throws SqlException {
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
                        " from long_sequence(10)" +
                        ") timestamp (timestamp)"
        );
    }
}
