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

import com.questdb.cairo.*;
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

public class AlterTableAddColumnTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAddBadSyntax() throws Exception {
        assertFailure("alter table x add column abc int k", 33, "',' expected");
    }

    @Test
    public void testAddBusyTable() throws Exception {
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
                    compiler.compile("alter table x add column xx int");
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
    public void testAddColumn() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column mycol int"));

                        assertQuery(
                                "c\tmycol\n" +
                                        "XYZ\tNaN\n" +
                                        "ABC\tNaN\n" +
                                        "ABC\tNaN\n" +
                                        "XYZ\tNaN\n" +
                                        "\tNaN\n" +
                                        "CDE\tNaN\n" +
                                        "CDE\tNaN\n" +
                                        "ABC\tNaN\n" +
                                        "\tNaN\n" +
                                        "XYZ\tNaN\n",
                                "select c, mycol from x",
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
    public void testAddDuplicateColumn() throws Exception {
        assertFailure("alter table x add column d int", 25, "column 'd' already exists");
    }

    @Test
    public void testAddExpectColumnKeyword() throws Exception {
        assertFailure("alter table x add", 17, "'column' expected");
    }

    @Test
    public void testAddExpectColumnName() throws Exception {
        assertFailure("alter table x add column", 24, "column name expected");
    }

    @Test
    public void testAddExpectColumnType() throws Exception {
        assertFailure("alter table x add column abc", 28, "column type expected");
    }

    @Test
    public void testAddInvalidType() throws Exception {
        assertFailure("alter table x add column abc blah", 29, "invalid type");
    }

    @Test
    public void testAddSymbolCache() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        engine.releaseAllWriters();
                        engine.releaseAllReaders();

                        // create default configuration with nocache

                        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                            @Override
                            public boolean getDefaultSymbolCacheFlag() {
                                return false;
                            }
                        };

                        try (CairoEngine engine = new CairoEngine(configuration)) {
                            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                                Assert.assertNull(compiler.compile("alter table x add column meh symbol cache"));

                                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                                    SymbolMapReader smr = reader.getSymbolMapReader(16);
                                    Assert.assertNotNull(smr);
                                    Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                                    Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                                    Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                                    Assert.assertTrue(smr.isCached());
                                }

                                Assert.assertEquals(0, engine.getBusyWriterCount());
                                Assert.assertEquals(0, engine.getBusyReaderCount());
                            }
                        }

                    } finally {
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                    }
                }
        );
    }

    @Test
    public void testAddSymbolCapacity() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column meh symbol capacity 2048"));

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            SymbolMapReader smr = reader.getSymbolMapReader(16);
                            Assert.assertNotNull(smr);
                            Assert.assertEquals(2048, smr.getSymbolCapacity());
                            Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                            Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                            Assert.assertEquals(configuration.getDefaultSymbolCacheFlag(), smr.isCached());
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
    public void testAddSymbolExpectCapacity() throws Exception {
        assertFailure("alter table x add column abc symbol capacity", 44, "symbol capacity expected");
    }

    @Test
    public void testAddSymbolExpectCapacityTooHigh() throws Exception {
        assertFailure("alter table x add column abc symbol capacity 1073741825 nocache", 45, "max symbol capacity is");
    }

    @Test
    public void testAddSymbolExpectCapacityTooHigh2() throws Exception {
        // cached symbol capacity is lower due to JVM limits
        assertFailure("alter table x add column abc symbol capacity 1073741824", 45, "max cached symbol capacity is");
    }

    @Test
    public void testAddSymbolExpectCapacityTooLow() throws Exception {
        assertFailure("alter table x add column abc symbol capacity -100", 45, "min symbol capacity is");
    }

    @Test
    public void testAddSymbolExpectCapacityTooLow2() throws Exception {
        assertFailure("alter table x add column abc symbol capacity 1", 45, "min symbol capacity is");
    }

    @Test
    public void testAddSymbolExpectNumericCapacity() throws Exception {
        assertFailure("alter table x add column abc symbol capacity 1b", 45, "numeric capacity expected");
    }

    @Test
    public void testAddSymbolIncorrectCapacity() throws Exception {
        assertFailure("alter table x add column abc symbol capacity -", 46, "symbol capacity expected");
    }

    @Test
    public void testAddSymbolIndex() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column meh symbol index"));

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            SymbolMapReader smr = reader.getSymbolMapReader(16);
                            Assert.assertNotNull(smr);
                            Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                            Assert.assertTrue(reader.getMetadata().isColumnIndexed(16));
                            Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                            Assert.assertEquals(configuration.getDefaultSymbolCacheFlag(), smr.isCached());
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
    public void testAddSymbolIndexCapacity() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column meh symbol index capacity 9000"));

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            SymbolMapReader smr = reader.getSymbolMapReader(16);
                            Assert.assertNotNull(smr);
                            Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                            Assert.assertTrue(reader.getMetadata().isColumnIndexed(16));
                            // power of 2
                            Assert.assertEquals(16384, reader.getMetadata().getIndexValueBlockCapacity(16));
                            Assert.assertEquals(configuration.getDefaultSymbolCacheFlag(), smr.isCached());
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
    public void testAddSymbolInvalidIndexCapacity() throws Exception {
        assertFailure("alter table x add column abc symbol index capacity a0", 51, "numeric capacity expected");
    }

    @Test
    public void testAddSymbolNoCache() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column meh symbol nocache"));

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            SymbolMapReader smr = reader.getSymbolMapReader(16);
                            Assert.assertNotNull(smr);
                            Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                            Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                            Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                            Assert.assertFalse(smr.isCached());
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
    public void testAddTwoColumns() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column mycol int, second symbol"));

                        assertQuery(
                                "c\tmycol\tsecond\n" +
                                        "XYZ\tNaN\t\n" +
                                        "ABC\tNaN\t\n" +
                                        "ABC\tNaN\t\n" +
                                        "XYZ\tNaN\t\n" +
                                        "\tNaN\t\n" +
                                        "CDE\tNaN\t\n" +
                                        "CDE\tNaN\t\n" +
                                        "ABC\tNaN\t\n" +
                                        "\tNaN\t\n" +
                                        "XYZ\tNaN\t\n",
                                "select c, mycol, second from x",
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
    public void testAddColumnUpperCase() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        try {
                            compiler.compile("alter table x add column D int");
                            Assert.fail();
                        } catch (SqlException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "Cannot add column [error=Duplicate column name: D]");
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
    public void testAddUnknown() throws Exception {
        assertFailure("alter table x add blah", 18, "'column' expected");
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
                        ") timestamp (timestamp);"
        );
    }
}
