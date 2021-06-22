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

import io.questdb.cairo.*;
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

public class AlterTableAddColumnTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAddBadColumnNameBackSlash() throws Exception {
        assertFailure("alter table x add column \\", 25, "new column name contains invalid characters");
    }

    @Test
    public void testAddBadColumnNameDot() throws Exception {
        assertFailure("alter table x add column .", 25, "new column name contains invalid characters");
    }

    @Test
    public void testAddBadColumnNameFwdSlash() throws Exception {
        assertFailure("alter table x add column /", 25, "new column name contains invalid characters");
    }

    @Test
    public void testAddBadSyntax() throws Exception {
        assertFailure("alter table x add column abc int k", 33, "',' expected");
    }

    @Test
    public void testAddBusyTable() throws Exception {
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
                    compiler.compile("alter table x add column xx int", sqlExecutionContext);
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
    public void testAddColumn() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add column mycol int", sqlExecutionContext).getType());

                    assertQueryPlain(
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
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnWithoutUsingColumnKeyword() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add mycol int", sqlExecutionContext).getType());

                    assertQueryPlain(
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
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnWithoutUsingColumnKeywordAndUsingNullKeyword() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add mycol int null", sqlExecutionContext).getType());

                    assertQueryPlain(
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
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnWithoutUsingColumnKeywordAndUsingNotNullKeyword() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add mycol int not null", sqlExecutionContext).getType());

                    assertQueryPlain(
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
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAdd2ColumnsWithoutUsingColumnKeywordAndUsingNotNullKeyword() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add mycol int not null, mycol2 int", sqlExecutionContext).getType());

                    assertQueryPlain(
                            "c\tmycol\tmycol2\n" +
                                    "XYZ\tNaN\tNaN\n" +
                                    "ABC\tNaN\tNaN\n" +
                                    "ABC\tNaN\tNaN\n" +
                                    "XYZ\tNaN\tNaN\n" +
                                    "\tNaN\tNaN\n" +
                                    "CDE\tNaN\tNaN\n" +
                                    "CDE\tNaN\tNaN\n" +
                                    "ABC\tNaN\tNaN\n" +
                                    "\tNaN\tNaN\n" +
                                    "XYZ\tNaN\tNaN\n",
                            "select c, mycol, mycol2 from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnWithQuotedColumnName() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add \"mycol\" int not null", sqlExecutionContext).getType());

                    assertQueryPlain(
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
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnWithQuotedColumnNameV2() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add column \"mycol\" int not null", sqlExecutionContext).getType());

                    assertQueryPlain(
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
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnUpperCase() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    try {
                        compiler.compile("alter table x add column D int", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column 'D' already exists");
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
        assertFailure("alter table x add", 17, "'column' or column name");
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
        assertMemoryLeak(
                () -> {
                    createX();

                    engine.clear();

                    // create default configuration with nocache

                    CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                        @Override
                        public boolean getDefaultSymbolCacheFlag() {
                            return false;
                        }
                    };

                    try (CairoEngine engine = new CairoEngine(configuration)) {
                        try (SqlCompiler compiler = new SqlCompiler(engine)) {
                            Assert.assertEquals(ALTER, compiler.compile("alter table x add column meh symbol cache", sqlExecutionContext).getType());

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
                }
        );
    }

    @Test
    public void testAddSymbolCapacity() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add column meh symbol capacity 2048", sqlExecutionContext).getType());

                    try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                        SymbolMapReader smr = reader.getSymbolMapReader(16);
                        Assert.assertNotNull(smr);
                        Assert.assertEquals(2048, smr.getSymbolCapacity());
                        Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                        Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                        Assert.assertEquals(configuration.getDefaultSymbolCacheFlag(), smr.isCached());
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
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add column meh symbol index", sqlExecutionContext).getType());

                    try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                        SymbolMapReader smr = reader.getSymbolMapReader(16);
                        Assert.assertNotNull(smr);
                        Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                        Assert.assertTrue(reader.getMetadata().isColumnIndexed(16));
                        Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                        Assert.assertEquals(configuration.getDefaultSymbolCacheFlag(), smr.isCached());
                    }
                }
        );
    }

    @Test
    public void testAddSymbolIndexCapacity() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add column meh symbol index capacity 9000", sqlExecutionContext).getType());

                    try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                        SymbolMapReader smr = reader.getSymbolMapReader(16);
                        Assert.assertNotNull(smr);
                        Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                        Assert.assertTrue(reader.getMetadata().isColumnIndexed(16));
                        // power of 2
                        Assert.assertEquals(16384, reader.getMetadata().getIndexValueBlockCapacity(16));
                        Assert.assertEquals(configuration.getDefaultSymbolCacheFlag(), smr.isCached());
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
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add column meh symbol nocache", sqlExecutionContext).getType());

                    try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                        SymbolMapReader smr = reader.getSymbolMapReader(16);
                        Assert.assertNotNull(smr);
                        Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                        Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                        Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                        Assert.assertFalse(smr.isCached());
                    }
                }
        );
    }

    @Test
    public void testAddSymbolWithoutSpecifyingCapacityOrCacheWhenDefaultSymbolCacheConfigIsSetToFalse() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    engine.clear();

                    CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                        @Override
                        public boolean getDefaultSymbolCacheFlag() {
                            return false;
                        }
                    };

                    try (CairoEngine engine = new CairoEngine(configuration)) {
                        try (SqlCompiler compiler = new SqlCompiler(engine)) {
                            Assert.assertEquals(ALTER, compiler.compile("alter table x add column meh symbol", sqlExecutionContext).getType());

                            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                                SymbolMapReader smr = reader.getSymbolMapReader(16);
                                Assert.assertNotNull(smr);
                                Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                                Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                                Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                                //check that both configuration and new column have cached  == true
                                Assert.assertFalse(engine.getConfiguration().getDefaultSymbolCacheFlag());
                                Assert.assertFalse(smr.isCached());
                            }

                            Assert.assertEquals(0, engine.getBusyWriterCount());
                            Assert.assertEquals(0, engine.getBusyReaderCount());
                        }
                    }
                }
        );
    }

    @Test
    public void testAddSymbolWithoutSpecifyingCapacityOrCacheWhenDefaultSymbolCacheConfigIsSetToTrue() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    engine.clear();

                    CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                        @Override
                        public boolean getDefaultSymbolCacheFlag() {
                            return true;
                        }
                    };

                    try (CairoEngine engine = new CairoEngine(configuration)) {
                        try (SqlCompiler compiler = new SqlCompiler(engine)) {
                            Assert.assertEquals(ALTER, compiler.compile("alter table x add column meh symbol", sqlExecutionContext).getType());

                            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                                SymbolMapReader smr = reader.getSymbolMapReader(16);
                                Assert.assertNotNull(smr);
                                Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                                Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                                Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
                                //check that both configuration and new column have cached  == true
                                Assert.assertTrue(engine.getConfiguration().getDefaultSymbolCacheFlag());
                                Assert.assertTrue(smr.isCached());
                            }

                            Assert.assertEquals(0, engine.getBusyWriterCount());
                            Assert.assertEquals(0, engine.getBusyReaderCount());
                        }
                    }
                }
        );
    }


    @Test
    public void testAddTwoColumns() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    Assert.assertEquals(ALTER, compiler.compile("alter table x add column mycol int, second symbol", sqlExecutionContext).getType());
                    assertQueryPlain(
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
                            "select c, mycol, second from x"
                    );
                }
        );
    }

    @Test
    public void testAddUnknown() throws Exception {
        assertFailure("alter table x add blah", 22, "column type expected");
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
    public void testTableDoesNotExist() throws Exception {
        assertFailure("alter table y", 12, "table 'y' does not");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
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
                        ") timestamp (timestamp);",
                sqlExecutionContext
        );
    }
}
