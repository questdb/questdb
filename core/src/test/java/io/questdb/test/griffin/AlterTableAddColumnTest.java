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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertContains;

@RunWith(Parameterized.class)
public class AlterTableAddColumnTest extends AbstractCairoTest {
    private final boolean isWal;

    public AlterTableAddColumnTest(WalMode walMode) {
        isWal = (WalMode.WITH_WAL == walMode);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL}, {WalMode.NO_WAL}
        });
    }

    @Test
    public void testAdd2ColumnsWithoutUsingColumnKeywordAndUsingNotNullKeyword() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    try {
                        execute("alter table x add mycol int not null, mycol2 int");
                        if (isWal) {
                            Assert.fail("Exception expected");
                        }
                    } catch (CairoException e) {
                        if (!isWal) {
                            throw e;
                        }
                        assertContains(e.getFlyweightMessage(), "statement is either no-op," +
                                " or contains multiple transactions, such as 'alter table add column col1, col2'," +
                                " and currently not supported for WAL tables [table=x, oldStructureVersion=0, newStructureVersion=2]");
                    }

                    drainWalQueue();

                    final String originalColumns = "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "sym\tSYMBOL\tfalse\t0\ttrue\t128\t3\tfalse\tfalse\n" +
                            "amt\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "timestamp\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\n" +
                            "b\tBOOLEAN\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "c\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "d\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "e\tFLOAT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "f\tSHORT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "g\tDATE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "ik\tSYMBOL\tfalse\t0\ttrue\t128\t4\tfalse\tfalse\n" +
                            "j\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "k\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "l\tBYTE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "m\tBINARY\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "n\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n";

                    assertQueryNoLeakCheck(
                            isWal
                                    ? originalColumns
                                    : originalColumns +
                                    "mycol\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                                    "mycol2\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n",
                            "show columns from x",
                            null,
                            false
                    );
                }
        );
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
        Assume.assumeFalse(isWal);

        assertMemoryLeak(() -> {
            CountDownLatch allHaltLatch = new CountDownLatch(1);
            try {
                createX();
                AtomicInteger errorCounter = new AtomicInteger();

                // start a thread that would lock table we about to alter
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
                    execute("alter table x add column xx int", sqlExecutionContext);
                    Assert.fail();
                } finally {
                    haltLatch.countDown();
                }
            } catch (EntryUnavailableException e) {
                assertContains(e.getFlyweightMessage(), "table busy");
            }

            Assert.assertTrue(allHaltLatch.await(2, TimeUnit.SECONDS));
        });
    }

    @Test
    public void testAddColumn() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x add column mycol int");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tmycol\n" +
                                    "XYZ\tnull\n" +
                                    "ABC\tnull\n" +
                                    "ABC\tnull\n" +
                                    "XYZ\tnull\n" +
                                    "\tnull\n" +
                                    "CDE\tnull\n" +
                                    "CDE\tnull\n" +
                                    "ABC\tnull\n" +
                                    "\tnull\n" +
                                    "XYZ\tnull\n",
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnINotExistsWithMissingNotToken() throws Exception {
        assertFailure("alter table x add column if exists b int", 28, "'not' expected");
    }

    @Test
    public void testAddColumnIfNotExists() throws Exception {
        createX();
        execute("alter table x add column if not exists a int");
        execute("alter table x add column description string");
    }

    @Test
    public void testAddColumnIfNotExistsUnexpectedToken() throws Exception {
        assertFailure("alter table x add column if not a int", 32,
                "unexpected token 'a' for if not exists");
    }

    @Test
    public void testAddColumnIfNotExistsWithMissingNotToken() throws Exception {
        assertFailure("alter table x add column if exists b int", 28, "'not' expected");
    }

    @Test
    public void testAddColumnUpperCase() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    try {
                        execute("alter table x add column D int", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        assertContains(e.getFlyweightMessage(), "column 'D' already exists");
                    }
                }
        );
    }

    @Test
    public void testAddColumnWithQuotedColumnName() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x add \"mycol\" int not null");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tmycol\n" +
                                    "XYZ\tnull\n" +
                                    "ABC\tnull\n" +
                                    "ABC\tnull\n" +
                                    "XYZ\tnull\n" +
                                    "\tnull\n" +
                                    "CDE\tnull\n" +
                                    "CDE\tnull\n" +
                                    "ABC\tnull\n" +
                                    "\tnull\n" +
                                    "XYZ\tnull\n",
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

                    execute("alter table x add column \"mycol\" int not null");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tmycol\n" +
                                    "XYZ\tnull\n" +
                                    "ABC\tnull\n" +
                                    "ABC\tnull\n" +
                                    "XYZ\tnull\n" +
                                    "\tnull\n" +
                                    "CDE\tnull\n" +
                                    "CDE\tnull\n" +
                                    "ABC\tnull\n" +
                                    "\tnull\n" +
                                    "XYZ\tnull\n",
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnWithSpaceInName() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x add \"spa ce\" string");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tspa ce\n" +
                                    "XYZ\t\n" +
                                    "ABC\t\n" +
                                    "ABC\t\n" +
                                    "XYZ\t\n" +
                                    "\t\n" +
                                    "CDE\t\n" +
                                    "CDE\t\n" +
                                    "ABC\t\n" +
                                    "\t\n" +
                                    "XYZ\t\n",
                            "select c, \"spa ce\" from x"
                    );
                }
        );
    }

    @Test
    public void testAddColumnWithoutUsingColumnKeyword() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x add mycol int");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tmycol\n" +
                                    "XYZ\tnull\n" +
                                    "ABC\tnull\n" +
                                    "ABC\tnull\n" +
                                    "XYZ\tnull\n" +
                                    "\tnull\n" +
                                    "CDE\tnull\n" +
                                    "CDE\tnull\n" +
                                    "ABC\tnull\n" +
                                    "\tnull\n" +
                                    "XYZ\tnull\n",
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

                    execute("alter table x add mycol int not null");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tmycol\n" +
                                    "XYZ\tnull\n" +
                                    "ABC\tnull\n" +
                                    "ABC\tnull\n" +
                                    "XYZ\tnull\n" +
                                    "\tnull\n" +
                                    "CDE\tnull\n" +
                                    "CDE\tnull\n" +
                                    "ABC\tnull\n" +
                                    "\tnull\n" +
                                    "XYZ\tnull\n",
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

                    execute("alter table x add mycol int null");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tmycol\n" +
                                    "XYZ\tnull\n" +
                                    "ABC\tnull\n" +
                                    "ABC\tnull\n" +
                                    "XYZ\tnull\n" +
                                    "\tnull\n" +
                                    "CDE\tnull\n" +
                                    "CDE\tnull\n" +
                                    "ABC\tnull\n" +
                                    "\tnull\n" +
                                    "XYZ\tnull\n",
                            "select c, mycol from x"
                    );
                }
        );
    }

    @Test
    public void testAddDuplicateColumn() throws Exception {
        assertFailure("alter table x add column d int", 25, "column 'd' already exists");
    }

    @Test
    public void testAddDuplicateColumnIfNotExists() throws Exception {
        createX();
        execute("alter table x add column a int");
        execute("alter table x add column if not exists a int");

        assertException("alter table x add column if not exists a hohoho", 41, "unrecognized column type: hohoho");
        assertException("alter table x add column if not exists a long", 41, "column already exists with a different column type [current type=INT, requested type=LONG]");
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
        assertFailure("alter table x add column abc blah", 29, "unsupported column type: blah");
    }

    @Test
    public void testAddSymbolCache() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();
                    engine.clear();

                    // create default configuration with nocache
                    CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                        @Override
                        public boolean getDefaultSymbolCacheFlag() {
                            return false;
                        }
                    };

                    try (CairoEngine engine = new CairoEngine(configuration)) {
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            execute(compiler, "alter table x add column meh symbol cache");
                            drainWalQueue(engine);

                            try (TableReader reader = getReader("x")) {
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
                });
    }

    @Test
    public void testAddSymbolCapacity() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x add column meh symbol capacity 2048");
                    drainWalQueue();

                    try (TableReader reader = getReader("x")) {
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

                    execute("alter table x add column meh symbol index");
                    drainWalQueue();

                    try (TableReader reader = getReader("x")) {
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

                    execute("alter table x add column meh symbol index capacity 9000");
                    drainWalQueue();

                    try (TableReader reader = getReader("x")) {
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

                    execute("alter table x add column meh symbol nocache");
                    drainWalQueue();

                    try (TableReader reader = getReader("x")) {
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
    public void testAddSymbolWithStatementEndingWithSemicolon_DoesntThrowException() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();
                    engine.clear();

                    execute("alter table x add column meh symbol;");
                    drainWalQueue();

                    try (TableReader reader = getReader("x")) {
                        SymbolMapReader smr = reader.getSymbolMapReader(16);
                        Assert.assertNotNull(smr);
                        Assert.assertEquals(configuration.getDefaultSymbolCapacity(), smr.getSymbolCapacity());
                        Assert.assertFalse(reader.getMetadata().isColumnIndexed(16));
                        Assert.assertEquals(configuration.getIndexValueBlockSize(), reader.getMetadata().getIndexValueBlockCapacity(16));
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

                    CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                        @Override
                        public boolean getDefaultSymbolCacheFlag() {
                            return false;
                        }
                    };

                    try (CairoEngine engine = new CairoEngine(configuration)) {
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            execute(compiler, "alter table x add column meh symbol", sqlExecutionContext);
                            drainWalQueue(engine);

                            try (TableReader reader = getReader("x")) {
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
                });
    }

    @Test
    public void testAddSymbolWithoutSpecifyingCapacityOrCacheWhenDefaultSymbolCacheConfigIsSetToTrue() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();
                    engine.clear();

                    execute("alter table x add column meh symbol");
                    drainWalQueue();

                    try (TableReader reader = getReader("x")) {
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
        );
    }

    @Test
    public void testAddTimestampNSColumn() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x add column nscol timestamp_ns");
                    drainWalQueue();

                    assertSql("ddl\n" +
                                    "CREATE TABLE 'x' ( \n" +
                                    "\ti INT,\n" +
                                    "\tsym SYMBOL CAPACITY 128 CACHE,\n" +
                                    "\tamt DOUBLE,\n" +
                                    "\ttimestamp TIMESTAMP,\n" +
                                    "\tb BOOLEAN,\n" +
                                    "\tc STRING,\n" +
                                    "\td DOUBLE,\n" +
                                    "\te FLOAT,\n" +
                                    "\tf SHORT,\n" +
                                    "\tg DATE,\n" +
                                    "\tik SYMBOL CAPACITY 128 CACHE,\n" +
                                    "\tj LONG,\n" +
                                    "\tk TIMESTAMP,\n" +
                                    "\tl BYTE,\n" +
                                    "\tm BINARY,\n" +
                                    "\tn STRING,\n" +
                                    "\tnscol TIMESTAMP_NS\n" +
                                    ") timestamp(timestamp) PARTITION BY DAY " + (isWal ? "" : "BYPASS ") + "WAL\n" +
                                    "WITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n",
                            "show create table x;");

                    assertQueryNoLeakCheck(
                            "c\tnscol\n" +
                                    "XYZ\t\n" +
                                    "ABC\t\n" +
                                    "ABC\t\n" +
                                    "XYZ\t\n" +
                                    "\t\n" +
                                    "CDE\t\n" +
                                    "CDE\t\n" +
                                    "ABC\t\n" +
                                    "\t\n" +
                                    "XYZ\t\n",
                            "select c, nscol from x"
                    );
                }
        );
    }

    @Test
    public void testAddTwoColumns() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    try {
                        execute("alter table x add column mycol int, second symbol");
                        if (isWal) {
                            Assert.fail("Exception expected");
                        }
                    } catch (CairoException e) {
                        if (!isWal) {
                            throw e;
                        }
                        assertContains(e.getFlyweightMessage(), "statement is either no-op," +
                                " or contains multiple transactions, such as 'alter table add column col1, col2'," +
                                " and currently not supported for WAL tables [table=x, oldStructureVersion=0, newStructureVersion=2]");
                    }

                    drainWalQueue();

                    final String originalColumns = "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "sym\tSYMBOL\tfalse\t0\ttrue\t128\t3\tfalse\tfalse\n" +
                            "amt\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "timestamp\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\n" +
                            "b\tBOOLEAN\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "c\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "d\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "e\tFLOAT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "f\tSHORT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "g\tDATE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "ik\tSYMBOL\tfalse\t0\ttrue\t128\t4\tfalse\tfalse\n" +
                            "j\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "k\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "l\tBYTE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "m\tBINARY\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "n\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n";

                    assertQueryNoLeakCheck(
                            isWal
                                    ? originalColumns
                                    : originalColumns +
                                    "mycol\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                                    "second\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\tfalse\n",
                            "show columns from x",
                            null,
                            false
                    );
                }
        );
    }

    @Test
    public void testAddTwoColumnsWithSemicolonAsSeparator() throws Exception {
        assertFailure("alter table x add column mycol int; second symbol", 36, "',' expected");
    }

    @Test
    public void testAddTwoColumnsWithSemicolonAtTheEnd() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x add column mycol int; \n");
                    execute("alter table x add column second symbol;");
                    drainWalQueue();

                    assertQueryNoLeakCheck(
                            "c\tmycol\tsecond\n" +
                                    "XYZ\tnull\t\n" +
                                    "ABC\tnull\t\n" +
                                    "ABC\tnull\t\n" +
                                    "XYZ\tnull\t\n" +
                                    "\tnull\t\n" +
                                    "CDE\tnull\t\n" +
                                    "CDE\tnull\t\n" +
                                    "ABC\tnull\t\n" +
                                    "\tnull\t\n" +
                                    "XYZ\tnull\t\n",
                            "select c, mycol, second from x"
                    );
                }
        );
    }

    @Test
    public void testAlterTableAddArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("alter table x add column arr double[]");

            drainWalQueue();

            assertSql("ddl\n" +
                            "CREATE TABLE 'x' ( \n" +
                            "\ti INT,\n" +
                            "\tsym SYMBOL CAPACITY 128 CACHE,\n" +
                            "\tamt DOUBLE,\n" +
                            "\ttimestamp TIMESTAMP,\n" +
                            "\tb BOOLEAN,\n" +
                            "\tc STRING,\n" +
                            "\td DOUBLE,\n" +
                            "\te FLOAT,\n" +
                            "\tf SHORT,\n" +
                            "\tg DATE,\n" +
                            "\tik SYMBOL CAPACITY 128 CACHE,\n" +
                            "\tj LONG,\n" +
                            "\tk TIMESTAMP,\n" +
                            "\tl BYTE,\n" +
                            "\tm BINARY,\n" +
                            "\tn STRING,\n" +
                            "\tarr DOUBLE[]\n" + // <-- array should be present
                            ") timestamp(timestamp) PARTITION BY DAY " + (isWal ? "" : "BYPASS ") + "WAL\n" +
                            "WITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n",
                    "show create table x;");
        });
    }

    @Test
    public void testAlterTableAddArrayColumnWithInvalidArrayType() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            assertException("alter table x add column arr varchar[];", 29, "unsupported array element type [type=VARCHAR]");
        });
    }

    @Test
    public void testAlterTableAddArrayColumnWithMismatchedBrackets() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            assertException("alter table x add column arr double[;", 35, "syntax error at column type definition, expected array type: 'DOUBLE[]...', but found: 'double['");
            assertException("alter table x add column arr double[][;", 37, "syntax error at column type definition, expected array type: 'DOUBLE[][]...', but found: 'double[]['");
            assertException("alter table x add column arr double];", 29, "arr has an unmatched `]` - were you trying to define an array?");
            assertException("alter table x add column arr double[]];", 29, "arr has an unmatched `]` - were you trying to define an array?");
        });
    }

    @Test
    public void testExpectActionKeyword() throws Exception {
        assertFailure("alter table x", 13, SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR);
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        assertFailure("alter x", 6, "'table' or 'materialized' expected");
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        assertFailure("alter", 5, "'table' or 'materialized' expected");
    }

    @Test
    public void testExpectTableName() throws Exception {
        assertFailure("alter table", 11, "table name expected");
    }

    @Test
    public void testQueryVarcharAboveColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x id, from long_sequence(3))");
            execute("alter table x add column a_varchar varchar");
            execute("insert into x values (4, 'added-1'), (5, 'added-2')");
            assertQuery("a_varchar\n\n\n\nadded-1\nadded-2\n",
                    "select a_varchar from x", null, null, true, true
            );
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertFailure("alter table y", 12, "table does not exist [table=y]");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            createX();
            assertExceptionNoLeakCheck(sql, position, message);
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
                        ") timestamp (timestamp)" +
                        " partition by day" +
                        (isWal ? " wal" : "") +
                        ";"
        );
    }
}
