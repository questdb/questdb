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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableRenameColumnTest extends AbstractCairoTest {

    @Test
    public void testBadSyntax() throws Exception {
        assertFailure("alter table x rename column l ,m", 30, "to' expected");
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
    public void testInvalidColumn() throws Exception {
        assertFailure("alter table x rename column y yy", 28, "Invalid column: y");
    }

    @Test
    public void testNewNameAlreadyExists() throws Exception {
        assertFailure("alter table x rename column l to b", 33, " column already exists");
    }

    @Test
    public void testRenameArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (arr double[]);");
            execute("alter table x rename column arr to arr2;");
            assertSql("column\ttype\narr2\tDOUBLE[]\n",
                    "select \"column\", \"type\" from table_columns('x')");
        });
    }

    @Test
    public void testRenameColumn() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        execute("alter table x rename column e to z");

                        String expected = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"i\",\"type\":\"INT\"},{\"index\":1,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":2,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":3,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":4,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":5,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":6,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":7,\"name\":\"z\",\"type\":\"FLOAT\"},{\"index\":8,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":9,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":10,\"name\":\"ik\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":13,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":14,\"name\":\"m\",\"type\":\"BINARY\"},{\"index\":15,\"name\":\"n\",\"type\":\"STRING\"}],\"timestampIndex\":3}";
                        try (TableReader reader = getReader("x")) {
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
    public void testRenameColumnAndCheckOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x1 (a int, b double, t timestamp) timestamp(t)");

            try (TableReader reader = getReader("x1")) {
                Assert.assertEquals("b", reader.getMetadata().getColumnName(1));

                try (TableWriter writer = getWriter("x1")) {
                    Assert.assertEquals("b", writer.getMetadata().getColumnName(1));
                    writer.renameColumn("b", "bb");
                    Assert.assertEquals("bb", writer.getMetadata().getColumnName(1));
                }

                Assert.assertTrue(reader.reload());
                Assert.assertEquals("bb", reader.getMetadata().getColumnName(1));
            }
        });
    }

    @Test
    public void testRenameColumnAndCheckOpenReaderWithCursor() throws Exception {
        String expectedBefore = """
                i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.55991614\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522494\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.09750569\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS
                4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL
                5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9820662\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t
                6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tnull\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t
                7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO
                8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.07594013\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ
                9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425297\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG
                10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tnull\tnull\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff
                00000010 9a ef 88 cb\tCNGTNLEGPUHH
                """;

        String expectedAfter = """
                i\tsym\tamt\ttimestamp\tbb\tcc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.55991614\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522494\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.09750569\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS
                4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL
                5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9820662\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t
                6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tnull\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t
                7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO
                8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.07594013\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ
                9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425297\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG
                10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tnull\tnull\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff
                00000010 9a ef 88 cb\tCNGTNLEGPUHH
                """;

        assertMemoryLeak(() -> {
            createX();

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals("b", reader.getMetadata().getColumnName(4));
                //check cursor before renaming column
                TestUtils.assertReader(expectedBefore, reader, sink);

                try (TableWriter writer = getWriter("x")) {
                    writer.renameColumn("b", "bb");
                    writer.renameColumn("c", "cc");
                    Assert.assertEquals("bb", writer.getMetadata().getColumnName(4));
                }
                //reload reader
                Assert.assertTrue(reader.reload());
                //check cursor after reload

                TestUtils.assertReader(expectedAfter, reader, sink);

                assertReader(expectedAfter, "x");

                Assert.assertEquals("bb", reader.getMetadata().getColumnName(4));
            }
        });
    }

    @Test
    public void testRenameColumnEndsWithSemicolon() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();
            execute("alter table x rename column i to i1;", sqlExecutionContext);
            engine.clear();
        });
    }

    @Test
    public void testRenameColumnEndsWithSemicolonEndingWithWhitesace() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();
            execute("alter table x rename column i to i1; \n", sqlExecutionContext);
            engine.clear();
        });
    }

    @Test
    public void testRenameColumnExistingReader() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        try (TableReader reader = getReader("x")) {
                            execute("alter table x rename column e to z");
                            String expected = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"i\",\"type\":\"INT\"},{\"index\":1,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":2,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":3,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":4,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":5,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":6,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":7,\"name\":\"z\",\"type\":\"FLOAT\"},{\"index\":8,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":9,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":10,\"name\":\"ik\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":13,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":14,\"name\":\"m\",\"type\":\"BINARY\"},{\"index\":15,\"name\":\"n\",\"type\":\"STRING\"}],\"timestampIndex\":3}";
                            sink.clear();
                            reader.reload();
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
    public void testRenameColumnWithBadName() throws Exception {
        assertFailure("alter table x rename column e to z/ssd", 34, "',' expected");
    }

    @Test
    public void testRenameColumnWithBadName2() throws Exception {
        assertFailure("alter table x rename column e to //", 33, "new column name contains invalid characters");
    }

    @Test
    public void testRenameColumnWithBadName3() throws Exception {
        assertFailure("alter table x rename column e to ..", 33, "new column name contains invalid characters");
    }

    @Test
    public void testRenameColumnWithBadName4() throws Exception {
        assertFailure("alter table x rename column e to a.", 34, "',' expected");
    }

    @Test
    public void testRenameColumnWithBadName5() throws Exception {
        assertFailure("alter table x rename column e to -", 33, "new column name contains invalid characters");
    }

    @Test
    public void testRenameColumnWithBadName6() throws Exception {
        assertFailure("alter table x rename column e to -", 33, "new column name contains invalid characters");
    }

    @Test
    public void testRenameColumnWithBadName7() throws Exception {
        assertFailure("alter table x rename column e to *", 33, "new column name contains invalid characters");
    }

    @Test
    public void testRenameExpectColumnKeyword() throws Exception {
        assertFailure("alter table x rename", 20, "'column' expected");
    }

    @Test
    public void testRenameExpectColumnName() throws Exception {
        assertFailure("alter table x rename column", 27, "column name expected");
    }

    @Test
    public void testRenameSymbolColumnReloadReader() throws Exception {

        FilesFacade ff = new FilesFacadeImpl();
        assertMemoryLeak(
                ff,
                () -> {
                    try {
                        execute(
                                "create table x as (" +
                                        "select" +
                                        " cast(x as int) i," +
                                        " rnd_symbol('msft','ibm', 'googl') sym," +
                                        " rnd_symbol('msft','ibm', 'googl') new_col_0," +
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
                                        " from long_sequence(100)" +
                                        ") timestamp (timestamp) PARTITION BY DAY WAL"
                        );

                        try (TableReader rdr1 = getReader("x")) {

                            rdr1.goPassive();

                            execute("alter table x rename column new_col_0 to new_col_1");
                            execute("alter table x rename column n to new_col_0");
                            execute("alter table x drop column sym");
                            execute("alter table x drop column i");
                            execute("alter table x rename column new_col_1 to new_col_2");
                            execute("alter table x rename column new_col_0 to new_col_1");
                            execute("alter table x rename column new_col_2 to new_col_0");
                            execute("alter table x rename column ik to new_col_3");

                            drainWalQueue();


                            rdr1.reload();

                            String expected = "{\"columnCount\":15,\"columns\":[{\"index\":0,\"name\":\"new_col_0\",\"type\":\"SYMBOL\"},{\"index\":1,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":2,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":3,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":4,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":5,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":7,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":8,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":9,\"name\":\"new_col_3\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":11,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":12,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":13,\"name\":\"m\",\"type\":\"BINARY\"},{\"index\":14,\"name\":\"new_col_1\",\"type\":\"STRING\"}],\"timestampIndex\":2}";
                            sink.clear();
                            rdr1.getMetadata().toJson(sink);
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
    public void testRenameSymbolColumnReloadReader2() throws Exception {

        // Don't use TestFilesFacadeImpl because we want to remove renamed columns file
        // while they are opened by passive table reader.
        FilesFacade ff = new FilesFacadeImpl();
        assertMemoryLeak(
                ff,
                () -> {
                    try {
                        execute(
                                "create table x as (" +
                                        "select" +
                                        " rnd_symbol('msft','ibm', 'googl') sym," +
                                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                                        " timestamp_sequence(0, 1000000000) k," +
                                        " from long_sequence(1)" +
                                        ") timestamp (timestamp) PARTITION BY DAY WAL"
                        );

                        try (TableReader rdr1 = getReader("x")) {

                            rdr1.goPassive();

                            // Circle rename symbol column
                            execute("alter table x rename column sym to new_col_1");
                            execute("alter table x rename column new_col_1 to sym");

                            drainWalQueue();
                            rdr1.reload();

                            String expected = "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":1,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":2,\"name\":\"k\",\"type\":\"TIMESTAMP\"}],\"timestampIndex\":1}";
                            sink.clear();
                            rdr1.getMetadata().toJson(sink);
                            TestUtils.assertEquals(expected, sink);

                            rdr1.goPassive();

                            // Circle rename non-symbol column
                            execute("alter table x rename column k to new_col_1");
                            execute("alter table x rename column new_col_1 to k");

                            drainWalQueue();

                            rdr1.reload();
                            sink.clear();
                            rdr1.getMetadata().toJson(sink);
                            TestUtils.assertEquals(expected, sink);

                            assertSql("""
                                    sym\ttimestamp\tk
                                    msft\t2018-01-01T00:12:00.000000Z\t1970-01-01T00:00:00.000000Z
                                    """, "select * from x");

                            rdr1.goPassive();

                            // Symple rename symbol column
                            execute("alter table x rename column sym to new_col_1");
                            drainWalQueue();
                            rdr1.reload();

                            String expected2 = "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"new_col_1\",\"type\":\"SYMBOL\"},{\"index\":1,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":2,\"name\":\"k\",\"type\":\"TIMESTAMP\"}],\"timestampIndex\":1}";
                            sink.clear();
                            rdr1.getMetadata().toJson(sink);
                            TestUtils.assertEquals(expected2, sink);
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
    public void testRenameTimestampColumnAndCheckOpenReaderWithCursor() throws Exception {
        String expectedBefore = """
                i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.55991614\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522494\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.09750569\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS
                4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL
                5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9820662\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t
                6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tnull\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t
                7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO
                8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.07594013\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ
                9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425297\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG
                10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tnull\tnull\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff
                00000010 9a ef 88 cb\tCNGTNLEGPUHH
                """;

        String expectedAfter = """
                i\tsym\tamt\tts\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.55991614\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522494\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.09750569\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS
                4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL
                5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9820662\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t
                6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tnull\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t
                7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO
                8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.07594013\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ
                9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425297\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG
                10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tnull\tnull\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff
                00000010 9a ef 88 cb\tCNGTNLEGPUHH
                """;

        assertMemoryLeak(() -> {
            createX();

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals("timestamp", reader.getMetadata().getColumnName(3));
                //check cursor before renaming column
                TestUtils.assertReader(expectedBefore, reader, sink);

                try (TableWriter writer = getWriter("x")) {
                    writer.renameColumn("timestamp", "ts");
                    Assert.assertEquals("ts", writer.getMetadata().getColumnName(3));
                }
                //reload reader
                Assert.assertTrue(reader.reload());
                //check cursor after reload
                TestUtils.assertReader(expectedAfter, reader, sink);

                assertReader(expectedAfter, "x");

                Assert.assertEquals("ts", reader.getMetadata().getColumnName(3));
            }
        });
    }

    @Test
    public void testRenameWithSemicolonHalfWay() throws Exception {
        assertFailure("alter table x rename column l; to b", 29, "'to' expected");
    }

    @Test
    public void testRenameWithSemicolonHalfWay2() throws Exception {
        assertFailure("alter table x rename column l to l2; c to d", 35, "',' expected");
    }

    @Test
    public void testSameColumnName() throws Exception {
        assertFailure("alter table x rename column b to b", 33, "new column name is identical to existing name");
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertFailure("alter table y", 12, "table does not exist [table=y]");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                execute(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }

            engine.clear();
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
