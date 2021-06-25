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

import static io.questdb.griffin.CompiledQuery.ALTER;

public class AlterTableRenameColumnTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBadSyntax() throws Exception {
        assertFailure("alter table x rename column l ,m", 30, "to' expected");
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
        assertFailure("alter table x rename column y yy", 28, "Invalid column: y");
    }

    @Test
    public void testNewNameAlreadyExists() throws Exception {
        assertFailure("alter table x rename column l to b", 33, " column already exists");
    }

    @Test
    public void testRenameColumn() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertEquals(ALTER, compiler.compile("alter table x rename column e to z", sqlExecutionContext).getType());

                        String expected = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"i\",\"type\":\"INT\"},{\"index\":1,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":2,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":3,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":4,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":5,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":6,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":7,\"name\":\"z\",\"type\":\"FLOAT\"},{\"index\":8,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":9,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":10,\"name\":\"ik\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":13,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":14,\"name\":\"m\",\"type\":\"BINARY\"},{\"index\":15,\"name\":\"n\",\"type\":\"STRING\"}],\"timestampIndex\":3}";
                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
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
            compiler.compile("create table x1 (a int, b double, t timestamp) timestamp(t)", sqlExecutionContext);

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x1")) {
                Assert.assertEquals("b", reader.getMetadata().getColumnName(1));

                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x1", "testing")) {
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
        String expectedBefore = "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.5599\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.0975\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS\n" +
                "4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9442\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\n" +
                "5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9821\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t\n" +
                "6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tNaN\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t\n" +
                "7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO\n" +
                "8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.0759\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\n" +
                "9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG\n" +
                "10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tNaN\tNaN\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff\n" +
                "00000010 9a ef 88 cb\tCNGTNLEGPUHH\n";

        String expectedAfter = "i\tsym\tamt\ttimestamp\tbb\tcc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.5599\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.0975\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS\n" +
                "4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9442\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\n" +
                "5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9821\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t\n" +
                "6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tNaN\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t\n" +
                "7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO\n" +
                "8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.0759\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\n" +
                "9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG\n" +
                "10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tNaN\tNaN\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff\n" +
                "00000010 9a ef 88 cb\tCNGTNLEGPUHH\n";

        assertMemoryLeak(() -> {
            createX();

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                Assert.assertEquals("b", reader.getMetadata().getColumnName(4));
                //check cursor before renaming column
                TestUtils.assertReader(expectedBefore, reader, sink);

                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "testing")) {
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
    public void testRenameColumnExistingReader() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            Assert.assertEquals(ALTER, compiler.compile("alter table x rename column e to z", sqlExecutionContext).getType());
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
    public void testRenameTimestampColumnAndCheckOpenReaderWithCursor() throws Exception {
        String expectedBefore = "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.5599\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.0975\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS\n" +
                "4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9442\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\n" +
                "5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9821\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t\n" +
                "6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tNaN\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t\n" +
                "7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO\n" +
                "8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.0759\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\n" +
                "9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG\n" +
                "10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tNaN\tNaN\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff\n" +
                "00000010 9a ef 88 cb\tCNGTNLEGPUHH\n";

        String expectedAfter = "i\tsym\tamt\tts\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.5599\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.0975\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS\n" +
                "4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9442\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\n" +
                "5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9821\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t\n" +
                "6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tNaN\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t\n" +
                "7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO\n" +
                "8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.0759\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\n" +
                "9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG\n" +
                "10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tNaN\tNaN\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff\n" +
                "00000010 9a ef 88 cb\tCNGTNLEGPUHH\n";

        assertMemoryLeak(() -> {
            createX();

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                Assert.assertEquals("timestamp", reader.getMetadata().getColumnName(3));
                //check cursor before renaming column
                TestUtils.assertReader(expectedBefore, reader, sink);

                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "testing")) {
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
    public void testSameColumnName() throws Exception {
        assertFailure("alter table x rename column b to b", 33, "new column name is identical to existing name");
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
