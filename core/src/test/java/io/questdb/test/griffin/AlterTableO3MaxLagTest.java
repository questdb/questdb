/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.NumericException;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableO3MaxLagTest extends AbstractGriffinTest {
    @Test
    public void setMaxUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tableSetMaxUncommittedRows";
            try (TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = getReader(tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 11111";
                compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT maxUncommittedRows FROM tables() WHERE name = '" + tableName + "'",
                        "maxUncommittedRows\n11111\n");
                rdr.reload();
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
            }
            assertX(tableName);
        });
    }

    @Test
    public void setMaxUncommittedRowsAndO3MaxLag() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tableSetMaxUncommittedRows";
            try (TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = getReader(tableName)) {
                String alterCommand2 = "ALTER TABLE " + tableName + " SET PARAM o3MaxLag = 1s";
                compile(alterCommand2, sqlExecutionContext);
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 11111";
                compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT maxUncommittedRows, o3MaxLag FROM tables() WHERE name = '" + tableName + "'",
                        "maxUncommittedRows\to3MaxLag\n" +
                                "11111\t1000000\n");
                rdr.reload();
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(1000000, rdr.getMetadata().getO3MaxLag());
            }
            engine.releaseAllReaders();
            try (TableReader rdr = getReader(tableName)) {
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(1000000, rdr.getMetadata().getO3MaxLag());
            }

            try (TableReader rdr = getReader(tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 0";
                compile(alterCommand, sqlExecutionContext);
                String alterCommand2 = "ALTER TABLE " + tableName + " SET PARAM o3MaxLag = 0s";
                compile(alterCommand2, sqlExecutionContext);

                assertSql("SELECT maxUncommittedRows, o3MaxLag FROM tables() WHERE name = '" + tableName + "'",
                        "maxUncommittedRows\to3MaxLag\n" +
                                "0\t0\n");
                rdr.reload();
                Assert.assertEquals(0, rdr.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(0, rdr.getMetadata().getO3MaxLag());
            }

            assertX(tableName);
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToReopenBackMetaFile() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                createX(tbl);
            }
            engine.releaseAllWriters();
            spinLockTimeout = 1;

            ff = new TestFilesFacadeImpl() {
                int attempt = 0;

                @Override
                public int openRO(LPSZ path) {
                    if (Chars.endsWith(path, TableUtils.META_FILE_NAME) && (attempt++ == 2)) {
                        return -1;
                    }
                    return super.openRO(path);
                }

            };
            String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
            try {
                compile(alterCommand, sqlExecutionContext);
                Assert.fail("Alter table should fail");
            } catch (CairoError e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-only");
            }

            engine.releaseAllReaders();
            try (TableReader ignore = getReader("X")) {
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Metadata read timeout");
            }

            // Reopen writer to fix
            try (TableWriter ignore = getWriter("X")) {
                try (TableReader reader = getReader("X")) {
                    Assert.assertEquals(1000, reader.getMaxUncommittedRows());
                }
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToSwapMetadataOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CreateTableTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                ff = new TestFilesFacadeImpl() {
                    int attempt = 0;

                    @Override
                    public int rename(LPSZ from, LPSZ to) {
                        if (Chars.endsWith(to, TableUtils.META_FILE_NAME) && attempt++ == 0) {
                            return Files.FILES_RENAME_ERR_OTHER;
                        }
                        return super.rename(from, to);
                    }
                };
                String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail("Alter table should fail");
                } catch (CairoException e) {
                    Assert.assertEquals(12, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
                }

                try (TableReader rdr = getReader("X")) {
                    Assert.assertEquals(configuration.getMaxUncommittedRows(), rdr.getMetadata().getMaxUncommittedRows());
                }

                // Now try with success.
                ff = new TestFilesFacadeImpl();
                compile(alterCommand, sqlExecutionContext);
                assertSql("SELECT maxUncommittedRows FROM tables() WHERE name = 'X'", "maxUncommittedRows\n11111\n");
            }
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToSwapMetadataUntilWriterReopen() throws Exception {
        assertMemoryLeak(() -> {
            spinLockTimeout = 1;
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CreateTableTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                ff = new TestFilesFacadeImpl() {
                    @Override
                    public int rename(LPSZ from, LPSZ to) {
                        if (Chars.endsWith(to, TableUtils.META_FILE_NAME)) {
                            return Files.FILES_RENAME_ERR_OTHER;
                        }
                        return super.rename(from, to);
                    }

                };
                String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail("Alter table should fail");
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
                }

                engine.releaseAllReaders();
                try (TableReader ignored = getReader("X")) {
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                // Now try with success.
                engine.clear();
                ff = new TestFilesFacadeImpl();
                compile(alterCommand, sqlExecutionContext);
                assertSql("SELECT maxUncommittedRows FROM tables() WHERE name = 'X'", "maxUncommittedRows\n11111\n");
            }
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToSwapMetadataUntilWriterReopen2() throws Exception {
        assertMemoryLeak(() -> {
            spinLockTimeout = 1;
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CreateTableTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                ff = new TestFilesFacadeImpl() {
                    @Override
                    public int rename(LPSZ from, LPSZ to) {
                        if (Chars.endsWith(to, TableUtils.META_FILE_NAME)) {
                            return Files.FILES_RENAME_ERR_OTHER;
                        }
                        return super.rename(from, to);
                    }

                };
                String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail("Alter table should fail");
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
                }

                engine.releaseAllReaders();
                try (TableReader ignored = getReader("X")) {
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                ff = new TestFilesFacadeImpl();
                // Now try with another failure.
                engine.releaseAllWriters();
                ff = new TestFilesFacadeImpl() {
                    @Override
                    public int openRO(LPSZ from) {
                        if (Chars.endsWith(from, TableUtils.META_FILE_NAME)) {
                            return -1;
                        }
                        return super.openRO(from);
                    }
                };
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (CairoException | SqlException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-only");
                }
            }
        });
    }

    @Test
    public void setMaxUncommittedRowsMissingEquals() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM maxUncommittedRows 100",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                43,
                "'=' expected");
    }

    @Test
    public void setMaxUncommittedRowsNegativeValue() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM maxUncommittedRows = -1",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                24,
                "invalid value [value=-,parameter=maxUncommittedRows]");
    }

    @Test
    public void setO3MaxLag() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "setO3MaxLagTable";
            try (TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = getReader(tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM o3MaxLag = 111s";
                compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT o3MaxLag FROM tables() WHERE name = '" + tableName + "'",
                        "o3MaxLag\n111000000\n");
                rdr.reload();
                Assert.assertEquals(111000000L, rdr.getMetadata().getO3MaxLag());
            }
            assertX(tableName);
        });
    }

    @Test
    public void setO3MaxLagWrongSetSyntax() throws Exception {
        assertFailure("ALTER TABLE X SET o3MaxLag = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                18,
                "'param' or 'type' expected");
    }

    @Test
    public void setO3MaxLagWrongSetSyntax2() throws Exception {
        assertFailure("ALTER TABLE X PARAM o3MaxLag = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                14,
                "'add', 'alter', 'attach', 'detach', 'drop', 'resume', 'rename', 'set' or 'squash' expected");
    }

    @Test
    public void setO3MaxLagWrongTimeQualifier() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM o3MaxLag = 111days",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                27,
                "interval qualifier");
    }

    @Test
    public void setO3MaxLagWrongTimeQualifier2() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM o3MaxLag = 111ml",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                29,
                "interval qualifier");
    }

    @Test
    public void setUnknownParameter() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader ignored = getReader("X")) {
                try {
                    compile("alter TABLE X SET PARAM vommitLag = 111s", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "unknown parameter 'vommitLag'");
                }
            }
            assertX("X");
        });
    }

    @Test
    public void testLagUnitsDays() throws Exception {
        assertLagUnits("alter table x1 set param o3MaxLag = 7d", "1\tx1\tts\tDAY\t1000\t604800000000\n");
    }

    @Test
    public void testLagUnitsHours() throws Exception {
        assertLagUnits("alter table x1 set param o3MaxLag = 2h", "1\tx1\tts\tDAY\t1000\t7200000000\n");
    }

    @Test
    public void testLagUnitsMinutes() throws Exception {
        assertLagUnits("alter table x1 set param o3MaxLag = 10m", "1\tx1\tts\tDAY\t1000\t600000000\n");
    }

    @Test
    public void testLagUnitsMs() throws Exception {
        assertLagUnits("alter table x1 set param o3MaxLag = 100ms", "1\tx1\tts\tDAY\t1000\t100000\n");
    }

    @Test
    public void testLagUnitsUs() throws Exception {
        assertLagUnits("alter table x1 set param o3MaxLag = 10us", "1\tx1\tts\tDAY\t1000\t10\n");
    }

    @Test
    public void testSetMaxUncommitted() throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile(
                            "create table x1(a int, b double, ts timestamp) timestamp(ts) partition by DAY",
                            sqlExecutionContext
                    );
                    compile("alter table x1 set param maxUncommittedRows = 150", sqlExecutionContext);
                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    "1\tx1\tts\tDAY\t150\t300000000\n"
                    );

                    // test open table writer
                    engine.releaseInactive();
                    engine.releaseAllWriters();
                    getWriter("x1").close();

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    "1\tx1\tts\tDAY\t150\t300000000\n"
                    );
                }
        );
    }

    private void assertLagUnits(String sql, String expected) throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile(
                            "create table x1(a int, b double, ts timestamp) timestamp(ts) partition by DAY",
                            sqlExecutionContext);
                    compile(sql, sqlExecutionContext);
                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    expected
                    );

                    // test open table writer
                    engine.releaseInactive();
                    engine.releaseAllWriters();
                    getWriter("x1").close();

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    expected
                    );
                }
        );
    }

    private void assertX(String tableName) throws SqlException {
        engine.releaseAllReaders();
        assertSql("select * from " + tableName,
                "ts\ti\tl\n" +
                        "2020-01-01T02:23:59.900000Z\t1\t1\n" +
                        "2020-01-01T04:47:59.800000Z\t2\t2\n" +
                        "2020-01-01T07:11:59.700000Z\t3\t3\n" +
                        "2020-01-01T09:35:59.600000Z\t4\t4\n" +
                        "2020-01-01T11:59:59.500000Z\t5\t5\n" +
                        "2020-01-01T14:23:59.400000Z\t6\t6\n" +
                        "2020-01-01T16:47:59.300000Z\t7\t7\n" +
                        "2020-01-01T19:11:59.200000Z\t8\t8\n" +
                        "2020-01-01T21:35:59.100000Z\t9\t9\n" +
                        "2020-01-01T23:59:59.000000Z\t10\t10\n");
    }

    private void createX(TableModel tbl) throws NumericException, SqlException {
        createPopulateTable(tbl.timestamp("ts")
                .col("i", ColumnType.INT)
                .col("l", ColumnType.LONG), 10, "2020-01-01", 1);
    }
}
