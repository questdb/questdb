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
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.NumericException;
import io.questdb.std.str.LPSZ;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableCommitLagTest extends AbstractGriffinTest {
    @Test
    public void setCommitLag() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "setCommitLagTable";
            try (TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM commitLag = 111s";
                compiler.compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT commitLag FROM tables() WHERE name = '" + tableName + "'",
                        "commitLag\n111000000\n");
                rdr.reload();
                Assert.assertEquals(111000000L, rdr.getMetadata().getCommitLag());
            }
            assertX(tableName);
        });
    }

    @Test
    public void setCommitLagWrongSetSyntax() throws Exception {
        assertFailure("ALTER TABLE X SET commitLag = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                18,
                "'param' expected");
    }

    @Test
    public void setCommitLagWrongSetSyntax2() throws Exception {
        assertFailure("ALTER TABLE X PARAM commitLag = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                14,
                "'set' or 'rename' expected");
    }

    @Test
    public void setCommitLagWrongTimeQualifier() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM commitLag = 111days",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                27,
                "interval qualifier");
    }

    @Test
    public void setCommitLagWrongTimeQualifier2() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM commitLag = 111ml",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                29,
                "interval qualifier");
    }

    @Test
    public void setMaxUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tableSetMaxUncommittedRows";
            try (TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 11111";
                compiler.compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT maxUncommittedRows FROM tables() WHERE name = '" + tableName + "'",
                        "maxUncommittedRows\n11111\n");
                rdr.reload();
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
            }
            assertX(tableName);
        });
    }

    @Test
    public void setMaxUncommittedRowsAndCommitLag() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tableSetMaxUncommittedRows";
            try (TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                String alterCommand2 = "ALTER TABLE " + tableName + " SET PARAM CommitLag = 1s";
                compiler.compile(alterCommand2, sqlExecutionContext);
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 11111";
                compiler.compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT maxUncommittedRows, commitLag FROM tables() WHERE name = '" + tableName + "'",
                        "maxUncommittedRows\tcommitLag\n" +
                                "11111\t1000000\n");
                rdr.reload();
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(1000000, rdr.getMetadata().getCommitLag());
            }
            engine.releaseAllReaders();
            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(1000000, rdr.getMetadata().getCommitLag());
            }

            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 0";
                compiler.compile(alterCommand, sqlExecutionContext);
                String alterCommand2 = "ALTER TABLE " + tableName + " SET PARAM CommitLag = 0s";
                compiler.compile(alterCommand2, sqlExecutionContext);

                assertSql("SELECT maxUncommittedRows, commitLag FROM tables() WHERE name = '" + tableName + "'",
                        "maxUncommittedRows\tcommitLag\n" +
                                "0\t0\n");
                rdr.reload();
                Assert.assertEquals(0, rdr.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(0, rdr.getMetadata().getCommitLag());
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
            ff = new FilesFacadeImpl() {
                int attempt = 0;

                @Override
                public long openRO(LPSZ path) {
                    if (Chars.endsWith(path, TableUtils.META_FILE_NAME) && attempt++ == 1) {
                        return -1;
                    }
                    return super.openRO(path);
                }

            };
            String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
            try {
                compiler.compile(alterCommand, sqlExecutionContext);
                Assert.fail("Alter table should fail");
            } catch (CairoError e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-only");
            }

            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToSwapMetadataOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CairoTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                ff = new FilesFacadeImpl() {
                    int attempt = 0;

                    @Override
                    public boolean rename(LPSZ from, LPSZ to) {
                        if (Chars.endsWith(to, TableUtils.META_FILE_NAME) && attempt++ == 0) {
                            return false;
                        }
                        return super.rename(from, to);
                    }

                };
                String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
                try {
                    compiler.compile(alterCommand, sqlExecutionContext);
                    Assert.fail("Alter table should fail");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table 'X' could not be altered");
                }

                try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                    Assert.assertEquals(configuration.getMaxUncommittedRows(), rdr.getMetadata().getMaxUncommittedRows());
                }

                // Now try with success.
                ff = new FilesFacadeImpl();
                compiler.compile(alterCommand, sqlExecutionContext);
                assertSql("SELECT maxUncommittedRows FROM tables() WHERE name = 'X'", "maxUncommittedRows\n11111\n");
            }
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToSwapMetadataUntilWriterReopen() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CairoTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                ff = new FilesFacadeImpl() {
                    @Override
                    public boolean rename(LPSZ from, LPSZ to) {
                        if (Chars.endsWith(to, TableUtils.META_FILE_NAME)) {
                            return false;
                        }
                        return super.rename(from, to);
                    }

                };
                String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
                try {
                    compiler.compile(alterCommand, sqlExecutionContext);
                    Assert.fail("Alter table should fail");
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
                }

                try (TableReader ignored = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                // Now try with success.
                engine.releaseAllWriters();
                ff = new FilesFacadeImpl();
                compiler.compile(alterCommand, sqlExecutionContext);
                assertSql("SELECT maxUncommittedRows FROM tables() WHERE name = 'X'", "maxUncommittedRows\n11111\n");
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
    public void setUnknownParameter() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader ignored = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                try {
                    compiler.compile("ALTER TABLE X SET PARAM vommitLag = 111s", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "unknown parameter 'vommitLag'");
                }
            }
            assertX("X");
        });
    }

    @Test
    public void testSetMaxUncommitted() throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile(
                            "create table x1(a int, b double, ts timestamp) timestamp(ts) partition by DAY",
                            sqlExecutionContext
                    );
                    compiler.compile("alter table x1 set param maxUncommittedRows = 150", sqlExecutionContext);
                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n" +
                                    "1\tx1\tts\tDAY\t150\t0\n"
                    );

                    // test open table writer
                    engine.releaseInactive();
                    engine.releaseAllWriters();
                    engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x1", "testing").close();

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n" +
                                    "1\tx1\tts\tDAY\t150\t0\n"
                    );
                }
        );
    }

    @Test
    public void testLagUnitsMs() throws Exception {
        assertLagUnits("alter table x1 set param commitLag = 100ms", "1\tx1\tts\tDAY\t1000\t100000\n");
    }

    @Test
    public void testLagUnitsUs() throws Exception {
        assertLagUnits("alter table x1 set param commitLag = 10us", "1\tx1\tts\tDAY\t1000\t10\n");
    }

    @Test
    public void testLagUnitsMinutes() throws Exception {
        assertLagUnits("alter table x1 set param commitLag = 10m", "1\tx1\tts\tDAY\t1000\t600000000\n");
    }

    @Test
    public void testLagUnitsHours() throws Exception {
        assertLagUnits("alter table x1 set param commitLag = 2h", "1\tx1\tts\tDAY\t1000\t7200000000\n");
    }

    @Test
    public void testLagUnitsDays() throws Exception {
        assertLagUnits("alter table x1 set param commitLag = 7d", "1\tx1\tts\tDAY\t1000\t604800000000\n");
    }

    private void assertLagUnits(String sql, String expected) throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile(
                            "create table x1(a int, b double, ts timestamp) timestamp(ts) partition by DAY",
                            sqlExecutionContext);
                    compiler.compile(sql, sqlExecutionContext);
                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n" +
                                    expected
                    );

                    // test open table writer
                    engine.releaseInactive();
                    engine.releaseAllWriters();
                    engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x1", "testing").close();

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "tables() where name = 'x1'",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n" +
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
