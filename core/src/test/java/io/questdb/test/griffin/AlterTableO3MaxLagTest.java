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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.NumericException;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableO3MaxLagTest extends AbstractCairoTest {
    @Test
    public void setMaxUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tableSetMaxUncommittedRows";
            TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY);
            createX(tbl);
            try (TableReader rdr = getReader(tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 11111";
                execute(alterCommand, sqlExecutionContext);

                assertSql("maxUncommittedRows\n11111\n", "SELECT maxUncommittedRows FROM tables() WHERE table_name = '" + tableName + "'"
                );
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
            TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY);
            createX(tbl);
            try (TableReader rdr = getReader(tableName)) {
                String alterCommand2 = "ALTER TABLE " + tableName + " SET PARAM o3MaxLag = 1s";
                execute(alterCommand2, sqlExecutionContext);
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM maxUncommittedRows = 11111";
                execute(alterCommand, sqlExecutionContext);

                assertSql("maxUncommittedRows\to3MaxLag\n" +
                        "11111\t1000000\n", "SELECT maxUncommittedRows, o3MaxLag FROM tables() WHERE table_name = '" + tableName + "'"
                );
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
                execute(alterCommand, sqlExecutionContext);
                String alterCommand2 = "ALTER TABLE " + tableName + " SET PARAM o3MaxLag = 0s";
                execute(alterCommand2, sqlExecutionContext);

                assertSql("maxUncommittedRows\to3MaxLag\n" +
                        "0\t0\n", "SELECT maxUncommittedRows, o3MaxLag FROM tables() WHERE table_name = '" + tableName + "'"
                );
                rdr.reload();
                Assert.assertEquals(0, rdr.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(0, rdr.getMetadata().getO3MaxLag());
            }

            assertX(tableName);
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToReopenBackMetaFile() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
        spinLockTimeout = 1;
        assertMemoryLeak(() -> {
            TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY);
            createX(tbl);
            engine.releaseAllWriters();

            ff = new TestFilesFacadeImpl() {
                int attempt = 0;

                @Override
                public int rename(LPSZ from, LPSZ to) {
                    if (Utf8s.endsWithAscii(to, TableUtils.META_FILE_NAME) && attempt++ < 2) {
                        return -1;
                    }
                    return super.rename(from, to);
                }

            };
            String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
            try {
                execute(alterCommand, sqlExecutionContext);
                Assert.fail("Alter table should fail");
            } catch (CairoError e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
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
            TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY);
            AbstractCairoTest.create(tbl.timestamp("ts")
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG));

            ff = new TestFilesFacadeImpl() {
                int attempt = 0;

                @Override
                public int rename(LPSZ from, LPSZ to) {
                    if (Utf8s.endsWithAscii(to, TableUtils.META_FILE_NAME) && attempt++ == 0) {
                        return Files.FILES_RENAME_ERR_OTHER;
                    }
                    return super.rename(from, to);
                }
            };
            String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
            try {
                execute(alterCommand, sqlExecutionContext);
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
            execute(alterCommand, sqlExecutionContext);
            assertSql("maxUncommittedRows\n11111\n", "SELECT maxUncommittedRows FROM tables() WHERE table_name = 'X'");
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToSwapMetadataUntilWriterReopen() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
            spinLockTimeout = 1;
            TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY);
            AbstractCairoTest.create(tbl.timestamp("ts")
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG));

            ff = new TestFilesFacadeImpl() {
                @Override
                public int rename(LPSZ from, LPSZ to) {
                    if (Utf8s.endsWithAscii(to, TableUtils.META_FILE_NAME)) {
                        return Files.FILES_RENAME_ERR_OTHER;
                    }
                    return super.rename(from, to);
                }

            };
            String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
            try {
                execute(alterCommand, sqlExecutionContext);
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
            execute(alterCommand, sqlExecutionContext);
            assertSql("maxUncommittedRows\n11111\n", "SELECT maxUncommittedRows FROM tables() WHERE table_name = 'X'");
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToSwapMetadataUntilWriterReopen2() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
            spinLockTimeout = 1;
            TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY);
            AbstractCairoTest.create(tbl.timestamp("ts")
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG));

            ff = new TestFilesFacadeImpl() {
                @Override
                public int rename(LPSZ from, LPSZ to) {
                    if (Utf8s.endsWithAscii(to, TableUtils.META_FILE_NAME)) {
                        return Files.FILES_RENAME_ERR_OTHER;
                    }
                    return super.rename(from, to);
                }

            };
            String alterCommand = "ALTER TABLE X SET PARAM maxUncommittedRows = 11111";
            try {
                execute(alterCommand, sqlExecutionContext);
                Assert.fail("Alter table should fail");
            } catch (CairoError e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
            }

            engine.releaseAllReaders();
            // change spin timeout for the test to fail faster
            spinLockTimeout = 100;
            try (TableReader ignored = getReader("X")) {
                Assert.fail();
            } catch (CairoException ignored) {
            }

            ff = new TestFilesFacadeImpl();
            // Now try with another failure.
            engine.releaseAllWriters();
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ from) {
                    if (Utf8s.endsWithAscii(from, TableUtils.META_FILE_NAME)) {
                        return -1;
                    }
                    return super.openRO(from);
                }
            };
            try {
                execute(alterCommand, sqlExecutionContext);
                Assert.fail();
            } catch (CairoException | SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open");
            }
        });
    }

    @Test
    public void setMaxUncommittedRowsMissingEquals() throws Exception {
        assertException("ALTER TABLE X SET PARAM maxUncommittedRows 100",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                43,
                "'=' expected");
    }

    @Test
    public void setMaxUncommittedRowsNegativeValue() throws Exception {
        assertException("ALTER TABLE X SET PARAM maxUncommittedRows = -1",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                24,
                "invalid value [value=-,parameter=maxUncommittedRows]");
    }

    @Test
    public void setO3MaxLag() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "setO3MaxLagTable";
            TableModel tbl = new TableModel(configuration, tableName, PartitionBy.DAY);
            createX(tbl);
            try (TableReader rdr = getReader(tableName)) {
                String alterCommand = "ALTER TABLE " + tableName + " SET PARAM o3MaxLag = 111s";
                execute(alterCommand, sqlExecutionContext);

                assertSql("o3MaxLag\n111000000\n", "SELECT o3MaxLag FROM tables() WHERE table_name = '" + tableName + "'"
                );
                rdr.reload();
                Assert.assertEquals(111000000L, rdr.getMetadata().getO3MaxLag());
            }
            assertX(tableName);
        });
    }

    @Test
    public void setO3MaxLagWrongSetSyntax() throws Exception {
        assertException("ALTER TABLE X SET o3MaxLag = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                18,
                "'param', 'ttl' or 'type' expected");
    }

    @Test
    public void setO3MaxLagWrongSetSyntax2() throws Exception {
        assertException("ALTER TABLE X PARAM o3MaxLag = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                14,
                SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR);
    }

    @Test
    public void setO3MaxLagWrongTimeQualifier() throws Exception {
        assertException("ALTER TABLE X SET PARAM o3MaxLag = 111days",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                27,
                "interval qualifier");
    }

    @Test
    public void setO3MaxLagWrongTimeQualifier2() throws Exception {
        assertException("ALTER TABLE X SET PARAM o3MaxLag = 111ml",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                29,
                "interval qualifier");
    }

    @Test
    public void setUnknownParameter() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY);
            createX(tbl);
            try (TableReader ignored = getReader("X")) {
                try {
                    execute("alter TABLE X SET PARAM vommitLag = 111s");
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
                    execute("create table x1(a int, b double, ts timestamp) timestamp(ts) partition by DAY");
                    execute("alter table x1 set param maxUncommittedRows = 150", sqlExecutionContext);
                    assertSql(
                            "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    "1\tx1\tts\tDAY\t150\t300000000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where table_name = 'x1'"
                    );

                    // test open table writer
                    engine.releaseInactive();
                    engine.releaseAllWriters();
                    getWriter("x1").close();

                    assertSql(
                            "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    "1\tx1\tts\tDAY\t150\t300000000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where table_name = 'x1'"
                    );
                }
        );
    }

    private void assertLagUnits(String sql, String expected) throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("create table x1(a int, b double, ts timestamp) timestamp(ts) partition by DAY");
                    execute(sql, sqlExecutionContext);
                    assertSql(
                            "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    expected, "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where table_name = 'x1'"
                    );

                    // test open table writer
                    engine.releaseInactive();
                    engine.releaseAllWriters();
                    getWriter("x1").close();

                    assertSql(
                            "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    expected, "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where table_name = 'x1'"
                    );
                }
        );
    }

    private void assertX(String tableName) throws SqlException {
        engine.releaseAllReaders();
        assertSql("ts\ti\tl\n" +
                "2020-01-01T02:23:59.900000Z\t1\t1\n" +
                "2020-01-01T04:47:59.800000Z\t2\t2\n" +
                "2020-01-01T07:11:59.700000Z\t3\t3\n" +
                "2020-01-01T09:35:59.600000Z\t4\t4\n" +
                "2020-01-01T11:59:59.500000Z\t5\t5\n" +
                "2020-01-01T14:23:59.400000Z\t6\t6\n" +
                "2020-01-01T16:47:59.300000Z\t7\t7\n" +
                "2020-01-01T19:11:59.200000Z\t8\t8\n" +
                "2020-01-01T21:35:59.100000Z\t9\t9\n" +
                "2020-01-01T23:59:59.000000Z\t10\t10\n", "select * from " + tableName
        );
    }

    private void createX(TableModel tbl) throws NumericException, SqlException {
        createPopulateTable(tbl.timestamp("ts")
                .col("i", ColumnType.INT)
                .col("l", ColumnType.LONG), 10, "2020-01-01", 1);
    }
}
