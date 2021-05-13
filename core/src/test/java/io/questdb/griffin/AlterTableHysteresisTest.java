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

public class AlterTableHysteresisTest extends AbstractGriffinTest {
    @Test
    public void setMaxUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                String alterCommand = "ALTER TABLE X SET PARAM O3MaxUncommittedRows = 11111";
                compiler.compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT o3MaxUncommittedRows FROM tables() WHERE name = 'X'", "o3MaxUncommittedRows\n11111\n");
                rdr.reload();
                Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
            }
            assertX();
        });
    }

    private void assertX() throws SqlException {
        engine.releaseAllReaders();
        assertSql("select * from x",
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

    @Test
    public void setCommitHysteresis() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                createX(tbl);
            }
            try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                String alterCommand = "ALTER TABLE X SET PARAM O3CommitHysteresis = 111s";
                compiler.compile(alterCommand, sqlExecutionContext);

                assertSql("SELECT o3CommitHysteresisMicros FROM tables() WHERE name = 'X'", "o3CommitHysteresisMicros\n111000000\n");
                rdr.reload();
                Assert.assertEquals(111000000L, rdr.getMetadata().getO3CommitHysteresisMicros());
            }
            assertX();
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
                String alterCommand = "ALTER TABLE X SET PARAM O3MaxUncommittedRows = 11111";
                try {
                    compiler.compile(alterCommand, sqlExecutionContext);
                    Assert.fail("Alter table should fail");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table 'X' could not be altered");
                }

                try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                    Assert.assertEquals(configuration.getO3MaxUncommittedRows(), rdr.getMetadata().getMaxUncommittedRows());
                }

                // Now try with success.
                ff = new FilesFacadeImpl();
                compiler.compile(alterCommand, sqlExecutionContext);
                assertSql("SELECT o3MaxUncommittedRows FROM tables() WHERE name = 'X'", "o3MaxUncommittedRows\n11111\n");
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
                String alterCommand = "ALTER TABLE X SET PARAM O3MaxUncommittedRows = 11111";
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
                assertSql("SELECT o3MaxUncommittedRows FROM tables() WHERE name = 'X'", "o3MaxUncommittedRows\n11111\n");
            }
        });
    }

    @Test
    public void setMaxUncommittedRowsFailsToReopenBackMetaFile() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CairoTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

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
                String alterCommand = "ALTER TABLE X SET PARAM O3MaxUncommittedRows = 11111";
                try {
                    compiler.compile(alterCommand, sqlExecutionContext);
                    Assert.fail("Alter table should fail");
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-only");
                }

                try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                    Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
                }
            }
        });
    }

    @Test
    public void setCommitHysteresisWrongTimeQualifier() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM O3CommitHysteresis = 111days",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                27,
                "interval qualifier");
    }

    @Test
    public void setCommitHysteresisWrongTimeQualifier2() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM O3CommitHysteresis = 111us",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                29,
                "interval qualifier");
    }

    @Test
    public void setCommitHysteresisWrongSetSyntax() throws Exception {
        assertFailure("ALTER TABLE X SET O3CommitHysteresis = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                18,
                "'param' expected");
    }

    @Test
    public void setCommitHysteresisWrongSetSyntax2() throws Exception {
        assertFailure("ALTER TABLE X PARAM O3CommitHysteresis = 111ms",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                14,
                "'set' or 'rename' expected");
    }

    @Test
    public void setMaxUncommittedRowsNegativeValue() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM O3MaxUncommittedRows = -1",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                24,
                "invalid value [value=-,parameter=O3MaxUncommittedRows]");
    }

    @Test
    public void setMaxUncommittedRowsMissingEquals() throws Exception {
        assertFailure("ALTER TABLE X SET PARAM O3MaxUncommittedRows 100",
                "CREATE TABLE X (ts TIMESTAMP, i INT, l LONG) timestamp(ts) PARTITION BY MONTH",
                45,
                "'=' expected");
    }
}
