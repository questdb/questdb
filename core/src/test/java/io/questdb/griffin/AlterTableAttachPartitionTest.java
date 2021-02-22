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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


public class AlterTableAttachPartitionTest extends AbstractGriffinTest {
    private final static Log LOG = LogFactory.getLog(AlterTableAttachPartitionTest.class);
    private final int DIR_MODE = configuration.getMkDirMode();

    @Test
    public void testAttachActive2Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                coyAttachPartition(src, dst, "2020-01-09", "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachActive3Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                // 3 partitions unordered
                coyAttachPartition(src, dst, "2020-01-09", "2020-01-10", "2020-01-01");
            }
        });
    }

    @Test
    public void testAttachActiveWrittenPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                coyAttachPartition(src, dst, "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {
                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compiler.compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[38] cannot attach missing partition folder '2020-01-01'", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                coyAttachPartition(src, dst, "2020-01-01");
            }
        });
    }

    private void copyDirectory(Path from, Path to) throws IOException {
        LOG.info().$("copying folder [from=").$(from).$(", to=").$(to).$(']').$();
        if (Files.mkdir(to, DIR_MODE) != 0) {
            Assert.fail("Cannot create " + to.toString() + ". Error: " + Os.errno());
        }

        java.nio.file.Path dest = java.nio.file.Path.of(to.toString() + Files.SEPARATOR);
        java.nio.file.Path src = java.nio.file.Path.of(from.toString() + Files.SEPARATOR);
        java.nio.file.Files.walk(src)
                .forEach(file -> {
                    java.nio.file.Path destination = dest.resolve(src.relativize(file));
                    try {
                        java.nio.file.Files.copy(file, destination, REPLACE_EXISTING);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    private void copyPartitionToBackup(String src, String partitionFolder, String dst) throws IOException {
        try (Path p1 = new Path().of(configuration.getRoot()).concat(src).concat(partitionFolder).$();
             Path backup = new Path().of(configuration.getRoot())) {

            copyDirectory(p1, backup.concat(dst).concat(partitionFolder).$());
        }
    }

    private void coyAttachPartition(TableModel src, TableModel dst, String... partitionList) throws IOException, SqlException, NumericException {
        StringBuilder partitions = new StringBuilder();
        for (int i = 0; i < partitionList.length; i++) {
            if (i > 0) {
                partitions.append(",");
            }
            partitions.append("'");
            partitions.append(partitionList[i]);
            partitions.append("'");
        }

        String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST " + partitions + ";";

        StringBuilder partitionsIn = new StringBuilder();
        for (int i = 0; i < partitionList.length; i++) {
            if (i > 0) {
                partitionsIn.append(" OR ");
            }
            partitionsIn.append("ts = '");
            partitionsIn.append(partitionList[i]);
            partitionsIn.append("'");
        }

        String withClause = ", t1 as (select 1 as id, count() as cnt from src WHERE " + partitionsIn + ")\n";
        for (int i = 0; i < partitionList.length; i++) {
            copyPartitionToBackup(src.getName(), partitionList[i], dst.getName());
        }

        compiler.compile(alterCommand, sqlExecutionContext);
        TestUtils.assertEquals(
                "cnt\n" +
                        "0\n",
                executeSql("with " +
                        "t2 as (select 1 as id, count() as cnt from dst)\n" +
                        withClause +
                        "select t1.cnt - t2.cnt as cnt\n" +
                        "from t2 cross join t1"
                )
        );

        long timestamp = 0;
        for (int i = 0; i < partitionList.length; i++) {
            long ts = TimestampFormatUtils.parseTimestamp(partitionList[i] + "T23:59:59.999z");
            if (ts > timestamp) {
                timestamp = ts;
            }
        }

        // Check table is writable after partition attach
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "dst")) {

            var row = writer.newRow(timestamp);
            row.putLong(0, 1L);
            row.putInt(1, 1);
            row.append();
            writer.commit();
        }

        TestUtils.assertEquals(
                "cnt\n" +
                        "-1\n",
                executeSql("with " +
                        "t2 as (select 1 as id, count() as cnt from dst)\n" +
                        withClause +
                        "select t1.cnt - t2.cnt as cnt\n" +
                        "from t2 cross join t1"
                )
        );
    }

    private CharSequence executeSql(String sql) throws SqlException {
        try (RecordCursorFactory rcf = compiler.compile(sql
                , sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = rcf.getCursor(sqlExecutionContext)) {
                sink.clear();
                printer.print(cursor, rcf.getMetadata(), true);
                return sink;
            }
        }
    }
}
