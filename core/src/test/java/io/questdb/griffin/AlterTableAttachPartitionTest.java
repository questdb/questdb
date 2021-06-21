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
import io.questdb.cairo.sql.DataFrame;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.concurrent.atomic.AtomicInteger;

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

                copyAttachPartition(src, dst, 0, "2020-01-09", "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachActive2PartitionsOneByOneInDescOrder() throws Exception {
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

                copyAttachPartition(src, dst, 0, "2020-01-10");
                copyAttachPartition(src, dst, 1001, "2020-01-09");
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
                copyAttachPartition(src, dst, 0, "2020-01-09", "2020-01-10", "2020-01-01");
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

                copyAttachPartition(src, dst, 0, "2020-01-10");
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
                    Assert.assertEquals("[38] attach partition failed, folder '2020-01-01' does not exist", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachNonExisting() throws Exception {
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
                    Assert.assertEquals("[38] attach partition failed, folder '2020-01-01' does not exist", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionInWrongDirectoryName() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyPartitionToBackup(src.getName(), "2020-01-01", dst.getName(), "2020-01-02");
                try {
                    String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-02'";
                    compiler.compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (io.questdb.griffin.SqlException e) {
                    TestUtils.assertEquals("[38] failed to attach partition '2020-01-02', data does not correspond to the partition folder or partition is empty", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionMissingColumnType() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                assertSchemaMismatch(src, dst -> dst.col("str", ColumnType.STRING));
                assertSchemaMismatch(src, dst -> dst.col("sym", ColumnType.SYMBOL));
                assertSchemaMismatch(src, dst -> dst.col("l1", ColumnType.LONG));
                assertSchemaMismatch(src, dst -> dst.col("i1", ColumnType.INT));
                assertSchemaMismatch(src, dst -> dst.col("b", ColumnType.BOOLEAN));
                assertSchemaMismatch(src, dst -> dst.col("db", ColumnType.DOUBLE));
                assertSchemaMismatch(src, dst -> dst.col("fl", ColumnType.FLOAT));
                assertSchemaMismatch(src, dst -> dst.col("dt", ColumnType.DATE));
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.TIMESTAMP));
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.LONG256));
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.BINARY));
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.BYTE));
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.CHAR));
                assertSchemaMismatch(src, dst -> dst.col("ts", ColumnType.SHORT));
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

                copyAttachPartition(src, dst, 0, "2020-01-01");
            }
        });
    }

    @Test
    public void testAttachPartitionWithColumnTypes() throws Exception {
        assertSchemaMatch(dst -> dst.col("str", ColumnType.STRING));
        assertSchemaMatch(dst -> dst.col("l1", ColumnType.LONG));
        assertSchemaMatch(dst -> dst.col("i1", ColumnType.INT));
        assertSchemaMatch(dst -> dst.col("b", ColumnType.BOOLEAN));
        assertSchemaMatch(dst -> dst.col("db", ColumnType.DOUBLE));
        assertSchemaMatch(dst -> dst.col("fl", ColumnType.FLOAT));
        assertSchemaMatch(dst -> dst.col("dt", ColumnType.DATE));
        assertSchemaMatch(dst -> dst.col("ts1", ColumnType.TIMESTAMP));
        assertSchemaMatch(dst -> dst.col("l256", ColumnType.LONG256));
        assertSchemaMatch(dst -> dst.col("byt", ColumnType.BYTE));
        assertSchemaMatch(dst -> dst.col("ch", ColumnType.CHAR));
        assertSchemaMatch(dst -> dst.col("sh", ColumnType.SHORT));
    }

    @Test
    public void testAttachPartitionsNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.NONE)) {

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

                try {
                    copyAttachPartition(src, dst, 0, "2020-01-09");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertEquals("[38] table is not partitioned[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsTableInTransaction() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                int partitionRowCount = 111;
                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        partitionRowCount,
                        "2020-01-09",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                long timestamp = TimestampFormatUtils.parseTimestamp("2020-01-09T00:00:00.000z");
                copyPartitionToBackup(src.getName(), "2020-01-09", dst.getName());

                // Add 1 row without commit
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "dst", "testing")) {
                    long insertTs = TimestampFormatUtils.parseTimestamp("2020-01-10T23:59:59.999z");
                    TableWriter.Row row = writer.newRow(insertTs);
                    row.putLong(0, 1L);
                    row.putInt(1, 1);
                    row.append();

                    Assert.assertTrue(writer.inTransaction());

                    // This commits the append before attaching
                    writer.attachPartition(timestamp);
                    Assert.assertEquals(partitionRowCount + 1, writer.size());

                    Assert.assertFalse(writer.inTransaction());
                }

            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbols() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .col("s", ColumnType.SYMBOL));

                try {
                    copyAttachPartition(src, dst, 0, "2020-01-09");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertEquals("[38] attaching partitions to tables with symbol columns not supported", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachSamePartitionTwice() throws Exception {
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

                copyAttachPartition(src, dst, 0, "2020-01-09");

                String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST '2020-01-09'";

                try {
                    compiler.compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[38] failed to attach partition '2020-01-09', partition already attached to the table", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testCannotMapTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            private long tsdFd;

            @Override
            public long mmap(long fd, long len, long offset, int flags) {
                if (tsdFd != fd) {
                    return super.mmap(fd, len, offset, flags);
                }
                tsdFd = 0;
                return -1;
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    this.tsdFd = fd;
                }
                return fd;
            }
        };

        testSqlFailedOnFsOperation(ff, "Cannot map");
    }

    @Test
    public void testCannotReadTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "table 'dst' could not be altered: [", "]: could not open");
    }

    @Test
    public void testCannotReadTimestampColumnFileDoesNotExist() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ name) {
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return false;
                }
                return super.exists(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "table 'dst' could not be altered: [0]: Doesn't exist:");
    }

    @Test
    public void testCannotRenameDetachedFolderOnAttach() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                if (Chars.endsWith(to, "2020-01-01") && counter.decrementAndGet() == 0) {
                    return false;
                }
                return super.rename(from, to);
            }
        };

        testSqlFailedOnFsOperation(ff, "table 'dst' could not be altered: [", "]: File system error on trying to rename [from=");
    }

    private void assertSchemaMatch(AddColumn tm) throws Exception {
        setUp();
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {
                src.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .timestamp("ts");
                tm.add(src);

                createPopulateTable(
                        src,
                        10000,
                        "2020-01-01",
                        10);

                dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG);
                tm.add(dst);

                CairoTestUtils.create(dst);
                copyAttachPartition(src, dst, 0, "2020-01-01");
            }
        });
        tearDown();
    }

    private void assertSchemaMismatch(TableModel src, AddColumn tm) throws IOException, NumericException {
        try (TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY);
             Path path = new Path()) {
            dst.timestamp("ts")
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG);

            tm.add(dst);
            CairoTestUtils.create(dst);

            try {
                copyAttachPartition(src, dst, 0, "2020-01-10");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Column file does not exist");
            }
            Files.rmdir(path.concat(root).concat("dst").concat("2020-01-10").put(TableUtils.DETACHED_DIR_MARKER).$());
        }
    }

    private void copyAttachPartition(TableModel src, TableModel dst, int countAjdustment, String... partitionList) throws SqlException, NumericException, IOException {
        copyAttachPartition(src, dst, countAjdustment, false, partitionList);
    }

    private void copyAttachPartition(TableModel src, TableModel dst, int countAjdustment, boolean skipCopy, String... partitionList) throws IOException, SqlException, NumericException {
        StringBuilder partitions = new StringBuilder();
        for (int i = 0; i < partitionList.length; i++) {
            if (i > 0) {
                partitions.append(",");
            }
            partitions.append("'");
            partitions.append(partitionList[i]);
            partitions.append("'");
        }

        try (TableReader tableReader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "dst")) {
            int rowCount = readAllRows(tableReader);

            String alterCommand = "ALTER TABLE dst ATTACH PARTITION LIST " + partitions + ";";

            StringBuilder partitionsIn = new StringBuilder();
            for (int i = 0; i < partitionList.length; i++) {
                if (i > 0) {
                    partitionsIn.append(" OR ");
                }
                partitionsIn.append("ts IN '");
                partitionsIn.append(partitionList[i]);
                partitionsIn.append("'");
            }

            String withClause = ", t1 as (select 1 as id, count() as cnt from src WHERE " + partitionsIn + ")\n";

            if (!skipCopy) {
                for (String s : partitionList) {
                    copyPartitionToBackup(src.getName(), s, dst.getName());
                }
            }

            // Alter table
            compiler.compile(alterCommand, sqlExecutionContext);

            // Assert existing reader reloads new partition
            Assert.assertTrue(tableReader.reload());
            int newRowCount = readAllRows(tableReader);
            Assert.assertTrue(newRowCount > rowCount);

            TestUtils.assertEquals(
                    "cnt\n" +
                            (-countAjdustment) + "\n",
                    executeSql("with t2 as (select 1 as id, count() as cnt from dst)\n" +
                            withClause +
                            "select t1.cnt - t2.cnt as cnt\n" +
                            "from t2 cross join t1"
                    )
            );

            long timestamp = 0;
            for (String s : partitionList) {
                long ts = TimestampFormatUtils.parseTimestamp(s + "T23:59:59.999z");
                if (ts > timestamp) {
                    timestamp = ts;
                }
            }

            // Check table is writable after partition attach
            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "dst", "testing")) {

                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 1L);
                row.putInt(1, 1);
                row.append();
                writer.commit();
            }

            TestUtils.assertEquals(
                    "cnt\n" +
                            (-1 - countAjdustment) + "\n",
                    executeSql("with " +
                            "t2 as (select 1 as id, count() as cnt from dst)\n" +
                            withClause +
                            "select t1.cnt - t2.cnt as cnt\n" +
                            "from t2 cross join t1"
                    )
            );
        }
    }

    private void copyDirectory(Path from, Path to) throws IOException {
        LOG.info().$("copying folder [from=").$(from).$(", to=").$(to).$(']').$();
        if (Files.mkdir(to, DIR_MODE) != 0) {
            Assert.fail("Cannot create " + to + ". Error: " + Os.errno());
        }

        java.nio.file.Path dest = FileSystems.getDefault().getPath(to.toString() + Files.SEPARATOR);
        java.nio.file.Path src = FileSystems.getDefault().getPath(from.toString() + Files.SEPARATOR);
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
        copyPartitionToBackup(src, partitionFolder, dst, partitionFolder);
    }

    private void copyPartitionToBackup(String src, String srcDir, String dst, String dstDir) throws IOException {
        try (Path p1 = new Path().of(configuration.getRoot()).concat(src).concat(srcDir).$();
             Path backup = new Path().of(configuration.getRoot())) {

            copyDirectory(p1, backup.concat(dst).concat(dstDir).put(TableUtils.DETACHED_DIR_MARKER).$());
        }
    }

    private CharSequence executeSql(String sql) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink
        );
        return sink;
    }

    private int readAllRows(TableReader tableReader) {
        try (FullFwdDataFrameCursor cursor = new FullFwdDataFrameCursor()) {
            cursor.of(tableReader);
            DataFrame frame;
            int count = 0;
            while ((frame = cursor.next()) != null) {
                for (long index = frame.getRowHi() - 1, lo = frame.getRowLo() - 1; index > lo; index--) {
                    count++;
                }
            }
            return count;
        }
    }

    private void testSqlFailedOnFsOperation(FilesFacadeImpl ff, String... errorContains) throws Exception {
        assertMemoryLeak(ff, () -> {
            try (
                    TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)
            ) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        100,
                        "2020-01-01",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    copyAttachPartition(src, dst, 0, "2020-01-01");
                    Assert.fail();
                } catch (SqlException e) {
                    for (String error : errorContains) {
                        TestUtils.assertContains(e.getFlyweightMessage(), error);
                    }
                }

                // second attempt without FilesFacade override should work ok
                copyAttachPartition(src, dst, 0, true, "2020-01-01");
            }
        });
    }

    @FunctionalInterface
    private interface AddColumn {
        void add(TableModel tm);
    }
}
