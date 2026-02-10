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

package io.questdb.test.recovery;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedMetaReader;
import io.questdb.recovery.MetaColumnState;
import io.questdb.recovery.MetaState;
import io.questdb.recovery.RecoveryIssueCode;
import io.questdb.recovery.TxnState;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class BoundedMetaReaderTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testReadMetaColumnCountExceedsCap() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_cap");

            MetaState state = readMetaState("meta_cap", new BoundedMetaReader(FF, 2));
            Assert.assertEquals(2, state.getColumns().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
            // Bug #1: column names must be correct even when capped.
            // The on-disk name offset depends on the real column count (6), not the cap (2).
            Assert.assertEquals("i", state.getColumns().getQuick(0).name());
            Assert.assertEquals("l", state.getColumns().getQuick(1).name());
        });
    }

    @Test
    public void testReadMetaColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_names");

            MetaState state = readMetaState("meta_names", new BoundedMetaReader(FF));
            Assert.assertTrue(state.getColumns().size() >= 6);
            Assert.assertEquals("i", state.getColumns().getQuick(0).name());
            Assert.assertEquals("l", state.getColumns().getQuick(1).name());
            Assert.assertEquals("d", state.getColumns().getQuick(2).name());
            Assert.assertEquals("s", state.getColumns().getQuick(3).name());
            Assert.assertEquals("sym", state.getColumns().getQuick(4).name());
            Assert.assertEquals("ts", state.getColumns().getQuick(5).name());
        });
    }

    @Test
    public void testReadMetaEmptyFile() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_empty");

            try (Path metaPath = new Path()) {
                Path path = metaPathOf("meta_empty", metaPath);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 0));
                } finally {
                    FF.close(fd);
                }

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            }
        });
    }

    @Test
    public void testReadMetaFileNotFound() throws Exception {
        assertMemoryLeak(() -> {
            try (Path metaPath = new Path()) {
                metaPath.of(configuration.getDbRoot()).concat("nonexistent").concat(TableUtils.META_FILE_NAME);
                MetaState state = new BoundedMetaReader(FF).read(metaPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE));
            }
        });
    }

    @Test
    public void testReadMetaFileTooShortForColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_short_names");

            try (Path metaPath = new Path()) {
                Path path = metaPathOf("meta_short_names", metaPath);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    // truncate right after column types (before column names)
                    // Column types end at: 128 + columnCount * 32
                    // We have at least 6 columns, so types end at 128 + 6*32 = 320
                    // Truncate at 320 to cut off the names
                    Assert.assertTrue(FF.truncate(fd, 320));
                } finally {
                    FF.close(fd);
                }

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
            }
        });
    }

    @Test
    public void testReadMetaFileTooShortForColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_short_types");

            try (Path metaPath = new Path()) {
                Path path = metaPathOf("meta_short_types", metaPath);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    // truncate after header (128 bytes) but mid column types
                    Assert.assertTrue(FF.truncate(fd, 140));
                } finally {
                    FF.close(fd);
                }

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                // should read header but fail on column types
                Assert.assertNotEquals(TxnState.UNSET_INT, state.getColumnCount());
                Assert.assertTrue(
                        hasIssue(state, RecoveryIssueCode.OUT_OF_RANGE)
                                || hasIssue(state, RecoveryIssueCode.PARTIAL_READ)
                );
            }
        });
    }

    @Test
    public void testReadMetaFileTooShortForHeader() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_short_hdr");

            try (Path metaPath = new Path()) {
                Path path = metaPathOf("meta_short_hdr", metaPath);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 2));
                } finally {
                    FF.close(fd);
                }

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            }
        });
    }

    @Test
    public void testReadMetaCannotOpen() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_noopen");

            try (Path metaPath = new Path()) {
                Path path = metaPathOf("meta_noopen", metaPath);
                final String metaPathStr = path.$().toString();

                FilesFacade failOpenFf = new TestFilesFacadeImpl() {
                    @Override
                    public long openRO(LPSZ name) {
                        if (name.toString().equals(metaPathStr)) {
                            return -1;
                        }
                        return super.openRO(name);
                    }
                };

                MetaState state = new BoundedMetaReader(failOpenFf).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertTrue(hasIssueContaining(state, "cannot open"));
            }
        });
    }

    @Test
    public void testReadMetaIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_indexed");

            MetaState state = readMetaState("meta_indexed", new BoundedMetaReader(FF));
            boolean foundIndexed = false;
            boolean foundNonIndexed = false;
            for (int i = 0, n = state.getColumns().size(); i < n; i++) {
                MetaColumnState col = state.getColumns().getQuick(i);
                if ("sym".equals(col.name())) {
                    Assert.assertTrue("sym should be indexed", col.indexed());
                    foundIndexed = true;
                } else {
                    Assert.assertFalse(col.name() + " should not be indexed", col.indexed());
                    foundNonIndexed = true;
                }
            }
            Assert.assertTrue(foundIndexed);
            Assert.assertTrue(foundNonIndexed);
        });
    }

    @Test
    public void testReadMetaMultipleColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_types");

            MetaState state = readMetaState("meta_types", new BoundedMetaReader(FF));
            Assert.assertTrue(state.getColumns().size() >= 6);

            Assert.assertEquals(ColumnType.INT, state.getColumns().getQuick(0).type());
            Assert.assertEquals("INT", state.getColumns().getQuick(0).typeName());

            Assert.assertEquals(ColumnType.LONG, state.getColumns().getQuick(1).type());
            Assert.assertEquals("LONG", state.getColumns().getQuick(1).typeName());

            Assert.assertEquals(ColumnType.DOUBLE, state.getColumns().getQuick(2).type());
            Assert.assertEquals("DOUBLE", state.getColumns().getQuick(2).typeName());

            Assert.assertEquals(ColumnType.STRING, state.getColumns().getQuick(3).type());
            Assert.assertEquals("STRING", state.getColumns().getQuick(3).typeName());

            Assert.assertEquals(ColumnType.SYMBOL, state.getColumns().getQuick(4).type());
            Assert.assertEquals("SYMBOL", state.getColumns().getQuick(4).typeName());

            Assert.assertEquals(ColumnType.TIMESTAMP, state.getColumns().getQuick(5).type());
            Assert.assertEquals("TIMESTAMP", state.getColumns().getQuick(5).typeName());
        });
    }

    @Test
    public void testReadMetaNegativeColumnCount() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_neg_cc");

            try (Path metaPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = metaPathOf("meta_neg_cc", metaPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(TableUtils.META_OFFSET_COUNT, -1);

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
                Assert.assertTrue(hasIssueContaining(state, "column count is negative"));
            }
        });
    }

    @Test
    public void testReadMetaNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTable("meta_nonwal", 3);

            MetaState state = readMetaState("meta_nonwal", new BoundedMetaReader(FF));
            Assert.assertEquals(0, state.getIssues().size());
            Assert.assertTrue(state.getColumnCount() > 0);
            Assert.assertEquals(PartitionBy.DAY, state.getPartitionBy());
        });
    }

    @Test
    public void testReadMetaPartitionByDay() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_day");

            MetaState state = readMetaState("meta_day", new BoundedMetaReader(FF));
            Assert.assertEquals(PartitionBy.DAY, state.getPartitionBy());
        });
    }

    @Test
    public void testReadMetaPartitionByMonth() throws Exception {
        assertMemoryLeak(() -> {
            createTablePartitionByMonth("meta_month", 2);

            MetaState state = readMetaState("meta_month", new BoundedMetaReader(FF));
            Assert.assertEquals(PartitionBy.MONTH, state.getPartitionBy());
        });
    }

    @Test
    public void testReadMetaPartitionByNone() throws Exception {
        assertMemoryLeak(() -> {
            createUnpartitionedTable("meta_none");

            MetaState state = readMetaState("meta_none", new BoundedMetaReader(FF));
            Assert.assertEquals(PartitionBy.NONE, state.getPartitionBy());
        });
    }

    @Test
    public void testReadMetaReadFailureOnColumnCount() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_fail_cc");

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                int callCount = 0;

                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableUtils.META_OFFSET_COUNT && len == Integer.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            MetaState state = readMetaStateWithFf("meta_fail_cc", failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "columnCount"));
        });
    }

    @Test
    public void testReadMetaReadFailureOnColumnName() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_fail_name");

            // Column names start at offset 128 + columnCount * 32
            // We need to fail on name data read. We know names start after column types.
            // The first name length read is at getColumnNameOffset(columnCount).
            // We'll fail on the 4th ff.read call which should be in the column name area.
            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                int callCount = 0;

                @Override
                public long read(long fd, long buf, long len, long offset) {
                    // The first 3 reads are: columnCount, partitionBy, timestampIndex
                    // Then column types + flags (2 reads per column, 6 columns = 12 reads)
                    // Then name lengths and data. Fail on the first name length read (call 16).
                    if (++callCount == 16) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            MetaState state = readMetaStateWithFf("meta_fail_name", failReadFf);
            Assert.assertTrue(
                    hasIssue(state, RecoveryIssueCode.IO_ERROR)
                            || hasIssue(state, RecoveryIssueCode.SHORT_FILE)
            );
        });
    }

    @Test
    public void testReadMetaReadFailureOnColumnType() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_fail_type");

            // Fail on first column type read (4th call: after columnCount, partitionBy, timestampIndex)
            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                int callCount = 0;

                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (++callCount == 4) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            MetaState state = readMetaStateWithFf("meta_fail_type", failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
        });
    }

    @Test
    public void testReadMetaReadFailureOnPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_fail_pb");

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableUtils.META_OFFSET_PARTITION_BY && len == Integer.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            MetaState state = readMetaStateWithFf("meta_fail_pb", failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "partitionBy"));
        });
    }

    @Test
    public void testReadMetaTimestampIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_ts_idx");

            MetaState state = readMetaState("meta_ts_idx", new BoundedMetaReader(FF));
            // ts is the 6th column (index 5)
            Assert.assertEquals(5, state.getTimestampIndex());
        });
    }

    @Test
    public void testReadMetaZeroColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_zero_cc");

            try (Path metaPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = metaPathOf("meta_zero_cc", metaPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(TableUtils.META_OFFSET_COUNT, 0);

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                Assert.assertEquals(0, state.getColumnCount());
                Assert.assertEquals(0, state.getColumns().size());
                Assert.assertFalse(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
            }
        });
    }

    @Test
    public void testReadValidMeta() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_valid");

            MetaState state = readMetaState("meta_valid", new BoundedMetaReader(FF));
            Assert.assertEquals(0, state.getIssues().size());
            Assert.assertEquals(6, state.getColumnCount());
            Assert.assertEquals(PartitionBy.DAY, state.getPartitionBy());
            Assert.assertEquals(5, state.getTimestampIndex());
            Assert.assertEquals(6, state.getColumns().size());
        });
    }

    @Test
    public void testReadMetaDroppedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table meta_dropped (val long, extra int, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("meta_dropped")) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 42);
                row.putInt(1, 7);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows("meta_dropped", 1);
            execute("alter table meta_dropped drop column extra");
            drainWalQueue(engine);

            MetaState state = readMetaState("meta_dropped", new BoundedMetaReader(FF));
            Assert.assertEquals(0, countIssuesWithCode(state, RecoveryIssueCode.INVALID_COUNT));
            // dropped column should have a negative type
            boolean foundDropped = false;
            for (int i = 0, n = state.getColumns().size(); i < n; i++) {
                MetaColumnState col = state.getColumns().getQuick(i);
                if (col.type() < 0) {
                    foundDropped = true;
                    break;
                }
            }
            Assert.assertTrue("should find a dropped column with negative type", foundDropped);
        });
    }

    @Test
    public void testReadMetaInvalidPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_bad_pb");

            try (Path metaPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = metaPathOf("meta_bad_pb", metaPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(TableUtils.META_OFFSET_PARTITION_BY, 99);

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                // invalid partitionBy is stored as-is (no validation in reader)
                Assert.assertEquals(99, state.getPartitionBy());
                // should still read columns successfully
                Assert.assertTrue(state.getColumns().size() > 0);
            }
        });
    }

    @Test
    public void testReadMetaTimestampIndexExceedsColumnCount() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("meta_bad_ts_idx");

            try (Path metaPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = metaPathOf("meta_bad_ts_idx", metaPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                // set timestampIndex to 999 (way beyond columnCount=6)
                mem.putInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX, 999);

                MetaState state = new BoundedMetaReader(FF).read(path.$());
                Assert.assertEquals(999, state.getTimestampIndex());
                // columns should still be readable
                Assert.assertTrue(state.getColumns().size() > 0);
            }
        });
    }

    private static int countIssuesWithCode(MetaState state, RecoveryIssueCode issueCode) {
        int count = 0;
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).code() == issueCode) {
                count++;
            }
        }
        return count;
    }

    private static void createNonWalTable(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY");
        execute(
                "insert into " + tableName
                        + " select x, timestamp_sequence('1970-01-01', 86400000000L) from long_sequence("
                        + rowCount + ")"
        );
    }

    private static void createTablePartitionByMonth(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by MONTH WAL");
        execute(
                "insert into " + tableName
                        + " select x, timestamp_sequence('1970-01-01', 2592000000000L) from long_sequence("
                        + rowCount + ")"
        );
        waitForAppliedRows(tableName, rowCount);
    }

    private static void createTableWithTypes(String tableName) throws SqlException {
        execute("create table " + tableName
                + " (i int, l long, d double, s string, sym symbol index, ts timestamp)"
                + " timestamp(ts) partition by DAY WAL");
        try (WalWriter walWriter = getWalWriter(tableName)) {
            TableWriter.Row row = walWriter.newRow(0);
            row.putInt(0, 42);
            row.putLong(1, 100L);
            row.putDouble(2, 3.14);
            row.putStr(3, "hello");
            row.putSym(4, "AA");
            row.append();
            walWriter.commit();
        }
        waitForAppliedRows(tableName, 1);
    }

    private static void createUnpartitionedTable(String tableName) throws SqlException {
        execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by NONE");
        execute("insert into " + tableName + " select x, timestamp_sequence('1970-01-01', 1000000L) from long_sequence(2)");
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (RecordCursorFactory factory = select("select count() from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static boolean hasIssue(MetaState state, RecoveryIssueCode issueCode) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).code() == issueCode) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasIssueContaining(MetaState state, String substring) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).message().contains(substring)) {
                return true;
            }
        }
        return false;
    }

    private static Path metaPathOf(String tableName, Path path) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME);
    }

    private static MetaState readMetaState(String tableName, BoundedMetaReader reader) {
        try (Path metaPath = new Path()) {
            return reader.read(metaPathOf(tableName, metaPath).$());
        }
    }

    private static MetaState readMetaStateWithFf(String tableName, FilesFacade customFf) {
        try (Path metaPath = new Path()) {
            return new BoundedMetaReader(customFf).read(metaPathOf(tableName, metaPath).$());
        }
    }

    private static void waitForAppliedRows(String tableName, int expectedRows) throws SqlException {
        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
            if (getRowCount(tableName) == expectedRows) {
                return;
            }
        }
        Assert.assertEquals(expectedRows, getRowCount(tableName));
    }
}
