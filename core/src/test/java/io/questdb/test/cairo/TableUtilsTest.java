/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.std.DirectIntList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static io.questdb.cairo.TableUtils.TABLE_RESERVED;
import static io.questdb.tasks.TableWriterTask.CMD_STORAGE_POLICY;
import static io.questdb.tasks.TableWriterTask.getCommandName;

public class TableUtilsTest extends AbstractTest {
    private final static FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    private Path path;

    @Before
    public void setUp() {
        path = new Path();
    }

    @After
    public void tearDown() {
        Misc.free(path);
    }

    @Test
    public void testCreateTableInVolumeFailsCauseTableExistsAsADir() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tableName = testName.getMethodName();
        File dbRoot = temp.newFolder(tableName, "db");
        File volumeRoot = temp.newFolder(tableName, "volume");
        path.of(volumeRoot.getAbsolutePath()).concat(tableName).$();
        Assert.assertTrue(new File(dbRoot, tableName).mkdir());
        try {
            TableUtils.createTableOrMatViewInVolume(
                    FF,
                    dbRoot.getAbsolutePath(),
                    null,
                    509,
                    null,
                    path,
                    tableName,
                    null,
                    0,
                    0
            );
            Assert.fail();
        } catch (CairoException e) {
            path.of(dbRoot.getAbsolutePath()).concat(tableName).$();
            TestUtils.assertContains(e.getFlyweightMessage(), "table directory already exists [path=" + path.toString() + ']');
        } finally {
            TestUtils.removeTestPath(dbRoot.getAbsolutePath());
            TestUtils.removeTestPath(volumeRoot.getAbsolutePath());
        }
    }

    @Test
    public void testCreateTableInVolumeFailsCauseTableExistsAsADirInVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tableName = testName.getMethodName();
        File dbRoot = temp.newFolder(tableName, "db");
        File volumeRoot = temp.newFolder(tableName, "volume");
        path.of(volumeRoot.getAbsolutePath()).concat(tableName).$();
        Assert.assertTrue(new File(volumeRoot, tableName).mkdir());
        try {
            TableUtils.createTableOrMatViewInVolume(
                    FF,
                    dbRoot.getAbsolutePath(),
                    null,
                    509,
                    null,
                    path,
                    tableName,
                    null,
                    0,
                    0
            );
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "table directory already exists in volume [path=" + path.toString() + ']');
        } finally {
            dbRoot.delete();
            volumeRoot.delete();
        }
    }

    @Test
    public void testCreateTableInVolumeFailsCauseTableExistsAsAFile() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tableName = testName.getMethodName();
        File dbRoot = temp.newFolder(tableName, "db");
        File volumeRoot = temp.newFolder(tableName, "volume");
        path.of(volumeRoot.getAbsolutePath()).concat(tableName).$();
        Assert.assertTrue(new File(dbRoot, tableName).createNewFile());
        try {
            TableUtils.createTableOrMatViewInVolume(
                    FF,
                    dbRoot.getAbsolutePath(),
                    null,
                    509,
                    null,
                    path,
                    tableName,
                    null,
                    0,
                    0
            );
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "could not create soft link [src=" + path.toString() + ", tableDir=" + tableName + ']');
            Assert.assertFalse(Files.exists(path.$()));
        } finally {
            dbRoot.delete();
            volumeRoot.delete();
        }
    }

    @Test
    public void testEstimateRecordSize() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT, metadata))
                .add(new TableColumnMetadata("b", ColumnType.STRING, metadata))
                .add(new TableColumnMetadata("c", -ColumnType.DOUBLE, metadata));
        Assert.assertEquals(4 + 28, TableUtils.estimateAvgRecordSize(metadata));
    }

    @Test
    public void testForeignDirectory() {
        try (Path path = new Path()) {
            Assert.assertEquals(TABLE_RESERVED, TableUtils.exists(FF, path, temp.getRoot().getAbsolutePath(), ""));
        }
    }

    @Test
    public void testGetMaxTimestampWallClockDisabled() {
        final TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        final TxReader txReader = new TxReader(FF) {
            {
                maxTimestamp = driver.fromDays(19_723) + driver.fromHours(48);
            }
        };

        // wallClockEnabled=false: always returns the data maxTimestamp regardless of wall clock
        long wallClockMicros = driver.fromDays(19_723);
        long result = TableUtils.getMaxTimestamp(txReader, driver, wallClockMicros, false);
        Assert.assertEquals(txReader.getMaxTimestamp(), result);
    }

    @Test
    public void testGetMaxTimestampWallClockEnabledClampsToWallClock() {
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        // Data maxTimestamp is far in the future (48h after day 19723)
        long futureMax = driver.fromDays(19_723) + driver.fromHours(48);
        TxReader txReader = new TxReader(FF) {
            {
                maxTimestamp = futureMax;
            }
        };

        // Wall clock is earlier than data maxTimestamp
        long wallClockMicros = driver.fromDays(19_723) + driver.fromHours(1);
        long result = TableUtils.getMaxTimestamp(txReader, driver, wallClockMicros, true);
        Assert.assertEquals(driver.fromMicros(wallClockMicros), result);
        Assert.assertTrue(result < txReader.getMaxTimestamp());
    }

    @Test
    public void testGetMaxTimestampWallClockEnabledReturnsDataMaxWhenEarlier() {
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        // Data maxTimestamp is in the past
        long pastMax = driver.fromDays(19_723) + driver.fromHours(1);
        TxReader txReader = new TxReader(FF) {
            {
                maxTimestamp = pastMax;
            }
        };

        // Wall clock is later than data maxTimestamp
        long wallClockMicros = driver.fromDays(19_723) + driver.fromHours(48);
        long result = TableUtils.getMaxTimestamp(txReader, driver, wallClockMicros, true);
        Assert.assertEquals(txReader.getMaxTimestamp(), result);
    }

    @Test
    public void testIsUnsolicitedTableLock() {
        // Internal background job reasons are NOT unsolicited
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(TableUtils.WAL_2_TABLE_WRITE_REASON));
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(TableUtils.WAL_2_TABLE_RESUME_REASON));
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(getCommandName(CMD_STORAGE_POLICY)));

        // Any other reason IS unsolicited
        Assert.assertTrue(TableUtils.isUnsolicitedTableLock("ALTER TABLE"));
        Assert.assertTrue(TableUtils.isUnsolicitedTableLock("test"));
    }

    @Test
    public void testIsValidColumnName() {
        testIsValidColumnName('?', false);
        testIsValidColumnName('.', false);
        testIsValidColumnName(',', false);
        testIsValidColumnName('\'', false);
        testIsValidColumnName('\"', false);
        testIsValidColumnName('\\', false);
        testIsValidColumnName('/', false);
        testIsValidColumnName('\0', false);
        testIsValidColumnName(':', false);
        testIsValidColumnName(')', false);
        testIsValidColumnName('(', false);
        testIsValidColumnName('+', false);
        testIsValidColumnName('-', false);
        testIsValidColumnName('*', false);
        testIsValidColumnName('%', false);
        testIsValidColumnName('~', false);
        testIsValidColumnName('\n', false);
        Assert.assertFalse(TableUtils.isValidColumnName("..", 127));
        Assert.assertFalse(TableUtils.isValidColumnName(".", 127));
        Assert.assertFalse(TableUtils.isValidColumnName("t\u007Ftcsv", 127));

        testIsValidColumnName('!', true);
        testIsValidColumnName('a', true);
        testIsValidColumnName('b', true);
        testIsValidColumnName('^', true);
        testIsValidColumnName('[', true);
        testIsValidColumnName('$', true);
        Assert.assertFalse(TableUtils.isValidColumnName("", 2));
        Assert.assertFalse(TableUtils.isValidColumnName("abc", 2));
    }

    @Test
    public void testIsValidTableName() {
        Assert.assertFalse(TableUtils.isValidTableName("?abcd", 127));
        Assert.assertFalse(TableUtils.isValidTableName("", 127));
        Assert.assertFalse(TableUtils.isValidTableName(" ", 127));
        Assert.assertFalse(TableUtils.isValidTableName("./", 127));
        Assert.assertFalse(TableUtils.isValidTableName("/asdf", 127));
        Assert.assertFalse(TableUtils.isValidTableName("\\asdf", 127));
        Assert.assertFalse(TableUtils.isValidTableName("asdf\rasdf", 127));
        Assert.assertFalse(TableUtils.isValidTableName("t..t.csv", 127));
        Assert.assertFalse(TableUtils.isValidTableName("\"", 127));
        Assert.assertFalse(TableUtils.isValidTableName("t\u007Ft.csv", 127));
        Assert.assertFalse(TableUtils.isValidTableName(".", 127));
        Assert.assertFalse(TableUtils.isValidTableName("..", 127));
        Assert.assertFalse(TableUtils.isValidTableName("...", 127));
        Assert.assertFalse(TableUtils.isValidTableName("..\\", 127));
        Assert.assertFalse(TableUtils.isValidTableName("\\..", 127));
        Assert.assertFalse(TableUtils.isValidTableName("/..", 127));
        Assert.assertFalse(TableUtils.isValidTableName("../", 127));

        Assert.assertTrue(TableUtils.isValidTableName("table name", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table name.csv", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table-name", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table_name", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table$name", 127));
        Assert.assertFalse(TableUtils.isValidTableName("asdf\nasdf", 127));

        Assert.assertFalse(TableUtils.isValidTableName("abc", 2));
        Assert.assertTrue(TableUtils.isValidTableName("الْعَرَبِيَّة", 127));
    }

    @Test
    public void testNullValue() {
        long mem1 = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        long mem2 = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int columnType = 0; columnType < ColumnType.NULL; columnType++) {
                if (!ColumnType.isVarSize(columnType)) {
                    int size = ColumnType.sizeOf(columnType);
                    if (size > 0) {
                        TableUtils.setNull(columnType, mem2, 1);
                        Unsafe.putLong(mem1, TableUtils.getNullLong(columnType, 0));
                        Unsafe.putLong(mem1 + 8, TableUtils.getNullLong(columnType, 1));
                        Unsafe.putLong(mem1 + 16, TableUtils.getNullLong(columnType, 2));
                        Unsafe.putLong(mem1 + 24, TableUtils.getNullLong(columnType, 3));

                        String type = ColumnType.nameOf(columnType);
                        for (int b = 0; b < size; b++) {
                            Assert.assertEquals(
                                    type,
                                    Unsafe.getByte(mem1 + b),
                                    Unsafe.getByte(mem2 + b)
                            );
                        }
                    }
                }
            }
        } finally {
            Unsafe.free(mem1, 32, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(mem2, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCheckTtlHoursExactBoundary() {
        // Partition 2024-01-01, ceiling is 2024-01-02T00:00:00Z.
        // maxTimestamp exactly 24h after ceiling => TTL of 24h is met.
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long partitionTimestamp = driver.fromDays(19_723); // 2024-01-01
        long partitionCeiling = txReader.getNextLogicalPartitionTimestamp(partitionTimestamp);
        long maxTimestamp = partitionCeiling + driver.fromHours(24);

        Assert.assertTrue(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp, 24));
        // one microsecond before the boundary — TTL not yet met
        Assert.assertFalse(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp - 1, 24));
    }

    @Test
    public void testCheckTtlHoursNotExpired() {
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long partitionTimestamp = driver.fromDays(19_723);
        long partitionCeiling = txReader.getNextLogicalPartitionTimestamp(partitionTimestamp);
        // maxTimestamp only 1h after ceiling, TTL is 48h
        long maxTimestamp = partitionCeiling + driver.fromHours(1);

        Assert.assertFalse(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp, 48));
    }

    @Test
    public void testCheckTtlMonthsBoundary() {
        // Negative TTL means months. -3 => 3 months.
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.MONTH);
        // 2024-01-01 partition, ceiling is 2024-02-01
        long partitionTimestamp = driver.fromDays(19_723);
        long partitionCeiling = txReader.getNextLogicalPartitionTimestamp(partitionTimestamp);
        // 3 months after ceiling: 2024-05-01
        long maxTimestamp = partitionCeiling + driver.fromDays(90);

        Assert.assertTrue(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp, -3));
        // 2 months after ceiling: not enough for 3-month TTL
        long maxTimestamp2Months = partitionCeiling + driver.fromDays(59);
        Assert.assertFalse(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp2Months, -3));
    }

    @Test(expected = AssertionError.class)
    public void testCheckTtlRejectsZero() {
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long partitionTimestamp = driver.fromDays(19_723);
        long maxTimestamp = partitionTimestamp + driver.fromHours(100);

        TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp, 0);
    }

    @Test
    public void testCheckTtlSmallHourValue() {
        // TTL of 1 hour with HOUR partitioning
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.HOUR);
        long partitionTimestamp = driver.fromHours(473_352); // some hour
        long partitionCeiling = txReader.getNextLogicalPartitionTimestamp(partitionTimestamp);
        long maxTimestamp = partitionCeiling + driver.fromHours(1);

        Assert.assertTrue(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp, 1));
        Assert.assertFalse(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp - 1, 1));
    }

    @Test
    public void testParseBloomFilterColumnIndexes() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT, metadata));
        metadata.add(new TableColumnMetadata("b", ColumnType.STRING, metadata));
        metadata.add(new TableColumnMetadata("c", ColumnType.DOUBLE, metadata));

        try (DirectIntList indexes = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT)) {
            TableUtils.parseBloomFilterColumnIndexes(metadata, "a,c", indexes);
            Assert.assertEquals(2, indexes.size());
            Assert.assertEquals(0, indexes.get(0));
            Assert.assertEquals(2, indexes.get(1));
        }
    }

    @Test
    public void testParseBloomFilterColumnIndexesDuplicatesIgnored() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT, metadata));
        metadata.add(new TableColumnMetadata("b", ColumnType.STRING, metadata));

        try (DirectIntList indexes = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT)) {
            TableUtils.parseBloomFilterColumnIndexes(metadata, "a, b, a", indexes);
            Assert.assertEquals(2, indexes.size());
            Assert.assertEquals(0, indexes.get(0));
            Assert.assertEquals(1, indexes.get(1));
        }
    }

    @Test
    public void testParseBloomFilterColumnIndexesNonExistentColumn() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT, metadata));

        try (DirectIntList indexes = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT)) {
            try {
                TableUtils.parseBloomFilterColumnIndexes(metadata, "z", indexes);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bloom_filter_columns contains non-existent column: z");
            }
        }
    }

    @Test
    public void testParseBloomFilterColumnIndexesSkipsDeletedColumns() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT, metadata));
        metadata.add(new TableColumnMetadata("deleted", -ColumnType.STRING, metadata));
        metadata.add(new TableColumnMetadata("b", ColumnType.DOUBLE, metadata));

        try (DirectIntList indexes = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT)) {
            // "b" is column index 2, but descriptor index 1 (deleted column skipped)
            TableUtils.parseBloomFilterColumnIndexes(metadata, "b", indexes);
            Assert.assertEquals(1, indexes.size());
            Assert.assertEquals(1, indexes.get(0));
        }
    }

    @Test
    public void testParseBloomFilterColumnIndexesWhitespaceTrimmed() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT, metadata));
        metadata.add(new TableColumnMetadata("b", ColumnType.STRING, metadata));

        try (DirectIntList indexes = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT)) {
            TableUtils.parseBloomFilterColumnIndexes(metadata, "  a , b  ", indexes);
            Assert.assertEquals(2, indexes.size());
            Assert.assertEquals(0, indexes.get(0));
            Assert.assertEquals(1, indexes.get(1));
        }
    }

    private void testIsValidColumnName(char c, boolean expected) {
        Assert.assertEquals(expected, TableUtils.isValidColumnName(Character.toString(c), 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName(c + "abc", 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("abc" + c, 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("ab" + c + "c", 127));
    }
}
