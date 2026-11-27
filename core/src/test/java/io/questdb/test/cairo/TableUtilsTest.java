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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.pool.AbstractMultiTenantPool;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.DirectIntList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
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
import static io.questdb.tasks.TableWriterTask.CMD_COMPOSITE_PARTITION_SWAP;
import static io.questdb.tasks.TableWriterTask.CMD_PARQUET_PARTITION_SWAP;
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
    public void testActivePartitionLastCommitSecondsRoundTrip() {
        // Absent and non-positive stamps persist as 0, "no stamp", which reads back as LONG_NULL.
        Assert.assertEquals(0, TableUtils.activePartitionLastCommitMicrosToSeconds(Numbers.LONG_NULL));
        Assert.assertEquals(0, TableUtils.activePartitionLastCommitMicrosToSeconds(0));
        Assert.assertEquals(0, TableUtils.activePartitionLastCommitMicrosToSeconds(-1));
        Assert.assertEquals(Numbers.LONG_NULL, TableUtils.activePartitionLastCommitSecondsToMicros(0));

        // A partial second rounds up, so the IDLE window the stamp opens never ends early.
        Assert.assertEquals(1, TableUtils.activePartitionLastCommitMicrosToSeconds(1));
        Assert.assertEquals(1, TableUtils.activePartitionLastCommitMicrosToSeconds(Micros.SECOND_MICROS));
        Assert.assertEquals(2, TableUtils.activePartitionLastCommitMicrosToSeconds(Micros.SECOND_MICROS + 1));
        Assert.assertEquals(Micros.SECOND_MICROS, TableUtils.activePartitionLastCommitSecondsToMicros(1));

        // A wall clock reading round trips at whole-second resolution.
        final long now = 1_767_225_600_123_456L; // 2026-01-01T00:00:00.123456Z
        Assert.assertEquals(
                1_767_225_601 * Micros.SECOND_MICROS,
                TableUtils.activePartitionLastCommitSecondsToMicros(TableUtils.activePartitionLastCommitMicrosToSeconds(now))
        );

        // The value is unsigned: the top of the 32-bit range, 2106-02-07T06:28:15Z, still round trips, and a
        // stamp past it does not fit and persists as "no stamp".
        final long maxMicros = 0xFFFF_FFFFL * Micros.SECOND_MICROS;
        Assert.assertEquals(-1, TableUtils.activePartitionLastCommitMicrosToSeconds(maxMicros));
        Assert.assertEquals(maxMicros, TableUtils.activePartitionLastCommitSecondsToMicros(-1));
        Assert.assertEquals(0, TableUtils.activePartitionLastCommitMicrosToSeconds(maxMicros + 1));
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
                    0,
                    CommitMode.NOSYNC
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
                    0,
                    CommitMode.NOSYNC
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
                    0,
                    CommitMode.NOSYNC
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
    public void testIsSupersededIndexFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Writer indexes differ from the positions, as in a reader's metadata after DROP COLUMN: the _cv records at
            // writer indexes 1 and 3 stand where a lookup by position would find the versions of sym and c
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP, IndexType.NONE, 0, false, null, 0, false))
                    .add(new TableColumnMetadata("sym", ColumnType.SYMBOL, IndexType.BITMAP, 256, true, null, 2, false))
                    .add(new TableColumnMetadata("p", ColumnType.SYMBOL, IndexType.POSTING, 256, true, null, 4, false))
                    .add(new TableColumnMetadata("c", ColumnType.SYMBOL, IndexType.POSTING, 256, true, null, 6, false))
                    .add(new TableColumnMetadata("u", ColumnType.SYMBOL, IndexType.NONE, 256, true, null, 7, false))
                    .add(new TableColumnMetadata("v", ColumnType.LONG, IndexType.NONE, 0, false, null, 8, false))
                    .add(new TableColumnMetadata("s2", ColumnType.SYMBOL, IndexType.BITMAP, 256, true, null, 9, false));
            final long day1 = MicrosTimestampDriver.INSTANCE.fromDays(19_723);
            final long day2 = MicrosTimestampDriver.INSTANCE.fromDays(19_724);
            try (
                    Path cvPath = new Path().of(temp.newFolder().getAbsolutePath()).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                    ColumnVersionWriter writer = new ColumnVersionWriter(new DefaultTestCairoConfiguration(root), cvPath.$(), true);
                    ColumnVersionReader columnVersionReader = new ColumnVersionReader()
            ) {
                writer.upsert(day1, 2, 5, 0);
                writer.upsert(day1, 4, 6, 0);
                writer.upsert(day1, 6, 7, 0);
                writer.upsert(day1, 1, 9, 0);
                writer.upsert(day1, 3, 9, 0);
                // s2 has no partition record, its default record names txn 7
                writer.upsertDefaultTxnName(9, 7, day1);
                writer.commit();
                columnVersionReader.ofRO(FF, cvPath.$());
                Assert.assertTrue(columnVersionReader.readSafe());
                // 2024-01-01: sym is live at txn 5, p at 6, c at 7, s2 at its default 7. The txn segment follows the
                // kind and is left out for txn -1; a .pv file ends with a seal txn, and a .pc<N> file has the host
                // token instead, 0 for txn -1 and txn 0 alike, and may name the covered column's txn before the seal.
                final String[][] day1Cases = {
                        {"sym.k", "true"},
                        {"sym.v", "true"},
                        {"sym.pk", "true"},
                        {"sym.pci", "true"},
                        {"sym.k.5", "false"},
                        {"sym.v.5", "false"},
                        {"sym.k.4", "true"},
                        {"sym.v.9223372036854775807", "true"},
                        {"SYM.k", "true"},
                        {"SYM.k.5", "false"},
                        {"sym.k.", "false"},
                        {"sym.k.x", "false"},
                        {"sym.k.5.1", "false"},
                        {"sym.d.4", "false"},
                        {"sym.kk.4", "false"},
                        {"sym.", "false"},
                        {"sym", "false"},
                        {"p.pk", "true"},
                        {"p.pk.5", "true"},
                        {"p.pk.6", "false"},
                        {"p.pv.0", "true"},
                        {"p.pv.6.0", "false"},
                        {"p.pv.6.12", "false"},
                        {"p.pv.5.1", "true"},
                        {"p.pv.", "false"},
                        {"p.pv.x.1", "false"},
                        {"c.pci.7", "false"},
                        {"c.pci.6", "true"},
                        {"c.pc0.0.0", "true"},
                        {"c.pc0.0.3.1", "true"},
                        {"c.pc0.7.1", "false"},
                        {"c.pc12.7.5.2", "false"},
                        {"c.pc0.6.1", "true"},
                        {"c.pc.1", "false"},
                        {"c.pc0.1", "false"},
                        {"c.pcx.7.1", "false"},
                        {"s2.k", "true"},
                        {"s2.k.7", "false"},
                        {"s2.v.7", "false"},
                        {"u.k.4", "false"},
                        {"v.k.4", "false"},
                        {"zz.k.4", "false"},
                        {".k", "false"},
                        {"k", "false"},
                        {"_pm", "false"},
                        {"data.parquet", "false"},
                        {"", "false"}
                };
                assertSupersededIndexFiles(day1Cases, metadata, columnVersionReader, day1);
                // 2024-01-02 has no record: sym, p and c are live at txn -1, whose .pc host token is 0, s2 at its
                // default 7. A .pv name, unlike the .pc host token, spells txn 0 apart from txn -1: p.pv.0.3 is txn 0,
                // seal 3.
                final String[][] day2Cases = {
                        {"sym.k", "false"},
                        {"sym.v", "false"},
                        {"sym.k.5", "true"},
                        {"p.pk", "false"},
                        {"p.pv.3", "false"},
                        {"p.pv.0.3", "true"},
                        {"p.pk.6", "true"},
                        {"p.pv.6.0", "true"},
                        {"c.pci", "false"},
                        {"c.pc0.0.3", "false"},
                        {"c.pc0.5.1", "true"},
                        {"s2.k", "true"},
                        {"s2.k.7", "false"}
                };
                assertSupersededIndexFiles(day2Cases, metadata, columnVersionReader, day2);
            }
        });
    }

    @Test
    public void testIsSupersededIndexFileKeepsParquetFilesOfColumnNamedData() throws Exception {
        // The name of an indexed column data starts the names of the parquet partition's own files, data.parquet and
        // data.parquet.staging, so the column resolves. Their kind, parquet, is no index kind: neither is superseded,
        // while the column's index files are classified against its live txn 5.
        TestUtils.assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP, IndexType.NONE, 0, false, null, 0, false))
                    .add(new TableColumnMetadata("data", ColumnType.SYMBOL, IndexType.BITMAP, 256, true, null, 1, false));
            final long day = MicrosTimestampDriver.INSTANCE.fromDays(19_723);
            try (
                    Path cvPath = new Path().of(temp.newFolder().getAbsolutePath()).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                    ColumnVersionWriter writer = new ColumnVersionWriter(new DefaultTestCairoConfiguration(root), cvPath.$(), true);
                    ColumnVersionReader columnVersionReader = new ColumnVersionReader()
            ) {
                writer.upsert(day, 1, 5, 0);
                writer.commit();
                columnVersionReader.ofRO(FF, cvPath.$());
                Assert.assertTrue(columnVersionReader.readSafe());
                final String[][] cases = {
                        {"data.parquet", "false"},
                        {"data.parquet.staging", "false"},
                        {"data.k", "true"},
                        {"data.v", "true"},
                        {"data.k.4", "true"},
                        {"data.k.5", "false"},
                        {"data.v.5", "false"}
                };
                assertSupersededIndexFiles(cases, metadata, columnVersionReader, day);
            }
        });
    }

    @Test
    public void testIsUnsolicitedTableLock() {
        // Internal background job reasons are NOT unsolicited
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(TableUtils.WAL_2_TABLE_WRITE_REASON));
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(TableUtils.WAL_2_TABLE_RESUME_REASON));
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(getCommandName(CMD_STORAGE_POLICY)));
        // The partition compaction sweep holds the writer to land its swap, exactly as
        // STORAGE POLICY does; apply must not log its own scheduled work as an intruder.
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(getCommandName(CMD_COMPOSITE_PARTITION_SWAP)));
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(getCommandName(CMD_PARQUET_PARTITION_SWAP)));
        // A publisher that finds the writer back in the pool ticks the queue under the pool's own reason.
        Assert.assertFalse(TableUtils.isUnsolicitedTableLock(WriterPool.OWNERSHIP_REASON_ASYNC_COMMAND));

        // Any other reason IS unsolicited
        Assert.assertTrue(TableUtils.isUnsolicitedTableLock("ALTER TABLE"));
        Assert.assertTrue(TableUtils.isUnsolicitedTableLock("test"));

        // The pool reports OWNERSHIP_REASON_UNKNOWN while a holder is still constructing the
        // writer and has not stamped its reason yet. That string interns to the same reference
        // as AbstractMultiTenantPool.NO_LOCK_REASON ("unknown"), which a reference-equality
        // escape used to misread as "no reason, treat as solicited" - silently dropping the
        // in-flight WAL apply notification with no recovery. An unidentified holder must be
        // unsolicited so ApplyWal2TableJob re-arms notification delivery.
        Assert.assertTrue(TableUtils.isUnsolicitedTableLock(WriterPool.OWNERSHIP_REASON_UNKNOWN));
        Assert.assertTrue(TableUtils.isUnsolicitedTableLock(AbstractMultiTenantPool.NO_LOCK_REASON));
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
        // 0x10-0x1f stay legal, including 0x1f and 0x1e, which RowExpiryUtil uses as the EXPIRE ROWS
        // policy separator and escape character. The policy encoding escapes them in its fields, so a
        // name an earlier release accepted keeps working.
        testIsValidColumnName((char) 0x10, true);
        testIsValidColumnName((char) 0x1b, true);
        testIsValidColumnName((char) 0x1e, true);
        testIsValidColumnName((char) 0x1f, true);
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
        // 0x10-0x1f stay legal in a table name too (see testIsValidColumnName).
        Assert.assertTrue(TableUtils.isValidTableName("t" + (char) 0x1f + "t", 127));
        Assert.assertTrue(TableUtils.isValidTableName("t" + (char) 0x1e + "t", 127));
        Assert.assertTrue(TableUtils.isValidTableName("t" + (char) 0x10 + "t", 127));
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
    public void testSymbolDataHasNullsAcrossScanChunks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long rowCount = TableUtils.MAX_SYMBOL_NULL_SCAN_ROWS + 2L;
            final long size = rowCount * Integer.BYTES;
            final long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Vect.memset(address, size, 0);
                Assert.assertFalse(TableUtils.symbolDataHasNulls(address, rowCount));
                final long[] nullRows = {0, TableUtils.MAX_SYMBOL_NULL_SCAN_ROWS - 1, TableUtils.MAX_SYMBOL_NULL_SCAN_ROWS, rowCount - 1};
                for (long row : nullRows) {
                    Unsafe.putInt(address + row * Integer.BYTES, SymbolTable.VALUE_IS_NULL);
                    Assert.assertTrue("null at row " + row, TableUtils.symbolDataHasNulls(address, rowCount));
                    Unsafe.putInt(address + row * Integer.BYTES, 0);
                }
                Assert.assertFalse(TableUtils.symbolDataHasNulls(address, rowCount));
            } finally {
                Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCheckStoragePolicyTtlHoursExactBoundary() {
        // Storage policies measure partition age from the partition floor (its start), not the
        // ceiling that table TTL uses. Partition 2024-01-01 with a 24h policy becomes eligible
        // once maxTimestamp reaches exactly 24h past the partition floor.
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long partitionTimestamp = driver.fromDays(19_723); // 2024-01-01
        long partitionFloor = txReader.getPartitionFloor(partitionTimestamp);
        long maxTimestamp = partitionFloor + driver.fromHours(24);

        Assert.assertTrue(TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, maxTimestamp, 24));
        // one microsecond before the boundary — not yet eligible
        Assert.assertFalse(TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, maxTimestamp - 1, 24));
    }

    @Test
    public void testCheckStoragePolicyTtlMonthsBoundary() {
        // Negative TTL means months; storage policies measure the gap from the partition floor.
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.MONTH);
        long partitionTimestamp = driver.fromDays(19_723); // 2024-01-01
        long partitionFloor = txReader.getPartitionFloor(partitionTimestamp); // 2024-01-01
        // 3 months after the floor: 2024-04-01
        long maxTimestamp = partitionFloor + driver.fromDays(91);
        Assert.assertTrue(TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, maxTimestamp, -3));
        // 2 months after the floor: not enough for a 3-month policy
        long maxTimestamp2Months = partitionFloor + driver.fromDays(60);
        Assert.assertFalse(TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, maxTimestamp2Months, -3));
    }

    @Test
    public void testCheckStoragePolicyTtlMonthsFutureFloor() {
        // A partition dated after the reference has no age, however many months ahead it is. The same distance
        // before the reference is past a 1-month policy.
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long maxTimestamp = driver.fromDays(19_723); // 2024-01-01
        long pastPartition = driver.fromDays(19_723 - 120); // 2023-09-03
        long futurePartition = driver.fromDays(19_723 + 120); // 2024-04-30

        Assert.assertTrue(TableUtils.checkStoragePolicyTtl(txReader, driver, pastPartition, maxTimestamp, -1));
        Assert.assertFalse(TableUtils.checkStoragePolicyTtl(txReader, driver, futurePartition, maxTimestamp, -1));
        Assert.assertFalse(TableUtils.checkStoragePolicyTtl(txReader, driver, futurePartition, maxTimestamp, -12));
        // Hour and zero TTLs agree.
        Assert.assertFalse(TableUtils.checkStoragePolicyTtl(txReader, driver, futurePartition, maxTimestamp, 24));
        Assert.assertFalse(TableUtils.checkStoragePolicyTtl(txReader, driver, futurePartition, maxTimestamp, 0));
    }

    @Test(expected = AssertionError.class)
    public void testCheckStoragePolicyTtlRejectsNull() {
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long partitionTimestamp = driver.fromDays(19_723);
        long maxTimestamp = partitionTimestamp + driver.fromHours(100);

        TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, maxTimestamp, Numbers.INT_NULL);
    }

    @Test
    public void testCheckStoragePolicyTtlShiftsOnePartitionVsTtl() {
        // Storage policy ages from the partition floor while table TTL ages from the ceiling,
        // so a storage policy threshold is effectively one partition wider. With daily
        // partitions and a 1h setting, the partition is eligible for storage policy as soon as
        // the next (active) partition starts, while table TTL still treats it as too young.
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long partitionTimestamp = driver.fromDays(19_723); // 2024-01-01
        // maxTimestamp sits exactly at the start of the next partition (2024-01-02).
        long maxTimestamp = txReader.getNextLogicalPartitionTimestamp(partitionTimestamp);

        Assert.assertTrue(TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, maxTimestamp, 1));
        Assert.assertFalse(TableUtils.checkTtl(txReader, driver, partitionTimestamp, maxTimestamp, 1));
    }

    @Test
    public void testCheckStoragePolicyTtlZeroBoundary() {
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.DAY);
        long partitionTimestamp = driver.fromDays(19_723); // 2024-01-01
        long partitionFloor = txReader.getPartitionFloor(partitionTimestamp);

        Assert.assertTrue(TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, partitionFloor, 0));
        Assert.assertFalse(TableUtils.checkStoragePolicyTtl(txReader, driver, partitionTimestamp, partitionFloor - 1, 0));
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

    @Test
    public void testCheckTtlMonthsFutureCeiling() {
        // Table TTL ages a partition from its ceiling. A ceiling after the reference has no age: neither a
        // partition dated months ahead, nor the partition holding the reference, expires.
        TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        TxReader txReader = new TxReader(FF);
        txReader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.MONTH);
        long maxTimestamp = driver.fromDays(19_723); // 2024-01-01, the floor of its partition
        long futurePartition = driver.fromDays(19_723 + 121); // 2024-05-01, ceiling 2024-06-01

        Assert.assertFalse(TableUtils.checkTtl(txReader, driver, futurePartition, maxTimestamp, -1));
        // Its ceiling, 2024-02-01, is a whole month after the reference.
        Assert.assertFalse(TableUtils.checkTtl(txReader, driver, maxTimestamp, maxTimestamp, -1));
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

    private static void assertSupersededIndexFiles(String[][] cases, GenericRecordMetadata metadata, ColumnVersionReader columnVersionReader, long partitionTimestamp) {
        for (String[] c : cases) {
            Assert.assertEquals(
                    c[0],
                    Boolean.parseBoolean(c[1]),
                    TableUtils.isSupersededIndexFile(c[0], metadata, columnVersionReader, partitionTimestamp)
            );
        }
    }

    private void testIsValidColumnName(char c, boolean expected) {
        Assert.assertEquals(expected, TableUtils.isValidColumnName(Character.toString(c), 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName(c + "abc", 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("abc" + c, 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("ab" + c + "c", 127));
    }
}
