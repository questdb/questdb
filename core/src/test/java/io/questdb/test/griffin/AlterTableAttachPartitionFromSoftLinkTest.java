/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


public class AlterTableAttachPartitionFromSoftLinkTest extends AbstractAlterTableAttachPartitionTest {

    // some tests begin with: Assume.assumeTrue(Os.type != Os.WINDOWS);
    // in WINDOWS the user performing the tests needs to have the 'Create Symbolic Links' privilege.
    // this privilege is not granted by default. in addition, if User Account Control (UAC) is on, and
    // the user has administrator privileges, tests must 'Run as administrator'.
    // besides this, isSoftLink is not supported in WINDOWS

    private static final String activePartitionName = "2022-10-18";
    private static final String expectedMaxTimestamp = "2022-10-18T23:59:59.000000Z";
    private static final String expectedMinTimestamp = "2022-10-17T00:00:17.279900Z";
    private static final String readOnlyPartitionName = "2022-10-17";
    private static final Rnd rnd = TestUtils.generateRandom(null);
    private long activePartitionTimestamp;
    private long readOnlyPartitionTimestamp;
    private int timestampType;
    private String timestampTypeName;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        Assert.assertEquals(TableUtils.ATTACHABLE_DIR_MARKER, configuration.getAttachPartitionSuffix());
        Assert.assertFalse(configuration.attachPartitionCopy());
        timestampType = rnd.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
        timestampTypeName = ColumnType.nameOf(timestampType);
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
        try {
            readOnlyPartitionTimestamp = timestampDriver.parseFloorLiteral(readOnlyPartitionName + "T00:00:00.000Z");
            activePartitionTimestamp = timestampDriver.parseFloorLiteral(activePartitionName + "T00:00:00.000Z");
        } catch (NumericException impossible) {
            throw new RuntimeException(impossible);
        }

    }

    @Test
    public void testAddColumn() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, ignore -> {
                        try {
                            execute("ALTER TABLE " + tableName + " ADD COLUMN ss SYMBOL");

                            // silently ignored because the table is read only
                            execute("INSERT INTO " + tableName + " VALUES(666, 666, 'queso', '" + readOnlyPartitionName + "T23:59:59.999999Z', '¶')");
                            execute("ALTER TABLE " + tableName + " ALTER COLUMN ss ADD INDEX CAPACITY 32");
                            assertSql(
                                    replaceTimestampSuffix1("min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(
                                    replaceTimestampSuffix1("l\ti\ts\tts\tss\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\t\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\t\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\t\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\t\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\t\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testDetachPartitionAttachedFromSoftLink() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "SNOW", tableToken -> {
                        try {
                            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );

                            // verify cold storage folder exists
                            Assert.assertTrue(Files.exists(other.$()));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);

                            // verify the link was removed
                            other.of(configuration.getDbRoot())
                                    .concat(tableToken)
                                    .concat(readOnlyPartitionName)
                                    .put(configuration.getAttachPartitionSuffix())
                                    .$();
                            Assert.assertFalse(ff.exists(other.$()));

                            // insert a row at the end of the partition, the only row, which will create the partition
                            // at this point there is no longer information as to weather it was read-only in the past
                            execute("INSERT INTO " + tableName + " (l, i, ts) VALUES(0, 0, '" + readOnlyPartitionName + "T23:59:59.500001Z')");
                            assertSql(replaceTimestampSuffix("min\tmax\tcount\n" +
                                    "2022-10-17T23:59:59.500001Z\t2022-10-18T23:59:59.000000Z\t5001\n"), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );

                            // drop the partition
                            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testDropIndex() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, ignore -> {
                        try {
                            execute("ALTER TABLE " + tableName + " ALTER COLUMN s DROP INDEX");
                            execute("ALTER TABLE " + tableName + " ALTER COLUMN s ADD INDEX");

                            // silently ignored because the partition is read-only
                            execute("INSERT INTO " + tableName + " VALUES(1492, 10, 'howdy', '" + readOnlyPartitionName + "T23:59:59.999999Z')");
                            assertSql(
                                    replaceTimestampSuffix1("min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );

                            assertSql(
                                    replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                            "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testDropPartition() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "IGLOO", tableToken -> {
                        try {
                            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );

                            // verify cold storage folder exists
                            Assert.assertTrue(Files.exists(other.$()));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);
                            path.of(configuration.getDbRoot())
                                    .concat(tableToken)
                                    .concat(readOnlyPartitionName)
                                    .put(".2")
                                    .$();
                            Assert.assertFalse(ff.exists(path.$()));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testDropPartitionWhileThereIsAReader() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "FINLAND", tableToken -> {
                        try {
                            try (TableReader ignore = engine.getReader(tableToken)) {
                                // drop the partition which was attached via soft link
                                execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                                // there is a reader, cannot unlink, thus the link will still exist
                                path.of(configuration.getDbRoot()) // <-- soft link path
                                        .concat(tableToken)
                                        .concat(readOnlyPartitionName)
                                        .put(".2")
                                        .$();
                                Assert.assertTrue(Files.exists(path.$()));
                            }
                            engine.releaseAllReaders();
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );

                            runO3PartitionPurgeJob();

                            // verify cold storage folder still exists
                            Assert.assertTrue(Files.exists(other.$()));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);
                            Assert.assertFalse(Files.exists(path.$()));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testDropPartitionWhileThereIsAReaderWindows() throws Exception {
        Assume.assumeTrue(Os.isWindows()); // for consistency with the test's name
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, tableToken -> {
                        TestUtils.unchecked(() -> {
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                    "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                    "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                    "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                    "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                            );

                            try (TableReader ignore = engine.getReader(tableToken)) {
                                execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            }

                            runO3PartitionPurgeJob();

                            path.of(configuration.getDbRoot()).concat(tableToken);
                            int plen = path.size();
                            // in Windows if this was a real soft link to a folder, the link would be deleted
                            Assert.assertFalse(ff.exists(path.concat(readOnlyPartitionName).$()));
                            Assert.assertTrue(ff.exists(path.trimTo(plen).concat("2022-10-18").$()));
                        });
                        return null;
                    }
            );
        });
    }

    @Test
    public void testDropPartitionWindows() throws Exception {
        Assume.assumeTrue(Os.isWindows()); // for consistency with the test's name
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, ignore -> {
                        try {
                            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testInsertInTransaction() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, tableToken -> {
                        try {
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "5001\t5001\t\t2022-10-18T00:00:16.779900Z\n" +
                                    "5002\t5002\tHYRX\t2022-10-18T00:00:34.059800Z\n" +
                                    "5003\t5003\tCPSW\t2022-10-18T00:00:51.339700Z\n" +
                                    "5004\t5004\tVTJW\t2022-10-18T00:01:08.619600Z\n" +
                                    "5005\t5005\tPEHN\t2022-10-18T00:01:25.899500Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + activePartitionName + "' LIMIT 5"
                            );

                            try (TableWriter writer = getWriter(tableToken)) {
                                TableWriter.Row row = writer.newRow(activePartitionTimestamp);
                                row.putLong(0, 2023);
                                row.putInt(1, 12);
                                row.putSym(2, "December");
                                row.append();

                                row = writer.newRow(readOnlyPartitionTimestamp);
                                row.putLong(0, 2023);
                                row.putInt(1, 11);
                                row.putSym(2, "Norway");
                                row.append();

                                // goes through but only one row makes it into the table
                                writer.commit();

                                assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                        "2023\t12\tDecember\t2022-10-18T00:00:00.000000Z\n" +
                                        "5001\t5001\t\t2022-10-18T00:00:16.779900Z\n" +
                                        "5002\t5002\tHYRX\t2022-10-18T00:00:34.059800Z\n" +
                                        "5003\t5003\tCPSW\t2022-10-18T00:00:51.339700Z\n" +
                                        "5004\t5004\tVTJW\t2022-10-18T00:01:08.619600Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + activePartitionName + "' LIMIT 5"
                                );

                                assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                        "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                        "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                        "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                        "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                        "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5"
                                );

                                row = writer.newRow(activePartitionTimestamp);
                                row.putLong(0, 2023);
                                row.putInt(1, 10);
                                row.putSym(2, "Octopus");
                                row.append();

                                writer.commit();
                            }
                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + readOnlyPartitionName + "T23:59:42.220100Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );

                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10002\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "2023\t10\tOctopus\t2022-10-18T00:00:00.000000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE s = 'Octopus'"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testInsertLastPartitionIsReadOnly() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = testName.getMethodName();
            TableToken tableToken = createPopulateTable(tableName, 5);

            // make all partitions, but last, read-only
            try (TableWriter writer = getWriter(tableToken)) {
                TxWriter txWriter = writer.getTxWriter();
                int partitionCount = txWriter.getPartitionCount();
                Assert.assertEquals(5, partitionCount);
                for (int i = 0, n = partitionCount - 1; i < n; i++) {
                    txWriter.setPartitionReadOnly(i, true);
                }
                try {
                    txWriter.setPartitionReadOnly(-1, true);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "bad partition index -1");
                }
                txWriter.bumpTruncateVersion();
                txWriter.commit(writer.getDenseSymbolMapWriters());
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(tableToken)) {
                TxReader txFile = reader.getTxFile();
                int partitionCount = txFile.getPartitionCount();
                Assert.assertEquals(5, partitionCount);
                for (int i = 0, n = partitionCount - 1; i < n; i++) {
                    Assert.assertTrue(txFile.isPartitionReadOnly(i));
                }
                Assert.assertFalse(txFile.isPartitionReadOnly(partitionCount - 1));
            }

            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    "2022-10-17T00:00:43.199900Z\t2022-10-18T00:00:42.999900Z\t2001\n" +
                    "2022-10-18T00:01:26.199800Z\t2022-10-19T00:00:42.799900Z\t2000\n" +
                    "2022-10-19T00:01:25.999800Z\t2022-10-20T00:00:42.599900Z\t2000\n" +
                    "2022-10-20T00:01:25.799800Z\t2022-10-21T00:00:42.399900Z\t2000\n" +
                    "2022-10-21T00:01:25.599800Z\t2022-10-21T23:59:59.000000Z\t1999\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO FIRST OBSERVATION"
            );

            String lastReadOnlyPartitionName = "2022-10-20";
            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                    "6001\t6001\t\t2022-10-20T00:00:42.599900Z\n" +
                    "6002\t6002\tPEHN\t2022-10-20T00:01:25.799800Z\n" +
                    "6003\t6003\t\t2022-10-20T00:02:08.999700Z\n" +
                    "6004\t6004\tCPSW\t2022-10-20T00:02:52.199600Z\n" +
                    "6005\t6005\tCPSW\t2022-10-20T00:03:35.399500Z\n", timestampTypeName), tableName + " WHERE ts IN '" + lastReadOnlyPartitionName + "' LIMIT 5"
            );
            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                    "7996\t7996\tVTJW\t2022-10-20T23:57:06.400400Z\n" +
                    "7997\t7997\t\t2022-10-20T23:57:49.600300Z\n" +
                    "7998\t7998\t\t2022-10-20T23:58:32.800200Z\n" +
                    "7999\t7999\tPEHN\t2022-10-20T23:59:16.000100Z\n" +
                    "8000\t8000\tPEHN\t2022-10-20T23:59:59.200000Z\n", timestampTypeName), tableName + " WHERE ts IN '" + lastReadOnlyPartitionName + "' LIMIT -5"
            );

            // silently ignored as the partition is read only
            execute("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(0, 0, 'ø','" + lastReadOnlyPartitionName + "T23:59:59.500001Z')");

            assertUpdateFailsBecausePartitionIsReadOnly(
                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + lastReadOnlyPartitionName + "T23:59:16.000100Z'",
                    tableName,
                    lastReadOnlyPartitionName
            );

            // silently ignored as the partition is read only
            execute("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(-1, -1, 'µ','" + lastReadOnlyPartitionName + "T00:00:00.100005Z')");

            assertUpdateFailsBecausePartitionIsReadOnly(
                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + lastReadOnlyPartitionName + "T00:02:08.999700Z'",
                    tableName,
                    lastReadOnlyPartitionName
            );

            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    "2022-10-17T00:00:43.199900Z\t2022-10-18T00:00:42.999900Z\t2001\n" +
                    "2022-10-18T00:01:26.199800Z\t2022-10-19T00:00:42.799900Z\t2000\n" +
                    "2022-10-19T00:01:25.999800Z\t2022-10-20T00:00:42.599900Z\t2000\n" +
                    "2022-10-20T00:01:25.799800Z\t2022-10-21T00:00:42.399900Z\t2000\n" +
                    "2022-10-21T00:01:25.599800Z\t2022-10-21T23:59:59.000000Z\t1999\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO FIRST OBSERVATION"
            );

            // drop active partition
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-10-21'", sqlExecutionContext);
            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    "2022-10-17T00:00:43.199900Z\t2022-10-18T00:00:42.999900Z\t2001\n" +
                    "2022-10-18T00:01:26.199800Z\t2022-10-19T00:00:42.799900Z\t2000\n" +
                    "2022-10-19T00:01:25.999800Z\t2022-10-20T00:00:42.599900Z\t2000\n" +
                    "2022-10-20T00:01:25.799800Z\t2022-10-20T23:59:59.200000Z\t1999\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO FIRST OBSERVATION"
            );

            // the previously read-only partition becomes now the active partition, and cannot be written to
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(tableToken)) {
                TxReader txFile = reader.getTxFile();
                for (int i = 0; i < 4; i++) {
                    Assert.assertTrue(txFile.isPartitionReadOnly(i));
                }
            }

            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                    "6001\t6001\t\t2022-10-20T00:00:42.599900Z\n" +
                    "6002\t6002\tPEHN\t2022-10-20T00:01:25.799800Z\n" +
                    "6003\t6003\t\t2022-10-20T00:02:08.999700Z\n" +
                    "6004\t6004\tCPSW\t2022-10-20T00:02:52.199600Z\n" +
                    "6005\t6005\tCPSW\t2022-10-20T00:03:35.399500Z\n", timestampTypeName), tableName + " WHERE ts in '" + lastReadOnlyPartitionName + "' LIMIT 5"
            );
            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                    "7996\t7996\tVTJW\t2022-10-20T23:57:06.400400Z\n" +
                    "7997\t7997\t\t2022-10-20T23:57:49.600300Z\n" +
                    "7998\t7998\t\t2022-10-20T23:58:32.800200Z\n" +
                    "7999\t7999\tPEHN\t2022-10-20T23:59:16.000100Z\n" +
                    "8000\t8000\tPEHN\t2022-10-20T23:59:59.200000Z\n", timestampTypeName), tableName + " WHERE ts in '" + lastReadOnlyPartitionName + "' LIMIT -5"
            );

            // silently ignored as the partition is read only
            execute("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(-1, -1, 'µ','" + lastReadOnlyPartitionName + "T23:59:59.990002Z')");
            assertUpdateFailsBecausePartitionIsReadOnly(
                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + lastReadOnlyPartitionName + "T23:59:59.200000Z'",
                    tableName,
                    lastReadOnlyPartitionName);

            // create new partition at the end and append data to it
            String newPartitionName = "2022-10-21";
            execute("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(-1, -1, 'µ','" + newPartitionName + "T20:00:00.202312Z')");
            update("UPDATE " + tableName + " SET l = 13 WHERE ts = '" + newPartitionName + "T20:00:00.202312Z'");
            assertSql(
                    replaceTimestampSuffix("l\ti\ts\tts\n" +
                            "13\t-1\tµ\t2022-10-21T20:00:00.202312Z\n"), tableName + " WHERE ts in '" + newPartitionName + "'"
            );

            assertSql(
                    replaceTimestampSuffix1("l\ti\ts\tts\n" +
                            "6001\t6001\t\t2022-10-20T00:00:42.599900Z\n" +
                            "6002\t6002\tPEHN\t2022-10-20T00:01:25.799800Z\n" +
                            "6003\t6003\t\t2022-10-20T00:02:08.999700Z\n" +
                            "6004\t6004\tCPSW\t2022-10-20T00:02:52.199600Z\n" +
                            "6005\t6005\tCPSW\t2022-10-20T00:03:35.399500Z\n", timestampTypeName), tableName + " WHERE ts in '" + lastReadOnlyPartitionName + "' LIMIT 5"
            );

            assertSql(
                    replaceTimestampSuffix1("l\ti\ts\tts\n" +
                            "7996\t7996\tVTJW\t2022-10-20T23:57:06.400400Z\n" +
                            "7997\t7997\t\t2022-10-20T23:57:49.600300Z\n" +
                            "7998\t7998\t\t2022-10-20T23:58:32.800200Z\n" +
                            "7999\t7999\tPEHN\t2022-10-20T23:59:16.000100Z\n" +
                            "8000\t8000\tPEHN\t2022-10-20T23:59:59.200000Z\n", timestampTypeName), tableName + " WHERE ts in '" + lastReadOnlyPartitionName + "' LIMIT -5"
            );

            assertSql(
                    replaceTimestampSuffix("min\tmax\tcount\n" +
                            "2022-10-17T00:00:43.199900Z\t2022-10-18T00:00:42.999900Z\t2001\n" +
                            "2022-10-18T00:01:26.199800Z\t2022-10-19T00:00:42.799900Z\t2000\n" +
                            "2022-10-19T00:01:25.999800Z\t2022-10-20T00:00:42.599900Z\t2000\n" +
                            "2022-10-20T00:01:25.799800Z\t2022-10-20T23:59:59.200000Z\t1999\n" +
                            "2022-10-21T20:00:00.202312Z\t2022-10-21T20:00:00.202312Z\t1\n"), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testInsertMultiRowAllPartitionsAreReadOnlyNo03() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = testName.getMethodName();
            TableToken tableToken = createPopulateTable(tableName, 5);
            makeAllPartitionsReadOnly(tableToken);

            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    "2022-10-17T00:00:43.199900Z\t2022-10-17T23:59:59.800000Z\t2000\n" +
                    "2022-10-18T00:00:42.999900Z\t2022-10-18T23:59:59.600000Z\t2000\n" +
                    "2022-10-19T00:00:42.799900Z\t2022-10-19T23:59:59.400000Z\t2000\n" +
                    "2022-10-20T00:00:42.599900Z\t2022-10-20T23:59:59.200000Z\t2000\n" +
                    "2022-10-21T00:00:42.399900Z\t2022-10-21T23:59:59.000000Z\t2000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR"
            );

            // silently ignored as the partition is read only
            String firstPartitionName = "2022-10-17";
            String lastPartitionName = "2022-10-21";
            String newPartitionName = "2022-10-22";
            String multiInsertStmt = "INSERT INTO " + tableName + " (l, i, s, ts) VALUES";
            multiInsertStmt += "(0, 0, 'ø', '" + firstPartitionName + "T23:59:59.900003Z'),";
            multiInsertStmt += "(0, 0, 'ø', '" + lastPartitionName + "T23:59:59.500003Z'),";
            multiInsertStmt += "(0, 1, 'ø', '" + lastPartitionName + "T23:59:59.500004Z'),";
            multiInsertStmt += "(1, 0, 'µ', '" + newPartitionName + "T01:00:27.202901Z'),";
            multiInsertStmt += "(1, 1, 'µ', '" + newPartitionName + "T01:00:27.202902Z');";
            execute(multiInsertStmt);
            assertSql(replaceTimestampSuffix("l\ti\ts\tts\n" +
                    "1\t0\tµ\t2022-10-22T01:00:27.202901Z\n" +
                    "1\t1\tµ\t2022-10-22T01:00:27.202902Z\n"), tableName + " WHERE ts in '" + newPartitionName + "'"
            );

            assertSql(replaceTimestampSuffix("min\tmax\tcount\n" +
                    "2022-10-17T00:00:43.199900Z\t2022-10-17T23:59:59.800000Z\t2000\n" +
                    "2022-10-18T00:00:42.999900Z\t2022-10-18T23:59:59.600000Z\t2000\n" +
                    "2022-10-19T00:00:42.799900Z\t2022-10-19T23:59:59.400000Z\t2000\n" +
                    "2022-10-20T00:00:42.599900Z\t2022-10-20T23:59:59.200000Z\t2000\n" +
                    "2022-10-21T00:00:42.399900Z\t2022-10-21T23:59:59.000000Z\t2000\n" +
                    "2022-10-22T01:00:27.202901Z\t2022-10-22T01:00:27.202902Z\t2\n"), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testInsertMultiRowAllPartitionsAreReadOnlyWith03() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = testName.getMethodName();
            TableToken tableToken = createPopulateTable(tableName, 5);
            makeAllPartitionsReadOnly(tableToken);

            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    "2022-10-17T00:00:43.199900Z\t2022-10-17T23:59:59.800000Z\t2000\n" +
                    "2022-10-18T00:00:42.999900Z\t2022-10-18T23:59:59.600000Z\t2000\n" +
                    "2022-10-19T00:00:42.799900Z\t2022-10-19T23:59:59.400000Z\t2000\n" +
                    "2022-10-20T00:00:42.599900Z\t2022-10-20T23:59:59.200000Z\t2000\n" +
                    "2022-10-21T00:00:42.399900Z\t2022-10-21T23:59:59.000000Z\t2000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR"
            );

            // silently ignored as the partition is read only
            String firstPartitionName = "2022-10-17";
            String lastPartitionName = "2022-10-21";
            String newPartitionName = "2022-10-22";
            String multiInsertStmt = "INSERT INTO " + tableName + " (l, i, s, ts) VALUES";
            multiInsertStmt += "(31, 10, 'ø', '" + lastPartitionName + "T23:59:59.500002Z'),";
            multiInsertStmt += "(31, 0, 'ø', '" + lastPartitionName + "T23:59:59.500001Z'),";
            multiInsertStmt += "(1, 1, 'µø', '" + newPartitionName + "T01:00:27.202901Z'),";
            multiInsertStmt += "(137, -3, 'P', '" + firstPartitionName + "T00:03:09.103056Z'),";
            multiInsertStmt += "(1, 0, 'µ', '" + newPartitionName + "T01:00:26.453476Z');";
            execute(multiInsertStmt);
            assertSql(replaceTimestampSuffix("l\ti\ts\tts\n" +
                    "1\t0\tµ\t2022-10-22T01:00:26.453476Z\n" +
                    "1\t1\tµø\t2022-10-22T01:00:27.202901Z\n"), tableName + " WHERE ts in '" + newPartitionName + "'"
            );

            assertSql(replaceTimestampSuffix("min\tmax\tcount\n" +
                    "2022-10-17T00:00:43.199900Z\t2022-10-17T23:59:59.800000Z\t2000\n" +
                    "2022-10-18T00:00:42.999900Z\t2022-10-18T23:59:59.600000Z\t2000\n" +
                    "2022-10-19T00:00:42.799900Z\t2022-10-19T23:59:59.400000Z\t2000\n" +
                    "2022-10-20T00:00:42.599900Z\t2022-10-20T23:59:59.200000Z\t2000\n" +
                    "2022-10-21T00:00:42.399900Z\t2022-10-21T23:59:59.000000Z\t2000\n" +
                    "2022-10-22T01:00:26.453476Z\t2022-10-22T01:00:27.202901Z\t2\n"), "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testInsertUpdate() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, ignore -> {
                        try {
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                    "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                    "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                    "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                    "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                            );

                            // silently ignored as the partition is read only
                            execute("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(0, 0, 'ø','" + readOnlyPartitionName + "T23:59:59.500001Z')");

                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + readOnlyPartitionName + "T23:59:42.220100Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );

                            // silently ignored as the partition is read only
                            execute("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(-1, -1, 'µ','" + readOnlyPartitionName + "T00:00:00.100005Z')");

                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '2022-10-17T00:00:34.559800Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );

                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" + // <-- update was skipped, l would have been 13
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                                    // <-- no new row at the end
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                    "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" + // <-- update was skipped, l would have been 13
                                    "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                    "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                    "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testPurgePartitions() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            String tableName = testName.getMethodName();
            String[] partitionName = {
                    "2022-10-17",
                    "2022-10-18",
                    "2022-10-19",
                    "2022-10-20",
                    "2022-10-21",
                    "2022-10-22",
            };
            int partitionCount = partitionName.length;
            String expectedMinTimestamp = "2022-10-17T00:00:51.839900Z";
            String expectedMaxTimestamp = "2022-10-22T23:59:59.000000Z";
            String otherLocation = "CON-CHIN-CHINA";
            int txn = 0;
            TableToken tableToken;
            TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);
            tableToken = createPopulateTable(
                    1,
                    src.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("s", ColumnType.SYMBOL).indexed(true, 32)
                            .timestamp("ts", timestampType),
                    10000,
                    partitionName[0],
                    partitionCount
            );
            txn++;
            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
            );

            // detach all partitions but last two and them from soft link
            path.of(configuration.getDbRoot()).concat(tableToken);
            int pathLen = path.size();
            for (int i = 0; i < partitionCount - 2; i++) {
                execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName[i] + "'", sqlExecutionContext);
                txn++;
                copyToDifferentLocationAndMakeAttachableViaSoftLink(tableToken, partitionName[i], otherLocation);
                execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName[i] + "'", sqlExecutionContext);
                txn++;

                // verify that the link has been renamed to what we expect
                path.trimTo(pathLen).concat(partitionName[i]);
                TestUtils.txnPartitionConditionally(path, txn - 1);
                Assert.assertTrue(Files.exists(path.$()));
            }

            // verify read-only flag
            try (TableReader reader = engine.getReader(tableToken)) {
                TxReader txFile = reader.getTxFile();
                for (int i = 0; i < partitionCount - 2; i++) {
                    Assert.assertTrue(txFile.isPartitionReadOnly(i));
                }
                Assert.assertFalse(txFile.isPartitionReadOnly(partitionCount - 2));
                Assert.assertFalse(txFile.isPartitionReadOnly(partitionCount - 1));
            }

            // verify content
            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
            );

            // create a reader, which will prevent partitions from being immediately purged
            try (TableReader ignore = engine.getReader(tableToken)) {
                // drop all partitions but the most recent
                for (int i = 0, expectedTxn = 2; i < partitionCount - 2; i++, expectedTxn += 2) {
                    execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName[i] + "'", sqlExecutionContext);
                    path.trimTo(pathLen).concat(partitionName[i]);
                    TestUtils.txnPartitionConditionally(path, expectedTxn);
                    Assert.assertTrue(Files.exists(path.$()));
                }
                execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName[partitionCount - 2] + "'", sqlExecutionContext);
                path.trimTo(pathLen).concat(partitionName[partitionCount - 2]);
                Assert.assertTrue(Files.exists(path.$()));
            }
            engine.releaseAllReaders();
            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    "2022-10-22T00:00:33.726600Z\t2022-10-22T23:59:59.000000Z\t1667\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
            );

            runO3PartitionPurgeJob();

            // verify cold storage still exists
            other.of(new File(temp.getRoot(), otherLocation).getAbsolutePath()).concat(tableToken);
            int otherLen = other.size();
            AtomicInteger fileCount = new AtomicInteger();
            for (int i = 0; i < partitionCount - 2; i++) {
                other.trimTo(otherLen).concat(partitionName[i]).put(TableUtils.DETACHED_DIR_MARKER).$();
                Assert.assertTrue(Files.exists(other.$()));
                fileCount.set(0);
                ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                Assert.assertTrue(fileCount.get() > 0);
            }

            // verify all partitions but last one are gone
            for (int i = 0; i < partitionCount - 1; i++) {
                path.trimTo(pathLen).concat(partitionName[i]).$();
                Assert.assertFalse(Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testRemoveColumn() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "REFRIGERATOR", tableToken -> {
                        TestUtils.unchecked(() -> {
                            execute("ALTER TABLE " + tableName + " DROP COLUMN s");

                            // this lad silently fails..... because the partition is read only
                            execute("INSERT INTO " + tableName + " VALUES(666, 666, '" + readOnlyPartitionName + "T23:59:59.999999Z')");

                            assertSql(
                                    replaceTimestampSuffix1("min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(
                                    replaceTimestampSuffix1("l\ti\tts\n" +
                                            "4996\t4996\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\t2022-10-17T23:59:59.500000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                            );
                        });

                        // check that the column files still exist within the partition folder (attached from soft link)
                        final int pathLen = path.size();
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.d").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.k").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.v").$()));

                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                        try (
                                ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine);
                                TableReader reader = engine.getReader(tableToken)
                        ) {
                            TxReader txReader = reader.getTxFile();
                            Assert.assertTrue(txReader.unsafeLoadAll());
                            Assert.assertTrue(txReader.isPartitionReadOnlyByPartitionTimestamp(txReader.getPartitionTimestampByIndex(0)));
                            Assert.assertFalse(txReader.isPartitionReadOnlyByPartitionTimestamp(txReader.getPartitionTimestampByIndex(1)));
                            if (Os.isWindows()) {
                                engine.releaseInactive();
                            }
                            purgeJob.run(0);
                        } catch (SqlException unexpected) {
                            Assert.fail(unexpected.getMessage());
                        }

                        // check that the column files still exist within the partition folder (attached from soft link)
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.d").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.k").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.v").$()));
                        return null;
                    }
            );
        });
    }

    @Test
    public void testRemoveColumnWindows() throws Exception {
        Assume.assumeTrue(Os.isWindows()); // for consistency with the test's name
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, ignore -> {
                        TestUtils.unchecked(() -> {
                            execute("ALTER TABLE " + tableName + " DROP COLUMN s");

                            // silently ignored as the partition is read only
                            execute("INSERT INTO " + tableName + " VALUES(666, 666, '" + readOnlyPartitionName + "T23:59:59.999999Z')");

                            assertSql(
                                    replaceTimestampSuffix1("min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );

                            assertSql(
                                    replaceTimestampSuffix1("l\ti\tts\n" +
                                            "4996\t4996\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\t2022-10-17T23:59:59.500000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                            );
                        });
                        return null;
                    }
            );
        });
    }

    @Test
    public void testRenameColumn() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, ignore -> {
                        try {
                            execute("ALTER TABLE " + tableName + " RENAME COLUMN s TO ss");
                            execute("ALTER TABLE " + tableName + " ALTER COLUMN ss DROP INDEX");

                            // silently ignored as the partition is read only
                            execute("INSERT INTO " + tableName + " VALUES(666, 666, 'queso', '" + readOnlyPartitionName + "T23:59:59.999999Z')");
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\tss\tts\n" +
                                    "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                    "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                    "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                    "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                    "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testTruncateTable() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "FRIO_DEL_15", tableToken -> {
                        try {
                            execute("TRUNCATE TABLE " + tableName, sqlExecutionContext);
                            assertSql("min\tmax\tcount\n" +
                                    "\t\t0\n", "SELECT min(ts), max(ts), count() FROM " + tableName
                            );

                            // verify cold storage folder exists
                            Assert.assertTrue(Files.exists(other.$()));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);
                            path.of(configuration.getDbRoot())
                                    .concat(tableToken)
                                    .concat(readOnlyPartitionName)
                                    .put(".2")
                                    .$();
                            Assert.assertFalse(ff.exists(path.$()));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testTruncateTableWindows() throws Exception {
        Assume.assumeTrue(Os.isWindows()); // for consistency with the test's name
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, tableToken -> {
                        try {
                            execute("TRUNCATE TABLE " + tableName, sqlExecutionContext);
                            path.of(configuration.getDbRoot()).concat(tableToken);
                            int plen = path.size();
                            Assert.assertFalse(ff.exists(path.concat(readOnlyPartitionName).$()));
                            Assert.assertFalse(ff.exists(path.trimTo(plen).concat("2022-10-18").$()));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testUpdate() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "LEGEND", ignore -> {
                        try {
                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + readOnlyPartitionName + "T00:00:17.279900Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                    "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                    "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                    "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                    "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5"
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                    "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                    "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                    "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                    "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    @Test
    public void testUpdateWindows() throws Exception {
        Assume.assumeTrue(Os.isWindows()); // for consistency with the test's name
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, ignore -> {
                        try {
                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + readOnlyPartitionName + "T00:00:17.279900Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );
                            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                                    "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
                            );
                            assertSql(replaceTimestampSuffix1("l\ti\ts\tts\n" +
                                    "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" + // update is skipped, l would have been 13
                                    "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                    "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                    "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                    "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n", timestampTypeName), "SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                        return null;
                    }
            );
        });
    }

    private static void makeAllPartitionsReadOnly(TableToken tableToken) {
        try (TableWriter writer = getWriter(tableToken)) {
            TxWriter txWriter = writer.getTxWriter();
            int partitionCount = txWriter.getPartitionCount();
            Assert.assertEquals(5, partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                txWriter.setPartitionReadOnly(i, true);
            }
            txWriter.bumpTruncateVersion();
            txWriter.commit(writer.getDenseSymbolMapWriters());
        }
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        try (TableReader reader = engine.getReader(tableToken)) {
            TxReader txFile = reader.getTxFile();
            int partitionCount = txFile.getPartitionCount();
            Assert.assertEquals(5, partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                Assert.assertTrue(txFile.isPartitionReadOnly(i));
            }
        }
    }

    private static void runO3PartitionPurgeJob() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
            while (purgeJob.run(0)) {
                Os.pause();
            }
        }
    }

    private void assertUpdateFailsBecausePartitionIsReadOnly(String updateSql, String tableName, String partitionName) {
        try {
            assertExceptionNoLeakCheck(updateSql);
        } catch (CairoException e) {
            TestUtils.assertContains(
                    "cannot update read-only partition [table=" + tableName + ", partitionTimestamp=" + partitionName + replaceTimestampSuffix("T00:00:00.000000Z]", timestampTypeName),
                    e.getFlyweightMessage());
        } catch (Throwable e) {
            Assert.fail("not expecting any Exception: " + e.getMessage());
        }
    }

    private void attachPartitionFromSoftLink(String tableName, String otherLocation, Function<TableToken, Void> test) throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            TableToken tableToken = createPopulateTable(tableName, 2);
            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
            );

            // detach partition and attach it from soft link
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableToken, readOnlyPartitionName, otherLocation);
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);

            // verify that the link has been renamed to what we expect
            path.of(configuration.getDbRoot()).concat(tableToken).concat(readOnlyPartitionName);
            TestUtils.txnPartitionConditionally(path, 2);
            Assert.assertTrue(Files.exists(path.$()));

            // verify RO flag
            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(tableToken)) {
                TxReader txFile = reader.getTxFile();
                Assert.assertNotNull(txFile);
                Assert.assertTrue(txFile.isPartitionReadOnly(0));
                Assert.assertFalse(txFile.isPartitionReadOnly(1));
            }
            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
            );
            test.apply(tableToken);
        });
    }

    private void copyToDifferentLocationAndMakeAttachableViaSoftLink(
            TableToken tableToken,
            CharSequence partitionName,
            String otherLocation
    ) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        // copy .detached folder to the different location
        CharSequence tmp;
        try {
            tmp = temp.newFolder(otherLocation).getAbsolutePath();
        } catch (IOException e) {
            tmp = new File(temp.getRoot(), otherLocation).getAbsolutePath();
        }
        final CharSequence s3Buckets = tmp;
        final String detachedPartitionName = partitionName + TableUtils.DETACHED_DIR_MARKER;
        copyPartitionAndMetadata( // this creates s3Buckets
                configuration.getDbRoot(),
                tableToken,
                detachedPartitionName,
                s3Buckets,
                tableToken.getDirName(),
                detachedPartitionName,
                null
        );

        // create the .attachable link in the table's data folder
        // with target the .detached folder in the different location
        other.of(s3Buckets)
                .concat(tableToken)
                .concat(detachedPartitionName)
                .$();
        path.of(configuration.getDbRoot()) // <-- soft link path
                .concat(tableToken)
                .concat(partitionName)
                .put(configuration.getAttachPartitionSuffix())
                .$();
        Assert.assertEquals(0, ff.softLink(other.$(), path.$()));
    }

    private TableToken createPopulateTable(String tableName, int partitionCount) throws Exception {
        TableToken tableToken;
        TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);
        tableToken = createPopulateTable(
                1,
                src.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL).indexed(true, 32)
                        .timestamp("ts", timestampType),
                10000,
                "2022-10-17",
                partitionCount
        );
        return tableToken;
    }

    private void createTableWithReadOnlyPartition(String tableName, Function<TableToken, Void> test) throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            TableToken tableToken = createPopulateTable(tableName, 2);
            // the read-only flag is only set when a partition is attached from soft link
            try (TableWriter writer = getWriter(tableToken)) {
                TxWriter txWriter = writer.getTxWriter();
                txWriter.setPartitionReadOnlyByTimestamp(readOnlyPartitionTimestamp, true);
                txWriter.bumpTruncateVersion();
                txWriter.commit(writer.getDenseSymbolMapWriters());
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(tableToken)) {
                TxReader txFile = reader.getTxFile();
                Assert.assertTrue(txFile.isPartitionReadOnlyByPartitionTimestamp(readOnlyPartitionTimestamp));
                Assert.assertTrue(txFile.isPartitionReadOnly(0));
                Assert.assertFalse(txFile.isPartitionReadOnly(1));
            }
            assertSql(replaceTimestampSuffix1("min\tmax\tcount\n" +
                    expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n", timestampTypeName), "SELECT min(ts), max(ts), count() FROM " + tableName
            );
            test.apply(tableToken);
        });
    }

    private String replaceTimestampSuffix(String expected) {
        return ColumnType.isTimestampNano(timestampType) ? expected.replaceAll("Z\t", "000Z\t").replaceAll("Z\n", "000Z\n") : expected;
    }
}
