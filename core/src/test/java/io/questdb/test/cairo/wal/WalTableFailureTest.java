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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.BitmapIndexUtils;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.questdb.cairo.ErrorTag.*;
import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.std.Files.SEPARATOR;
import static io.questdb.test.tools.TestUtils.assertEventually;

public class WalTableFailureTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
    }

    @Test
    public void testAddColumnFailToApplySequencerMetadataStructureChangeTransaction() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            try (TableWriterAPI twa = engine.getTableWriterAPI(tableName, "test")) {
                AtomicInteger counter = new AtomicInteger(2);
                AlterOperation dodgyAlterOp = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        if (counter.decrementAndGet() == 0) {
                            throw new IndexOutOfBoundsException();
                        }
                        svc.addColumn(
                                "new_column",
                                ColumnType.INT,
                                0,
                                false,
                                false,
                                12,
                                false,
                                false,
                                null
                        );
                        return 0;
                    }

                    @Override
                    public boolean isStructural() {
                        return true;
                    }
                };

                try {
                    twa.apply(dodgyAlterOp, true);
                    Assert.fail("Expected exception is missing");
                } catch (CairoException ex) {
                    //expected
                    TestUtils.assertContains(ex.getFlyweightMessage(), "invalid alter table command");
                }
            }

            drainWalQueue();
            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableName.getTableName());
        });
    }

    @Test
    public void testAddColumnFailToSerialiseToSequencerTransactionLog() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            try (TableWriterAPI twa = engine.getTableWriterAPI(tableName, "test")) {
                AlterOperation dodgyAlterOp = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        svc.addColumn(
                                "new_column",
                                ColumnType.INT,
                                0,
                                false,
                                false,
                                12,
                                false,
                                false,
                                null
                        );
                        return 0;
                    }

                    @Override
                    public boolean isStructural() {
                        return true;
                    }

                    @Override
                    public void serializeBody(MemoryA sink) {
                        throw new IndexOutOfBoundsException();
                    }
                };

                try {
                    twa.apply(dodgyAlterOp, true);
                    Assert.fail("Expected exception is missing");
                } catch (IndexOutOfBoundsException ex) {
                    //expected
                }
            }

            drainWalQueue();
            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableName.getTableName());
        });
    }

    @Test
    public void testAlterTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set", "'param', 'ttl' or 'type' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set typ", "'param' or 'type' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set type", "'bypass' or 'wal' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set type byoass", "'bypass' or 'wal' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set type bypass", "'wal' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set type wall", "'bypass' or 'wal' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set type bypass wa", "'wal' expected");
        });
    }

    @Test
    public void testApplyJobFailsToApplyDataFirstTime() throws Exception {
        FilesFacade dodgyFacade = new TestFilesFacadeImpl() {
            int counter = 0;

            @Override
            public long openRW(LPSZ name, int mode) {
                if (Utf8s.endsWithAscii(name, "2022-02-25" + Files.SEPARATOR + "x.d.1") && counter++ < 2) {
                    return -1;
                }
                return super.openRW(name, mode);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();
            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-25', 'ef')");

            // Data is not there, job failed to apply the data.
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableName.getTableName());

            drainWalQueue();

            // Second time lucky, 2 line in.
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-25T00:00:00.000000Z\tef\n", tableName.getTableName());
        });
    }

    @Test
    public void testDataTxnFailToCommitInWalWriter() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            drainWalQueue();

            IntHashSet badWalIds = new IntHashSet();

            try (
                    WalWriter walWriter1 = engine.getWalWriter(tableToken);
                    WalWriter walWriter2 = engine.getWalWriter(tableToken);
                    WalWriter walWriter3 = engine.getWalWriter(tableToken)
            ) {
                Assert.assertEquals(1, walWriter1.getWalId());
                Assert.assertEquals(2, walWriter2.getWalId());
                Assert.assertEquals(3, walWriter3.getWalId());
            }

            AlterOperationBuilder alterOpBuilder = new AlterOperationBuilder().ofDropColumn(1, tableToken, 0);
            AlterOperation alterOp = alterOpBuilder.ofDropColumn("non_existing_column").build();

            int badWriterId;
            try (
                    TableWriterAPI alterWriter = engine.getTableWriterAPI(tableToken, "test");
                    TableWriterAPI insertWriter = engine.getTableWriterAPI(tableToken, "test")
            ) {

                badWalIds.add(badWriterId = ((WalWriter) alterWriter).getWalId());

                // Serialize into WAL sequencer a drop column operation of non-existing column
                // So that it will fail during application to WAL sequencer
                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        svc.removeColumn("x");
                        return 0;
                    }

                    @Override
                    public boolean isStructural() {
                        return true;
                    }

                    @Override
                    public void serializeBody(MemoryA sink) {
                        alterOp.serializeBody(sink);
                    }
                };

                try {
                    alterWriter.apply(dodgyAlter, true);
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "Column not found: non_existing_column");
                }

                TableWriter.Row row = insertWriter.newRow(MicrosTimestampDriver.floor("2022-02-25"));
                row.putLong(0, 123L);
                row.append();
                insertWriter.commit();

            } finally {
                Misc.free(alterOp);
            }

            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                Assert.assertTrue(badWalIds.excludes(walWriter1.getWalId()));

                // Assert wal writer 2 is not in the pool after failure to apply structure change
                // wal writer 3 will fail to go active because of dodgy Alter in the WAL sequencer

                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    Assert.assertTrue(badWalIds.excludes(walWriter2.getWalId()));

                    try (WalWriter walWriter3 = engine.getWalWriter(tableToken)) {
                        Assert.assertTrue(badWalIds.excludes(walWriter3.getWalId()));
                        Assert.assertNotEquals(badWriterId, walWriter3.getWalId());
                    }
                }
            }
        });
    }

    @Test
    public void testDataTxnFailToHardLinkSymbolCharFile() throws Exception {
        testFailToLinkSymbolFile("sym.c");
    }

    @Test
    public void testDataTxnFailToHardLinkSymbolKeyFile() throws Exception {
        testFailToLinkSymbolFile("sym.k");
    }

    @Test
    public void testDataTxnFailToHardLinkSymbolOffsetFile() throws Exception {
        testFailToLinkSymbolFile("sym.o");
    }

    @Test
    public void testDataTxnFailToHardLinkSymbolValueFile() throws Exception {
        testFailToLinkSymbolFile("sym.v");
    }

    @Test
    public void testDataTxnFailToRenameWalColumnOnCommit() throws Exception {
        FilesFacade dodgyFf = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.endsWithAscii(from, "wal2" + Files.SEPARATOR + "0" + Files.SEPARATOR + "x.d")) {
                    return -1;
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(dodgyFf, () -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            drainWalQueue();

            AlterOperation alterOp = null;
            try (
                    TableWriterAPI alterWriter = engine.getTableWriterAPI(tableToken, "test");
                    TableWriterAPI insertWriter = engine.getTableWriterAPI(tableToken, "test")
            ) {

                AlterOperationBuilder alterBuilder = new AlterOperationBuilder().ofRenameColumn(1, tableToken, 0);
                alterBuilder.ofRenameColumn("x", "x2");
                alterOp = alterBuilder.build();
                alterWriter.apply(alterOp, true);

                TableWriter.Row row = insertWriter.newRow(MicrosTimestampDriver.floor("2022-02-25"));
                row.putLong(0, 123L);
                row.append();

                try {
                    insertWriter.commit();
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not rename WAL column file");
                }

            } finally {
                Misc.free(alterOp);
            }

            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                Assert.assertEquals(1, walWriter1.getWalId());

                // Assert wal writer 2 is not in the pool after failure to apply structure change
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    Assert.assertEquals(3, walWriter2.getWalId());
                }
            }

            execute("insert into " + tableToken.getTableName() + " values (3, 'ab', '2022-02-25', 'abcd')");
            drainWalQueue();

            assertSql("x2\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n", tableToken.getTableName());
        });
    }

    @Test
    public void testDodgyAddColumDoesNotChangeMetadata() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            try (TableWriterAPI twa = engine.getTableWriterAPI(tableToken, "test")) {
                AtomicInteger counter = new AtomicInteger(2);
                AlterOperation dodgyAlterOp = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        if (counter.decrementAndGet() == 0) {
                            return 0;
                        }
                        svc.addColumn(
                                "new_column",
                                ColumnType.INT,
                                0,
                                false,
                                false,
                                12,
                                false,
                                false,
                                null
                        );
                        return 0;
                    }

                    @Override
                    public boolean isStructural() {
                        return true;
                    }
                };

                try {
                    twa.apply(dodgyAlterOp, true);
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "invalid alter table command [code=0]");
                }
            }

            drainWalQueue();
            execute("insert into " + tableToken.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableToken.getTableName());
        });
    }

    @Test
    public void testDodgyAlterSerializesBrokenStructureChange() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createStandardWalTable(tableName);

            try (TableWriterAPI twa = engine.getTableWriterAPI(tableToken, "test")) {
                AlterOperation dodgyAlterOp = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        svc.addColumn(
                                "new_column",
                                ColumnType.INT,
                                0,
                                false,
                                false,
                                12,
                                false,
                                false,
                                null
                        );
                        return 0;
                    }

                    @Override
                    public boolean isStructural() {
                        return true;
                    }

                    @Override
                    public void serializeBody(MemoryA sink) {
                        // Do nothing, deserialization should fail
                    }
                };

                try {
                    twa.apply(dodgyAlterOp, true);
                    Assert.fail("Dodgy alter application should fail");
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "cannot read alter statement serialized, data is too short to read 10 bytes header");
                }
            }

            drainWalQueue();
            execute("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableName);
        });
    }

    @Test
    public void testDropDesignatedTimestampFails() throws Exception {
        failToApplyAlter("cannot remove designated timestamp column", token -> {
            AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                    .ofDropColumn(1, token, 0);
            alterBuilder.ofDropColumn("ts");

            return alterBuilder.build();
        });
    }

    @Test
    public void testDropPartitionRangeNotOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            execute("insert into " + tableName.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-24T01', 1000000L * 60 * 60 * 6), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(10 * 4)");
            drainWalQueue();
            Path tempPath = Path.getThreadLocal(root).concat(tableName);
            long initialTs = MicrosTimestampDriver.floor("2022-02-24");
            FilesFacade ff = engine.getConfiguration().getFilesFacade();

            int dropPartitions = 5;
            for (int i = 0; i < dropPartitions; i++) {
                long ts = initialTs + i * Micros.DAY_MICROS;
                tempPath.concat(Micros.toString(ts).substring(0, 10)).$();
                Assert.assertTrue(ff.rmdir(tempPath));
                tempPath.of(root).concat(tableName);
            }

            execute("alter table " + tableName.getTableName() + " drop partition WHERE ts <= '"
                    + Micros.toString(initialTs + (dropPartitions - 3) * Micros.DAY_MICROS) + "'");

            drainWalQueue();

            try {
                try (RecordCursorFactory factory = select(tableName.getTableName())) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        cursor.hasNext();
                        Assert.fail();
                    }
                }
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Partition '2022-02-27' does not exist in table '" + tableName.getTableName() + "'");
            }

            execute("alter table " + tableName.getTableName() + " drop partition WHERE ts <= '"
                    + Micros.toString(initialTs + dropPartitions * Micros.DAY_MICROS) + "'");

            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                            "25\tBC\t2022-03-02T01:00:00.000000Z\tFG\n" +
                            "26\tCD\t2022-03-02T07:00:00.000000Z\tEF\n" +
                            "27\tBC\t2022-03-02T13:00:00.000000Z\tDE\n" +
                            "28\tCD\t2022-03-02T19:00:00.000000Z\tFG\n" +
                            "29\tCD\t2022-03-03T01:00:00.000000Z\tDE\n" +
                            "30\tCD\t2022-03-03T07:00:00.000000Z\tDE\n" +
                            "31\tAB\t2022-03-03T13:00:00.000000Z\tEF\n" +
                            "32\tCD\t2022-03-03T19:00:00.000000Z\tEF\n" +
                            "33\tCD\t2022-03-04T01:00:00.000000Z\tDE\n" +
                            "34\tCD\t2022-03-04T07:00:00.000000Z\tEF\n" +
                            "35\tCD\t2022-03-04T13:00:00.000000Z\tFG\n" +
                            "36\tBC\t2022-03-04T19:00:00.000000Z\tDE\n" +
                            "37\tCD\t2022-03-05T01:00:00.000000Z\tFG\n" +
                            "38\tAB\t2022-03-05T07:00:00.000000Z\t\n" +
                            "39\tBC\t2022-03-05T13:00:00.000000Z\tFG\n" +
                            "40\tBC\t2022-03-05T19:00:00.000000Z\tFG\n",
                    tableName.getTableName()
            );

            assertSql("min\tmax\n" +
                            "2022-03-02T01:00:00.000000Z\t2022-03-05T19:00:00.000000Z\n",
                    "select min(ts), max(ts) from " + tableName.getTableName()
            );
        });
    }

    @Test
    public void testErrorReadingWalEFileSuspendTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();

            TableToken tableToken = createStandardWalTable(tableName);

            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    final String eventIndexName = SEPARATOR + tableToken.getDirName() +
                            SEPARATOR + "wal1" +
                            SEPARATOR + "0" +
                            SEPARATOR + "_event.i";
                    if (Utf8s.endsWithAscii(name, eventIndexName)) {
                        return -1;
                    }

                    return super.openRO(name);
                }
            };

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
        });
    }

    @Test
    public void testFailToRollUncommittedToNewWalSegmentDesignatedTimestamp() throws Exception {
        String failToRollFile = "ts.d";
        failToCopyDataToFile(failToRollFile);
    }

    @Test
    public void testFailToRollUncommittedToNewWalSegmentFixedColumn() throws Exception {
        String failToRollFile = "sym.d";
        failToCopyDataToFile(failToRollFile);
    }

    @Test
    public void testFailToRollUncommittedToNewWalSegmentVarLenDataFile() throws Exception {
        String failToRollFile = "str.d";
        failToCopyDataToFile(failToRollFile);
    }

    @Test
    public void testForceDropPartitionFailsAndRolledBack() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            execute("insert into " + tableToken.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-24T01', 1000000L * 60 * 60 * 6), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(10 * 4)");

            drainWalQueue();

            assertSql("count\tmin\tmax\n" +
                            "33\t2022-02-24T00:00:00.000000Z\t2022-03-05T19:00:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableToken.getTableName() +
                            " where ts not in '2022-03-04' and ts not in '2022-03-02'"
            );
            // Evict reader to simulate reader that is not able to read partition
            engine.releaseInactive();

            // Remove one but last partition from the disk
            Path tempPath = Path.getThreadLocal(root).concat(tableToken).concat("2022-03-04");
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            Assert.assertTrue(ff.rmdir(tempPath));

            try {
                // This should execute immediately, drop partition before the last one
                execute("alter table " + tableToken.getTableName() + " force drop partition list '2022-03-05'");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open, file does not exist:");
                TestUtils.assertContains(e.getFlyweightMessage(), "2022-03-04");
            }

            try {
                assertSql("count\tmin\tmax\n" +
                                "20\t2022-03-01T01:00:00.000000Z\t2022-03-05T19:00:00.000000Z\n",
                        "select count(), min(ts), max(ts) from " + tableToken.getTableName()
                );
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Partition '2022-03-04' does not exist in table");
            }

            // Previous command to delete 2022-03-05 is not executed because writer rolled back correctly
            execute("alter table " + tableToken.getTableName() + " force drop partition list '2022-03-02'");
            tempPath = Path.getThreadLocal(root).concat(tableToken).concat("2022-03-05");
            Assert.assertTrue(ff.exists(tempPath.$()));

            // Force delete partition that is not on disk to unblock reading
            execute("alter table " + tableToken.getTableName() + " force drop partition list '2022-03-04'");

            assertSql("count\tmin\tmax\n" +
                            "33\t2022-02-24T00:00:00.000000Z\t2022-03-05T19:00:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableToken.getTableName()
            );

            // Check writer is healthy and can insert records to the last partition
            execute("insert into " + tableToken.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-03-05', 1000000L * 60 * 60 * 6), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(10 * 4)");
            drainWalQueue();
            assertSql("count\tmin\tmax\n" +
                            "73\t2022-02-24T00:00:00.000000Z\t2022-03-14T18:00:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableToken.getTableName() + " where ts not in '2022-03-04'"
            );
        });
    }

    @Test
    public void testForceDropPartitionRangeNotOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            execute("insert into " + tableName.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-24T01', 1000000L * 60 * 60 * 6), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(10 * 4)");

            drainWalQueue();

            Path tempPath = Path.getThreadLocal(root).concat(tableName);
            long initialTs = MicrosTimestampDriver.floor("2022-02-24");
            FilesFacade ff = engine.getConfiguration().getFilesFacade();

            int dropPartitions = 5;
            for (int i = 0; i < dropPartitions; i++) {
                long ts = initialTs + i * Micros.DAY_MICROS;
                tempPath.concat(Micros.toString(ts).substring(0, 10)).$();
                Assert.assertTrue(ff.rmdir(tempPath));
                tempPath.of(root).concat(tableName);
            }

            // This should execute immediately
            execute("alter table " + tableName.getTableName() + " force drop partition list '2022-02-24', '2022-02-25', '2022-02-26', '2022-02-27', '2022-02-28'");

            assertSql("count\tmin\tmax\n" +
                            "20\t2022-03-01T01:00:00.000000Z\t2022-03-05T19:00:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableName.getTableName()
            );
        });
    }

    @Test
    public void testForceDropPartitionRangeNotOnDiskWithSplits() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            execute("insert into " + tableName.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-24', 1000000L * 60), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(60 * 24 * 3 - 1)");

            drainWalQueue();

            execute("insert into " + tableName.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-26T19', 1000000L * 60), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(60)");

            drainWalQueue();

            execute("insert into " + tableName.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-26T16', 1000000L * 60), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(60)");

            drainWalQueue();

            Path tempPath = Path.getThreadLocal(root).concat(tableName).concat("2022-02-26T155900-000001.2");
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            Assert.assertTrue(ff.rmdir(tempPath));

            // This should execute immediately
            execute("alter table " + tableName.getTableName() + " force drop partition list '2022-02-26T155900-000001'");

            assertSql("count\tmin\tmax\n" +
                            "4200\t2022-02-24T00:00:00.000000Z\t2022-02-26T23:58:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableName.getTableName()
            );

            // Drop last partition
            execute("alter table " + tableName.getTableName() + " force drop partition list '2022-02-26T185900-000001', '2022-02-26'");

            assertSql("count\tmin\tmax\n" +
                            "2881\t2022-02-24T00:00:00.000000Z\t2022-02-25T23:59:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableName.getTableName()
            );

            // Insert more data
            execute("insert into " + tableName.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-26T16', 1000000L * 60), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(60)");

            drainWalQueue();

            // Drop all partitions
            execute("alter table " + tableName.getTableName() + " force drop partition list '2022-02-25', '2022-02-24', '2022-02-26'");
            assertSql("count\tmin\tmax\n" +
                            "0\t\t\n",
                    "select count(), min(ts), max(ts) from " + tableName.getTableName()
            );

            // Insert more data
            execute("insert into " + tableName.getTableName() + " " +
                    "select x, rnd_symbol('AB', 'BC', 'CD'), timestamp_sequence('2022-02-26T16', 1000000L * 60), rnd_symbol('DE', null, 'EF', 'FG') " +
                    "from long_sequence(60 * 24)");

            drainWalQueue();

            assertSql("count\tmin\tmax\n" +
                            "1440\t2022-02-26T16:00:00.000000Z\t2022-02-27T15:59:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableName.getTableName()
            );

            // Force drop first partition, but keep last
            if (Os.isWindows()) {
                engine.releaseInactive();
            }
            tempPath = Path.getThreadLocal(root).concat(tableName).concat("2022-02-26.6");
            Assert.assertTrue(ff.exists(tempPath.$()));
            execute("alter table " + tableName.getTableName() + " force drop partition list '2022-02-26'");

            assertSql("count\tmin\tmax\n" +
                            "960\t2022-02-27T00:00:00.000000Z\t2022-02-27T15:59:00.000000Z\n",
                    "select count(), min(ts), max(ts) from " + tableName.getTableName()
            );

            // Check that partitions are removed from disk, there are no open readers, directory removal should happen at the end
            // of alter table operation
            tempPath = Path.getThreadLocal(root).concat(tableName).concat("2022-02-26.6");
            Assert.assertFalse(ff.exists(tempPath.$()));
        });
    }

    @Test
    public void testInvalidNonStructureAlter() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            try (TableWriterAPI twa = engine.getTableWriterAPI(tableToken, "test")) {
                AlterOperation dodgyAlterOp = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        return 0;
                    }

                    @Override
                    public boolean isStructural() {
                        return false;
                    }
                };

                try {
                    twa.apply(dodgyAlterOp, true);
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "failed to commit ALTER SQL to WAL, sql context is empty ");
                }
            }

            drainWalQueue();
            execute("insert into " + tableToken.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableToken.getTableName());
        });
    }

    @Test
    public void testInvalidNonStructureChangeMakeWalWriterDistressed() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            try (WalWriter walWriter = engine.getWalWriter(tableName)) {
                Assert.assertEquals(1, walWriter.getWalId());

                AlterOperation dodgyAlterOp = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        return 0;
                    }

                    @Override
                    public SqlExecutionContext getSqlExecutionContext() {
                        return sqlExecutionContext;
                    }

                    @Override
                    public CharSequence getSqlText() {
                        throw new IndexOutOfBoundsException();
                    }

                    @Override
                    public int getTableId() {
                        return 1;
                    }

                    @Override
                    public boolean isStructural() {
                        return false;
                    }
                };

                try {
                    walWriter.apply(dodgyAlterOp, true);
                    Assert.fail("Expected exception is missing");
                } catch (IndexOutOfBoundsException ex) {
                    //expected
                }
            }

            try (WalWriter walWriter = engine.getWalWriter(tableName)) {
                // Wal Writer 1 is not pooled
                Assert.assertEquals(2, walWriter.getWalId());
            }

            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableName.getTableName());
        });
    }

    @Test
    public void testMainAddDuplicateColumnSequentiallyFailsWithSqlException() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            execute("alter table " + tableToken.getTableName() + " add column new_column int");

            try {
                execute("alter table " + tableToken.getTableName() + " add column new_column int");
            } catch (SqlException ex) {
                // Critical error
                TestUtils.assertContains(ex.getFlyweightMessage(), "column 'new_column' already exists");
            }
        });
    }

    @Test
    public void testMainAndWalTableAddColumnFailed() throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);

        FilesFacade ffOverride = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, "new_column.d") && fail.get()) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ffOverride, () -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            String tableName = tableToken.getTableName();

            execute("alter table " + tableName + " add column new_column int");

            try {
                execute("insert into " + tableName +
                        " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
                Assert.fail();
            } catch (CairoException ex) {
                // Critical error
                Assert.assertTrue(ex.getErrno() >= 0);
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write");
            }

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tnull\n", tableName);

            fail.set(false);
            execute("insert into " + tableName +
                    " values (102, 'dfd', '2022-02-24T01', 'asd', 123)");
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tnull\n" +
                    "102\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t123\n", tableName);
        });
    }

    @Test
    public void testMainTableAddColumnFailed() throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);

        FilesFacade ffOverride = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, "new_column.d.1") && fail.get()) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ffOverride, () -> {
            String tableName = testName.getMethodName();
            createStandardWalTable(tableName);

            execute("alter table " + tableName + " add column new_column int");

            execute("insert into " + tableName + " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableName);

            fail.set(false);

            execute("insert into " + tableName + " values (102, 'dfd', '2022-02-24T01', 'asd', 123)");
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableName);
        });
    }

    @Test
    public void testMissingWalSegmentColumnFile() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            execute("insert into " + tableToken.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

            engine.releaseInactive();
            Path path = Path.getThreadLocal(root).concat(tableToken).concat("wal1").concat("0").concat("x.d");
            Assert.assertTrue(configuration.getFilesFacade().removeQuiet(path.$()));

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            execute("insert into " + tableToken.getTableName() + " values (1, 'ac', '2022-02-24T23', 'ef')");
            execute("ALTER TABLE " + tableToken.getTableName() + " RESUME WAL FROM TXN 3");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tac\t2022-02-24T23:00:00.000000Z\tef\n", tableToken.getTableName());
        });
    }

    @Test
    public void testNonWalTableTransactionNotificationIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken ignored = new TableToken(tableName, tableName, null, 123, false, false, false);
            createStandardWalTable(tableName);

            drainWalQueue();
            engine.notifyWalTxnCommitted(ignored);

            execute("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");
            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableName);
        });
    }

    @Test
    public void testRecompileUpdateWithOutOfDateStructure() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();

            try (WalWriter writer = engine.getWalWriter(tableName)) {
                writer.apply(new UpdateOperation(tableName, 1, 22, 1) {
                    @Override
                    public SqlExecutionContext getSqlExecutionContext() {
                        return sqlExecutionContext;
                    }
                });
                Assert.fail();
            } catch (TableReferenceOutOfDateException ignore) {
            }

            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");
            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableName.getTableName());
        });
    }

    @Test
    public void testRenameColumnDoesNotExist() throws Exception {
        failToApplyDoubleAlter(
                tableToken -> {
                    AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                            .ofRenameColumn(1, tableToken, 0);
                    alterBuilder.ofRenameColumn("x", "x2");
                    return alterBuilder.build();
                }
        );
    }

    @Test
    public void testRenameDesignatedTimestampFails() throws Exception {
        failToApplyAlter(
                "cannot rename designated timestamp column",
                tableToken -> {
                    AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                            .ofRenameColumn(1, tableToken, 0);
                    alterBuilder.ofRenameColumn("ts", "ts2");
                    return alterBuilder.build();
                }
        );
    }

    @Test
    public void testRenameToExistingNameFails() throws Exception {
        failToApplyAlter(
                "cannot rename, column with the name already exists",
                tableToken -> {
                    AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                            .ofRenameColumn(1, tableToken, 0);
                    alterBuilder.ofRenameColumn("x", "sym");
                    return alterBuilder.build();
                }
        );
    }

    @Test
    public void testRenameToInvalidColumnNameFails() throws Exception {
        failToApplyAlter(
                "invalid column name",
                tableToken -> {
                    AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                            .ofRenameColumn(1, tableToken, 0);
                    alterBuilder.ofRenameColumn("x", "/../tb");
                    return alterBuilder.build();
                }
        );
    }

    @Test
    public void testRollbackSupport() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            int counter = 0;

            @Override
            public long openRW(LPSZ name, int mode) {
                if (Utf8s.containsAscii(name, "b.d.") && counter++ == 0) {
                    return -1;
                }
                return super.openRW(name, mode);
            }
        };
        assertMemoryLeak(ff, () -> {

            execute("create table tab (b boolean, ts timestamp, sym symbol) timestamp(ts) partition by DAY WAL");
            TableToken tt = engine.verifyTableName("tab");

            execute("insert into tab select true, (1)::timestamp, null from long_sequence(1)");
            execute("insert into tab select true, (2)::timestamp, null from long_sequence(1)");
            execute("insert into tab select true, (3)::timestamp, null from long_sequence(1)");
            execute("insert into tab select true, (4)::timestamp, null from long_sequence(1)");
            update("update tab set b=false");
            execute("insert into tab select true, (5)::timestamp, null from long_sequence(1)");
            drainWalQueue();

            assertEventually(() -> Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt)));

            execute("alter table tab resume wal");
            execute("insert into tab select true, (6)::timestamp, null from long_sequence(1)");
            drainWalQueue();

            assertSql(
                    "b\tts\tsym\n" +
                            "false\t1970-01-01T00:00:00.000001Z\t\n" +
                            "false\t1970-01-01T00:00:00.000002Z\t\n" +
                            "false\t1970-01-01T00:00:00.000003Z\t\n" +
                            "false\t1970-01-01T00:00:00.000004Z\t\n" +
                            "true\t1970-01-01T00:00:00.000005Z\t\n" +
                            "true\t1970-01-01T00:00:00.000006Z\t\n",
                    "tab"
            );
        });
    }

    @Test
    public void testSuspendNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            final String nonWalTable = "tab1";
            createStandardNonWalTable(nonWalTable);

            assertAlterTableTypeFail("alter table " + nonWalTable + " suspend wal", nonWalTable + " is not a WAL table");
        });
    }

    @Test
    public void testSuspendWal() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tableToken = createStandardWalTable(testName.getMethodName());

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            tableToken.getTableName() + "\tfalse\t1\t0\t1\t\t\t0\n",
                    "wal_tables()"
            );

            execute("alter table " + tableToken.getTableName() + " suspend wal");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            tableToken.getTableName() + "\ttrue\t1\t0\t1\t\t\t0\n",
                    "wal_tables()"
            );

            execute("update " + tableToken.getTableName() + " set x = 1111;");
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            execute("alter table " + tableToken.getTableName() + " suspend wal with "
                    + (Os.isWindows() ? 112 : 28) + ", 'test error message'");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            tableToken.getTableName() + "\ttrue\t1\t0\t2\tDISK FULL\ttest error message\t0\n",
                    "wal_tables()"
            );

            execute("alter table " + tableToken.getTableName() + " resume wal;");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            tableToken.getTableName() + "\tfalse\t1\t0\t2\t\t\t0\n",
                    "wal_tables()"
            );

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n1111\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());
        });
    }

    @Test
    public void testSuspendWalFailure() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tableToken = createStandardWalTable(testName.getMethodName());

            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspen wal", SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR);
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wall", "'wal' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal witj", "'with' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with", "error code/tag expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 24a", "invalid value [value=24a]");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with '24a'", "invalid value [value=24a]");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 24", "',' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 'DISK FULL'", "',' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 24 'test error'", "',' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 'DISK FULL' 'test error'", "',' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 24,", "error message expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 'DISK FULL',", "error message expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 24, 'test error' hoppa", "unexpected token [token=hoppa]");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 'DISK FULL', 'test error' hoppa", "unexpected token [token=hoppa]");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 24, test error", "unexpected token [token=error]");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " suspend wal with 'DISK FULL', test error", "unexpected token [token=error]");
        });
    }

    @Test
    public void testTableWriterDirectAddColumnStopsWal() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();
            try (TableWriter writer = getWriter(tableName)) {
                writer.addColumn("abcd", ColumnType.INT);
            }

            execute("alter table " + tableName.getTableName() + " add column dddd2 long");
            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-25', 'abcd', 123)");
            drainWalQueue();

            // No SQL applied
            assertSql("x\tsym\tts\tsym2\tabcd\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tnull\n", tableName.getTableName());
        });
    }

    @Test
    public void testTableWriterDirectDropColumnStopsWal() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();
            try (TableWriter writer = getWriter(tableName)) {
                writer.removeColumn("x");
                writer.removeColumn("sym");
            }

            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-25', 'abcde')");
            execute("insert into " + tableName.getTableName() + " values (2, 'ab', '2022-02-25', 'abcdr')");
            // inserts do not check structure version
            // it fails only when structure is changing through the WAL
            execute("alter table " + tableName.getTableName() + " add column dddd2 long");
            execute("insert into " + tableName.getTableName() + " values (3, 'ab', '2022-02-25', 'abcdt', 123L)");

            drainWalQueue();
            assertSql("ts\tsym2\n" +
                    "2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2022-02-25T00:00:00.000000Z\tabcde\n" +
                    "2022-02-25T00:00:00.000000Z\tabcdr\n", tableName.getTableName());
        });
    }

    @Test
    public void testWalApplyMetrics() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };
        assertMemoryLeak(filesFacade, () -> {
            assertWalApplyMetrics(0, 0, 0);

            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            assertWalApplyMetrics(0, 1, 0);

            execute("update " + tableToken.getTableName() + " set x = 11;");
            execute("update " + tableToken.getTableName() + " set x = 111;");
            execute("update " + tableToken.getTableName() + " set x = 1111;");
            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertWalApplyMetrics(1, 4, 1);

            execute("alter table " + tableToken.getTableName() + " resume wal;");

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            drainWalQueue();
            assertWalApplyMetrics(0, 4, 4);
        });
    }

    @Test
    public void testWalMultipleColumnConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table abc (x0 symbol, x string, y string, y1 symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into abc values('aa', 'a', 'b', 'bb', '2022-02-24T01')");
            drainWalQueue();

            execute("alter table abc add column new_col SYMBOL INDEX");
            execute("update abc set new_col = x");
            execute("alter table abc drop column x");
            execute("alter table abc rename column new_col to x");

            execute("alter table abc add column new_col SYMBOL INDEX");
            execute("update abc set new_col = y");
            execute("alter table abc drop column y");
            execute("alter table abc rename column new_col to y");

            drainWalQueue();

            assertSql("x0\ty1\tts\tx\ty\n" +
                    "aa\tbb\t2022-02-24T01:00:00.000000Z\ta\tb\n", "abc");
        });
    }

    @Test
    public void testWalTableAddColumnFailedNoDiskSpaceShouldSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " ADD COLUMN sym5 SYMBOL CAPACITY 1024";
        runCheckTableSuspended(tableName, query, new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "sym5.c")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    @Test
    public void testWalTableAttachFailedDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " attach partition list '2022-02-25'";
        assertMemoryLeak(() -> runCheckTableNonSuspended(tableName, query));
    }

    @Test
    public void testWalTableCannotOpenSeqTxnFileToCheckTransactions() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {

            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, META_FILE_NAME)) {
                    fd = super.openRO(name);
                    return fd;
                }
                return super.openRO(name);
            }

            @Override
            public int readNonNegativeInt(long fd, long offset) {
                if (fd == this.fd) {
                    return -1;
                }
                return super.readNonNegativeInt(fd, offset);
            }
        };

        assertMemoryLeak(ff, () -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();

            engine.getTableSequencerAPI().releaseInactive();
            final CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
            checkWalTransactionsJob.run(0);

            execute("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");
            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n", tableName.getTableName());
        });
    }

    @Test
    public void testWalTableDropNonExistingIndexDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " ALTER COLUMN sym DROP INDEX";
        assertMemoryLeak(() -> runCheckTableNonSuspended(tableName, query));
    }

    @Test
    public void testWalTableDropPartitionFailedDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " drop partition list '2022-02-25'";
        assertMemoryLeak(() -> runCheckTableNonSuspended(tableName, query));
    }

    @Test
    public void testWalTableDropPartitionFailedDoesNotSuspendTable3() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " drop partition list '2022'";
        assertMemoryLeak(() -> {
            try {
                runCheckTableNonSuspended(tableName, query);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'yyyy-MM-dd' expected, found [ts=2022]");
            }
        });
    }

    @Test
    public void testWalTableEmptyUpdateDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "update " + tableName + " set x = 1 where x < 0";
        assertMemoryLeak(() -> runCheckTableNonSuspended(tableName, query));
    }

    @Test
    public void testWalTableIndexCachedFailedDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " alter column sym NOCACHE";
        assertMemoryLeak(() -> runCheckTableNonSuspended(tableName, query));
    }

    @Test
    public void testWalTableMultiColumnAddNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            execute("insert into " + tableToken.getTableName() +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");

            try {
                execute("alter table " + tableToken.getTableName() + " add column jjj int, column2 long");
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(), "statement is either no-op," +
                                " or contains multiple transactions, such as 'alter table add column col1, col2'," +
                                " and currently not supported for WAL tables" +
                                " [table=testWalTableMultiColumnAddNotSupported, oldStructureVersion=0, newStructureVersion=2]"
                );
            }

            execute("insert into " + tableToken.getTableName() +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n", tableToken.getTableName());

        });
    }

    @Test
    public void testWalTableResumeContinuesAfterEject() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "x.d.") && attempt++ < 2) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);

            //1
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            //2 fail
            execute("update " + tableToken.getTableName() + " set x = 1111");
            //3
            execute("insert into " + tableToken.getTableName() + "(x, sym, sym2, ts) values (1, 'AB', 'EF', '2022-02-24T01')");
            //4
            execute("insert into " + tableToken.getTableName() + "(x, sym, sym2, ts) values (2, 'AB', 'EF', '2022-02-24T02')");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            engine.getTableSequencerAPI().releaseAll();

            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            engine.getTableSequencerAPI().releaseAll();
            execute("alter table " + tableToken.getTableName() + " resume wal");

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1111\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tAB\t2022-02-24T01:00:00.000000Z\tEF\n" +
                    "2\tAB\t2022-02-24T02:00:00.000000Z\tEF\n", tableToken.getTableName());
            assertSql("name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                    tableToken.getTableName() + "\tfalse\t4\t0\t4\t\t\t0\n", "wal_tables()");
        });
    }

    @Test
    public void testWalTableSuspendResume() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            execute("update " + tableToken.getTableName() + " set x = 1111;");
            execute("update " + tableToken.getTableName() + " set sym = 'XXX';");
            execute("update " + tableToken.getTableName() + " set sym2 = 'YYY';");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            execute("alter table " + tableToken.getTableName() + " resume wal;");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n1111\tXXX\t2022-02-24T00:00:00.000000Z\tYYY\n", tableToken.getTableName());
        });
    }

    @Test
    public void testWalTableSuspendResumeFromTxn() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            //1
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            //2 fail
            execute("update " + tableToken.getTableName() + " set x = 1111");
            //3
            execute("update " + tableToken.getTableName() + " set sym = 'XXX'");
            //4
            execute("update " + tableToken.getTableName() + " set sym2 = 'YYY'");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            try {
                execute("alter table " + tableToken.getTableName() + " resume wal from transaction 999;"); // fails
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getMessage(), "[-1] resume txn is higher than next available transaction [resumeFromTxn=999, nextTxn=5]");
            }

            execute("alter table " + tableToken.getTableName() + " resume wal from txn 3;");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            engine.releaseInactive(); // release writer from the pool
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n1\tXXX\t2022-02-24T00:00:00.000000Z\tYYY\n", tableToken.getTableName());
        });
    }

    @Test
    public void testWalTableSuspendResumeSql() throws Exception {
        assertMemoryLeak(ff, () -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            String nonWalTable = "W";
            createStandardNonWalTable(nonWalTable);

            assertAlterTableTypeFail("alter table " + nonWalTable + " resume wal", nonWalTable + " is not a WAL table");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resum wal", SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR);
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wall", "'wal' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal frol", "'from' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from", "'transaction' or 'txn' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from tx", "'transaction' or 'txn' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from txn", "transaction value expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from txn -10", "invalid value [value=-]");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from txn 10AA", "invalid value [value=10AA]");

            engine.getTableSequencerAPI().suspendTable(tableToken, NONE, "wal apply error");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertAlterTableTypeFail(
                    "alter table " + tableToken.getTableName() + "ererer resume wal from txn 2",
                    "table does not exist [table=" + tableToken.getTableName() + "ererer]"
            );
        });
    }

    @Test
    public void testWalTableSuspendResumeStatusTable() throws Exception {
        testWalTableSuspendResumeStatusTable("1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", "table1", 999, NONE.text());
        if (Os.isWindows()) {
            testWalTableSuspendResumeStatusTable("1\tBC\t2022-02-24T00:00:00.000000Z\tFG\n", "table3", 39, DISK_FULL.text());
            testWalTableSuspendResumeStatusTable("1\tCD\t2022-02-24T00:00:00.000000Z\tFG\n", "table4", 112, DISK_FULL.text());
            testWalTableSuspendResumeStatusTable("1\tCD\t2022-02-24T00:00:00.000000Z\tFG\n", "table5", 4, TOO_MANY_OPEN_FILES.text());
            testWalTableSuspendResumeStatusTable("1\tAB\t2022-02-24T00:00:00.000000Z\tDE\n", "table6", 8, OUT_OF_MMAP_AREAS.text());
        } else {
            testWalTableSuspendResumeStatusTable("1\tBC\t2022-02-24T00:00:00.000000Z\tFG\n", "table3", 28, DISK_FULL.text());
            testWalTableSuspendResumeStatusTable("1\tCD\t2022-02-24T00:00:00.000000Z\tFG\n", "table4", 24, TOO_MANY_OPEN_FILES.text());
            testWalTableSuspendResumeStatusTable("1\tCD\t2022-02-24T00:00:00.000000Z\tFG\n", "table5", 12, OUT_OF_MMAP_AREAS.text());
        }
    }

    @Test
    public void testWalUpdateFailedCompilationSuspendsTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "update " + tableName + " set x = 1111";
        Overrides overrides = node1.getConfigurationOverrides();
        overrides.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
        spinLockTimeout = 1;
        runCheckTableSuspended(tableName, query, new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.containsAscii(name, "_meta") && attempt++ >= 2) {
                    if (!engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName))) {
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        });
    }

    @Test
    public void testWalUpdateFailedSuspendsTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "update " + tableName + " set x = 1111";
        runCheckTableSuspended(tableName, query, new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    private static void assertAlterTableTypeFail(String alterStmt, String expected) {
        try {
            execute(alterStmt);
            Assert.fail("expected SQLException is not thrown");
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), expected);
        }
    }

    private void assertWalApplyMetrics(int suspendedTables, int seqTxn, int writerTxn) {
        String tagSuspendedTables = "questdb_suspended_tables";
        String tagSeqTxn = "questdb_wal_apply_seq_txn";
        String tagWriterTxn = "questdb_wal_apply_writer_txn";
        Assert.assertEquals(tagSuspendedTables, suspendedTables, TestUtils.getMetricValue(engine, tagSuspendedTables));
        Assert.assertEquals(tagSeqTxn, seqTxn, TestUtils.getMetricValue(engine, tagSeqTxn));
        Assert.assertEquals(tagWriterTxn, writerTxn, TestUtils.getMetricValue(engine, tagWriterTxn));
    }

    private void createStandardNonWalTable(String tableName) throws SqlException {
        createStandardTable(tableName, false);
    }

    private TableToken createStandardTable(String tableName, boolean isWal) throws SqlException {
        execute("create table " + tableName + " as (" +
                "select x, " +
                " rnd_symbol('AB', 'BC', 'CD') sym, " +
                " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                " from long_sequence(1)" +
                ") timestamp(ts) partition by DAY " + (isWal ? "" : "BYPASS ") + "WAL");
        return engine.verifyTableName(tableName);
    }

    private TableToken createStandardWalTable(String tableName) throws SqlException {
        return createStandardTable(tableName, true);
    }

    private void failToApplyAlter(String error, Function<TableToken, AlterOperation> alterOperationFunc) throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createStandardWalTable(tableName);
            drainWalQueue();

            AlterOperation alterOperation = null;
            try (TableWriterAPI alterWriter2 = engine.getTableWriterAPI(tableToken, "test")) {
                try {
                    alterOperation = alterOperationFunc.apply(tableToken);
                    alterWriter2.apply(alterOperation, true);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), error);
                }
            } finally {
                Misc.free(alterOperation);
            }
        });
    }

    private void failToApplyDoubleAlter(Function<TableToken, AlterOperation> alterOperationFunc) throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createStandardWalTable(tableName);
            drainWalQueue();

            AlterOperation alterOperation = null;
            try (TableWriterAPI alterWriter1 = engine.getTableWriterAPI(tableToken, "test");
                 TableWriterAPI alterWriter2 = engine.getTableWriterAPI(tableToken, "test")) {

                alterOperation = alterOperationFunc.apply(tableToken);
                alterWriter1.apply(alterOperation, true);
                try {
                    alterWriter2.apply(alterOperation, true);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot rename, column does not exist");
                }
            } finally {
                Misc.free(alterOperation);
            }
        });
    }

    private void failToCopyDataToFile(String failToRollFile) throws Exception {
        FilesFacade dodgyFf = new TestFilesFacadeImpl() {

            @Override
            public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
                if (destFd == fd) {
                    return -1;
                }
                return super.copyData(srcFd, destFd, offsetSrc, length);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, "1" + Files.SEPARATOR + failToRollFile)) {
                    fd = super.openRW(name, opts);
                    return fd;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(dodgyFf, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.commit();

                    insertMethod.execute(sqlExecutionContext);
                    execute("alter table " + tableName + " add column new_column int");

                    try {
                        insertMethod.commit();
                    } catch (CairoException e) {
                        // todo: check all assertContains() usages
                        TestUtils.assertContains(e.getFlyweightMessage(), "failed to copy column file to new segment");
                    }
                }
            }

            execute("insert into " + tableName + " values (103, 'dfd', 'str-2', '2022-02-24T02', 'asdd', 1234)");

            drainWalQueue();
            assertSql("x\tsym\tstr\tts\tsym2\tnew_column\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\n" +
                    "103\tdfd\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\t1234\n", tableName);
        });
    }

    private void runCheckTableNonSuspended(String tableName, String query) throws Exception {
        createStandardWalTable(tableName);

        // Drop partition which does not exist
        execute(query);

        // Table should not be suspended
        execute("insert into " + tableName +
                " values (101, 'dfd', '2022-02-25T01', 'asd')");

        drainWalQueue();

        assertSql("x\tsym\tts\tsym2\n" +
                "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                "101\tdfd\t2022-02-25T01:00:00.000000Z\tasd\n", tableName);
    }

    private void runCheckTableSuspended(String tableName, String query, FilesFacade ff) throws Exception {
        assertMemoryLeak(ff, () -> {
            createStandardWalTable(tableName);

            // Drop partition which does not exist
            execute(query);

            // Table should be suspended
            execute("update " + tableName + " set x = 1111");

            // minimize time spent opening metadata that cannot be opened
            spinLockTimeout = 100;
            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));

            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableName);

        });
    }

    private void testFailToLinkSymbolFile(String fileName) throws Exception {
        FilesFacade dodgyFf = new TestFilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                if (Utf8s.endsWithAscii(src, Files.SEPARATOR + fileName)) {
                    return -1;
                }
                return Files.hardLink(src, hardLink);
            }
        };

        assertMemoryLeak(dodgyFf, () -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();

            try (WalWriter ignore = engine.getWalWriter(tableName)) {
                execute("insert into " + tableName.getTableName() + " values (3, 'ab', '2022-02-25', 'abcd')");
                try (WalWriter insertedWriter = engine.getWalWriter(tableName)) {
                    try (Path path = new Path()) {
                        String columnName = "sym";
                        path.of(engine.getConfiguration().getDbRoot()).concat(tableName).put(Files.SEPARATOR).put(WAL_NAME_BASE).put(insertedWriter.getWalId());
                        int trimTo = path.size();

                        if (Os.type != Os.WINDOWS) {
                            // TODO: find out why files remain on Windows. They are not opened by anything
                            Assert.assertFalse(ff.exists(TableUtils.charFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE)));
                            Assert.assertFalse(ff.exists(TableUtils.offsetFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE)));
                            Assert.assertFalse(ff.exists(BitmapIndexUtils.keyFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE)));
                            Assert.assertFalse(ff.exists(BitmapIndexUtils.valueFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE)));
                        }
                    }

                }
            }

            execute("insert into " + tableName.getTableName() + " values (3, 'ab', '2022-02-25', 'abcd')");
            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n", tableName.getTableName());
        });
    }

    private void testWalTableSuspendResumeStatusTable(String startState, String tableName, int errorCode, String expectedTag) throws Exception {
        final FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public int errno() {
                return errorCode;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            //1
            final TableToken tableToken = createStandardWalTable(tableName);
            //2 fail
            execute("update " + tableToken.getTableName() + " set x = 1111");
            //3
            execute("update " + tableToken.getTableName() + " set sym = 'XXX'");
            //4
            execute("update " + tableToken.getTableName() + " set sym2 = 'YYY'");

            drainWalQueue();

            final String errorMessage = "could not open read-write [file=" + root + SEPARATOR +
                    tableToken.getDirName() + SEPARATOR + "2022-02-24" + SEPARATOR + "x.d.1]";

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertSql("x\tsym\tts\tsym2\n" + startState, tableToken.getTableName());

            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            tableToken.getTableName() + "\ttrue\t1\t0\t4\t" + expectedTag +
                            "\t" + errorMessage + "\t0\n",
                    "wal_tables()"
            );

            execute("alter table " + tableToken.getTableName() + " resume wal");
            execute("alter table " + tableToken.getTableName() + " resume wal from transaction 0"); // ignored
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

            engine.releaseInactive(); // release writer from the pool
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n1111\tXXX\t2022-02-24T00:00:00.000000Z\tYYY\n", tableToken.getTableName());
            assertSql("name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                    tableToken.getTableName() + "\tfalse\t4\t0\t4\t\t\t0\n", "wal_tables()");

            execute("drop table " + tableToken.getTableName());
        });
    }
}
