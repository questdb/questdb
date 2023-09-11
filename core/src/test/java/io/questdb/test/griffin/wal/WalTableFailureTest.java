/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.EVENT_INDEX_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

public class WalTableFailureTest extends AbstractCairoTest {
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
                        svc.addColumn("new_column", ColumnType.INT, 0, false, false, 12, false);
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
            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

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
                        svc.addColumn("new_column", ColumnType.INT, 0, false, false, 12, false);
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
            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

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
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " set", "'param' or 'type' expected");
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
            public int openRW(LPSZ name, long mode) {
                if (Chars.endsWith(name, "2022-02-25" + Files.SEPARATOR + "x.d.1") && counter++ < 2) {
                    return -1;
                }
                return super.openRW(name, mode);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();
            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-25', 'ef')");

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

                TableWriter.Row row = insertWriter.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-25"));
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
                if (Chars.endsWith(from, "wal2" + Files.SEPARATOR + "0" + Files.SEPARATOR + "x.d")) {
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

                TableWriter.Row row = insertWriter.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-25"));
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

            compile("insert into " + tableToken.getTableName() + " values (3, 'ab', '2022-02-25', 'abcd')");
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
                        svc.addColumn("new_column", ColumnType.INT, 0, false, false, 12, false);
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
            compile("insert into " + tableToken.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

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
                        svc.addColumn("new_column", ColumnType.INT, 0, false, false, 12, false);
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
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");

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
    public void testErrorReadingWalEFileSuspendTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();

            TableToken tableToken = createStandardWalTable(tableName);

            FilesFacade ff = configuration.getFilesFacade();
            int waldFd = TableUtils.openRW(
                    ff,
                    Path.getThreadLocal(root).concat(tableToken).concat(WAL_NAME_BASE).put(1).concat("0").concat(EVENT_INDEX_FILE_NAME).$(),
                    LOG,
                    configuration.getWriterFileOpenOpts()
            );
            Files.truncate(waldFd, 0);
            ff.close(waldFd);

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
            compile("insert into " + tableToken.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

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

            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");

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

            compile("alter table " + tableToken.getTableName() + " add column new_column int");

            try {
                compile("alter table " + tableToken.getTableName() + " add column new_column int");
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
            public int openRW(LPSZ name, long opts) {
                if (Chars.endsWith(name, "new_column.d") && fail.get()) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ffOverride, () -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            String tableName = tableToken.getTableName();

            compile("alter table " + tableName + " add column new_column int");

            try {
                insert("insert into " + tableName +
                        " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
                Assert.fail();
            } catch (CairoException ex) {
                // Critical error
                Assert.assertTrue(ex.getErrno() >= 0);
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write");
            }

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n", tableName);

            fail.set(false);
            insert("insert into " + tableName +
                    " values (102, 'dfd', '2022-02-24T01', 'asd', 123)");
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n" +
                    "102\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t123\n", tableName);
        });
    }

    @Test
    public void testMainTableAddColumnFailed() throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);

        FilesFacade ffOverride = new TestFilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.endsWith(name, "new_column.d.1") && fail.get()) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ffOverride, () -> {
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                String tableName = testName.getMethodName();
                createStandardWalTable(tableName);

                compile("alter table " + tableName + " add column new_column int");

                insert("insert into " + tableName + " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
                drainWalQueue(walApplyJob);
                assertSql("x\tsym\tts\tsym2\n" +
                        "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableName);

                fail.set(false);

                insert("insert into " + tableName + " values (102, 'dfd', '2022-02-24T01', 'asd', 123)");
                drainWalQueue(walApplyJob);
                assertSql("x\tsym\tts\tsym2\n" +
                        "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableName);
            }
        });
    }

    @Test
    public void testNonWalTableTransactionNotificationIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken ignored = new TableToken(tableName, tableName, 123, false, false);
            createStandardWalTable(tableName);

            drainWalQueue();
            engine.notifyWalTxnCommitted(ignored);

            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");
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
            //noinspection CatchMayIgnoreException
            try (WalWriter writer = engine.getWalWriter(tableName)) {
                writer.apply(new UpdateOperation(tableName, 1, 22, 1) {
                    @Override
                    public SqlExecutionContext getSqlExecutionContext() {
                        return sqlExecutionContext;
                    }
                });
                Assert.fail();
            } catch (TableReferenceOutOfDateException e) {
            }

            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");
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
                "annot rename column, column with the name already exists",
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
    public void testTableWriterDirectAddColumnStopsWal() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();
            try (TableWriter writer = getWriter(tableName)) {
                writer.addColumn("abcd", ColumnType.INT);
            }

            compile("alter table " + tableName.getTableName() + " add column dddd2 long");
            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-25', 'abcd', 123)");
            drainWalQueue();

            // No SQL applied
            assertSql("x\tsym\tts\tsym2\tabcd\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n", tableName.getTableName());
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

            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-25', 'abcde')");
            compile("insert into " + tableName.getTableName() + " values (2, 'ab', '2022-02-25', 'abcdr')");
            // inserts do not check structure version
            // it fails only when structure is changing through the WAL
            compile("alter table " + tableName.getTableName() + " add column dddd2 long");
            compile("insert into " + tableName.getTableName() + " values (3, 'ab', '2022-02-25', 'abcdt', 123L)");

            drainWalQueue();
            assertSql("ts\tsym2\n" +
                    "2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2022-02-25T00:00:00.000000Z\tabcde\n" +
                    "2022-02-25T00:00:00.000000Z\tabcdr\n", tableName.getTableName());
        });
    }

    @Test
    public void testWalTableAddColumnFailedNoDiskSpaceShouldSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " ADD COLUMN sym5 SYMBOL CAPACITY 1024";
        runCheckTableSuspended(tableName, query, new TestFilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "sym5.c")) {
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
        runCheckTableNonSuspended(tableName, query);
    }

    @Test
    public void testWalTableCannotOpenSeqTxnFileToCheckTransactions() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {

            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, META_FILE_NAME)) {
                    fd = super.openRO(name);
                    return fd;
                }
                return super.openRO(name);
            }

            @Override
            public int readNonNegativeInt(int fd, long offset) {
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

            compile("insert into " + tableName.getTableName() + " values (1, 'ab', '2022-02-24T23', 'ef')");
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
        runCheckTableNonSuspended(tableName, query);
    }

    @Test
    public void testWalTableDropPartitionFailedDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " drop partition list '2022-02-25'";
        runCheckTableNonSuspended(tableName, query);
    }

    @Test
    public void testWalTableEmptyUpdateDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "update " + tableName + " set x = 1 where x < 0";
        runCheckTableNonSuspended(tableName, query);
    }

    @Test
    public void testWalTableIndexCachedFailedDoesNotSuspendTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "alter table " + tableName + " alter column sym NOCACHE";
        runCheckTableNonSuspended(tableName, query);
    }

    @Test
    public void testWalTableMultiColumnAddNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            insert("insert into " + tableToken.getTableName() +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");

            try {
                compile("alter table " + tableToken.getTableName() + " add column jjj int, column2 long");
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "statements containing multiple transactions, such as 'alter table add column col1, col2'" +
                                " are currently not supported for WAL tables"
                );
            }

            insert("insert into " + tableToken.getTableName() +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n", tableToken.getTableName());

        });
    }

    @Test
    public void testWalTableSuspendResume() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            TableToken tableToken = createStandardWalTable(testName.getMethodName());

            compile("update " + tableToken.getTableName() + " set x = 1111");
            compile("update " + tableToken.getTableName() + " set sym = 'XXX'");
            compile("update " + tableToken.getTableName() + " set sym2 = 'YYY'");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            compile("alter table " + tableToken.getTableName() + " resume wal");
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
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {

            //1
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            //2 fail
            compile("update " + tableToken.getTableName() + " set x = 1111");
            //3
            compile("update " + tableToken.getTableName() + " set sym = 'XXX'");
            //4
            compile("update " + tableToken.getTableName() + " set sym2 = 'YYY'");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            try {
                compile("alter table " + tableToken.getTableName() + " resume wal from transaction 999"); // fails
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getMessage(), "[-1] resume txn is higher than next available transaction [resumeFromTxn=999, nextTxn=5]");
            }

            compile("alter table " + tableToken.getTableName() + " resume wal from txn 3");
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
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resum wal", "'add', 'alter', 'attach', 'detach', 'drop', 'resume', 'rename', 'set' or 'squash' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wall", "'wal' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal frol", "'from' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from", "'transaction' or 'txn' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from tx", "'transaction' or 'txn' expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from txn", "transaction value expected");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from txn -10", "invalid value [value=-]");
            assertAlterTableTypeFail("alter table " + tableToken.getTableName() + " resume wal from txn 10AA", "invalid value [value=10AA]");

            engine.getTableSequencerAPI().suspendTable(tableToken);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertAlterTableTypeFail(
                    "alter table " + tableToken.getTableName() + "ererer resume wal from txn 2",
                    "table does not exist [table=" + tableToken.getTableName() + "ererer]"
            );
        });
    }

    @Test
    public void testWalTableResumeContinuesAfterEject() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "x.d.") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            node1.getConfigurationOverrides().setWalApplyTableTimeQuota(0);

            //1
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            //2 fail
            compile("update " + tableToken.getTableName() + " set x = 1111");
            //3
            compile("insert into " + tableToken.getTableName() + "(x, sym, sym2, ts) values (1, 'AB', 'EF', '2022-02-24T01')");
            //4
            compile("insert into " + tableToken.getTableName() + "(x, sym, sym2, ts) values (2, 'AB', 'EF', '2022-02-24T02')");

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
            compile("alter table " + tableToken.getTableName() + " resume wal");

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1111\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tAB\t2022-02-24T01:00:00.000000Z\tEF\n" +
                    "2\tAB\t2022-02-24T02:00:00.000000Z\tEF\n", tableToken.getTableName());
            assertSql("name\tsuspended\twriterTxn\twriterLagTxnCount\tsequencerTxn\n" + tableToken.getTableName() + "\tfalse\t4\t0\t4\n", "wal_tables()");
        });
    }

    @Test
    public void testWalTableSuspendResumeStatusTable() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            //1
            TableToken tableToken = createStandardWalTable(testName.getMethodName());
            //2 fail
            compile("update " + tableToken.getTableName() + " set x = 1111");
            //3
            compile("update " + tableToken.getTableName() + " set sym = 'XXX'");
            //4
            compile("update " + tableToken.getTableName() + " set sym2 = 'YYY'");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertSql("x\tsym\tts\tsym2\n1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n", tableToken.getTableName());

            assertSql("name\tsuspended\twriterTxn\twriterLagTxnCount\tsequencerTxn\n" + tableToken.getTableName() + "\ttrue\t1\t0\t4\n", "wal_tables()");

            compile("alter table " + tableToken.getTableName() + " resume wal");
            compile("alter table " + tableToken.getTableName() + " resume wal from transaction 0"); // ignored
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

            engine.releaseInactive(); // release writer from the pool
            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n1111\tXXX\t2022-02-24T00:00:00.000000Z\tYYY\n", tableToken.getTableName());
            assertSql("name\tsuspended\twriterTxn\twriterLagTxnCount\tsequencerTxn\n" + tableToken.getTableName() + "\tfalse\t4\t0\t4\n", "wal_tables()");
        });
    }

    @Test
    public void testWalUpdateFailedSuspendsTable() throws Exception {
        String tableName = testName.getMethodName();
        String query = "update " + tableName + " set x = 1111";
        runCheckTableSuspended(tableName, query, new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    private static void assertAlterTableTypeFail(String alterStmt, String expected) {
        try {
            compile(alterStmt);
            Assert.fail("expected SQLException is not thrown");
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), expected);
        }
    }

    private void createStandardNonWalTable(String tableName) throws SqlException {
        createStandardTable(tableName, false);
    }

    private TableToken createStandardTable(String tableName, boolean isWal) throws SqlException {
        compile("create table " + tableName + " as (" +
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
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot rename column, column does not exist");
                }
            } finally {
                Misc.free(alterOperation);
            }
        });
    }

    private void failToCopyDataToFile(String failToRollFile) throws Exception {
        FilesFacade dodgyFf = new TestFilesFacadeImpl() {

            @Override
            public long copyData(int srcFd, int destFd, long offsetSrc, long length) {
                if (destFd == fd) {
                    return -1;
                }
                return super.copyData(srcFd, destFd, offsetSrc, length);
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.endsWith(name, "1" + Files.SEPARATOR + failToRollFile)) {
                    fd = super.openRW(name, opts);
                    return fd;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(dodgyFf, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
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
                        InsertOperation insertOperation = compiledQuery.getInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    insertMethod.execute();
                    insertMethod.execute();
                    insertMethod.commit();

                    insertMethod.execute();
                    compile("alter table " + tableName + " add column new_column int");

                    try {
                        insertMethod.commit();
                    } catch (CairoException e) {
                        // todo: check all assertContains() usages
                        TestUtils.assertContains(e.getFlyweightMessage(), "failed to copy column file to new segment");
                    }
                }
            }

            insert("insert into " + tableName + " values (103, 'dfd', 'str-2', '2022-02-24T02', 'asdd', 1234)");

            drainWalQueue();
            assertSql("x\tsym\tstr\tts\tsym2\tnew_column\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\t1234\n", tableName);
        });
    }

    private void runCheckTableNonSuspended(String tableName, String query) throws Exception {
        assertMemoryLeak(() -> {
            createStandardWalTable(tableName);

            // Drop partition which does not exist
            compile(query);

            // Table should not be suspended
            insert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-25T01', 'asd')");

            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "101\tdfd\t2022-02-25T01:00:00.000000Z\tasd\n", tableName);
        });
    }

    private void runCheckTableSuspended(String tableName, String query, FilesFacade ff) throws Exception {
        assertMemoryLeak(ff, () -> {
            createStandardWalTable(tableName);

            // Drop partition which does not exist
            compile(query);

            // Table should be suspended
            compile("update " + tableName + " set x = 1111");

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
                if (Chars.endsWith(src, Files.SEPARATOR + fileName)) {
                    return -1;
                }
                return Files.hardLink(src, hardLink);
            }
        };

        assertMemoryLeak(dodgyFf, () -> {
            TableToken tableName = createStandardWalTable(testName.getMethodName());

            drainWalQueue();

            try (WalWriter ignore = engine.getWalWriter(tableName)) {
                compile("insert into " + tableName.getTableName() + " values (3, 'ab', '2022-02-25', 'abcd')");
                try (WalWriter insertedWriter = engine.getWalWriter(tableName)) {
                    try (Path path = new Path()) {
                        String columnName = "sym";
                        path.of(engine.getConfiguration().getRoot()).concat(tableName).put(Files.SEPARATOR).put(WAL_NAME_BASE).put(insertedWriter.getWalId());
                        int trimTo = path.length();

                        if (Os.type != Os.WINDOWS) {
                            // TODO: find out why files remain on Windows. They are not opened by anything
                            Assert.assertFalse(ff.exists(TableUtils.charFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE).$()));
                            Assert.assertFalse(ff.exists(TableUtils.offsetFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE).$()));
                            Assert.assertFalse(ff.exists(BitmapIndexUtils.keyFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE).$()));
                            Assert.assertFalse(ff.exists(BitmapIndexUtils.valueFileName(path.trimTo(trimTo), columnName, COLUMN_NAME_TXN_NONE).$()));
                        }
                    }

                }
            }

            compile("insert into " + tableName.getTableName() + " values (3, 'ab', '2022-02-25', 'abcd')");
            drainWalQueue();

            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n", tableName.getTableName());
        });
    }
}
