/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.wal.TableWriterBackend;
import io.questdb.cairo.wal.TableWriterFrontend;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class WalTableFailureTest extends AbstractGriffinTest {
    @Test
    public void testAddColumnFailToApplySequencerMetadataStructureChangeTransaction() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            try (TableWriterFrontend twf = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                AtomicInteger counter = new AtomicInteger(2);
                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        if (counter.decrementAndGet() == 0) {
                            throw new IndexOutOfBoundsException();
                        }
                        tableWriter.addColumn("new_column", ColumnType.INT, 0, false, false, 12, true);
                        return 0;
                    }

                    @Override
                    public boolean isStructureChange() {
                        return true;
                    }
                };

                try {
                    twf.applyAlter(dodgyAlter, true);
                    Assert.fail("Expected exception is missing");
                } catch (IndexOutOfBoundsException ex) {
                    //expected
                }
            }

            drainWalQueue();
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n");
        });
    }

    @Test
    public void testAddColumnFailToSerialiseToSequencerTransactionLog() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            try (TableWriterFrontend twf = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        tableWriter.addColumn("new_column", ColumnType.INT, 0, false, false, 12, true);
                        return 0;
                    }

                    @Override
                    public boolean isStructureChange() {
                        return true;
                    }

                    @Override
                    public void serializeBody(MemoryA sink) {
                        throw new IndexOutOfBoundsException();
                    }
                };

                try {
                    twf.applyAlter(dodgyAlter, true);
                    Assert.fail("Expected exception is missing");
                } catch (IndexOutOfBoundsException ex) {
                    //expected
                }
            }

            drainWalQueue();
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n");
        });
    }

    @Test
    public void testApplyJobFailsToApplyDataFirstTime() throws Exception {
        FilesFacade dodgyFacade = new FilesFacadeImpl() {
            int counter = 0;

            @Override
            public long openRW(LPSZ name, long mode) {
                if (Chars.endsWith(name, "2022-02-25" + Files.SEPARATOR + "x.d.1") && counter++ < 2) {
                    return -1;
                }
                return super.openRW(name, mode);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            drainWalQueue();
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-25', 'ef')");

            // Data is not there, job failed to apply the data.
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n");

            drainWalQueue();

            // Second time lucky, 2 line in.
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-25T00:00:00.000000Z\tef\n");
        });
    }

    @Test
    public void testApplyJobFailsToApplyStructureChange() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            try (TableWriterFrontend twf = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        tableWriter.addColumn("new_column", ColumnType.INT, 0, false, false, 12, true);
                        return 0;
                    }

                    @Override
                    public boolean isStructureChange() {
                        return true;
                    }

                    @Override
                    public void serializeBody(MemoryA sink) {
                    }
                };

                twf.applyAlter(dodgyAlter, true);
            }

            drainWalQueue();
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef', null)");

            drainWalQueue();
            // WAL table is not affected, cannot process dodgy alter.
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n");
        });
    }

    @Test
    public void testDataTxnFailToCommitInWalWriter() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            drainWalQueue();

            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                 WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                 WalWriter walWriter3 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {

                MatcherAssert.assertThat(walWriter1.getWalId(), is(1));
                MatcherAssert.assertThat(walWriter2.getWalId(), is(2));
                MatcherAssert.assertThat(walWriter3.getWalId(), is(3));
            }

            AlterOperationBuilder alterBuilder = new AlterOperationBuilder().ofDropColumn(1, tableName, 0);
            AlterOperation alterOperation = alterBuilder.ofDropColumn("non_existing_column").build();

            try (TableWriterFrontend alterWriter = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test");
                 TableWriterFrontend insertWriter = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {

                // Serialize into WAL sequencer a drop column operation of non-existing column
                // So that it will fail during application to other WAL writers
                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        tableWriter.removeColumn("x");
                        return 0;
                    }

                    @Override
                    public boolean isStructureChange() {
                        return true;
                    }

                    @Override
                    public void serializeBody(MemoryA sink) {
                        alterOperation.serializeBody(sink);
                    }
                };
                alterWriter.applyAlter(dodgyAlter, true);

                TableWriter.Row row = insertWriter.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-25"));
                row.putLong(0, 123L);
                row.append();

                try {
                    insertWriter.commit();
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not apply table definition changes to the current transaction. Invalid column: non_existing_column");
                }
            } finally {
                Misc.free(alterOperation);
            }

            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                MatcherAssert.assertThat(walWriter1.getWalId(), is(1));
                MatcherAssert.assertThat(walWriter1.getSegmentId(), is(0));

                // Assert wal writer 2 is not in the pool after failure to apply structure change
                // wal writer 3 will fail to go active because of dodgy Alter in the WAL sequencer

                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    MatcherAssert.assertThat(walWriter2.getWalId(), is(4));
                    MatcherAssert.assertThat(walWriter1.getSegmentId(), is(0));
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
        FilesFacade dodgyFf = new FilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Chars.endsWith(from, "wal2" + Files.SEPARATOR + "0" + Files.SEPARATOR + "x.d")) {
                    return -1;
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(dodgyFf, () -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            drainWalQueue();

            AlterOperation alterOperation = null;
            try (TableWriterFrontend alterWriter = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test");
                 TableWriterFrontend insertWriter = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {

                AlterOperationBuilder alterBuilder = new AlterOperationBuilder().ofRenameColumn(1, tableName, 0);
                alterBuilder.ofRenameColumn("x", "x2");
                alterOperation = alterBuilder.build();
                alterWriter.applyAlter(alterOperation, true);

                TableWriter.Row row = insertWriter.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-25"));
                row.putLong(0, 123L);
                row.append();

                try {
                    insertWriter.commit();
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not apply table definition changes to the current transaction. cannot rename column \"x\", errno=");
                }

            } finally {
                Misc.free(alterOperation);
            }

            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                MatcherAssert.assertThat(walWriter1.getWalId(), is(1));

                // Assert wal writer 2 is not in the pool after failure to apply structure change
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    MatcherAssert.assertThat(walWriter2.getWalId(), is(3));
                }
            }

            compile("insert into " + tableName + " values (3, 'ab', '2022-02-25', 'abcd')");
            drainWalQueue();

            // No SQL applied
            assertSql(tableName, "x2\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n");
        });
    }

    @Test
    public void testDodgyAddColumDoesNotChangeMetadata() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            try (TableWriterFrontend twf = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                AtomicInteger counter = new AtomicInteger(2);
                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        if (counter.decrementAndGet() == 0) {
                            return 0;
                        }
                        tableWriter.addColumn("new_column", ColumnType.INT, 0, false, false, 12, true);
                        return 0;
                    }

                    @Override
                    public boolean isStructureChange() {
                        return true;
                    }
                };

                try {
                    twf.applyAlter(dodgyAlter, true);
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(),
                            "applying structure change to WAL table failed " +
                                    "[table=testDodgyAddColumDoesNotChangeMetadata, oldVersion: 0, newVersion: 0]");
                }
            }

            drainWalQueue();
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n");
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
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            try (TableWriterFrontend twf = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        return 0;
                    }

                    @Override
                    public boolean isStructureChange() {
                        return false;
                    }
                };

                try {
                    twf.applyAlter(dodgyAlter, true);
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "failed to commit ALTER SQL to WAL, sql context is empty ");
                }
            }

            drainWalQueue();
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n");
        });
    }

    @Test
    public void testInvalidNonStructureChangeMakeWalWriterDistressed() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                MatcherAssert.assertThat(walWriter.getWalId(), is(1));

                AlterOperation dodgyAlter = new AlterOperation() {
                    @Override
                    public long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
                        return 0;
                    }

                    @Override
                    public boolean isStructureChange() {
                        return false;
                    }

                    public SqlExecutionContext getSqlExecutionContext() {
                        return sqlExecutionContext;
                    }

                    @Override
                    public CharSequence getSqlStatement() {
                        throw new IndexOutOfBoundsException();
                    }
                };

                try {
                    walWriter.applyAlter(dodgyAlter, true);
                    Assert.fail("Expected exception is missing");
                } catch (IndexOutOfBoundsException ex) {
                    //expected
                }
            }

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                // Wal Writer 1 is not pooled
                MatcherAssert.assertThat(walWriter.getWalId(), is(2));
            }

            compile("insert into " + tableName + " values (1, 'ab', '2022-02-24T23', 'ef')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "1\tab\t2022-02-24T23:00:00.000000Z\tef\n");
        });
    }

    @Test
    public void testMainAddDuplicateColumnSequentiallyFailsWithSqlException() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            compile("alter table " + tableName + " add column new_column int");

            try {
                compile("alter table " + tableName + " add column new_column int");
            } catch (SqlException ex) {
                // Critical error
                TestUtils.assertContains(ex.getFlyweightMessage(), "column 'new_column' already exists");
            }
        });
    }

    @Test
    public void testMainAndWalTableAddColumnFailed() throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);

        FilesFacade ffOverride = new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, long opts) {
                if (Chars.endsWith(name, "new_column.d") && fail.get()) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(ffOverride, () -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            compile("alter table " + tableName + " add column new_column int");

            try {
                executeInsert("insert into " + tableName +
                        " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
                Assert.fail();
            } catch (CairoException ex) {
                // Critical error
                MatcherAssert.assertThat(ex.getErrno(), greaterThanOrEqualTo(0));
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write");
            }

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n");

            fail.set(false);
            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t123\n");
        });
    }

    @Test
    public void testMainTableAddColumnFailed() throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);

        FilesFacade ffOverride = new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, long opts) {
                if (Chars.endsWith(name, "new_column.d.1") && fail.get()) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(ffOverride, () -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            compile("alter table " + tableName + " add column new_column int");

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n");

            fail.set(false);

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd', 123)");
            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tnew_column\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t123\n");
        });
    }

    @Test
    public void testRenameColumnDoesNotExist() throws Exception {
        String tableName = testName.getMethodName();
        AlterOperationBuilder alterBuilder = new AlterOperationBuilder().ofRenameColumn(1, tableName, 0);
        alterBuilder.ofRenameColumn("x", "x2");
        AlterOperation alterOperation = alterBuilder.build();
        failToApplyDoubleAlter(alterOperation);
    }

    @Test
    public void testRenameDesignatedTimestampFails() throws Exception {
        String tableName = testName.getMethodName();
        AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                .ofRenameColumn(1, tableName, 0);
        alterBuilder.ofRenameColumn("ts", "ts2");
        AlterOperation alterOperation = alterBuilder.build();

        failToApplyAlter(alterOperation, "cannot rename designated timestamp column");
    }

    @Test
    public void testRenameToExistingNameFails() throws Exception {
        String tableName = testName.getMethodName();
        AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                .ofRenameColumn(1, tableName, 0);
        alterBuilder.ofRenameColumn("x", "sym");
        AlterOperation alterOperation = alterBuilder.build();

        failToApplyAlter(alterOperation, "annot rename column, column with the name already exists");
    }

    @Test
    public void testRenameToInvalidColumnNameFails() throws Exception {
        String tableName = testName.getMethodName();
        AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                .ofRenameColumn(1, tableName, 0);
        alterBuilder.ofRenameColumn("x", "/../tb");
        AlterOperation alterOperation = alterBuilder.build();

        failToApplyAlter(alterOperation, "invalid column name: /../tb");
    }

    @Test
    public void testDropDesignatedTimestampFails() throws Exception {
        String tableName = testName.getMethodName();
        AlterOperationBuilder alterBuilder = new AlterOperationBuilder()
                .ofDropColumn(1, tableName, 0);
        alterBuilder.ofDropColumn("ts");

        AlterOperation alterOperation = alterBuilder.build();
        failToApplyAlter(alterOperation, "cannot remove designated timestamp column");
    }

    @Test
    public void testTableWriterDirectAddColumnStopsWall() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            drainWalQueue();
            try (TableWriter writer = engine.getWriter(
                    sqlExecutionContext.getCairoSecurityContext(),
                    tableName,
                    "wal killer")
            ) {
                writer.addColumn("abcd", ColumnType.INT);
            }

            compile("alter table " + tableName + " add column dddd2 long");
            compile("insert into " + tableName + " values (1, 'ab', '2022-02-25', 'abcd', 123)");
            drainWalQueue();

            // No SQL applied
            assertSql(tableName, "x\tsym\tts\tsym2\tabcd\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n");
        });
    }

    @Test
    public void testTableWriterDirectDropColumnStopsWall() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            drainWalQueue();
            try (TableWriter writer = engine.getWriter(
                    sqlExecutionContext.getCairoSecurityContext(),
                    tableName,
                    "wal killer")
            ) {
                writer.removeColumn("x");
                writer.removeColumn("sym");
            }

            compile("insert into " + tableName + " values (1, 'ab', '2022-02-25', 'abcd')");
            compile("insert into " + tableName + " values (2, 'ab', '2022-02-25', 'abcd')");
            compile("alter table " + tableName + " add column dddd2 long");
            compile("insert into " + tableName + " values (3, 'ab', '2022-02-25', 'abcd', 123L)");

            drainWalQueue();

            // No SQL applied
            assertSql(tableName, "ts\tsym2\n" +
                    "2022-02-24T00:00:00.000000Z\tEF\n");
        });
    }

    @Test
    public void testWalTableMultiColumnAddNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");

            try {
                compile("alter table " + tableName + " add column jjj int, column2 long");
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table structure change did not contain 1 transaction");
            }

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");
            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n");

        });
    }

    private void creatStandardWalTable(String tableName) throws SqlException {
        compile("create table " + tableName + " as (" +
                "select x, " +
                " rnd_symbol('AB', 'BC', 'CD') sym, " +
                " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                " from long_sequence(1)" +
                ") timestamp(ts) partition by DAY WAL");
    }

    private void failToApplyDoubleAlter(AlterOperation alterOperation) throws Exception {
        try {
            assertMemoryLeak(() -> {
                String tableName = testName.getMethodName();
                creatStandardWalTable(tableName);
                drainWalQueue();

                try (TableWriterFrontend alterWriter1 = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test");
                     TableWriterFrontend alterWriter2 = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {

                    alterWriter1.applyAlter(alterOperation, true);
                    try {
                        alterWriter2.applyAlter(alterOperation, true);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "cannot rename column, column does not exists");
                    }
                }
            });
        } finally {
            Misc.free(alterOperation);
        }
    }

    private void failToApplyAlter(AlterOperation alterOperation, String error) throws Exception {
        try {
            assertMemoryLeak(() -> {
                String tableName = testName.getMethodName();
                creatStandardWalTable(tableName);
                drainWalQueue();

                try (TableWriterFrontend alterWriter2 = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {

                    try {
                        alterWriter2.applyAlter(alterOperation, true);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), error);
                    }
                }
            });
        } finally {
            Misc.free(alterOperation);
        }
    }

    private void failToCopyDataToFile(String failToRollFile) throws Exception {
        FilesFacade dodgyFf = new FilesFacadeImpl() {
            long fd = -1;

            @Override
            public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
                if (destFd == fd) {
                    return -1;
                }
                return super.copyData(srcFd, destFd, offsetSrc, length);
            }

            @Override
            public long openRW(LPSZ name, long opts) {
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
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not add column [error=failed to copy column file to new segment");
                }
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', 'str-2', '2022-02-24T02', 'asdd', 1234)");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tstr\tts\tsym2\tnew_column\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\t1234\n");
        });
    }

    private void testFailToLinkSymbolFile(String fileName) throws Exception {
        FilesFacade dodgyFf = new FilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                if (Chars.endsWith(src, Files.SEPARATOR + fileName)) {
                    return -1;
                }
                return Files.hardLink(src, hardLink);
            }
        };

        assertMemoryLeak(dodgyFf, () -> {
            String tableName = testName.getMethodName();
            creatStandardWalTable(tableName);

            drainWalQueue();

            try (WalWriter ignore = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                compile("insert into " + tableName + " values (3, 'ab', '2022-02-25', 'abcd')");
                try (WalWriter insertedWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
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

            compile("insert into " + tableName + " values (3, 'ab', '2022-02-25', 'abcd')");
            drainWalQueue();

            // No SQL applied
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n" +
                    "3\tab\t2022-02-25T00:00:00.000000Z\tabcd\n");

        });
    }
}
