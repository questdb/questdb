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

package io.questdb.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.mp.FanOut;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.griffin.engine.ops.AlterOperation.ADD_COLUMN;
import static io.questdb.cairo.sql.OperationFuture.QUERY_COMPLETE;
import static io.questdb.cairo.sql.OperationFuture.QUERY_NO_RESPONSE;

public class TableWriterAsyncCmdTest extends AbstractGriffinTest {

    private final SCSequence commandReplySequence = new SCSequence();
    private final int engineCmdQueue = engine.getConfiguration().getWriterCommandQueueCapacity();
    private final int engineEventQueue = engine.getConfiguration().getWriterCommandQueueCapacity();

    @Test
    public void testAsyncAlterCommandInvalidSerialisation() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);
            OperationFuture fut = null;
            AlterOperation operation = null;
            try {
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    CompiledQueryImpl cc = new CompiledQueryImpl(engine).withContext(sqlExecutionContext);
                    AlterOperation creepyAlterOperation = new AlterOperation();
                    creepyAlterOperation.of((short) 1000, "product", writer.getMetadata().getId(), 1000);
                    cc.ofAlter(creepyAlterOperation);
                    operation = cc.getOperation();
                    fut = cc.getDispatcher().execute(operation, sqlExecutionContext, commandReplySequence);
                }
                fut.await();
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertEquals("Invalid alter table command [code=1000]", ex.getFlyweightMessage());
            } finally {
                Misc.free(fut);
                Misc.free(operation);
            }
        });
    }

    @Test
    public void testAsyncAlterCommandsExceedEngineEventQueue() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp)", sqlExecutionContext);

            // Block event queue with stale sequence
            SCSequence staleSequence = new SCSequence();
            setUpEngineAsyncWriterEventWait(engine, staleSequence);

            SCSequence tempSequence = new SCSequence();
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                for (int i = 0; i < engineEventQueue; i++) {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column" + i + " int", sqlExecutionContext);
                    executeNoWait(tempSequence, cc);
                    writer.tick();
                }

                // Add column when event queue is stalled
                CompiledQuery cc = compiler.compile("ALTER TABLE product add column column5 int", sqlExecutionContext);
                try (OperationFuture fut = cc.execute(tempSequence)) {
                    fut.await(0);
                    writer.tick();
                    Assert.assertEquals(QUERY_NO_RESPONSE, fut.await(500_000));
                }

                // Remove sequence
                stopEngineAsyncWriterEventWait(engine, staleSequence);

                // Re-execute last query
                try (OperationFuture qf = cc.execute(tempSequence)) {
                    qf.await(0);
                    writer.tick();

                    try {
                        qf.await();
                        Assert.fail();
                    } catch (SqlException exception) {
                        TestUtils.assertContains(exception.getFlyweightMessage(), "Duplicate column name: column5");
                    }
                }
            }
        });
    }

    @Test
    public void testAsyncAlterCommandsExceedsEngineCmdQueue() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp)", sqlExecutionContext);
            SCSequence tempSequence = new SCSequence();

            // Block table
            try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                for (int i = 0; i < engineCmdQueue; i++) {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column" + i + " int", sqlExecutionContext);
                    executeNoWait(tempSequence, cc);
                }

                try {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column5 int", sqlExecutionContext);
                    try (OperationFuture ignored1 = cc.execute(tempSequence)) {
                        Assert.fail();
                    }
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot publish, command queue is full [table=product]");
                }
            } // Unblock table

            CompiledQuery cc = compiler.compile("ALTER TABLE product add column column5 int", sqlExecutionContext);
            try (OperationFuture fut = cc.execute(tempSequence)) {
                // Should execute in sync since writer is unlocked
                Assert.assertEquals(QUERY_COMPLETE, fut.getStatus());
            }
        });
    }

    @Test
    public void testAsyncAlterCommandsFailsToDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                int attempt = 0;

                @Override
                public boolean rename(LPSZ from, LPSZ to) {
                    if (Chars.endsWith(from, "_meta") && attempt++ < configuration.getFileOperationRetryCount()) {
                        return false;
                    }
                    return super.rename(from, to);
                }
            };
            compile("create table product as (select x, x as to_remove from long_sequence(100))", sqlExecutionContext);

            OperationFuture fut;
            // Block table
            try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                CompiledQuery cc = compiler.compile("ALTER TABLE product drop column to_remove", sqlExecutionContext);
                fut = cc.execute(commandReplySequence);
            } // Unblock table

            try {
                fut.await();
            } catch (SqlException exception) {
                TestUtils.assertContains(exception.getFlyweightMessage(), "cannot drop column. Try again later");
            } finally {
                fut.close();
            }
            compile("ALTER TABLE product drop column to_remove", sqlExecutionContext);
        });
    }

    @Test
    public void testAsyncAlterCommandsFailsToDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public int rmdir(Path name) {
                    if (Chars.contains(name, "2020-01-01")) {
                        throw CairoException.instance(11);
                    }
                    return super.rmdir(name);
                }
            };
            compile("create table product as (select x, timestamp_sequence('2020-01-01', 1000000000) ts from long_sequence(100))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            OperationFuture fut;
            // Block table
            try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                CompiledQuery cc = compiler.compile("ALTER TABLE product drop partition LIST '2020-01-01'", sqlExecutionContext);
                fut = cc.execute(commandReplySequence);
            } // Unblock table

            try {
                fut.await();
                Assert.fail();
            } catch (SqlException ex) {
                fut.close();
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not remove partition '2020-01-01'");
            }
        });
    }

    @Test
    public void testAsyncAlterCommandsFailsToRemoveColumn() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                int attempt = 0;

                @Override
                public boolean rename(LPSZ from, LPSZ to) {
                    if (Chars.endsWith(from, "_meta") && attempt++ < configuration.getFileOperationRetryCount()) {
                        return false;
                    }
                    return super.rename(from, to);
                }
            };
            compile("create table product as (select x, x as to_remove from long_sequence(100))", sqlExecutionContext);

            // Block table
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                CompiledQuery cc = compiler.compile("ALTER TABLE product drop column to_remove", sqlExecutionContext);
                try (OperationFuture fut = cc.execute(new SCSequence())) {
                    writer.tick(true);

                    try {
                        fut.await(Timestamps.SECOND_MICROS);
                        Assert.fail();
                    } catch (SqlException exception) {
                        Assert.assertNotNull(exception);
                        TestUtils.assertContains(exception.getFlyweightMessage(), "cannot drop column. Try again later");
                    }
                }

            } // Unblock table
            int status = compiler.compile("ALTER TABLE product drop column to_remove", sqlExecutionContext).execute(null).getStatus();
            Assert.assertEquals(QUERY_COMPLETE, status);
        });
    }

    @Test
    public void testAsyncAlterDeserializationFails() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product as (select x, timestamp_sequence('2020-01-01', 1000000000) ts from long_sequence(100))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            OperationFuture fut;
            // Block table
            String tableName = "product";
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "test lock")) {
                final int tableId = writer.getMetadata().getId();
                short command = ADD_COLUMN;
                AlterOperation creepyAlter = new AlterOperation() {
                    @Override
                    public void serialize(TableWriterTask event) {
                        event.of(TableWriterTask.CMD_ALTER_TABLE, tableId, tableName);
                        event.setInstance(1);
                        event.putShort(command);
                        event.putInt(-1);
                        event.putInt(1000);
                    }
                };
                creepyAlter.of(command, tableName, tableId, 100);
                CompiledQueryImpl cc = new CompiledQueryImpl(engine).withContext(sqlExecutionContext);
                cc.ofAlter(creepyAlter);
                fut = cc.execute(commandReplySequence);
            } // Unblock table

            try {
                fut.await();
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "invalid alter statement serialized to writer queue [2]");
            }
            fut.close();
        });
    }

    @Test
    public void testAsyncAlterDoesNotCommitUncommittedRowsOnWriterClose() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache) timestamp(timestamp)", sqlExecutionContext);
            OperationFuture fut = null;
            try {
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    CompiledQuery cc = compiler.compile("alter table product alter column name cache", sqlExecutionContext);
                    fut = cc.execute(commandReplySequence);

                    // Add 1 row
                    TableWriter.Row row = writer.newRow(0);
                    row.putSym(1, "s");
                    row.append();
                    // No commit
                }

                fut.await();
                engine.releaseAllReaders();

                try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "product")) {
                    Assert.assertEquals(0, rdr.size());
                }
            } finally {
                if (fut != null) {
                    fut.close();
                }
            }
        });
    }

    @Test
    public void testAsyncAlterNonExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);
            OperationFuture fut = null;
            try {
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    AlterOperationBuilder creepyAlter = new AlterOperationBuilder();
                    creepyAlter.ofDropColumn(1, "product", writer.getMetadata().getId());
                    creepyAlter.ofDropColumn("timestamp");
                    CompiledQueryImpl cc = new CompiledQueryImpl(engine).withContext(sqlExecutionContext);
                    cc.ofAlter(creepyAlter.build());
                    fut = cc.execute(commandReplySequence);
                }
                compile("drop table product", sqlExecutionContext);

                // ALTER TABLE should be executed successfully on writer.close()
                fut.await();
            } finally {
                if (fut != null) {
                    fut.close();
                }
            }
        });
    }

    @Test
    public void testAsyncAlterSymbolCache() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);
            OperationFuture fut = null;
            try {
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    CompiledQuery cc = compiler.compile("alter table product alter column name cache", sqlExecutionContext);
                    fut = cc.execute(commandReplySequence);
                    writer.tick();
                }

                fut.await();
                engine.releaseAllReaders();

                try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "product")) {
                    int colIndex = rdr.getMetadata().getColumnIndex("name");
                    Assert.assertTrue(rdr.getSymbolMapReader(colIndex).isCached());
                }
            } finally {
                if (fut != null) {
                    fut.close();
                }
            }
        });
    }

    @Test
    public void testAsyncRenameMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);
            OperationFuture fut = null;
            try {

                try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    CompiledQuery cc = compiler.compile("alter table product rename column name to name1, timestamp to timestamp1", sqlExecutionContext);
                    fut = cc.execute(commandReplySequence);
                }
                fut.await();

                engine.releaseAllReaders();
                try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "product")) {
                    Assert.assertEquals(0, rdr.getMetadata().getColumnIndex("timestamp1"));
                    Assert.assertEquals(1, rdr.getMetadata().getColumnIndex("name1"));
                }
            } finally {
                if (fut != null) {
                    fut.close();
                }
            }
        });
    }

    @Test
    public void testCommandQueueReused() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp)", sqlExecutionContext);

            // Block event queue with stale sequence
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                for (int i = 0; i < 2 * engineEventQueue; i++) {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column" + i + " int", sqlExecutionContext);
                    try (OperationFuture fut = cc.execute(commandReplySequence)) {
                        writer.tick();
                        fut.await();
                    }
                }

                Assert.assertEquals(2L * engineEventQueue + 1, writer.getMetadata().getColumnCount());
            }
        });
    }

    @Test
    public void testCommandQueueBufferOverflow() throws Exception {
        long tmpWriterCommandQueueSlotSize = writerCommandQueueSlotSize;
        writerCommandQueueSlotSize = 4L;
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp)", sqlExecutionContext);

            // Get the lock so command has to be serialized to writer command queue
            try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                CompiledQuery cc = compiler.compile("ALTER TABLE product add column colTest int", sqlExecutionContext);
                try {
                    cc.execute(commandReplySequence);
                    Assert.fail();
                } catch (CairoException exception) {
                    TestUtils.assertContains(exception.getFlyweightMessage(), "async command/event queue buffer overflow");
                }
            }
        });
        writerCommandQueueSlotSize = tmpWriterCommandQueueSlotSize;
    }

    @Test
    public void testInvalidAlterDropPartitionStatementQueued() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);

            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                AlterOperationBuilder creepyAlter = new AlterOperationBuilder();
                creepyAlter.ofDropPartition(0, "product", writer.getMetadata().getId()).ofPartition(0);
                CompiledQueryImpl cc = new CompiledQueryImpl(engine).withContext(sqlExecutionContext);
                cc.ofAlter(creepyAlter.build());
                try (OperationFuture fut = cc.execute(commandReplySequence)) {
                    writer.tick();

                    try {
                        fut.await();
                        Assert.fail();
                    } catch (SqlException exception) {
                        TestUtils.assertContains(exception.getFlyweightMessage(), "could not remove partition 'default'");
                    }
                }
            }
        });
    }

    @Test
    public void testInvalidAlterStatementQueued() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);

            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {

                AlterOperationBuilder creepyAlter = new AlterOperationBuilder();
                creepyAlter.ofDropColumn(1, "product", writer.getMetadata().getId());
                creepyAlter.ofDropColumn("timestamp").ofDropColumn("timestamp");
                CompiledQueryImpl cc = new CompiledQueryImpl(engine).withContext(sqlExecutionContext);
                cc.ofAlter(creepyAlter.build());

                try (OperationFuture fut = cc.execute(commandReplySequence)) {
                    writer.tick(true);
                    try {
                        fut.await();
                        Assert.fail();
                    } catch (SqlException exception) {
                        TestUtils.assertContains(exception.getFlyweightMessage(), "Invalid column: timestamp");
                    }
                }
            }
        });
    }

    /***
     *
     * @param engine Cairo Engine to consume events from
     * @param sequence sequence to subscribe ot the events
     */
    private static void setUpEngineAsyncWriterEventWait(CairoEngine engine, SCSequence sequence) {
        final FanOut writerEventFanOut = engine.getMessageBus().getTableWriterEventFanOut();
        writerEventFanOut.and(sequence);
    }

    /***
     * Cleans up execution wait sequence to listen to the Engine async writer events
     * @param engine Cairo Engine subscribed to
     * @param sequence to unsubscribe from Writer Events
     */
    private static void stopEngineAsyncWriterEventWait(CairoEngine engine, SCSequence sequence) {
        engine.getMessageBus().getTableWriterEventFanOut().remove(sequence);
        sequence.clear();
    }

    private void executeNoWait(SCSequence tempSequence, CompiledQuery cc) throws SqlException {
        try (OperationFuture cq = cc.execute(tempSequence)) {
            cq.await(0);
        }
    }

}
