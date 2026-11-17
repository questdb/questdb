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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * A WAL UPDATE is compiled against the table structure its WAL writer had when the statement took it. A structural
 * change that another WAL writer commits before the UPDATE is sequenced must make the UPDATE recompile, not land in
 * the WAL after the change, where its apply fails and suspends the table.
 */
public class WalUpdateStructureRaceTest extends AbstractCairoTest {

    @After
    public void tearDownObserver() {
        OperationDispatcher.setPreApplyObserver(null);
    }

    @Test
    public void testNonStructuralAlterRacingAddColumnIsSequenced() throws Exception {
        // An ALTER that is not structural does not depend on the columns it was compiled against, so it is
        // sequenced after the concurrent change, as before.
        assertMemoryLeak(() -> {
            createTable();
            execute("INSERT INTO t VALUES (2, 20, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            final String alterSql = "ALTER TABLE t DROP PARTITION LIST '2024-01-01'";
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    AlterOperation alter = compiler.compile(alterSql, sqlExecutionContext).getAlterOperation();
                    WalWriter writer = engine.getWalWriter(token)
            ) {
                execute("ALTER TABLE t ADD COLUMN z INT");
                // the WAL apply compiles the statement text again, as CompiledQuery.execute() would set it
                alter.withSqlStatement(alterSql);
                alter.withContext(sqlExecutionContext);
                writer.apply(alter, true);
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\ty\tts\tz
                    2\t20\t2024-01-02T00:00:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testUpdateAsFirstRecordOfWalWriterIsRefused() throws Exception {
        assertUpdateSequencedAfterDropColumnIsRefused(true);
    }

    @Test
    public void testUpdateRacingAddColumnRecompiles() throws Exception {
        // The statement retries on TableReferenceOutOfDateException: it compiles again against the new
        // structure and applies once.
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            armConcurrentDdl("ALTER TABLE t ADD COLUMN z INT");
            execute("UPDATE t SET y = x + 1");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\ty\tts\tz
                    1\t2\t2024-01-01T00:00:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testUpdateRacingDropColumnFailsToRecompile() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            armConcurrentDdl("ALTER TABLE t DROP COLUMN x");
            try {
                execute("UPDATE t SET y = x + 1");
                Assert.fail("UPDATE must not compile against the dropped column");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: x");
                Assert.assertEquals(17, e.getPosition());
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    y\tts
                    10\t2024-01-01T00:00:00.000000Z
                    """);
            // the table keeps applying its WAL
            execute("INSERT INTO t VALUES (20, '2024-01-02T00:00:00.000000Z')");
            execute("UPDATE t SET y = y + 1");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    y\tts
                    11\t2024-01-01T00:00:00.000000Z
                    21\t2024-01-02T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testUpdateSequencedAfterDropColumnIsRefused() throws Exception {
        assertUpdateSequencedAfterDropColumnIsRefused(false);
    }

    @Test
    public void testUpdateSequencedAfterDropColumnIsRefusedAdaptiveGroupCommit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // a group commit window defers the sequencer flush of each txn to a batch
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 10_000_000L);
        assertUpdateSequencedAfterDropColumnIsRefused(false);
    }

    @Test
    public void testUpdateSequencedAfterDropColumnIsRefusedAdaptiveNoGroupCommit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // each txn flushes the sequencer itself
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        assertUpdateSequencedAfterDropColumnIsRefused(false);
    }

    @Test
    public void testUpdateSequencedAfterDropColumnIsRefusedSync() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertUpdateSequencedAfterDropColumnIsRefused(false);
    }

    private static void armConcurrentDdl(String ddl) {
        // Runs the DDL through another WAL writer of the table after the UPDATE took its own and before it is
        // sequenced. The observer disarms itself first, since the DDL goes through the dispatcher too.
        OperationDispatcher.setPreApplyObserver(() -> {
            OperationDispatcher.setPreApplyObserver(null);
            try {
                execute(ddl);
            } catch (SqlException e) {
                throw new AssertionError(e);
            }
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t (x INT, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t VALUES (1, 10, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
    }

    private void assertUpdateSequencedAfterDropColumnIsRefused(boolean isFreshWriter) throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            final String updateSql = "UPDATE t SET y = x + 1";
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    UpdateOperation update = compiler.compile(updateSql, sqlExecutionContext).getUpdateOperation();
                    // holds the pooled WAL writer that wrote the rows, so that the UPDATE takes a fresh one
                    WalWriter busyWriter = isFreshWriter ? engine.getWalWriter(token) : null;
                    // the WAL writer the UPDATE executes through, taken before the DROP commits
                    WalWriter writer = engine.getWalWriter(token)
            ) {
                Assert.assertEquals(isFreshWriter ? 2 : 1, writer.getWalId());
                final long lastSeqTxn = writer.getLastSeqTxn();
                // another WAL writer of the table commits the DROP
                execute("ALTER TABLE t DROP COLUMN x");
                update.withSqlStatement(updateSql);
                update.withContext(sqlExecutionContext);
                try {
                    writer.apply(update);
                    Assert.fail("UPDATE compiled before the DROP must not be sequenced after it");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table=t");
                }
                // nothing was sequenced for the UPDATE: the INSERT is txn 1, the DROP txn 2
                Assert.assertEquals(lastSeqTxn, writer.getLastSeqTxn());
                Assert.assertEquals(2, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
                Assert.assertFalse(writer.isDistressed());

                // the writer is still good: it caught up with the DROP and writes the new structure
                final TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor("2024-01-02"));
                row.putInt(1, 20);
                row.append();
                writer.commit();
                Assert.assertEquals(3, writer.getLastSeqTxn());
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    y\tts
                    10\t2024-01-01T00:00:00.000000Z
                    20\t2024-01-02T00:00:00.000000Z
                    """);

            // an UPDATE compiled against the new structure goes through
            execute("UPDATE t SET y = y + 1");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertEquals(4, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    y\tts
                    11\t2024-01-01T00:00:00.000000Z
                    21\t2024-01-02T00:00:00.000000Z
                    """);
        });
    }
}
