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
import io.questdb.cairo.CairoException;
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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cutlass.pgwire.BasePGTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A WAL UPDATE is compiled against the table structure its WAL writer had when the statement took it. A structural
 * change that another WAL writer commits before the UPDATE is sequenced must make the UPDATE recompile, not land in
 * the WAL after the change, where its apply fails and suspends the table. The class extends BasePGTest to run the
 * race through PostgreSQL wire too, which retries the statement itself.
 */
public class WalUpdateStructureRaceTest extends BasePGTest {

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
    public void testTruncateRacingAddColumnIsSequenced() throws Exception {
        // A TRUNCATE does not depend on the table structure either: its WAL writer takes the concurrent change while
        // sequencing, and sequences the TRUNCATE after it.
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            try (WalWriter writer = engine.getWalWriter(token)) {
                // another WAL writer of the table commits the ADD COLUMN
                execute("ALTER TABLE t ADD COLUMN z INT");
                writer.truncateSoft();
                // the INSERT is txn 1, the ADD COLUMN txn 2, the TRUNCATE txn 3
                Assert.assertEquals(3, writer.getLastSeqTxn());
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("x\ty\tts\tz\n");
        });
    }

    @Test
    public void testUpdateAsFirstRecordOfWalWriterIsRefused() throws Exception {
        assertUpdateSequencedAfterDropColumnIsRefused(true);
    }

    @Test
    public void testUpdateRacingAddColumnFailsToOpenSegment() throws Exception {
        // The refused UPDATE drops its record and moves its WAL writer to a new segment. When the writer cannot
        // create the segment, the statement fails with that error rather than retry, and nothing is sequenced for it.
        final String failingSegmentDir = Files.SEPARATOR + "wal1" + Files.SEPARATOR + "1" + Files.SEPARATOR;
        final AtomicInteger segmentDirFailures = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                // the second segment of the WAL writer that wrote the first row, which the UPDATE takes again
                if (Utf8s.endsWithAscii(path, failingSegmentDir)) {
                    segmentDirFailures.incrementAndGet();
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            final AtomicInteger dispatches = armConcurrentDdl("ALTER TABLE t ADD COLUMN z INT");
            try {
                execute("UPDATE t SET y = y + 1");
                Assert.fail("UPDATE must fail when its WAL writer cannot open a new segment");
            } catch (CairoException e) {
                TestUtils.assertEquals(
                        "Cannot create WAL segment directory: " + engine.getConfiguration().getDbRoot()
                                + Files.SEPARATOR + token.getDirName() + failingSegmentDir,
                        e.getFlyweightMessage()
                );
            }
            Assert.assertEquals(1, segmentDirFailures.get());
            // the UPDATE and the ADD COLUMN: the UPDATE did not retry
            Assert.assertEquals(2, dispatches.get());
            // the INSERT is txn 1, the ADD COLUMN txn 2, and nothing was sequenced for the UPDATE
            Assert.assertEquals(2, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());

            // the table keeps taking UPDATEs
            execute("UPDATE t SET y = y + 1");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\ty\tts\tz
                    1\t11\t2024-01-01T00:00:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testUpdateRacingAddColumnRecompiles() throws Exception {
        assertUpdateRacingAddColumnRecompiles();
    }

    @Test
    public void testUpdateRacingAddColumnRecompilesAdaptiveGroupCommit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // a group commit window defers the sequencer flush of each txn to a batch
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 10_000_000L);
        assertUpdateRacingAddColumnRecompiles();
    }

    @Test
    public void testUpdateRacingAddColumnRecompilesAdaptiveNoGroupCommit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // each txn flushes the sequencer itself
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        assertUpdateRacingAddColumnRecompiles();
    }

    @Test
    public void testUpdateRacingAddColumnRecompilesPgWire() throws Exception {
        // PostgreSQL wire retries the statement itself: it compiles the statement again, keeping the value bound to
        // it, and the next execution of the prepared statement goes through once.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            final AtomicInteger dispatches = armConcurrentDdl("ALTER TABLE t ADD COLUMN z INT");
            try (PreparedStatement update = connection.prepareStatement("UPDATE t SET y = y + ?")) {
                update.setInt(1, 1);
                update.executeUpdate();
                // the UPDATE, the ADD COLUMN, and the UPDATE again after it compiled against the new structure
                Assert.assertEquals(3, dispatches.get());
                update.executeUpdate();
                Assert.assertEquals(4, dispatches.get());
            }
            // one txn each for the INSERT, the ADD COLUMN and the two executions of the UPDATE
            Assert.assertEquals(4, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\ty\tts\tz
                    1\t12\t2024-01-01T00:00:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testUpdateRacingAddColumnRecompilesSync() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertUpdateRacingAddColumnRecompiles();
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

    /**
     * Runs the DDL through another WAL writer of the table after the UPDATE took its own and before it is
     * sequenced. Returns the count of the operations that reach the point before apply: the UPDATE, the DDL, which
     * goes through the dispatcher too, and every retry of the UPDATE. Only the first of them runs the DDL.
     */
    private static AtomicInteger armConcurrentDdl(String ddl) {
        final AtomicInteger dispatches = new AtomicInteger();
        OperationDispatcher.setPreApplyObserver(() -> {
            if (dispatches.incrementAndGet() == 1) {
                try {
                    execute(ddl);
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            }
        });
        return dispatches;
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t (x INT, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t VALUES (1, 10, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
    }

    private void assertUpdateRacingAddColumnRecompiles() throws Exception {
        // The statement retries on TableReferenceOutOfDateException: it compiles again against the new
        // structure and applies once.
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            final AtomicInteger dispatches = armConcurrentDdl("ALTER TABLE t ADD COLUMN z INT");
            execute("UPDATE t SET y = y + 1");
            // the UPDATE, the ADD COLUMN, and the UPDATE again after it compiled against the new structure
            Assert.assertEquals(3, dispatches.get());
            // one txn each for the INSERT, the ADD COLUMN and the UPDATE
            Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\ty\tts\tz
                    1\t11\t2024-01-01T00:00:00.000000Z\tnull
                    """);
        });
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
