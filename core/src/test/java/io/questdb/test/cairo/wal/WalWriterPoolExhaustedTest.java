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
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.mp.SCSequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A statement that finds every pooled WAL writer of a WAL table in use must not fall back to the table's
 * TableWriter: that applies the change outside the WAL, which the sequencer never learns about.
 */
public class WalWriterPoolExhaustedTest extends AbstractCairoTest {
    private static final String TABLE_COLUMNS = """
            column\ttype\tindexed\tupsertKey
            a\tINT\tfalse\tfalse
            s\tSYMBOL\tfalse\tfalse
            ts\tTIMESTAMP\tfalse\tfalse
            """;
    private static final String TABLE_ROWS = """
            a\ts\tts
            1\tk\t2024-01-01T00:00:00.000000Z
            2\tk\t2024-01-02T00:00:00.000000Z
            """;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // 32 pooled WAL writers per table rather than 320 keeps exhausting the pool quick; the refusal does not
        // depend on the size
        setProperty(PropertyKey.CAIRO_WAL_WRITER_POOL_MAX_SEGMENTS, 1);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAddColumnIsRefused() throws Exception {
        assertRefusedThenApplied(
                "ALTER TABLE w ADD COLUMN b INT",
                """
                        column\ttype\tindexed\tupsertKey
                        a\tINT\tfalse\tfalse
                        s\tSYMBOL\tfalse\tfalse
                        ts\tTIMESTAMP\tfalse\tfalse
                        b\tINT\tfalse\tfalse
                        """,
                """
                        a\ts\tts\tb
                        1\tk\t2024-01-01T00:00:00.000000Z\tnull
                        2\tk\t2024-01-02T00:00:00.000000Z\tnull
                        """
        );
    }

    @Test
    public void testAddIndexIsRefused() throws Exception {
        assertRefusedThenApplied(
                "ALTER TABLE w ALTER COLUMN s ADD INDEX",
                """
                        column\ttype\tindexed\tupsertKey
                        a\tINT\tfalse\tfalse
                        s\tSYMBOL\ttrue\tfalse
                        ts\tTIMESTAMP\tfalse\tfalse
                        """,
                TABLE_ROWS
        );
    }

    @Test
    public void testChangeColumnTypeIsRefused() throws Exception {
        assertRefusedThenApplied(
                "ALTER TABLE w ALTER COLUMN a TYPE LONG",
                """
                        column\ttype\tindexed\tupsertKey
                        a\tLONG\tfalse\tfalse
                        s\tSYMBOL\tfalse\tfalse
                        ts\tTIMESTAMP\tfalse\tfalse
                        """,
                TABLE_ROWS
        );
    }

    @Test
    public void testDedupEnableIsRefused() throws Exception {
        assertRefusedThenApplied(
                "ALTER TABLE w DEDUP ENABLE UPSERT KEYS(ts)",
                """
                        column\ttype\tindexed\tupsertKey
                        a\tINT\tfalse\tfalse
                        s\tSYMBOL\tfalse\tfalse
                        ts\tTIMESTAMP\tfalse\ttrue
                        """,
                TABLE_ROWS
        );
    }

    @Test
    public void testDropColumnIsRefused() throws Exception {
        assertRefusedThenApplied(
                "ALTER TABLE w DROP COLUMN s",
                """
                        column\ttype\tindexed\tupsertKey
                        a\tINT\tfalse\tfalse
                        ts\tTIMESTAMP\tfalse\tfalse
                        """,
                """
                        a\tts
                        1\t2024-01-01T00:00:00.000000Z
                        2\t2024-01-02T00:00:00.000000Z
                        """
        );
    }

    @Test
    public void testDropPartitionIsRefused() throws Exception {
        assertRefusedThenApplied(
                "ALTER TABLE w DROP PARTITION LIST '2024-01-01'",
                TABLE_COLUMNS,
                """
                        a\ts\tts
                        2\tk\t2024-01-02T00:00:00.000000Z
                        """
        );
    }

    @Test
    public void testForceDropPartitionWithBusyTableWriterIsQueued() throws Exception {
        // FORCE DROP PARTITION bypasses the WAL by design: it goes to the TableWriter, and when that is busy, to its
        // command queue.
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("w");
            try (
                    TableWriter writer = engine.getWriter(token, "test");
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE w FORCE DROP PARTITION LIST '2024-01-01'", sqlExecutionContext)
                            .execute(new SCSequence())
            ) {
                Assert.assertEquals(OperationFuture.QUERY_NO_RESPONSE, future.getStatus());
                writer.tick(true);
                future.await();
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, future.getStatus());
            }
            assertQuery("SELECT * FROM w").noLeakCheck().timestamp("ts").expectSize().returns("""
                    a\ts\tts
                    2\tk\t2024-01-02T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testRenameColumnIsRefused() throws Exception {
        assertRefusedThenApplied(
                "ALTER TABLE w RENAME COLUMN a TO b",
                """
                        column\ttype\tindexed\tupsertKey
                        b\tINT\tfalse\tfalse
                        s\tSYMBOL\tfalse\tfalse
                        ts\tTIMESTAMP\tfalse\tfalse
                        """,
                """
                        b\ts\tts
                        1\tk\t2024-01-01T00:00:00.000000Z
                        2\tk\t2024-01-02T00:00:00.000000Z
                        """
        );
    }

    @Test
    public void testUpdateIsRefused() throws Exception {
        assertRefusedThenApplied(
                "UPDATE w SET a = a + 10",
                TABLE_COLUMNS,
                """
                        a\ts\tts
                        11\tk\t2024-01-01T00:00:00.000000Z
                        12\tk\t2024-01-02T00:00:00.000000Z
                        """
        );
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE w (a INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO w VALUES (1, 'k', '2024-01-01'), (2, 'k', '2024-01-02')");
        drainWalQueue();
    }

    private static void executeWithFuture(String sql) throws SqlException {
        // The future-based execution is what PostgreSQL wire and HTTP use, and the one that has the fallback.
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                OperationFuture future = compiler.compile(sql, sqlExecutionContext).execute(new SCSequence())
        ) {
            future.await();
        }
    }

    private void assertRefusedThenApplied(String sql, String expectedColumnsAfter, String expectedRowsAfter) throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("w");
            final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn();

            // hold every pooled WAL writer of the table, as long-lived ingest connections do
            final ObjList<WalWriter> walWriters = new ObjList<>();
            try {
                while (true) {
                    try {
                        walWriters.add(engine.getWalWriter(token));
                    } catch (EntryUnavailableException e) {
                        break;
                    }
                }
                try {
                    executeWithFuture(sql);
                    Assert.fail("the statement must not apply outside the WAL");
                } catch (EntryUnavailableException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table busy [reason=pool size exceeded]");
                }
            } finally {
                Misc.freeObjList(walWriters);
            }

            // nothing changed, in the table or the sequencer
            Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
            assertColumns(TABLE_COLUMNS);
            assertTable(TABLE_ROWS);

            // a WAL writer is free again, so a retry applies the statement through the WAL
            executeWithFuture(sql);
            execute("INSERT INTO w (ts) VALUES ('2024-01-03')");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertEquals(seqTxn + 2, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
            assertColumns(expectedColumnsAfter);
            assertQuery("SELECT * FROM w WHERE ts < '2024-01-03'").noLeakCheck().timestamp("ts").returns(expectedRowsAfter);
            assertQuery("SELECT count() FROM w WHERE ts = '2024-01-03'").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    private void assertColumns(String expected) throws Exception {
        assertQuery("SELECT \"column\", type, indexed, upsertKey FROM table_columns('w')").noLeakCheck().noRandomAccess().returns(expected);
    }

    private void assertTable(String expected) throws Exception {
        assertQuery("SELECT * FROM w").noLeakCheck().timestamp("ts").expectSize().returns(expected);
    }
}
