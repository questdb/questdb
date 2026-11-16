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

package io.questdb.test.fuzz;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;

public class FuzzVacuumOperationTest extends AbstractCairoTest {
    @Test
    public void testApplyPreservesLiveCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isWal : new boolean[]{false, true}) {
                for (boolean isParquet : new boolean[]{false, true}) {
                    for (int indexKind = 0; indexKind < 3; indexKind++) {
                        final String indexType = switch (indexKind) {
                            case 0 -> "POSTING";
                            case 1 -> "POSTING DELTA";
                            default -> "POSTING EF";
                        };
                        execute("CREATE TABLE t (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
                        execute("ALTER TABLE t ADD COLUMN sym SYMBOL");
                        execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE " + indexType + " INCLUDE (val)");
                        execute("""
                                INSERT INTO t VALUES
                                    ('2024-01-01', 1.0, 'A'),
                                    ('2024-01-01T01:00:00', 2.0, 'B'),
                                    ('2024-01-02', 3.0, 'A')
                                """);
                        if (isWal) {
                            drainWalQueue();
                        }
                        if (isParquet) {
                            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                            drainWalQueue();
                            releaseSavePointHolds("t");
                        }
                        final File tableDir = new File(configuration.getDbRoot(), engine.verifyTableName("t").getDirName());
                        final File[] partitions = tableDir.listFiles((dir, name) -> name.startsWith("2024-01-01"));
                        Assert.assertNotNull(partitions);
                        Assert.assertEquals(1, partitions.length);
                        final File staleFile = new File(partitions[0], "dropped.d");
                        Assert.assertTrue(configuration.getFilesFacade().touch(Path.getThreadLocal(staleFile.getAbsolutePath()).$()));

                        try (TableWriterAPI writer = isWal ? getWalWriter("t") : getWriter("t")) {
                            FuzzVacuumOperation operation = new FuzzVacuumOperation();
                            Assert.assertFalse(operation.apply(new Rnd(), engine, writer, -1, null));
                            Assert.assertFalse(operation.apply(new Rnd(), engine, writer, -1, null));
                        }
                        Assert.assertFalse("VACUUM must delete the rogue column file", staleFile.exists());
                        assertQuery("SELECT sym, val, ts FROM t WHERE sym = 'A'")
                                .noLeakCheck()
                                .timestamp("ts")
                                .expectSize()
                                .noRandomAccess()
                                .withPlanContaining("CoveringIndex on: sym with: val, ts")
                                .returns("""
                                        sym\tval\tts
                                        A\t1.0\t2024-01-01T00:00:00.000000Z
                                        A\t3.0\t2024-01-02T00:00:00.000000Z
                                        """);
                        execute("DROP TABLE t");
                        engine.releaseInactive();
                    }
                }
            }
        });
    }

    @Test
    public void testApplyPreservesOldReader() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (ts TIMESTAMP, val DOUBLE, sym SYMBOL INDEX TYPE POSTING INCLUDE (val))
                    TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t VALUES ('2024-01-01', 1.0, 'A'), ('2024-01-02', 2.0, 'A')");
            try (RecordCursorFactory factory = select("SELECT val FROM t WHERE sym = 'A'");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1.0, cursor.getRecord().getDouble(0), 0);
                execute("UPDATE t SET val = val + 100");
                try (TableWriterAPI writer = getWriter("t")) {
                    new FuzzVacuumOperation().apply(new Rnd(), engine, writer, -1, null);
                }
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1.0, cursor.getRecord().getDouble(0), 0);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(2.0, cursor.getRecord().getDouble(0), 0);
                Assert.assertFalse(cursor.hasNext());
            }
            try (TableWriterAPI writer = getWriter("t")) {
                new FuzzVacuumOperation().apply(new Rnd(), engine, writer, -1, null);
            }
            assertQuery("SELECT val FROM t WHERE sym = 'A'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("val\n101.0\n102.0\n");
        });
    }

    @Test
    public void testApplyPropagatesUnexpectedFailure() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("checkpoint create");
            try (TableWriterAPI writer = getWriter("t")) {
                CairoException exception = Assert.assertThrows(CairoException.class,
                        () -> new FuzzVacuumOperation().apply(new Rnd(), engine, writer, -1, null));
                TestUtils.assertContains(exception.getFlyweightMessage(), "cannot vacuum while checkpoint is in progress");
            } finally {
                execute("checkpoint release");
            }
        });
    }

    @Test
    public void testApplyRetriesWhenPurgeDiscoveryQueueIsFull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01', 1.0), ('2024-01-02', 2.0)");
            final TableToken tableToken = engine.verifyTableName("t");
            final MessageBus messageBus = engine.getMessageBus();
            int queued = 0;
            while (TableUtils.schedulePurgeO3Partitions(messageBus, tableToken, ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                queued++;
            }
            Assert.assertTrue(queued > 0);
            Assert.assertEquals(-1, messageBus.getO3PurgeDiscoveryPubSeq().next());
            try (TableWriterAPI writer = getWriter("t")) {
                Assert.assertFalse(new FuzzVacuumOperation().apply(new Rnd(), engine, writer, -1, null));
            }
            // The operation drained the full queue, then its retried VACUUM queued and drained its own task.
            Assert.assertTrue(messageBus.getO3PurgeDiscoverySubSeq().next() < 0);
            Assert.assertTrue(TableUtils.schedulePurgeO3Partitions(messageBus, tableToken, ColumnType.TIMESTAMP, PartitionBy.DAY));
            messageBus.getO3PurgeDiscoverySubSeq().clear();
            assertQuery("SELECT val FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("val\n1.0\n2.0\n");
        });
    }

    @Test
    public void testApplySkipsWhileColumnPurgeQueueIsFull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01', 1.0), ('2024-01-02', 2.0)");
            try (RecordCursorFactory factory = select("SELECT val FROM t");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                // The open reader pins the pre-UPDATE column version, so VACUUM must queue its purge.
                Assert.assertTrue(cursor.hasNext());
                execute("UPDATE t SET val = val + 100");

                final MessageBus messageBus = engine.getMessageBus();
                final Sequence pubSeq = messageBus.getColumnPurgePubSeq();
                long lastQueued = -1;
                long queueCursor;
                while ((queueCursor = pubSeq.next()) != -1) {
                    if (queueCursor > -1) {
                        pubSeq.done(queueCursor);
                        lastQueued = queueCursor;
                    }
                }
                final long filledUpTo = lastQueued;

                try (TableWriterAPI writer = getWriter("t")) {
                    // Default test configuration runs no ColumnPurgeJob, so nothing drains the queue.
                    Assert.assertFalse(new FuzzVacuumOperation().apply(new Rnd(), engine, writer, -1, null));
                    Assert.assertEquals("VACUUM skips while the queue is full", filledUpTo, pubSeq.current());

                    messageBus.getColumnPurgeSubSeq().clear();
                    Assert.assertFalse(new FuzzVacuumOperation().apply(new Rnd(), engine, writer, -1, null));
                }
                Assert.assertTrue("the next VACUUM re-discovers the column version", pubSeq.current() > filledUpTo);
            }
        });
    }

    @Test
    public void testApplyToleratesDroppedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (TableWriterAPI writer = getWalWriter("t")) {
                execute("DROP TABLE t");
                Assert.assertFalse(new FuzzVacuumOperation().apply(new Rnd(), engine, writer, -1, null));
            }
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01')");
            drainWalQueue();
            assertQuery("SELECT count(*) FROM t").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");
        });
    }

    @Test
    public void testDefaultVacuumGenerationIsRare() throws Exception {
        assertMemoryLeak(() -> {
            FuzzRunner fuzzer = newGenerator(10_000);
            final int count = countVacuums(fuzzer);
            Assert.assertTrue("the default must generate VACUUM", count > 0);
            Assert.assertTrue("VACUUM must be rare by default", count < 100);
        });
    }

    @Test
    public void testVacuumGenerationCanBeDisabled() throws Exception {
        assertMemoryLeak(() -> {
            FuzzRunner fuzzer = newGenerator(30);
            fuzzer.setVacuumProbability(0);
            Assert.assertEquals(0, countVacuums(fuzzer));
        });
    }

    @Test
    public void testVacuumGenerationCanRunBeforeEveryTransaction() throws Exception {
        assertMemoryLeak(() -> {
            FuzzRunner fuzzer = newGenerator(30);
            fuzzer.setVacuumProbability(1);
            Assert.assertEquals(30, countVacuums(fuzzer));
        });
    }

    @Test
    public void testVacuumGenerationKeepsSchemaBarriers() throws Exception {
        assertMemoryLeak(() -> {
            FuzzRunner fuzzer = newGenerator(30);
            fuzzer.setFuzzProbabilities(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            fuzzer.setVacuumProbability(1);
            Assert.assertEquals(30, countVacuums(fuzzer));
        });
    }

    private static int countVacuums(FuzzRunner fuzzer) {
        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions("generate", new Rnd(42, 24), 0);
        try {
            int count = 0;
            for (int i = 0; i < transactions.size(); i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                if (transaction.operationList.size() > 0 && transaction.operationList.getQuick(0) instanceof FuzzVacuumOperation) {
                    count++;
                    Assert.assertEquals(1, transaction.operationList.size());
                    Assert.assertTrue("VACUUM must not add a WAL transaction", transaction.rollback);
                    FuzzTransaction next = transactions.getQuick(i + 1);
                    Assert.assertEquals(next.structureVersion, transaction.structureVersion);
                    Assert.assertEquals(next.waitBarrierVersion, transaction.waitBarrierVersion);
                }
            }
            return count;
        } finally {
            Misc.freeObjListAndClear(transactions);
        }
    }

    private static FuzzRunner newGenerator(int transactionCount) throws Exception {
        execute("CREATE TABLE generate (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
        FuzzRunner fuzzer = new FuzzRunner();
        fuzzer.withDb(engine, sqlExecutionContext);
        fuzzer.setFuzzCounts(false, 1, transactionCount, 1, 1, 10, 0, 1);
        fuzzer.setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0);
        return fuzzer;
    }
}
