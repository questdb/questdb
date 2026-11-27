package io.questdb.test.fuzz;

import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FuzzDedupTransactionGeneratorTest extends AbstractCairoTest {
    @Test
    public void testCopiedRowsDoNotDependOnSourceLifetime() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE source (ts TIMESTAMP, id LONG, sym SYMBOL, s STRING, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO source VALUES (0, 1, 'original', null, 'detached varchar value')");
            drainWalQueue();
            execute("CREATE TABLE replay AS (SELECT * FROM source WHERE false) TIMESTAMP(ts) PARTITION BY DAY WAL");
            ObjList<FuzzTransaction> input = new ObjList<>();
            for (int i = 0; i < 2; i++) {
                FuzzTransaction transaction = new FuzzTransaction();
                transaction.operationList.add(new FuzzInsertOperation(1, 2, 100 + i, 0, 0, 1, 10, new String[]{"fresh"}));
                input.add(transaction);
            }
            IntList keys = new IntList();
            keys.add(0);
            keys.add(1);
            ObjList<FuzzTransaction> output = null;
            try {
                output = FuzzDedupTransactionGenerator.augment(engine, sqlExecutionContext, "source", input, keys, new Rnd(11, 12), 1, 1, 0);
                execute("DROP TABLE source");
                engine.releaseAllReaders();
                FuzzRunner fuzzer = new FuzzRunner();
                fuzzer.withDb(engine, sqlExecutionContext);
                fuzzer.applyWal(output, "replay", 1, new Rnd(13, 14));
                assertQuery("SELECT * FROM replay").timestamp("ts").expectSize().returns("""
                        ts\tid\tsym\ts\tv
                        1970-01-01T00:00:00.000000Z\t1\toriginal\t\tdetached varchar value
                        """);
            } finally {
                Misc.freeObjListAndClear(input);
                if (output != null) {
                    Misc.freeObjListAndClear(output);
                }
            }
        });
    }

    @Test
    public void testFailedReplayClosesWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE replay (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final FuzzTransaction transaction = new FuzzTransaction();
            transaction.operationList.add((rnd, engine, writer, timestampIndex, excluded) -> {
                throw new IllegalStateException("injected replay failure");
            });
            final ObjList<FuzzTransaction> transactions = new ObjList<>();
            transactions.add(transaction);
            try {
                final FuzzRunner fuzzer = new FuzzRunner();
                fuzzer.withDb(engine, sqlExecutionContext);
                Assert.assertThrows(IllegalStateException.class,
                        () -> fuzzer.applyToWal(transactions, "replay", 3, new Rnd()));
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testRandomKeyAndSchemaHistoryWithParallelWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE source (ts TIMESTAMP, id LONG, sym SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO source VALUES (0, 1, 'A', 'copied varchar'), (1, 2, null, null), (2, 3, 'B', '')");
            drainWalQueue();
            execute("CREATE TABLE replay AS (SELECT * FROM source) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final FuzzRunner fuzzer = new FuzzRunner();
            fuzzer.withDb(engine, sqlExecutionContext);
            final Rnd rnd = new Rnd(123, 456);
            final IntList keys = new IntList();
            keys.add(0);
            keys.add(1);
            keys.add(2);
            for (int round = 0; round < 2; round++) {
                final int structure;
                try (var metadata = engine.getLegacyMetadata(engine.verifyTableName("source"))) {
                    structure = (int) metadata.getMetadataVersion();
                }
                final ObjList<FuzzTransaction> inserts = new ObjList<>();
                ObjList<FuzzTransaction> transactions = null;
                try {
                    for (int i = 0; i < 8; i++) {
                        final FuzzTransaction transaction = new FuzzTransaction();
                        transaction.structureVersion = structure;
                        transaction.rollback = i == 3;
                        transaction.operationList.add(new FuzzInsertOperation(17 + i, 41 + round,
                                100 + round * 100 + i, 0.1, 0.2, 0, 20, new String[]{"A", "B", "C"}));
                        inserts.add(transaction);
                    }
                    transactions = FuzzDedupTransactionGenerator.augment(engine, sqlExecutionContext,
                            "source", inserts, keys, rnd, 4, 3, 0, "v");
                    Assert.assertEquals(14, transactions.size());
                    int changes = 0;
                    int dedupChanges = 0;
                    for (int i = 0; i < transactions.size(); i++) {
                        final FuzzTransaction transaction = transactions.getQuick(i);
                        Assert.assertEquals(i, transaction.waitBarrierVersion);
                        Assert.assertEquals(structure + changes, transaction.structureVersion);
                        Assert.assertTrue(transaction.waitAllDone);
                        final var operation = transaction.operationList.getQuick(0);
                        if (operation instanceof FuzzDedupOperation dedup) {
                            Assert.assertEquals(dedupChanges++ < 3, dedup.isEnabled());
                            changes++;
                        } else if (operation instanceof FuzzAddColumnOperation || operation instanceof FuzzDropColumnOperation) {
                            changes++;
                        }
                    }
                    Assert.assertEquals(6, changes);
                    Assert.assertEquals(4, dedupChanges);
                    Misc.freeObjListAndClear(inserts);
                    fuzzer.applyWal(transactions, "source", 1, new Rnd(5, 6));
                    final ObjList<ObjList<FuzzTransaction>> batches = new ObjList<>();
                    batches.add(transactions);
                    fuzzer.applyManyWalParallel(batches, new Rnd(7, 8), "replay", false, true);
                    drainWalQueue();
                    fuzzer.checkNoSuspendedTables();
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, "source", "replay", LOG);
                    // A reused name is a new column, not a destination for the old snapshot's value.
                    assertQuery("SELECT count() FROM replay WHERE ts < 3 AND v IS NOT NULL")
                            .noRandomAccess().expectSize().returns("count\n0\n");
                } finally {
                    Misc.freeObjListAndClear(inserts);
                    if (transactions != null) {
                        Misc.freeObjListAndClear(transactions);
                    }
                }
            }
        });
    }

    @Test
    public void testRejectsEmptySourceAndSchemaChanges() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE source (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            ObjList<FuzzTransaction> input = new ObjList<>();
            input.add(new FuzzTransaction());
            input.add(new FuzzTransaction());
            IntList keys = new IntList();
            keys.add(0);
            try {
                IllegalArgumentException empty = Assert.assertThrows(IllegalArgumentException.class,
                        () -> FuzzDedupTransactionGenerator.augment(engine, sqlExecutionContext, "source", input, keys, new Rnd(), 1, 1, 0));
                Assert.assertEquals("duplicate source table is empty", empty.getMessage());
                input.getQuick(1).structureVersion = 1;
                IllegalArgumentException schema = Assert.assertThrows(IllegalArgumentException.class,
                        () -> FuzzDedupTransactionGenerator.augment(engine, sqlExecutionContext, "source", input, keys, new Rnd(), 1, 1, 0));
                Assert.assertEquals("dedup augmentation requires fixed-schema insert transactions", schema.getMessage());
            } finally {
                Misc.freeObjListAndClear(input);
            }
        });
    }

    @Test
    public void testReplayCopiesAndDedupChangesWithParallelWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE source (ts TIMESTAMP, id LONG, sym SYMBOL, s STRING, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO source VALUES (0, 1, 'A', '', 'long copied varchar value'), (1, 2, null, null, null), (2, 3, 'B', 'copied string', '')");
            drainWalQueue();
            execute("CREATE TABLE replay AS (SELECT * FROM source) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            ObjList<FuzzTransaction> inserts = new ObjList<>();
            int structure;
            try (var metadata = engine.getLegacyMetadata(engine.verifyTableName("replay"))) {
                structure = (int) metadata.getMetadataVersion();
            }
            for (int i = 0; i < 8; i++) {
                FuzzTransaction transaction = new FuzzTransaction();
                transaction.structureVersion = structure;
                transaction.rollback = i == 3;
                for (int row = 0; row < 2; row++) {
                    transaction.operationList.add(new FuzzInsertOperation(17 + i, 41 + row, 100 + i * 2 + row,
                            0.1, 0.2, row == 0 ? 0 : 1, 20, new String[]{"A", "B", "C"}));
                }
                inserts.add(transaction);
            }
            IntList keys = new IntList();
            keys.add(0);
            keys.add(1);
            ObjList<FuzzTransaction> transactions = null;
            try {
                transactions = FuzzDedupTransactionGenerator.augment(engine, sqlExecutionContext, "source", inserts,
                        keys, new Rnd(123, 456), 4, 3, 0);
                Assert.assertEquals(12, transactions.size());
                int changes = 0;
                int rollbacks = 0;
                int data = 0;
                for (int i = 0; i < transactions.size(); i++) {
                    FuzzTransaction transaction = transactions.getQuick(i);
                    Assert.assertEquals(i, transaction.waitBarrierVersion);
                    Assert.assertEquals(structure + changes, transaction.structureVersion);
                    Assert.assertTrue(transaction.waitAllDone);
                    if (transaction.operationList.getQuick(0) instanceof FuzzDedupOperation dedup) {
                        Assert.assertEquals(changes % 2 == 0, dedup.isEnabled());
                        changes++;
                    } else {
                        int copies = 0;
                        int fresh = 0;
                        for (int j = 0; j < transaction.operationList.size(); j++) {
                            var operation = transaction.operationList.getQuick(j);
                            if (operation instanceof FuzzCopyRowOperation copy) {
                                Assert.assertTrue(copy.getTimestamp() >= 0 && copy.getTimestamp() <= 2);
                                copies++;
                            } else {
                                Assert.assertTrue(operation instanceof FuzzInsertOperation);
                                fresh++;
                            }
                        }
                        Assert.assertEquals(3, copies);
                        Assert.assertEquals(2, fresh);
                        rollbacks += transaction.rollback ? 1 : 0;
                        data++;
                    }
                }
                Assert.assertEquals(4, changes);
                Assert.assertEquals(8, data);
                Assert.assertEquals(1, rollbacks);
                // Closing the original operations must not affect the independently owned stream.
                Misc.freeObjListAndClear(inserts);
                FuzzRunner fuzzer = new FuzzRunner();
                fuzzer.withDb(engine, sqlExecutionContext);
                fuzzer.applyWal(transactions, "source", 1, new Rnd(5, 6));
                ObjList<ObjList<FuzzTransaction>> batches = new ObjList<>();
                batches.add(transactions);
                fuzzer.applyManyWalParallel(batches, new Rnd(7, 8), "replay", false, true);
                drainWalQueue();
                fuzzer.checkNoSuspendedTables();
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "source", "replay", LOG);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                        "SELECT count() FROM source", "SELECT count() FROM replay", LOG);
            } finally {
                Misc.freeObjListAndClear(inserts);
                if (transactions != null) {
                    Misc.freeObjListAndClear(transactions);
                }
            }
        });
    }
}
