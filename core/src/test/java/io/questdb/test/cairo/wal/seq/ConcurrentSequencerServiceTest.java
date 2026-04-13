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

package io.questdb.test.cairo.wal.seq;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.LocalSequencerService;
import io.questdb.cairo.wal.seq.SequencerService;
import io.questdb.cairo.wal.seq.SequencerServiceFactory;
import io.questdb.cairo.wal.seq.SequencerServiceListener;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableRecordMetadataSink;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.ObjHashSet;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QuestDBTestNode;
import io.questdb.test.cairo.CairoTestConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests concurrent table creation across two engines sharing a single
 * central sequencer, simulating multi-primary coordination.
 */
public class ConcurrentSequencerServiceTest extends AbstractCairoTest {

    private static final AtomicInteger nodeCounter = new AtomicInteger(10);
    private static CentralSequencer centralSequencer;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        centralSequencer = new CentralSequencer();
        configurationFactory = (root, telemetryConfig, overrides) ->
                new CairoTestConfiguration(root, telemetryConfig, overrides) {
                    @Override
                    public boolean isWalSupported() {
                        return true;
                    }

                    @Override
                    public SequencerServiceFactory getSequencerServiceFactory() {
                        return (engine, config) -> {
                            LocalSequencerService localService = new LocalSequencerService(
                                    engine,
                                    config,
                                    new io.questdb.std.ConcurrentHashMap<>(false),
                                    dir -> new io.questdb.cairo.wal.seq.SeqTxnTracker(config)
                            );
                            return centralSequencer.createProxy(localService);
                        };
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testConcurrentCreateSameTable() throws Exception {
        // Two engines race to create the same table via IF NOT EXISTS.
        // The central sequencer allows only the first registration.
        centralSequencer.reset();

        QuestDBTestNode node2 = newNode(nodeCounter.incrementAndGet());
        node2.initGriffin();
        CairoEngine engine2 = node2.getEngine();

        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Throwable> error1 = new AtomicReference<>();
        AtomicReference<Throwable> error2 = new AtomicReference<>();

        String ddl = "CREATE TABLE IF NOT EXISTS concurrent_tbl "
                + "(ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL";

        Thread t1 = new Thread(() -> {
            try {
                barrier.await();
                engine.execute(ddl);
            } catch (Throwable e) {
                error1.set(e);
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                barrier.await();
                engine2.execute(ddl, node2.getSqlExecutionContext());
            } catch (Throwable e) {
                error2.set(e);
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // At least one must succeed
        Assert.assertTrue(
                "at least one engine must succeed, error1=" + error1.get()
                        + ", error2=" + error2.get(),
                error1.get() == null || error2.get() == null
        );

        // The central sequencer received registrations
        Assert.assertTrue("registerTable should be called",
                centralSequencer.totalRegisterCount.get() >= 1);
    }

    @Test
    public void testSequentialCreateSameTableSecondRefused() throws Exception {
        // Engine 1 creates a table. Engine 2 tries to create the same table
        // (without IF NOT EXISTS) and is refused by the central sequencer.
        centralSequencer.reset();

        QuestDBTestNode node2 = newNode(nodeCounter.incrementAndGet());
        node2.initGriffin();
        CairoEngine engine2 = node2.getEngine();

        // Engine 1 creates the table
        execute("CREATE TABLE shared_tbl (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        Assert.assertEquals(1, centralSequencer.totalRegisterCount.get());

        // Engine 2 tries the same (no IF NOT EXISTS) — should fail
        try {
            engine2.execute(
                    "CREATE TABLE shared_tbl (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL",
                    node2.getSqlExecutionContext()
            );
            Assert.fail("Should have thrown — table already registered on central sequencer");
        } catch (Exception e) {
            Assert.assertTrue("expected refusal, got: " + e.getMessage(),
                    e.getMessage().contains("table registration refused")
                            || e.getMessage().contains("table exists"));
        }

        // Both calls reached the sequencer
        Assert.assertEquals(2, centralSequencer.totalRegisterCount.get());
    }

    @Test
    public void testTwoEnginesCreateDifferentTables() throws Exception {
        // Two engines each create a different table — both succeed.
        centralSequencer.reset();

        QuestDBTestNode node2 = newNode(nodeCounter.incrementAndGet());
        node2.initGriffin();
        CairoEngine engine2 = node2.getEngine();

        // Verify engine2 is wired to the shared sequencer
        Assert.assertNotNull(
                "engine2 config must provide sequencer factory",
                engine2.getConfiguration().getSequencerServiceFactory()
        );
        int countBefore = centralSequencer.totalRegisterCount.get();

        // Verify engine2 has WAL support
        Assert.assertTrue("engine2 must support WAL", engine2.getConfiguration().isWalSupported());

        execute("CREATE TABLE alpha (ts TIMESTAMP, a INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        Assert.assertEquals(countBefore + 1, centralSequencer.totalRegisterCount.get());

        // Use engine2 directly
        engine2.execute(
                "CREATE TABLE beta (ts TIMESTAMP, b INT) TIMESTAMP(ts) PARTITION BY DAY WAL",
                node2.getSqlExecutionContext()
        );

        // Verify beta was created as WAL
        TableToken betaToken = engine2.getTableTokenIfExists("beta");
        Assert.assertNotNull("beta should exist on engine2", betaToken);
        Assert.assertTrue("beta should be WAL", betaToken.isWal());

        Assert.assertEquals(countBefore + 2, centralSequencer.totalRegisterCount.get());
    }

    /**
     * Simulates a central sequencer service shared across multiple nodes.
     * Tracks registered table names globally — only one node can register
     * a given table name; subsequent attempts are refused.
     */
    private static class CentralSequencer {
        final AtomicInteger totalRegisterCount = new AtomicInteger();
        private final AtomicLong databaseVersion = new AtomicLong();
        private final ConcurrentHashMap<String, Boolean> registeredTables = new ConcurrentHashMap<>();

        NodeProxy createProxy(LocalSequencerService localService) {
            return new NodeProxy(localService);
        }

        void reset() {
            totalRegisterCount.set(0);
            registeredTables.clear();
            databaseVersion.set(0);
        }

        private class NodeProxy implements SequencerService {
            private final LocalSequencerService local;

            NodeProxy(LocalSequencerService local) {
                this.local = local;
            }

            @Override
            public void applyRename(TableToken tableToken) {
                local.applyRename(tableToken);
            }

            @Override
            public void close() {
                local.close();
            }

            @Override
            public long dropTable(TableToken tableToken) {
                registeredTables.remove(tableToken.getTableName());
                return local.dropTable(tableToken);
            }

            @Override
            public void forAllWalTables(ObjHashSet<TableToken> bucket, boolean includeDropped, TableSequencerCallback callback) {
                local.forAllWalTables(bucket, includeDropped, callback);
            }

            @Override
            public @NotNull TransactionLogCursor getCursor(TableToken tableToken, long seqTxn) {
                return local.getCursor(tableToken, seqTxn);
            }

            @Override
            public long getDatabaseVersion() {
                return databaseVersion.get();
            }

            @Override
            public @NotNull TableMetadataChangeLog getMetadataChangeLog(TableToken tableToken, long structureVersionLo) {
                return local.getMetadataChangeLog(tableToken, structureVersionLo);
            }

            @Override
            public int getNextWalId(TableToken tableToken) {
                return local.getNextWalId(tableToken);
            }

            @Override
            public long getTableMetadata(TableToken tableToken, TableRecordMetadataSink sink) {
                return local.getTableMetadata(tableToken, sink);
            }

            @Override
            public void initSequencerFiles(int tableId, TableStructure structure, TableToken tableToken) {
                local.initSequencerFiles(tableId, structure, tableToken);
            }

            @Override
            public long lastTxn(TableToken tableToken) {
                return local.lastTxn(tableToken);
            }

            @Override
            public long nextStructureTxn(TableToken tableToken, long structureVersion, AlterOperation alterOp) {
                return local.nextStructureTxn(tableToken, structureVersion, alterOp);
            }

            @Override
            public long nextTxn(TableToken tableToken, int walId, long expectedSchemaVersion, int segmentId, int segmentTxn, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount) {
                return local.nextTxn(tableToken, walId, expectedSchemaVersion, segmentId, segmentTxn, txnMinTimestamp, txnMaxTimestamp, txnRowCount);
            }

            @Override
            public long registerTable(int tableId, TableStructure structure, TableToken tableToken, long callerDatabaseVersion) {
                totalRegisterCount.incrementAndGet();

                // Central coordination: first registration wins
                if (registeredTables.putIfAbsent(tableToken.getTableName(), Boolean.TRUE) != null) {
                    // Already registered by another node
                    long version = databaseVersion.get();
                    return version == 0 ? -1 : -version;
                }

                long result = local.registerTable(tableId, structure, tableToken, callerDatabaseVersion);
                databaseVersion.incrementAndGet();
                return result;
            }

            @Override
            public boolean releaseAll() {
                return local.releaseAll();
            }

            @Override
            public boolean releaseInactive() {
                return local.releaseInactive();
            }

            @Override
            public TableToken reload(TableToken tableToken) {
                return local.reload(tableToken);
            }

            @Override
            public void setListener(SequencerServiceListener listener) {
                local.setListener(listener);
            }
        }
    }
}
