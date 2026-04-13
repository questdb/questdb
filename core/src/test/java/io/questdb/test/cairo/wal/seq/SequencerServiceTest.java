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
import io.questdb.test.cairo.CairoTestConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests table creation with a mock SequencerService, simulating multi-primary
 * edge cases where a central sequencer coordinates table creation.
 */
public class SequencerServiceTest extends AbstractCairoTest {

    private static MockSequencerService mockSequencer;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        mockSequencer = new MockSequencerService();
        configurationFactory = (root, telemetryConfig, overrides) ->
                new CairoTestConfiguration(root, telemetryConfig, overrides) {
                    @Override
                    public SequencerServiceFactory getSequencerServiceFactory() {
                        return (engine, config) -> {
                            // Wrap mock with a local service for file operations
                            mockSequencer.setDelegate(new LocalSequencerService(
                                    engine,
                                    config,
                                    new io.questdb.std.ConcurrentHashMap<>(false),
                                    dir -> new io.questdb.cairo.wal.seq.SeqTxnTracker(config)
                            ));
                            return mockSequencer;
                        };
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCreateTableIfNotExistsWhenTableAlreadyExistsLocally() throws Exception {
        // When a table already exists locally, IF NOT EXISTS returns early without
        // calling registerTable at all (name lock check finds existing table).
        assertMemoryLeak(() -> {
            mockSequencer.reset();

            // First create succeeds normally
            execute("CREATE TABLE trades (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            Assert.assertEquals(1, mockSequencer.registerCount);

            // Second IF NOT EXISTS — table exists locally, so CairoEngine returns the
            // existing token immediately during name locking, without calling registerTable
            execute("CREATE TABLE IF NOT EXISTS trades (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // registerTable NOT called again — reconciled locally during name lock
            Assert.assertEquals(1, mockSequencer.registerCount);
        });
    }

    @Test
    public void testCreateTableRefusedButExistsLocally() throws Exception {
        // Simulates: table exists locally and on the sequencer. A second CREATE TABLE
        // with IF NOT EXISTS should succeed by reconciling locally, even if the sequencer
        // would refuse the registration (because it already has the table).
        assertMemoryLeak(() -> {
            mockSequencer.reset();

            // Create table — this succeeds
            execute("CREATE TABLE local_exists (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            Assert.assertEquals(1, mockSequencer.registerCount);

            // Now make sequencer refuse (simulating it already knows about this table)
            mockSequencer.refuseNextRegister = true;
            mockSequencer.refuseDbVersion = 10;

            // IF NOT EXISTS — the name lock check finds the table locally, returns early
            // without even calling registerTable
            execute("CREATE TABLE IF NOT EXISTS local_exists (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // registerTable NOT called again — reconciled during name lock
            Assert.assertEquals(1, mockSequencer.registerCount);
        });
    }

    @Test
    public void testCreateTableRefusedNoLocalTable() throws Exception {
        assertMemoryLeak(() -> {
            mockSequencer.reset();

            // Make mock refuse registration (simulating stale db version)
            // but no local table exists with this name
            mockSequencer.refuseNextRegister = true;
            mockSequencer.refuseDbVersion = 10;

            try {
                execute("CREATE TABLE unknown_remote (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                Assert.fail("Should have thrown");
            } catch (Exception e) {
                // SqlException wraps CairoException — check the message
                Assert.assertTrue(e.getMessage().contains("table registration refused"));
                Assert.assertTrue(e.getMessage().contains("serviceVersion=10"));
            }
        });
    }

    @Test
    public void testCreateTableSucceedsWhenServiceAccepts() throws Exception {
        assertMemoryLeak(() -> {
            mockSequencer.reset();
            execute("CREATE TABLE orders (ts TIMESTAMP, qty INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Table should be created successfully
            assertSql("ts\tqty\n", "SELECT * FROM orders");
            Assert.assertEquals(1, mockSequencer.registerCount);
        });
    }

    @Test
    public void testDatabaseVersionIncrementsOnCreate() throws Exception {
        assertMemoryLeak(() -> {
            mockSequencer.reset();
            long v0 = mockSequencer.getDatabaseVersion();

            execute("CREATE TABLE t1 (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long v1 = mockSequencer.getDatabaseVersion();
            Assert.assertTrue("dbVersion should increase after create", v1 > v0);

            execute("CREATE TABLE t2 (ts TIMESTAMP, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long v2 = mockSequencer.getDatabaseVersion();
            Assert.assertTrue("dbVersion should increase after second create", v2 > v1);
        });
    }

    @Test
    public void testListenerCalledOnCreate() throws Exception {
        assertMemoryLeak(() -> {
            mockSequencer.reset();

            execute("CREATE TABLE evented (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            Assert.assertNotNull("onTableRegistered should have been called", mockSequencer.lastRegisteredTable);
            Assert.assertEquals("evented", mockSequencer.lastRegisteredTable.getTableName());
        });
    }

    @Test
    public void testSequencerCleanupOnDropTable() throws Exception {
        assertMemoryLeak(() -> {
            mockSequencer.reset();

            execute("CREATE TABLE to_drop (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            Assert.assertEquals(0, mockSequencer.dropCount);

            execute("DROP TABLE to_drop");
            Assert.assertEquals(1, mockSequencer.dropCount);
        });
    }

    /**
     * Mock SequencerService that wraps a real LocalSequencerService but allows
     * controlling behavior for testing edge cases.
     */
    private static class MockSequencerService implements SequencerService {
        int dropCount;
        TableToken lastRegisteredTable;
        int refuseDbVersion;
        boolean refuseNextRegister;
        int registerCount;
        private LocalSequencerService delegate;

        @Override
        public void applyRename(TableToken tableToken) {
            delegate.applyRename(tableToken);
        }

        @Override
        public void close() {
            if (delegate != null) {
                delegate.close();
            }
        }

        @Override
        public long dropTable(TableToken tableToken) {
            dropCount++;
            return delegate.dropTable(tableToken);
        }

        @Override
        public void forAllWalTables(ObjHashSet<TableToken> bucket, boolean includeDropped, TableSequencerCallback callback) {
            delegate.forAllWalTables(bucket, includeDropped, callback);
        }

        @Override
        public @NotNull TransactionLogCursor getCursor(TableToken tableToken, long seqTxn) {
            return delegate.getCursor(tableToken, seqTxn);
        }

        @Override
        public long getDatabaseVersion() {
            return delegate.getDatabaseVersion();
        }

        @Override
        public @NotNull TableMetadataChangeLog getMetadataChangeLog(TableToken tableToken, long structureVersionLo) {
            return delegate.getMetadataChangeLog(tableToken, structureVersionLo);
        }

        @Override
        public int getNextWalId(TableToken tableToken) {
            return delegate.getNextWalId(tableToken);
        }

        @Override
        public long getTableMetadata(TableToken tableToken, TableRecordMetadataSink sink) {
            return delegate.getTableMetadata(tableToken, sink);
        }

        @Override
        public void initSequencerFiles(int tableId, TableStructure structure, TableToken tableToken) {
            delegate.initSequencerFiles(tableId, structure, tableToken);
        }

        @Override
        public long lastTxn(TableToken tableToken) {
            return delegate.lastTxn(tableToken);
        }

        @Override
        public long nextStructureTxn(TableToken tableToken, long structureVersion, AlterOperation alterOp) {
            return delegate.nextStructureTxn(tableToken, structureVersion, alterOp);
        }

        @Override
        public long nextTxn(TableToken tableToken, int walId, long expectedSchemaVersion, int segmentId, int segmentTxn, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount) {
            return delegate.nextTxn(tableToken, walId, expectedSchemaVersion, segmentId, segmentTxn, txnMinTimestamp, txnMaxTimestamp, txnRowCount);
        }

        @Override
        public long registerTable(int tableId, TableStructure structure, TableToken tableToken, long callerDatabaseVersion) {
            registerCount++;
            lastRegisteredTable = tableToken;

            if (refuseNextRegister) {
                refuseNextRegister = false;
                return -refuseDbVersion;
            }

            return delegate.registerTable(tableId, structure, tableToken, callerDatabaseVersion);
        }

        @Override
        public boolean releaseAll() {
            return delegate.releaseAll();
        }

        @Override
        public boolean releaseInactive() {
            return delegate.releaseInactive();
        }

        @Override
        public TableToken reload(TableToken tableToken) {
            return delegate.reload(tableToken);
        }

        @Override
        public void setListener(SequencerServiceListener listener) {
            delegate.setListener(listener);
        }

        void reset() {
            registerCount = 0;
            dropCount = 0;
            refuseNextRegister = false;
            refuseDbVersion = 0;
            lastRegisteredTable = null;
        }

        void setDelegate(LocalSequencerService delegate) {
            this.delegate = delegate;
        }
    }
}
