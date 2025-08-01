/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalMetrics;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SeqTxnMetricsTest extends AbstractCairoTest {
    private static final HashMap<String, String> SERVER_ENV = new HashMap<>() {{
        put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
        put(PropertyKey.METRICS_ENABLED.getEnvVarName(), "true");
    }};
    private static final String TAG_SEQ_TXN = "questdb_wal_apply_seq_txn";
    private static final String TAG_WRITER_TXN = "questdb_wal_apply_writer_txn";

    @Test
    public void testMetricsConsistencyAfterRestartWithDroppedTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String survivor = "survivor_table";
            final String dropped = "table_to_drop";

            long expectedSeqTxn = 2;

            // First server instance - create tables, drop one
            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Create two tables
                execute(engine, "create table " + survivor + " (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + survivor + " values ('2024-01-01T00:00:00.000Z', 1)");
                execute(engine, "insert into " + survivor + " values ('2024-01-01T01:00:00.000Z', 2)");

                execute(engine, "create table " + dropped + " (ts timestamp, y int) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + dropped + " values ('2024-01-01T00:00:00.000Z', 10)");
                drainWalQueue(engine);

                // Drop one table
                execute(engine, "drop table " + dropped);
                drainWalQueue(engine);
                engine.releaseInactive();

                assertEquals(expectedSeqTxn, TestUtils.getPrometheusMetric(engine, TAG_SEQ_TXN));
                assertEquals(expectedSeqTxn, TestUtils.getPrometheusMetric(engine, TAG_WRITER_TXN));
            }

            // Restart and verify metrics match the state before restart (excluding dropped table)
            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Verify the persistent table's tracker is properly initialized
                TableToken token1 = engine.verifyTableName(survivor);
                SeqTxnTracker tracker1 = engine.getTableSequencerAPI().getTxnTracker(token1);
                assertTrue("Survivor table's seq txn tracker should be initialized", tracker1.isInitialised());
                assertEquals(expectedSeqTxn, tracker1.getSeqTxn());
                assertEquals(expectedSeqTxn, tracker1.getWriterTxn());

                // Total seq txn must be equal to the single survivor table's seq txn
                assertEquals(expectedSeqTxn, TestUtils.getPrometheusMetric(engine, TAG_SEQ_TXN));
                assertEquals(expectedSeqTxn, TestUtils.getPrometheusMetric(engine, TAG_WRITER_TXN));
            }
        });
    }

    @Test
    public void testMetricsDecreaseWhenTableDropped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String tableToDrop = "table_to_drop";
            final String tableToKeep = "table_to_keep";

            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Create tables and insert data
                execute(engine, "create table " + tableToDrop + " (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + tableToDrop + " values ('2024-01-01T00:00:00.000Z', 1)");
                execute(engine, "insert into " + tableToDrop + " values ('2024-01-01T01:00:00.000Z', 2)");

                execute(engine, "create table " + tableToKeep + " (ts timestamp, y int) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + tableToKeep + " values ('2024-01-01T00:00:00.000Z', 10)");

                drainWalQueue(engine);

                // Get metrics before dropping table
                WalMetrics walMetrics = engine.getMetrics().walMetrics();
                long seqTxnBeforeDrop = walMetrics.seqTxnGauge.getValue();
                long writerTxnBeforeDrop = walMetrics.writerTxnGauge.getValue();

                // Get the txn counts for the table that will be dropped
                TableToken tokenToDrop = engine.verifyTableName(tableToDrop);
                SeqTxnTracker trackerToDrop = engine.getTableSequencerAPI().getTxnTracker(tokenToDrop);
                long seqTxnToBeDropped = trackerToDrop.getSeqTxn();
                long writerTxnToBeDropped = trackerToDrop.getWriterTxn();

                assertTrue("Table to drop should have positive seqTxn", seqTxnToBeDropped > 0);
                assertTrue("Table to drop should have positive writerTxn", writerTxnToBeDropped > 0);

                // Drop the table
                execute(engine, "drop table " + tableToDrop);
                drainWalQueue(engine);
                engine.releaseInactive();

                // Verify metrics decreased by exactly the dropped table's count
                long seqTxnAfterDrop = walMetrics.seqTxnGauge.getValue();
                long writerTxnAfterDrop = walMetrics.writerTxnGauge.getValue();

                assertEquals("seqTxn should decrease by dropped table count",
                        seqTxnBeforeDrop - seqTxnToBeDropped, seqTxnAfterDrop);
                assertEquals("writerTxn should decrease by dropped table count",
                        writerTxnBeforeDrop - writerTxnToBeDropped, writerTxnAfterDrop);

                // Verify the kept table is still contributing
                TableToken tokenToKeep = engine.verifyTableName(tableToKeep);
                SeqTxnTracker trackerToKeep = engine.getTableSequencerAPI().getTxnTracker(tokenToKeep);
                assertTrue("Kept table tracker should still be initialized", trackerToKeep.isInitialised());
                assertTrue("Kept table should still contribute to metrics", trackerToKeep.getSeqTxn() > 0);
            }
        });
    }

    @Test
    public void testMetricsPersistenceAcrossDatabaseRestart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String table1 = "metrics_test_1";
            final String table2 = "metrics_test_2";
            final int txnCount1 = 2;
            final int txnCount2 = 3;
            final int txnCountTotal = txnCount1 + txnCount2;

            // First server instance - create tables and perform operations
            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Create first table and insert data
                execute(engine, "create table " + table1 +
                        " (ts timestamp, x int, s symbol) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + table1 + " values (0, 1, 'A')");
                execute(engine, "insert into " + table1 + " values (1, 2, 'B')");
                drainWalQueue(engine);

                // Create second table and insert data
                execute(engine, "create table " + table2 +
                        " (ts timestamp, y int, t symbol) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + table2 + " values (0, 10, 'X')");
                execute(engine, "insert into " + table2 + " values (1, 20, 'Y')");
                execute(engine, "insert into " + table2 + " values (2, 30, 'Z')");
                drainWalQueue(engine);

                // Verify individual tracker states
                TableToken token1 = engine.verifyTableName(table1);
                TableToken token2 = engine.verifyTableName(table2);
                SeqTxnTracker tracker1 = engine.getTableSequencerAPI().getTxnTracker(token1);
                SeqTxnTracker tracker2 = engine.getTableSequencerAPI().getTxnTracker(token2);
                assertTrue("Table1 tracker should be initialized", tracker1.isInitialised());
                assertTrue("Table2 tracker should be initialized", tracker2.isInitialised());
                assertEquals(txnCount1, tracker1.getSeqTxn());
                assertEquals(txnCount1, tracker1.getWriterTxn());
                assertEquals(txnCount2, tracker2.getSeqTxn());
                assertEquals(txnCount2, tracker2.getWriterTxn());

                // Get metrics before restart
                assertEquals(txnCountTotal, TestUtils.getPrometheusMetric(engine, TAG_SEQ_TXN));
                assertEquals(txnCountTotal, TestUtils.getPrometheusMetric(engine, TAG_WRITER_TXN));
            }

            // Restart and check metrics are the same
            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Verify individual trackers are properly initialized
                TableToken token1 = engine.verifyTableName(table1);
                TableToken token2 = engine.verifyTableName(table2);
                SeqTxnTracker tracker1 = engine.getTableSequencerAPI().getTxnTracker(token1);
                SeqTxnTracker tracker2 = engine.getTableSequencerAPI().getTxnTracker(token2);

                assertTrue("Table1 tracker should be initialized after restart", tracker1.isInitialised());
                assertTrue("Table2 tracker should be initialized after restart", tracker2.isInitialised());
                assertEquals(txnCount1, tracker1.getSeqTxn());
                assertEquals(txnCount1, tracker1.getWriterTxn());
                assertEquals(txnCount2, tracker2.getSeqTxn());
                assertEquals(txnCount2, tracker2.getWriterTxn());

                // Verify metrics are properly restored after restart
                WalMetrics walMetrics = engine.getMetrics().walMetrics();
                long actualSeqTxnTotal = walMetrics.seqTxnGauge.getValue();
                long actualWriterTxnTotal = walMetrics.writerTxnGauge.getValue();

                assertEquals("seqTxn metrics should persist across restart", txnCountTotal, actualSeqTxnTotal);
                assertEquals("writerTxn metrics should persist across restart", txnCountTotal, actualWriterTxnTotal);
            }
        });
    }

    @Test
    public void testRaceConditionPendingWalTransactionsDuringTableDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String testTable = "race_condition_table";

            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Create table and insert data
                execute(engine, "create table " + testTable + " (ts timestamp, x int, s symbol) timestamp(ts) partition by DAY WAL");

                // Insert multiple transactions but don't drain WAL queue yet (creates pending transactions)
                execute(engine, "insert into " + testTable + " values ('2024-01-01T00:00:00.000Z', 1, 'A')");
                execute(engine, "insert into " + testTable + " values ('2024-01-01T01:00:00.000Z', 2, 'B')");
                execute(engine, "insert into " + testTable + " values ('2024-01-01T02:00:00.000Z', 3, 'C')");

                // At this point, we have pending WAL transactions that haven't been applied to the table
                TableToken tableToken = engine.verifyTableName(testTable);
                SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

                // Verify we have pending transactions (seqTxn > writerTxn means there are unapplied transactions)
                long seqTxnBeforeApply = tracker.getSeqTxn();
                long writerTxnBeforeApply = tracker.getWriterTxn();
                assertTrue("Should have pending transactions", seqTxnBeforeApply > writerTxnBeforeApply);

                // Get global metrics before applying
                WalMetrics walMetrics = engine.getMetrics().walMetrics();
                long globalSeqTxnBefore = walMetrics.seqTxnGauge.getValue();
                long globalWriterTxnBefore = walMetrics.writerTxnGauge.getValue();

                // Now apply all pending transactions
                drainWalQueue(engine);

                // Verify transactions were applied (seqTxn == writerTxn when all transactions are applied)
                long seqTxnAfterApply = tracker.getSeqTxn();
                long writerTxnAfterApply = tracker.getWriterTxn();
                assertEquals("All transactions should be applied", seqTxnAfterApply, writerTxnAfterApply);

                // Verify global metrics are in sync after all WAL is applied
                long globalSeqTxnAfter = walMetrics.seqTxnGauge.getValue();
                long globalWriterTxnAfter = walMetrics.writerTxnGauge.getValue();

                assertTrue("Global seqTxn should have increased", globalSeqTxnAfter > globalSeqTxnBefore);
                assertTrue("Global writerTxn should have increased", globalWriterTxnAfter > globalWriterTxnBefore);

                // The key test: when all WAL is applied, the difference between seqTxn and writerTxn
                // should be the same at both table and global level (both should be 0 for a single table)
                long tableLag = seqTxnAfterApply - writerTxnAfterApply;
                assertEquals("Table should have no lag when all transactions applied", 0, tableLag);
                assertEquals("Seq and writer txn trackers should be in sync", seqTxnAfterApply, writerTxnAfterApply);

                // Now test the race condition: drop the table after transactions are applied
                // Record the table's contribution to metrics
                long tableSeqTxnContribution = tracker.getSeqTxn();
                long tableWriterTxnContribution = tracker.getWriterTxn();

                // Drop the table
                execute(engine, "drop table " + testTable);
                drainWalQueue(engine);
                engine.releaseInactive();

                // Verify metrics decreased by exactly the table's contribution
                long globalSeqTxnAfterDrop = walMetrics.seqTxnGauge.getValue();
                long globalWriterTxnAfterDrop = walMetrics.writerTxnGauge.getValue();

                assertEquals("Global seqTxn should decrease by table contribution",
                        globalSeqTxnAfter - tableSeqTxnContribution, globalSeqTxnAfterDrop);
                assertEquals("Global writerTxn should decrease by table contribution",
                        globalWriterTxnAfter - tableWriterTxnContribution, globalWriterTxnAfterDrop);

                // Final verification: global metrics should remain in sync after drop
                assertEquals("Global seq and writer txn should remain in sync after drop when no pending transactions",
                        globalSeqTxnAfterDrop, globalWriterTxnAfterDrop);
            }
        });
    }

    private void execute(CairoEngine engine, CharSequence sqlText) throws SqlException {
        SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        bindVariableService,
                        null,
                        -1,
                        circuitBreaker
                );
        context.initNow();
        engine.execute(sqlText, context);
    }

}
