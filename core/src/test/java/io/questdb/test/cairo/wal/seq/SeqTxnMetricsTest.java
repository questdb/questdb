/*******************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.HashMap;

import static io.questdb.test.tools.TestUtils.assertEventually;
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
            final String toKeep = "table_to_keep";
            final String toDrop = "table_to_drop";

            final long expectedSeqTxn = 2;

            // First server instance - create tables, drop one
            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Create two tables
                execute(engine, "create table " + toKeep + " (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + toKeep + " values ('2024-01-01T00:00:00.000Z', 1)");
                execute(engine, "insert into " + toKeep + " values ('2024-01-01T01:00:00.000Z', 2)");

                execute(engine, "create table " + toDrop + " (ts timestamp, y int) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + toDrop + " values ('2024-01-01T00:00:00.000Z', 10)");
                drainWalQueue(engine);

                // Drop one table
                execute(engine, "drop table " + toDrop);
                drainWalQueue(engine);
                engine.releaseInactive();

                assertEquals(expectedSeqTxn, TestUtils.getMetricValue(engine, TAG_SEQ_TXN));
                assertEquals(expectedSeqTxn, TestUtils.getMetricValue(engine, TAG_WRITER_TXN));
            }

            // Restart and verify metrics match the state before restart (excluding dropped table)
            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Verify the kept table's tracker is properly initialized
                TableToken token1 = engine.verifyTableName(toKeep);
                SeqTxnTracker tracker1 = engine.getTableSequencerAPI().getTxnTracker(token1);
                assertEventually(() -> {
                    assertTrue("Survivor table's seq txn tracker should be initialized", tracker1.isInitialised());
                });
                assertEquals(expectedSeqTxn, tracker1.getSeqTxn());
                assertEquals(expectedSeqTxn, tracker1.getWriterTxn());

                // Total seq txn must be equal to the single kept table's seq txn
                assertEquals(expectedSeqTxn, TestUtils.getMetricValue(engine, TAG_SEQ_TXN));
                assertEquals(expectedSeqTxn, TestUtils.getMetricValue(engine, TAG_WRITER_TXN));
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
                execute(engine, "insert into " + tableToDrop + " values (1, 1)");
                execute(engine, "insert into " + tableToDrop + " values (2, 2)");

                execute(engine, "create table " + tableToKeep + " (ts timestamp, y int) timestamp(ts) partition by DAY WAL");
                execute(engine, "insert into " + tableToKeep + " values (0, 10)");

                drainWalQueue(engine);

                // Get metrics before dropping table
                long seqTxnMetricBeforeDrop = TestUtils.getMetricValue(engine, TAG_SEQ_TXN);
                long writerTxnMetricBeforeDrop = TestUtils.getMetricValue(engine, TAG_WRITER_TXN);

                // Get the txn counts for the table that will be dropped
                TableToken tokenToDrop = engine.verifyTableName(tableToDrop);
                SeqTxnTracker trackerToDrop = engine.getTableSequencerAPI().getTxnTracker(tokenToDrop);
                assertTrue("table_to_drop's seq txn tracker should be initialized", trackerToDrop.isInitialised());
                long seqTxnToBeDropped = trackerToDrop.getSeqTxn();
                long writerTxnToBeDropped = trackerToDrop.getWriterTxn();

                assertTrue("Table to drop should have positive seqTxn", seqTxnToBeDropped > 0);
                assertTrue("Table to drop should have positive writerTxn", writerTxnToBeDropped > 0);

                // Drop the table
                execute(engine, "drop table " + tableToDrop);
                drainWalQueue(engine);
                engine.releaseInactive();

                // Verify metrics decreased by exactly the dropped table's txn count
                long seqTxnMetricAfterDrop = TestUtils.getMetricValue(engine, TAG_SEQ_TXN);
                long writerTxnMetricAfterDrop = TestUtils.getMetricValue(engine, TAG_WRITER_TXN);

                assertEquals("seqTxn should decrease by dropped table count",
                        seqTxnMetricBeforeDrop - seqTxnToBeDropped, seqTxnMetricAfterDrop);
                assertEquals("writerTxn should decrease by dropped table count",
                        writerTxnMetricBeforeDrop - writerTxnToBeDropped, writerTxnMetricAfterDrop);
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
                assertEventually(() -> {
                    assertTrue("Table1 tracker should be initialized", tracker1.isInitialised());
                    assertTrue("Table2 tracker should be initialized", tracker2.isInitialised());
                });
                assertEquals(txnCount1, tracker1.getSeqTxn());
                assertEquals(txnCount1, tracker1.getWriterTxn());
                assertEquals(txnCount2, tracker2.getSeqTxn());
                assertEquals(txnCount2, tracker2.getWriterTxn());

                // Get metrics before restart
                assertEquals(txnCountTotal, TestUtils.getMetricValue(engine, TAG_SEQ_TXN));
                assertEquals(txnCountTotal, TestUtils.getMetricValue(engine, TAG_WRITER_TXN));
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

                assertEventually(() -> {
                    assertTrue("Table1 tracker should be initialized after restart", tracker1.isInitialised());
                    assertTrue("Table2 tracker should be initialized after restart", tracker2.isInitialised());
                });
                assertEquals(txnCount1, tracker1.getSeqTxn());
                assertEquals(txnCount1, tracker1.getWriterTxn());
                assertEquals(txnCount2, tracker2.getSeqTxn());
                assertEquals(txnCount2, tracker2.getWriterTxn());

                // Verify metrics are properly restored after restart
                long actualSeqTxnTotal = TestUtils.getMetricValue(engine, TAG_SEQ_TXN);
                long actualWriterTxnTotal = TestUtils.getMetricValue(engine, TAG_WRITER_TXN);

                assertEquals("seqTxn metrics should persist across restart", txnCountTotal, actualSeqTxnTotal);
                assertEquals("writerTxn metrics should persist across restart", txnCountTotal, actualWriterTxnTotal);
            }
        });
    }

    @Test
    public void testRaceConditionPendingWalTransactionsDuringTableDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String toKeep = "table_to_keep";
            final String toDrop = "table_to_drop";

            try (final ServerMain serverMain = ServerMain.create(root, SERVER_ENV)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Create tables
                execute(engine, "create table " + toKeep + " (ts timestamp, x int, s symbol) timestamp(ts) partition by DAY WAL");
                execute(engine, "create table " + toDrop + " (ts timestamp, x int) timestamp(ts) partition by DAY WAL");

                // Insert multiple transactions but don't drain WAL queue yet (creates pending transactions)
                execute(engine, "insert into " + toDrop + " values (1, 1)");
                execute(engine, "insert into " + toDrop + " values (2, 2)");
                execute(engine, "insert into " + toKeep + " values (0, 1, 'A')");
                execute(engine, "insert into " + toKeep + " values (1, 2, 'B')");
                execute(engine, "insert into " + toKeep + " values (2, 3, 'C')");

                // Drop the table with transactions in flight. They'll never get applied,
                // but writerTxn metric must stay in sync with seqTxn metric.
                execute(engine, "drop table " + toDrop);

                SeqTxnTracker toKeepTracker = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName(toKeep));
                assertEventually(() -> {
                    assertTrue("table_to_keep's seq txn tracker should be initialized", toKeepTracker.isInitialised());
                });
                assertTrue("tracker's seq txn should be ahead of writer txn",
                        toKeepTracker.getSeqTxn() > toKeepTracker.getWriterTxn());

                long seqTxnMetricBefore = TestUtils.getMetricValue(engine, TAG_SEQ_TXN);
                long writerTxnMetricBefore = TestUtils.getMetricValue(engine, TAG_WRITER_TXN);
                assertTrue("seqTxn metric should be ahead of writerTxn",
                        seqTxnMetricBefore > writerTxnMetricBefore);

                // Now apply all pending transactions
                drainWalQueue(engine);

                // Check that the kept table's writer txn has caught up
                assertEquals("writer txn should have caught up with seq txn",
                        toKeepTracker.getSeqTxn(), toKeepTracker.getWriterTxn());

                // Check that writer txn metric has caught up to seq txn metric,
                // even though the txns themselves were never applied
                long seqTxnMetricAfter = TestUtils.getMetricValue(engine, TAG_SEQ_TXN);
                long writerTxnMetricAfter = TestUtils.getMetricValue(engine, TAG_WRITER_TXN);
                assertEquals("seq txn should have stayed the same", seqTxnMetricBefore, seqTxnMetricAfter);
                assertEquals("writer txn should have caught up wit seq txn", seqTxnMetricAfter, writerTxnMetricAfter);
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
