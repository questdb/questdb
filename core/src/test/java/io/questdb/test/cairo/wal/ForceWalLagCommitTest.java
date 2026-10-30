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
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.std.Files;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class ForceWalLagCommitTest extends AbstractCairoTest {
    @Test
    public void testEmptyTableWithPersistedLag() throws Exception {
        configureForLag();
        final NoWalReadFilesFacade ff = new NoWalReadFilesFacade();
        assertMemoryLeak(ff, () -> {
            createVariableTable();
            final TableToken token = engine.verifyTableName("lag");
            execute("INSERT INTO lag VALUES (1, 'new', 'later', 'out of line varchar payload', from_base64('AQID'), '2024-01-02T10:00:00')");
            execute("INSERT INTO lag VALUES (2, null, null, null, null, '2024-01-01T10:00:00')");
            execute("INSERT INTO lag VALUES (99, 'tail', 'not applied', 'tail', null, '2024-01-03')");

            applyTransactionsToLag(token, 2, 2, false);
            try (TableWriter writer = getWriter(token)) {
                Assert.assertEquals(0, writer.getRowCount());
                Assert.assertEquals(1, writer.getPartitionCount());
                Assert.assertEquals(0, writer.getPartitionSize(0));
                Assert.assertEquals(MicrosTimestampDriver.floor("2024-01-02"), writer.getPartitionTimestamp(0));
            }
            forceCurrentLagThenReopen(ff, token, 2, 2, 2, false);

            try (TableWriter writer = getWriter(token)) {
                Assert.assertEquals(2, writer.getPartitionCount());
                Assert.assertEquals(MicrosTimestampDriver.floor("2024-01-01"), writer.getPartitionTimestamp(0));
                Assert.assertEquals(MicrosTimestampDriver.floor("2024-01-02"), writer.getPartitionTimestamp(1));
            }
            assertQuery("SELECT x, sym, s, vc, b, ts FROM lag")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tsym\ts\tvc\tb\tts
                            2\t\t\t\t\t2024-01-01T10:00:00.000000Z
                            1\tnew\tlater\tout of line varchar payload\t00000000 01 02 03\t2024-01-02T10:00:00.000000Z
                            """);
            assertQuery("SELECT x FROM lag WHERE sym = 'new'").noLeakCheck().sizeMayVary().returns("x\n1\n");
        });
    }

    @Test
    public void testKeyedDedupFromPersistedLag() throws Exception {
        configureForLag();
        final NoWalReadFilesFacade ff = new NoWalReadFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE lag (x LONG, k INT, sym SYMBOL INDEX, s STRING, vc VARCHAR, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, sym, vc)");
            execute("INSERT INTO lag VALUES (0, 1, 'a', 'base', 'long varchar dedup key', '2024-01-02T01:00:00')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("lag");

            execute("""
                    INSERT INTO lag VALUES
                    (1, 1, 'a', 'first', 'long varchar dedup key', '2024-01-02T01:00:00'),
                    (3, 2, null, 'first null key', null, '2024-01-02T01:00:00')
                    """);
            execute("""
                    INSERT INTO lag VALUES
                    (2, 1, 'a', 'last', 'long varchar dedup key', '2024-01-02T01:00:00'),
                    (4, 2, null, 'last null key', null, '2024-01-02T01:00:00'),
                    (5, 3, 'new', 'different fixed key', 'inline', '2024-01-02T01:00:00')
                    """);
            execute("INSERT INTO lag VALUES (99, 9, 'tail', 'not applied', 'tail', '2024-01-03')");

            applyTransactionsToLag(token, 2, 5, false);
            forceCurrentLagThenReopen(ff, token, 3, 2, 5, false);

            assertQuery("SELECT x, k, sym, s, vc, ts FROM lag ORDER BY ts, k")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tk\tsym\ts\tvc\tts
                            2\t1\ta\tlast\tlong varchar dedup key\t2024-01-02T01:00:00.000000Z
                            4\t2\t\tlast null key\t\t2024-01-02T01:00:00.000000Z
                            5\t3\tnew\tdifferent fixed key\tinline\t2024-01-02T01:00:00.000000Z
                            """);
            assertQuery("SELECT x FROM lag WHERE sym = 'a'").noLeakCheck().sizeMayVary().returns("x\n2\n");
            assertQuery("SELECT x FROM lag WHERE sym IS NULL").noLeakCheck().sizeMayVary().returns("x\n4\n");
        });
    }

    @Test
    public void testOrderedPersistedLagWithVariableColumnsAndIndex() throws Exception {
        configureForLag();
        final NoWalReadFilesFacade ff = new NoWalReadFilesFacade();
        assertMemoryLeak(ff, () -> {
            createVariableTable();
            execute("INSERT INTO lag VALUES (0, 'seed', 'base', 'base', from_base64('AA=='), '2024-01-02T08:00:00')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("lag");

            execute("""
                    INSERT INTO lag VALUES
                    (1, 'new', 'first', 'inline', from_base64('AQID'), '2024-01-02T10:00:00'),
                    (2, null, null, null, null, '2024-01-02T10:00:00'),
                    (3, 'new', 'third', 'out of line varchar payload', from_base64('BAUGBw=='), '2024-01-02T10:00:00')
                    """);
            // The earlier following transaction makes lookahead retain the ordered batch in lag.
            execute("INSERT INTO lag VALUES (99, 'tail', 'not applied', 'tail', null, '2024-01-02T09:00:00')");
            execute("INSERT INTO lag VALUES (100, 'tail', 'not applied', 'tail', null, '2024-01-03')");

            applyTransactionsToLag(token, 1, 3, true);
            forceCurrentLagThenReopen(ff, token, 2, 1, 3, true);

            assertQuery("SELECT x, sym, s, vc, b, ts FROM lag ORDER BY ts, x")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tsym\ts\tvc\tb\tts
                            0\tseed\tbase\tbase\t00000000 00\t2024-01-02T08:00:00.000000Z
                            1\tnew\tfirst\tinline\t00000000 01 02 03\t2024-01-02T10:00:00.000000Z
                            2\t\t\t\t\t2024-01-02T10:00:00.000000Z
                            3\tnew\tthird\tout of line varchar payload\t00000000 04 05 06 07\t2024-01-02T10:00:00.000000Z
                            """);
            assertQuery("SELECT x FROM lag WHERE sym = 'new' ORDER BY x")
                    .noLeakCheck().sizeMayVary().returns("x\n1\n3\n");
            assertQuery("SELECT x FROM lag WHERE sym IS NULL")
                    .noLeakCheck().sizeMayVary().returns("x\n2\n");
        });
    }

    @Test
    public void testTimestampDedupFromPersistedLag() throws Exception {
        configureForLag();
        final NoWalReadFilesFacade ff = new NoWalReadFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE lag (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO lag VALUES (0, '2024-01-02T01:00:00')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("lag");
            execute("INSERT INTO lag VALUES (1, '2024-01-02T01:00:00')");
            execute("INSERT INTO lag VALUES (2, '2024-01-02T01:00:00')");
            execute("INSERT INTO lag VALUES (99, '2024-01-03')");

            applyTransactionsToLag(token, 2, 2, false);
            forceCurrentLagThenReopen(ff, token, 3, 2, 2, false);

            assertQuery("lag").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\tts
                    2\t2024-01-02T01:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testUnorderedPersistedLagAcrossPartitions() throws Exception {
        configureForLag();
        final NoWalReadFilesFacade ff = new NoWalReadFilesFacade();
        assertMemoryLeak(ff, () -> {
            createVariableTable();
            execute("INSERT INTO lag VALUES (0, 'seed', 'base', 'base', from_base64('AA=='), '2024-01-02T08:00:00')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("lag");

            execute("INSERT INTO lag VALUES (1, 'new', 'later', 'out of line varchar payload', from_base64('AQID'), '2024-01-03T01:00:00')");
            execute("INSERT INTO lag VALUES (2, null, null, null, null, '2024-01-01T01:00:00')");
            execute("INSERT INTO lag VALUES (99, 'tail', 'not applied', 'tail', null, '2024-01-04')");

            applyTransactionsToLag(token, 2, 2, false);
            forceCurrentLagThenReopen(ff, token, 3, 2, 2, false);

            assertQuery("SELECT x, sym, s, vc, b, ts FROM lag")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tsym\ts\tvc\tb\tts
                            2\t\t\t\t\t2024-01-01T01:00:00.000000Z
                            0\tseed\tbase\tbase\t00000000 00\t2024-01-02T08:00:00.000000Z
                            1\tnew\tlater\tout of line varchar payload\t00000000 01 02 03\t2024-01-03T01:00:00.000000Z
                            """);
            assertQuery("SELECT x FROM lag WHERE sym = 'new'").noLeakCheck().sizeMayVary().returns("x\n1\n");
            assertQuery("SELECT x FROM lag WHERE sym IS NULL").noLeakCheck().sizeMayVary().returns("x\n2\n");
        });
    }

    private static void assertCommitted(TableWriter writer, long targetTxn) {
        Assert.assertEquals(targetTxn, writer.getSeqTxn());
        Assert.assertEquals(targetTxn, writer.getAppliedSeqTxn());
        assertLag(writer, 0, 0, true);
        Assert.assertEquals(Long.MAX_VALUE, writer.getTxWriter().getLagMinTimestamp());
        Assert.assertEquals(Long.MIN_VALUE, writer.getTxWriter().getLagMaxTimestamp());
    }

    private static void assertLag(TableWriter writer, int txnCount, int rowCount, boolean isOrdered) {
        final TxWriter tx = writer.getTxWriter();
        Assert.assertEquals(txnCount, tx.getLagTxnCount());
        Assert.assertEquals(rowCount, tx.getLagRowCount());
        Assert.assertEquals(isOrdered, tx.isLagOrdered());
    }

    private static void configureForLag() {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setCurrentMicros(1_000_000);
    }

    private void applyTransactionsToLag(TableToken token, int txnCount, int rowCount, boolean isOrdered) {
        final TableWriterPressureControl pressure = engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl();
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
        pressure.setMaxBlockRowCount(1);
        final MicrosecondClock savedClock = testMicrosClock;
        testMicrosClock = () -> currentMicros++;
        try (ApplyWal2TableJob job = createWalApplyJob()) {
            for (int i = 0; i < txnCount; i++) {
                job.run();
            }
        } finally {
            testMicrosClock = savedClock;
            node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, -1);
            pressure.setMaxBlockRowCount(Integer.MAX_VALUE);
        }
        try (TableWriter writer = getWriter(token)) {
            assertLag(writer, txnCount, rowCount, isOrdered);
        }
    }

    private void createVariableTable() throws Exception {
        execute("CREATE TABLE lag (x LONG, sym SYMBOL INDEX, s STRING, vc VARCHAR, b BINARY, ts TIMESTAMP) " +
                "TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void forceCurrentLagThenReopen(
            NoWalReadFilesFacade ff,
            TableToken token,
            long targetTxn,
            int lagTxnCount,
            int lagRowCount,
            boolean isOrdered
    ) {
        final TableWriterPressureControl pressure = engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl();
        ff.denyWalReads = true;
        try {
            try (TableWriter writer = getWriter(token)) {
                // The Delta switch runs on the active apply writer, where the lag is still available. A physical
                // writer reopen intentionally discards saved lag and replays it from WAL, so keep this writer pooled
                // until the forced commit and test the physical reopen only after the lag has been committed.
                assertLag(writer, lagTxnCount, lagRowCount, isOrdered);
                Assert.assertEquals(targetTxn, writer.getAppliedSeqTxn());
                writer.forceWalLagCommit(pressure);
                assertCommitted(writer, targetTxn);
                writer.forceWalLagCommit(pressure);
                assertCommitted(writer, targetTxn);
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            try (TableWriter writer = getWriter(token)) {
                assertCommitted(writer, targetTxn);
                writer.forceWalLagCommit(pressure);
                assertCommitted(writer, targetTxn);
            }
        } finally {
            ff.denyWalReads = false;
        }
    }

    private static class NoWalReadFilesFacade extends TestFilesFacadeImpl {
        private boolean denyWalReads;

        @Override
        public long openRO(LPSZ name) {
            if (denyWalReads && Utf8s.containsAscii(name, Files.SEPARATOR + "wal")) {
                Assert.fail("forcing persisted lag must not read a WAL segment: " + name);
            }
            return super.openRO(name);
        }
    }
}
