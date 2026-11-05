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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * Drives the real client against a real server in the shapes where the client
 * packs several published QWP frames into one TCP write: a producer that
 * flushes every row runs ahead of the I/O thread, so one write carries a
 * burst of small frames and the server's reads end mid-frame. The data tests
 * give every row a new symbol, so each frame carries a symbol-dictionary
 * delta and a lost, duplicated or reordered frame shows up as a wrong or NULL
 * symbol, not only as a wrong row count. The auto-flush tests cover the
 * {@code ws::} auto-flush settings that ship with the same client change.
 */
public class QwpSenderBatchedSendE2ETest extends AbstractQwpWebSocketTest {
    private static final long BASE_TS_MICROS = 1_700_000_000_000_000L;
    private static final long CLOSE_FLUSH_TIMEOUT_MS = 300_000L;
    private static final long DRAIN_TIMEOUT_MS = 120_000L;

    @Test
    public void testAutoFlushIntervalOffKeepsRowCap() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connect(port, "auto_flush_interval=off;auto_flush_rows=50;")) {
                for (int i = 0; i < 120; i++) {
                    sender.table("interval_off")
                            .longColumn("id", i)
                            .at(BASE_TS_MICROS + i, ChronoUnit.MICROS);
                }
                // Rows 50 and 100 hit the row cap and published FSNs 0 and 1;
                // the 20 rows left over go out with the explicit flush.
                Assert.assertEquals(2L, sender.flushAndGetSequence());
                Assert.assertTrue(sender.drain(DRAIN_TIMEOUT_MS));
            }
            drainWalQueue();
            assertQuery("SELECT count() n, count_distinct(id) ids FROM interval_off")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\tids\n120\t120\n");
        });
    }

    @Test
    public void testAutoFlushOffPublishesOnlyOnFlushAndClose() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connect(port, "auto_flush=off;")) {
                for (int i = 0; i < 100; i++) {
                    sender.table("auto_flush_off")
                            .longColumn("id", i)
                            .at(BASE_TS_MICROS + i, ChronoUnit.MICROS);
                }
                // Outlast the 100 ms default interval: with auto-flush on, the
                // next row would publish the batch on its own.
                Thread.sleep(250);
                sender.table("auto_flush_off")
                        .longColumn("id", 100)
                        .at(BASE_TS_MICROS + 100, ChronoUnit.MICROS);
                // Nothing was published before: this flush publishes FSN 0.
                Assert.assertEquals(0L, sender.flushAndGetSequence());
                Assert.assertTrue(sender.drain(DRAIN_TIMEOUT_MS));
                // close() publishes the rows written after the last flush.
                for (int i = 101; i < 150; i++) {
                    sender.table("auto_flush_off")
                            .longColumn("id", i)
                            .at(BASE_TS_MICROS + i, ChronoUnit.MICROS);
                }
            }
            drainWalQueue();
            assertQuery("SELECT count() n, count_distinct(id) ids, min(id) lo, max(id) hi FROM auto_flush_off")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\tids\tlo\thi\n150\t150\t0\t149\n");
        });
    }

    @Test
    public void testMixedFrameSizesAroundTheBatchCap() throws Exception {
        // Every 50th frame is larger than the client's 64 KiB batch cap, so it
        // goes out in a write of its own and grows the send buffer, while the
        // small frames around it share writes. Fragmentation is covered by the
        // per-row tests.
        recvChunk = Integer.MAX_VALUE;
        sendChunk = Integer.MAX_VALUE;
        final int rows = 2_000;
        final int bigEvery = 50;
        final int bigLen = 80_000;
        final String big = "b".repeat(bigLen);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connect(port, "")) {
                for (int i = 0; i < rows; i++) {
                    sender.table("mixed_sizes")
                            .symbol("sym", "s" + i)
                            .longColumn("id", i)
                            .stringColumn("payload", i % bigEvery == 0 ? big : "x")
                            .at(BASE_TS_MICROS + i, ChronoUnit.MICROS);
                    sender.flush();
                }
                Assert.assertTrue(sender.drain(DRAIN_TIMEOUT_MS));
            }
            drainWalQueue();
            final long bigRows = rows / bigEvery;
            final long payloadBytes = bigRows * bigLen + (rows - bigRows);
            assertQuery("""
                    SELECT count() n, count_distinct(id) ids, sum(length(payload)) bytes,
                           sum(CASE WHEN sym = concat('s', id) THEN 1 ELSE 0 END) matched
                    FROM mixed_sizes
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\tids\tbytes\tmatched\n" + rows + "\t" + rows + "\t" + payloadBytes + "\t" + rows + "\n");
        }, 1 << 20);
    }

    @Test
    public void testPerRowFlushNewSymbolPerRow() throws Exception {
        // Random server-side fragmentation: reads end mid-frame, often mid-header.
        assertPerRowFlushLandsExactlyOnce("per_row_flush", 500);
    }

    @Test
    public void testPerRowFlushNewSymbolPerRowUnfragmented() throws Exception {
        // Unfragmented server reads see whole bursts of frames per recv.
        recvChunk = Integer.MAX_VALUE;
        sendChunk = Integer.MAX_VALUE;
        assertPerRowFlushLandsExactlyOnce("per_row_flush_unfragmented", 10_000);
    }

    @Test
    public void testTransactionsSpanningSendBatches() throws Exception {
        // One deferred-commit frame per row: a transaction's frames, and the
        // commit frame that closes it, can share TCP writes with the frames of
        // the next transaction.
        final int txns = 50;
        final int rowsPerTxn = 40;
        runInContext((port) -> {
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port)
                    .transactional(true)
                    .autoFlushRows(1)
                    .autoFlushBytes(0)
                    .closeFlushTimeoutMillis(CLOSE_FLUSH_TIMEOUT_MS)
                    .build()) {
                long id = 0;
                for (int t = 0; t < txns; t++) {
                    for (int r = 0; r < rowsPerTxn; r++, id++) {
                        sender.table("txn_batched")
                                .symbol("sym", "s" + id)
                                .longColumn("txn", t)
                                .longColumn("id", id)
                                .at(BASE_TS_MICROS + id, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                Assert.assertTrue(sender.drain(DRAIN_TIMEOUT_MS));
            }
            drainWalQueue();
            final int rows = txns * rowsPerTxn;
            assertQuery("""
                    SELECT count() n, count_distinct(id) ids, count_distinct(txn) txns,
                           sum(CASE WHEN sym = concat('s', id) THEN 1 ELSE 0 END) matched
                    FROM txn_batched
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\tids\ttxns\tmatched\n" + rows + "\t" + rows + "\t" + txns + "\t" + rows + "\n");
        });
    }

    private static QwpWebSocketSender connect(int port, String extraConfig) {
        return (QwpWebSocketSender) Sender.fromConfig("ws::addr=localhost:" + port + ";"
                + extraConfig + "close_flush_timeout_millis=" + CLOSE_FLUSH_TIMEOUT_MS + ";");
    }

    private void assertPerRowFlushLandsExactlyOnce(String table, int rows) throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connect(port, "")) {
                for (int i = 0; i < rows; i++) {
                    sender.table(table)
                            .symbol("sym", "s" + i)
                            .longColumn("id", i)
                            .at(BASE_TS_MICROS + i, ChronoUnit.MICROS);
                    sender.flush();
                }
                Assert.assertTrue("every frame must be acked", sender.drain(DRAIN_TIMEOUT_MS));
                Assert.assertEquals(rows, sender.getTotalFramesSent());
            }
            drainWalQueue();
            assertQuery("SELECT count() n, count_distinct(id) ids, count_distinct(sym) syms, min(id) lo, max(id) hi,"
                    + " sum(CASE WHEN sym = concat('s', id) THEN 1 ELSE 0 END) matched FROM " + table)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\tids\tsyms\tlo\thi\tmatched\n"
                            + rows + "\t" + rows + "\t" + rows + "\t0\t" + (rows - 1) + "\t" + rows + "\n");
        });
    }
}
