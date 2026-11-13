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

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * Exercises frequent flushes with the real client and server.
 * A new symbol per data row checks symbol-dictionary updates.
 */
public class QwpSenderBatchedSendE2ETest extends AbstractQwpWebSocketTest {
    private static final long BASE_TS_MICROS = 1_700_000_000_000_000L;
    private static final long DRAIN_TIMEOUT_MS = 120_000L;

    @Test
    public void testMixedFrameSizesAroundTheBatchCap() throws Exception {
        // Mix frames above the client's 64 KiB batching cap with small frames.
        recvChunk = Integer.MAX_VALUE;
        sendChunk = Integer.MAX_VALUE;
        final int rows = 2_000;
        final int bigEvery = 50;
        final int bigLen = 80_000;
        final String big = "b".repeat(bigLen);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
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
        // Exercise random server-side fragmentation.
        assertPerRowFlushLandsExactlyOnce("per_row_flush", 500);
    }

    @Test
    public void testPerRowFlushNewSymbolPerRowUnfragmented() throws Exception {
        // Disable artificial server-side fragmentation.
        recvChunk = Integer.MAX_VALUE;
        sendChunk = Integer.MAX_VALUE;
        assertPerRowFlushLandsExactlyOnce("per_row_flush_unfragmented", 10_000);
    }

    private void assertPerRowFlushLandsExactlyOnce(String table, int rows) throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
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
