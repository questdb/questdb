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

package io.questdb.test.cutlass.qwp.udp;

import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.cutlass.qwp.server.DefaultQwpUdpReceiverConfiguration;
import io.questdb.cutlass.qwp.server.QwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiverConfiguration;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class QwpUdpInsertTest extends AbstractCairoTest {

    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = 19002;

    private static final QwpUdpReceiverConfiguration LOW_COMMIT_RATE_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getCommitRate() {
            return 1;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean ownThread() {
            return false;
        }
    };

    private static final QwpUdpReceiverConfiguration RCVR_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getCommitRate() {
            return 10;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean ownThread() {
            return false;
        }
    };

    @Test
    public void testCancelRowBetweenCompleteRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    // Row 1 -- complete
                    sender.table("cancel_between")
                            .longColumn("id", 1L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    // Row 2 -- partial, then cancelled
                    sender.table("cancel_between")
                            .longColumn("id", 99L)
                            .doubleColumn("noise", 123.456);
                    sender.cancelRow();
                    // Row 3 -- complete
                    sender.table("cancel_between")
                            .longColumn("id", 3L)
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "id\n" +
                            "1\n" +
                            "3\n",
                    "SELECT id FROM cancel_between ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testCancelRowDiscardsPartialRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    // Partial row -- cancelled
                    sender.table("cancel_partial")
                            .longColumn("id", 42L)
                            .stringColumn("note", "discard me");
                    sender.cancelRow();
                    // Complete row
                    sender.table("cancel_partial")
                            .longColumn("id", 1L)
                            .stringColumn("note", "keep me")
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "id\tnote\n" +
                            "1\tkeep me\n",
                    "SELECT id, note FROM cancel_partial ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testCancelRowNoOpWhenNoRowInProgress() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("cancel_noop");
                    sender.cancelRow();
                    // Normal row after no-op cancel
                    sender.table("cancel_noop")
                            .longColumn("id", 1L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "id\n1\n",
                    "SELECT id FROM cancel_noop"
            );
        });
    }

    @Test
    public void testCloseFlushes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                // No explicit flush -- sender close should flush
                try (QwpUdpSender sender = newSender()) {
                    sender.table("close_flush")
                            .symbol("host", "srv-1")
                            .doubleColumn("usage", 50.0)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("close_flush")
                            .symbol("host", "srv-2")
                            .doubleColumn("usage", 60.0)
                            .at(2_000_000L, ChronoUnit.MICROS);
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM close_flush"
            );
        });
    }

    @Test
    public void testManyDatagramsWithLowCommitRate() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    for (int i = 0; i < 20; i++) {
                        sender.table("low_commit")
                                .longColumn("v", i)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                        sender.flush();
                    }
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\tsum\n20\t190\n",
                    "SELECT count(), sum(v) FROM low_commit"
            );
        });
    }

    @Test
    public void testMultiRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("multi_row")
                                .longColumn("value", i * 10L)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\tsum\n10\t450\n",
                    "SELECT count(), sum(value) FROM multi_row"
            );
        });
    }

    @Test
    public void testMultiTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    for (int i = 0; i < 3; i++) {
                        sender.table("table_a")
                                .longColumn("x", i)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    for (int i = 0; i < 5; i++) {
                        sender.table("table_b")
                                .longColumn("y", i * 100L)
                                .at(2_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\n3\n",
                    "SELECT count() FROM table_a"
            );
            assertSql(
                    "count\n5\n",
                    "SELECT count() FROM table_b"
            );
        });
    }

    @Test
    public void testMultiTableDifferentSchemas() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("schema_a")
                            .symbol("region", "us-east")
                            .doubleColumn("temp", 22.5)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("schema_b")
                            .longColumn("count", 42L)
                            .stringColumn("label", "alpha")
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "region\ttemp\n" +
                            "us-east\t22.5\n",
                    "SELECT region, temp FROM schema_a"
            );
            assertSql(
                    "count\tlabel\n" +
                            "42\talpha\n",
                    "SELECT count, label FROM schema_b"
            );
        });
    }

    @Test
    public void testMultiTableInterleavedRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("interleave_a")
                            .longColumn("x", 1L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("interleave_b")
                            .longColumn("y", 10L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.table("interleave_a")
                            .longColumn("x", 2L)
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.table("interleave_b")
                            .longColumn("y", 20L)
                            .at(4_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "x\n" +
                            "1\n" +
                            "2\n",
                    "SELECT x FROM interleave_a ORDER BY timestamp"
            );
            assertSql(
                    "y\n" +
                            "10\n" +
                            "20\n",
                    "SELECT y FROM interleave_b ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testMultiTableSeparateFlushes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("sep_flush_a")
                            .longColumn("v", 1L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("sep_flush_a")
                            .longColumn("v", 2L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();

                    sender.table("sep_flush_b")
                            .longColumn("v", 10L)
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.table("sep_flush_b")
                            .longColumn("v", 20L)
                            .at(4_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\tsum\n2\t3\n",
                    "SELECT count(), sum(v) FROM sep_flush_a"
            );
            assertSql(
                    "count\tsum\n2\t30\n",
                    "SELECT count(), sum(v) FROM sep_flush_b"
            );
        });
    }

    @Test
    public void testMultiTableSwitchBackToSameTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("switchback_a")
                            .longColumn("x", 1L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("switchback_b")
                            .longColumn("y", 10L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    // Switch back to table_a
                    sender.table("switchback_a")
                            .longColumn("x", 2L)
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "x\n" +
                            "1\n" +
                            "2\n",
                    "SELECT x FROM switchback_a ORDER BY timestamp"
            );
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM switchback_b"
            );
        });
    }

    @Test
    public void testMultipleDatagrams() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    // First datagram
                    sender.table("multi_dgram")
                            .longColumn("v", 1L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Second datagram
                    sender.table("multi_dgram")
                            .longColumn("v", 2L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Third datagram
                    sender.table("multi_dgram")
                            .longColumn("v", 3L)
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\tsum\n3\t6\n",
                    "SELECT count(), sum(v) FROM multi_dgram"
            );
        });
    }

    @Test
    public void testNullableColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    // Row with string
                    sender.table("nullable_test")
                            .longColumn("id", 1L)
                            .stringColumn("note", "hello")
                            .at(1_000_000L, ChronoUnit.MICROS);
                    // Row without string (null)
                    sender.table("nullable_test")
                            .longColumn("id", 2L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    // Row with string again
                    sender.table("nullable_test")
                            .longColumn("id", 3L)
                            .stringColumn("note", "world")
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "id\tnote\n" +
                            "1\thello\n" +
                            "2\t\n" +
                            "3\tworld\n",
                    "SELECT id, note FROM nullable_test ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testNullableDouble() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("nullable_double")
                            .longColumn("id", 1L)
                            .doubleColumn("temperature", 36.6)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("nullable_double")
                            .longColumn("id", 2L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.table("nullable_double")
                            .longColumn("id", 3L)
                            .doubleColumn("temperature", 38.1)
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "id\ttemperature\n" +
                            "1\t36.6\n" +
                            "2\tnull\n" +
                            "3\t38.1\n",
                    "SELECT id, temperature FROM nullable_double ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testNullableLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("nullable_long")
                            .longColumn("id", 1L)
                            .longColumn("count", 100L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("nullable_long")
                            .longColumn("id", 2L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.table("nullable_long")
                            .longColumn("id", 3L)
                            .longColumn("count", 300L)
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "id\tcount\n" +
                            "1\t100\n" +
                            "2\tnull\n" +
                            "3\t300\n",
                    "SELECT id, count FROM nullable_long ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("single_row")
                            .symbol("host", "srv-1")
                            .doubleColumn("usage", 73.2)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "host\tusage\ttimestamp\n" +
                            "srv-1\t73.2\t1970-01-01T00:00:01.000000Z\n",
                    "SELECT * FROM single_row"
            );
        });
    }

    @Test
    public void testSymbolRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("sym_trip")
                                .symbol("region", "region-" + i)
                                .longColumn("val", i)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\n5\n",
                    "SELECT count() FROM sym_trip"
            );
            assertSql(
                    "count_distinct\n5\n",
                    "SELECT count_distinct(region) AS count_distinct FROM sym_trip"
            );
        });
    }

    private static void drainReceiver(QwpUdpReceiver receiver) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        boolean everReceived = false;
        while (System.nanoTime() < deadline) {
            boolean received = receiver.runSerially();
            if (received) {
                everReceived = true;
            } else if (everReceived) {
                break;
            }
            Os.pause();
        }
        Assert.assertTrue("timeout: receiver did not process any datagrams", everReceived);
    }

    private static QwpUdpSender newSender() {
        return new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0);
    }
}
