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

package io.questdb.test.cutlass.qwp.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.cutlass.qwp.server.DefaultQwpUdpReceiverConfiguration;
import io.questdb.cutlass.qwp.server.LinuxMMQwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiverConfiguration;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class QwpUdpInsertTest extends AbstractCairoTest {

    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = 19_002;
    private static final QwpUdpReceiverConfiguration LOW_COMMIT_RATE_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getMaxUncommittedDatagrams() {
            return 1;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean isOwnThread() {
            return false;
        }
    };
    private static final QwpUdpReceiverConfiguration RCVR_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getMaxUncommittedDatagrams() {
            return 10;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean isOwnThread() {
            return false;
        }
    };
    private static final QwpUdpReceiverConfiguration TIMER_COMMIT_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public long getCommitInterval() {
            return 50;
        }

        @Override
        public int getMaxUncommittedDatagrams() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean isOwnThread() {
            return false;
        }
    };
    private final ReceiverFactory receiverFactory;

    @SuppressWarnings("unused")
    public QwpUdpInsertTest(String name, ReceiverFactory factory) {
        this.receiverFactory = factory;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{"base", (ReceiverFactory) QwpUdpReceiver::new});
        if (Os.isLinux()) {
            params.add(new Object[]{"recvmmsg", (ReceiverFactory) LinuxMMQwpUdpReceiver::new});
        }
        return params;
    }

    @Test
    public void testAutoCreatedMultiDimArrayColumnReadsBack() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_udp_array_exec (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_exec")
                            .doubleArray("arr", cube())
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            arr
                            [[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]
                            """,
                    "SELECT arr FROM qwp_udp_array_exec"
            );
        });
    }

    @Test
    public void testAutoCreatedTableWithMultiDimArrayReadsBack() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_new_table")
                            .doubleArray("arr", cube())
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            arr
                            [[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]
                            """,
                    "SELECT arr FROM qwp_udp_array_new_table"
            );
        });
    }

    @Test
    public void testAutoFlushManyColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(300)) {
                    for (int i = 0; i < 20; i++) {
                        sender.table("auto_many_types")
                                .symbol("host", "srv-" + (i % 3))
                                .longColumn("id", i)
                                .doubleColumn("temp", 20.0 + i * 0.1)
                                .stringColumn("note", "row-" + i)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\n20\n",
                    "SELECT count() FROM auto_many_types"
            );
            assertSql(
                    """
                            host\tid\ttemp\tnote
                            srv-0\t0\t20.0\trow-0
                            """,
                    "SELECT host, id, temp, note FROM auto_many_types ORDER BY timestamp LIMIT 1"
            );
            assertSql(
                    """
                            host\tid\ttemp\tnote
                            srv-1\t19\t21.9\trow-19
                            """,
                    "SELECT host, id, temp, note FROM auto_many_types ORDER BY timestamp DESC LIMIT 1"
            );
        });
    }

    @Test
    public void testAutoFlushMultipleSplitsValueCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(200)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("auto_splits")
                                .longColumn("id", i)
                                .doubleColumn("val", i * 1.5)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            count\tsum_id\tmin_id\tmax_id
                            100\t4950\t0\t99
                            """,
                    "SELECT count(), sum(id) AS sum_id, min(id) AS min_id, max(id) AS max_id FROM auto_splits"
            );
        });
    }

    @Test
    public void testAutoFlushSingleRowExceedsMtu() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpSender sender = newSender(100)) {
                sender.table("exceed_mtu")
                        .stringColumn("big", "x".repeat(200));
                try {
                    sender.at(1_000_000L, ChronoUnit.MICROS);
                    Assert.fail("expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("single row exceeds maximum datagram size"));
                }
            }
        });
    }

    @Test
    public void testAutoFlushSingleRowExceedsMtuAfterReplay() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpSender sender = newSender(200)) {
                // Add 2 small committed rows that fit within 200 bytes
                sender.table("exceed_replay")
                        .longColumn("id", 1L)
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.table("exceed_replay")
                        .longColumn("id", 2L)
                        .at(2_000_000L, ChronoUnit.MICROS);
                // Start a row with a very large string that alone exceeds 200 bytes.
                // at() triggers maybeAutoFlush() -> cancel + flush + replay.
                // After replay, the single replayed row still exceeds the limit.
                sender.table("exceed_replay")
                        .longColumn("id", 3L)
                        .stringColumn("big", "x".repeat(200));
                try {
                    sender.at(3_000_000L, ChronoUnit.MICROS);
                    Assert.fail("expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("single row exceeds maximum datagram size"));
                }
            }
        });
    }

    @Test
    public void testAutoFlushSmallMtu() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(200)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("auto_small")
                                .doubleColumn("value", i * 1.1)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
                Assert.assertTrue(
                        "expected multiple datagrams, got " + receiver.getProcessedCount(),
                        receiver.getProcessedCount() > 1
                );
            }

            drainWalQueue();
            assertSql(
                    "count\n50\n",
                    "SELECT count() FROM auto_small"
            );
        });
    }

    @Test
    public void testAutoFlushValueCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(200)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("auto_values")
                                .doubleColumn("value", i * 2.5)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            min_val\tmax_val
                            0.0\t122.5
                            """,
                    "SELECT min(value) AS min_val, max(value) AS max_val FROM auto_values"
            );
            assertSql(
                    "value\n0.0\n",
                    "SELECT value FROM auto_values ORDER BY timestamp LIMIT 1"
            );
            assertSql(
                    "value\n122.5\n",
                    "SELECT value FROM auto_values ORDER BY timestamp DESC LIMIT 1"
            );
        });
    }

    @Test
    public void testAutoFlushWithAtNow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(200)) {
                    for (int i = 0; i < 30; i++) {
                        sender.table("auto_at_now")
                                .longColumn("id", i)
                                .doubleColumn("val", i * 0.5);
                        sender.atNow();
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            count\tmin_id\tmax_id
                            30\t0\t29
                            """,
                    "SELECT count(), min(id) AS min_id, max(id) AS max_id FROM auto_at_now"
            );
        });
    }

    @Test
    public void testAutoFlushWithString() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(300)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("auto_string")
                                .stringColumn("msg", "hello-" + i)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
                Assert.assertTrue(
                        "expected multiple datagrams, got " + receiver.getProcessedCount(),
                        receiver.getProcessedCount() > 1
                );
            }

            drainWalQueue();
            assertSql(
                    "count\n50\n",
                    "SELECT count() FROM auto_string"
            );
        });
    }

    @Test
    public void testAutoFlushWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(200)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("auto_sym")
                                .symbol("host", "srv-" + (i % 5))
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
                Assert.assertTrue(
                        "expected multiple datagrams, got " + receiver.getProcessedCount(),
                        receiver.getProcessedCount() > 1
                );
            }

            drainWalQueue();
            assertSql(
                    "count\n50\n",
                    "SELECT count() FROM auto_sym"
            );
        });
    }

    @Test
    public void testCancelRowAfterAutoFlush() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(200)) {
                    // Fill up enough rows to ensure auto-flush triggers on the next at()
                    for (int i = 0; i < 10; i++) {
                        sender.table("cancel_after_af")
                                .longColumn("id", i)
                                .doubleColumn("val", i * 1.0)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    // Start a new row, then cancel it
                    sender.table("cancel_after_af")
                            .longColumn("id", 999L)
                            .doubleColumn("val", 999.0);
                    sender.cancelRow();
                    // Add one more valid row
                    sender.table("cancel_after_af")
                            .longColumn("id", 10L)
                            .doubleColumn("val", 10.0)
                            .at(1_000_010L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            count\tmax_id
                            11\t10
                            """,
                    "SELECT count(), max(id) AS max_id FROM cancel_after_af"
            );
        });
    }

    @Test
    public void testCancelRowBetweenCompleteRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            id
                            1
                            3
                            """,
                    "SELECT id FROM cancel_between ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testCancelRowDiscardsPartialRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            id\tnote
                            1\tkeep me
                            """,
                    "SELECT id, note FROM cancel_partial ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testCancelRowNoOpWhenNoRowInProgress() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
    public void testCommitIntervalDelaysSparseDatagramCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table timer_commit (ts timestamp, v long) timestamp(ts) partition by DAY WAL WITH maxUncommittedRows=1000, o3MaxLag=1s");

            try (QwpUdpReceiver receiver = receiverFactory.create(TIMER_COMMIT_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("timer_commit")
                            .longColumn("v", 1)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                // UDP loopback delivery is not synchronous with send(): the datagram may
                // still be in flight through the kernel after the sender has closed. Spin
                // until the receiver observes it. This is safe even under the short commit
                // interval configured here because nextCommitTime stays at Long.MAX_VALUE
                // until the first datagram is processed, so the interval-based commit
                // cannot fire during the wait.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                boolean received = false;
                while (System.nanoTime() < deadline) {
                    if (receiver.runSerially()) {
                        received = true;
                        break;
                    }
                    Os.pause();
                }
                Assert.assertTrue("receiver did not process the datagram", received);
                drainWalQueue();
                assertSql("count\n0\n", "SELECT count() FROM timer_commit");

                TestUtils.assertEventually(() -> {
                    receiver.runSerially();
                    drainWalQueue();
                    assertSql("count\n1\n", "SELECT count() FROM timer_commit");
                }, 5);
            }
        });
    }

    @Test
    public void testDatagramTriggeredCommitResetsUncommittedDatagramCount() throws Exception {
        final QwpUdpReceiverConfiguration conf = new DefaultQwpUdpReceiverConfiguration() {
            @Override
            public long getCommitInterval() {
                return TimeUnit.SECONDS.toMillis(5);
            }

            @Override
            public int getMaxUncommittedDatagrams() {
                return 2;
            }

            @Override
            public int getPort() {
                return PORT;
            }

            @Override
            public boolean isOwnThread() {
                return false;
            }
        };

        assertMemoryLeak(() -> {
            execute("create table datagram_trigger_reset (ts timestamp, v long) timestamp(ts) partition by DAY WAL WITH maxUncommittedRows=2, o3MaxLag=1s");

            try (InspectingQwpUdpReceiver receiver = new InspectingQwpUdpReceiver(conf, engine)) {
                sendSingleRow("datagram_trigger_reset", 1L, 1_000_000L);
                drainReceiver(receiver);
                drainWalQueue();
                assertSql("count\n0\n", "SELECT count() FROM datagram_trigger_reset");
                Assert.assertEquals(1, receiver.getTotalCount());

                sendSingleRow("datagram_trigger_reset", 2L, 2_000_000L);
                drainReceiver(receiver);
                drainWalQueue();
                assertSql("count\n2\n", "SELECT count() FROM datagram_trigger_reset");
                Assert.assertEquals(0, receiver.getTotalCount());

                sendSingleRow("datagram_trigger_reset", 3L, 3_000_000L);
                drainReceiver(receiver);
                drainWalQueue();
                assertSql("count\n2\n", "SELECT count() FROM datagram_trigger_reset");
                Assert.assertEquals(1, receiver.getTotalCount());
            }

            drainWalQueue();
            assertSql("count\n3\n", "SELECT count() FROM datagram_trigger_reset");
        });
    }

    @Test
    public void testDeferredArrayColumnDimensionalityMismatchRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_udp_array_deferred_schema_mismatch (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_deferred_schema_mismatch");
                    sender.stageNullDoubleArrayForTest("arr");
                    sender.at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "ts\n1970-01-01T00:00:01.000000Z\n",
                    "SELECT * FROM qwp_udp_array_deferred_schema_mismatch"
            );

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_deferred_schema_mismatch")
                            .doubleArray("arr", new double[]{1.0, 2.0})
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "arr\nnull\n[1.0,2.0]\n",
                    "SELECT arr FROM qwp_udp_array_deferred_schema_mismatch ORDER BY ts"
            );

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_deferred_schema_mismatch")
                            .doubleArray("arr", new double[][]{{3.0, 4.0}})
                            .at(3_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql("count\n2\n", "SELECT count() FROM qwp_udp_array_deferred_schema_mismatch");
            assertSql(
                    "arr\nnull\n[1.0,2.0]\n",
                    "SELECT arr FROM qwp_udp_array_deferred_schema_mismatch ORDER BY ts"
            );
        });
    }

    @Test
    public void testExistingArrayColumnDimensionalityMismatchRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_udp_array_schema_mismatch (arr DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_schema_mismatch")
                            .doubleArray("arr", new double[][]{{1.0, 2.0}})
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql("count\n0\n", "SELECT count() FROM qwp_udp_array_schema_mismatch");
            assertSql(
                    "column\narr\nts\n",
                    "SELECT \"column\" FROM table_columns('qwp_udp_array_schema_mismatch') ORDER BY \"column\""
            );
        });
    }

    @Test
    public void testManyDatagramsWithLowCommitRate() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
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
    public void testMixedArrayDimensionalityBatchRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_udp_array_mixed_dims (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_mixed_dims")
                            .doubleArray("arr", new double[]{1.0, 2.0})
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("qwp_udp_array_mixed_dims")
                            .doubleArray("arr", new double[][]{{3.0, 4.0}})
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql("count\n0\n", "SELECT count() FROM qwp_udp_array_mixed_dims");
            assertSql(
                    """
                            column
                            ts
                            """,
                    """
                            SELECT "column" FROM table_columns('qwp_udp_array_mixed_dims') ORDER BY "column"
                            """
            );
        });
    }

    @Test
    public void testMixedNullAndNonNullArrayRowsAutoCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_mixed_batch_new_table");
                    sender.stageNullDoubleArrayForTest("arr");
                    sender.at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("qwp_udp_array_mixed_batch_new_table")
                            .doubleArray("arr", cube())
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "arr\nnull\n[[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]\n",
                    "SELECT arr FROM qwp_udp_array_mixed_batch_new_table ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testMixedNullAndNonNullArrayRowsExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_udp_array_mixed_batch_existing (arr DOUBLE[][][], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_mixed_batch_existing");
                    sender.stageNullDoubleArrayForTest("arr");
                    sender.at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("qwp_udp_array_mixed_batch_existing")
                            .doubleArray("arr", cube())
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "arr\nnull\n[[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]\n",
                    "SELECT arr FROM qwp_udp_array_mixed_batch_existing ORDER BY ts"
            );
        });
    }

    @Test
    public void testMultiRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            region\ttemp
                            us-east\t22.5
                            """,
                    "SELECT region, temp FROM schema_a"
            );
            assertSql(
                    """
                            count\tlabel
                            42\talpha
                            """,
                    "SELECT count, label FROM schema_b"
            );
        });
    }

    @Test
    public void testMultiTableInterleavedRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            x
                            1
                            2
                            """,
                    "SELECT x FROM interleave_a ORDER BY timestamp"
            );
            assertSql(
                    """
                            y
                            10
                            20
                            """,
                    "SELECT y FROM interleave_b ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testMultiTableSeparateFlushes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            x
                            1
                            2
                            """,
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
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
    public void testNullOnlyArrayColumnCreationIsDeferredDuringTableAutoCreate() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_new_table_deferred");
                    sender.stageNullDoubleArrayForTest("arr");
                    sender.at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "timestamp\n1970-01-01T00:00:01.000000Z\n",
                    "SELECT * FROM qwp_udp_array_new_table_deferred"
            );

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_new_table_deferred")
                            .doubleArray("arr", cube())
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "arr\nnull\n[[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]\n",
                    "SELECT arr FROM qwp_udp_array_new_table_deferred ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testNullOnlyArrayColumnCreationIsDeferredUntilFirstNonNullBatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_udp_array_deferred (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_deferred");
                    sender.stageNullDoubleArrayForTest("arr");
                    sender.at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "ts\n1970-01-01T00:00:01.000000Z\n",
                    "SELECT * FROM qwp_udp_array_deferred"
            );

            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_udp_array_deferred")
                            .doubleArray("arr", cube())
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "arr\nnull\n[[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]\n",
                    "SELECT arr FROM qwp_udp_array_deferred ORDER BY ts"
            );
        });
    }

    @Test
    public void testNullableColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            id\tnote
                            1\thello
                            2\t
                            3\tworld
                            """,
                    "SELECT id, note FROM nullable_test ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testNullableDouble() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            id\ttemperature
                            1\t36.6
                            2\tnull
                            3\t38.1
                            """,
                    "SELECT id, temperature FROM nullable_double ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testNullableLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            id\tcount
                            1\t100
                            2\tnull
                            3\t300
                            """,
                    "SELECT id, count FROM nullable_long ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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
                    """
                            host\tusage\ttimestamp
                            srv-1\t73.2\t1970-01-01T00:00:01.000000Z
                            """,
                    "SELECT * FROM single_row"
            );
        });
    }

    @Test
    public void testSymbolRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
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

    @Test
    public void testTableSwitchTriggersFlush() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(65_535)) {
                    // Add rows to table "a"
                    for (int i = 0; i < 3; i++) {
                        sender.table("switch_a")
                                .longColumn("x", i)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    // This table switch should flush "switch_a" immediately
                    sender.table("switch_b");

                    // Drain and assert switch_a arrived before any explicit flush
                    drainReceiver(receiver);
                    drainWalQueue();
                    assertSql(
                            "count\tsum\n3\t3\n",
                            "SELECT count(), sum(x) FROM switch_a"
                    );

                    // Now complete switch_b and flush it separately
                    sender.longColumn("y", 100L)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM switch_b"
            );
        });
    }

    private static double[][][] cube() {
        return new double[][][]{
                {
                        {1.0, 2.0},
                        {3.0, 4.0}
                },
                {
                        {5.0, 6.0},
                        {7.0, 8.0}
                }
        };
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

    private static QwpUdpSender newSender(int maxDatagramSize) {
        return new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0, maxDatagramSize);
    }

    private static QwpUdpSender newSender() {
        return new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0);
    }

    private static void sendSingleRow(String table, long value, long timestampMicros) {
        try (QwpUdpSender sender = newSender()) {
            sender.table(table)
                    .longColumn("v", value)
                    .at(timestampMicros, ChronoUnit.MICROS);
            sender.flush();
        }
    }

    @FunctionalInterface
    public interface ReceiverFactory {
        QwpUdpReceiver create(QwpUdpReceiverConfiguration config, CairoEngine engine);
    }

    private static class InspectingQwpUdpReceiver extends QwpUdpReceiver {
        private InspectingQwpUdpReceiver(QwpUdpReceiverConfiguration configuration, CairoEngine engine) {
            super(configuration, engine);
        }

        private long getTotalCount() {
            return totalCount;
        }
    }
}
