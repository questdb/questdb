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
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class QwpUdpInsertTest extends AbstractCairoTest {

    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = 19002;

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
                receiver.runSerially();
            }

            refreshTablesInBaseEngine();
            drainWalQueue();
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM close_flush"
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
                receiver.runSerially();
            }

            refreshTablesInBaseEngine();
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
                receiver.runSerially();
            }

            refreshTablesInBaseEngine();
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
                receiver.runSerially();
            }

            refreshTablesInBaseEngine();
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
                receiver.runSerially();
            }

            refreshTablesInBaseEngine();
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
                receiver.runSerially();
            }

            refreshTablesInBaseEngine();
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
                receiver.runSerially();
            }

            refreshTablesInBaseEngine();
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

    private static QwpUdpSender newSender() {
        return new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0);
    }
}
