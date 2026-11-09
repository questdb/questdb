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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.cutlass.qwp.server.DefaultQwpUdpReceiverConfiguration;
import io.questdb.cutlass.qwp.server.LinuxMMQwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiverConfiguration;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * QWP over UDP into tables whose partitions are composite (merge-append, see
 * {@code COMPOSITE_PARTITIONS.md}). QWP UDP writes through a {@code WalWriter}, so every
 * datagram batch reaches the table as a WAL commit that {@code ApplyWal2TableJob} applies
 * through the merge-append path. These tests make the receiver's own commits cut a day into
 * pieces, then keep writing into that composite partition - backfill, spanning batches,
 * dedup upserts - and into the table's active partition, which merge-append leaves closed.
 */
@RunWith(Parameterized.class)
public class QwpUdpCompositePartitionTest extends AbstractCairoTest {

    // Keeps every datagram small, so a batch spans several datagrams and every one of
    // them fits the loopback maximum on every OS (macOS caps a UDP datagram at 9216 bytes).
    private static final int DATAGRAM_SIZE = 1400;
    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = 19_002;
    // Commits after every datagram, so one flush lands as many small WAL transactions.
    private static final QwpUdpReceiverConfiguration COMMIT_PER_DATAGRAM_CONF = new DefaultQwpUdpReceiverConfiguration() {
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
    private final ReceiverFactory receiverFactory;

    @SuppressWarnings("unused")
    public QwpUdpCompositePartitionTest(String name, ReceiverFactory factory) {
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
    public void testActivePartitionTakesInOrderAndO3Batches() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();

            // The table's only partition is its active one. Merge-append leaves it closed, so
            // every commit below - in-order or not - goes through the merge-append dispatch
            // rather than an in-place append. This is the regime in which a direct
            // TableWriter.newRow() is refused for a WAL table; QWP UDP must not depend on it.
            send(COMMIT_PER_DATAGRAM_CONF, sender -> {
                for (int i = 0; i < 40; i++) {
                    row(sender, "udp_active", i, "2020-02-03T10:00:00Z", i * 60);
                }
            });
            send(COMMIT_PER_DATAGRAM_CONF, sender -> {
                for (int i = 40; i < 80; i++) {
                    row(sender, "udp_active", i, "2020-02-03T10:00:00Z", i * 60);
                }
            });
            // Backdated into the active partition, between rows that are already there.
            send(COMMIT_PER_DATAGRAM_CONF, sender -> {
                for (int i = 0; i < 20; i++) {
                    row(sender, "udp_active", 1000 + i, "2020-02-03T10:20:30Z", i * 60);
                }
            });

            assertComposite("udp_active", 0);
            assertQuery("SELECT count(), sum(v), min(timestamp), max(timestamp) FROM udp_active")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmin\tmax
                            100\t23350\t2020-02-03T10:00:00.000000Z\t2020-02-03T11:19:00.000000Z
                            """);
            assertQuery("SELECT v, timestamp FROM udp_active WHERE timestamp BETWEEN '2020-02-03T10:20' AND '2020-02-03T10:23'")
                    .timestamp("timestamp")
                    .returns("""
                            v\ttimestamp
                            20\t2020-02-03T10:20:00.000000Z
                            1000\t2020-02-03T10:20:30.000000Z
                            21\t2020-02-03T10:21:00.000000Z
                            1001\t2020-02-03T10:21:30.000000Z
                            22\t2020-02-03T10:22:00.000000Z
                            1002\t2020-02-03T10:22:30.000000Z
                            23\t2020-02-03T10:23:00.000000Z
                            """);
            assertTimestampsAscending("udp_active", "timestamp");
        });
    }

    @Test
    public void testAutoCreatedTableBackfillCutsPartitionIntoPieces() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();

            // The receiver auto-creates the table, as a WAL table. The row on a later day keeps
            // 2020-02-03 from being the active partition, so every later write to it goes
            // through the O3 path.
            send(RCVR_CONF, sender -> seedDay(sender, "udp_auto"));
            // A lone backdated row between the 05:00 and 05:30 rows cuts the day into pieces.
            send(RCVR_CONF, sender -> row(sender, "udp_auto", 100, "2020-02-03T05:10:00Z", 0));
            assertComposite("udp_auto", 0);

            // A batch whose rows straddle the 05:10 piece without landing on it.
            send(RCVR_CONF, sender -> {
                row(sender, "udp_auto", 101, "2020-02-03T03:10:00Z", 0);
                row(sender, "udp_auto", 102, "2020-02-03T07:10:00Z", 0);
            });

            assertNotSuspended("udp_auto");
            assertQuery("SELECT count(), sum(v), min(timestamp), max(timestamp) FROM udp_auto")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmin\tmax
                            52\t2430\t2020-02-03T00:00:00.000000Z\t2020-02-06T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, timestamp FROM udp_auto WHERE timestamp BETWEEN '2020-02-03T03:00' AND '2020-02-03T07:30'")
                    .timestamp("timestamp")
                    .returns("""
                            v\ttimestamp
                            6\t2020-02-03T03:00:00.000000Z
                            101\t2020-02-03T03:10:00.000000Z
                            7\t2020-02-03T03:30:00.000000Z
                            8\t2020-02-03T04:00:00.000000Z
                            9\t2020-02-03T04:30:00.000000Z
                            10\t2020-02-03T05:00:00.000000Z
                            100\t2020-02-03T05:10:00.000000Z
                            11\t2020-02-03T05:30:00.000000Z
                            12\t2020-02-03T06:00:00.000000Z
                            13\t2020-02-03T06:30:00.000000Z
                            14\t2020-02-03T07:00:00.000000Z
                            102\t2020-02-03T07:10:00.000000Z
                            15\t2020-02-03T07:30:00.000000Z
                            """);
            assertTimestampsAscending("udp_auto", "timestamp");
        });
    }

    @Test
    public void testBackfillIntoExistingWalTableKeepsUntouchedPieces() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("""
                    CREATE TABLE udp_sensors (
                        sym SYMBOL, v LONG, d DOUBLE, s VARCHAR, ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            // A whole day at 5-minute steps (288 rows), plus a row on a later day.
            send(RCVR_CONF, sender -> {
                for (int i = 0; i < 288; i++) {
                    sensorRow(sender, i, "2020-02-03T00:00:00Z", i * 300);
                }
                sensorRow(sender, 9999, "2020-02-06T00:00:00Z", 0);
            });
            // Ten backdated rows, all between the existing 04:00 and 04:05 rows.
            send(RCVR_CONF, sender -> {
                for (int i = 0; i < 10; i++) {
                    sensorRow(sender, 5000 + i, "2020-02-03T04:00:07Z", i * 5);
                }
            });

            final long deadRows = assertComposite("udp_sensors", 0);
            // Only the rows the backfill merged with were rewritten, not the whole day.
            Assert.assertTrue("the backfill rewrote the whole partition [deadRows=" + deadRows + ']', deadRows < 288);

            assertQuery("SELECT count(), sum(v), min(ts), max(ts) FROM udp_sensors")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmin\tmax
                            299\t101372\t2020-02-03T00:00:00.000000Z\t2020-02-06T00:00:00.000000Z
                            """);
            // An interval scan across the piece the backfill landed in.
            assertQuery("SELECT sym, v, d, s, ts FROM udp_sensors WHERE ts BETWEEN '2020-02-03T03:55' AND '2020-02-03T04:10'")
                    .timestamp("ts")
                    .returns("""
                            sym\tv\td\ts\tts
                            s3\t47\t23.5\trow-47\t2020-02-03T03:55:00.000000Z
                            s0\t48\t24.0\trow-48\t2020-02-03T04:00:00.000000Z
                            s0\t5000\t2500.0\trow-5000\t2020-02-03T04:00:07.000000Z
                            s1\t5001\t2500.5\trow-5001\t2020-02-03T04:00:12.000000Z
                            s2\t5002\t2501.0\trow-5002\t2020-02-03T04:00:17.000000Z
                            s3\t5003\t2501.5\trow-5003\t2020-02-03T04:00:22.000000Z
                            s0\t5004\t2502.0\trow-5004\t2020-02-03T04:00:27.000000Z
                            s1\t5005\t2502.5\trow-5005\t2020-02-03T04:00:32.000000Z
                            s2\t5006\t2503.0\trow-5006\t2020-02-03T04:00:37.000000Z
                            s3\t5007\t2503.5\trow-5007\t2020-02-03T04:00:42.000000Z
                            s0\t5008\t2504.0\trow-5008\t2020-02-03T04:00:47.000000Z
                            s1\t5009\t2504.5\trow-5009\t2020-02-03T04:00:52.000000Z
                            s1\t49\t24.5\trow-49\t2020-02-03T04:05:00.000000Z
                            s2\t50\t25.0\trow-50\t2020-02-03T04:10:00.000000Z
                            """);
            assertQuery("SELECT sym, count() FROM udp_sensors ORDER BY sym")
                    .expectSize()
                    .returns("""
                            sym\tcount
                            s0\t75
                            s1\t75
                            s2\t74
                            s3\t75
                            """);
            assertTimestampsAscending("udp_sensors", "ts");
        });
    }

    @Test
    public void testDedupUpsertIntoCompositePartition() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("CREATE TABLE udp_dedup (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS(timestamp)");

            send(RCVR_CONF, sender -> seedDay(sender, "udp_dedup"));
            send(RCVR_CONF, sender -> row(sender, "udp_dedup", 100, "2020-02-03T05:10:00Z", 0));
            assertComposite("udp_dedup", 0);

            // New values for keys spread over the composite day's pieces, plus one new key. The
            // upsert has to find each match in whichever piece holds it.
            send(RCVR_CONF, sender -> {
                row(sender, "udp_dedup", 1000, "2020-02-03T00:00:00Z", 0);
                row(sender, "udp_dedup", 1010, "2020-02-03T05:00:00Z", 0);
                row(sender, "udp_dedup", 1100, "2020-02-03T05:10:00Z", 0);
                row(sender, "udp_dedup", 2000, "2020-02-03T05:20:00Z", 0);
                row(sender, "udp_dedup", 1024, "2020-02-03T12:00:00Z", 0);
                row(sender, "udp_dedup", 1047, "2020-02-03T23:30:00Z", 0);
            });

            assertNotSuspended("udp_dedup");
            assertQuery("SELECT v, timestamp FROM udp_dedup WHERE v >= 1000")
                    .timestamp("timestamp")
                    .returns("""
                            v\ttimestamp
                            1000\t2020-02-03T00:00:00.000000Z
                            1010\t2020-02-03T05:00:00.000000Z
                            1100\t2020-02-03T05:10:00.000000Z
                            2000\t2020-02-03T05:20:00.000000Z
                            1024\t2020-02-03T12:00:00.000000Z
                            1047\t2020-02-03T23:30:00.000000Z
                            """);
            final String totals = """
                    count\tsum
                    51\t9227
                    """;
            assertQuery("SELECT count(), sum(v) FROM udp_dedup")
                    .noRandomAccess()
                    .expectSize()
                    .returns(totals);

            // Re-sending identical rows must change nothing.
            send(RCVR_CONF, sender -> {
                row(sender, "udp_dedup", 1000, "2020-02-03T00:00:00Z", 0);
                row(sender, "udp_dedup", 2000, "2020-02-03T05:20:00Z", 0);
            });
            assertQuery("SELECT count(), sum(v) FROM udp_dedup")
                    .noRandomAccess()
                    .expectSize()
                    .returns(totals);
            assertTimestampsAscending("udp_dedup", "timestamp");
        });
    }

    /**
     * Asserts the partition is genuinely composite and returns its dead row count.
     */
    private static long assertComposite(String tableName, int partitionIndex) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
        try (TableReader reader = engine.getReader(tableToken)) {
            Assert.assertTrue("partition should be composite", reader.getTxFile().isPartitionComposite(partitionIndex));
            final int pieceCount = reader.getGeometry().getPieceCount(partitionIndex);
            Assert.assertTrue("partition should hold more than one piece [pieceCount=" + pieceCount + ']', pieceCount > 1);
            return reader.getPartitionPhysicalRowCount(partitionIndex) - reader.getTxFile().getPartitionSize(partitionIndex);
        }
    }

    private static void assertNotSuspended(String tableName) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
    }

    /**
     * Pumps the receiver until it has handled at least one datagram and then stayed idle for
     * a quiet period, so a multi-datagram flush still in flight through loopback is not cut short.
     */
    private static void drainReceiver(QwpUdpReceiver receiver) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        final long quietPeriod = TimeUnit.MILLISECONDS.toNanos(200);
        long handled = receiver.getProcessedCount() + receiver.getTotalDroppedCount();
        long lastProgress = 0;
        while (System.nanoTime() < deadline) {
            receiver.runSerially();
            final long handledNow = receiver.getProcessedCount() + receiver.getTotalDroppedCount();
            if (handledNow > handled) {
                handled = handledNow;
                lastProgress = System.nanoTime();
            } else if (lastProgress > 0 && System.nanoTime() - lastProgress > quietPeriod) {
                return;
            }
            Os.pause();
        }
        Assert.assertTrue("timeout: receiver did not process any datagrams", lastProgress > 0);
    }

    private static void enableMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        // The defaults (50MB split size, 4096 average rows a piece) only cut partitions far
        // larger than these tests write: a pre-split keeps pieces of at least 2x the average
        // rows limit. Lowered so a day of a few dozen rows is cut around a backfill.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "16");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
    }

    private static long micros(String isoTimestamp) {
        final Instant instant = Instant.parse(isoTimestamp);
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

    private static void row(QwpUdpSender sender, String tableName, long v, String isoBase, long offsetSeconds) {
        sender.table(tableName)
                .longColumn("v", v)
                .at(micros(isoBase) + TimeUnit.SECONDS.toMicros(offsetSeconds), ChronoUnit.MICROS);
    }

    /**
     * A day of rows at 30-minute steps (v = 0..47), plus one row on a later day.
     */
    private static void seedDay(QwpUdpSender sender, String tableName) {
        for (int i = 0; i < 48; i++) {
            row(sender, tableName, i, "2020-02-03T00:00:00Z", i * 1800L);
        }
        row(sender, tableName, 999, "2020-02-06T00:00:00Z", 0);
    }

    private static void sensorRow(QwpUdpSender sender, long v, String isoBase, long offsetSeconds) {
        sender.table("udp_sensors")
                .symbol("sym", "s" + (v % 4))
                .longColumn("v", v)
                .doubleColumn("d", v * 0.5)
                .stringColumn("s", "row-" + v)
                .at(micros(isoBase) + TimeUnit.SECONDS.toMicros(offsetSeconds), ChronoUnit.MICROS);
    }

    private void assertTimestampsAscending(String tableName, String tsColumn) throws Exception {
        assertQuery("SELECT count() FROM (SELECT " + tsColumn + ", lag(" + tsColumn + ") OVER () prev FROM " + tableName + ") WHERE " + tsColumn + " < prev")
                .noRandomAccess()
                .expectSize()
                .returns("count\n0\n");
    }

    /**
     * One receiver session: the rows go out, the receiver drains them, and closing it commits
     * whatever it still holds. The WAL is then applied, so each call is at least one WAL commit.
     */
    private void send(QwpUdpReceiverConfiguration conf, Consumer<QwpUdpSender> rows) {
        try (QwpUdpReceiver receiver = receiverFactory.create(conf, engine)) {
            try (QwpUdpSender sender = new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0, DATAGRAM_SIZE)) {
                rows.accept(sender);
                sender.flush();
            }
            drainReceiver(receiver);
            Assert.assertEquals("receiver dropped datagrams", 0, receiver.getTotalDroppedCount());
        }
        drainWalQueue();
    }

    @FunctionalInterface
    public interface ReceiverFactory {
        QwpUdpReceiver create(QwpUdpReceiverConfiguration config, CairoEngine engine);
    }
}
