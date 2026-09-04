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

package io.questdb.test.cutlass.line.tcp;

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

/**
 * Integration coverage for NOT NULL constraint enforcement on the QWP ingest
 * path. Constructing a {@code QwpTableBlockCursor} from scratch in a unit test
 * is awkward, so these tests drive the full {@code QwpUdpReceiver} →
 * {@code QwpWalAppender} → {@code WalWriter.rowAppend()} pipeline.
 * <p>
 * NOT NULL is enforced inside {@code WalWriter.rowAppend()} which throws a
 * {@code CairoException} with the message "NOT NULL constraint violation".
 * The QWP receiver has no special handling for that exception today; it
 * propagates from the WAL apply step. The expected end-state is "the row does
 * not appear in the table".
 */
public class QwpNotNullIntegrationTest extends AbstractCairoTest {

    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = 19_003;
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

    @Test
    public void testQwpAppendNullToNotNullColumnRejected() throws Exception {
        // QWP does not transmit a "NULL marker" per scalar value; sending only
        // the timestamp + a non-NOT NULL column ("y") is the equivalent of
        // omitting the NOT NULL "x" column. The columnar finishColumnarWrite
        // path in WalWriter must throw symmetrically with the row-at-a-time
        // rowAppend path -- the row must not land in the table.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE qwp_nn_omitted (
                        ts TIMESTAMP NOT NULL,
                        x DOUBLE NOT NULL,
                        y DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    // Omit the NOT NULL "x" column entirely.
                    sender.table("qwp_nn_omitted")
                            .doubleColumn("y", 2.0)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            count
                            0
                            """,
                    "SELECT count() FROM qwp_nn_omitted"
            );
        });
    }

    @Test
    public void testQwpAppendValidRowAcceptedWithNotNull() throws Exception {
        // Sanity check: a row that does supply the NOT NULL column must land.
        // Confirms the rejection path above is the discriminator, not a setup
        // failure.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE qwp_nn_valid (
                        ts TIMESTAMP NOT NULL,
                        x DOUBLE NOT NULL,
                        y DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            try (QwpUdpReceiver receiver = new QwpUdpReceiver(RCVR_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("qwp_nn_valid")
                            .doubleColumn("x", 1.5)
                            .doubleColumn("y", 2.0)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            x\ty\tts
                            1.5\t2.0\t1970-01-01T00:00:01.000000Z
                            """,
                    "SELECT x, y, ts FROM qwp_nn_valid"
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
