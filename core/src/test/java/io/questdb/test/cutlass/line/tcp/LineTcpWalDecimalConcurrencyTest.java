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

import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.assertEventually;

/**
 * Regression test: the ILP/TCP path used to share a single LineWalAppender across
 * all network IO workers, so its Decimal256 scratch state raced when concurrent
 * connections wrote bare doubles into DECIMAL columns, bleeding values between
 * rows. LineTcpMeasurementScheduler now keeps one appender per network IO worker.
 */
public class LineTcpWalDecimalConcurrencyTest extends AbstractLineTcpReceiverTest {
    private static final double[] EXPECTED_PRICES = {1.25, 4096.75, 0.03125, 999999.875};
    private static final double[] EXPECTED_QUANTITIES = {1024.5, 0.125, 8192.0625, 17.5};
    private static final int ROWS_PER_SENDER = 25_000;
    private static final int SENDER_COUNT = 16;

    @Test
    public void testWalBareDoubleToDecimalDoesNotBleedAcrossTcpWorkers() throws Exception {
        runInContext(_receiver -> {
            execute(
                    "CREATE TABLE trades (" +
                            "sender_id LONG, " +
                            "row_id LONG, " +
                            "variant LONG, " +
                            "price DECIMAL(38, 18), " +
                            "quantity DECIMAL(38, 18), " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );

            CyclicBarrier barrier = new CyclicBarrier(SENDER_COUNT);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread[] threads = new Thread[SENDER_COUNT];

            for (int s = 0; s < SENDER_COUNT; s++) {
                final int senderIdx = s;
                threads[s] = new Thread(() -> {
                    try (Socket socket = newSocket()) {
                        barrier.await();
                        StringBuilder lines = new StringBuilder(128 * 1024);
                        for (int r = 0; r < ROWS_PER_SENDER; r++) {
                            appendLine(lines, senderIdx, r);
                            if ((r & 511) == 511) {
                                sendToSocket(socket, lines.toString());
                                lines.setLength(0);
                            }
                        }
                        if (lines.length() > 0) {
                            sendToSocket(socket, lines.toString());
                        }
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                }, "ilp-decimal-race-" + senderIdx);
                threads[s].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
            if (error.get() != null) {
                throw new RuntimeException("sender thread failed", error.get());
            }

            assertEventually(() -> {
                drainWalQueue();
                assertQuery("SELECT count() FROM trades")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n" + (long) SENDER_COUNT * ROWS_PER_SENDER + "\n");
            });
            assertQuery(
                    "SELECT count() FROM trades WHERE " +
                            "(variant = 0 AND (price != 1.25::decimal(38,18) OR quantity != 1024.5::decimal(38,18))) OR " +
                            "(variant = 1 AND (price != 4096.75::decimal(38,18) OR quantity != 0.125::decimal(38,18))) OR " +
                            "(variant = 2 AND (price != 0.03125::decimal(38,18) OR quantity != 8192.0625::decimal(38,18))) OR " +
                            "(variant = 3 AND (price != 999999.875::decimal(38,18) OR quantity != 17.5::decimal(38,18)))"
            )
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
        });
    }

    @Override
    protected int getWorkerCount() {
        return SENDER_COUNT;
    }

    private static void appendLine(StringBuilder sink, int senderIdx, int rowIdx) {
        int variant = (senderIdx + rowIdx) & 3;
        double expectedPrice = EXPECTED_PRICES[variant];
        double expectedQuantity = EXPECTED_QUANTITIES[variant];
        sink.append("trades sender_id=").append(senderIdx)
                .append("i,row_id=").append(rowIdx)
                .append("i,variant=").append(variant)
                .append("i,price=").append(expectedPrice)
                .append(",quantity=").append(expectedQuantity)
                .append('\n');
    }
}
