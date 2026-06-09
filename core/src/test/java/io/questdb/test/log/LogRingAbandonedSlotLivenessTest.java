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

package io.questdb.test.log;

import io.questdb.log.GuaranteedLogger;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogFileWriter;
import io.questdb.log.LogLevel;
import io.questdb.log.LogWriterConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Guards the log ring's liveness when a guaranteed-logging producer hits a failure (e.g. an
 * OutOfMemoryError) inside the lazy CursorHolder allocation that runs on the first log call per thread.
 * <p>
 * The guaranteed-logging variants claim a ring slot with the blocking nextBully() and never drop a
 * record. If the lazy CursorHolder allocation ran AFTER the slot was claimed and then failed, the slot
 * would never be published or released: the producer barrier would never advance and every later
 * guaranteed producer would spin forever in nextBully while the consumer idles. That is exactly the
 * process-wide log-ring wedge observed in CI when a GC-overhead OutOfMemoryError storm struck the
 * guaranteed-logging test harness.
 */
public class LogRingAbandonedSlotLivenessTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test(timeout = 60_000)
    public void testRingStaysLiveAfterCursorHolderAllocationFailure() throws Exception {
        final File logFile = temp.newFile();
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.ALL, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(logFile.getAbsolutePath());
                return w;
            }));
            factory.bind();
            factory.startThread();

            LogFactory.enableGuaranteedLogging();
            try {
                final Log logger = factory.create("x");
                Assert.assertEquals(GuaranteedLogger.class, logger.getClass());

                // The producer runs on its own thread so its first guaranteed log call materialises a
                // fresh per-thread CursorHolder. We arm the allocation to fail exactly once, simulating
                // an OutOfMemoryError landing inside the holder's lazy allocation.
                final Throwable[] producerError = new Throwable[1];
                final boolean[] sawSimulatedOom = new boolean[1];
                final boolean[] finishedAllWrites = new boolean[1];

                Thread producer = new Thread(() -> {
                    try {
                        LogFactory.failNextCursorHolderInit();
                        try {
                            logger.xerror().$("first record, allocation fails here").$();
                        } catch (OutOfMemoryError expected) {
                            sawSimulatedOom[0] = true;
                        }

                        // After the simulated failure the ring must still be live. With guaranteed
                        // logging a stranded slot would wedge nextBully forever once the ring wraps past
                        // the abandoned position; enough records to wrap the default ring several times
                        // proves liveness. A healthy ring completes these in well under the test timeout.
                        for (int i = 0; i < 8192; i++) {
                            logger.xerror().$("record after recovery ").$(i).$();
                        }
                        finishedAllWrites[0] = true;
                    } catch (Throwable t) {
                        producerError[0] = t;
                    }
                }, "log-ring-liveness-producer");

                producer.setDaemon(true);
                producer.start();
                producer.join(45_000);

                Assert.assertNull("producer threw unexpectedly", producerError[0]);
                Assert.assertTrue("the simulated CursorHolder allocation failure did not fire", sawSimulatedOom[0]);
                Assert.assertFalse(
                        "the log ring wedged: a guaranteed producer never finished publishing after the "
                                + "CursorHolder allocation failure (slot stranded, nextBully spinning)",
                        producer.isAlive()
                );
                Assert.assertTrue("producer did not complete all writes", finishedAllWrites[0]);

                factory.flushJobs();
                Assert.assertTrue("no records reached the consumer", logFile.length() > 0);
            } finally {
                LogFactory.disableGuaranteedLogging();
            }
        }
    }
}
