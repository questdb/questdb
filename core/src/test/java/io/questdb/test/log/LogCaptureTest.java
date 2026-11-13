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

import io.questdb.log.Log;
import io.questdb.log.LogConsoleWriter;
import io.questdb.log.LogFactory;
import io.questdb.log.LogWriter;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.test.tools.LogCapture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LogCaptureTest {
    private static final String BOUNDED_STOP_MARKER = "log-capture-test-bounded-stop-marker";
    private static final Log LOG = LogFactory.getLog(LogCaptureTest.class);
    private static final String MARKER = "log-capture-test-stop-marker";
    private static final String POISON = "log-capture-test-poison-marker";
    private static final long RELEASE_DELAY_MS = 100;

    @Test
    public void testIsInstalledFollowsTheLastInstalledCapture() {
        final LogConsoleWriter consoleWriter = getFirstConsoleWriter();
        try {
            final LogCapture first = new LogCapture();
            Assert.assertTrue(first.isInstalled());
            first.start();
            Assert.assertTrue(first.isInstalled());

            final LogCapture second = new LogCapture();
            Assert.assertFalse(first.isInstalled());
            Assert.assertTrue(second.isInstalled());
            second.start();
            second.stop();
            Assert.assertFalse(first.isInstalled());
            Assert.assertFalse(second.isInstalled());

            first.start();
            Assert.assertTrue(first.isInstalled());
            first.stop();
            Assert.assertFalse(first.isInstalled());
        } finally {
            consoleWriter.setInterceptor(null);
        }
    }

    /**
     * A record an earlier test enqueued must not land in a later test's capture.
     * The interceptor runs on the logging worker thread, so blocking inside it
     * parks the writer deterministically, holding POISON in the ring queue until
     * after the capture starts.
     */
    @Test
    public void testStartDropsRecordsEnqueuedBeforeIt() throws Exception {
        final LogConsoleWriter consoleWriter = getFirstConsoleWriter();
        final CountDownLatch parked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LogCapture capture = new LogCapture();
        Thread releaser = null;
        try {
            consoleWriter.setInterceptor(_ -> {
                parked.countDown();
                try {
                    release.await();
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            });

            LOG.advisory().$("log-capture-test-park-trigger").$();
            Assert.assertTrue(
                    "test setup: the logging worker must be parked inside the interceptor",
                    parked.await(30, TimeUnit.SECONDS)
            );

            LOG.advisory().$(POISON).$();

            // should start() lag past the delay, POISON drains ahead of the window
            // and the test passes without proving anything -- it cannot fail spuriously
            releaser = new Thread(() -> {
                Os.sleep(RELEASE_DELAY_MS);
                release.countDown();
            }, "log-capture-test-releaser");
            releaser.start();

            capture.start();
            try {
                // gives POISON the chance to land in the window it must not land in
                capture.drain();
                capture.assertNotLogged(POISON);
            } finally {
                capture.stop();
            }
        } finally {
            release.countDown();
            if (releaser != null) {
                releaser.join();
            }
            consoleWriter.setInterceptor(null);
        }
    }

    /**
     * A record logged before stop() must be captured even when the logging worker
     * has not delivered it yet: the worker is parked until stop() is already
     * waiting, so without the drain in stop() MARKER reaches the console instead.
     */
    @Test
    public void testStopDrainsRecordsEnqueuedBeforeIt() throws Exception {
        final LogConsoleWriter consoleWriter = getFirstConsoleWriter();
        final CountDownLatch parked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LogCapture capture = new LogCapture();
        Thread releaser = null;
        try {
            consoleWriter.setInterceptor(_ -> {
                parked.countDown();
                try {
                    release.await();
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            });

            LOG.advisory().$("log-capture-test-park-trigger").$();
            Assert.assertTrue(
                    "test setup: the logging worker must be parked inside the interceptor",
                    parked.await(30, TimeUnit.SECONDS)
            );

            // start() drains against the parked worker and gives up after
            // DRAIN_TIMEOUT_MS. The release is scheduled only after that, so it
            // lands while stop() below is draining.
            capture.start();
            LOG.advisory().$(MARKER).$();

            releaser = new Thread(() -> {
                Os.sleep(RELEASE_DELAY_MS);
                release.countDown();
            }, "log-capture-test-releaser");
            releaser.start();

            capture.stop();
            capture.assertLogged(MARKER);
        } finally {
            release.countDown();
            if (releaser != null) {
                releaser.join();
            }
            consoleWriter.setInterceptor(null);
        }
    }

    /**
     * stop(long) waits for a parked worker no longer than its bound, then takes the interceptor
     * away, so a record the worker still holds never reaches the capture. The capture's fake clock
     * measures the wait, so no real time decides it. testStopDrainsRecordsEnqueuedBeforeIt is the
     * contrast: there the worker comes back inside stop()'s wait.
     */
    @Test
    public void testStopWithABoundGivesUpOnAParkedWorker() throws Exception {
        final LogConsoleWriter consoleWriter = getFirstConsoleWriter();
        final CountDownLatch parked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LogCapture capture = new LogCapture();
        try {
            consoleWriter.setInterceptor(_ -> {
                parked.countDown();
                try {
                    release.await();
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            });

            LOG.advisory().$("log-capture-test-park-trigger").$();
            Assert.assertTrue(
                    "test setup: the logging worker must be parked inside the interceptor",
                    parked.await(30, TimeUnit.SECONDS)
            );

            final long[] nowMs = {0};
            capture.setClockForTest(() -> nowMs[0]);
            capture.setSleeperForTest(ms -> nowMs[0] += ms);
            capture.start();
            LOG.advisory().$(BOUNDED_STOP_MARKER).$();
            final long stopStartMs = nowMs[0];
            capture.stop(100);

            Assert.assertEquals("stop(100) waits 100 ms for the parked worker", 100, nowMs[0] - stopStartMs);
            Assert.assertFalse(capture.isInstalled());
            Assert.assertEquals(0, capture.countOccurrences(BOUNDED_STOP_MARKER));
        } finally {
            release.countDown();
            consoleWriter.setInterceptor(null);
        }
    }

    private static LogConsoleWriter getFirstConsoleWriter() {
        ObjHashSet<LogWriter> jobs = LogFactory.getInstance().getJobs();
        for (int i = 0, n = jobs.size(); i < n; i++) {
            LogWriter logWriter = jobs.get(i);
            if (logWriter instanceof LogConsoleWriter consoleWriter) {
                return consoleWriter;
            }
        }
        throw new AssertionError("no LogConsoleWriter configured");
    }
}
