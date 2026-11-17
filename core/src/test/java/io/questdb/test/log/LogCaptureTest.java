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
    private static final Log LOG = LogFactory.getLog(LogCaptureTest.class);
    private static final String MARKER = "log-capture-test-stop-marker";
    private static final String POISON = "log-capture-test-poison-marker";
    private static final long RELEASE_DELAY_MS = 100;
    // Must exceed LogCapture.DRAIN_TIMEOUT_MS: released sooner, POISON drains inside
    // start()'s deadline and the test passes without pinning anything. Released later,
    // start() has already given up on its sentinel and cleared the sink, so POISON is
    // still in flight and lands in the fresh capture -- the cross-test leak this test
    // pins.
    private static final long START_RELEASE_DELAY_MS = LogCapture.DRAIN_TIMEOUT_MS + 1_000;

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

            // Released only after DRAIN_TIMEOUT_MS has elapsed, so a start() that
            // gives up on its sentinel that early clears an empty sink and lets POISON
            // land in the new capture (red). A start() that drains with the stop
            // deadline waits past the release, POISON drains ahead of the clear, and
            // the window stays clean (green).
            releaser = new Thread(() -> {
                Os.sleep(START_RELEASE_DELAY_MS);
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
            // STOP_DRAIN_TIMEOUT_MS, so this test parks through start()'s full
            // deadline. The release is scheduled only after that, so it lands
            // while stop() below is draining -- which is what pins stop()'s
            // drain: released any earlier, MARKER would be delivered before
            // stop() and the assertion could no longer tell a draining stop()
            // from one that drops the record on the console.
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
