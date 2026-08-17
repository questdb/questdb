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
    private static final String POISON = "log-capture-test-poison-marker";
    private static final long RELEASE_DELAY_MS = 100;

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
