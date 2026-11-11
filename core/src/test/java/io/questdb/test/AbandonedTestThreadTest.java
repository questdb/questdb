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

package io.questdb.test;

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runners.model.TestTimedOutException;

import java.util.concurrent.CountDownLatch;

/**
 * A timeout rule fails a test that runs too long, but it cannot stop the test's thread. Every later test in the
 * JVM must then fail and name the timed-out test, instead of running beside its thread and reporting what that
 * thread does (memory it maps, tables it holds) as its own failure.
 * <p>
 * Not an {@link AbstractTest} itself: it runs one through {@link JUnitCore}, and the two would share the static
 * test root.
 */
public class AbandonedTestThreadTest {

    @Test
    public void testLaterTestFailsWhileTimedOutTestThreadRuns() throws Exception {
        StuckThenNext.release = new CountDownLatch(1);
        StuckThenNext.enabled = true;
        try {
            final Result stuck = new JUnitCore().run(Request.method(StuckThenNext.class, "testStuck"));
            Assert.assertEquals(1, stuck.getFailureCount());
            Assert.assertTrue(stuck.getFailures().getFirst().getException() instanceof TestTimedOutException);
            Assert.assertTrue(StuckThenNext.stuckThread.isAlive());

            for (int i = 0; i < 2; i++) {
                StuckThenNext.nextRan = false;
                final Result next = new JUnitCore().run(Request.method(StuckThenNext.class, "testNext"));
                Assert.assertEquals(1, next.getFailureCount());
                TestUtils.assertContains(next.getFailures().getFirst().getMessage(), "StuckThenNext#testStuck");
                Assert.assertFalse(StuckThenNext.nextRan);
            }

            StuckThenNext.release.countDown();
            StuckThenNext.stuckThread.join();
            StuckThenNext.nextRan = false;
            final Result afterExit = new JUnitCore().run(Request.method(StuckThenNext.class, "testNext"));
            Assert.assertEquals(0, afterExit.getFailureCount());
            Assert.assertTrue(StuckThenNext.nextRan);
        } finally {
            StuckThenNext.release.countDown();
            if (StuckThenNext.stuckThread != null) {
                StuckThenNext.stuckThread.join();
            }
            StuckThenNext.enabled = false;
        }
    }

    /**
     * Runs only from {@link AbandonedTestThreadTest}: run on its own, both tests are skipped.
     */
    public static class StuckThenNext extends AbstractTest {
        static volatile boolean enabled;
        static volatile boolean nextRan;
        static volatile CountDownLatch release;
        static volatile Thread stuckThread;
        @Rule
        public final TestRule timeout = (base, description) -> Timeout.millis(
                "testStuck".equals(description.getMethodName()) ? 500 : 60_000
        ).apply(base, description);

        @Test
        public void testNext() {
            Assume.assumeTrue(enabled);
            nextRan = true;
        }

        @Test
        public void testStuck() {
            Assume.assumeTrue(enabled);
            stuckThread = Thread.currentThread();
            // ignores the interrupt on timeout, like a test busy in native code
            while (true) {
                try {
                    release.await();
                    return;
                } catch (InterruptedException ignore) {
                }
            }
        }
    }
}
