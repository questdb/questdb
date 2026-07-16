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

package io.questdb.test.cutlass.parquet;

import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestMillisecondClock;
import io.questdb.test.tools.TestNetworkSqlExecutionCircuitBreaker;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

// A copy/export failing while the client has disconnected must classify as CANCELLED even when a
// recent probe has opened the connection-check throttle window; reverting classifyFailureStatus to
// the throttled checkIfTripped() reads the broken-connection case as FAILED.
public class CopyExportRequestTaskTest extends AbstractCairoTest {

    @Test
    public void testClassifyFailureStatusCancelledOnBrokenConnectionWithinThrottleWindow() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                // A first probe against a live connection opens the throttle window.
                Assert.assertEquals(CopyExportRequestTask.Status.FAILED, CopyExportRequestTask.classifyFailureStatus(breaker));

                breaker.isConnectionBroken = true;
                // Inside the window the throttled check still reads the socket as alive, so the classifier
                // must bypass the throttle to attribute the failure to the disconnect.
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(CopyExportRequestTask.Status.CANCELLED, CopyExportRequestTask.classifyFailureStatus(breaker));
            }
        });
    }

    @Test
    public void testClassifyFailureStatusCancelledOnCancellation() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                breaker.setCancelledFlag(new AtomicBoolean(true));
                Assert.assertEquals(CopyExportRequestTask.Status.CANCELLED, CopyExportRequestTask.classifyFailureStatus(breaker));
            }
        });
    }

    @Test
    public void testClassifyFailureStatusCancelledOnTimeout() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertEquals(CopyExportRequestTask.Status.FAILED, CopyExportRequestTask.classifyFailureStatus(breaker));

                clock.millis += 100_001;
                Assert.assertEquals(
                        "an export failing past query.timeout must classify as CANCELLED",
                        CopyExportRequestTask.Status.CANCELLED,
                        CopyExportRequestTask.classifyFailureStatus(breaker)
                );
            }
        });
    }

    @Test
    public void testClassifyFailureStatusFailedOnHealthyConnection() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertEquals(CopyExportRequestTask.Status.FAILED, CopyExportRequestTask.classifyFailureStatus(breaker));
            }
        });
    }

    private static TestNetworkSqlExecutionCircuitBreaker newBreaker(MillisecondClock clock) {
        return TestNetworkSqlExecutionCircuitBreaker.create(engine, clock, 100, 100_000);
    }
}
