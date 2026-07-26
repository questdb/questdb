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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestMillisecondClock;
import io.questdb.test.tools.TestNetworkSqlExecutionCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pins the wall-clock throttle on the breaker's connection probe: cancellation and timeout
 * are checked on every call, the probe fires at most once per
 * {@link SqlExecutionCircuitBreakerConfiguration#getCircuitBreakerConnectionCheckThrottle()}
 * window, and the window survives {@code of()}/{@code rearmTimer()} for the same fd so the
 * per-task wrapper re-init on the parallel reduce path cannot turn the probe into a
 * once-per-frame syscall.
 */
public class NetworkSqlExecutionCircuitBreakerTest extends AbstractCairoTest {

    private static final long THROTTLE = 100;

    @Test
    public void testCheckIfTrippedNoThrottleBypassesWindow() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100_000)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);

                breaker.isConnectionBroken = true;
                Assert.assertFalse("probe inside the window must be throttled", breaker.checkIfTripped());
                Assert.assertTrue("classifier variant must probe despite the window", breaker.checkIfTrippedNoThrottle());
                Assert.assertEquals(2, breaker.probeCount);
            }
        });
    }

    @Test
    public void testCheckIfTrippedThrottlesConnectionProbe() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100_000)) {
                breaker.of(1);
                breaker.resetTimer();

                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals("second check inside the window must not probe", 1, breaker.probeCount);

                clock.millis += THROTTLE - 1;
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);

                clock.millis += 1;
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals("check after the window elapsed must probe", 2, breaker.probeCount);
            }
        });
    }

    @Test
    public void testGetStateReportsBrokenConnectionOncePerWindow() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100_000)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, breaker.getState());
                Assert.assertEquals(1, breaker.probeCount);

                breaker.isConnectionBroken = true;
                Assert.assertEquals("disconnect inside the window is reported on the next probe, not immediately",
                        SqlExecutionCircuitBreaker.STATE_OK, breaker.getState());

                clock.millis += THROTTLE;
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION, breaker.getState());
                Assert.assertEquals(2, breaker.probeCount);
            }
        });
    }

    @Test
    public void testOfKeepsThrottleWindowForSameFd() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100_000)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);

                breaker.of(1);
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals("re-binding the same fd must keep the window", 1, breaker.probeCount);

                breaker.of(2);
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals("binding a different fd must force a prompt probe", 2, breaker.probeCount);
            }
        });
    }

    @Test
    public void testRearmTimerKeepsThrottleWindow() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 1_000)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);

                clock.millis = 1_050;
                breaker.rearmTimer();
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals("rearmTimer must not reopen the window", 1, breaker.probeCount);

                // powerUpTime moved to 1050: 1010ms of runtime against the original arm point
                // is under the 1000ms timeout against the new one.
                clock.millis = 2_010;
                Assert.assertFalse(breaker.checkIfTripped());

                clock.millis = 2_100;
                Assert.assertTrue("timeout must be measured from the rearm point", breaker.checkIfTripped());
            }
        });
    }

    @Test
    public void testResetTimerForcesPromptProbe() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100_000)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);

                clock.millis += 1;
                breaker.resetTimer();
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals("a new query must probe promptly", 2, breaker.probeCount);
            }
        });
    }

    @Test
    public void testTimeThrottledThrowsOnDisconnectOutsideWindow() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100_000)) {
                breaker.of(1);
                breaker.resetTimer();
                breaker.isConnectionBroken = true;
                try {
                    breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    Assert.fail("expected remote disconnect");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "remote disconnected");
                }
                Assert.assertEquals(1, breaker.probeCount);

                // The failed probe opened the window; the next check inside it must not probe.
                breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                Assert.assertEquals(1, breaker.probeCount);
            }
        });
    }

    @Test
    public void testTimeoutAndCancellationBypassProbe() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100)) {
                breaker.of(1);
                breaker.resetTimer();
                clock.millis = 1_101;
                Assert.assertTrue(breaker.checkIfTripped());
                Assert.assertEquals("timeout must trip without a connection probe", 0, breaker.probeCount);
            }

            TestMillisecondClock cancelClock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(cancelClock, 100_000)) {
                AtomicBoolean cancelledFlag = new AtomicBoolean();
                breaker.setCancelledFlag(cancelledFlag);
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);

                cancelledFlag.set(true);
                Assert.assertTrue("cancellation must trip on every call, unthrottled", breaker.checkIfTripped());
                Assert.assertEquals(1, breaker.probeCount);
            }
        });
    }

    @Test
    public void testWrapperInitKeepsThrottleWindowAcrossTasks() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            int[] probeCount = new int[1];
            SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public long getCircuitBreakerConnectionCheckThrottle() {
                    return THROTTLE;
                }

                @Override
                public @NotNull MillisecondClock getClock() {
                    return clock;
                }

                @Override
                public @NotNull NetworkFacade getNetworkFacade() {
                    return new NetworkFacadeImpl() {
                        @Override
                        public boolean testConnection(long fd, long buffer, int bufferSize) {
                            probeCount[0]++;
                            return false;
                        }
                    };
                }

                @Override
                public long getQueryTimeout() {
                    return 100_000;
                }
            };
            try (
                    NetworkSqlExecutionCircuitBreaker ownerBreaker = new NetworkSqlExecutionCircuitBreaker(engine, config);
                    SqlExecutionCircuitBreakerWrapper wrapper = new SqlExecutionCircuitBreakerWrapper(engine, config)
            ) {
                ownerBreaker.of(1);
                ownerBreaker.resetTimer();

                wrapper.init(ownerBreaker);
                wrapper.statefulThrowExceptionIfTrippedTimeThrottled();
                Assert.assertEquals(1, probeCount[0]);

                wrapper.init(ownerBreaker);
                wrapper.statefulThrowExceptionIfTrippedTimeThrottled();
                Assert.assertEquals("per-task wrapper re-init must not reopen the probe window", 1, probeCount[0]);

                clock.millis += THROTTLE;
                wrapper.init(ownerBreaker);
                wrapper.statefulThrowExceptionIfTrippedTimeThrottled();
                Assert.assertEquals("probe must fire once the window elapses across re-inits", 2, probeCount[0]);
            }
        });
    }

    private static TestNetworkSqlExecutionCircuitBreaker newBreaker(MillisecondClock clock, long queryTimeout) {
        return TestNetworkSqlExecutionCircuitBreaker.create(engine, clock, THROTTLE, queryTimeout);
    }
}
