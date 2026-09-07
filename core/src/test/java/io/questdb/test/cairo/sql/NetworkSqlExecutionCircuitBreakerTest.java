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

import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestMillisecondClock;
import io.questdb.test.tools.TestNetworkSqlExecutionCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
    public void testAtomicBooleanConditionalClearLinearizesWithCancel() throws Exception {
        assertMemoryLeak(() -> assertConditionalClearLinearizesWithCancel(
                new AtomicBooleanCircuitBreaker(engine)
        ));
    }

    @Test
    public void testCooperativePollVariantsUseEngineHookAfterBreakerCheck() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger pollCount = new AtomicInteger();
            final AtomicLong pollClockTicks = new AtomicLong();
            final NanosecondClock pollClock = pollClockTicks::get;
            final CairoConfigurationWrapper pollingConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public NanosecondClock getNanosecondClock() {
                    return pollClock;
                }
            };
            try (CairoEngine pollingEngine = new CairoEngine(pollingConfiguration, false) {
                {
                    enableSqlExecutionCooperativePolling();
                }

                @Override
                public void onSqlExecutionCooperativePoll() {
                    pollCount.incrementAndGet();
                }
            }) {
                final int statefulStride = SqlExecutionCircuitBreaker.STATEFUL_COOPERATIVE_POLL_STRIDE;
                Assert.assertEquals(
                        "the hot-path mask requires a power-of-two stride",
                        1,
                        Integer.bitCount(statefulStride)
                );

                final AtomicBooleanCircuitBreaker sharedAtomicBreaker =
                        new AtomicBooleanCircuitBreaker(pollingEngine);
                assertSharedAtomicCooperativePoll(sharedAtomicBreaker, pollCount);
                final AtomicBooleanCircuitBreaker throttledAtomicBreaker =
                        new AtomicBooleanCircuitBreaker(pollingEngine, 2 * statefulStride);
                assertCoarseCooperativePollCadence(throttledAtomicBreaker, pollCount);

                final TestMillisecondClock clock = new TestMillisecondClock(1_000);
                final SqlExecutionCircuitBreakerConfiguration coarseConfig =
                        new DefaultSqlExecutionCircuitBreakerConfiguration() {
                            @Override
                            public int getCircuitBreakerThrottle() {
                                return 2 * statefulStride;
                            }

                            @Override
                            public @NotNull MillisecondClock getClock() {
                                return clock;
                            }
                        };
                try (
                        NetworkSqlExecutionCircuitBreaker networkBreaker =
                                new NetworkSqlExecutionCircuitBreaker(pollingEngine, coarseConfig);
                        SqlExecutionCircuitBreakerWrapper wrapper =
                                new SqlExecutionCircuitBreakerWrapper(pollingEngine, coarseConfig)
                ) {
                    networkBreaker.resetTimer();
                    assertCoarseCooperativePollCadence(networkBreaker, pollCount);
                    final int countBeforeRearm = pollCount.get();
                    networkBreaker.statefulThrowExceptionIfTrippedOrYield();
                    networkBreaker.rearmTimer();
                    networkBreaker.statefulThrowExceptionIfTrippedOrYield();
                    Assert.assertEquals(
                            "a task rearm must preserve cross-task cooperative coalescing",
                            countBeforeRearm + 1,
                            pollCount.get()
                    );
                    wrapper.init(networkBreaker);
                    assertCoarseCooperativePollCadence(wrapper, pollCount);
                }

                final AtomicBooleanCircuitBreaker coalescedStatefulBreaker =
                        new AtomicBooleanCircuitBreaker(pollingEngine, 2 * statefulStride);
                final int countBeforeCoalescedStatefulChecks = pollCount.get();
                for (int i = 0; i < statefulStride; i++) {
                    coalescedStatefulBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
                Assert.assertEquals(
                        "the time window must coalesce successful stateful checks",
                        countBeforeCoalescedStatefulChecks + 1,
                        pollCount.get()
                );
                coalescedStatefulBreaker.reset();
                coalescedStatefulBreaker.statefulThrowExceptionIfTrippedOrYield();
                Assert.assertEquals(
                        "reset must restart the cooperative cadence",
                        countBeforeCoalescedStatefulChecks + 2,
                        pollCount.get()
                );
                pollClockTicks.addAndGet(-SqlExecutionCircuitBreaker.COOPERATIVE_POLL_INTERVAL_NANOS);
                for (int i = 0; i < statefulStride; i++) {
                    coalescedStatefulBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
                Assert.assertEquals(
                        "a clock rollback must restart cooperative polling",
                        countBeforeCoalescedStatefulChecks + 3,
                        pollCount.get()
                );

                // Exercise the stateful hot path with the two legal edge throttles and a larger
                // non-divisor of the cooperative stride. Small throttles retain the coarse visit
                // cadence; large throttles also poll on their real breaker checks.
                final int[] throttles = {0, 5, statefulStride + 513};
                for (int throttle : throttles) {
                    final AtomicBooleanCircuitBreaker statefulAtomicBreaker =
                            new AtomicBooleanCircuitBreaker(pollingEngine, throttle);
                    assertStatefulCooperativePollCadence(statefulAtomicBreaker, pollClockTicks, pollCount, throttle);

                    final SqlExecutionCircuitBreakerConfiguration statefulConfig =
                            new DefaultSqlExecutionCircuitBreakerConfiguration() {
                                @Override
                                public int getCircuitBreakerThrottle() {
                                    return throttle;
                                }

                                @Override
                                public @NotNull MillisecondClock getClock() {
                                    return clock;
                                }
                            };
                    try (NetworkSqlExecutionCircuitBreaker statefulNetworkBreaker =
                                 new NetworkSqlExecutionCircuitBreaker(pollingEngine, statefulConfig)) {
                        assertStatefulCooperativePollCadence(
                                statefulNetworkBreaker,
                                pollClockTicks,
                                pollCount,
                                throttle
                        );
                    }
                }

                final AtomicBooleanCircuitBreaker cancelledBreaker =
                        new AtomicBooleanCircuitBreaker(pollingEngine, 2 * statefulStride);
                cancelledBreaker.setCancelledFlag(new AtomicBoolean(true));
                final int countBeforeCancel = pollCount.get();
                Assert.assertTrue(cancelledBreaker.checkIfTrippedOrYield());
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, cancelledBreaker.getStateOrYield());
                try {
                    cancelledBreaker.statefulThrowExceptionIfTrippedOrYield();
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isInterruption());
                }
                Assert.assertEquals("a tripped breaker must suppress the engine hook", countBeforeCancel, pollCount.get());

                final SqlExecutionCircuitBreakerConfiguration timeoutConfig =
                        new DefaultSqlExecutionCircuitBreakerConfiguration() {
                            @Override
                            public @NotNull MillisecondClock getClock() {
                                return clock;
                            }

                            @Override
                            public long getQueryTimeout() {
                                return 100;
                            }
                        };
                try (NetworkSqlExecutionCircuitBreaker timeoutBreaker =
                             new NetworkSqlExecutionCircuitBreaker(pollingEngine, timeoutConfig)) {
                    timeoutBreaker.resetTimer();
                    clock.millis += 101;
                    final int countBeforeTimeout = pollCount.get();
                    Assert.assertTrue(timeoutBreaker.checkIfTrippedOrYield());
                    try {
                        timeoutBreaker.statefulThrowExceptionIfTrippedOrYield();
                        Assert.fail("expected timeout");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isInterruption());
                    }
                    Assert.assertEquals("a timed-out breaker must suppress the engine hook", countBeforeTimeout, pollCount.get());
                }
            }
        });
    }

    @Test
    public void testDisabledCooperativePollingKeepsOriginalBreakerPath() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger clockReadCount = new AtomicInteger();
            final AtomicInteger pollCount = new AtomicInteger();
            final CairoConfigurationWrapper disabledConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public NanosecondClock getNanosecondClock() {
                    return () -> clockReadCount.getAndIncrement();
                }
            };
            try (CairoEngine disabledEngine = new CairoEngine(disabledConfiguration, false) {
                @Override
                public void onSqlExecutionCooperativePoll() {
                    pollCount.incrementAndGet();
                }
            }) {
                Assert.assertFalse(disabledEngine.isSqlExecutionCooperativePollingEnabled());
                final int throttle = 2 * SqlExecutionCircuitBreaker.STATEFUL_COOPERATIVE_POLL_STRIDE;
                final AtomicBooleanCircuitBreaker atomicBreaker =
                        new AtomicBooleanCircuitBreaker(disabledEngine, throttle);
                final TestMillisecondClock networkClock = new TestMillisecondClock(1_000);
                final SqlExecutionCircuitBreakerConfiguration networkConfiguration =
                        new DefaultSqlExecutionCircuitBreakerConfiguration() {
                            @Override
                            public int getCircuitBreakerThrottle() {
                                return throttle;
                            }

                            @Override
                            public @NotNull MillisecondClock getClock() {
                                return networkClock;
                            }
                        };
                try (NetworkSqlExecutionCircuitBreaker networkBreaker =
                             new NetworkSqlExecutionCircuitBreaker(disabledEngine, networkConfiguration)) {
                    atomicBreaker.resetTimer();
                    networkBreaker.resetTimer();
                    for (int i = 0; i < 3 * throttle; i++) {
                        atomicBreaker.statefulThrowExceptionIfTrippedOrYield();
                        networkBreaker.statefulThrowExceptionIfTrippedOrYield();
                    }
                    Assert.assertFalse(atomicBreaker.checkIfTrippedOrYield());
                    Assert.assertFalse(atomicBreaker.checkIfTrippedOrYield(0, -1));
                    Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, atomicBreaker.getStateOrYield());
                    Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, atomicBreaker.getStateOrYield(0, -1));
                    atomicBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
                    atomicBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                    Assert.assertFalse(networkBreaker.checkIfTrippedOrYield());
                    Assert.assertFalse(networkBreaker.checkIfTrippedOrYield(1_000, -1));
                    Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, networkBreaker.getStateOrYield());
                    Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, networkBreaker.getStateOrYield(1_000, -1));
                    networkBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
                    networkBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();

                    atomicBreaker.cancel();
                    Assert.assertTrue(atomicBreaker.checkIfTrippedOrYield());
                    try {
                        atomicBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
                        Assert.fail("expected cancellation");
                    } catch (CairoException e) {
                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                    }

                    networkBreaker.cancel();
                    Assert.assertTrue(networkBreaker.checkIfTrippedOrYield());
                    try {
                        networkBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
                        Assert.fail("expected cancellation");
                    } catch (CairoException e) {
                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                    }
                }
                Assert.assertEquals("disabled polling must not read the cooperative clock", 0, clockReadCount.get());
                Assert.assertEquals("disabled polling must not invoke the engine hook", 0, pollCount.get());
            }
        });
    }

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
    public void testHighThrottleStatefulPollSamplesClockAtStride() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(50_000L, SqlExecutionCircuitBreaker.COOPERATIVE_POLL_INTERVAL_NANOS);
            final AtomicInteger clockReadCount = new AtomicInteger();
            final CairoConfigurationWrapper pollingConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public NanosecondClock getNanosecondClock() {
                    return () -> {
                        clockReadCount.incrementAndGet();
                        return 0;
                    };
                }
            };
            try (CairoEngine pollingEngine = new CairoEngine(pollingConfiguration, false) {
                {
                    enableSqlExecutionCooperativePolling();
                }
            }) {
                final int stride = SqlExecutionCircuitBreaker.STATEFUL_COOPERATIVE_POLL_STRIDE;
                final int throttle = 3 * stride + 1;
                final AtomicBooleanCircuitBreaker atomicBreaker =
                        new AtomicBooleanCircuitBreaker(pollingEngine, throttle);
                atomicBreaker.resetTimer();
                for (int i = 0; i < 3 * stride; i++) {
                    atomicBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
                Assert.assertEquals(3, clockReadCount.get());

                final SqlExecutionCircuitBreakerConfiguration networkConfiguration =
                        new DefaultSqlExecutionCircuitBreakerConfiguration() {
                            @Override
                            public int getCircuitBreakerThrottle() {
                                return throttle;
                            }
                        };
                clockReadCount.set(0);
                try (NetworkSqlExecutionCircuitBreaker networkBreaker =
                             new NetworkSqlExecutionCircuitBreaker(pollingEngine, networkConfiguration)) {
                    networkBreaker.resetTimer();
                    for (int i = 0; i < 3 * stride; i++) {
                        networkBreaker.statefulThrowExceptionIfTrippedOrYield();
                    }
                    Assert.assertEquals(3, clockReadCount.get());
                }
            }
        });
    }

    @Test
    public void testNetworkConditionalClearLinearizesWithCancel() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock, 100_000)) {
                assertConditionalClearLinearizesWithCancel(breaker);
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

    @Test
    public void testWrapperIsolatesExactAtomicBreakerPerWorker() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger pollCount = new AtomicInteger();
            final CairoConfigurationWrapper pollingConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public NanosecondClock getNanosecondClock() {
                    return () -> 0;
                }
            };
            try (CairoEngine pollingEngine = new CairoEngine(pollingConfiguration, false) {
                {
                    enableSqlExecutionCooperativePolling();
                }

                @Override
                public void onSqlExecutionCooperativePoll() {
                    pollCount.incrementAndGet();
                }
            }) {
                final AtomicBoolean cancellationFlag = new AtomicBoolean();
                final AtomicBooleanCircuitBreaker ownerBreaker = new AtomicBooleanCircuitBreaker(pollingEngine, 5);
                ownerBreaker.setCancelledFlag(cancellationFlag);
                ownerBreaker.setFd(42);
                final SqlExecutionCircuitBreakerConfiguration wrapperConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                    @Override
                    public int getCircuitBreakerThrottle() {
                        return 17;
                    }
                };
                try (
                        SqlExecutionCircuitBreakerWrapper first = new SqlExecutionCircuitBreakerWrapper(
                                pollingEngine,
                                wrapperConfiguration
                        );
                        SqlExecutionCircuitBreakerWrapper second = new SqlExecutionCircuitBreakerWrapper(
                                pollingEngine,
                                wrapperConfiguration
                        )
                ) {
                    Assert.assertTrue(first.hasLocalAtomicCircuitBreaker());
                    Assert.assertTrue(second.hasLocalAtomicCircuitBreaker());
                    first.init(ownerBreaker);
                    second.init(ownerBreaker);
                    Assert.assertNotSame(ownerBreaker, first.getDelegate());
                    Assert.assertNotSame(ownerBreaker, second.getDelegate());
                    Assert.assertNotSame(first.getDelegate(), second.getDelegate());
                    Assert.assertEquals(AtomicBooleanCircuitBreaker.class, first.getDelegate().getClass());
                    Assert.assertEquals(42, first.getFd());

                    final int initialPollCount = pollCount.get();
                    first.statefulThrowExceptionIfTrippedOrYield();
                    second.statefulThrowExceptionIfTrippedOrYield();
                    Assert.assertEquals(
                            "each worker copy must start with an independent cooperative cadence",
                            initialPollCount + 2,
                            pollCount.get()
                    );

                    cancellationFlag.set(true);
                    for (int i = 0; i < 4; i++) {
                        first.statefulThrowExceptionIfTrippedOrYield();
                    }
                    try {
                        first.statefulThrowExceptionIfTrippedOrYield();
                        Assert.fail("expected copied throttle boundary to observe cancellation");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isInterruption());
                    }
                    cancellationFlag.set(false);

                    final int pollCountBeforeRebind = pollCount.get();
                    first.clear();
                    first.init(ownerBreaker);
                    first.statefulThrowExceptionIfTrippedOrYield();
                    Assert.assertEquals(
                            "worker-local cooperative cadence must span reduce-task rebinds",
                            pollCountBeforeRebind,
                            pollCount.get()
                    );

                    final FiberCancellationSignal signal = new FiberCancellationSignal();
                    final long staleGeneration = signal.getGeneration();
                    ownerBreaker.setCancelledFlag(signal, staleGeneration);
                    final long currentGeneration = signal.reopen();
                    first.init(ownerBreaker);
                    try {
                        first.statefulThrowExceptionIfTrippedNoThrottle();
                        Assert.fail("expected copied stale cancellation binding to fail closed");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isInterruption());
                    }
                    first.cancel();
                    Assert.assertFalse(signal.isCancelled(currentGeneration));

                    final AtomicBooleanCircuitBreaker customBreaker =
                            new AtomicBooleanCircuitBreaker(pollingEngine) {
                            };
                    second.init(customBreaker);
                    Assert.assertSame("custom subclasses must retain their behavior", customBreaker, second.getDelegate());
                }
            }
        });
    }

    @Test
    public void testWrapperSharesAtomicDelegateWhenCooperativePollingIsDisabled() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBooleanCircuitBreaker ownerBreaker = new AtomicBooleanCircuitBreaker(engine, 5);
            try (SqlExecutionCircuitBreakerWrapper wrapper = new SqlExecutionCircuitBreakerWrapper(
                    engine,
                    new DefaultSqlExecutionCircuitBreakerConfiguration()
            )) {
                Assert.assertFalse(wrapper.hasLocalAtomicCircuitBreaker());
                wrapper.init(ownerBreaker);
                Assert.assertSame(ownerBreaker, wrapper.getDelegate());
            }
        });
    }

    @Test
    public void testWrapperPreservesCancellationGeneration() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionCircuitBreakerConfiguration config =
                    new DefaultSqlExecutionCircuitBreakerConfiguration();
            try (
                    NetworkSqlExecutionCircuitBreaker ownerBreaker =
                            new NetworkSqlExecutionCircuitBreaker(engine, config);
                    SqlExecutionCircuitBreakerWrapper wrapper =
                            new SqlExecutionCircuitBreakerWrapper(engine, config)
            ) {
                final FiberCancellationSignal signal = new FiberCancellationSignal();
                final long staleGeneration = signal.getGeneration();
                ownerBreaker.setCancelledFlag(signal, staleGeneration);
                final long currentGeneration = signal.reopen();

                wrapper.init(ownerBreaker);
                try {
                    wrapper.statefulThrowExceptionIfTrippedNoThrottle();
                    Assert.fail("expected stale cancellation binding to fail closed");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isInterruption());
                    Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                }

                wrapper.cancel();
                Assert.assertFalse(signal.isCancelled(currentGeneration));
            }
        });
    }

    private static void assertConditionalClearLinearizesWithCancel(SqlExecutionCircuitBreaker breaker) throws Exception {
        FiberCancellationSignal signal = new FiberCancellationSignal();
        breaker.setCancelledFlag(signal);

        CountDownLatch cancelStarted = new CountDownLatch(1);
        CountDownLatch clearStarted = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread cancelThread = new Thread(() -> {
            cancelStarted.countDown();
            try {
                breaker.cancel();
            } catch (Throwable th) {
                failure.compareAndSet(null, th);
            }
        }, "circuit-breaker-cancel");
        Thread clearThread = new Thread(() -> {
            clearStarted.countDown();
            try {
                breaker.clearCancelledFlag(signal);
            } catch (Throwable th) {
                failure.compareAndSet(null, th);
            }
        }, "circuit-breaker-clear");

        boolean isClearBlocked = false;
        try {
            synchronized (signal) {
                cancelThread.start();
                await(cancelStarted);
                Assert.assertTrue("cancel did not block on the cancellation signal", awaitBlocked(cancelThread));

                clearThread.start();
                await(clearStarted);
                isClearBlocked = awaitBlocked(clearThread);
            }
        } finally {
            join(cancelThread);
            join(clearThread);
        }

        Throwable th = failure.get();
        if (th != null) {
            throw new AssertionError(th);
        }
        Assert.assertTrue("conditional clear completed before the in-flight cancel", isClearBlocked);
        Assert.assertTrue(signal.get());
        Assert.assertNull(breaker.getCancelledFlag());

        AtomicBoolean replacement = new AtomicBoolean();
        breaker.setCancelledFlag(replacement);
        breaker.clearCancelledFlag(signal);
        Assert.assertSame(replacement, breaker.getCancelledFlag());
        breaker.clearCancelledFlag(replacement);
        Assert.assertNull(breaker.getCancelledFlag());
    }

    private static void assertCoarseCooperativePollCadence(
            SqlExecutionCircuitBreaker breaker,
            AtomicInteger pollCount
    ) {
        final int initialPollCount = pollCount.get();
        final int stride = SqlExecutionCircuitBreaker.COOPERATIVE_POLL_STRIDE;
        Assert.assertFalse(breaker.checkIfTrippedOrYield());
        Assert.assertEquals(initialPollCount + 1, pollCount.get());
        Assert.assertFalse(breaker.checkIfTrippedOrYield(0, -1));
        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, breaker.getStateOrYield());
        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, breaker.getStateOrYield(0, -1));
        breaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
        breaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        Assert.assertEquals(initialPollCount + 1, pollCount.get());
        for (int i = 6; i < stride; i++) {
            Assert.assertFalse(breaker.checkIfTrippedOrYield());
        }
        Assert.assertEquals(initialPollCount + 1, pollCount.get());
        Assert.assertFalse(breaker.checkIfTrippedOrYield());
        Assert.assertEquals(initialPollCount + 2, pollCount.get());
        for (int i = 1; i < stride; i++) {
            Assert.assertFalse(breaker.checkIfTrippedOrYield());
        }
        Assert.assertEquals(initialPollCount + 2, pollCount.get());
        Assert.assertFalse(breaker.checkIfTrippedOrYield());
        Assert.assertEquals(initialPollCount + 3, pollCount.get());
    }

    private static void assertSharedAtomicCooperativePoll(
            AtomicBooleanCircuitBreaker breaker,
            AtomicInteger pollCount
    ) throws InterruptedException {
        final int initialPollCount = pollCount.get();
        Assert.assertFalse(breaker.checkIfTrippedOrYield());
        Assert.assertFalse(breaker.checkIfTrippedOrYield(0, -1));
        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, breaker.getStateOrYield());
        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, breaker.getStateOrYield(0, -1));
        Assert.assertEquals(
                "shared boolean/state checks must each poll without mutable shared cadence",
                initialPollCount + 4,
                pollCount.get()
        );

        final int callsPerThread = 1_000;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Runnable poll = () -> {
            try {
                start.await();
                for (int i = 0; i < callsPerThread; i++) {
                    Assert.assertFalse(breaker.checkIfTrippedOrYield());
                }
            } catch (Throwable th) {
                failure.compareAndSet(null, th);
            }
        };
        final Thread first = new Thread(poll, "atomic-breaker-poll-1");
        final Thread second = new Thread(poll, "atomic-breaker-poll-2");
        first.start();
        second.start();
        start.countDown();
        join(first);
        join(second);
        Assert.assertNull(failure.get());
        Assert.assertEquals(initialPollCount + 4 + 2 * callsPerThread, pollCount.get());
    }

    private static void assertStatefulCooperativePollCadence(
            SqlExecutionCircuitBreaker breaker,
            AtomicLong pollClockTicks,
            AtomicInteger pollCount,
            int throttle
    ) {
        final int stride = SqlExecutionCircuitBreaker.STATEFUL_COOPERATIVE_POLL_STRIDE;
        final int visitCount = 3 * Math.max(stride, Math.max(throttle, 1)) + 1;
        int breakerVisitCount = 0;
        int expectedPollCount = pollCount.get();
        breaker.resetTimer();
        for (int i = 0; i < visitCount; i++) {
            final boolean isRealCheck = breakerVisitCount == 0 || breakerVisitCount >= throttle;
            final boolean isBatchBoundary = (breakerVisitCount & (stride - 1)) == 0;
            final boolean isCooperativePoll = throttle <= stride
                    ? i % SqlExecutionCircuitBreaker.COOPERATIVE_POLL_STRIDE == 0
                    : isRealCheck || isBatchBoundary;
            if (i > 0 && isCooperativePoll) {
                pollClockTicks.addAndGet(SqlExecutionCircuitBreaker.COOPERATIVE_POLL_INTERVAL_NANOS);
            }
            breaker.statefulThrowExceptionIfTrippedOrYield();
            if (isRealCheck) {
                breakerVisitCount = 0;
            }
            breakerVisitCount++;
            if (isCooperativePoll) {
                expectedPollCount++;
            }
            Assert.assertEquals("unexpected poll cadence [throttle=" + throttle + ", visit=" + i + ']',
                    expectedPollCount, pollCount.get());
        }
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        Assert.assertTrue("thread did not start", latch.await(10, TimeUnit.SECONDS));
    }

    private static boolean awaitBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.BLOCKED) {
                return true;
            }
            if (state == Thread.State.TERMINATED) {
                return false;
            }
            LockSupport.parkNanos(100_000);
        }
        Assert.fail("thread did not block or terminate [name=" + thread.getName() + ", state=" + thread.getState() + ']');
        return false;
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(10));
        Assert.assertFalse("thread did not terminate [name=" + thread.getName() + ']', thread.isAlive());
    }

    private static TestNetworkSqlExecutionCircuitBreaker newBreaker(MillisecondClock clock, long queryTimeout) {
        return TestNetworkSqlExecutionCircuitBreaker.create(engine, clock, THROTTLE, queryTimeout);
    }
}
