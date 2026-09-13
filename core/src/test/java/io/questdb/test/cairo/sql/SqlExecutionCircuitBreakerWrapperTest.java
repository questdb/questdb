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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SqlExecutionCircuitBreakerWrapperTest extends AbstractCairoTest {
    @Test
    public void testSharedDelegateHasIndependentThrottlesAndRebinds() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger checks = new AtomicInteger();
            AtomicBooleanCircuitBreaker delegate = new AtomicBooleanCircuitBreaker(engine, 8) {
                @Override
                public void statefulThrowExceptionIfTripped() {
                    Assert.fail("workers must not share the delegate's mutable row counter");
                }

                @Override
                public void statefulThrowExceptionIfTrippedNoThrottle() {
                    checks.incrementAndGet();
                    super.statefulThrowExceptionIfTrippedNoThrottle();
                }
            };
            DefaultSqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public int getCircuitBreakerThrottle() {
                    return 8;
                }
            };
            try (SqlExecutionCircuitBreakerWrapper a = new SqlExecutionCircuitBreakerWrapper(engine, config);
                 SqlExecutionCircuitBreakerWrapper b = new SqlExecutionCircuitBreakerWrapper(engine, config)) {
                a.init(delegate);
                b.init(delegate);
                for (int i = 0; i < 8; i++) {
                    a.statefulThrowExceptionIfTripped();
                    b.statefulThrowExceptionIfTripped();
                }
                Assert.assertEquals(2, checks.get());
                delegate.cancel();
                for (SqlExecutionCircuitBreakerWrapper wrapper : new SqlExecutionCircuitBreakerWrapper[]{a, b}) {
                    try {
                        wrapper.statefulThrowExceptionIfTripped();
                        Assert.fail("each slot must check within its own throttle window");
                    } catch (CairoException ex) {
                        Assert.assertTrue(ex.isCancellation());
                    }
                }
                AtomicBooleanCircuitBreaker peer = new AtomicBooleanCircuitBreaker(engine, 8);
                a.init(peer);
                a.statefulThrowExceptionIfTripped();
                Assert.assertFalse(a.checkIfTripped());
                Assert.assertTrue(b.checkIfTripped());
                a.init(delegate);
                try {
                    a.statefulThrowExceptionIfTripped();
                    Assert.fail("a newly bound execution must check immediately");
                } catch (CairoException ex) {
                    Assert.assertTrue(ex.isCancellation());
                }
            }
        });
    }

    @Test
    public void testNetworkSlotsCopyTimeoutAndCancellationAndRebind() throws Exception {
        assertMemoryLeak(() -> {
            AtomicLong clock = new AtomicLong(1000);
            DefaultSqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public io.questdb.std.datetime.millitime.MillisecondClock getClock() {
                    return clock::get;
                }

                @Override
                public int getCircuitBreakerThrottle() {
                    return 8;
                }
            };
            try (NetworkSqlExecutionCircuitBreaker owner = new NetworkSqlExecutionCircuitBreaker(engine, config);
                 SqlExecutionCircuitBreakerWrapper a = new SqlExecutionCircuitBreakerWrapper(engine, config);
                 SqlExecutionCircuitBreakerWrapper b = new SqlExecutionCircuitBreakerWrapper(engine, config)) {
                owner.setTimeout(10);
                owner.resetTimer();
                owner.setCancelledFlag(new AtomicBoolean());
                a.init(owner);
                b.init(owner);
                Assert.assertNotSame(owner, a.getDelegate());
                Assert.assertNotSame(a.getDelegate(), b.getDelegate());
                a.statefulThrowExceptionIfTripped();
                b.statefulThrowExceptionIfTripped();
                clock.set(1011);
                for (SqlExecutionCircuitBreakerWrapper wrapper : new SqlExecutionCircuitBreakerWrapper[]{a, b}) {
                    int calls = 0;
                    try {
                        while (calls++ < 8) {
                            wrapper.statefulThrowExceptionIfTripped();
                        }
                        Assert.fail("timeout must fire within eight row checks");
                    } catch (CairoException ex) {
                        Assert.assertEquals(io.questdb.cairo.sql.SqlExecutionCircuitBreaker.STATE_TIMEOUT, ex.getInterruptionReason());
                    }
                }
                owner.resetTimer();
                a.init(owner);
                b.init(owner);
                owner.cancel();
                Assert.assertTrue(a.checkIfTripped());
                Assert.assertTrue(b.checkIfTripped());
                owner.setCancelledFlag(new AtomicBoolean());
                owner.resetTimer();
                a.init(owner);
                a.statefulThrowExceptionIfTrippedTimeThrottled();
                Assert.assertFalse(a.checkIfTripped());
                Assert.assertTrue(b.checkIfTripped());
            }
        });
    }
}
