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

package io.questdb.test.std;

import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.FiberLocal;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

public class FiberLocalTest {
    private static final long DEFAULT_TIMEOUT_S = 30;
    private static final FiberLocal<TestValue> LOCAL = new FiberLocal<>(TestValue::new);

    @Before
    public void setUp() {
        LOCAL.removeAndFree();
    }

    @Test
    public void testNestedEnterRestoresOuterSlots() throws Exception {
        runOnCarrier(id -> {
            final TestValue carrierValue = LOCAL.get();
            final ObjList<Object> outerSlots = new ObjList<>();
            final ObjList<Object> innerSlots = new ObjList<>();

            final ObjList<Object> beforeOuter = FiberLocal.enter(outerSlots);
            final TestValue outerValue = LOCAL.get();
            Assert.assertNotSame(carrierValue, outerValue);
            Assert.assertSame(outerValue, LOCAL.get());

            final ObjList<Object> beforeInner = FiberLocal.enter(innerSlots);
            Assert.assertSame(outerSlots, beforeInner);
            final TestValue innerValue = LOCAL.get();
            Assert.assertNotSame(outerValue, innerValue);

            FiberLocal.exit(beforeInner);
            Assert.assertSame(outerValue, LOCAL.get());
            FiberLocal.exit(beforeOuter);
            Assert.assertSame(carrierValue, LOCAL.get());
        });
    }

    @Test
    public void testRemoveAndFreeFreesOnlyTheMountedSlot() throws Exception {
        runOnCarrier(id -> {
            final TestValue carrierValue = LOCAL.get();
            final ObjList<Object> previous = FiberLocal.enter(new ObjList<>());
            final TestValue fiberValue = LOCAL.get();

            LOCAL.removeAndFree();
            Assert.assertTrue(fiberValue.isClosed);
            Assert.assertFalse(carrierValue.isClosed);
            final TestValue replacement = LOCAL.get();
            Assert.assertNotSame(fiberValue, replacement);
            Assert.assertFalse(replacement.isClosed);

            FiberLocal.exit(previous);
            Assert.assertSame(carrierValue, LOCAL.get());
            LOCAL.removeAndFree();
            Assert.assertTrue(carrierValue.isClosed);
            Assert.assertFalse(replacement.isClosed);
        });
    }

    @Test
    public void testUnbindReleasesCarrierSlots() throws Exception {
        final AtomicInteger boundId = new AtomicInteger(CarrierIdentity.UNBOUND);
        final AtomicReference<TestValue> released = new AtomicReference<>();
        runOnCarrier(id -> {
            Assert.assertTrue(FiberLocal.isCarrierRegisteredForTesting(id));
            boundId.set(id);
            released.set(LOCAL.get());
        });
        Assert.assertFalse(FiberLocal.isCarrierRegisteredForTesting(boundId.get()));
        runOnCarrier(id -> Assert.assertNotSame(released.get(), LOCAL.get()));
    }

    @Test
    public void testUnboundThreadKeepsItsOwnSlots() throws Exception {
        final AtomicReference<TestValue> unboundValue = new AtomicReference<>();
        runUnbound(id -> {
            Assert.assertEquals(CarrierIdentity.UNBOUND, id);
            final TestValue value = LOCAL.get();
            Assert.assertSame(value, LOCAL.get());
            unboundValue.set(value);

            final ObjList<Object> previous = FiberLocal.enter(new ObjList<>());
            Assert.assertNotSame(value, LOCAL.get());
            FiberLocal.exit(previous);
            Assert.assertSame(value, LOCAL.get());
        });
        runUnbound(id -> Assert.assertNotSame(unboundValue.get(), LOCAL.get()));
        runOnCarrier(id -> Assert.assertNotSame(unboundValue.get(), LOCAL.get()));
    }

    @Test
    public void testValueTravelsWithFiberAcrossCarriers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 1);
            final AtomicReference<TestValue> firstMountValue = new AtomicReference<>();
            final AtomicReference<TestValue> secondMountValue = new AtomicReference<>();
            final AtomicReference<TestValue> firstCarrierValue = new AtomicReference<>();
            final AtomicReference<TestValue> secondCarrierValue = new AtomicReference<>();
            final FiberTask task = new FiberTask() {
                @Override
                protected boolean runStep() {
                    firstMountValue.set(LOCAL.get());
                    Assert.assertTrue(Fiber.yieldCooperatively());
                    secondMountValue.set(LOCAL.get());
                    return true;
                }
            };

            runOnCarrier(id -> {
                final TestValue beforeMount = LOCAL.get();
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertSame(beforeMount, LOCAL.get());
                firstCarrierValue.set(beforeMount);
            });
            Assert.assertNotNull(firstMountValue.get());
            Assert.assertNull(secondMountValue.get());
            Assert.assertNotSame(firstCarrierValue.get(), firstMountValue.get());

            runOnCarrier(id -> {
                Assert.assertEquals(1, runtime.drain(1));
                secondCarrierValue.set(LOCAL.get());
            });
            Assert.assertTrue(task.isDone());
            Assert.assertSame(firstMountValue.get(), secondMountValue.get());
            Assert.assertNotSame(secondCarrierValue.get(), firstMountValue.get());
            Assert.assertNotSame(firstCarrierValue.get(), secondCarrierValue.get());
            Assert.assertFalse(firstMountValue.get().isClosed);

            close(runtime);
            Assert.assertTrue(firstMountValue.get().isClosed);
        });
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEFAULT_TIMEOUT_S);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void runOnCarrier(IntConsumer body) throws Exception {
        runOnThread(true, body);
    }

    private static void runOnThread(boolean isBound, IntConsumer body) throws Exception {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);
        final Thread thread = new Thread(() -> {
            try {
                final int id = isBound ? CarrierIdentity.bind() : CarrierIdentity.current();
                try {
                    body.accept(id);
                } finally {
                    if (isBound) {
                        CarrierIdentity.unbind();
                    }
                }
            } catch (Throwable th) {
                error.set(th);
            } finally {
                done.countDown();
            }
        }, "fiber-local-test");
        thread.start();
        Assert.assertTrue("test thread did not finish in time", done.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
    }

    private static void runUnbound(IntConsumer body) throws Exception {
        runOnThread(false, body);
    }

    private static final class TestValue implements QuietCloseable {
        private boolean isClosed;

        @Override
        public void close() {
            isClosed = true;
        }
    }
}
