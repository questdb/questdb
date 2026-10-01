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

package io.questdb.test.griffin.engine.functions.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.test.TestWorkerCloneFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestWorkerCloneFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConcurrentRunsRemainIsolated() throws Exception {
        assertMemoryLeak(() -> {
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicReference<Function> firstClone = new AtomicReference<>();
            final AtomicReference<Function> secondClone = new AtomicReference<>();
            final Thread first = new Thread(() -> runOverlappingOwner(10, firstClone, secondClone, barrier, failure));
            final Thread second = new Thread(() -> runOverlappingOwner(20, secondClone, firstClone, barrier, failure));
            first.start();
            second.start();
            first.join(10_000);
            second.join(10_000);
            Assert.assertFalse("first run owner hung", first.isAlive());
            Assert.assertFalse("second run owner hung", second.isAlive());
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
        });
    }

    @Test
    public void testDevCompilationRequiresArm() throws Exception {
        assertMemoryLeak(() -> {
            try {
                newProbe(BooleanConstant.TRUE, DoubleConstant.ONE, configuration);
                Assert.fail("unarmed dev compilation must fail");
            } catch (Throwable e) {
                Assert.assertTrue(e.getMessage().contains("must be armed"));
            }
        });
    }

    @Test
    public void testMoreThan32InstancesHaveIndependentCounters() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> functions = new ObjList<>();
            TestWorkerCloneFunctionFactory.arm(0);
            try {
                for (int i = 0; i < 40; i++) {
                    functions.add(newProbe(BooleanConstant.TRUE, DoubleConstant.ONE, configuration));
                }
                Assert.assertEquals(40, TestWorkerCloneFunctionFactory.created());
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final Thread worker = new Thread(() -> {
                    try {
                        for (int i = 0; i < functions.size(); i++) {
                            functions.getQuick(i).getBool(null);
                        }
                    } catch (Throwable th) {
                        failure.set(th);
                    }
                });
                worker.start();
                worker.join(10_000);
                Assert.assertFalse("counter worker hung", worker.isAlive());
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                for (int i = 0; i < functions.size(); i++) {
                    Assert.assertEquals("instance " + i, 1, TestWorkerCloneFunctionFactory.evaluations(i));
                }
                Assert.assertEquals(39, TestWorkerCloneFunctionFactory.workerEvaluations());
                Assert.assertEquals(0, TestWorkerCloneFunctionFactory.mismatches());
            } finally {
                TestWorkerCloneFunctionFactory.disarm();
                Misc.freeObjList(functions);
            }
        });
    }

    @Test
    public void testNestedArmIsRejectedWithoutReplacingRun() throws Exception {
        assertMemoryLeak(() -> {
            TestWorkerCloneFunctionFactory.arm(1);
            try {
                try {
                    TestWorkerCloneFunctionFactory.arm(2);
                    Assert.fail("nested arm must fail");
                } catch (IllegalStateException e) {
                    Assert.assertTrue(e.getMessage().contains("already armed"));
                }
                Assert.assertEquals(0, TestWorkerCloneFunctionFactory.created());
                newProbe(BooleanConstant.TRUE, new DoubleConstant(2), configuration);
                Assert.assertEquals(1, TestWorkerCloneFunctionFactory.created());
            } finally {
                TestWorkerCloneFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testNonDevFoldClosesDiscardedArgumentExactlyOnce() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingDoubleFunction discarded = new TrackingDoubleFunction(null);
            final ObjList<Function> args = args(BooleanConstant.TRUE, discarded);
            final Function result = new TestWorkerCloneFunctionFactory().newInstance(0, args, new IntList(), nonDevConfiguration(), null);
            Assert.assertSame(BooleanConstant.TRUE, result);
            Assert.assertNull(args.getQuick(1));
            Assert.assertEquals(1, discarded.closeCount);
            Misc.freeObjList(args);
            Assert.assertEquals(1, discarded.closeCount);
        });
    }

    @Test
    public void testNonDevFoldPreservesThrowingCloseAsPrimary() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException closeFailure = new RuntimeException("discarded close failure");
            final TrackingDoubleFunction discarded = new TrackingDoubleFunction(closeFailure);
            final ObjList<Function> args = args(BooleanConstant.TRUE, discarded);
            try {
                new TestWorkerCloneFunctionFactory().newInstance(0, args, new IntList(), nonDevConfiguration(), null);
                Assert.fail("discarded close must fail");
            } catch (RuntimeException e) {
                Assert.assertSame(closeFailure, e);
            }
            Assert.assertNull(args.getQuick(1));
            Assert.assertEquals(1, discarded.closeCount);
            Misc.freeObjList(args);
            Assert.assertEquals(1, discarded.closeCount);
        });
    }

    @Test
    public void testThrowingWorkerComparisonAndValueReleaseOwner() throws Exception {
        assertMemoryLeak(() -> {
            assertThrowingWorkerReleasesOwner(true);
            assertThrowingWorkerReleasesOwner(false);
        });
    }

    private static ObjList<Function> args(Function comparison, Function value) {
        final ObjList<Function> args = new ObjList<>();
        args.add(comparison);
        args.add(value);
        return args;
    }

    private static void assertThrowingWorkerReleasesOwner(boolean isComparisonThrowing) throws Exception {
        final CountDownLatch ownerEntered = new CountDownLatch(1);
        final RuntimeException workerFailure = new RuntimeException("worker evaluation failure");
        final Function ownerComparison = new SignallingBooleanFunction(ownerEntered);
        final Function workerComparison = isComparisonThrowing
                ? new AwaitingBooleanFunction(ownerEntered, workerFailure)
                : BooleanConstant.TRUE;
        final Function workerValue = isComparisonThrowing
                ? DoubleConstant.ONE
                : new AwaitingDoubleFunction(ownerEntered, workerFailure);
        final AtomicReference<Throwable> observedWorkerFailure = new AtomicReference<>();

        TestWorkerCloneFunctionFactory.arm(0);
        try {
            final Function owner = newProbe(ownerComparison, DoubleConstant.ONE, configuration);
            final Function worker = newProbe(workerComparison, workerValue, configuration);
            final Thread workerThread = new Thread(() -> {
                try {
                    worker.getBool(null);
                } catch (Throwable th) {
                    observedWorkerFailure.set(th);
                }
            });
            workerThread.start();
            Assert.assertTrue(owner.getBool(null));
            workerThread.join(2_000);
            Assert.assertFalse("throwing worker did not finish", workerThread.isAlive());
            Assert.assertSame(workerFailure, observedWorkerFailure.get());
            Assert.assertEquals(1, TestWorkerCloneFunctionFactory.workerEvaluations());
        } finally {
            TestWorkerCloneFunctionFactory.disarm();
        }
    }

    private static CairoConfiguration nonDevConfiguration() {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public boolean isDevModeEnabled() {
                return false;
            }
        };
    }

    private static Function newProbe(Function comparison, Function value, CairoConfiguration configuration) throws Exception {
        final CairoConfiguration devConfiguration = new CairoConfigurationWrapper(configuration) {
            @Override
            public boolean isDevModeEnabled() {
                return true;
            }
        };
        return new TestWorkerCloneFunctionFactory().newInstance(0, args(comparison, value), new IntList(), devConfiguration, null);
    }

    private static void runOverlappingOwner(
            double threshold,
            AtomicReference<Function> ownClone,
            AtomicReference<Function> peerClone,
            CyclicBarrier barrier,
            AtomicReference<Throwable> failure
    ) {
        TestWorkerCloneFunctionFactory.arm(threshold);
        try {
            newProbe(BooleanConstant.TRUE, new DoubleConstant(threshold + 1), configuration);
            ownClone.set(newProbe(BooleanConstant.TRUE, new DoubleConstant(threshold + 1), configuration));
            barrier.await(10, TimeUnit.SECONDS);
            peerClone.get().getBool(null);
            barrier.await(10, TimeUnit.SECONDS);
            Assert.assertEquals(2, TestWorkerCloneFunctionFactory.created());
            Assert.assertEquals(0, TestWorkerCloneFunctionFactory.evaluations(0));
            Assert.assertEquals(1, TestWorkerCloneFunctionFactory.evaluations(1));
            Assert.assertEquals(1, TestWorkerCloneFunctionFactory.workerEvaluations());
            Assert.assertEquals(0, TestWorkerCloneFunctionFactory.mismatches());
        } catch (Throwable th) {
            failure.compareAndSet(null, th);
        } finally {
            TestWorkerCloneFunctionFactory.disarm();
        }
    }

    private static class AwaitingBooleanFunction extends BooleanFunction {
        private final CountDownLatch ownerEntered;
        private final RuntimeException workerFailure;

        private AwaitingBooleanFunction(CountDownLatch ownerEntered, RuntimeException workerFailure) {
            this.ownerEntered = ownerEntered;
            this.workerFailure = workerFailure;
        }

        @Override
        public boolean getBool(Record rec) {
            await(ownerEntered);
            throw workerFailure;
        }
    }

    private static class AwaitingDoubleFunction extends DoubleFunction {
        private final CountDownLatch ownerEntered;
        private final RuntimeException workerFailure;

        private AwaitingDoubleFunction(CountDownLatch ownerEntered, RuntimeException workerFailure) {
            this.ownerEntered = ownerEntered;
            this.workerFailure = workerFailure;
        }

        @Override
        public double getDouble(Record rec) {
            await(ownerEntered);
            throw workerFailure;
        }
    }

    private static class SignallingBooleanFunction extends BooleanFunction {
        private final CountDownLatch ownerEntered;

        private SignallingBooleanFunction(CountDownLatch ownerEntered) {
            this.ownerEntered = ownerEntered;
        }

        @Override
        public boolean getBool(Record rec) {
            ownerEntered.countDown();
            return true;
        }
    }

    private static class TrackingDoubleFunction extends DoubleFunction {
        private final RuntimeException closeFailure;
        private int closeCount;

        private TrackingDoubleFunction(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return 1;
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(2, TimeUnit.SECONDS)) {
                throw new AssertionError("owner did not enter evaluation");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        }
    }
}
