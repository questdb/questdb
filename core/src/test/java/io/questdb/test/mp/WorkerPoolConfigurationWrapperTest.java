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

package io.questdb.test.mp;

import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolConfigurationWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerPoolConfigurationWrapperTest {

    @Test
    public void testConcurrentListenerRegistrationDeliversNewestDelegateLast() throws Exception {
        final WorkerPoolConfigurationWrapper wrapper = new WorkerPoolConfigurationWrapper();
        final CountDownLatch currentDelegateRead = new CountDownLatch(1);
        final CountDownLatch releaseCurrentDelegate = new CountDownLatch(1);
        wrapper.setDelegate(new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                currentDelegateRead.countDown();
                await(releaseCurrentDelegate);
                return 3;
            }

            @Override
            public int getFiberMountBudget() {
                return 1;
            }

            @Override
            public int getFiberRetainedCount() {
                return 2;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }
        });

        final AtomicReference<Throwable> asyncError = new AtomicReference<>();
        final AtomicInteger callbackCount = new AtomicInteger();
        final AtomicInteger lastMaxLiveCount = new AtomicInteger();
        final CountDownLatch registrationReturned = new CountDownLatch(1);
        final Thread register = new Thread(() -> {
            try {
                wrapper.setFiberConfigurationListener((maxLiveCount, retainedCount, mountBudget) -> {
                    // Call 1 carries the delegate the registration had already read when
                    // setDelegate landed; call 2 must carry the newer one, which is the
                    // property under test.
                    final int call = callbackCount.incrementAndGet();
                    if (call == 1) {
                        Assert.assertEquals(3, maxLiveCount);
                        Assert.assertEquals(2, retainedCount);
                        Assert.assertEquals(1, mountBudget);
                    } else if (call == 2) {
                        Assert.assertEquals(6, maxLiveCount);
                        Assert.assertEquals(5, retainedCount);
                        Assert.assertEquals(4, mountBudget);
                    } else {
                        Assert.fail("unexpected callback [call=" + call + ']');
                    }
                    lastMaxLiveCount.set(maxLiveCount);
                });
            } catch (Throwable th) {
                asyncError.compareAndSet(null, th);
            } finally {
                registrationReturned.countDown();
            }
        });
        register.start();
        try {
            Assert.assertTrue(currentDelegateRead.await(10, TimeUnit.SECONDS));
            wrapper.setDelegate(configuration(6, 5, 4));
        } finally {
            releaseCurrentDelegate.countDown();
            register.join(10_000);
            Assert.assertFalse(register.isAlive());
        }
        Assert.assertTrue(registrationReturned.await(10, TimeUnit.SECONDS));
        Assert.assertEquals(2, callbackCount.get());
        Assert.assertEquals(6, lastMaxLiveCount.get());
        if (asyncError.get() != null) {
            throw new AssertionError(asyncError.get());
        }
    }

    @Test
    public void testListenerDeliveryIsCurrentReentrantAndOutsideWrapperLock() throws Exception {
        final WorkerPoolConfigurationWrapper wrapper = new WorkerPoolConfigurationWrapper();
        wrapper.setDelegate(configuration(3, 2, 3));

        final AtomicReference<Throwable> asyncError = new AtomicReference<>();
        final AtomicInteger callbackCount = new AtomicInteger();
        final CountDownLatch releaseUpdate = new CountDownLatch(1);
        final CountDownLatch unregisterReturned = new CountDownLatch(1);
        final CountDownLatch updateEntered = new CountDownLatch(1);
        final CountDownLatch updateReturned = new CountDownLatch(1);

        wrapper.setFiberConfigurationListener((maxLiveCount, retainedCount, mountBudget) -> {
            final int call = callbackCount.incrementAndGet();
            if (call == 1) {
                Assert.assertEquals(3, maxLiveCount);
                Assert.assertEquals(2, retainedCount);
                Assert.assertEquals(3, mountBudget);
                Assert.assertEquals(3, wrapper.getFiberMaxLiveCount());
                wrapper.setDelegate(configuration(6, 5, 6));
            } else if (call == 2) {
                Assert.assertEquals(6, maxLiveCount);
                Assert.assertEquals(5, retainedCount);
                Assert.assertEquals(6, mountBudget);
            } else if (call == 3) {
                Assert.assertEquals(9, maxLiveCount);
                Assert.assertEquals(8, retainedCount);
                Assert.assertEquals(9, mountBudget);
                updateEntered.countDown();
                await(releaseUpdate);
            } else {
                Assert.fail("unexpected callback [call=" + call + ']');
            }
        });

        final Thread updater = new Thread(() -> {
            try {
                wrapper.setDelegate(configuration(9, 8, 9));
            } catch (Throwable th) {
                asyncError.compareAndSet(null, th);
            } finally {
                updateReturned.countDown();
            }
        });
        updater.start();
        try {
            Assert.assertTrue(updateEntered.await(10, TimeUnit.SECONDS));

            final Thread unregister = new Thread(() -> {
                try {
                    wrapper.setFiberConfigurationListener(null);
                } catch (Throwable th) {
                    asyncError.compareAndSet(null, th);
                } finally {
                    unregisterReturned.countDown();
                }
            });
            unregister.start();
            try {
                Assert.assertTrue(unregisterReturned.await(10, TimeUnit.SECONDS));
                wrapper.setDelegate(configuration(12, 11, 12));
            } finally {
                unregister.join(10_000);
                Assert.assertFalse(unregister.isAlive());
            }
        } finally {
            releaseUpdate.countDown();
            updater.join(10_000);
            Assert.assertFalse(updater.isAlive());
        }
        Assert.assertTrue(updateReturned.await(10, TimeUnit.SECONDS));
        Assert.assertEquals(3, callbackCount.get());
        Assert.assertEquals(12, wrapper.getFiberMaxLiveCount());
        Assert.assertEquals(11, wrapper.getFiberRetainedCount());
        Assert.assertEquals(12, wrapper.getFiberMountBudget());
        if (asyncError.get() != null) {
            throw new AssertionError(asyncError.get());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static WorkerPoolConfiguration configuration(
            int maxLiveCount,
            int retainedCount,
            int mountBudget
    ) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return maxLiveCount;
            }

            @Override
            public int getFiberMountBudget() {
                return mountBudget;
            }

            @Override
            public int getFiberRetainedCount() {
                return retainedCount;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }
        };
    }
}
