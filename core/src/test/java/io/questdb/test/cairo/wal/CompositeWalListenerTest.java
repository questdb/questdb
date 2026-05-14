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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.CompositeWalListener;
import io.questdb.cairo.wal.WalListener;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CompositeWalListenerTest extends AbstractCairoTest {

    /**
     * Belt-and-braces cleanup of the static {@link AbstractCairoTest#engine}'s
     * legacy-shim listener slot. The {@code testLegacySetWalListener*} tests
     * use the deprecated {@code setWalListener} API which installs a shim
     * wrapper; the in-test finally blocks call {@code clearWalListenerLegacyShim}
     * but any assertion failure before that line would leak the shim to a
     * sibling test sharing the same static engine. Running it in {@code @After}
     * makes the cleanup unconditional.
     */
    @After
    public void clearLegacyShimAfter() {
        if (engine != null) {
            engine.clearWalListenerLegacyShim();
        }
    }

    @Test
    public void testAddIsIdempotentByReference() {
        CompositeWalListener composite = new CompositeWalListener();
        CountingWalListener listener = new CountingWalListener();
        composite.add(listener);
        composite.add(listener);
        composite.add(listener);
        Assert.assertEquals(1, composite.size());
        composite.dataTxnCommitted(null, 1, 0, 1, 0, 0);
        Assert.assertEquals("idempotent add must not double-fire", 1, listener.dataTxn.get());
    }

    @Test
    public void testConcurrentAddRemoveDuringFanOut() throws InterruptedException {
        // Snapshot-publish design must let mutators add and remove while
        // the commit thread iterates without producing torn reads, NPEs,
        // or duplicate / missed deliveries to a single listener instance.
        CompositeWalListener composite = new CompositeWalListener();
        CountingWalListener stable = new CountingWalListener();
        composite.add(stable);

        final int notifyCount = 10_000;
        final int churnCount = 5_000;
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        Thread notifier = new Thread(() -> {
            try {
                start.await();
                for (int i = 0; i < notifyCount; i++) {
                    composite.dataTxnCommitted(null, i, 0, 1, 0, 0);
                }
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            } finally {
                done.countDown();
            }
        }, "composite-notifier");

        Thread churner = new Thread(() -> {
            try {
                start.await();
                for (int i = 0; i < churnCount; i++) {
                    CountingWalListener transient_ = new CountingWalListener();
                    composite.add(transient_);
                    composite.remove(transient_);
                }
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            } finally {
                done.countDown();
            }
        }, "composite-churner");

        notifier.start();
        churner.start();
        start.countDown();

        Assert.assertTrue("threads did not finish", done.await(20, TimeUnit.SECONDS));
        Assert.assertNull("composite raised under concurrent mutation",
                error.get() == null ? null : error.get().toString());
        // The stable listener was registered for the full run, so it must
        // have observed every commit.
        Assert.assertEquals(notifyCount, stable.dataTxn.get());
        Assert.assertEquals(1, composite.size());
    }

    @Test
    public void testAddDuringFanOutDoesNotObservePriorCommits() throws InterruptedException {
        // Snapshot-publish contract: a listener added at time T must NOT
        // observe commits whose dispatch began BEFORE T was visible. The
        // commit thread reads {@code listeners} once per call into a stable
        // snapshot; a later add cannot retroactively insert itself into a
        // snapshot that has already been iterated. Pinning this contract
        // explicitly so a future refactor (e.g. swapping the COW snapshot
        // for a re-read-per-listener loop) cannot quietly regress it.
        CompositeWalListener composite = new CompositeWalListener();
        CountingWalListener stable = new CountingWalListener();
        composite.add(stable);

        final int preAddCommits = 1000;
        final CountDownLatch addReady = new CountDownLatch(1);
        final CountDownLatch addDone = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<CountingWalListener> lateRef = new AtomicReference<>();

        Thread notifier = new Thread(() -> {
            try {
                // Fire all pre-add commits BEFORE signalling the adder.
                for (int i = 0; i < preAddCommits; i++) {
                    composite.dataTxnCommitted(null, i, 0, 1, 0, 0);
                }
                addReady.countDown();
                addDone.await();
                // After the late listener is in, fire a marker commit so we
                // can assert that the late listener observes only this one.
                composite.dataTxnCommitted(null, Long.MAX_VALUE, 0, 1, 0, 0);
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            }
        }, "composite-add-during-fanout-notifier");

        Thread adder = new Thread(() -> {
            try {
                addReady.await();
                CountingWalListener late = new CountingWalListener();
                composite.add(late);
                lateRef.set(late);
                addDone.countDown();
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            }
        }, "composite-late-adder");

        notifier.start();
        adder.start();
        notifier.join(20_000);
        adder.join(20_000);

        Assert.assertNull("error during snapshot test", error.get());
        Assert.assertEquals("stable listener observes every commit including the marker",
                preAddCommits + 1, stable.dataTxn.get());
        CountingWalListener late = lateRef.get();
        Assert.assertNotNull("late listener was registered", late);
        Assert.assertEquals("late listener observes only the post-add marker commit",
                1, late.dataTxn.get());
    }

    @Test
    public void testFanOutDeliversToAllListenersInRegistrationOrder() {
        CompositeWalListener composite = new CompositeWalListener();
        CountingWalListener a = new CountingWalListener();
        CountingWalListener b = new CountingWalListener();
        CountingWalListener c = new CountingWalListener();
        composite.add(a);
        composite.add(b);
        composite.add(c);

        composite.dataTxnCommitted(null, 1, 0, 1, 0, 0);
        composite.nonDataTxnCommitted(null, 2, 0);
        composite.tableCreated(null, 0);
        composite.tableDropped(null, 3, 0);
        composite.segmentClosed(null, 4, 1, 0);
        composite.walClosed(null, 5, 1);

        for (CountingWalListener listener : new CountingWalListener[]{a, b, c}) {
            Assert.assertEquals(1, listener.dataTxn.get());
            Assert.assertEquals(1, listener.nonDataTxn.get());
            Assert.assertEquals(1, listener.tableCreated.get());
            Assert.assertEquals(1, listener.tableDropped.get());
            Assert.assertEquals(1, listener.segmentClosed.get());
            Assert.assertEquals(1, listener.walClosed.get());
        }
    }

    @Test
    public void testLegacySetWalListenerReplaceSemantics() throws Exception {
        // Backward-compat shim for the deprecated setWalListener API: each
        // call must replace any previously installed shim listener so
        // callers that relied on the single-listener model continue to
        // observe single-delivery semantics.
        assertMemoryLeak(() -> {
            CairoEngine eng = engine;
            CountingWalListener first = new CountingWalListener();
            CountingWalListener second = new CountingWalListener();
            CountingWalListener parallel = new CountingWalListener();

            eng.addWalListener(parallel);
            //noinspection deprecation
            eng.setWalListener(first);

            // simulate one event delivered - both listeners must fire.
            eng.getWalListener().dataTxnCommitted(null, 1, 0, 1, 0, 0);
            Assert.assertEquals(1, first.dataTxn.get());
            Assert.assertEquals(0, second.dataTxn.get());
            Assert.assertEquals(1, parallel.dataTxn.get());

            //noinspection deprecation
            eng.setWalListener(second);

            eng.getWalListener().dataTxnCommitted(null, 2, 0, 1, 0, 0);
            // first must be evicted by the second setWalListener call;
            // second now receives; parallel (registered via addWalListener)
            // is independent and continues to receive.
            Assert.assertEquals("first must not see events after replacement",
                    1, first.dataTxn.get());
            Assert.assertEquals(1, second.dataTxn.get());
            Assert.assertEquals(2, parallel.dataTxn.get());

            eng.removeWalListener(parallel);
            // Drop the shim wrapper so the next @Test in this class
            // (which shares AbstractCairoTest's static engine) starts
            // with a clean composite.
            eng.clearWalListenerLegacyShim();
        });
    }

    @Test
    public void testLegacySetWalListenerWrapperReferenceDistinct() throws Exception {
        // The shim wraps the supplied listener so its registration in the
        // composite is reference-distinct from any registration the caller
        // made via addWalListener. Without this distinction, the sequence
        // addWalListener(X) -> setWalListener(Y) -> setWalListener(X) ->
        // setWalListener(Z) would silently revoke the user's explicit
        // addWalListener(X) on the last call (composite.remove(X) would
        // match the user's add). With the wrapper, set/remove operate
        // strictly on the wrapper instances.
        assertMemoryLeak(() -> {
            CairoEngine eng = engine;
            CountingWalListener x = new CountingWalListener();
            CountingWalListener y = new CountingWalListener();
            CountingWalListener z = new CountingWalListener();

            eng.addWalListener(x);
            //noinspection deprecation
            eng.setWalListener(y);
            //noinspection deprecation
            eng.setWalListener(x);
            //noinspection deprecation
            eng.setWalListener(z);

            eng.getWalListener().dataTxnCommitted(null, 1, 0, 1, 0, 0);
            // x must still receive: the user's addWalListener(x) was never
            // touched. z is the current shim listener. y was evicted.
            Assert.assertEquals("addWalListener(x) must survive subsequent setWalListener calls",
                    1, x.dataTxn.get());
            Assert.assertEquals(0, y.dataTxn.get());
            Assert.assertEquals(1, z.dataTxn.get());

            eng.removeWalListener(x);
            // Drop the shim wrapper so the next @Test in this class
            // (which shares AbstractCairoTest's static engine) starts
            // with a clean composite.
            eng.clearWalListenerLegacyShim();
        });
    }

    @Test
    public void testRemoveOfUnregisteredIsNoOp() {
        CompositeWalListener composite = new CompositeWalListener();
        CountingWalListener registered = new CountingWalListener();
        CountingWalListener never = new CountingWalListener();
        composite.add(registered);

        composite.remove(never);
        Assert.assertEquals("unregistered remove must not affect the snapshot",
                1, composite.size());

        composite.dataTxnCommitted(null, 1, 0, 1, 0, 0);
        Assert.assertEquals(1, registered.dataTxn.get());
        Assert.assertEquals(0, never.dataTxn.get());
    }

    private static class CountingWalListener implements WalListener {
        final AtomicInteger dataTxn = new AtomicInteger();
        final AtomicInteger nonDataTxn = new AtomicInteger();
        final AtomicInteger segmentClosed = new AtomicInteger();
        final AtomicInteger tableCreated = new AtomicInteger();
        final AtomicInteger tableDropped = new AtomicInteger();
        final AtomicInteger tableRenamed = new AtomicInteger();
        final AtomicInteger walClosed = new AtomicInteger();

        @Override
        public void dataTxnCommitted(TableToken tableToken, long txn, long timestamp,
                                     int walId, int segmentId, int segmentTxn) {
            dataTxn.incrementAndGet();
        }

        @Override
        public void nonDataTxnCommitted(TableToken tableToken, long txn, long timestamp) {
            nonDataTxn.incrementAndGet();
        }

        @Override
        public void segmentClosed(TableToken tableToken, long txn, int walId, int segmentId) {
            segmentClosed.incrementAndGet();
        }

        @Override
        public void tableCreated(TableToken tableToken, long timestamp) {
            tableCreated.incrementAndGet();
        }

        @Override
        public void tableDropped(TableToken tableToken, long txn, long timestamp) {
            tableDropped.incrementAndGet();
        }

        @Override
        public void tableRenamed(TableToken tableToken, long txn, long timestamp, TableToken oldTableToken) {
            tableRenamed.incrementAndGet();
        }

        @Override
        public void walClosed(TableToken tableToken, long txn, int walId) {
            walClosed.incrementAndGet();
        }
    }
}
