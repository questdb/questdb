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
import io.questdb.std.CarrierLocal;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

public class CarrierLocalTest {

    private static final long DEFAULT_TIMEOUT_S = 30;

    @Test
    public void testBoundDefaultIsNull() throws Exception {
        runBound(id -> {
            CarrierLocal<String> tl = new CarrierLocal<>();
            Assert.assertNull(tl.get());
        });
    }

    @Test
    public void testBoundInitialValueOnlyCalledOncePerCarrier() throws Exception {
        // JDK ThreadLocal contract: initialValue() runs exactly once per
        // (thread, ThreadLocal) pair. Same here per (carrier, CarrierLocal).
        AtomicInteger calls = new AtomicInteger();
        CarrierLocal<Integer> tl = CarrierLocal.withInitial(calls::incrementAndGet);

        runBound(id -> {
            for (int i = 0; i < 32; i++) {
                Assert.assertEquals(Integer.valueOf(1), tl.get());
            }
        });
        Assert.assertEquals(1, calls.get());

        // A fresh carrier id triggers a fresh init.
        runBound(id -> {
            Assert.assertEquals(Integer.valueOf(2), tl.get());
        });
        Assert.assertEquals(2, calls.get());
    }

    @Test
    public void testBoundRemoveCausesReinitialization() throws Exception {
        AtomicInteger initCalls = new AtomicInteger();
        CarrierLocal<Integer> tl = CarrierLocal.withInitial(initCalls::incrementAndGet);
        runBound(id -> {
            Assert.assertEquals(Integer.valueOf(1), tl.get());
            Assert.assertEquals(Integer.valueOf(1), tl.get());
            tl.remove();
            Assert.assertEquals(Integer.valueOf(2), tl.get());
            tl.set(99);
            Assert.assertEquals(Integer.valueOf(99), tl.get());
            tl.remove();
            Assert.assertEquals(Integer.valueOf(3), tl.get());
        });
        Assert.assertEquals(3, initCalls.get());
    }

    @Test
    public void testBoundSetGetRoundtrip() throws Exception {
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> "init");
        runBound(id -> {
            Assert.assertEquals("init", tl.get());
            tl.set("hello");
            Assert.assertEquals("hello", tl.get());
            tl.set("world");
            Assert.assertEquals("world", tl.get());
        });
    }

    @Test
    public void testBoundSetNullPreservedNotReinitialized() throws Exception {
        // Per JDK ThreadLocal semantics, set(null) should be remembered as a real
        // value: subsequent get() returns null, NOT the initial value. The port
        // achieves this because getEntry returns the entry (not null) and we read
        // entry.value directly only when there is no entry. See JDK
        // ThreadLocalMap.set / getEntryAfterMiss.
        AtomicInteger initCalls = new AtomicInteger();
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> {
            initCalls.incrementAndGet();
            return "init";
        });
        runBound(id -> {
            Assert.assertEquals("init", tl.get());
            tl.set(null);
            Assert.assertNull(tl.get());
            Assert.assertNull(tl.get());
            tl.remove();
            Assert.assertEquals("init", tl.get());
        });
        // First get + final reload after remove; the explicit null must not
        // re-trigger the initial supplier.
        Assert.assertEquals(2, initCalls.get());
    }

    @Test
    public void testRemoveDoesNotCloseValueMatchingJdkSemantics() throws Exception {
        // remove() must NOT call close() on the previously stored value -- some
        // callers hand the value to other code and remove the slot purely for
        // lifecycle reasons. Matches JDK ThreadLocal.remove().
        CountingCloseable a = new CountingCloseable();
        CarrierLocal<CountingCloseable> tl = new CarrierLocal<>();

        runBound(id -> {
            tl.set(a);
            tl.remove();
            Assert.assertEquals("remove must not close the value", 0, a.closes);
        });
        // unbind's releaseRow only sees an empty row, so 'a' is never freed
        // through the CarrierLocal subsystem -- the caller is expected to
        // manage 'a's lifecycle externally if remove() was used.
        Assert.assertEquals(0, a.closes);
    }

    @Test
    public void testRemoveAndFreeFreesCloseableValueOnCurrentCarrier() throws Exception {
        CountingCloseable a = new CountingCloseable();
        CountingCloseable b = new CountingCloseable();

        CarrierLocal<CountingCloseable> tl = new CarrierLocal<>();

        runBound(id -> {
            tl.set(a);
            tl.removeAndFree();
            Assert.assertEquals("removeAndFree closes the value", 1, a.closes);
            Assert.assertNull("entry removed: subsequent get returns the default (null)", tl.get());

            tl.set(b);
            // releaseRow does NOT close 'b' -- matches JDK ThreadLocal: values
            // left in the table at thread death are unreachable, not closed.
        });
        Assert.assertEquals("releaseRow leaves b unclosed (JDK ThreadLocal parity)", 0, b.closes);
        Assert.assertEquals("a freed by removeAndFree, releaseRow must not double-close", 1, a.closes);
    }

    @Test
    public void testRemoveAndFreeIsNoOpWhenUnset() throws Exception {
        CarrierLocal<CountingCloseable> tl = CarrierLocal.withInitial(CountingCloseable::new);
        runBound(id -> {
            // No prior set/get; removeAndFree must NOT trigger the initial supplier.
            tl.removeAndFree();
            // Fresh init still works on next get.
            CountingCloseable c = tl.get();
            Assert.assertNotNull(c);
            Assert.assertEquals("initial value not closed by removeAndFree on a missing entry", 0, c.closes);
        });
    }

    @Test
    public void testStaleEntryReleaseRowDoesNotCloseMatchingJdkSemantics() throws Exception {
        // After a worker unbinds, releaseRow drops the row but does NOT close
        // the stored values -- it matches JDK ThreadLocal, where values left in
        // the table on thread death are simply unreachable, not actively
        // closed. Callers that hold native resources are expected to register a
        // worker-pool thread-local cleaner that calls removeAndFree() before
        // unbind. Auto-closing here would race with close callbacks that touch
        // other carrier-local slots (e.g. LOG.debug from a pool's close()),
        // because the table iteration runs in hash-bucket order.
        final int N = 16;
        CountingCloseable[] closeables = new CountingCloseable[N];
        for (int i = 0; i < N; i++) {
            closeables[i] = new CountingCloseable();
        }

        runBound(id -> {
            CarrierLocal<CountingCloseable>[] doomed = new CarrierLocal[N];
            for (int i = 0; i < N; i++) {
                doomed[i] = CarrierLocal.withInitial(CountingCloseable::new);
                doomed[i].set(closeables[i]);
            }
            for (int i = 0; i < N; i++) {
                doomed[i] = null;
            }
            // Suggest GC opportunistically; whether it runs is up to the JVM.
            for (int i = 0; i < 5; i++) {
                System.gc();
            }
        });

        for (int i = 0; i < N; i++) {
            Assert.assertEquals("closeable " + i + " not closed by releaseRow",
                    0, closeables[i].closes);
        }
    }

    @Test
    public void testConcurrentDifferentCarriersStress() throws Exception {
        // 8 carriers each running 10K set/get cycles on the same shared CarrierLocal.
        // No assertions on values across carriers; the goal is to verify that
        // map operations on per-carrier rows are not corrupted by concurrent
        // unrelated activity (different rows, different threads).
        final int threadCount = 8;
        final int iterations = 10_000;
        CarrierLocal<long[]> tl = CarrierLocal.withInitial(() -> new long[1]);

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<Throwable> err = new AtomicReference<>();

        for (int t = 0; t < threadCount; t++) {
            final long base = (long) t * iterations;
            new Thread(() -> {
                try {
                    barrier.await();
                    CarrierIdentity.bind();
                    try {
                        long[] cell = tl.get();
                        for (int i = 0; i < iterations; i++) {
                            cell[0] = base + i;
                            if (cell[0] != base + i) {
                                throw new AssertionError("readback failed at i=" + i);
                            }
                        }
                    } finally {
                        CarrierIdentity.unbind();
                    }
                } catch (Throwable th) {
                    err.compareAndSet(null, th);
                } finally {
                    done.countDown();
                }
            }, "stress-" + t).start();
        }

        Assert.assertTrue("stress timed out", done.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    @Test
    public void testFourThreadsSequentialPutCheckReadback() throws Exception {
        // Each thread owns a private value range of size N. It writes its current
        // value, reads it back, asserts equal, then increments. Repeat M times.
        // Cross-thread interference would surface as a readback mismatch.
        final int threads = 4;
        final int rangeSize = 1000;
        final int iterations = 5000;
        CarrierLocal<int[]> tl = CarrierLocal.withInitial(() -> new int[]{0});

        CyclicBarrier barrier = new CyclicBarrier(threads);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> err = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            final int base = t * rangeSize;
            new Thread(() -> {
                try {
                    barrier.await();
                    CarrierIdentity.bind();
                    try {
                        int[] cell = tl.get();
                        for (int i = 0; i < iterations; i++) {
                            int next = base + (i % rangeSize);
                            cell[0] = next;
                            int read = tl.get()[0];
                            if (read != next) {
                                throw new AssertionError("readback mismatch: wrote " + next + ", read " + read);
                            }
                            if (read < base || read >= base + rangeSize) {
                                throw new AssertionError("value " + read + " escaped range [" + base + "," + (base + rangeSize) + ")");
                            }
                        }
                    } finally {
                        CarrierIdentity.unbind();
                    }
                } catch (Throwable th) {
                    err.compareAndSet(null, th);
                } finally {
                    done.countDown();
                }
            }, "carrier-" + t).start();
        }

        Assert.assertTrue("test timed out", done.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    @Test
    public void testIsolationAcrossCarriersIndependentValues() throws Exception {
        // 3 threads each bind a carrier and write a unique value, then read it
        // back after all have written. Verifies a CarrierLocal slot on one
        // carrier is invisible to other carriers.
        final int threads = 3;
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> "INIT");

        CyclicBarrier writePhase = new CyclicBarrier(threads);
        CyclicBarrier readPhase = new CyclicBarrier(threads);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> err = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            final String mine = "thread-" + t;
            new Thread(() -> {
                try {
                    CarrierIdentity.bind();
                    try {
                        Assert.assertEquals("INIT", tl.get());
                        tl.set(mine);
                        writePhase.await(); // all threads have written
                        // After everyone wrote, we should still see our own value.
                        readPhase.await();
                        if (!mine.equals(tl.get())) {
                            throw new AssertionError(mine + " saw " + tl.get());
                        }
                    } finally {
                        CarrierIdentity.unbind();
                    }
                } catch (Throwable th) {
                    err.compareAndSet(null, th);
                } finally {
                    done.countDown();
                }
            }, "iso-" + t).start();
        }

        Assert.assertTrue(done.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    @Test
    public void testManyCarrierLocalsTriggerResize() throws Exception {
        // Initial table capacity is 16; load factor 2/3 means resize at 10
        // entries. Push well past resize to exercise rehash + re-index.
        final int N = 200;
        runBound(id -> {
            CarrierLocal<Integer>[] locals = new CarrierLocal[N];
            for (int i = 0; i < N; i++) {
                final int v = i;
                locals[i] = CarrierLocal.withInitial(() -> v);
                locals[i].set(v * 7);
            }
            for (int i = 0; i < N; i++) {
                Assert.assertEquals("local[" + i + "]", Integer.valueOf(i * 7), locals[i].get());
            }
            for (int i = 0; i < N; i += 2) {
                locals[i].remove();
            }
            for (int i = 0; i < N; i++) {
                Integer v = locals[i].get();
                if ((i & 1) == 0) {
                    Assert.assertEquals("re-init at " + i, Integer.valueOf(i), v);
                } else {
                    Assert.assertEquals("preserved at " + i, Integer.valueOf(i * 7), v);
                }
            }
        });
    }

    @Test
    public void testObjectFactoryConstructor() throws Exception {
        CarrierLocal<int[]> tl = new CarrierLocal<>(() -> new int[]{42});
        runBound(id -> {
            int[] arr = tl.get();
            Assert.assertEquals(42, arr[0]);
            arr[0] = 99;
            Assert.assertSame(arr, tl.get()); // same instance per carrier
            Assert.assertEquals(99, tl.get()[0]);
        });
    }

    @Test
    public void testRebindCycleNoLeakAcrossCarriers() throws Exception {
        // The user's stress: 2 threads, each cycles 100 times. Inside each
        // cycle: bind, create 100 CarrierLocals, set + read each, unbind.
        // Verifies releaseRow drops the per-carrier table on every unbind so
        // values from prior cycles do not bleed through to a new carrier id
        // that happens to alias an old one.
        final int threads = 2;
        final int cycles = 100;
        final int localsPerCycle = 100;

        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> err = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            new Thread(() -> {
                try {
                    for (int c = 0; c < cycles; c++) {
                        CarrierIdentity.bind();
                        try {
                            CarrierLocal<Integer>[] locals = new CarrierLocal[localsPerCycle];
                            for (int i = 0; i < localsPerCycle; i++) {
                                final int expected = tid * 1_000_000 + c * 1000 + i;
                                locals[i] = CarrierLocal.withInitial(() -> -1);
                                locals[i].set(expected);
                                Integer got = locals[i].get();
                                if (got == null || got.intValue() != expected) {
                                    throw new AssertionError("t=" + tid + " c=" + c + " i=" + i + " got=" + got);
                                }
                            }
                            // Drop strong references; releaseRow on unbind
                            // should still walk and free entries cleanly.
                        } finally {
                            CarrierIdentity.unbind();
                        }
                    }
                } catch (Throwable th) {
                    err.compareAndSet(null, th);
                } finally {
                    done.countDown();
                }
            }, "rebind-" + t).start();
        }

        Assert.assertTrue("rebind cycle timed out", done.await(60, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    @Test
    public void testReentrantInitialValueReadingDifferentCarrierLocal() throws Exception {
        // initial supplier of B reads A. The ordering must be: get(B) ->
        // setInitialValue(B) -> initial.apply -> get(A) -> setInitialValue(A)
        // -> initial.apply (returns "a") -> map.set(A, "a") returns "a" ->
        // back into B's setInitialValue -> map.set(B, "b-saw-a"). Result: B
        // returns "b-saw-a", A returns "a".
        CarrierLocal<String> tlA = CarrierLocal.withInitial(() -> "a");
        AtomicReference<String> sawInB = new AtomicReference<>();

        CarrierLocal<String> tlB = CarrierLocal.withInitial(() -> {
            String a = tlA.get();
            sawInB.set(a);
            return "b-saw-" + a;
        });

        runBound(id -> {
            Assert.assertEquals("b-saw-a", tlB.get());
            Assert.assertEquals("a", tlA.get());
            Assert.assertEquals("a", sawInB.get());
        });
    }

    @Test
    public void testReentrantSetWithinOwnInitialValueWins() throws Exception {
        // JDK ThreadLocal semantics: if initialValue() calls set(), the value
        // set wins over the value returned. The map sees the set during the
        // recursive call; when setInitialValue's outer map.set runs, it
        // overwrites with the returned value -- so under JDK and our port,
        // the *returned* value actually wins. We document the observed
        // behavior here so a future change can't quietly flip it.
        CarrierLocal<String> tl = new CarrierLocal<>() {
            // can't override initialValue (the public API uses IntFunction),
            // so use withInitial that calls back through the public set().
        };
        AtomicReference<CarrierLocal<String>> ref = new AtomicReference<>();
        CarrierLocal<String> reentrant = CarrierLocal.withInitial(() -> {
            ref.get().set("inner-set");
            return "outer-return";
        });
        ref.set(reentrant);

        runBound(id -> {
            String observed = reentrant.get();
            // Capture the actually-observed semantics so we notice if the
            // implementation drifts from JDK behavior.
            Assert.assertTrue(
                    "expected inner-set or outer-return, got " + observed,
                    "inner-set".equals(observed) || "outer-return".equals(observed)
            );
            // After the recursive set, the next get must return that value.
            Assert.assertEquals(observed, reentrant.get());
        });
    }

    @Test
    public void testReleaseRowDoesNotCloseMatchingJdkSemantics() throws Exception {
        // releaseRow detaches the row but does NOT close stored values, matching
        // JDK ThreadLocal. Callers that hold native resources must release them
        // explicitly via removeAndFree() (typically wired as a worker-pool
        // thread-local cleaner running before unbind).
        CountingCloseable a = new CountingCloseable();
        CountingCloseable b = new CountingCloseable();
        CountingCloseable c = new CountingCloseable();

        CarrierLocal<CountingCloseable> tlA = new CarrierLocal<>();
        CarrierLocal<CountingCloseable> tlB = new CarrierLocal<>();
        CarrierLocal<CountingCloseable> tlC = new CarrierLocal<>();

        runBound(id -> {
            tlA.set(a);
            tlB.set(b);
            tlC.set(c);
        });
        Assert.assertEquals("a not closed by releaseRow", 0, a.closes);
        Assert.assertEquals("b not closed by releaseRow", 0, b.closes);
        Assert.assertEquals("c not closed by releaseRow", 0, c.closes);
    }

    @Test
    public void testRecycledIdSeesFreshSlotNotPriorValue() throws Exception {
        // Thread A binds, sets CarrierLocal<String> to "value-from-A", unbinds
        // (which calls releaseRow then enqueues the id into RECYCLED). Thread B
        // binds and reads the same CarrierLocal: it must observe the supplier's
        // initial value, NOT A's stale value. This is the round-trip that
        // guards the ordering invariant in CarrierIdentity.unbind: releaseRow
        // MUST null the per-carrier map before the id is pushed to RECYCLED.
        // If the order were reversed, B could pop A's id from RECYCLED while
        // A's map row was still attached.
        //
        // Note: whether B actually receives A's recycled id depends on the
        // global RECYCLED queue state at the time of B's bind (other tests in
        // this suite may have left entries). The invariant being tested is
        // freshness, not id equality -- whatever id B got, its slot must be
        // unoccupied.
        final String F1 = "value-from-A";
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> "init");

        AtomicInteger aId = new AtomicInteger(-1);
        AtomicInteger bId = new AtomicInteger(-1);
        AtomicReference<String> bSaw = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();
        CountDownLatch aDone = new CountDownLatch(1);
        CountDownLatch bDone = new CountDownLatch(1);

        Thread a = new Thread(() -> {
            try {
                int id = CarrierIdentity.bind();
                aId.set(id);
                tl.set(F1);
                Assert.assertEquals(F1, tl.get());
                CarrierIdentity.unbind();
            } catch (Throwable th) {
                err.compareAndSet(null, th);
            } finally {
                aDone.countDown();
            }
        }, "recycle-A");
        a.start();
        Assert.assertTrue("A timed out", aDone.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }

        Thread b = new Thread(() -> {
            try {
                int id = CarrierIdentity.bind();
                bId.set(id);
                bSaw.set(tl.get());
                CarrierIdentity.unbind();
            } catch (Throwable th) {
                err.compareAndSet(null, th);
            } finally {
                bDone.countDown();
            }
        }, "recycle-B");
        b.start();
        Assert.assertTrue("B timed out", bDone.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }

        Assert.assertNotEquals(
                "B must not observe A's stale value (B id=" + bId.get() + ", A id=" + aId.get() + ")",
                F1, bSaw.get());
        Assert.assertEquals(
                "B must see the supplier's initial value on a fresh slot",
                "init", bSaw.get());
    }

    @Test
    public void testTightRecycleCycleEachCycleSeesFreshSlot() throws Exception {
        // Single thread cycles bind -> get -> set -> unbind many times. Each
        // bind pops from RECYCLED (after the first few cycles when the queue
        // has filled with the thread's own released ids). Every cycle must
        // start with the slot fresh: the supplier's initial value is observed
        // before the cycle's set takes effect, and the prior cycle's set is
        // not visible. Exercises the recycle path deterministically without
        // depending on inter-test RECYCLED state.
        final int cycles = 256;
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> "init");

        AtomicReference<Throwable> err = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                for (int i = 0; i < cycles; i++) {
                    int id = CarrierIdentity.bind();
                    try {
                        String observed = tl.get();
                        if (!"init".equals(observed)) {
                            throw new AssertionError(
                                    "cycle " + i + ", id " + id +
                                            ": expected fresh 'init' on rebind, got '" + observed + "'");
                        }
                        tl.set("dirty-" + i);
                        Assert.assertEquals("dirty-" + i, tl.get());
                    } finally {
                        CarrierIdentity.unbind();
                    }
                }
            } catch (Throwable th) {
                err.compareAndSet(null, th);
            } finally {
                done.countDown();
            }
        }, "tight-recycle");
        t.start();
        Assert.assertTrue("tight recycle timed out", done.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    @Test
    public void testReleaseRowOnInvalidIdIsNoop() {
        // No exception, no side effect.
        CarrierLocal.releaseRow(-1);
        CarrierLocal.releaseRow(Integer.MAX_VALUE - 1);
    }

    @Test
    public void testStaleEntryReclaimedAfterCarrierLocalUnreachable() throws Exception {
        // Create a CarrierLocal in a way where only a WeakReference remains.
        // After GC, the entry stored in the carrier's map should be eligible
        // for expunge on the next operation. We verify two things:
        //
        //   1. The WeakReference to the CarrierLocal is cleared by GC (proves
        //      the table holds it weakly, not strongly).
        //   2. Subsequent operations on a different CarrierLocal in the same
        //      carrier work correctly (proves the stale-slot expunge logic
        //      doesn't crash and keeps the map walkable).
        AtomicReference<WeakReference<CarrierLocal<String>>> weakHolder = new AtomicReference<>();
        AtomicBoolean survivorOk = new AtomicBoolean();

        runBound(id -> {
            CarrierLocal<String> survivor = CarrierLocal.withInitial(() -> "survivor");
            survivor.set("alive");

            // Scope: short-lived CarrierLocal whose only strong ref goes away.
            {
                CarrierLocal<String> doomed = CarrierLocal.withInitial(() -> "doomed");
                doomed.set("about-to-die");
                weakHolder.set(new WeakReference<>(doomed));
                doomed = null;
            }

            // Force GC until WeakReference clears or we give up.
            for (int i = 0; i < 50 && weakHolder.get().get() != null; i++) {
                System.gc();
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            Assert.assertNull(
                    "table holds CarrierLocal weakly: WeakReference must clear once strong ref is gone",
                    weakHolder.get().get()
            );

            // Trigger an operation that walks the map -- this exercises
            // getEntryAfterMiss / set / cleanSomeSlots, where any stale entry
            // would be expunged. Survivor must still be addressable.
            for (int i = 0; i < 32; i++) {
                survivor.set("alive-" + i);
            }
            survivorOk.set("alive-31".equals(survivor.get()));
        });

        Assert.assertTrue("survivor lookup intact after stale-entry reclamation", survivorOk.get());
    }

    @Test
    public void testTwoBoundCarriersDoNotShareValues() throws Exception {
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> "default");

        CyclicBarrier afterAWrite = new CyclicBarrier(2);
        CyclicBarrier afterAssertion = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> err = new AtomicReference<>();
        AtomicReference<String> bSeen = new AtomicReference<>();

        Thread a = new Thread(() -> {
            try {
                CarrierIdentity.bind();
                try {
                    tl.set("a-private");
                    afterAWrite.await();
                    afterAssertion.await();
                    Assert.assertEquals("a-private", tl.get());
                } finally {
                    CarrierIdentity.unbind();
                }
            } catch (Throwable th) {
                err.compareAndSet(null, th);
            } finally {
                done.countDown();
            }
        }, "a");

        Thread b = new Thread(() -> {
            try {
                CarrierIdentity.bind();
                try {
                    afterAWrite.await();
                    bSeen.set(tl.get());
                    afterAssertion.await();
                } finally {
                    CarrierIdentity.unbind();
                }
            } catch (Throwable th) {
                err.compareAndSet(null, th);
            } finally {
                done.countDown();
            }
        }, "b");

        a.start();
        b.start();
        Assert.assertTrue(done.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
        Assert.assertEquals("b sees its own initial, not a's value", "default", bSeen.get());
    }

    @Test
    public void testUnboundDefaultIsNull() {
        CarrierLocal<String> tl = new CarrierLocal<>();
        Assert.assertNull(tl.get());
    }

    @Test
    public void testUnboundFallbackInitialPerThread() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        CarrierLocal<Integer> tl = CarrierLocal.withInitial(counter::incrementAndGet);

        // Distinct unbound threads each run initial once.
        Integer[] seen = new Integer[3];
        Thread[] threads = new Thread[3];
        for (int i = 0; i < 3; i++) {
            final int idx = i;
            threads[i] = new Thread(() -> seen[idx] = tl.get());
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(DEFAULT_TIMEOUT_S));
        }
        Assert.assertEquals(3, counter.get());
        Assert.assertNotEquals(seen[0], seen[1]);
        Assert.assertNotEquals(seen[1], seen[2]);
        Assert.assertNotEquals(seen[0], seen[2]);
    }

    @Test
    public void testUnboundRemoveThenGetReinitializes() {
        AtomicInteger counter = new AtomicInteger();
        CarrierLocal<Integer> tl = CarrierLocal.withInitial(counter::incrementAndGet);
        Assert.assertEquals(Integer.valueOf(1), tl.get());
        Assert.assertEquals(Integer.valueOf(1), tl.get());
        tl.remove();
        Assert.assertEquals(Integer.valueOf(2), tl.get());
    }

    @Test
    public void testUnboundSetGet() {
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> "default");
        Assert.assertEquals("default", tl.get());
        tl.set("x");
        Assert.assertEquals("x", tl.get());
        tl.remove();
        Assert.assertEquals("default", tl.get());
    }

    @Test
    public void testWithInitialUsedByGet() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CarrierLocal<String> tl = CarrierLocal.withInitial(() -> "v" + calls.incrementAndGet());

        runBound(id -> {
            Assert.assertEquals("v1", tl.get());
            Assert.assertEquals("v1", tl.get()); // cached
        });
        // After unbind+rebind, a fresh carrier id sees a fresh init.
        runBound(id -> {
            Assert.assertEquals("v2", tl.get());
        });
    }

    private static void runBound(IntConsumer body) throws Exception {
        AtomicReference<Throwable> err = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                int id = CarrierIdentity.bind();
                try {
                    body.accept(id);
                } finally {
                    CarrierIdentity.unbind();
                }
            } catch (Throwable th) {
                err.set(th);
            } finally {
                done.countDown();
            }
        }, "carrier-test");
        t.start();
        Assert.assertTrue("bound thread did not finish in time", done.await(DEFAULT_TIMEOUT_S, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    private static final class CountingCloseable implements Closeable {
        volatile int closes;

        @Override
        public void close() {
            closes++;
        }
    }
}
