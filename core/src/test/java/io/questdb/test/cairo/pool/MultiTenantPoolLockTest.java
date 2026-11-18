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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.AbstractMultiTenantPool;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.PoolTenant;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.std.Rnd;
import io.questdb.std.str.CharSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link AbstractMultiTenantPool#lock(TableToken)} either takes the lock of the entry or leaves the entry as it found
 * it: unlocked, every slot free, whether it fails on a busy tenant or a tenant that does not close. The tenants of
 * this pool hold no resource, so a close can fail without leaking one.
 */
public class MultiTenantPoolLockTest extends AbstractCairoTest {
    private static final TableToken TOKEN = new TableToken("t", "t~1", null, 1, false, false, false);

    @Test
    public void testFailedLocksAmongConcurrentGetsLeaveNoSlotHeld() throws Exception {
        // Lockers race tenants taken and returned over three segments. A lock that wins finds every slot its own; one
        // that loses must hand back what it took: the locker holds no slot right after, and once all stop, every slot
        // is free and any thread can lock. A later lock and unlock drops the entry, and with it what a lost lock kept,
        // so the check after each lost lock is the one that sees a slot kept.
        final int segmentSize = 2;
        final int segmentCount = 3;
        final int getterCount = 3;
        final int lockerCount = 2;
        final int iterations = 2_000;
        final Rnd seedRnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            try (TestPool pool = new TestPool(poolConfiguration(segmentSize, segmentCount))) {
                final CyclicBarrier start = new CyclicBarrier(getterCount + lockerCount);
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final AtomicInteger lockCount = new AtomicInteger();
                final AtomicInteger failedLockCount = new AtomicInteger();
                final Thread[] threads = new Thread[getterCount + lockerCount];
                for (int t = 0; t < getterCount; t++) {
                    final long seed0 = seedRnd.nextLong();
                    final long seed1 = seedRnd.nextLong();
                    threads[t] = new Thread(() -> {
                        final Rnd rnd = new Rnd(seed0, seed1);
                        try {
                            TestUtils.await(start);
                            for (int i = 0; i < iterations; i++) {
                                final int count = 1 + rnd.nextInt(segmentSize);
                                final TestTenant[] tenants = new TestTenant[count];
                                try {
                                    for (int k = 0; k < count; k++) {
                                        tenants[k] = pool.get(TOKEN);
                                    }
                                } catch (EntryLockedException | EntryUnavailableException ignore) {
                                    // a lock holds the entry, or the other getters hold every slot
                                } finally {
                                    for (int k = 0; k < count; k++) {
                                        if (tenants[k] != null) {
                                            tenants[k].close();
                                        }
                                    }
                                }
                            }
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        }
                    });
                }
                for (int t = getterCount; t < getterCount + lockerCount; t++) {
                    threads[t] = new Thread(() -> {
                        try {
                            TestUtils.await(start);
                            for (int i = 0; i < iterations; i++) {
                                if (pool.lock(TOKEN)) {
                                    lockCount.incrementAndGet();
                                    Assert.assertEquals("a tenant is busy under the lock", 0, pool.getBusyCount());
                                    pool.unlock(TOKEN);
                                } else {
                                    failedLockCount.incrementAndGet();
                                    assertNoSlotHeldBy(pool, Thread.currentThread().threadId());
                                }
                            }
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        }
                    });
                }
                for (Thread thread : threads) {
                    thread.start();
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                LOG.info().$("lock race [locks=").$(lockCount.get()).$(", failedLocks=").$(failedLockCount.get()).I$();
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }

                assertEverySlotFree(pool);
                Assert.assertTrue(lockAndUnlockInThread(pool));
                assertCapacity(pool, segmentSize * segmentCount);
            }
        });
    }

    @Test
    public void testLockHeldByItsThread() throws Exception {
        // The thread that holds the lock takes it again; another thread cannot take it, or a tenant, until the unlock
        assertMemoryLeak(() -> {
            try (TestPool pool = new TestPool(poolConfiguration(2, 2))) {
                final TestTenant t0 = pool.get(TOKEN);
                t0.close();
                Assert.assertTrue(pool.lock(TOKEN));
                Assert.assertTrue(t0.isClosed);
                Assert.assertTrue(pool.lock(TOKEN));

                Assert.assertFalse(lockAndUnlockInThread(pool));
                final Throwable thrown = runInThread(() -> pool.get(TOKEN));
                Assert.assertTrue(String.valueOf(thrown), thrown instanceof EntryLockedException);
                assertNoSlotHeldBy(pool, -1L);

                pool.unlock(TOKEN);
                assertEverySlotFree(pool);
                Assert.assertTrue(lockAndUnlockInThread(pool));
                assertCapacity(pool, 4);
            }
        });
    }

    @Test
    public void testLockThatThrowsLeavesEntryUnlocked() throws Exception {
        // The second idle tenant fails to close inside lock(): the lock throws, hands back the two slots it took and
        // unlocks the entry. The tenant that failed to close leaves its slot, and the one past it stays pooled.
        assertMemoryLeak(() -> {
            try (TestPool pool = new TestPool(poolConfiguration(2, 2))) {
                final TestTenant t0 = pool.get(TOKEN);
                final TestTenant t1 = pool.get(TOKEN);
                final TestTenant t2 = pool.get(TOKEN);
                t0.close();
                t1.close();
                t2.close();
                pool.failCloseAfter(1);

                final Throwable thrown = lockInThreadAndCatch(pool);
                Assert.assertNotNull("the lock must throw", thrown);
                TestUtils.assertContains(thrown.getMessage(), "tenant close failed");
                pool.failCloseAfter(-1);

                Assert.assertTrue(t0.isClosed);
                Assert.assertFalse(t1.isClosed);
                Assert.assertFalse(t2.isClosed);
                assertEverySlotFree(pool);
                final AbstractMultiTenantPool.Entry<TestTenant> rootEntry = pool.entries().get(TOKEN.getDirName());
                Assert.assertNull(rootEntry.getTenant(0));
                Assert.assertNull(rootEntry.getTenant(1));
                Assert.assertSame(t2, rootEntry.getNext().getTenant(0));

                Assert.assertTrue(lockAndUnlockInThread(pool));
                assertCapacity(pool, 4);
            }
        });
    }

    @Test
    public void testReleaseAllThatThrowsFreesSlot() throws Exception {
        // An idle tenant fails to close while the pool releases it: its slot must be free and empty afterwards
        assertMemoryLeak(() -> {
            try (TestPool pool = new TestPool(poolConfiguration(2, 1))) {
                final TestTenant t0 = pool.get(TOKEN);
                t0.close();
                pool.failCloseAfter(0);
                try {
                    pool.releaseAll();
                    Assert.fail("the release must throw");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "tenant close failed");
                } finally {
                    pool.failCloseAfter(-1);
                }

                assertEverySlotFree(pool);
                Assert.assertNull(pool.entries().get(TOKEN.getDirName()).getTenant(0));
                Assert.assertTrue(lockAndUnlockInThread(pool));
                try (TestTenant t = pool.get(TOKEN)) {
                    Assert.assertNotSame(t0, t);
                }
                assertCapacity(pool, 2);
            }
        });
    }

    // Takes n tenants at once, each from its own slot, and returns them
    private static void assertCapacity(TestPool pool, int n) {
        final TestTenant[] tenants = new TestTenant[n];
        try {
            for (int i = 0; i < n; i++) {
                tenants[i] = pool.get(TOKEN);
                for (int k = 0; k < i; k++) {
                    Assert.assertNotSame(tenants[k], tenants[i]);
                }
            }
            Assert.assertEquals(n, pool.getBusyCount());
        } finally {
            for (TestTenant tenant : tenants) {
                if (tenant != null) {
                    tenant.close();
                }
            }
        }
    }

    private static void assertEverySlotFree(TestPool pool) {
        for (Map.Entry<CharSequence, AbstractMultiTenantPool.Entry<TestTenant>> me : pool.entries().entrySet()) {
            for (AbstractMultiTenantPool.Entry<TestTenant> e = me.getValue(); e != null; e = e.getNext()) {
                for (int i = 0, n = pool.getSegmentSize(); i < n; i++) {
                    Assert.assertEquals("slot " + e.getIndex() + ':' + i, -1L, e.getOwnerVolatile(i));
                }
            }
        }
    }

    // No other thread takes a slot for this one: what it holds, it took itself
    private static void assertNoSlotHeldBy(TestPool pool, long thread) {
        for (AbstractMultiTenantPool.Entry<TestTenant> e = pool.entries().get(TOKEN.getDirName()); e != null; e = e.getNext()) {
            for (int i = 0, n = pool.getSegmentSize(); i < n; i++) {
                Assert.assertNotEquals("a failed lock kept slot " + e.getIndex() + ':' + i, thread, e.getOwnerVolatile(i));
            }
        }
    }

    private static boolean lockAndUnlockInThread(TestPool pool) throws InterruptedException {
        final AtomicBoolean isLocked = new AtomicBoolean();
        final Throwable thrown = runInThread(() -> {
            if (pool.lock(TOKEN)) {
                isLocked.set(true);
                pool.unlock(TOKEN);
            }
        });
        if (thrown != null) {
            throw new AssertionError(thrown);
        }
        return isLocked.get();
    }

    private static Throwable lockInThreadAndCatch(TestPool pool) throws InterruptedException {
        return runInThread(() -> {
            if (pool.lock(TOKEN)) {
                pool.unlock(TOKEN);
                Assert.fail("the lock must fail");
            }
        });
    }

    private static CairoConfiguration poolConfiguration(int segmentSize, int maxSegments) {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public int getPoolSegmentSize() {
                return segmentSize;
            }

            @Override
            public int getReaderPoolMaxSegments() {
                return maxSegments;
            }
        };
    }

    private static Throwable runInThread(Runnable code) throws InterruptedException {
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                code.run();
            } catch (Throwable th) {
                thrown.set(th);
            }
        });
        thread.start();
        thread.join();
        return thrown.get();
    }

    private static class TestPool extends AbstractMultiTenantPool<TestTenant> {
        // closes left before a physical close fails, -1 when closes do not fail
        private final AtomicInteger closesBeforeFailure = new AtomicInteger(-1);

        TestPool(CairoConfiguration configuration) {
            super(configuration, configuration.getReaderPoolMaxSegments(), configuration.getInactiveReaderTTL());
        }

        void failCloseAfter(int closeCount) {
            closesBeforeFailure.set(closeCount);
        }

        @Override
        protected byte getListenerSrc() {
            return PoolListener.SRC_READER;
        }

        @Override
        protected TestTenant newTenant(
                TableToken tableToken,
                Entry<TestTenant> rootEntry,
                Entry<TestTenant> entry,
                int index,
                @Nullable ResourcePoolSupervisor<TestTenant> supervisor
        ) {
            return new TestTenant(this, tableToken, rootEntry, entry, index);
        }

        boolean returnTenant(TestTenant tenant) {
            return returnToPool(tenant);
        }

        void tenantClosing() {
            final int left = closesBeforeFailure.getAndUpdate(n -> n > 0 ? n - 1 : n);
            if (left == 0) {
                throw CairoException.critical(0).put("tenant close failed");
            }
        }
    }

    private static class TestTenant implements PoolTenant<TestTenant> {
        private final int index;
        private final TestPool owner;
        private final AbstractMultiTenantPool.Entry<TestTenant> rootEntry;
        private AbstractMultiTenantPool.Entry<TestTenant> entry;
        private volatile boolean isClosed;
        private TestPool pool;
        private TableToken tableToken;

        TestTenant(
                TestPool pool,
                TableToken tableToken,
                AbstractMultiTenantPool.Entry<TestTenant> rootEntry,
                AbstractMultiTenantPool.Entry<TestTenant> entry,
                int index
        ) {
            this.owner = pool;
            this.pool = pool;
            this.tableToken = tableToken;
            this.rootEntry = rootEntry;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            final TestPool pool = this.pool;
            if (pool != null && entry != null && pool.returnTenant(this)) {
                return;
            }
            owner.tenantClosing();
            isClosed = true;
        }

        @Override
        public AbstractMultiTenantPool.Entry<TestTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public AbstractMultiTenantPool.Entry<TestTenant> getRootEntry() {
            return rootEntry;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void refresh(@Nullable ResourcePoolSupervisor<TestTenant> supervisor) {
            Assert.assertFalse("a closed tenant is handed out", isClosed);
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("TestTenant{index=").put(index).put('}');
        }

        @Override
        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }
    }
}
