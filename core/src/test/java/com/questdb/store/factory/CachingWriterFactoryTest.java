/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store.factory;

import com.questdb.ex.FactoryClosedException;
import com.questdb.ex.JournalLockedException;
import com.questdb.ex.WriterBusyException;
import com.questdb.std.ex.JournalException;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.AbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class CachingWriterFactoryTest extends AbstractTest {

    private CachingWriterFactory wf;

    @Before
    public void setUp() {
        this.wf = new CachingWriterFactory(factoryContainer.getConfiguration(), 0);
    }

    @After
    public void tearDown() {
        this.wf.close();
    }

    @Test
    public void testAllocateAndClear() throws Exception {
        final JournalMetadata<?> m = new JournalStructure("z").$date("ts").$().build();

        int n = 2;
        final CyclicBarrier barrier = new CyclicBarrier(n);
        final CountDownLatch halt = new CountDownLatch(n);
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger writerCount = new AtomicInteger();

        new Thread(() -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    try (JournalWriter ignored = wf.writer(m)) {
                        writerCount.incrementAndGet();
                    } catch (WriterBusyException ignored) {
                    }

                    if (i == 1) {
                        barrier.await();
                    }
                    LockSupport.parkNanos(10L);
                }
            } catch (Exception e) {
                e.printStackTrace();
                errors.incrementAndGet();
            } finally {
                halt.countDown();
            }
        }).start();

        new Thread(() -> {
            try {
                barrier.await();

                for (int i = 0; i < 1000; i++) {
                    wf.releaseInactive();
                    LockSupport.parkNanos(10L);
                }

            } catch (Exception e) {
                e.printStackTrace();
                errors.incrementAndGet();
            } finally {
                halt.countDown();
            }
        }).start();

        halt.await();

        Assert.assertTrue(writerCount.get() > 0);
        Assert.assertEquals(0, errors.get());
    }

    @Test
    public void testFactoryCloseBeforeRelease() throws Exception {

        final JournalMetadata<?> m = new JournalStructure("x").$date("ts").$().build();

        JournalWriter x;

        x = wf.writer(m);
        try {
            Assert.assertEquals(0, wf.countFreeWriters());
            Assert.assertNotNull(x);
            Assert.assertTrue(x.isOpen());
            Assert.assertTrue(x == wf.writer(m));
            wf.close();
        } finally {
            x.close();
        }

        Assert.assertFalse(x.isOpen());
        try {
            wf.writer(m);
        } catch (FactoryClosedException ignored) {
        }
    }

    @Test
    public void testLockNonExisting() throws Exception {
        final JournalMetadata<?> x = new JournalStructure("x").$date("ts").$().build();

        wf.lock(x.getName());

        try {
            wf.writer(x);
            Assert.fail();
        } catch (JournalLockedException ignored) {
        }

        wf.unlock(x.getName());

        try (JournalWriter wx = wf.writer(x)) {
            Assert.assertNotNull(wx);
        }
    }

    @Test
    public void testLockUnlock() throws Exception {

        final JournalMetadata<?> x = new JournalStructure("x").$date("ts").$().build();
        final JournalMetadata<?> y = new JournalStructure("y").$date("ts").$().build();

        JournalWriter wx = wf.writer(x);
        Assert.assertNotNull(wx);
        Assert.assertTrue(wx.isOpen());

        JournalWriter wy = wf.writer(y);
        Assert.assertNotNull(wy);
        Assert.assertTrue(wy.isOpen());

        try {

            // check that lock is successful
            wf.lock(x.getName());

            // check that writer x is closed and writer y is open (lock must not spill out to other writers)
            Assert.assertFalse(wx.isOpen());
            Assert.assertTrue(wy.isOpen());

            // check that when name is locked writers are not created
            try {
                wf.writer(x);
            } catch (JournalLockedException ignored) {

            }

            final CountDownLatch done = new CountDownLatch(1);
            final AtomicBoolean result = new AtomicBoolean();

            // have new thread try to allocated this writers
            new Thread(() -> {
                try (JournalWriter ignored = wf.writer(x)) {
                    result.set(false);
                } catch (WriterBusyException ignored) {
                    result.set(true);
                } catch (JournalException e) {
                    e.printStackTrace();
                    result.set(false);
                }
                done.countDown();
            }).start();

            Assert.assertTrue(done.await(1, TimeUnit.SECONDS));
            Assert.assertTrue(result.get());

            wf.unlock(x.getName());

            wx = wf.writer(x);
            Assert.assertNotNull(wx);
            Assert.assertTrue(wx.isOpen());

            try {
                // unlocking writer that has not been locked must produce exception
                // and not affect open writer
                wf.unlock(wx.getName());
                Assert.fail();
            } catch (IllegalStateException ignored) {
            }

            Assert.assertTrue(wx.isOpen());

        } finally {
            wx.close();
            wy.close();
        }
    }

    @Test
    public void testNewLock() throws Exception {

        final JournalMetadata<?> m = new JournalStructure("x").$date("ts").$().build();


        wf.lock("x");
        try {
            wf.writer(m);
            Assert.fail();
        } catch (JournalException ignored) {
        }

        wf.unlock("x");
    }

    @Test
    public void testOneThreadGetRelease() throws Exception {

        final JournalMetadata<?> m = new JournalStructure("x").$date("ts").$().build();

        JournalWriter x;
        JournalWriter y;

        x = wf.writer(m);
        try {
            Assert.assertEquals(0, wf.countFreeWriters());
            Assert.assertNotNull(x);
            Assert.assertTrue(x.isOpen());
            Assert.assertTrue(x == wf.writer(m));
        } finally {
            x.close();
        }

        Assert.assertEquals(1, wf.countFreeWriters());

        y = wf.writer(m);
        try {
            Assert.assertNotNull(y);
            Assert.assertTrue(y.isOpen());
            Assert.assertTrue(y == x);
        } finally {
            y.close();
        }

        Assert.assertEquals(1, wf.countFreeWriters());
    }

    @Test
    public void testTwoThreadsRaceToAllocate() throws Exception {
        final JournalMetadata<?> m = new JournalStructure("x").$date("ts").$().build();

        int n = 2;
        final CyclicBarrier barrier = new CyclicBarrier(n);
        final CountDownLatch halt = new CountDownLatch(n);
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger writerCount = new AtomicInteger();

        for (int i = 0; i < n; i++) {
            new Thread(() -> {
                try {
                    barrier.await();


                    try (JournalWriter ignored = wf.writer(m)) {
                        writerCount.incrementAndGet();
                    } catch (WriterBusyException ignored) {
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    halt.countDown();
                }
            }).start();
        }

        halt.await();

        // this check is unreliable on slow build servers
        // it is very often the case that there are limited number of cores
        // available and threads execute sequentially rather than
        // simultaneously. We should check that none of the threads
        // receive error.
//        Assert.assertEquals(1, writerCount.get());
        Assert.assertEquals(0, errors.get());
        Assert.assertEquals(1, wf.countFreeWriters());
    }
}