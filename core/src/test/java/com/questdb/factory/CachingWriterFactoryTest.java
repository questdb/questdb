/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.factory;

import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class CachingWriterFactoryTest extends AbstractTest {

    @Test
    public void testAllocateAndClear() throws Exception {
        final JournalMetadata<?> m = new JournalStructure("z").$date("ts").$().build();
        final CachingWriterFactory wf = theFactory.getCachingWriterFactory();

        int n = 2;
        final CyclicBarrier barrier = new CyclicBarrier(n);
        final CountDownLatch halt = new CountDownLatch(n);
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger writerCount = new AtomicInteger();

        new Thread() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 1000; i++) {
                        try (JournalWriter w = wf.writer(m)) {
                            if (w != null) {
                                writerCount.incrementAndGet();
                            }
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
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        wf.run();
                        LockSupport.parkNanos(10L);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    halt.countDown();
                }
            }
        }.start();

        halt.await();

        Assert.assertTrue(writerCount.get() > 0);
        Assert.assertEquals(0, errors.get());
    }

    @Test
    public void testFactoryCloseBeforeRelease() throws Exception {

        final JournalMetadata<?> m = new JournalStructure("x").$date("ts").$().build();
        CachingWriterFactory wf = theFactory.getCachingWriterFactory();

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
        Assert.assertNull(wf.writer(m));
    }

    @Test
    public void testLockUnlock() throws Exception {

        final JournalMetadata<?> x = new JournalStructure("x").$date("ts").$().build();
        final JournalMetadata<?> y = new JournalStructure("y").$date("ts").$().build();

        final CachingWriterFactory wf = theFactory.getCachingWriterFactory();

        JournalWriter wx = wf.writer(x);
        Assert.assertNotNull(wx);
        Assert.assertTrue(wx.isOpen());

        JournalWriter wy = wf.writer(y);
        Assert.assertNotNull(wy);
        Assert.assertTrue(wy.isOpen());

        try {

            // check that lock is successful
            Assert.assertTrue(wf.lock(x.getName()));

            // check that writer x is closed and writer y is open (lock must not spill out to other writers)
            Assert.assertFalse(wx.isOpen());
            Assert.assertTrue(wy.isOpen());

            // check that when name is locked writers are not created
            Assert.assertNull(wf.writer(x));
            Assert.assertEquals(LastError.E_NAME_LOCKED, LastError.getError());

            final CountDownLatch done = new CountDownLatch(1);
            final AtomicBoolean result = new AtomicBoolean();

            // have new thread try to allocated this writers
            new Thread() {
                @Override
                public void run() {
                    try (JournalWriter w = wf.writer(x)) {
                        result.set(w == null);
                    } catch (JournalException e) {
                        e.printStackTrace();
                        result.set(false);
                    }
                    done.countDown();
                }
            }.start();

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
    public void testLockNonExisting() throws Exception {
        final JournalMetadata<?> x = new JournalStructure("x").$date("ts").$().build();

        final CachingWriterFactory wf = theFactory.getCachingWriterFactory();

        wf.lock(x.getName());

        Assert.assertNull(wf.writer(x));
        Assert.assertEquals(LastError.E_NAME_LOCKED, LastError.getError());

        wf.unlock(x.getName());

        try (JournalWriter wx = wf.writer(x)) {
            Assert.assertNotNull(wx);
        }
    }

    @Test
    public void testOneThreadGetRelease() throws Exception {

        final JournalMetadata<?> m = new JournalStructure("x").$date("ts").$().build();
        CachingWriterFactory wf = theFactory.getCachingWriterFactory();

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
        final CachingWriterFactory wf = theFactory.getCachingWriterFactory();

        int n = 2;
        final CyclicBarrier barrier = new CyclicBarrier(n);
        final CountDownLatch halt = new CountDownLatch(n);
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger writerCount = new AtomicInteger();

        for (int i = 0; i < n; i++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        barrier.await();


                        try (JournalWriter w = wf.writer(m)) {
                            if (w != null) {
                                writerCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        errors.incrementAndGet();
                    } finally {
                        halt.countDown();
                    }
                }
            }.start();
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