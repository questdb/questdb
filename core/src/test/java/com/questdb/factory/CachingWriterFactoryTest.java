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
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class CachingWriterFactoryTest extends AbstractTest {

    @Test
    public void testOneThreadGetRelease() throws Exception {

        JournalStructure s = new JournalStructure("x").$date("ts").$();
        CachingWriterFactory wf = theFactory.getCachingWriterFactory();

        JournalWriter x;
        JournalWriter y;

        x = wf.writer(s);
        try {
            Assert.assertEquals(0, wf.countFreeWriters());
            Assert.assertNotNull(x);
            Assert.assertTrue(x.isOpen());
            Assert.assertTrue(x == wf.writer(s));
        } finally {
            x.close();
        }

        Assert.assertEquals(1, wf.countFreeWriters());

        y = wf.writer(s);
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
        final JournalStructure s = new JournalStructure("x").$date("ts").$();
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

                        try (JournalWriter w = wf.writer(s)) {
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

        Assert.assertEquals(1, writerCount.get());
        Assert.assertEquals(0, errors.get());
        Assert.assertEquals(1, wf.countFreeWriters());

    }
}