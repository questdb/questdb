/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OsTest {
    @Test
    public void testAffinity() throws Exception {
        Assert.assertEquals(0, Os.setCurrentThreadAffinity(0));

        AtomicInteger result = new AtomicInteger(-1);
        CountDownLatch threadHalt = new CountDownLatch(1);

        new Thread(() -> {
            result.set(Os.setCurrentThreadAffinity(1));
            threadHalt.countDown();
        }).start();

        Assert.assertTrue(threadHalt.await(1, TimeUnit.SECONDS));
        Assert.assertEquals(0, result.get());

        Assert.assertEquals(0, Os.setCurrentThreadAffinity(-1));
    }

    @Test
    public void testCurrentTimeMicros() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeMicros();
        long delta = actual / 1000 - reference;
        Assert.assertTrue(delta < 200);
    }

    @Test
    public void testCurrentTimeNanos() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeNanos();
        Assert.assertTrue(actual > 0);
        long delta = actual / 1_000_000 - reference;
        Assert.assertTrue(delta < 200);
        System.out.println(reference);
        System.out.println(actual);
    }
}