/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.mon;

import com.questdb.Journal;
import com.questdb.factory.FactoryEventListener;
import com.questdb.iter.clock.MilliClock;
import com.questdb.test.tools.AbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.LockSupport;

public class FactoryEventLoggerTest extends AbstractTest {

    @Test
    @Ignore
    public void testThroughput() throws Exception {
        final FactoryEventLogger logger = new FactoryEventLogger(factoryContainer.getFactory(), 1000, 1000, MilliClock.INSTANCE);

        final int count = 1000;
        final CountDownLatch done = new CountDownLatch(1);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final FactoryEventListener listener = factoryContainer.getFactory().getEventListener();

        new Thread(() -> {
            try (Journal r = factoryContainer.getFactory().reader("$mon_factory")) {
                barrier.await();
                int i = 0;
                while (i < count) {
                    if (logger.run()) {
                        i++;
                    } else {
                        r.refresh();
                        if (r.size() == count) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                done.countDown();
            }
        }).start();

        barrier.await();


        int i = 0;
        while (i < count) {
            if (listener.onEvent((byte) 1, 1, "test", (short) 1, (short) 0, (short) 5)) {
                i++;
            } else {
                LockSupport.parkNanos(1);
            }
        }

        done.await();

        logger.close();

        try (Journal r = factoryContainer.getFactory().reader("$mon_factory")) {
            System.out.println(r.size());
        }

    }
}