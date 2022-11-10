/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.mp;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static junit.framework.Assert.assertFalse;

public class SimpleLockTest {
    @Test
    public void testLock() {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        Assert.assertFalse(lock.isLocked());
        lock.lock();
        Assert.assertTrue(lock.isLocked());
        lock.unlock();
        Assert.assertFalse(lock.isLocked());

        Assert.assertTrue(lock.tryLock());
        Assert.assertFalse(lock.tryLock());

        lock.unlock();
        Assert.assertTrue(lock.tryLock(10, TimeUnit.MILLISECONDS));
        Assert.assertFalse(lock.tryLock(10, TimeUnit.MILLISECONDS));
        Assert.assertTrue(lock.isLocked());

        Assert.assertTrue(lock.unlock());
        Assert.assertFalse(lock.unlock());
        Assert.assertFalse(lock.isLocked());
    }

    @Test
    public void testLock2() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        AtomicBoolean holdsLock = new AtomicBoolean();

        Thread thread;
        lock.lock();
        try {
            thread = new Thread(() -> {
                lock.lock();  // should block
                holdsLock.set(true);
                LockSupport.park();
                lock.unlock();
                holdsLock.set(false);
            });
            thread.start();

            // give time for thread to block
            Thread.sleep(500);
            Assert.assertFalse(holdsLock.get());
        } finally {
            lock.unlock();
        }

        // thread should acquire lock, park, unpark, and then release lock
        while (!holdsLock.get()) {
            Thread.sleep(20);
        }
        LockSupport.unpark(thread);
        while (holdsLock.get()) {
            Thread.sleep(20);
        }
        thread.join();
    }

    @Test
    public void testLock3() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        Thread thread = new Thread(() -> {
            lock.lock();
            try {
                LockSupport.park();
            } finally {
                lock.unlock();
            }
        });
        thread.start();

        // wat for thread to acquire lock
        while (!lock.isLocked()) {
            Thread.sleep(20);
        }

        // thread cannot acquire lock
        try {
            Assert.assertFalse(lock.tryLock());
        } finally {
            // thread should unlock
            LockSupport.unpark(thread);

            // thread should be able to acquire lock
            lock.lock();
            lock.unlock();
            thread.join();
        }
    }


    @Test
    public void testLock4() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();

        Thread thread1 = new Thread(() -> {
            lock.lock();
            try {
                LockSupport.park();
            } finally {
                lock.unlock();
            }
        });
        thread1.start();

        // wat for virtual thread to acquire lock
        while (!lock.isLocked()) {
            Thread.sleep(10);
        }

        AtomicBoolean holdsLock = new AtomicBoolean();
        Thread thread2 = new Thread(() -> {
            lock.lock();
            holdsLock.set(true);
            LockSupport.park();
            lock.unlock();
            holdsLock.set(false);
        });
        thread2.start();

        // thread2 should block
        Thread.sleep(1000);
        assertFalse(holdsLock.get());

        // unpark thread1
        LockSupport.unpark(thread1);

        // thread2 should acquire lock
        while (!holdsLock.get()) {
            Thread.sleep(20);
        }
        // unpark thread and it should release lock
        LockSupport.unpark(thread2);
        while (holdsLock.get()) {
            Thread.sleep(20);
        }

        thread1.join();
        thread2.join();
    }

    @Test
    public void testLock5() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        Thread thread = new Thread(() -> {
            lock.lock();
            try {
                LockSupport.park();
            } finally {
                lock.unlock();
            }
        });
        thread.start();

        // wat for thread to acquire lock
        while (!lock.isLocked()) {
            Thread.sleep(20);
        }

        // thread cannot acquire lock
        try {
            Assert.assertFalse(lock.tryLock(100, TimeUnit.MILLISECONDS));
        } finally {
            // thread should unlock
            LockSupport.unpark(thread);

            // thread should be able to acquire lock
            lock.lock();
            lock.unlock();
            thread.join();
        }
    }

    @Test
    public void testLock6() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        Thread thread = new Thread(() -> {
            lock.lock();
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        });
        thread.start();

        // wat for thread to acquire lock
        while (!lock.isLocked()) {
            Thread.sleep(10);
        }

        Assert.assertTrue(lock.isLocked());

        // thread can acquire lock
        try {
            Assert.assertTrue(lock.tryLock(100, TimeUnit.MILLISECONDS));
        } finally {
            lock.unlock();
            thread.join();
        }
    }
}