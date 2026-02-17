/*******************************************************************************
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

import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class SimpleLockTest {
    @Test
    public void testLock1() {
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

        try {
            lock.unlock();
            lock.unlock();
            Assert.fail();
        } catch (IllegalStateException ex) {
            Assert.assertFalse(lock.isLocked());
        }
    }

    @Test
    public void testLock2() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        AtomicBoolean holdsLock = new AtomicBoolean();
        AtomicBoolean unparkFlag = new AtomicBoolean();
        Thread thread;
        lock.lock();
        try {
            thread = new Thread(() -> {
                lock.lock();  // should block
                holdsLock.set(true);
                while (!unparkFlag.get()) {
                    LockSupport.park();
                }
                if (Thread.interrupted()) {
                    throw new RuntimeException("Interrupted");
                }
                lock.unlock();
                holdsLock.set(false);
            }, "thread");
            thread.start();

            // give time for thread to block
            Thread.sleep(500);
            Assert.assertFalse(holdsLock.get());
        } finally {
            lock.unlock();
        }

        // thread should acquire lock, park, unpark, and then release lock
        while (!holdsLock.get()) {
            Os.pause();
        }
        unparkFlag.set(true);
        LockSupport.unpark(thread);
        while (holdsLock.get()) {
            Os.pause();
        }
        thread.join();
    }

    @Test
    public void testLock3() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        AtomicBoolean unparkFlag = new AtomicBoolean();

        Thread thread = new Thread(() -> {
            lock.lock();
            try {
                while (!unparkFlag.get()) {
                    LockSupport.park();
                }
                if (Thread.interrupted()) {
                    throw new RuntimeException("Interrupted");
                }
            } finally {
                lock.unlock();
            }
        }, "thread");
        thread.start();

        // wait for thread to acquire lock
        while (!lock.isLocked()) {
            Os.pause();
        }

        // thread cannot acquire lock
        try {
            Assert.assertFalse(lock.tryLock());
        } finally {
            // thread should unlock
            unparkFlag.set(true);
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
        AtomicBoolean unparkFlag = new AtomicBoolean();

        Thread thread1 = new Thread(() -> {
            lock.lock();
            try {
                while (!unparkFlag.get()) {
                    LockSupport.park();
                }
                if (Thread.interrupted()) {
                    throw new RuntimeException("Interrupted");
                }
            } finally {
                lock.unlock();
            }
        }, "thread1");
        thread1.start();

        // wait for thread to acquire lock
        while (!lock.isLocked()) {
            Os.pause();
        }

        AtomicBoolean holdsLock = new AtomicBoolean();
        Thread thread2 = new Thread(() -> {
            lock.lock();
            holdsLock.set(true);
            LockSupport.park();
            lock.unlock();
            holdsLock.set(false);
        }, "thread2");
        thread2.start();

        // thread2 should block
        Thread.sleep(500);
        Assert.assertFalse(holdsLock.get());

        // unpark thread1
        unparkFlag.set(true);
        LockSupport.unpark(thread1);

        // thread2 should acquire lock
        while (!holdsLock.get()) {
            Os.pause();
        }
        // unpark thread and it should release lock
        LockSupport.unpark(thread2);
        while (holdsLock.get()) {
            Os.pause();
        }

        thread1.join();
        thread2.join();
    }

    @Test
    public void testLock5() throws Exception {
        SimpleWaitingLock lock = new SimpleWaitingLock();
        AtomicBoolean unparkFlag = new AtomicBoolean();

        Thread thread = new Thread(() -> {
            lock.lock();
            try {
                while (!unparkFlag.get()) {
                    LockSupport.park();
                }
                if (Thread.interrupted()) {
                    throw new RuntimeException("Interrupted");
                }
            } finally {
                lock.unlock();
            }
        }, "thread");
        thread.start();

        // wait for thread to acquire lock
        while (!lock.isLocked()) {
            Os.pause();
        }

        // thread cannot acquire lock
        try {
            Assert.assertFalse(lock.tryLock(100, TimeUnit.MILLISECONDS));
        } finally {
            // thread should unlock
            unparkFlag.set(true);
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
            Os.sleep(25);
            lock.unlock();
        }, "thread");
        thread.start();

        // wait for thread to acquire lock
        while (!lock.isLocked()) {
            Os.pause();
        } // spin

        Assert.assertTrue(lock.isLocked());

        // thread can acquire lock
        try {
            Assert.assertTrue(lock.tryLock(250, TimeUnit.MILLISECONDS));
        } finally {
            lock.unlock();
            thread.join();
        }
    }
}
