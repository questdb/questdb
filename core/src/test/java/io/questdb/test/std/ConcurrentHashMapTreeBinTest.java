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

import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ConcurrentIntHashMap;
import io.questdb.std.ConcurrentLongHashMap;
import io.questdb.std.Hash;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class ConcurrentHashMapTreeBinTest {
    private static final int BIN_MASK = 127;
    private static final int ENTRY_COUNT = 16;
    private static final int READER = 4;
    private static final int READER_AND_WAITER = 6;
    private static final int WAITER = 2;

    @Test
    public void testCharSequenceMapInterruptedTreeBinWait() throws Exception {
        final ConcurrentHashMap<Integer> map = new ConcurrentHashMap<>(64);
        final List<String> keys = new ArrayList<>();
        for (int i = 0; keys.size() < ENTRY_COUNT; i++) {
            final String key = "key" + i;
            if ((spread(Chars.hashCode(key)) & BIN_MASK) == 0) {
                keys.add(key);
            }
        }
        keys.sort(Comparator.comparingInt(key -> spread(Chars.hashCode(key))));
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(keys.get(i), i);
        }

        final String keyToRemove = keys.get(0);
        assertInterruptedTreeBinWait(map, () -> Assert.assertNotNull(map.remove(keyToRemove)));
        Assert.assertFalse(map.containsKey(keyToRemove));
    }

    @Test
    public void testIntMapInterruptedTreeBinWait() throws Exception {
        final ConcurrentIntHashMap<Integer> map = new ConcurrentIntHashMap<>(64);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i * 65_537, i);
        }

        assertInterruptedTreeBinWait(map, () -> Assert.assertNotNull(map.remove(0)));
        Assert.assertFalse(map.containsKey(0));
    }

    @Test
    public void testLongMapInterruptedTreeBinWait() throws Exception {
        final ConcurrentLongHashMap<Integer> map = new ConcurrentLongHashMap<>(64);
        final List<Long> keys = new ArrayList<>();
        for (long key = 0; keys.size() < ENTRY_COUNT; key++) {
            if ((spread(Hash.hashLong32(key)) & BIN_MASK) == 0) {
                keys.add(key);
            }
        }
        keys.sort(Comparator.comparingInt(key -> spread(Hash.hashLong32(key))));
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(keys.get(i), i);
        }

        final long keyToRemove = keys.get(0);
        assertInterruptedTreeBinWait(map, () -> Assert.assertNotNull(map.remove(keyToRemove)));
        Assert.assertFalse(map.containsKey(keyToRemove));
    }

    private static void assertInterruptedTreeBinWait(Object map, Runnable operation) throws Exception {
        final Object treeBin = findTreeBin(map);
        final sun.misc.Unsafe unsafe = Unsafe.getUnsafe();
        final long lockStateOffset = Unsafe.getFieldOffset(treeBin.getClass(), "lockState");
        final long waiterOffset = Unsafe.getFieldOffset(treeBin.getClass(), "waiter");
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean isInterruptedAfter = new AtomicBoolean();
        final AtomicBoolean isOperationComplete = new AtomicBoolean();

        unsafe.putIntVolatile(treeBin, lockStateOffset, READER);
        final Thread contender = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                operation.run();
                isOperationComplete.set(true);
            } catch (Throwable th) {
                failure.set(th);
            } finally {
                isInterruptedAfter.set(Thread.currentThread().isInterrupted());
            }
        }, "tree-bin-contender");
        contender.setDaemon(true);
        contender.start();

        boolean isReaderReleased = false;
        try {
            TestUtils.assertEventually(() -> {
                Assert.assertSame(contender, unsafe.getObjectVolatile(treeBin, waiterOffset));
                Assert.assertEquals(Thread.State.WAITING, contender.getState());
                Assert.assertSame(treeBin, LockSupport.getBlocker(contender));
            }, 5);
            Assert.assertTrue(unsafe.compareAndSwapInt(treeBin, lockStateOffset, READER_AND_WAITER, WAITER));
            isReaderReleased = true;
            LockSupport.unpark(contender);
        } finally {
            if (!isReaderReleased) {
                unsafe.putIntVolatile(treeBin, lockStateOffset, 0);
                LockSupport.unpark(contender);
            }
            contender.join(TimeUnit.SECONDS.toMillis(5));
        }

        Assert.assertFalse("tree-bin contender did not stop", contender.isAlive());
        if (failure.get() != null) {
            throw new AssertionError("tree-bin contender failed", failure.get());
        }
        Assert.assertTrue(isOperationComplete.get());
        Assert.assertTrue(isInterruptedAfter.get());
    }

    private static Object findTreeBin(Object map) {
        final long tableOffset = Unsafe.getFieldOffset(map.getClass(), "table");
        final sun.misc.Unsafe unsafe = Unsafe.getUnsafe();
        final Object[] table = (Object[]) unsafe.getObjectVolatile(map, tableOffset);
        final long arrayOffset = unsafe.arrayBaseOffset(Object[].class);
        final long arrayScale = unsafe.arrayIndexScale(Object[].class);
        for (int i = 0, n = table.length; i < n; i++) {
            final Object bin = unsafe.getObjectVolatile(table, arrayOffset + i * arrayScale);
            if (bin != null && bin.getClass().getSimpleName().equals("TreeBin")) {
                return bin;
            }
        }
        Assert.fail("map did not treeify a bin");
        return null;
    }

    private static int spread(int hash) {
        return (hash ^ (hash >>> 16)) & 0x7fffffff;
    }
}
