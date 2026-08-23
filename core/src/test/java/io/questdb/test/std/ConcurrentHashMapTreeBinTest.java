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
        final long lockStateOffset = Unsafe.getFieldOffset(treeBin.getClass(), "lockState");
        final long waiterOffset = Unsafe.getFieldOffset(treeBin.getClass(), "waiter");

        setLockState(treeBin, lockStateOffset, READER);
        TestUtils.assertInterruptedWaitDoesNotSpin(
                "tree-bin contended lock",
                operation,
                treeBin,
                () -> {
                    final Object waiter = Unsafe.getObjectVolatile(treeBin, waiterOffset);
                    if (!(waiter instanceof Thread waiterThread)) {
                        Assert.fail("tree-bin waiter was not registered");
                        return;
                    }
                    Assert.assertSame(treeBin, LockSupport.getBlocker(waiterThread));
                },
                () -> {
                    final boolean isWaiterRegistered = Unsafe.cas(
                            treeBin,
                            lockStateOffset,
                            READER_AND_WAITER,
                            WAITER
                    );
                    if (!isWaiterRegistered) {
                        setLockState(treeBin, lockStateOffset, 0);
                    }
                    Assert.assertTrue("tree-bin waiter was not registered", isWaiterRegistered);
                }
        );
    }

    private static Object findTreeBin(Object map) {
        final long tableOffset = Unsafe.getFieldOffset(map.getClass(), "table");
        final Object[] table = (Object[]) Unsafe.getObjectVolatile(map, tableOffset);
        for (Object bin : table) {
            if (bin != null) {
                return bin;
            }
        }
        Assert.fail("map did not treeify a bin");
        return null;
    }

    private static void setLockState(Object treeBin, long lockStateOffset, int value) {
        int current;
        do {
            current = Unsafe.getIntVolatile(treeBin, lockStateOffset);
        } while (!Unsafe.cas(treeBin, lockStateOffset, current, value));
    }

    private static int spread(int hash) {
        return (hash ^ (hash >>> 16)) & 0x7fffffff;
    }
}
