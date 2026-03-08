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

package io.questdb.test.std;

import io.questdb.std.Mutable;
import io.questdb.std.WeakMutableObjectPool;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WeakMutableObjectPoolTest {
    private static final int initSize = 10;

    @Test
    public void testClose() {
        final List<MutablePoolElement> tmp = new ArrayList<>();
        final WeakMutableObjectPool<MutablePoolElement> pool = new WeakMutableObjectPool<>(() -> {
            MutablePoolElement element = new MutablePoolElement();
            tmp.add(element);
            return element;
        }, initSize);

        for (int i = 0; i < initSize; i++) {
            MutablePoolElement element = tmp.get(i);
            assertFalse(element.cleared);
            assertFalse(element.closed);
        }

        pool.close();
        assertEquals(0, pool.size());
        for (int i = 0; i < initSize; i++) {
            MutablePoolElement element = tmp.get(i);
            assertFalse(element.cleared);
            assertTrue(element.closed);
        }
    }

    @Test
    public void testMaxSize() {
        try (
                WeakMutableObjectPool<MutablePoolElement> pool = new WeakMutableObjectPool<>(MutablePoolElement::new, initSize)
        ) {
            assertEquals(initSize, pool.size());

            for (int i = 0; i < initSize; i++) {
                MutablePoolElement element = new MutablePoolElement();
                pool.push(element);
                assertTrue(element.cleared);
                assertFalse(element.closed);
                assertEquals(initSize + 1 + i, pool.size());
            }

            for (int i = 0; i < 100; i++) {
                MutablePoolElement element = new MutablePoolElement();
                pool.push(element);
                assertFalse(element.cleared);
                assertTrue(element.closed);
                assertEquals(2 * initSize, pool.size());
            }
        }
    }

    @Test
    public void testPopPush() {
        try (
                WeakMutableObjectPool<MutablePoolElement> pool = new WeakMutableObjectPool<>(MutablePoolElement::new, initSize)
        ) {
            assertEquals(initSize, pool.size());

            final List<MutablePoolElement> tmp = new ArrayList<>();
            for (int i = 0; i < initSize; i++) {
                MutablePoolElement element = pool.pop();
                assertFalse(element.cleared);
                assertFalse(element.closed);
                tmp.add(element);
            }
            assertEquals(0, pool.size());

            for (int i = 0; i < initSize; i++) {
                MutablePoolElement element = tmp.get(i);
                pool.push(element);
                assertTrue(element.cleared);
                assertFalse(element.closed);
            }
            assertEquals(initSize, pool.size());
        }
    }

    private static class MutablePoolElement implements Mutable, Closeable {
        private boolean cleared = false;
        private boolean closed = false;

        @Override
        public void clear() {
            cleared = true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
