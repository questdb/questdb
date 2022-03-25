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

package io.questdb.std;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WeakObjectPoolTest {
    private final int initSize = 10;

    @Test
    public void testClose() {
        final List<MutablePoolElement> tmp = new ArrayList<>();
        final WeakObjectPool<MutablePoolElement> pool = new WeakObjectPool<>(() -> {
            MutablePoolElement element = new MutablePoolElement();
            tmp.add(element);
            return element;
        }, initSize);

        for (int i = 0; i < initSize; i++) {
            MutablePoolElement element = tmp.get(i);
            assertFalse(element.cleared);
        }

        pool.close();
        assertEquals(0, pool.cache.size());
        for (int i = 0; i < initSize; i++) {
            MutablePoolElement element = tmp.get(i);
            assertFalse(element.cleared);
        }
    }

    @Test
    public void testMaxSize() {
        final WeakObjectPool<MutablePoolElement> pool = new WeakObjectPool<>(MutablePoolElement::new, initSize);
        assertEquals(initSize, pool.cache.size());

        for (int i = 0; i < initSize; i++) {
            MutablePoolElement element = new MutablePoolElement();
            pool.push(element);
            assertTrue(element.cleared);
            assertEquals(initSize + 1 + i, pool.cache.size());
        }

        for (int i = 0; i < 100; i++) {
            MutablePoolElement element = new MutablePoolElement();
            pool.push(element);
            assertFalse(element.cleared);
            assertEquals(2 * initSize, pool.cache.size());
        }
    }

    @Test
    public void testPopPush() {
        final WeakObjectPool<MutablePoolElement> pool = new WeakObjectPool<>(MutablePoolElement::new, initSize);
        assertEquals(initSize, pool.cache.size());

        final List<MutablePoolElement> tmp = new ArrayList<>();
        for (int i = 0; i < initSize; i++) {
            MutablePoolElement element = pool.pop();
            assertFalse(element.cleared);
            tmp.add(element);
        }
        assertEquals(0, pool.cache.size());

        for (int i = 0; i < initSize; i++) {
            MutablePoolElement element = tmp.get(i);
            pool.push(element);
            assertTrue(element.cleared);
        }
        assertEquals(initSize, pool.cache.size());
    }

    private static class MutablePoolElement implements Mutable {
        private boolean cleared = false;

        @Override
        public void clear() {
            cleared = true;
        }
    }
}
