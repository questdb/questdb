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

import io.questdb.std.WeakClosableObjectPool;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WeakClosableObjectPoolTest {
    private static final int initSize = 12;

    @Test
    public void testClose() {
        final List<ClosablePoolElement> tmp = new ArrayList<>();
        final WeakClosableObjectPool<ClosablePoolElement> pool = new WeakClosableObjectPool<>(() -> {
            ClosablePoolElement element = new ClosablePoolElement();
            tmp.add(element);
            return element;
        }, initSize);

        for (int i = 0; i < initSize; i++) {
            ClosablePoolElement element = tmp.get(i);
            assertFalse(element.closed);
        }

        pool.close();
        assertEquals(0, pool.size());
        for (int i = 0; i < initSize; i++) {
            ClosablePoolElement element = tmp.get(i);
            assertTrue(element.closed);
        }
    }

    @Test
    public void testMaxSize() {
        try (
                WeakClosableObjectPool<ClosablePoolElement> pool = new WeakClosableObjectPool<>(ClosablePoolElement::new, initSize)
        ) {
            assertEquals(initSize, pool.size());

            for (int i = 0; i < initSize; i++) {
                ClosablePoolElement element = new ClosablePoolElement();
                pool.push(element);
                assertFalse(element.closed);
                assertEquals(initSize + 1 + i, pool.size());
            }

            for (int i = 0; i < 100; i++) {
                ClosablePoolElement element = new ClosablePoolElement();
                pool.push(element);
                assertTrue(element.closed);
                assertEquals(2 * initSize, pool.size());
            }
        }
    }

    @Test
    public void testPopPush() {
        try (
                WeakClosableObjectPool<ClosablePoolElement> pool = new WeakClosableObjectPool<>(ClosablePoolElement::new, initSize)
        ) {
            assertEquals(initSize, pool.size());

            final List<ClosablePoolElement> tmp = new ArrayList<>();
            for (int i = 0; i < initSize; i++) {
                ClosablePoolElement element = pool.pop();
                assertFalse(element.closed);
                tmp.add(element);
            }
            assertEquals(0, pool.size());

            for (int i = 0; i < initSize; i++) {
                ClosablePoolElement element = tmp.get(i);
                pool.push(element);
                assertFalse(element.closed);
            }
            assertEquals(initSize, pool.size());
        }
    }

    private static class ClosablePoolElement implements Closeable {
        private boolean closed = false;

        @Override
        public void close() {
            closed = true;
        }
    }
}
