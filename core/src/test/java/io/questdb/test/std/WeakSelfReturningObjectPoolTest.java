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

import io.questdb.std.AbstractSelfReturningObject;
import io.questdb.std.WeakSelfReturningObjectPool;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WeakSelfReturningObjectPoolTest {
    private static final int initSize = 12;

    @Test
    public void testPopPush() {
        final int elementCount = 10 * initSize;

        final WeakSelfReturningObjectPool<SelfReturningPoolElement> pool = new WeakSelfReturningObjectPool<>(SelfReturningPoolElement::new, initSize);

        assertEquals(initSize, pool.size());

        final List<SelfReturningPoolElement> tmp = new ArrayList<>();
        for (int i = 0; i < elementCount; i++) {
            SelfReturningPoolElement element = pool.pop();
            assertFalse(element.closed);
            tmp.add(element);
        }
        assertEquals(0, pool.size());

        for (int i = 0; i < elementCount; i++) {
            SelfReturningPoolElement element = tmp.get(i);
            pool.push(element);
            // The pool should not attempt to close objects.
            assertFalse(element.closed);
        }
        assertEquals(2 * initSize, pool.size());
    }

    @Test
    public void testSelfReturnOnClose() {
        final int elementCount = 10 * initSize;

        final List<SelfReturningPoolElement> tmp = new ArrayList<>();
        final WeakSelfReturningObjectPool<SelfReturningPoolElement> pool = new WeakSelfReturningObjectPool<>(SelfReturningPoolElement::new, initSize);

        for (int i = 0; i < elementCount; i++) {
            SelfReturningPoolElement element = pool.pop();
            assertFalse(element.closed);
            tmp.add(element);
        }

        assertEquals(0, pool.size());
        for (int i = 0; i < elementCount; i++) {
            SelfReturningPoolElement element = tmp.get(i);
            element.close();
            assertTrue(element.closed);
        }

        assertEquals(2 * initSize, pool.size());
    }

    private static class SelfReturningPoolElement extends AbstractSelfReturningObject<SelfReturningPoolElement> {
        boolean closed;

        public SelfReturningPoolElement(WeakSelfReturningObjectPool<SelfReturningPoolElement> parentPool) {
            super(parentPool);
        }

        @Override
        public void close() {
            super.close();
            closed = true;
        }
    }
}
