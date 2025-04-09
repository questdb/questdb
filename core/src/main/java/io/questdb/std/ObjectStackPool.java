/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Single-threaded object pool based on ObjList. The goal is to optimise intermediate allocation of objects.
 * <p>
 * There are 2 ways to use this pool:
 * <ul>
 *     <li>Mass release: You keep acquiring objects via @link {@link #next()} and then release them all at once via
 *     {@link #clear()}. This is the fastest way to use the pool.</li>
 *     <li>Individual release: You acquire objects via @link {@link #next()} and then release them individually via
 *     {@link #release(Mutable)}. This method has complexity O(n) where n is the number of objects in the pool thus
 *     should be used with care.</li>
 * </ul>
 */
public class ObjectStackPool<T extends Mutable> implements Mutable {
    private static final Log LOG = LogFactory.getLog(ObjectStackPool.class);
    private final ObjectFactory<T> factory;
    private final int initialSize;
    private final ObjStack<T> stack;
    private int outieCount;

    public ObjectStackPool(@NotNull ObjectFactory<T> factory, int size) {
        this.stack = new ObjStack<>(size);
        this.factory = factory;
        this.initialSize = size;
        this.outieCount = 0;
        fill(size);
    }

    @Override
    public void clear() {
        resetCapacity();
    }

    public T next() {
        if (!stack.notEmpty()) {
            expand();
        }
        outieCount++;
        T o = stack.pop();
        o.clear();
        return o;
    }

    public void release(T o) {
        if (o == null) {
            return;
        }
        outieCount--;
        if (outieCount < initialSize) {
            stack.push(o);
        }
    }

    public void resetCapacity() {
        stack.resetCapacity();
    }

    private void expand() {
        fill(outieCount);
        LOG.debug().$("stack pool resize [class=").$(factory.getClass().getName()).$(", outieCount=").$(outieCount).$(']').$();
    }

    private void fill(int count) {
        for (int i = 0; i < count; i++) {
            stack.push(factory.newInstance());
        }
    }
}
