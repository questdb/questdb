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

package io.questdb.std;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Single-threaded object pool based on ObjStack. The goal is to optimise intermediate allocation of objects.
 * <p>
 * This pool acts very much like a stack, may be with some exceptions:
 * - it pre-allocates new objects, so that stack is non-empty to begin with
 * - it can reset its capacity, when capacity is reduced, unreferenced objects will be GCd
 * - technically there is nothing stopping calling code to return objects that have never been part of the pool
 * the client code must be diligent not to return stray objects
 * - returning null is allowed, it would not break the pool
 * - getting objects out of the pool using `next()` when pool is empty will double capacity of the pool
 */
public class ObjectStackPool<T extends Mutable> implements Mutable {
    private static final Log LOG = LogFactory.getLog(ObjectStackPool.class);
    private final ObjectFactory<T> factory;
    private final int initialCapacity;
    private final ObjStack<T> stack;
    private int outieCount;

    public ObjectStackPool(@NotNull ObjectFactory<T> factory, int initialCapacity) {
        this.stack = new ObjStack<>(initialCapacity);
        this.factory = factory;
        // take capacity of the stack, stack will ceil the initial capacity value to the next power of 2
        this.initialCapacity = this.stack.getCapacity();
        this.outieCount = 0;
        fill(this.initialCapacity);
    }

    @Override
    public void clear() {
        stack.clear();
    }

    public int getCapacity() {
        return stack.getCapacity();
    }

    public int getInitialCapacity() {
        return initialCapacity;
    }

    public int getOutieCount() {
        return outieCount;
    }

    public int getSize() {
        return stack.size();
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
        stack.push(o);
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
