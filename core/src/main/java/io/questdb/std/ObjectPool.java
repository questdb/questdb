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
public class ObjectPool<T extends Mutable> implements Mutable {
    private static final Log LOG = LogFactory.getLog(ObjectPool.class);
    private final ObjectFactory<T> factory;
    private final int initialSize;
    private ObjList<T> list;
    private int pos = 0;
    private int size;

    public ObjectPool(@NotNull ObjectFactory<T> factory, int size) {
        this.list = new ObjList<>(size);
        this.factory = factory;
        this.size = size;
        this.initialSize = size;
        fill();
    }

    @Override
    public void clear() {
        pos = 0;
    }

    public int getPos() {
        return pos;
    }

    public T next() {
        if (pos == size) {
            expand();
        }

        T o = list.getQuick(pos++);
        o.clear();
        return o;
    }

    /**
     * Gives access to an object in the pool without incrementing the position.
     * This method does not validate the position, it's only safe to use it if you know that the position is valid
     * (i.e. it's less than the number of objects in the pool).
     *
     * @param pos position of the object in the pool
     * @return object at the specified position
     */
    public T peekQuick(int pos) {
        return list.getQuick(pos);
    }

    /**
     * Return an individual object to the pool.
     * <p>
     * This method has complexity O(n) where n is the number of objects in the pool thus should be used with care.
     * It cannot be used after {@link #resetCapacity()} or {@link #clear()} have been called since they automatically
     * mark all objects as released.
     *
     * @param o object to return to the pool
     */
    public void release(T o) {
        assert pos > 0 : "returnObject called more times than next()";
        pos--;

        int objectPos = pos;
        while (list.getQuick(objectPos) != o) {
            objectPos--;
            if (objectPos < 0) {
                throw new AssertionError("Object not found in pool [object=" + o + ']');
            }
        }
        T objectToSwap = list.getQuick(pos);
        list.setQuick(pos, o);
        list.setQuick(objectPos, objectToSwap);
    }

    public void resetCapacity() {
        this.list = new ObjList<>(initialSize);
        this.size = initialSize;
        fill();
        pos = 0;
    }

    private void expand() {
        fill();
        size <<= 1;
        LOG.debug().$("pool resize [class=").$(factory.getClass().getName()).$(", size=").$(size).$(']').$();
    }

    private void fill() {
        for (int i = 0; i < size; i++) {
            list.add(factory.newInstance());
        }
    }
}
