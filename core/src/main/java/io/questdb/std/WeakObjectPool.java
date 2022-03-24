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

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.ArrayDeque;

/**
 * Weak object pool can be used in multi-threaded environment. While
 * weak object pool is a factory of new instances, when object is popped
 * out of pool there is no reference to tie this object back to pool.
 * <p>
 * In multi-threaded environment weak pools must be local to thread and
 * objects they create can travel between pools, e.g. one thread gets
 * object from pool and last thread to finish with this object then
 * returns it to its own local pool.
 */
public class WeakObjectPool<T extends Mutable> implements Closeable {

    private final ArrayDeque<T> cache;
    private final ObjectFactory<T> factory;
    private final int size;

    public WeakObjectPool(@NotNull ObjectFactory<T> factory, int size) {
        this.cache = new ArrayDeque<>();
        this.factory = factory;
        this.size = size;
        fill();
    }

    public int currentSize() {
        return cache.size();
    }

    @Override
    public void close() {
        while (cache.size() > 0) {
            Misc.free(cache.pop());
        }
    }

    public T pop() {
        final T obj = cache.poll();
        return obj == null ? factory.newInstance() : obj;
    }

    public void push(T obj) {
        assert obj != null;
        obj.clear();
        cache.push(obj);
    }

    private void fill() {
        for (int i = 0; i < size; i++) {
            cache.add(factory.newInstance());
        }
    }
}
