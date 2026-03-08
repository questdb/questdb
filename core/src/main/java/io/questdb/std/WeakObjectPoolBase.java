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

import java.util.ArrayDeque;

/**
 * Weak object pool can be used in multithreaded environment. While
 * weak object pool is a factory of new instances, when object is popped
 * out of pool there is no reference to tie this object back to pool.
 * <p>
 * In multithreaded environment weak pools must be local to thread and
 * objects they create can travel between pools, e.g. one thread gets
 * object from pool and last thread to finish with this object then
 * returns it to its own local pool.
 * <p>
 * In an extreme situation this object migration could keep feeding some
 * pools until we run out of memory. Hence, we limit the maximum number of
 * objects a pool can hold.
 */
abstract class WeakObjectPoolBase<T> {
    // package private for testing
    final ArrayDeque<T> cache = new ArrayDeque<>();
    private final int initSize;
    private final int maxSize;
    int leased = 0;

    public WeakObjectPoolBase(int initSize) {
        this.initSize = initSize;
        this.maxSize = 2 * initSize;
    }

    public T pop() {
        leased++;
        final T obj = cache.poll();
        return obj == null ? newInstance() : obj;
    }

    public boolean push(T obj) {
        leased--;
        assert obj != null;
        if (cache.size() < maxSize) {
            clear(obj);
            cache.push(obj);
            return true;
        } else {
            close(obj);
            return false;
        }
    }

    public int resetLeased() {
        int l = leased;
        leased = 0;
        return l;
    }

    public int size() {
        return cache.size();
    }

    void clear(T obj) {
    }

    void close(T obj) {
    }

    final void fill() {
        for (int i = 0; i < initSize; i++) {
            cache.add(newInstance());
        }
    }

    abstract T newInstance();
}
