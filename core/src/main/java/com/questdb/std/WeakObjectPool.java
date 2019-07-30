/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

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
