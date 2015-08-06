/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections;

import org.jetbrains.annotations.NotNull;

/**
 * Single threaded object pool based on ObjList. The goal is to optimise intermediate allocation of intermediate objects.
 */
public class ObjectPool<T extends Mutable> {

    private final ObjList<T> list;
    private final ObjectPoolFactory<T> factory;
    private int pos = 0;
    private int size = 0;

    public ObjectPool(@NotNull ObjectPoolFactory<T> factory, int size) {
        this.list = new ObjList<>(size);
        this.factory = factory;
        this.size = size;
        fill();
    }

    public T next() {
        if (pos == size) {
            expand();
        }

        T o = list.getQuick(pos++);
        o.clear();
        return o;
    }

    public void reset() {
        pos = 0;
    }

    private void expand() {
        fill();
        size <<= 1;
    }

    private void fill() {
        for (int i = 0; i < size; i++) {
            list.add(factory.newInstance());
        }
    }
}
