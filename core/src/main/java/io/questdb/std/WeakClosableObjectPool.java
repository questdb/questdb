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

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class WeakClosableObjectPool<T> extends WeakObjectPoolBase<T> implements Pool<T>, Closeable {
    private final ObjectFactory<T> factory;

    public WeakClosableObjectPool(@NotNull ObjectFactory<T> factory, int initSize) {
        this(factory, initSize, false);
    }

    public WeakClosableObjectPool(@NotNull ObjectFactory<T> factory, int initSize, boolean lazy) {
        super(initSize);
        this.factory = factory;
        if (!lazy) {
            fill();
        }
    }

    @Override
    public void close() {
        while (cache.size() > 0) {
            Misc.freeIfCloseable(cache.pop());
        }
    }

    @Override
    public boolean push(T obj) {
        return super.push(obj);
    }

    @Override
    void close(T obj) {
        Misc.freeIfCloseable(obj);
    }

    @Override
    T newInstance() {
        return factory.newInstance();
    }
}
