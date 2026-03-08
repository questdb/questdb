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

import io.questdb.mp.ValueHolder;
import org.jetbrains.annotations.NotNull;

public class ValueHolderList<T extends ValueHolder<T>> {
    private final ObjectFactory<T> factory;
    private final ObjList<T> storage;
    private int size;

    public ValueHolderList(ObjectFactory<T> factory, int initialCapacity) {
        this.factory = factory;
        this.storage = new ObjList<>(initialCapacity);
        for (int i = 0; i < initialCapacity; i++) {
            storage.add(factory.newInstance());
        }
    }

    public void clear() {
        size = 0;
    }

    public void commitNextHolder() {
        assert size < storage.size();
        size++;
    }

    /**
     * Copies the data from the ith item of this list to dest, and then clears the item.
     */
    public void moveQuick(int i, @NotNull T dest) {
        assert i >= 0 && i < size;
        T item = storage.getQuick(i);
        item.copyTo(dest);
        item.clear();
    }

    public @NotNull T peekNextHolder() {
        T holder;
        if (size < storage.size()) {
            holder = storage.getQuick(size);
        } else {
            holder = factory.newInstance();
            storage.add(holder);
        }
        return holder;
    }

    public int size() {
        return size;
    }
}
