/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.str.CharSink;

public class ObjListList<T> implements Mutable, Sinkable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private ObjList<T>[] buffer;
    private int pos = 0;
    private int cursor;

    @SuppressWarnings("unchecked")
    public ObjListList() {
        this.buffer = (ObjList<T>[]) new ObjList[DEFAULT_ARRAY_SIZE];
    }

    @SuppressWarnings("unchecked")
    public ObjListList(int capacity) {
        this.buffer = (ObjList<T>[]) new ObjList[Math.max(capacity, DEFAULT_ARRAY_SIZE)];
    }

    public void resetCursor() {
        cursor = -1;
    }

    public void advanceCursor() {
        if (++cursor == pos) {
            addList(new ObjList<>());
        }
    }

    public void add(T value) {
        buffer[cursor].add(value);
    }

    private void addList(ObjList<T> list) {
        ensureCapacity(pos + 1);
        buffer[pos++] = list;
    }

    @SuppressWarnings("unchecked")
    private void ensureCapacity(int capacity) {
        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            ObjList<T>[] buf = (ObjList<T>[]) new ObjList[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    public ObjList<T> get(int index) {
        if (index < pos) {
            return buffer[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public ObjList<T> getQuick(int index) {
        return buffer[index];
    }

    public int size() {
        return cursor + 1;
    }

    @Override
    public void clear() {
        for (int i = 0, k = pos; i < k; i++) {
            buffer[i].clear();
        }
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[');
        for (int i = 0, k = pos; i < k; i++) {
            if (i > 0) {
                sink.put(',');
            }
            ObjList<T> list = getQuick(i);
            sink.put(list);
        }
        sink.put(']');
    }
}
