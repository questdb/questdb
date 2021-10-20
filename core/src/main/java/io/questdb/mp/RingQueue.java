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

package io.questdb.mp;

import io.questdb.std.Misc;
import io.questdb.std.ObjectFactory;

import java.io.Closeable;

public class RingQueue<T> implements Closeable {
    private final int mask;
    private final T[] buf;

    @SuppressWarnings("unchecked")
    public RingQueue(ObjectFactory<T> factory, int cycle) {
        this.mask = cycle - 1;
        this.buf = (T[]) new Object[cycle];

        for (int i = 0; i < cycle; i++) {
            buf[i] = factory.newInstance();
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = buf.length; i < n; i++) {
            Misc.free(buf[i]);
        }
    }

    public T get(long cursor) {
        return buf[(int) (cursor & mask)];
    }

    public int getCapacity() {
        return buf.length;
    }
}
