/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.mp;

import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.ObjectFactory;

public class RingQueue<T> {
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

    public T get(long cursor) {
        return Unsafe.arrayGet(buf, (int) (cursor & mask));
    }

    public int getCapacity() {
        return buf.length;
    }
}
