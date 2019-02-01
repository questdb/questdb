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

package com.questdb.mp;

import com.questdb.std.ObjectFactory;
import com.questdb.std.Unsafe;

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
