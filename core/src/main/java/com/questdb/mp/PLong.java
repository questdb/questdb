/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.misc.Unsafe;

class PLong {
    private static final long INITIAL_VALUE = -1;
    private final static long VALUE_OFFSET = Unsafe.LONG_OFFSET + 4 * Unsafe.LONG_SCALE;
    private final long value[] = new long[7];

    PLong() {
        this(INITIAL_VALUE);
    }

    private PLong(final long initialValue) {
        Unsafe.getUnsafe().putOrderedLong(value, VALUE_OFFSET, initialValue);
    }

    @Override
    public String toString() {
        return Long.toString(fencedGet());
    }

    boolean cas(final long expected, final long value) {
        return Unsafe.getUnsafe().compareAndSwapLong(this.value, VALUE_OFFSET, expected, value);
    }

    long fencedGet() {
        return Unsafe.getUnsafe().getLongVolatile(value, VALUE_OFFSET);
    }

    void fencedSet(final long value) {
        Unsafe.getUnsafe().putLongVolatile(this.value, VALUE_OFFSET, value);
    }
}
