/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.concurrent;

import com.nfsdb.misc.Unsafe;

class PLong {
    private static final long INITIAL_VALUE = -1;
    private final static long VALUE_OFFSET = Unsafe.LONG_OFFSET + 4 * Unsafe.LONG_SCALE;
    private final long value[] = new long[7];

    public PLong() {
        this(INITIAL_VALUE);
    }

    private PLong(final long initialValue) {
        Unsafe.getUnsafe().putOrderedLong(value, VALUE_OFFSET, initialValue);
    }

    public boolean cas(final long expected, final long value) {
        return Unsafe.getUnsafe().compareAndSwapLong(this.value, VALUE_OFFSET, expected, value);
    }

    public long fencedGet() {
        return Unsafe.getUnsafe().getLongVolatile(value, VALUE_OFFSET);
    }

    public void fencedSet(final long value) {
        Unsafe.getUnsafe().putLongVolatile(this.value, VALUE_OFFSET, value);
    }

    @Override
    public String toString() {
        return Long.toString(fencedGet());
    }
}
