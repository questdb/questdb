/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.line.array;

import io.questdb.cairo.ColumnType;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class LongArray extends AbstractArray {

    private static final long DEFAULT_VALUE = 0L;

    private LongArray(int[] shape, boolean initDefault, long defaultValue) {
        super(shape, ColumnType.LONG);
        if (initDefault) {
            // fill default value
            long ptr = array.ptr();
            for (int i = 0, size = array.getFlatViewLength(); i < size; i++) {
                Unsafe.getUnsafe().putLong(ptr, defaultValue);
                ptr += Long.BYTES;
            }
        }
    }

    public static LongArray create(int... shape) {
        return new LongArray(shape, true, DEFAULT_VALUE);
    }

    public static LongArray create(long defaultValue, int... shape) {
        return new LongArray(shape, true, defaultValue);
    }

    public static LongArray create(long[] values) {
        LongArray ndArray = createWithoutDefault(values.length);
        FlattenArrayUtils.copyToBuf(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    public static LongArray create(long[][] values) {
        LongArray ndArray = createWithoutDefault(values.length, values[0].length);
        FlattenArrayUtils.copyToBuf(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    public static LongArray create(long[][][] values) {
        LongArray ndArray = createWithoutDefault(values.length, values[0].length, values[0][0].length);
        FlattenArrayUtils.copyToBuf(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    public static LongArray createWithoutDefault(int... shape) {
        return new LongArray(shape, false, DEFAULT_VALUE);
    }

    public LongArray set(LongArray value, boolean move, int... shape) {
        assert !closed && !value.closed;
        validateSubarrayShape(value, shape);
        int flawLength = toFlatOffset(shape, false);
        Vect.memcpy(array.ptr() + (long) flawLength * Long.BYTES, value.array.ptr(), value.array.size());
        if (move) {
            value.close();
        }
        return this;
    }

    public LongArray set(long value, int... shape) {
        assert !closed;
        array.putLong(toFlatOffset(shape, true), value);
        return this;
    }
}
