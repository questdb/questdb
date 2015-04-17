/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.utils;

import com.nfsdb.exceptions.JournalRuntimeException;

import java.lang.reflect.Field;

public final class Unsafe {
    private static final sun.misc.Unsafe UNSAFE;
    private static final long OFFSET;
    private static final long SCALE;

    private Unsafe() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T arrayGet(T[] array, int index) {
        return (T) Unsafe.getUnsafe().getObject(array, OFFSET + (index * SCALE));
    }

    public static <T> void arrayPut(T[] array, int index, T obj) {
        Unsafe.getUnsafe().putObject(array, OFFSET + index * SCALE, obj);
    }

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);
            OFFSET = Unsafe.getUnsafe().arrayBaseOffset(Object[].class);
            SCALE = Unsafe.getUnsafe().arrayIndexScale(Object[].class);
        } catch (Exception e) {
            throw new JournalRuntimeException(e);
        }
    }
}
