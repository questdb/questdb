/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.utils;

import com.nfsdb.journal.exceptions.JournalRuntimeException;

import java.lang.reflect.Field;

public final class Unsafe {
    private static final sun.misc.Unsafe UNSAFE;
    private static final long BYTE_ARRAY_OFFSET;
    private static final long CHAR_ARRAY_OFFSET;

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            CHAR_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(char[].class);
        } catch (Exception e) {
            throw new JournalRuntimeException(e);
        }
    }

    private Unsafe() {
    }

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    public static long getByteArrayOffset() {
        return BYTE_ARRAY_OFFSET;
    }

    public static long getCharArrayOffset() {
        return CHAR_ARRAY_OFFSET;
    }
}
