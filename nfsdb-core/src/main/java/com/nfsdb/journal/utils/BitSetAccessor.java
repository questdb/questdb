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

import java.util.BitSet;

public final class BitSetAccessor {
    private static final long wordsOffset;
    private static final long wordsInUseOffset;

    static {
        try {
            wordsOffset = Unsafe.getUnsafe().objectFieldOffset(BitSet.class.getDeclaredField("words"));
            wordsInUseOffset = Unsafe.getUnsafe().objectFieldOffset(BitSet.class.getDeclaredField("wordsInUse"));
        } catch (NoSuchFieldException e) {
            throw new JournalRuntimeException("Incompatible BitSet class", e);
        }
    }

    public static long[] getWords(BitSet instance) {
        return (long[]) Unsafe.getUnsafe().getObject(instance, wordsOffset);
    }

    public static void setWords(BitSet instance, long[] value) {
        Unsafe.getUnsafe().putObject(instance, wordsOffset, value);
    }

    public static void setWordsInUse(BitSet instance, int value) {
        Unsafe.getUnsafe().putInt(instance, wordsInUseOffset, value);
    }
}
