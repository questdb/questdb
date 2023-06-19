/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.std.str;

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

/**
 * An immutable UTF-8 string over owned native memory, originating from a Java String.
 */
public class Utf8String implements Utf8NativeCharSequence {
    private static final long BUFFER_ADDRESS_OFFSET;
    @SuppressWarnings("FieldCanBeLocal")  // We need to hold a reference to `buffer` or it will be GC'd.
    private final ByteBuffer buffer;  // We use a ByteBuffer here, instead of a ptr, to avoid Closeable requirement.
    private final String original;
    private final long ptr;
    private final int size;

    public Utf8String(@NotNull String original) {
        this.original = original;
        final byte[] bytes = original.getBytes(StandardCharsets.UTF_8);
        this.buffer = ByteBuffer.allocateDirect(bytes.length);
        this.buffer.put(bytes);
        this.buffer.rewind();
        this.ptr = Unsafe.getUnsafe().getLong(this.buffer, BUFFER_ADDRESS_OFFSET);
        this.size = bytes.length;
    }

    /**
     * Get 16-bit code unit.
     */
    @Override
    public char charAt(int index) {
        return original.charAt(index);
    }

    @NotNull
    @Override
    public IntStream chars() {
        return original.chars();
    }

    @NotNull
    @Override
    public IntStream codePoints() {
        return original.codePoints();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj == this) {
            return true;
        } else if (obj instanceof String) {
            return original.equals(obj);
        }

        if (obj instanceof Utf8Native) {
            final Utf8Native other = (Utf8Native) obj;
            if (ptr == other.ptr() && size == other.size()) {
                return true;
            }
        }

        if (obj instanceof Utf8Sequence) {
            final Utf8Sequence other = (Utf8Sequence) obj;
            if (size == other.size()) {
                for (int index = 0; index < size; ++index) {
                    if (byteAt(index) != other.byteAt(index)) {
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode() {
        return original.hashCode();
    }

    /**
     * Number of 16-bit code units in the string.
     */
    @Override
    public int length() {
        return original.length();
    }

    @Override
    public long ptr() {
        return ptr;
    }

    @Override
    public int size() {
        return size;
    }

    @NotNull
    @Override
    public CharSequence subSequence(int start, int end) {
        return original.subSequence(start, end);
    }

    /**
     * Get original string.
     */
    @Override
    public String toString() {
        return original;
    }

    static {
        Field addressField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
        } catch (NoSuchFieldException ex) {
            throw new RuntimeException(ex);
        }
        BUFFER_ADDRESS_OFFSET = Unsafe.getUnsafe().objectFieldOffset(addressField);
    }
}
