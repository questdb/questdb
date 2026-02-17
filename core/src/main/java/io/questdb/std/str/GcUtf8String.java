/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

/**
 * A garbage-collected immutable UTF-8 string over owned native memory, originating from a Java String.
 */
public class GcUtf8String implements DirectUtf8Sequence {
    private static final long BUFFER_ADDRESS_OFFSET;
    @SuppressWarnings("FieldCanBeLocal")  // We need to hold a reference to `buffer` or it will be GC'd.
    private final ByteBuffer buffer;  // We use a ByteBuffer here, instead of a ptr, to avoid Closeable requirement.
    @NotNull
    private final String original;
    private final long ptr;
    private final int size;
    private final long zeroPaddedSixPrefix;

    public GcUtf8String(@NotNull String original) {
        // ***** NOTE *****
        // This class causes garbage collection.
        // It should be used with care.
        // It is currently intended to be used for the `dirName` and `tableName` fields
        // of `TableToken` and similar things.
        this.original = original;
        final byte[] bytes = original.getBytes(StandardCharsets.UTF_8);
        this.buffer = ByteBuffer.allocateDirect(bytes.length);
        this.buffer.put(bytes);
        this.buffer.rewind();
        this.ptr = Unsafe.getUnsafe().getLong(this.buffer, BUFFER_ADDRESS_OFFSET);
        this.size = bytes.length;
        this.zeroPaddedSixPrefix = Utf8s.zeroPaddedSixPrefix(this);
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return original;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GcUtf8String that = (GcUtf8String) o;
        return original.equals(that.original);
    }

    @Override
    public int hashCode() {
        return original.hashCode();
    }

    @Override
    public boolean isAscii() {
        return original.length() == size;
    }

    @Override
    public long ptr() {
        return ptr;
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * Get original string.
     */
    @Override
    public String toString() {
        return original;
    }

    @Override
    public long zeroPaddedSixPrefix() {
        return zeroPaddedSixPrefix;
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
