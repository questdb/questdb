/*+*****************************************************************************
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

import java.nio.charset.StandardCharsets;

/**
 * A garbage-collected immutable UTF-8 string over owned native memory, originating from a Java String.
 *
 * Uses Unsafe.allocateMemory instead of ByteBuffer.allocateDirect to avoid
 * DirectByteBuffer objects in the GraalVM native-image heap. The byte data
 * is kept in a heap array so native memory can be re-allocated lazily after
 * image deserialization (where build-time pointers are invalid).
 */
public class GcUtf8String implements DirectUtf8Sequence {
    private final byte[] data;
    @NotNull
    private final String original;
    private final int size;
    private final long zeroPaddedSixPrefix;
    private volatile long ptr;

    public GcUtf8String(@NotNull String original) {
        // ***** NOTE *****
        // This class causes garbage collection.
        // It should be used with care.
        // It is currently intended to be used for the `dirName` and `tableName` fields
        // of `TableToken` and similar things.
        this.original = original;
        this.data = original.getBytes(StandardCharsets.UTF_8);
        this.size = data.length;
        this.zeroPaddedSixPrefix = computeZeroPaddedSixPrefix();
        // ptr is allocated lazily on first ptr() call. This ensures native-image
        // instances don't carry stale build-time pointers.
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return original;
    }

    @Override
    public byte byteAt(int index) {
        return data[index];
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
        long p = ptr;
        if (p == 0 && size > 0) {
            synchronized (this) {
                p = ptr;
                if (p == 0) {
                    p = allocateNative();
                    ptr = p;
                }
            }
        }
        return p;
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

    private long allocateNative() {
        if (size == 0) {
            return 0;
        }
        final long addr = Unsafe.getUnsafe().allocateMemory(size);
        Unsafe.getUnsafe().copyMemory(data, Unsafe.BYTE_OFFSET, null, addr, size);
        return addr;
    }

    private long computeZeroPaddedSixPrefix() {
        final int limit = Math.min(size, 6);
        long result = 0;
        for (int i = 0; i < limit; i++) {
            result |= (data[i] & 0xffL) << (8 * i);
        }
        return result;
    }
}
