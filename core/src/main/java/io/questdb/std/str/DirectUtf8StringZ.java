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

/**
 * An immutable flyweight for a NULL-terminated UTF-8 string stored in native memory.
 * Useful when integrating with C LPSZ type.
 */
public class DirectUtf8StringZ implements LPSZ {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private boolean ascii;
    private long ptr;
    private int size;

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(ptr + index);
    }

    @Override
    public boolean isAscii() {
        return ascii;
    }

    public DirectUtf8StringZ of(long address) {
        this.ptr = address;
        long p = address;
        this.ascii = true;
        byte b;
        while ((b = Unsafe.getUnsafe().getByte(p++)) != 0) {
            this.ascii &= (b >= 0);
        }
        this.size = (int) (p - address - 1);
        return this;
    }

    @Override
    public long ptr() {
        return ptr;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(ptr, ptr + size);
    }
}
