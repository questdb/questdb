/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public final class CharSequenceZ extends AbstractCharSequence implements Closeable, LPSZ {
    private long ptr = 0;
    private int capacity;
    private int len;

    public CharSequenceZ(CharSequence str) {
        int l = str.length();
        alloc(l);
        cpyz(str, l);
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.free(ptr, capacity + 1, MemoryTag.NATIVE_DEFAULT);
            ptr = 0;
        }
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(ptr + index);
    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = Unsafe.malloc(capacity + 1, MemoryTag.NATIVE_DEFAULT);
    }

    private void cpyz(CharSequence str, int len) {
        Chars.asciiStrCpy(str, len, ptr);
        Unsafe.getUnsafe().putByte(ptr + len, (byte) 0);
        this.len = len;
    }
}
