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

package com.nfsdb.collections;

import com.nfsdb.misc.Unsafe;

public final class LPSZ {
    private long ptr;
    private int len;

    public LPSZ() {
        alloc(128);
    }

    public LPSZ(CharSequence str) {
        int l = str.length();
        alloc(l);
        copy(str, l);
    }

    public long address() {
        return ptr;
    }

    public long of(CharSequence str) {
        int l = str.length();
        if (l >= len) {
            Unsafe.getUnsafe().freeMemory(ptr);
            alloc(l);
        }
        copy(str, l);
        return ptr;
    }

    private void alloc(int len) {
        this.len = len;
        this.ptr = Unsafe.getUnsafe().allocateMemory(len + 1);
    }

    private void copy(CharSequence str, int len) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, (byte) str.charAt(i));
        }
        Unsafe.getUnsafe().putByte(ptr + len, (byte) 0);
    }
}
