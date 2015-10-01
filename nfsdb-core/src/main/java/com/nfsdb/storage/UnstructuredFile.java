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

package com.nfsdb.storage;

import com.nfsdb.JournalMode;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Unsafe;

import java.io.File;

public class UnstructuredFile extends MemoryFile {
    private long pos = 0;

    public UnstructuredFile(File file, int bitHint, JournalMode mode) throws JournalException {
        super(file, bitHint, mode);
    }

    public int[] get(int[] container) {
        int len = getInt();
        if (len == -1) {
            return null;
        } else {
            if (container == null || len != container.length) {
                container = new int[len];
            }
            long address = nextAddress(len * 4);
            for (int i = 0; i < len; i++) {
                container[i] = Unsafe.getUnsafe().getInt(address);
                address += 4;
            }
            return container;
        }
    }

    public long[] get(long[] container) {
        int len = getInt();
        if (len == -1) {
            return null;
        } else {
            if (container == null || len != container.length) {
                container = new long[len];
            }
            long address = nextAddress(len * 8);
            for (int i = 0; i < len; i++) {
                container[i] = Unsafe.getUnsafe().getLong(address);
                address += 8;
            }
            return container;
        }
    }

    public byte get() {
        return Unsafe.getUnsafe().getByte(nextAddress(1));
    }

    public boolean getBool() {
        return Unsafe.getBool(nextAddress(1));
    }

    public int getInt() {
        return Unsafe.getUnsafe().getInt(nextAddress(4));
    }

    public long getLong() {
        return Unsafe.getUnsafe().getLong(nextAddress(8));
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public String getStr() {
        int len = Unsafe.getUnsafe().getInt(nextAddress(4));
        if (len == -1) {
            return null;
        } else {
            long address = nextAddress(len * 2);
            char c[] = new char[len];
            for (int i = 0; i < len; i++) {
                c[i] = Unsafe.getUnsafe().getChar(address);
                address += 2;
            }
            return new String(c);
        }
    }

    public void put(CharSequence value) {
        if (value == null) {
            put(-1);
        } else {
            Chars.put(nextAddress(value.length() * 2 + 4), value);
        }
    }

    public void put(int[] values) {
        if (values == null) {
            put(-1);
        } else {
            long address = nextAddress(values.length * 4 + 4);
            Unsafe.getUnsafe().putInt(address, values.length);
            address += 4;
            for (int i = 0; i < values.length; i++) {
                Unsafe.getUnsafe().putInt(address, values[i]);
                address += 4;
            }
        }
    }

    public void put(long[] values) {
        if (values == null) {
            put(-1);
        } else {
            long address = nextAddress(values.length * 8 + 4);
            Unsafe.getUnsafe().putInt(address, values.length);
            address += 4;
            for (int i = 0; i < values.length; i++) {
                Unsafe.getUnsafe().putLong(address, values[i]);
                address += 8;
            }
        }
    }

    public void put(int value) {
        Unsafe.getUnsafe().putInt(nextAddress(4), value);
    }

    public void put(byte value) {
        Unsafe.getUnsafe().putByte(nextAddress(1), value);
    }

    public void put(long value) {
        Unsafe.getUnsafe().putLong(nextAddress(8), value);
    }

    public void put(boolean value) {
        Unsafe.getUnsafe().putByte(nextAddress(1), (byte) (value ? 1 : 0));
    }

    private long nextAddress(int len) {
        long a = addressOf(pos, len);
        pos += len;
        return a;
    }
}
