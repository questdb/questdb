/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store;

import com.questdb.std.Chars;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;

import java.io.File;

public class UnstructuredFile extends MemoryFile {
    private long pos = 0;

    public UnstructuredFile(File file, int bitHint, int journalMode) throws JournalException {
        super(file, bitHint, journalMode, false);
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
            Chars.strcpyw(value, nextAddress(value.length() * 2 + 4));
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
