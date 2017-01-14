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

import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;

public class FixedColumn extends AbstractColumn {
    private final int width;
    private final int bits;

    public FixedColumn(MemoryFile mappedFile, int width) {
        super(mappedFile);
        this.width = width;
        this.bits = Numbers.msb(width);
    }

    public long bsearchEdge(long val, BSearchType type) {
        return bsearchEdge(val, type, 0, size() - 1);
    }

    public long bsearchEdge(long val, BSearchType type, long lo, long hi) {
        if (hi == -1) {
            return -1;
        }

        long res = bsearchAny(val, type, lo, hi);
        switch (type) {
            case NEWER_OR_SAME:
                while (res > lo) {
                    if (getLong(--res) < val) {
                        return res + 1;
                    }
                }
                break;
            default:
                while (res < hi) {
                    if (getLong(++res) > val) {
                        return res - 1;
                    }
                }
        }
        return res;
    }

    public void copy(Object obj, long offset) {
        Unsafe.getUnsafe().copyMemory(obj, offset, null, getAddress(), width);
    }

    public boolean getBool(long localRowID) {
        return Unsafe.getBool(mappedFile.addressOf(getOffset(localRowID), 1));
    }

    public byte getByte(long localRowID) {
        return Unsafe.getUnsafe().getByte(mappedFile.addressOf(getOffset(localRowID), 1));
    }

    public double getDouble(long localRowID) {
        return Unsafe.getUnsafe().getDouble(mappedFile.addressOf(getOffset(localRowID), 8));
    }

    public float getFloat(long localRowID) {
        return Unsafe.getUnsafe().getFloat(mappedFile.addressOf(getOffset(localRowID), 4));
    }

    public int getInt(long localRowID) {
        return Unsafe.getUnsafe().getInt(mappedFile.addressOf(getOffset(localRowID), 4));
    }

    public long getLong(long localRowID) {
        return Unsafe.getUnsafe().getLong(mappedFile.addressOf(getOffset(localRowID), 8));
    }

    @Override
    public long getOffset(long localRowID) {
        return localRowID << bits;
    }

    @Override
    public long size() {
        return getOffset() >> bits;
    }

    @Override
    public void truncate(long size) {
        preCommit(size <= 0 ? 0 : size << bits);
    }

    public short getShort(long localRowID) {
        return Unsafe.getUnsafe().getShort(mappedFile.addressOf(getOffset(localRowID), 2));
    }

    public void putBool(boolean value) {
        Unsafe.getUnsafe().putByte(getAddress(), (byte) (value ? 1 : 0));
    }

    public void putByte(byte b) {
        Unsafe.getUnsafe().putByte(getAddress(), b);
    }

    public void putDouble(double value) {
        Unsafe.getUnsafe().putDouble(getAddress(), value);
    }

    public void putFloat(float value) {
        Unsafe.getUnsafe().putFloat(getAddress(), value);
    }

    public long putInt(int value) {
        Unsafe.getUnsafe().putInt(getAddress(), value);
        return (txAppendOffset >> bits) - 1;
    }

    public long putLong(long value) {
        Unsafe.getUnsafe().putLong(getAddress(), value);
        return (txAppendOffset >> bits) - 1;
    }

    public void putNull() {
        getAddress();
//        Unsafe.getUnsafe().setMemory(addressOf(), width, (byte) 0);
    }

    public void putShort(short value) {
        Unsafe.getUnsafe().putShort(getAddress(), value);
    }

    private long bsearchAny(long val, BSearchType type, long lo, long hi) {
        long _lo = lo;
        long _hi = hi;
        while (_lo < _hi) {
            long mid = _lo + (_hi - _lo) / 2;
            long res = val - getLong(mid);

            if (res < 0) {
                _hi = mid;
            } else if (res > 0) {
                _lo = mid + 1;
            } else {
                return mid;
            }
        }

        switch (type) {
            case NEWER_OR_SAME:
                return val < getLong(_lo) ? _lo : -2;
            default:
                return val > getLong(_hi) ? _hi : -1;
        }
    }

    private long getAddress() {
        long appendOffset = mappedFile.getAppendOffset();
        preCommit(appendOffset + width);
        return mappedFile.addressOf(appendOffset, width);
    }

}
