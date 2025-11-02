/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.binary;

import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.std.Long256;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;

/**
 * Implementation of BinaryDataSink using Unsafe for direct memory access.
 */
public class BinaryDataSinkImpl implements BinaryDataSink {
    private long address;
    private int position;
    private int size;

    public BinaryDataSinkImpl() {
    }

    public BinaryDataSinkImpl(long address, int size) {
        of(address, size);
    }

    @Override
    public int available() {
        return size - position;
    }

    @Override
    public int bookmark() {
        return position;
    }

    @Override
    public void of(long address, int size) {
        this.address = address;
        this.size = size;
        this.position = 0;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public void putByte(byte value) {
        if (position + 1 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(1);
        }
        Unsafe.getUnsafe().putByte(address + position, value);
        position += 1;
    }

    @Override
    public int putByteArray(byte[] data, int offset, int length) {
        int available = size - position;
        if (available == 0) {
            throw NoSpaceLeftInResponseBufferException.instance(length);
        }

        int toWrite = Math.min(length, available);
        for (int i = 0; i < toWrite; i++) {
            Unsafe.getUnsafe().putByte(address + position + i, data[offset + i]);
        }
        position += toWrite;
        return toWrite;
    }

    @Override
    public int putBytes(long srcAddress, long length) {
        int available = size - position;
        if (available == 0) {
            throw NoSpaceLeftInResponseBufferException.instance(length);
        }

        int toWrite = (int) Math.min(length, available);
        Vect.memcpy(address + position, srcAddress, toWrite);
        position += toWrite;
        return toWrite;
    }

    @Override
    public void putChar(char value) {
        if (position + 2 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(2);
        }
        Unsafe.getUnsafe().putChar(address + position, value);
        position += 2;
    }

    @Override
    public void putDouble(double value) {
        if (position + 8 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(8);
        }
        Unsafe.getUnsafe().putDouble(address + position, value);
        position += 8;
    }

    @Override
    public void putFloat(float value) {
        if (position + 4 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(4);
        }
        Unsafe.getUnsafe().putFloat(address + position, value);
        position += 4;
    }

    @Override
    public void putInt(int value) {
        if (position + 4 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(4);
        }
        Unsafe.getUnsafe().putInt(address + position, value);
        position += 4;
    }

    @Override
    public void putLong(long value) {
        if (position + 8 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(8);
        }
        Unsafe.getUnsafe().putLong(address + position, value);
        position += 8;
    }

    @Override
    public void putLong128(long lo, long hi) {
        if (position + 16 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(16);
        }
        Unsafe.getUnsafe().putLong(address + position, lo);
        Unsafe.getUnsafe().putLong(address + position + 8, hi);
        position += 16;
    }

    @Override
    public void putLong256(Long256 value) {
        if (position + 32 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(32);
        }
        Unsafe.getUnsafe().putLong(address + position, value.getLong0());
        Unsafe.getUnsafe().putLong(address + position + 8, value.getLong1());
        Unsafe.getUnsafe().putLong(address + position + 16, value.getLong2());
        Unsafe.getUnsafe().putLong(address + position + 24, value.getLong3());
        position += 32;
    }

    @Override
    public void putShort(short value) {
        if (position + 2 > size) {
            throw NoSpaceLeftInResponseBufferException.instance(2);
        }
        Unsafe.getUnsafe().putShort(address + position, value);
        position += 2;
    }

    @Override
    public int putUtf8(Utf8Sequence value, int offset, int length) {
        int available = size - position;
        if (available == 0) {
            throw NoSpaceLeftInResponseBufferException.instance(length);
        }

        int toWrite = Math.min(length, available);
        for (int i = 0; i < toWrite; i++) {
            Unsafe.getUnsafe().putByte(address + position + i, value.byteAt(offset + i));
        }
        position += toWrite;
        return toWrite;
    }

    @Override
    public void rollback(int bookmarkedPosition) {
        this.position = bookmarkedPosition;
    }
}
