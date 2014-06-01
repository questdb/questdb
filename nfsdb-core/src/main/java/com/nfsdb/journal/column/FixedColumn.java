/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.column;

import java.nio.ByteBuffer;

public class FixedColumn extends AbstractColumn {
    private final int width;

    public FixedColumn(MappedFile mappedFile, int width) {
        super(mappedFile);
        this.width = width;
    }

    public ByteBuffer getBuffer(long localRowID) {
        return getBuffer(getOffset(localRowID), width);
    }

    public boolean getBool(long localRowID) {
        return getBuffer(localRowID).get() == 1;
    }

    public byte getByte(long localRowID) {
        return getBuffer(localRowID).get();
    }

    public double getDouble(long localRowID) {
        return getBuffer(localRowID).getDouble();
    }

    public float getFloat(long localRowID) {
        return getBuffer(localRowID).getFloat();
    }

    public int getInt(long localRowID) {
        return getBuffer(localRowID).getInt();
    }

    public long getLong(long localRowID) {
        return getBuffer(localRowID).getLong();
    }

    public short getShort(long localRowID) {
        return getBuffer(localRowID).getShort();
    }

    public void putBool(boolean value) {
        getBuffer().put((byte) (value ? 1 : 0));
    }

    public void putByte(byte b) {
        getBuffer().put(b);
    }

    public void putDouble(double value) {
        getBuffer().putDouble(value);
    }

    public void putFloat(float value) {
        getBuffer().putFloat(value);
    }

    public long putInt(int value) {
        getBuffer().putInt(value);
        return txAppendOffset / width - 1;
    }

    public void putLong(long value) {
        getBuffer().putLong(value);
    }

    public void putShort(short value) {
        getBuffer().putShort(value);
    }

    public void putNull() {
        getBuffer();
        preCommit(getOffset() + width);
    }

    @Override
    public long getOffset(long localRowID) {
        return localRowID * width;
    }

    @Override
    public long size() {
        return getOffset() / width;
    }

    @Override
    public void truncate(long size) {
        if (size < 0) {
            size = 0;
        }
        preCommit(size * width);
    }

    ByteBuffer getBuffer() {
        long appendOffset = mappedFile.getAppendOffset();
        preCommit(appendOffset + width);
        return mappedFile.getBuffer(appendOffset, width);
    }
}
