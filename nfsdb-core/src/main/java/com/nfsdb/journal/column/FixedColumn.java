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
        ByteBuffer bb = getBuffer(localRowID);
        return bb.get(bb.position()) == 1;
    }

    public byte getByte(long localRowID) {
        ByteBuffer bb = getBuffer(localRowID);
        return bb.get(bb.position());
    }

    public double getDouble(long localRowID) {
        ByteBuffer bb = getBuffer(localRowID);
        return bb.getDouble(bb.position());
    }

    public float getFloat(long localRowID) {
        ByteBuffer bb = getBuffer(localRowID);
        return bb.getFloat(bb.position());
    }

    public int getInt(long localRowID) {
        ByteBuffer bb = getBuffer(localRowID);
        return bb.getInt(bb.position());
    }

    public long getLong(long localRowID) {
        ByteBuffer bb = getBuffer(localRowID);
        return bb.getLong(bb.position());
    }

    public short getShort(long localRowID) {
        ByteBuffer bb = getBuffer(localRowID);
        return bb.getShort(bb.position());
    }

    public void putBool(boolean value) {
        ByteBuffer bb = getBuffer();
        bb.put(bb.position(), (byte) (value ? 1 : 0));
    }

    public void putByte(byte b) {
        ByteBuffer bb = getBuffer();
        bb.put(bb.position(), b);
    }

    public void putDouble(double value) {
        ByteBuffer bb = getBuffer();
        bb.putDouble(bb.position(), value);
    }

    public void putFloat(float value) {
        ByteBuffer bb = getBuffer();
        bb.putFloat(bb.position(), value);
    }

    public long putInt(int value) {
        ByteBuffer bb = getBuffer();
        bb.putInt(bb.position(), value);
        return txAppendOffset / width - 1;
    }

    public long putLong(long value) {
        ByteBuffer bb = getBuffer();
        bb.putLong(bb.position(), value);
        return txAppendOffset / width - 1;
    }

    public void putShort(short value) {
        ByteBuffer bb = getBuffer();
        bb.putShort(bb.position(), value);
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
