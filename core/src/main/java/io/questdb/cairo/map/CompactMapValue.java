/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.map;

import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.Long256;

public class CompactMapValue implements MapValue {

    private final MemoryARW entries;
    private final long[] columnOffsets;
    private long currentValueOffset;
    private boolean _new;
    private CompactMapRecord record;

    CompactMapValue(MemoryARW entries, long[] columnOffsets) {
        this.entries = entries;
        this.columnOffsets = columnOffsets;
    }

    @Override
    public long getAddress() {
        return currentValueOffset;
    }

    @Override
    public boolean getBool(int index) {
        return entries.getBool(getValueColumnOffset(index));
    }

    @Override
    public byte getByte(int index) {
        return entries.getByte(getValueColumnOffset(index));
    }

    @Override
    public long getDate(int index) {
        return getLong(index);
    }

    @Override
    public double getDouble(int columnIndex) {
        return entries.getDouble(getValueColumnOffset(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) {
        return entries.getFloat(getValueColumnOffset(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) {
        return entries.getInt(getValueColumnOffset(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) {
        return entries.getLong(getValueColumnOffset(columnIndex));
    }

    @Override
    public void addLong(int index, long value) {
        final long o = getValueColumnOffset(index);
        entries.putLong(o, entries.getLong(o) + value);
    }

    @Override
    public void addByte(int index, byte value) {
        final long o = getValueColumnOffset(index);
        entries.putByte(o, (byte) (entries.getByte(o) + value));
    }

    @Override
    public void addShort(int index, short value) {
        final long o = getValueColumnOffset(index);
        entries.putShort(o, (short) (entries.getShort(o) + value));
    }

    @Override
    public void addInt(int index, int value) {
        final long o = getValueColumnOffset(index);
        entries.putInt(o, entries.getInt(o) + value);
    }

    @Override
    public void addDouble(int index, double value) {
        final long o = getValueColumnOffset(index);
        entries.putDouble(o, entries.getDouble(o) + value);
    }

    @Override
    public void putStr(int index, CharSequence value) {

    }

    @Override
    public void addFloat(int index, float value) {
        final long o = getValueColumnOffset(index);
        entries.putFloat(o, entries.getFloat(o) + value);
    }

    @Override
    public short getShort(int columnIndex) {
        return entries.getShort(getValueColumnOffset(columnIndex));
    }

    @Override
    public char getChar(int columnIndex) {
        return entries.getChar(getValueColumnOffset(columnIndex));
    }

    @Override
    public CharSequence getStr(int index) {
        return null;
    }

    @Override
    public long getTimestamp(int index) {
        return getLong(index);
    }

    @Override
    public byte getGeoByte(int col) {
        return getByte(col);
    }

    @Override
    public short getGeoShort(int col) {
        return getShort(col);
    }

    @Override
    public int getGeoInt(int col) {
        return getInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        return getLong(col);
    }

    @Override
    public boolean isNew() {
        return _new;
    }

    @Override
    public void putBool(int index, boolean value) {
        entries.putBool(getValueColumnOffset(index), value);
    }

    @Override
    public void putByte(int columnIndex, byte value) {
        entries.putByte(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putDate(int columnIndex, long value) {
        putLong(columnIndex, value);
    }

    @Override
    public void putDouble(int columnIndex, double value) {
        entries.putDouble(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putFloat(int columnIndex, float value) {
        entries.putFloat(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putInt(int columnIndex, int value) {
        entries.putInt(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putLong(int columnIndex, long value) {
        entries.putLong(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putShort(int columnIndex, short value) {
        entries.putShort(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putChar(int columnIndex, char value) {
        entries.putChar(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putTimestamp(int index, long value) {
        putLong(index, value);
    }

    @Override
    public void setMapRecordHere() {
        record.of(currentValueOffset);
    }

    @Override
    public void addLong256(int index, Long256 value) {

    }

    @Override
    public void putLong256(int index, Long256 value) {

    }

    private long getValueColumnOffset(int columnIndex) {
        assert currentValueOffset != -1;
        return currentValueOffset + columnOffsets[columnIndex];
    }

    void linkRecord(CompactMapRecord record) {
        this.record = record;
    }

    void of(long offset, boolean _new) {
        this.currentValueOffset = offset;
        this._new = _new;
    }
}
