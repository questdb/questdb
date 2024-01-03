/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.Long256Impl;
import io.questdb.std.Long256Util;

public class CompactMapValue implements MapValue {
    private final long[] columnOffsets;
    private final MemoryARW entries;
    private final Long256Impl long256 = new Long256Impl();
    private boolean _new;
    private long currentValueOffset;
    private CompactMapRecord record;

    CompactMapValue(MemoryARW entries, long[] columnOffsets) {
        this.entries = entries;
        this.columnOffsets = columnOffsets;
    }

    @Override
    public void addByte(int index, byte value) {
        final long o = getValueColumnOffset(index);
        entries.putByte(o, (byte) (entries.getByte(o) + value));
    }

    @Override
    public void addDouble(int index, double value) {
        final long o = getValueColumnOffset(index);
        entries.putDouble(o, entries.getDouble(o) + value);
    }

    @Override
    public void addFloat(int index, float value) {
        final long o = getValueColumnOffset(index);
        entries.putFloat(o, entries.getFloat(o) + value);
    }

    @Override
    public void addInt(int index, int value) {
        final long o = getValueColumnOffset(index);
        entries.putInt(o, entries.getInt(o) + value);
    }

    @Override
    public void addLong(int index, long value) {
        final long o = getValueColumnOffset(index);
        entries.putLong(o, entries.getLong(o) + value);
    }

    @Override
    public void addLong256(int index, Long256 value) {
        Long256 acc = getLong256A(index);
        Long256Util.add(acc, value);
        final long o = getValueColumnOffset(index);
        entries.putLong(o, acc.getLong0());
        entries.putLong(o + Long.BYTES, acc.getLong1());
        entries.putLong(o + 2 * Long.BYTES, acc.getLong2());
        entries.putLong(o + 3 * Long.BYTES, acc.getLong3());
    }

    @Override
    public void addShort(int index, short value) {
        final long o = getValueColumnOffset(index);
        entries.putShort(o, (short) (entries.getShort(o) + value));
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
    public char getChar(int columnIndex) {
        return entries.getChar(getValueColumnOffset(columnIndex));
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
    public byte getGeoByte(int col) {
        return getByte(col);
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
    public short getGeoShort(int col) {
        return getShort(col);
    }

    @Override
    public int getIPv4(int columnIndex) {
        return entries.getIPv4(getValueColumnOffset(columnIndex));
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
    public long getLong128Hi(int columnIndex) {
        return entries.getLong(getValueColumnOffset(columnIndex) + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int columnIndex) {
        return entries.getLong(getValueColumnOffset(columnIndex));
    }

    @Override
    public Long256 getLong256A(int index) {
        final long o = getValueColumnOffset(index);
        long256.setAll(
                entries.getLong(o),
                entries.getLong(o + Long.BYTES),
                entries.getLong(o + 2 * Long.BYTES),
                entries.getLong(o + 3 * Long.BYTES)
        );
        return long256;
    }

    @Override
    public short getShort(int columnIndex) {
        return entries.getShort(getValueColumnOffset(columnIndex));
    }

    @Override
    public long getStartAddress() {
        return currentValueOffset;
    }

    @Override
    public long getTimestamp(int index) {
        return getLong(index);
    }

    @Override
    public boolean isNew() {
        return _new;
    }

    @Override
    public void maxLong(int index, long value) {
        throw new UnsupportedOperationException();
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
    public void putChar(int columnIndex, char value) {
        entries.putChar(getValueColumnOffset(columnIndex), value);
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
    public void putLong128(int columnIndex, long lo, long hi) {
        long valueColumnOffset = getValueColumnOffset(columnIndex);
        entries.putLong(valueColumnOffset, lo);
        entries.putLong(valueColumnOffset + Long.BYTES, hi);
    }

    @Override
    public void putLong256(int index, Long256 value) {
        final long o = getValueColumnOffset(index);
        entries.putLong(o, value.getLong0());
        entries.putLong(o + Long.BYTES, value.getLong1());
        entries.putLong(o + 2 * Long.BYTES, value.getLong2());
        entries.putLong(o + 3 * Long.BYTES, value.getLong3());
    }

    @Override
    public void putShort(int columnIndex, short value) {
        entries.putShort(getValueColumnOffset(columnIndex), value);
    }

    @Override
    public void putTimestamp(int index, long value) {
        putLong(index, value);
    }

    @Override
    public void setMapRecordHere() {
        record.of(currentValueOffset);
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
