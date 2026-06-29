/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.lv;

import io.questdb.cairo.sql.Record;
import io.questdb.std.ObjList;

/**
 * A flyweight {@link Record} view over one row of a {@link LiveViewInMemoryBuffer}.
 * <p>
 * The flush path uses it to feed the compiled {@link io.questdb.griffin.RecordToRowCopier}
 * the same way the refresh path feeds it a window-cursor record: the copier copies
 * the un-flushed lead rows out of the in-mem tier into the LV's {@code WalWriter}
 * row, so the byte-level serialisation matches the inline-apply write path exactly.
 * Only the fixed-width column accessors the tier stores are overridden; all other
 * {@link Record} methods inherit the throwing defaults, which the copier never
 * reaches because the tier rejects unsupported column types up front
 * (see {@link LiveViewInMemoryBuffer#areColumnTypesSupported}).
 * <p>
 * Single-threaded, reused: {@link #of(LiveViewInMemoryBuffer, long)} rebinds the
 * buffer + row before each copy. Not thread-safe.
 */
public class LiveViewBufferRecord implements Record {
    private LiveViewInMemoryBuffer buffer;
    private long row;
    // Per-column symbol resolvers, set by the flush so getSymA/getSymB turn a
    // stored LV-table-consistent symbol id back into its string before the copier
    // re-interns it into the WAL. Null (or a null per-column entry) for non-SYMBOL
    // columns; the copier only calls getSymA on a SYMBOL source column.
    private ObjList<LiveViewSymbolTable> symbolResolvers;

    @Override
    public boolean getBool(int col) {
        return buffer.getBool(row, col);
    }

    @Override
    public byte getByte(int col) {
        return buffer.getByte(row, col);
    }

    @Override
    public char getChar(int col) {
        return (char) buffer.getShort(row, col);
    }

    @Override
    public long getDate(int col) {
        return buffer.getLong(row, col);
    }

    @Override
    public double getDouble(int col) {
        return buffer.getDouble(row, col);
    }

    @Override
    public float getFloat(int col) {
        return buffer.getFloat(row, col);
    }

    @Override
    public byte getGeoByte(int col) {
        return buffer.getByte(row, col);
    }

    @Override
    public int getGeoInt(int col) {
        return buffer.getInt(row, col);
    }

    @Override
    public long getGeoLong(int col) {
        return buffer.getLong(row, col);
    }

    @Override
    public short getGeoShort(int col) {
        return buffer.getShort(row, col);
    }

    @Override
    public int getIPv4(int col) {
        return buffer.getInt(row, col);
    }

    @Override
    public int getInt(int col) {
        return buffer.getInt(row, col);
    }

    @Override
    public long getLong(int col) {
        return buffer.getLong(row, col);
    }

    @Override
    public short getShort(int col) {
        return buffer.getShort(row, col);
    }

    @Override
    public CharSequence getSymA(int col) {
        final LiveViewSymbolTable resolver = symbolResolvers != null ? symbolResolvers.getQuick(col) : null;
        return resolver != null ? resolver.valueOf(buffer.getInt(row, col)) : null;
    }

    @Override
    public CharSequence getSymB(int col) {
        final LiveViewSymbolTable resolver = symbolResolvers != null ? symbolResolvers.getQuick(col) : null;
        return resolver != null ? resolver.valueBOf(buffer.getInt(row, col)) : null;
    }

    @Override
    public long getTimestamp(int col) {
        return buffer.getLong(row, col);
    }

    public void of(LiveViewInMemoryBuffer buffer, long row) {
        this.buffer = buffer;
        this.row = row;
    }

    public void setSymbolResolvers(ObjList<LiveViewSymbolTable> symbolResolvers) {
        this.symbolResolvers = symbolResolvers;
    }
}
