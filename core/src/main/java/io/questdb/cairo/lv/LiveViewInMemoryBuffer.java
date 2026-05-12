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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * One slot of the N=2 live-view in-memory tier (RFC 123 §"In-memory tier").
 * Holds a column-major slab of fixed-width values; one
 * {@link MemoryCARWImpl} per column, primary buffer only.
 * <p>
 * Variable-length columns (STRING / VARCHAR / BINARY) are not yet supported —
 * Phase 1b currently targets fixed-width output schemas (numeric window
 * functions over a numeric / timestamp / symbol-id row). Calling {@code put*}
 * with a column index typed as a var-length type throws
 * {@link UnsupportedOperationException}; var-length support layers on later
 * when an LV with a var-length output column ships.
 * <p>
 * The slot's {@code rowCount} and {@code seamTs} bookkeeping is owned by the
 * caller: {@link #setRowCount(long)} bumps the row counter once all column
 * writes for a row are done, and {@link #setSeamTs(long)} records the lowest
 * timestamp retained after a copy / append cycle. The buffer itself does not
 * enforce a write order — callers are expected to write all columns for a
 * given row index before bumping {@code rowCount}.
 * <p>
 * All native memory is tagged {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM} so leak
 * accounting and operator-facing memory metrics are clean.
 */
public class LiveViewInMemoryBuffer implements QuietCloseable {

    private final IntList columnTypeSizes;
    private final IntList columnTypes;
    private final ObjList<MemoryCARWImpl> columns;
    private final int timestampColumnIndex;
    private long rowCount;
    private long seamTs;

    /**
     * @param columnTypes          column-type tags (per {@link ColumnType}); fixed-width types only
     * @param timestampColumnIndex index of the designated timestamp column; used only for
     *                             reporting and routing in the tier above
     * @param pageSize             initial page size for each column buffer; grows on demand
     */
    public LiveViewInMemoryBuffer(IntList columnTypes, int timestampColumnIndex, long pageSize) {
        this.columnTypes = new IntList(columnTypes.size());
        this.columnTypeSizes = new IntList(columnTypes.size());
        this.columns = new ObjList<>(columnTypes.size());
        for (int i = 0, n = columnTypes.size(); i < n; i++) {
            int type = columnTypes.getQuick(i);
            this.columnTypes.add(type);
            // Var-size types intentionally not supported in Phase 1b — see class
            // javadoc. Track the per-row footprint so allocation + slice copy stay
            // honest; for var-size the entry is 0 and we will assert at the put
            // site if the caller ever tries to write.
            int sz = ColumnType.isVarSize(type) ? 0 : ColumnType.sizeOf(type);
            this.columnTypeSizes.add(sz);
            this.columns.add(new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
        }
        this.timestampColumnIndex = timestampColumnIndex;
        this.rowCount = 0;
        this.seamTs = Numbers.LONG_NULL;
    }

    @Override
    public void close() {
        Misc.freeObjList(columns);
        columns.clear();
        columnTypes.clear();
        columnTypeSizes.clear();
    }

    public int columnCount() {
        return columnTypes.size();
    }

    public int columnType(int col) {
        return columnTypes.getQuick(col);
    }

    /**
     * Returns the sum of all column buffers' high-water-mark addresses in bytes.
     * Used by {@code live_views().in_mem_bytes} to report the slot's footprint.
     */
    public long footprintBytes() {
        long sum = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            sum += columns.getQuick(i).getAppendOffset();
        }
        return sum;
    }

    public boolean getBool(long row, int col) {
        return columns.getQuick(col).getByte(row) != 0;
    }

    public byte getByte(long row, int col) {
        return columns.getQuick(col).getByte(row);
    }

    public double getDouble(long row, int col) {
        return columns.getQuick(col).getDouble(row << 3);
    }

    public float getFloat(long row, int col) {
        return columns.getQuick(col).getFloat(row << 2);
    }

    public int getInt(long row, int col) {
        return columns.getQuick(col).getInt(row << 2);
    }

    public long getLong(long row, int col) {
        return columns.getQuick(col).getLong(row << 3);
    }

    public short getShort(long row, int col) {
        return columns.getQuick(col).getShort(row << 1);
    }

    public long getTimestamp(long row, int col) {
        return getLong(row, col);
    }

    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    public void putBool(long row, int col, boolean value) {
        columns.getQuick(col).putByte(row, (byte) (value ? 1 : 0));
    }

    public void putByte(long row, int col, byte value) {
        columns.getQuick(col).putByte(row, value);
    }

    public void putDouble(long row, int col, double value) {
        columns.getQuick(col).putDouble(row << 3, value);
    }

    public void putFloat(long row, int col, float value) {
        columns.getQuick(col).putFloat(row << 2, value);
    }

    public void putInt(long row, int col, int value) {
        columns.getQuick(col).putInt(row << 2, value);
    }

    public void putLong(long row, int col, long value) {
        columns.getQuick(col).putLong(row << 3, value);
    }

    public void putShort(long row, int col, short value) {
        columns.getQuick(col).putShort(row << 1, value);
    }

    public void putTimestamp(long row, int col, long value) {
        putLong(row, col, value);
    }

    /**
     * Resets row count to zero and clears seam timestamp. Column buffers retain
     * their allocated pages so the next refill reuses memory.
     */
    public void reset() {
        rowCount = 0;
        seamTs = Numbers.LONG_NULL;
    }

    public long rowCount() {
        return rowCount;
    }

    public long seamTs() {
        return seamTs;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public void setSeamTs(long seamTs) {
        this.seamTs = seamTs;
    }
}
