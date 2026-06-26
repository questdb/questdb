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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * One slot of the N=2 live-view in-memory tier.
 * Holds a column-major slab of fixed-width values; one
 * {@link MemoryCARWImpl} per column, primary buffer only.
 * <p>
 * Variable-length columns (STRING / VARCHAR / BINARY) are not yet supported —
 * the buffer currently targets fixed-width output schemas (numeric window
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
 * Fast-path: the refresh worker calls {@link #copyRowFromRecord(Record, long)}
 * directly into the published slot, then bumps {@link #setRowCount(long)} for
 * each appended row. {@code seamTs} is left unchanged on the fast-path (the
 * minimum retained timestamp does not move when appending at the tail). The
 * slow-path swap path resets the buffer and rewrites both {@code rowCount}
 * and {@code seamTs} from scratch.
 * <p>
 * All native memory is tagged {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM} so leak
 * accounting and operator-facing memory metrics are clean.
 */
public class LiveViewInMemoryBuffer implements QuietCloseable {

    private final IntList columnTypeSizes;
    private final IntList columnTypes;
    private final ObjList<MemoryCARWImpl> columns;
    private final int timestampColumnIndex;
    // LV-table applied seqTxn this slot reflects. The read-path fence serves the
    // slot only when this equals the disk reader's getSeqTxn() (same table
    // version => in-mem agrees with disk). LONG_NULL = not stamped yet.
    private long lvSeqTxn;
    // Highest base seqTxn whose rows the slot reflects. Eviction only ages
    // out rows when this value is covered by the LV's applied_watermark —
    // protects unflushed rows from
    // being dropped before they reach disk. The refresh worker always advances this
    // after a successful apply, so the clamp is vacuous today; a future
    // hand-off ring is the regime where the clamp does real work.
    // LONG_NULL means "no seqTxn information yet" — treated as durable.
    private long maxSeqTxn;
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
            // Var-size types intentionally not supported yet — see class
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
        this.maxSeqTxn = Numbers.LONG_NULL;
        this.lvSeqTxn = Numbers.LONG_NULL;
    }

    @Override
    public void close() {
        Misc.freeObjList(columns);
        columns.clear();
        columnTypes.clear();
        columnTypeSizes.clear();
    }

    /**
     * Returns true iff every column type in {@code columnTypes} is supported by
     * the in-memory tier. The tier ships fixed-width-only — variable-length
     * STRING / VARCHAR / BINARY columns and ARRAY return false. SYMBOL is
     * supported (stored as INT). Used by {@code LiveViewRefreshJob} to decide
     * whether to populate the tier for a given LV; unsupported schemas fall
     * back to disk-only reads.
     */
    public static boolean areColumnTypesSupported(IntList columnTypes) {
        for (int i = 0, n = columnTypes.size(); i < n; i++) {
            int type = ColumnType.tagOf(columnTypes.getQuick(i));
            if (!isFixedWidthSupported(type)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true iff a single column of {@code columnType} is supported by the
     * in-memory tier (fixed-width). Tags the type before the width probe, so
     * callers may pass a full column type. SYMBOL is supported (stored as INT);
     * the refresh worker rewrites the stored ids with LV-table-space ids after
     * apply so the read path resolves them against the disk reader's symbol
     * table.
     */
    public static boolean isColumnTypeSupported(int columnType) {
        return isFixedWidthSupported(ColumnType.tagOf(columnType));
    }

    public int columnCount() {
        return columnTypes.size();
    }

    public int columnType(int col) {
        return columnTypes.getQuick(col);
    }

    private static boolean isFixedWidthSupported(int type) {
        switch (type) {
            case ColumnType.LONG:
            case ColumnType.TIMESTAMP:
            case ColumnType.DATE:
            case ColumnType.GEOLONG:
            case ColumnType.INT:
            case ColumnType.SYMBOL:
            case ColumnType.GEOINT:
            case ColumnType.IPv4:
            case ColumnType.DOUBLE:
            case ColumnType.FLOAT:
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
            case ColumnType.CHAR:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
            case ColumnType.BOOLEAN:
                return true;
            default:
                return false;
        }
    }

    /**
     * Copies one row's fixed-width values, column by column, from {@code src}
     * into this buffer. Caller is responsible for advancing
     * {@link #setRowCount(long)} after the row is written. Throws
     * {@link UnsupportedOperationException} on var-length column types, which
     * the tier does not support — callers should check
     * {@link #areColumnTypesSupported(IntList)} before deciding to use the tier.
     */
    public void copyRowFrom(LiveViewInMemoryBuffer src, long srcRow, long dstRow) {
        for (int c = 0, n = columnTypes.size(); c < n; c++) {
            int type = ColumnType.tagOf(columnTypes.getQuick(c));
            switch (type) {
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                case ColumnType.GEOLONG:
                    putLong(dstRow, c, src.getLong(srcRow, c));
                    break;
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                case ColumnType.GEOINT:
                case ColumnType.IPv4:
                    putInt(dstRow, c, src.getInt(srcRow, c));
                    break;
                case ColumnType.DOUBLE:
                    putDouble(dstRow, c, src.getDouble(srcRow, c));
                    break;
                case ColumnType.FLOAT:
                    putFloat(dstRow, c, src.getFloat(srcRow, c));
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                case ColumnType.CHAR:
                    putShort(dstRow, c, src.getShort(srcRow, c));
                    break;
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                case ColumnType.BOOLEAN:
                    putByte(dstRow, c, src.getByte(srcRow, c));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "live view in-memory tier does not support column type: " + ColumnType.nameOf(columnTypes.getQuick(c))
                    );
            }
        }
    }

    /**
     * Copies one row's fixed-width values from the given {@code record} into
     * this buffer. The {@code metadata} must match {@link #columnTypes} the
     * buffer was constructed with — the caller is responsible for ensuring
     * shape compatibility (this is the staging-buffer path in
     * {@code LiveViewRefreshJob}). SYMBOL columns store the record's raw int (a
     * base WAL-segment-local id) here; the refresh worker rewrites those columns
     * with LV-table-space ids after apply via
     * {@code LiveViewRefreshJob.translateStagingSymbolsToLvSpace} before the slot
     * is published, so the read path can resolve them.
     */
    public void copyRowFromRecord(Record record, long dstRow) {
        for (int c = 0, n = columnTypes.size(); c < n; c++) {
            int type = ColumnType.tagOf(columnTypes.getQuick(c));
            switch (type) {
                case ColumnType.LONG:
                case ColumnType.GEOLONG:
                    putLong(dstRow, c, record.getLong(c));
                    break;
                case ColumnType.TIMESTAMP:
                    putLong(dstRow, c, record.getTimestamp(c));
                    break;
                case ColumnType.DATE:
                    putLong(dstRow, c, record.getDate(c));
                    break;
                case ColumnType.INT:
                case ColumnType.GEOINT:
                case ColumnType.IPv4:
                case ColumnType.SYMBOL:
                    putInt(dstRow, c, record.getInt(c));
                    break;
                case ColumnType.DOUBLE:
                    putDouble(dstRow, c, record.getDouble(c));
                    break;
                case ColumnType.FLOAT:
                    putFloat(dstRow, c, record.getFloat(c));
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    putShort(dstRow, c, record.getShort(c));
                    break;
                case ColumnType.CHAR:
                    putShort(dstRow, c, (short) record.getChar(c));
                    break;
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    putByte(dstRow, c, record.getByte(c));
                    break;
                case ColumnType.BOOLEAN:
                    putBool(dstRow, c, record.getBool(c));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "live view in-memory tier does not support column type: " + ColumnType.nameOf(columnTypes.getQuick(c))
                    );
            }
        }
    }

    /**
     * Returns the sum of all column buffers' allocated sizes in bytes. Reports
     * the slot's native memory footprint for {@code live_views().in_mem_bytes}
     * — i.e. what the operator should see as the LV's RAM cost, not the
     * logical row content size. {@link MemoryCARWImpl} grows by page so the
     * value lands on the next page boundary after each write.
     */
    public long footprintBytes() {
        long sum = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            sum += columns.getQuick(i).size();
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

    public long lvSeqTxn() {
        return lvSeqTxn;
    }

    public long maxSeqTxn() {
        return maxSeqTxn;
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
     * Resets row count to zero, clears seam timestamp and the durability
     * watermark. Column buffers retain their allocated pages so the next refill
     * reuses memory.
     */
    public void reset() {
        rowCount = 0;
        seamTs = Numbers.LONG_NULL;
        maxSeqTxn = Numbers.LONG_NULL;
        lvSeqTxn = Numbers.LONG_NULL;
    }

    public long rowCount() {
        return rowCount;
    }

    public long seamTs() {
        return seamTs;
    }

    public void setLvSeqTxn(long lvSeqTxn) {
        this.lvSeqTxn = lvSeqTxn;
    }

    public void setMaxSeqTxn(long maxSeqTxn) {
        this.maxSeqTxn = maxSeqTxn;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    // Records the in-mem/disk seam timestamp - the lowest timestamp retained in
    // this slot. The Mode B read path consults it to split the scan: the disk
    // cursor serves rows with ts < seamTs and stops at the seam, then the slot
    // serves every row with ts >= seamTs (see LiveViewRecordCursor.hasNext). The
    // slot holds the whole suffix from seamTs up, so the boundary has neither a
    // duplicate nor a gap. In V1 inline-apply the slot is a subset of disk, so a
    // cursor that cannot pass the seqTxn fence falls back to a disk-only scan that
    // ignores the seam and is always correct.
    public void setSeamTs(long seamTs) {
        this.seamTs = seamTs;
    }
}
