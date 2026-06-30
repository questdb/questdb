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
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * One slot of the N=2 live-view in-memory tier.
 * Holds a column-major slab of values as a (data, aux) pair per column, mirroring
 * {@code TableWriter}'s primary/secondary column model: {@link #dataMem} is the
 * always-real primary buffer carrying the payload, and {@link #auxMem} is the
 * optional secondary buffer carrying the per-row offset/header vector a
 * variable-length column needs. A fixed-width / SYMBOL column writes its value
 * into {@code dataMem} at the absolute offset {@code row << shift} and parks its
 * {@code auxMem} slot at the shared {@link NullMemory#INSTANCE} stub (never
 * written, read, or measured), exactly as {@code TableWriter} parks the secondary
 * of a fixed-width column.
 * <p>
 * Which column types the tier actually stores is governed solely by the per-type
 * support gate ({@link #isFixedWidthSupported}); the (data, aux) storage itself
 * imposes no further restriction. Today the gate admits fixed-width / SYMBOL
 * schemas only, so every {@code auxMem} slot is the stub; variable-length
 * (STRING / VARCHAR / BINARY / ARRAY) support layers on by widening that gate and
 * allocating a real aux buffer for those columns.
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

    // Secondary/aux region per column: a real MemoryCARWImpl holding the per-row
    // offset/header vector for a var-size column, the shared NullMemory.INSTANCE
    // stub for a fixed-width / SYMBOL column (whose payload lives wholly in
    // dataMem at an absolute offset, with no aux vector). Typed ObjList<MemoryCARW>
    // so it can hold the stub. Today every entry is the stub - see class javadoc.
    private final ObjList<MemoryCARW> auxMem;
    private final IntList columnTypeSizes;
    private final IntList columnTypes;
    // Primary/data region per column: always a real MemoryCARWImpl. Carries the
    // value at row << shift for a fixed-width / SYMBOL column, or the appended
    // payload bytes for a var-size column.
    private final ObjList<MemoryCARWImpl> dataMem;
    // Per output column, the exclusive upper bound of the lead's new-symbol id band
    // as of this slot's publish - the slot's "symbol horizon". A reader bounds its
    // LiveViewSymbolCache key scan to [committedCount, newSymbolMaxIds[col]) so it
    // only resolves symbols that belong to the slot it pinned, never ids a later
    // refresh cycle is concurrently interning (that unbounded scan would race the
    // cache's backing-array growth - see LiveViewSymbolCache threading note). 0 for
    // non-SYMBOL columns and before the first stamp. The tier stamps it under the
    // writer sentinel just before publish.
    private final int[] newSymbolMaxIds;
    private final int timestampColumnIndex;
    // Number of trailing rows in this slot that are NOT yet on the LV's on-disk
    // tier (the un-flushed lead). Rows [rowCount - leadRowCount, rowCount) are
    // the lead; rows [0, rowCount - leadRowCount) are the overlap (also on disk).
    // A read that serves the lead adds it on top of disk: size() = disk.size() +
    // leadRowCount. When the tier is a strict subset of disk this stays 0. The
    // slow-path eviction never ages out a lead row (it has no durable disk copy).
    private long leadRowCount;
    // LV-table applied seqTxn this slot reflects. The read-path fence serves the
    // slot only when this equals the disk reader's getSeqTxn() (same table
    // version => in-mem agrees with disk). LONG_NULL = not stamped yet.
    private long lvSeqTxn;
    private long rowCount;
    private long seamTs;

    /**
     * @param columnTypes          column-type tags (per {@link ColumnType}); a var-size type
     *                             gets a real aux buffer, a fixed-width / SYMBOL type the stub
     * @param timestampColumnIndex index of the designated timestamp column; used only for
     *                             reporting and routing in the tier above
     * @param pageSize             initial page size for each column buffer; grows on demand
     */
    public LiveViewInMemoryBuffer(IntList columnTypes, int timestampColumnIndex, long pageSize) {
        this.columnTypes = new IntList(columnTypes.size());
        this.columnTypeSizes = new IntList(columnTypes.size());
        this.dataMem = new ObjList<>(columnTypes.size());
        this.auxMem = new ObjList<>(columnTypes.size());
        this.newSymbolMaxIds = new int[columnTypes.size()];
        for (int i = 0, n = columnTypes.size(); i < n; i++) {
            int type = columnTypes.getQuick(i);
            this.columnTypes.add(type);
            // Per-row footprint of the fixed-width primary write (row << shift); 0
            // for a var-size column, whose payload size is not a fixed per-row
            // stride but is tracked by the dataMem / auxMem append cursors instead.
            int sz = ColumnType.isVarSize(type) ? 0 : ColumnType.sizeOf(type);
            this.columnTypeSizes.add(sz);
            this.dataMem.add(new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
            // Var-size columns get a real aux buffer for the offset/header vector;
            // fixed-width / SYMBOL columns park the secondary at the shared stub.
            this.auxMem.add(
                    ColumnType.isVarSize(type)
                            ? new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM)
                            : NullMemory.INSTANCE
            );
        }
        this.timestampColumnIndex = timestampColumnIndex;
        this.rowCount = 0;
        this.seamTs = Numbers.LONG_NULL;
        this.lvSeqTxn = Numbers.LONG_NULL;
        this.leadRowCount = 0;
    }

    @Override
    public void close() {
        Misc.freeObjList(dataMem);
        dataMem.clear();
        // NullMemory.close() is a no-op, so freeing a list that parks fixed-width
        // columns at the shared stub is safe - exactly how TableWriter frees a
        // column list that holds NullMemory.INSTANCE.
        Misc.freeObjList(auxMem);
        auxMem.clear();
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
     * the refresh worker eager-interns the value into the LV table's id space (see
     * {@link LiveViewSymbolCache}) and stores that id, so the read path resolves it
     * against the disk reader's symbol table (committed values) or the tier's
     * symbol cache (values new to the un-flushed lead).
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
     * base WAL-segment-local id) here; the refresh worker immediately overwrites
     * those columns with eager-interned, LV-table-consistent ids (see
     * {@link LiveViewSymbolCache}) before the slot is published, so the read path
     * can resolve them.
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
        for (int i = 0, n = dataMem.size(); i < n; i++) {
            sum += dataMem.getQuick(i).size();
            // Add the aux region only for var-size columns; a fixed-width column's
            // aux is the NullMemory stub, whose size() throws.
            if (ColumnType.isVarSize(columnTypes.getQuick(i))) {
                sum += auxMem.getQuick(i).size();
            }
        }
        return sum;
    }

    public boolean getBool(long row, int col) {
        return dataMem.getQuick(col).getByte(row) != 0;
    }

    public byte getByte(long row, int col) {
        return dataMem.getQuick(col).getByte(row);
    }

    public double getDouble(long row, int col) {
        return dataMem.getQuick(col).getDouble(row << 3);
    }

    public float getFloat(long row, int col) {
        return dataMem.getQuick(col).getFloat(row << 2);
    }

    public int getInt(long row, int col) {
        return dataMem.getQuick(col).getInt(row << 2);
    }

    public long getLong(long row, int col) {
        return dataMem.getQuick(col).getLong(row << 3);
    }

    public short getShort(long row, int col) {
        return dataMem.getQuick(col).getShort(row << 1);
    }

    public long getTimestamp(long row, int col) {
        return getLong(row, col);
    }

    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    public long leadRowCount() {
        return leadRowCount;
    }

    public long lvSeqTxn() {
        return lvSeqTxn;
    }

    /**
     * Exclusive upper bound of the lead's new-symbol id band for {@code col} as of
     * this slot's publish - the slot's symbol horizon. A reader bounds its
     * {@link LiveViewSymbolCache} key scan to {@code [committedCount, horizon)} so
     * it only resolves symbols that belong to this slot, never ids a later refresh
     * cycle is concurrently interning. 0 for non-SYMBOL columns. Stamped under the
     * writer sentinel before publish; see {@link #setNewSymbolMaxId}.
     */
    public int newSymbolMaxId(int col) {
        return newSymbolMaxIds[col];
    }

    public void putBool(long row, int col, boolean value) {
        dataMem.getQuick(col).putByte(row, (byte) (value ? 1 : 0));
    }

    public void putByte(long row, int col, byte value) {
        dataMem.getQuick(col).putByte(row, value);
    }

    public void putDouble(long row, int col, double value) {
        dataMem.getQuick(col).putDouble(row << 3, value);
    }

    public void putFloat(long row, int col, float value) {
        dataMem.getQuick(col).putFloat(row << 2, value);
    }

    public void putInt(long row, int col, int value) {
        dataMem.getQuick(col).putInt(row << 2, value);
    }

    public void putLong(long row, int col, long value) {
        dataMem.getQuick(col).putLong(row << 3, value);
    }

    public void putShort(long row, int col, short value) {
        dataMem.getQuick(col).putShort(row << 1, value);
    }

    public void putTimestamp(long row, int col, long value) {
        putLong(row, col, value);
    }

    /**
     * Resets row count and lead count to zero and clears the seam timestamp and
     * the stamped LV-table seqTxn. Column buffers retain their allocated pages so
     * the next refill reuses memory. For var-size columns the append cursors of
     * both the payload ({@code dataMem}) and the offset/header vector
     * ({@code auxMem}) are rewound to zero so the next refill appends from the
     * start; fixed-width columns need no rewind because they overwrite in place at
     * an absolute offset (and their aux is the {@link NullMemory} stub, whose
     * {@code jumpTo} throws).
     */
    public void reset() {
        rowCount = 0;
        seamTs = Numbers.LONG_NULL;
        lvSeqTxn = Numbers.LONG_NULL;
        leadRowCount = 0;
        for (int c = 0, n = columnTypes.size(); c < n; c++) {
            if (ColumnType.isVarSize(columnTypes.getQuick(c))) {
                dataMem.getQuick(c).jumpTo(0);
                auxMem.getQuick(c).jumpTo(0);
            }
        }
    }

    public long rowCount() {
        return rowCount;
    }

    public long seamTs() {
        return seamTs;
    }

    public void setLeadRowCount(long leadRowCount) {
        this.leadRowCount = leadRowCount;
    }

    public void setLvSeqTxn(long lvSeqTxn) {
        this.lvSeqTxn = lvSeqTxn;
    }

    /**
     * Stamps {@code col}'s symbol horizon - the exclusive upper bound of the lead's
     * new-symbol id band - onto this slot. The tier calls it under the writer
     * sentinel just before the slot becomes reader-visible (publish / sentinel
     * release), so a reader that pins the slot sees a stable, in-bounds horizon via
     * the slot-pin CAS happens-before edge. See {@link #newSymbolMaxId}.
     */
    public void setNewSymbolMaxId(int col, int maxIdExclusive) {
        newSymbolMaxIds[col] = maxIdExclusive;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    // Records the in-mem/disk seam timestamp - the lowest timestamp retained in
    // this slot. The read path consults it to split the scan: the disk cursor
    // serves rows with ts < seamTs and stops at the seam, then the slot serves
    // every row with ts >= seamTs (see LiveViewRecordCursor.hasNext). The slot
    // holds the whole suffix from seamTs up, so the boundary has neither a
    // duplicate nor a gap. A cursor that cannot pass the seqTxn fence falls back
    // to a disk-only scan that ignores the seam and is always correct.
    public void setSeamTs(long seamTs) {
        this.seamTs = seamTs;
    }
}
