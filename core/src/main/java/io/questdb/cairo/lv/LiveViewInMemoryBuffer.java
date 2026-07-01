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
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

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
 * support gate ({@link #isTierSupported}); the (data, aux) storage itself imposes
 * no further restriction. The gate admits fixed-width / SYMBOL columns (whose
 * {@code auxMem} slot is the stub) and every variable-length type - STRING, BINARY,
 * VARCHAR and ARRAY (whose {@code auxMem} buffer holds the per-row offset/header
 * vector that encoding needs - an 8-byte start offset into the appended
 * {@code dataMem} payload for STRING / BINARY, the 16-byte driver-owned header for
 * VARCHAR and ARRAY). STRING / BINARY / VARCHAR reuse the data region's own reusable
 * flyweights on read; ARRAY binds a per-column {@link BorrowedArray} over the
 * {@code (auxMem, dataMem)} pair, exactly as {@code PageFrameMemoryRecord} does over
 * its page addresses.
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

    // Per-column ARRAY read flyweight, lazily allocated for ARRAY columns (null for
    // every other type). BorrowedArray.of binds a view over the column's
    // (auxMem, dataMem) pair without copying, exactly like
    // PageFrameMemoryRecord.arrayBuffers binds over its page addresses; holding one
    // per column lets a caller keep an ArrayView from one column live across a
    // getArray on another. The flyweight owns no native memory of its own.
    private final ObjList<BorrowedArray> arrayBuffers;
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
    // Write-path scratch sinks for the wide decimals, lazily allocated on the first
    // DECIMAL128 / DECIMAL256 row the writer copies (null for a buffer with no such
    // column). Only ever touched by the single refresh-worker writer in
    // copyRowFromRecord / copyRowFrom; the read getters fill a caller-supplied sink,
    // so no reader ever races these.
    private Decimal128 decimal128;
    private Decimal256 decimal256;
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
        // Lazily populated per ARRAY column on first read (borrowedArray); other
        // entries stay null. Sized up front to avoid a grow on first bind.
        this.arrayBuffers = new ObjList<>(columnTypes.size());
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
        // BorrowedArray holds no native memory (it is a flyweight over dataMem /
        // auxMem); close() no-ops, so freeing the list only drops references, as
        // PageFrameMemoryRecord does for its arrayBuffers.
        Misc.freeObjList(arrayBuffers);
        arrayBuffers.clear();
        columnTypes.clear();
        columnTypeSizes.clear();
    }

    /**
     * Returns true iff every column type in {@code columnTypes} is supported by
     * the in-memory tier. Fixed-width types (including LONG256, LONG128, UUID and
     * every DECIMAL width), SYMBOL (stored as INT), and every variable-length type -
     * STRING, BINARY, VARCHAR and ARRAY - are supported. Used by
     * {@code LiveViewRefreshJob} to decide whether to populate the tier for a given
     * LV; unsupported schemas (e.g. INTERVAL, a non-persisted type an LV cannot
     * project) fall back to disk-only reads.
     */
    public static boolean areColumnTypesSupported(IntList columnTypes) {
        for (int i = 0, n = columnTypes.size(); i < n; i++) {
            int type = ColumnType.tagOf(columnTypes.getQuick(i));
            if (!isTierSupported(type)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true iff a single column of {@code columnType} is supported by the
     * in-memory tier. Tags the type before the probe, so callers may pass a full
     * column type. Fixed-width types are stored in place at {@code row << shift}
     * (LONG256 / DECIMAL256 at {@code row << 5}, LONG128 / UUID / DECIMAL128 at
     * {@code row << 4}, the narrower DECIMAL8 / DECIMAL16 / DECIMAL32 / DECIMAL64 at
     * their natural byte stride, each with the aux region parked at the
     * {@link NullMemory} stub); SYMBOL is stored as INT,
     * with the refresh worker eager-interning the value into the LV table's id space
     * (see {@link LiveViewSymbolCache}) so the read path resolves it against the disk
     * reader's symbol table (committed values) or the tier's symbol cache (values new
     * to the un-flushed lead); STRING and
     * BINARY are stored as an appended payload plus a per-row start-offset vector;
     * VARCHAR is stored via {@link VarcharTypeDriver} (appended payload plus the
     * driver's 16-byte-per-row aux header); ARRAY is stored via
     * {@link ArrayTypeDriver} (appended payload plus the driver's 16-byte-per-row
     * aux header) and read back through a per-column {@link BorrowedArray}.
     */
    public static boolean isColumnTypeSupported(int columnType) {
        return isTierSupported(ColumnType.tagOf(columnType));
    }

    public int columnCount() {
        return columnTypes.size();
    }

    public int columnType(int col) {
        return columnTypes.getQuick(col);
    }

    // Appends one ARRAY value as the next row of column col via ArrayTypeDriver:
    // the driver writes its 16-byte-per-row aux header (CRC + offset + size) into
    // auxMem and the shape + element payload into dataMem. appendValue handles a
    // null array (a zero-size aux entry that BorrowedArray.of decodes back to a null
    // ArrayView). The order assert catches a caller that writes rows out of order:
    // the driver appends, so the aux cursor must sit at dstRow * ARRAY_AUX_WIDTH_BYTES
    // before this row's header is appended.
    void appendArray(int col, long dstRow, ArrayView value) {
        final MemoryCARWImpl data = dataMem.getQuick(col);
        final MemoryCARW aux = auxMem.getQuick(col);
        assert (dstRow * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES) == aux.getAppendOffset();
        ArrayTypeDriver.appendValue(aux, data, value);
    }

    // Appends one BINARY value as the next row of column col: the payload bytes go
    // to the tail of the dataMem region and the payload's start offset is appended
    // as the next 8-byte aux entry. putBin handles null (a negative length marker)
    // and distinguishes it from a real, empty (len == 0) value. The order assert
    // catches a caller that writes rows out of order: aux is append-only, so its
    // cursor must sit at dstRow * 8 before this row's offset is appended.
    void appendBin(int col, long dstRow, BinarySequence value) {
        final MemoryCARWImpl data = dataMem.getQuick(col);
        final MemoryCARW aux = auxMem.getQuick(col);
        assert (dstRow << 3) == aux.getAppendOffset();
        final long off = data.getAppendOffset();
        data.putBin(value);
        aux.putLong(off);
    }

    // Appends one STRING value as the next row of column col: the 4-byte length
    // prefix + UTF-16 payload goes to the tail of the dataMem region and the
    // payload's start offset is appended as the next 8-byte aux entry. putStr
    // stores a null marker for a null value that getStrA reports back as null. The
    // order assert catches a caller that writes rows out of order: aux is
    // append-only, so its cursor must sit at dstRow * 8 before this row's offset is
    // appended.
    void appendStr(int col, long dstRow, CharSequence value) {
        final MemoryCARWImpl data = dataMem.getQuick(col);
        final MemoryCARW aux = auxMem.getQuick(col);
        assert (dstRow << 3) == aux.getAppendOffset();
        final long off = data.getAppendOffset();
        data.putStr(value);
        aux.putLong(off);
    }

    // Appends one VARCHAR value as the next row of column col via VarcharTypeDriver:
    // the driver writes its 16-byte-per-row aux header (inlined prefix + flags +
    // offset, carrying the ascii flag) into auxMem and any non-inlined payload into
    // dataMem. appendValue handles null and a fully-inlined value (no payload bytes).
    // The order assert catches a caller that writes rows out of order: the driver
    // appends, so the aux cursor must sit at dstRow * VARCHAR_AUX_WIDTH_BYTES before
    // this row's header is appended.
    void appendVarchar(int col, long dstRow, Utf8Sequence value) {
        final MemoryCARWImpl data = dataMem.getQuick(col);
        final MemoryCARW aux = auxMem.getQuick(col);
        assert (dstRow * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES) == aux.getAppendOffset();
        VarcharTypeDriver.appendValue(aux, data, value);
    }

    // Lazily allocates and caches the per-column ARRAY read flyweight, mirroring
    // PageFrameMemoryRecord.borrowedArray. Only ever called for ARRAY columns.
    private BorrowedArray borrowedArray(int col) {
        BorrowedArray ba = arrayBuffers.getQuiet(col);
        if (ba == null) {
            arrayBuffers.extendAndSet(col, ba = new BorrowedArray());
        }
        return ba;
    }

    // Lazily allocates the writer's DECIMAL128 scratch sink (see the field javadoc).
    private Decimal128 decimal128() {
        if (decimal128 == null) {
            decimal128 = new Decimal128();
        }
        return decimal128;
    }

    // Lazily allocates the writer's DECIMAL256 scratch sink (see the field javadoc).
    private Decimal256 decimal256() {
        if (decimal256 == null) {
            decimal256 = new Decimal256();
        }
        return decimal256;
    }

    private static boolean isTierSupported(int type) {
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
            case ColumnType.LONG256:
            case ColumnType.LONG128:
            case ColumnType.UUID:
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
            case ColumnType.DECIMAL128:
            case ColumnType.DECIMAL256:
            case ColumnType.STRING:
            case ColumnType.BINARY:
            case ColumnType.VARCHAR:
            case ColumnType.ARRAY:
                return true;
            default:
                return false;
        }
    }

    /**
     * Copies one row's values, column by column, from {@code src} into this
     * buffer. Fixed-width / SYMBOL columns overwrite in place at the absolute
     * {@code dstRow} offset; STRING / BINARY / VARCHAR / ARRAY columns re-append the
     * value read from {@code src}'s buffer getter (the value bytes are copied into
     * this buffer's {@code dataMem} / {@code auxMem} before {@code src}'s flyweight is
     * reused), so {@code dstRow} must be the next dense append row. Caller is responsible
     * for advancing {@link #setRowCount(long)} after the row is written. Throws
     * {@link UnsupportedOperationException} on column types the tier does not store
     * — callers should check {@link #areColumnTypesSupported(IntList)} before
     * deciding to use the tier.
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
                case ColumnType.LONG256:
                    putLong256(dstRow, c, src.getLong256A(srcRow, c));
                    break;
                case ColumnType.LONG128:
                case ColumnType.UUID:
                    putLong128(dstRow, c, src.getLong128Lo(srcRow, c), src.getLong128Hi(srcRow, c));
                    break;
                case ColumnType.DECIMAL8:
                    putByte(dstRow, c, src.getDecimal8(srcRow, c));
                    break;
                case ColumnType.DECIMAL16:
                    putShort(dstRow, c, src.getDecimal16(srcRow, c));
                    break;
                case ColumnType.DECIMAL32:
                    putInt(dstRow, c, src.getDecimal32(srcRow, c));
                    break;
                case ColumnType.DECIMAL64:
                    putLong(dstRow, c, src.getDecimal64(srcRow, c));
                    break;
                case ColumnType.DECIMAL128: {
                    final Decimal128 d = decimal128();
                    src.getDecimal128(srcRow, c, d);
                    putDecimal128(dstRow, c, d);
                    break;
                }
                case ColumnType.DECIMAL256: {
                    final Decimal256 d = decimal256();
                    src.getDecimal256(srcRow, c, d);
                    putDecimal256(dstRow, c, d);
                    break;
                }
                case ColumnType.STRING:
                    appendStr(c, dstRow, src.getStrA(srcRow, c));
                    break;
                case ColumnType.BINARY:
                    appendBin(c, dstRow, src.getBin(srcRow, c));
                    break;
                case ColumnType.VARCHAR:
                    appendVarchar(c, dstRow, src.getVarcharA(srcRow, c));
                    break;
                case ColumnType.ARRAY:
                    appendArray(c, dstRow, src.getArray(srcRow, c));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "live view in-memory tier does not support column type: " + ColumnType.nameOf(columnTypes.getQuick(c))
                    );
            }
        }
    }

    /**
     * Copies one row's values from the given {@code record} into this buffer. The
     * record's column types must match {@link #columnTypes} the buffer was
     * constructed with — the caller is responsible for ensuring shape
     * compatibility (this is the staging-buffer path in
     * {@code LiveViewRefreshJob}). SYMBOL columns store the record's raw int (a
     * base WAL-segment-local id) here; the refresh worker immediately overwrites
     * those columns with eager-interned, LV-table-consistent ids (see
     * {@link LiveViewSymbolCache}) before the slot is published, so the read path
     * can resolve them. STRING / BINARY columns append the value's payload to
     * {@code dataMem} and its start offset to {@code auxMem}; VARCHAR appends via
     * {@link VarcharTypeDriver} and ARRAY via {@link ArrayTypeDriver} (payload to
     * {@code dataMem}, 16-byte header to {@code auxMem}). Either way {@code dstRow}
     * must be the next dense append row.
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
                case ColumnType.LONG256:
                    putLong256(dstRow, c, record.getLong256A(c));
                    break;
                case ColumnType.LONG128:
                case ColumnType.UUID:
                    putLong128(dstRow, c, record.getLong128Lo(c), record.getLong128Hi(c));
                    break;
                case ColumnType.DECIMAL8:
                    putByte(dstRow, c, record.getDecimal8(c));
                    break;
                case ColumnType.DECIMAL16:
                    putShort(dstRow, c, record.getDecimal16(c));
                    break;
                case ColumnType.DECIMAL32:
                    putInt(dstRow, c, record.getDecimal32(c));
                    break;
                case ColumnType.DECIMAL64:
                    putLong(dstRow, c, record.getDecimal64(c));
                    break;
                case ColumnType.DECIMAL128: {
                    final Decimal128 d = decimal128();
                    record.getDecimal128(c, d);
                    putDecimal128(dstRow, c, d);
                    break;
                }
                case ColumnType.DECIMAL256: {
                    final Decimal256 d = decimal256();
                    record.getDecimal256(c, d);
                    putDecimal256(dstRow, c, d);
                    break;
                }
                case ColumnType.STRING:
                    appendStr(c, dstRow, record.getStrA(c));
                    break;
                case ColumnType.BINARY:
                    appendBin(c, dstRow, record.getBin(c));
                    break;
                case ColumnType.VARCHAR:
                    appendVarchar(c, dstRow, record.getVarcharA(c));
                    break;
                case ColumnType.ARRAY:
                    // Pass the full column type (carries dimensionality + element
                    // type) so the record decodes the array shape correctly.
                    appendArray(c, dstRow, record.getArray(c, columnTypes.getQuick(c)));
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

    // Resolves an ARRAY value: binds the column's per-column BorrowedArray over the
    // (auxMem, dataMem) pair, exactly as PageFrameMemoryRecord.getArray binds over
    // its page addresses. BorrowedArray.of decodes the per-row 16-byte aux entry
    // (CRC + offset + size) and the dimensionality / element type from the column
    // type, then points the flat view at the shape + payload in dataMem. A row the
    // driver wrote as null (zero-size aux entry) decodes back to a null ArrayView.
    // The view is reused per column, so two columns never clobber each other's array.
    public ArrayView getArray(long row, int col) {
        final BorrowedArray ba = borrowedArray(col);
        final MemoryCARW aux = auxMem.getQuick(col);
        final MemoryCARWImpl data = dataMem.getQuick(col);
        return ba.of(
                columnTypes.getQuick(col),
                aux.addressOf(0),
                aux.addressHi(),
                data.addressOf(0),
                data.addressHi(),
                row
        );
    }

    // Resolves a BINARY value: the per-row start offset lives in auxMem at
    // row << 3, the payload (8-byte length + bytes) in dataMem at that offset.
    // Returns null for a null marker; a real, empty (len == 0) value is distinct.
    // The returned BinarySequence is the dataMem column's own reusable bsview.
    public BinarySequence getBin(long row, int col) {
        return dataMem.getQuick(col).getBin(auxMem.getQuick(col).getLong(row << 3));
    }

    public long getBinLen(long row, int col) {
        return dataMem.getQuick(col).getBinLen(auxMem.getQuick(col).getLong(row << 3));
    }

    public boolean getBool(long row, int col) {
        return dataMem.getQuick(col).getByte(row) != 0;
    }

    public byte getByte(long row, int col) {
        return dataMem.getQuick(col).getByte(row);
    }

    // Resolves the wide DECIMAL128 value (16 bytes at the absolute offset row << 4,
    // high 64 bits first, low at + 8) into the caller-supplied sink, mirroring the
    // on-disk fixed-width layout. A stored null decodes back to the sink's null.
    public void getDecimal128(long row, int col, Decimal128 sink) {
        dataMem.getQuick(col).getDecimal128(row << 4, sink);
    }

    // Resolves a DECIMAL16 value (2 bytes at the absolute offset row << 1). A stored
    // null rides back as the DECIMAL16 null sentinel.
    public short getDecimal16(long row, int col) {
        return dataMem.getQuick(col).getDecimal16(row << 1);
    }

    // Resolves the wide DECIMAL256 value (32 bytes at the absolute offset row << 5)
    // into the caller-supplied sink, mirroring the on-disk fixed-width layout. A
    // stored null decodes back to the sink's null.
    public void getDecimal256(long row, int col, Decimal256 sink) {
        dataMem.getQuick(col).getDecimal256(row << 5, sink);
    }

    // Resolves a DECIMAL32 value (4 bytes at the absolute offset row << 2). A stored
    // null rides back as the DECIMAL32 null sentinel.
    public int getDecimal32(long row, int col) {
        return dataMem.getQuick(col).getDecimal32(row << 2);
    }

    // Resolves a DECIMAL64 value (8 bytes at the absolute offset row << 3). A stored
    // null rides back as the DECIMAL64 null sentinel.
    public long getDecimal64(long row, int col) {
        return dataMem.getQuick(col).getDecimal64(row << 3);
    }

    // Resolves a DECIMAL8 value (1 byte at the absolute offset row). A stored null
    // rides back as the DECIMAL8 null sentinel.
    public byte getDecimal8(long row, int col) {
        return dataMem.getQuick(col).getDecimal8(row);
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

    // Resolves the high 64 bits of a LONG128 / UUID value: stored as two longs at
    // the absolute offset row << 4 (lo first, hi at + 8), mirroring the on-disk
    // 16-byte fixed-width layout PageFrameMemoryRecord.getLong128Hi reads.
    public long getLong128Hi(long row, int col) {
        return dataMem.getQuick(col).getLong((row << 4) + Long.BYTES);
    }

    // Resolves the low 64 bits of a LONG128 / UUID value (see getLong128Hi).
    public long getLong128Lo(long row, int col) {
        return dataMem.getQuick(col).getLong(row << 4);
    }

    // Renders a LONG256 value (4 longs at the absolute offset row << 5) into the
    // given sink, mirroring the on-disk 32-byte fixed-width layout.
    public void getLong256(long row, int col, CharSink<?> sink) {
        dataMem.getQuick(col).getLong256(row << 5, sink);
    }

    // Resolves a LONG256 value (4 longs at the absolute offset row << 5). getLong256A
    // / getLong256B return the dataMem column's own distinct reusable flyweights
    // (long256A / long256B), so a caller may hold both at once for an A/B comparison.
    public Long256 getLong256A(long row, int col) {
        return dataMem.getQuick(col).getLong256A(row << 5);
    }

    public Long256 getLong256B(long row, int col) {
        return dataMem.getQuick(col).getLong256B(row << 5);
    }

    public short getShort(long row, int col) {
        return dataMem.getQuick(col).getShort(row << 1);
    }

    // Resolves a STRING value: the per-row start offset lives in auxMem at
    // row << 3, the payload (4-byte length + UTF-16) in dataMem at that offset.
    // Returns null for a null marker. getStrA / getStrB return the dataMem column's
    // own distinct reusable views (csviewA / csviewB), so a caller may hold both.
    public CharSequence getStrA(long row, int col) {
        return dataMem.getQuick(col).getStrA(auxMem.getQuick(col).getLong(row << 3));
    }

    public CharSequence getStrB(long row, int col) {
        return dataMem.getQuick(col).getStrB(auxMem.getQuick(col).getLong(row << 3));
    }

    public int getStrLen(long row, int col) {
        return dataMem.getQuick(col).getStrLen(auxMem.getQuick(col).getLong(row << 3));
    }

    public long getTimestamp(long row, int col) {
        return getLong(row, col);
    }

    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    // Resolves a VARCHAR value via VarcharTypeDriver: the per-row 16-byte header
    // lives in auxMem at row * VARCHAR_AUX_WIDTH_BYTES, the non-inlined payload (if
    // any) in dataMem. Returns null for a null marker; getSplitValue handles the
    // inlined / split cases and the ascii flag. getVarcharA / getVarcharB return the
    // aux memory's own distinct reusable views (utf8SplitViewA / utf8SplitViewB), so
    // a caller may hold both.
    public Utf8Sequence getVarcharA(long row, int col) {
        return VarcharTypeDriver.getSplitValue(auxMem.getQuick(col), dataMem.getQuick(col), row, 1);
    }

    public Utf8Sequence getVarcharB(long row, int col) {
        return VarcharTypeDriver.getSplitValue(auxMem.getQuick(col), dataMem.getQuick(col), row, 2);
    }

    public int getVarcharSize(long row, int col) {
        return VarcharTypeDriver.getValueSize(auxMem.getQuick(col), row);
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

    // Stores a DECIMAL128 value (high 64 bits + low 64 bits) at the absolute offset
    // row << 4, the 16-byte fixed-width layout getDecimal128 reads back. The aux
    // region stays the NullMemory stub.
    public void putDecimal128(long row, int col, Decimal128 value) {
        dataMem.getQuick(col).putDecimal128(row << 4, value.getHigh(), value.getLow());
    }

    // Stores a DECIMAL256 value (four 64-bit limbs) at the absolute offset row << 5,
    // the 32-byte fixed-width layout getDecimal256 reads back. The aux region stays
    // the NullMemory stub.
    public void putDecimal256(long row, int col, Decimal256 value) {
        dataMem.getQuick(col).putDecimal256(row << 5, value.getHh(), value.getHl(), value.getLh(), value.getLl());
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

    // Stores a LONG128 / UUID value as two longs at the absolute offset row << 4
    // (lo first, hi at + 8), the 16-byte fixed-width layout getLong128Lo / getLong128Hi
    // read back. The aux region stays the NullMemory stub.
    public void putLong128(long row, int col, long lo, long hi) {
        final long offset = row << 4;
        final MemoryCARWImpl data = dataMem.getQuick(col);
        data.putLong(offset, lo);
        data.putLong(offset + Long.BYTES, hi);
    }

    // Stores a LONG256 value (4 longs) at the absolute offset row << 5, the 32-byte
    // fixed-width layout getLong256A / getLong256B read back. The aux region stays
    // the NullMemory stub.
    public void putLong256(long row, int col, Long256 value) {
        dataMem.getQuick(col).putLong256(row << 5, value);
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
