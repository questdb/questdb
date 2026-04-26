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

package io.questdb.cutlass.qwp.codec;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;

/**
 * Column-major accumulator for one QWP egress {@code RESULT_BATCH} table block.
 * <p>
 * Each column has its own {@link QwpColumnScratch} with type-specialised native
 * buffers. {@code appendRow} writes directly to native memory (no JVM-heap
 * allocations on the hot path after warmup). {@code emitTableBlock} memcpys
 * the scratch into the caller-owned wire buffer in one pass per column.
 * <p>
 * One instance is reused across batches on the same connection; the scratch
 * buffers grow to the maximum size observed and never shrink.
 */
public class QwpResultBatchBuffer implements QuietCloseable {

    private static final QwpEgressColumnDef[] EMPTY_DEFS = new QwpEgressColumnDef[0];
    private static final int[] EMPTY_INTS = new int[0];
    private static final QwpColumnScratch[] EMPTY_SCRATCHES = new QwpColumnScratch[0];
    private static final SymbolTable[] EMPTY_SYMBOL_TABLES = new SymbolTable[0];
    private static final byte[] EMPTY_WIRE_TYPES = new byte[0];
    // Per-column encoding discriminator for TIMESTAMP / TIMESTAMP_NANOS / DATE
    // columns when {@code FLAG_GORILLA} is set on the message. 0x00 = raw int64s
    // (mixed-encoding escape for unordered or jumpy columns); 0x01 = Gorilla
    // delta-of-delta bitstream. The byte sits immediately before the column's
    // value bytes so a single batch can mix encoded / unencoded timestamp columns.
    private static final byte ENCODING_GORILLA = 0x01;
    private static final byte ENCODING_UNCOMPRESSED = 0x00;
    /**
     * Largest array (in elements) we'll serialise in one row. Caps the per-row payload at
     * roughly 256 MB (8 bytes x this) so the byte-size math fits in {@code int} and the
     * scratch grow-and-write path can never see a negative or wrap-around length.
     */
    private static final long MAX_ARRAY_ELEMENTS = (Integer.MAX_VALUE - 1024) / 8L;
    // Reused Gorilla encoder. One instance per batch buffer (i.e. per connection).
    private final QwpGorillaEncoder gorillaEncoder = new QwpGorillaEncoder();
    private final ObjList<QwpColumnScratch> scratches = new ObjList<>();
    // Snapshot of connection-scoped dict size taken at {@link #beginBatch}. Any conn
    // ids allocated by this batch's SYMBOL rows sit in [batchDeltaStart..connDict.size()),
    // which is exactly what {@link #emitDeltaSection} needs to ship.
    private int batchDeltaStart;
    private int columnCount;
    private ObjList<QwpEgressColumnDef> columns;
    // Connection-scoped SYMBOL dictionary. Populated directly from appendRow's SYMBOL
    // branch when a native key has never been seen on this connection (per column).
    private QwpEgressConnSymbolDict connDict;
    // Per-batch caches, sized to max column count observed so far. Hoisted out of
    // the appendRow hot loop so the inner iteration reads from plain arrays instead
    // of ObjList.getQuick + def getter chains per cell.
    private QwpEgressColumnDef[] defsArr = EMPTY_DEFS;
    private int[] qdbTypesArr = EMPTY_INTS;
    private int rowCount;
    private QwpColumnScratch[] scratchesArr = EMPTY_SCRATCHES;
    private SymbolTable[] symbolTablesArr = EMPTY_SYMBOL_TABLES;
    private byte[] wireTypesArr = EMPTY_WIRE_TYPES;

    public QwpResultBatchBuffer() {
    }

    /**
     * Bulk-appends {@code (hi - lo)} rows of the given {@code frame}, driving the
     * column emit in column-major order when the frame is in NATIVE format. Each
     * supported column type reads its page memory directly from
     * {@link PageFrame#getPageAddress} and hands a bulk-append call to the
     * corresponding column scratch; types without a columnar fast path fall
     * back to per-row iteration over {@code record}, touching only the columns
     * that need it.
     * <p>
     * The parquet-format fallback iterates rows through {@code record} /
     * {@link #appendRow}, preserving correctness without the columnar speedup.
     * Replace with a parquet-specific columnar decoder as a follow-up.
     */
    public void appendPageFrame(PageFrame frame, PageFrameMemoryRecord record, long lo, long hi) {
        final int rows = (int) (hi - lo);
        if (rows <= 0) {
            return;
        }
        if (frame.getFormat() != PartitionFormat.NATIVE) {
            // Parquet frame: no contiguous column addresses we can memcpy from.
            // Fall back to per-row using the existing single-row path. Row count
            // advances via appendRow itself.
            for (long r = lo; r < hi; r++) {
                record.setRowIndex(r);
                appendRow(record);
            }
            return;
        }
        final int n = columnCount;
        final QwpColumnScratch[] scs = scratchesArr;
        final byte[] wts = wireTypesArr;
        final SymbolTable[] sts = symbolTablesArr;
        for (int ci = 0; ci < n; ci++) {
            final QwpColumnScratch scratch = scs[ci];
            final byte wt = wts[ci];
            // Column-top check moved INSIDE each fixed-width case. For VARCHAR /
            // STRING / BINARY, {@code getPageAddress} returning 0 does NOT mean
            // column top -- it can also mean all values in this frame are
            // inline-stored in the aux vector with no overflow to the data
            // vector. Those types take the per-row fallback which uses
            // {@code record.getX} to distinguish correctly.
            switch (wt) {
                case QwpConstants.TYPE_LONG:
                case QwpConstants.TYPE_DATE:
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                case QwpConstants.TYPE_DECIMAL64: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnLong8WithSentinel(base + lo * 8L, rows);
                    }
                    break;
                }
                case QwpConstants.TYPE_DOUBLE: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnDouble8(base + lo * 8L, rows);
                    }
                    break;
                }
                case QwpConstants.TYPE_INT: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnInt4WithSentinel(base + lo * 4L, rows, Numbers.INT_NULL);
                    }
                    break;
                }
                case QwpConstants.TYPE_IPV4: {
                    // QuestDB stores IPv4 NULL as the bit pattern 0 (Numbers.IPv4_NULL).
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnInt4WithSentinel(base + lo * 4L, rows, Numbers.IPv4_NULL);
                    }
                    break;
                }
                case QwpConstants.TYPE_FLOAT: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnFloat4(base + lo * 4L, rows);
                    }
                    break;
                }
                case QwpConstants.TYPE_SHORT:
                case QwpConstants.TYPE_CHAR: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnFixedNoNull(base + lo * 2L, rows, 2);
                    }
                    break;
                }
                case QwpConstants.TYPE_BYTE: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnFixedNoNull(base + lo, rows, 1);
                    }
                    break;
                }
                case QwpConstants.TYPE_BOOLEAN: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else {
                        scratch.appendColumnBoolean(base + lo, rows);
                    }
                    break;
                }
                case QwpConstants.TYPE_SYMBOL: {
                    SymbolTable st = sts[ci];
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        fillNulls(scratch, rows);
                    } else if (st != null) {
                        scratch.appendColumnSymbolKeys(base + lo * 4L, rows, st, connDict);
                    } else {
                        // No per-column symbol table (synthetic records): per-row fallback
                        // through record.getSymA + bytes-keyed dedup in the conn dict.
                        perColumnRowLoop(record, lo, hi, ci);
                    }
                    break;
                }
                default:
                    // VARCHAR, STRING, BINARY, UUID, LONG256, DECIMAL128/256, GEOHASH,
                    // ARRAYS: columnar path not implemented yet -- fall back to per-row
                    // for this one column. Other columns on this frame still took the
                    // columnar path above.
                    perColumnRowLoop(record, lo, hi, ci);
            }
        }
        rowCount += rows;
    }

    /**
     * Appends one row's worth of values from the given record.
     */
    public void appendRow(Record record) {
        // Hoist all per-column caches into locals so the JIT can trust they don't
        // alias across columns; ObjList.getQuick chains were the dominant overhead
        // in the previous version.
        final int n = columnCount;
        final QwpColumnScratch[] scs = scratchesArr;
        final byte[] wts = wireTypesArr;
        final int[] qts = qdbTypesArr;
        final QwpEgressColumnDef[] defs = defsArr;
        final SymbolTable[] sts = symbolTablesArr;
        for (int ci = 0; ci < n; ci++) {
            appendCell(record, ci, scs[ci], wts[ci], qts[ci], defs[ci], sts[ci]);
        }
        rowCount++;
    }

    /**
     * Starts a new batch with the given schema. Re-sizes the scratch pool, resets each
     * scratch, and populates the per-column hot-path caches read by {@link #appendRow}.
     * {@code connDict} is the connection-scoped SYMBOL dictionary: SYMBOL columns
     * append new UTF-8 bytes to it on first sight per native key and emit the
     * returned conn-id straight into the wire. When {@code symbolTables} is null or
     * doesn't expose a table for a given column, {@code appendRow} falls back to
     * {@code record.getSymA} without per-column dedup.
     */
    public void beginBatch(
            ObjList<QwpEgressColumnDef> columns,
            SymbolTableSource symbolTables,
            QwpEgressConnSymbolDict connDict
    ) {
        this.columns = columns;
        this.columnCount = columns.size();
        this.rowCount = 0;
        this.connDict = connDict;
        this.batchDeltaStart = connDict.size();
        while (scratches.size() < columnCount) {
            scratches.add(new QwpColumnScratch());
        }
        if (defsArr.length < columnCount) {
            int cap = Math.max(columnCount, defsArr.length * 2);
            defsArr = new QwpEgressColumnDef[cap];
            scratchesArr = new QwpColumnScratch[cap];
            wireTypesArr = new byte[cap];
            qdbTypesArr = new int[cap];
            symbolTablesArr = new SymbolTable[cap];
        }
        for (int i = 0; i < columnCount; i++) {
            QwpEgressColumnDef def = columns.getQuick(i);
            QwpColumnScratch scratch = scratches.getQuick(i);
            scratch.beginBatch(def);
            defsArr[i] = def;
            scratchesArr[i] = scratch;
            wireTypesArr[i] = def.getWireType();
            qdbTypesArr[i] = def.getQuestdbColumnType();
            SymbolTable st = null;
            if (symbolTables != null && wireTypesArr[i] == QwpConstants.TYPE_SYMBOL) {
                try {
                    st = symbolTables.getSymbolTable(i);
                } catch (UnsupportedOperationException ignored) {
                    // Cursor doesn't expose symbol tables (rare) - fall back to getSymA.
                }
            }
            symbolTablesArr[i] = st;
        }
        // Poison any trailing slots from a wider previous batch so a stray SYMBOL
        // lookup against an old table can never fire.
        for (int i = columnCount; i < defsArr.length; i++) {
            symbolTablesArr[i] = null;
        }
    }

    @Override
    public void close() {
        // Misc.freeObjListIfCloseable iterates and frees in a try/finally pattern,
        // so a throwing close on element K does not strand elements K+1..n. The
        // hand-rolled loop this replaced would skip the remainder on any throw.
        Misc.freeObjListIfCloseable(scratches);
        scratches.clear();
        columns = null;
        rowCount = 0;
        columnCount = 0;
    }

    /**
     * Writes the message-level delta section for {@code FLAG_DELTA_SYMBOL_DICT}:
     * <pre>
     *   [deltaStartId: varint]
     *   [deltaCount:   varint]
     *   for each new entry: [length: varint][UTF-8 bytes]
     * </pre>
     * New entries are whatever {@code appendRow} appended to the connection dict
     * during this batch, i.e. {@code [batchDeltaStart..connDict.size())}. No
     * probing, no hashing, no equality checks -- the dict is already populated.
     *
     * @return bytes written, or -1 if the delta section would overflow wireLimit
     */
    public int emitDeltaSection(long wireBuf, long wireLimit) {
        final int deltaStart = batchDeltaStart;
        final int deltaCount = connDict.size() - deltaStart;
        long p = wireBuf;
        if (p + 2 * QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
        p = QwpVarint.encode(p, deltaStart);
        p = QwpVarint.encode(p, deltaCount);
        if (deltaCount == 0) {
            return (int) (p - wireBuf);
        }
        long heapAddr = connDict.getHeapAddr();
        int prevEnd = connDict.entryStart(deltaStart);
        for (int i = 0; i < deltaCount; i++) {
            int entryId = deltaStart + i;
            int endOff = connDict.entryEnd(entryId);
            int entryLen = endOff - prevEnd;
            if (p + QwpVarint.MAX_VARINT_BYTES + entryLen > wireLimit) return -1;
            p = QwpVarint.encode(p, entryLen);
            Vect.memcpy(p, heapAddr + prevEnd, entryLen);
            p += entryLen;
            prevEnd = endOff;
        }
        return (int) (p - wireBuf);
    }

    /**
     * Emits the full table block body for this batch starting at {@code wireBuf}:
     * name_length (=0) + row_count + column_count + schema + column data sections.
     *
     * @return number of bytes written, or -1 if the data would overflow wireLimit
     */
    public int emitTableBlock(long wireBuf, long wireLimit, long schemaId, boolean writeFullSchema) {
        long p = wireBuf;
        // Preflight the fixed prelude: 1 byte empty-name + rowCount varint +
        // columnCount varint. QwpVarint.encode has no internal bound check and
        // can emit up to MAX_VARINT_BYTES per value; without the guard this
        // loop walks past wireLimit whenever the caller's budget is tight.
        if (p + 1 + 2L * QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
        // Anonymous result-set: empty name
        Unsafe.putByte(p++, (byte) 0);
        p = QwpVarint.encode(p, rowCount);
        p = QwpVarint.encode(p, columnCount);
        if (writeFullSchema) {
            int needed = QwpEgressSchemaWriter.worstCaseFullSize(columns);
            if (p + needed > wireLimit) return -1;
            p = QwpEgressSchemaWriter.writeFull(p, schemaId, columns);
        } else {
            if (p + 1 + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            p = QwpEgressSchemaWriter.writeReference(p, schemaId);
        }
        for (int ci = 0; ci < columnCount; ci++) {
            p = emitColumn(scratches.getQuick(ci), p, wireLimit);
            if (p < 0) return -1;
        }
        return (int) (p - wireBuf);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void reset() {
        for (int i = 0, n = scratches.size(); i < n; i++) {
            scratches.getQuick(i).beginBatch(null);
        }
        rowCount = 0;
        columnCount = 0;
        columns = null;
    }

    /**
     * Clears the per-column native-key -> connId caches. Called once per query
     * start; within a single query these caches amortise across all batches so
     * the same symbol values aren't re-emitted into the delta section.
     */
    public void resetForNewQuery() {
        for (int i = 0, n = scratches.size(); i < n; i++) {
            scratches.getQuick(i).resetForNewQuery();
        }
    }

    /**
     * Undoes any dict entries committed by the current batch. Called from the
     * server's error path before {@code endStreaming}: a batch that called
     * {@link QwpEgressConnSymbolDict#addEntry} but never shipped its
     * {@code RESULT_BATCH} frame must not leave orphan entries in the connection
     * dict, because a subsequent query could hit the dedup map on the same bytes,
     * receive the orphan id back, and emit row payload referencing an id the
     * client has never been taught.
     * <p>
     * Idempotent: safe to call when no batch is in flight (no-op).
     */
    public void rollbackCurrentBatch() {
        if (connDict != null) {
            connDict.rollbackTo(batchDeltaStart);
        }
    }

    /**
     * Serialises an array into {@code scratch.arrayHeapAddr} without allocating a {@code byte[]}.
     * Format: {@code [nDims u8] [dim lengths: nDims x i32 LE] [values: product(dims) x 8 bytes LE]}.
     * Throws if the element-count product overflows; the buffer state is unchanged in that case.
     */
    private static void appendArrayBytesDirect(QwpColumnScratch scratch, ArrayView av, byte wireType) {
        int nDims = av.getDimCount();
        long elements = 1;
        for (int d = 0; d < nDims; d++) {
            int dim = av.getDimLen(d);
            if (dim < 0) {
                throw CairoException.nonCritical()
                        .put("QWP egress: ARRAY dim is negative [dim=").put(d).put(", value=").put(dim).put(']');
            }
            elements *= dim;
            if (elements > MAX_ARRAY_ELEMENTS) {
                throw CairoException.nonCritical()
                        .put("QWP egress: ARRAY element count exceeds limit [elements=").put(elements)
                        .put(", limit=").put(MAX_ARRAY_ELEMENTS).put(']');
            }
        }
        int totalBytes = 1 + 4 * nDims + 8 * (int) elements;
        scratch.ensureArrayHeapCapacity(scratch.arrayHeapPos + totalBytes);
        long p = scratch.arrayHeapAddr + scratch.arrayHeapPos;
        Unsafe.putByte(p++, (byte) nDims);
        for (int d = 0; d < nDims; d++) {
            Unsafe.putInt(p, av.getDimLen(d));
            p += 4;
        }
        if (wireType == QwpConstants.TYPE_DOUBLE_ARRAY) {
            for (int i = 0; i < elements; i++) {
                Unsafe.putLong(p, Double.doubleToRawLongBits(av.getDouble(i)));
                p += 8;
            }
        } else {
            for (int i = 0; i < elements; i++) {
                Unsafe.putLong(p, av.getLong(i));
                p += 8;
            }
        }
        scratch.arrayHeapPos += totalBytes;
        scratch.markNonNullAndAdvanceRow();
    }

    private static long emitStringColumn(QwpColumnScratch scratch, long p, long wireLimit) {
        int nonNull = scratch.nonNullCount;
        long offsetsBytes = 4L * (nonNull + 1);
        long bytesBytes = scratch.stringHeapPos;
        if (p + offsetsBytes + bytesBytes > wireLimit) return -1;
        // offset[0] = 0; offsets[1..nonNull] were populated during append.
        Unsafe.putInt(p, 0);
        // Copy offsets[1..nonNull] from scratch.stringOffsetsAddr (which stores them at indices 1..nonNull)
        // to p + 4 .. p + 4 * nonNull + 4.
        Vect.memcpy(p + 4L, scratch.stringOffsetsAddr + 4L, 4L * nonNull);
        Vect.memcpy(p + offsetsBytes, scratch.stringHeapAddr, bytesBytes);
        return p + offsetsBytes + bytesBytes;
    }

    private static long emitSymbolColumn(QwpColumnScratch scratch, long p, long wireLimit) {
        // Per-column dict is omitted -- bytes for new entries went out already in
        // the message-level delta section. Per-row payload is just the connection
        // dict ids, which appendRow already stored into symbolIdsAddr (no
        // translation required).
        final int nonNull = scratch.nonNullCount;
        final long idsAddr = scratch.symbolIdsAddr;
        for (int i = 0; i < nonNull; i++) {
            if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            int connId = Unsafe.getInt(idsAddr + 4L * i);
            p = QwpVarint.encode(p, connId);
        }
        return p;
    }

    private static void fillNulls(QwpColumnScratch scratch, int n) {
        // Bulk bitmap fill -- one partial-byte OR at each end and a memset for
        // the full bytes in between, instead of n iterations of read-modify-
        // write. Matters when an ALTER TABLE ADD COLUMN leaves many columns as
        // column-tops and every page-frame triggers fillNulls per such column.
        scratch.appendNullColumn(n);
    }

    private static long readGeoBits(Record record, int col, int precisionBits) {
        if (precisionBits <= 7) return record.getGeoByte(col);
        if (precisionBits <= 15) return record.getGeoShort(col);
        if (precisionBits <= 31) return record.getGeoInt(col);
        return record.getGeoLong(col);
    }

    /**
     * Appends a single cell (one row, one column) to {@code scratch}. Factored
     * out of {@link #appendRow} so the per-row path and the per-column fallback
     * inside {@link #appendPageFrame} share the switch body. The JIT inlines
     * this into both callers.
     */
    private void appendCell(Record record, int ci, QwpColumnScratch scratch, byte wt, int qt,
                            QwpEgressColumnDef def, SymbolTable st) {
        switch (wt) {
            case QwpConstants.TYPE_BOOLEAN:
                scratch.appendBool(record.getBool(ci));
                break;
            case QwpConstants.TYPE_BYTE:
                scratch.appendByte(record.getByte(ci));
                break;
            case QwpConstants.TYPE_SHORT:
                scratch.appendShort(record.getShort(ci));
                break;
            case QwpConstants.TYPE_CHAR:
                scratch.appendChar(record.getChar(ci));
                break;
            case QwpConstants.TYPE_INT:
                scratch.appendIntOrNull(record.getInt(ci));
                break;
            case QwpConstants.TYPE_IPV4:
                // QuestDB stores IPv4 NULL as the bit pattern 0 (Numbers.IPv4_NULL).
                // The wire reader still cannot represent the literal address 0.0.0.0
                // as non-null - that's a QuestDB-level limitation inherited by the
                // wire format.
                scratch.appendIPv4OrNull(record.getInt(ci));
                break;
            case QwpConstants.TYPE_LONG:
                scratch.appendLongOrNull(record.getLong(ci));
                break;
            case QwpConstants.TYPE_DATE:
                scratch.appendLongOrNull(record.getDate(ci));
                break;
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
                scratch.appendLongOrNull(record.getTimestamp(ci));
                break;
            case QwpConstants.TYPE_FLOAT:
                // QuestDB FLOAT NULL == NaN. Spec sec 11.5 documents that NaN values
                // (including a "legitimate" NaN such as 0/0) round-trip as NULL.
                scratch.appendFloatOrNull(record.getFloat(ci));
                break;
            case QwpConstants.TYPE_DOUBLE:
                // Same NaN-as-NULL convention as FLOAT (spec sec 11.5).
                scratch.appendDoubleOrNull(record.getDouble(ci));
                break;
            case QwpConstants.TYPE_VARCHAR: {
                // Egress advertises TYPE_VARCHAR for both QuestDB STRING and VARCHAR source
                // columns (identical wire layout); branch on the source type to reach the
                // right Record getter.
                if (ColumnType.tagOf(qt) == ColumnType.STRING) {
                    CharSequence cs = record.getStrA(ci);
                    if (cs == null) scratch.appendNull();
                    else scratch.appendString(cs);
                } else {
                    Utf8Sequence us = record.getVarcharA(ci);
                    if (us == null) scratch.appendNull();
                    else scratch.appendVarchar(us);
                }
                break;
            }
            case QwpConstants.TYPE_BINARY: {
                io.questdb.std.BinarySequence bin = record.getBin(ci);
                if (bin == null) {
                    scratch.appendNull();
                } else {
                    long len = bin.length();
                    // Per-row BINARY length is encoded as the offset delta in a uint32 array,
                    // so individual values cannot exceed Integer.MAX_VALUE bytes. The total
                    // batch is also bounded by the 16 MiB QwpConstants.DEFAULT_MAX_BATCH_SIZE.
                    if (len < 0 || len > Integer.MAX_VALUE) {
                        throw CairoException.nonCritical()
                                .put("QWP egress: BINARY value too large [len=").put(len).put(']');
                    }
                    scratch.appendBinary(bin);
                }
                break;
            }
            case QwpConstants.TYPE_SYMBOL: {
                if (st != null) {
                    int key = record.getInt(ci);
                    if (key == SymbolTable.VALUE_IS_NULL) {
                        scratch.appendNull();
                    } else {
                        IntIntHashMap k2c = scratch.connKeyToConnId;
                        int mapIdx = k2c.keyIndex(key);
                        int connId;
                        if (mapIdx < 0) {
                            connId = k2c.valueAt(mapIdx);
                        } else {
                            // First sight of this native key on this connection. Encode the
                            // UTF-8 bytes once into the shared connection dict and remember
                            // the assigned conn-id. Subsequent rows hit the cached branch.
                            connId = connDict.addEntry(st.valueOf(key));
                            k2c.putAt(mapIdx, key, connId);
                        }
                        scratch.appendSymbolConnId(connId);
                    }
                } else {
                    // No SymbolTable exposed by the cursor -- rare path (synthetic records).
                    // Fall back to getSymA + bytes-keyed dedup via the connection dict. This
                    // ships each value once per occurrence (no dedup) to keep the common path
                    // above branch-free.
                    CharSequence cs = record.getSymA(ci);
                    if (cs == null) {
                        scratch.appendNull();
                    } else {
                        int connId = connDict.addEntry(cs);
                        scratch.appendSymbolConnId(connId);
                    }
                }
                break;
            }
            case QwpConstants.TYPE_UUID: {
                long lo = record.getLong128Lo(ci);
                long hi = record.getLong128Hi(ci);
                if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) scratch.appendNull();
                else scratch.appendUuid(lo, hi);
                break;
            }
            case QwpConstants.TYPE_LONG256: {
                Long256 l256 = record.getLong256A(ci);
                if (l256 == null || (l256.getLong0() == Numbers.LONG_NULL
                        && l256.getLong1() == Numbers.LONG_NULL
                        && l256.getLong2() == Numbers.LONG_NULL
                        && l256.getLong3() == Numbers.LONG_NULL)) {
                    scratch.appendNull();
                } else {
                    scratch.appendLong256(l256.getLong0(), l256.getLong1(), l256.getLong2(), l256.getLong3());
                }
                break;
            }
            case QwpConstants.TYPE_GEOHASH: {
                int precBits = def.getPrecisionBits();
                long bits = readGeoBits(record, ci, precBits);
                if (bits == -1L) scratch.appendNull();
                else scratch.appendGeohash(bits, (precBits + 7) >>> 3);
                break;
            }
            case QwpConstants.TYPE_DECIMAL64:
                scratch.appendLongOrNull(record.getDecimal64(ci));
                break;
            case QwpConstants.TYPE_DECIMAL128: {
                record.getDecimal128(ci, scratch.decimal128Sink);
                if (scratch.decimal128Sink.isNull()) scratch.appendNull();
                else scratch.appendDecimal128(scratch.decimal128Sink.getLow(), scratch.decimal128Sink.getHigh());
                break;
            }
            case QwpConstants.TYPE_DECIMAL256: {
                record.getDecimal256(ci, scratch.decimal256Sink);
                if (scratch.decimal256Sink.isNull()) scratch.appendNull();
                else scratch.appendDecimal256(
                        scratch.decimal256Sink.getLl(),
                        scratch.decimal256Sink.getLh(),
                        scratch.decimal256Sink.getHl(),
                        scratch.decimal256Sink.getHh());
                break;
            }
            case QwpConstants.TYPE_DOUBLE_ARRAY:
            case QwpConstants.TYPE_LONG_ARRAY: {
                ArrayView av = record.getArray(ci, qt);
                if (av == null || av.isNull()) {
                    scratch.appendNull();
                } else {
                    appendArrayBytesDirect(scratch, av, wt);
                }
                break;
            }
            default:
                throw CairoException.nonCritical()
                        .put("QWP egress append: unsupported wire type [code=")
                        .put(wt & 0xFF).put(']');
        }
    }

    /**
     * Writes one column to the wire buffer starting at {@code p}. Layout:
     * null flag + optional bitmap, then the type-specific payload (bit-packed
     * BOOLEAN; VARCHAR / BINARY offsets + bytes; SYMBOL ids; GEOHASH precision +
     * values; DOUBLE / LONG arrays; DECIMAL scale + values; TIMESTAMP family
     * with Gorilla or raw; generic fixed-width memcpy for BYTE / SHORT / INT /
     * CHAR / LONG / FLOAT / DOUBLE / UUID / LONG256). Returns the write pointer
     * past the column, or {@code -1} if {@code wireLimit} would be exceeded.
     */
    private long emitColumn(QwpColumnScratch scratch, long p, long wireLimit) {
        // 1. Null flag + optional bitmap
        if (scratch.nullCount == 0) {
            if (p >= wireLimit) return -1;
            Unsafe.putByte(p++, (byte) 0);
        } else {
            int bitmapBytes = (rowCount + 7) >>> 3;
            if (p + 1 + bitmapBytes > wireLimit) return -1;
            Unsafe.putByte(p++, (byte) 1);
            Vect.memcpy(p, scratch.nullBitmapAddr, bitmapBytes);
            p += bitmapBytes;
        }

        byte wire = scratch.def.getWireType();
        int nonNull = scratch.nonNullCount;

        if (wire == QwpConstants.TYPE_BOOLEAN) {
            int bytes = (nonNull + 7) >>> 3;
            if (p + bytes > wireLimit) return -1;
            Vect.memcpy(p, scratch.valuesAddr, bytes);
            return p + bytes;
        }
        if (wire == QwpConstants.TYPE_VARCHAR || wire == QwpConstants.TYPE_BINARY) {
            // Both share the same wire layout: (N+1) x uint32 offsets + concatenated bytes.
            // BINARY differs from VARCHAR only in that the bytes are opaque (no UTF-8 contract).
            return emitStringColumn(scratch, p, wireLimit);
        }
        if (wire == QwpConstants.TYPE_SYMBOL) {
            return emitSymbolColumn(scratch, p, wireLimit);
        }
        if (wire == QwpConstants.TYPE_GEOHASH) {
            if (p + QwpVarint.MAX_VARINT_BYTES > wireLimit) return -1;
            p = QwpVarint.encode(p, scratch.def.getPrecisionBits());
            if (p + scratch.valuesPos > wireLimit) return -1;
            Vect.memcpy(p, scratch.valuesAddr, scratch.valuesPos);
            return p + scratch.valuesPos;
        }
        if (wire == QwpConstants.TYPE_DOUBLE_ARRAY || wire == QwpConstants.TYPE_LONG_ARRAY) {
            if (p + scratch.arrayHeapPos > wireLimit) return -1;
            Vect.memcpy(p, scratch.arrayHeapAddr, scratch.arrayHeapPos);
            return p + scratch.arrayHeapPos;
        }

        // Decimal: scale byte prefix, then dense values
        if (wire == QwpConstants.TYPE_DECIMAL64
                || wire == QwpConstants.TYPE_DECIMAL128
                || wire == QwpConstants.TYPE_DECIMAL256) {
            if (p + 1 > wireLimit) return -1;
            Unsafe.putByte(p++, (byte) scratch.def.getScale());
            if (p + scratch.valuesPos > wireLimit) return -1;
            Vect.memcpy(p, scratch.valuesAddr, scratch.valuesPos);
            return p + scratch.valuesPos;
        }

        // Timestamp-ish types: prefix a per-column encoding byte and try Gorilla.
        // Fall back to raw int64s when delta-of-delta overflows int32 (unordered
        // or jumpy timestamps) or when the bitstream wouldn't save space.
        if (wire == QwpConstants.TYPE_TIMESTAMP
                || wire == QwpConstants.TYPE_TIMESTAMP_NANOS
                || wire == QwpConstants.TYPE_DATE) {
            return emitTimestampColumn(scratch, p, wireLimit, nonNull);
        }

        // Generic fixed-width (BYTE/SHORT/INT/CHAR/LONG/FLOAT/DOUBLE/UUID/LONG256)
        if (p + scratch.valuesPos > wireLimit) return -1;
        Vect.memcpy(p, scratch.valuesAddr, scratch.valuesPos);
        return p + scratch.valuesPos;
    }

    private long emitTimestampColumn(QwpColumnScratch scratch, long p, long wireLimit, int nonNull) {
        if (p >= wireLimit) return -1;
        // nonNull * 8 is carried as long throughout: int multiplication would
        // overflow if MAX_ROWS_PER_BATCH is ever raised past ~2^28. Today the
        // cap is 4096, so no overflow can happen -- the cast is defence in depth.
        long rawBytes = (long) nonNull * 8L;
        // 0, 1, 2 values have no delta-of-delta to emit; ship raw int64s under the
        // uncompressed discriminator so the decoder sees a consistent layout.
        if (nonNull < 3) {
            Unsafe.putByte(p++, ENCODING_UNCOMPRESSED);
            if (p + rawBytes > wireLimit) return -1;
            if (rawBytes > 0) {
                Vect.memcpy(p, scratch.valuesAddr, rawBytes);
            }
            return p + rawBytes;
        }
        // Probe first: -1 = delta-of-delta doesn't fit int32 anywhere; fall back
        // to raw rather than silently truncate.
        int gorillaBytes = QwpGorillaEncoder.calculateEncodedSizeIfSupported(scratch.valuesAddr, nonNull);
        if (gorillaBytes >= 0 && gorillaBytes < rawBytes) {
            if (p + 1 + gorillaBytes > wireLimit) return -1;
            Unsafe.putByte(p++, ENCODING_GORILLA);
            int written = gorillaEncoder.encodeTimestamps(p, wireLimit - p, scratch.valuesAddr, nonNull);
            return p + written;
        }
        if (p + 1 + rawBytes > wireLimit) return -1;
        Unsafe.putByte(p++, ENCODING_UNCOMPRESSED);
        Vect.memcpy(p, scratch.valuesAddr, rawBytes);
        return p + rawBytes;
    }

    /**
     * Per-column fallback used by {@link #appendPageFrame} for wire types that
     * don't have a columnar fast path yet (VARCHAR, STRING, BINARY, UUID, etc.).
     * Walks rows in {@code [lo, hi)} and writes only column {@code ci}; other
     * columns of the same frame are handled by the columnar dispatch.
     */
    private void perColumnRowLoop(PageFrameMemoryRecord record, long lo, long hi, int ci) {
        final QwpColumnScratch scratch = scratchesArr[ci];
        final byte wt = wireTypesArr[ci];
        final int qt = qdbTypesArr[ci];
        final QwpEgressColumnDef def = defsArr[ci];
        final SymbolTable st = symbolTablesArr[ci];
        for (long r = lo; r < hi; r++) {
            record.setRowIndex(r);
            appendCell(record, ci, scratch, wt, qt, def, st);
        }
    }

}
