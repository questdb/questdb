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
    private long batchDeltaWireBytesAtStart;
    private int columnCount;
    private ObjList<QwpEgressColumnDef> columns;
    // Connection-scoped SYMBOL dictionary. Populated directly from appendRow's SYMBOL
    // branch when a native key has never been seen on this connection (per column).
    private QwpEgressConnSymbolDict connDict;
    // Per-batch caches, sized to max column count observed so far. Hoisted out of
    // the appendRow hot loop so the inner iteration reads from plain arrays instead
    // of ObjList.getQuick + def getter chains per cell.
    private QwpEgressColumnDef[] defsArr = EMPTY_DEFS;
    // Byte size of the first batch's inline schema block (col_count varint +
    // per-column descriptors). It depends only on the column shape, so beginBatch
    // computes it once and emitTableBlockImpl reuses it on every dry-run and real
    // emit -- without it the partial-emit binary search re-walks the column list
    // on every probe.
    private int inlineSchemaBytes;
    private int physicalRowCount;
    private int[] qdbTypesArr = EMPTY_INTS;
    private QwpColumnScratch[] scratchesArr = EMPTY_SCRATCHES;
    private int startRow;
    private SymbolTable[] symbolTablesArr = EMPTY_SYMBOL_TABLES;
    private byte[] wireTypesArr = EMPTY_WIRE_TYPES;

    public QwpResultBatchBuffer() {
    }

    /**
     * Advances {@code batchDeltaStart} to {@code connDict.size()}. Called from
     * inside the send functions after a successful partial-emit frame so the
     * subsequent emit's delta section ships no overlap with the bytes already
     * sent.
     */
    public void advanceDeltaStart() {
        if (connDict != null) {
            batchDeltaStart = connDict.size();
            batchDeltaWireBytesAtStart = connDict.getTotalWireBytes();
        }
    }

    /**
     * Advances the logical start past {@code k} just-shipped rows. When the
     * advance drains the live row count to zero, scratch positions reset in
     * place so the next batch reuses the native heap from byte 0.
     */
    public void advanceStartRow(int k) {
        assert startRow <= Integer.MAX_VALUE - k : "startRow int overflow";
        startRow += k;
        if (startRow == physicalRowCount) {
            for (int i = 0; i < columnCount; i++) {
                scratches.getQuick(i).resetPositions();
            }
            physicalRowCount = 0;
            startRow = 0;
        }
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
                        // Wire spec sec 11.5: SHORT / CHAR cannot carry NULL.
                        // INSERT NULL stores 0 and the wire row keeps the null
                        // bitmap bit clear, so a column-top frame ships literal
                        // zero values rather than driving fillNulls (which would
                        // set bits in the null bitmap that the client rejects).
                        scratch.appendColumnFixedZero(rows, 2);
                    } else {
                        scratch.appendColumnFixedNoNull(base + lo * 2L, rows, 2);
                    }
                    break;
                }
                case QwpConstants.TYPE_BYTE: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        // Wire spec sec 11.5: BYTE cannot carry NULL. See the
                        // SHORT / CHAR case above for the column-top rationale.
                        scratch.appendColumnFixedZero(rows, 1);
                    } else {
                        scratch.appendColumnFixedNoNull(base + lo, rows, 1);
                    }
                    break;
                }
                case QwpConstants.TYPE_BOOLEAN: {
                    long base = frame.getPageAddress(ci);
                    if (base == 0) {
                        // Wire spec sec 11.5: BOOLEAN cannot carry NULL. The
                        // column-top fill is n bit-packed false values; the
                        // null bitmap stays clear.
                        scratch.appendColumnBooleanZero(rows);
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
        assert physicalRowCount <= Integer.MAX_VALUE - rows : "physicalRowCount int overflow";
        physicalRowCount += rows;
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
        assert physicalRowCount < Integer.MAX_VALUE : "physicalRowCount int overflow";
        physicalRowCount++;
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
        this.physicalRowCount = 0;
        this.startRow = 0;
        this.connDict = connDict;
        this.batchDeltaStart = connDict.size();
        this.batchDeltaWireBytesAtStart = connDict.getTotalWireBytes();
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
        // Cache the first-batch schema block size once; it depends only on the
        // column shape, so the partial-emit binary search reads the field rather
        // than re-deriving it on every probe.
        this.inlineSchemaBytes = QwpVarint.encodedLength(columnCount) + inlineColumnsSize();
    }

    /**
     * Like {@link #resetForNewQuery} but without the buffer-size shrink pass, so a
     * following {@code beginStreaming} owns the single shrink and the previous
     * query's sizing isn't discarded.
     */
    public void clearConnKeyMaps() {
        for (int i = 0, n = scratches.size(); i < n; i++) {
            scratches.getQuick(i).clearConnKeyMap();
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
        physicalRowCount = 0;
        startRow = 0;
        columnCount = 0;
        inlineSchemaBytes = 0;
        batchDeltaStart = 0;
        batchDeltaWireBytesAtStart = 0;
    }

    public int computeDeltaSize() {
        return emitDeltaSectionImpl(0L, Long.MAX_VALUE, true);
    }

    public int currentBatchDeltaWireBytes() {
        int deltaCount = connDict.size() - batchDeltaStart;
        long entryBytes = connDict.getTotalWireBytes() - batchDeltaWireBytesAtStart;
        return QwpVarint.encodedLength(batchDeltaStart)
                + QwpVarint.encodedLength(deltaCount)
                + (int) entryBytes;
    }

    /**
     * Returns the exact number of bytes {@link #emitTableBlockPrefix} would
     * produce for {@code rowsToEmit} logical rows starting at {@code startRow}.
     * Uses {@link #emitTableBlockImpl} in dry-run mode so emit and size share
     * the same byte-layout source -- no drift risk.
     */
    public int computeTableBlockSize(int rowsToEmit, boolean isFirstBatch) {
        // wireLimit = Long.MAX_VALUE so the dry run can never report overflow.
        return emitTableBlockImpl(0L, Long.MAX_VALUE, startRow, rowsToEmit, isFirstBatch, true);
    }

    /**
     * Writes the message-level delta section for {@code FLAG_DELTA_SYMBOL_DICT}:
     * <pre>
     *   [deltaStartId: varint]
     *   [deltaCount:   varint]
     *   for each new entry: [length: varint][UTF-8 bytes]
     * </pre>
     * New entries are whatever {@code appendRow} appended to the connection dict
     * during this batch, i.e. {@code [batchDeltaStart..connDict.size())}.
     *
     * @return bytes written, or -1 if the delta section would overflow wireLimit
     */
    public int emitDeltaSection(long wireBuf, long wireLimit) {
        return emitDeltaSectionImpl(wireBuf, wireLimit, false);
    }

    /**
     * Emits the full table block body for the rows currently buffered
     * (physical rows {@code [0, physicalRowCount)}) starting at {@code wireBuf}.
     * Convenience wrapper over {@link #emitTableBlockImpl} for the happy path.
     *
     * @return number of bytes written, or -1 if the data would overflow wireLimit
     */
    public int emitTableBlock(long wireBuf, long wireLimit, boolean isFirstBatch) {
        return emitTableBlockImpl(wireBuf, wireLimit, 0, physicalRowCount, isFirstBatch, false);
    }

    /**
     * Emits a prefix of the currently buffered rows: {@code rowsToEmit} logical
     * rows starting at {@code startRow}. The slice's wire layout is
     * byte-identical to a freshly built batch of that size -- decoder is
     * unchanged. Used by the partial-emit path when the full buffered batch
     * doesn't fit in the wire frame.
     *
     * @return number of bytes written, or -1 if the data would overflow wireLimit
     */
    public int emitTableBlockPrefix(long wireBuf, long wireLimit, int rowsToEmit, boolean isFirstBatch) {
        return emitTableBlockImpl(wireBuf, wireLimit, startRow, rowsToEmit, isFirstBatch, false);
    }

    /**
     * Binary-searches the largest {@code K} in {@code [0, getRowCount()]} such
     * that {@link #computeTableBlockSize}{@code (K, isFirstBatch) <= budget}.
     * <p>
     * Return values:
     * <ul>
     *   <li>{@code K == getRowCount()}: every buffered row fits, take the happy path;</li>
     *   <li>{@code 0 < K < getRowCount()}: ship a partial emit of {@code K} rows;</li>
     *   <li>{@code K == 0}: the table-block header fits but no row does. Caller
     *       must treat as single-row overflow;</li>
     *   <li>{@code K == -1}: even the empty header overflows the budget --
     *       pathological send-buffer config.</li>
     * </ul>
     */
    public int findLargestEmittablePrefix(long budget, boolean isFirstBatch) {
        int hi = getRowCount();
        if (computeTableBlockSize(0, isFirstBatch) > budget) return -1;
        if (hi == 0) return 0;
        if (computeTableBlockSize(hi, isFirstBatch) <= budget) return hi;
        int lo = 0;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (computeTableBlockSize(mid, isFirstBatch) <= budget) lo = mid;
            else hi = mid - 1;
        }
        return lo;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getRowCount() {
        return physicalRowCount - startRow;
    }

    public void reset() {
        for (int i = 0, n = scratches.size(); i < n; i++) {
            scratches.getQuick(i).beginBatch(null);
        }
        physicalRowCount = 0;
        startRow = 0;
        columnCount = 0;
        columns = null;
        inlineSchemaBytes = 0;
        batchDeltaStart = 0;
        batchDeltaWireBytesAtStart = 0;
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
     * Undoes any dict entries committed by the current batch and discards any
     * un-shipped scratch rows. Called from the server's error path before
     * {@code endStreaming}: a batch that called
     * {@link QwpEgressConnSymbolDict#addEntry} but never shipped its
     * {@code RESULT_BATCH} frame must not leave orphan entries in the connection
     * dict, because a subsequent query could hit the dedup map on the same bytes,
     * receive the orphan id back, and emit row payload referencing an id the
     * client has never been taught. The scratch-position reset is the matching
     * step for the partial-emit suffix: without it the next query would skip
     * {@code beginBatch} (because {@code getRowCount() &gt; 0}) and append into
     * stale scratches holding ids that no longer exist in the rolled-back dict.
     * <p>
     * Idempotent: safe to call when no batch is in flight (no-op on a fresh
     * buffer).
     */
    public void rollbackCurrentBatch() {
        if (connDict != null) {
            connDict.rollbackTo(batchDeltaStart);
        }
        physicalRowCount = 0;
        startRow = 0;
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
        scratch.recordArrayOffset();
        scratch.markNonNullAndAdvanceRow();
    }

    /**
     * Copies {@code lenBits} bits from {@code src} starting at bit
     * {@code srcBitOffset} to bit 0 of {@code dst}. Aligned case ({@code
     * srcBitOffset % 8 == 0}) is a plain memcpy; misaligned uses per-byte
     * stitching of two source bytes. Output bits past {@code lenBits} are zeroed
     * in the trailing partial byte so the wire format matches a freshly built
     * dense bitmap.
     */
    private static void bitMemcpyShifted(long dst, long src, int srcBitOffset, int lenBits) {
        if (lenBits == 0) return;
        int srcByteOff = srcBitOffset >>> 3;
        int shift = srcBitOffset & 7;
        int fullBytes = lenBits >>> 3;
        int tailBits = lenBits & 7;
        if (shift == 0) {
            if (fullBytes > 0) Vect.memcpy(dst, src + srcByteOff, fullBytes);
            if (tailBits > 0) {
                int mask = (1 << tailBits) - 1;
                int b = Unsafe.getByte(src + srcByteOff + fullBytes) & mask;
                Unsafe.putByte(dst + fullBytes, (byte) b);
            }
            return;
        }
        int invShift = 8 - shift;
        for (int i = 0; i < fullBytes; i++) {
            int cur = Unsafe.getByte(src + srcByteOff + i) & 0xFF;
            int next = Unsafe.getByte(src + srcByteOff + i + 1) & 0xFF;
            Unsafe.putByte(dst + i, (byte) ((cur >>> shift) | (next << invShift)));
        }
        if (tailBits > 0) {
            int cur = Unsafe.getByte(src + srcByteOff + fullBytes) & 0xFF;
            int out = cur >>> shift;
            if (tailBits > invShift) {
                int next = Unsafe.getByte(src + srcByteOff + fullBytes + 1) & 0xFF;
                out |= next << invShift;
            }
            int mask = (1 << tailBits) - 1;
            Unsafe.putByte(dst + fullBytes, (byte) (out & mask));
        }
    }

    /**
     * Copies the null bitmap slice for rows {@code [srcRowStart, srcRowStart +
     * rowsToEmit)} to byte 0 of {@code dst}. Supports any {@code srcRowStart}
     * via {@link #bitMemcpyShifted}; the aligned case collapses to a memcpy
     * inside that helper.
     */
    private static void emitNullBitmapSlice(long bitmapAddr, int srcRowStart, int rowsToEmit, long dst) {
        bitMemcpyShifted(dst, bitmapAddr, srcRowStart, rowsToEmit);
    }

    /**
     * Emits the dense values slice for STRING/VARCHAR/BINARY columns:
     * {@code (sliceNonNull + 1) x i32} rebased offsets followed by the
     * concatenated UTF-8 bytes of the slice's non-null rows.
     */
    private static long emitStringSlice(QwpColumnScratch scratch, long p, long wireLimit,
                                        int nonNullsBefore, int sliceNonNull, boolean dryRun) {
        int baseOff = (nonNullsBefore == 0) ? 0
                : Unsafe.getInt(scratch.stringOffsetsAddr + (long) nonNullsBefore * 4);
        int endIdx = nonNullsBefore + sliceNonNull;
        int endOff = (endIdx == 0) ? 0
                : Unsafe.getInt(scratch.stringOffsetsAddr + (long) endIdx * 4);
        int heapBytes = endOff - baseOff;
        long offsetsBytes = 4L * (sliceNonNull + 1);
        if (p + offsetsBytes + heapBytes > wireLimit) return -1;
        if (!dryRun) {
            Unsafe.putInt(p, 0);
            if (nonNullsBefore == 0) {
                // Fast path: stored offsets already match the wire layout (baseOff = 0).
                Vect.memcpy(p + 4L, scratch.stringOffsetsAddr + 4L, 4L * sliceNonNull);
            } else {
                long dst = p + 4L;
                for (int i = 1; i <= sliceNonNull; i++) {
                    int physOff = Unsafe.getInt(scratch.stringOffsetsAddr + (long) (nonNullsBefore + i) * 4);
                    Unsafe.putInt(dst, physOff - baseOff);
                    dst += 4;
                }
            }
            Vect.memcpy(p + offsetsBytes, scratch.stringHeapAddr + baseOff, heapBytes);
        }
        return p + offsetsBytes + heapBytes;
    }

    private static long emitSymbolSlice(QwpColumnScratch scratch, long p, long wireLimit,
                                        int nonNullsBefore, int sliceNonNull, boolean dryRun) {
        // Dry-run and the wire-overflow guard both consult the prefix-sum
        // (O(1) per column instead of O(rows) per probe).
        int startCum = nonNullsBefore == 0 ? 0
                : Unsafe.getInt(scratch.symbolBytesCumAddr + 4L * nonNullsBefore);
        int endIdx = nonNullsBefore + sliceNonNull;
        int endCum = endIdx == 0 ? 0
                : Unsafe.getInt(scratch.symbolBytesCumAddr + 4L * endIdx);
        long bytes = endCum - startCum;
        if (p + bytes > wireLimit) return -1;
        if (dryRun) {
            return p + bytes;
        }
        final long idsAddr = scratch.symbolIdsAddr;
        for (int i = 0; i < sliceNonNull; i++) {
            int connId = Unsafe.getInt(idsAddr + 4L * (nonNullsBefore + i));
            p = QwpVarint.encode(p, connId);
        }
        return p;
    }

    private static void fillNulls(QwpColumnScratch scratch, int n) {
        // Bulk bitmap fill -- one partial-byte OR at each end and a memset for
        // the full bytes in between, instead of n iterations of read-modify-
        // write. Matters when an ALTER TABLE ADD COLUMN leaves many columns as
        // column-tops and every page-frame triggers fillNulls per such column.
        // Callable only for types that can carry NULL on the wire; BYTE /
        // SHORT / CHAR / BOOLEAN take appendColumnFixedZero /
        // appendColumnBooleanZero instead so the wire row's null bitmap bit
        // stays clear (egress spec sec 11.5).
        scratch.appendNullColumn(n);
    }

    /**
     * Counts SET bits in {@code [startBit, startBit + lenBits)}. Handles
     * arbitrary alignment of both start and end via masked partial-byte
     * reads at each end.
     */
    private static int nullsInRange(long bitmapAddr, int startBit, int lenBits) {
        if (lenBits == 0) return 0;
        int endBit = startBit + lenBits;
        int firstByte = startBit >>> 3;
        int lastByte = (endBit - 1) >>> 3;
        int firstBitInFirstByte = startBit & 7;
        if (firstByte == lastByte) {
            // All bits in one byte.
            int mask = ((1 << lenBits) - 1) << firstBitInFirstByte;
            return Integer.bitCount(Unsafe.getByte(bitmapAddr + firstByte) & mask & 0xFF);
        }
        int count = 0;
        // First partial byte: bits [firstBitInFirstByte, 8).
        int firstMask = (0xFF << firstBitInFirstByte) & 0xFF;
        count += Integer.bitCount(Unsafe.getByte(bitmapAddr + firstByte) & firstMask);
        // Middle full bytes [firstByte + 1, lastByte).
        int byteOff = firstByte + 1;
        while (byteOff + 8 <= lastByte) {
            count += Long.bitCount(Unsafe.getLong(bitmapAddr + byteOff));
            byteOff += 8;
        }
        while (byteOff < lastByte) {
            count += Integer.bitCount(Unsafe.getByte(bitmapAddr + byteOff) & 0xFF);
            byteOff++;
        }
        // Last partial byte: bits [0, (endBit-1)&7 + 1).
        int lastMask = (1 << (((endBit - 1) & 7) + 1)) - 1;
        count += Integer.bitCount(Unsafe.getByte(bitmapAddr + lastByte) & lastMask);
        return count;
    }

    private static long readGeoBits(Record record, int col, int precisionBits) {
        if (precisionBits <= 7) return record.getGeoByte(col);
        if (precisionBits <= 15) return record.getGeoShort(col);
        if (precisionBits <= 31) return record.getGeoInt(col);
        return record.getGeoLong(col);
    }

    /**
     * Returns the fixed byte width per non-null value for "generic fixed-width"
     * wire types (BYTE, SHORT, CHAR, INT, IPV4, FLOAT, LONG, DOUBLE, UUID,
     * LONG256). Types with a per-column prefix byte (GEOHASH, DECIMAL,
     * TIMESTAMP family) are handled in dedicated branches and never reach this
     * helper.
     */
    private static int wireTypeFixedSize(byte wireType) {
        return switch (wireType) {
            case QwpConstants.TYPE_BYTE -> 1;
            case QwpConstants.TYPE_SHORT, QwpConstants.TYPE_CHAR -> 2;
            case QwpConstants.TYPE_INT, QwpConstants.TYPE_IPV4, QwpConstants.TYPE_FLOAT -> 4;
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DOUBLE -> 8;
            case QwpConstants.TYPE_UUID -> 16;
            case QwpConstants.TYPE_LONG256 -> 32;
            default -> throw CairoException.nonCritical()
                    .put("QWP egress emit: unsupported fixed-width wire type [code=")
                    .put(wireType & 0xFF).put(']');
        };
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
     * Emits one column for rows {@code [srcRowStart, srcRowStart + rowsToEmit)}
     * starting at {@code p}. Layout: null-flag byte + optional bitmap slice,
     * then the type-specific payload restricted to the slice's non-null rows.
     * When {@code dryRun} is true, computes the byte advance without writing
     * to {@code p} -- used by {@link #computeTableBlockSize} as the single
     * source of truth for prefix sizing.
     * <p>
     * Returns the post-write pointer or {@code -1} if {@code wireLimit} would
     * be exceeded.
     */
    private long emitColumnSlice(QwpColumnScratch scratch, long p, long wireLimit,
                                 int srcRowStart, int rowsToEmit, boolean dryRun) {
        // Derive the dense-storage range from the null bitmap. Three cases:
        //   (a) column has no nulls anywhere -- skip popcount entirely;
        //   (b) full physical slice -- collapse to existing counters;
        //   (c) general slice -- bitmap popcount.
        int nonNullsBefore;
        int sliceNonNull;
        if (scratch.nullCount == 0) {
            nonNullsBefore = srcRowStart;
            sliceNonNull = rowsToEmit;
        } else if (srcRowStart == 0 && rowsToEmit == physicalRowCount) {
            nonNullsBefore = 0;
            sliceNonNull = scratch.nonNullCount;
        } else {
            nonNullsBefore = srcRowStart - nullsInRange(scratch.nullBitmapAddr, 0, srcRowStart);
            sliceNonNull = rowsToEmit - nullsInRange(scratch.nullBitmapAddr, srcRowStart, rowsToEmit);
        }
        boolean hasNulls = sliceNonNull < rowsToEmit;
        int nullBitmapBytes = (rowsToEmit + 7) >>> 3;

        // 1. Null-flag byte + optional bitmap slice. Matches the pre-slicing
        //    layout: flag byte 0 means "no bitmap follows", flag byte 1 means
        //    the bitmap covers `rowsToEmit` rows starting at the slice's row 0.
        if (hasNulls) {
            if (p + 1 + nullBitmapBytes > wireLimit) return -1;
            if (!dryRun) {
                Unsafe.putByte(p, (byte) 1);
                emitNullBitmapSlice(scratch.nullBitmapAddr, srcRowStart, rowsToEmit, p + 1);
            }
            p += 1 + nullBitmapBytes;
        } else {
            if (p >= wireLimit) return -1;
            if (!dryRun) Unsafe.putByte(p, (byte) 0);
            p++;
        }

        // 2. Type-specific payload restricted to the slice's non-null rows.
        byte wire = scratch.def.getWireType();
        if (wire == QwpConstants.TYPE_BOOLEAN) {
            int bytes = (sliceNonNull + 7) >>> 3;
            if (p + bytes > wireLimit) return -1;
            if (!dryRun) bitMemcpyShifted(p, scratch.valuesAddr, nonNullsBefore, sliceNonNull);
            return p + bytes;
        }
        if (wire == QwpConstants.TYPE_VARCHAR || wire == QwpConstants.TYPE_BINARY) {
            // Both share the same wire layout: (N+1) x uint32 offsets + concatenated bytes.
            // BINARY differs from VARCHAR only in that the bytes are opaque (no UTF-8 contract).
            return emitStringSlice(scratch, p, wireLimit, nonNullsBefore, sliceNonNull, dryRun);
        }
        if (wire == QwpConstants.TYPE_SYMBOL) {
            return emitSymbolSlice(scratch, p, wireLimit, nonNullsBefore, sliceNonNull, dryRun);
        }
        if (wire == QwpConstants.TYPE_GEOHASH) {
            int precBits = scratch.def.getPrecisionBits();
            int precBytes = (precBits + 7) >>> 3;
            int precVarintBytes = QwpVarint.encodedLength(precBits);
            long dataBytes = (long) sliceNonNull * precBytes;
            if (p + precVarintBytes + dataBytes > wireLimit) return -1;
            if (!dryRun) {
                p = QwpVarint.encode(p, precBits);
                Vect.memcpy(p, scratch.valuesAddr + (long) nonNullsBefore * precBytes, dataBytes);
                return p + dataBytes;
            }
            return p + precVarintBytes + dataBytes;
        }
        if (wire == QwpConstants.TYPE_DOUBLE_ARRAY || wire == QwpConstants.TYPE_LONG_ARRAY) {
            int baseOff = (nonNullsBefore == 0) ? 0
                    : Unsafe.getInt(scratch.arrayOffsetsAddr + (long) nonNullsBefore * 4);
            int endIdx = nonNullsBefore + sliceNonNull;
            int endOff = (endIdx == 0) ? 0
                    : Unsafe.getInt(scratch.arrayOffsetsAddr + (long) endIdx * 4);
            int heapBytes = endOff - baseOff;
            if (p + heapBytes > wireLimit) return -1;
            if (!dryRun) Vect.memcpy(p, scratch.arrayHeapAddr + baseOff, heapBytes);
            return p + heapBytes;
        }
        if (wire == QwpConstants.TYPE_DECIMAL64
                || wire == QwpConstants.TYPE_DECIMAL128
                || wire == QwpConstants.TYPE_DECIMAL256) {
            int elemSize = (wire == QwpConstants.TYPE_DECIMAL64) ? 8
                    : (wire == QwpConstants.TYPE_DECIMAL128) ? 16 : 32;
            long dataBytes = (long) sliceNonNull * elemSize;
            if (p + 1 + dataBytes > wireLimit) return -1;
            if (!dryRun) {
                Unsafe.putByte(p, (byte) scratch.def.getScale());
                Vect.memcpy(p + 1, scratch.valuesAddr + (long) nonNullsBefore * elemSize, dataBytes);
            }
            return p + 1 + dataBytes;
        }
        if (wire == QwpConstants.TYPE_TIMESTAMP
                || wire == QwpConstants.TYPE_TIMESTAMP_NANOS
                || wire == QwpConstants.TYPE_DATE) {
            return emitTimestampSlice(scratch, p, wireLimit, nonNullsBefore, sliceNonNull, dryRun);
        }

        // Generic fixed-width (BYTE/SHORT/CHAR/INT/IPV4/FLOAT/LONG/DOUBLE/UUID/LONG256).
        int elemSize = wireTypeFixedSize(wire);
        long dataBytes = (long) sliceNonNull * elemSize;
        if (p + dataBytes > wireLimit) return -1;
        if (!dryRun) Vect.memcpy(p, scratch.valuesAddr + (long) nonNullsBefore * elemSize, dataBytes);
        return p + dataBytes;
    }

    /**
     * Single source of truth for table-block emit. Writes the wire layout for
     * rows {@code [srcRowStart, srcRowStart + rowsToEmit)} at {@code wireBuf}:
     * empty-name byte + row_count varint + (first batch only) col_count varint
     * + inline column descriptors + one column-slice per column. When {@code dryRun}
     * is true, computes the byte count without writing -- the same code path
     * powers both {@link #emitTableBlock} / {@link #emitTableBlockPrefix} and
     * {@link #computeTableBlockSize}, eliminating drift between emit and
     * size estimation.
     *
     * @return number of bytes written / computed, or -1 if {@code wireLimit}
     * would be exceeded (dry runs called with MAX_VALUE never return -1).
     */
    private int emitDeltaSectionImpl(long wireBuf, long wireLimit, boolean dryRun) {
        final int deltaStart = batchDeltaStart;
        final int deltaCount = connDict.size() - deltaStart;
        final int startVarLen = QwpVarint.encodedLength(deltaStart);
        final int countVarLen = QwpVarint.encodedLength(deltaCount);
        long p = wireBuf;
        if (p + startVarLen + countVarLen > wireLimit) return -1;
        if (!dryRun) {
            p = QwpVarint.encode(p, deltaStart);
            p = QwpVarint.encode(p, deltaCount);
        } else {
            p += startVarLen + countVarLen;
        }
        if (deltaCount == 0) {
            return (int) (p - wireBuf);
        }
        long heapAddr = dryRun ? 0 : connDict.getHeapAddr();
        int prevEnd = connDict.entryStart(deltaStart);
        for (int i = 0; i < deltaCount; i++) {
            int endOff = connDict.entryEnd(deltaStart + i);
            int entryLen = endOff - prevEnd;
            int lenVar = QwpVarint.encodedLength(entryLen);
            if (p + lenVar + entryLen > wireLimit) return -1;
            if (!dryRun) {
                p = QwpVarint.encode(p, entryLen);
                Vect.memcpy(p, heapAddr + prevEnd, entryLen);
                p += entryLen;
            } else {
                p += lenVar + entryLen;
            }
            prevEnd = endOff;
        }
        return (int) (p - wireBuf);
    }

    private int emitTableBlockImpl(long wireBuf, long wireLimit, int srcRowStart, int rowsToEmit,
                                   boolean isFirstBatch, boolean dryRun) {
        long p = wireBuf;
        int rowsVarLen = QwpVarint.encodedLength(rowsToEmit);
        // table_name (zero-length varint, one 0x00 byte) + row_count varint.
        if (p + 1 + rowsVarLen > wireLimit) return -1;
        if (!dryRun) {
            Unsafe.putByte(p, (byte) 0);
            p++;
            p = QwpVarint.encode(p, rowsToEmit);
        } else {
            p += 1 + rowsVarLen;
        }
        // The first batch of a query (batch_seq == 0) carries col_count plus the
        // inline column descriptors; continuation batches carry rows only -- the
        // client holds the schema between batches of the same query.
        if (isFirstBatch) {
            int schemaBytes = inlineSchemaBytes;
            if (p + schemaBytes > wireLimit) return -1;
            if (!dryRun) {
                long startP = p;
                p = QwpVarint.encode(p, columnCount);
                for (int i = 0, n = columns.size(); i < n; i++) {
                    QwpEgressColumnDef col = columns.getQuick(i);
                    byte[] nameBytes = col.getNameUtf8();
                    p = QwpVarint.encode(p, nameBytes.length);
                    for (byte b : nameBytes) {
                        Unsafe.putByte(p++, b);
                    }
                    Unsafe.putByte(p++, col.getWireType());
                }
                assert p - startP == schemaBytes : "inline schema emit drifted from cached inlineSchemaBytes";
            } else {
                p += schemaBytes;
            }
        }
        for (int ci = 0; ci < columnCount; ci++) {
            p = emitColumnSlice(scratches.getQuick(ci), p, wireLimit,
                    srcRowStart, rowsToEmit, dryRun);
            if (p < 0) return -1;
        }
        return (int) (p - wireBuf);
    }

    /**
     * Emits the dense values slice for TIMESTAMP / TIMESTAMP_NANOS / DATE
     * columns: per-column encoding discriminator byte followed by either
     * Gorilla delta-of-delta or raw int64s, computed over the slice's non-null
     * values. Heuristic per slice -- short slices and Gorilla failures fall
     * back to RAW.
     */
    private long emitTimestampSlice(QwpColumnScratch scratch, long p, long wireLimit,
                                    int nonNullsBefore, int sliceNonNull, boolean dryRun) {
        if (p >= wireLimit) return -1;
        long sliceValuesAddr = scratch.valuesAddr + (long) nonNullsBefore * 8L;
        long rawBytes = (long) sliceNonNull * 8L;
        // 0, 1, 2 values have no delta-of-delta to emit; ship raw int64s under
        // the uncompressed discriminator so the decoder sees a consistent layout.
        if (sliceNonNull < 3) {
            if (p + 1 + rawBytes > wireLimit) return -1;
            if (!dryRun) {
                Unsafe.putByte(p, ENCODING_UNCOMPRESSED);
                if (rawBytes > 0) Vect.memcpy(p + 1, sliceValuesAddr, rawBytes);
            }
            return p + 1 + rawBytes;
        }
        // Cache hit when (nonNullsBefore, sliceNonNull, nonNullCount) matches.
        // nonNullCount acts as a version: any append mutates it.
        int gorillaBytes;
        if (scratch.cachedGorillaNonNullsBefore == nonNullsBefore
                && scratch.cachedGorillaSliceNonNull == sliceNonNull
                && scratch.cachedGorillaNonNullCount == scratch.nonNullCount) {
            gorillaBytes = scratch.cachedGorillaBytes;
        } else {
            gorillaBytes = QwpGorillaEncoder.calculateEncodedSizeIfSupported(sliceValuesAddr, sliceNonNull);
            scratch.cachedGorillaNonNullsBefore = nonNullsBefore;
            scratch.cachedGorillaSliceNonNull = sliceNonNull;
            scratch.cachedGorillaNonNullCount = scratch.nonNullCount;
            scratch.cachedGorillaBytes = gorillaBytes;
        }
        if (gorillaBytes >= 0 && gorillaBytes < rawBytes) {
            if (p + 1 + gorillaBytes > wireLimit) return -1;
            if (!dryRun) {
                Unsafe.putByte(p, ENCODING_GORILLA);
                int written = gorillaEncoder.encodeTimestamps(p + 1, wireLimit - p - 1, sliceValuesAddr, sliceNonNull);
                assert written == gorillaBytes : "Gorilla encode size drifted from probe";
            }
            return p + 1 + gorillaBytes;
        }
        if (p + 1 + rawBytes > wireLimit) return -1;
        if (!dryRun) {
            Unsafe.putByte(p, ENCODING_UNCOMPRESSED);
            Vect.memcpy(p + 1, sliceValuesAddr, rawBytes);
        }
        return p + 1 + rawBytes;
    }

    /**
     * Byte count of the inline column descriptors (per col: {@code name_len}
     * varint + UTF-8 name + wire-type byte) for the current schema, excluding
     * the leading {@code col_count} varint. Mirrors the per-column emit loop in
     * {@link #emitTableBlockImpl} so dry-run sizing matches the real emit
     * byte-for-byte.
     */
    private int inlineColumnsSize() {
        int total = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpEgressColumnDef col = columns.getQuick(i);
            int nameLen = col.getNameUtf8().length;
            total += QwpVarint.encodedLength(nameLen) + nameLen + 1 /* wire type */;
        }
        return total;
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
