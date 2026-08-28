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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

/**
 * Shared, stateless writer that materializes one row of a covering query's
 * covered (posting-index sidecar) columns into native page-frame buffers, plus
 * the broadcast of a symbol key column. The byte layout it produces is exactly
 * what a downstream native page-frame consumer (vectorized aggregation, JIT /
 * non-JIT filter, group-by) reads. This class is the single source of truth for
 * the covered byte layout: it drives both the worker-side covered decode (in
 * {@link PageFrameMemoryPool}) and the eager, multi-key frame-production path in
 * {@code CoveringIndexRecordCursorFactory}, which delegates to
 * {@link #writeCoveredRow} (each supplies its own {@link VarDataSink} over its
 * per-column data buffers). The two paths never decode the same value (workers
 * skip multi-key frames) but now share one writer, so the layout cannot drift.
 * <p>
 * Var-size columns (VARCHAR / STRING / BINARY / ARRAY) append into a per-column
 * data buffer fronted by a {@link VarDataSink} that the caller supplies; the
 * caller owns the data-buffer lifecycle (grow / free), this class only writes.
 */
public final class CoveredColumnDecoder {

    private CoveredColumnDecoder() {
    }

    /**
     * Broadcast {@code rawSymbolKey} into {@code count} consecutive int slots at
     * {@code addr}. Mirrors the single-key symbol-column fill done at frame
     * production. Two ints are written per 8-byte store where possible.
     */
    public static void fillSymbolKey(long addr, int rawSymbolKey, int count) {
        long longKey = Integer.toUnsignedLong(rawSymbolKey) | ((long) rawSymbolKey << 32);
        int i = 0;
        int pairs = count & ~1; // round down to even
        for (; i < pairs; i += 2) {
            Unsafe.putLong(addr + (long) i * Integer.BYTES, longKey);
        }
        if (i < count) {
            Unsafe.putInt(addr + (long) i * Integer.BYTES, rawSymbolKey);
        }
    }

    /**
     * Write the covered value of every covered query column for the cursor's
     * current row into row slot {@code count} of the per-column buffers. A
     * column is covered when {@code coveredIncludeIdx[q] >= 0}, in which case
     * the value comes from {@code crc.getCoveredXxx(coveredIncludeIdx[q])}. The
     * symbol-key column (and any non-covered column) is skipped here.
     */
    public static void writeCoveredRow(
            long[] addrs,
            VarDataSink varData,
            int count,
            CoveringRowCursor crc,
            int queryColCount,
            int[] coveredIncludeIdx,
            int[] columnTypeTags,
            int[] columnTypes
    ) {
        for (int q = 0; q < queryColCount; q++) {
            final int includeIdx = coveredIncludeIdx[q];
            if (includeIdx < 0) {
                continue;
            }
            final long addr = addrs[q];
            final int tag = columnTypeTags[q];
            // Fixed-width types share the SAME layout in the eager and worker paths, so they
            // go through the single source of truth below; only the var-size sink differs.
            if (!writeFixedWidthCovered(addr, count, tag, crc, includeIdx)) {
                switch (tag) {
                    case ColumnType.VARCHAR ->
                            writeVarchar(addr, varData, q, count, crc.getCoveredVarcharA(includeIdx));
                    case ColumnType.STRING -> writeString(addr, varData, q, count, crc.getCoveredStrA(includeIdx));
                    case ColumnType.BINARY -> writeBinary(addr, varData, q, count, crc.getCoveredBin(includeIdx));
                    case ColumnType.ARRAY ->
                            writeArray(addr, varData, q, count, crc.getCoveredArray(includeIdx, columnTypes[q]));
                    // Neither a fixed-width type handled above nor a known var-size type: a covered
                    // column type the decoder cannot materialize. Fail loud rather than leave the
                    // slot uninitialized (which would feed garbage into aggregation/projection).
                    default ->
                            throw CairoException.critical(0).put("unsupported covered column type [tag=").put(tag).put(']');
                }
            }
        }
    }

    /**
     * Writes one FIXED-WIDTH covered value to {@code addr} at slot {@code count}. Returns
     * {@code true} if {@code columnTypeTag} is a fixed-width type written here, or {@code false}
     * for a var-size type (VARCHAR / STRING / BINARY / ARRAY) — or an unknown tag — which the
     * caller must handle via its own var-data sink. This is the single source of truth for the
     * fixed-width covered layout, shared by the worker decode (above) and the eager
     * multi-key merge in {@code CoveringIndexRecordCursorFactory}, so the two cannot drift.
     */
    public static boolean writeFixedWidthCovered(long addr, int count, int columnTypeTag, CoveringRowCursor crc, int includeIdx) {
        switch (columnTypeTag) {
            case ColumnType.DOUBLE -> Unsafe.putDouble(
                    addr + (long) count * Double.BYTES, crc.getCoveredDouble(includeIdx));
            case ColumnType.FLOAT -> Unsafe.putFloat(
                    addr + (long) count * Float.BYTES, crc.getCoveredFloat(includeIdx));
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG,
                 ColumnType.DECIMAL64 ->
                    Unsafe.putLong(addr + (long) count * Long.BYTES, crc.getCoveredLong(includeIdx));
            case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL, ColumnType.DECIMAL32 ->
                    Unsafe.putInt(addr + (long) count * Integer.BYTES, crc.getCoveredInt(includeIdx));
            case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                    Unsafe.putShort(addr + (long) count * Short.BYTES, crc.getCoveredShort(includeIdx));
            case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                    Unsafe.putByte(addr + count, crc.getCoveredByte(includeIdx));
            case ColumnType.LONG128, ColumnType.UUID, ColumnType.DECIMAL128 -> {
                long off128 = (long) count * 16;
                Unsafe.putLong(addr + off128, crc.getCoveredLong128Lo(includeIdx));
                Unsafe.putLong(addr + off128 + 8, crc.getCoveredLong128Hi(includeIdx));
            }
            case ColumnType.LONG256, ColumnType.DECIMAL256 -> {
                long off256 = (long) count * 32;
                Unsafe.putLong(addr + off256, crc.getCoveredLong256_0(includeIdx));
                Unsafe.putLong(addr + off256 + 8, crc.getCoveredLong256_1(includeIdx));
                Unsafe.putLong(addr + off256 + 16, crc.getCoveredLong256_2(includeIdx));
                Unsafe.putLong(addr + off256 + 24, crc.getCoveredLong256_3(includeIdx));
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    private static void writeArray(long auxAddr, VarDataSink varData, int q, int count, @Nullable ArrayView value) {
        // ARRAY aux: 16 bytes per row [8-byte data offset][8-byte data size].
        long auxEntry = auxAddr + (long) count * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
        long dataOffset = varData.position(q);
        Unsafe.putLong(auxEntry, dataOffset);

        if (value == null || value.isNull()) {
            Unsafe.putLong(auxEntry + Long.BYTES, 0L);
            return;
        }

        int nDims = value.getDimCount();
        short elemType = value.getElemType();
        int elemSize = ColumnType.sizeOf(elemType);
        long cardinality = value.getCardinality();
        int shapeBytes = nDims * Integer.BYTES;
        int prePad = elemSize > 1
                ? (int) ((-(dataOffset + shapeBytes)) & (elemSize - 1))
                : 0;
        long dataBytes = cardinality * elemSize;
        long postPad = (-(dataOffset + shapeBytes + prePad + dataBytes)) & (Integer.BYTES - 1);
        // The var-data sink is int-addressed, so an array whose framed size overflows int
        // cannot be stored; guard before the (int) cast so a pathological >2GB ARRAY fails
        // loud rather than truncating the size and copying its element data past the
        // under-allocated buffer (mirrors writeString/writeBinary).
        long totalBytesLong = shapeBytes + prePad + dataBytes + postPad;
        if (totalBytesLong > Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put("covered ARRAY value too large [bytes=").put(totalBytesLong).put(']');
        }
        int totalBytes = (int) totalBytesLong;

        Unsafe.putLong(auxEntry + Long.BYTES, totalBytes);
        long dst = varData.ensureCapacity(q, totalBytes) + dataOffset;

        for (int d = 0; d < nDims; d++) {
            Unsafe.putInt(dst, value.getDimLen(d));
            dst += Integer.BYTES;
        }
        if (prePad > 0) {
            Unsafe.setMemory(dst, prePad, (byte) 0);
            dst += prePad;
        }
        // Copy the real element data (bulk for a vanilla DOUBLE array, strided for a non-vanilla
        // slice/transpose) via the shared primitive — never zero-fill — so a non-vanilla covered
        // array decodes correctly and the worker and eager covered paths stay byte-identical.
        ArrayTypeDriver.appendArrayData(dst, value);
        dst += dataBytes;
        if (postPad > 0) {
            Unsafe.setMemory(dst, postPad, (byte) 0);
        }
        varData.advance(q, totalBytes);
    }

    private static void writeBinary(long auxAddr, VarDataSink varData, int q, int count, @Nullable BinarySequence value) {
        // BINARY aux: 8-byte offset per row into data vector.
        long auxEntry = auxAddr + (long) count * Long.BYTES;
        long dataOffset = varData.position(q);
        Unsafe.putLong(auxEntry, dataOffset);

        if (value == null) {
            long dst = varData.ensureCapacity(q, Long.BYTES) + varData.position(q);
            Unsafe.putLong(dst, TableUtils.NULL_LEN);
            varData.advance(q, Long.BYTES);
        } else {
            long len = value.length();
            // The var-data sink is int-addressed, so a value whose framed size overflows int
            // cannot be stored; guard before the (int) cast so a pathological >2GB BINARY fails
            // loud rather than truncating the size and writing past the under-allocated buffer.
            long totalBytesLong = Long.BYTES + len;
            if (totalBytesLong > Integer.MAX_VALUE) {
                throw CairoException.nonCritical().put("covered BINARY value too large [len=").put(len).put(']');
            }
            int totalBytes = (int) totalBytesLong;
            long base = varData.ensureCapacity(q, totalBytes);
            long dst = base + varData.position(q);
            Unsafe.putLong(dst, len);
            value.copyTo(dst + Long.BYTES, 0, len);
            varData.advance(q, totalBytes);
        }
    }

    private static void writeString(long auxAddr, VarDataSink varData, int q, int count, @Nullable CharSequence value) {
        // STRING aux: 8-byte offset per row into data vector.
        long auxEntry = auxAddr + (long) count * Long.BYTES;
        long dataOffset = varData.position(q);
        Unsafe.putLong(auxEntry, dataOffset);

        if (value == null) {
            long dst = varData.ensureCapacity(q, Integer.BYTES) + varData.position(q);
            Unsafe.putInt(dst, TableUtils.NULL_LEN);
            varData.advance(q, Integer.BYTES);
        } else {
            int charCount = value.length();
            // charCount * 2 can overflow int for a multi-GB STRING; compute in long and guard
            // before the cast so an oversized value fails loud rather than under-allocating and
            // writing the UTF-16 body past the buffer.
            long totalBytesLong = Integer.BYTES + (long) charCount * Character.BYTES;
            if (totalBytesLong > Integer.MAX_VALUE) {
                throw CairoException.nonCritical().put("covered STRING value too large [chars=").put(charCount).put(']');
            }
            int totalBytes = (int) totalBytesLong;
            long base = varData.ensureCapacity(q, totalBytes);
            long dst = base + varData.position(q);
            Unsafe.putInt(dst, charCount);
            for (int c = 0; c < charCount; c++) {
                Unsafe.putChar(dst + Integer.BYTES + (long) c * Character.BYTES, value.charAt(c));
            }
            varData.advance(q, totalBytes);
        }
    }

    private static void writeVarchar(long auxAddr, VarDataSink varData, int q, int count, @Nullable Utf8Sequence value) {
        long auxEntry = auxAddr + (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
        long dataOffset = varData.position(q);

        if (value == null) {
            Unsafe.putInt(auxEntry, VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL);
            Unsafe.putInt(auxEntry + 4, 0);
            Unsafe.putShort(auxEntry + 8, (short) 0);
            Unsafe.putShort(auxEntry + 10, (short) dataOffset);
            Unsafe.putInt(auxEntry + 12, (int) (dataOffset >> 16));
        } else {
            int size = value.size();
            if (size <= VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                int header = (size << 4) | 1; // HEADER_FLAG_INLINED
                if (value.isAscii()) header |= 2; // HEADER_FLAG_ASCII
                Unsafe.putByte(auxEntry, (byte) header);
                for (int b = 0; b < size; b++) {
                    Unsafe.putByte(auxEntry + 1 + b, value.byteAt(b));
                }
                for (int b = size; b < VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED; b++) {
                    Unsafe.putByte(auxEntry + 1 + b, (byte) 0);
                }
                Unsafe.putShort(auxEntry + 10, (short) dataOffset);
                Unsafe.putInt(auxEntry + 12, (int) (dataOffset >> 16));
            } else {
                int header = (size << 4);
                if (value.isAscii()) header |= 2;
                Unsafe.putInt(auxEntry, header);
                for (int b = 0; b < VarcharTypeDriver.VARCHAR_INLINED_PREFIX_BYTES; b++) {
                    Unsafe.putByte(auxEntry + 4 + b, value.byteAt(b));
                }
                long base = varData.ensureCapacity(q, size);
                long pos = varData.position(q);
                long srcPtr = value.ptr();
                if (srcPtr != 0) {
                    Unsafe.copyMemory(srcPtr, base + pos, size);
                } else for (int b = 0; b < size; b++) {
                    Unsafe.putByte(base + pos + b, value.byteAt(b));
                }
                Unsafe.putShort(auxEntry + 10, (short) dataOffset);
                Unsafe.putInt(auxEntry + 12, (int) (dataOffset >> 16));
                varData.advance(q, size);
            }
        }
    }

    /**
     * Per-column var-size data buffer access for the covered writers. The caller
     * owns the underlying native buffers; {@link #ensureCapacity} grows the
     * buffer for column {@code q} so that at least {@code needed} more bytes fit
     * past the current {@link #position}, and returns the (possibly relocated)
     * buffer base address. {@link #advance} bumps the write position after a
     * write.
     */
    public interface VarDataSink {
        /**
         * Bump the write position of column {@code q} by {@code written} bytes.
         */
        void advance(int q, int written);

        /**
         * Ensure column {@code q}'s data buffer can hold {@code needed} more bytes
         * past its current position, growing (and possibly relocating) it if
         * required. Returns the buffer base address.
         */
        long ensureCapacity(int q, int needed);

        /**
         * Current write position (byte offset) of column {@code q}.
         */
        long position(int q);
    }
}
