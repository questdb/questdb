/*******************************************************************************
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

package io.questdb.cairo.arr;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.O3Utils;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryOM;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reads and writes arrays. Arrays are organised as follows:
 * <h2>AUX entries</h2>
 *
 * <h3>Data Offset Handling</h3>
 * <p>
 * Like the <code>VARCHAR</code> type, <code>ARRAY</code> uses <code>N</code>
 * (not <code>N + 1</code>) entries in the AUX table.
 *
 * <h3>AUX entry format</h3>
 *
 * <pre>
 * 128-bit entries
 *     * offset_and_hash: 64 bits
 *         * bits 0 to =47: offset, a 48-bit unsigned integer
 *             * byte-level offset into the data vector
 *         * bits 48 to =64: reserved
 *     * data_size: 32 bits
 *         * number of bytes used to the store the array (along with any additional metadata) in the data vector.
 *     * reserved: 32 bits
 * </pre>
 *
 * <h3>Encoding NULLs</h3>
 * <ul>
 *     <li>A null value has zero size.</li>
 *     <li>We however <em>do</em> populate the <code>offset</code> field with
 *     the end of the previous non-null value.</li>
 *     <li>This allows mapping the data vector for a specific range of values.</li>
 * </ul>
 *
 * <h3>Data vector</h3>
 * <pre>
 * variable length encoding, starting at the offset specified in the `aux` entry.
 *     * START ALIGNMENT: the start of each entry in the data vector is aligned at 32 bits.
 *     * Shape: len-prefixed ints
 *         * A list of dimension sizes of the array.
 *         * Starts with a 32-bit length (number of dimensions).
 *         * Each dimension size is a 32-bit int, but uses only 27 bits.
 *     * Padding:
 *         * enough padding to satisfy the datatype alignment requirements.
 *         * e.g. for 64-bit numeric types, the following section starts on an
 *           8-byte boundary; for a 32-bit type, on a 4 byte boundary.
 *           This is to avoid unaligned data reads.
 *         * In practice, this would be either 0 or 4 bytes of padding (given we've just written ints).
 *     * raw values buffer
 *         * a buffer of bytes, containing the values in row-major order.
 *           E.g. for the 2x3 array
 *               {{1, 2, 3},
 *                {4, 5, 6}}
 *           The numbers are recorded in the order 1, 2, 3, 4, 5, 6.
 *         * its size is calculated as:
 *               bits_per_elem = pow(2, ColumnType.getArrayElementTypePrecision(type))
 *               n_bytes_size = (bits_per_elem * product(all_dimensions) + 7) / 8
 *           where `product(all_dimensions)` is each dimension multiplied
 *
 *           A few examples:
 *               boolean 2x33 2D array:
 *                   precision: 0
 *                   n_bytes_size = 6
 *                       (bits_per_elem:1 * product(all_dimensions):44 + 7) / 8
 *               32-bit unsigned in 2x7x3 3D array:
 *                   precision: 5
 *                   n_bytes_size = 168
 *                       (bits_per_elem:32 * product(all_dimensions):42 + 7) / 8
 *         * enough padding for `int` alignment, ready for the next record (see START ALIGNMENT note).
 * </pre>
 */
public class ArrayTypeDriver implements ColumnTypeDriver {
    // ensure that writeArrayEntry appends correct amount of bytes, for the width
    public static final int ARRAY_AUX_WIDTH_BYTES = 4 * Integer.BYTES;
    public static final ArrayTypeDriver INSTANCE = new ArrayTypeDriver();
    public static final long OFFSET_MAX = (1L << 48) - 1L;
    private static final ArrayValueAppender VALUE_APPENDER_DOUBLE = ArrayTypeDriver::appendDoubleFromArrayToSink;
    private static final ArrayValueAppender VALUE_APPENDER_LONG = ArrayTypeDriver::appendLongFromArrayToSink;
    private static final ArrayValueAppender VALUE_APPENDER_VARCHAR = ArrayTypeDriver::appendVarcharFromArrayToSink;

    /**
     * Appends an array in compact format, used by {@link io.questdb.griffin.engine.groupby.GroupByArraySink}.
     * <p>
     * Layout:
     * <pre>
     * | dataSize  |   dim0  |  dim1  | ... |     dimN-1     | array data |
     * +-----------+---------+--------+-----+----------------+------------+
     * |  4 bytes  |                 N * 4 bytes             |     -      |
     * +-----------+-----------------------------------------+------------+
     * </pre>
     * <p>
     * This differs from {@link #appendPlainValue} which includes an 8-byte totalSize field
     * and a 4-byte type field. This compact format omits the type (known from context) and
     * uses a 4-byte dataSize instead of 8-byte totalSize.
     *
     * @param addr     the memory address to write to
     * @param value    the array to append
     * @param nDims    the number of dimensions
     * @param elemSize the size of each element in bytes
     */
    public static void appendCompactPlainValue(long addr, ArrayView value, int nDims, int elemSize) {
        if (value == null || value.isNull()) {
            Unsafe.getUnsafe().putInt(addr, TableUtils.NULL_LEN);
            return;
        }

        int dataSize = value.getCardinality() * elemSize;

        Unsafe.getUnsafe().putInt(addr, dataSize);
        addr += Integer.BYTES;

        for (int i = 0; i < nDims; i++) {
            Unsafe.getUnsafe().putInt(addr, value.getDimLen(i));
            addr += Integer.BYTES;
        }

        if (value.isVanilla()) {
            short elemType = value.getElemType();
            if (elemType == ColumnType.DOUBLE) {
                value.flatView().appendPlainDoubleValue(addr, value.getFlatViewOffset(), value.getFlatViewLength());
            } else {
                throw new UnsupportedOperationException("Unsupported array element type: " + elemType);
            }
        } else {
            appendToMemRecursive(value, 0, 0, addr);
        }
    }

    public static void appendDoubleFromArrayToSink(
            @NotNull ArrayView array,
            int index,
            @NotNull CharSink<?> sink,
            @NotNull String nullLiteral
    ) {
        double d = array.getDouble(index);
        if (Numbers.isFinite(d)) {
            sink.put(d);
        } else {
            sink.put(nullLiteral);
        }
    }

    public static long appendPlainValue(long appendAddress, ArrayView value) {
        long startAddress = appendAddress;
        if (value == null || value.isNull()) {
            Unsafe.getUnsafe().putLong(appendAddress, TableUtils.NULL_LEN);
            return Long.BYTES;
        }
        Unsafe.getUnsafe().putLong(appendAddress, value.getVanillaMemoryLayoutSize());
        appendAddress += Long.BYTES;
        Unsafe.getUnsafe().putInt(appendAddress, value.getType());
        appendAddress += Integer.BYTES;
        for (int nDims = value.getDimCount(), i = 0; i < nDims; i++) {
            Unsafe.getUnsafe().putInt(appendAddress, value.getDimLen(i));
            appendAddress += Integer.BYTES;
        }
        if (value.isVanilla()) {
            short elemType = value.getElemType();
            if (elemType == ColumnType.DOUBLE) {
                appendAddress = value.flatView().appendPlainDoubleValue(appendAddress, value.getFlatViewOffset(), value.getFlatViewLength());
            } else {
                throw new UnsupportedOperationException("Unsupported array element type: " + elemType);
            }
        } else {
            appendAddress = appendToMemRecursive(value, 0, 0, appendAddress);
        }
        long bytesWritten = appendAddress - startAddress;
        assert bytesWritten > 0;
        return bytesWritten;
    }

    public static void appendValue(
            @NotNull MemoryA auxMem,
            @NotNull MemoryA dataMem,
            @NotNull ArrayView array
    ) {
        if (array.isNull()) {
            appendNullImpl(auxMem, dataMem);
            return;
        }

        final long beginOffset = dataMem.getAppendOffset();
        writeDataEntry(dataMem, array);
        final long endOffset = dataMem.getAppendOffset();
        final int size = (int) (endOffset - beginOffset);
        writeAuxEntry(auxMem, beginOffset, size);
    }

    public static void arrayToJson(
            @NotNull ArrayView array,
            @NotNull CharSink<?> sink,
            @NotNull ArrayValueAppender appender,
            ArrayWriteState arrayState
    ) {
        arrayToText(array, sink, appender, '[', ']', "null", arrayState);
    }

    /**
     * Appends a JSON representation of the provided array to the provided character sink.
     */
    public static void arrayToJson(
            @Nullable ArrayView arrayView,
            @NotNull CharSink<?> sink,
            @NotNull ArrayWriteState arrayState
    ) {
        if (arrayView == null) {
            sink.put("null");
        } else {
            arrayToJson(arrayView, sink, resolveAppender(arrayView), arrayState);
        }
    }

    /**
     * Appends a PG Wire representation of the provided array to the provided character sink.
     */
    public static void arrayToPgWire(
            @NotNull ArrayView arrayView,
            @NotNull CharSink<?> sink
    ) {
        arrayToText(
                arrayView,
                sink,
                resolveAppender(arrayView),
                '{',
                '}',
                "NULL",
                NoopArrayWriteState.INSTANCE
        );
    }

    /**
     * Recursively builds a string representation of a multidimensional array stored in row‑major order.
     * With [] as open-close chars, it produces JSON. With {}, it produces PG Wire format.
     *
     * @param array       flat array containing all the elements
     * @param sink        sink that accumulates the JSON string
     * @param openChar    opening character for each array plane
     * @param closeChar   closing character for each array plane
     * @param nullLiteral text that represents a null value
     * @param arrayState  state management object to allow this builder to restart if the output sink runs out of space.
     */
    public static void arrayToText(
            @NotNull ArrayView array,
            @NotNull CharSink<?> sink,
            @NotNull ArrayValueAppender appender,
            char openChar,
            char closeChar,
            @NotNull String nullLiteral,
            ArrayWriteState arrayState
    ) {
        if (array.isNull()) {
            sink.putAscii(nullLiteral);
            return;
        }
        // An empty array can have various shapes, such as (100_000_000, 100_000_000, 0).
        // The arrayToText() call below would output 10 quadrillion empty brackets in that case.
        if (array.isEmpty()) {
            sink.put(openChar).put(closeChar);
            return;
        }
        arrayToText(array, 0, 0, sink, appender, openChar, closeChar, nullLiteral, arrayState);
    }

    /**
     * Determine the number of bytes to skip in order to get to the next aligned address/offset.
     */
    public static int bytesToSkipForAlignment(long unaligned, int byteAlignment) {
        final int pastBy = (int) (unaligned % byteAlignment);
        if (pastBy == 0) {
            return 0;
        }
        // The number of bytes to skip is the complement of how many we're past by.
        return byteAlignment - pastBy;
    }

    public static long getAuxVectorOffsetStatic(long row) {
        return ARRAY_AUX_WIDTH_BYTES * row;
    }

    /**
     * Reads an array from compact format (see {@link #appendCompactPlainValue} for layout).
     * <p>
     * This is the counterpart to {@link #appendCompactPlainValue} and differs from {@link #getPlainValue}
     * which expects an 8-byte size field and a type field in the layout.
     *
     * @param addr  the memory address to read from
     * @param type  the encoded array type (provided from context)
     * @param nDims the number of dimensions
     * @param value the borrowed array to populate
     * @return the populated array
     */
    public static BorrowedArray getCompactPlainValue(long addr, int type, int nDims, @NotNull BorrowedArray value) {
        final int dataSize = Unsafe.getUnsafe().getInt(addr);
        if (dataSize < 0) {
            value.ofNull();
            return value;
        }

        addr += Integer.BYTES;
        int shapeLen = nDims * Integer.BYTES;
        value.of(type, addr, addr + shapeLen, dataSize);
        return value;
    }

    /**
     * Calculates the size needed to store an array in compact format.
     * This is used by {@link io.questdb.griffin.engine.groupby.GroupByArraySink}.
     * <p>
     * The size includes:
     * <ul>
     *     <li>4 bytes for dataSize field</li>
     *     <li>N * 4 bytes for shape (dimension lengths)</li>
     *     <li>cardinality * elemSize bytes for array data</li>
     * </ul>
     * <p>
     * This method uses {@link ArrayView#getCardinality()} which works for all array types.
     *
     * @param value the array to calculate size for (must not be null)
     * @return the total size in bytes needed to store the array in compact format
     */
    public static long getCompactPlainValueSize(@NotNull ArrayView value) {
        long elemSize = ColumnType.sizeOf(ColumnType.decodeArrayElementType(value.getType()));
        long intBytes = Integer.BYTES;
        return intBytes + value.getDimCount() * intBytes + value.getCardinality() * elemSize;
    }

    public static BorrowedArray getPlainValue(long addr, @NotNull BorrowedArray value) {
        final long totalSize = Unsafe.getUnsafe().getLong(addr);
        addr += Long.BYTES;
        if (totalSize <= 0) {
            value.ofNull();
            return value;
        }
        final int type = Unsafe.getUnsafe().getInt(addr);
        addr += Integer.BYTES;
        int nDims = ColumnType.decodeArrayDimensionality(type);
        int shapeLen = nDims * Integer.BYTES;
        int headerLen = Integer.BYTES + shapeLen;
        value.of(type, addr, addr + shapeLen, (int) (totalSize - headerLen));
        return value;
    }

    public static long getPlainValueSize(long arrayAddress) {
        return Long.BYTES + Unsafe.getUnsafe().getLong(arrayAddress);
    }

    public static long getPlainValueSize(@NotNull ArrayView value) {
        return Long.BYTES + value.getVanillaMemoryLayoutSize();
    }

    @Override
    public void appendNull(MemoryA auxMem, MemoryA dataMem) {
        appendNullImpl(auxMem, dataMem);
    }

    @Override
    public long auxRowsToBytes(long rowCount) {
        return ARRAY_AUX_WIDTH_BYTES * rowCount;
    }

    @Override
    public void configureAuxMemMA(MemoryMA auxMem) {
        // no-op
    }

    @Override
    public void configureAuxMemMA(
            FilesFacade ff,
            MemoryMA auxMem,
            LPSZ fileName,
            long dataAppendPageSize,
            int memoryTag,
            int opts,
            int madviseOpts
    ) {
        auxMem.of(
                ff,
                fileName,
                dataAppendPageSize,
                -1,
                MemoryTag.MMAP_TABLE_WRITER,
                opts,
                madviseOpts
        );
    }

    @Override
    public void configureAuxMemO3RSS(MemoryARW auxMem) {
        // no-op
    }

    @Override
    public void configureAuxMemOM(
            FilesFacade ff,
            MemoryOM auxMem,
            long fd,
            LPSZ fileName,
            long rowLo,
            long rowHi,
            int memoryTag,
            int opts
    ) {
        auxMem.ofOffset(
                ff,
                fd,
                false,
                fileName,
                ARRAY_AUX_WIDTH_BYTES * rowLo,
                ARRAY_AUX_WIDTH_BYTES * rowHi,
                memoryTag,
                opts
        );
    }

    @Override
    public void configureDataMemOM(
            FilesFacade ff,
            MemoryR auxMem,
            MemoryOM dataMem,
            long dataFd,
            LPSZ fileName,
            long rowLo,
            long rowHi,
            int memoryTag,
            int opts
    ) {
        long lo = rowLo > 0 ? readDataOffset(auxMem, ARRAY_AUX_WIDTH_BYTES * rowLo) : 0;
        long hi = rowHi > 0 ? calcDataOffsetEnd(auxMem, ARRAY_AUX_WIDTH_BYTES * (rowHi - 1)) : 0;
        dataMem.ofOffset(
                ff,
                dataFd,
                false,
                fileName,
                lo,
                hi,
                memoryTag,
                opts
        );
    }

    @Override
    public long dedupMergeVarColumnSize(long mergeIndexAddr, long mergeIndexCount, long srcDataFixAddr, long srcOooFixAddr) {
        return Vect.dedupMergeArrayColumnSize(mergeIndexAddr, mergeIndexCount, srcDataFixAddr, srcOooFixAddr);
    }

    @Override
    public long getAuxVectorOffset(long row) {
        return getAuxVectorOffsetStatic(row);
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        return ARRAY_AUX_WIDTH_BYTES * storageRowCount;
    }

    @Override
    public long getDataVectorMinEntrySize() {
        return 0;
    }

    @Override
    public long getDataVectorOffset(long auxMemAddr, long row) {
        final long auxEntry = auxMemAddr + ARRAY_AUX_WIDTH_BYTES * row;
        return readDataOffset(auxEntry);
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        if (rowLo > rowHi) {
            return 0;
        }
        if (rowLo > 0) {
            return getDataVectorSizeAt(auxMemAddr, rowHi) - getDataVectorOffset(auxMemAddr, rowLo);
        }
        return getDataVectorSizeAt(auxMemAddr, rowHi);
    }

    @Override
    public long getDataVectorSizeAt(long auxMemAddr, long row) {
        return calcDataOffsetEnd(auxMemAddr + (ARRAY_AUX_WIDTH_BYTES * row));
    }

    @Override
    public long getDataVectorSizeAtFromFd(FilesFacade ff, long auxFd, long row) {
        if (row < 0) {
            return 0;
        }
        final long auxFileOffset = ARRAY_AUX_WIDTH_BYTES * row;
        final long offset = readLong(ff, auxFd, auxFileOffset) & OFFSET_MAX;
        final int size = readInt(ff, auxFd, auxFileOffset + Long.BYTES);
        return offset + size;
    }

    @Override
    public long getMinAuxVectorSize() {
        return 0;
    }

    @Override
    public boolean isSparseDataVector(long auxMemAddr, long dataMemAddr, long rowCount) {
        long lastSizeInDataVector = 0;
        for (int row = 0; row < rowCount; row++) {
            long offset = getDataVectorOffset(auxMemAddr, row);
            if (offset != lastSizeInDataVector) {
                // Swiss cheese hole in var col file
                return true;
            }
            lastSizeInDataVector = getDataVectorSizeAt(auxMemAddr, row);
        }
        return false;
    }

    @Override
    public long mergeShuffleColumnFromManyAddresses(long indexFormat, long primaryAddressList, long secondaryAddressList, long outPrimaryAddress, long outSecondaryAddress, long mergeIndex, long destDataOffset, long destDataSize) {
        return Vect.mergeShuffleArrayColumnFromManyAddresses(
                indexFormat,
                primaryAddressList,
                secondaryAddressList,
                outPrimaryAddress,
                outSecondaryAddress,
                mergeIndex,
                destDataOffset,
                destDataSize
        );
    }

    @Override
    public void o3ColumnMerge(
            long timestampMergeIndexAddr,
            long timestampMergeIndexCount,
            long srcAuxAddr1,
            long srcDataAddr1,
            long srcAuxAddr2,
            long srcDataAddr2,
            long dstAuxAddr,
            long dstDataAddr,
            long dstDataOffset
    ) {
        Vect.oooMergeCopyArrayColumn(
                timestampMergeIndexAddr,
                timestampMergeIndexCount,
                srcAuxAddr1,
                srcDataAddr1,
                srcAuxAddr2,
                srcDataAddr2,
                dstAuxAddr,
                dstDataAddr,
                dstDataOffset
        );
    }

    @Override
    public void o3copyAuxVector(
            FilesFacade ff,
            long srcAddr,
            long srcLo,
            long srcHi,
            long dstAddr,
            long dstFileOffset,
            long dstFd,
            boolean mixedIOFlag
    ) {
        O3Utils.o3Copy(
                ff,
                dstAddr,
                dstFileOffset,
                dstFd,
                srcAddr + (srcLo * ARRAY_AUX_WIDTH_BYTES),
                (srcHi - srcLo + 1) * ARRAY_AUX_WIDTH_BYTES,
                mixedIOFlag
        );
    }

    @Override
    public void o3sort(
            long sortedTimestampsAddr,
            long sortedTimestampsRowCount,
            MemoryCR srcDataMem,
            MemoryCR srcAuxMem,
            MemoryCARW dstDataMem,
            MemoryCARW dstAuxMem
    ) {
        // ensure we have enough memory allocated
        final long srcDataAddr = srcDataMem.addressOf(0);
        final long srcAuxAddr = srcAuxMem.addressOf(0);
        // exclude the trailing offset from shuffling
        final long tgtAuxAddr = dstAuxMem.resize(getAuxVectorSize(sortedTimestampsRowCount));
        final long tgtDataAddr = dstDataMem.resize(getDataVectorSizeAt(srcAuxAddr, sortedTimestampsRowCount - 1));

        assert srcAuxAddr != 0;
        assert tgtAuxAddr != 0;

        // add max offset so that we do not have conditionals inside loop
        final long offset = Vect.sortArrayColumn(
                sortedTimestampsAddr,
                sortedTimestampsRowCount,
                srcDataAddr,
                srcAuxAddr,
                tgtDataAddr,
                tgtAuxAddr
        );
        dstDataMem.jumpTo(offset);
        dstAuxMem.jumpTo(ARRAY_AUX_WIDTH_BYTES * sortedTimestampsRowCount);
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, MemoryMA dataMem, int columnType, long rowCount) {
        if (rowCount == 0) {
            auxMem.jumpTo(0);
            return 0;
        }

        // jump to the previous entry and calculate its data offset + data size
        auxMem.jumpTo(ARRAY_AUX_WIDTH_BYTES * (rowCount - 1));
        final long nextDataMemOffset = calcDataOffsetEnd(auxMem.getAppendAddress());

        // Jump to the end of file to correctly trim the file
        auxMem.jumpTo(ARRAY_AUX_WIDTH_BYTES * rowCount);
        return nextDataMemOffset;
    }

    @Override
    public long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem) {
        if (pos > 0) {
            // first we need to calculate already used space. both data and aux vectors.
            long auxVectorOffset = getAuxVectorOffset(pos - 1); // the last entry we are NOT overwriting
            auxMem.jumpTo(auxVectorOffset);
            long auxEntryPtr = auxMem.getAppendAddress();

            long dataVectorSize = calcDataOffsetEnd(auxEntryPtr);
            long auxVectorSize = getAuxVectorSize(pos);
            long totalDataSizeBytes = dataVectorSize + auxVectorSize;

            auxVectorOffset = getAuxVectorOffset(pos); // the entry we are about to overwrite with the next append
            // Jump to the end of file to correctly trim the file
            auxMem.jumpTo(auxVectorOffset);
            dataMem.jumpTo(dataVectorSize);
            return totalDataSizeBytes;
        }

        dataMem.jumpTo(0);
        auxMem.jumpTo(0);
        return 0;
    }

    @Override
    public void setDataVectorEntriesToNull(long dataMemAddr, long rowCount) {
        // this is a no-op, NULLs do not occupy space in the data vector
    }

    @Override
    public void setFullAuxVectorNull(long auxMemAddr, long rowCount) {
        Vect.setArrayColumnNullRefs(auxMemAddr, 0, rowCount);
    }

    @Override
    public void setPartAuxVectorNull(long auxMemAddr, long initialOffset, long columnTop) {
        Vect.setArrayColumnNullRefs(auxMemAddr, initialOffset, columnTop);
    }

    @Override
    public void shiftCopyAuxVector(long shift, long srcAddr, long srcLo, long srcHi, long dstAddr, long dstAddrSize) {
        // +1 since srcHi is inclusive
        assert (srcHi - srcLo + 1) * ARRAY_AUX_WIDTH_BYTES <= dstAddrSize;
        Vect.shiftCopyArrayColumnAux(shift, srcAddr, srcLo, srcHi, dstAddr);
    }

    private static void appendNullImpl(MemoryA auxMem, long offset) {
        assert auxMem != null;
        assert offset >= 0;
        assert offset < OFFSET_MAX;
        auxMem.putLong(offset);
        // size & padding
        auxMem.putLong(0);
    }

    private static void appendNullImpl(MemoryA auxMem, MemoryA dataMem) {
        final long offset = dataMem.getAppendOffset();
        appendNullImpl(auxMem, offset);
    }

    private static long appendToMemRecursive(@NotNull ArrayView value, int dim, int flatIndex, long appendAddress) {
        short elemType = value.getElemType();
        assert elemType == ColumnType.DOUBLE || elemType == ColumnType.LONG : "implemented only for long and double";

        final int count = value.getDimLen(dim);
        final int stride = value.getStride(dim);
        final boolean atDeepestDim = dim == value.getDimCount() - 1;
        if (atDeepestDim) {
            if (elemType == ColumnType.DOUBLE) {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(appendAddress, value.getDouble(flatIndex));
                    appendAddress += Double.BYTES;
                    flatIndex += stride;
                }
            } else {
                throw new UnsupportedOperationException("Unsupported array element type: " + elemType);
            }
        } else {
            for (int i = 0; i < count; i++) {
                appendAddress = appendToMemRecursive(value, dim + 1, flatIndex, appendAddress);
                flatIndex += stride;
            }
        }
        return appendAddress;
    }

    private static void arrayToText(
            @NotNull ArrayView array,
            int dim,
            int flatIndex,
            @NotNull CharSink<?> sink,
            @NotNull ArrayValueAppender appender,
            char openChar,
            char closeChar,
            @NotNull String nullLiteral,
            ArrayWriteState writeState
    ) {
        final int count = array.getDimLen(dim);
        final int stride = array.getStride(dim);
        final boolean atDeepestDim = dim == array.getDimCount() - 1;

        writeState.putCharIfNew(sink, openChar);

        if (atDeepestDim) {
            for (int i = 0; i < count; i++) {
                if (i != 0) {
                    writeState.putCharIfNew(sink, ',');
                }
                if (writeState.incAndSayIfNewOp()) {
                    appender.appendItemAtFlatIndex(array, flatIndex, sink, nullLiteral);
                    flatIndex += stride;
                    writeState.performedOp();
                } else {
                    flatIndex += stride;
                }
            }
        } else {
            for (int i = 0; i < count; i++) {
                if (i != 0) {
                    writeState.putCharIfNew(sink, ',');
                }
                arrayToText(array, dim + 1, flatIndex, sink, appender, openChar, closeChar, nullLiteral, writeState);
                flatIndex += stride;
            }
        }
        writeState.putCharIfNew(sink, closeChar);
    }

    private static void padTo(@NotNull MemoryA dataMem, int byteAlignment) {
        dataMem.zeroMem(bytesToSkipForAlignment(dataMem.getAppendOffset(), byteAlignment));
    }

    /**
     * Read the data offset from the aux entry that starts at the specified offset.
     */
    private static long readDataOffset(MemoryR auxMem, long offset) {
        return auxMem.getLong(offset) & OFFSET_MAX;
    }

    private static long readDataOffset(long auxEntryAddress) {
        return Unsafe.getUnsafe().getLong(auxEntryAddress) & OFFSET_MAX;
    }

    private static int readInt(FilesFacade ff, long fd, long offset) {
        long res = ff.readIntAsUnsignedLong(fd, offset);
        if (res < 0) {
            throw CairoException.critical(ff.errno())
                    .put("Invalid data read from array aux file [fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", fileSize=").put(ff.length(fd))
                    .put(", result=").put(res)
                    .put(']');
        }
        return Numbers.decodeLowInt(res);
    }

    private static long readLong(FilesFacade ff, long fd, long offset) {
        long res = ff.readNonNegativeLong(fd, offset);
        if (res < 0) {
            throw CairoException.critical(ff.errno())
                    .put("Invalid data read from array aux file [fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", fileSize=").put(ff.length(fd))
                    .put(", result=").put(res)
                    .put(']');
        }
        return res;
    }

    private static @NotNull ArrayValueAppender resolveAppender(@NotNull ArrayView array) {
        int elemType = array.getElemType();
        switch (elemType) {
            case ColumnType.DOUBLE:
                return VALUE_APPENDER_DOUBLE;
            case ColumnType.LONG:
            case ColumnType.NULL:
                return VALUE_APPENDER_LONG;
            case ColumnType.VARCHAR:
                return VALUE_APPENDER_VARCHAR;
            default:
                if (array.isEmpty()) {
                    return VALUE_APPENDER_LONG;
                }
                throw new AssertionError("No appender for ColumnType " + elemType);
        }
    }

    private static void writeAuxEntry(MemoryA auxMem, long offset, int size) {
        assert offset >= 0;
        assert offset <= OFFSET_MAX;
        assert size >= 0;
        auxMem.putLong(offset);
        auxMem.putLong(size);
    }

    /**
     * Write the values.
     **/
    private static void writeDataEntry(@NotNull MemoryA dataMem, @NotNull ArrayView array) {
        writeShape(dataMem, array);
        // We could be storing values of different datatypes.
        // We thus need to align accordingly. I.e., if we store doubles, we need to align
        // on an 8-byte boundary.
        // for shorts, it's on a 2-byte boundary. For booleans, we align to the byte.
        final int requiredByteAlignment = ColumnType.sizeOf(array.getElemType());
        padTo(dataMem, requiredByteAlignment);
        array.appendDataToMem(dataMem);
        // We pad at the end, ready for the next entry that starts with an int.
        padTo(dataMem, Integer.BYTES);
    }

    /**
     * Write the dimensions.
     */
    private static void writeShape(@NotNull MemoryA dataMem, @NotNull ArrayView array) {
        assert dataMem.getAppendOffset() % Integer.BYTES == 0; // aligned integer write
        array.appendShapeToMem(dataMem);
    }

    /**
     * Given a <code>auxAddr</code> pointer to a specific aux entry,
     * return the data offset end, in other words the offset in the data vector
     * to the start of the next entry following the one pointed to by
     * <code>auxAddr</code>.
     */
    private long calcDataOffsetEnd(long auxAddr) {
        final long offset = Unsafe.getUnsafe().getLong(auxAddr) & OFFSET_MAX;
        final int size = Unsafe.getUnsafe().getInt(auxAddr + Long.BYTES);
        return offset + size;
    }

    private long calcDataOffsetEnd(@NotNull MemoryR mem, long auxOffset) {
        final long offset = mem.getLong(auxOffset) & OFFSET_MAX;
        final int size = mem.getInt(auxOffset + Long.BYTES);
        return offset + size;
    }

    static void appendLongFromArrayToSink(@NotNull ArrayView array, int index, @NotNull CharSink<?> sink, @NotNull String nullLiteral) {
        long d = array.getLong(index);
        if (d == Numbers.LONG_NULL) {
            // currently, this branch shouldn't be hit, as dimensions are non-null
            // when we add long arrays, it becomes important
            sink.put(nullLiteral);
        } else {
            // todo: wtf is this?
            sink.put(d);
        }
    }

    static void appendVarcharFromArrayToSink(@NotNull ArrayView array, int index, @NotNull CharSink<?> sink, @NotNull String nullLiteral) {
        Utf8Sequence utf8Sequence = array.getVarchar(index);
        if (utf8Sequence == null) {
            sink.put(nullLiteral);
        } else {
            sink.put(utf8Sequence);
        }
    }

    @FunctionalInterface
    public interface ArrayValueAppender {
        void appendItemAtFlatIndex(@NotNull ArrayView array, int index, @NotNull CharSink<?> sink, @NotNull String nullLiteral);
    }
}
