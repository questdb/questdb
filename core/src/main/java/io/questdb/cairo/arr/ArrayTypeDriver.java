/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.O3Utils;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryOM;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reads and writes arrays. Arrays are organised as follows:
 * <h1>AUX entries</h1>
 *
 * <h2>Data Offset Handling</h2>
 * <p>
 * Like the <code>VARCHAR</code> type, <code>ARRAY</code> uses <code>N</code>
 * (not <code>N + 1</code>) entries in the AUX table.
 *
 * <h2>AUX entry format</h2>
 *
 * <strong>IMPORTANT!</strong>: Since we store 96 bit entries, every other entry
 * is unaligned for reading via <code>Unsafe.getLong()</code>, as such if you find
 * any <code>{MemoryR,Unsafe}.{get,set}Long</code> calls operating on the aux data in
 * this code, it's probably a bug! See
 * <a href="https://medium.com/@jkstoyanov/aligned-and-unaligned-memory-access-9b5843b7f4ac">blog post</a>.
 *
 * <pre>
 * 96-bit entries
 *     * offset_and_hash: 64 bits
 *         * bits 0 to =47: offset, a 48-bit unsigned integer
 *             * byte-level offset into the data vector
 *         * bits 48 to =64: hash, 16-bit
 *             * CRC-16/XMODEM hash used to speed up equality comparisons
 *     * data_size: 32 bits
 *         * number of bytes used to the store the array (along with any additional metadata) in the data vector.
 * </pre>
 *
 * <h2>Encoding NULLs</h2>
 * <ul>
 *     <li>A null value has zero size.</li>
 *     <li>The CRC of a null array is 0, so <code>auxLo &gt;&gt; CRC16_SHIFT == 0</code></li>
 *     <li>We however <em>do</em> populate the <code>offset</code> field with
 *     the end of the previous non-null value.</li>
 *     <li>This allows mapping the data vector for a specific range of values.</li>
 * </ul>
 *
 * <h2>Data vector</h2>
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
    public static final int CRC16_SHIFT = 48;
    public static final ArrayTypeDriver INSTANCE = new ArrayTypeDriver();
    public static final long OFFSET_MAX = (1L << 48) - 1L;
    private static final long U32_MASK = 0xFFFFFFFFL;

    public static void appendValue(
            @NotNull MemoryA auxMem,
            @NotNull MemoryA dataMem,
            @Nullable ArrayView array
    ) {
        if (array == null) {
            appendNullImpl(auxMem, dataMem);
            return;
        }

        final long beginOffset = dataMem.getAppendOffset();
        writeDataEntry(dataMem, array);
        final long endOffset = dataMem.getAppendOffset();
        final int size = (int) (endOffset - beginOffset);
        writeAuxEntry(auxMem, beginOffset, size);
    }

    /**
     * Recursively builds a JSON string representation of a multi-dimensional array stored in row‑major order.
     *
     * @param valueAppender the function used to append the JSON representation of a single element
     * @param arrayView     the flat array containing all the elements
     * @param sink          the StringBuilder used to accumulate the JSON string
     */
    public static void arrayToJson(
            JsonValueAppender valueAppender,
            ArrayView arrayView,
            CharSink<?> sink
    ) {
        arrayToJson(valueAppender, arrayView, 0, 0, sink, '[', ']');
    }

    /**
     * Recursively builds a JSON string representation of a multi-dimensional array stored in row‑major order.
     *
     * @param valueAppender the function used to append the JSON representation of a single element
     * @param arrayView     the flat array containing all the elements
     * @param dim           the current dimension being processed, starting at 0
     * @param currentIndex  the index of the current element in the flat array, starting at 0
     * @param sink          the StringBuilder used to accumulate the JSON string
     * @param bracketLo     the opening bracket for the current dimension, e.g. '[' for each dimension
     * @param bracketHi     the closing bracket for the current dimension, e.g. ']' for each dimension
     */
    public static int arrayToJson(
            JsonValueAppender valueAppender,
            ArrayView arrayView,
            int dim,
            int currentIndex,
            CharSink<?> sink,
            char bracketLo,
            char bracketHi
    ) {
        sink.putAscii(bracketLo);
        int count = arrayView.getDimLength(dim); // Number of elements or subarrays at this dimension.
        for (int i = 0; i < count; i++) {
            if (dim == arrayView.getDim() - 1) {
                // If we're at the last dimension, append the flat array element.
                valueAppender.appendFromIndex(arrayView, sink, currentIndex);
                currentIndex++; // Move to the next element in the flat array.
            } else {
                // Recursively build the JSON for the next dimension.
                currentIndex = arrayToJson(valueAppender, arrayView, dim + 1, currentIndex, sink, bracketLo, bracketHi);
            }
            // Append a comma if this is not the last element in the current dimension.
            if (i < count - 1) {
                sink.putAscii(',');
            }
        }
        sink.putAscii(bracketHi);
        return currentIndex;
    }

    public static long getAuxVectorOffsetStatic(long row) {
        return ARRAY_AUX_WIDTH_BYTES * row;
    }

    public static long getIntAlignedLong(@NotNull MemoryR mem, long offset) {
        final int lower = mem.getInt(offset);
        final int upper = mem.getInt(offset + Integer.BYTES);
        return ((long) upper << 32) | (lower & U32_MASK);
    }

    public static long getIntAlignedLong(long address) {
        final int lower = Unsafe.getUnsafe().getInt(address);
        final int upper = Unsafe.getUnsafe().getInt(address + Integer.BYTES);
        return ((long) upper << 32) | (lower & U32_MASK);
    }

    /**
     * Number of bytes to skip to find the next aligned address/offset.
     */
    public static int skipsToAlign(long unaligned, int byteAlignment) {
        final int pastBy = (int) (unaligned % byteAlignment);
        if (pastBy == 0) {
            return 0;
        }

        // The number of bytes to skip is the complement of how many we're past by.
        return byteAlignment - pastBy;
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
            long opts,
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
            long opts
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
            long opts
    ) {
        long lo;
        if (rowLo > 0) {
            lo = readDataOffset(auxMem, ARRAY_AUX_WIDTH_BYTES * rowLo);
        } else {
            lo = 0;
        }
        long hi = calcDataOffsetEnd(auxMem, ARRAY_AUX_WIDTH_BYTES * (rowHi - 1));
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
        // todo: impl
        throw new UnsupportedOperationException("nyi");
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
        long auxEntry = auxMemAddr + ARRAY_AUX_WIDTH_BYTES * row;
        final long offset = readDataOffset(auxMemAddr);
        final int size = Unsafe.getUnsafe().getInt(auxEntry + Long.BYTES);
        assert size != 0;
        return offset;
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
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getMinAuxVectorSize() {
        return 0;
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
        // todo: impl
        throw new UnsupportedOperationException("nyi");
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
        // todo: impl
        throw new UnsupportedOperationException("nyi");
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
        // todo: impl
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void setPartAuxVectorNull(long auxMemAddr, long initialOffset, long columnTop) {
        // todo: impl
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void shiftCopyAuxVector(long shift, long srcAddr, long srcLo, long srcHi, long dstAddr, long dstAddrSize) {
        // +1 since srcHi is inclusive
        assert (srcHi - srcLo + 1) * ARRAY_AUX_WIDTH_BYTES <= dstAddrSize;
        O3Utils.shiftCopyVarcharColumnAux(
                shift,
                srcAddr,
                srcLo,
                srcHi,
                dstAddr
        );
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

    private static void padTo(@NotNull MemoryA dataMem, int byteAlignment) {
        dataMem.zeroMem(skipsToAlign(dataMem.getAppendOffset(), byteAlignment));
    }

    /**
     * Read the data offset from the aux entry that starts at the specified offset.
     */
    private static long readDataOffset(MemoryR auxMem, long offset) {
        return auxMem.getLong(offset) & OFFSET_MAX;
    }

    private static long readDataOffset(long address) {
        return getIntAlignedLong(address) & OFFSET_MAX;
    }

    private static void writeAuxEntry(MemoryA auxMem, long offset, int size) {
        assert offset >= 0;
        assert offset <= OFFSET_MAX;
        assert size >= 0;
        final long crcAndOffset = ((long) (short) 0 << CRC16_SHIFT) | offset;
        auxMem.putLong(crcAndOffset);
        auxMem.putInt(size);
        auxMem.putInt(0);
    }

    /**
     * Write the values and -- while doing so, also calculate the crc value, unless it was already cached.
     **/
    private static void writeDataEntry(@NotNull MemoryA dataMem, @NotNull ArrayView arrayView) {
        writeShape(dataMem, arrayView);
        // We could be storing values of different datatypes.
        // We thus need to align accordingly. I.e., if we store doubles, we need to align on an 8-byte boundary.
        // for shorts, it's on a 2-byte boundary. For booleans, we align to the byte.
        final int bitWidth = 1 << ColumnType.decodeArrayElementTypePrecision(arrayView.getType());
        final int requiredByteAlignment = (bitWidth + 7) / 8;
        padTo(dataMem, requiredByteAlignment);
        arrayView.appendRowMajor(dataMem);
        // We pad at the end, ready for the next entry that starts with an int.
        padTo(dataMem, Integer.BYTES);
    }

    /**
     * Write the dimensions.
     */
    private static void writeShape(@NotNull MemoryA dataMem, @NotNull ArrayView arrayView) {
        assert dataMem.getAppendOffset() % Integer.BYTES == 0; // aligned integer write
        int dim = arrayView.getDim();
        for (int i = 0; i < dim; ++i) {
            dataMem.putInt(arrayView.getDimLength(i));
        }
    }

    /**
     * Given a <code>auxAddr</code> pointer to a specific aux entry,
     * return the data offset end, in other words the offset in the data vector
     * to the start of the next entry following the one pointed to by
     * <code>auxAddr</code>.
     */
    private long calcDataOffsetEnd(long auxAddr) {
        final long offset = getIntAlignedLong(auxAddr) & OFFSET_MAX;
        final int size = Unsafe.getUnsafe().getInt(auxAddr + Long.BYTES);
        return offset + size;
    }

    private long calcDataOffsetEnd(@NotNull MemoryR mem, long auxOffset) {
        final long offset = getIntAlignedLong(mem, auxOffset) & OFFSET_MAX;
        final int size = mem.getInt(auxOffset + Long.BYTES);
        return offset + size;
    }
}
