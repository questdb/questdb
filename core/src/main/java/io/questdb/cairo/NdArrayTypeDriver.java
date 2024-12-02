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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryOM;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ndarr.NdArrayMeta;
import io.questdb.std.ndarr.NdArrayView;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reads and writes arrays.
 * <p>Arrays are organised as such</p>
 * <h1>AUX entries</h1>
 * <pre>
 * 128-bit fixed size entries
 *     * auxHi ======
 *         * manydims_and_dim0: int =======
 *             * bit 0 to =26: dim0: 27-bit unsigned integer
 *                 * the first dimension of the array,
 *                 * or `0` if null.
 *             * bits 27 to =30: unused
 *             * bit 31: manydims: 1-bit bool
 *                 * if `1`, this nd array has more than two dimensions
 *         * dim1_and_format: int =======
 *             * bits 0 to =26: dim1: 27-bit unsigned integer
 *                 * the second dimension of the array
 *                 * or `0` if a 1D array.
 *             * bits 27 to =31: unused, reserved for format use (in case we want to support sparse arrays)
 *     * auxLo ======
 *         * offset_and_hash: long ======
 *             * bits 0 to =47: offset: 48-bit unsigned integer
 *                 * byte count offset into the data vector
 *             * bits 48 to =64: hash: 16-bit
 *                 * CRC-16/XMODEM hash used to speed up equality comparisons
 * </pre>
 * <p><strong>Special NULL value encoding:</strong> All bits of the entry are 0.</p>
 * <h1>Data vector</h1>
 * <pre>
 * variable length encoding, starting at the offset specified in the `aux` entry.
 *     * A sequence of extra dimensions.
 *         * padding:
 *             * enough padding to satisfy the datatype alignment requirements for `int` (i.e. on a 4 byte boundary).
 *         * Optional, present if more than 2 dimensions (see `manydims` field in aux)
 *         * Each dimension is:
 *             * last marker: 1-bit
 *                 * if set to 1, then this is the last additional dimension
 *             * number: 31-bits unsigned integer
 *     * padding:
 *         * enough padding to satisfy the datatype alignment requirements.
 *         * e.g. for 64-bit numeric types ensures that the following section
 *           starts on an 8 byte boundary, for a 32-bit type, on a 4 byte boundary.
 *           This is to avoid unaligned data reads later.
 *           See the following <a
 *           href="https://medium.com/@jkstoyanov/aligned-and-unaligned-memory-access-9b5843b7f4ac"
 *           >blog post</a>.
 *     * raw values buffer
 *         * a buffer of bytes, containing the values in row-major order.
 *           E.g. for the 2x3 array
 *               {{1, 2, 3},
 *                {4, 5, 6}}
 *           The numbers are recorded in the order 1, 2, 3, 4, 5, 6.
 *         * its size is calculated as:
 *               bits_per_elem = pow(2, ColumnType.getNdArrayElementTypePrecision(type))
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
 * </pre>
 */
public class NdArrayTypeDriver implements ColumnTypeDriver {
    public static final NdArrayTypeDriver INSTANCE = new NdArrayTypeDriver();
    public static final int ND_ARRAY_AUX_WIDTH_BYTES = 2 * Long.BYTES;
    private static final long MAX_OFFSET = 1L << 48 - 1L;

    public static void appendValue(@NotNull MemoryA auxMem, @NotNull MemoryA dataMem, @Nullable NdArrayView array) {
        if (array == null) {
            NdArrayTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            return;
        }

        final long offset = writeDataEntry(dataMem, array);
        writeAuxEntry(auxMem, array, offset);

        if (NdArrayMeta.isDefaultStrides(array.getShape(), array.getStrides())) {

        } else {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    private static long writeDataEntry(@NotNull MemoryA dataMem, @Nullable NdArrayView array) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private static void writeAuxEntry(MemoryA auxMem, @NotNull NdArrayView array, long offset) {
        final DirectIntSlice shape = array.getShape();
        final int nDims = array.getShape().length();

        // The fields we will hold in the aux header.
        final boolean manyDims = nDims > 2;
        final int dim0 = shape.get(0);
        final int dim1 = nDims >= 2 ? shape.get(1) : 0;
        assert dim0 <= NdArrayMeta.MAX_DIM_SIZE;
        assert dim1 <= NdArrayMeta.MAX_DIM_SIZE;
        assert offset <= MAX_OFFSET;
        final short crc = array.getCrc();

        // Encoded into 128 bits.
        final long auxHiHi = (manyDims ? (1L << 31) : 0) | dim0;
        final long auxHi = (auxHiHi << 32) | dim1;
        final long auxLo = (((long) crc) << 48) | offset;
        auxMem.putLong128(auxLo, auxHi);
    }

    @Override
    public void appendNull(MemoryA auxMem, MemoryA dataMem) {
        auxMem.putLong128(0, 0);
    }

    @Override
    public long auxRowsToBytes(long rowCount) {
        return ND_ARRAY_AUX_WIDTH_BYTES * rowCount;
    }

    @Override
    public void configureAuxMemMA(MemoryMA auxMem) {
        // no-op
    }

    @Override
    public void configureAuxMemMA(FilesFacade ff, MemoryMA auxMem, LPSZ fileName, long dataAppendPageSize, int memoryTag, long opts, int madviseOpts) {
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
    public void configureAuxMemOM(FilesFacade ff, MemoryOM auxMem, long fd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, long opts) {
        auxMem.ofOffset(
                ff,
                fd,
                fileName,
                ND_ARRAY_AUX_WIDTH_BYTES * rowLo,
                ND_ARRAY_AUX_WIDTH_BYTES * rowHi,
                memoryTag,
                opts
        );
    }

    @Override
    public void configureDataMemOM(FilesFacade ff, MemoryR auxMem, MemoryOM dataMem, long dataFd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, long opts) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long dedupMergeVarColumnSize(long mergeIndexAddr, long mergeIndexCount, long srcDataFixAddr, long srcOooFixAddr) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getAuxVectorOffset(long row) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getDataVectorMinEntrySize() {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getDataVectorOffset(long auxMemAddr, long row) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getDataVectorSizeAt(long auxMemAddr, long row) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getDataVectorSizeAtFromFd(FilesFacade ff, long auxFd, long row) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getMinAuxVectorSize() {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void o3ColumnMerge(long timestampMergeIndexAddr, long timestampMergeIndexCount, long srcAuxAddr1, long srcDataAddr1, long srcAuxAddr2, long srcDataAddr2, long dstAuxAddr, long dstDataAddr, long dstDataOffset) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void o3copyAuxVector(FilesFacade ff, long srcAddr, long srcLo, long srcHi, long dstAddr, long dstFileOffset, long dstFd, boolean mixedIOFlag) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void o3sort(long sortedTimestampsAddr, long sortedTimestampsRowCount, MemoryCR srcDataMem, MemoryCR srcAuxMem, MemoryCARW dstDataMem, MemoryCARW dstAuxMem) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, long rowCount) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void setDataVectorEntriesToNull(long dataMemAddr, long rowCount) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void setFullAuxVectorNull(long auxMemAddr, long rowCount) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void setPartAuxVectorNull(long auxMemAddr, long initialOffset, long columnTop) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public void shiftCopyAuxVector(long shift, long src, long srcLo, long srcHi, long dstAddr, long dstAddrSize) {
        throw new UnsupportedOperationException("nyi");
    }
}
