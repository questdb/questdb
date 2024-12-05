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
import io.questdb.std.CRC16XModem;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.ndarr.NdArrayMeta;
import io.questdb.std.ndarr.NdArrayRowMajorTraversal;
import io.questdb.std.ndarr.NdArrayValuesSlice;
import io.questdb.std.ndarr.NdArrayView;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

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
 *         * Each dimension is: 32-bit int
 *             * last marker: 1-bit (most significant bit)
 *                 * if set to 1, then this is the last additional dimension
 *              * 3 bits unused.
 *             * number: 27-bits unsigned integer (lowest bits)
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
    private static final int CRC16_MASK = 0xFFFF;
    private static final int DIM0_SHIFT = 32;
    private static final long MANYDIMS_BIT = 1L << 63;
    private static final long OFFSET_MAX = (1L << 48) - 1L;
    private static final int CRC16_SHIFT = 48;
    private static final ThreadLocal<DirectIntList> SHAPE = new ThreadLocal<>(NdArrayTypeDriver::newShape);
    public static final Closeable THREAD_LOCAL_CLEANER = NdArrayTypeDriver::clearThreadLocals;

    public static void appendValue(@NotNull MemoryA auxMem, @NotNull MemoryA dataMem, @Nullable NdArrayView array) {
        if ((array == null) || array.isNull()) {
            NdArrayTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            return;
        }

        final long crcAndOffset = writeDataEntry(dataMem, array);
        writeAuxEntry(auxMem, array, crcAndOffset);
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
//        long lo;
//        if (rowLo > 0) {
//            lo = getDataOffset(auxMem, VARCHAR_AUX_WIDTH_BYTES * rowLo);
//        } else {
//            lo = 0;
//        }
//        long hi = getDataVectorSize(auxMem, VARCHAR_AUX_WIDTH_BYTES * (rowHi - 1));
//        dataMem.ofOffset(
//                ff,
//                dataFd,
//                fileName,
//                lo,
//                hi,
//                memoryTag,
//                opts
//        );
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
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, MemoryMA dataMem, int columnType, long rowCount) {
        if (rowCount == 0) {
            auxMem.jumpTo(0);
            return 0;
        }
        // jump to the previous entry and calculate its data offset + data size
        auxMem.jumpTo(ND_ARRAY_AUX_WIDTH_BYTES * (rowCount - 1));
        final long nextDataMemOffset = getEntryDataOffsetEnd(auxMem.getAppendAddress(), dataMem, columnType);

        // Jump to the end of file to correctly trim the file
        auxMem.jumpTo(ND_ARRAY_AUX_WIDTH_BYTES * rowCount);
        return nextDataMemOffset;
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

    private static void clearThreadLocals() {
        SHAPE.close();
    }

    private static short initCrc16FromShape(@NotNull DirectIntSlice shape) {
        short checksum = CRC16XModem.updateInt(CRC16XModem.init(), shape.length());
        for (int dimIndex = 0, nDims = shape.length(); dimIndex < nDims; ++dimIndex) {
            final int dim = shape.get(dimIndex);
            checksum = CRC16XModem.updateInt(checksum, dim);
        }
        return checksum;
    }

    /**
     * Determine if the values buffer's values inside the array are all byte aligned.
     */
    private static boolean isByteAligned(int bitWidth, @NotNull NdArrayView array) {
        return array.getValuesOffset() * bitWidth % 8 == 0;
    }

    private static DirectIntList newShape() {
        return new DirectIntList(8, MemoryTag.NATIVE_ND_ARRAY);
    }

    private static void padTo(@NotNull MemoryA dataMem, long byteAlignment) {
        final long requiredPadding = dataMem.getAppendOffset() % byteAlignment;
        for (long paddingIndex = 0; paddingIndex < requiredPadding; ++paddingIndex) {
            dataMem.putByte((byte) 0);
        }
    }

    private static void writeAuxEntry(MemoryA auxMem, @NotNull NdArrayView array, long crcAndOffset) {
        final DirectIntSlice shape = array.getShape();
        final int nDims = array.getShape().length();

        // The fields we will hold in the aux header.
        final boolean manyDims = nDims > 2;
        final int dim0 = shape.get(0);
        final int dim1 = nDims >= 2 ? shape.get(1) : 0;
        assert dim0 <= NdArrayMeta.DIM_MAX_SIZE;
        assert dim1 <= NdArrayMeta.DIM_MAX_SIZE;

        // Encoded into 128 bits.
        final long auxHi = ((manyDims ? MANYDIMS_BIT : 0L) | ((long) dim0 << DIM0_SHIFT)) | dim1;
        auxMem.putLong128(crcAndOffset, auxHi);
    }

    /**
     * Write the values and -- while doing so, also calculate the crc value, unless it was already cached.
     **/
    private static long writeDataEntry(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        final long offset = writeExtraDims(dataMem, array);
        final short crc = writeValues(dataMem, array);
        return (((long) crc) << CRC16_SHIFT) | offset;
    }

    /**
     * Write the additional dimensions and return the starting offset that we will hold in the aux entry.
     */
    private static long writeExtraDims(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {

        if (array.getShape().length() <= 2) {
            return dataMem.getAppendOffset();
        }

        // If we have extra dims, we first need to pad for integer access.
        padTo(dataMem, Integer.BYTES);

        final long offset = dataMem.getAppendOffset();

        final DirectIntSlice shape = array.getShape();
        for (int dimIndex = 2, nDims = shape.length(); dimIndex < nDims; ++dimIndex) {
            final int dim = shape.get(dimIndex);
            final boolean isLast = dimIndex == nDims - 1;
            if (isLast) {
                dataMem.putInt(dim * -1);  // the last value is stored negative as marker that there's no more.
            } else {
                dataMem.putInt(dim);
            }
        }

        return offset;
    }

    private static short writeFlatValueBytes(@NotNull MemoryA dataMem, @NotNull NdArrayView array, int bitWidth, NdArrayValuesSlice values) {
        final int bytesToSkip = array.getValuesOffset() * bitWidth / 8;
        final short cachedCrc = array.getCachedCrc();
        if (cachedCrc != 0) {
            // Pre-computed checksum.
            // Most optimised path - works for all types.
            dataMem.putBlockOfBytes(values.ptr() + bytesToSkip, values.size() - bytesToSkip);
            return cachedCrc;
        } else {
            // We can still copy the buffer without strides, but we also need to
            // compute the CRC as we copy the bytes one by one.
            // We do this intrusively to avoid two passes of the same data.
            short crc = initCrc16FromShape(array.getShape());
            for (int index = 0, size = values.size() - bytesToSkip; index < size; ++index) {
                final byte value = values.getByte(index);
                crc = CRC16XModem.update(crc, value);
            }
            return CRC16XModem.finalize(crc);
        }
    }

    private static short writeStrided1ByteValues(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        NdArrayRowMajorTraversal t = NdArrayRowMajorTraversal.LOCAL.get().of(array);
        DirectIntSlice coordinates;
        final short cachedCrc = array.getCachedCrc();
        if (cachedCrc != 0) {
            while ((coordinates = t.next()) != null) {
                final byte value = array.getByte(coordinates);
                dataMem.putByte(value);
            }
            return cachedCrc;
        } else {
            short crc = initCrc16FromShape(array.getShape());
            while ((coordinates = t.next()) != null) {
                final byte value = array.getByte(coordinates);
                crc = CRC16XModem.update(crc, value);
                dataMem.putByte(value);
            }
            return CRC16XModem.finalize(crc);
        }
    }

    private static short writeStrided2ByteValues(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        NdArrayRowMajorTraversal t = NdArrayRowMajorTraversal.LOCAL.get().of(array);
        DirectIntSlice coordinates;
        final short cachedCrc = array.getCachedCrc();
        if (cachedCrc != 0) {
            while ((coordinates = t.next()) != null) {
                final short value = array.getShort(coordinates);
                dataMem.putShort(value);
            }
            return cachedCrc;
        } else {
            short crc = initCrc16FromShape(array.getShape());
            while ((coordinates = t.next()) != null) {
                final short value = array.getShort(coordinates);
                crc = CRC16XModem.updateShort(crc, value);
                dataMem.putShort(value);
            }
            return CRC16XModem.finalize(crc);
        }
    }

    /**
     * Write INT or FLOAT -- works for both since we're just doing bit-wise copies.
     */
    private static short writeStrided4ByteValues(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        NdArrayRowMajorTraversal t = NdArrayRowMajorTraversal.LOCAL.get().of(array);
        DirectIntSlice coordinates;
        final short cachedCrc = array.getCachedCrc();
        if (cachedCrc != 0) {
            while ((coordinates = t.next()) != null) {
                // N.B. this will work for FLOAT too, since we're just performing a bitwise copy.
                final int value = array.getInt(coordinates);
                dataMem.putInt(value);
            }
            return cachedCrc;
        } else {
            short crc = initCrc16FromShape(array.getShape());
            while ((coordinates = t.next()) != null) {
                final int value = array.getInt(coordinates);
                crc = CRC16XModem.updateInt(crc, value);
                dataMem.putInt(value);
            }
            return CRC16XModem.finalize(crc);
        }
    }

    /**
     * Write LONG or DOUBLE -- works for both since we're just doing bit-wise copies.
     */
    private static short writeStrided8ByteValues(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        NdArrayRowMajorTraversal t = NdArrayRowMajorTraversal.LOCAL.get().of(array);
        DirectIntSlice coordinates;
        final short cachedCrc = array.getCachedCrc();
        if (cachedCrc != 0) {
            while ((coordinates = t.next()) != null) {
                final long value = array.getLong(coordinates);
                dataMem.putLong(value);
            }
            return cachedCrc;
        } else {
            short crc = initCrc16FromShape(array.getShape());
            while ((coordinates = t.next()) != null) {
                final long value = array.getLong(coordinates);
                crc = CRC16XModem.updateLong(crc, value);
                dataMem.putLong(value);
            }
            return CRC16XModem.finalize(crc);
        }
    }

    private static short writeStridedBooleanValues(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        // Accumulate the bits into bytes, then write them.
        byte buf = 0;
        int bitIndex = 0;
        NdArrayRowMajorTraversal t = NdArrayRowMajorTraversal.LOCAL.get().of(array);
        DirectIntSlice coordinates;
        final short cachedCrc = array.getCachedCrc();
        if (cachedCrc != 0) {
            while ((coordinates = t.next()) != null) {
                final byte value = array.getBoolean(coordinates) ? (byte) 1 : (byte) 0;
                buf |= (byte) (value << bitIndex);
                ++bitIndex;
                if (bitIndex >= 8) {
                    dataMem.putByte(buf);
                    bitIndex = 0;
                }
            }
            // If there's an incompletely written byte, write it.
            if (bitIndex > 0) {
                dataMem.putByte(buf);
            }
            return cachedCrc;
        } else {
            short crc = initCrc16FromShape(array.getShape());
            while ((coordinates = t.next()) != null) {
                final byte value = array.getBoolean(coordinates) ? (byte) 1 : (byte) 0;
                buf |= (byte) (value << bitIndex);
                ++bitIndex;
                if (bitIndex >= 8) {
                    crc = CRC16XModem.update(crc, buf);
                    dataMem.putByte(buf);
                    bitIndex = 0;
                }
            }
            // If there's an incompletely written byte, write it.
            if (bitIndex > 0) {
                crc = CRC16XModem.update(crc, buf);
                dataMem.putByte(buf);
            }
            return CRC16XModem.finalize(crc);
        }
    }

    private static short writeStridedByteAlignedValues(int byteWidth, @NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        // Yes, the code would be shorter written by striding first, then switching on type,
        // but this way we avoid conditionals inside a loop.
        switch (byteWidth) {
            case 1:  // BYTE
                return writeStrided1ByteValues(dataMem, array);
            case 2: // SHORT
                return writeStrided2ByteValues(dataMem, array);
            case 4:  // INT & FLOAT
                return writeStrided4ByteValues(dataMem, array);
            case 8:  // LONG & DOUBLE
                return writeStrided8ByteValues(dataMem, array);
            default:
                throw new UnsupportedOperationException("unsupported byte width: " + byteWidth);
        }
    }

    /**
     * Writes the values and returns the CRC16 value
     */
    private static short writeValues(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        // We could be storing values of different datatypes.
        // We thus need to align accordingly. I.e., if we store doubles, we need to align on an 8-byte boundary.
        // for shorts, it's on a 2-byte boundary. For booleans, we align to the byte.
        final int bitWidth = 1 << ColumnType.getNdArrayElementTypePrecision(array.getType());
        final int requiredByteAlignment = Math.max(1, bitWidth / 8);
        padTo(dataMem, requiredByteAlignment);

        final NdArrayValuesSlice values = array.getValues();
        if (array.hasDefaultStrides() && isByteAligned(bitWidth, array)) {
            return writeFlatValueBytes(dataMem, array, bitWidth, values);
        } else if (bitWidth == 1) {
            return writeStridedBooleanValues(dataMem, array);
        } else {
            assert bitWidth >= 8;
            return writeStridedByteAlignedValues(bitWidth / 8, dataMem, array);
        }
    }

    /**
     * Given a <code>auxAddr</code> pointer to a specific aux entry,
     * return the data offset end, in other words the offset in the data vector
     * to the start of the next entry following the one pointed to by
     * <code>auxAddr</code>.
     */
    private long getEntryDataOffsetEnd(long auxAddr, MemoryMA dataMem, int columnType) {
        final long auxLo = Unsafe.getUnsafe().getLong(auxAddr);
        final long auxHi = Unsafe.getUnsafe().getLong(auxAddr + Long.BYTES);
        final boolean manyDims = (auxHi & MANYDIMS_BIT) != 0;
        final int dim0 = (int) (auxHi >>> DIM0_SHIFT) & NdArrayMeta.DIM_MAX_SIZE;
        final int dim1 = (int) (auxHi & NdArrayMeta.DIM_MAX_SIZE);
        final short crc = (short) ((auxLo >>> CRC16_SHIFT) & CRC16_MASK);
        final long offset = auxLo & OFFSET_MAX;
        if (dim0 == 0) {
            // a NULL entry, nothing associated with it in the `dataMem`.
            return offset;
        } else if (manyDims) {
            final long origOffset = dataMem.getAppendOffset();
            try {
                // We first need to read all our remaining dimensions.
                dataMem.jumpTo(offset);
                final DirectIntSlice shape = readManyDimsShape(dataMem.getAppendAddress(), dim0, dim1);

                final int dimsBytes = shape.length() * Integer.BYTES;
                dataMem.jumpTo(offset + dimsBytes);

                // Figure out how many bytes of padding have been inserted.
                final int bitWidth = 1 << ColumnType.getNdArrayElementTypePrecision(columnType);
                final int requiredByteAlignment = Math.max(1, bitWidth / 8);
                final int paddingByteCount = (int) (dataMem.getAppendOffset() % requiredByteAlignment);

                final int elementCount = NdArrayMeta.flatLength(shape);
                final int valuesSize = NdArrayMeta.calcRequiredValuesByteSize(columnType, elementCount);

                return offset + dimsBytes + paddingByteCount + valuesSize;
            } finally {
                dataMem.jumpTo(origOffset);
            }
        } else {
            // N.B., since we don't have extra dims with different `int` alignment requirements,
            // the offset recorded in the `aux` entry includes any additional alignment padding
            // as was recorded during insertion.

            // We have up to two dimensions, product them to find the count.
            final int elementCount = dim0 * (dim1 > 0 ? dim1 : 1);
            final int valuesSize = NdArrayMeta.calcRequiredValuesByteSize(columnType, elementCount);
            return offset + valuesSize;
        }
    }

    private DirectIntSlice readManyDimsShape(long readAddr, int dim0, int dim1) {
        DirectIntList shape = SHAPE.get();
        shape.clear();
        shape.add(dim0);
        shape.add(dim1);
        for (; ; ) {
            final int dim = Unsafe.getUnsafe().getInt(readAddr);
            if (dim > 0) {
                shape.add(dim);
            } else {
                // if negative, it's the last dim.
                shape.add(dim * -1);
                break;
            }
            readAddr += Integer.BYTES;
        }
        return shape.asSlice();
    }
}
