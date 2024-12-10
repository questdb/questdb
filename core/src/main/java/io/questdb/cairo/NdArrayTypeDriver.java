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
 * <h2>Data Offset Handling</h2>
 * <p>Like the <code>VARCHAR</code> type, <code>ARRAY</code> uses an <code>N</code>
 * (not <code>N + 1</code> encoding scheme.</p>
 * <h2>AUX entry format</h2>
 * <p><strong>IMPORTANT!</strong>: Since we store 96 bit entries, every second entry is unaligned for reading
 * via <code>Unsafe.getLong()</code>, as such if you find any <code>{MemoryR,Unsafe}.{get,set}Long</code> calls
 * operating on the aux data in this code, it's probably a bug!</p>
 * <pre>
 * 96-bit fixed size entries
 *     * crc_and_offset: 64-bits
 *         * offset_and_hash: long ======
 *             * bits 0 to =47: offset: 48-bit unsigned integer
 *                 * byte count offset into the data vector
 *             * bits 48 to =64: hash: 16-bit
 *                 * CRC-16/XMODEM hash used to speed up equality comparisons
 *     * data_size: 32-bits
 *         * number of bytes used to the store the array (along with any additional metadata) in the data vector.
 * </pre>
 * <h2>Encoding NULLs</h2>
 * <ul>
 *     <li>A null value has no size.</li>
 *     <li>The CRC of a null array is 0, so <code>auxLo >> CRC16_SHIFT == 0</code></li>
 *     <li>We however <em>do</em> populate the <code>offset</code> field with
 *     the end of the previous non-null value.</li>
 *     <li>This allows mapping the data vector for a specific range of values.</li>
 * </ul></p>
 * <h1>Data vector</h1>
 * <pre>
 * variable length encoding, starting at the offset specified in the `aux` entry.
 *     * START ALIGNMENT: Each offset pointed to by an aux entry is guaranteed to have at least `int` alignment`.
 *     * Shape: len-prefixed ints
 *         * The series of dimensions of the array.
 *         * Starts with an 32-bit int with the number of dimensions.
 *         * Each dimension then written as a 32-bit int.
 *         * Note that each dimension only ever uses the lowest 27 bits.
 *     * padding:
 *         * enough padding to satisfy the datatype alignment requirements.
 *         * e.g. for 64-bit numeric types ensures that the following section
 *           starts on an 8 byte boundary, for a 32-bit type, on a 4 byte boundary.
 *           This is to avoid unaligned data reads later.
 *           See the following <a
 *           href="https://medium.com/@jkstoyanov/aligned-and-unaligned-memory-access-9b5843b7f4ac"
 *           >blog post</a>.
 *         * In practice, this would be either 0 or 4 bytes of padding (given we've just written ints).
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
 *         * enough padding for `int` alignment, ready for the next record (see START ALIGNMENT note).
 * </pre>
 */
public class NdArrayTypeDriver implements ColumnTypeDriver {
    public static final NdArrayTypeDriver INSTANCE = new NdArrayTypeDriver();
    public static final long OFFSET_MAX = (1L << 48) - 1L;
    public static final int CRC16_SHIFT = 48;
    private static final int ND_ARRAY_AUX_WIDTH_BYTES = 3 * Integer.BYTES;
    private static final ThreadLocal<DirectIntList> SHAPE = new ThreadLocal<>(NdArrayTypeDriver::newShape);
    public static final Closeable THREAD_LOCAL_CLEANER = NdArrayTypeDriver::clearThreadLocals;
    private static final long U32_MASK = 0xFFFFFFFFL;

    public static void appendValue(@NotNull MemoryA auxMem, @NotNull MemoryA dataMem, @Nullable NdArrayView array) {
        if ((array == null) || array.isNull()) {
            appendNullImpl(auxMem, dataMem);
            return;
        }

        final long beginOffset = dataMem.getAppendOffset();
        final short crc = writeDataEntry(dataMem, array);
        final long endOffset = dataMem.getAppendOffset();
        final int size = (int) (endOffset - beginOffset);
        writeAuxEntry(auxMem, beginOffset, crc, size);
    }

    public static long getIntAlignedLong(@NotNull MemoryR mem, long offset) {
        final int lower = mem.getInt(offset);
        final int upper = mem.getInt(offset + Integer.BYTES);
        return ((long) upper << 32) | (lower & U32_MASK);
    }

    /** Number of bytes to skip to find the next aligned address/offset. */
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
        long lo;
        if (rowLo > 0) {
            lo = readDataOffset(auxMem, ND_ARRAY_AUX_WIDTH_BYTES * rowLo);
        } else {
            lo = 0;
        }
        long hi = calcDataOffsetEnd(auxMem, ND_ARRAY_AUX_WIDTH_BYTES * (rowHi - 1));
        dataMem.ofOffset(
                ff,
                dataFd,
                fileName,
                lo,
                hi,
                memoryTag,
                opts
        );
    }

    @Override
    public long dedupMergeVarColumnSize(long mergeIndexAddr, long mergeIndexCount, long srcDataFixAddr, long srcOooFixAddr) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long getAuxVectorOffset(long row) {
        return ND_ARRAY_AUX_WIDTH_BYTES * row;
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        return ND_ARRAY_AUX_WIDTH_BYTES * storageRowCount;
    }

    @Override
    public long getDataVectorMinEntrySize() {
        return 0;
    }

    @Override
    public long getDataVectorOffset(long auxMemAddr, long row) {
        long auxEntry = auxMemAddr + ND_ARRAY_AUX_WIDTH_BYTES * row;
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
        return calcDataOffsetEnd(auxMemAddr + (ND_ARRAY_AUX_WIDTH_BYTES * row));
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
        final long nextDataMemOffset = calcDataOffsetEnd(auxMem.getAppendAddress());

        // Jump to the end of file to correctly trim the file
        auxMem.jumpTo(ND_ARRAY_AUX_WIDTH_BYTES * rowCount);
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
    public void shiftCopyAuxVector(long shift, long srcAddr, long srcLo, long srcHi, long dstAddr, long dstAddrSize) {
        // +1 since srcHi is inclusive
        assert (srcHi - srcLo + 1) * ND_ARRAY_AUX_WIDTH_BYTES <= dstAddrSize;
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
        putIntAlignedLong(auxMem, offset);
        auxMem.putInt(0);  // zero-length array
    }

    private static void appendNullImpl(MemoryA auxMem, MemoryA dataMem) {
        final long offset = dataMem.getAppendOffset();
        appendNullImpl(auxMem, offset);
    }

    private static void clearThreadLocals() {
        SHAPE.close();
    }

    public static long getIntAlignedLong(long address) {
        final int lower = Unsafe.getUnsafe().getInt(address);
        final int upper = Unsafe.getUnsafe().getInt(address + Integer.BYTES);
        return ((long) upper << 32) | (lower & U32_MASK);
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

    private static void padTo(@NotNull MemoryA dataMem, int byteAlignment) {
        final int requiredPadding = skipsToAlign(dataMem.getAppendOffset(), byteAlignment);
        for (int paddingIndex = 0; paddingIndex < requiredPadding; ++paddingIndex) {
            dataMem.putByte((byte) 0);
        }
    }

    private static void putIntAlignedLong(MemoryA mem, long value) {
        final int lower = (int) (value & U32_MASK);
        final int upper = (int) (value >> 32);
        mem.putInt(lower);
        mem.putInt(upper);
    }

    /**
     * Read the data offset from the aux entry that starts at the specified offset.
     */
    private static long readDataOffset(MemoryR auxMem, long offset) {
        return getIntAlignedLong(auxMem, offset) & OFFSET_MAX;
    }

    private static long readDataOffset(long address) {
        return getIntAlignedLong(address) & OFFSET_MAX;
    }

    private static void writeAuxEntry(MemoryA auxMem, long offset, short crc, int size) {
        assert offset >= 0;
        assert offset <= OFFSET_MAX;
        assert size >= 0;
        final long crcAndOffset = ((long) crc << CRC16_SHIFT) | offset;
        putIntAlignedLong(auxMem, crcAndOffset);
        auxMem.putInt(size);
    }

    /**
     * Write the values and -- while doing so, also calculate the crc value, unless it was already cached.
     *
     * @return the CRC16/XModem value
     **/
    private static short writeDataEntry(@NotNull MemoryA dataMem, @NotNull NdArrayView array) {
        writeShape(dataMem, array.getShape());

        // We could be storing values of different datatypes.
        // We thus need to align accordingly. I.e., if we store doubles, we need to align on an 8-byte boundary.
        // for shorts, it's on a 2-byte boundary. For booleans, we align to the byte.
        final int bitWidth = 1 << ColumnType.getNdArrayElementTypePrecision(array.getType());
        final int requiredByteAlignment = (bitWidth + 7) / 8;
        padTo(dataMem, requiredByteAlignment);
        final short crc = writeValues(dataMem, array, bitWidth);
        // We pad at the end, ready for the next entry that starts with an int.
        padTo(dataMem, Integer.BYTES);
        return crc;
    }

    private static short writeFlatValueBytes(@NotNull MemoryA dataMem, @NotNull NdArrayView array, int bitWidth, NdArrayValuesSlice values) {
        final int requiredByteAlignment = (bitWidth + 7) / 8;
        final int bytesToSkip = skipsToAlign(array.getValuesOffset(), requiredByteAlignment);
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
                dataMem.putByte(value);
            }
            return CRC16XModem.finalize(crc);
        }
    }

    /**
     * Write the dimensions.
     */
    private static void writeShape(@NotNull MemoryA dataMem, @NotNull DirectIntSlice shape) {
        assert dataMem.getAppendOffset() % Integer.BYTES == 0; // aligned integer write
        dataMem.putInt(shape.length());
        for (int dimIndex = 0, nDims = shape.length(); dimIndex < nDims; ++dimIndex) {
            final int dim = shape.get(dimIndex);
            dataMem.putInt(dim);
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
    private static short writeValues(@NotNull MemoryA dataMem, @NotNull NdArrayView array, int bitWidth) {
        final NdArrayValuesSlice values = array.getValues();
        final short crc;
        if (array.hasDefaultStrides() && isByteAligned(bitWidth, array)) {
            crc = writeFlatValueBytes(dataMem, array, bitWidth, values);
        } else if (bitWidth == 1) {
            crc = writeStridedBooleanValues(dataMem, array);
        } else {
            assert bitWidth >= 8;
            crc = writeStridedByteAlignedValues(bitWidth / 8, dataMem, array);
        }
        return crc;
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
