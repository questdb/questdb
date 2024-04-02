/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.vm.api.*;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.ColumnType.VARCHAR_AUX_SHL;

public class VarcharTypeDriver implements ColumnTypeDriver {
    public static final VarcharTypeDriver INSTANCE = new VarcharTypeDriver();
    public static final int VARCHAR_AUX_WIDTH_BYTES = 2 * Long.BYTES;
    // Maximum byte length that we can fully inline into auxiliary memory. In this case
    // there is no need to store any part of the string in data memory.
    // When the string is longer than this, we store the first few bytes in auxiliary memory,
    // and the full value in data memory.
    public static final int VARCHAR_MAX_BYTES_FULLY_INLINED = 9;
    // We store a prefix of this many bytes in auxiliary memory when the value is too large to inline.
    public static final int VARCHAR_INLINED_PREFIX_BYTES = 6;
    public static final long VARCHAR_INLINED_PREFIX_MASK = (1L << 8 * VARCHAR_INLINED_PREFIX_BYTES) - 1L;
    public static final long VARCHAR_MAX_COLUMN_SIZE = 1L << 48;

    private static final int HEADER_FLAGS_WIDTH = 4;
    private static final int HEADER_FLAGS_MASK = (1 << HEADER_FLAGS_WIDTH) - 1;
    private static final int HEADER_FLAG_INLINED = 1;
    private static final int HEADER_FLAG_ASCII = 2;
    private static final int HEADER_FLAG_NULL = 4;
    private static final int FULLY_INLINED_STRING_OFFSET = 1;
    private static final int INLINED_PREFIX_OFFSET = 4;
    private static final int INLINED_LENGTH_MASK = (1 << 4) - 1;
    // The exclusive limit on the byte length of a varchar value. The length is encoded in 28 bits.
    private static final int LENGTH_LIMIT_BYTES = 1 << 28;
    private static final int DATA_LENGTH_MASK = LENGTH_LIMIT_BYTES - 1;

    /**
     * Appends UTF8 varchar type to the data and aux vectors.
     *
     * @param dataMem data vector, contains UTF8 bytes
     * @param auxMem  aux vector, contains pointer to data vector, size, flags and statistics about UTF8 string
     * @param value   the UTF8 string to be stored
     */
    public static void appendValue(MemoryA dataMem, MemoryA auxMem, @Nullable Utf8Sequence value) {
        final long offset;
        if (value != null) {
            int size = value.size();
            if (size <= VARCHAR_MAX_BYTES_FULLY_INLINED) {
                int flags = HEADER_FLAG_INLINED;
                if (value.isAscii()) {
                    flags |= HEADER_FLAG_ASCII;
                }
                // size is known to be at most 4 bits
                auxMem.putByte((byte) ((size << HEADER_FLAGS_WIDTH) | flags));
                auxMem.putVarchar(value, 0, size);
                auxMem.skip(VARCHAR_MAX_BYTES_FULLY_INLINED - size);
                offset = dataMem.getAppendOffset();
            } else {
                if (size >= LENGTH_LIMIT_BYTES) {
                    throw CairoException.critical(0).put("varchar value is too long [size=")
                            .put(size).put(", max=").put(LENGTH_LIMIT_BYTES).put(']');
                }

                int flags = 0;  // not inlined
                if (value.isAscii()) {
                    flags |= HEADER_FLAG_ASCII;
                }
                auxMem.putInt((size << HEADER_FLAGS_WIDTH) | flags);
                auxMem.putVarchar(value, 0, VARCHAR_INLINED_PREFIX_BYTES);
                offset = dataMem.putVarchar(value, 0, size);
                if (offset >= VARCHAR_MAX_COLUMN_SIZE) {
                    throw CairoException.critical(0).put("varchar data column is too large [offset=")
                            .put(offset).put(", max=").put(VARCHAR_MAX_COLUMN_SIZE).put(']');
                }
            }
        } else {
            auxMem.putInt(HEADER_FLAG_NULL);
            auxMem.skip(VARCHAR_INLINED_PREFIX_BYTES);
            offset = dataMem.getAppendOffset();
        }
        // write 48 bit offset (little-endian)
        auxMem.putShort((short) offset);
        auxMem.putInt((int) (offset >> 16));
    }

    /**
     * Appends varchar to single data vector. The storage in this data vector is
     * length prefixed. The ascii flag is encoded to the highest bit of the length.
     *
     * @param dataMem the target append memory
     * @param value   the nullable varchar value, UTF8 encoded
     */
    public static void appendValue(MemoryA dataMem, @Nullable Utf8Sequence value) {
        if (value == null) {
            dataMem.putInt(TableUtils.NULL_LEN);
            return;
        }
        final int size = value.size();
        dataMem.putInt(value.isAscii() ? size | Integer.MIN_VALUE : size);
        dataMem.putVarchar(value, 0, size);
    }

    /**
     * Appends UTF8 varchar type to the memory address with a header.
     * This is unsafe method, and it is assumed that the memory address is valid and has enough space to store the header and UTF8 bytes.
     * The number of bytes to be written can be obtained from {@link #getSingleMemValueByteCount(Utf8Sequence)}
     *
     * @param dataMemAddr    memory address to store header and UTF8 bytes in
     * @param value          the UTF8 string to be stored
     * @param eraseAsciiFlag when set to true, the written sequence will have ASCII flag set to false;
     *                       when set to false, the output flag will have the same value as {@code value.isAscii()}.
     */
    public static void appendValue(long dataMemAddr, @Nullable Utf8Sequence value, boolean eraseAsciiFlag) {
        if (value == null) {
            Unsafe.getUnsafe().putInt(dataMemAddr, TableUtils.NULL_LEN); // NULL
            return;
        }
        final int hi = value.size();
        value.writeTo(dataMemAddr + Integer.BYTES, 0, hi);
        if (eraseAsciiFlag) {
            Unsafe.getUnsafe().putInt(dataMemAddr, hi);
        } else {
            final boolean ascii = value.isAscii();
            // ASCII flag is signaled with the highest bit
            Unsafe.getUnsafe().putInt(dataMemAddr, ascii ? hi | Integer.MIN_VALUE : hi);
        }
    }

    public static long getDataOffset(long auxEntry) {
        // the first 4 bytes cannot ever be 0
        // why? the first 4 bytes contains size and flags and there are 3 possibilities:
        // 1. null string -> the null flag is set
        // 2. empty string -> it's fully inlined -> the inline flag is set
        // 3. non-empty string -> the size is non-zero
        assert Unsafe.getUnsafe().getInt(auxEntry) != 0;

        return Unsafe.getUnsafe().getLong(auxEntry + Long.BYTES) >>> 16;
    }

    public static long getDataVectorSize(MemoryR auxMem, long offset) {
        final int raw = auxMem.getInt(offset);
        assert raw != 0;
        final long dataOffset = getDataOffset(auxMem, offset);

        if (hasNullOrInlinedFlag(raw)) {
            return dataOffset;
        }
        // size of the string at this offset
        final int size = (raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK;
        return dataOffset + size;
    }

    public static int getSingleMemValueByteCount(@Nullable Utf8Sequence value) {
        return value != null ? Integer.BYTES + value.size() : Integer.BYTES;
    }

    /*
     * Reads UTF8 varchar type from the memory with a header.
     *
     * @param dataMem memory contains UTF8 bytes
     * @param offset in the memory
     * @param ab 1 for A memory
     */
    public static Utf8Sequence getValue(@NotNull MemoryR dataMem, long offset, int ab) {
        long address = dataMem.addressOf(offset);
        int header = Unsafe.getUnsafe().getInt(address);
        assert header != 0;
        if (isNull(header)) {
            return null;
        }
        return (ab == 1) ?
                dataMem.getVarcharA(offset + Integer.BYTES, size(header), isAscii(header)) :
                dataMem.getVarcharB(offset + Integer.BYTES, size(header), isAscii(header));
    }

    /**
     * Reads UTF8 varchar type from the memory with a header.
     *
     * @param dataMemAddr memory address contains UTF8 bytes
     * @param sequence    to wrap UTF8 bytes with
     * @return the provided UTF8 wrapper or null.
     */
    public static DirectUtf8Sequence getValue(long dataMemAddr, @NotNull DirectUtf8String sequence) {
        int header = Unsafe.getUnsafe().getInt(dataMemAddr);
        if (isNull(header)) {
            return null;
        }
        return sequence.of(dataMemAddr + Integer.BYTES, dataMemAddr + Integer.BYTES + size(header), isAscii(header));
    }

    /**
     * Reads a UTF-8 value from a VARCHAR column.
     *
     * @param rowNum the row number to read
     * @param dataMem base pointer of the data vector
     * @param auxMem base pointer of the auxiliary vector
     * @param ab whether to return the A or B flyweight
     * @return a <code>Utf8Seqence</code> representing the value at <code>rowNum</code>
     */
    public static Utf8Sequence getValue(long rowNum, MemoryR dataMem, MemoryR auxMem, int ab) {
        final long auxOffset = VARCHAR_AUX_WIDTH_BYTES * rowNum;
        int raw = auxMem.getInt(auxOffset);
        assert raw != 0;

        if (hasNullFlag(raw)) {
            return null;
        }

        boolean ascii = hasAsciiFlag(raw);

        if (hasInlinedFlag(raw)) {
            // inlined string
            int size = (raw >> HEADER_FLAGS_WIDTH) & INLINED_LENGTH_MASK;
            return ab == 1 ? auxMem.getVarcharA(auxOffset + 1, size, ascii) : auxMem.getVarcharB(auxOffset + 1, size, ascii);
        }
        // string is split, prefix is in auxMem and the suffix is in data mem
        Utf8SplitString utf8SplitString = ab == 1 ? auxMem.borrowUtf8SplitStringA() : auxMem.borrowUtf8SplitStringB();

        if (utf8SplitString != null) {
            return utf8SplitString.of(
                    auxMem.addressOf(auxOffset + INLINED_PREFIX_OFFSET),
                    dataMem.addressOf(getDataOffset(auxMem, auxOffset)),
                    (raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK,
                    ascii
            );
        }
        return null;
    }

    /**
     * Reads a UTF-8 value from a VARCHAR column.
     *
     * @param auxAddr base pointer of the auxiliary vector
     * @param dataAddr base pointer of the data vector
     * @param rowNum the row number to read
     * @param utf8view flyweight for the inlined string
     * @param utf8SplitView flyweight for the split string
     * @return utf8view or utf8SplitView loaded with the read value
     */
    public static Utf8Sequence getValue(
            long auxAddr,
            long dataAddr,
            long rowNum,
            InlinedVarchar utf8view,
            Utf8SplitString utf8SplitView
    ) {
        long auxEntry = auxAddr + VARCHAR_AUX_WIDTH_BYTES * rowNum;
        int raw = Unsafe.getUnsafe().getInt(auxEntry);
        assert raw != 0;

        if (hasNullFlag(raw)) {
            return null;
        }

        boolean ascii = hasAsciiFlag(raw);

        if (hasInlinedFlag(raw)) {
            // inlined string
            int size = (raw >> HEADER_FLAGS_WIDTH) & INLINED_LENGTH_MASK;
            return utf8view.of(auxEntry + FULLY_INLINED_STRING_OFFSET, (byte) size, ascii);
        }
        // string is split, prefix is in aux mem and the full string is in data mem
        return utf8SplitView.of(
                auxEntry + INLINED_PREFIX_OFFSET,
                dataAddr + getDataOffset(auxEntry),
                (raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK,
                ascii
        );
    }

    private static boolean hasNullFlag(int auxHeader) {
        return (auxHeader & HEADER_FLAG_NULL) == HEADER_FLAG_NULL;
    }

    private static boolean hasInlinedFlag(int auxHeader) {
        return (auxHeader & HEADER_FLAG_INLINED) == HEADER_FLAG_INLINED;
    }

    private static boolean hasNullOrInlinedFlag(int auxHeader) {
        return (auxHeader & (HEADER_FLAG_NULL | HEADER_FLAG_INLINED)) != 0;
    }

    private static boolean hasAsciiFlag(int auxHeader) {
        return (auxHeader & HEADER_FLAG_ASCII) == HEADER_FLAG_ASCII;
    }

    @Override
    public void appendNull(MemoryA dataMem, MemoryA auxMem) {
        appendValue(dataMem, auxMem, null);
    }

    @Override
    public long auxRowsToBytes(long rowCount) {
        return VARCHAR_AUX_WIDTH_BYTES * rowCount;
    }

    @Override
    public void configureAuxMemMA(MemoryMA auxMem) {
        // noop
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
        // no-op for varchar
    }

    @Override
    public void configureAuxMemOM(FilesFacade ff, MemoryOM auxMem, int fd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, long opts) {
        auxMem.ofOffset(
                ff,
                fd,
                fileName,
                VARCHAR_AUX_WIDTH_BYTES * rowLo,
                VARCHAR_AUX_WIDTH_BYTES * rowHi,
                memoryTag,
                opts
        );
    }

    @Override
    public void configureDataMemOM(
            FilesFacade ff,
            MemoryR auxMem,
            MemoryOM dataMem,
            int dataFd,
            LPSZ fileName,
            long rowLo,
            long rowHi,
            int memoryTag,
            long opts
    ) {
        long lo;
        if (rowLo > 0) {
            lo = getDataOffset(auxMem, VARCHAR_AUX_WIDTH_BYTES * rowLo);
        } else {
            lo = 0;
        }
        long hi = getDataVectorSize(auxMem, VARCHAR_AUX_WIDTH_BYTES * (rowHi - 1));
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
    public long getAuxVectorOffset(long row) {
        return VARCHAR_AUX_WIDTH_BYTES * row;
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        return VARCHAR_AUX_WIDTH_BYTES * storageRowCount;
    }

    @Override
    public long getDataVectorMinEntrySize() {
        return 0;
    }

    public long getDataVectorOffset(long auxMemAddr, long row) {
        return getDataOffset(auxMemAddr + VARCHAR_AUX_WIDTH_BYTES * row);
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
        return getDataVectorSize(auxMemAddr + VARCHAR_AUX_WIDTH_BYTES * row);
    }

    @Override
    public long getDataVectorSizeAtFromFd(FilesFacade ff, int auxFd, long row) {
        long auxFileOffset = VARCHAR_AUX_WIDTH_BYTES * row;
        if (row < 0) {
            return 0;
        }
        final int raw = readInt(ff, auxFd, auxFileOffset);

        final int offsetLo = readInt(ff, auxFd, auxFileOffset + 8L);
        final int offsetHi = readInt(ff, auxFd, auxFileOffset + 12L);
        final long dataOffset = Numbers.encodeLowHighInts(offsetLo, offsetHi) >>> 16;

        if (hasNullOrInlinedFlag(raw)) {
            // null flag is set or fully inlined value
            return dataOffset;
        }
        // size of the string at this offset
        final int size = (raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK;
        return dataOffset + size;
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
        Vect.oooMergeCopyVarcharColumn(
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
    public void o3copyAuxVector(FilesFacade ff, long srcAddr, long srcLo, long srcHi, long dstAddr, long dstFileOffset, int dstFd, boolean mixedIOFlag) {
        O3CopyJob.copyFixedSizeCol(ff, srcAddr, srcLo, srcHi, dstAddr, dstFileOffset, dstFd, VARCHAR_AUX_SHL, mixedIOFlag);
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
        final long offset = Vect.sortVarcharColumn(
                sortedTimestampsAddr,
                sortedTimestampsRowCount,
                srcDataAddr,
                srcAuxAddr,
                tgtDataAddr,
                tgtAuxAddr
        );
        dstDataMem.jumpTo(offset);
        dstAuxMem.jumpTo(VARCHAR_AUX_WIDTH_BYTES * sortedTimestampsRowCount);
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, long rowCount) {
        if (rowCount > 0) {
            auxMem.jumpTo(VARCHAR_AUX_WIDTH_BYTES * (rowCount - HEADER_FLAG_INLINED));
            final long dataMemOffset = getDataVectorSize(auxMem.getAppendAddress());
            // Jump to the end of file to correctly trim the file
            auxMem.jumpTo(VARCHAR_AUX_WIDTH_BYTES * rowCount);
            return dataMemOffset;
        }
        auxMem.jumpTo(0);
        return 0;
    }

    @Override
    public long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem) {
        if (pos > 0) {
            // first we need to calculate already used space. both data and aux vectors.
            long auxVectorOffset = getAuxVectorOffset(pos - 1); // the last entry we are NOT overwriting
            auxMem.jumpTo(auxVectorOffset);
            long auxEntryPtr = auxMem.getAppendAddress();

            long dataVectorSize = getDataVectorSize(auxEntryPtr);
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
        // Assume var length columns use 28 bytes per value to estimate the record size
        // if there are no rows in the partition yet.
        // The record size used to estimate the partition size
        // to split partition in O3 commit when necessary
        return TableUtils.ESTIMATED_VAR_COL_SIZE;
    }

    @Override
    public void setColumnRefs(long address, long initialOffset, long count) {
        Vect.setVarcharColumnNullRefs(address, initialOffset, count);
    }

    @Override
    public void setDataVectorEntriesToNull(long dataMemAddr, long rowCount) {
        // this is a no-op, NULLs do not occupy space in the data vector
    }

    @Override
    public void shiftCopyAuxVector(long shift, long srcAddr, long srcLo, long srcHi, long dstAddr) {
        O3Utils.shiftCopyVarcharColumnAux(
                shift,
                srcAddr,
                srcLo,
                srcHi,
                dstAddr
        );
    }

    private static long getDataOffset(MemoryR auxMem, long offset) {
        return auxMem.getLong(offset + 8L) >>> 16;
    }

    private static long getDataVectorSize(long auxEntry) {
        final int raw = Unsafe.getUnsafe().getInt(auxEntry);
        final long dataOffset = getDataOffset(auxEntry);

        if (hasNullOrInlinedFlag(raw)) {
            return dataOffset;
        }
        // size of the string at this offset
        final int size = (raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK;
        assert size > VARCHAR_MAX_BYTES_FULLY_INLINED : String.format("size %,d <= %d, dataOffset %,d",
                size, VARCHAR_MAX_BYTES_FULLY_INLINED, dataOffset);

        return dataOffset + size;
    }

    private static boolean isAscii(int header) {
        // ASCII flag is signaled with the highest bit
        return (header & Integer.MIN_VALUE) != 0;
    }

    private static boolean isNull(int header) {
        return header == TableUtils.NULL_LEN;
    }

    private static int readInt(FilesFacade ff, int fd, long offset) {
        long res = ff.readIntAsUnsignedLong(fd, offset);
        if (res < 0) {
            throw CairoException.critical(ff.errno())
                    .put("Invalid data read from varchar aux file [fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", fileSize=").put(ff.length(fd))
                    .put(", result=").put(res)
                    .put(']');
        }
        return Numbers.decodeLowInt(res);
    }

    private static int size(int header) {
        return header & Integer.MAX_VALUE;
    }
}
