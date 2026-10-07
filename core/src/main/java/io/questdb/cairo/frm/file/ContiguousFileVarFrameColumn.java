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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;
import static io.questdb.cairo.TableWriter.TIMESTAMP_MERGE_ENTRY_BYTES;

public class ContiguousFileVarFrameColumn implements FrameColumn {
    private static final Log LOG = LogFactory.getLog(ContiguousFileFixFrameColumn.class);
    private static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WRITER;
    private final FilesFacade ff;
    private final int fileOpts;
    private final boolean mixedIOFlag;
    private final ColumnWriteBuffer writeBuffer = new ColumnWriteBuffer();
    // The least lengths this column knows its two files to have, learnt from the files once per open.
    // See ContiguousFileFixFrameColumn#allocatedBytes.
    private long allocatedAuxBytes;
    private long allocatedDataBytes;
    private long appendOffsetRowCount = -1;
    private long auxFd = -1;
    private long auxMapAddr;
    private long auxMapSize;
    private boolean closed = false;
    private int columnIndex;
    private long columnTop;
    private int columnType;
    private ColumnTypeDriver columnTypeDriver;
    private long dataAppendOffsetBytes = -1;
    private long dataFd = -1;
    private long dataMapAddr;
    private long dataMapSize;
    // See ContiguousFileFixFrameColumn#isAllocatedBytesKnown.
    private boolean isAllocatedAuxBytesKnown;
    private boolean isAllocatedDataBytesKnown;
    private boolean isReadOnly;
    // See setReadWindow: the top getColumnTop() reports is capped here, while columnTop stays the file's own.
    private long logicalRowHi = Long.MAX_VALUE;
    private long mapRowHi;
    private RecycleBin<FrameColumn> recycleBin;

    public ContiguousFileVarFrameColumn(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.fileOpts = configuration.getWriterFileOpenOpts();
        this.mixedIOFlag = configuration.isWriterMixedIOEnabled();
    }

    @Override
    public void addTop(long value) {
        this.columnTop += value;
    }

    @Override
    public void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        final int sourceStorageType = sourceColumn.getStorageType();
        if (sourceStorageType != COLUMN_CONTIGUOUS_FILE && sourceStorageType != COLUMN_MEMORY) {
            throw new UnsupportedOperationException();
        }

        // The source's mappings are asked for by logical row, top included.
        final long sourceRowHi = sourceHi;
        // Each side offsets by its OWN column top: a column whose data starts at a top does not hold the rows below it.
        sourceLo -= sourceColumn.getColumnTop();
        sourceHi -= sourceColumn.getColumnTop();
        appendOffsetRowCount -= columnTop;

        assert sourceLo >= 0;
        assert sourceHi >= 0;
        assert appendOffsetRowCount >= 0;

        if (sourceHi <= sourceLo) {
            return;
        }

        // Either source hands its aux vector over as the address of its row 0: a memory source already is one, and a
        // file source maps itself - once for the whole extent when its frame keeps it open, so a plan does not map it
        // per action.
        final boolean isFileSource = sourceStorageType == COLUMN_CONTIGUOUS_FILE;
        final long srcAuxAddr = sourceColumn.getContiguousAuxAddr(isFileSource ? sourceRowHi : sourceHi);
        final long targetDataOffset = getDataAppendOffsetBytes(appendOffsetRowCount);
        final long srcDataOffset = columnTypeDriver.getDataVectorOffset(srcAuxAddr, sourceLo);
        assert (sourceLo == 0 && srcDataOffset == 0) || (sourceLo > 0 && srcDataOffset >= columnTypeDriver.getDataVectorMinEntrySize() && srcDataOffset < 1L << 40);
        final long srcDataSize = columnTypeDriver.getDataVectorSize(srcAuxAddr, sourceLo, sourceHi - 1);

        if (srcDataSize > 0) {
            assert srcDataSize < 1L << 40;
            appendData(sourceColumn, isFileSource, isFileSource ? sourceRowHi : sourceHi, srcDataOffset, srcDataSize, targetDataOffset, commitMode);
        }

        final long dstAuxOffset = columnTypeDriver.getAuxVectorOffset(appendOffsetRowCount);
        final long dstAuxSize = columnTypeDriver.getAuxVectorSize(sourceHi - sourceLo);
        if (mixedIOFlag) {
            if (!isAllocatedAuxBytesKnown) {
                // See ContiguousFileFixFrameColumn#append: reserve() ran through an earlier open of this file.
                ensureAuxAllocated(dstAuxOffset + dstAuxSize);
            }
            assertAuxWriteReserved(dstAuxOffset + dstAuxSize);
            writeShiftedAux(srcDataOffset - targetDataOffset, srcAuxAddr, sourceLo, sourceHi, appendOffsetRowCount);
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(auxFd);
            }
        } else {
            final long dstAuxAddr = mapAuxWritable(dstAuxOffset + dstAuxSize) + dstAuxOffset;
            columnTypeDriver.shiftCopyAuxVector(
                    srcDataOffset - targetDataOffset,
                    srcAuxAddr,
                    sourceLo,
                    sourceHi - 1, // inclusive
                    dstAuxAddr,
                    dstAuxSize
            );
            if (commitMode != CommitMode.NOSYNC) {
                TableUtils.msync(ff, dstAuxAddr, dstAuxSize, commitMode == CommitMode.ASYNC);
            }
        }

        this.appendOffsetRowCount = appendOffsetRowCount + (sourceHi - sourceLo);
        this.dataAppendOffsetBytes = targetDataOffset + srcDataSize;
    }

    /**
     * Copies one contiguous run of the source's DATA vector to {@code targetDataOffset} in this column's data file.
     *
     * @param sourceRowHi the row the source's mapping is asked to reach: logical, top included, for a file source
     */
    private void appendData(
            FrameColumn sourceColumn,
            boolean isFileSource,
            long sourceRowHi,
            long srcDataOffset,
            long srcDataSize,
            long targetDataOffset,
            int commitMode
    ) {
        if (mixedIOFlag) {
            // reserve() allocated the plan's full extent before this positioned write; mixed I/O needs no target
            // mapping. Only a file source has an fd to copy from, so only it takes the kernel's fd-to-fd path.
            if (!isAllocatedDataBytesKnown) {
                // See ContiguousFileFixFrameColumn#append: reserve() ran through an earlier open of this file.
                ensureDataAllocated(targetDataOffset + srcDataSize);
            }
            assertDataWriteReserved(targetDataOffset + srcDataSize);
            if (isFileSource) {
                final long sourceFd = sourceColumn.getPrimaryFd();
                if (ff.copyData(sourceFd, dataFd, srcDataOffset, targetDataOffset, srcDataSize) != srcDataSize) {
                    throw CairoException.critical(ff.errno()).put("Cannot copy data [fd=").put(dataFd)
                            .put(", destOffset=").put(targetDataOffset)
                            .put(", size=").put(srcDataSize)
                            .put(", fileSize=").put(ff.length(dataFd))
                            .put(", srcFd=").put(sourceFd)
                            .put(", srcOffset=").put(srcDataOffset)
                            .put(", srcFileSize=").put(ff.length(sourceFd))
                            .put(']');
                }
            } else {
                ColumnWriteBuffer.write(ff, dataFd, sourceColumn.getContiguousDataAddr(sourceRowHi) + srcDataOffset, srcDataSize, targetDataOffset);
            }
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(dataFd);
            }
            return;
        }

        final long srcDataAddress = sourceColumn.getContiguousDataAddr(sourceRowHi) + srcDataOffset;
        final long dstDataAddress = mapDataWritable(targetDataOffset + srcDataSize) + targetDataOffset;
        Vect.memcpy(dstDataAddress, srcDataAddress, srcDataSize);
        if (commitMode != CommitMode.NOSYNC) {
            TableUtils.msync(ff, dstDataAddress, srcDataSize, commitMode == CommitMode.ASYNC);
        }
    }

    @Override
    public void appendNulls(long rowCount, long sourceColumnTop, int commitMode) {
        rowCount -= columnTop;
        assert rowCount >= 0;

        if (sourceColumnTop > 0) {
            long targetDataOffset = getDataAppendOffsetBytes(rowCount);
            long srcDataSize = sourceColumnTop * columnTypeDriver.getDataVectorMinEntrySize();
            if (srcDataSize > 0) {
                // Set nulls in variable file
                final long targetDataMemAddr = mapDataWritable(targetDataOffset + srcDataSize) + targetDataOffset;
                columnTypeDriver.setDataVectorEntriesToNull(targetDataMemAddr, sourceColumnTop);
                if (commitMode != CommitMode.NOSYNC) {
                    TableUtils.msync(ff, targetDataMemAddr, srcDataSize, commitMode == CommitMode.ASYNC);
                }

                // Cache the new data append offset
                this.appendOffsetRowCount = rowCount + sourceColumnTop;
                this.dataAppendOffsetBytes = targetDataOffset + srcDataSize;
            }

            // Set pointers to nulls
            long srcAuxSize = columnTypeDriver.getAuxVectorSize(sourceColumnTop);
            long dstAuxOffset = columnTypeDriver.getAuxVectorSize(rowCount);
            final long targetAuxMemAddr = mapAuxWritable(dstAuxOffset + srcAuxSize) + dstAuxOffset;
            // We need to write pointer to nulls in aux vector.
            // If the destination is empty (0 rows) and we need to write 1 null
            // then we need to write value -1 to offset 0 (targetDataOffset) in data vector
            // and value 4 (targetDataOffset + columnTypeDriver.getDataVectorMinEntrySize()) at offset 8 (dstAuxOffset) at aux vector.
            columnTypeDriver.setPartAuxVectorNull(
                    targetAuxMemAddr,
                    targetDataOffset + columnTypeDriver.getDataVectorMinEntrySize(),
                    sourceColumnTop
            );
            if (commitMode != CommitMode.NOSYNC) {
                TableUtils.msync(ff, targetAuxMemAddr, srcAuxSize, commitMode == CommitMode.ASYNC);
            }
        }
    }

    @Override
    public void close() {
        if (!closed) {
            if (auxMapAddr != 0) {
                ff.munmap(auxMapAddr, auxMapSize, MEMORY_TAG);
                auxMapAddr = 0;
                auxMapSize = 0;
            }

            if (dataMapAddr != 0) {
                ff.munmap(dataMapAddr, dataMapSize, MEMORY_TAG);
                dataMapAddr = 0;
                dataMapSize = 0;
            }

            if (auxFd != -1) {
                ff.close(auxFd);
                auxFd = -1;
            }
            if (dataFd != -1) {
                ff.close(dataFd);
                dataFd = -1;
            }
            writeBuffer.close();
            closed = true;

            if (recycleBin != null && !recycleBin.isClosed()) {
                appendOffsetRowCount = 0;
                dataAppendOffsetBytes = 0;
                recycleBin.put(this);
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getColumnTop() {
        return Math.min(columnTop, logicalRowHi);
    }

    @Override
    public int getColumnType() {
        return columnType;
    }

    @Override
    public long getContiguousAuxAddr(long rowHi) {
        if (rowHi <= columnTop) {
            return 0;
        }

        mapAllRows(rowHi);
        return auxMapAddr;
    }

    @Override
    public long getContiguousDataAddr(long rowHi) {
        if (rowHi <= columnTop) {
            return 0;
        }

        mapAllRows(rowHi);
        return dataMapAddr;
    }

    @Override
    public long getPrimaryFd() {
        return dataFd;
    }

    @Override
    public long getSecondaryFd() {
        return auxFd;
    }

    @Override
    public int getStorageType() {
        return COLUMN_CONTIGUOUS_FILE;
    }

    @Override
    public void merge(
            long appendOffsetRowCount,
            FrameColumn sourceColumn1,
            long source1Lo,
            long source1Hi,
            FrameColumn sourceColumn2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            int commitMode
    ) {
        // The target offsets by its OWN column top, exactly as append does; each SOURCE does the same in
        // rowZeroAuxAddr below.
        appendOffsetRowCount -= columnTop;

        assert appendOffsetRowCount >= 0;
        // Not an equality: a deduplicating commit drops rows, so the index is SHORTER than both sides added together.
        assert mergeIndexRows <= (source1Hi - source1Lo) + (source2Hi - source2Lo);

        if (mergeIndexRows == 0) {
            return;
        }

        // Only the DATA side can carry a column top - the O3 side is a batch this commit is writing now, so
        // every row of it exists. When the slice reaches below that top the top-aware kernel takes the top
        // directly and emits this type's NULL for the rows underneath.
        final long src1Top = sourceColumn1.getColumnTop();
        final boolean readsBelowTop = source1Lo < source1Hi && source1Lo < src1Top;
        // Both vectors are addressed from ROW 0 and BYTE 0 of their own column: the merge index carries absolute row
        // ids, and an aux entry carries an absolute data offset, so neither side is relative to the slice being read.
        final long src1AuxAddr = readsBelowTop
                ? sourceColumn1.getContiguousAuxAddr(source1Hi)
                : rowZeroAuxAddr(sourceColumn1, source1Lo, source1Hi);
        final long src2AuxAddr = rowZeroAuxAddr(sourceColumn2, source2Lo, source2Hi);
        final long src1DataAddr = source1Lo < source1Hi ? sourceColumn1.getContiguousDataAddr(source1Hi) : 0;
        final long src2DataAddr = source2Lo < source2Hi ? sourceColumn2.getContiguousDataAddr(source2Hi) : 0;

        final long dataSize;
        if (mergeIndexRows == (source1Hi - source1Lo) + (source2Hi - source2Lo)) {
            // The index kept every row of both sides, which only a non-deduplicating index does. Then each row is
            // written out exactly once and carries its own bytes with it, so the merged image is as long as the two
            // slices put together - the interleaving moves bytes around but creates none.
            final long belowTopRows = readsBelowTop ? Math.min(source1Hi, src1Top) - source1Lo : 0;
            dataSize = (readsBelowTop
                    ? sourceDataSize(src1AuxAddr, Math.max(source1Lo, src1Top) - src1Top, source1Hi - src1Top)
                      + belowTopRows * columnTypeDriver.getDataVectorMinEntrySize()
                    : sourceDataSize(src1AuxAddr, source1Lo, source1Hi))
                    + sourceDataSize(src2AuxAddr, source2Lo, source2Hi);
        } else if (!readsBelowTop) {
            // A shorter index means a DEDUP commit dropped rows, and then the sum above is not an upper bound: a
            // dedup index emits one entry per DATA row and an entry whose key collided carries the INCOMING row's
            // id, so N pre-existing duplicate keys select the same incoming value N times. The sizing has to walk
            // the index, exactly as the classic O3 path's dedupMergeVarColumnSize does - same native, same
            // convention: bit 63 of an entry picks the side and the rest is a row id into that side's row-zero
            // aux vector.
            dataSize = columnTypeDriver.dedupMergeVarColumnSize(mergeIndexAddr, mergeIndexRows, src1AuxAddr, src2AuxAddr);
        } else {
            dataSize = dedupMergedDataSizeBelowTop(mergeIndexAddr, mergeIndexRows, src1AuxAddr, src1Top, src2AuxAddr);
        }
        final long targetDataOffset = getDataAppendOffsetBytes(appendOffsetRowCount);
        final long dstAuxOffset = columnTypeDriver.getAuxVectorOffset(appendOffsetRowCount);
        final long dstAuxSize = columnTypeDriver.getAuxVectorSize(mergeIndexRows);

        final long dstAuxAddr = mapAuxWritable(dstAuxOffset + dstAuxSize) + dstAuxOffset;
        // The kernel writes ABSOLUTE data offsets into the aux entries and addresses its writes by the same value,
        // so it is handed the address of byte 0 of the data file - which is what the column's mapping starts at.
        final long dstDataBase = dataSize > 0 ? mapDataWritable(targetDataOffset + dataSize) : 0;
        final long dstDataAddr = dataSize > 0 ? dstDataBase + targetDataOffset : 0;
        if (readsBelowTop) {
            columnTypeDriver.o3ColumnMergeWithTop(
                    mergeIndexAddr,
                    mergeIndexRows,
                    src1Top,
                    src1AuxAddr,
                    src1DataAddr,
                    src2AuxAddr,
                    src2DataAddr,
                    dstAuxAddr,
                    dstDataBase,
                    targetDataOffset
            );
        } else {
            columnTypeDriver.o3ColumnMerge(
                    mergeIndexAddr,
                    mergeIndexRows,
                    src1AuxAddr,
                    src1DataAddr,
                    src2AuxAddr,
                    src2DataAddr,
                    dstAuxAddr,
                    dstDataBase,
                    targetDataOffset
            );
        }

        if (commitMode != CommitMode.NOSYNC) {
            TableUtils.msync(ff, dstAuxAddr, dstAuxSize, commitMode == CommitMode.ASYNC);
            if (dstDataAddr != 0) {
                TableUtils.msync(ff, dstDataAddr, dataSize, commitMode == CommitMode.ASYNC);
            }
        }

        this.appendOffsetRowCount = appendOffsetRowCount + mergeIndexRows;
        this.dataAppendOffsetBytes = targetDataOffset + dataSize;
    }

    public void ofRO(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex, boolean isEmpty) {
        assert auxFd == -1;
        closed = false;
        int plen = partitionPath.size();

        try {
            this.columnType = columnType;
            this.columnTypeDriver = ColumnType.getDriver(columnType);
            this.columnTop = columnTop;
            this.columnIndex = columnIndex;
            this.appendOffsetRowCount = -1;
            // A pooled column must not carry the previous owner's window into this open.
            this.logicalRowHi = Long.MAX_VALUE;
            this.mapRowHi = 0;

            if (!isEmpty) {
                dFile(partitionPath, columnName, columnTxn);
                this.dataFd = TableUtils.openRO(ff, partitionPath.$(), LOG);
                partitionPath.trimTo(plen);
                iFile(partitionPath, columnName, columnTxn);
                this.auxFd = TableUtils.openRO(ff, partitionPath.$(), LOG);
            }
            this.isReadOnly = true;
        } catch (Exception e) {
            close();
            throw e;
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    public void ofRW(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex) {
        assert auxFd == -1;
        closed = false;
        int plen = partitionPath.size();
        allocatedAuxBytes = 0;
        allocatedDataBytes = 0;
        isAllocatedAuxBytesKnown = false;
        isAllocatedDataBytesKnown = false;

        try {
            // Negative col top means column does not exist in the partition.
            // Create it.
            this.columnType = columnType;
            this.columnTypeDriver = ColumnType.getDriver(columnType);
            this.columnTop = columnTop;
            this.columnIndex = columnIndex;
            this.appendOffsetRowCount = -1;
            // A pooled column must not carry the previous owner's window into this open.
            this.logicalRowHi = Long.MAX_VALUE;
            this.mapRowHi = 0;

            dFile(partitionPath, columnName, columnTxn);
            this.dataFd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);
            partitionPath.trimTo(plen);
            iFile(partitionPath, columnName, columnTxn);
            this.auxFd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);
            this.isReadOnly = false;
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    @Override
    public void reserve(long rowLo, long rowHi, long dataBytes, boolean isMerging) {
        final long rows = rowHi - columnTop;
        if (rows > 0) {
            final long auxSize = columnTypeDriver.getAuxVectorSize(rows);
            if (mixedIOFlag) {
                ensureAuxAllocated(auxSize);
            } else {
                mapAuxWritable(auxSize);
            }
        }
        if (dataBytes > 0) {
            // The data file grows from wherever the rows already in it end, which the aux vector says.
            final long dataOffset = getDataAppendOffsetBytes(Math.max(0, rowLo - columnTop));
            if (mixedIOFlag) {
                ensureDataAllocated(dataOffset + dataBytes);
            } else {
                mapDataWritable(dataOffset + dataBytes);
            }
        }
    }

    @Override
    public void setReadWindow(long logicalRowHi, long mapRowHi) {
        this.logicalRowHi = logicalRowHi;
        this.mapRowHi = mapRowHi;
    }

    public void setRecycleBin(RecycleBin<FrameColumn> recycleBin) {
        assert this.recycleBin == null;
        this.recycleBin = recycleBin;
    }

    /**
     * The exact byte length of the merged data vector for a DEDUP index whose data side reaches BELOW its column
     * top. {@link ColumnTypeDriver#dedupMergeVarColumnSize} cannot size that shape: it addresses the data side by
     * absolute row id, and the rows under the top have no aux entry to address. The top-aware kernel writes this
     * type's NULL for them instead, which costs {@link ColumnTypeDriver#getDataVectorMinEntrySize()} bytes each -
     * the same per-row price the non-dedup branch above charges for them.
     */
    private long dedupMergedDataSizeBelowTop(
            long mergeIndexAddr,
            long mergeIndexRows,
            long src1AuxAddr,
            long src1Top,
            long src2AuxAddr
    ) {
        final long nullSize = columnTypeDriver.getDataVectorMinEntrySize();
        long dataSize = 0;
        for (long i = 0; i < mergeIndexRows; i++) {
            // An index entry is a (timestamp, row id) pair; bit 63 of the row id is set when the row comes from the
            // DATA column and clear when it comes from the O3 batch.
            final long rowId = Unsafe.getUnsafe().getLong(mergeIndexAddr + i * TIMESTAMP_MERGE_ENTRY_BYTES + Long.BYTES);
            if (rowId < 0) {
                // The data side's aux vector is addressed from ITS row zero, which is the column top's row.
                final long storageRow = (rowId & Long.MAX_VALUE) - src1Top;
                dataSize += storageRow < 0 ? nullSize : columnTypeDriver.getDataVectorSize(src1AuxAddr, storageRow, storageRow);
            } else {
                dataSize += columnTypeDriver.getDataVectorSize(src2AuxAddr, rowId, rowId);
            }
        }
        return dataSize;
    }

    /**
     * Makes the aux file at least {@code size} bytes long; see ContiguousFileFixFrameColumn#ensureAllocated.
     */
    private void ensureAuxAllocated(long size) {
        if (size > allocatedAuxBytes) {
            if (!isAllocatedAuxBytesKnown) {
                allocatedAuxBytes = Math.max(allocatedAuxBytes, ff.length(auxFd));
                isAllocatedAuxBytesKnown = true;
            }
            if (size > allocatedAuxBytes) {
                allocatedAuxBytes = allocate(auxFd, allocatedAuxBytes, size);
            }
        }
    }

    /**
     * Makes the data file at least {@code size} bytes long; see ContiguousFileFixFrameColumn#ensureAllocated.
     */
    private void ensureDataAllocated(long size) {
        if (size > allocatedDataBytes) {
            if (!isAllocatedDataBytesKnown) {
                allocatedDataBytes = Math.max(allocatedDataBytes, ff.length(dataFd));
                isAllocatedDataBytesKnown = true;
                if (size <= allocatedDataBytes) {
                    return;
                }
            }
            allocatedDataBytes = allocate(dataFd, allocatedDataBytes, size);
        }
    }

    /**
     * Grows the file to {@code size}, page-aligned, allocating only past {@code allocatedSize}, the length the file is
     * known to have - see ContiguousFileFixFrameColumn#ensureAllocated.
     */
    private long allocate(long fd, long allocatedSize, long size) {
        size = Files.ceilPageSize(size);
        if (!ff.allocate(fd, allocatedSize, size)) {
            throw CairoException.critical(ff.errno()).put("No space left [size=").put(size).put(", fd=").put(fd).put(']');
        }
        return size;
    }

    /**
     * Writable columns only: makes the aux file at least {@code size} bytes long and its one mapping cover all of it,
     * and returns the address of its byte 0; see ContiguousFileFixFrameColumn#mapWritable.
     */
    private long mapAuxWritable(long size) {
        assert !isReadOnly;
        ensureAuxAllocated(size);
        if (auxMapSize < allocatedAuxBytes) {
            auxMapAddr = auxMapAddr == 0
                    ? TableUtils.mapRWNoAlloc(ff, auxFd, allocatedAuxBytes, 0, MEMORY_TAG)
                    : TableUtils.mremap(ff, auxFd, auxMapAddr, auxMapSize, allocatedAuxBytes, Files.MAP_RW, MEMORY_TAG);
            auxMapSize = allocatedAuxBytes;
        }
        return auxMapAddr;
    }

    /**
     * The data-file counterpart of {@link #mapAuxWritable}.
     */
    private long mapDataWritable(long size) {
        assert !isReadOnly;
        ensureDataAllocated(size);
        if (dataMapSize < allocatedDataBytes) {
            dataMapAddr = dataMapAddr == 0
                    ? TableUtils.mapRWNoAlloc(ff, dataFd, allocatedDataBytes, 0, MEMORY_TAG)
                    : TableUtils.mremap(ff, dataFd, dataMapAddr, dataMapSize, allocatedDataBytes, Files.MAP_RW, MEMORY_TAG);
            dataMapSize = allocatedDataBytes;
        }
        return dataMapAddr;
    }

    private long getDataAppendOffsetBytes(long appendOffsetRowCount) {
        // cache repeated calls to this method provided the append offset row count is the same
        if (this.appendOffsetRowCount != appendOffsetRowCount) {
            dataAppendOffsetBytes = columnTypeDriver.getDataVectorSizeAtFromFd(ff, auxFd, appendOffsetRowCount - 1);
            this.appendOffsetRowCount = appendOffsetRowCount;
        }
        return dataAppendOffsetBytes;
    }

    private void assertAuxWriteReserved(long fileOffsetHi) {
        assert isAllocatedAuxBytesKnown;
        assert fileOffsetHi <= allocatedAuxBytes : "positioned aux write exceeds reservation [column=" + columnIndex
                + ", writeHi=" + fileOffsetHi + ", allocated=" + allocatedAuxBytes + ']';
    }

    private void assertDataWriteReserved(long fileOffsetHi) {
        assert isAllocatedDataBytesKnown;
        assert fileOffsetHi <= allocatedDataBytes : "positioned data write exceeds reservation [column=" + columnIndex
                + ", writeHi=" + fileOffsetHi + ", allocated=" + allocatedDataBytes + ']';
    }

    private void mapAllRows(long rowHi) {
        if (!isReadOnly) {
            // Writable columns are not used yet, can be easily implemented if needed
            throw new UnsupportedOperationException("Cannot map writable column");
        }

        final long mapHi = Math.max(rowHi, mapRowHi);
        final long newAuxMemSize = columnTypeDriver.getAuxVectorSize(mapHi - columnTop);
        if (newAuxMemSize <= auxMapSize) {
            // The aux mapping already covers these rows, and the data mapping was sized from it.
            return;
        }

        // Grow both mappings. A kept-open column serves one piece after another, and a later piece - or the next
        // commit's plan, while the frame is cached - can reach higher than the first did. The files only grow at their
        // tails and every caller takes the addresses afresh after this call, so each mapping is remapped bigger.
        auxMapAddr = auxMapAddr == 0
                ? TableUtils.mapRO(ff, auxFd, newAuxMemSize, 0, MEMORY_TAG)
                : TableUtils.mremap(ff, auxFd, auxMapAddr, auxMapSize, newAuxMemSize, Files.MAP_RO, MEMORY_TAG);
        auxMapSize = newAuxMemSize;

        final long newDataMemSize = columnTypeDriver.getDataVectorSize(auxMapAddr, 0, mapHi - columnTop - 1);
        if (newDataMemSize > dataMapSize) {
            dataMapAddr = dataMapAddr == 0
                    ? TableUtils.mapRO(ff, dataFd, newDataMemSize, 0, MEMORY_TAG)
                    : TableUtils.mremap(ff, dataFd, dataMapAddr, dataMapSize, newDataMemSize, Files.MAP_RO, MEMORY_TAG);
            dataMapSize = newDataMemSize;
        }
    }

    /**
     * The address the source's row 0 WOULD be at in its AUX vector, which is what the merge index's absolute row ids
     * address.
     */
    private long rowZeroAuxAddr(FrameColumn column, long lo, long hi) {
        if (lo >= hi) {
            return 0;
        }
        final long top = column.getColumnTop();
        if (lo < top) {
            throw CairoException.critical(0).put("merge reads below a column top [column=").put(columnIndex)
                    .put(", rowLo=").put(lo)
                    .put(", columnTop=").put(top)
                    .put(']');
        }
        return column.getContiguousAuxAddr(hi) - columnTypeDriver.getAuxVectorOffset(top);
    }

    /**
     * Writes the aux entries of source rows {@code [lo, hi)}, shifted by {@code shift} onto this column's data offsets,
     * at the aux offset of target row {@code appendOffsetRowCount}: shifted into the write buffer and written from
     * there, a buffer's worth of rows at a time. A chunk's aux size counts every entry the chunk needs, so with an N+1
     * aux vector consecutive chunks write their shared boundary entry twice, with the same value.
     */
    private void writeShiftedAux(long shift, long srcAuxAddr, long lo, long hi, long appendOffsetRowCount) {
        long rowsPerChunk = ColumnWriteBuffer.MAX_SIZE >> 3;
        while (rowsPerChunk > 1 && columnTypeDriver.getAuxVectorSize(rowsPerChunk) > ColumnWriteBuffer.MAX_SIZE) {
            rowsPerChunk >>= 1;
        }
        final long buffer = writeBuffer.reserve(columnTypeDriver.getAuxVectorSize(Math.min(hi - lo, rowsPerChunk)));
        for (long chunkLo = lo; chunkLo < hi; chunkLo += rowsPerChunk) {
            final long chunkHi = Math.min(chunkLo + rowsPerChunk, hi);
            final long chunkAuxSize = columnTypeDriver.getAuxVectorSize(chunkHi - chunkLo);
            columnTypeDriver.shiftCopyAuxVector(shift, srcAuxAddr, chunkLo, chunkHi - 1, buffer, chunkAuxSize);
            ColumnWriteBuffer.write(
                    ff,
                    auxFd,
                    buffer,
                    chunkAuxSize,
                    columnTypeDriver.getAuxVectorOffset(appendOffsetRowCount + (chunkLo - lo))
            );
        }
    }

    private long sourceDataSize(long auxAddr, long lo, long hi) {
        return lo < hi ? columnTypeDriver.getDataVectorSize(auxAddr, lo, hi - 1) : 0;
    }
}
