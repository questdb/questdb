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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.DirectIntList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

/**
 * Stages the columns a COVERING posting index includes while the index is built off a Parquet partition: opens a
 * mmap-backed scratch file pair per covered column in the partition directory, appends each decoded row group to
 * it - converting a lazily ALTERed column to its table type on the way - hands the scratch addresses to the index
 * writer for its seal, and removes the scratch files afterwards. Shared by every path that builds a covering posting
 * index off a Parquet partition.
 * <p>
 * Usage per index build: {@link #clear()}, one {@link #addSlot} or {@link #addDroppedSlot()} per covered column in
 * INCLUDE order, {@link #openScratchFiles}, {@link #accumulate} per decoded row group, {@link #configureCovering}
 * before the seal, and {@link #releaseScratchFiles} on every exit path. Not thread-safe: holds scratch sinks.
 */
final class ParquetCoveredColumnAccumulator {
    private final LongList coveringAddrs = new LongList();
    private final LongList coveringAuxAddrs = new LongList();
    private final IntList coveringShifts = new IntList();
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final Decimal64 decimal64 = new Decimal64();
    private final DirectUtf8String directUtf8String = new DirectUtf8String();
    // Two per slot, [aux, data]: aux is null for a fixed-size column, both are null for a slot that is not decoded.
    private final ObjList<MemoryMARW> mmaps = new ObjList<>();
    // Bytes appended to the slot's data vector so far; offsets the aux entries of every later row group.
    private final LongList slotDataBytesWritten = new LongList();
    // Index of the slot's chunk in the decoded row group, -1 when the slot is not decoded.
    private final IntList slotDecodedChunks = new IntList();
    private final LongList slotNameTxns = new LongList();
    private final ObjList<CharSequence> slotNames = new ObjList<>();
    // The parquet column to decode the slot from, -1 when it is not decoded.
    private final IntList slotParquetColumns = new IntList();
    // The parquet-stored type, which differs from the table type while a lazy ALTER COLUMN TYPE is pending.
    private final IntList slotParquetTypes = new IntList();
    private final LongList slotTops = new LongList();
    private final IntList slotTypes = new IntList();
    // -1 for a dropped covered column.
    private final IntList slotWriterIndices = new IntList();
    private final StringSink utf16Sink = new StringSink();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();

    /**
     * Appends one decoded row group's worth of every decoded slot to its scratch files, which grow via mmap
     * extend. Called once per row group, in row group order, from the index build's row-group loop.
     */
    void accumulate(RowGroupBuffers rowGroupBuffers, int rowGroupIndex, long rowGroupRowCount) {
        for (int slot = 0, n = slotWriterIndices.size(); slot < n; slot++) {
            final int decodedChunkIdx = slotDecodedChunks.getQuick(slot);
            if (decodedChunkIdx < 0) {
                continue;
            }
            final int columnType = slotTypes.getQuick(slot);
            final int parquetColType = slotParquetTypes.getQuick(slot);
            final long srcDataPtr = rowGroupBuffers.getChunkDataPtr(decodedChunkIdx);
            final long srcDataSize = rowGroupBuffers.getChunkDataSize(decodedChunkIdx);
            final long srcAuxPtr = rowGroupBuffers.getChunkAuxPtr(decodedChunkIdx);
            final long srcAuxSize = rowGroupBuffers.getChunkAuxSize(decodedChunkIdx);

            final MemoryMARW dataMem = mmaps.getQuick(2 * slot + 1);
            if (ColumnType.isVarSize(columnType)) {
                final MemoryMARW auxMem = mmaps.getQuick(2 * slot);
                final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                final long dataVecBytesWritten = slotDataBytesWritten.getQuick(slot);

                if (srcDataSize == 0 && srcAuxSize == 0) {
                    accumulateAllNullVarSizeChunk(
                            driver, auxMem, dataMem, rowGroupIndex,
                            rowGroupRowCount, dataVecBytesWritten);
                    slotDataBytesWritten.setQuick(slot,
                            dataVecBytesWritten + rowGroupRowCount * driver.getDataVectorMinEntrySize());
                    continue;
                }

                if (srcAuxSize == 0 && srcDataPtr != 0) {
                    final long convertedDataSize = accumulateFixedToVarChunk(
                            driver, columnType, parquetColType, dataMem, auxMem,
                            rowGroupBuffers, decodedChunkIdx, srcDataPtr,
                            rowGroupIndex, rowGroupRowCount, dataVecBytesWritten);
                    slotDataBytesWritten.setQuick(slot, dataVecBytesWritten + convertedDataSize);
                    continue;
                }

                long auxPtr = srcAuxPtr;
                long auxSize = srcAuxSize;
                if (rowGroupIndex > 0) {
                    driver.shiftCopyAuxVector(
                            -dataVecBytesWritten, auxPtr, 0,
                            rowGroupRowCount - 1, auxPtr, auxSize);
                    final long adjust = driver.getMinAuxVectorSize();
                    auxPtr += adjust;
                    auxSize -= adjust;
                }
                dataMem.putBlockOfBytes(srcDataPtr, srcDataSize);
                auxMem.putBlockOfBytes(auxPtr, auxSize);
                slotDataBytesWritten.setQuick(slot, dataVecBytesWritten + srcDataSize);
            } else {
                if (srcDataSize == 0 && srcAuxSize == 0) {
                    accumulateAllNullFixedChunk(dataMem, columnType, rowGroupRowCount);
                    continue;
                }
                final int srcTag = ColumnType.tagOf(parquetColType);
                final int dstTag = ColumnType.tagOf(columnType);
                if ((ColumnType.isVarSize(srcTag) || ColumnType.isSymbol(srcTag))
                        && !ColumnType.isVarSize(dstTag) && !ColumnType.isSymbol(dstTag)) {
                    final int effectiveSrcType = ColumnType.isSymbol(srcTag) ? ColumnType.VARCHAR : parquetColType;
                    // Convert straight into the destination mmap. The fixed output size is
                    // exact (rowCount * entrySize) and convertVarColumnToFixed writes every
                    // row positionally (a value or a null sentinel), so the whole region is
                    // filled with no intermediate buffer or copy.
                    final long fixSize = rowGroupRowCount * ColumnType.sizeOf(columnType);
                    final long dstPtr = dataMem.appendAddressFor(fixSize);
                    ParquetColumnTypeConverter.convertVarColumnToFixed(
                            effectiveSrcType, columnType, srcDataPtr, srcAuxPtr,
                            (int) rowGroupRowCount, dstPtr,
                            directUtf8String, utf16Sink,
                            decimal64, decimal128, decimal256);
                } else {
                    dataMem.putBlockOfBytes(srcDataPtr, srcDataSize);
                }
            }
        }
    }

    /**
     * Registers the next covered column as dropped: the index writer gets no data for it.
     */
    void addDroppedSlot() {
        addSlot(-1, -1, null, TableUtils.COLUMN_NAME_TXN_NONE, 0, -1);
    }

    /**
     * Registers the next covered column, in INCLUDE order.
     *
     * @param writerIndex        the covered column's writer index
     * @param columnType         its table type
     * @param columnName         its name, which names its scratch files
     * @param columnNameTxn      its column name txn in the partition
     * @param columnTop          the column top the index writer reads the column with
     * @param parquetColumnIndex the parquet column to decode it from, or -1 when it is not decoded, e.g. because
     *                           the column holds no data in this partition
     */
    void addSlot(
            int writerIndex,
            int columnType,
            CharSequence columnName,
            long columnNameTxn,
            long columnTop,
            int parquetColumnIndex
    ) {
        slotWriterIndices.add(writerIndex);
        slotTypes.add(columnType);
        slotNames.add(columnName);
        slotNameTxns.add(columnNameTxn);
        slotTops.add(columnTop);
        slotParquetColumns.add(parquetColumnIndex);
        slotParquetTypes.add(-1);
        slotDecodedChunks.add(-1);
        slotDataBytesWritten.add(0);
    }

    /**
     * Forgets the registered slots. Their scratch files must already be released.
     */
    void clear() {
        assert mmaps.size() == 0;
        slotWriterIndices.clear();
        slotTypes.clear();
        slotNames.clear();
        slotNameTxns.clear();
        slotTops.clear();
        slotParquetColumns.clear();
        slotParquetTypes.clear();
        slotDecodedChunks.clear();
        slotDataBytesWritten.clear();
    }

    /**
     * Hands the covered columns to the index writer: the scratch base addresses of every decoded slot - read after
     * the row-group loop, since an mmap extend can move them - and the registered types, tops and name txns.
     *
     * @param timestampWriterIndex the writer index of the designated timestamp, or -1
     */
    void configureCovering(ColumnIndexer indexer, int timestampWriterIndex) {
        final int coverCount = slotWriterIndices.size();
        coveringAddrs.setPos(coverCount);
        coveringAuxAddrs.setPos(coverCount);
        coveringShifts.setPos(coverCount);
        for (int slot = 0; slot < coverCount; slot++) {
            final int columnType = slotTypes.getQuick(slot);
            final MemoryMARW dataMem = mmaps.getQuiet(2 * slot + 1);
            final MemoryMARW auxMem = mmaps.getQuiet(2 * slot);
            coveringAddrs.setQuick(slot, dataMem != null && dataMem.isOpen() ? dataMem.addressOf(0) : 0);
            coveringAuxAddrs.setQuick(slot, auxMem != null && auxMem.isOpen() ? auxMem.addressOf(0) : 0);
            coveringShifts.setQuick(slot, slotWriterIndices.getQuick(slot) < 0 ? 0 : ColumnType.pow2SizeOf(columnType));
        }
        indexer.configureCovering(
                coveringAddrs,
                coveringAuxAddrs,
                slotTops,
                coveringShifts,
                slotWriterIndices,
                slotTypes,
                coverCount,
                timestampWriterIndex
        );
        indexer.setCoveredColumnNameTxns(slotNameTxns);
    }

    /**
     * Opens a scratch file pair in {@code partitionDir} for every registered slot with a parquet column, and adds
     * that column to {@code decodeColumns}, whose next chunk it then reads.
     *
     * @return how many covered columns are decoded
     */
    int openScratchFiles(
            CairoConfiguration configuration,
            FilesFacade ff,
            Path partitionDir,
            int dirLen,
            ParquetMetaFileReader parquetMetadata,
            long partitionRowCount,
            DirectIntList decodeColumns
    ) {
        assert mmaps.size() == 0;
        int decodedCount = 0;
        try {
            for (int slot = 0, n = slotWriterIndices.size(); slot < n; slot++) {
                mmaps.add(null);
                mmaps.add(null);
                final int parquetColumnIndex = slotParquetColumns.getQuick(slot);
                if (parquetColumnIndex < 0) {
                    continue;
                }
                final int columnType = slotTypes.getQuick(slot);
                final int parquetColumnType = parquetMetadata.getColumnType(parquetColumnIndex);
                final CharSequence columnName = slotNames.getQuick(slot);
                final long columnNameTxn = slotNameTxns.getQuick(slot);
                final boolean isVarSize = ColumnType.isVarSize(columnType);
                slotParquetTypes.setQuick(slot, parquetColumnType);
                slotDecodedChunks.setQuick(slot, (int) (decodeColumns.size() / 2));

                // Fixed-size data: total bytes are exact (rowCount * entrySize),
                // so the scratch file is pre-sized exactly and never extends.
                // Var-size data: decoded size depends on actual entry contents and
                // is not derivable from parquet metadata, so fall back to the
                // configured data-append page size and let the mmap grow on demand
                // at that pace -- the same extend pace TableWriter uses for every
                // other var-size data vector.
                final long dataSize = isVarSize
                        ? configuration.getDataAppendPageSize()
                        : Files.ceilPageSize((long) ColumnType.sizeOf(columnType) * partitionRowCount);
                // Registered before it opens, so a failed open is still freed by releaseScratchFiles.
                final MemoryMARW dataMem = Vm.getCMARWInstance();
                mmaps.setQuick(2 * slot + 1, dataMem);
                ff.removeQuiet(TableUtils.dFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn));
                dataMem.of(ff, TableUtils.dFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn), dataSize, 0L, MemoryTag.NATIVE_TABLE_WRITER);
                if (isVarSize) {
                    // Var-size aux: bytes are exact (driver-defined fixed entry
                    // width times row count, accounting for the N+1 storage model
                    // where applicable).
                    final long auxSize = Files.ceilPageSize(ColumnType.getDriver(columnType).getAuxVectorSize(partitionRowCount));
                    final MemoryMARW auxMem = Vm.getCMARWInstance();
                    mmaps.setQuick(2 * slot, auxMem);
                    ff.removeQuiet(TableUtils.iFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn));
                    auxMem.of(ff, TableUtils.iFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn), auxSize, 0L, MemoryTag.NATIVE_TABLE_WRITER);
                }

                decodeColumns.add(parquetColumnIndex);
                decodeColumns.add(ParquetColumnTypeConverter.chooseDecodeType(parquetColumnType, columnType));
                decodedCount++;
            }
        } finally {
            partitionDir.trimTo(dirLen);
        }
        return decodedCount;
    }

    /**
     * Drops the index writer's read mappings of the scratch files, unmaps them, removes them from
     * {@code partitionDir} and forgets the slots. Safe to call on every exit path, including after a partial
     * {@link #openScratchFiles} or none at all.
     */
    void releaseScratchFiles(ColumnIndexer indexer, FilesFacade ff, Path partitionDir, int dirLen) {
        try {
            indexer.releaseCoveredColumnReadMappings();
        } finally {
            Misc.freeObjListAndClear(mmaps);
            try {
                for (int slot = 0, n = slotWriterIndices.size(); slot < n; slot++) {
                    if (slotWriterIndices.getQuick(slot) < 0) {
                        continue;
                    }
                    final CharSequence columnName = slotNames.getQuick(slot);
                    final long columnNameTxn = slotNameTxns.getQuick(slot);
                    ff.removeQuiet(TableUtils.dFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn));
                    if (ColumnType.isVarSize(slotTypes.getQuick(slot))) {
                        ff.removeQuiet(TableUtils.iFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn));
                    }
                }
            } finally {
                partitionDir.trimTo(dirLen);
                clear();
            }
        }
    }

    private void accumulateAllNullFixedChunk(MemoryMARW mem, int columnType, long rowCount) {
        final long fixSize = rowCount * ColumnType.sizeOf(columnType);
        if (fixSize == 0) {
            return;
        }
        long nullBuf = Unsafe.malloc(fixSize, MemoryTag.NATIVE_TABLE_WRITER);
        try {
            TableUtils.setNull(columnType, nullBuf, rowCount);
            mem.putBlockOfBytes(nullBuf, fixSize);
        } finally {
            Unsafe.free(nullBuf, fixSize, MemoryTag.NATIVE_TABLE_WRITER);
        }
    }

    private void accumulateAllNullVarSizeChunk(
            ColumnTypeDriver driver,
            MemoryMARW auxMem,
            MemoryMARW dataMem,
            int rowGroupIndex,
            long rowCount,
            long dataVecBytesWritten
    ) {
        final long dataSize = rowCount * driver.getDataVectorMinEntrySize();
        final long auxSize = driver.getAuxVectorSize(rowCount);

        long nullDataBuf = 0;
        long nullAuxBuf = 0;
        try {
            if (dataSize > 0) {
                nullDataBuf = Unsafe.malloc(dataSize, MemoryTag.NATIVE_TABLE_WRITER);
                driver.setDataVectorEntriesToNull(nullDataBuf, rowCount);
                dataMem.putBlockOfBytes(nullDataBuf, dataSize);
            }
            if (auxSize > 0) {
                nullAuxBuf = Unsafe.malloc(auxSize, MemoryTag.NATIVE_TABLE_WRITER);
                driver.setFullAuxVectorNull(nullAuxBuf, rowCount);
                if (dataVecBytesWritten > 0 && rowCount > 0) {
                    driver.shiftCopyAuxVector(
                            -dataVecBytesWritten, nullAuxBuf, 0,
                            rowCount - 1, nullAuxBuf, auxSize);
                }
                long auxPtr = nullAuxBuf;
                long auxBytes = auxSize;
                if (rowGroupIndex > 0) {
                    final long adjust = driver.getMinAuxVectorSize();
                    auxPtr += adjust;
                    auxBytes -= adjust;
                }
                auxMem.putBlockOfBytes(auxPtr, auxBytes);
            }
        } finally {
            if (nullAuxBuf != 0) {
                Unsafe.free(nullAuxBuf, auxSize, MemoryTag.NATIVE_TABLE_WRITER);
            }
            if (nullDataBuf != 0) {
                Unsafe.free(nullDataBuf, dataSize, MemoryTag.NATIVE_TABLE_WRITER);
            }
        }
    }

    private long accumulateFixedToVarChunk(
            ColumnTypeDriver driver,
            int columnType,
            int parquetColType,
            MemoryMARW dataMem,
            MemoryMARW auxMem,
            RowGroupBuffers rowGroupBuffers,
            int decodedChunkIdx,
            long srcDataPtr,
            int rowGroupIndex,
            long rowGroupRowCount,
            long dataVecBytesWritten
    ) {
        final long auxSize = driver.getAuxVectorSize(rowGroupRowCount);
        final long auxBuf = Unsafe.malloc(auxSize, MemoryTag.NATIVE_TABLE_WRITER);
        try {
            final long dataBufCap;
            final long dataBuf;
            if (ColumnType.isVarchar(columnType)) {
                dataBufCap = ParquetColumnTypeConverter.estimateVarcharDataSize(parquetColType, (int) rowGroupRowCount);
                dataBuf = dataBufCap > 0 ? Unsafe.malloc(dataBufCap, MemoryTag.NATIVE_TABLE_WRITER) : 0;
            } else {
                dataBufCap = ParquetColumnTypeConverter.estimateStringDataSize(parquetColType, (int) rowGroupRowCount);
                dataBuf = Unsafe.malloc(dataBufCap, MemoryTag.NATIVE_TABLE_WRITER);
            }
            try {
                final int convColumnTop = (int) rowGroupBuffers.getChunkColumnTop(decodedChunkIdx);
                if (ColumnType.isVarchar(columnType)) {
                    ParquetColumnTypeConverter.convertFixedColumnToVarchar(parquetColType, srcDataPtr, (int) rowGroupRowCount, convColumnTop, auxBuf, dataBuf, dataBufCap, utf8Sink);
                } else {
                    ParquetColumnTypeConverter.convertFixedColumnToString(parquetColType, srcDataPtr, (int) rowGroupRowCount, convColumnTop, auxBuf, dataBuf, dataBufCap, utf16Sink);
                }
                final long actualDataSize = driver.getDataVectorSizeAt(auxBuf, rowGroupRowCount - 1);
                long auxWritePtr = auxBuf;
                long auxWriteSize = auxSize;
                if (rowGroupIndex > 0) {
                    driver.shiftCopyAuxVector(-dataVecBytesWritten, auxBuf, 0, rowGroupRowCount - 1, auxBuf, auxSize);
                    final long adjust = driver.getMinAuxVectorSize();
                    auxWritePtr += adjust;
                    auxWriteSize -= adjust;
                }
                if (actualDataSize > 0) {
                    dataMem.putBlockOfBytes(dataBuf, actualDataSize);
                }
                auxMem.putBlockOfBytes(auxWritePtr, auxWriteSize);
                return actualDataSize;
            } finally {
                if (dataBuf != 0) {
                    Unsafe.free(dataBuf, dataBufCap, MemoryTag.NATIVE_TABLE_WRITER);
                }
            }
        } finally {
            Unsafe.free(auxBuf, auxSize, MemoryTag.NATIVE_TABLE_WRITER);
        }
    }
}
