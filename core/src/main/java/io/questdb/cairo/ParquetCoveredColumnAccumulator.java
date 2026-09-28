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

import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

/**
 * Appends decoded Parquet row groups of the columns a COVERING posting index includes to mmap-backed scratch
 * files, converting a lazily ALTERed column to its table type on the way. Shared by every path that builds a
 * covering posting index off a Parquet partition. Not thread-safe: holds scratch sinks.
 */
final class ParquetCoveredColumnAccumulator {
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final Decimal64 decimal64 = new Decimal64();
    private final DirectUtf8String directUtf8String = new DirectUtf8String();
    private final StringSink utf16Sink = new StringSink();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();

    /**
     * Accumulates one decoded row group's worth of covered column data into
     * mmap-backed temp files. Called from the merged row-group loop inside
     * the parquet index builders. Each covered slot's MemoryMARW pair
     * (data + aux) in {@code covMmaps} grows via mmap extend; the caller
     * passes the final base addresses to PostingIndexWriter after the loop.
     *
     * <p>{@code covSlotMeta} layout per slot (4 longs):
     * [0] decodedChunkIdx (-1 if skipped), [1] colType, [2] dataVecBytesWritten,
     * [3] parquetColType (the parquet-stored type, which differs from colType
     * when a lazy ALTER COLUMN TYPE is pending on the covered column).
     */
    void accumulateCoveredColumnsFromRowGroup(
            IntList coveringColumnIndices,
            DirectLongList covSlotMeta,
            ObjList<MemoryMARW> covMmaps,
            RowGroupBuffers rowGroupBuffers,
            int rowGroupIndex,
            long rowGroupRowCount
    ) {
        final int coverCount = coveringColumnIndices.size();
        for (int slot = 0; slot < coverCount; slot++) {
            final int decodedChunkIdx = (int) covSlotMeta.get(4L * slot);
            if (decodedChunkIdx < 0) {
                continue;
            }
            final int columnType = (int) covSlotMeta.get(4L * slot + 1);
            final int parquetColType = (int) covSlotMeta.get(4L * slot + 3);
            final long srcDataPtr = rowGroupBuffers.getChunkDataPtr(decodedChunkIdx);
            final long srcDataSize = rowGroupBuffers.getChunkDataSize(decodedChunkIdx);
            final long srcAuxPtr = rowGroupBuffers.getChunkAuxPtr(decodedChunkIdx);
            final long srcAuxSize = rowGroupBuffers.getChunkAuxSize(decodedChunkIdx);

            final MemoryMARW dataMem = covMmaps.getQuick(2 * slot + 1);
            if (ColumnType.isVarSize(columnType)) {
                final MemoryMARW auxMem = covMmaps.getQuick(2 * slot);
                final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                final long dataVecBytesWritten = covSlotMeta.get(4L * slot + 2);

                if (srcDataSize == 0 && srcAuxSize == 0) {
                    accumulateAllNullVarSizeChunk(
                            driver, auxMem, dataMem, rowGroupIndex,
                            rowGroupRowCount, dataVecBytesWritten);
                    covSlotMeta.set(4L * slot + 2,
                            dataVecBytesWritten + rowGroupRowCount * driver.getDataVectorMinEntrySize());
                    continue;
                }

                if (srcAuxSize == 0 && srcDataPtr != 0) {
                    final long convertedDataSize = accumulateFixedToVarChunk(
                            driver, columnType, parquetColType, dataMem, auxMem,
                            rowGroupBuffers, decodedChunkIdx, srcDataPtr,
                            rowGroupIndex, rowGroupRowCount, dataVecBytesWritten);
                    covSlotMeta.set(4L * slot + 2, dataVecBytesWritten + convertedDataSize);
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
                covSlotMeta.set(4L * slot + 2, dataVecBytesWritten + srcDataSize);
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
