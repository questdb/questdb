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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

@SuppressWarnings("unused")
public class DebugUtils {
    public static final Log LOG = LogFactory.getLog(DebugUtils.class);

    // ============================================================================================
    // TEMPORARY - file-length guards, added while hunting the var-column undergrowth bug that still
    // reproduces on the composite-partition (merge-append) branch: see COMPOSITE_PARTITION_STATE.md,
    // sections 25/26. A SIGBUS reading unbacked pages past a file's real on-disk length is uncatchable
    // and carries no Java-level detail; each of these turns that crash into a diagnosable
    // CairoException naming the table, column, partition and byte counts instead. Remove this whole
    // block once the write-side root cause is found and fixed - these are debug aids, not meant to
    // ship as permanent behavior.
    // ============================================================================================

    /**
     * Was {@code O3PartitionJob.processCompositePartition}'s inline check on the timestamp column it maps
     * to plan cuts and piece bounds.
     */
    public static void assertCompositeTimestampColumnLength(
            FilesFacade ff,
            long fd,
            long tsMapSize,
            TableToken tableToken,
            int partitionIndex,
            long e
    ) {
        final long tsFileLen = ff.length(fd);
        if (tsFileLen < tsMapSize) {
            throw CairoException.critical(0).put("composite timestamp column file too short [table=")
                    .put(tableToken).put(", partitionIndex=").put(partitionIndex)
                    .put(", fileLen=").put(tsFileLen).put(", tsMapSize=").put(tsMapSize)
                    .put(", e=").put(e).put(']');
        }
    }

    /**
     * Was {@code O3PartitionJob.executeCompositePlan}'s inline check on a FIXED-size column's file, after
     * the plan's action loop finishes writing.
     */
    public static void assertCompositePlanColumnLength(
            FilesFacade ff,
            long fd,
            long expectedBytes,
            CharSequence tableName,
            long partitionTimestamp,
            CharSequence columnName,
            int columnIndex,
            long columnTop,
            long e
    ) {
        final long actualBytes = ff.length(fd);
        if (actualBytes < expectedBytes) {
            throw CairoException.critical(0).put("composite plan undergrew a column file [table=")
                    .put(tableName)
                    .put(", partitionTs=").put(partitionTimestamp)
                    .put(", column=").put(columnName)
                    .put(", columnIndex=").put(columnIndex)
                    .put(", columnTop=").put(columnTop)
                    .put(", e=").put(e)
                    .put(", expectedBytes=").put(expectedBytes)
                    .put(", actualBytes=").put(actualBytes)
                    .put(']');
        }
    }

    /**
     * Was {@code O3PartitionJob.executeCompositePlan}'s inline check on a VAR-size column's AUX file,
     * after the plan's action loop finishes writing.
     */
    public static void assertCompositePlanVarColumnAuxLength(
            FilesFacade ff,
            long auxFd,
            long expectedAuxBytes,
            CharSequence tableName,
            long partitionTimestamp,
            CharSequence columnName,
            int columnIndex,
            long columnTop,
            long e
    ) {
        final long actualAuxBytes = ff.length(auxFd);
        if (actualAuxBytes < expectedAuxBytes) {
            throw CairoException.critical(0).put("composite plan undergrew a column's aux file [table=")
                    .put(tableName)
                    .put(", partitionTs=").put(partitionTimestamp)
                    .put(", column=").put(columnName)
                    .put(", columnIndex=").put(columnIndex)
                    .put(", columnTop=").put(columnTop)
                    .put(", e=").put(e)
                    .put(", expectedAuxBytes=").put(expectedAuxBytes)
                    .put(", actualAuxBytes=").put(actualAuxBytes)
                    .put(']');
        }
    }

    /**
     * Was {@code O3PartitionJob.executeCompositePlan}'s inline check on a VAR-size column's DATA file,
     * after the plan's action loop finishes writing.
     */
    public static void assertCompositePlanVarColumnDataLength(
            FilesFacade ff,
            long dataFd,
            long expectedDataBytes,
            CharSequence tableName,
            long partitionTimestamp,
            CharSequence columnName,
            int columnIndex,
            long columnTop,
            long e
    ) {
        final long actualDataBytes = ff.length(dataFd);
        if (actualDataBytes < expectedDataBytes) {
            throw CairoException.critical(0).put("composite plan undergrew a column's data file [table=")
                    .put(tableName)
                    .put(", partitionTs=").put(partitionTimestamp)
                    .put(", column=").put(columnName)
                    .put(", columnIndex=").put(columnIndex)
                    .put(", columnTop=").put(columnTop)
                    .put(", e=").put(e)
                    .put(", expectedDataBytes=").put(expectedDataBytes)
                    .put(", actualDataBytes=").put(actualDataBytes)
                    .put(']');
        }
    }

    /**
     * Was {@code ColumnTypeConverter.convertFixedToFixed}'s inline check on the conversion source file.
     */
    public static void assertConversionSourceFileLength(
            FilesFacade ff,
            long srcFixFd,
            long skipBytes,
            long mapBytes,
            long rowCount,
            long srcColumnTypeSize
    ) {
        final long srcFileLen = ff.length(srcFixFd);
        if (srcFileLen < skipBytes + mapBytes) {
            throw CairoException.critical(0).put("composite conversion source file too short [srcFixFd=").put(srcFixFd)
                    .put(", fileLen=").put(srcFileLen).put(", skipBytes=").put(skipBytes)
                    .put(", mapBytes=").put(mapBytes).put(", rowCount=").put(rowCount)
                    .put(", srcColumnTypeSize=").put(srcColumnTypeSize).put(']');
        }
    }

    /**
     * Was {@code ColumnTypeConverter.convertFromSymbol}'s inline check on the conversion source file.
     */
    public static void assertSymbolConversionSourceFileLength(
            FilesFacade ff,
            long srcFixFd,
            long skipBytes,
            long mapBytes,
            long rowCount
    ) {
        final long srcFileLen = ff.length(srcFixFd);
        if (srcFileLen < skipBytes + mapBytes) {
            throw CairoException.critical(0).put("composite symbol conversion source file too short [srcFixFd=").put(srcFixFd)
                    .put(", fileLen=").put(srcFileLen).put(", skipBytes=").put(skipBytes)
                    .put(", mapBytes=").put(mapBytes).put(", rowCount=").put(rowCount)
                    .put(']');
        }
    }

    /**
     * Was {@code ContiguousFileVarFrameColumn.mapAllRows}'s inline check on the AUX file, before mapping
     * it - the SOURCE side of a composite MERGE, over a piece an earlier commit may have left short.
     */
    public static void assertVarColumnAuxMapLength(
            FilesFacade ff,
            long auxFd,
            long expectedAuxBytes,
            int columnIndex,
            long rowHi,
            long columnTop
    ) {
        final long onDiskAuxLen = ff.length(auxFd);
        if (onDiskAuxLen < expectedAuxBytes) {
            throw CairoException.critical(0).put("var column aux file too short to map [columnIndex=").put(columnIndex)
                    .put(", rowHi=").put(rowHi)
                    .put(", columnTop=").put(columnTop)
                    .put(", expectedAuxBytes=").put(expectedAuxBytes)
                    .put(", onDiskAuxBytes=").put(onDiskAuxLen)
                    .put(']');
        }
    }

    /**
     * Was {@code ContiguousFileVarFrameColumn.mapAllRows}'s inline check on the DATA file, before mapping
     * it.
     */
    public static void assertVarColumnDataMapLength(
            FilesFacade ff,
            long dataFd,
            long expectedDataBytes,
            int columnIndex,
            long rowHi,
            long columnTop
    ) {
        final long onDiskDataLen = ff.length(dataFd);
        if (onDiskDataLen < expectedDataBytes) {
            throw CairoException.critical(0).put("var column data file too short to map [columnIndex=").put(columnIndex)
                    .put(", rowHi=").put(rowHi)
                    .put(", columnTop=").put(columnTop)
                    .put(", expectedDataBytes=").put(expectedDataBytes)
                    .put(", onDiskDataBytes=").put(onDiskDataLen)
                    .put(']');
        }
    }

    /**
     * Was {@code TableReader.reloadColumnAt}'s inline check on a var-size column's AUX file, before
     * mapping it - the plain read path, independent of the composite write path above.
     */
    public static void assertReaderVarColumnAuxLength(
            FilesFacade ff,
            Path path,
            long auxSize,
            CharSequence tableName,
            CharSequence columnName,
            int columnIndex,
            int partitionIndex,
            long partitionTimestamp,
            long columnTop,
            long columnRowCount,
            long partitionRowCount
    ) {
        final long onDiskAuxLen = ff.length(path.$());
        if (onDiskAuxLen < auxSize) {
            throw CairoException.critical(0).put("reader aux file too short [table=")
                    .put(tableName)
                    .put(", column=").put(columnName)
                    .put(", columnIndex=").put(columnIndex)
                    .put(", partitionIndex=").put(partitionIndex)
                    .put(", partitionTimestamp=").put(partitionTimestamp)
                    .put(", columnTop=").put(columnTop)
                    .put(", columnRowCount=").put(columnRowCount)
                    .put(", partitionRowCount=").put(partitionRowCount)
                    .put(", expectedAuxBytes=").put(auxSize)
                    .put(", onDiskAuxBytes=").put(onDiskAuxLen)
                    .put(']');
        }
    }
    // ============================================================================================
    // END TEMPORARY file-length guards
    // ============================================================================================

    // For debugging purposes
    public static boolean checkAscendingTimestamp(FilesFacade ff, long size, long fd) {
        if (size > 0) {
            long buffer = TableUtils.mapAppendColumnBuffer(ff, fd, 0, size * Long.BYTES, false, MemoryTag.MMAP_DEFAULT);
            try {
                long ts = Long.MIN_VALUE;
                for (int i = 0; i < size; i++) {
                    long nextTs = Unsafe.getLong(buffer + (long) i * Long.BYTES);
                    if (nextTs < ts) {
                        return false;
                    }
                    ts = nextTs;
                }
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, buffer, 0, size * Long.BYTES, MemoryTag.MMAP_DEFAULT);
            }
        }
        return true;
    }

    public static boolean isSparseVarCol(long colRowCount, long auxMemAddr, long dataMemAddr, int colType) {
        return ColumnType.getDriver(colType).isSparseDataVector(auxMemAddr, dataMemAddr, colRowCount);
    }

    // Useful debugging method
    public static boolean reconcileColumnTops(int partitionsSlotSize, LongList openPartitionInfo, ColumnVersionReader columnVersionReader, TableReader reader) {
        int partitionCount = reader.getPartitionCount();
        TimestampDriver driver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
        for (int p = 0; p < partitionCount; p++) {
            long partitionRowCount = reader.getPartitionRowCount(p);
            if (partitionRowCount != -1) {
                long partitionTimestamp = openPartitionInfo.getQuick(p * partitionsSlotSize);
                for (int c = 0; c < reader.getColumnCount(); c++) {
                    long colTop = Math.min(reader.getColumnTop(reader.getColumnBase(p), c), partitionRowCount);
                    long columnTopRaw = columnVersionReader.getColumnTop(partitionTimestamp, c);
                    long columnTop = Math.min(columnTopRaw == -1 ? partitionRowCount : columnTopRaw, partitionRowCount);
                    if (columnTop != colTop) {
                        LOG.critical().$("failed to reconcile column top [partition=").$ts(driver, partitionTimestamp)
                                .$(", column=").$(c)
                                .$(", expected=").$(columnTop)
                                .$(", actual=").$(colTop).$(']').
                                $();
                        return false;
                    }
                }
            }
        }
        return true;
    }

    static void assertO3IndexSorted(long indexAddr, long indexSize) {
        long lastTs = Long.MIN_VALUE;
        for (long i = 0; i < indexSize; i++) {
            long ts = Unsafe.getLong(indexAddr + 16 * i);
            long rowId = Unsafe.getLong(indexAddr + 16 * i + 8);
            assert ts >= lastTs : String.format("ts %,d lastTs %,d rowId %,d", ts, lastTs, rowId);
            lastTs = ts;
        }
    }

    static void assertTimestampColumnSorted(long columnAddr, long columnSize) {
        long lastTs = Long.MIN_VALUE;
        for (long i = 0; i < columnSize; i++) {
            long ts = Unsafe.getLong(columnAddr + 8 * i);
            assert ts >= lastTs : String.format("ts %,d lastTs %,d", ts, lastTs);
            lastTs = ts;
        }
    }

    static void logO3Index(TimestampDriver driver, long indexAddr, long indexSize, long tailLen) {
        long start = Math.max(0, indexSize - tailLen);
        for (long i = start; i < indexSize; i++) {
            long ts = Unsafe.getLong(indexAddr + 16 * i);
            long rowId = Unsafe.getLong(indexAddr + 16 * i + 8);
            LOG.info().$("index [").$(i).$("] = ").$ts(driver, ts).$(", ts=").$(ts).$(", rowId=").$(rowId).$();
        }
    }

    static void logTimestampColumn(TimestampDriver driver, long colAddr, long colSize, long tailLen) {
        long start = Math.max(0, colSize - tailLen);
        for (long i = start; i < colSize; i++) {
            long ts = Unsafe.getLong(colAddr + 8 * i);
            LOG.info().$("ts_col [").$(i).$("] = ").$ts(driver, ts).$(", ts=").$(ts).$();
        }
    }
}
