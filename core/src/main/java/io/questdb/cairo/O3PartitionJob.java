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

import io.questdb.MessageBus;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.OwnedMemoryPartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.griffin.engine.table.parquet.RowGroupStatBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectIntList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3OpenColumnTask;
import io.questdb.tasks.O3PartitionTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.O3OpenColumnJob.*;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class O3PartitionJob extends AbstractQueueConsumerJob<O3PartitionTask> {

    private static final Log LOG = LogFactory.getLog(O3PartitionJob.class);

    public O3PartitionJob(MessageBus messageBus) {
        super(messageBus.getO3PartitionQueue(), messageBus.getO3PartitionSubSeq());
    }

    public static void processParquetPartition(
            Path pathToTable,
            int timestampType,
            int partitionBy,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long partitionTimestamp,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            long srcNameTxn,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr,
            long o3TimestampMin,
            O3Basket o3Basket,
            long newPartitionSize,
            long oldPartitionSize
    ) {
        // Number of rows to insert from the O3 segment into this partition.
        final long srcOooBatchRowSize = srcOooHi - srcOooLo + 1;
        final TableRecordMetadata tableWriterMetadata = tableWriter.getMetadata();
        Path path = Path.getThreadLocal(pathToTable);
        setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);

        final int partitionIndex = tableWriter.getPartitionIndexByTimestamp(partitionTimestamp);
        final long parquetSize = tableWriter.getPartitionParquetFileSize(partitionIndex);
        long duplicateCount = 0;
        CairoConfiguration cairoConfiguration = tableWriter.getConfiguration();
        FilesFacade ff = tableWriter.getFilesFacade();
        try (
                PartitionDecoder partitionDecoder = new PartitionDecoder();
                RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
                DirectIntList parquetColumns = new DirectIntList(2L * tableWriterMetadata.getColumnCount(), MemoryTag.NATIVE_O3)
        ) {
            long parquetAddr = 0;
            try (
                    RowGroupStatBuffers rowGroupStatBuffers = new RowGroupStatBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
                    PartitionUpdater partitionUpdater = new PartitionUpdater(ff);
                    PartitionDescriptor partitionDescriptor = new OwnedMemoryPartitionDescriptor()
            ) {
                parquetAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                partitionDecoder.of(
                        parquetAddr,
                        parquetSize,
                        MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER
                );

                final int rowGroupCount = partitionDecoder.metadata().getRowGroupCount();
                assert rowGroupCount > 0;
                final int timestampIndex = tableWriterMetadata.getTimestampIndex();
                final int timestampColumnType = tableWriterMetadata.getColumnType(timestampIndex);
                assert ColumnType.isTimestamp(timestampColumnType);

                // for API completeness, we'll use the same configuration as the initial partition encoder.
                final int compressionCodec = cairoConfiguration.getPartitionEncoderParquetCompressionCodec();
                final int compressionLevel = cairoConfiguration.getPartitionEncoderParquetCompressionLevel();
                final int rowGroupSize = cairoConfiguration.getPartitionEncoderParquetRowGroupSize();
                final int dataPageSize = cairoConfiguration.getPartitionEncoderParquetDataPageSize();
                final boolean statisticsEnabled = cairoConfiguration.isPartitionEncoderParquetStatisticsEnabled();
                final boolean rawArrayEncoding = cairoConfiguration.isPartitionEncoderParquetRawArrayEncoding();

                // partitionUpdater is the owner of the partitionDecoder descriptor
                final int opts = cairoConfiguration.getWriterFileOpenOpts();
                partitionUpdater.of(
                        path.$(),
                        opts,
                        parquetSize,
                        timestampIndex,
                        ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                        statisticsEnabled,
                        rawArrayEncoding,
                        rowGroupSize,
                        dataPageSize
                );

                // The O3 range [srcOooLo, srcOooHi] has been split into intervals between row group minimums: [rowGroupN-1.min, rowGroupN.min].
                // Each of these intervals is merged into the previous row group (rowGroupN-1).
                //   +------+          <- rg0.min
                //   | rg0  |  +-----+ <- srcOooLo
                //   |      |  | OOO |
                //   +------+  |     |
                //             |     |
                //   +------+  |     | <- rg1.min
                //   | rg1  |  |     |
                //   |      |  |     |
                //   +------+  |     |
                //             |     |
                //   +------+  |     | <- rg2.min
                //   | rg2  |  |     |
                //   |      |  |     |
                //   +------+  |     |
                //             |     |
                //             +-----+ <- srcOooHi

                // on the first iteration, ooo range [srcOooLo, rg1.min]
                // is merged into row group 0.
                // on the second iteration, ooo range [rg1.min, rg2.min]
                // is merged into row group 1.
                // as a tail case, ooo range [rg2.min, srcOooHi]
                // is merged into row group 2.

                //   +------+          <- rg0.min
                //   | rg0  |  +-----+ <- srcOooLo
                //   |      |  | OOO |
                //   +------+  |     |
                //             +-----+
                //   +------+         <- rg1.min
                //   | rg1  |
                //   |      |
                //   +------+
                //
                //   +------+         <- rg2.min
                //   | rg2  |
                //   |      |
                //   +------+
                //
                //   +------+         <- rg3.min
                //   | rg3  |  +-----+ <- mergeRangeLo
                //   |      |  | OOO |
                //   +------+  |     |
                //             |     |
                //             +-----+ <- srcOooHi

                // on the first iteration, ooo range [srcOooLo, rg1.min]
                // is merged into row group 0.
                // on the second iteration, ooo range [rg1.min, rg2.min]
                // has no data, continue to the next row group.
                // on the third iteration, ooo range [rg2.min, rg3.min]
                // has no data, continue to the next row group.
                // as a tail case, ooo range [mergeRangeLo, srcOooHi]
                // is merged into row group 3.

                long mergeRangeLo = srcOooLo;
                for (int rowGroup = 1; rowGroup < rowGroupCount; rowGroup++) {
                    parquetColumns.clear();
                    parquetColumns.add(timestampIndex);
                    parquetColumns.add(timestampColumnType);
                    partitionDecoder.readRowGroupStats(rowGroupStatBuffers, parquetColumns, rowGroup);
                    final long min = rowGroupStatBuffers.getMinValueLong(0);
                    final long mergeRangeHi = Vect.boundedBinarySearchIndexT(
                            sortedTimestampsAddr,
                            min,
                            mergeRangeLo,
                            srcOooHi,
                            Vect.BIN_SEARCH_SCAN_DOWN
                    );

                    // has no data to merge, continue to the next row group
                    if (mergeRangeHi < mergeRangeLo) {
                        continue;
                    }

                    duplicateCount += mergeRowGroup(
                            partitionDescriptor,
                            partitionUpdater,
                            parquetColumns,
                            oooColumns,
                            sortedTimestampsAddr,
                            tableWriter,
                            partitionDecoder,
                            rowGroupBuffers,
                            rowGroup - 1,
                            timestampIndex,
                            partitionTimestamp,
                            mergeRangeLo,
                            mergeRangeHi,
                            tableWriterMetadata,
                            srcOooBatchRowSize,
                            dedupColSinkAddr
                    );
                    mergeRangeLo = mergeRangeHi + 1;
                }

                if (mergeRangeLo <= srcOooHi) {
                    // merge the tail [mergeRangeLo, srcOooHi] into the last row group
                    // this also handles the case where there is only a single row group
                    duplicateCount += mergeRowGroup(
                            partitionDescriptor,
                            partitionUpdater,
                            parquetColumns,
                            oooColumns,
                            sortedTimestampsAddr,
                            tableWriter,
                            partitionDecoder,
                            rowGroupBuffers,
                            rowGroupCount - 1,
                            timestampIndex,
                            partitionTimestamp,
                            mergeRangeLo,
                            srcOooHi,
                            tableWriterMetadata,
                            srcOooBatchRowSize,
                            dedupColSinkAddr
                    );
                }
                partitionUpdater.updateFileMetadata();
            } finally {
                if (parquetAddr != 0) {
                    ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                }
            }

            // Update indexes
            final long newParquetSize = Files.length(path.$());
            updateParquetIndexes(
                    partitionBy,
                    partitionTimestamp,
                    tableWriter,
                    srcNameTxn,
                    o3Basket,
                    newPartitionSize,
                    newParquetSize,
                    pathToTable,
                    path,
                    ff,
                    partitionDecoder,
                    tableWriterMetadata,
                    parquetColumns,
                    rowGroupBuffers
            );
        } catch (Throwable th) {
            LOG.error().$("process partition error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(th)
                    .I$();
            // the file is re-opened here because PartitionUpdater owns the file descriptor
            path.of(pathToTable);
            setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
            final long fd = TableUtils.openRW(ff, path.$(), LOG, cairoConfiguration.getWriterFileOpenOpts());
            // truncate partition file to the previous uncorrupted size
            if (!ff.truncate(fd, parquetSize)) {
                LOG.error().$("could not truncate partition file [path=").$(path).I$();
            }
            ff.close(fd);
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(th));
        } finally {
            path.of(pathToTable);
            setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
            final long fileSize = Files.length(path.$());
            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr, partitionTimestamp);
            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + Long.BYTES, o3TimestampMin);
            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, newPartitionSize - duplicateCount);
            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, oldPartitionSize);
            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, 1); // partitionMutates
            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 5 * Long.BYTES, 0); // o3SplitPartitionSize
            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 7 * Long.BYTES, fileSize); // update parquet partition file size

            tableWriter.o3CountDownDoneLatch();
            tableWriter.o3ClockDownPartitionUpdateCount();
        }
    }

    public static void processPartition(
            Path pathToTable,
            int partitionBy,
            ObjList<MemoryMA> columns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long o3TimestampMin,
            long partitionTimestamp,
            long maxTimestamp,
            long srcDataMax,
            long srcNameTxn,
            boolean last,
            long txn,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long newPartitionSize,
            final long oldPartitionSize,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr,
            boolean isParquet,
            long o3TimestampLo,
            long o3TimestampHi
    ) {
        // is out of order data hitting the last partition?
        // if so we do not need to re-open files and write to existing file descriptors
        final RecordMetadata metadata = tableWriter.getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(metadata.getTimestampType());

        if (isParquet) {
            processParquetPartition(
                    pathToTable,
                    tableWriter.getMetadata().getTimestampType(),
                    partitionBy,
                    oooColumns,
                    srcOooLo,
                    srcOooHi,
                    partitionTimestamp,
                    sortedTimestampsAddr,
                    tableWriter,
                    srcNameTxn,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr,
                    o3TimestampMin,
                    o3Basket,
                    newPartitionSize,
                    oldPartitionSize
            );
            return;
        }

        final Path path = Path.getThreadLocal(pathToTable);

        long srcTimestampFd = 0;
        long dataTimestampLo;
        long dataTimestampHi;
        final FilesFacade ff = tableWriter.getFilesFacade();
        long oldPartitionTimestamp;

        // partition might be subject to split
        // if this happens the size of the original (srcDataPartition) partition will decrease
        // and the size of new (o3Partition) will be non-zero
        long srcDataNewPartitionSize = newPartitionSize;

        long o3SplitPartitionSize = 0;

        if (srcDataMax < 1) {
            // This has to be a brand-new partition for any of three cases:
            // - This partition is above min partition of the table.
            // - This partition is below max partition of the table.
            // - This is last partition that is empty.
            // pure OOO data copy into new partition

            if (!last) {
                try {
                    LOG.debug().$("would create [path=").$(path.slash$()).I$();
                    TableUtils.setPathForNativePartition(
                            path.trimTo(pathToTable.size()),
                            tableWriter.getMetadata().getTimestampType(),
                            partitionBy,
                            partitionTimestamp,
                            txn - 1
                    );
                    createDirsOrFail(ff, path, tableWriter.getConfiguration().getMkDirMode());
                } catch (Throwable e) {
                    LOG.error().$("process new partition error [table=").$(tableWriter.getTableToken())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                    tableWriter.o3ClockDownPartitionUpdateCount();
                    tableWriter.o3CountDownDoneLatch();
                    throw e;
                }
            }

            assert oldPartitionSize == 0;

            if (tableWriter.isCommitReplaceMode()) {
                assert srcOooLo <= srcOooHi;
                // Recalculate the resulting min timestamp to be the first row
                // of the new data.
                // o3TimestampMin is the min replaceRangeLo timestamp
                o3TimestampMin = getTimestampIndexValue(sortedTimestampsAddr, srcOooLo);
            }

            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    o3TimestampMin,
                    partitionTimestamp,
                    partitionTimestamp,
                    // below parameters are unused by this type of append
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    srcNameTxn,
                    OPEN_NEW_PARTITION_FOR_APPEND,
                    0,  // timestamp fd
                    0,
                    0,
                    timestampIndex,
                    timestampDriver,
                    sortedTimestampsAddr,
                    newPartitionSize,
                    oldPartitionSize,
                    0,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr
            );
        } else {
            long srcTimestampAddr = 0;
            long srcTimestampSize = 0;
            int prefixType;
            long prefixLo;
            long prefixHi;
            int mergeType;
            long mergeDataLo;
            long mergeDataHi;
            long mergeO3Lo;
            long mergeO3Hi;
            int suffixType;
            long suffixLo;
            long suffixHi;
            final int openColumnMode;
            long newMinPartitionTimestamp;

            try {
                // out of order is hitting existing partition
                // partitionTimestamp is in fact a ceil of ooo timestamp value for the given partition
                // so this check is for matching ceilings
                if (last) {
                    dataTimestampHi = maxTimestamp;
                    srcTimestampSize = srcDataMax * 8L;
                    // negative fd indicates descriptor reuse
                    srcTimestampFd = -columns.getQuick(getPrimaryColumnIndex(timestampIndex)).getFd();
                    srcTimestampAddr = mapRW(ff, -srcTimestampFd, srcTimestampSize, MemoryTag.MMAP_O3);
                } else {
                    srcTimestampSize = srcDataMax * 8L;
                    // out of order data is going into archive partition
                    // we need to read "low" and "high" boundaries of the partition. "low" being the oldest timestamp
                    // and "high" being newest

                    TableUtils.setPathForNativePartition(
                            path.trimTo(pathToTable.size()),
                            tableWriter.getMetadata().getTimestampType(),
                            partitionBy,
                            partitionTimestamp,
                            srcNameTxn
                    );

                    // also track the fd that we need to eventually close
                    // Open src timestamp column as RW in case append happens
                    srcTimestampFd = openRW(ff, dFile(path, metadata.getColumnName(timestampIndex), COLUMN_NAME_TXN_NONE), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                    srcTimestampAddr = mapRW(ff, srcTimestampFd, srcTimestampSize, MemoryTag.MMAP_O3);
                    dataTimestampHi = Unsafe.getUnsafe().getLong(srcTimestampAddr + srcTimestampSize - Long.BYTES);
                }
                dataTimestampLo = Unsafe.getUnsafe().getLong(srcTimestampAddr);

                // create copy jobs
                // we will have maximum of 3 stages:
                // - prefix data
                // - merge job
                // - suffix data
                //
                // prefix and suffix can be sourced either from OO fully or from Data (written to disk) fully
                // so for prefix and suffix we will need a flag indicating source of the data
                // as well as range of rows in that source

                mergeType = O3_BLOCK_NONE;
                mergeDataLo = -1;
                mergeDataHi = -1;
                mergeO3Lo = -1;
                mergeO3Hi = -1;
                suffixType = O3_BLOCK_NONE;
                suffixLo = -1;
                suffixHi = -1;

                assert srcTimestampFd != -1 && srcTimestampFd != 1;

                int branch;

                // When deduplication is enabled, we want to take into the merge
                // the rows from the partition which equals to the O3 min and O3 max.
                // Without taking equal rows, deduplication will be incorrect.
                // Without deduplication, taking timestamp == o3TimestampHi into merge
                // can result into unnecessary partition rewrites, when instead of appending
                // rows with equal timestamp a merge is triggered.
                long mergeEquals = tableWriter.isCommitDedupMode() || tableWriter.isCommitReplaceMode() ? 1 : 0;

                if (o3TimestampLo >= dataTimestampLo) {
                    //   +------+
                    //   | data |  +-----+
                    //   |      |  | OOO |
                    //   |      |  |     |

                    // When deduplication is enabled, take into the merge the rows which are equals
                    // to the dataTimestampHi in the else block
                    if (o3TimestampLo >= dataTimestampHi + mergeEquals) {

                        // +------+
                        // | data |
                        // |      |
                        // +------+
                        //
                        //           +-----+
                        //           | OOO |
                        //           |     |
                        //
                        branch = 1;
                        suffixType = O3_BLOCK_O3;
                        suffixLo = srcOooLo;
                        suffixHi = srcOooHi;

                        prefixType = O3_BLOCK_DATA;
                        prefixLo = 0;
                        prefixHi = srcDataMax - 1;
                    } else {

                        //
                        // +------+
                        // |      |
                        // |      | +-----+
                        // | data | | OOO |
                        // +------+

                        prefixLo = 0;
                        // When deduplication is enabled, take into the merge the rows which are equals
                        // to the o3TimestampLo in the else block, e.g. reduce the prefix size
                        prefixHi = Vect.boundedBinarySearch64Bit(
                                srcTimestampAddr,
                                o3TimestampLo - mergeEquals,
                                0,
                                srcDataMax - 1,
                                Vect.BIN_SEARCH_SCAN_DOWN
                        );
                        prefixType = prefixLo <= prefixHi ? O3_BLOCK_DATA : O3_BLOCK_NONE;
                        mergeDataLo = prefixHi + 1;
                        mergeO3Lo = srcOooLo;

                        if (o3TimestampHi < dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | +-----+
                            // +------+

                            branch = 2;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampHi,
                                    mergeDataLo,
                                    srcDataMax - 1,
                                    Vect.BIN_SEARCH_SCAN_DOWN
                            );
                            assert mergeDataHi > -1;

                            if (mergeDataLo > mergeDataHi) {
                                // the o3 data implodes right between rows of existing data
                                // so we will have both data prefix and suffix and the middle bit
                                // is the out of order
                                mergeType = O3_BLOCK_O3;
                            } else {
                                mergeType = O3_BLOCK_MERGE;
                            }

                            suffixType = O3_BLOCK_DATA;
                            suffixLo = mergeDataHi + 1;
                            suffixHi = srcDataMax - 1;
                            assert suffixLo <= suffixHi;

                        } else if (o3TimestampHi > dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ |     |
                            //          |     |
                            //          +-----+

                            branch = 3;
                            // When deduplication is enabled, take in to the merge
                            // all o3 rows that are equal to the last row in the data
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi,
                                    srcOooLo,
                                    srcOooHi,
                                    tableWriter.isCommitDedupMode() ? Vect.BIN_SEARCH_SCAN_DOWN : Vect.BIN_SEARCH_SCAN_UP
                            );

                            mergeDataHi = srcDataMax - 1;
                            assert mergeDataLo <= mergeDataHi;

                            mergeType = O3_BLOCK_MERGE;
                            suffixType = O3_BLOCK_O3;
                            suffixLo = mergeO3Hi + 1;
                            if (suffixLo > srcOooHi && tableWriter.isCommitReplaceMode()) {
                                // In replace mode o3TimestampHi can be greater than the highest timestamp in the o3 data
                                // This means that the suffix has to include all the o3 data
                                suffixLo = srcOooHi;
                            }
                            suffixHi = srcOooHi;
                            assert suffixLo <= suffixHi : String.format("Branch %,d suffixLo %,d > suffixHi %,d",
                                    branch, suffixLo, suffixHi);
                        } else {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ +-----+
                            //

                            branch = 4;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = srcDataMax - 1;
                            assert mergeDataLo <= mergeDataHi;
                        }
                    }
                } else {

                    //            +-----+
                    //            | OOO |
                    //
                    //  +------+
                    //  | data |

                    prefixType = O3_BLOCK_O3;
                    prefixLo = srcOooLo;
                    if (dataTimestampLo <= o3TimestampHi) {

                        //
                        //  +------+  | OOO |
                        //  | data |  +-----+
                        //  |      |

                        mergeDataLo = 0;
                        // To make inserts stable o3 rows with timestamp == dataTimestampLo
                        // should go into the merge section.
                        prefixHi = Vect.boundedBinarySearchIndexT(
                                sortedTimestampsAddr,
                                dataTimestampLo - 1,
                                srcOooLo,
                                srcOooHi,
                                Vect.BIN_SEARCH_SCAN_DOWN
                        );
                        mergeO3Lo = prefixHi + 1;

                        if (o3TimestampHi < dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | +-----+
                            // |      |
                            // +------+

                            branch = 5;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            // To make inserts stable table rows with timestamp == o3TimestampHi
                            // should go into the merge section.
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampHi,
                                    0,
                                    srcDataMax - 1,
                                    Vect.BIN_SEARCH_SCAN_DOWN
                            );

                            suffixLo = mergeDataHi + 1;
                            suffixType = O3_BLOCK_DATA;
                            suffixHi = srcDataMax - 1;
                            assert suffixLo <= suffixHi : String.format("Branch %,d suffixLo %,d > suffixHi %,d",
                                    branch, suffixLo, suffixHi);
                        } else if (o3TimestampHi > dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ |     |
                            //          +-----+

                            branch = 6;
                            mergeDataHi = srcDataMax - 1;
                            // To deduplicate O3 rows with timestamp == dataTimestampHi
                            // should go into the merge section.
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi - 1 + mergeEquals,
                                    mergeO3Lo,
                                    srcOooHi,
                                    Vect.BIN_SEARCH_SCAN_DOWN
                            );

                            if (mergeO3Lo > mergeO3Hi && !tableWriter.isCommitReplaceMode()) {
                                mergeType = O3_BLOCK_DATA;
                            } else {
                                mergeType = O3_BLOCK_MERGE;
                            }

                            if (mergeO3Hi < srcOooHi) {
                                suffixLo = mergeO3Hi + 1;
                                suffixType = O3_BLOCK_O3;
                                suffixHi = Math.max(suffixLo, srcOooHi);
                            }
                        } else {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ +-----+

                            branch = 7;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = srcDataMax - 1;
                        }
                    } else {
                        //            +-----+
                        //            | OOO |
                        //            +-----+
                        //
                        //  +------+
                        //  | data |
                        //
                        branch = 8;
                        prefixHi = srcOooHi;
                        suffixType = O3_BLOCK_DATA;
                        suffixLo = 0;
                        suffixHi = srcDataMax - 1;
                    }
                }

                // Save initial overlap state, mergeType can be re-written in commit replace mode
                boolean overlaps = mergeType == O3_BLOCK_MERGE;
                if (tableWriter.isCommitReplaceMode()) {
                    if (prefixHi < prefixLo) {
                        prefixType = O3_BLOCK_NONE;
                        // prefixType == O3_BLOCK_NONE and prefixHi >= also is used
                        // to indicate split partition. To avoid that, set prefixHi to -1.
                        prefixLo = 0;
                        prefixHi = -1;
                    }
                    if (suffixHi < suffixLo) {
                        suffixType = O3_BLOCK_NONE;
                    }
                }

                // Recalculate min timestamp value for this partition, it can be used
                // to replace table min timestamp value if it's the first partition.
                // Do not take existing data timestamp min value if it's being fully replaced.
                if (!tableWriter.isCommitReplaceMode()) {
                    newMinPartitionTimestamp = Math.min(o3TimestampMin, dataTimestampLo);
                } else {
                    newMinPartitionTimestamp = calculateMinDataTimestampAfterReplacement(
                            srcTimestampAddr,
                            sortedTimestampsAddr,
                            prefixType,
                            suffixType,
                            prefixLo,
                            suffixLo,
                            srcOooLo,
                            srcOooHi
                    );
                }

                if (tableWriter.isCommitReplaceMode()) {

                    if (mergeType == O3_BLOCK_MERGE) {
                        // When replace range deduplication mode is enabled, we need to take into the merge
                        // prefix and suffix it's O3 type.
                        newPartitionSize -= mergeDataHi - mergeDataLo + 1;
                        srcDataNewPartitionSize -= mergeDataHi - mergeDataLo + 1;
                    }

                    if (srcOooLo <= srcOooHi) {
                        if (mergeType == O3_BLOCK_MERGE) {

                            long removedDataRangeLo, removedDataRangeHi, o3RangeLo, o3RangeHi;
                            if (prefixType == O3_BLOCK_O3) {
                                // O3 in prefix, partition data in the suffix.
                                prefixHi = mergeO3Hi;
                                mergeType = O3_BLOCK_NONE;
                                mergeO3Hi = -1;
                                mergeO3Lo = -1;
                                mergeDataHi = -1;
                                mergeDataLo = -1;

                                removedDataRangeLo = 0;
                                removedDataRangeHi = suffixLo - 1;
                                o3RangeLo = prefixLo;
                                o3RangeHi = prefixHi;
                            } else if (suffixType == O3_BLOCK_O3) {
                                // Partition data in the prefix, O3 in suffix.
                                suffixLo = mergeO3Lo;
                                mergeType = O3_BLOCK_NONE;
                                mergeO3Hi = -1;
                                mergeO3Lo = -1;
                                mergeDataHi = -1;
                                mergeDataLo = -1;

                                removedDataRangeLo = prefixHi + 1;
                                removedDataRangeHi = srcDataMax - 1;
                                o3RangeLo = suffixLo;
                                o3RangeHi = suffixHi;
                            } else {
                                // Replacing partition data with new data in the middle of the partition.
                                removedDataRangeLo = mergeDataLo;
                                removedDataRangeHi = mergeDataHi;
                                o3RangeLo = mergeO3Lo;
                                o3RangeHi = mergeO3Hi;
                            }

                            if (removedDataRangeHi - removedDataRangeLo > 0 && removedDataRangeHi - removedDataRangeLo == o3RangeHi - o3RangeLo) {

                                // Check that replace first timestamp matches exactly the first timestamp in the partition
                                // and the last timestamp matches the last timestamp in the partition.
                                if (Unsafe.getUnsafe().getLong(srcTimestampAddr + removedDataRangeLo * Long.BYTES)
                                        == getTimestampIndexValue(sortedTimestampsAddr, o3RangeLo)
                                        && Unsafe.getUnsafe().getLong(srcTimestampAddr + removedDataRangeHi * Long.BYTES)
                                        == getTimestampIndexValue(sortedTimestampsAddr, o3RangeHi)) {

                                    // We are replacing with exactly the same number of rows
                                    // Maybe the rows are of the same data, then we don't need to rewrite the partition
                                    if (tableWriter.checkReplaceCommitIdenticalToPartition(
                                            partitionTimestamp,
                                            srcNameTxn,
                                            srcDataMax,
                                            removedDataRangeLo,
                                            removedDataRangeHi,
                                            o3RangeLo,
                                            o3RangeHi
                                    )) {
                                        LOG.info().$("replace commit resulted in identical data [table=").$(tableWriter.getTableToken())
                                                .$(", partitionTimestamp=").$ts(timestampDriver, partitionTimestamp)
                                                .$(", srcNameTxn=").$(srcNameTxn)
                                                .I$();
                                        // No need to update partition, it is identical to the existing one
                                        updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, newMinPartitionTimestamp, oldPartitionSize, oldPartitionSize, 0);
                                        return;
                                    }
                                }
                            }
                        }
                    } else {
                        // Replacing data with no O3 data, e.g. effectively deleting a part of the partition

                        // O3 data is supposed to be merged into the middle of an existing partition
                        // but there is no O3 data, it's a replacing commit with no new rows, just the range.
                        // At the end we have existing column data prefix, suffix and nothing to insert in between.
                        // We can finish here without modifying this partition.
                        boolean noop = mergeType == O3_BLOCK_O3 && prefixType == O3_BLOCK_DATA && suffixType == O3_BLOCK_DATA;

                        // No intersection with existing partition data, the whole replace range is before the existing partition
                        noop |= suffixType == O3_BLOCK_DATA && suffixLo == 0 && suffixHi == srcDataMax - 1;

                        // No intersection with existing partition data, the whole replace range is after the existing partition
                        noop |= prefixType == O3_BLOCK_DATA && prefixLo == 0 && prefixHi == srcDataMax - 1;

                        if (noop) {
                            updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, newMinPartitionTimestamp, oldPartitionSize, oldPartitionSize, 0);
                            return;
                        }

                        // srcOooLo > srcOooHi means that O3 data is empty
                        if (prefixType == O3_BLOCK_O3) {
                            prefixType = O3_BLOCK_NONE;
                            // prefixType == O3_BLOCK_NONE and prefixHi >= also is used
                            // to indicate split partition. To avoid that, set prefixHi to -1.
                            prefixLo = 0;
                            prefixHi = -1;
                        }

                        // srcOooLo > srcOooHi means that O3 data is empty
                        mergeType = O3_BLOCK_NONE;
                        if (suffixType == O3_BLOCK_O3) {
                            suffixType = O3_BLOCK_NONE;
                        }

                        if (prefixType == O3_BLOCK_NONE && suffixType == O3_BLOCK_NONE) {
                            // full partition removal
                            updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, Long.MAX_VALUE, 0, oldPartitionSize, 1);
                            return;
                        }

                        if (prefixType == O3_BLOCK_DATA && prefixHi >= prefixLo && suffixType == O3_BLOCK_NONE) {
                            // No merge, no suffix, only data prefix

                            if (prefixHi - prefixLo + 1 == srcDataMax) {
                                // If the number of rows is the same as the number of rows in the partition
                                // There is nothing to do

                                // Nothing to do, use the existing partition to the prefix size
                                updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, newMinPartitionTimestamp, prefixHi + 1, oldPartitionSize, 0);
                                return;
                            }

                            // The number of rows in the partition is lower.
                            // We cannot simply trim the partition to lower row numbers
                            // because the next commit can start overwriting the tail of the partition
                            // while there can be old readers that still read the data
                            // Proceed with another copy of the partition, but instead of
                            // copying, spit the last line into the suffix so that if it's economical
                            // to split the partition, it will be split.
                            // Split the last line of the partition
                            if (prefixHi > prefixLo) {
                                suffixHi = suffixLo = prefixHi;
                                suffixType = O3_BLOCK_DATA;
                                prefixHi--;
                            }
                        }
                    }
                }

                LOG.debug()
                        .$("o3 merge [branch=").$(branch)
                        .$(", prefixType=").$(prefixType)
                        .$(", prefixLo=").$(prefixLo)
                        .$(", prefixHi=").$(prefixHi)
                        .$(", o3TimestampLo=").$ts(timestampDriver, o3TimestampLo)
                        .$(", o3TimestampHi=").$ts(timestampDriver, o3TimestampHi)
                        .$(", o3TimestampMin=").$ts(timestampDriver, o3TimestampMin)
                        .$(", dataTimestampLo=").$ts(timestampDriver, dataTimestampLo)
                        .$(", dataTimestampHi=").$ts(timestampDriver, dataTimestampHi)
                        .$(", partitionTimestamp=").$ts(timestampDriver, partitionTimestamp)
                        .$(", srcDataMax=").$(srcDataMax)
                        .$(", mergeType=").$(mergeType)
                        .$(", mergeDataLo=").$(mergeDataLo)
                        .$(", mergeDataHi=").$(mergeDataHi)
                        .$(", mergeO3Lo=").$(mergeO3Lo)
                        .$(", mergeO3Hi=").$(mergeO3Hi)
                        .$(", suffixType=").$(suffixType)
                        .$(", suffixLo=").$(suffixLo)
                        .$(", suffixHi=").$(suffixHi)
                        .$(", table=").$(pathToTable)
                        .I$();

                oldPartitionTimestamp = partitionTimestamp;
                boolean partitionSplit = false;

                // Split partition if the prefix is large enough (relatively and absolutely)
                if (
                        prefixType == O3_BLOCK_DATA
                                && (mergeType == O3_BLOCK_MERGE || mergeType == O3_BLOCK_O3)
                                && prefixHi >= tableWriter.getPartitionO3SplitThreshold()
                                && prefixHi > 2 * (mergeDataHi - mergeDataLo + suffixHi - suffixLo + mergeO3Hi - mergeO3Lo)
                ) {
                    // large prefix copy, better to split the partition
                    long maxSourceTimestamp = Unsafe.getUnsafe().getLong(srcTimestampAddr + prefixHi * Long.BYTES);
                    assert maxSourceTimestamp <= o3TimestampLo;
                    boolean canSplit = true;

                    if (maxSourceTimestamp == o3TimestampLo) {
                        // We cannot split the partition if existing data has timestamp with exactly same value
                        // because 2 partition parts cannot have data with exactly same timestamp.
                        // To make this work, we can reduce the prefix by the size of the rows which equals to o3TimestampLo.
                        long newPrefixHi = -1 + Vect.boundedBinarySearch64Bit(
                                srcTimestampAddr,
                                o3TimestampLo,
                                prefixLo,
                                prefixHi - 1,
                                Vect.BIN_SEARCH_SCAN_UP
                        );

                        if (newPrefixHi > -1L) {
                            long shiftLeft = prefixHi - newPrefixHi;
                            long newMergeDataLo = mergeDataLo - shiftLeft;
                            // Check that splitting still makes sense
                            if (newPrefixHi >= tableWriter.getPartitionO3SplitThreshold()
                                    && newPrefixHi > 2 * (mergeDataHi - newMergeDataLo + suffixHi - suffixLo + mergeO3Hi - mergeO3Lo)
                            ) {
                                prefixHi = newPrefixHi;
                                mergeDataLo = newMergeDataLo;
                                maxSourceTimestamp = Unsafe.getUnsafe().getLong(srcTimestampAddr + prefixHi * Long.BYTES);
                                mergeType = O3_BLOCK_MERGE;
                                assert maxSourceTimestamp < o3TimestampLo;
                            } else {
                                canSplit = false;
                            }
                        } else {
                            canSplit = false;
                        }
                    }

                    if (canSplit) {
                        partitionSplit = true;
                        partitionTimestamp = maxSourceTimestamp + 1;
                        prefixType = O3_BLOCK_NONE;

                        // we are splitting existing partition along the "prefix" line
                        // this action creates two partitions:
                        // 1. Prefix of the srcDataPartition
                        // 2. Merge and suffix of srcDataPartition

                        // size of old data partition will be reduced
                        srcDataNewPartitionSize = prefixHi + 1;

                        // size of the new partition, old (0) and new
                        o3SplitPartitionSize = newPartitionSize - srcDataNewPartitionSize;

                        // large prefix copy, better to split the partition
                        LOG.info().$("o3 split partition [table=").$(tableWriter.getTableToken())
                                .$(", timestamp=").$ts(timestampDriver, oldPartitionTimestamp)
                                .$(", nameTxn=").$(srcNameTxn)
                                .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                                .$(", o3SplitPartitionSize=").$(o3SplitPartitionSize)
                                .$(", newPartitionTimestamp=").$ts(timestampDriver, partitionTimestamp)
                                .$(", nameTxn=").$(txn)
                                .I$();
                    }
                }

                boolean canAppendOnly = !partitionSplit;
                if (tableWriter.isCommitReplaceMode()) {
                    canAppendOnly &= (!overlaps && suffixType == O3_BLOCK_O3);
                } else {
                    canAppendOnly &= mergeType == O3_BLOCK_NONE && (prefixType == O3_BLOCK_NONE || prefixType == O3_BLOCK_DATA);
                }
                if (canAppendOnly) {
                    // We do not need to create a copy of partition when we simply need to append
                    // to the existing one.
                    openColumnMode = OPEN_MID_PARTITION_FOR_APPEND;
                } else {
                    TableUtils.setPathForNativePartition(
                            path.trimTo(pathToTable.size()),
                            tableWriter.getMetadata().getTimestampType(),
                            partitionBy,
                            partitionTimestamp,
                            txn
                    );
                    createDirsOrFail(ff, path, tableWriter.getConfiguration().getMkDirMode());
                    if (last) {
                        openColumnMode = OPEN_LAST_PARTITION_FOR_MERGE;
                    } else {
                        openColumnMode = OPEN_MID_PARTITION_FOR_MERGE;
                    }
                }
            } catch (Throwable e) {
                LOG.error().$("process existing partition error [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                O3Utils.close(ff, srcTimestampFd);
                tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
                throw e;
            }

            // Compute max timestamp as maximum of out of order data and
            // data in existing partition.
            // When partition is new, the data timestamp is MIN_LONG

            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr, partitionTimestamp);
            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    newMinPartitionTimestamp,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeDataLo,
                    mergeDataHi,
                    mergeO3Lo,
                    mergeO3Hi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    srcDataMax,
                    srcNameTxn,
                    openColumnMode,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    timestampIndex,
                    timestampDriver,
                    sortedTimestampsAddr,
                    srcDataNewPartitionSize,
                    oldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr
            );
        }
    }

    public static void processPartition(
            O3PartitionTask task,
            long cursor,
            Sequence subSeq
    ) {
        // find "current" partition boundary in the out-of-order data
        // once we know the boundary we can move on to calculating another one
        // srcOooHi is index inclusive of value
        final Path pathToTable = task.getPathToTable();
        final int partitionBy = task.getPartitionBy();
        final ObjList<MemoryMA> columns = task.getColumns();
        final ReadOnlyObjList<? extends MemoryCR> oooColumns = task.getO3Columns();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long oooTimestampMin = task.getOooTimestampMin();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long maxTimestamp = task.getMaxTimestamp();
        final long srcDataMax = task.getSrcDataMax();
        final long srcNameTxn = task.getSrcNameTxn();
        final boolean last = task.isLast();
        final long txn = task.getTxn();
        final long sortedTimestampsAddr = task.getSortedTimestampsAddr();
        final TableWriter tableWriter = task.getTableWriter();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final O3Basket o3Basket = task.getO3Basket();
        final long newPartitionSize = task.getNewPartitionSize();
        final long oldPartitionSize = task.getOldPartitionSize();
        final long partitionUpdateSinkAddr = task.getPartitionUpdateSinkAddr();
        final long dedupColSinkAddr = task.getDedupColSinkAddr();
        final boolean isParquet = task.isParquet();
        final long o3TimestampLo = task.getO3TimestampLo();
        final long o3TimestampHi = task.getO3TimestampHi();

        subSeq.done(cursor);

        processPartition(
                pathToTable,
                partitionBy,
                columns,
                oooColumns,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                partitionTimestamp,
                maxTimestamp,
                srcDataMax,
                srcNameTxn,
                last,
                txn,
                sortedTimestampsAddr,
                tableWriter,
                columnCounter,
                o3Basket,
                newPartitionSize,
                oldPartitionSize,
                partitionUpdateSinkAddr,
                dedupColSinkAddr,
                isParquet,
                o3TimestampLo,
                o3TimestampHi
        );
    }

    private static long calculateMinDataTimestampAfterReplacement(
            long srcDataTimestampAddr,
            long o3TimestampsAddr,
            int prefixType,
            int suffixType,
            long prefixLo,
            long suffixLo,
            long srcOooLo,
            long srcOooHi
    ) {
        if (prefixType == O3_BLOCK_DATA) {
            return Unsafe.getUnsafe().getLong(srcDataTimestampAddr + prefixLo * Long.BYTES);
        }
        if (srcOooLo <= srcOooHi) {
            // If there is O3 data, it will replace the partition data in merge section
            return getTimestampIndexValue(o3TimestampsAddr, srcOooLo);
        }
        if (suffixType == O3_BLOCK_DATA) {
            // No prefix, no merge, just suffix from the partition data
            return Unsafe.getUnsafe().getLong(srcDataTimestampAddr + suffixLo * Long.BYTES);
        }
        return Long.MAX_VALUE;
    }

    private static long createMergeIndex(
            long srcDataTimestampAddr,
            long sortedTimestampsAddr,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            long indexSize
    ) {
        // Create "index" for existing timestamp column. When we reshuffle timestamps during merge we will
        // have to go back and find data rows we need to move accordingly
        long timestampIndexAddr = Unsafe.malloc(indexSize, MemoryTag.NATIVE_O3);
        Vect.mergeTwoLongIndexesAsc(
                srcDataTimestampAddr,
                mergeDataLo,
                mergeDataHi - mergeDataLo + 1,
                sortedTimestampsAddr + mergeOOOLo * 16,
                mergeOOOHi - mergeOOOLo + 1,
                timestampIndexAddr
        );
        return timestampIndexAddr;
    }

    private static long getDedupRows(
            long partitionTimestamp,
            long srcNameTxn,
            long srcTimestampAddr,
            long mergeDataLo,
            long mergeDataHi,
            long sortedTimestampsAddr,
            long mergeOOOLo,
            long mergeOOOHi,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            DedupColumnCommitAddresses dedupCommitAddresses,
            long dedupColSinkAddr,
            TableWriter tableWriter,
            Path tableRootPath,
            long tempIndexAddr
    ) {
        if (dedupCommitAddresses == null || dedupCommitAddresses.getColumnCount() == 0) {
            return Vect.mergeDedupTimestampWithLongIndexAsc(
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    tempIndexAddr
            );
        } else {
            return getDedupRowsWithAdditionalKeys(
                    partitionTimestamp,
                    srcNameTxn,
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    oooColumns,
                    dedupCommitAddresses,
                    dedupColSinkAddr,
                    tableWriter,
                    tableRootPath,
                    tempIndexAddr
            );
        }
    }

    private static long getDedupRowsWithAdditionalKeys(
            long partitionTimestamp,
            long srcNameTxn,
            long srcTimestampAddr,
            long mergeDataLo,
            long mergeDataHi,
            long sortedTimestampsAddr,
            long mergeOOOLo,
            long mergeOOOHi,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            DedupColumnCommitAddresses dedupCommitAddresses,
            long dedupColSinkAddr,
            TableWriter tableWriter,
            Path tableRootPath,
            long tempIndexAddr
    ) {
        LOG.info().$("merge dedup with additional keys [table=").$(tableWriter.getTableToken())
                .$(", columnRowCount=").$(mergeDataHi - mergeDataLo + 1)
                .$(", o3RowCount=").$(mergeOOOHi - mergeOOOLo + 1)
                .I$();
        TableRecordMetadata metadata = tableWriter.getMetadata();
        int dedupColumnIndex = 0;
        int tableRootPathLen = tableRootPath.size();
        FilesFacade ff = tableWriter.getFilesFacade();

        int mapMemTag = MemoryTag.MMAP_O3;
        try {
            dedupCommitAddresses.clear(dedupColSinkAddr);
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                int columnType = metadata.getColumnType(i);
                if (columnType > 0 && metadata.isDedupKey(i) && i != metadata.getTimestampIndex()) {
                    final int columnSize = !ColumnType.isVarSize(columnType) ? ColumnType.sizeOf(columnType) : -1;
                    final long columnTop = tableWriter.getColumnTop(partitionTimestamp, i, mergeDataHi + 1);
                    CharSequence columnName = metadata.getColumnName(i);
                    long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, i);

                    long addr = DedupColumnCommitAddresses.setColValues(
                            dedupColSinkAddr,
                            dedupColumnIndex,
                            columnType,
                            columnSize,
                            columnTop
                    );

                    if (columnTop > mergeDataHi) {
                        // column is all nulls because of column top
                        DedupColumnCommitAddresses.setColAddressValues(addr, DedupColumnCommitAddresses.NULL);

                        if (columnSize > 0) {
                            final long oooColAddress = oooColumns.get(getPrimaryColumnIndex(i)).addressOf(0);
                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooColAddress);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    -1,
                                    -1,
                                    -1
                            );
                        } else {
                            // Var len columns
                            final long oooVarColAddress = oooColumns.get(getPrimaryColumnIndex(i)).addressOf(0);
                            final long oooVarColSize = oooColumns.get(getPrimaryColumnIndex(i)).addressHi() - oooVarColAddress;
                            final long oooAuxColAddress = oooColumns.get(getSecondaryColumnIndex(i)).addressOf(0);

                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooAuxColAddress, oooVarColAddress, oooVarColSize);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    -1,
                                    -1,
                                    -1
                            );
                            DedupColumnCommitAddresses.setReservedValuesSet2(
                                    addr,
                                    0,
                                    -1
                            );
                        }
                    } else { // if (columnTop > mergeDataHi)
                        if (columnSize > 0) {
                            // Fixed length column
                            TableUtils.setSinkForNativePartition(
                                    tableRootPath.trimTo(tableRootPathLen).slash(),
                                    tableWriter.getMetadata().getTimestampType(),
                                    tableWriter.getPartitionBy(),
                                    partitionTimestamp,
                                    srcNameTxn
                            );
                            long fd = TableUtils.openRO(ff, TableUtils.dFile(tableRootPath, columnName, columnNameTxn), LOG);

                            long fixMapSize = (mergeDataHi + 1 - columnTop) * columnSize;
                            long fixMappedAddress = TableUtils.mapAppendColumnBuffer(
                                    ff,
                                    fd,
                                    0,
                                    fixMapSize,
                                    false,
                                    mapMemTag
                            );

                            DedupColumnCommitAddresses.setColAddressValues(addr, Math.abs(fixMappedAddress) - columnTop * columnSize);

                            final long oooColAddress = oooColumns.get(getPrimaryColumnIndex(i)).addressOf(0);
                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooColAddress);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    fixMappedAddress,
                                    fixMapSize,
                                    fd
                            );
                        } else {
                            // Variable length column
                            long rows = mergeDataHi + 1 - columnTop;
                            ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                            long auxMapSize = driver.getAuxVectorSize(rows);

                            TableUtils.setSinkForNativePartition(
                                    tableRootPath.trimTo(tableRootPathLen).slash(),
                                    tableWriter.getMetadata().getTimestampType(),
                                    tableWriter.getPartitionBy(),
                                    partitionTimestamp,
                                    srcNameTxn
                            );
                            long auxFd = TableUtils.openRO(ff, TableUtils.iFile(tableRootPath, columnName, columnNameTxn), LOG);
                            long auxMappedAddress = TableUtils.mapAppendColumnBuffer(
                                    ff,
                                    auxFd,
                                    0,
                                    auxMapSize,
                                    false,
                                    mapMemTag
                            );

                            long varMapSize = driver.getDataVectorSizeAt(auxMappedAddress, rows - 1);
                            if (varMapSize > 0) {
                                TableUtils.setSinkForNativePartition(
                                        tableRootPath.trimTo(tableRootPathLen).slash(),
                                        tableWriter.getMetadata().getTimestampType(),
                                        tableWriter.getPartitionBy(),
                                        partitionTimestamp,
                                        srcNameTxn
                                );
                            }
                            long varFd = varMapSize > 0 ? TableUtils.openRO(ff, TableUtils.dFile(tableRootPath, columnName, columnNameTxn), LOG) : -1;
                            long varMappedAddress = varMapSize > 0 ? TableUtils.mapAppendColumnBuffer(
                                    ff,
                                    varFd,
                                    0,
                                    varMapSize,
                                    false,
                                    mapMemTag
                            ) : 0;

                            long auxRecSize = driver.auxRowsToBytes(1);
                            DedupColumnCommitAddresses.setColAddressValues(addr, auxMappedAddress - columnTop * auxRecSize, varMappedAddress, varMapSize);

                            MemoryCR oooVarCol = oooColumns.get(getPrimaryColumnIndex(i));
                            final long oooVarColAddress = oooVarCol.addressOf(0);
                            final long oooVarColSize = oooVarCol.addressHi() - oooVarColAddress;
                            final long oooAuxColAddress = oooColumns.get(getSecondaryColumnIndex(i)).addressOf(0);

                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooAuxColAddress, oooVarColAddress, oooVarColSize);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    auxMappedAddress,
                                    auxMapSize,
                                    auxFd
                            );
                            DedupColumnCommitAddresses.setReservedValuesSet2(
                                    addr,
                                    varMappedAddress,
                                    varFd
                            );
                        }
                    }
                    dedupColumnIndex++;
                } // if (columnType > 0 && metadata.isDedupKey(i) && i != metadata.getTimestampIndex())
            }

            return Vect.mergeDedupTimestampWithLongIndexIntKeys(
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    tempIndexAddr,
                    dedupCommitAddresses.getColumnCount(),
                    DedupColumnCommitAddresses.getAddress(dedupColSinkAddr)
            );
        } finally {
            for (int i = 0, n = dedupCommitAddresses.getColumnCount(); i < n; i++) {
                final long mappedAddress = DedupColumnCommitAddresses.getColReserved1(dedupColSinkAddr, i);
                final long mappedAddressSize = DedupColumnCommitAddresses.getColReserved2(dedupColSinkAddr, i);
                if (mappedAddressSize > 0) {
                    TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, 0, mappedAddressSize, mapMemTag);
                    final long fd = DedupColumnCommitAddresses.getColReserved3(dedupColSinkAddr, i);
                    ff.close(fd);
                }

                final long varMappedAddress = DedupColumnCommitAddresses.getColReserved4(dedupColSinkAddr, i);
                if (varMappedAddress > 0) {
                    final long varMappedLength = DedupColumnCommitAddresses.getColVarDataLen(dedupColSinkAddr, i);
                    TableUtils.mapAppendColumnBufferRelease(ff, varMappedAddress, 0, varMappedLength, mapMemTag);
                    final long varFd = DedupColumnCommitAddresses.getColReserved5(dedupColSinkAddr, i);
                    ff.close(varFd);
                }
            }
        }
    }

    // returns number of duplicate rows
    private static long mergeRowGroup(
            PartitionDescriptor partitionDescriptor,
            PartitionUpdater partitionUpdater,
            DirectIntList parquetColumns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            PartitionDecoder decoder,
            RowGroupBuffers rowGroupBuffers,
            int rowGroupIndex,
            int timestampIndex,
            long partitionTimestamp,
            long mergeRangeLo,
            long mergeRangeHi,
            TableRecordMetadata tableWriterMetadata,
            long srcOooBatchRowSize,
            long dedupColSinkAddr
    ) {
        // decode column chunks for the row group in advance
        parquetColumns.clear();
        int timestampColumnChunkIndex = -1;
        // TODO(eugene): Verify Parquet and TableWriter metadata consistency.
        // Currently assuming metadata is in sync and consistent, as there were no column operations.
        // After table DDL implementation, this may no longer hold true, so index remapping is required.
        final int columnCount = tableWriterMetadata.getColumnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = tableWriterMetadata.getColumnType(columnIndex);
            if (columnType < 0) {
                continue;
            }
            if (columnIndex == timestampIndex) {
                timestampColumnChunkIndex = (int) parquetColumns.size() / 2;
            }
            parquetColumns.add(columnIndex);
            parquetColumns.add(columnType);
        }

        final int rowGroupSize = decoder.metadata().getRowGroupSize(rowGroupIndex);
        decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroupIndex, 0, rowGroupSize);

        assert timestampColumnChunkIndex > -1;
        final long timestampDataPtr = rowGroupBuffers.getChunkDataPtr(timestampColumnChunkIndex);
        assert timestampDataPtr != 0;
        long mergeBatchRowCount = mergeRangeHi - mergeRangeLo + 1;
        long mergeRowCount = mergeBatchRowCount + rowGroupSize;
        long duplicateCount = 0;

        long timestampMergeIndexSize = mergeRowCount * TIMESTAMP_MERGE_ENTRY_BYTES;
        long timestampMergeIndexAddr;
        if (!tableWriter.isCommitDedupMode()) {
            timestampMergeIndexAddr = createMergeIndex(
                    timestampDataPtr,
                    sortedTimestampsAddr,
                    0,
                    rowGroupSize - 1,
                    mergeRangeLo,
                    mergeRangeHi,
                    timestampMergeIndexSize
            );
        } else {
            final DedupColumnCommitAddresses dedupCommitAddresses = tableWriter.getDedupCommitAddresses();
            final long dedupRows;
            timestampMergeIndexAddr = Unsafe.malloc(timestampMergeIndexSize, MemoryTag.NATIVE_O3);
            try {
                if (dedupCommitAddresses == null || dedupCommitAddresses.getColumnCount() == 0) {
                    dedupRows = Vect.mergeDedupTimestampWithLongIndexAsc(
                            timestampDataPtr,
                            0,
                            rowGroupSize - 1,
                            sortedTimestampsAddr,
                            mergeRangeLo,
                            mergeRangeHi,
                            timestampMergeIndexAddr
                    );
                } else {
                    int dedupColumnIndex = 0;
                    dedupCommitAddresses.clear(dedupColSinkAddr);
                    for (int bufferIndex = 0, n = (int) parquetColumns.size() / 2; bufferIndex < n; bufferIndex++) {
                        int columnIndex = parquetColumns.get(bufferIndex * 2L);
                        int columnType = tableWriterMetadata.getColumnType(columnIndex);
                        assert columnIndex >= 0;
                        assert columnType >= 0;
                        if (tableWriterMetadata.isDedupKey(columnIndex) && columnIndex != timestampIndex) {
                            final int columnSize = !ColumnType.isVarSize(columnType) ? ColumnType.sizeOf(columnType) : -1;
                            final long columnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, rowGroupSize);
                            long addr = DedupColumnCommitAddresses.setColValues(
                                    dedupColSinkAddr,
                                    dedupColumnIndex++,
                                    columnType,
                                    columnSize,
                                    columnTop
                            );
                            if (columnSize > 0) {
                                DedupColumnCommitAddresses.setColAddressValues(addr, rowGroupBuffers.getChunkDataPtr(bufferIndex));
                                final long oooColAddress = oooColumns.get(getPrimaryColumnIndex(columnIndex)).addressOf(0);
                                DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooColAddress);
                            } else {
                                DedupColumnCommitAddresses.setColAddressValues(
                                        addr,
                                        rowGroupBuffers.getChunkAuxPtr(bufferIndex),
                                        rowGroupBuffers.getChunkDataPtr(bufferIndex),
                                        rowGroupBuffers.getChunkDataSize(bufferIndex)
                                );
                                MemoryCR oooVarCol = oooColumns.get(getPrimaryColumnIndex(columnIndex));
                                final long oooVarColAddress = oooVarCol.addressOf(0);
                                final long oooVarColSize = oooVarCol.addressHi() - oooVarColAddress;
                                final long oooAuxColAddress = oooColumns.get(getSecondaryColumnIndex(columnIndex)).addressOf(0);
                                DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooAuxColAddress, oooVarColAddress, oooVarColSize);
                            }
                        }
                    }

                    dedupRows = Vect.mergeDedupTimestampWithLongIndexIntKeys(
                            timestampDataPtr,
                            0,
                            rowGroupSize - 1,
                            sortedTimestampsAddr,
                            mergeRangeLo,
                            mergeRangeHi,
                            timestampMergeIndexAddr,
                            dedupCommitAddresses.getColumnCount(),
                            DedupColumnCommitAddresses.getAddress(dedupColSinkAddr)
                    );
                }

                timestampMergeIndexAddr = Unsafe.realloc(
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        dedupRows * TIMESTAMP_MERGE_ENTRY_BYTES,
                        MemoryTag.NATIVE_O3
                );
            } catch (Throwable e) {
                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                throw e;
            }

            timestampMergeIndexSize = dedupRows * TIMESTAMP_MERGE_ENTRY_BYTES;
            duplicateCount = mergeRowCount - dedupRows;
            mergeRowCount = dedupRows;
        }

        assert timestampMergeIndexAddr != 0;

        try {
            partitionDescriptor.of(tableWriter.getTableToken().getTableName(), mergeRowCount, timestampIndex);

            for (int bufferIndex = 0, n = (int) parquetColumns.size() / 2; bufferIndex < n; bufferIndex++) {
                int columnIndex = parquetColumns.get(bufferIndex * 2L);
                int columnType = tableWriterMetadata.getColumnType(columnIndex);
                assert columnIndex >= 0;
                assert columnType >= 0;
                final String columnName = tableWriterMetadata.getColumnName(columnIndex);
                final int columnId = tableWriterMetadata.getColumnMetadata(columnIndex).getWriterIndex();

                final boolean notTheTimestamp = columnIndex != timestampIndex;
                final int columnOffset = getPrimaryColumnIndex(columnIndex);
                final MemoryCR oooMem1 = oooColumns.getQuick(columnOffset);
                final MemoryCR oooMem2 = oooColumns.getQuick(columnOffset + 1);


                if (ColumnType.isVarSize(columnType)) {
                    final ColumnTypeDriver ctd = ColumnType.getDriver(columnType);

                    final long columnDataPtr = rowGroupBuffers.getChunkDataPtr(bufferIndex);
                    assert columnDataPtr != 0;
                    final long columnAuxPtr = rowGroupBuffers.getChunkAuxPtr(bufferIndex);
                    assert columnAuxPtr != 0;

                    final long srcOooFixAddr = oooMem2.addressOf(0);
                    final long srcOooVarAddr = oooMem1.addressOf(0);

                    long dstFixSize = ctd.auxRowsToBytes(srcOooBatchRowSize) + ctd.getAuxVectorSize(rowGroupSize);

                    long dstVarSize = ctd.getDataVectorSize(srcOooFixAddr, mergeRangeLo, mergeRangeHi)
                            + ctd.getDataVectorSizeAt(columnAuxPtr, rowGroupSize - 1);

                    final long dstFixMemAddr = Unsafe.malloc(dstFixSize, MemoryTag.NATIVE_O3);
                    final long dstVarMemAddr = Unsafe.malloc(dstVarSize, MemoryTag.NATIVE_O3);

                    O3CopyJob.mergeCopy(
                            columnType,
                            timestampMergeIndexAddr,
                            mergeRowCount,
                            columnAuxPtr,
                            columnDataPtr,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            dstFixMemAddr,
                            dstVarMemAddr,
                            0
                    );

                    partitionDescriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            0,
                            dstVarMemAddr,
                            dstVarSize,
                            dstFixMemAddr,
                            dstFixSize,
                            0,
                            0
                    );
                } else {
                    final long srcOooFixAddr = oooMem1.addressOf(0);
                    long dstFixSize = mergeRowCount * ColumnType.sizeOf(columnType);
                    final long dstFixMemAddr = Unsafe.malloc(dstFixSize, MemoryTag.NATIVE_O3);

                    // TODO(eugene): can be null in case of column top
                    final long columnDataPtr = rowGroupBuffers.getChunkDataPtr(bufferIndex);
                    assert columnDataPtr != 0;
                    // Merge column data
                    O3CopyJob.mergeCopy(
                            notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                            timestampMergeIndexAddr,
                            mergeRowCount,
                            columnDataPtr,
                            0,
                            srcOooFixAddr,
                            0,
                            dstFixMemAddr,
                            0,
                            0
                    );

                    if (ColumnType.isSymbol(columnType)) {
                        final MapWriter symbolMapWriter = tableWriter.getSymbolMapWriter(columnIndex);
                        final MemoryR offsetsMem = symbolMapWriter.getSymbolOffsetsMemory();
                        final MemoryR valuesMem = symbolMapWriter.getSymbolValuesMemory();

                        final int symbolCount = symbolMapWriter.getSymbolCount();
                        final long offset = SymbolMapWriter.keyToOffset(symbolCount);
                        final long offsetsMemSize = offset - SymbolMapWriter.HEADER_SIZE;
                        assert offsetsMemSize <= offsetsMem.size();
                        final long valuesMemSize = offsetsMem.getLong(offset);
                        assert valuesMemSize <= valuesMem.size();

                        partitionDescriptor.addColumn(
                                columnName,
                                columnType,
                                columnId,
                                0,
                                dstFixMemAddr,
                                dstFixSize,
                                valuesMem.addressOf(0),
                                valuesMemSize,
                                // Skip header
                                offsetsMem.addressOf(SymbolMapWriter.HEADER_SIZE),
                                offsetsMemSize
                        );
                    } else {
                        partitionDescriptor.addColumn(
                                columnName,
                                columnType,
                                columnId,
                                0,
                                dstFixMemAddr,
                                dstFixSize,
                                0,
                                0,
                                0,
                                0
                        );
                    }
                }
            }
            partitionUpdater.updateRowGroup((short) rowGroupIndex, partitionDescriptor);
        } finally {
            Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
        }

        return duplicateCount;
    }

    private static void publishOpenColumnTaskContended(
            long cursor,
            int openColumnMode,
            Path pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
            long txn,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int indexBlockCapacity,
            long activeFixFd,
            long activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
    ) {
        while (cursor == -2) {
            Os.pause();
            cursor = tableWriter.getO3OpenColumnPubSeq().next();
        }

        if (cursor > -1) {
            publishOpenColumnTaskHarmonized(
                    cursor,
                    openColumnMode,
                    pathToTable,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    srcNameTxn,
                    txn,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeDataLo,
                    mergeDataHi,
                    mergeOOOLo,
                    mergeOOOHi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    activeFixFd,
                    activeVarFd,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr,
                    columnIndex,
                    columnNameTxn
            );
        } else {
            O3OpenColumnJob.openColumn(
                    openColumnMode,
                    pathToTable,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    srcNameTxn,
                    txn,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeOOOLo,
                    mergeOOOHi,
                    mergeDataLo,
                    mergeDataHi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    indexBlockCapacity,
                    activeFixFd,
                    activeVarFd,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr,
                    columnIndex,
                    columnNameTxn
            );
        }
    }

    private static void publishOpenColumnTaskHarmonized(
            long cursor,
            int openColumnMode,
            Path pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
            long txn,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long activeFixFd,
            long activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
    ) {
        final O3OpenColumnTask openColumnTask = tableWriter.getO3OpenColumnQueue().get(cursor);
        openColumnTask.of(
                openColumnMode,
                pathToTable,
                columnName,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                partitionTimestamp,
                oldPartitionTimestamp,
                srcDataTop,
                srcDataMax,
                srcNameTxn,
                txn,
                prefixType,
                prefixLo,
                prefixHi,
                mergeType,
                mergeDataLo,
                mergeDataHi,
                mergeOOOLo,
                mergeOOOHi,
                suffixType,
                suffixLo,
                suffixHi,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                indexBlockCapacity,
                activeFixFd,
                activeVarFd,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr,
                columnIndex,
                columnNameTxn
        );
        tableWriter.getO3OpenColumnPubSeq().done(cursor);
    }

    private static void publishOpenColumnTasks(
            long txn,
            ObjList<MemoryMA> columns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            Path pathToTable,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long srcDataMax,
            long srcNameTxn,
            int openColumnMode,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int timestampIndex,
            TimestampDriver timestampDriver,
            long sortedTimestampsAddr,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr
    ) {
        // Number of rows to insert from the O3 segment into this partition.
        final long srcOooBatchRowSize = srcOooHi - srcOooLo + 1;

        long timestampMergeIndexAddr = 0;
        long timestampMergeIndexSize = 0;
        final TableRecordMetadata metadata = tableWriter.getMetadata();
        if (mergeType == O3_BLOCK_MERGE) {
            long mergeRowCount = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
            long tempIndexSize = mergeRowCount * TIMESTAMP_MERGE_ENTRY_BYTES;
            assert tempIndexSize > 0; // avoid SIGSEGV

            try {
                if (tableWriter.isCommitPlainInsert()) {
                    timestampMergeIndexSize = tempIndexSize;

                    timestampMergeIndexAddr = createMergeIndex(
                            srcTimestampAddr,
                            sortedTimestampsAddr,
                            mergeDataLo,
                            mergeDataHi,
                            mergeOOOLo,
                            mergeOOOHi,
                            timestampMergeIndexSize
                    );
                } else if (tableWriter.isCommitDedupMode()) {
                    final long tempIndexAddr = Unsafe.malloc(tempIndexSize, MemoryTag.NATIVE_O3);
                    final DedupColumnCommitAddresses dedupCommitAddresses = tableWriter.getDedupCommitAddresses();
                    final Path tempTablePath = Path.getThreadLocal(tableWriter.getConfiguration().getDbRoot()).concat(tableWriter.getTableToken());

                    final long dedupRows = getDedupRows(
                            oldPartitionTimestamp,
                            srcNameTxn,
                            srcTimestampAddr,
                            mergeDataLo,
                            mergeDataHi,
                            sortedTimestampsAddr,
                            mergeOOOLo,
                            mergeOOOHi,
                            oooColumns,
                            dedupCommitAddresses,
                            dedupColSinkAddr,
                            tableWriter,
                            tempTablePath,
                            tempIndexAddr
                    );
                    timestampMergeIndexSize = dedupRows * TIMESTAMP_MERGE_ENTRY_BYTES;
                    timestampMergeIndexAddr = Unsafe.realloc(tempIndexAddr, tempIndexSize, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                    final long duplicateCount = mergeRowCount - dedupRows;
                    boolean appendOnly = false;
                    if (duplicateCount > 0) {
                        if (duplicateCount == mergeOOOHi - mergeOOOLo + 1 && prefixType != O3_BLOCK_O3) {

                            // All the rows are duplicates, the commit does not add any new lines.
                            // Check non-key columns if they are exactly the same as the rows they replace
                            if (tableWriter.checkDedupCommitIdenticalToPartition(
                                    oldPartitionTimestamp,
                                    srcNameTxn,
                                    srcDataOldPartitionSize,
                                    mergeDataLo,
                                    mergeDataHi,
                                    mergeOOOLo,
                                    mergeOOOHi,
                                    timestampMergeIndexAddr,
                                    dedupRows
                            )) {

                                if (suffixType != O3_BLOCK_O3) {
                                    LOG.info().$("deduplication resulted in noop [table=").$(tableWriter.getTableToken())
                                            .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                                            .I$();

                                    timestampMergeIndexAddr = Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);

                                    // Remove empty partition dir
                                    Path path = Path.getThreadLocal(pathToTable);
                                    setPathForNativePartition(path, tableWriter.getTimestampType(), tableWriter.getPartitionBy(), partitionTimestamp, txn);
                                    tableWriter.getConfiguration().getFilesFacade().rmdir(path);

                                    // nothing to do, skip the partition
                                    updatePartition(
                                            tableWriter.getFilesFacade(),
                                            srcTimestampAddr,
                                            srcTimestampSize,
                                            srcTimestampFd,
                                            tableWriter,
                                            partitionUpdateSinkAddr,
                                            oldPartitionTimestamp,
                                            Long.MAX_VALUE,
                                            -1,
                                            srcDataOldPartitionSize,
                                            0
                                    );

                                    return;
                                } else {
                                    // suffixType == O3_BLOCK_O3
                                    // we don't need to do the merge, but we need to append the suffix
                                    appendOnly = true;
                                }


                            }
                        }
                    } else if (suffixType != O3_BLOCK_DATA) {
                        // No duplicates.
                        // Maybe it's append only, if the OOO data "touches" the partition data then we
                        // do not need to merge, append is good enough
                        long dataMergeMaxTimestamp = Unsafe.getUnsafe().getLong(srcTimestampAddr + mergeDataHi * Long.BYTES);
                        appendOnly = oooTimestampMin >= dataMergeMaxTimestamp;
                    }

                    if (appendOnly) {
                        mergeType = O3_BLOCK_NONE;
                        mergeDataHi = -1;
                        mergeDataLo = 0;
                        mergeOOOHi = -1;
                        mergeOOOLo = 0;

                        if (duplicateCount > 0) {
                            // Append procs may ignore suffixLo when appending using srcOooLo instead.
                            // Adjust suffixLo to match the srcOooLo.
                            srcOooLo = suffixLo;
                        } else {
                            suffixType = O3_BLOCK_O3;
                            suffixLo = srcOooLo;
                            suffixHi = srcOooHi;
                        }

                        if (o3SplitPartitionSize > 0) {
                            LOG.info().$("dedup resulted in no merge, undo partition split [table=")
                                    .$(tableWriter.getTableToken())
                                    .$(", partition=").$ts(timestampDriver, oldPartitionTimestamp)
                                    .$(", split=").$ts(timestampDriver, partitionTimestamp)
                                    .I$();
                            partitionTimestamp = oldPartitionTimestamp;
                            srcDataNewPartitionSize += o3SplitPartitionSize;
                            o3SplitPartitionSize = 0;
                        }

                        // No merge anymore, free the merge index
                        timestampMergeIndexAddr = Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);

                        prefixType = O3_BLOCK_DATA;
                        prefixLo = 0;
                        prefixHi = srcDataMax - 1;

                        if (openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE) {
                            openColumnMode = OPEN_LAST_PARTITION_FOR_APPEND;
                        } else if (openColumnMode == OPEN_MID_PARTITION_FOR_MERGE) {
                            openColumnMode = OPEN_MID_PARTITION_FOR_APPEND;
                        } else {
                            assert false : "unexpected open column mode: " + openColumnMode;
                        }
                    }

                    // we could be de-duping a split partition
                    // in which case only its size will be affected
                    if (o3SplitPartitionSize > 0) {
                        o3SplitPartitionSize -= duplicateCount;
                    } else {
                        srcDataNewPartitionSize -= duplicateCount;
                    }
                    LOG.info()
                            .$("dedup row reduction [table=").$(tableWriter.getTableToken())
                            .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                            .$(", duplicateCount=").$(duplicateCount)
                            .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                            .$(", srcDataOldPartitionSize=").$(srcDataOldPartitionSize)
                            .$(", o3SplitPartitionSize=").$(srcDataOldPartitionSize)
                            .$(", mergeDataLo=").$(mergeDataLo)
                            .$(", mergeDataHi=").$(mergeDataHi)
                            .$(", mergeOOOLo=").$(mergeOOOLo)
                            .$(", mergeOOOHi=").$(mergeOOOHi)
                            .I$();
                } else if (tableWriter.isCommitReplaceMode()) {
                    // merge range is replaced by new data.
                    // Merge row count is the count of the new rows, compensated by 1 row that is start of the range
                    // and 1 row that is in the end of the range.
                    mergeType = O3_BLOCK_O3;
                } else {
                    throw new IllegalStateException("commit mode not supported");
                }
            } catch (Throwable e) {
                tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                LOG.critical().$("open column error [table=").$(tableWriter.getTableToken())
                        .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                        .$(", e=").$(e)
                        .I$();

                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                O3CopyJob.closeColumnIdleQuick(
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter
                );
                throw e;
            }
        }

        tableWriter.addPhysicallyWrittenRows(
                isOpenColumnModeForAppend(openColumnMode)
                        ? srcOooBatchRowSize
                        : o3SplitPartitionSize == 0 ? srcDataNewPartitionSize : o3SplitPartitionSize
        );

        final int columnCount = metadata.getColumnCount();
        columnCounter.set(compressColumnCount(metadata));
        int columnsInFlight = columnCount;
        if (openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE || openColumnMode == OPEN_MID_PARTITION_FOR_MERGE) {
            // Partition will be re-written. Jobs will set new column top values but by default they are 0
            Vect.memset(partitionUpdateSinkAddr + PARTITION_SINK_COL_TOP_OFFSET, (long) Long.BYTES * columnCount, 0);
        }

        try {
            for (int i = 0; i < columnCount; i++) {
                final int columnType = metadata.getColumnType(i);
                if (columnType < 0) {
                    continue;
                }
                final int colOffset = getPrimaryColumnIndex(i);
                final boolean notTheTimestamp = i != timestampIndex;
                final MemoryCR oooMem1 = oooColumns.getQuick(colOffset);
                final MemoryCR oooMem2 = oooColumns.getQuick(colOffset + 1);
                final MemoryMA mem1 = columns.getQuick(colOffset);
                final MemoryMA mem2 = columns.getQuick(colOffset + 1);
                final long activeFixFd;
                final long activeVarFd;
                final long srcDataTop;
                final long srcOooFixAddr;
                final long srcOooVarAddr;
                if (!ColumnType.isVarSize(columnType)) {
                    activeFixFd = mem1.getFd();
                    activeVarFd = 0;
                    srcOooFixAddr = (i == timestampIndex) ? sortedTimestampsAddr : oooMem1.addressOf(0);
                    srcOooVarAddr = 0;
                } else {
                    activeFixFd = mem2.getFd();
                    activeVarFd = mem1.getFd();
                    srcOooFixAddr = oooMem2.addressOf(0);
                    srcOooVarAddr = oooMem1.addressOf(0);
                }

                final CharSequence columnName = metadata.getColumnName(i);
                final boolean isIndexed = metadata.isColumnIndexed(i);
                final int indexBlockCapacity = isIndexed ? metadata.getIndexValueBlockCapacity(i) : -1;
                if (openColumnMode == OPEN_LAST_PARTITION_FOR_APPEND || openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE) {
                    srcDataTop = tableWriter.getColumnTop(i);
                } else {
                    srcDataTop = -1; // column open job will have to find out if top exists and its value
                }

                final BitmapIndexWriter indexWriter;
                if (isIndexed) {
                    indexWriter = o3Basket.nextIndexer();
                } else {
                    indexWriter = null;
                }

                try {
                    final long cursor = tableWriter.getO3OpenColumnPubSeq().next();
                    final long columnNameTxn = tableWriter.getColumnNameTxn(oldPartitionTimestamp, i);
                    if (cursor > -1) {
                        publishOpenColumnTaskHarmonized(
                                cursor,
                                openColumnMode,
                                pathToTable,
                                columnName,
                                columnCounter,
                                o3Basket.nextPartCounter(),
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                timestampMergeIndexAddr,
                                timestampMergeIndexSize,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                partitionTimestamp,
                                oldPartitionTimestamp,
                                srcDataTop,
                                srcDataMax,
                                srcNameTxn,
                                txn,
                                prefixType,
                                prefixLo,
                                prefixHi,
                                mergeType,
                                mergeDataLo,
                                mergeDataHi,
                                mergeOOOLo,
                                mergeOOOHi,
                                suffixType,
                                suffixLo,
                                suffixHi,
                                indexBlockCapacity,
                                srcTimestampFd,
                                srcTimestampAddr,
                                srcTimestampSize,
                                activeFixFd,
                                activeVarFd,
                                srcDataNewPartitionSize,
                                srcDataOldPartitionSize,
                                o3SplitPartitionSize,
                                tableWriter,
                                indexWriter,
                                partitionUpdateSinkAddr,
                                i,
                                columnNameTxn
                        );
                    } else {
                        publishOpenColumnTaskContended(
                                cursor,
                                openColumnMode,
                                pathToTable,
                                columnName,
                                columnCounter,
                                o3Basket.nextPartCounter(),
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                timestampMergeIndexAddr,
                                timestampMergeIndexSize,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                partitionTimestamp,
                                oldPartitionTimestamp,
                                srcDataTop,
                                srcDataMax,
                                srcNameTxn,
                                txn,
                                prefixType,
                                prefixLo,
                                prefixHi,
                                mergeType,
                                mergeDataLo,
                                mergeDataHi,
                                mergeOOOLo,
                                mergeOOOHi,
                                suffixType,
                                suffixLo,
                                suffixHi,
                                srcTimestampFd,
                                srcTimestampAddr,
                                srcTimestampSize,
                                indexBlockCapacity,
                                activeFixFd,
                                activeVarFd,
                                srcDataNewPartitionSize,
                                srcDataOldPartitionSize,
                                o3SplitPartitionSize,
                                tableWriter,
                                indexWriter,
                                partitionUpdateSinkAddr,
                                i,
                                columnNameTxn
                        );
                    }
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                    LOG.critical().$("open column error [table=").$(tableWriter.getTableToken())
                            .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                            .$(", e=").$(e)
                            .I$();
                    columnsInFlight = i + 1;
                    throw e;
                }
            }
        } finally {
            final int delta = columnsInFlight - columnCount;
            LOG.debug().$("idle [delta=").$(delta).I$();
            if (delta < 0 && columnCounter.addAndGet(delta) == 0) {
                O3CopyJob.closeColumnIdleQuick(
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter
                );
            }
        }
    }

    private static void updateParquetIndexes(
            int partitionBy,
            long partitionTimestamp,
            TableWriter tableWriter,
            long srcNameTxn,
            O3Basket o3Basket,
            long newPartitionSize,
            long newParquetSize,
            Path pathToTable,
            Path path,
            FilesFacade ff,
            PartitionDecoder partitionDecoder,
            TableRecordMetadata tableWriterMetadata,
            DirectIntList parquetColumns,
            RowGroupBuffers rowGroupBuffers
    ) {
        long parquetAddr = 0;
        try {
            parquetAddr = TableUtils.mapRO(ff, path.$(), LOG, newParquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            partitionDecoder.of(
                    parquetAddr,
                    newParquetSize,
                    MemoryTag.NATIVE_PARQUET_PARTITION_DECODER
            );
            path.of(pathToTable);
            setPathForNativePartition(
                    path,
                    tableWriterMetadata.getTimestampType(),
                    partitionBy,
                    partitionTimestamp,
                    srcNameTxn
            );
            final int pLen = path.size();

            BitmapIndexWriter indexWriter = null;
            final int columnCount = tableWriterMetadata.getColumnCount();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                if (tableWriterMetadata.getColumnType(columnIndex) == ColumnType.SYMBOL && tableWriterMetadata.isColumnIndexed(columnIndex)) {
                    final int indexBlockCapacity = tableWriterMetadata.getIndexValueBlockCapacity(columnIndex);
                    if (indexBlockCapacity < 0) {
                        continue;
                    }

                    final CharSequence columnName = tableWriterMetadata.getColumnName(columnIndex);
                    final long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);

                    long kFd = 0;
                    long vFd = 0;
                    try {
                        kFd = openRW(
                                ff,
                                BitmapIndexUtils.keyFileName(path.trimTo(pLen), columnName, columnNameTxn),
                                LOG,
                                tableWriter.getConfiguration().getWriterFileOpenOpts()
                        );
                        vFd = openRW(
                                ff,
                                BitmapIndexUtils.valueFileName(path.trimTo(pLen), columnName, columnNameTxn),
                                LOG,
                                tableWriter.getConfiguration().getWriterFileOpenOpts()
                        );
                    } catch (Throwable th) {
                        O3Utils.close(ff, kFd);
                        O3Utils.close(ff, vFd);
                        throw th;
                    }

                    if (indexWriter == null) {
                        indexWriter = o3Basket.nextIndexer();
                    }

                    try {
                        final PartitionDecoder.Metadata parquetMetadata = partitionDecoder.metadata();

                        int parquetColumnIndex = -1;
                        for (int idx = 0, cnt = parquetMetadata.getColumnCount(); idx < cnt; idx++) {
                            if (parquetMetadata.getColumnId(idx) == columnIndex) {
                                parquetColumnIndex = idx;
                                break;
                            }
                        }
                        if (parquetColumnIndex == -1) {
                            path.trimTo(pLen);
                            LOG.error().$("could not find symbol column for indexing in parquet, skipping [path=").$(path)
                                    .$(", columnIndex=").$(columnIndex)
                                    .I$();
                            continue;
                        }

                        indexWriter.of(tableWriter.getConfiguration(), kFd, vFd, true, indexBlockCapacity);

                        final long columnTop = tableWriter.columnVersionReader().getColumnTop(partitionTimestamp, columnIndex);
                        if (columnTop > -1 && newPartitionSize > columnTop) {
                            parquetColumns.clear();
                            parquetColumns.add(parquetColumnIndex);
                            parquetColumns.add(ColumnType.SYMBOL);

                            long rowCount = 0;
                            final int rowGroupCount = parquetMetadata.getRowGroupCount();
                            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                                final int rowGroupSize = parquetMetadata.getRowGroupSize(rowGroupIndex);
                                if (rowCount + rowGroupSize <= columnTop) {
                                    rowCount += rowGroupSize;
                                    continue;
                                }

                                partitionDecoder.decodeRowGroup(
                                        rowGroupBuffers,
                                        parquetColumns,
                                        rowGroupIndex,
                                        (int) Math.max(0, columnTop - rowCount),
                                        rowGroupSize
                                );

                                long rowId = Math.max(rowCount, columnTop);
                                final long addr = rowGroupBuffers.getChunkDataPtr(0);
                                final long size = rowGroupBuffers.getChunkDataSize(0);
                                for (long p = addr, lim = addr + size; p < lim; p += 4, rowId++) {
                                    indexWriter.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(p)), rowId);
                                }

                                rowCount += rowGroupSize;
                            }
                            indexWriter.setMaxValue(newPartitionSize - 1);
                        }

                        indexWriter.commit();
                    } finally {
                        Misc.free(indexWriter);
                    }
                }
            }
        } finally {
            if (parquetAddr != 0) {
                ff.munmap(parquetAddr, newParquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
        }
    }

    private static void updatePartition(
            FilesFacade ff,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcTimestampFd,
            TableWriter tableWriter,
            long partitionUpdateSinkAddr,
            long partitionTimestamp,
            long timestampMin,
            long newPartitionSize,
            long oldPartitionSize,
            int partitionMutates
    ) {
        updatePartitionSink(partitionUpdateSinkAddr, partitionTimestamp, timestampMin, newPartitionSize, oldPartitionSize, partitionMutates);

        O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
        O3Utils.close(ff, srcTimestampFd);

        tableWriter.o3ClockDownPartitionUpdateCount();
        tableWriter.o3CountDownDoneLatch();
    }

    private static void updatePartitionSink(long partitionUpdateSinkAddr, long partitionTimestamp, long o3TimestampMin, long newPartitionSize, long oldPartitionSize, long partitionMutates) {
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr, partitionTimestamp);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + Long.BYTES, o3TimestampMin);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, newPartitionSize); // new partition size
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, oldPartitionSize);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, partitionMutates); // partitionMutates
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 5 * Long.BYTES, 0); // o3SplitPartitionSize
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 7 * Long.BYTES, -1); // update parquet partition file size
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        processPartition(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
