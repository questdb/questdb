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

package io.questdb.cairo.mig;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Generates {@code _pm} metadata files for existing parquet partitions and
 * stores the pm file size in field 3 of each partition entry in {@code _txn}.
 * <p>
 * Field 3 was previously "parquet file size" and is now "parquet metadata file size."
 * For non-parquet partitions, the field is set to -1.
 */
public final class Mig940 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);

    // Local copies of constants to avoid depending on values that may change.
    private static final int LONGS_PER_PARTITION = 4;
    private static final int PARTITION_MASKED_SIZE_IDX = 1;
    private static final int PARTITION_NAME_TX_IDX = 2;
    private static final int PARTITION_PM_FILE_SIZE_IDX = 3;
    private static final int PARQUET_FORMAT_BIT = 61;
    private static final long META_OFFSET_PARTITION_BY = 4;
    private static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final long META_OFFSET_COUNT = 0;
    private static final long META_OFFSET_COLUMN_TYPES = 128;
    private static final long META_COLUMN_DATA_SIZE = 32;

    public static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();

        // Read _meta to get partitionBy and timestampType.
        path.concat(META_FILE_NAME);
        if (!ff.exists(path.$())) {
            LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        final int partitionBy;
        final int timestampType;
        long metaFileSize = ff.length(path.$());
        try (MemoryMARW metaMem = Vm.getCMARWInstance(
                ff, path.$(), ff.getPageSize(), metaFileSize, MemoryTag.NATIVE_MIG_MMAP, CairoConfiguration.O_NONE
        )) {
            partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
            int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            if (timestampIndex >= 0) {
                int columnCount = metaMem.getInt(META_OFFSET_COUNT);
                if (timestampIndex < columnCount) {
                    timestampType = metaMem.getInt(META_OFFSET_COLUMN_TYPES + timestampIndex * META_COLUMN_DATA_SIZE);
                } else {
                    timestampType = ColumnType.TIMESTAMP;
                }
            } else {
                timestampType = ColumnType.TIMESTAMP;
            }
        }
        path.trimTo(plen);

        if (!PartitionBy.isPartitioned(partitionBy)) {
            // Non-partitioned tables never have parquet partitions.
            return;
        }

        // Open _txn file.
        path.concat(TXN_FILE_NAME);
        if (!ff.exists(path.$())) {
            LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }
        EngineMigration.backupFile(ff, path, migrationContext.getTablePath2(), TXN_FILE_NAME, 426);

        LOG.info().$("generating parquet metadata files [path=").$(path).I$();
        try (MemoryMARW txMem = migrationContext.createRwMemoryOf(ff, path.$())) {
            long version = txMem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
            boolean isA = (version & 1) == 0;

            int baseOffset = isA
                    ? txMem.getInt(TableUtils.TX_BASE_OFFSET_A_32)
                    : txMem.getInt(TableUtils.TX_BASE_OFFSET_B_32);

            int symbolColumnCount = txMem.getInt(baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32);
            long partitionTableSizeOffset = baseOffset
                    + TableUtils.getPartitionTableSizeOffset(symbolColumnCount);
            int partitionTableSize = txMem.getInt(partitionTableSizeOffset);
            int partitionLongs = partitionTableSize / Long.BYTES;
            int partitionCount = partitionLongs / LONGS_PER_PARTITION;

            long dataStart = partitionTableSizeOffset + Integer.BYTES;
            path.trimTo(plen);

            for (int i = 0; i < partitionCount; i++) {
                long entryOffset = dataStart + (long) i * LONGS_PER_PARTITION * Long.BYTES;
                long maskedSize = txMem.getLong(entryOffset + PARTITION_MASKED_SIZE_IDX * Long.BYTES);
                boolean isParquet = ((maskedSize >>> PARQUET_FORMAT_BIT) & 1) == 1;
                long fieldOffset = entryOffset + PARTITION_PM_FILE_SIZE_IDX * Long.BYTES;

                if (!isParquet) {
                    txMem.putLong(fieldOffset, -1L);
                    continue;
                }

                long partitionTs = txMem.getLong(entryOffset);
                long nameTxn = txMem.getLong(entryOffset + PARTITION_NAME_TX_IDX * Long.BYTES);

                long parquetMetaFileSize = generateParquetMetaForPartition(ff, path, plen, timestampType, partitionBy, partitionTs, nameTxn);
                txMem.putLong(fieldOffset, parquetMetaFileSize);
            }
        }
        path.trimTo(plen);
    }

    private static long generateParquetMetaForPartition(
            FilesFacade ff,
            Path path,
            int pathRootLen,
            int timestampType,
            int partitionBy,
            long partitionTs,
            long nameTxn
    ) {
        TableUtils.setPathForNativePartition(path.trimTo(pathRootLen), timestampType, partitionBy, partitionTs, nameTxn);
        int partitionDirLen = path.size();

        // Open data.parquet for reading.
        path.concat(TableUtils.PARQUET_PARTITION_NAME).$();
        if (!ff.exists(path.$())) {
            LOG.error().$("parquet file not found, skipping [path=").$(path).I$();
            path.trimTo(partitionDirLen);
            return -1L;
        }

        long parquetFileSize = ff.length(path.$());
        if (parquetFileSize <= 0) {
            LOG.error().$("parquet file empty or unreadable [path=").$(path).I$();
            path.trimTo(partitionDirLen);
            return -1L;
        }

        long parquetFd = ff.openRO(path.$());
        if (parquetFd < 0) {
            LOG.error().$("cannot open parquet file [path=").$(path).$(", errno=").$(ff.errno()).I$();
            path.trimTo(partitionDirLen);
            return -1L;
        }

        // Create _pm for writing.
        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
        long parquetMetaFd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
        if (parquetMetaFd < 0) {
            LOG.error().$("cannot create parquet metadata file [path=").$(path).$(", errno=").$(ff.errno()).I$();
            ff.close(parquetFd);
            path.trimTo(partitionDirLen);
            return -1L;
        }

        try {
            long parquetMetaSize = ParquetMetadataWriter.generate(Files.toOsFd(parquetFd), parquetFileSize, Files.toOsFd(parquetMetaFd));
            LOG.info().$("generated parquet metadata [path=").$(path).$(", parquetMetadataFileSize=").$(parquetMetaSize).I$();
            return parquetMetaSize;
        } catch (Throwable t) {
            LOG.error().$("failed to generate parquet metadata [path=").$(path).$(", error=").$(t.getMessage()).I$();
            return -1L;
        } finally {
            ff.close(parquetFd);
            ff.close(parquetMetaFd);
            path.trimTo(partitionDirLen);
        }
    }
}
