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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Records in the {@code _pm} of every parquet partition whether QuestDB's parquet writer wrote it, for
 * {@code _pm} files written before the record existed. A partition whose parquet {@code created_by} is
 * QuestDB's gains a footer that carries the record; one another writer wrote, such as an attached external
 * file, stays as it is. Row group pruning trusts NaN to be a Parquet null only in a recorded partition.
 * <p>
 * The migration reads {@code _meta} and {@code _txn} through read-only mappings and never modifies them.
 * {@link EngineMigration} skips a table whose {@code _meta} is shorter than its header before any migration
 * reads it. A table whose {@code _txn} is shorter than the fields it reads, or holds a partition table that does
 * not fit, is logged and left as it is: its partitions are still read correctly without the record, and the
 * damaged file fails only that table when it opens, not the server start. Each {@code _pm} gains its
 * footer through the same append-then-publish sequence as an incremental update, so a crash leaves the
 * committed snapshot in place, and a rerun skips partitions already recorded.
 * <p>
 * The migration must stay {@link MigrationAction.StorageVersionAgnostic}. Composite partitions shipped in
 * 10.1.0, before this migration, so a table can already carry {@code ColumnType.MAX_STORAGE_VERSION} in its
 * {@code _meta} when the migration first runs, and {@link EngineMigration} runs only agnostic migrations on
 * such a table. Its parquet partitions would otherwise never gain the record, and their FLOAT and DOUBLE
 * columns would never prune a negated range comparison.
 * The migration qualifies because it reads only the partitioning and the designated timestamp type from
 * {@code _meta}, the {@code _txn} partition table and the files of parquet partitions, whose formats no
 * storage version changes, and a parquet partition is never composite.
 */
public final class Mig1011 {
    private static Log LOG = LogFactory.getLog(EngineMigration.class);

    // Local copies of constants to avoid depending on values that may change.
    private static final int LONGS_PER_PARTITION = 4;
    private static final long META_COLUMN_DATA_SIZE = 32;
    private static final long META_OFFSET_COLUMN_TYPES = 128;
    private static final long META_OFFSET_COUNT = 0;
    private static final long META_OFFSET_PARTITION_BY = 4;
    private static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final long PARQUET_FILE_SIZE_VALUE_MASK = ~(0xFFL << 56);
    private static final int PARQUET_FORMAT_BIT = 61;
    private static final int PARQUET_GENERATED_BIT = 60;
    private static final long PARQUET_REMOTE_BIT = 1L << 63;
    private static final int PARTITION_MASKED_SIZE_IDX = 1;
    private static final int PARTITION_NAME_TX_IDX = 2;
    private static final int PARTITION_PARQUET_FILE_SIZE_IDX = 3;

    public static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();

        path.concat(META_FILE_NAME);
        final long metaFileSize = ff.length(path.$());
        final int partitionBy;
        final int timestampType;
        try (MemoryCMR metaMem = Vm.getCMRInstance(ff, path.$(), metaFileSize, MemoryTag.NATIVE_MIG_MMAP)) {
            partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
            final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            final long timestampTypeOffset = META_OFFSET_COLUMN_TYPES + timestampIndex * META_COLUMN_DATA_SIZE;
            timestampType = timestampIndex >= 0 && timestampIndex < metaMem.getInt(META_OFFSET_COUNT)
                    && timestampTypeOffset + Integer.BYTES <= metaFileSize
                    ? metaMem.getInt(timestampTypeOffset)
                    : ColumnType.TIMESTAMP;
        }
        path.trimTo(plen);
        if (!PartitionBy.isPartitioned(partitionBy)) {
            return;
        }

        path.concat(TXN_FILE_NAME);
        if (!ff.exists(path.$())) {
            path.trimTo(plen);
            return;
        }
        final long txFileSize = ff.length(path.$());
        if (txFileSize < TableUtils.TX_BASE_HEADER_SIZE) {
            LOG.error().$("skipping table, _txn is shorter than its header [path=").$(path)
                    .$(", size=").$(txFileSize).I$();
            path.trimTo(plen);
            return;
        }
        int marked = 0;
        int unchanged = 0;
        int failed = 0;
        try (MemoryCMR txMem = Vm.getCMRInstance(ff, path.$(), txFileSize, MemoryTag.NATIVE_MIG_MMAP)) {
            final long version = txMem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
            final int baseOffset = (version & 1) == 0
                    ? txMem.getInt(TableUtils.TX_BASE_OFFSET_A_32)
                    : txMem.getInt(TableUtils.TX_BASE_OFFSET_B_32);
            final long symbolCountOffset = baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32;
            final int symbolColumnCount = baseOffset >= 0 && symbolCountOffset + Integer.BYTES <= txFileSize
                    ? txMem.getInt(symbolCountOffset)
                    : -1;
            final long partitionTableSizeOffset = symbolColumnCount >= 0
                    ? baseOffset + TableUtils.getPartitionTableSizeOffset(symbolColumnCount)
                    : txFileSize;
            final int partitionCount = partitionTableSizeOffset + Integer.BYTES <= txFileSize
                    ? txMem.getInt(partitionTableSizeOffset) / Long.BYTES / LONGS_PER_PARTITION
                    : -1;
            final long dataStart = partitionTableSizeOffset + Integer.BYTES;
            if (partitionCount < 0 || dataStart + (long) partitionCount * LONGS_PER_PARTITION * Long.BYTES > txFileSize) {
                LOG.error().$("skipping table, corrupt _txn partition table [path=").$(path)
                        .$(", baseOffset=").$(baseOffset)
                        .$(", symbolColumnCount=").$(symbolColumnCount)
                        .$(", partitionCount=").$(partitionCount)
                        .$(", size=").$(txFileSize)
                        .I$();
                path.trimTo(plen);
                return;
            }
            path.trimTo(plen);

            for (int i = 0; i < partitionCount; i++) {
                final long entryOffset = dataStart + (long) i * LONGS_PER_PARTITION * Long.BYTES;
                final long maskedSize = txMem.getLong(entryOffset + PARTITION_MASKED_SIZE_IDX * Long.BYTES);
                if (((maskedSize >>> PARQUET_FORMAT_BIT) & 1) == 0) {
                    continue;
                }
                final long rawParquetFileSize = txMem.getLong(entryOffset + PARTITION_PARQUET_FILE_SIZE_IDX * Long.BYTES);
                final boolean isParquetGenerated = ((maskedSize >>> PARQUET_GENERATED_BIT) & 1) == 1;
                final boolean isRemote = rawParquetFileSize != -1L && (rawParquetFileSize & PARQUET_REMOTE_BIT) != 0;
                if (rawParquetFileSize == -1L || (isRemote && !isParquetGenerated)) {
                    continue;
                }
                TableUtils.setPathForNativePartition(
                        path.trimTo(plen),
                        timestampType,
                        partitionBy,
                        txMem.getLong(entryOffset),
                        txMem.getLong(entryOffset + PARTITION_NAME_TX_IDX * Long.BYTES)
                );
                final long result = markPartition(ff, path, rawParquetFileSize & PARQUET_FILE_SIZE_VALUE_MASK);
                if (result > 0) {
                    marked++;
                } else if (result == Long.MIN_VALUE) {
                    failed++;
                } else {
                    unchanged++;
                }
            }
        }
        path.trimTo(plen);
        LOG.info().$("recorded parquet writer in _pm files [path=").$(path)
                .$(", marked=").$(marked)
                .$(", unchanged=").$(unchanged)
                .$(", failed=").$(failed)
                .I$();
    }

    // Returns the new _pm size, 0 or -1 for a partition left as it is, or Long.MIN_VALUE when it failed.
    private static long markPartition(FilesFacade ff, Path path, long parquetFileSize) {
        final int partitionDirLen = path.size();
        long parquetFd = -1;
        long parquetMetaFd = -1;
        try {
            parquetFd = ff.openRO(path.concat(TableUtils.PARQUET_PARTITION_NAME).$());
            if (parquetFd < 0) {
                LOG.error().$("could not open parquet file [path=").$(path).$(", errno=").$(ff.errno()).I$();
                return Long.MIN_VALUE;
            }
            path.trimTo(partitionDirLen);
            if (!ff.exists(path.concat(TableUtils.PARQUET_METADATA_FILE_NAME).$())) {
                return -1;
            }
            parquetMetaFd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            if (parquetMetaFd < 0) {
                LOG.error().$("could not open parquet metadata file [path=").$(path).$(", errno=").$(ff.errno()).I$();
                return Long.MIN_VALUE;
            }
            return ParquetMetadataWriter.markWrittenByQuestDB(Files.toOsFd(parquetFd), parquetFileSize, Files.toOsFd(parquetMetaFd));
        } catch (CairoException e) {
            // The record only enables row group pruning; a partition without it is still read correctly.
            LOG.error().$("could not record parquet writer in _pm [path=").$(path).$(", msg=").$(e.getFlyweightMessage()).I$();
            return Long.MIN_VALUE;
        } finally {
            if (parquetFd >= 0) {
                ff.close(parquetFd);
            }
            if (parquetMetaFd >= 0) {
                ff.close(parquetMetaFd);
            }
            path.trimTo(partitionDirLen);
        }
    }
}
