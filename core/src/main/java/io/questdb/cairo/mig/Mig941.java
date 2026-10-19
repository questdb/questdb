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
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Generates {@code _pm} metadata files for existing parquet partitions.
 * <p>
 * Field 3 in {@code _txn} remains the parquet file size (unchanged).
 * The migration only reads {@code _txn} to locate partitions and generates
 * {@code _pm} files; it does not modify {@code _txn}.
 */
public final class Mig941 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);

    // Local copies of constants to avoid depending on values that may change.
    private static final int LONGS_PER_PARTITION = 4;
    private static final long META_COLUMN_DATA_SIZE = 32;
    private static final long META_OFFSET_COLUMN_TYPES = 128;
    private static final long META_OFFSET_COUNT = 0;
    private static final long META_OFFSET_PARTITION_BY = 4;
    private static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final long PARQUET_FILE_SIZE_VALUE_MASK = ~(0xFFL << 56); // strip the flag byte (bits 56-63), mirrors TxReader.PARTITION_VERSION_VALUE_MASK
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
        LOG.info().$("generating parquet metadata files [path=").$(path).I$();
        long txFileSize = ff.length(path.$());
        try (MemoryMARW txMem = Vm.getCMARWInstance(
                ff, path.$(), ff.getPageSize(), txFileSize, MemoryTag.NATIVE_MIG_MMAP, CairoConfiguration.O_NONE
        )) {
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
            // Reject a partitionCount that does not fit within the mapped _txn region.
            // A corrupt or truncated _txn could otherwise drive the loop below past the
            // mapped pages and SIGBUS the JVM. Mirrors the analogous bound check in Mig620.
            long maxPartitionCount = partitionCount > 0
                    ? Math.max(0L, (txFileSize - dataStart) / ((long) LONGS_PER_PARTITION * Long.BYTES))
                    : 0L;
            if (partitionCount > maxPartitionCount) {
                throw CairoException.critical(0)
                        .put("migration failed, corrupt _txn file, partitionCount exceeds mapped region [path=").put(path)
                        .put(", partitionCount=").put(partitionCount)
                        .put(", maxPartitionCount=").put(maxPartitionCount)
                        .put(", txFileSize=").put(txFileSize)
                        .put(']');
            }
            path.trimTo(plen);

            for (int i = 0; i < partitionCount; i++) {
                long entryOffset = dataStart + (long) i * LONGS_PER_PARTITION * Long.BYTES;
                long maskedSize = txMem.getLong(entryOffset + PARTITION_MASKED_SIZE_IDX * Long.BYTES);
                boolean isParquet = ((maskedSize >>> PARQUET_FORMAT_BIT) & 1) == 1;

                if (!isParquet) {
                    continue;
                }

                long partitionTs = txMem.getLong(entryOffset);
                long nameTxn = txMem.getLong(entryOffset + PARTITION_NAME_TX_IDX * Long.BYTES);
                long rawParquetFileSize = txMem.getLong(entryOffset + PARTITION_PARQUET_FILE_SIZE_IDX * Long.BYTES);

                final boolean parquetGenerated = ((maskedSize >>> PARQUET_GENERATED_BIT) & 1) == 1;
                final boolean remote = rawParquetFileSize != -1L
                        && (rawParquetFileSize & PARQUET_REMOTE_BIT) != 0;
                if (remote && !parquetGenerated) {
                    continue;
                }

                // Field 3 carries the REMOTE marker in bit 63 once a partition has a remote copy; the
                // actual parquet file size is the low bits. Mask it off before using it as a size,
                // mirroring TxReader.getPartitionParquetFileSize.
                long parquetFileSizeFromTxn = rawParquetFileSize == -1L
                        ? -1L
                        : rawParquetFileSize & PARQUET_FILE_SIZE_VALUE_MASK;
                generateParquetMetaForPartition(ff, path, plen, timestampType, partitionBy, partitionTs, nameTxn, parquetFileSizeFromTxn);
            }
        }
        path.trimTo(plen);
    }

    private static void generateParquetMetaForPartition(
            FilesFacade ff,
            Path path,
            int pathRootLen,
            int timestampType,
            int partitionBy,
            long partitionTs,
            long nameTxn,
            long parquetFileSizeFromTxn
    ) {
        TableUtils.setPathForNativePartition(path.trimTo(pathRootLen), timestampType, partitionBy, partitionTs, nameTxn);
        int partitionDirLen = path.size();

        // Open data.parquet for reading.
        path.concat(TableUtils.PARQUET_PARTITION_NAME).$();
        if (!ff.exists(path.$())) {
            path.trimTo(partitionDirLen);
            throw CairoException.critical(0).put("parquet file not found [path=").put(path).put(']');
        }

        long parquetFileSize = ff.length(path.$());
        if (parquetFileSize <= 0 || parquetFileSize < parquetFileSizeFromTxn) {
            path.trimTo(partitionDirLen);
            throw CairoException.critical(0).put("parquet file empty or unreadable [path=").put(path).put(']');
        }

        long parquetFd = ff.openRO(path.$());
        if (parquetFd < 0) {
            int errno = ff.errno();
            path.trimTo(partitionDirLen);
            throw CairoException.critical(errno).put("cannot open parquet file [path=").put(path).put(']');
        }

        // Create _pm for writing.
        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
        long parquetMetaFd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
        if (parquetMetaFd < 0) {
            int errno = ff.errno();
            ff.close(parquetFd);
            path.trimTo(partitionDirLen);
            throw CairoException.critical(errno).put("cannot create parquet metadata file [path=").put(path).put(']');
        }

        try {
            if (!ff.truncate(parquetMetaFd, 0)) {
                throw CairoException.critical(ff.errno()).put("could not truncate _pm [path=").put(path).put(']');
            }
            long allocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_MIG);
            long parquetMetaSize = ParquetMetadataWriter.generate(allocator, Files.toOsFd(parquetFd), parquetFileSizeFromTxn, Files.toOsFd(parquetMetaFd));
            // Persist the new _pm before the migration completes. If the process
            // crashes between this generate call and the next time the engine
            // syncs the partition dir, partitions referenced by _txn would
            // otherwise come back without a usable _pm sidecar.
            ff.fsync(parquetMetaFd);
            LOG.debug().$("generated parquet metadata [path=").$(path).$(", parquetMetadataFileSize=").$(parquetMetaSize).I$();
        } catch (Throwable t) {
            // Remove partially written _pm file so a retry regenerates it.
            if (!ff.removeQuiet(path.$())) {
                LOG.advisory().$("could not remove partial parquet metadata file [path=").$(path)
                        .$(", errno=").$(ff.errno()).I$();
            }
            throw t;
        } finally {
            ff.close(parquetFd);
            ff.close(parquetMetaFd);
            // _pm is brand new in this dir; fsync the parent so the dirent survives a crash.
            if (!Os.isWindows()) {
                path.trimTo(partitionDirLen).$();
                final long dirFd = TableUtils.openRONoCache(ff, path.$(), LOG);
                if (dirFd != -1) {
                    ff.fsyncAndClose(dirFd);
                }
            }
            path.trimTo(partitionDirLen);
        }
    }
}
