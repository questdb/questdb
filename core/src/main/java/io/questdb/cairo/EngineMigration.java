/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.ColumnType.VERSION_THAT_ADDED_TABLE_ID;
import static io.questdb.cairo.TableUtils.*;

public class EngineMigration {
    public static final int VERSION_TX_STRUCT_UPDATE_1 = 418;

    // All offsets hardcoded here in case TableUtils offset calculation changes
    // in future code version
    public static final long TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT = 72;
    public static final long TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY = 4;
    public static final long TX_STRUCT_UPDATE_1_OFFSET_MIN_TIMESTAMP = 24;
    public static final long TX_STRUCT_UPDATE_1_OFFSET_MAX_TIMESTAMP = 32;
    public static final String TX_STRUCT_UPDATE_1_ARCHIVE_FILE_NAME = "_archive";

    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final ObjList<MigrationAction> MIGRATIONS = new ObjList<>();
    private static final IntList MIGRATIONS_CRITICALITY = new IntList();
    private static final int MIGRATIONS_LIST_OFFSET = VERSION_THAT_ADDED_TABLE_ID;
    private final CairoEngine engine;
    private final CairoConfiguration configuration;
    private boolean updateSuccess;

    public EngineMigration(CairoEngine engine, CairoConfiguration configuration) {
        this.engine = engine;
        this.configuration = configuration;
    }

    public void migrateEngineTo(int latestVersion) {
        final FilesFacade ff = configuration.getFilesFacade();
        int tempMemSize = 8;
        long mem = Unsafe.malloc(tempMemSize);

        try (var virtualMem = new VirtualMemory(ff.getPageSize(), 8);
             var path = new Path();
             var rwMemory = new ReadWriteMemory()) {

            var context = new MigrationContext(mem, tempMemSize, virtualMem, rwMemory);
            path.of(configuration.getRoot());

            // check if all tables have been upgraded already
            path.concat(TableUtils.UPGRADE_FILE_NAME).$();
            final boolean existed = ff.exists(path);
            long upgradeFd = openFileRWOrFail(ff, path);
            LOG.debug()
                    .$("open [fd=").$(upgradeFd)
                    .$(", path=").$(path)
                    .$(']').$();
            if (existed) {
                long readLen = ff.read(upgradeFd, mem, Integer.BYTES, 0);
                if (readLen == Integer.BYTES && Unsafe.getUnsafe().getInt(mem) >= latestVersion) {
                    LOG.info().$("table structures are up to date").$();
                    ff.close(upgradeFd);
                    upgradeFd = -1;
                }
            }

            if (upgradeFd != -1) {
                try {
                    LOG.info().$("upgrading database [version=").$(latestVersion).I$();
                    if (upgradeTables(context, latestVersion)) {
                        Unsafe.getUnsafe().putInt(mem, latestVersion);
                        long writeLen = ff.write(upgradeFd, mem, Integer.BYTES, 0);
                        if (writeLen < Integer.BYTES) {
                            LOG.error().$("could not write to ").$(UPGRADE_FILE_NAME)
                                    .$(" [fd=").$(upgradeFd).$(",errno=").$(ff.errno()).I$();
                        }
                    }
                } finally {
                    ff.close(upgradeFd);
                }
            }
        } finally {
            Unsafe.free(mem, tempMemSize);
        }
    }

    static MigrationAction getMigrationToVersion(int version) {
        return MIGRATIONS.getQuick(version - MIGRATIONS_LIST_OFFSET);
    }

    private static int getMigrationToVersionCriticality(int version) {
        return MIGRATIONS_CRITICALITY.getQuick(version - MIGRATIONS_LIST_OFFSET);
    }

    static void setByVersion(int version, MigrationAction action, int criticality) {
        MIGRATIONS.setQuick(version - MIGRATIONS_LIST_OFFSET, action);
        MIGRATIONS_CRITICALITY.setQuick(version - MIGRATIONS_LIST_OFFSET, criticality);
    }

    private boolean upgradeTables(MigrationContext context, int latestVersion) {
        final FilesFacade ff = configuration.getFilesFacade();
        long mem = context.getTempMemory(8);
        updateSuccess = true;

        try (Path path = new Path()) {
            path.of(configuration.getRoot());
            final int rootLen = path.length();

            final NativeLPSZ nativeLPSZ = new NativeLPSZ();
            ff.iterateDir(path.$(), (name, type) -> {
                if (type == Files.DT_DIR) {
                    nativeLPSZ.of(name);
                    if (Chars.notDots(nativeLPSZ)) {
                        path.trimTo(rootLen);
                        path.concat(nativeLPSZ);
                        final int plen = path.length();
                        path.concat(TableUtils.META_FILE_NAME);

                        if (ff.exists(path.$())) {
                            final long fd = openFileRWOrFail(ff, path);
                            try {
                                if (ff.read(fd, mem, Integer.BYTES, META_OFFSET_VERSION) == Integer.BYTES) {
                                    int currentTableVersion = Unsafe.getUnsafe().getInt(mem);

                                    if (currentTableVersion < latestVersion) {
                                        LOG.info().$("upgrading [path=").$(path).$(",fromVersion=").$(currentTableVersion)
                                                .$(",toVersion=").$(latestVersion).I$();

                                        path.trimTo(plen);
                                        context.of(path, fd);

                                        for (int i = currentTableVersion + 1; i <= latestVersion; i++) {
                                            var migration = getMigrationToVersion(i);
                                            try {
                                                if (migration != null) {
                                                    LOG.info().$("upgrading table [path=").$(path).$(",toVersion=").$(i).I$();
                                                    migration.migrate(context);
                                                }
                                            } catch (Exception e) {
                                                LOG.error().$("failed to upgrade table path=")
                                                        .$(path.trimTo(plen))
                                                        .$(", exception: ")
                                                        .$(e).$();

                                                if (getMigrationToVersionCriticality(i) != 0) {
                                                    throw e;
                                                }
                                                updateSuccess = false;
                                                return;
                                            }

                                            Unsafe.getUnsafe().putInt(mem, i);
                                            if (ff.write(fd, mem, Integer.BYTES, META_OFFSET_VERSION) != Integer.BYTES) {
                                                // Table is migrated but we cannot write new version
                                                // to meta file
                                                // This is critical, table potentially left in unusable state
                                                throw CairoException.instance(ff.errno())
                                                        .put("failed to write updated version to table Metadata file [path=")
                                                        .put(path.trimTo(plen))
                                                        .put(",latestVersion=")
                                                        .put(i)
                                                        .put(']');
                                            }
                                        }
                                    }
                                    return;
                                }
                                updateSuccess = false;
                                throw CairoException.instance(ff.errno()).put("Could not update table [path=").put(path).put(']');
                            } finally {
                                ff.close(fd);
                                path.trimTo(plen);
                            }
                        }
                    }
                }
            });
            LOG.info().$("upgraded tables to ").$(latestVersion).$();
        }
        return updateSuccess;
    }

    @FunctionalInterface
    interface MigrationAction {
        void migrate(MigrationContext context);
    }

    private static class MigrationActions {
        private static void assignTableId(MigrationContext migrationContext) {
            long mem = migrationContext.getTempMemory(8);
            FilesFacade ff = migrationContext.getFf();
            Path path = migrationContext.getTablePath();

            LOG.info().$("setting table id in [path=").$(path).I$();
            Unsafe.getUnsafe().putInt(mem, migrationContext.getNextTableId());
            if (ff.write(migrationContext.getMetadataFd(), mem, Integer.BYTES, META_OFFSET_TABLE_ID) == Integer.BYTES) {
                return;
            }
            throw CairoException.instance(ff.errno()).put("Could not update table id [path=").put(path).put(']');
        }

        private static void rebuildTransactionFile(MigrationContext migrationContext) {
            // Update transaction file
            // Before there was 1 int per symbol and list of removed partitions
            // Now there is 2 ints per symbol and 4 longs per each non-removed partition

            var path = migrationContext.getTablePath();
            final var ff = migrationContext.getFf();
            int pathDirLen = path.length();

            path.concat(TXN_FILE_NAME).$();
            var txMem = migrationContext.getRwMemory();
            LOG.debug().$("opening for rw [path=").$(path).I$();
            txMem.of(ff, path.$(), ff.getPageSize());
            var tempMem8b = migrationContext.getTempMemory(8);

            var txFileUpdate = migrationContext.getTempVirtualMem();
            txFileUpdate.clear();
            txFileUpdate.jumpTo(0);

            try {
                int symbolsCount = txMem.getInt(TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT);
                for (int i = 0; i < symbolsCount; i++) {
                    long symbolCountOffset = TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + (i + 1L) * Integer.BYTES;
                    int symDistinctCount = txMem.getInt(symbolCountOffset);
                    txFileUpdate.putInt(symDistinctCount);
                    txFileUpdate.putInt(symDistinctCount);
                }

                // Set partition segment size as 0 for now
                long partitionSegmentOffset = txFileUpdate.getAppendOffset();
                txFileUpdate.putInt(0);

                int partitionBy = readIntAtOffset(ff, path, tempMem8b, TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY, migrationContext.getMetadataFd());
                if (partitionBy != PartitionBy.NONE) {
                    path.trimTo(pathDirLen);
                    writeAttachedPartitions(ff, tempMem8b, path, txMem, partitionBy, symbolsCount, txFileUpdate);
                }
                long updateSize = txFileUpdate.getAppendOffset();
                long partitionSegmentSize = updateSize - partitionSegmentOffset - Integer.BYTES;
                txFileUpdate.putInt(partitionSegmentOffset, (int) partitionSegmentSize);

                // Save txFileUpdate to tx file starting at LOCAL_TX_OFFSET_MAP_WRITER_COUNT + 4
                long writeOffset = TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + Integer.BYTES;
                txMem.jumpTo(writeOffset);

                for (int i = 0, size = txFileUpdate.getPageCount(); i < size && updateSize > 0; i++) {
                    long writeSize = Math.min(updateSize, txFileUpdate.getPageSize(i));
                    txMem.putBlockOfBytes(txFileUpdate.getPageAddress(i), writeSize);
                    updateSize -= writeSize;
                }

                assert updateSize == 0;
            } finally {
                txMem.close();
            }
        }

        private static void writeAttachedPartitions(
                FilesFacade ff,
                long tempMem8b,
                Path path,
                ReadWriteMemory txMem,
                int partitionBy,
                int symbolsCount,
                VirtualMemory writeTo) {
            int rootLen = path.length();

            long minTimestamp = txMem.getLong(TX_STRUCT_UPDATE_1_OFFSET_MIN_TIMESTAMP);
            long maxTimestamp = txMem.getLong(TX_STRUCT_UPDATE_1_OFFSET_MAX_TIMESTAMP);

            var timestampFloorMethod = getPartitionFloor(partitionBy);
            var timestampAddMethod = getPartitionAdd(partitionBy);

            final long tsLimit = timestampFloorMethod.floor(maxTimestamp);
            for (long ts = timestampFloorMethod.floor(minTimestamp); ts < tsLimit; ts = timestampAddMethod.calculate(ts, 1)) {
                path.trimTo(rootLen);
                setPathForPartition(path, partitionBy, ts);
                if (ff.exists(path.concat(TX_STRUCT_UPDATE_1_ARCHIVE_FILE_NAME).$())) {
                    if (!removedPartitionsIncludes(ts, txMem, symbolsCount)) {
                        long partitionSize = TableUtils.readLongAtOffset(ff, path, tempMem8b, 0);

                        // Update tx file with 4 longs per partition
                        writeTo.putLong(ts);
                        writeTo.putLong(partitionSize);
                        writeTo.putLong(0L);
                        writeTo.putLong(0L);
                    }
                }
            }
        }

        private static boolean removedPartitionsIncludes(long ts, ReadWriteMemory txMem, int symbolsCount) {
            long removedPartitionLo = TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + (symbolsCount + 1L) * Integer.BYTES;
            long removedPartitionCount = txMem.getInt(removedPartitionLo);
            long removedPartitionsHi = removedPartitionLo + Long.BYTES * removedPartitionCount;

            for (long offset = removedPartitionLo + Integer.BYTES; offset < removedPartitionsHi; offset += Long.BYTES) {
                long removedPartition = txMem.getLong(offset);
                if (removedPartition == ts) {
                    return true;
                }
            }
            return false;
        }
    }

    static int readIntAtOffset(FilesFacade ff, Path path, long tempMem4b, long offset, long fd) {
        if (ff.read(fd, tempMem4b, Integer.BYTES, offset) != Integer.BYTES) {
            throw CairoException.instance(ff.errno()).put("Cannot read: ").put(path);
        }
        return Unsafe.getUnsafe().getInt(tempMem4b);
    }

    class MigrationContext {
        private final long tempMemory;
        private final int tempMemoryLen;
        private final VirtualMemory tempVirtualMem;
        private final ReadWriteMemory rwMemory;
        private Path tablePath;
        private long metadataFd;

        public MigrationContext(long mem, int tempMemSize, VirtualMemory tempVirtualMem, ReadWriteMemory rwMemory) {
            this.tempMemory = mem;
            this.tempMemoryLen = tempMemSize;
            this.tempVirtualMem = tempVirtualMem;
            this.rwMemory = rwMemory;
        }

        public FilesFacade getFf() {
            return configuration.getFilesFacade();
        }

        public long getMetadataFd() {
            return metadataFd;
        }

        public int getNextTableId() {
            return (int) engine.getNextTableId();
        }

        public ReadWriteMemory getRwMemory() {
            return rwMemory;
        }

        public Path getTablePath() {
            return tablePath;
        }

        public long getTempMemory(int size) {
            if (size <= tempMemoryLen) {
                return tempMemory;
            }
            throw new UnsupportedOperationException("No temp memory of size "
                    + size
                    + " is allocate. Only "
                    + tempMemoryLen
                    + " is available");
        }

        public VirtualMemory getTempVirtualMem() {
            return tempVirtualMem;
        }

        public MigrationContext of(Path path, long metadataFd) {
            this.tablePath = path;
            this.metadataFd = metadataFd;
            return this;
        }
    }

    static {
        MIGRATIONS.extendAndSet(ColumnType.VERSION - MIGRATIONS_LIST_OFFSET, null);
        setByVersion(VERSION_THAT_ADDED_TABLE_ID, MigrationActions::assignTableId, 1);
        setByVersion(VERSION_TX_STRUCT_UPDATE_1, MigrationActions::rebuildTransactionFile, 0);
    }
}
