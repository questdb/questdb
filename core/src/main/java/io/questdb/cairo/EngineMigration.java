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
import io.questdb.std.datetime.DateFormat;
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

    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final ObjList<MigrationAction> MIGRATIONS = new ObjList<>();
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
             var path = new Path()) {

            var context = new MigrationContext(mem, tempMemSize, virtualMem);
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

    static void setByVersion(int version, MigrationAction action) {
        MIGRATIONS.setQuick(version - MIGRATIONS_LIST_OFFSET, action);
    }

    private boolean upgradeTables(MigrationContext context, int latestVersion) {
        final FilesFacade ff = configuration.getFilesFacade();
        long mem = context.getTempMemory(8);
        updateSuccess = true;

        try (Path path = new Path()) {
            path.of(configuration.getRoot());
            final int rootLen = path.length();

            ff.iterateDir(path.$(), (name, type) -> {
                if (type == Files.DT_DIR) {
                    final NativeLPSZ nativeLPSZ = new NativeLPSZ();
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

                                        try {
                                            for (int i = currentTableVersion + 1; i <= latestVersion; i++) {
                                                var migration = getMigrationToVersion(i);
                                                if (migration != null) {
                                                    LOG.info().$("upgrading table [path=").$(path).$(",toVersion=").$(i).I$();
                                                    migration.migrate(context);
                                                }
                                            }
                                        } catch (CairoException e) {
                                            LOG.error().$("failed to upgrade table path=")
                                                    .$(path.trimTo(plen))
                                                    .$(", exception: ")
                                                    .$((Sinkable) e).$();
                                            return;
                                        }

                                        Unsafe.getUnsafe().putInt(mem, latestVersion);
                                        if (ff.write(fd, mem, Integer.BYTES, META_OFFSET_VERSION) != Integer.BYTES) {
                                            LOG.error().$("failed to write updated version to table TX file [path=")
                                                    .$(path.trimTo(plen))
                                                    .$(",latestVersion=")
                                                    .$(latestVersion).I$();
                                            updateSuccess = false;
                                        }
                                    }
                                    return;
                                }
                                updateSuccess = false;
                                LOG.error().$("Could not update table id [path=").$(path).I$();
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
            Unsafe.getUnsafe().putInt(mem, ColumnType.VERSION);
            Unsafe.getUnsafe().putInt(mem + Integer.BYTES, migrationContext.getNextTableId());
            if (ff.write(migrationContext.getMetadataFd(), mem, 8, META_OFFSET_VERSION) == 8) {
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
            var tempMem8b = migrationContext.getTempMemory(8);

            LOG.debug().$("opening for rw [path=").$(path).I$();
            var fd = openFileRWOrFail(ff, path.$());
            var txFileUpdate = migrationContext.getTempVirtualMem();
            txFileUpdate.clear();
            txFileUpdate.jumpTo(0);

            try {
                int symbolsCount = readIntAtOffset(ff, path, tempMem8b, TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT, fd);
                for (int i = 0; i < symbolsCount; i++) {
                    long symbolCountOffset = TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + i * Integer.BYTES;
                    int symbolCount = readIntAtOffset(ff, path, tempMem8b, symbolCountOffset, fd);
                    txFileUpdate.putInt(symbolCount);
                    txFileUpdate.putInt(symbolCount);
                }

                // Set partition segment size as 0 for now
                long partitionSegmentOffset = txFileUpdate.getAppendOffset();
                txFileUpdate.putInt(0);

                int partitionBy = readIntAtOffset(ff, path, tempMem8b, TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY, migrationContext.getMetadataFd());
                if (partitionBy != PartitionBy.NONE) {
                    path.trimTo(pathDirLen);
                    long oldPartitionSectionOffset = TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + 4L + 4L * symbolsCount;
                    writeAttachedPartitions(path, ff, tempMem8b, fd, txFileUpdate, partitionBy, oldPartitionSectionOffset);
                }
                long updateSize = txFileUpdate.getAppendOffset();
                long partitionSegmentSize = updateSize - partitionSegmentOffset - 4;
                txFileUpdate.putInt(partitionSegmentOffset, (int) partitionSegmentSize);

                // Save txFileUpdate to tx file starting at LOCAL_TX_OFFSET_MAP_WRITER_COUNT + 4
                long writeOffset = TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + 4;
                for (int i = 0, size = txFileUpdate.getPageCount(); i < size && updateSize > 0; i++) {
                    long writeSize = Math.min(updateSize, txFileUpdate.getPageSize(i));
                    if (ff.write(fd, txFileUpdate.getPageAddress(i), writeSize, writeOffset) != writeSize) {
                        throw CairoException.instance(ff.errno())
                                .put("cannot write to transaction file [path=")
                                .put(path)
                                .put(",writeOffset=")
                                .put(writeOffset)
                                .put(",writeSize")
                                .put(writeSize)
                                .put(']');
                    }
                    updateSize -= writeSize;
                }

                assert updateSize == 0;
            } finally {
                ff.close(fd);
            }
        }

        private static void writeAttachedPartitions(Path path, FilesFacade ff, long tempMem8b, long fd, VirtualMemory txFileUpdate, int partitionBy, long removedPartitionsCountOffset) {
            int pathDirLen = path.length();
            int removedPartitionCount = readIntAtOffset(ff, path, tempMem8b, removedPartitionsCountOffset, fd);
            int removedPartitionBuffSize = removedPartitionCount * Long.BYTES;
            long removedPartitionsBuff = Unsafe.malloc(removedPartitionBuffSize);
            var nativeLPSZ = new NativeLPSZ();

            try {
                // read removed partitions
                if (ff.read(fd, removedPartitionsBuff, removedPartitionBuffSize, removedPartitionsCountOffset + Integer.BYTES) != removedPartitionBuffSize) {
                    // Corrupted removed partition segment. Ignore them.
                    LOG.error().$("cannot read removed partitions list from transaction file, proceeding on assumption of no removed partitions").$();
                    removedPartitionCount = 0;
                }

                // Discover all partition folders
                path.trimTo(pathDirLen);
                final int finalRemovedPartitionCount = removedPartitionCount;
                final int plen = path.length();
                ff.iterateDir(path.$(), (pName, type) -> {
                    try {
                        nativeLPSZ.of(pName);
                        if (type == Files.DT_DIR && !Files.isDots(nativeLPSZ)) {
                            long dirTimestamp = getPartitionDateFmt(partitionBy).parse(nativeLPSZ, null);

                            // Check dir in removed list
                            for (int i = 0; i < finalRemovedPartitionCount; i++) {
                                long removedPartitionTs = Unsafe.getUnsafe().getLong(removedPartitionsBuff + i * Long.BYTES);
                                if (dirTimestamp == removedPartitionTs) {
                                    // Skip dir
                                    return;
                                }
                            }

                            // Dir is not in removed list
                            path.concat(nativeLPSZ);
                            long partitionSize = readPartitionSize(ff, path, tempMem8b);
                            if (partitionSize > 0) {
                                txFileUpdate.putLong(dirTimestamp);
                                txFileUpdate.putLong(partitionSize);
                                txFileUpdate.putLong(0L);
                                txFileUpdate.putLong(0L);
                            }
                        }
                    } catch (NumericException e) {
                        // Ignore the directory
                    } finally {
                        path.trimTo(plen);
                    }
                });

            } finally {
                Unsafe.free(removedPartitionsBuff, removedPartitionBuffSize);
            }
        }
//
//        private static int countDirs(Path path, FilesFacade ff, long tempMem8b) {
//            Unsafe.getUnsafe().putInt(tempMem8b, 0);
//            ff.iterateDir(path, (pName, type) -> {
//                if (type == Files.DT_DIR) {
//                    int dirCount = Unsafe.getUnsafe().getInt(tempMem8b) + 1;
//                    Unsafe.getUnsafe().putInt(tempMem8b, dirCount);
//                }
//            });
//            return Unsafe.getUnsafe().getInt(tempMem8b);
//        }

        private static long readPartitionSize(FilesFacade ff, Path path, long tempMem8b) {
            int plen = path.length();
            try {
                if (ff.exists(path.concat("_archive").$())) {
                    long fd = ff.openRO(path);
                    if (fd == -1) {
                        throw CairoException.instance(Os.errno()).put("Cannot open: ").put(path);
                    }

                    try {
                        if (ff.read(fd, tempMem8b, 8, 0) != 8) {
                            throw CairoException.instance(Os.errno()).put("Cannot read: ").put(path);
                        }
                        return Unsafe.getUnsafe().getLong(tempMem8b);
                    } finally {
                        ff.close(fd);
                    }
                } else {
                    return -1;
                }
            } finally {
                path.trimTo(plen);
            }
        }

        private static DateFormat getPartitionDateFmt(int partitionBy) {
            switch (partitionBy) {
                case PartitionBy.DAY:
                    return fmtDay;
                case PartitionBy.MONTH:
                    return fmtMonth;
                case PartitionBy.YEAR:
                    return fmtYear;
                default:
                    throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
            }
        }
    }

    class MigrationContext {
        private final long tempMemory;
        private final int tempMemoryLen;
        private final VirtualMemory tempVirtualMem;
        private Path tablePath;
        private long metadataFd;

        public MigrationContext(long mem, int tempMemSize, VirtualMemory tempVirtualMem) {
            this.tempMemory = mem;
            this.tempMemoryLen = tempMemSize;
            this.tempVirtualMem = tempVirtualMem;
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
        setByVersion(VERSION_THAT_ADDED_TABLE_ID, MigrationActions::assignTableId);
        setByVersion(VERSION_TX_STRUCT_UPDATE_1, MigrationActions::rebuildTransactionFile);
    }
}
