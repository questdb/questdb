///*******************************************************************************
// *     ___                  _   ____  ____
// *    / _ \ _   _  ___  ___| |_|  _ \| __ )
// *   | | | | | | |/ _ \/ __| __| | | |  _ \
// *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
// *    \__\_\\__,_|\___||___/\__|____/|____/
// *
// *  Copyright (c) 2014-2019 Appsicle
// *  Copyright (c) 2019-2020 QuestDB
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// ******************************************************************************/
//
//package io.questdb.cairo;
//
//import io.questdb.log.Log;
//import io.questdb.log.LogFactory;
//import io.questdb.std.*;
//import io.questdb.std.str.NativeLPSZ;
//import io.questdb.std.str.Path;
//
//import static io.questdb.cairo.ColumnType.VERSION_THAT_ADDED_TABLE_ID;
//
//public class TableMigration {
//    private static final Log LOG = LogFactory.getLog(TableMigration.class);
//    private final CairoEngine engine;
//    private final CairoConfiguration configuration;
//    private static final ObjList<MigrateVersionAction> MIGRATIONS = new ObjList<>();
//
//    public TableMigration(CairoEngine engine, CairoConfiguration configuration) {
//        this.engine = engine;
//        this.configuration = configuration;
//    }
//
//    static {
//        MIGRATIONS.extendAndSet(ColumnType.VERSION - 1, null);
//        MIGRATIONS.setQuick(VERSION_THAT_ADDED_TABLE_ID - 1, TableMigration::assignTableId);
//    }
//
//    public void migrateEngineTo(int latestVersion) {
//        final FilesFacade ff = configuration.getFilesFacade();
//        int tempMemSize = 8;
//        long mem = Unsafe.malloc(tempMemSize);
//
//        try {
//            try (Path path = new Path()) {
//                path.of(configuration.getRoot());
//                final int rootLen = path.length();
//
//                // check if all tables have been upgraded already
//                path.concat(TableUtils.UPGRADE_FILE_NAME).$();
//                final boolean existed = ff.exists(path);
//                long upgradeFd = TableUtils.openFileRWOrFail(ff, path);
//                LOG.debug()
//                        .$("open [fd=").$(upgradeFd)
//                        .$(", path=").$(path)
//                        .$(']').$();
//                if (existed) {
//                    long readLen = ff.read(upgradeFd, mem, Integer.BYTES, 0);
//                    if (readLen == Integer.BYTES) {
//                        if (Unsafe.getUnsafe().getInt(mem) >= VERSION_THAT_ADDED_TABLE_ID) {
//                            LOG.info().$("table IDs are up to date").$();
//                            ff.close(upgradeFd);
//                            upgradeFd = -1;
//                        }
//                    } else {
//                        ff.close(upgradeFd);
//                        throw CairoException.instance(ff.errno()).put("could not read [fd=").put(upgradeFd).put(", path=").put(path).put(']');
//                    }
//                }
//
//                if (upgradeFd != -1) {
//                    try {
//                        LOG.info().$("upgrading table IDs").$();
//                        final NativeLPSZ nativeLPSZ = new NativeLPSZ();
//                        ff.iterateDir(path.trimTo(rootLen).$(), (name, type) -> {
//                            if (type == Files.DT_DIR) {
//                                nativeLPSZ.of(name);
//                                if (Chars.notDots(nativeLPSZ)) {
//                                    final int plen = path.length();
//                                    path.chopZ().concat(nativeLPSZ).concat(TableUtils.META_FILE_NAME).$();
//                                    if (ff.exists(path)) {
//                                        assignTableId(ff, path, mem);
//                                    }
//                                    path.trimTo(plen);
//                                }
//                            }
//                        });
//                        LOG.info().$("upgraded table IDs").$();
//
//                        Unsafe.getUnsafe().putInt(mem, ColumnType.VERSION);
//                        long writeLen = ff.write(upgradeFd, mem, Integer.BYTES, 0);
//                        if (writeLen < Integer.BYTES) {
//                            throw CairoException.instance(ff.errno()).put("Could not write to [fd=").put(upgradeFd).put(']');
//                        }
//                    } finally {
//                        ff.close(upgradeFd);
//                    }
//                }
//            }
//        } finally {
//            Unsafe.free(mem, tempMemSize);
//        }
//    }
//
//    private static void assignTableId(MigrateContext migrateContext) {
//        long mem = migrateContext.getTemMemory(8);
//        FilesFacade ff = migrateContext.getFf();
//        Path path = migrateContext.getTablePath();
//
//        LOG.info().$("setting table id in [path=").$(path).$(']').$();
//        Unsafe.getUnsafe().putInt(mem, ColumnType.VERSION);
//        Unsafe.getUnsafe().putInt(mem + Integer.BYTES, migrateContext.getNextTableId());
//        if (ff.write(migrateContext.getMetadataFd(), mem, 8, TableUtils.META_OFFSET_VERSION) == 8) {
//            return;
//        }
//        throw CairoException.instance(ff.errno()).put("Could not update table id [path=").put(path).put(']');
//    }
//
//
//    private void upgradeTableVersions() {
//        final FilesFacade ff = configuration.getFilesFacade();
//        long mem = Unsafe.malloc(8);
//
//        try (Path path = new Path()) {
//            path.of(configuration.getRoot());
//            final int rootLen = path.length();
//
//            ff.iterateDir(path.trimTo(rootLen).$(), (name, type) -> {
//                if (type == Files.DT_DIR) {
//                    final NativeLPSZ nativeLPSZ = new NativeLPSZ();
//                    nativeLPSZ.of(name);
//                    if (Chars.notDots(nativeLPSZ)) {
//                        final int plen = path.length();
//                        path.chopZ().concat(nativeLPSZ).concat(TableUtils.META_FILE_NAME).$();
//                        if (ff.exists(path)) {
//
//                            final long fd = TableUtils.openFileRWOrFail(ff, path);
//                            if (ff.read(fd, mem, 8, TableUtils.META_OFFSET_VERSION) == 8) {
//                                int currentTableVersion = Unsafe.getUnsafe().getInt(mem);
//
//                                if (currentTableVersion < ColumnType.VERSION) {
//                                    LOG.info().$("upgrading [path=").$(path).$(",tableVersion=").$(currentTableVersion).$(']').$();
//                                    TODO
//
//                                    Unsafe.getUnsafe().putInt(mem + Integer.BYTES, (int) getNextTableId());
//                                    if (ff.write(fd, mem, 8, TableUtils.META_OFFSET_VERSION) == 8) {
//                                        ff.close(fd);
//                                        return;
//                                    }
//
//                                    Unsafe.getUnsafe().putInt(mem, ColumnType.VERSION);
//                                }
//                                ff.close(fd);
//                                return;
//                            }
//                            ff.close(fd);
//                            throw CairoException.instance(ff.errno()).put("Could not update table id [path=").put(path).put(']');
//                        }
//                        path.trimTo(plen);
//                    }
//                }
//            });
//            LOG.info().$("upgraded table Versions to ").$(ColumnType.VERSION).$();
//        } finally {
//            Unsafe.free(mem, 8);
//        }
//    }
//
//    class MigrateContext {
//        private Path tablePath;
//        private int metadataFd;
//        private long tempMemory;
//        private int tempMemoryLen;
//
//        public FilesFacade getFf() {
//            return configuration.getFilesFacade();
//        }
//
//        public int getMetadataFd() {
//            return metadataFd;
//        }
//
//        public int getNextTableId() {
//            return (int)engine.getNextTableId();
//        }
//
//        public Path getTablePath() {
//            return tablePath;
//        }
//
//        public long getTemMemory(int size) {
//            if (size <= tempMemoryLen) {
//                return tempMemory;
//            }
//            throw new UnsupportedOperationException("No temp memory of size "
//                    + size
//                    + " is allocate. Only "
//                    + tempMemoryLen
//                    + " is available");
//        }
//    }
//
//    @FunctionalInterface
//    interface MigrateVersionAction {
//        void migrate(MigrateContext context);
//    }
//}
