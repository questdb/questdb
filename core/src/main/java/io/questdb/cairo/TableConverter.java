/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.CONVERT_FILE_NAME;

public class TableConverter {
    private static final Log LOG = LogFactory.getLog(TableConverter.class);

    public static ObjList<TableToken> convertTables(CairoConfiguration configuration, TableSequencerAPI tableSequencerAPI) {
        final ObjList<TableToken> convertedTables = new ObjList<>();
        if (!configuration.isTableTypeConversionEnabled()) {
            LOG.info().$("Table type conversion is disabled").$();
            return null;
        }
        if (configuration.isReadOnlyInstance()) {
            LOG.info().$("Read only instance is not allowed to perform table type conversion").$();
            return null;
        }

        final Path path = Path.getThreadLocal(configuration.getRoot());
        final int rootLen = path.length();
        final StringSink sink = Misc.getThreadLocalBuilder();
        final FilesFacade ff = configuration.getFilesFacade();
        final long findPtr = ff.findFirst(path.$());
        TxWriter txWriter = null;
        try {
            do {
                if (ff.isDirOrSoftLinkDirNoDots(path, rootLen, ff.findName(findPtr), ff.findType(findPtr), sink)) {
                    if (!ff.exists(path.concat(WalUtils.CONVERT_FILE_NAME).$())) {
                        continue;
                    }
                    try {
                        final String dirName = sink.toString();
                        final boolean walEnabled = readWalEnabled(path, ff);
                        LOG.info().$("Converting table [dirName=").utf8(dirName).$(", walEnabled=").$(walEnabled).I$();

                        path.trimTo(rootLen).concat(dirName);
                        try (final MemoryMARW metaMem = Vm.getMARWInstance()) {
                            openSmallFile(ff, path, rootLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER_METADATA);
                            if (metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED) == walEnabled) {
                                LOG.info().$("Skipping conversion, table already has the expected type [dirName=").utf8(dirName).$(", walEnabled=").$(walEnabled).I$();
                            } else {
                                final String tableName;
                                try (final MemoryCMR mem = Vm.getCMRInstance()) {
                                    final String name = TableUtils.readTableName(path.of(configuration.getRoot()).concat(dirName), rootLen, mem, ff);
                                    tableName = name != null ? name : dirName;
                                }

                                final int tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
                                final TableToken token = new TableToken(tableName, dirName, tableId, walEnabled);

                                if (txWriter == null) {
                                    txWriter = new TxWriter(ff, configuration);
                                }
                                txWriter.ofRW(path.trimTo(rootLen).concat(dirName).concat(TXN_FILE_NAME).$(), PartitionBy.DAY);
                                txWriter.resetLagValuesUnsafe();

                                if (walEnabled) {
                                    try (TableWriterMetadata metadata = new TableWriterMetadata(token, metaMem)) {
                                        tableSequencerAPI.registerTable(tableId, metadata, token);
                                    }

                                    // Reset structure versoin in _meta and _txn files
                                    metaMem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, 0);
                                    path.trimTo(rootLen).concat(dirName);
                                    txWriter.resetStructureVersionUnsafe();
                                } else {
                                    tableSequencerAPI.deregisterTable(token);
                                    removeWalPersistence(path, rootLen, ff, dirName);
                                }
                                metaMem.putBool(TableUtils.META_OFFSET_WAL_ENABLED, walEnabled);
                                convertedTables.add(token);
                            }

                            path.trimTo(rootLen).concat(dirName).concat(CONVERT_FILE_NAME).$();
                            if (!ff.remove(path)) {
                                LOG.error().$("Could not remove _convert file [path=").utf8(path).I$();
                            }
                        }
                    } catch (Exception e) {
                        LOG.error().$("Table conversion failed [path=").utf8(path).$(", e=").$(e).I$();
                    } finally {
                        if (txWriter != null) {
                            txWriter.close();
                        }
                    }
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
        }
        return convertedTables;
    }

    private static boolean readWalEnabled(Path path, FilesFacade ff) {
        int fd = -1;
        try {
            fd = ff.openRO(path);
            if (fd < 1) {
                throw CairoException.critical(ff.errno()).put("Could not open file [path=").put(path).put(']');
            }

            final byte walType = ff.readNonNegativeByte(fd, 0);
            switch (walType) {
                case TABLE_TYPE_WAL:
                    return true;
                case TABLE_TYPE_NON_WAL:
                    return false;
                default:
                    throw CairoException.critical(ff.errno()).put("Could not read walType from file [path=").put(path).put(']');
            }
        } finally {
            ff.close(fd);
        }
    }

    private static void removeWalPersistence(Path path, int rootLen, FilesFacade ff, String dirName) {
        path.trimTo(rootLen).concat(dirName).concat(WalUtils.SEQ_DIR).$();
        if (ff.rmdir(path) != 0) {
            LOG.error()
                    .$("Could not remove sequencer dir [errno=").$(ff.errno())
                    .$(", path=").$(path)
                    .I$();
        }

        path.trimTo(rootLen).concat(dirName).$();
        int plen = path.length();
        final long pFind = ff.findFirst(path);
        if (pFind > 0) {
            try {
                do {
                    long name = ff.findName(pFind);
                    int type = ff.findType(pFind);
                    if (CairoKeywords.isWal(name)) {
                        path.trimTo(plen).concat(name).$();
                        if (type == Files.DT_FILE && CairoKeywords.isLock(name)) {
                            if (!ff.remove(path)) {
                                LOG.error()
                                        .$("Could not remove wal lock file [errno=").$(ff.errno())
                                        .$(", path=").$(path)
                                        .I$();
                            }
                        } else {
                            if (ff.rmdir(path) != 0) {
                                LOG.error()
                                        .$("Could not remove wal dir [errno=").$(ff.errno())
                                        .$(", path=").$(path)
                                        .I$();
                            }
                        }
                    }
                } while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
            }
        }
    }
}
