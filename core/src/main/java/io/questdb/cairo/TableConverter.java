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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.CONVERT_FILE_NAME;

public class TableConverter {
    private static final Log LOG = LogFactory.getLog(TableConverter.class);

    public static ObjList<TableToken> convertTables(
            CairoEngine engine,
            TableSequencerAPI tableSequencerAPI,
            TableFlagResolver tableFlagResolver,
            TableNameRegistry tableNameRegistry
    ) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final ObjList<TableToken> convertedTables = new ObjList<>();
        if (!configuration.isTableTypeConversionEnabled()) {
            LOG.info().$("table type conversion is disabled").$();
            return null;
        }
        if (configuration.isReadOnlyInstance()) {
            LOG.info().$("read only instance is not allowed to perform table type conversion").$();
            return null;
        }

        final Path path = Path.getThreadLocal(configuration.getDbRoot());
        final int rootLen = path.size();
        final Utf8StringSink dirNameSink = Misc.getThreadLocalUtf8Sink();
        final FilesFacade ff = configuration.getFilesFacade();
        final long findPtr = ff.findFirst(path.$());
        TxWriter txWriter = null;
        try {
            do {
                if (ff.isDirOrSoftLinkDirNoDots(path, rootLen, ff.findName(findPtr), ff.findType(findPtr), dirNameSink)) {
                    if (!ff.exists(path.concat(WalUtils.CONVERT_FILE_NAME).$())) {
                        continue;
                    }
                    try {
                        final boolean walEnabled = readWalEnabled(path.$(), ff);
                        LOG.info().$("converting table [dirName=").$(dirNameSink)
                                .$(", walEnabled=").$(walEnabled)
                                .I$();

                        path.trimTo(rootLen).concat(dirNameSink);
                        try (final MemoryMARW metaMem = Vm.getCMARWInstance()) {
                            openSmallFile(ff, path, rootLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER_METADATA);
                            final String dirName = dirNameSink.toString();
                            TableToken existingToken = tableNameRegistry.getTableTokenByDirName(dirName);

                            if (metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED) == walEnabled && existingToken != null && existingToken.isWal() == walEnabled) {
                                LOG.info().$("skipping conversion, table already has the expected type [dirName=").$(dirNameSink)
                                        .$(", walEnabled=").$(walEnabled)
                                        .I$();
                            } else {
                                final String tableName;
                                try (final MemoryCMR mem = Vm.getCMRInstance()) {
                                    final String name = TableUtils.readTableName(path.of(configuration.getDbRoot()).concat(dirNameSink), rootLen, mem, ff);
                                    tableName = name != null ? name : dirName;
                                }

                                final int tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
                                boolean isProtected = tableFlagResolver.isProtected(tableName);
                                boolean isSystem = tableFlagResolver.isSystem(tableName);
                                boolean isPublic = tableFlagResolver.isPublic(tableName);
                                boolean isMatView = isMatViewDefinitionFileExists(configuration, path, dirName);
                                final TableToken token = new TableToken(tableName, dirName, engine.getConfiguration().getDbLogName(), tableId, isMatView, walEnabled, isSystem, isProtected, isPublic);

                                if (txWriter == null) {
                                    txWriter = new TxWriter(ff, configuration);
                                }
                                txWriter.ofRW(path.trimTo(rootLen).concat(dirNameSink).concat(TXN_FILE_NAME).$(), ColumnType.UNDEFINED, PartitionBy.DAY);
                                txWriter.resetLagValuesUnsafe();

                                if (walEnabled) {
                                    try (TableWriterMetadata metadata = new TableWriterMetadata(token)) {
                                        metadata.reload(metaMem);
                                        tableSequencerAPI.registerTable(tableId, metadata, token);
                                    }

                                    // Reset structure version in _meta and _txn files
                                    metaMem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, 0);
                                    path.trimTo(rootLen).concat(dirNameSink);
                                    txWriter.resetStructureVersionUnsafe();
                                } else {
                                    if (!tableNameRegistry.isWalTableDropped(dirName) && tableSequencerAPI.prepareToConvertToNonWal(token)) {
                                        removeWalPersistence(path, rootLen, ff, dirNameSink);
                                    } else {
                                        LOG.info().$("WAL table will not be converted to non-WAL, table is dropped [dirName=").$(dirNameSink).I$();
                                        continue;
                                    }
                                }
                                metaMem.putBool(TableUtils.META_OFFSET_WAL_ENABLED, walEnabled);
                                convertedTables.add(token);

                                try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                                    metadataRW.hydrateTable(token);
                                }
                            }

                            path.trimTo(rootLen).concat(dirNameSink).concat(CONVERT_FILE_NAME);
                            if (!ff.removeQuiet(path.$())) {
                                LOG.critical().$("could not remove _convert file [path=").$(path).I$();
                            }
                        }
                    } catch (Exception e) {
                        LOG.error().$("table conversion failed [path=").$(path).$(", e=").$(e).I$();
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

    private static boolean readWalEnabled(LPSZ path, FilesFacade ff) {
        long fd = -1;
        try {
            fd = ff.openRO(path);
            if (fd < 1) {
                throw CairoException.critical(ff.errno()).put("could not open file [path=").put(path).put(']');
            }

            final byte walType = ff.readNonNegativeByte(fd, 0);
            switch (walType) {
                case TABLE_TYPE_WAL:
                    // fall through
                case TABLE_TYPE_MAT:
                    return true;
                case TABLE_TYPE_NON_WAL:
                    return false;
                default:
                    throw CairoException.critical(ff.errno()).put("could not read walType from file [path=").put(path).put(']');
            }
        } finally {
            ff.close(fd);
        }
    }

    private static void removeWalPersistence(Path path, int rootLen, FilesFacade ff, Utf8Sequence dirName) {
        path.trimTo(rootLen).concat(dirName).concat(WalUtils.SEQ_DIR).$();
        if (!ff.rmdir(path)) {
            LOG.error()
                    .$("could not remove sequencer dir [errno=").$(ff.errno())
                    .$(", path=").$(path)
                    .I$();
        }

        path.trimTo(rootLen).concat(dirName);
        int plen = path.size();
        final long pFind = ff.findFirst(path.$());
        if (pFind > 0) {
            try {
                do {
                    long name = ff.findName(pFind);
                    int type = ff.findType(pFind);
                    if (CairoKeywords.isWal(name)) {
                        path.trimTo(plen).concat(name);
                        if (type == Files.DT_FILE && CairoKeywords.isLock(name)) {
                            if (!ff.removeQuiet(path.$())) {
                                LOG.error()
                                        .$("could not remove wal lock file [errno=").$(ff.errno())
                                        .$(", path=").$(path)
                                        .I$();
                            }
                        } else {
                            if (!ff.rmdir(path)) {
                                LOG.error()
                                        .$("could not remove wal dir [errno=").$(ff.errno())
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
