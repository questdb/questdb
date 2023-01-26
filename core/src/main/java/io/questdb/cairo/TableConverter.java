/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.TableRecordMetadata;
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

class TableConverter {
    private static final Log LOG = LogFactory.getLog(TableConverter.class);

    static void convertTables(CairoConfiguration configuration, TableSequencerAPI tableSequencerAPI) {
        final Path path = Path.getThreadLocal(configuration.getRoot());
        final int rootLen = path.length();
        final StringSink sink = Misc.getThreadLocalBuilder();
        final FilesFacade ff = configuration.getFilesFacade();
        final long findPtr = ff.findFirst(path.$());
        do {
            if (ff.isDirOrSoftLinkDirNoDots(path, rootLen, ff.findName(findPtr), ff.findType(findPtr), sink)) {
                if (!ff.exists(path.concat(WalUtils.CONVERT_FILE_NAME).$())) {
                    continue;
                }
                final String dirName = sink.toString();
                final boolean walEnabled = readWalEnabled(path, ff);
                LOG.info().$("Converting table [dirName=").utf8(dirName).$(", walEnabled=").$(walEnabled).I$();

                path.trimTo(rootLen).concat(dirName);
                try (final MemoryMARW metaMem = Vm.getMARWInstance()) {
                    openSmallFile(ff, path, rootLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER_METADATA);
                    if (metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED) == walEnabled) {
                        LOG.info().$("Skipping conversion, table already has the expected type [dirName=").utf8(dirName).$(", walEnabled=").$(walEnabled).I$();
                    } else {
                        if (walEnabled) {
                            final String tableName;
                            try (final MemoryCMR mem = Vm.getCMRInstance()) {
                                final String name = TableUtils.readTableName(path, mem, ff);
                                tableName = name != null ? name : dirName;
                            }

                            final int tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
                            final TableToken token = new TableToken(tableName, dirName, tableId, true);
                            try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, token)) {
                                metadata.load();
                                tableSequencerAPI.registerTable(tableId, new TableDescriptorImpl(metadata), token);
                            }
                        }
                        metaMem.putBool(TableUtils.META_OFFSET_WAL_ENABLED, walEnabled);
                    }

                    path.trimTo(rootLen).concat(dirName).concat(CONVERT_FILE_NAME);
                    if (!ff.remove(path)) {
                        LOG.error().$("Could not remove _convert file [path=").utf8(path).I$();
                    }
                }
            }
        } while (ff.findNext(findPtr) > 0);
        ff.findClose(findPtr);
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

    private static class TableDescriptorImpl implements TableDescriptor {
        private final int timestampIndex;
        private final ObjList<CharSequence> columnNames = new ObjList<>();
        private final IntList columnTypes = new IntList();

        private TableDescriptorImpl(TableRecordMetadata metadata) {
            timestampIndex = metadata.getTimestampIndex();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                columnNames.add(metadata.getColumnName(i));
                columnTypes.add(metadata.getColumnType(i));
            }
        }

        @Override
        public int getColumnCount() {
            return columnNames.size();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return columnNames.get(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            return columnTypes.get(columnIndex);
        }

        @Override
        public int getTimestampIndex() {
            return timestampIndex;
        }
    }
}
