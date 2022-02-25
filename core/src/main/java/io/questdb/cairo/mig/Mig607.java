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

package io.questdb.cairo.mig;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;

final class Mig607 {
    private static final String TXN_FILE_NAME = "_txn";
    private static final String META_FILE_NAME = "_meta";
    private static final String DEFAULT_PARTITION_NAME = "default";

    private static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    private static final int LONGS_PER_TX_ATTACHED_PARTITION = 4;
    private static final Log LOG = LogFactory.getLog(Mig607.class);

    public static void migrate(
            FilesFacade ff,
            Path path,
            MigrationContext migrationContext,
            MemoryMARW metaMem,
            int columnCount,
            long rowCount,
            long columnNameOffset
    ) {
        final int plen2 = path.length();
        if (rowCount > 0) {
            long mem = migrationContext.getTempMemory();
            long currentColumnNameOffset = columnNameOffset;
            for (int i = 0; i < columnCount; i++) {
                final int columnType = ColumnType.tagOf(
                        metaMem.getInt(
                                MigrationActions.prefixedBlockOffset(MigrationActions.META_OFFSET_COLUMN_TYPES_606, i, MigrationActions.META_COLUMN_DATA_SIZE_606)
                        )
                );
                final CharSequence columnName = metaMem.getStr(currentColumnNameOffset);
                currentColumnNameOffset += Vm.getStorageLength(columnName);
                if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
                    final long columnTop = readColumnTop(
                            ff,
                            path.trimTo(plen2),
                            columnName,
                            plen2,
                            false
                    );
                    final long columnRowCount = rowCount - columnTop;
                    long offset = columnRowCount * 8L;
                    iFile(path.trimTo(plen2), columnName);
                    long fd = TableUtils.openRW(ff, path, MigrationActions.LOG);
                    try {
                        long fileLen = ff.length(fd);

                        if (fileLen < offset) {
                            throw CairoException.instance(0).put("file is too short [path=").put(path).put("]");
                        }

                        TableUtils.allocateDiskSpace(ff, fd, offset + 8);
                        long dataOffset = TableUtils.readLongOrFail(ff, fd, offset - 8L, mem, path);
                        dFile(path.trimTo(plen2), columnName);
                        final long fd2 = TableUtils.openRO(ff, path, MigrationActions.LOG);
                        try {
                            if (columnType == ColumnType.BINARY) {
                                long len = TableUtils.readLongOrFail(ff, fd2, dataOffset, mem, path);
                                if (len == -1) {
                                    dataOffset += 8;
                                } else {
                                    dataOffset += 8 + len;
                                }
                            } else {
                                long len = TableUtils.readIntOrFail(ff, fd2, dataOffset, mem, path);
                                if (len == -1) {
                                    dataOffset += 4;
                                } else {
                                    dataOffset += MigrationActions.prefixedBlockOffset(4, 2, len);
                                }

                            }
                        } finally {
                            ff.close(fd2);
                        }
                        TableUtils.writeLongOrFail(ff, fd, offset, dataOffset, mem, path);
                    } finally {
                        Vm.bestEffortClose(ff, MigrationActions.LOG, fd, true, offset + 8);
                    }
                }
            }
        }
    }

    /**
     * Reads 8 bytes from "top" file. Moved from TableUtils when refactored column tops into column version file
     *
     * @param ff                 files facade, - intermediary to intercept OS file system calls.
     * @param path               path has to be set to location of "top" file, excluding file name. Zero terminated string.
     * @param name               name of top file
     * @param plen               path length to truncate "path" back to, path is reusable.
     * @param failIfCouldNotRead if true the method will throw exception if top file cannot be read. Otherwise, 0.
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    public static long readColumnTop(FilesFacade ff, Path path, CharSequence name, int plen, boolean failIfCouldNotRead) {
        try {
            if (ff.exists(topFile(path.chop$(), name))) {
                final long fd = TableUtils.openRO(ff, path, LOG);
                try {
                    long n;
                    if ((n = ff.readULong(fd, 0)) < 0) {
                        if (failIfCouldNotRead) {
                            throw CairoException.instance(Os.errno())
                                    .put("could not read top of column [file=").put(path)
                                    .put(", read=").put(n).put(']');
                        } else {
                            LOG.error().$("could not read top of column [file=").$(path)
                                    .$(", read=").$(n)
                                    .$(", errno=").$(ff.errno())
                                    .I$();
                            return 0L;
                        }
                    }
                    return n;
                } finally {
                    ff.close(fd);
                }
            }
            return 0L;
        } finally {
            path.trimTo(plen);
        }
    }

    public static void txnPartition(CharSink path, long txn) {
        path.put('.').put(txn);
    }

    private static void dFile(Path path, CharSequence columnName) {
        path.concat(columnName).put(FILE_SUFFIX_D);
        path.$();
    }

    static LPSZ topFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".top").$();
    }

    private static void trimFile(FilesFacade ff, Path path, long size) {
        long fd = TableUtils.openFileRWOrFail(ff, path);
        if (!ff.truncate(fd, size)) {
            // This should never happens on migration but better to be on safe side anyway
            throw CairoException.instance(ff.errno()).put("Cannot trim to size [file=").put(path).put(']');
        }
        if (!ff.close(fd)) {
            // This should never happens on migration but better to be on safe side anyway
            throw CairoException.instance(ff.errno()).put("Cannot close [file=").put(path).put(']');
        }
    }

        private static void trimFile(FilesFacade ff, Path path, long size, long opts) {
        long fd = TableUtils.openFileRWOrFail(ff, path, opts);
        if (!ff.truncate(fd, size)) {
            // This should never happens on migration but better to be on safe side anyway
            throw CairoException.instance(ff.errno()).put("Cannot trim to size [file=").put(path).put(']');
        }
        if (!ff.close(fd)) {
            // This should never happens on migration but better to be on safe side anyway
            throw CairoException.instance(ff.errno()).put("Cannot close [file=").put(path).put(']');
        }
    }

    public static void migrate(
            FilesFacade ff,
            Path path,
            MigrationContext migrationContext,
            MemoryMARW metaMem,
            int columnCount,
            long rowCount,
            long columnNameOffset
    ) {
        final int plen2 = path.length();
        if (rowCount > 0) {
            long mem = migrationContext.getTempMemory();
            long currentColumnNameOffset = columnNameOffset;
            for (int i = 0; i < columnCount; i++) {
                final int columnType = ColumnType.tagOf(
                        metaMem.getInt(
                                MigrationActions.prefixedBlockOffset(MigrationActions.META_OFFSET_COLUMN_TYPES_606, i, MigrationActions.META_COLUMN_DATA_SIZE_606)
                        )
                );
                final CharSequence columnName = metaMem.getStr(currentColumnNameOffset);
                currentColumnNameOffset += Vm.getStorageLength(columnName);
                if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
                    final long columnTop = readColumnTop(
                            ff,
                            path.trimTo(plen2),
                            columnName,
                            plen2,
                            false
                    );
                    final long columnRowCount = rowCount - columnTop;
                    long offset = columnRowCount * 8L;
                    iFile(path.trimTo(plen2), columnName);
                    long fd = TableUtils.openRW(ff, path, MigrationActions.LOG, migrationContext.getConfiguration().getWriterFileOpenOpts());
                    try {
                        long fileLen = ff.length(fd);

                        if (fileLen < offset) {
                            throw CairoException.instance(0).put("file is too short [path=").put(path).put("]");
                        }

                        TableUtils.allocateDiskSpace(ff, fd, offset + 8);
                        long dataOffset = TableUtils.readLongOrFail(ff, fd, offset - 8L, mem, path);
                        dFile(path.trimTo(plen2), columnName);
                        final long fd2 = TableUtils.openRO(ff, path, MigrationActions.LOG);
                        try {
                            if (columnType == ColumnType.BINARY) {
                                long len = TableUtils.readLongOrFail(ff, fd2, dataOffset, mem, path);
                                if (len == -1) {
                                    dataOffset += 8;
                                } else {
                                    dataOffset += 8 + len;
                                }
                            } else {
                                long len = TableUtils.readIntOrFail(ff, fd2, dataOffset, mem, path);
                                if (len == -1) {
                                    dataOffset += 4;
                                } else {
                                    dataOffset += MigrationActions.prefixedBlockOffset(4, 2, len);
                                }

                            }
                        } finally {
                            ff.close(fd2);
                        }
                        TableUtils.writeLongOrFail(ff, fd, offset, dataOffset, mem, path);
                    } finally {
                        Vm.bestEffortClose(ff, MigrationActions.LOG, fd, true, offset + 8);
                    }
                }
            }
        }
    }

    private static void offsetFileName(Path path, CharSequence columnName) {
        path.concat(columnName).put(".o").$();
    }

    private static void charFileName(Path path, CharSequence columnName) {
        path.concat(columnName).put(".c").$();
    }

    private static void iFile(Path path, CharSequence columnName) {
        path.concat(columnName).put(FILE_SUFFIX_I).$();
    }
}
