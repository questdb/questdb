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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Map;

import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;
import static io.questdb.std.Files.DT_FILE;

public class TableNameRegistryFileStore implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableNameRegistryFileStore.class);
    private static final int OPERATION_ADD = 0;
    private static final int OPERATION_REMOVE = -1;
    private final static long TABLE_NAME_ENTRY_RESERVED_LONGS = 8;
    private static final int TABLE_TYPE_NON_WAL = 0;
    private static final int TABLE_TYPE_WAL = 1;
    private final CairoConfiguration configuration;
    private final StringSink nameSink = new StringSink();
    private final IntHashSet tableIds = new IntHashSet();
    private final MemoryMARW tableNameMemory = Vm.getCMARWInstance();
    private final MemoryCMR tableNameRoMemory = Vm.getCMRInstance();
    private long lockFd = -1;

    public TableNameRegistryFileStore(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    public synchronized void appendEntry(final TableToken tableToken) {
        writeEntry(tableToken, OPERATION_ADD);
    }

    @Override
    public void close() {
        if (lockFd != -1) {
            configuration.getFilesFacade().close(lockFd);
            lockFd = -1;
        }
        tableNameMemory.close(false);
    }

    public boolean lock() {
        if (lockFd != -1) {
            throw CairoException.critical(0).put("table registry already locked");
        }

        // Windows does not allow to lock directories, so we lock a special lock file
        FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).put(".lock").$();
        if (ff.exists(path)) {
            ff.touch(path);
        }
        lockFd = TableUtils.lock(ff, path);
        return lockFd != -1;
    }

    public synchronized void logDropTable(final TableToken tableToken) {
        writeEntry(tableToken, OPERATION_REMOVE);
    }

    @TestOnly
    public synchronized void resetMemory() {
        if (!isLocked()) {
            if (!lock()) {
                throw CairoException.critical(0).put("table registry is not locked");
            }
        }
        tableNameMemory.close();

        final Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).put(".0").$();
        configuration.getFilesFacade().remove(path);

        tableNameMemory.smallFile(configuration.getFilesFacade(), path, MemoryTag.MMAP_DEFAULT);
    }

    private void compactTableNameFile(Map<CharSequence, TableToken> nameTableTokenMap, int lastFileVersion, FilesFacade ff, Path path, long currentOffset) {
        // compact the memory, remove deleted entries.
        // write to the tmp file.
        int pathRootLen = path.length();
        path.concat(TABLE_REGISTRY_NAME_FILE).put(".tmp").$();

        try {
            tableNameMemory.close(false);
            tableNameMemory.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            tableNameMemory.putLong(0L);

            for (TableToken token : nameTableTokenMap.values()) {
                writeEntry(token, OPERATION_ADD);
            }
            tableNameMemory.sync(false);
            long newAppendOffset = tableNameMemory.getAppendOffset();
            tableNameMemory.close();

            // rename tmp to next version file, everyone will automatically switch to new file
            Path path2 = Path.getThreadLocal2(configuration.getRoot())
                    .concat(TABLE_REGISTRY_NAME_FILE).put('.').put(lastFileVersion + 1).$();
            if (ff.rename(path, path2) == Files.FILES_RENAME_OK) {
                LOG.info().$("compacted tables file [path=").$(path2).$(']').$();
                lastFileVersion++;
                currentOffset = newAppendOffset;
                // best effort to remove old files, but we don't care if it fails
                path.trimTo(pathRootLen).concat(TABLE_REGISTRY_NAME_FILE).put('.').put(lastFileVersion - 1).$();
                ff.remove(path);
            } else {
                // Not critical, if rename fails, compaction will be done next time
                LOG.error().$("could not rename tables file, tables file will not be compacted [from=").$(path)
                        .$(", to=").$(path2).$(']').$();
            }
        } finally {
            path.trimTo(pathRootLen).concat(TABLE_REGISTRY_NAME_FILE).put('.').put(lastFileVersion).$();
            tableNameMemory.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            tableNameMemory.jumpTo(currentOffset);
        }
        tableNameMemory.jumpTo(currentOffset);
    }

    private int findLastTablesFileVersion(FilesFacade ff, Path path) {
        long findPtr = ff.findFirst(path.$());
        if (findPtr == 0) {
            throw CairoException.critical(0).put("database root directory does not exist at ").put(path);
        }
        try {
            int lastVersion = 0;
            do {
                long pUtf8NameZ = ff.findName(findPtr);
                if (ff.findType(findPtr) == DT_FILE) {
                    nameSink.clear();
                    Chars.utf8DecodeZ(pUtf8NameZ, nameSink);
                    if (Chars.startsWith(nameSink, TABLE_REGISTRY_NAME_FILE) && nameSink.length() > TABLE_REGISTRY_NAME_FILE.length() + 1) {
                        //noinspection CatchMayIgnoreException
                        try {
                            int version = Numbers.parseInt(nameSink, TABLE_REGISTRY_NAME_FILE.length() + 1, nameSink.length());
                            if (version > lastVersion) {
                                lastVersion = version;
                            }
                        } catch (NumericException e) {
                        }
                    }
                }
            } while (ff.findNext(findPtr) > 0);
            return lastVersion;
        } finally {
            ff.findClose(findPtr);
        }
    }

    private boolean isLocked() {
        return lockFd != -1;
    }

    private int readTableId(Path path, String dirName, FilesFacade ff) {
        path.of(configuration.getRoot()).concat(dirName).concat(TableUtils.META_FILE_NAME).$();
        long fd = ff.openRO(path);
        if (fd < 1) {
            return 0;
        }
        try {

            int tableId = ff.readNonNegativeInt(fd, TableUtils.META_OFFSET_TABLE_ID);
            if (tableId < 0) {
                LOG.error().$("cannot read table id from metadata file [path=").$(path).I$();
                return 0;
            }
            byte isWal = (byte) (ff.readNonNegativeInt(fd, TableUtils.META_OFFSET_WAL_ENABLED) >>> 24);
            return isWal == 0 ? tableId : -tableId;
        } finally {
            ff.close(fd);
        }
    }

    private void reloadFromRootDirectory(
            Map<CharSequence, TableToken> nameTableTokenMap,
            Map<CharSequence, ReverseTableMapItem> reverseTableNameTokenMap
    ) {
        Path path = Path.getThreadLocal(configuration.getRoot());
        FilesFacade ff = configuration.getFilesFacade();
        long findPtr = ff.findFirst(path.$());
        StringSink sink = Misc.getThreadLocalBuilder();

        do {
            long fileName = ff.findName(findPtr);
            if (Files.isDir(fileName, ff.findType(findPtr), sink)) {
                if (nameTableTokenMap.get(sink) == null
                        && TableUtils.exists(ff, path, configuration.getRoot(), sink) == TableUtils.TABLE_EXISTS) {

                    String dirName = sink.toString();
                    String tableName = TableUtils.getTableNameFromDirName(dirName);
                    int tableId = readTableId(path, dirName, configuration.getFilesFacade());
                    boolean isWal = tableId < 0;
                    tableId = Math.abs(tableId);

                    if (tableName != null && tableId != 0) {
                        if (tableIds.contains(tableId)) {
                            LOG.critical().$("duplicate table id found [dirName=").$(dirName).$(", id=").$(tableId).$(']').$();
                            continue;
                        }

                        TableToken token = new TableToken(tableName, dirName, tableId, isWal);
                        nameTableTokenMap.put(tableName, token);
                        reverseTableNameTokenMap.put(dirName, ReverseTableMapItem.of(token));
                    }
                }
            }
        } while (ff.findNext(findPtr) > 0);
        ff.findClose(findPtr);
    }

    private void reloadFromTablesFile(
            Map<CharSequence, TableToken> nameTableTokenMap,
            Map<CharSequence, ReverseTableMapItem> reverseTableNameTokenMap
    ) {
        int lastFileVersion;
        FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.getThreadLocal(configuration.getRoot());
        path.of(configuration.getRoot());
        int pathRootLen = path.length();

        MemoryMR memory = isLocked() ? tableNameMemory : tableNameRoMemory;
        do {
            lastFileVersion = findLastTablesFileVersion(ff, path.trimTo(pathRootLen).$());
            path.trimTo(pathRootLen).concat(TABLE_REGISTRY_NAME_FILE).put('.').put(lastFileVersion).$();
            try {
                memory.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                LOG.info().$("reloading tables file [path=").utf8(path).I$();
                if (memory.size() >= 2 * Long.BYTES) {
                    break;
                }
            } catch (CairoException e) {
                if (!isLocked()) {
                    if (e.errnoPathDoesNotExist()) {
                        if (lastFileVersion == 0) {
                            // This is RO mode and file and tables.d.0 does not exist.
                            return;
                        } else {
                            // This is RO mode and file we want to read was just swapped to new one by the RW instance.
                            continue;
                        }
                    }
                }
                throw e;
            }
        } while (true);

        long mapMem = memory.getLong(0);
        long currentOffset = Long.BYTES;
        memory.extend(mapMem);

        int deletedRecordsFound = 0;
        while (currentOffset < mapMem) {
            int operation = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;
            String tableName = Chars.toString(memory.getStr(currentOffset));
            currentOffset += Vm.getStorageLength(tableName);
            String dirName = Chars.toString(memory.getStr(currentOffset));
            currentOffset += Vm.getStorageLength(dirName);
            int tableId = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;
            int tableType = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;

            TableToken token = new TableToken(tableName, dirName, tableId, tableType == TABLE_TYPE_WAL);
            if (operation == OPERATION_REMOVE) {
                // remove from registry
                TableToken tableToken = nameTableTokenMap.remove(tableName);
                if (tableToken != null) {
                    reverseTableNameTokenMap.put(dirName, ReverseTableMapItem.ofDropped(tableToken));
                    deletedRecordsFound++;
                }
            } else {
                if (tableIds.contains(tableId)) {
                    LOG.critical().$("duplicate table id found, table will not be accessible " +
                            "[dirName=").$(dirName).$(", id=").$(tableId).$(']').$();
                    continue;
                }

                nameTableTokenMap.put(tableName, token);
                if (!Chars.startsWith(token.getDirName(), token.getTableName())) {
                    // This table is renamed, log system to real table name mapping
                    LOG.advisory().$("renamed WAL table system name [table=").utf8(tableName).$(", dirName=").utf8(dirName).$();
                }

                reverseTableNameTokenMap.put(token.getDirName(), ReverseTableMapItem.of(token));
                currentOffset += TABLE_NAME_ENTRY_RESERVED_LONGS * Long.BYTES;
            }
        }

        if (isLocked()) {
            if (deletedRecordsFound > 0) {
                LOG.info().$("compacting tables file [path=").$(path).$(']').$();
                path.trimTo(pathRootLen);
                compactTableNameFile(nameTableTokenMap, lastFileVersion, ff, path, currentOffset);
            } else {
                tableNameMemory.jumpTo(currentOffset);
            }
        } else {
            tableNameRoMemory.close();
        }
    }

    private void writeEntry(TableToken tableToken, int operation) {
        if (!isLocked()) {
            throw CairoException.critical(0).put("table registry is not locked");
        }
        tableNameMemory.putInt(operation);
        tableNameMemory.putStr(tableToken.getTableName());
        tableNameMemory.putStr(tableToken.getDirName());
        tableNameMemory.putInt(tableToken.getTableId());
        tableNameMemory.putInt(tableToken.isWal() ? TableNameRegistryFileStore.TABLE_TYPE_WAL : TABLE_TYPE_NON_WAL);

        if (operation != OPERATION_REMOVE) {
            for (int i = 0; i < TABLE_NAME_ENTRY_RESERVED_LONGS; i++) {
                tableNameMemory.putLong(0);
            }
        }
        tableNameMemory.putLong(0L, tableNameMemory.getAppendOffset());
    }

    void reload(
            Map<CharSequence, TableToken> nameTableTokenMap,
            Map<CharSequence, ReverseTableMapItem> reverseTableNameTokenMap
    ) {
        tableIds.clear();
        reloadFromTablesFile(nameTableTokenMap, reverseTableNameTokenMap);
        reloadFromRootDirectory(nameTableTokenMap, reverseTableNameTokenMap);
    }
}
