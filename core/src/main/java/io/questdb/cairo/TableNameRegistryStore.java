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
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Map;
import java.util.function.Predicate;

import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;
import static io.questdb.std.Files.DT_FILE;

public class TableNameRegistryStore extends GrowOnlyTableNameRegistryStore {
    private static final Log LOG = LogFactory.getLog(TableNameRegistryStore.class);
    private final CairoConfiguration configuration;
    private final StringSink nameSink = new StringSink();
    private final Predicate<CharSequence> protectedTableResolver;
    private final IntHashSet tableIds = new IntHashSet();
    private final MemoryCMR tableNameRoMemory = Vm.getCMRInstance();
    private int lockFd = -1;

    public TableNameRegistryStore(CairoConfiguration configuration, Predicate<CharSequence> protectedTableResolver) {
        super(configuration.getFilesFacade());
        this.configuration = configuration;
        this.protectedTableResolver = protectedTableResolver;
    }

    public static int findLastTablesFileVersion(FilesFacade ff, Path path, StringSink nameSink) {
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
                    boolean validUtf8 = Utf8s.utf8ToUtf16Z(pUtf8NameZ, nameSink);
                    assert validUtf8 : "invalid UTF-8 in file name";
                    if (Chars.startsWith(nameSink, TABLE_REGISTRY_NAME_FILE) && nameSink.length() > TABLE_REGISTRY_NAME_FILE.length() + 1) {
                        try {
                            int version = Numbers.parseInt(nameSink, TABLE_REGISTRY_NAME_FILE.length() + 1, nameSink.length());
                            if (version > lastVersion) {
                                lastVersion = version;
                            }
                        } catch (NumericException ignore) {
                            // no-op
                        }
                    }
                }
            } while (ff.findNext(findPtr) > 0);
            return lastVersion;
        } finally {
            ff.findClose(findPtr);
        }
    }

    @Override
    public void close() {
        super.close();
        if (lockFd != -1) {
            configuration.getFilesFacade().close(lockFd);
            lockFd = -1;
        }
    }

    public boolean isLocked() {
        return lockFd != -1;
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

    private void compactTableNameFile(
            Map<CharSequence, TableToken> nameTableTokenMap,
            Map<CharSequence, ReverseTableMapItem> reverseNameMap,
            int lastFileVersion,
            FilesFacade ff,
            Path path,
            long currentOffset
    ) {
        // compact the memory, remove deleted entries.
        // write to the tmp file.
        int pathRootLen = path.size();
        path.concat(TABLE_REGISTRY_NAME_FILE).putAscii(".tmp").$();

        try {
            tableNameMemory.close(false);
            tableNameMemory.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            tableNameMemory.putLong(0L);

            // Save tables not fully deleted yet to complete the deletion.
            for (ReverseTableMapItem reverseMapItem : reverseNameMap.values()) {
                if (reverseMapItem.isDropped()) {
                    writeEntry(reverseMapItem.getToken(), OPERATION_ADD);
                    writeEntry(reverseMapItem.getToken(), OPERATION_REMOVE);
                }
            }

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
                LOG.info().$("compacted tables file [path=").$(path2).I$();
                lastFileVersion++;
                currentOffset = newAppendOffset;
                // best effort to remove old files, but we don't care if it fails
                path.trimTo(pathRootLen).concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(lastFileVersion - 1).$();
                ff.remove(path);
            } else {
                // Not critical, if rename fails, compaction will be done next time
                LOG.error().$("could not rename tables file, tables file will not be compacted [from=").$(path)
                        .$(", to=").$(path2).I$();
            }
        } finally {
            path.trimTo(pathRootLen).concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(lastFileVersion).$();
            tableNameMemory.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            tableNameMemory.jumpTo(currentOffset);
        }
    }

    private int findLastTablesFileVersion(FilesFacade ff, Path path) {
        return findLastTablesFileVersion(ff, path, nameSink);
    }

    private int readTableId(Path path, CharSequence dirName, FilesFacade ff) {
        path.of(configuration.getRoot()).concat(dirName).concat(TableUtils.META_FILE_NAME).$();
        int fd = ff.openRO(path);
        if (fd < 1) {
            return 0;
        }

        try {
            int tableId = ff.readNonNegativeInt(fd, TableUtils.META_OFFSET_TABLE_ID);
            if (tableId < 0) {
                LOG.error().$("cannot read table id from metadata file [path=").$(path).I$();
                return 0;
            }
            byte isWal = (byte) (ff.readNonNegativeInt(fd, TableUtils.META_OFFSET_WAL_ENABLED) & 0xFF);
            return isWal == 0 ? tableId : -tableId;
        } finally {
            ff.close(fd);
        }
    }

    private void reloadFromRootDirectory(
            ConcurrentHashMap<TableToken> nameTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> reverseTableNameTokenMap
    ) {
        Path path = Path.getThreadLocal(configuration.getRoot()).$();
        int plimit = path.size();
        FilesFacade ff = configuration.getFilesFacade();
        long findPtr = ff.findFirst(path);
        try {
            Utf8StringSink dirNameSink = Misc.getThreadLocalUtf8Sink();
            do {
                if (ff.isDirOrSoftLinkDirNoDots(path, plimit, ff.findName(findPtr), ff.findType(findPtr), dirNameSink)) {
                    String dirName = Utf8s.toString(dirNameSink);
                    if (
                            !reverseTableNameTokenMap.containsKey(dirName)
                                    && TableUtils.exists(ff, path, configuration.getRoot(), dirNameSink) == TableUtils.TABLE_EXISTS
                    ) {
                        int tableId;
                        boolean isWal;
                        String tableName;

                        try {
                            tableId = readTableId(path, dirName, ff);
                            isWal = tableId < 0;
                            tableId = Math.abs(tableId);
                            tableName = TableUtils.readTableName(path.of(configuration.getRoot()).concat(dirNameSink), plimit, tableNameRoMemory, ff);
                        } catch (CairoException e) {
                            if (e.errnoReadPathDoesNotExist()) {
                                // table is being removed.
                                continue;
                            } else {
                                throw e;
                            }
                        } finally {
                            tableNameRoMemory.close();
                        }

                        if (tableName == null) {
                            if (isWal) {
                                LOG.error().$("could not read table name, table will not be available [dirName=").$(dirNameSink).I$();
                                continue;
                            } else {
                                // Non-wal tables may not have _name file.
                                tableName = Chars.toString(TableUtils.getTableNameFromDirName(dirName));
                            }
                        }

                        if (tableId > -1L) {
                            if (tableIds.contains(tableId)) {
                                LOG.critical().$("duplicate table id found, table will not be available " +
                                        "[dirName=").$(dirNameSink).$(", id=").$(tableId).I$();
                                continue;
                            }

                            if (nameTableTokenMap.containsKey(tableName)) {
                                LOG.critical().$("duplicate table name found, table will not be available [dirName=").$(dirNameSink)
                                        .$(", name=").utf8(tableName)
                                        .$(", existingTableDir=").utf8(nameTableTokenMap.get(tableName).getDirName())
                                        .I$();
                                continue;
                            }

                            boolean isProtected = protectedTableResolver.test(tableName);
                            TableToken token = new TableToken(tableName, dirName, tableId, isWal, isProtected);
                            nameTableTokenMap.put(tableName, token);
                            reverseTableNameTokenMap.put(dirName, ReverseTableMapItem.of(token));
                        }
                    }
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
        }
    }

    private void reloadFromTablesFile(
            ConcurrentHashMap<TableToken> nameTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> reverseTableNameTokenMap,
            @Nullable ObjList<TableToken> convertedTables
    ) {
        int lastFileVersion;
        FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.getThreadLocal(configuration.getRoot());
        int plimit = path.size();

        MemoryMR memory = isLocked() ? tableNameMemory : tableNameRoMemory;
        do {
            lastFileVersion = findLastTablesFileVersion(ff, path.trimTo(plimit).$());
            path.trimTo(plimit).concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(lastFileVersion).$();
            try {
                memory.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                LOG.info().$("reloading tables file [path=").$(path).I$();
                if (memory.size() >= 2 * Long.BYTES) {
                    break;
                }
            } catch (CairoException e) {
                if (!isLocked()) {
                    if (e.errnoReadPathDoesNotExist()) {
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

        int tableToCompact = 0;
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

            if (operation == OPERATION_REMOVE) {
                // remove from registry
                final TableToken token = nameTableTokenMap.remove(tableName);
                if (token != null) {
                    if (!ff.exists(path.trimTo(plimit).concat(dirName).$())) {
                        // table already fully removed
                        tableToCompact++;
                        reverseTableNameTokenMap.remove(token.getDirName());
                    } else {
                        reverseTableNameTokenMap.put(dirName, ReverseTableMapItem.ofDropped(token));
                    }
                } else {
                    reverseTableNameTokenMap.remove(dirName);
                    tableToCompact++;
                }
            } else {
                if (tableIds.contains(tableId)) {
                    LOG.critical().$("duplicate table id found, table will not be accessible " +
                            "[dirName=").utf8(dirName).$(", id=").$(tableId).I$();
                    continue;
                }

                boolean isProtected = protectedTableResolver.test(tableName);
                final TableToken token = new TableToken(tableName, dirName, tableId, tableType == TableUtils.TABLE_TYPE_WAL, isProtected);
                nameTableTokenMap.put(tableName, token);
                if (!Chars.startsWith(token.getDirName(), token.getTableName())) {
                    // This table is renamed, log system to real table name mapping
                    LOG.advisory().$("table dir name does not match logical name [table=").utf8(tableName).$(", dirName=").utf8(dirName).I$();
                }

                reverseTableNameTokenMap.put(token.getDirName(), ReverseTableMapItem.of(token));
                currentOffset += TABLE_NAME_ENTRY_RESERVED_LONGS * Long.BYTES;
            }
        }

        int forceCompact = Integer.MAX_VALUE / 2;
        if (isLocked()) {
            // Check that the table directories exist
            for (TableToken token : nameTableTokenMap.values()) {
                if (TableUtils.exists(ff, path, configuration.getRoot(), token.getDirName()) != TableUtils.TABLE_EXISTS) {
                    LOG.error().$("table directory directly removed from File System, table will not be available [path=").$(path)
                            .$(", dirName=").utf8(token.getDirName()).
                            $(", table=").utf8(token.getTableName())
                            .I$();

                    nameTableTokenMap.remove(token.getTableName());
                    reverseTableNameTokenMap.remove(token.getDirName());
                    tableToCompact++;
                }
            }

            if (convertedTables != null) {
                for (int i = 0, n = convertedTables.size(); i < n; i++) {
                    final TableToken token = convertedTables.get(i);
                    if (token.isWal()) {
                        nameTableTokenMap.put(token.getTableName(), token);
                        reverseTableNameTokenMap.put(token.getDirName(), ReverseTableMapItem.of(token));
                    } else {
                        nameTableTokenMap.remove(token.getTableName());
                        reverseTableNameTokenMap.remove(token.getDirName());
                    }
                    tableToCompact = Integer.MAX_VALUE / 2;
                }
            }

            int tableRegistryCompactionThreshold = configuration.getTableRegistryCompactionThreshold();
            if ((tableRegistryCompactionThreshold > -1 && tableToCompact > tableRegistryCompactionThreshold) || tableToCompact >= forceCompact) {
                path.trimTo(plimit);
                LOG.info().$("compacting tables file").$();
                compactTableNameFile(nameTableTokenMap, reverseTableNameTokenMap, lastFileVersion, ff, path, currentOffset);
            } else {
                tableNameMemory.jumpTo(currentOffset);
            }
        } else {
            tableNameRoMemory.close();
        }
    }

    void reload(
            ConcurrentHashMap<TableToken> nameTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> reverseTableNameTokenMap,
            @Nullable ObjList<TableToken> convertedTables
    ) {
        tableIds.clear();
        reloadFromTablesFile(nameTableTokenMap, reverseTableNameTokenMap, convertedTables);
        reloadFromRootDirectory(nameTableTokenMap, reverseTableNameTokenMap);
    }

    @Override
    protected void writeEntry(TableToken tableToken, int operation) {
        if (!isLocked()) {
            throw CairoException.critical(0).put("table registry is not locked");
        }
        super.writeEntry(tableToken, operation);
    }
}
