/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Map;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.std.Files.DT_FILE;

public class TableNameRegistryStore extends GrowOnlyTableNameRegistryStore {
    private static final Log LOG = LogFactory.getLog(TableNameRegistryStore.class);
    private final CairoConfiguration configuration;
    private final StringSink nameSink = new StringSink();
    private final TableFlagResolver tableFlagResolver;
    private final MemoryCMR tableNameRoMemory = Vm.getCMRInstance();
    private long lockFd = -1;
    private long longBuffer;

    public TableNameRegistryStore(CairoConfiguration configuration, TableFlagResolver tableFlagResolver) {
        super(configuration.getFilesFacade());
        this.configuration = configuration;
        this.tableFlagResolver = tableFlagResolver;
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
        LPSZ path = Path.getThreadLocal(configuration.getDbRoot()).concat(TABLE_REGISTRY_NAME_FILE).put(".lock").$();
        if (ff.exists(path)) {
            ff.touch(path);
        }
        lockFd = TableUtils.lock(ff, path);
        return lockFd != -1;
    }

    public boolean reload(
            ConcurrentHashMap<TableToken> tableNameToTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> dirNameToTokenMap,
            @Nullable ObjList<TableToken> convertedTables
    ) {
        boolean consistent = reloadFromTablesFile(tableNameToTokenMap, dirNameToTokenMap, convertedTables);
        reloadFromRootDirectory(tableNameToTokenMap, dirNameToTokenMap);
        return consistent;
    }

    @TestOnly
    public synchronized void resetMemory() {
        if (!isLocked()) {
            if (!lock()) {
                throw CairoException.critical(0).put("table registry is not locked");
            }
        }
        tableNameMemory.close();

        final LPSZ path = Path.getThreadLocal(configuration.getDbRoot()).concat(TABLE_REGISTRY_NAME_FILE).put(".0").$();
        configuration.getFilesFacade().remove(path);

        tableNameMemory.smallFile(configuration.getFilesFacade(), path, MemoryTag.MMAP_DEFAULT);
    }

    @Override
    public void writeEntry(TableToken tableToken, int operation) {
        if (!isLocked()) {
            throw CairoException.critical(0).put("table registry is not locked");
        }
        super.writeEntry(tableToken, operation);
    }

    private boolean checkWalTableInPendingDropState(TableToken tableToken, FilesFacade ff, Path path, int plimit) {
        if (longBuffer == 0) {
            // lazy init
            longBuffer = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        path.trimTo(plimit).concat(tableToken.getDirName()).concat(SEQ_DIR).concat(META_FILE_NAME);
        long seqMetaFd = ff.openRO(path.$());
        if (seqMetaFd == -1) {
            LOG.error().$("cannot open seq meta file, assume table is being dropped [path=").$(path).I$();
            return true;
        }

        try {
            if (ff.read(seqMetaFd, longBuffer, Long.BYTES, SEQ_META_OFFSET_STRUCTURE_VERSION) == Long.BYTES) {
                long structureVersion = Unsafe.getUnsafe().getLong(longBuffer);
                return structureVersion == DROP_TABLE_STRUCTURE_VERSION;
            } else {
                LOG.error().$("cannot read structure version, assume table is being dropped [path=").$(path).I$();
                // cannot read structure version, assume table is being dropped
                return true;
            }
        } finally {
            ff.close(seqMetaFd);
        }
    }

    private void clearRegistryToReloadFromFileSystem(
            ConcurrentHashMap<TableToken> tableNameToTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap,
            int lastFileVersion,
            String errorTableName,
            String errorDirName,
            TableToken conflictTableToken
    ) {
        LOG.critical().$("duplicate table dir to name mapping found [tableName=").$safe(errorTableName)
                .$(", dirName1=").$(conflictTableToken.getDirNameUtf8())
                .$(", dirName2=").$safe(errorDirName)
                .I$();
        dumpTableRegistry(lastFileVersion);
        if (isLocked()) {
            // Reset existing registry to empty state
            tableNameMemory.putLong(0, Long.BYTES);
            tableNameMemory.jumpTo(Long.BYTES);
        }
        tableNameToTableTokenMap.clear();
        dirNameToTableTokenMap.clear();
    }

    private void compactTableNameFile(
            Map<CharSequence, TableToken> nameTableTokenMap,
            Map<CharSequence, ReverseTableMapItem> reverseNameMap,
            int lastFileVersion,
            FilesFacade ff,
            Path path
    ) {
        // compact the memory, remove deleted entries.
        // write to the tmp file.
        int pathRootLen = path.size();
        path.concat(TABLE_REGISTRY_NAME_FILE).putAscii(".tmp");
        long currentOffset;

        tableNameMemory.close(false);
        tableNameMemory.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
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
        LPSZ path2 = Path.getThreadLocal2(configuration.getDbRoot())
                .concat(TABLE_REGISTRY_NAME_FILE).put('.').put(lastFileVersion + 1).$();
        if (ff.rename(path.$(), path2) == Files.FILES_RENAME_OK) {
            LOG.info().$("compacted tables file [path=").$(path2).I$();
            lastFileVersion++;
            currentOffset = newAppendOffset;
            // best effort to remove old files, but we don't care if it fails
            path.trimTo(pathRootLen).concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(lastFileVersion - 1);
            ff.removeQuiet(path.$());

            path.trimTo(pathRootLen).concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(lastFileVersion);
            tableNameMemory.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
            tableNameMemory.jumpTo(currentOffset);
        } else {
            // Not critical, if rename fails, compaction will be done next time
            // Reopen the existing, non-compacted file
            path2 = Path.getThreadLocal2(configuration.getDbRoot())
                    .concat(TABLE_REGISTRY_NAME_FILE).put('.').put(lastFileVersion).$();
            tableNameMemory.smallFile(ff, path2, MemoryTag.MMAP_DEFAULT);
            long appendOffset = tableNameMemory.getLong(0);
            tableNameMemory.jumpTo(appendOffset);

            LOG.error().$("could not rename tables file, tables file will not be compacted [from=").$(path)
                    .$(", to=").$(path2).I$();
        }
    }

    private void dumpTableRegistry(int lastFileVersion) {
        MemoryMR memory = isLocked() ? tableNameMemory : tableNameRoMemory;
        long mapMem = memory.getLong(0);
        long currentOffset = Long.BYTES;

        LOG.advisoryW().$("dumping table registry [file=").$(TABLE_REGISTRY_NAME_FILE).$('.').$(lastFileVersion)
                .$(", size=").$(mapMem).I$();

        while (currentOffset < mapMem) {
            int operation = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;
            String tableName = Chars.toString(memory.getStrA(currentOffset));
            currentOffset += Vm.getStorageLength(tableName);
            String dirName = Chars.toString(memory.getStrA(currentOffset));
            currentOffset += Vm.getStorageLength(dirName);
            int tableId = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;
            int tableType = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;

            LOG.advisoryW().$("operation=").$(operation == OPERATION_ADD ? "add (" : "remove (").$(operation)
                    .$("), tableName=").$safe(tableName)
                    .$(", dirName=").$safe(dirName)
                    .$(", tableId=").$(tableId)
                    .$(", tableType=").$(tableType)
                    .$(']').$();

            if (operation == OPERATION_ADD) {
                currentOffset += TABLE_NAME_ENTRY_RESERVED_LONGS * Long.BYTES;
            }
        }
        LOG.advisoryW().$("table registry dump complete").$();
    }

    private int findLastTablesFileVersion(FilesFacade ff, Path path) {
        return findLastTablesFileVersion(ff, path, nameSink);
    }

    private int readTableId(Path path, CharSequence dirName, FilesFacade ff) {
        path.of(configuration.getDbRoot()).concat(dirName);
        int pathLen = path.size();
        path.concat(META_FILE_NAME);
        long fd = ff.openRO(path.$());
        if (fd < 1) {
            // check if it is a view
            path.trimTo(pathLen).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
            if (ff.exists(path.$())) {
                // negative table id means WAL table,
                // views are considered to be WAL tables
                return -getTableIdFromTableDir(dirName);
            }
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
            ConcurrentHashMap<TableToken> tableNameToTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap
    ) {
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        int plimit = path.size();
        FilesFacade ff = configuration.getFilesFacade();
        long findPtr = ff.findFirst(path.$());
        try {
            Utf8StringSink dirNameSink = Misc.getThreadLocalUtf8Sink();
            do {
                if (ff.isDirOrSoftLinkDirNoDots(path, plimit, ff.findName(findPtr), ff.findType(findPtr), dirNameSink)) {
                    String dirName = Utf8s.toString(dirNameSink);
                    if (
                            !dirNameToTableTokenMap.containsKey(dirName)
                                    && TableUtils.exists(ff, path, configuration.getDbRoot(), dirNameSink) == TableUtils.TABLE_EXISTS
                    ) {
                        int tableId;
                        boolean isWal;
                        String tableName;

                        try {
                            tableId = readTableId(path, dirName, ff);
                            isWal = tableId < 0;
                            tableId = Math.abs(tableId);
                            tableName = TableUtils.readTableName(path.of(configuration.getDbRoot()).concat(dirNameSink), plimit, tableNameRoMemory, ff);
                        } catch (CairoException e) {
                            if (e.isFileCannotRead()) {
                                // table is being removed.
                                continue;
                            } else {
                                throw e;
                            }
                        } finally {
                            tableNameRoMemory.close();
                        }

                        if (tableName == null) {
                            LOG.info().$("could not read table name, table will use directory name [dirName=").$(dirNameSink).I$();
                            tableName = Chars.toString(TableUtils.getTableNameFromDirName(dirName));
                        }

                        if (tableId > -1L) {
                            boolean isProtected = tableFlagResolver.isProtected(tableName);
                            boolean isSystem = tableFlagResolver.isSystem(tableName);
                            boolean isPublic = tableFlagResolver.isPublic(tableName);
                            boolean isMatView = isMatViewDefinitionFileExists(configuration, path, dirName);
                            boolean isView = isViewDefinitionFileExists(configuration, path, dirName);
                            String dbLogName = configuration.getDbLogName();
                            TableToken token = new TableToken(tableName, dirName, dbLogName, tableId, isView, isMatView, isWal, isSystem, isProtected, isPublic);
                            TableToken existingTableToken = tableNameToTableTokenMap.get(tableName);

                            if (existingTableToken != null) {
                                // One of the tables can be in pending drop state.
                                if (!resolveTableNameConflict(tableNameToTableTokenMap, dirNameToTableTokenMap, token, existingTableToken, ff, path, plimit)) {
                                    LOG.critical().$("duplicate table name found, table will not be available [dirName=").$(dirNameSink)
                                            .$(", name=").$safe(tableName)
                                            .$(", existingTableDir=")
                                            .$(tableNameToTableTokenMap.get(tableName).getDirNameUtf8())
                                            .I$();
                                }
                                continue;
                            }

                            tableNameToTableTokenMap.put(tableName, token);
                            dirNameToTableTokenMap.put(dirName, ReverseTableMapItem.of(token));
                        }
                    }
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
            if (longBuffer != 0) {
                longBuffer = Unsafe.free(longBuffer, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private boolean reloadFromTablesFile(
            ConcurrentHashMap<TableToken> tableNameToTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap,
            @Nullable ObjList<TableToken> convertedTables
    ) {
        int lastFileVersion;
        FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        int plimit = path.size();

        MemoryMR memory = isLocked() ? tableNameMemory : tableNameRoMemory;
        do {
            lastFileVersion = findLastTablesFileVersion(ff, path.trimTo(plimit));
            path.trimTo(plimit).concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(lastFileVersion).$();
            try {
                memory.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                LOG.info()
                        .$("reloading tables file [path=").$(path)
                        .$(", threadId=").$(Thread.currentThread().getId())
                        .I$();
                if (memory.size() >= 2 * Long.BYTES) {
                    break;
                }
            } catch (CairoException e) {
                if (!isLocked()) {
                    if (e.isFileCannotRead()) {
                        if (lastFileVersion == 0) {
                            // This is RO mode and file and tables.d.0 does not exist.
                            return false;
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
        int forceCompact = Integer.MAX_VALUE / 2;

        int tableToCompact = 0;
        while (currentOffset < mapMem) {
            int operation = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;
            String tableName = Chars.toString(memory.getStrA(currentOffset));
            currentOffset += Vm.getStorageLength(tableName);
            String dirName = Chars.toString(memory.getStrA(currentOffset));
            currentOffset += Vm.getStorageLength(dirName);
            int tableId = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;
            int tableType = memory.getInt(currentOffset);
            currentOffset += Integer.BYTES;

            if (operation == OPERATION_REMOVE) {
                TableToken token = tableNameToTableTokenMap.remove(tableName);
                if (!ff.exists(path.trimTo(plimit).concat(dirName).$())) {
                    // table already fully removed
                    tableToCompact++;
                    dirNameToTableTokenMap.remove(dirName);
                } else {
                    if (token == null) {
                        boolean isProtected = tableFlagResolver.isProtected(tableName);
                        boolean isSystem = tableFlagResolver.isSystem(tableName);
                        boolean isPublic = tableFlagResolver.isPublic(tableName);
                        boolean isView = tableType == TABLE_TYPE_VIEW;
                        boolean isMatView = tableType == TABLE_TYPE_MAT;
                        boolean isWal = tableType == TABLE_TYPE_WAL || isView || isMatView;
                        String dbLogName = configuration.getDbLogName();
                        token = new TableToken(tableName, dirName, dbLogName, tableId, isView, isMatView, isWal, isSystem, isProtected, isPublic);
                    }
                    dirNameToTableTokenMap.put(dirName, ReverseTableMapItem.ofDropped(token));
                }
            } else {
                assert operation == OPERATION_ADD;
                if (TableUtils.exists(ff, path, configuration.getDbRoot(), dirName) != TABLE_EXISTS) {
                    // This can be BAU, remove record will follow
                    tableToCompact++;
                } else {
                    final TableToken existing = tableNameToTableTokenMap.get(tableName);
                    if (existing != null) {
                        clearRegistryToReloadFromFileSystem(
                                tableNameToTableTokenMap,
                                dirNameToTableTokenMap,
                                lastFileVersion,
                                tableName,
                                dirName,
                                existing
                        );
                        return false;
                    }

                    boolean isProtected = tableFlagResolver.isProtected(tableName);
                    boolean isSystem = tableFlagResolver.isSystem(tableName);
                    boolean isPublic = tableFlagResolver.isPublic(tableName);
                    boolean isView = tableType == TABLE_TYPE_VIEW;
                    boolean isMatView = tableType == TableUtils.TABLE_TYPE_MAT;
                    boolean isWal = tableType == TableUtils.TABLE_TYPE_WAL || isView || isMatView;
                    String dbLogName = configuration.getDbLogName();
                    final TableToken token = new TableToken(tableName, dirName, dbLogName, tableId, isView, isMatView, isWal, isSystem, isProtected, isPublic);
                    tableNameToTableTokenMap.put(tableName, token);
                    if (!Chars.startsWith(token.getDirName(), token.getTableName())) {
                        // This table is renamed, log system to real table name mapping
                        LOG.debug().$("table dir name does not match logical name [table=").$safe(tableName).$(", dirName=").$safe(dirName).I$();
                    }
                    dirNameToTableTokenMap.put(token.getDirName(), ReverseTableMapItem.of(token));
                }
                currentOffset += TABLE_NAME_ENTRY_RESERVED_LONGS * Long.BYTES;
            }
        }

        if (isLocked()) {
            tableNameMemory.jumpTo(currentOffset);
            if (convertedTables != null) {
                for (int i = 0, n = convertedTables.size(); i < n; i++) {
                    final TableToken token = convertedTables.get(i);
                    final TableToken existing = tableNameToTableTokenMap.get(token.getTableName());

                    if (existing != null && !Chars.equals(existing.getDirName(), token.getDirName())) {
                        // Table with different directory already exists pointing to the same name
                        clearRegistryToReloadFromFileSystem(
                                tableNameToTableTokenMap,
                                dirNameToTableTokenMap,
                                lastFileVersion,
                                token.getTableName(),
                                token.getDirName(),
                                existing
                        );
                        return false;
                    }

                    if (token.isWal()) {
                        tableNameToTableTokenMap.put(token.getTableName(), token);
                        dirNameToTableTokenMap.put(token.getDirName(), ReverseTableMapItem.of(token));
                    } else {
                        tableNameToTableTokenMap.remove(token.getTableName());
                        dirNameToTableTokenMap.remove(token.getDirName());
                    }

                    // Force the compaction
                    tableToCompact = forceCompact;
                }
            }

            final int tableRegistryCompactionThreshold = configuration.getTableRegistryCompactionThreshold();
            if ((tableRegistryCompactionThreshold > -1 && tableToCompact > tableRegistryCompactionThreshold) || tableToCompact >= forceCompact) {
                path.trimTo(plimit);
                LOG.info().$("compacting tables file").$();
                compactTableNameFile(tableNameToTableTokenMap, dirNameToTableTokenMap, lastFileVersion, ff, path);
            } else {
                tableNameMemory.jumpTo(currentOffset);
            }
        } else {
            tableNameRoMemory.close();
        }
        return true;
    }

    private boolean resolveTableNameConflict(
            ConcurrentHashMap<TableToken> tableNameToTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap,
            TableToken newToken,
            TableToken existingTableToken,
            FilesFacade ff,
            Path path,
            int plimit
    ) {
        boolean existingDropped = false;
        boolean newDropped = false;

        if (existingTableToken.isWal()) {
            existingDropped = checkWalTableInPendingDropState(existingTableToken, ff, path, plimit);
        }

        if (!existingDropped) {
            // Check if new table is dropped
            if (newToken.isWal()) {
                newDropped = checkWalTableInPendingDropState(newToken, ff, path, plimit);
            }
        } else {
            // existing table token table is partially dropped
            tableNameToTableTokenMap.remove(existingTableToken.getTableName());
            dirNameToTableTokenMap.remove(existingTableToken.getDirName());

            // mark existing as pending dropped
            dirNameToTableTokenMap.put(existingTableToken.getDirName(), ReverseTableMapItem.ofDropped(existingTableToken));

            // add new table
            tableNameToTableTokenMap.put(newToken.getTableName(), newToken);
            dirNameToTableTokenMap.put(newToken.getDirName(), ReverseTableMapItem.of(newToken));

            return true;
        }

        // don't add new table to registry
        return newDropped;
    }
}
