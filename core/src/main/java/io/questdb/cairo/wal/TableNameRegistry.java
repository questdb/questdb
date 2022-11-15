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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;

public class TableNameRegistry implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableNameRegistry.class);
    private static final int OPERATION_ADD = 0;
    private static final int OPERATION_REMOVE = -1;
    private static final String TABLE_DROPPED_MARKER = "TABLE_DROPPED_MARKER:..";
    private final static long TABLE_NAME_ENTRY_RESERVED_LONGS = 8;
    private final ConcurrentHashMap<String> reverseTableNameCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TableNameRecord> systemTableNameCache = new ConcurrentHashMap<>();
    private final MemoryMARW tableNameMemory = Vm.getCMARWInstance();

    @Override
    public void close() {
        systemTableNameCache.clear();
        reverseTableNameCache.clear();
        Misc.free(tableNameMemory);
    }

    public void deleteNonWalName(CharSequence tableName, String systemTableName) {
        systemTableNameCache.remove(tableName);
        reverseTableNameCache.remove(systemTableName);
    }

    public String getSystemName(CharSequence tableName) {
        TableNameRecord nameRecord = systemTableNameCache.get(tableName);
        return nameRecord != null ? nameRecord.systemTableName : null;
    }

    public String getTableNameBySystemName(CharSequence systemTableName) {
        String tableName = reverseTableNameCache.get(systemTableName);
        if (tableName == null) {
            tableName = TableUtils.toTableNameFromSystemName(Chars.toString(systemTableName));
            if (tableName != null) {
                reverseTableNameCache.putIfAbsent(systemTableName, tableName);
            }
        }

        //noinspection StringEquality
        if (tableName == TABLE_DROPPED_MARKER) {
            return null;
        }

        return tableName;
    }

    public TableNameRecord getTableNameRecord(CharSequence tableName) {
        return systemTableNameCache.get(tableName);
    }

    public Iterable<CharSequence> getTableSystemNames() {
        return reverseTableNameCache.keySet();
    }

    public boolean isWalSystemTableName(CharSequence systemTableName) {
        String tableName = reverseTableNameCache.get(systemTableName);
        if (tableName != null) {
            return isWalTableName(tableName);
        }
        return false;
    }

    public boolean isWalTableDropped(CharSequence systemTableName) {
        //noinspection StringEquality
        return reverseTableNameCache.get(systemTableName) == TABLE_DROPPED_MARKER;
    }

    public boolean isWalTableName(CharSequence tableName) {
        TableNameRecord nameRecord = systemTableNameCache.get(tableName);
        return nameRecord != null && nameRecord.isWal;
    }

    public String registerName(String tableName, String systemTableName, boolean isWal) {
        TableNameRecord newNameRecord = new TableNameRecord(systemTableName, isWal);
        TableNameRecord registeredRecord = systemTableNameCache.putIfAbsent(tableName, newNameRecord);

        if (registeredRecord == null) {
            if (isWal) {
                appendEntry(tableName, systemTableName);
            }
            reverseTableNameCache.put(systemTableName, tableName);
            return systemTableName;
        } else {
            return null;
        }
    }

    public synchronized void reloadTableNameCache(CairoConfiguration configuration) {
        LOG.info().$("reloading table to system name mappings").$();
        systemTableNameCache.clear();
        reverseTableNameCache.clear();

        reloadFromFile(configuration);
        reloadFromRootDirectory(configuration);
    }

    public boolean removeName(CharSequence tableName, String systemTableName) {
        TableNameRecord nameRecord = systemTableNameCache.get(tableName);
        if (nameRecord != null && nameRecord.systemTableName.equals(systemTableName) && systemTableNameCache.remove(tableName, nameRecord)) {
            removeEntry(tableName, systemTableName);
            reverseTableNameCache.put(systemTableName, TABLE_DROPPED_MARKER);
            return true;
        }
        return false;
    }

    public void removeTableSystemName(CharSequence systemTableName) {
        reverseTableNameCache.remove(systemTableName);
    }

    public String rename(CharSequence oldName, CharSequence newName, String systemTableName) {
        TableNameRecord tableRecord = systemTableNameCache.get(oldName);
        String newTableNameStr = Chars.toString(newName);

        if (systemTableNameCache.putIfAbsent(newTableNameStr, tableRecord) == null) {
            removeEntry(oldName, systemTableName);
            appendEntry(newName, systemTableName);
            reverseTableNameCache.put(systemTableName, newTableNameStr);
            systemTableNameCache.remove(oldName, tableRecord);
            return newTableNameStr;
        } else {
            throw CairoException.nonCritical().put("table '").put(newName).put("' already exists");
        }
    }

    @TestOnly
    public void resetMemory(CairoConfiguration configuration) {
        tableNameMemory.jumpTo(0L);
        try (final Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).$()) {
            tableNameMemory.close();
            tableNameMemory.smallFile(configuration.getFilesFacade(), path, MemoryTag.MMAP_TABLE_WAL_WRITER);
        }
    }

    private synchronized void appendEntry(final CharSequence tableName, final CharSequence systemTableName) {
        writeEntry(tableName, systemTableName, OPERATION_ADD);
    }

    private void reloadFromFile(CairoConfiguration configuration) {
        FilesFacade ff = configuration.getFilesFacade();
        try (final Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).$()) {
            tableNameMemory.smallFile(ff, path, MemoryTag.MMAP_TABLE_WAL_WRITER);

            long entryCount = tableNameMemory.getLong(0);
            long currentOffset = Long.BYTES;

            int deletedRecordsFound = 0;
            for (int i = 0; i < entryCount; i++) {
                int operation = tableNameMemory.getInt(currentOffset);
                currentOffset += Integer.BYTES;
                CharSequence tableName = tableNameMemory.getStr(currentOffset);
                currentOffset += Vm.getStorageLength(tableName);

                String tableNameStr = Chars.toString(tableName);
                String systemName = Chars.toString(tableNameMemory.getStr(currentOffset));
                currentOffset += Vm.getStorageLength(systemName);

                if (operation == OPERATION_REMOVE) {
                    // remove from registry
                    systemTableNameCache.remove(tableNameStr);
                    reverseTableNameCache.put(systemName, TABLE_DROPPED_MARKER);
                    deletedRecordsFound++;
                } else {
                    boolean systemNameExists = reverseTableNameCache.get(systemName) != null;
                    systemTableNameCache.put(tableNameStr, new TableNameRecord(systemName, true));
                    if (systemNameExists) {
                        // This table is renamed, log system to real table name mapping
                        LOG.advisory().$("renamed WAL table system name [table=").utf8(tableNameStr).$(", systemTableName=").utf8(systemName).$();
                    }
                    reverseTableNameCache.put(systemName, tableNameStr);
                    currentOffset += TABLE_NAME_ENTRY_RESERVED_LONGS * Long.BYTES;
                }
            }
            tableNameMemory.jumpTo(currentOffset);

            // compact the memory, remove deleted entries
            // TODO: make this rewrite power cut stable
            if (deletedRecordsFound > 0) {
                tableNameMemory.jumpTo(Long.BYTES);

                for (CharSequence tableName : systemTableNameCache.keySet()) {
                    String systemName = systemTableNameCache.get(tableName).systemTableName;
                    appendEntry(tableName, systemName);
                }
                tableNameMemory.putLong(0, systemTableNameCache.size());
            }

        }
    }

    private void reloadFromRootDirectory(CairoConfiguration configuration) {
        Path path = Path.getThreadLocal(configuration.getRoot());
        FilesFacade ff = configuration.getFilesFacade();
        long findPtr = ff.findFirst(path.$());
        StringSink sink = Misc.getThreadLocalBuilder();

        do {
            long fileName = ff.findName(findPtr);
            if (Files.isDir(fileName, ff.findType(findPtr), sink)) {
                if (systemTableNameCache.get(sink) == null
                        && TableUtils.exists(ff, path, configuration.getRoot(), sink) == TableUtils.TABLE_EXISTS) {

                    String systemTableName = sink.toString();
                    String tableName = TableUtils.toTableNameFromSystemName(systemTableName);
                    if (tableName != null) {
                        registerName(tableName, systemTableName, false);
                    }
                }
            }
        } while (ff.findNext(findPtr) > 0);
        ff.findClose(findPtr);
    }

    private synchronized void removeEntry(final CharSequence tableName, final CharSequence systemTableName) {
        writeEntry(tableName, systemTableName, OPERATION_REMOVE);
    }

    private void writeEntry(CharSequence tableName, CharSequence systemTableName, int operation) {
        long entryCount = tableNameMemory.getLong(0);
        tableNameMemory.putInt(operation);
        tableNameMemory.putStr(tableName);
        tableNameMemory.putStr(systemTableName);
        if (operation != OPERATION_REMOVE) {
            for (int i = 0; i < TABLE_NAME_ENTRY_RESERVED_LONGS; i++) {
                tableNameMemory.putLong(0);
            }
        }
        tableNameMemory.putLong(0, entryCount + 1);
    }
}
