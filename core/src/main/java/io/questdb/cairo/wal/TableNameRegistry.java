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
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;

public class TableNameRegistry implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableNameRegistry.class);
    private static final int OPERATION_ADD = 0;
    private static final int OPERATION_REMOVE = -1;
    private static final String TABLE_DROPPED_MARKER = "TABLE_DROPPED_MARKER:..";
    private final static long TABLE_NAME_ENTRY_RESERVED_LONGS = 8;
    private final boolean mangleDefaultTableNames;
    private final ConcurrentHashMap<String> nonWalSystemTableNames = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String> reverseNonWalTableNameRegistry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String> reverseWalTableNameRegistry = new ConcurrentHashMap<>();
    private final MemoryMARW tableNameMemory = Vm.getCMARWInstance();
    private final ConcurrentHashMap<String> walSystemTableNameRegistry = new ConcurrentHashMap<>();

    public TableNameRegistry(boolean mangleDefaultTableNames) {

        this.mangleDefaultTableNames = mangleDefaultTableNames;
    }

    @Override
    public void close() {
        walSystemTableNameRegistry.clear();
        reverseWalTableNameRegistry.clear();
        nonWalSystemTableNames.clear();
        reverseNonWalTableNameRegistry.clear();
        Misc.free(tableNameMemory);
    }

    public String getDefaultSystemTableName(CharSequence tableName) {
        String defaultSystemName = nonWalSystemTableNames.get(tableName);
        if (defaultSystemName == null) {
            defaultSystemName = mangleDefaultTableNames ? tableName.toString() + TableUtils.SYSTEM_TABLE_NAME_SUFFIX : Chars.toString(tableName);
            nonWalSystemTableNames.put(tableName, defaultSystemName);
        }
        return defaultSystemName;
    }

    public String getSystemName(CharSequence tableName) {
        return walSystemTableNameRegistry.get(tableName);
    }

    public String getTableNameBySystemName(CharSequence systemTableName) {
        String tableName = reverseWalTableNameRegistry.get(systemTableName);
        if (tableName == null) {
            tableName = reverseNonWalTableNameRegistry.get(systemTableName);
            if (tableName == null) {
                tableName = TableUtils.toTableNameFromSystemName(Chars.toString(systemTableName));
                if (tableName != null) {
                    reverseNonWalTableNameRegistry.putIfAbsent(systemTableName, tableName);
                }
            }
        }

        //noinspection StringEquality
        if (tableName == TABLE_DROPPED_MARKER) {
            return null;
        }

        return tableName;
    }

    public String getWalTableSystemName(CharSequence tableName) {
        return walSystemTableNameRegistry.get(tableName);
    }

    public ConcurrentHashMap.KeySetView<String> getWalTableSystemNames() {
        return reverseWalTableNameRegistry.keySet();
    }

    public boolean isWalSystemName(String systemTableName) {
        String tableName = reverseWalTableNameRegistry.get(systemTableName);
        //noinspection StringEquality
        if (tableName == null || tableName == TABLE_DROPPED_MARKER) {
            return false;
        }
        return walSystemTableNameRegistry.get(tableName) != null;
    }

    public boolean isWalTableDropped(CharSequence systemTableName) {
        //noinspection StringEquality
        return reverseWalTableNameRegistry.get(systemTableName) == TABLE_DROPPED_MARKER;
    }

    public String registerName(String tableName, String systemTableName) {
        String str = walSystemTableNameRegistry.putIfAbsent(tableName, systemTableName);
        if (str == null) {
            appendEntry(tableName, systemTableName);
            reverseWalTableNameRegistry.put(systemTableName, tableName);
            return systemTableName;
        } else {
            return null;
        }
    }

    public synchronized void reloadTableNameCache(CairoConfiguration configuration) {
        LOG.info().$("reloading table to system name mappings").$();
        walSystemTableNameRegistry.clear();
        reverseWalTableNameRegistry.clear();
        final FilesFacade ff = configuration.getFilesFacade();

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
                    walSystemTableNameRegistry.remove(tableNameStr);
                    reverseWalTableNameRegistry.remove(systemName);
                    deletedRecordsFound++;
                } else {
                    boolean systemNameExists = reverseWalTableNameRegistry.get(systemName) != null;
                    walSystemTableNameRegistry.put(tableNameStr, systemName);
                    if (systemNameExists) {
                        // This table is renamed, log system to real table name mapping
                        LOG.advisory().$("renamed WAL table system name [table=").utf8(tableNameStr).$(", systemTableName=").utf8(systemName).$();
                    }
                    reverseWalTableNameRegistry.put(systemName, tableNameStr);
                    currentOffset += TABLE_NAME_ENTRY_RESERVED_LONGS * Long.BYTES;
                }
            }
            tableNameMemory.jumpTo(currentOffset);

            // compact the memory, remove deleted entries
            if (deletedRecordsFound > 0) {
                tableNameMemory.jumpTo(Long.BYTES);

                for (CharSequence tableName : walSystemTableNameRegistry.keySet()) {
                    String systemName = walSystemTableNameRegistry.get(tableName);
                    appendEntry(tableName, systemName);
                }
                tableNameMemory.putLong(0, walSystemTableNameRegistry.size());
            }

        }
    }

    public boolean removeName(CharSequence tableName, String systemTableName) {
        if (walSystemTableNameRegistry.remove(tableName, systemTableName)) {
            removeEntry(tableName, systemTableName);
            reverseWalTableNameRegistry.put(systemTableName, TABLE_DROPPED_MARKER);
            return true;
        }
        return false;
    }

    public void removeTableSystemName(CharSequence systemTableName) {
        reverseWalTableNameRegistry.remove(systemTableName);
    }

    public String rename(CharSequence oldName, CharSequence newName, String systemTableName) {
        if (walSystemTableNameRegistry.putIfAbsent(newName, systemTableName) == null) {
            removeEntry(oldName, systemTableName);
            appendEntry(newName, systemTableName);

            String newTableNameStr = Chars.toString(newName);
            reverseWalTableNameRegistry.put(systemTableName, newTableNameStr);
            walSystemTableNameRegistry.remove(oldName, systemTableName);
            return newTableNameStr;
        } else {
            throw CairoException.nonCritical().put("table '").put(newName).put("' already exists");
        }
    }

    @TestOnly
    public void reset(CairoConfiguration configuration) {
        walSystemTableNameRegistry.clear();
        reverseWalTableNameRegistry.clear();
        tableNameMemory.jumpTo(0L);
        try (final Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).$()) {
            tableNameMemory.close();
            tableNameMemory.smallFile(configuration.getFilesFacade(), path, MemoryTag.MMAP_TABLE_WAL_WRITER);
        }
    }

    private synchronized void appendEntry(final CharSequence tableName, final CharSequence systemTableName) {
        writeEntry(tableName, systemTableName, OPERATION_ADD);
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
