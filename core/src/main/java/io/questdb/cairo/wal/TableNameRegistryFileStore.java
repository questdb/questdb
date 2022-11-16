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
import java.util.Map;

import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;

public class TableNameRegistryFileStore implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableNameRegistryFileStore.class);
    private static final int OPERATION_ADD = 0;
    private static final int OPERATION_REMOVE = -1;
    private final static long TABLE_NAME_ENTRY_RESERVED_LONGS = 8;
    private final CairoConfiguration configuration;
    private final MemoryMARW tableNameMemory = Vm.getCMARWInstance();
    private long lockFd = -1;

    public TableNameRegistryFileStore(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    public synchronized void appendEntry(final CharSequence tableName, final CharSequence systemTableName) {
        writeEntry(tableName, systemTableName, OPERATION_ADD);
    }

    @Override
    public void close() {
        if (lockFd != -1) {
            configuration.getFilesFacade().close(lockFd);
            lockFd = -1;
        }
        Misc.free(tableNameMemory);
    }

    public boolean lock() {
        if (lockFd != -1) {
            throw CairoException.critical(0).put("table registry already locked");
        }
        Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).$();
        lockFd = TableUtils.lock(configuration.getFilesFacade(), path, true);
        return lockFd != -1;
    }

    public void reload(
            Map<CharSequence, TableNameRecord> systemTableNameCache,
            Map<CharSequence, String> reverseTableNameCache,
            String droppedMarker
    ) {
        FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).$();
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
                reverseTableNameCache.put(systemName, droppedMarker);
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
        if (isLocked() && deletedRecordsFound > 0) {
            tableNameMemory.jumpTo(Long.BYTES);
            for (CharSequence tableName : systemTableNameCache.keySet()) {
                String systemName = systemTableNameCache.get(tableName).systemTableName;
                appendEntry(tableName, systemName);
            }
            tableNameMemory.putLong(0, systemTableNameCache.size());
        }

        reloadFromRootDirectory(systemTableNameCache, reverseTableNameCache);
    }

    public void reloadFromRootDirectory(
            Map<CharSequence, TableNameRecord> systemTableNameCache,
            Map<CharSequence, String> reverseTableNameCache
    ) {
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
                        systemTableNameCache.put(tableName, new TableNameRecord(systemTableName, false));
                        reverseTableNameCache.put(systemTableName, tableName);
                    }
                }
            }
        } while (ff.findNext(findPtr) > 0);
        ff.findClose(findPtr);
    }

    public synchronized void removeEntry(final CharSequence tableName, final CharSequence systemTableName) {
        writeEntry(tableName, systemTableName, OPERATION_REMOVE);
    }

    @TestOnly
    public synchronized void resetMemory() {
        if (!isLocked()) {
            throw CairoException.critical(0).put("table registry is not locked");
        }
        tableNameMemory.jumpTo(0L);
        final Path path = Path.getThreadLocal(configuration.getRoot()).concat(TABLE_REGISTRY_NAME_FILE).$();
        tableNameMemory.close();
        tableNameMemory.smallFile(configuration.getFilesFacade(), path, MemoryTag.MMAP_TABLE_WAL_WRITER);
    }

    private boolean isLocked() {
        return lockFd != -1;
    }

    private void writeEntry(CharSequence tableName, CharSequence systemTableName, int operation) {
        if (!isLocked()) {
            throw CairoException.critical(0).put("table registry is not locked");
        }
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
