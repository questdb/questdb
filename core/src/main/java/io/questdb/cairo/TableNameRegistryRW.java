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

import io.questdb.cairo.wal.AbstractTableNameRegistry;
import io.questdb.std.Chars;

import java.util.concurrent.ConcurrentHashMap;

public class TableNameRegistryRW extends AbstractTableNameRegistry {
    private final ConcurrentHashMap<CharSequence, TableToken> nameTableTokenMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TableToken, String> reverseTableNameTokenMap = new ConcurrentHashMap<>();

    public TableNameRegistryRW(CairoConfiguration configuration) {
        super(configuration);
        if (!this.tableNameStore.lock()) {
            if (!configuration.getAllowTableRegistrySharedWrite()) {
                throw CairoException.critical(0).put("cannot lock table name registry file [path=").put(configuration.getRoot()).put(']');
            }
        }
        setNameMaps(nameTableTokenMap, reverseTableNameTokenMap);
    }

    @Override
    public TableToken lockTableName(String tableName, String privateTableName, int tableId, boolean isWal) {
        TableToken newNameRecord = new TableToken(tableName, privateTableName, tableId, isWal);
        TableToken registeredRecord = nameTableTokenMap.putIfAbsent(tableName, LOCKED_TOKEN);

        if (registeredRecord == null) {
            return newNameRecord;
        } else {
            return null;
        }
    }

    @Override
    public void registerName(TableToken tableToken) {
        String tableName = tableToken.getLoggingName();
        if (!nameTableTokenMap.replace(tableName, LOCKED_TOKEN, tableToken)) {
            throw CairoException.critical(0).put("cannot register table, name is not locked [name=").put(tableName).put(']');
        }
        if (tableToken.isWal()) {
            tableNameStore.appendEntry(tableToken);
        }
        reverseTableNameTokenMap.put(tableToken, tableName);
    }

    @Override
    public synchronized void reloadTableNameCache() {
        nameTableTokenMap.clear();
        reverseTableNameTokenMap.clear();
        tableNameStore.reload(nameTableTokenMap, reverseTableNameTokenMap, TABLE_DROPPED_MARKER);
    }

    @Override
    public boolean removeTableName(CharSequence tableName, TableToken tableToken) {
        if (nameTableTokenMap.remove(tableName, tableToken)) {
            if (tableToken.isWal()) {
                tableNameStore.removeEntry(tableToken);
                reverseTableNameTokenMap.put(tableToken, TABLE_DROPPED_MARKER);
                return true;
            } else {
                return reverseTableNameTokenMap.remove(tableToken) != null;
            }
        }
        return false;
    }

    @Override
    public void removeTableToken(TableToken tableToken) {
        reverseTableNameTokenMap.remove(tableToken);
    }

    @Override
    public TableToken rename(CharSequence oldName, CharSequence newName, TableToken tableToken) {
        String newTableNameStr = Chars.toString(newName);
        TableToken newNameRecord = new TableToken(newTableNameStr, tableToken.getPrivateTableName(), tableToken.getTableId(), tableToken.isWal());

        if (nameTableTokenMap.putIfAbsent(newTableNameStr, newNameRecord) == null) {
            // Persist to file
            tableNameStore.removeEntry(tableToken);
            tableNameStore.appendEntry(newNameRecord);

            // Delete out of date key
            reverseTableNameTokenMap.remove(tableToken);
            reverseTableNameTokenMap.put(newNameRecord, newTableNameStr);

            nameTableTokenMap.remove(oldName, tableToken);
            return newNameRecord;
        } else {
            throw CairoException.nonCritical().put("table '").put(newName).put("' already exists");
        }
    }

    @Override
    public void resetMemory() {
        tableNameStore.resetMemory();
    }

    @Override
    public void unlockTableName(TableToken tableToken) {
        nameTableTokenMap.remove(tableToken.getLoggingName(), LOCKED_TOKEN);
    }
}
