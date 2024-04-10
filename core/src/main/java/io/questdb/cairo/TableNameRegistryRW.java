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

import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import java.util.function.Predicate;

public class TableNameRegistryRW extends AbstractTableNameRegistry {

    public TableNameRegistryRW(CairoConfiguration configuration, Predicate<CharSequence> protectedTableResolver) {
        super(configuration, protectedTableResolver);
        if (!nameStore.lock()) {
            if (!configuration.getAllowTableRegistrySharedWrite()) {
                throw CairoException.critical(0).put("cannot lock table name registry file [path=").put(configuration.getRoot()).put(']');
            }
        }
        this.tableNameToTableTokenMap = new ConcurrentHashMap<>(false);
        this.dirNameToTableTokenMap = new ConcurrentHashMap<>();
    }

    @Override
    public TableToken addTableAlias(String newName, TableToken tableToken) {
        final TableToken newTableToken = tableToken.renamed(newName);
        final TableToken oldToken = tableNameToTableTokenMap.putIfAbsent(newName, newTableToken);
        return oldToken == null ? newTableToken : null;
    }

    @Override
    public boolean dropTable(TableToken token) {
        final ReverseTableMapItem reverseMapItem = dirNameToTableTokenMap.get(token.getDirName());
        if (reverseMapItem != null && tableNameToTableTokenMap.remove(token.getTableName(), token)) {
            if (token.isWal()) {
                nameStore.logDropTable(token);
                dirNameToTableTokenMap.put(token.getDirName(), ReverseTableMapItem.ofDropped(token));
            } else {
                dirNameToTableTokenMap.remove(token.getDirName(), reverseMapItem);
            }
            return true;
        }
        return false;
    }

    @Override
    public TableToken lockTableName(String tableName, String dirName, int tableId, boolean isWal) {
        final TableToken registeredRecord = tableNameToTableTokenMap.putIfAbsent(tableName, LOCKED_TOKEN);
        if (registeredRecord == null) {
            boolean isProtected = protectedTableResolver.test(tableName);
            boolean isSystem = TableUtils.isSystemTable(tableName, configuration);
            return new TableToken(tableName, dirName, tableId, isWal, isSystem, isProtected);
        } else {
            return null;
        }
    }

    @Override
    public void purgeToken(TableToken token) {
        dirNameToTableTokenMap.remove(token.getDirName());
    }

    @Override
    public void registerName(TableToken tableToken) {
        String tableName = tableToken.getTableName();
        if (!tableNameToTableTokenMap.replace(tableName, LOCKED_TOKEN, tableToken)) {
            throw CairoException.critical(0).put("cannot register table, name is not locked [name=").put(tableName).put(']');
        }
        if (tableToken.isWal()) {
            nameStore.logAddTable(tableToken);
        }
        dirNameToTableTokenMap.put(tableToken.getDirName(), ReverseTableMapItem.of(tableToken));
    }

    @Override
    public synchronized void reload(@Nullable ObjList<TableToken> convertedTables) {
        tableNameToTableTokenMap.clear();
        dirNameToTableTokenMap.clear();
        if (!nameStore.isLocked()) {
            nameStore.lock();
        }
        nameStore.reload(tableNameToTableTokenMap, dirNameToTableTokenMap, convertedTables);
    }

    @Override
    public void removeAlias(TableToken tableToken) {
        tableNameToTableTokenMap.remove(tableToken.getTableName());
    }

    @Override
    public TableToken rename(CharSequence oldName, CharSequence newName, TableToken tableToken) {
        String newTableNameStr = Chars.toString(newName);
        TableToken renamedTableToken = tableToken.renamed(newTableNameStr);

        if (tableNameToTableTokenMap.putIfAbsent(newTableNameStr, renamedTableToken) == null) {
            if (tableNameToTableTokenMap.remove(oldName, tableToken)) {
                // Persist to file
                nameStore.logDropTable(tableToken);
                nameStore.logAddTable(renamedTableToken);
                dirNameToTableTokenMap.put(renamedTableToken.getDirName(), ReverseTableMapItem.of(renamedTableToken));
                return renamedTableToken;
            } else {
                // Already renamed by another thread. Revert new name reservation.
                tableNameToTableTokenMap.remove(newTableNameStr, renamedTableToken);
                throw CairoException.tableDoesNotExist(oldName);
            }
        } else {
            throw CairoException.nonCritical().put("table '").put(newName).put("' already exists");
        }
    }

    @Override
    public void rename(TableToken oldToken, TableToken newToken) {
        if (tableNameToTableTokenMap.remove(oldToken.getTableName(), oldToken)) {
            nameStore.logDropTable(oldToken);
            nameStore.logAddTable(newToken);
            dirNameToTableTokenMap.put(newToken.getDirName(), ReverseTableMapItem.of(newToken));
        }
    }

    @Override
    public void unlockTableName(TableToken tableToken) {
        tableNameToTableTokenMap.remove(tableToken.getTableName(), LOCKED_TOKEN);
    }
}
