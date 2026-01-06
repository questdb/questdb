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

import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class TableNameRegistryRW extends AbstractTableNameRegistry {

    public TableNameRegistryRW(CairoEngine engine, TableFlagResolver tableFlagResolver) {
        super(engine, tableFlagResolver);
        if (!nameStore.lock()) {
            if (!engine.getConfiguration().getAllowTableRegistrySharedWrite()) {
                throw CairoException.critical(0).put("cannot lock table name registry file [path=").put(engine.getConfiguration().getDbRoot()).put(']');
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
        assert !TableNameRegistry.isLocked(token);
        final ReverseTableMapItem reverseMapItem = dirNameToTableTokenMap.get(token.getDirName());

        // we do not want to remove the token mapping and release the name of the table for another table create
        // before the change saved in the name store. This is why we lock the name here for dropping.
        if (reverseMapItem != null && tableNameToTableTokenMap.replace(token.getTableName(), token, LOCKED_DROP_TOKEN)) {
            if (token.isWal()) {
                nameStore.logDropTable(token);
                dirNameToTableTokenMap.put(token.getDirName(), ReverseTableMapItem.ofDropped(token));
            } else {
                dirNameToTableTokenMap.remove(token.getDirName(), reverseMapItem);
            }
            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.dropTable(token);
            }

            // remove the token from the map and release the name.
            boolean removed = tableNameToTableTokenMap.remove(token.getTableName(), LOCKED_DROP_TOKEN);
            assert removed;

            return true;
        }
        return false;
    }

    @Override
    public TableToken lockTableName(String tableName, String dirName, int tableId, boolean isView, boolean isMatView, boolean isWal) {
        final TableToken registeredRecord = tableNameToTableTokenMap.putIfAbsent(tableName, LOCKED_TOKEN);
        if (registeredRecord == null) {
            boolean isProtected = tableFlagResolver.isProtected(tableName);
            boolean isSystem = tableFlagResolver.isSystem(tableName);
            boolean isPublic = tableFlagResolver.isPublic(tableName);
            String dbLogName = engine.getConfiguration().getDbLogName();
            return new TableToken(tableName, dirName, dbLogName, tableId, isView, isMatView, isWal, isSystem, isProtected, isPublic);
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
        if (tableNameToTableTokenMap.get(tableName) != LOCKED_TOKEN) {
            throw CairoException.critical(0).put("cannot register table, name is not locked [name=").put(tableName).put(']');
        }

        // This most unsafe, can throw, run it first.
        if (!tableToken.isView()) {
            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(tableToken);
            }
        }

        if (tableToken.isWal()) {
            nameStore.logAddTable(tableToken);
        }
        dirNameToTableTokenMap.put(tableToken.getDirName(), ReverseTableMapItem.of(tableToken));

        // Finish the name registration, table is queryable from this moment.
        boolean stillLocked = tableNameToTableTokenMap.replace(tableName, LOCKED_TOKEN, tableToken);
        assert stillLocked;
    }

    @Override
    public synchronized boolean reload(@Nullable ObjList<TableToken> convertedTables) {
        tableNameToTableTokenMap.clear();
        dirNameToTableTokenMap.clear();
        if (!nameStore.isLocked()) {
            nameStore.lock();
        }
        return nameStore.reload(tableNameToTableTokenMap, dirNameToTableTokenMap, convertedTables);
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
            if (renameToNew(tableToken, renamedTableToken)) {
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
        renameToNew(oldToken, newToken);
    }

    @Override
    public void unlockTableName(TableToken tableToken) {
        tableNameToTableTokenMap.remove(tableToken.getTableName(), LOCKED_TOKEN);
    }

    private boolean renameToNew(TableToken oldToken, TableToken newToken) {
        // Mark the old name as about to be released, do not release the new name in
        // the map before it is logged in name store file.
        if (tableNameToTableTokenMap.replace(oldToken.getTableName(), oldToken, LOCKED_DROP_TOKEN)) {
            // Log the change in the name store file.
            nameStore.logDropTable(oldToken);
            nameStore.logAddTable(newToken);

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                // Save the new name in the table dir.
                metadataRW.renameTable(oldToken, newToken);
            }

            // Update the reverse map.
            dirNameToTableTokenMap.put(newToken.getDirName(), ReverseTableMapItem.of(newToken));

            // Release the new name in the map.
            boolean removed = tableNameToTableTokenMap.remove(oldToken.getTableName(), LOCKED_DROP_TOKEN);
            assert removed;

            return true;
        }
        return false;
    }
}
