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

import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;

public class TableNameRegistryRW extends AbstractTableNameRegistry {

    public TableNameRegistryRW(CairoConfiguration configuration) {
        super(configuration);
        if (!nameStore.lock()) {
            if (!configuration.getAllowTableRegistrySharedWrite()) {
                throw CairoException.critical(0).put("cannot lock table name registry file [path=").put(configuration.getRoot()).put(']');
            }
        }
        setNameMaps(new ConcurrentHashMap<>(false), new ConcurrentHashMap<>());
    }

    @Override
    public boolean dropTable(TableToken token) {
        String dirName = token.getDirName();
        final ReverseTableMapItem reverseMapItem = reverseNameTokenMap.get(dirName);
        if (reverseMapItem != null && nameTokenMap.remove(token.getTableName(), token)) {
            if (token.isWal()) {
                nameStore.logDropTable(token);
                reverseNameTokenMap.put(dirName, ReverseTableMapItem.ofDropped(token));
            } else {
                reverseNameTokenMap.remove(dirName, reverseMapItem);
            }
            return true;
        }
        return false;
    }

    @Override
    public TableToken lockTableName(String tableName, String dirName, int tableId, boolean isWal) {
        TableToken registeredRecord = nameTokenMap.putIfAbsent(tableName, LOCKED_TOKEN);
        return registeredRecord == null ? new TableToken(tableName, dirName, tableId, isWal) : null;
    }

    @Override
    public void purgeToken(TableToken token) {
        reverseNameTokenMap.remove(token.getDirName());
    }

    @Override
    public void registerName(TableToken tableToken) {
        String tableName = tableToken.getTableName();
        if (!nameTokenMap.replace(tableName, LOCKED_TOKEN, tableToken)) {
            throw CairoException.critical(0).put("cannot register table, name is not locked [name=").put(tableName).put(']');
        }
        if (tableToken.isWal()) {
            nameStore.appendEntry(tableToken);
        }
        reverseNameTokenMap.put(tableToken.getDirName(), ReverseTableMapItem.of(tableToken));
    }

    @Override
    public synchronized void reloadTableNameCache() {
        nameTokenMap.clear();
        reverseNameTokenMap.clear();
        if (!nameStore.isLocked()) {
            nameStore.lock();
        }
        nameStore.reload(nameTokenMap, reverseNameTokenMap);
    }

    @Override
    public TableToken rename(CharSequence oldName, CharSequence newName, TableToken tableToken) {
        String newTableNameStr = Chars.toString(newName);
        TableToken newNameRecord = new TableToken(newTableNameStr, tableToken.getDirName(), tableToken.getTableId(), tableToken.isWal());

        if (nameTokenMap.putIfAbsent(newTableNameStr, newNameRecord) == null) {
            if (nameTokenMap.remove(oldName, tableToken)) {
                // Persist to file
                nameStore.logDropTable(tableToken);
                nameStore.appendEntry(newNameRecord);
                reverseNameTokenMap.put(newNameRecord.getDirName(), ReverseTableMapItem.of(newNameRecord));
                return newNameRecord;
            } else {
                // Already renamed by another thread. Revert new name reservation.
                nameTokenMap.remove(newTableNameStr, newNameRecord);
                throw CairoException.tableDoesNotExist(oldName);
            }
        } else {
            throw CairoException.nonCritical().put("table '").put(newName).put("' already exists");
        }
    }

    @Override
    public void resetMemory() {
        nameStore.resetMemory();
    }

    @Override
    public void unlockTableName(TableToken tableToken) {
        nameTokenMap.remove(tableToken.getTableName(), LOCKED_TOKEN);
    }
}
