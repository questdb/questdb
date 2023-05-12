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

import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;

public abstract class AbstractTableNameRegistry implements TableNameRegistry {
    // drop marker must contain special symbols to avoid a table created by the same name
    protected final TableNameRegistryFileStore nameStore;
    private ConcurrentHashMap<TableToken> nameTokenMap;
    private ConcurrentHashMap<ReverseTableMapItem> reverseNameTokenMap;

    public AbstractTableNameRegistry(CairoConfiguration configuration) {
        this.nameStore = new TableNameRegistryFileStore(configuration);
    }

    @Override
    public void close() {
        nameTokenMap.clear();
        reverseNameTokenMap.clear();
        Misc.free(nameStore);
    }

    @Override
    public TableToken getTableToken(CharSequence tableName) {
        return nameTokenMap.get(tableName);
    }

    @Override
    public TableToken getTableToken(String dirName, int tableId) {
        ReverseTableMapItem rmi = reverseNameTokenMap.get(dirName);
        if (rmi != null && !rmi.isDropped() && rmi.getToken().getTableId() == tableId) {
            return rmi.getToken();
        }
        return null;
    }

    @Override
    public void getTableTokens(ObjHashSet<TableToken> target, boolean includeDropped) {
        target.clear();
        for (ReverseTableMapItem entry : reverseNameTokenMap.values()) {
            if (includeDropped || !entry.isDropped()) {
                target.add(entry.getToken());
            }
        }
    }

    @Override
    public TableToken getTokenByDirName(CharSequence dirName) {
        ReverseTableMapItem entry = reverseNameTokenMap.get(dirName);
        return entry == null ? null : entry.getToken();
    }

    @Override
    public boolean isTableDropped(TableToken tableToken) {
        ReverseTableMapItem rmi = reverseNameTokenMap.get(tableToken.getDirName());
        return rmi != null && rmi.isDropped();
    }

    @Override
    public void resetMemory() {
        nameStore.resetMemory();
    }

    void setNameMaps(
            ConcurrentHashMap<TableToken> nameTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> reverseTableNameTokenMap) {
        this.nameTokenMap = nameTableTokenMap;
        this.reverseNameTokenMap = reverseTableNameTokenMap;
    }
}
