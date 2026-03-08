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

import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public abstract class AbstractTableNameRegistry implements TableNameRegistry {
    protected final CairoEngine engine;
    // drop marker must contain special symbols to avoid a table created by the same name
    protected final TableNameRegistryStore nameStore;
    protected final TableFlagResolver tableFlagResolver;
    protected ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap;
    protected ConcurrentHashMap<TableToken> tableNameToTableTokenMap;

    public AbstractTableNameRegistry(CairoEngine engine, TableFlagResolver tableFlagResolver) {
        this.engine = engine;
        this.nameStore = new TableNameRegistryStore(engine.configuration, tableFlagResolver);
        this.tableFlagResolver = tableFlagResolver;
    }

    @Override
    public synchronized void close() {
        tableNameToTableTokenMap.clear();
        dirNameToTableTokenMap.clear();
        Misc.free(nameStore);
    }

    @Override
    public TableToken getTableToken(CharSequence tableName) {
        return tableNameToTableTokenMap.get(tableName);
    }

    @Override
    public TableToken getTableTokenByDirName(CharSequence dirName) {
        ReverseTableMapItem rmi = dirNameToTableTokenMap.get(dirName);
        if (rmi != null && !rmi.isDropped()) {
            return rmi.getToken();
        }
        return null;
    }

    @Override
    public int getTableTokenCount(boolean includeDropped) {
        int count = 0;
        for (ReverseTableMapItem entry : dirNameToTableTokenMap.values()) {
            if (includeDropped || !entry.isDropped()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void getTableTokens(ObjHashSet<TableToken> target, boolean includeDropped) {
        target.clear();
        for (ReverseTableMapItem entry : dirNameToTableTokenMap.values()) {
            if (includeDropped || !entry.isDropped()) {
                target.add(entry.getToken());
            }
        }
    }

    @Override
    public @Nullable TableToken getTokenByDirName(CharSequence dirName) {
        ReverseTableMapItem entry = dirNameToTableTokenMap.get(dirName);
        return entry == null ? null : entry.getToken();
    }

    @Override
    public boolean isTableDropped(TableToken tableToken) {
        if (tableToken.isWal()) {
            return isWalTableDropped(tableToken.getDirName());
        }
        TableToken currentTableToken = tableNameToTableTokenMap.get(tableToken.getTableName());
        return currentTableToken == LOCKED_DROP_TOKEN;
    }

    @Override
    public boolean isWalTableDropped(CharSequence tableDir) {
        ReverseTableMapItem rmi = dirNameToTableTokenMap.get(tableDir);
        return rmi != null && rmi.isDropped();
    }

    @Override
    public synchronized void reconcile() {
        for (Map.Entry<CharSequence, TableToken> e : tableNameToTableTokenMap.entrySet()) {
            TableToken tableNameTableToken = e.getValue();

            if (TableNameRegistry.isLocked(tableNameTableToken)) {
                continue;
            }

            ReverseTableMapItem dirNameTableToken = dirNameToTableTokenMap.get(tableNameTableToken.getDirName());

            if (dirNameTableToken == null) {
                throw new IllegalStateException("table " + tableNameTableToken + " does not have directory mapping");
            }

            if (dirNameTableToken.isDropped()) {
                throw new IllegalStateException("table " + tableNameTableToken + " should not be in dropped state");
            }

            if (dirNameTableToken.getToken().getTableId() != tableNameTableToken.getTableId()) {
                throw new IllegalStateException("table " + tableNameTableToken + " ID mismatch");
            }
        }
        for (Map.Entry<CharSequence, ReverseTableMapItem> e : dirNameToTableTokenMap.entrySet()) {
            ReverseTableMapItem item = e.getValue();
            TableToken dirToNameToken = item.getToken();
            TableToken tokenByName = tableNameToTableTokenMap.get(dirToNameToken.getTableName());
            if (item.isDropped()) {
                if (tokenByName != null && tokenByName.equals(dirToNameToken)) {
                    throw new IllegalStateException("table " + tokenByName.getTableName()
                            + " is dropped but still present in table name registry");
                }
            } else {
                if (tokenByName == null) {
                    throw new IllegalStateException("table " + dirToNameToken.getTableName()
                            + " is not dropped but name is not present in table name registry");
                }

                if (!dirToNameToken.equals(tokenByName)) {
                    throw new IllegalStateException("table " + dirToNameToken.getTableName() + " tokens mismatch");
                }
            }
        }
    }

    @Override
    public synchronized void resetMemory() {
        nameStore.resetMemory();
    }

    void setNameMaps(
            ConcurrentHashMap<TableToken> tableNameToTableTokenMap,
            ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap
    ) {
        this.tableNameToTableTokenMap = tableNameToTableTokenMap;
        this.dirNameToTableTokenMap = dirNameToTableTokenMap;
    }
}
