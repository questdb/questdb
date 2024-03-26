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

import java.util.Map;
import java.util.function.Predicate;

public abstract class AbstractTableNameRegistry implements TableNameRegistry {
    protected final CairoConfiguration configuration;
    // drop marker must contain special symbols to avoid a table created by the same name
    protected final TableNameRegistryStore nameStore;
    protected final Predicate<CharSequence> protectedTableResolver;
    protected ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap;
    protected ConcurrentHashMap<TableToken> tableNameToTableTokenMap;

    public AbstractTableNameRegistry(CairoConfiguration configuration, Predicate<CharSequence> protectedTableResolver) {
        this.configuration = configuration;
        this.nameStore = new TableNameRegistryStore(configuration, protectedTableResolver);
        this.protectedTableResolver = protectedTableResolver;
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
    public TableToken getTableTokenByDirName(String dirName) {
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
    public TableToken getTokenByDirName(CharSequence dirName) {
        ReverseTableMapItem entry = dirNameToTableTokenMap.get(dirName);
        return entry == null ? null : entry.getToken();
    }

    @Override
    public boolean isTableDropped(CharSequence tableDir) {
        ReverseTableMapItem rmi = dirNameToTableTokenMap.get(tableDir);
        return rmi != null && rmi.isDropped();
    }

    @Override
    public synchronized void reconcile() {
        for (Map.Entry<CharSequence, TableToken> e : tableNameToTableTokenMap.entrySet()) {
            TableToken tableNameTableToken = e.getValue();

            if (tableNameTableToken == LOCKED_TOKEN) {
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
            ReverseTableMapItem rtmi = e.getValue();
            TableToken dirToNameToken = rtmi.getToken();
            if (rtmi.isDropped()) {
                TableToken tokenByName = tableNameToTableTokenMap.get(dirToNameToken.getTableName());
                if (tokenByName != null && tokenByName.equals(dirToNameToken)) {
                    throw new IllegalStateException("table " + tokenByName.getTableName()
                            + " is dropped but still present in table name registry");
                }
            } else {
                TableToken tokenByName = tableNameToTableTokenMap.get(dirToNameToken.getTableName());
                if (tokenByName == null) {
                    throw new IllegalStateException("table " + tokenByName.getTableName()
                            + " is not dropped but name is not present in table name registry");
                }

                if (!dirToNameToken.equals(tokenByName)) {
                    throw new IllegalStateException("table " + tokenByName.getTableName() + " tokens mismatch");
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
