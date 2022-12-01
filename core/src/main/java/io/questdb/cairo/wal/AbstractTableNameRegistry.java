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
import io.questdb.cairo.TableNameRegistry;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Misc;

import java.util.Map;

public abstract class AbstractTableNameRegistry implements TableNameRegistry {
    protected static final String TABLE_DROPPED_MARKER = "TABLE_DROPPED_MARKER:..";
    protected final TableNameRegistryFileStore tableNameStore;
    private Map<CharSequence, TableToken> nameTableTokenMap;
    private Map<TableToken, String> reverseTableNameTokenMap;

    public AbstractTableNameRegistry(CairoConfiguration configuration) {
        this.tableNameStore = new TableNameRegistryFileStore(configuration);
    }

    @Override
    public void close() {
        nameTableTokenMap.clear();
        reverseTableNameTokenMap.clear();
        Misc.free(tableNameStore);
    }

    @Override
    public String getTableNameByTableToken(TableToken tableToken) {
        String tableName = reverseTableNameTokenMap.get(tableToken);

        //noinspection StringEquality
        if (tableName == TABLE_DROPPED_MARKER) {
            return null;
        }

        return tableName;
    }

    @Override
    public TableToken getTableToken(CharSequence tableName) {
        return nameTableTokenMap.get(tableName);
    }

    @Override
    public TableToken getTableTokenByPrivateTableName(String privateTableName, int tableId) {
        TableToken token = nameTableTokenMap.get(TableUtils.toTableNameFromPrivateName(privateTableName));
        if (token != null && token.getTableId() == tableId) {
            return token;
        }
        return null;
    }

    @Override
    public Iterable<TableToken> getTableTokens() {
        return reverseTableNameTokenMap.keySet();
    }

    @Override
    public boolean isTableDropped(TableToken tableToken) {
        //noinspection StringEquality
        return reverseTableNameTokenMap.get(tableToken) == TABLE_DROPPED_MARKER;
    }

    @Override
    public void resetMemory() {
        tableNameStore.resetMemory();
    }

    public void setNameMaps(
            Map<CharSequence, TableToken> nameTableTokenMap,
            Map<TableToken, String> reverseTableNameTokenMap) {
        this.nameTableTokenMap = nameTableTokenMap;
        this.reverseTableNameTokenMap = reverseTableNameTokenMap;
    }
}
