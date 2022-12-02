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
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public abstract class AbstractTableNameRegistry implements TableNameRegistry {
    // drop marker must contain special symbols to avoid a table created by the same name
    protected static final String TABLE_DROPPED_MARKER = "TABLE_DROPPED_MARKER:..";
    protected final TableNameRegistryFileStore nameStore;
    private Map<CharSequence, TableToken> nameTokenMap;
    private Map<TableToken, String> reverseNameTokenMap;

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
    public @NotNull String getTableName(TableToken token) {
        String tableName = reverseNameTokenMap.get(token);

        //noinspection StringEquality
        if (tableName == TABLE_DROPPED_MARKER) {
            // todo: we must throw exception from here because callers are not equipped to deal with NULL
            return null;
        }

        return tableName;
    }

    @Override
    public TableToken getTableToken(CharSequence tableName) {
        return nameTokenMap.get(tableName);
    }

    @Override
    public TableToken getTableToken(String dirName, int tableId) {
        // todo: there is no reliable mechanism to arrive at table name from directory name
        TableToken token = nameTokenMap.get(TableUtils.getTableNameFromDirName(dirName));
        if (token != null && token.getTableId() == tableId) {
            return token;
        }
        return null;
    }

    @Override
    // todo: provide an external bucket to be filled with the desired results otherwise "stashing" the iterator on a "cursor" might lead to unpredictable behaviour 
    public Iterable<TableToken> getTableTokens() {
        return reverseNameTokenMap.keySet();
    }

    @Override
    public boolean isTableDropped(TableToken tableToken) {
        //noinspection StringEquality
        return reverseNameTokenMap.get(tableToken) == TABLE_DROPPED_MARKER;
    }

    public TableToken refreshTableToken(TableToken tableToken) {
        String tableName = reverseNameTokenMap.get(tableToken);
        if (tableName != null) {
            return nameTokenMap.get(tableName);
        }
        return null;
    }

    @Override
    public void resetMemory() {
        nameStore.resetMemory();
    }

    public void setNameMaps(
            Map<CharSequence, TableToken> nameTableTokenMap,
            Map<TableToken, String> reverseTableNameTokenMap) {
        this.nameTokenMap = nameTableTokenMap;
        this.reverseNameTokenMap = reverseTableNameTokenMap;
    }
}
