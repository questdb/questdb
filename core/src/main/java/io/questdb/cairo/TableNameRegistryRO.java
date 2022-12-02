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
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.util.HashMap;


public class TableNameRegistryRO extends AbstractTableNameRegistry {
    private final long autoReloadTimeout;
    private final MillisecondClock clockMs;
    private volatile long lastReloadTimestampMs = 0;
    private HashMap<CharSequence, TableToken> nameTableTokenMap = new HashMap<>();
    private HashMap<CharSequence, TableToken> nameTableTokenMap2 = new HashMap<>();
    private HashMap<TableToken, String> reverseTableNameTokenMap = new HashMap<>();
    private HashMap<TableToken, String> reverseTableNameTokenMap2 = new HashMap<>();

    public TableNameRegistryRO(CairoConfiguration configuration) {
        super(configuration);
        this.clockMs = configuration.getMillisecondClock();
        long timeout = configuration.getTableRegistryAutoReloadTimeout();
        this.autoReloadTimeout = timeout > 0 ? timeout : Long.MAX_VALUE;
        setNameMaps(nameTableTokenMap, reverseTableNameTokenMap);
    }

    @Override
    public TableToken getTableToken(CharSequence tableName) {
        TableToken record = nameTableTokenMap.get(tableName);
        if (record == null && clockMs.getTicks() - lastReloadTimestampMs > autoReloadTimeout) {
            reloadTableNameCache();
            record = nameTableTokenMap.get(tableName);
        }
        return record;
    }

    @Override
    public TableToken lockTableName(String tableName, String privateTableName, int tableId, boolean isWal) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public void registerName(TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public synchronized void reloadTableNameCache() {
        nameTableTokenMap2.clear();
        reverseTableNameTokenMap2.clear();
        nameStore.reload(nameTableTokenMap2, reverseTableNameTokenMap2, TABLE_DROPPED_MARKER);

        // Swap the maps
        setNameMaps(nameTableTokenMap2, reverseTableNameTokenMap2);

        HashMap<CharSequence, TableToken> tmp = nameTableTokenMap2;
        nameTableTokenMap2 = nameTableTokenMap;
        nameTableTokenMap = tmp;

        HashMap<TableToken, String> tmp2 = reverseTableNameTokenMap2;
        reverseTableNameTokenMap2 = reverseTableNameTokenMap;
        reverseTableNameTokenMap = tmp2;

        lastReloadTimestampMs = clockMs.getTicks();
    }

    @Override
    public boolean dropTable(CharSequence tableName, TableToken token) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public void purgeToken(TableToken token) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public TableToken rename(CharSequence oldName, CharSequence newName, TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public void unlockTableName(TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }
}
