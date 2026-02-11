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
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;

public class TableNameRegistryRO extends AbstractTableNameRegistry {
    private final long autoReloadTimeout;
    private final MillisecondClock clockMs;
    private ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap1 = new ConcurrentHashMap<>();
    private ConcurrentHashMap<ReverseTableMapItem> dirNameToTableTokenMap2 = new ConcurrentHashMap<>();
    private volatile long lastReloadTimestampMs = 0;
    private ConcurrentHashMap<TableToken> tableNameToTableTokenMap1 = new ConcurrentHashMap<>(false);
    private ConcurrentHashMap<TableToken> tableNameToTableTokenMap2 = new ConcurrentHashMap<>(false);

    public TableNameRegistryRO(CairoEngine engine, TableFlagResolver tableFlagResolver) {
        super(engine, tableFlagResolver);
        this.clockMs = engine.getConfiguration().getMillisecondClock();
        long timeout = engine.getConfiguration().getTableRegistryAutoReloadFrequency();
        this.autoReloadTimeout = timeout > 0 ? timeout : Long.MAX_VALUE;
        setNameMaps(tableNameToTableTokenMap1, dirNameToTableTokenMap1);
    }

    @Override
    public TableToken addTableAlias(String newName, TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public boolean dropTable(TableToken token) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public TableToken getTableToken(CharSequence tableName) {
        TableToken record = super.getTableToken(tableName);
        if (record == null && clockMs.getTicks() - lastReloadTimestampMs > autoReloadTimeout) {
            reloadThrottled();
            return super.getTableToken(tableName);
        }
        return record;
    }

    @Override
    public TableToken lockTableName(String tableName, String dirName, int tableId, boolean isView, boolean isMatView, boolean isWal) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public void purgeToken(TableToken token) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public void registerName(TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public synchronized boolean reload(@Nullable ObjList<TableToken> convertedTables) {
        tableNameToTableTokenMap2.clear();
        dirNameToTableTokenMap2.clear();
        boolean consistent = nameStore.reload(tableNameToTableTokenMap2, dirNameToTableTokenMap2, convertedTables);

        // Swap the maps
        setNameMaps(tableNameToTableTokenMap2, dirNameToTableTokenMap2);

        ConcurrentHashMap<TableToken> tmp = tableNameToTableTokenMap2;
        tableNameToTableTokenMap2 = tableNameToTableTokenMap1;
        tableNameToTableTokenMap1 = tmp;

        ConcurrentHashMap<ReverseTableMapItem> tmp2 = dirNameToTableTokenMap2;
        dirNameToTableTokenMap2 = dirNameToTableTokenMap1;
        dirNameToTableTokenMap1 = tmp2;

        lastReloadTimestampMs = clockMs.getTicks();
        return consistent;
    }

    @Override
    public void removeAlias(TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public TableToken rename(CharSequence oldName, CharSequence newName, TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public void rename(TableToken alias, TableToken replaceWith) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public void unlockTableName(TableToken tableToken) {
        throw CairoException.critical(0).put("instance is read only");
    }

    private synchronized void reloadThrottled() {
        if (clockMs.getTicks() - lastReloadTimestampMs > autoReloadTimeout) {
            reload();
        }
    }
}
