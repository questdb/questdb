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
import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.util.HashMap;


public class TableNameRegistryRO extends AbstractTableNameRegistry {
    private static final Log LOG = LogFactory.getLog(TableNameRegistryRO.class);
    private final long autoReloadTimeout;
    private final MillisecondClock clockMs;
    private volatile long lastReloadTimestampMs = 0;
    private HashMap<CharSequence, String> reverseTableNameCache = new HashMap<>();
    private HashMap<CharSequence, String> reverseTableNameCache2 = new HashMap<>();
    private HashMap<CharSequence, TableNameRecord> systemTableNameCache = new HashMap<>();
    private HashMap<CharSequence, TableNameRecord> systemTableNameCache2 = new HashMap<>();

    public TableNameRegistryRO(CairoConfiguration configuration) {
        super(configuration);
        this.clockMs = configuration.getMillisecondClock();
        long timeout = configuration.getTableRegistryAutoReloadTimeout();
        this.autoReloadTimeout = timeout > 0 ? timeout : Long.MAX_VALUE;
        setNameMaps(systemTableNameCache, reverseTableNameCache);
    }

    @Override
    public void deleteNonWalName(CharSequence tableName, String systemTableName) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public TableNameRecord getTableNameRecord(CharSequence tableName) {
        TableNameRecord record = systemTableNameCache.get(tableName);
        if (record == null && clockMs.getTicks() - lastReloadTimestampMs > autoReloadTimeout) {
            reloadTableNameCache();
            record = systemTableNameCache.get(tableName);
        }
        return record;
    }

    @Override
    public String registerName(String tableName, String systemTableName, boolean isWal) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public synchronized void reloadTableNameCache() {
        LOG.info().$("reloading table to system name mappings").$();

        systemTableNameCache2.clear();
        reverseTableNameCache2.clear();
        tableNameStore.reload(systemTableNameCache2, reverseTableNameCache2, TABLE_DROPPED_MARKER);

        // Swap the maps
        setNameMaps(systemTableNameCache2, reverseTableNameCache2);

        HashMap<CharSequence, TableNameRecord> tmp = systemTableNameCache2;
        systemTableNameCache2 = systemTableNameCache;
        systemTableNameCache = tmp;

        HashMap<CharSequence, String> tmp2 = reverseTableNameCache2;
        reverseTableNameCache2 = reverseTableNameCache;
        reverseTableNameCache = tmp2;

        lastReloadTimestampMs = clockMs.getTicks();
    }

    @Override
    public void removeTableSystemName(CharSequence systemTableName) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public boolean removeWalTableName(CharSequence tableName, String systemTableName) {
        throw CairoException.critical(0).put("instance is read only");
    }

    @Override
    public String rename(CharSequence oldName, CharSequence newName, String systemTableName) {
        throw CairoException.critical(0).put("instance is read only");
    }
}
