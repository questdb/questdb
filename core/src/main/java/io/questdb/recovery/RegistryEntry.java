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

package io.questdb.recovery;

public final class RegistryEntry {
    private final String dirName;
    private final boolean removed;
    private final int tableId;
    private final String tableName;
    private final int tableType;

    public RegistryEntry(String tableName, String dirName, int tableId, int tableType, boolean removed) {
        this.tableName = tableName;
        this.dirName = dirName;
        this.tableId = tableId;
        this.tableType = tableType;
        this.removed = removed;
    }

    public String getDirName() {
        return dirName;
    }

    public int getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableType() {
        return tableType;
    }

    public boolean isRemoved() {
        return removed;
    }
}
