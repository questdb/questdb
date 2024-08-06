/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.SimpleReadWriteLock;
import org.jetbrains.annotations.NotNull;


public class CairoMetadata {
    public static final CairoMetadata INSTANCE = new CairoMetadata();
    private final SimpleReadWriteLock lock; // consider StampedLock
    private final CharSequenceObjHashMap<CairoTable> tables;

    public CairoMetadata() {
        this.tables = new CharSequenceObjHashMap<>();
        this.lock = new SimpleReadWriteLock();
    }

    // fails if table already exists
    public void addTable(@NotNull CairoTable newTable) {
        lock.writeLock().lock();
        final String tableName = newTable.getNameUnsafe();
        final CairoTable existingTable = tables.get(tableName);
        if (existingTable != null) {
            throw CairoException.nonCritical().put("already exists in CairoMetadata [table=").put(tableName).put("]");
        }
        tables.put(tableName, newTable);
        lock.writeLock().unlock();
    }

    public CairoTable getTableQuick(@NotNull CharSequence tableName) {
        final CairoTable tbl = getTableQuiet(tableName);
        if (tbl == null) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return tbl;
    }

    public CairoTable getTableQuiet(@NotNull CharSequence tableName) {
        lock.readLock().lock();
        final CairoTable tbl = tables.get(tableName);
        lock.readLock().unlock();
        return tbl;
    }

    public int getTablesCountUnsafe() {
        return tables.size();
    }
}


