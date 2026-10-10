/*+*****************************************************************************
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

package io.questdb.griffin.bind;

import io.questdb.cairo.TableToken;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The target of the UPDATE the binder bound last: its table, as the statement names it and as it resolved, and the
 * table columns its SET clause assigns, in assignment order.
 */
public final class UpdateTarget implements Mutable {
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private long metadataVersion;
    private int tableId;
    private CharSequence tableName;
    private int tablePosition;
    private TableToken tableToken;

    @Override
    public void clear() {
        columnNames.clear();
        metadataVersion = 0;
        tableId = 0;
        tableName = null;
        tablePosition = 0;
        tableToken = null;
    }

    public ObjList<CharSequence> getColumnNames() {
        return columnNames;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    public int getTableId() {
        return tableId;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public int getTablePosition() {
        return tablePosition;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    void of(CharSequence tableName, int tablePosition, TableToken tableToken, int tableId, long metadataVersion) {
        this.tableName = tableName;
        this.tablePosition = tablePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.metadataVersion = metadataVersion;
    }
}
