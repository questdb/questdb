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
 * The target of the UPDATE the binder bound last: its table, as the statement names it and as it resolved, the table
 * columns its SET clause assigns, in assignment order, and whether the WAL sequences the statement.
 */
public final class UpdateTarget implements Mutable {
    private final ObjList<CharSequence> columnNames;
    private boolean isSequenced;
    private long metadataVersion;
    private int tableId;
    private CharSequence tableName;
    private int tablePosition;
    private TableToken tableToken;

    /**
     * Records the assigned columns in {@code columnNames}, which no one else writes while the UPDATE binds and
     * authorizes.
     */
    public UpdateTarget(ObjList<CharSequence> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public void clear() {
        isSequenced = false;
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

    /**
     * True when the target is a WAL table and the statement binds outside WAL apply: the WAL sequences the statement
     * and its apply job runs it, so the client compiles it only to validate it, and every scan the statement binds
     * reads no rows, see {@link io.questdb.griffin.plan.logical.ScanPlan#isSequencedUpdate()}, but the scans of a
     * sub-query whose rows binding consumes, which the client runs.
     */
    public boolean isSequenced() {
        return isSequenced;
    }

    void of(CharSequence tableName, int tablePosition, TableToken tableToken, int tableId, long metadataVersion, boolean isSequenced) {
        this.isSequenced = isSequenced;
        this.tableName = tableName;
        this.tablePosition = tablePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.metadataVersion = metadataVersion;
    }
}
