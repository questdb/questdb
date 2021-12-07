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

package io.questdb.griffin.update;

import io.questdb.cairo.sql.*;
import io.questdb.std.Misc;

import java.io.Closeable;

public class UpdateStatement implements Closeable {
    public final static UpdateStatement EMPTY = new UpdateStatement();
    private final String updateTableName;
    private RecordCursorFactory rowIdFactory;
    private int tableId;
    private long tableVersion;

    public UpdateStatement(
            String tableName,
            RecordCursorFactory rowIdFactory,
            int tableId,
            long tableVersion
    ) {
        this.updateTableName = tableName;
        this.rowIdFactory = rowIdFactory;
        this.tableId = tableId;
        this.tableVersion = tableVersion;
    }

    private UpdateStatement() {
        updateTableName = "";
    }

    @Override
    public void close() {
        rowIdFactory = Misc.free(rowIdFactory);
    }

    public int getTableId() {
        return tableId;
    }

    public long getTableVersion() {
        return tableVersion;
    }

    public RecordCursorFactory getRowIdFactory() {
        return rowIdFactory;
    }

    public CharSequence getUpdateTableName() {
        return updateTableName;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public void setTableVersion(long tableVersion) {
        this.tableVersion = tableVersion;
    }
}
