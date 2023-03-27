/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

abstract class AbstractDescendingRecordListCursor extends AbstractDataFrameRecordCursor {

    protected final DirectLongList rows;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected boolean isOpen;
    private long index;
    private boolean isTreeMapBuilt;

    public AbstractDescendingRecordListCursor(DirectLongList rows, @NotNull IntList columnIndexes) {
        super(columnIndexes);
        this.rows = rows;
        this.isOpen = true;
    }

    @Override
    public void close() {
        this.isOpen = false;
        super.close();
    }

    @Override
    public boolean hasNext() {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            index = rows.size() - 1;
            isTreeMapBuilt = true;
        }
        if (index > -1) {
            long row = rows.get(index--);
            recordA.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            return true;
        }
        return false;
    }

    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.dataFrameCursor = dataFrameCursor;
        recordA.of(dataFrameCursor.getTableReader());
        recordB.of(dataFrameCursor.getTableReader());
        circuitBreaker = executionContext.getCircuitBreaker();
        rows.clear();
        isTreeMapBuilt = false;
        isOpen = true;
    }

    @Override
    public long size() {
        return rows.size();
    }

    @Override
    public void toTop() {
        index = rows.size() - 1;
    }

    abstract protected void buildTreeMap();
}
