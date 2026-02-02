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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

abstract class AbstractDescendingRecordListCursor extends AbstractPageFrameRecordCursor {
    protected final DirectLongList rows;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected boolean isOpen;
    private boolean isTreeMapBuilt;
    private long rowIndex;

    public AbstractDescendingRecordListCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @Nullable DirectLongList rows
    ) {
        super(configuration, metadata);
        this.rows = rows;
        this.isOpen = true;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            rowIndex = rows.size() - 1;
            isTreeMapBuilt = true;
        }

        if (rowIndex > -1) {
            counter.add(rowIndex + 1);
            rowIndex = -1;
        }
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
            rowIndex = rows.size() - 1;
            isTreeMapBuilt = true;
        }
        if (rowIndex > -1) {
            long rowId = rows.get(rowIndex--);
            frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId), recordA);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            return true;
        }
        return false;
    }

    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.frameCursor = pageFrameCursor;
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        rows.clear();
        isTreeMapBuilt = false;
        isOpen = true;
        // prepare for page frame iteration
        super.init();
    }

    @Override
    public long preComputedStateSize() {
        return isTreeMapBuilt ? 1 : 0;
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? rows.size() : -1;
    }

    @Override
    public void skipRows(Counter rowCount) {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            rowIndex = rows.size() - 1;
            isTreeMapBuilt = true;
        }

        if (rowIndex > -1) {
            long rowsLeft = rowIndex + 1;
            long rowsToSkip = Math.min(rowsLeft, rowCount.get());

            rowCount.dec(rowsToSkip);
            rowIndex -= rowsToSkip;
        }
    }

    @Override
    public void toTop() {
        rowIndex = rows.size() - 1;
    }

    abstract protected void buildTreeMap();
}
