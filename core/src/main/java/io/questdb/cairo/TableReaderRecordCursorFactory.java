/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

public class TableReaderRecordCursorFactory extends AbstractRecordCursorFactory {
    private final TableReaderSelectedColumnRecordCursor cursor;
    private final CairoEngine engine;
    private final String tableName;
    private final long tableVersion;
    private final IntList columnIndexes;
    private final IntList columnSizes;
    private TablePageFrameCursor pageFrameCursor = null;
    private final boolean framingSupported;

    public TableReaderRecordCursorFactory(
            RecordMetadata metadata,
            CairoEngine engine,
            String tableName,
            long tableVersion,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizes,
            boolean framingSupported
    ) {
        super(metadata);
        this.cursor = new TableReaderSelectedColumnRecordCursor(columnIndexes);
        this.engine = engine;
        this.tableName = tableName;
        this.tableVersion = tableVersion;
        this.columnIndexes = columnIndexes;
        this.columnSizes = columnSizes;
        this.framingSupported = framingSupported;
    }

    @Override
    public void close() {
        Misc.free(cursor);
        Misc.free(pageFrameCursor);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName, tableVersion));
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext) {
        if (pageFrameCursor != null) {
            return pageFrameCursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName), Long.MAX_VALUE, -1,
                    columnIndexes, columnSizes);
        } else if (framingSupported) {
            pageFrameCursor = new TablePageFrameCursor();
            return pageFrameCursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName), Long.MAX_VALUE, -1,
                    columnIndexes, columnSizes);
        } else {
            return null;
        }
    }

    @Override
    public boolean supportPageFrameCursor() {
        return framingSupported;
    }
}
