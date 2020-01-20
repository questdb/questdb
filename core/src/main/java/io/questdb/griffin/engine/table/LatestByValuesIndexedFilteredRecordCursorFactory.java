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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestByValuesIndexedFilteredRecordCursorFactory extends AbstractDeferredTreeSetRecordCursorFactory {

    private final Function filter;
    private final DirectLongList rowidList = new DirectLongList(1024 * 1024);

    public LatestByValuesIndexedFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Transient CharSequenceHashSet keyValues,
            @Transient SymbolMapReader symbolMapReader,
            @Nullable Function filter) {
        super(configuration, metadata, dataFrameCursorFactory, columnIndex, keyValues, symbolMapReader);
        if (filter != null) {
            this.cursor = new LatestByValuesIndexedFilteredRecordCursor(columnIndex, rows, symbolKeys, filter);
        } else {
            this.cursor = new LatestByValuesIndexedRecordCursor(columnIndex, symbolKeys, rowidList);
        }
        this.filter = filter;
    }

    @Override
    public Record newRecord() {
        return cursor.newRecord();
    }

    @Override
    public void close() {
        super.close();
        if (filter != null) {
            filter.close();
        }
        Misc.free(rowidList);
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    @Override
    protected AbstractDataFrameRecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        if (filter != null) {
            AbstractDataFrameRecordCursor cursor = super.getCursorInstance(dataFrameCursor, executionContext);
            filter.init(cursor, executionContext);
            return cursor;
        }

        return super.getCursorInstance(dataFrameCursor, executionContext);
    }
}
