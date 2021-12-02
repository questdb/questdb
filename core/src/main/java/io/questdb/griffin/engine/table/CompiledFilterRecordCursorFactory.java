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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

public class CompiledFilterRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory factory;
    private final CompiledFilter filter;
    private final CompiledFilterRecordCursor cursor;
    private final DirectLongList rows;
    private final DirectLongList columns;

    public CompiledFilterRecordCursorFactory(@NotNull RecordCursorFactory factory, @NotNull IntList columnIndexes, @NotNull CompiledFilter filter) {
        assert !(factory instanceof FilteredRecordCursorFactory);
        assert !(factory instanceof CompiledFilterRecordCursorFactory);
        this.factory = factory;
        this.filter = filter;
        this.cursor = new CompiledFilterRecordCursor(columnIndexes);
        this.rows = new DirectLongList(1024);
        this.columns = new DirectLongList(16);
    }

    @Override
    public void close() {
        factory.close();
        filter.close();
        rows.close();
        columns.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        this.cursor.of(factory, filter, rows, columns, executionContext);
        return this.cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return factory.getMetadata();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }
}
