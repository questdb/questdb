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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public class FilteredRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final FilteredRecordCursor cursor;
    private final Function filter;

    public FilteredRecordCursorFactory(RecordCursorFactory base, Function filter) {
        assert !(base instanceof FilteredRecordCursorFactory);
        this.base = base;
        this.cursor = new FilteredRecordCursor(filter);
        this.filter = filter;
    }

    @Override
    public void close() {
        base.close();
        filter.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursor = base.getCursor(executionContext);
        this.cursor.of(cursor, executionContext);
        return this.cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }
}
