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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class EmptyTableRecordCursorFactory extends AbstractRecordCursorFactory {
    private final TableToken tableToken;

    public EmptyTableRecordCursorFactory(RecordMetadata metadata) {
        this(metadata, null);
    }

    public EmptyTableRecordCursorFactory(RecordMetadata metadata, TableToken tableToken) {
        super(metadata);
        this.tableToken = tableToken;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return EmptyTableRecordCursor.INSTANCE;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return this.tableToken == null || this.tableToken == tableToken;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (tableToken != null) {
            sink.type("on").val(": ").val(tableToken.getTableName());
        } else {
            sink.type("Empty table");
        }
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
    }
}
