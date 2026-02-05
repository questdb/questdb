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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for single-threaded read_parquet() SQL function.
 */
public class ReadParquetRecordCursorFactory extends ProjectableRecordCursorFactory {
    private ReadParquetRecordCursor cursor;
    private Path path;
    private @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;

    public ReadParquetRecordCursorFactory(@Transient Path path, RecordMetadata metadata) {
        super(metadata);
        this.path = new Path().of(path);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            final CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
            cursor = new ReadParquetRecordCursor(configuration.getFilesFacade(), getMetadata(), pushdownFilterConditions);
        }
        try {
            cursor.of(path.$(), executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean mayHasParquetFormatPartition(SqlExecutionContext executionContext) {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions) {
        this.pushdownFilterConditions = pushdownFilterConditions;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("parquet file sequential scan");
        sink.attr("columns").val(getMetadata());
    }

    @Override
    protected void _close() {
        cursor = Misc.free(cursor);
        path = Misc.free(path);
        Misc.freeObjList(pushdownFilterConditions);
    }
}
