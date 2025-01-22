/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.FwdPageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * Factory for parallel read_parquet() SQL function.
 */
public class ReadParquetPageFrameRecordCursorFactory extends AbstractRecordCursorFactory {
    private final PageFrameRecordCursorImpl cursor;
    private final ReadParquetPageFrameCursor pageFrameCursor;
    private Path path;

    public ReadParquetPageFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @Transient Path path,
            RecordMetadata metadata
    ) {
        super(metadata);
        this.path = new Path().of(path);
        this.cursor = new PageFrameRecordCursorImpl(
                configuration,
                metadata,
                new FwdPageFrameRowCursorFactory(),
                true,
                null
        );
        this.pageFrameCursor = new ReadParquetPageFrameCursor(configuration.getFilesFacade(), metadata);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        pageFrameCursor.of(path.$());
        try {
            cursor.of(pageFrameCursor, executionContext);
            return cursor;
        } catch (Throwable e) {
            pageFrameCursor.close();
            throw e;
        }
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        assert order != ORDER_DESC;
        pageFrameCursor.of(path.$());
        return pageFrameCursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("parquet page frame scan");
    }

    @Override
    protected void _close() {
        Misc.free(cursor);
        Misc.free(pageFrameCursor);
        path = Misc.free(path);
    }
}
