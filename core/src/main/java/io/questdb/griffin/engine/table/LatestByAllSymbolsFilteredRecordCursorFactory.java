/*+*****************************************************************************
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestByAllSymbolsFilteredRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    private Function filter;

    public LatestByAllSymbolsFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            @NotNull IntList partitionByColumnIndexes,
            @Nullable IntList partitionBySymbolCounts,
            @Nullable Function filter,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(configuration, metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);

        try {
            this.filter = filter;
            this.cursor = new LatestByAllSymbolsFilteredRecordCursor(
                    configuration,
                    metadata,
                    rows,
                    filter,
                    partitionByColumnIndexes,
                    partitionBySymbolCounts
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean usesCompiledFilter() {
        return filter instanceof LatestByCompiledFilter;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestByAllSymbolsFiltered");
        sink.optAttr("filter", ((LatestByAllSymbolsFilteredRecordCursor) cursor).getFilter());
        if (usesCompiledFilter()) {
            sink.attr("jit").val(true);
        }
        sink.child(cursor);
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    protected void _close() {
        final PageFrameRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeBestEffort(failure, cursor);
        final Function filter = this.filter;
        this.filter = null;
        failure = Misc.freeBestEffort(failure, filter);
        CairoException.rethrowCleanupFailure(failure);
    }
}
