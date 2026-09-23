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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import org.jetbrains.annotations.NotNull;

class LatestByValueFilteredRecordCursor extends AbstractLatestByValueRecordCursor {
    private final Function filter;

    public LatestByValueFilteredRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            int columnIndex,
            int symbolKey,
            @NotNull Function filter
    ) {
        super(configuration, metadata, columnIndex, symbolKey);
        this.filter = filter;
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            LatestByCompiledFilter.closeCursor(filter);
        }
    }

    @Override
    public boolean hasNext() {
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        if (!isFindPending) {
            findRecord();
            hasNext = isRecordFound;
            isFindPending = true;
        }
        if (hasNext) {
            hasNext = false;
            return true;
        }
        return false;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.frameCursor = pageFrameCursor;
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        filter.init(pageFrameCursor, executionContext);
        isRecordFound = false;
        isFindPending = false;
        // prepare for page frame iteration
        super.init(executionContext.getMemoryTracker());
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public long preComputedStateSize() {
        return isFindPending ? 1 : 0;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Row backward scan");
        sink.attr("symbolFilter").putColumnName(columnIndex).val('=').val(symbolKey);
        sink.attr("filter").val(filter);
    }

    @Override
    public void toTop() {
        hasNext = isRecordFound;
        filter.toTop();
    }

    private void findRecord() {
        // The reserved NULL key does not imply that this snapshot contains a NULL.
        if (symbolKey == SymbolTable.VALUE_IS_NULL && !frameCursor.getSymbolTable(columnIndex).containsNullValue()) {
            return;
        }
        PageFrame frame;
        OUT:
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);
            for (long batchHi = partitionHi - partitionLo + 1; batchHi > 0; ) {
                long batchLo = Math.max(0, batchHi - LatestByCompiledFilter.BATCH_SIZE);
                final DirectLongList matches = LatestByCompiledFilter.apply(
                        filter, frameMemoryPool, frameAddressCache, frameIndex, batchLo, batchHi
                );
                if (matches == null) {
                    // Unsupported frames retain the original whole-frame Java scan.
                    batchLo = 0;
                }
                final long rowCount = matches != null ? matches.size() : batchHi;

                for (long iRow = rowCount - 1; iRow >= 0; iRow--) {
                    long row = matches != null ? matches.get(iRow) + batchLo : iRow;
                    recordA.setRowIndex(row);
                    if (matches != null || filter.getBool(recordA)) {
                        int key = recordA.getInt(columnIndex);
                        if (key == symbolKey) {
                            isRecordFound = true;
                            break OUT;
                        }
                    }
                }
                batchHi = batchLo;
                if (batchHi > 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
            }
        }
    }
}
