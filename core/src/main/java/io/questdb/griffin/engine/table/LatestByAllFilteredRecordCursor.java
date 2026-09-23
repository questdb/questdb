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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class LatestByAllFilteredRecordCursor extends AbstractDescendingRecordListCursor {
    protected final Function filter;
    private final Map map;
    private final RecordSink recordSink;

    public LatestByAllFilteredRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull Map map,
            @NotNull DirectLongList rows,
            @NotNull RecordSink recordSink,
            @NotNull Function filter
    ) {
        super(configuration, metadata, rows);
        this.map = map;
        this.recordSink = recordSink;
        this.filter = filter;
    }

    @Override
    public void close() {
        try {
            if (isOpen()) {
                map.close();
                super.close();
            }
        } finally {
            LatestByCompiledFilter.closeCursor(filter);
        }
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        // open before the first allocation so close() frees the map if a later alloc in of() breaches
        isOpen = true;
        map.setMemoryTracker(executionContext.getMemoryTracker());
        map.reopen();
        super.of(pageFrameCursor, executionContext);
        filter.init(pageFrameCursor, executionContext);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Row backward scan");
        sink.attr("filter").val(filter);
    }

    @Override
    protected void buildTreeMap() {
        PageFrame frame;
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
                        MapKey key = map.withKey();
                        key.put(recordA, recordSink);
                        if (key.create()) {
                            rows.add(Rows.toRowID(frameIndex, row));
                        }
                    }
                }
                batchHi = batchLo;
                if (batchHi > 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
            }
        }
        map.clear();
    }
}
