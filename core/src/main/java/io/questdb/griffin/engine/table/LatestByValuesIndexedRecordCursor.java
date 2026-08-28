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
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValuesIndexedRecordCursor extends AbstractPageFrameRecordCursor {
    private final int columnIndex;
    private final IntHashSet deferredSymbolKeys;
    private final IntList remainingKeys = new IntList();
    private final DirectLongList rows;
    private final IntHashSet symbolKeys;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long index = 0;
    private boolean isTreeMapBuilt;

    public LatestByValuesIndexedRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            int columnIndex,
            @NotNull IntHashSet symbolKeys,
            @Nullable IntHashSet deferredSymbolKeys,
            DirectLongList rows
    ) {
        super(configuration, metadata);
        this.rows = rows;
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbolKeys = deferredSymbolKeys;
    }

    @Override
    public void close() {
        // Free the shared rows list under the per-query tracker bound in of().
        rows.close();
        super.close();
    }

    @Override
    public boolean hasNext() {
        circuitBreaker.statefulThrowExceptionIfTripped();
        buildTreeMapConditionally();
        if (index < rows.size()) {
            final long rowId = rows.get(index++);
            // Undo the partition-index inversion applied in addFoundKey().
            final int frameIndex = Rows.MAX_SAFE_PARTITION_INDEX - Rows.toPartitionIndex(rowId);
            frameMemoryPool.navigateTo(frameIndex, recordA);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            return true;
        }
        return false;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) {
        this.frameCursor = pageFrameCursor;
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        rows.setMemoryTracker(executionContext.getMemoryTracker());
        rows.reopen();
        remainingKeys.clear();
        isTreeMapBuilt = false;
        // prepare for page frame iteration
        super.init(executionContext.getMemoryTracker());
    }

    @Override
    public long preComputedStateSize() {
        return (isTreeMapBuilt ? 1 : 0) + rows.size();
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? rows.size() : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
    }

    @Override
    public void toTop() {
        index = 0;
    }

    private static boolean keysDisjoint(IntHashSet symbolKeys, @Nullable IntHashSet deferredSymbolKeys) {
        if (deferredSymbolKeys != null) {
            for (int i = 0, n = deferredSymbolKeys.size(); i < n; i++) {
                if (symbolKeys.contains(deferredSymbolKeys.get(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean addFoundKey(int symbolKey, IndexReader indexReader, int frameIndex, long partitionLo, long partitionHi) {
        try (RowCursor cursor = indexReader.getCursor(symbolKey, partitionLo, partitionHi)) {
            if (cursor.hasNext()) {
                rows.add(Rows.toRowID(frameIndex, cursor.next()));
                return true;
            }
        }
        return false;
    }

    private void buildTreeMap() {
        // remainingKeys drives both per-frame iteration and the early-exit condition below, so a
        // duplicate between symbolKeys and deferredSymbolKeys would be probed twice per frame instead
        // of once. The deduping is done by the factory
        // (AbstractDeferredTreeSetRecordCursorFactory.initRecordCursor); assert the invariant here, and
        // defensively skip a duplicate below too, in case a future caller wires these sets up directly.
        assert keysDisjoint(symbolKeys, deferredSymbolKeys)
                : "deferredSymbolKeys must be deduped against symbolKeys (see AbstractDeferredTreeSetRecordCursorFactory.initRecordCursor)";
        remainingKeys.clear();
        for (int i = 0, n = symbolKeys.size(); i < n; i++) {
            remainingKeys.add(symbolKeys.get(i));
        }
        if (deferredSymbolKeys != null) {
            for (int i = 0, n = deferredSymbolKeys.size(); i < n; i++) {
                int symbolKey = deferredSymbolKeys.get(i);
                if (!symbolKeys.contains(symbolKey)) {
                    remainingKeys.add(symbolKey);
                }
            }
        }

        PageFrame frame;
        while (remainingKeys.size() > 0 && (frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final IndexReader indexReader = frame.getIndexReader(columnIndex, IndexReader.DIR_BACKWARD);
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            // Invert page frame indexes, so that they grow asc in time order.
            // That's to be able to do post-processing (sorting) of the result set.
            final int invertedFrameIndex = Rows.MAX_SAFE_PARTITION_INDEX - frameIndex;
            // Iterate backward with swap-remove: a found key is replaced by the current last entry and
            // the list shrinks, so the next (older) frame only probes keys that are still unresolved.
            for (int i = remainingKeys.size() - 1; i >= 0; i--) {
                int symbolKey = remainingKeys.getQuick(i);
                if (addFoundKey(symbolKey, indexReader, invertedFrameIndex, partitionLo, partitionHi)) {
                    int last = remainingKeys.size() - 1;
                    remainingKeys.setQuick(i, remainingKeys.getQuick(last));
                    remainingKeys.setPos(last);
                }
            }
        }

        // Sort the collected row ids (which carry inverted partition indexes) ascending so the cursor emits
        // in ascending designated-timestamp order, matching the FORWARD scan direction the factory reports.
        rows.sortAsUnsigned();
        index = 0;
    }

    private void buildTreeMapConditionally() {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            isTreeMapBuilt = true;
        }
    }
}
