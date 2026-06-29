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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValuesIndexedFilteredRecordCursor extends AbstractPageFrameRecordCursor {
    private final int columnIndex;
    private final IntHashSet deferredSymbolKeys;
    private final Function filter;
    private final IntHashSet found = new IntHashSet();
    private final DirectLongList rows;
    private final IntHashSet symbolKeys;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long index;
    private boolean isTreeMapBuilt;
    private int keyCount;
    private long lim;

    public LatestByValuesIndexedFilteredRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            int columnIndex,
            DirectLongList rows,
            @NotNull IntHashSet symbolKeys,
            @Nullable IntHashSet deferredSymbolKeys,
            Function filter
    ) {
        super(configuration, metadata);
        this.rows = rows;
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbolKeys = deferredSymbolKeys;
        this.filter = filter;
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
        if (!isTreeMapBuilt) {
            buildTreeMap();
            isTreeMapBuilt = true;
        }
        if (index < lim) {
            long rowId = rows.get(index++);
            // We inverted frame indexes when posting tasks.
            final int frameIndex = Rows.MAX_SAFE_PARTITION_INDEX - Rows.toPartitionIndex(rowId);
            frameMemoryPool.navigateTo(frameIndex, recordA);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            return true;
        }
        return false;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.frameCursor = pageFrameCursor;
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        filter.init(pageFrameCursor, executionContext);
        circuitBreaker = executionContext.getCircuitBreaker();
        rows.setMemoryTracker(executionContext.getMemoryTracker());
        rows.reopen();
        found.clear();
        keyCount = -1;
        isTreeMapBuilt = false;
        // prepare for page frame iteration
        super.init(executionContext.getMemoryTracker());
    }

    @Override
    public long preComputedStateSize() {
        return isTreeMapBuilt ? 1 : 0;
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? lim : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
        sink.optAttr("filter", filter);
    }

    @Override
    public void toTop() {
        index = 0;
        filter.toTop();
    }

    private void addFoundKey(int symbolKey, IndexReader indexReader, int frameIndex, long partitionLo, long partitionHi) {
        int index = found.keyIndex(symbolKey);
        if (index > -1) {
            try (RowCursor cursor = indexReader.getCursor(symbolKey, partitionLo, partitionHi)) {
                while (cursor.hasNext()) {
                    // cursor.next() is already frame-relative (BitmapIndex*Reader subtracts
                    // minValue == partitionLo). Do not subtract partitionLo again, or the record
                    // is positioned partitionLo rows too early when partitionLo > 0.
                    final long row = cursor.next();
                    recordA.setRowIndex(row);
                    if (filter.getBool(recordA)) {
                        rows.add(Rows.toRowID(frameIndex, row));
                        found.addAt(index, symbolKey);
                        break;
                    }
                }
            }
        }
    }

    private void buildTreeMap() {
        if (keyCount < 0) {
            // The found.size() == keyCount early exit (and the dropped per-frame overlap guard) relies on
            // symbolKeys and deferredSymbolKeys being disjoint; a duplicate would inflate keyCount and stop
            // the exit from ever firing (full scan). The deduping is done by the factory, so guard the
            // invariant here against a future caller that wires these sets up directly.
            assert keysDisjoint(symbolKeys, deferredSymbolKeys)
                    : "deferredSymbolKeys must be deduped against symbolKeys (see AbstractDeferredTreeSetRecordCursorFactory.initRecordCursor)";
            keyCount = symbolKeys.size();
            if (deferredSymbolKeys != null) {
                keyCount += deferredSymbolKeys.size();
            }
        }

        PageFrame frame;
        while (found.size() < keyCount && (frame = frameCursor.next()) != null) {
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
            for (int i = 0, n = symbolKeys.size(); i < n; i++) {
                int symbolKey = symbolKeys.get(i);
                addFoundKey(symbolKey, indexReader, invertedFrameIndex, partitionLo, partitionHi);
            }
            if (deferredSymbolKeys != null) {
                // deferredSymbolKeys is deduped against symbolKeys at factory level
                // (AbstractDeferredTreeSetRecordCursorFactory.initRecordCursor), so no overlap guard
                // is needed here; addFoundKey is idempotent regardless.
                for (int i = 0, n = deferredSymbolKeys.size(); i < n; i++) {
                    int symbolKey = deferredSymbolKeys.get(i);
                    addFoundKey(symbolKey, indexReader, invertedFrameIndex, partitionLo, partitionHi);
                }
            }
        }

        rows.sortAsUnsigned();
        index = 0;
        lim = rows.size();
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
}
