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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValuesIndexedRecordCursor extends AbstractPageFrameRecordCursor {
    private final int columnIndex;
    private final IntHashSet deferredSymbolKeys;
    private final IntHashSet found = new IntHashSet();
    private final DirectLongList rows;
    private final IntHashSet symbolKeys;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long index = 0;
    private boolean isTreeMapBuilt;
    private int keyCount;

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
    public boolean hasNext() {
        buildTreeMapConditionally();
        if (index > -1) {
            final long rowId = rows.get(index);
            frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId), recordA);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            index--;
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
        keyCount = -1;
        rows.clear();
        found.clear();
        isTreeMapBuilt = false;
        // prepare for page frame iteration
        super.init();
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? rows.size() : -1;
    }

    @Override
    public long preComputedStateSize() {
        return (isTreeMapBuilt ? 1 : 0) + rows.size();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
    }

    @Override
    public void toTop() {
        index = rows.size() - 1;
    }

    private void addFoundKey(int symbolKey, BitmapIndexReader indexReader, int frameIndex, long partitionLo, long partitionHi) {
        int index = found.keyIndex(symbolKey);
        if (index > -1) {
            RowCursor cursor = indexReader.getCursor(false, symbolKey, partitionLo, partitionHi);
            if (cursor.hasNext()) {
                final long rowId = Rows.toRowID(frameIndex, cursor.next());
                rows.add(rowId);
                found.addAt(index, symbolKey);
            }
        }
    }

    private void buildTreeMap() {
        if (keyCount < 0) {
            keyCount = symbolKeys.size();
            if (deferredSymbolKeys != null) {
                keyCount += deferredSymbolKeys.size();
            }
        }

        PageFrame frame;
        while ((frame = frameCursor.next()) != null && found.size() < keyCount) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (int i = 0, n = symbolKeys.size(); i < n; i++) {
                int symbolKey = symbolKeys.get(i);
                addFoundKey(symbolKey, indexReader, frameIndex, partitionLo, partitionHi);
            }
            if (deferredSymbolKeys != null) {
                for (int i = 0, n = deferredSymbolKeys.size(); i < n; i++) {
                    int symbolKey = deferredSymbolKeys.get(i);
                    if (!symbolKeys.contains(symbolKey)) {
                        addFoundKey(symbolKey, indexReader, frameIndex, partitionLo, partitionHi);
                    }
                }
            }
        }

        index = rows.size() - 1;
    }

    private void buildTreeMapConditionally() {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            isTreeMapBuilt = true;
        }
    }
}
