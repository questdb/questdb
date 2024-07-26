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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
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
    public boolean hasNext() {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            isTreeMapBuilt = true;
        }
        if (index < lim) {
            long rowId = rows.get(index++);
            frameMemory = frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId));
            recordA.init(frameMemory);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            return true;
        }
        return false;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.frameCursor = pageFrameCursor;
        recordA.of(pageFrameCursor.getTableReader());
        recordB.of(pageFrameCursor.getTableReader());
        filter.init(this, executionContext);
        circuitBreaker = executionContext.getCircuitBreaker();
        rows.clear();
        found.clear();
        keyCount = -1;
        isTreeMapBuilt = false;
        // prepare for page frame iteration
        super.toTop();
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
    }

    private void addFoundKey(int symbolKey, BitmapIndexReader indexReader, int frameIndex, long partitionLo, long partitionHi) {
        int index = found.keyIndex(symbolKey);
        if (index > -1) {
            RowCursor cursor = indexReader.getCursor(false, symbolKey, partitionLo, partitionHi, true);
            while (cursor.hasNext()) {
                final long row = cursor.next();
                recordA.setRowIndex(row - partitionLo);
                if (filter.getBool(recordA)) {
                    rows.add(Rows.toRowID(frameIndex, row - partitionLo));
                    found.addAt(index, symbolKey);
                    break;
                }
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
            frameMemory = frameMemoryPool.navigateTo(frameCount++);
            recordA.init(frameMemory);

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

        rows.sortAsUnsigned();
        index = 0;
        lim = rows.size();
    }
}
