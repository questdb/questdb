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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import org.jetbrains.annotations.NotNull;

class LatestByValueIndexedFilteredRecordCursor extends AbstractLatestByValueRecordCursor {
    private final Function filter;
    private SqlExecutionCircuitBreaker circuitBreaker;

    public LatestByValueIndexedFilteredRecordCursor(
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
    public boolean hasNext() {
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
        super.init();
    }

    @Override
    public void setSymbolKey(int symbolKey) {
        super.setSymbolKey(TableUtils.toIndexKey(symbolKey));
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
        sink.optAttr("filter", filter);
    }

    @Override
    public void toTop() {
        hasNext = isRecordFound;
        filter.toTop();
    }

    private void findRecord() {
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            RowCursor cursor = indexReader.getCursor(false, symbolKey, partitionLo, partitionHi);
            while (cursor.hasNext()) {
                recordA.setRowIndex(cursor.next() - partitionLo);
                if (filter.getBool(recordA)) {
                    isRecordFound = true;
                    return;
                }
            }
        }
    }
}
