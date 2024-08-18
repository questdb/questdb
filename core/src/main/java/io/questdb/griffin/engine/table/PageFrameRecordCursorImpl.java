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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

class PageFrameRecordCursorImpl extends AbstractPageFrameRecordCursor {
    private final boolean entityCursor;
    private final Function filter;
    private final RowCursorFactory rowCursorFactory;
    private boolean areCursorsPrepared;
    private boolean isSkipped;
    private RowCursor rowCursor;

    public PageFrameRecordCursorImpl(
            CairoConfiguration configuration,
            @Transient RecordMetadata metadata,
            RowCursorFactory rowCursorFactory,
            boolean entityCursor,
            // this cursor owns "toTop()" lifecycle of filter
            @Nullable Function filter
    ) {
        super(configuration, metadata);
        this.rowCursorFactory = rowCursorFactory;
        this.entityCursor = entityCursor;
        this.filter = filter;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor);
            areCursorsPrepared = true;
        }

        if (!frameCursor.supportsSizeCalculation() || filter != null || rowCursorFactory.isUsingIndex()) {
            while (hasNext()) {
                counter.inc();
            }
            return;
        }

        if (rowCursor != null) {
            while (rowCursor.hasNext()) {
                rowCursor.next();
                counter.inc();
            }
            rowCursor = null;
        }

        frameCursor.calculateSize(counter);
    }

    public RowCursorFactory getRowCursorFactory() {
        return rowCursorFactory;
    }

    @Override
    public boolean hasNext() {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor);
            areCursorsPrepared = true;
        }

        try {
            if (rowCursor != null && rowCursor.hasNext()) {
                final int frameIndex = frameCount - 1;
                final long rowIndex = rowCursor.next();
                frameMemoryPool.navigateTo(frameIndex, recordA);
                recordA.setRowIndex(rowIndex);
                return true;
            }

            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                frameAddressCache.add(frameCount, frame);
                final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameCount++);
                rowCursor = rowCursorFactory.getCursor(frame, frameMemory);
                if (rowCursor.hasNext()) {
                    recordA.init(frameMemory);
                    recordA.setRowIndex(rowCursor.next());
                    return true;
                }
            }
        } catch (NoMoreFramesException ignore) {
            return false;
        }

        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return rowCursorFactory.isUsingIndex();
    }

    @Override
    public void of(PageFrameCursor frameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (this.frameCursor != frameCursor) {
            close();
            this.frameCursor = frameCursor;
        }
        recordA.of(frameCursor);
        recordB.of(frameCursor);
        rowCursorFactory.init(frameCursor, sqlExecutionContext);
        areCursorsPrepared = false;
        rowCursor = null;
        isSkipped = false;
        // prepare for page frame iteration
        super.init();
    }

    @Override
    public long size() {
        return entityCursor ? frameCursor.size() : -1;
    }

    @Override
    public void skipRows(Counter rowCount) {
        if (isSkipped) {
            return;
        }

        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor);
            areCursorsPrepared = true;
        }

        if (filter != null || rowCursorFactory.isUsingIndex()) {
            while (rowCount.get() > 0 && hasNext()) {
                rowCount.dec();
            }
            isSkipped = true;
            return;
        }

        long skipToPosition = rowCount.get();
        PageFrame pageFrame;
        while ((pageFrame = frameCursor.next()) != null) {
            frameAddressCache.add(frameCount++, pageFrame);

            long frameSize = pageFrame.getPartitionHi() - pageFrame.getPartitionLo();
            if (frameSize > skipToPosition) {
                rowCount.dec(skipToPosition);
                break;
            }
            rowCount.dec(frameSize);
            skipToPosition -= frameSize;
        }

        final int frameIndex = frameCount - 1;
        isSkipped = true;
        // page frame is null when table has no partitions so there's nothing to skip
        if (pageFrame != null) {
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            // move to frame, rowlo doesn't matter
            recordA.init(frameMemory);
            recordA.setRowIndex(0);
            rowCursor = rowCursorFactory.getCursor(pageFrame, frameMemory);
            rowCursor.jumpTo(skipToPosition);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Page frame scan");
    }

    @Override
    public void toTop() {
        if (filter != null) {
            filter.toTop();
        }
        areCursorsPrepared = false;
        rowCursor = null;
        isSkipped = false;
        super.toTop();
    }
}
