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
import io.questdb.std.IntList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class PageFrameRecordCursorImpl extends AbstractPageFrameRecordCursor {
    private final boolean entityCursor;
    private final Function filter;
    private final RowCursorFactory rowCursorFactory;
    private boolean areCursorsPrepared;
    private int frameCount = 0;
    private boolean isSkipped;
    private RowCursor rowCursor;

    public PageFrameRecordCursorImpl(
            CairoConfiguration configuration,
            @Transient RecordMetadata metadata,
            RowCursorFactory rowCursorFactory,
            boolean entityCursor,
            // this cursor owns "toTop()" lifecycle of filter
            @Nullable Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(configuration, metadata, columnIndexes);
        this.rowCursorFactory = rowCursorFactory;
        this.entityCursor = entityCursor;
        this.filter = filter;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor.getTableReader());
            areCursorsPrepared = true;
        }

        if (filter != null || rowCursorFactory.isUsingIndex()) {
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
            rowCursorFactory.prepareCursor(frameCursor.getTableReader());
            areCursorsPrepared = true;
        }

        try {
            if (rowCursor != null && rowCursor.hasNext()) {
                final long rowIndex = rowCursor.next();
                recordA.init(frameMemory);
                recordA.setRowIndex(rowIndex);
                return true;
            }

            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                rowCursor = rowCursorFactory.getCursor(frame);
                if (rowCursor.hasNext()) {
                    frameAddressCache.add(frameCount, frame);
                    frameMemory = frameMemoryPool.navigateTo(frameCount++);

                    final long rowIndex = rowCursor.next();
                    recordA.init(frameMemory);
                    recordA.setRowIndex(rowIndex);
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
        recordA.of(frameCursor.getTableReader());
        recordB.of(frameCursor.getTableReader());
        rowCursorFactory.init(frameCursor.getTableReader(), sqlExecutionContext);
        rowCursor = null;
        areCursorsPrepared = false;
        frameCount = 0;
        // prepare for page frame iteration
        super.toTop();
    }

    @Override
    public long size() {
        if (entityCursor) {
            // TODO(puzpuzpuz): this may mmap column files; consider keeping data frame-based size calculation here
            long size = 0;
            PageFrame pageFrame;
            while ((pageFrame = frameCursor.next()) != null) {
                size += pageFrame.getPartitionHi() - pageFrame.getPartitionLo();
            }
            frameCursor.toTop();
            return size;
        }
        return -1;
    }

    @Override
    public void skipRows(Counter rowCount) {
        if (isSkipped) {
            return;
        }

        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor.getTableReader());
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
            frameAddressCache.add(frameCount, pageFrame);
            frameMemory = frameMemoryPool.navigateTo(frameCount++);

            long frameSize = pageFrame.getPartitionHi() - pageFrame.getPartitionLo();
            if (frameSize > skipToPosition) {
                rowCount.dec(skipToPosition);
                break;
            }
            rowCount.dec(frameSize);
            skipToPosition -= frameSize;
        }

        isSkipped = true;
        // page frame is null when table has no partitions so there's nothing to skip
        if (pageFrame != null) {
            rowCursor = rowCursorFactory.getCursor(pageFrame);

            recordA.init(frameMemory);
            // move to frame, rowlo doesn't matter
            recordA.setRowIndex(0);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Data frame scan");
    }

    @Override
    public void toTop() {
        if (filter != null) {
            filter.toTop();
        }
        frameCursor.toTop();
        frameCount = 0;
        rowCursor = null;
        isSkipped = false;
    }
}
