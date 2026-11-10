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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * The page frames of a single-symbol index scan's parallel plan: the scan's partitions, as a
 * table scan frames them, wrapped so that the Async Filter over them reads each frame's rows of
 * the symbol from the frame's POSTING index (see {@link IndexKeyPageFrameCursor}). It shares the
 * serial scan's partition frame factory and key function, which the serial scan owns; it offers
 * page frames only.
 */
public class IndexKeyScanFrameFactory extends AbstractRecordCursorFactory {
    private final IntList columnIndexes;
    private final IntList columnSizeShifts;
    private final int keyColumnIndex;
    private final IndexKeyPageFrameCursor indexKeyCursor;
    // borrowed from the serial scan, which frees them
    private final PartitionFrameCursorFactory partitionFrameCursorFactory;
    private final Function symbolFunc;
    private FwdTableReaderPageFrameCursor fwdPageFrameCursor;

    public IndexKeyScanFrameFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            @NotNull Function symbolFunc,
            int keyColumnIndex,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata);
        this.partitionFrameCursorFactory = partitionFrameCursorFactory;
        this.symbolFunc = symbolFunc;
        this.keyColumnIndex = keyColumnIndex;
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        this.indexKeyCursor = new IndexKeyPageFrameCursor(keyColumnIndex);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        assert order == PartitionFrameCursorFactory.ORDER_ASC || order == PartitionFrameCursorFactory.ORDER_ANY;
        final PartitionFrameCursor partitionFrameCursor = partitionFrameCursorFactory.getCursor(executionContext, columnIndexes, PartitionFrameCursorFactory.ORDER_ASC);
        if (fwdPageFrameCursor == null) {
            fwdPageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    partitionFrameCursorFactory.getPushdownFilterConditions(),
                    executionContext.getSharedQueryWorkerCount()
            );
        }
        final PageFrameCursor frames;
        try {
            frames = fwdPageFrameCursor.of(executionContext, partitionFrameCursor);
        } catch (Throwable th) {
            Misc.free(partitionFrameCursor);
            throw th;
        }
        try {
            symbolFunc.init(frames, executionContext);
            final int symbolKey = frames.getSymbolTable(keyColumnIndex).keyOf(symbolFunc.getStrA(null));
            return indexKeyCursor.of(frames, symbolKey == SymbolTable.VALUE_NOT_FOUND ? -1 : TableUtils.toIndexKey(symbolKey));
        } catch (Throwable th) {
            Misc.free(frames);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return SCAN_DIRECTION_FORWARD;
    }

    @Override
    public TableToken getTableToken() {
        return partitionFrameCursorFactory.getTableToken();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("IndexKeyFrames");
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    protected void _close() {
        Misc.free(indexKeyCursor);
        fwdPageFrameCursor = Misc.free(fwdPageFrameCursor);
    }
}
