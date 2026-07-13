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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The index delegate used by {@link AdaptiveSymbolPatternRecordCursorFactory}.
 * The adaptive owner refreshes and costs the effective key list before opening this
 * factory. This delegate only binds those keys to bitmap row cursors; it does not own
 * the key list or enumerate the symbol dictionary.
 */
public class SymbolPatternIndexRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    @TestOnly
    public static final AtomicLong testFallbackInvocations = new AtomicLong();
    @TestOnly
    public static final AtomicLong testIndexInvocations = new AtomicLong();
    private final int columnIndex;
    private final int[] cursorFactoriesIdx = new int[]{0};
    private final IntList effectiveKeys;
    private final PageFrameRecordCursorImpl indexCursor;
    private final int indexDirection;
    private final boolean isHeapCursorUsed;
    private final ObjList<SymbolFunctionRowCursorFactory> perKeyFactories = new ObjList<>();
    private final RowCursorFactory rowCursorFactory;

    public SymbolPatternIndexRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @NotNull IntList effectiveKeys,
            boolean isOrderByTimestamp,
            int indexDirection,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.columnIndex = columnIndex;
        this.effectiveKeys = effectiveKeys;
        this.indexDirection = indexDirection;
        if (isOrderByTimestamp) {
            isHeapCursorUsed = true;
            rowCursorFactory = new HeapRowCursorFactory(perKeyFactories, cursorFactoriesIdx);
        } else {
            isHeapCursorUsed = false;
            rowCursorFactory = new SequentialRowCursorFactory(perKeyFactories, cursorFactoriesIdx);
        }
        indexCursor = new PageFrameRecordCursorImpl(configuration, metadata, rowCursorFactory, false, null);
    }

    @Override
    public int getScanDirection() {
        if (partitionFrameCursorFactory.getOrder() == PartitionFrameCursorFactory.ORDER_ASC && isHeapCursorUsed) {
            return SCAN_DIRECTION_FORWARD;
        }
        return SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @TestOnly
    public static void resetTestCounters() {
        testIndexInvocations.set(0);
        testFallbackInvocations.set(0);
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("SymbolPatternIndex");
        sink.attr("on").putColumnName(columnIndex);
        sink.child(rowCursorFactory);
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    private void buildPerKeyFactories(int keyCount) {
        for (int i = perKeyFactories.size(); i < keyCount; i++) {
            perKeyFactories.add(new SymbolIndexRowCursorFactory(columnIndex, 0, indexDirection, null));
        }
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(rowCursorFactory);
        Misc.free(indexCursor);
        Misc.freeObjList(perKeyFactories);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int n = effectiveKeys.size();
        buildPerKeyFactories(n);
        for (int i = 0; i < n; i++) {
            perKeyFactories.getQuick(i).of(effectiveKeys.getQuick(i));
        }
        cursorFactoriesIdx[0] = n;
        indexCursor.of(pageFrameCursor, executionContext);
        testIndexInvocations.incrementAndGet();
        return indexCursor;
    }
}
