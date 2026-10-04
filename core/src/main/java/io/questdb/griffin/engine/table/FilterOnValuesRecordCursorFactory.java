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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

public class FilterOnValuesRecordCursorFactory extends AbstractPageFrameRecordCursorFactory implements KeyMajorScanFactory {
    private static final Comparator<FunctionBasedRowCursorFactory> COMPARATOR = FilterOnValuesRecordCursorFactory::compareStrFunctions;
    private static final Comparator<FunctionBasedRowCursorFactory> COMPARATOR_DESC = FilterOnValuesRecordCursorFactory::compareStrFunctionsDesc;
    private final int columnIndex;
    private final int[] cursorFactoriesIdx;
    private final boolean followedOrderByAdvice;
    private final boolean heapCursorUsed;
    private final int orderDirection;
    private AbstractPageFrameRecordCursor cursor;
    private ObjList<FunctionBasedRowCursorFactory> cursorFactories;
    private Function filter;
    private RowCursorFactory rowCursorFactory;

    public FilterOnValuesRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            @NotNull @Transient ObjList<Function> keyValues,
            int columnIndex,
            @NotNull @Transient TableReader reader,
            @Nullable Function filter,
            int orderByMnemonic,
            boolean orderByKeyColumn,
            boolean orderByTimestamp,
            int orderDirection,
            int indexDirection,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);

        final int nKeyValues = keyValues.size();
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.orderDirection = orderDirection;
        cursorFactories = new ObjList<>(nKeyValues);
        cursorFactoriesIdx = new int[]{0};
        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndexes.getQuick(columnIndex));
        for (int i = 0; i < nKeyValues; i++) {
            final Function symbol = keyValues.get(i);
            if (symbol.isConstant()) {
                addSymbolKey(symbolMapReader.keyOf(symbol.getStrA(null)), symbol, indexDirection);
            } else {
                addSymbolKey(SymbolTable.VALUE_NOT_FOUND, symbol, indexDirection);
            }
        }
        if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT && !orderByTimestamp) {
            heapCursorUsed = false;
            final SequentialRowCursorFactory sequentialFactory = new SequentialRowCursorFactory(
                    cursorFactories,
                    cursorFactoriesIdx,
                    columnIndex,
                    indexDirection
            );
            rowCursorFactory = sequentialFactory;
            if (orderByKeyColumn) {
                // ORDER BY the key column: walk each key across all page frames, not just
                // within one, so that the output is in key order as a whole
                cursor = new KeyMajorPageFrameRecordCursor(
                        configuration,
                        metadata,
                        sequentialFactory,
                        partitionFrameCursorFactory.getOrder(),
                        filter
                );
            } else {
                cursor = new PageFrameRecordCursorImpl(configuration, metadata, rowCursorFactory, false, filter);
            }
        } else {
            heapCursorUsed = true;
            rowCursorFactory = new HeapRowCursorFactory(cursorFactories, cursorFactoriesIdx);
            cursor = new PageFrameRecordCursorImpl(configuration, metadata, rowCursorFactory, false, filter);
        }
        // the heap cursor merges keys into row order, so it never follows ORDER BY the key column
        this.followedOrderByAdvice = (orderByKeyColumn && !heapCursorUsed) || orderByTimestamp;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followedOrderByAdvice;
    }

    @Override
    public int getKeyMajorColumnIndex() {
        return cursor instanceof KeyMajorPageFrameRecordCursor ? columnIndex : -1;
    }

    @Override
    public int getKeyMajorKeyCount() {
        return cursorFactories.size();
    }

    @Override
    public int getScanDirection() {
        if (partitionFrameCursorFactory.getOrder() == PartitionFrameCursorFactory.ORDER_ASC && heapCursorUsed) {
            return SCAN_DIRECTION_FORWARD;
        }
        return SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("FilterOnValues");
        if (!heapCursorUsed) { // sorting symbols makes no sense for heap factory
            sink.meta("symbolOrder").val(followedOrderByAdvice && orderDirection == IQueryModel.ORDER_DIRECTION_ASCENDING ? "asc" : "desc");
        }
        if (cursor instanceof KeyMajorPageFrameRecordCursor) {
            sink.attr("keyMajor").val(true);
        }
        sink.child(rowCursorFactory);
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    private static int compareStrFunctions(FunctionBasedRowCursorFactory a, FunctionBasedRowCursorFactory b) {
        return Chars.compare(a.getFunction().getStrA(null), b.getFunction().getStrB(null));
    }

    private static int compareStrFunctionsDesc(FunctionBasedRowCursorFactory a, FunctionBasedRowCursorFactory b) {
        return Chars.compareDescending(a.getFunction().getStrA(null), b.getFunction().getStrB(null));
    }

    private static boolean equals(CharSequence cs1, CharSequence cs2) {
        if (cs1 == null) {
            return cs2 == null;
        } else {
            return cs2 != null && Chars.equals(cs1, cs2);
        }
    }

    private void addSymbolKey(int symbolKey, Function symbolFunction, int indexDirection) {
        final FunctionBasedRowCursorFactory rowCursorFactory;
        if (filter == null) {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                rowCursorFactory = new DeferredSymbolIndexRowCursorFactory(
                        columnIndex,
                        symbolFunction,
                        indexDirection
                );
            } else {
                rowCursorFactory = new SymbolIndexRowCursorFactory(
                        columnIndex,
                        symbolKey,
                        indexDirection,
                        symbolFunction
                );
            }
        } else {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                rowCursorFactory = new DeferredSymbolIndexFilteredRowCursorFactory(
                        columnIndex,
                        symbolFunction,
                        filter,
                        indexDirection
                );
            } else {
                rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(
                        columnIndex,
                        symbolKey,
                        filter,
                        indexDirection,
                        symbolFunction
                );
            }
        }
        cursorFactories.add(rowCursorFactory);
    }

    private void findDuplicates() {
        // Bind variable values may repeat. The list is sorted, so equal values are adjacent:
        // compact the distinct values to the front, in order, and swap each duplicate behind
        // them. The row cursor factories scan only the first cursorFactoriesIdx[0] entries.
        // The duplicates stay in the list, so they are still closed with it.
        final int n = cursorFactories.size();
        int distinct = n > 0 ? 1 : 0;
        for (int i = 1; i < n; i++) {
            if (!equals(symbol(distinct - 1), symbol(i))) {
                if (i != distinct) {
                    final FunctionBasedRowCursorFactory duplicate = cursorFactories.getQuick(distinct);
                    cursorFactories.setQuick(distinct, cursorFactories.getQuick(i));
                    cursorFactories.setQuick(i, duplicate);
                }
                distinct++;
            }
        }
        cursorFactoriesIdx[0] = distinct;
    }

    private CharSequence symbol(int idx) {
        return cursorFactories.get(idx).getFunction().getSymbol(null);
    }

    @Override
    protected void _close() {
        final AbstractPageFrameRecordCursor cursor = this.cursor;
        this.cursor = null;
        final ObjList<FunctionBasedRowCursorFactory> cursorFactories = this.cursorFactories;
        this.cursorFactories = null;
        final Function filter = this.filter;
        this.filter = null;
        final RowCursorFactory rowCursorFactory = this.rowCursorFactory;
        this.rowCursorFactory = null;
        Throwable failure = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeBestEffort(failure, filter);
        failure = Misc.freeBestEffort(failure, rowCursorFactory);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeObjListBestEffort(failure, cursorFactories);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            cursorFactories.getQuick(i).getFunction().init(pageFrameCursor, sqlExecutionContext);
        }

        // sort values to facilitate duplicate removal (even for heap row cursor)
        // sorting here can produce order of cursorFactories different from one shown by explain command       
        if (followedOrderByAdvice && orderDirection == IQueryModel.ORDER_DIRECTION_ASCENDING) {
            cursorFactories.sort(COMPARATOR);
        } else {
            cursorFactories.sort(COMPARATOR_DESC);
        }

        findDuplicates();

        cursor.of(pageFrameCursor, sqlExecutionContext);
        if (filter != null) {
            filter.init(cursor, sqlExecutionContext);
        }
        return cursor;
    }
}
