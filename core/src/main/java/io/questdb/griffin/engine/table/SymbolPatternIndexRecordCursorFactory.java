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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Index fast path for a positive {@code LIKE}/{@code ILIKE}/{@code ~} predicate on an indexed,
 * static-symbol-table column. Structurally mirrors {@link FilterOnValuesRecordCursorFactory}, with three
 * differences:
 * <ol>
 *     <li>the set of symbol keys to scan is not fixed at construction: it is read at
 *     {@link #initRecordCursor(PageFrameCursor, SqlExecutionContext)} from a {@link SymbolKeySetProvider}
 *     (the compiled pattern predicate), which computes the matching keys once against the symbol table;</li>
 *     <li>a second, full-scan {@code fallbackCursor} (running {@code fallbackFilter} = the whole original
 *     filter) is chosen when the match set is larger than {@code threshold}, so a low-selectivity pattern
 *     does not degrade into a many-key index merge;</li>
 *     <li>the per-key row cursor factory list is grown lazily to the runtime key count.</li>
 * </ol>
 * SP1 accelerates only the single-threaded, non-page-frame path: {@link #supportsPageFrameCursor()} is
 * {@code false}, so parallel/page-frame consumers keep using the surrounding scan+filter pipeline.
 * <p>
 * <b>Ordering note:</b> this factory does NOT override {@code followedOrderByAdvice()} (stays {@code false}),
 * so advice-driven sort-elision is deferred. Ordering is still correct in every case, by one of two
 * mechanisms: for {@code ORDER BY ts} <i>ascending</i> the outer Sort is elided by {@code generateOrderBy}
 * because {@link #getScanDirection()} advertises {@code SCAN_DIRECTION_FORWARD} (ASC + heap), and correctness
 * then comes from the forward, row-id-ordered {@code HeapRowCursor} merge (row-id order == designated-timestamp
 * order) — exactly as in {@link FilterOnValuesRecordCursorFactory}; for {@code ORDER BY ts DESC} and any other
 * ordering, {@code getScanDirection()} returns {@code SCAN_DIRECTION_OTHER} so the outer Sort is retained and
 * provides the order. The per-key index scan always runs {@code DIR_FORWARD} — passing {@code DIR_BACKWARD}
 * when {@code orderByTimestamp} is set would be inert (no {@code followedOrderByAdvice()} short-circuit to act
 * on it) and therefore misleading.
 * </p>
 */
public class SymbolPatternIndexRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    private final int columnIndex;
    private final int[] cursorFactoriesIdx = new int[]{0};
    private final PageFrameRecordCursorImpl fallbackCursor;
    private final Function fallbackFilter;         // full filter (pattern AND residual)
    private final boolean heapCursorUsed;
    private final PageFrameRecordCursorImpl indexCursor;
    private final int indexDirection;
    private final ObjList<SymbolFunctionRowCursorFactory> perKeyFactories = new ObjList<>();
    private final SymbolKeySetProvider provider;
    private final Function providerFunction;        // == (Function) provider; the compiled pattern predicate
    private final Function residualFilter;          // applied on index rows (nullable)
    private final RowCursorFactory rowCursorFactory;
    private final int threshold;
    @TestOnly
    public static long testFallbackInvocations;
    @TestOnly
    public static long testIndexInvocations;

    public SymbolPatternIndexRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @NotNull Function providerFunction,
            @Nullable Function residualFilter,
            @NotNull Function fallbackFilter,
            boolean orderByTimestamp,
            int indexDirection,
            int threshold,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.columnIndex = columnIndex;
        this.providerFunction = providerFunction;
        this.provider = (SymbolKeySetProvider) providerFunction;
        this.residualFilter = residualFilter;
        this.fallbackFilter = fallbackFilter;
        this.indexDirection = indexDirection;
        this.threshold = threshold;
        // Use the timestamp-merge heap only when the planner explicitly requested timestamp ordering.
        // For unordered queries (ORDER_BY_UNKNOWN) and symbol-key-ordered queries (ORDER_BY_INVARIANT
        // without timestamp), Sequential is cheaper: the outer Sort node (retained because
        // followedOrderByAdvice() is false) will provide any ordering the consumer needs.
        if (orderByTimestamp) {
            heapCursorUsed = true;
            rowCursorFactory = new HeapRowCursorFactory(perKeyFactories, cursorFactoriesIdx);
        } else {
            heapCursorUsed = false;
            rowCursorFactory = new SequentialRowCursorFactory(perKeyFactories, cursorFactoriesIdx);
        }
        indexCursor = new PageFrameRecordCursorImpl(configuration, metadata, rowCursorFactory, false, residualFilter);
        fallbackCursor = new PageFrameRecordCursorImpl(
                configuration,
                metadata,
                new PageFrameRowCursorFactory(partitionFrameCursorFactory.getOrder()),
                false,
                fallbackFilter
        );
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
    public boolean supportsPageFrameCursor() {
        // SP1: parallel/page-frame consumers use the surrounding pipeline, not this fast path.
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("SymbolPatternIndex");
        sink.attr("on").putColumnName(columnIndex);
        sink.child(providerFunction);
        sink.child(rowCursorFactory);   // emits "Cursor-order scan" when !heapCursorUsed
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    private void buildPerKeyFactories(int keyCount) {
        for (int i = perKeyFactories.size(); i < keyCount; i++) {
            perKeyFactories.add(residualFilter == null
                    ? new SymbolIndexRowCursorFactory(columnIndex, 0, indexDirection, null)
                    : new SymbolIndexFilteredRowCursorFactory(columnIndex, 0, residualFilter, indexDirection, null));
        }
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(providerFunction);
        Misc.free(residualFilter);
        Misc.free(fallbackFilter);
        Misc.free(rowCursorFactory);
        Misc.free(indexCursor);
        Misc.free(fallbackCursor);
        Misc.freeObjList(perKeyFactories);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // Computes the matched symbol key set (init cascades to the arg symbol column, binding its
        // static symbol table), then reads the keys.
        providerFunction.init(pageFrameCursor, executionContext);
        final IntList keys = provider.getMatchedSymbolKeys();
        final int n = keys.size();
        if (n > threshold) {
            //noinspection AssignmentToStaticFieldFromInstanceMethod
            testFallbackInvocations++;
            fallbackCursor.of(pageFrameCursor, executionContext);
            if (fallbackFilter != null) {
                fallbackFilter.init(fallbackCursor, executionContext);
            }
            return fallbackCursor;
        }
        buildPerKeyFactories(n);
        for (int i = 0; i < n; i++) {
            perKeyFactories.getQuick(i).of(keys.getQuick(i));
        }
        cursorFactoriesIdx[0] = n;
        indexCursor.of(pageFrameCursor, executionContext);
        if (residualFilter != null) {
            residualFilter.init(indexCursor, executionContext);
        }
        //noinspection AssignmentToStaticFieldFromInstanceMethod
        testIndexInvocations++;
        return indexCursor;
    }

    @TestOnly
    public static void resetTestCounters() {
        testIndexInvocations = 0;
        testFallbackInvocations = 0;
    }
}
