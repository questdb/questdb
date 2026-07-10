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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Index fast path for a {@code LIKE}/{@code ILIKE}/{@code ~} predicate (or its negation
 * {@code NOT LIKE}/{@code NOT ILIKE}/{@code !~}) on an indexed, static-symbol-table column. The provider
 * always compiles the POSITIVE pattern; when {@code negated} is set the factory scans the COMPLEMENT of the
 * matched key set — every symbol key the pattern does not match, plus the NULL key when the column contains
 * nulls (mirroring {@link FilterOnExcludedValuesRecordCursorFactory} / "NOT IN" semantics). Structurally
 * mirrors {@link FilterOnValuesRecordCursorFactory}, with three differences:
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
    @TestOnly
    public static final AtomicLong testFallbackInvocations = new AtomicLong();
    @TestOnly
    public static final AtomicLong testIndexInvocations = new AtomicLong();
    private final int columnIndex;
    // reused per initRecordCursor for the negated (complement) path: every symbol key NOT matched by the
    // positive pattern, plus VALUE_IS_NULL when the column contains nulls (mirrors "NOT IN" semantics).
    private final IntList complementKeys = new IntList();
    private final int[] cursorFactoriesIdx = new int[]{0};
    private final PageFrameRecordCursorImpl fallbackCursor;
    private final Function fallbackFilter;         // full filter (pattern AND residual)
    // Applies fallbackFilter per row over the full-scan fallbackCursor. The plain fallbackCursor emits every
    // row (its PageFrameRowCursorFactory does no filtering, and PageFrameRecordCursorImpl treats its filter
    // arg as a toTop-lifecycle handle, NOT a per-row predicate), so without this wrapper the > threshold
    // fallback would silently return ALL rows and drop the pattern predicate.
    private final FilteredRecordCursor fallbackFilteredCursor;
    private final RowCursorFactory fallbackRowCursorFactory;
    private final PageFrameRecordCursorImpl indexCursor;
    private final int indexDirection;
    private final boolean isHeapCursorUsed;
    // false: scan the keys the (positive) pattern matches; true: scan the complement (NOT LIKE / !~).
    private final boolean isNegated;
    private final ObjList<SymbolFunctionRowCursorFactory> perKeyFactories = new ObjList<>();
    private final SymbolKeySetProvider provider;
    private final Function providerFunction;        // == (Function) provider; the compiled POSITIVE pattern predicate
    private final Function residualFilter;          // applied on index rows (nullable)
    private final RowCursorFactory rowCursorFactory;
    private final int threshold;

    public SymbolPatternIndexRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @NotNull Function providerFunction,
            @Nullable Function residualFilter,
            @NotNull Function fallbackFilter,
            boolean negated,
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
        this.isNegated = negated;
        this.indexDirection = indexDirection;
        this.threshold = threshold;
        // Use the timestamp-merge heap only when the planner explicitly requested timestamp ordering.
        // For unordered queries (ORDER_BY_UNKNOWN) and symbol-key-ordered queries (ORDER_BY_INVARIANT
        // without timestamp), Sequential is cheaper: the outer Sort node (retained because
        // followedOrderByAdvice() is false) will provide any ordering the consumer needs.
        if (orderByTimestamp) {
            isHeapCursorUsed = true;
            rowCursorFactory = new HeapRowCursorFactory(perKeyFactories, cursorFactoriesIdx);
        } else {
            isHeapCursorUsed = false;
            rowCursorFactory = new SequentialRowCursorFactory(perKeyFactories, cursorFactoriesIdx);
        }
        indexCursor = new PageFrameRecordCursorImpl(configuration, metadata, rowCursorFactory, false, residualFilter);
        fallbackRowCursorFactory = new PageFrameRowCursorFactory(partitionFrameCursorFactory.getOrder());
        fallbackCursor = new PageFrameRecordCursorImpl(
                configuration,
                metadata,
                fallbackRowCursorFactory,
                false,
                fallbackFilter
        );
        fallbackFilteredCursor = new FilteredRecordCursor(fallbackFilter);
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
        sink.child(rowCursorFactory);   // emits "Cursor-order scan" when !isHeapCursorUsed
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
        Misc.free(fallbackRowCursorFactory);
        Misc.freeObjList(perKeyFactories);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // Computes the POSITIVE matched symbol key set (init cascades to the arg symbol column, binding its
        // static symbol table), then reads the keys. For the negated path these are the keys to EXCLUDE.
        providerFunction.init(pageFrameCursor, executionContext);
        final IntList matched = provider.getMatchedSymbolKeys(); // sorted, unique, no NULL

        // "Included" count decides fast-vs-fallback: |matched| for the positive path, the complement size
        // for the negated path (computed cheaply, WITHOUT enumerating). A positive pattern never matches
        // NULL, so NULL is always in the negated complement when the column has nulls.
        final StaticSymbolTable symTab = isNegated ? pageFrameCursor.getSymbolTable(columnIndex) : null;
        final int includedCount = isNegated
                ? symTab.getSymbolCount() - matched.size() + (symTab.containsNullValue() ? 1 : 0)
                : matched.size();
        if (includedCount > threshold) {
            testFallbackInvocations.incrementAndGet();
            fallbackCursor.of(pageFrameCursor, executionContext);
            // Wrap the unfiltered full scan so fallbackFilter (pattern AND residual) is evaluated per row;
            // of() also runs fallbackFilter.init(). Returning the raw fallbackCursor would drop the predicate.
            fallbackFilteredCursor.of(fallbackCursor, executionContext);
            return fallbackFilteredCursor;
        }

        // Materialize the effective key list: the matched keys for the positive path, or the complement
        // (mirrors FilterOnExcludedValues -> "NOT IN" semantics, with the NULL key included) for the
        // negated path. VALUE_IS_NULL is mapped to index key 0 by the per-key factory's toIndexKey().
        final IntList keys;
        if (!isNegated) {
            keys = matched;
        } else {
            complementKeys.clear();
            // matched is sorted ascending/unique and k walks ascending, so a single-pass two-pointer
            // merge yields the complement in O(symCount) (vs O(symCount*log|matched|) for per-k search).
            int p = 0;
            final int matchedSize = matched.size();
            for (int k = 0, symCount = symTab.getSymbolCount(); k < symCount; k++) {
                if (p < matchedSize && matched.getQuick(p) == k) {
                    p++; // k is matched -> excluded from the complement
                } else {
                    complementKeys.add(k);
                }
            }
            if (symTab.containsNullValue() && matched.binarySearchUniqueList(SymbolTable.VALUE_IS_NULL) < 0) {
                complementKeys.add(SymbolTable.VALUE_IS_NULL);
            }
            keys = complementKeys;
        }

        final int n = keys.size();
        buildPerKeyFactories(n);
        for (int i = 0; i < n; i++) {
            perKeyFactories.getQuick(i).of(keys.getQuick(i));
        }
        cursorFactoriesIdx[0] = n;
        indexCursor.of(pageFrameCursor, executionContext);
        if (residualFilter != null) {
            residualFilter.init(indexCursor, executionContext);
        }
        testIndexInvocations.incrementAndGet();
        return indexCursor;
    }

    @TestOnly
    public static void resetTestCounters() {
        testIndexInvocations.set(0);
        testFallbackInvocations.set(0);
    }
}
