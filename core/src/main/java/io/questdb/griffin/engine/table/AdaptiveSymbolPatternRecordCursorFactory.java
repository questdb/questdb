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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime owner for symbol-pattern access paths. It refreshes the provider once per
 * cursor open, estimates posting-row selectivity with bounded metadata probes, and
 * then opens exactly one of its covering, bitmap-index, or scan delegates.
 *
 * The conservative policy chooses an index delegate only after an exact bounded
 * estimate proves that matching postings cover at most 25% of the selected rows.
 * Any mixed posting generation, non-posting partition, unknown row count, or probe
 * budget exhaustion selects the scan delegate. The probe budget is
 * {@code max(1, configuredThreshold)}; it bounds planning work independently of
 * dictionary cardinality. The configured value now limits posting metadata probes,
 * rather than using distinct-key count as a selectivity proxy.
 */
public class AdaptiveSymbolPatternRecordCursorFactory extends AbstractRecordCursorFactory {
    @TestOnly
    public static final AtomicLong testCoveringInvocations = new AtomicLong();
    @TestOnly
    public static final AtomicLong testScanInvocations = new AtomicLong();
    private static final ThreadLocal<CloseObserver> TEST_CLOSE_OBSERVER = new ThreadLocal<>();
    private final CloseObserver closeObserver;
    private final IntList columnIndexes;
    private final RecordCursorFactory coveringDelegate;
    private final PartitionFrameCursorFactory dfcFactory;
    private final IntList effectiveKeys;
    private final RecordCursorFactory indexDelegate;
    private final int indexReaderColumnIndex;
    private final boolean isNegated;
    private final int maxEstimateProbes;
    private final PreparedSymbolPatternFilter patternFilter;
    private final RecordCursorFactory scanDelegate;
    private final SymbolTableSourceMapper symbolTableSourceMapper;

    public AdaptiveSymbolPatternRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            @NotNull IntList columnIndexes,
            @NotNull IntList effectiveKeys,
            int indexReaderColumnIndex,
            boolean isNegated,
            int threshold,
            @NotNull PreparedSymbolPatternFilter patternFilter,
            @NotNull RecordCursorFactory indexDelegate,
            @Nullable RecordCursorFactory coveringDelegate,
            @NotNull RecordCursorFactory scanDelegate
    ) {
        super(metadata);
        this.closeObserver = TEST_CLOSE_OBSERVER.get();
        this.dfcFactory = dfcFactory;
        this.columnIndexes = columnIndexes;
        this.effectiveKeys = effectiveKeys;
        this.indexReaderColumnIndex = indexReaderColumnIndex;
        this.isNegated = isNegated;
        this.maxEstimateProbes = Math.max(1, threshold);
        this.patternFilter = patternFilter;
        this.indexDelegate = indexDelegate;
        this.coveringDelegate = coveringDelegate;
        this.scanDelegate = scanDelegate;
        this.symbolTableSourceMapper = new SymbolTableSourceMapper(columnIndexes);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        final boolean isSelective = prepareAndEstimate(executionContext);
        if (isSelective && coveringDelegate != null) {
            testCoveringInvocations.incrementAndGet();
            return coveringDelegate.getPageFrameCursor(executionContext, order);
        }
        testScanInvocations.incrementAndGet();
        if (!isSelective) {
            SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.incrementAndGet();
        }
        return scanDelegate.getPageFrameCursor(executionContext, order);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final boolean isSelective = prepareAndEstimate(executionContext);
        if (isSelective) {
            if (coveringDelegate != null) {
                testCoveringInvocations.incrementAndGet();
                return coveringDelegate.getCursor(executionContext);
            }
            return indexDelegate.getCursor(executionContext);
        }
        testScanInvocations.incrementAndGet();
        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.incrementAndGet();
        return scanDelegate.getCursor(executionContext);
    }

    public IntList getEffectiveKeys() {
        return effectiveKeys;
    }

    @Override
    public int getScanDirection() {
        // The runtime-selected index and scan delegates do not promise the same order.
        return SCAN_DIRECTION_OTHER;
    }

    @Override
    public TableToken getTableToken() {
        return dfcFactory.getTableToken();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @TestOnly
    public static void clearCloseObserverForTesting() {
        TEST_CLOSE_OBSERVER.remove();
    }

    @TestOnly
    public static void resetTestCounters() {
        testCoveringInvocations.set(0);
        testScanInvocations.set(0);
    }

    @TestOnly
    public static void setCloseObserverForTesting(CloseObserver observer) {
        TEST_CLOSE_OBSERVER.set(observer);
    }

    @Override
    public boolean supportsPageFrameCursor() {
        // Only the covered positive route benefits from runtime page-frame selection. Classic and
        // negated patterns retain their record-cursor index route; their broad fallback stays serial.
        return coveringDelegate != null;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("AdaptiveSymbolPattern");
        sink.meta("policy").val("posting rows <= 25%, bounded probes");
        sink.child(indexDelegate);
        if (coveringDelegate != null) {
            sink.child(coveringDelegate);
        }
        sink.child(scanDelegate);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("{\"name\":\"AdaptiveSymbolPatternRecordCursorFactory\"}");
    }

    @Override
    public boolean usesIndex() {
        // The factory can select an index at runtime, but callers must not assume every open does.
        return false;
    }

    @Override
    protected void _close() {
        Misc.free(coveringDelegate);
        Misc.free(indexDelegate);
        Misc.free(scanDelegate);
        if (closeObserver != null) {
            closeObserver.onPartitionFrameFactoryClose();
        }
        Misc.free(dfcFactory);
    }

    private boolean prepareAndEstimate(SqlExecutionContext executionContext) throws SqlException {
        try (PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        )) {
            symbolTableSourceMapper.of(frameCursor);
            patternFilter.prepare(symbolTableSourceMapper, executionContext);
            buildEffectiveKeys(symbolTableSourceMapper);
            if (effectiveKeys.size() == 0) {
                return true;
            }

            final long totalRows = frameCursor.size();
            if (totalRows < 0) {
                return false;
            }
            final long maxIndexRows = Math.max(1, totalRows / 4);
            final TableReader tableReader = frameCursor.getTableReader();
            long matchedRows = 0;
            int probes = 0;
            PartitionFrame frame;
            while ((frame = frameCursor.next()) != null) {
                final IndexReader reader = tableReader.getIndexReader(
                        frame.getPartitionIndex(),
                        indexReaderColumnIndex,
                        IndexReader.DIR_FORWARD
                );
                final long rowLo = frame.getRowLo();
                final long callerHiInclusive = frame.getRowHi() - 1;
                for (int i = 0, n = effectiveKeys.size(); i < n; i++) {
                    if (++probes > maxEstimateProbes) {
                        return false;
                    }
                    final int indexKey = TableUtils.toIndexKey(effectiveKeys.getQuick(i));
                    long count = Numbers.LONG_NULL;
                    if (reader instanceof AbstractPostingIndexReader posting) {
                        final long entryMax = posting.getEntryMaxValue();
                        final long clampedMax = entryMax >= 0
                                ? Math.min(callerHiInclusive, entryMax)
                                : callerHiInclusive;
                        count = posting.countMatchesClamped(indexKey, rowLo, callerHiInclusive, clampedMax);
                    }
                    if (count == Numbers.LONG_NULL) {
                        // A mixed/unsealed generation cannot supply an exact metadata count. Traverse only
                        // until the remaining 25% budget is exceeded; this preserves a bounded estimate.
                        count = 0;
                        try (RowCursor rowCursor = reader.getCursor(indexKey, rowLo, callerHiInclusive)) {
                            while (rowCursor.hasNext() && matchedRows + count <= maxIndexRows) {
                                rowCursor.next();
                                count++;
                            }
                        }
                    }
                    matchedRows += count;
                    if (matchedRows > maxIndexRows) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private void buildEffectiveKeys(SymbolTableSource symbolTableSource) {
        final IntList matched = patternFilter.getMatchedSymbolKeys();
        effectiveKeys.clear();
        if (!isNegated) {
            effectiveKeys.addAll(matched);
            return;
        }

        final StaticSymbolTable symbolTable = (StaticSymbolTable) symbolTableSource.getSymbolTable(
                patternFilter.getSymbolColumnIndex()
        );
        int matchedIndex = 0;
        final int matchedSize = matched.size();
        for (int key = 0, symbolCount = symbolTable.getSymbolCount(); key < symbolCount; key++) {
            if (matchedIndex < matchedSize && matched.getQuick(matchedIndex) == key) {
                matchedIndex++;
            } else {
                effectiveKeys.add(key);
            }
        }
        if (symbolTable.containsNullValue()) {
            effectiveKeys.add(SymbolTable.VALUE_IS_NULL);
        }
    }

    @TestOnly
    public interface CloseObserver {
        void onPartitionFrameFactoryClose();
    }

    public static final class NonOwningPartitionFrameCursorFactory implements PartitionFrameCursorFactory {
        private final PartitionFrameCursorFactory delegate;

        public NonOwningPartitionFrameCursorFactory(PartitionFrameCursorFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
        }

        @Override
        public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, IntList columnIndexes, int order) throws SqlException {
            return delegate.getCursor(executionContext, columnIndexes, order);
        }

        @Override
        public RecordMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public int getOrder() {
            return delegate.getOrder();
        }

        @Override
        public ObjList<PushdownFilterExtractor.PushdownFilterCondition> getPushdownFilterConditions() {
            return delegate.getPushdownFilterConditions();
        }

        @Override
        public TableToken getTableToken() {
            return delegate.getTableToken();
        }

        @Override
        public boolean hasParquetFormatPartitions(SqlExecutionContext executionContext) {
            return delegate.hasParquetFormatPartitions(executionContext);
        }

        @Override
        public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> conditions) {
            delegate.setPushdownFilterCondition(conditions);
        }

        @Override
        public boolean supportsTableRowId(TableToken tableToken) {
            return delegate.supportsTableRowId(tableToken);
        }

        @Override
        public void toPlan(PlanSink sink) {
            delegate.toPlan(sink);
        }
    }

    public static final class PreparedSymbolPatternFilter extends BooleanFunction {
        private final boolean isNegated;
        private final SymbolKeySetProvider provider;
        private final Function providerFunction;
        private final Function residualFilter;
        private final int symbolColumnIndex;

        public PreparedSymbolPatternFilter(
                @NotNull Function providerFunction,
                @Nullable Function residualFilter,
                boolean isNegated,
                int symbolColumnIndex
        ) {
            this.providerFunction = providerFunction;
            this.provider = (SymbolKeySetProvider) providerFunction;
            this.residualFilter = residualFilter;
            this.isNegated = isNegated;
            this.symbolColumnIndex = symbolColumnIndex;
        }

        @Override
        public void close() {
            Misc.free(providerFunction);
            Misc.free(residualFilter);
        }

        @Override
        public void cursorClosed() {
            providerFunction.cursorClosed();
            if (residualFilter != null) {
                residualFilter.cursorClosed();
            }
        }

        @Override
        public boolean getBool(Record rec) {
            final boolean isMatch = providerFunction.getBool(rec);
            return (isNegated != isMatch) && (residualFilter == null || residualFilter.getBool(rec));
        }

        public IntList getMatchedSymbolKeys() {
            return provider.getMatchedSymbolKeys();
        }

        public int getSymbolColumnIndex() {
            return symbolColumnIndex;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (residualFilter != null) {
                residualFilter.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isThreadSafe() {
            // Provider getBool() only reads the prepared key list and the record's integer symbol key.
            return residualFilter == null || residualFilter.isThreadSafe();
        }

        public void prepare(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            providerFunction.init(symbolTableSource, executionContext);
        }

        @Override
        public void toPlan(PlanSink sink) {
            if (isNegated) {
                sink.val("not(");
            }
            sink.val(providerFunction);
            if (isNegated) {
                sink.val(')');
            }
            if (residualFilter != null) {
                sink.val(" and ").val(residualFilter);
            }
        }

        @Override
        public void toTop() {
            providerFunction.toTop();
            if (residualFilter != null) {
                residualFilter.toTop();
            }
        }
    }

    private static final class SymbolTableSourceMapper implements SymbolTableSource {
        private final IntList columnIndexes;
        private PartitionFrameCursor frameCursor;

        private SymbolTableSourceMapper(IntList columnIndexes) {
            this.columnIndexes = columnIndexes;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return frameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return frameCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        private void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
        }
    }
}
