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
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.DirectLongHashSet;
import io.questdb.std.DirectLongList;
import io.questdb.std.DirectMultiIntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByAllSymbolsFilteredRecordCursor extends AbstractDescendingRecordListCursor {
    private static final Function NO_OP_FILTER = BooleanConstant.TRUE;
    private final Function filter;
    private final DirectLongHashSet pairKeys;
    private final IntList partitionByColumnIndexes;
    private final IntList partitionBySymbolCounts;
    private final LatestByFrameScanner scanner;
    private final DirectMultiIntHashSet symbolKeys;
    private long possibleCombinations;

    public LatestByAllSymbolsFilteredRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DirectLongList rows,
            @Nullable Function filter,
            @NotNull IntList partitionByColumnIndexes,
            @Nullable IntList partitionBySymbolCounts
    ) {
        super(configuration, metadata, rows);
        final int keyCount = partitionByColumnIndexes.size();
        final double loadFactor = configuration.getSqlFastMapLoadFactor();
        final int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        final int maxResizes = configuration.getSqlMapMaxResizes();
        final int unchargedKeyCapacity = (int) Math.min(
                Integer.MAX_VALUE,
                configuration.getSqlSmallMapPageSize() / Bytes.align8b((long) keyCount * Integer.BYTES)
        );
        if (keyCount == 2) {
            pairKeys = new DirectLongHashSet(keyCapacity, loadFactor, MemoryTag.NATIVE_UNORDERED_MAP, maxResizes, unchargedKeyCapacity, false);
            symbolKeys = null;
        } else {
            pairKeys = null;
            symbolKeys = new DirectMultiIntHashSet(keyCount, keyCapacity, loadFactor, maxResizes, unchargedKeyCapacity);
        }
        this.filter = filter != null ? filter : NO_OP_FILTER;
        this.scanner = new LatestByFrameScanner(this.filter, frameMemoryPool, frameAddressCache);
        this.partitionByColumnIndexes = partitionByColumnIndexes;
        this.partitionBySymbolCounts = partitionBySymbolCounts;
    }

    @Override
    public void close() {
        if (isOpen()) {
            Throwable failure = null;
            try {
                filter.cursorClosed();
            } catch (Throwable th) {
                failure = th;
            }
            failure = Misc.freeBestEffort(failure, pairKeys);
            failure = Misc.freeBestEffort(failure, symbolKeys);
            try {
                super.close();
            } catch (Throwable th) {
                failure = Misc.foldCleanupFailure(failure, th);
            }
            CairoException.rethrowCleanupFailure(failure);
        }
    }

    public Function getFilter() {
        return filter;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        isOpen = true;
        if (pairKeys != null) {
            pairKeys.setMemoryTracker(executionContext.getMemoryTracker());
            pairKeys.reopen();
        } else {
            symbolKeys.setMemoryTracker(executionContext.getMemoryTracker());
            symbolKeys.reopen();
        }
        super.of(pageFrameCursor, executionContext);
        filter.init(pageFrameCursor, executionContext);
        possibleCombinations = -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Row backward scan");
        sink.attr("expectedSymbolsCount").val(countSymbolCombinationsSimple());
    }

    private long countSymbolCombinations() {
        long combinations = 1;
        for (int i = 0, n = partitionByColumnIndexes.size(); i < n; i++) {
            int symbolCount = partitionBySymbolCounts != null ? partitionBySymbolCounts.getQuick(i) : Integer.MAX_VALUE;
            assert symbolCount > 0;
            int columnIndex = partitionByColumnIndexes.getQuick(i);
            StaticSymbolTable symbolMapReader = frameCursor.getSymbolTable(columnIndex);
            int distinctSymbols = symbolMapReader.getSymbolCount();
            if (symbolMapReader.containsNullValue()) {
                distinctSymbols++;
            }
            try {
                combinations = Math.multiplyExact(combinations, Math.min(symbolCount, distinctSymbols));
            } catch (ArithmeticException ignore) {
                return Long.MAX_VALUE;
            }
        }
        return combinations;
    }

    private long countSymbolCombinationsSimple() {
        long combinations = 1;
        for (int i = 0, n = partitionByColumnIndexes.size(); i < n; i++) {
            int symbolCount = partitionBySymbolCounts != null ? partitionBySymbolCounts.getQuick(i) : Integer.MAX_VALUE;
            try {
                combinations = Math.multiplyExact(combinations, symbolCount);
            } catch (ArithmeticException ignore) {
                return Long.MAX_VALUE;
            }
        }
        return combinations;
    }

    @Override
    protected void buildTreeMap() {
        if (possibleCombinations < 0) {
            possibleCombinations = countSymbolCombinations();
        }

        final Function javaFilter = scanner.getJavaFilter();
        PageFrame frame;
        OUTER:
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final int frameIndex = frameCount;
            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);
            scanner.of(frameIndex, frame.getPartitionHi() - frame.getPartitionLo());
            while (scanner.nextBatch(circuitBreaker)) {
                final DirectLongList matches = scanner.getMatches();
                if (matches == null) {
                    for (long row = scanner.getRowCount() - 1; row >= 0; row--) {
                        recordA.setRowIndex(row);
                        if (javaFilter.getBool(recordA) && addRow(frameIndex, row)) {
                            break OUTER;
                        }
                    }
                } else {
                    final long batchLo = scanner.getBatchLo();
                    for (long i = matches.size() - 1; i >= 0; i--) {
                        final long row = matches.get(i) + batchLo;
                        recordA.setRowIndex(row);
                        if (addRow(frameIndex, row)) {
                            break OUTER;
                        }
                    }
                }
            }
        }
    }

    private boolean addKey(Record record) {
        if (pairKeys != null) {
            final long key = ((long) record.getInt(partitionByColumnIndexes.getQuick(0)) << 32)
                    | (record.getInt(partitionByColumnIndexes.getQuick(1)) & 0xffff_ffffL);
            return pairKeys.add(key);
        }
        final long keyAddress = symbolKeys.getKeyAddress();
        for (int i = 0, n = partitionByColumnIndexes.size(); i < n; i++) {
            Unsafe.putInt(keyAddress + (long) i * Integer.BYTES, record.getInt(partitionByColumnIndexes.getQuick(i)));
        }
        return symbolKeys.add();
    }

    private boolean addRow(int frameIndex, long row) {
        if (addKey(recordA)) {
            rows.add(Rows.toRowID(frameIndex, row));
            return rows.size() == possibleCombinations;
        }
        return false;
    }
}
