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
import io.questdb.cairo.sql.*;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByAllSymbolsFilteredRecordCursor extends AbstractDescendingRecordListCursor {
    private static final Function NO_OP_FILTER = BooleanConstant.TRUE;
    private final Function filter;
    private final DirectLongHashSet pairKeys;
    private final DirectMultiIntHashSet symbolKeys;
    private final IntList partitionByColumnIndexes;
    private final IntList partitionBySymbolCounts;
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
        if (partitionByColumnIndexes.size() == 2) {
            pairKeys = new DirectLongHashSet(configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor(), MemoryTag.NATIVE_UNORDERED_MAP, configuration.getSqlMapMaxResizes(), false);
            symbolKeys = null;
        } else {
            pairKeys = null;
            symbolKeys = new DirectMultiIntHashSet(partitionByColumnIndexes.size(), configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor(), configuration.getSqlMapMaxResizes());
        }
        this.filter = filter != null ? filter : NO_OP_FILTER;
        this.partitionByColumnIndexes = partitionByColumnIndexes;
        this.partitionBySymbolCounts = partitionBySymbolCounts;
    }

    @Override
    public void close() {
        try {
            if (isOpen()) {
                Misc.free(pairKeys);
                Misc.free(symbolKeys);
                super.close();
            }
        } finally {
            LatestByCompiledFilter.closeCursor(filter);
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

        PageFrame frame;
        OUTER:
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);
            for (long batchHi = partitionHi - partitionLo + 1; batchHi > 0; ) {
                long batchLo = Math.max(0, batchHi - LatestByCompiledFilter.BATCH_SIZE);
                final DirectLongList matches = LatestByCompiledFilter.apply(
                        filter, frameMemoryPool, frameAddressCache, frameIndex, batchLo, batchHi
                );
                if (matches == null) {
                    // Unsupported frames retain the original whole-frame Java scan.
                    batchLo = 0;
                }
                final long rowCount = matches != null ? matches.size() : batchHi;

                if (pairKeys != null) {
                    final int firstColumn = partitionByColumnIndexes.getQuick(0);
                    final int secondColumn = partitionByColumnIndexes.getQuick(1);
                    for (long iRow = rowCount - 1; iRow >= 0; iRow--) {
                        long row = matches != null ? matches.get(iRow) + batchLo : iRow;
                        recordA.setRowIndex(row);
                        if (matches != null || filter.getBool(recordA)) {
                            long key = ((long) recordA.getInt(firstColumn) << 32) | (recordA.getInt(secondColumn) & 0xffff_ffffL);
                            if (pairKeys.add(key)) {
                                rows.add(Rows.toRowID(frameIndex, row));
                                if (rows.size() == possibleCombinations) {
                                    break OUTER;
                                }
                            }
                        }
                    }
                } else {
                    final int columnCount = partitionByColumnIndexes.size();
                    for (long iRow = rowCount - 1; iRow >= 0; iRow--) {
                        long row = matches != null ? matches.get(iRow) + batchLo : iRow;
                        recordA.setRowIndex(row);
                        if (matches != null || filter.getBool(recordA)) {
                            final long keyAddress = symbolKeys.getKeyAddress();
                            for (int i = 0; i < columnCount; i++) {
                                Unsafe.putInt(keyAddress + (long) i * Integer.BYTES, recordA.getInt(partitionByColumnIndexes.getQuick(i)));
                            }
                            if (symbolKeys.add()) {
                                rows.add(Rows.toRowID(frameIndex, row));
                                if (rows.size() == possibleCombinations) {
                                    break OUTER;
                                }
                            }
                        }
                    }
                }
                batchHi = batchLo;
                if (batchHi > 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
            }
        }
        if (pairKeys != null) {
            pairKeys.clear();
        } else {
            symbolKeys.clear();
        }
    }
}
