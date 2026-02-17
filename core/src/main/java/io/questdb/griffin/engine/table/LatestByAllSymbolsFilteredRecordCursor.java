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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByAllSymbolsFilteredRecordCursor extends AbstractDescendingRecordListCursor {
    private static final Function NO_OP_FILTER = BooleanConstant.TRUE;
    private final Function filter;
    private final Map map;
    private final IntList partitionByColumnIndexes;
    private final IntList partitionBySymbolCounts;
    private final RecordSink recordSink;
    private long possibleCombinations;

    public LatestByAllSymbolsFilteredRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull Map map,
            @NotNull DirectLongList rows,
            @NotNull RecordSink recordSink,
            @Nullable Function filter,
            @NotNull IntList partitionByColumnIndexes,
            @Nullable IntList partitionBySymbolCounts
    ) {
        super(configuration, metadata, rows);
        this.map = map;
        this.recordSink = recordSink;
        this.filter = filter != null ? filter : NO_OP_FILTER;
        this.partitionByColumnIndexes = partitionByColumnIndexes;
        this.partitionBySymbolCounts = partitionBySymbolCounts;
    }

    @Override
    public void close() {
        if (isOpen()) {
            Misc.free(filter);
            Misc.free(map);
            super.close();
        }
    }

    public Function getFilter() {
        return filter;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        if (!isOpen) {
            isOpen = true;
            map.reopen();
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
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = partitionHi - partitionLo; row >= 0; row--) {
                recordA.setRowIndex(row);
                if (filter.getBool(recordA)) {
                    MapKey key = map.withKey();
                    key.put(recordA, recordSink);
                    if (key.create()) {
                        rows.add(Rows.toRowID(frameIndex, row));
                        if (rows.size() == possibleCombinations) {
                            break OUTER;
                        }
                    }
                }
            }
        }
        map.clear();
    }
}
