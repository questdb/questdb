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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class EarliestByValueListRecordCursor extends AbstractPageFrameRecordCursor {
    private final int columnIndex;
    private final Function filter;
    private final boolean restrictedByExcludedValues;
    private final boolean restrictedByIncludedValues;
    private final DirectLongList rowIds;
    private final int shrinkToCapacity;
    private boolean areRecordsFound;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private int currentRow;
    // Distinct symbol count targeted by the current scan. Zero means nothing to scan;
    // a positive value bounds rowIds capacity and doubles as the early-exit counter
    // inside the find* methods. Caching this avoids reading it back as (int)rowIds.getCapacity(),
    // which is stale after rowIds.clear() and loses precision on the long->int cast.
    private int distinctSymbolCount;
    private IntHashSet excludedSymbolKeys;
    private IntHashSet foundKeys;
    private int foundSize;
    private IntHashSet includedSymbolKeys;

    public EarliestByValueListRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            int columnIndex,
            @Nullable Function filter,
            int shrinkToCapacity,
            boolean restrictedByIncludedValues,
            boolean restrictedByExcludedValues
    ) {
        super(configuration, metadata);
        this.shrinkToCapacity = shrinkToCapacity;
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.restrictedByIncludedValues = restrictedByIncludedValues;
        this.restrictedByExcludedValues = restrictedByExcludedValues;
        if (restrictedByIncludedValues || restrictedByExcludedValues) {
            this.includedSymbolKeys = new IntHashSet(shrinkToCapacity);
            this.excludedSymbolKeys = new IntHashSet(shrinkToCapacity);
        }
        this.foundKeys = new IntHashSet(shrinkToCapacity);
        this.rowIds = new DirectLongList(shrinkToCapacity, MemoryTag.NATIVE_LONG_LIST);
    }

    @Override
    public void close() {
        super.close();
        if (rowIds.getCapacity() > shrinkToCapacity) {
            foundKeys = new IntHashSet(shrinkToCapacity);
        }
        Misc.free(rowIds);
    }

    @Override
    public boolean hasNext() {
        if (!areRecordsFound) {
            findRecords();
            toTop();
            areRecordsFound = true;
        }
        if (currentRow < rowIds.size()) {
            long rowId = rowIds.get(currentRow++);
            frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId), recordA);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            return true;
        }
        return false;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.frameCursor = pageFrameCursor;
        rowIds.reopen();
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        pageFrameCursor.toTop();
        foundSize = 0;
        foundKeys.clear();
        rowIds.clear();
        if (filter != null) {
            filter.init(pageFrameCursor, executionContext);
            filter.toTop();
        }
        distinctSymbolCount = 0;
        if (restrictedByIncludedValues) {
            if (includedSymbolKeys.size() > 0) {
                distinctSymbolCount = includedSymbolKeys.size();
                rowIds.setCapacity(distinctSymbolCount);
            }
        } else if (restrictedByExcludedValues) {
            final StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(columnIndex);
            final int symbolCount = symbolTable.getSymbolCount();
            final boolean tableHasNull = symbolTable.containsNullValue();
            int distinctSymbols = symbolCount;
            if (tableHasNull) {
                distinctSymbols++;
            }
            // Only subtract excluded keys that actually exist in the table; otherwise
            // distinctCount would be an under-estimate and the scan would terminate
            // before every reachable row has been found.
            int actuallyExcluded = 0;
            for (int i = 0, n = excludedSymbolKeys.size(); i < n; i++) {
                final int key = excludedSymbolKeys.get(i);
                if (key == SymbolTable.VALUE_IS_NULL) {
                    if (tableHasNull) {
                        actuallyExcluded++;
                    }
                } else if (key >= 0 && key < symbolCount) {
                    actuallyExcluded++;
                }
            }
            distinctSymbols -= actuallyExcluded;
            if (distinctSymbols > 0) {
                distinctSymbolCount = distinctSymbols;
                rowIds.setCapacity(distinctSymbolCount);
            }
        } else {
            StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(columnIndex);
            int distinctSymbols = symbolTable.getSymbolCount();
            if (symbolTable.containsNullValue()) {
                distinctSymbols++;
            }
            if (distinctSymbols > 0) {
                distinctSymbolCount = distinctSymbols;
                rowIds.setCapacity(distinctSymbolCount);
            }
        }
        areRecordsFound = false;
        // prepare for page frame iteration
        super.init();
    }

    @Override
    public long preComputedStateSize() {
        return rowIds.size();
    }

    @Override
    public long size() {
        return areRecordsFound ? rowIds.size() : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("FilterOnValueList").meta("on").putColumnName(columnIndex);
    }

    @Override
    public void toTop() {
        currentRow = 0;
    }

    private void findAllNoFilter(int distinctCount) {
        assert filter == null;
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = 0; row <= partitionHi - partitionLo; row++) {
                recordA.setRowIndex(row);
                int key = recordA.getInt(columnIndex);
                if (foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frameIndex, row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
        }
    }

    private void findAllWithFilter(int distinctCount) {
        assert filter != null;
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = 0; row <= partitionHi - partitionLo; row++) {
                recordA.setRowIndex(row);
                int key = recordA.getInt(columnIndex);
                if (filter.getBool(recordA) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frameIndex, row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
        }
    }

    private void findRecords() {
        if (restrictedByIncludedValues) {
            if (includedSymbolKeys.size() > 0) {
                if (filter != null) {
                    findRestrictedWithFilter();
                } else {
                    findRestrictedNoFilter();
                }
            }
        } else if (restrictedByExcludedValues) {
            if (distinctSymbolCount > 0) {
                if (filter != null) {
                    findRestrictedExcludedOnlyWithFilter(distinctSymbolCount);
                } else {
                    findRestrictedExcludedOnlyNoFilter(distinctSymbolCount);
                }
            }
        } else {
            if (distinctSymbolCount > 0) {
                if (filter != null) {
                    findAllWithFilter(distinctSymbolCount);
                } else {
                    findAllNoFilter(distinctSymbolCount);
                }
            }
        }
    }

    private void findRestrictedExcludedOnlyNoFilter(int distinctCount) {
        assert filter == null;
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = 0; row <= partitionHi - partitionLo; row++) {
                recordA.setRowIndex(row);
                int key = recordA.getInt(columnIndex);
                if (excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frameIndex, row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
        }
    }

    private void findRestrictedExcludedOnlyWithFilter(int distinctCount) {
        assert filter != null;
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = 0; row <= partitionHi - partitionLo; row++) {
                recordA.setRowIndex(row);
                int key = recordA.getInt(columnIndex);
                if (filter.getBool(recordA) && excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frameIndex, row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
        }
    }

    private void findRestrictedNoFilter() {
        assert filter == null;
        final int searchSize = includedSymbolKeys.size();
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = 0; row <= partitionHi - partitionLo; row++) {
                recordA.setRowIndex(row);
                int key = recordA.getInt(columnIndex);
                if (includedSymbolKeys.contains(key) && excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frameIndex, row));
                    if (++foundSize == searchSize) {
                        return;
                    }
                }
            }
        }
    }

    private void findRestrictedWithFilter() {
        assert filter != null;
        int searchSize = includedSymbolKeys.size();
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = 0; row <= partitionHi - partitionLo; row++) {
                recordA.setRowIndex(row);
                int key = recordA.getInt(columnIndex);
                if (filter.getBool(recordA) && includedSymbolKeys.contains(key) && excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frameIndex, row));
                    if (++foundSize == searchSize) {
                        return;
                    }
                }
            }
        }
    }

    IntHashSet getExcludedSymbolKeys() {
        return excludedSymbolKeys;
    }

    IntHashSet getIncludedSymbolKeys() {
        return includedSymbolKeys;
    }
}
