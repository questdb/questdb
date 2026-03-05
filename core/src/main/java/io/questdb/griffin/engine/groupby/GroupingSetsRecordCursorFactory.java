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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.GroupingFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

/**
 * Executes GROUP BY with GROUPING SETS using a single-pass, N-map approach.
 * Scans the source data once and updates N aggregation maps (one per grouping
 * set) with different key projections.
 *
 * <p>All maps share the same key layout (union of all GROUP BY columns) and
 * the same value layout. For each grouping set, a {@link NullingRecord} wraps
 * the source record and returns NULL for columns not active in that set.
 * The standard {@link RecordSink} copies from this wrapped record, so the
 * map key has NULLs for inactive columns. Rows with different values for
 * inactive columns then hash to the same map entry, achieving correct
 * aggregation per grouping set.</p>
 */
public class GroupingSetsRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final GroupingSetsRecordCursor cursor;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<Function> keyFunctions;
    private final ObjList<Function> recordFunctions;

    public GroupingSetsRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> keyFunctions,
            ObjList<Function> recordFunctions,
            ObjList<IntList> groupingSets,
            IntList keyColumnIndices
    ) throws SqlException {
        super(groupByMetadata);
        try {
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.keyFunctions = keyFunctions;
            this.recordFunctions = recordFunctions;

            int setCount = groupingSets.size();
            int totalKeyColumns = keyColumnIndices.size();

            // GROUPING_ID bitmask is int-based (31 usable bits).
            if (totalKeyColumns > 31) {
                throw SqlException.$(0, "GROUPING SETS supports at most 31 key columns");
            }

            // Build the single RecordSink that copies all key columns.
            RecordSink mapSink = RecordSinkFactory.getInstance(
                    configuration, asm, base.getMetadata(), listColumnFilter, keyFunctions, null
            );

            // Build per-set lists of base column indices to null out. For each
            // grouping set, columns NOT in its active list must return NULL.
            ObjList<IntList> nulledColumnsPerSet = new ObjList<>(setCount);
            for (int s = 0; s < setCount; s++) {
                IntList activeIndices = groupingSets.getQuick(s);
                IntList nulled = new IntList();
                for (int k = 0; k < totalKeyColumns; k++) {
                    if (!containsValue(activeIndices, k)) {
                        nulled.add(keyColumnIndices.getQuick(k));
                    }
                }
                nulledColumnsPerSet.add(nulled);
            }

            // Compute the GROUPING_ID bitmask for each grouping set. Bit positions
            // are assigned right-to-left per the SQL standard: the rightmost GROUP BY
            // column occupies bit 0.
            IntList groupingIds = new IntList(setCount);
            for (int s = 0; s < setCount; s++) {
                IntList activeIndices = groupingSets.getQuick(s);
                int mask = 0;
                for (int k = 0; k < totalKeyColumns; k++) {
                    if (!containsValue(activeIndices, k)) {
                        mask |= (1 << (totalKeyColumns - 1 - k));
                    }
                }
                groupingIds.add(mask);
            }

            // Find GROUPING() functions among the group-by functions and
            // initialize them with the grouping set layout. Each GroupingFunction
            // stores its bitmask in the map value (it implements GroupByFunction),
            // and setCurrentSet() is called before each set's map is populated
            // so computeFirst() stores the correct bitmask.
            ObjList<GroupingFunction> groupingFunctions = new ObjList<>();
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                GroupByFunction f = groupByFunctions.getQuick(i);
                if (f instanceof GroupingFunction gf) {
                    gf.init(groupingIds, keyColumnIndices);
                    groupingFunctions.add(gf);
                }
            }

            GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            this.cursor = new GroupingSetsRecordCursor(
                    configuration,
                    recordFunctions,
                    groupByFunctions,
                    updater,
                    keyTypes,
                    valueTypes,
                    mapSink,
                    nulledColumnsPerSet,
                    groupingFunctions,
                    setCount
            );
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            Function.init(recordFunctions, baseCursor, executionContext, null);
        } catch (Throwable th) {
            baseCursor.close();
            throw th;
        }

        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupBy");
        sink.meta("vectorized").val(false);
        sink.attr("groupingSets").val(true);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", groupByFunctions, true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        Misc.freeObjList(recordFunctions);
        Misc.freeObjList(keyFunctions);
        Misc.free(base);
        Misc.free(cursor);
    }

    private static boolean containsValue(IntList list, int value) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == value) {
                return true;
            }
        }
        return false;
    }

    /**
     * Cursor that iterates N aggregation maps sequentially. Each map corresponds
     * to one grouping set. Within each map, rows are iterated normally. When
     * one map is exhausted, the cursor advances to the next map.
     */
    private class GroupingSetsRecordCursor extends VirtualFunctionSkewedSymbolRecordCursor {
        private final GroupByAllocator allocator;
        private final ObjList<Map> dataMaps;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private final ObjList<GroupingFunction> groupingFunctions;
        private final RecordSink mapSink;
        private final ObjList<IntList> nulledColumnsPerSet;
        private final ObjList<NullingRecord> nullingRecords;
        private final int setCount;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private int currentSetIndex;
        private boolean isDataMapBuilt;
        private boolean isOpen;
        private long rowId;

        public GroupingSetsRecordCursor(
                CairoConfiguration configuration,
                ObjList<Function> functions,
                ObjList<GroupByFunction> groupByFunctions,
                GroupByFunctionsUpdater groupByFunctionsUpdater,
                @Transient @NotNull ArrayColumnTypes keyTypes,
                @Transient @NotNull ArrayColumnTypes valueTypes,
                RecordSink mapSink,
                ObjList<IntList> nulledColumnsPerSet,
                ObjList<GroupingFunction> groupingFunctions,
                int setCount
        ) {
            super(functions);
            try {
                this.isOpen = true;
                this.groupByFunctionsUpdater = groupByFunctionsUpdater;
                this.groupingFunctions = groupingFunctions;
                this.mapSink = mapSink;
                this.nulledColumnsPerSet = nulledColumnsPerSet;
                this.setCount = setCount;

                this.dataMaps = new ObjList<>(setCount);
                for (int i = 0; i < setCount; i++) {
                    dataMaps.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes));
                }

                // Pre-allocate NullingRecord wrappers (one per set)
                this.nullingRecords = new ObjList<>(setCount);
                for (int i = 0; i < setCount; i++) {
                    nullingRecords.add(new NullingRecord());
                }

                this.allocator = GroupByAllocatorFactory.createAllocator(configuration);
                GroupByUtils.setAllocator(groupByFunctions, allocator);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            buildMapConditionally();
            long total = 0;
            for (int i = 0; i < setCount; i++) {
                total += dataMaps.getQuick(i).size();
            }
            counter.add(total);
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                for (int i = 0, n = dataMaps.size(); i < n; i++) {
                    Misc.free(dataMaps.getQuick(i));
                }
                Misc.free(allocator);
                Misc.clearObjList(groupByFunctions);
                super.close();
            }
        }

        @Override
        public boolean hasNext() {
            buildMapConditionally();
            if (baseCursor != null && baseCursor.hasNext()) {
                return true;
            }
            // Advance to next map
            while (++currentSetIndex < setCount) {
                of(dataMaps.getQuick(currentSetIndex).getCursor());
                if (baseCursor.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        public void of(RecordCursor managedCursor, SqlExecutionContext executionContext) throws SqlException {
            this.managedCursor = managedCursor;
            if (!isOpen) {
                isOpen = true;
                for (int i = 0; i < setCount; i++) {
                    dataMaps.getQuick(i).reopen();
                }
                allocator.reopen();
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            Function.init(keyFunctions, managedCursor, executionContext, null);
            isDataMapBuilt = false;
            currentSetIndex = -1;
            rowId = 0;
        }

        @Override
        public long preComputedStateSize() {
            return isDataMapBuilt ? 1 : 0;
        }

        @Override
        public void toTop() {
            currentSetIndex = 0;
            of(dataMaps.getQuick(0).getCursor());
            rowId = 0;
        }

        /**
         * Scans the source data once and populates all N maps. For each input
         * row, wraps it in a NullingRecord per grouping set and updates the
         * corresponding map. This is the single-pass core of GROUPING SETS.
         *
         * <p>Before updating each set's map, setCurrentSet(s) is called on all
         * GROUPING() functions so that computeFirst() stores the correct
         * bitmask in the map value for that set.</p>
         */
        private void buildDataMaps() {
            final Record baseRecord = managedCursor.getRecord();
            // Initialize NullingRecords with the base record
            for (int s = 0; s < setCount; s++) {
                nullingRecords.getQuick(s).of(baseRecord, nulledColumnsPerSet.getQuick(s));
            }

            while (managedCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                for (int s = 0; s < setCount; s++) {
                    // Set the current grouping set index so GROUPING() functions
                    // store the correct bitmask when computeFirst() is called.
                    updateGroupingFunctions(s);
                    final MapKey key = dataMaps.getQuick(s).withKey();
                    // The NullingRecord returns NULL for inactive columns,
                    // so the map key has NULLs for non-grouped columns.
                    mapSink.copy(nullingRecords.getQuick(s), key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        groupByFunctionsUpdater.updateNew(value, baseRecord, rowId);
                    } else {
                        groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId);
                    }
                }
                rowId++;
            }
            // Start iteration from first map
            currentSetIndex = 0;
            of(dataMaps.getQuick(0).getCursor());
            isDataMapBuilt = true;
        }

        private void buildMapConditionally() {
            if (!isDataMapBuilt) {
                buildDataMaps();
            }
        }

        private void updateGroupingFunctions(int setIndex) {
            for (int i = 0, n = groupingFunctions.size(); i < n; i++) {
                groupingFunctions.getQuick(i).setCurrentSet(setIndex);
            }
        }
    }
}
