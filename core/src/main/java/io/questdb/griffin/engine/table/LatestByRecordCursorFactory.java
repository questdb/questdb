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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Used only in the latest by over sub-query case.
 */
public class LatestByRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final int RECORD_INDEX_VALUE_IDX = 0;
    private static final int TIMESTAMP_VALUE_IDX = 1;

    private final RecordCursorFactory base;
    private final LatestByRecordCursor cursor;
    private final RecordSink recordSink;
    private final DirectLongList rowIndexes;
    private final long rowIndexesInitialCapacity;

    public LatestByRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull ColumnTypes columnTypes,
            int timestampIndex
    ) {
        // The cursor selects, per PARTITION BY key, the base row with the highest timestamp, then
        // replays those winning rows in ascending BASE-CURSOR-ORDINAL-POSITION order (buildMap()
        // sorts rowIndexes, see below) -- it does NOT re-derive order from timestamp values. That
        // only coincides with ascending timestamp order when the base itself happens to scan in
        // ascending timestamp order; base is an arbitrary sub-query factory here (this
        // constructor is only used when base does not support random access, e.g. a UNION ALL),
        // so nothing guarantees that. Like LatestByLightRecordCursorFactory (see its constructor
        // comment and FilterOnValuesRecordCursorFactory, the precedent for this split), the fix
        // keeps ts DESIGNATED -- via timestampIndex, the column LATEST ON actually named, which
        // may differ from base's own designated column -- while getScanDirection() below
        // (overridden to SCAN_DIRECTION_OTHER) independently tells consumers not to trust it as
        // ordered. Confirmed empirically: base.getMetadata() alone (the pre-fix code) left this
        // factory's own output with NO designated timestamp at all (getTimestampIndex() == -1)
        // whenever base itself did not designate one (e.g. a UNION ALL of two tables) -- the same
        // "unusable as input to a nested time-series op" bug Task 1 fixed for the light sibling,
        // not merely a scan-direction gap.
        super(buildMetadata(base, timestampIndex));
        assert !base.recordCursorSupportsRandomAccess();
        this.base = base;
        this.recordSink = recordSink;
        Map latestByMap = null;
        try {
            ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
            mapValueTypes.add(RECORD_INDEX_VALUE_IDX, ColumnType.LONG);
            mapValueTypes.add(TIMESTAMP_VALUE_IDX, base.getMetadata().getColumnType(timestampIndex));
            // openOnInit=false: the cursor binds the per-query tracker and reopens the map in of(),
            // so the first allocation is charged to the per-query counter.
            latestByMap = MapFactory.createOrderedMap(configuration, columnTypes, mapValueTypes, false);
            this.cursor = new LatestByRecordCursor(latestByMap, timestampIndex);
            latestByMap = null; // cursor owns the map now
            this.rowIndexesInitialCapacity = configuration.getSqlLatestByRowCount();
            // keepClosed=true: rowIndexes is allocated lazily on the first reopen() under the bound tracker.
            this.rowIndexes = new DirectLongList(rowIndexesInitialCapacity, MemoryTag.NATIVE_LATEST_BY_LONG_LIST, true);
        } catch (Throwable th) {
            Misc.free(latestByMap);
            close();
            throw th;
        }
    }

    // A private static helper (rather than inline in the super(...) call) because a constructor
    // cannot run statements before its super() call; timestampIndex is the column the LATEST ON
    // clause actually named (via "LATEST ON <col>"), which the caller resolved and may differ
    // from base's OWN designated timestamp column, so it -- not base.getMetadata().getTimestampIndex()
    // -- is the correct index to designate here. Mirrors LatestByLightRecordCursorFactory.buildMetadata().
    private static GenericRecordMetadata buildMetadata(RecordCursorFactory base, int timestampIndex) {
        GenericRecordMetadata metadata = GenericRecordMetadata.copyOfSansTimestamp(base.getMetadata());
        metadata.setTimestampIndex(timestampIndex);
        return metadata;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, recordSink, rowIndexes, rowIndexesInitialCapacity, executionContext.getCircuitBreaker(), executionContext.getMemoryTracker());
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        // See the constructor comment: rows replay in ascending base-cursor-ordinal-position
        // order, not in timestamp order, so this is neither a forward nor a backward ts-ordered
        // scan (unless base itself happens to be, which this factory does not track or assume).
        return SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestBy");
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
        Misc.free(rowIndexes);
        Misc.free(cursor);
        Misc.free(base);
    }

    private static class LatestByRecordCursor implements NoRandomAccessRecordCursor {

        // contains <[latest_by columns...], [row index, timestamp column]> pairs
        private final Map latestByMap;
        private final int timestampIndex;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long index = 0;
        private boolean isMapBuilt;
        private boolean isOpen;
        private RecordSink recordSink;
        private DirectLongList rowIndexes;
        private long rowIndexesCapacityThreshold;
        private long rowIndexesPos = 0;

        public LatestByRecordCursor(Map latestByMap, int timestampIndex) {
            this.latestByMap = latestByMap;
            this.timestampIndex = timestampIndex;
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                baseCursor = Misc.free(baseCursor);
                // Free rowIndexes (and the map) here, under the per-query tracker bound in of(),
                // so the next cursor reallocates from zero against its own tracker.
                Misc.free(rowIndexes);
                latestByMap.close();
            }
        }

        @Override
        public Record getRecord() {
            return baseRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                buildMap();
                toTop();
                isMapBuilt = true;
            }

            if (rowIndexesPos == rowIndexes.size()) {
                return false;
            }

            final long nextIndex = rowIndexes.get(rowIndexesPos);
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (index++ == nextIndex) {
                    rowIndexesPos++;
                    return true;
                }
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public void of(
                RecordCursor baseCursor,
                RecordSink recordSink,
                DirectLongList rowIndexes,
                long rowIndexesCapacityThreshold,
                SqlExecutionCircuitBreaker circuitBreaker,
                MemoryTracker memoryTracker
        ) {
            this.baseCursor = baseCursor;
            baseRecord = baseCursor.getRecord();
            isOpen = true;
            // Bind the per-query tracker before (re)allocating either the map (dominant allocator,
            // one entry per distinct key) or the rowIndexes list, so both are charged to the
            // per-query counter and freed against it at close.
            latestByMap.setMemoryTracker(memoryTracker);
            latestByMap.reopen();
            this.recordSink = recordSink;
            this.rowIndexes = rowIndexes;
            rowIndexes.setMemoryTracker(memoryTracker);
            rowIndexes.reopen();
            this.circuitBreaker = circuitBreaker;
            this.rowIndexesCapacityThreshold = rowIndexesCapacityThreshold;
            rowIndexesPos = 0;
            index = 0;
            isMapBuilt = false;
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(isMapBuilt) + baseCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return isMapBuilt ? rowIndexes.size() : -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            index = 0;
            rowIndexesPos = 0;
        }

        private void buildMap() {
            final Record baseRecord = baseCursor.getRecord();
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final MapKey key = latestByMap.withKey();
                recordSink.copy(baseRecord, key);
                final MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putLong(RECORD_INDEX_VALUE_IDX, index);
                    value.putTimestamp(TIMESTAMP_VALUE_IDX, baseRecord.getTimestamp(timestampIndex));
                } else {
                    long prevTimestamp = value.getTimestamp(TIMESTAMP_VALUE_IDX);
                    long newTimestamp = baseRecord.getTimestamp(timestampIndex);
                    if (newTimestamp >= prevTimestamp) {
                        value.putLong(RECORD_INDEX_VALUE_IDX, index);
                        value.putTimestamp(TIMESTAMP_VALUE_IDX, newTimestamp);
                    }
                }

                index++;
            }

            // Copy row indexes into the long list.
            try (final RecordCursor mapCursor = latestByMap.getCursor()) {
                final MapRecord mapRecord = (MapRecord) mapCursor.getRecord();
                while (mapCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final MapValue value = mapRecord.getValue();
                    final long rowId = value.getLong(RECORD_INDEX_VALUE_IDX);
                    rowIndexes.add(rowId);
                }
            }

            // Sort the indexes, so that we can use them when iterating the base cursor.
            rowIndexes.sortAsUnsigned();
            // Map is no longer needed, deallocate native memory.
            latestByMap.close();
        }
    }
}
