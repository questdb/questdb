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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Used only in the latest by over sub-query case. Assumes that the base factory supports random access.
 */
public class LatestByLightRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final int ROW_ID_VALUE_IDX = 0;
    private static final int TIMESTAMP_VALUE_IDX = 1;

    private final boolean orderedByTimestampAsc;
    private final RecordSink recordSink;
    private final int timestampIndex;
    private RecordCursorFactory base;
    private LatestByLightRecordCursor cursor;

    public LatestByLightRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull ColumnTypes columnTypes,
            int timestampIndex,
            boolean orderedByTimestampAsc
    ) {
        // The cursor emits one row per partition key in map (key-insertion) order, NOT in
        // designated-timestamp order, so this factory must not advertise a designated timestamp:
        // advertising one would imply the output is ordered by it (ascending or descending), which
        // it is not. Strip the timestamp from the base metadata. The sibling LatestByRecordCursorFactory
        // (the non-random-access path) sorts its row indexes before replaying the base cursor, so it
        // emits in base-scan order and legitimately keeps the timestamp; this light path trades that
        // sort for random access and loses the ordering. With no designated timestamp the scan
        // direction is vacuous, so -- like keyed GROUP BY and DISTINCT -- this factory does not
        // override getScanDirection() and inherits the default.
        super(GenericRecordMetadata.copyOfSansTimestamp(base.getMetadata()));
        assert base.recordCursorSupportsRandomAccess();
        this.base = base;
        this.recordSink = recordSink;
        ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
        mapValueTypes.add(ROW_ID_VALUE_IDX, ColumnType.LONG);
        if (!orderedByTimestampAsc) {
            mapValueTypes.add(TIMESTAMP_VALUE_IDX, base.getMetadata().getColumnType(timestampIndex));
        }
        // openOnInit=false: the cursor binds the per-query tracker and reopens the map in of(),
        // so the map's malloc/free pairs are charged symmetrically to the per-query counter.
        Map latestByMap = MapFactory.createOrderedMap(configuration, columnTypes, mapValueTypes, false);
        this.cursor = new LatestByLightRecordCursor(latestByMap);
        this.timestampIndex = timestampIndex;
        this.orderedByTimestampAsc = orderedByTimestampAsc;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            // Now that of() reopens the tracker-bound map, it can throw a per-query breach;
            // close the cursor to free the base and the (partly) reopened map under the
            // tracker before it propagates, matching the sibling LatestByRecordCursorFactory.
            cursor.of(baseCursor, executionContext.getCircuitBreaker(), executionContext.getMemoryTracker());
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestBy light");
        sink.meta("order_by_timestamp").val(orderedByTimestampAsc);
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
        final RecordCursorFactory base = this.base;
        this.base = null;
        final LatestByLightRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    private class LatestByLightRecordCursor implements RecordCursor {

        private final Map latestByMap;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private RecordCursor mapCursor;
        private MapRecord mapRecord;

        public LatestByLightRecordCursor(Map latestByMap) {
            this.latestByMap = latestByMap;
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                baseCursor = Misc.free(baseCursor);
                Misc.free(mapCursor);
                Misc.free(latestByMap);
            }
        }

        @Override
        public Record getRecord() {
            return baseRecord;
        }

        @Override
        public Record getRecordB() {
            return baseCursor.getRecordB();
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
            if (!mapCursor.hasNext()) {
                return false;
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
            final MapValue value = mapRecord.getValue();
            final long rowId = value.getLong(ROW_ID_VALUE_IDX);
            baseCursor.recordAt(baseRecord, rowId);
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public void of(RecordCursor baseCursor, SqlExecutionCircuitBreaker circuitBreaker, MemoryTracker memoryTracker) {
            // of() rebinds the tracker and reopens the map unconditionally (see below). A second
            // of() without an intervening close() would rebind onto a still-open, still-charged
            // map and underflow the per-query counter on free. close() nulls baseCursor, so a
            // null field here means fresh-or-closed.
            assert this.baseCursor == null : "of() without intervening close(): rebinding the memory tracker would underflow the per-query counter";
            this.baseCursor = baseCursor;
            baseRecord = baseCursor.getRecord();
            this.circuitBreaker = circuitBreaker;
            // We emit out of order, so pin the base to SCATTERED decode; see this cursor's own
            // setParquetDecodeHint override for why an outer MONOTONIC push must not downgrade it.
            baseCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            isOpen = true;
            // Bind the per-query tracker before reopening the map -- its only growing structure,
            // one entry per distinct partition key -- so the map's malloc/free pairs charge
            // symmetrically to the per-query counter and a runaway LATEST BY trips the limit at the
            // offending map allocation. reopen() is a no-op while the map is open, so binding and
            // reopening on every of() is safe and, unlike a !isOpen guard, leaves no stale open
            // state that would make a retry after a breach skip the (re)allocation.
            latestByMap.setMemoryTracker(memoryTracker);
            latestByMap.reopen();
            isMapBuilt = false;
        }

        @Override
        public long preComputedStateSize() {
            return isMapBuilt ? 1 : 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            // We emit out of order, so of() pins the base to SCATTERED. An outer MONOTONIC push
            // (e.g. an ASOF light join slave) must not downgrade it and force base re-decodes.
        }

        @Override
        public long size() {
            return isMapBuilt ? mapCursor.size() : -1;
        }

        @Override
        public void toTop() {
            if (mapCursor != null) {
                mapCursor.toTop();
            }
        }

        private void buildMap() {
            if (orderedByTimestampAsc) {
                // We don't need to store and compare timestamps if the sub-query returns them in asc order.
                // In this case we'll be good with the very last row id per each unique key.
                buildMapForOrderedSubQuery();
            } else {
                // Otherwise - we have to deal with the timestamps.
                buildMapForUnorderedSubQuery();
            }
            mapCursor = latestByMap.getCursor();
            mapRecord = (MapRecord) mapCursor.getRecord();
        }

        private void buildMapForOrderedSubQuery() {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final MapKey key = latestByMap.withKey();
                recordSink.copy(baseRecord, key);
                final MapValue value = key.createValue();
                value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
            }
        }

        private void buildMapForUnorderedSubQuery() {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final MapKey key = latestByMap.withKey();
                recordSink.copy(baseRecord, key);
                final MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
                    value.putTimestamp(TIMESTAMP_VALUE_IDX, baseRecord.getTimestamp(timestampIndex));
                } else {
                    long prevTimestamp = value.getTimestamp(TIMESTAMP_VALUE_IDX);
                    long newTimestamp = baseRecord.getTimestamp(timestampIndex);
                    if (newTimestamp >= prevTimestamp) {
                        value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
                        value.putTimestamp(TIMESTAMP_VALUE_IDX, newTimestamp);
                    }
                }
            }
        }
    }
}
