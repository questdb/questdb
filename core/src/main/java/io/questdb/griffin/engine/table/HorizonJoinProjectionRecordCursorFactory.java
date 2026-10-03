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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Serial HORIZON JOIN without aggregation: one output row per master row and offset.
 * <p>
 * The cursor reads the master in batches. It matches a batch in horizon timestamp order, which
 * keeps the slave scans monotonic, and then emits it in master row order, one row per offset in
 * offset order. The output is therefore ordered by the master's designated timestamp, the same
 * order the parallel {@link AsyncHorizonJoinProjectionRecordCursorFactory} produces. A batch holds
 * one slave row id per row, offset and slave, so its size shrinks as offsets and slaves grow.
 */
public class HorizonJoinProjectionRecordCursorFactory extends AbstractRecordCursorFactory {
    private final int offsetCount;
    private HorizonJoinProjectionRecordCursor cursor;
    private JoinRecordMetadata horizonJoinMetadata;
    private RecordCursorFactory masterFactory;
    private ObjList<HorizonJoinSlaveState> slaveStates;

    public HorizonJoinProjectionRecordCursorFactory(
            CairoConfiguration configuration,
            JoinRecordMetadata metadata,
            RecordCursorFactory masterFactory,
            ObjList<HorizonJoinSlaveState> slaveStates,
            Class<RecordSink>[] masterAsOfJoinMapSinkClasses,
            Class<RecordSink>[] slaveAsOfJoinMapSinkClasses,
            long[] offsets,
            int masterTimestampColumnIndex,
            int[] columnSources,
            int[] columnIndexes
    ) {
        super(metadata);
        this.horizonJoinMetadata = metadata;
        this.masterFactory = masterFactory;
        this.slaveStates = slaveStates;
        this.offsetCount = offsets.length;
        try {
            final long slotsPerRow = (long) offsets.length * slaveStates.size();
            cursor = new HorizonJoinProjectionRecordCursor(
                    configuration,
                    slaveStates,
                    masterAsOfJoinMapSinkClasses,
                    slaveAsOfJoinMapSinkClasses,
                    offsets,
                    Math.max(1, configuration.getSqlSmallPageFrameMaxRows() / slotsPerRow),
                    masterTimestampColumnIndex,
                    columnSources,
                    columnIndexes
            );
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        try {
            // Adopt each cursor before opening the next resource, including before of() can throw.
            cursor.masterCursor = masterFactory.getCursor(executionContext);
            // Matching jumps between the rows of a batch in horizon timestamp order.
            cursor.masterCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            for (int s = 0, n = slaveStates.size(); s < n; s++) {
                final TimeFrameCursor slaveCursor = slaveStates.getQuick(s).getFactory().getTimeFrameCursor(executionContext);
                cursor.slaveCursors.setQuick(s, slaveCursor);
                // Emitting a batch revisits the slave rows of every offset of a master row.
                slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            }
            cursor.of(executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor, th);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        // Rows follow the master in ascending timestamp order, one row per offset.
        return SCAN_DIRECTION_FORWARD;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Horizon Join Projection").meta("offsets").val(offsetCount);
        sink.child(masterFactory);
        for (int s = 0, n = slaveStates.size(); s < n; s++) {
            sink.child(slaveStates.getQuick(s).getFactory());
        }
    }

    @Override
    public boolean usesExternalDataSource() {
        if (masterFactory != null && masterFactory.usesExternalDataSource()) {
            return true;
        }
        for (int s = 0, n = slaveStates != null ? slaveStates.size() : 0; s < n; s++) {
            if (slaveStates.getQuick(s).getFactory().usesExternalDataSource()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void _close() {
        final HorizonJoinProjectionRecordCursor cursor = this.cursor;
        this.cursor = null;
        final RecordCursorFactory masterFactory = this.masterFactory;
        this.masterFactory = null;
        final ObjList<HorizonJoinSlaveState> slaveStates = this.slaveStates;
        this.slaveStates = null;
        final JoinRecordMetadata metadata = horizonJoinMetadata;
        horizonJoinMetadata = null;
        Throwable failure = null;
        if (cursor != null) {
            failure = Misc.freeBestEffort(null, cursor);
            failure = Misc.freeBestEffort(failure, cursor.matcher);
        }
        failure = Misc.freeBestEffort(failure, masterFactory);
        failure = Misc.freeObjListBestEffort(failure, slaveStates);
        failure = Misc.freeBestEffort(failure, metadata);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static class HorizonJoinProjectionRecordCursor implements NoRandomAccessRecordCursor {
        private final long batchCapacity;
        private final AsyncHorizonTimestampIterator horizonIterator;
        private final Counter masterRowCounter = new Counter();
        private final int masterTimestampIndex;
        private final ObjList<Record> matchedSlaveRecords;
        private final HorizonJoinMatcher matcher;
        private final ObjList<Record> nullSlaveRecords;
        private final int offsetCount;
        private final long[] offsets;
        private final MultiHorizonJoinRecord record;
        // Row ids and timestamps of the master rows in the current batch.
        private final DirectLongList rowIds;
        private final ObjList<TimeFrameCursor> slaveCursors;
        private final int slaveCount;
        private final ObjList<SymbolTableSource> slaveSymbolTableSources;
        // Slave row ids of the current batch: row i and offset k start at (i * offsetCount + k) * slaveCount.
        private final DirectLongList slots;
        private final MultiHorizonJoinSymbolTableSource symbolTableSource;
        private final DirectLongList timestamps;
        private long batchRowCount;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterExhausted;
        private RecordCursor masterCursor;
        private Record masterRecord;
        private long masterTimestamp;
        private int offsetPosition;
        private long rowPosition;

        private HorizonJoinProjectionRecordCursor(
                CairoConfiguration configuration,
                ObjList<HorizonJoinSlaveState> slaveStates,
                Class<RecordSink>[] masterAsOfJoinMapSinkClasses,
                Class<RecordSink>[] slaveAsOfJoinMapSinkClasses,
                long[] offsets,
                long batchCapacity,
                int masterTimestampIndex,
                int[] columnSources,
                int[] columnIndexes
        ) {
            this.slaveCount = slaveStates.size();
            this.offsets = offsets;
            this.offsetCount = offsets.length;
            this.batchCapacity = batchCapacity;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveCursors = new ObjList<>(slaveCount);
            slaveCursors.setPos(slaveCount);
            this.slaveSymbolTableSources = new ObjList<>(slaveCount);
            slaveSymbolTableSources.setPos(slaveCount);
            this.matchedSlaveRecords = new ObjList<>(slaveCount);
            matchedSlaveRecords.setPos(slaveCount);
            this.nullSlaveRecords = new ObjList<>(slaveCount);
            this.record = new MultiHorizonJoinRecord(slaveCount);
            record.init(columnSources, columnIndexes);
            this.symbolTableSource = new MultiHorizonJoinSymbolTableSource(columnSources, columnIndexes, slaveCount);
            this.horizonIterator = new AsyncHorizonTimestampIterator(offsets);
            // The lists allocate once a cursor opens, under the per-query tracker of() binds.
            this.rowIds = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
            this.timestamps = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
            this.slots = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
            for (int s = 0; s < slaveCount; s++) {
                // Typed NULL records keep the column type semantics of an unmatched slave row: a
                // SYMBOL column reads VALUE_IS_NULL as its key, not INT_NULL.
                nullSlaveRecords.add(NullRecordFactory.getInstance(slaveStates.getQuick(s).getFactory().getMetadata()));
            }
            this.matcher = new HorizonJoinMatcher(configuration, slaveStates, masterAsOfJoinMapSinkClasses, slaveAsOfJoinMapSinkClasses);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            // The rows left in the current batch, then offsetCount rows per master row left.
            if (rowPosition < batchRowCount) {
                counter.add((batchRowCount - rowPosition) * offsetCount - offsetPosition);
            }
            rowPosition = batchRowCount;
            offsetPosition = 0;
            if (!isMasterExhausted) {
                masterRowCounter.clear();
                masterCursor.calculateSize(circuitBreaker, masterRowCounter);
                counter.add(masterRowCounter.get() * offsetCount);
                isMasterExhausted = true;
            }
        }

        @Override
        public void close() {
            final RecordCursor masterCursor = this.masterCursor;
            this.masterCursor = null;
            masterRecord = null;
            Throwable failure = Misc.freeBestEffort(null, masterCursor);
            failure = Misc.freeObjListBestEffort(failure, slaveCursors);
            // Keep the matcher and the lists so the cached factory can reopen them on its next execution.
            failure = Misc.clearBestEffort(failure, matcher);
            failure = Misc.freeBestEffort(failure, rowIds);
            failure = Misc.freeBestEffort(failure, timestamps);
            failure = Misc.freeBestEffort(failure, slots);
            failure = Misc.freeBestEffort(failure, horizonIterator);
            CairoException.rethrowCleanupFailure(failure);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbolTableSource.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (rowPosition == batchRowCount && !nextBatch()) {
                return false;
            }
            if (offsetPosition == 0) {
                masterCursor.recordAt(masterRecord, rowIds.get(rowPosition));
                masterTimestamp = timestamps.get(rowPosition);
            }
            final long slotAddress = slots.getAddress() + (((rowPosition * offsetCount + offsetPosition) * slaveCount) << 3);
            for (int s = 0; s < slaveCount; s++) {
                final long slaveRowId = Unsafe.getLong(slotAddress + ((long) s << 3));
                if (slaveRowId != Long.MIN_VALUE) {
                    // Matching the batch has finished, so positioning the helper's record for
                    // output cannot disturb a lookup.
                    final HorizonJoinTimeFrameHelper helper = matcher.getHelper(s);
                    helper.recordAt(slaveRowId);
                    matchedSlaveRecords.setQuick(s, helper.getRecord());
                } else {
                    matchedSlaveRecords.setQuick(s, nullSlaveRecords.getQuick(s));
                }
            }
            final long offset = offsets[offsetPosition];
            // Matching already added every offset to the master timestamp without overflow.
            record.of(masterRecord, offset, masterTimestamp + offset, matchedSlaveRecords);
            if (++offsetPosition == offsetCount) {
                offsetPosition = 0;
                rowPosition++;
            }
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return symbolTableSource.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            final long masterSize = masterCursor.size();
            return masterSize >= 0 && masterSize <= Long.MAX_VALUE / offsetCount ? masterSize * offsetCount : -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            batchRowCount = 0;
            rowPosition = 0;
            offsetPosition = 0;
            isMasterExhausted = false;
        }

        private boolean nextBatch() {
            if (isMasterExhausted) {
                return false;
            }
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            rowIds.clear();
            timestamps.clear();
            final Record masterRecordA = masterCursor.getRecord();
            long rowCount = 0;
            while (rowCount < batchCapacity && masterCursor.hasNext()) {
                rowIds.add(masterRecordA.getRowId());
                timestamps.add(masterRecordA.getTimestamp(masterTimestampIndex));
                rowCount++;
            }
            isMasterExhausted = rowCount < batchCapacity;
            batchRowCount = rowCount;
            rowPosition = 0;
            offsetPosition = 0;
            if (rowCount == 0) {
                return false;
            }

            final long slotCount = rowCount * offsetCount * slaveCount;
            slots.clear();
            slots.ensureCapacity(slotCount);
            final long slotsAddress = slots.getAddress();
            // The timestamps are contiguous and ascending, so the iterator can read them as a frame.
            horizonIterator.of(timestamps.getAddress(), 0, rowCount);
            matcher.toTop();
            final boolean isKeyed = matcher.isKeyed();
            while (horizonIterator.next()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final long rowIndex = horizonIterator.getMasterRowIndex();
                if (isKeyed) {
                    masterCursor.recordAt(masterRecord, rowIds.get(rowIndex));
                }
                final long slot = (rowIndex * offsetCount + horizonIterator.getOffsetIndex()) * slaveCount;
                matcher.match(horizonIterator.getHorizonTimestamp(), masterRecord, slotsAddress + (slot << 3));
            }
            return true;
        }

        private void of(SqlExecutionContext executionContext) {
            circuitBreaker = executionContext.getCircuitBreaker();
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            masterRecord = masterCursor.getRecordB();
            batchRowCount = 0;
            rowPosition = 0;
            offsetPosition = 0;
            isMasterExhausted = false;
            rowIds.setMemoryTracker(executionContext.getMemoryTracker());
            timestamps.setMemoryTracker(executionContext.getMemoryTracker());
            slots.setMemoryTracker(executionContext.getMemoryTracker());
            final long initialCapacity = Math.min(batchCapacity, 1024);
            rowIds.setCapacity(initialCapacity);
            timestamps.setCapacity(initialCapacity);
            for (int s = 0; s < slaveCount; s++) {
                final TimeFrameCursor slaveCursor = slaveCursors.getQuick(s);
                slaveSymbolTableSources.setQuick(s, slaveCursor);
                matcher.of(s, slaveCursor, masterCursor, slaveCursor, executionContext.getMemoryTracker());
            }
            symbolTableSource.of(masterCursor, slaveSymbolTableSources);
        }
    }
}
