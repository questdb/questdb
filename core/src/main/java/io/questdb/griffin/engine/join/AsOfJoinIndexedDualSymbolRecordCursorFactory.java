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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.LongLongHashMap;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;

/**
 * ASOF JOIN on two SYMBOL keys, the first of which is indexed on the right-hand table. Per master
 * row, it walks the index rows of the first key back from the ASOF position until the second key
 * matches, so a lookup costs the rows of its own first key, not the rows of every key in between as
 * the Dense join's scan does. It remembers, per key pair, the last position and its match: a later
 * position of the pair reads only the index rows above the last one. Taken on the asof_index hint.
 */
public final class AsOfJoinIndexedDualSymbolRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private static final long NO_ENTRY = Long.MIN_VALUE + 1;
    private final AsOfJoinIndexedDualSymbolRecordCursor cursor;
    private final SymbolJoinKeyMapping indexedKeyMapping;
    private final SymbolJoinKeyMapping otherKeyMapping;
    private final int slaveIndexedColumnIndex;
    private final int slaveOtherColumnIndex;
    private final long toleranceInterval;

    /**
     * @param slaveIndexedColumnIndex the indexed slave column of the first key, which indexedKeyMapping translates to
     * @param slaveOtherColumnIndex   the slave column of the second key, which otherKeyMapping translates to
     */
    public AsOfJoinIndexedDualSymbolRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int slaveIndexedColumnIndex,
            int slaveOtherColumnIndex,
            SymbolJoinKeyMapping indexedKeyMapping,
            SymbolJoinKeyMapping otherKeyMapping,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.slaveIndexedColumnIndex = slaveIndexedColumnIndex;
        this.slaveOtherColumnIndex = slaveOtherColumnIndex;
        this.indexedKeyMapping = indexedKeyMapping;
        this.otherKeyMapping = otherKeyMapping;
        this.toleranceInterval = toleranceInterval;
        final RecordMetadata masterMeta = masterFactory.getMetadata();
        final RecordMetadata slaveMeta = slaveFactory.getMetadata();
        this.cursor = new AsOfJoinIndexedDualSymbolRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveMeta),
                masterMeta.getTimestampIndex(),
                masterMeta.getTimestampType(),
                slaveMeta.getTimestampIndex(),
                slaveMeta.getTimestampType(),
                configuration.getSqlAsOfJoinLookAhead()
        );
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            // Bind before of(), which reopens the symbol key caches before adopting the cursors.
            cursor.setMemoryTracker(executionContext.getMemoryTracker());
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("AsOf Join Indexed Dual Symbol");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, indexedKeyMapping);
        failure = Misc.freeBestEffort(failure, otherKeyMapping);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static long pack(int key0, int key1) {
        return ((long) key0 << 32) | (key1 & 0xffffffffL);
    }

    private class AsOfJoinIndexedDualSymbolRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {
        // key pair -> the ASOF position of its last lookup, and the match found there (Long.MIN_VALUE: none)
        private final LongLongHashMap lastMatches = new LongLongHashMap(64, 0.5, NO_ENTRY, NO_ENTRY);
        private final LongLongHashMap lastPositions = new LongLongHashMap(64, 0.5, NO_ENTRY, NO_ENTRY);

        public AsOfJoinIndexedDualSymbolRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
        }

        @Override
        public void close() {
            indexedKeyMapping.close();
            otherKeyMapping.close();
            super.close();
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            // Reopen the symbol key caches before super.of() adopts the cursors so an open-time breach frees each exactly once.
            indexedKeyMapping.reopen();
            otherKeyMapping.reopen();
            super.of(masterCursor, slaveCursor, circuitBreaker);
            indexedKeyMapping.of(slaveCursor);
            otherKeyMapping.of(slaveCursor);
            lastPositions.clear();
            lastMatches.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            indexedKeyMapping.setMemoryTracker(tracker);
            otherKeyMapping.setMemoryTracker(tracker);
        }

        @Override
        public void toTop() {
            super.toTop();
            lastPositions.clear();
            lastMatches.clear();
        }

        // The latest row at or below hiRowId, and above loRowId unless that is Long.MIN_VALUE, whose
        // indexed key is indexKey and whose other key is otherKey; Long.MIN_VALUE when there is none.
        private long findLatest(int indexKey, int otherKey, long loRowId, long hiRowId) {
            final int frameHi = Rows.toPartitionIndex(hiRowId);
            final int frameLo = loRowId == Long.MIN_VALUE ? 0 : Rows.toPartitionIndex(loRowId);
            for (int frameIndex = frameHi; frameIndex >= frameLo; frameIndex--) {
                slaveTimeFrameCursor.jumpTo(frameIndex);
                if (slaveTimeFrameCursor.open() == 0) {
                    continue;
                }
                final long frameRowLo = slaveTimeFrame.getRowLo();
                final long localHi = frameIndex == frameHi ? Rows.toLocalRowID(hiRowId) : slaveTimeFrame.getRowHi() - 1;
                final long localLo = frameIndex == frameLo && loRowId != Long.MIN_VALUE
                        ? Rows.toLocalRowID(loRowId) + 1
                        : frameRowLo;
                if (localLo > localHi) {
                    continue;
                }
                final IndexReader indexReader = slaveTimeFrameCursor.getIndexReaderForCurrentFrame(
                        slaveIndexedColumnIndex,
                        IndexReader.DIR_BACKWARD
                );
                // the index numbers the frame's rows from getIndexRowLoForCurrentFrame(), and the
                // cursor returns them relative to its minValue
                final long indexRowLo = slaveTimeFrameCursor.getIndexRowLoForCurrentFrame();
                try (RowCursor rowCursor = indexReader.getCursor(indexKey, indexRowLo + localLo, indexRowLo + localHi)) {
                    while (rowCursor.hasNext()) {
                        final long rowId = Rows.toRowID(frameIndex, localLo + rowCursor.next());
                        slaveTimeFrameCursor.recordAt(slaveRecB, rowId);
                        if (slaveRecB.getInt(slaveOtherColumnIndex) == otherKey) {
                            return rowId;
                        }
                    }
                }
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
            return Long.MIN_VALUE;
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            final int key0 = indexedKeyMapping.getSlaveKey(masterRecord);
            final int key1 = otherKeyMapping.getSlaveKey(masterRecord);
            if (key0 == StaticSymbolTable.VALUE_NOT_FOUND || key1 == StaticSymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                return;
            }
            final long position = slaveRecB.getRowId();
            final long pair = pack(key0, key1);
            final long lastPosition = lastPositions.get(pair);
            long matchRowId;
            if (lastPosition != NO_ENTRY && lastPosition <= position) {
                // the rows at or below the last position hold the last match at best
                matchRowId = findLatest(TableUtils.toIndexKey(key0), key1, lastPosition, position);
                if (matchRowId == Long.MIN_VALUE) {
                    matchRowId = lastMatches.get(pair);
                }
            } else {
                matchRowId = findLatest(TableUtils.toIndexKey(key0), key1, Long.MIN_VALUE, position);
            }
            lastPositions.put(pair, position);
            lastMatches.put(pair, matchRowId);
            if (matchRowId == Long.MIN_VALUE) {
                record.hasSlave(false);
                return;
            }
            slaveTimeFrameCursor.recordAt(slaveRecB, matchRowId);
            final long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            record.hasSlave(toleranceInterval == Numbers.LONG_NULL || slaveTimestamp >= masterTimestamp - toleranceInterval);
        }
    }
}
