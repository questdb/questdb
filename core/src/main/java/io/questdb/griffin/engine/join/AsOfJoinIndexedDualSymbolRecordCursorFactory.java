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
import io.questdb.cairo.idx.CoveringRowCursor;
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
import io.questdb.std.IntList;
import io.questdb.std.LongLongHashMap;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * ASOF JOIN on two SYMBOL keys, the first of which is indexed on the right-hand table, taken on the
 * asof_index hint. Per first key, it walks that key's index rows back from the ASOF position and
 * records the latest row of every second key it meets, until the master row's second key turns up;
 * a later position reads only the index rows above the last one. Each index row of a first key is
 * read once per execution, however many second keys ask for it, so a join costs the rows of its
 * master's first keys, not the rows of every key as the Dense join's scan does. The second key is
 * read from the index's covering sidecar when the index includes it, else from its column.
 */
public final class AsOfJoinIndexedDualSymbolRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private static final long NO_ENTRY = Long.MIN_VALUE + 1;
    private final AsOfJoinIndexedDualSymbolRecordCursor cursor;
    private final SymbolJoinKeyMapping indexedKeyMapping;
    // The covering sidecar include index of the second key, or -1; its getCursor() argument, or null
    private final int[] otherIncludeColumns;
    private final int otherIncludeIndex;
    private final SymbolJoinKeyMapping otherKeyMapping;
    private final int slaveIndexedColumnIndex;
    private final int slaveOtherColumnIndex;
    private final long toleranceInterval;

    /**
     * @param slaveIndexedColumnIndex the indexed slave column of the first key, which indexedKeyMapping translates to
     * @param slaveOtherColumnIndex   the slave column of the second key, which otherKeyMapping translates to
     * @param otherIncludeIndex       the second key's include index in the covering sidecar of the first
     *                                key's index, or -1 when the index does not include it
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
            int otherIncludeIndex,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.slaveIndexedColumnIndex = slaveIndexedColumnIndex;
        this.slaveOtherColumnIndex = slaveOtherColumnIndex;
        this.indexedKeyMapping = indexedKeyMapping;
        this.otherKeyMapping = otherKeyMapping;
        this.otherIncludeIndex = otherIncludeIndex;
        this.otherIncludeColumns = otherIncludeIndex >= 0 ? new int[]{otherIncludeIndex} : null;
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
        if (otherIncludeIndex >= 0) {
            // the second key is read from the index's covering sidecar
            sink.attr("covered").val(otherIncludeIndex);
        }
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
        // (first key, second key) -> its row in the catalog of the first key, see catalogPositions
        private final LongLongHashMap catalogRows = new LongLongHashMap(64, 0.5, NO_ENTRY, NO_ENTRY);
        // (first key, second key) -> the ASOF position of the pair's last lookup, and its match there
        private final LongLongHashMap pairMatches = new LongLongHashMap(64, 0.5, NO_ENTRY, NO_ENTRY);
        private final LongLongHashMap pairPositions = new LongLongHashMap(64, 0.5, NO_ENTRY, NO_ENTRY);
        // The second keys a walk met, and the first row of each, for a catalog of the walk's first key
        private final IntList walkOtherKeys = new IntList();
        private final LongLongHashMap walkRows = new LongLongHashMap(64, 0.5, NO_ENTRY, NO_ENTRY);
        // Per first key: the position of its catalog, the latest row of every second key at or below
        // it, which a walk that read all of the key's rows at or below it recorded; NO_ENTRY: none
        private long[] catalogPositions = new long[0];

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
            resetMemos();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            indexedKeyMapping.setMemoryTracker(tracker);
            otherKeyMapping.setMemoryTracker(tracker);
        }

        @Override
        public void toTop() {
            super.toTop();
            resetMemos();
        }

        private long catalogPosition(int indexKey) {
            return indexKey < catalogPositions.length ? catalogPositions[indexKey] : NO_ENTRY;
        }

        private void resetMemos() {
            pairPositions.clear();
            pairMatches.clear();
            catalogRows.clear();
            Arrays.fill(catalogPositions, NO_ENTRY);
        }

        // Walks the first key's index rows from hiRowId down, above loRowId unless that is
        // Long.MIN_VALUE, until a row whose second key is otherKey, and returns that row; Long.MIN_VALUE
        // when there is none. With record, it also records the first row of every second key it meets
        // in walkOtherKeys/walkRows.
        private long walkDown(int indexKey, long hiRowId, long loRowId, int otherKey, boolean record) {
            final int frameHi = Rows.toPartitionIndex(hiRowId);
            final int frameLo = loRowId == Long.MIN_VALUE ? 0 : Rows.toPartitionIndex(loRowId);
            for (int frameIndex = frameHi; frameIndex >= frameLo; frameIndex--) {
                slaveTimeFrameCursor.jumpTo(frameIndex);
                if (slaveTimeFrameCursor.open() == 0) {
                    continue;
                }
                final long frameRowLo = slaveTimeFrame.getRowLo();
                final long localHi = frameIndex == frameHi ? Rows.toLocalRowID(hiRowId) : slaveTimeFrame.getRowHi() - 1;
                final long localLo = frameIndex == frameLo && loRowId != Long.MIN_VALUE ? Rows.toLocalRowID(loRowId) + 1 : frameRowLo;
                if (localLo > localHi) {
                    continue;
                }
                final IndexReader indexReader = slaveTimeFrameCursor.getIndexReaderForCurrentFrame(slaveIndexedColumnIndex, IndexReader.DIR_BACKWARD);
                // the index numbers the frame's rows from getIndexRowLoForCurrentFrame(), and the
                // cursor returns them relative to its minValue
                final long indexRowLo = slaveTimeFrameCursor.getIndexRowLoForCurrentFrame();
                try (RowCursor rowCursor = otherIncludeColumns != null
                        ? indexReader.getCursor(indexKey, indexRowLo + localLo, indexRowLo + localHi, otherIncludeColumns)
                        : indexReader.getCursor(indexKey, indexRowLo + localLo, indexRowLo + localHi)) {
                    final CoveringRowCursor covering = rowCursor instanceof CoveringRowCursor c && c.isCoveredAvailable(otherIncludeIndex) ? c : null;
                    while (rowCursor.hasNext()) {
                        final long rowId = Rows.toRowID(frameIndex, localLo + rowCursor.next());
                        final int rowOtherKey;
                        if (covering != null) {
                            rowOtherKey = covering.getCoveredInt(otherIncludeIndex);
                        } else {
                            slaveTimeFrameCursor.recordAt(slaveRecB, rowId);
                            rowOtherKey = slaveRecB.getInt(slaveOtherColumnIndex);
                        }
                        if (rowOtherKey == otherKey) {
                            return rowId;
                        }
                        if (record && walkRows.get(rowOtherKey) == NO_ENTRY) {
                            // downwards: the first row met is the second key's latest
                            walkRows.put(rowOtherKey, rowId);
                            walkOtherKeys.add(rowOtherKey);
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
            final int indexKey = TableUtils.toIndexKey(key0);
            final long position = slaveRecB.getRowId();
            final long pair = pack(key0, key1);
            final long lastPosition = pairPositions.get(pair);
            long matchRowId;
            if (lastPosition != NO_ENTRY && lastPosition <= position) {
                // the rows at or below the pair's last position hold its last match at best
                matchRowId = walkDown(indexKey, position, lastPosition, key1, false);
                if (matchRowId == Long.MIN_VALUE) {
                    matchRowId = pairMatches.get(pair);
                }
            } else {
                final long catalogPosition = catalogPosition(indexKey);
                if (catalogPosition != NO_ENTRY && catalogPosition <= position) {
                    // the rows at or below the catalog's position hold the catalog's row at best
                    matchRowId = walkDown(indexKey, position, catalogPosition, key1, false);
                    if (matchRowId == Long.MIN_VALUE) {
                        final long catalogRow = catalogRows.get(pair);
                        matchRowId = catalogRow == NO_ENTRY ? Long.MIN_VALUE : catalogRow;
                    }
                } else {
                    // A walk that reads all of the key's rows at or below the position without the
                    // pair records the latest row of every other pair met: the key's catalog.
                    walkOtherKeys.clear();
                    walkRows.clear();
                    matchRowId = walkDown(indexKey, position, Long.MIN_VALUE, key1, true);
                    if (matchRowId == Long.MIN_VALUE) {
                        if (indexKey >= catalogPositions.length) {
                            final int capacity = Math.max(indexKey + 1, catalogPositions.length * 2);
                            final int oldLength = catalogPositions.length;
                            catalogPositions = Arrays.copyOf(catalogPositions, capacity);
                            Arrays.fill(catalogPositions, oldLength, capacity, NO_ENTRY);
                        }
                        catalogPositions[indexKey] = position;
                        for (int i = 0, n = walkOtherKeys.size(); i < n; i++) {
                            final int otherKey = walkOtherKeys.getQuick(i);
                            catalogRows.put(pack(key0, otherKey), walkRows.get(otherKey));
                        }
                    }
                }
            }
            pairPositions.put(pair, position);
            pairMatches.put(pair, matchRowId);
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
