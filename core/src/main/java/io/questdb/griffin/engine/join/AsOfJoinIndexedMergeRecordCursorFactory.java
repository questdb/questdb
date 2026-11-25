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
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * ASOF JOIN on one SYMBOL key that is indexed on the right-hand table, taken on the asof_index hint.
 * It never positions on the right-hand table's timeline: per master row, a forward cursor over the
 * index rows of the row's key moves up to the master timestamp, into later frames as time passes.
 * A key starts, and starts over when its master timestamps go back, with one backward index lookup.
 * A join reads each index row of its master's keys once, so it costs the master rows and the rows of
 * their keys, not the rows of every key in between as a scan of the right-hand table does.
 */
public final class AsOfJoinIndexedMergeRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    // Forward cursors one join keeps open at most: further keys start over at every master row
    private static final int MAX_KEY_CURSORS = 1024;
    // The cursor of a key whose frame has no rows above its position
    private static final RowCursor NO_ROWS = new RowCursor() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long next() {
            throw new UnsupportedOperationException();
        }
    };
    private final AsOfJoinIndexedMergeRecordCursor cursor;
    private final int slaveSymbolColumnIndex;
    private final SymbolJoinKeyMapping symbolJoinKeyMapping;
    private final long toleranceInterval;

    public AsOfJoinIndexedMergeRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int slaveSymbolColumnIndex,
            SymbolJoinKeyMapping symbolJoinKeyMapping,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.symbolJoinKeyMapping = symbolJoinKeyMapping;
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        this.toleranceInterval = toleranceInterval;
        final RecordMetadata masterMeta = masterFactory.getMetadata();
        final RecordMetadata slaveMeta = slaveFactory.getMetadata();
        this.cursor = new AsOfJoinIndexedMergeRecordCursor(
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
        sink.type("AsOf Join Indexed Merge");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, symbolJoinKeyMapping);
        CairoException.rethrowCleanupFailure(failure);
    }

    private class AsOfJoinIndexedMergeRecordCursor extends AbstractAsOfJoinFastRecordCursor {
        // Per frame, its first timestamp and its row bounds, read once per execution
        private final LongList frameRowHis = new LongList();
        private final LongList frameRowLos = new LongList();
        private final LongList frameTsLos = new LongList();
        // Per index key: a forward cursor over the key's index rows in frame keyFrames[key], its next
        // local row (keyNextRows, -1 when done), the local row the cursor's values count from
        // (keyBases), the latest row at or below the ASOF position of the last master timestamp
        // (keyMatches) and that timestamp.
        private final ObjList<RowCursor> keyCursors = new ObjList<>();
        private long[] keyBases = new long[0];
        private int keyCursorCount;
        private int[] keyFrames = new int[0];
        private long[] keyMatches = new long[0];
        private long[] keyNextRows = new long[0];
        private long[] keyTimestamps = new long[0];
        private SqlExecutionCircuitBreaker circuitBreaker;
        // The ASOF position of positionTimestamp: frame -1 when no row is at or below it, else the last
        // local row at or below it, which is the frame's row lo - 1 when only earlier frames have such rows
        private int positionFrame = -1;
        private long positionRow;
        private long positionTimestamp;

        public AsOfJoinIndexedMergeRecordCursor(
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
            closeKeyCursors();
            symbolJoinKeyMapping.close();
            super.close();
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (!masterCursor.hasNext()) {
                return false;
            }
            final int indexKey = TableUtils.toIndexKey(symbolJoinKeyMapping.getSlaveKey(masterRecord));
            if (indexKey < 0) {
                record.hasSlave(false);
                return true;
            }
            final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
            updatePosition(masterTimestamp);
            final long matchRowId = findMatch(indexKey, masterTimestamp);
            if (matchRowId == Long.MIN_VALUE) {
                record.hasSlave(false);
                return true;
            }
            slaveTimeFrameCursor.recordAt(slaveRecB, matchRowId);
            final long slaveTimestamp = slaveTimestamp(slaveRecB);
            record.hasSlave(toleranceInterval == Numbers.LONG_NULL || slaveTimestamp >= masterTimestamp - toleranceInterval);
            return true;
        }

        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            symbolJoinKeyMapping.reopen();
            super.of(masterCursor, slaveCursor);
            this.circuitBreaker = circuitBreaker;
            symbolJoinKeyMapping.of(slaveCursor);
            closeKeyCursors();
            positionFrame = -1;
            frameTsLos.clear();
            frameRowLos.clear();
            frameRowHis.clear();
            slaveTimeFrameCursor.toTop();
            while (slaveTimeFrameCursor.next()) {
                slaveTimeFrameCursor.open();
                frameTsLos.add(scaleTimestamp(slaveTimeFrame.getTimestampLo(), slaveTimestampScale));
                frameRowLos.add(slaveTimeFrame.getRowLo());
                frameRowHis.add(slaveTimeFrame.getRowHi());
            }
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            symbolJoinKeyMapping.setMemoryTracker(tracker);
        }

        @Override
        public void toTop() {
            super.toTop();
            closeKeyCursors();
            positionFrame = -1;
        }

        private void closeKeyCursors() {
            for (int i = 0, n = keyCursors.size(); i < n; i++) {
                final RowCursor cursor = keyCursors.getQuick(i);
                if (cursor != null) {
                    cursor.close();
                    keyCursors.setQuick(i, null);
                }
            }
            keyCursorCount = 0;
        }

        // The latest frame whose first timestamp is at or below the timestamp, or -1.
        private int findFrame(long timestamp) {
            int lo = 0;
            int hi = frameTsLos.size() - 1;
            int frame = -1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                if (frameTsLos.getQuick(mid) <= timestamp) {
                    frame = mid;
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            return frame;
        }

        // The ASOF match of the key at the master timestamp: the key's latest row at or below the ASOF
        // position, see updatePosition(). The key's cursor moves up to the position by row numbers.
        private long findMatch(int indexKey, long masterTimestamp) {
            final RowCursor cursor = indexKey < keyCursors.size() ? keyCursors.getQuick(indexKey) : null;
            if (cursor == null || keyTimestamps[indexKey] > masterTimestamp) {
                return startKey(indexKey, masterTimestamp);
            }
            keyTimestamps[indexKey] = masterTimestamp;
            if (positionFrame == -1) {
                return Long.MIN_VALUE;
            }
            long matchRowId = keyMatches[indexKey];
            if (keyFrames[indexKey] < positionFrame) {
                // The position moved past the key's frame: its latest row lies in the frames from the
                // key's frame to the one before the position's, or is the old match.
                final long latest = findLatestBackward(indexKey, positionFrame - 1, frameRowHis.getQuick(positionFrame - 1) - 1, keyFrames[indexKey]);
                if (latest != Long.MIN_VALUE) {
                    matchRowId = latest;
                }
                cursor.close();
                final RowCursor frameCursor = openKeyCursor(indexKey, positionFrame, frameRowLos.getQuick(positionFrame));
                keyCursors.setQuick(indexKey, frameCursor);
                keyFrames[indexKey] = positionFrame;
                keyNextRows[indexKey] = frameCursor.hasNext() ? keyBases[indexKey] + frameCursor.next() : -1;
            }
            final RowCursor frameCursor = keyCursors.getQuick(indexKey);
            long nextRow = keyNextRows[indexKey];
            while (nextRow != -1 && nextRow <= positionRow) {
                matchRowId = Rows.toRowID(positionFrame, nextRow);
                nextRow = frameCursor.hasNext() ? keyBases[indexKey] + frameCursor.next() : -1;
            }
            keyNextRows[indexKey] = nextRow;
            keyMatches[indexKey] = matchRowId;
            return matchRowId;
        }

        // The key's latest row in the frames from frameHi down to frameLo, at or below rowHi in frameHi;
        // Long.MIN_VALUE when there is none.
        private long findLatestBackward(int indexKey, int frameHi, long rowHi, int frameLo) {
            long rowMax = rowHi;
            for (int f = frameHi; f >= frameLo; f--) {
                final long rowLo = frameRowLos.getQuick(f);
                if (f < frameHi) {
                    rowMax = frameRowHis.getQuick(f) - 1;
                }
                if (rowMax < rowLo) {
                    continue;
                }
                slaveTimeFrameCursor.jumpTo(f);
                slaveTimeFrameCursor.open();
                final IndexReader indexReader = slaveTimeFrameCursor.getIndexReaderForCurrentFrame(slaveSymbolColumnIndex, IndexReader.DIR_BACKWARD);
                final long indexRowLo = slaveTimeFrameCursor.getIndexRowLoForCurrentFrame();
                try (RowCursor rowCursor = indexReader.getCursor(indexKey, indexRowLo + rowLo, indexRowLo + rowMax)) {
                    if (rowCursor.hasNext()) {
                        // backward: the first entry is the latest
                        return Rows.toRowID(f, rowLo + rowCursor.next());
                    }
                }
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
            return Long.MIN_VALUE;
        }

        // The last local row of the frame at or below the timestamp, or frame row lo - 1.
        private long findRow(int frameIndex, long timestamp) {
            long lo = frameRowLos.getQuick(frameIndex);
            long hi = frameRowHis.getQuick(frameIndex) - 1;
            long row = lo - 1;
            if (lo <= hi) {
                probeFrame(frameIndex);
            }
            while (lo <= hi) {
                final long mid = (lo + hi) >>> 1;
                if (probeTimestamp(mid) <= timestamp) {
                    row = mid;
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            return row;
        }

        // A forward cursor over the key's index rows of the frame from fromLocalRow on.
        private RowCursor openKeyCursor(int indexKey, int frameIndex, long fromLocalRow) {
            keyBases[indexKey] = fromLocalRow;
            if (fromLocalRow >= frameRowHis.getQuick(frameIndex)) {
                return NO_ROWS;
            }
            slaveTimeFrameCursor.jumpTo(frameIndex);
            slaveTimeFrameCursor.open();
            final IndexReader indexReader = slaveTimeFrameCursor.getIndexReaderForCurrentFrame(slaveSymbolColumnIndex, IndexReader.DIR_FORWARD);
            final long indexRowLo = slaveTimeFrameCursor.getIndexRowLoForCurrentFrame();
            // the cursor returns rows relative to its minValue
            return indexReader.getCursor(indexKey, indexRowLo + fromLocalRow, indexRowLo + frameRowHis.getQuick(frameIndex) - 1);
        }

        // Puts slaveRecA on the frame, for probeTimestamp() of its rows.
        private void probeFrame(int frameIndex) {
            slaveTimeFrameCursor.recordAt(slaveRecA, Rows.toRowID(frameIndex, frameRowLos.getQuick(frameIndex)));
        }

        // The timestamp of a row of the frame that probeFrame() put slaveRecA on.
        private long probeTimestamp(long localRow) {
            slaveTimeFrameCursor.recordAtRowIndex(slaveRecA, localRow);
            return slaveTimestamp(slaveRecA);
        }

        private long slaveTimestamp(Record slaveRecord) {
            return scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
        }

        // The ASOF match of a key the join has no cursor for at the master timestamp: one backward
        // index lookup from the ASOF position, then a forward cursor above it.
        private long startKey(int indexKey, long masterTimestamp) {
            if (indexKey < keyCursors.size()) {
                final RowCursor cursor = keyCursors.getQuick(indexKey);
                if (cursor != null) {
                    cursor.close();
                    keyCursors.setQuick(indexKey, null);
                    keyCursorCount--;
                }
            }
            if (positionFrame == -1) {
                return Long.MIN_VALUE;
            }
            final long matchRowId = findLatestBackward(indexKey, positionFrame, positionRow, 0);
            if (keyCursorCount >= MAX_KEY_CURSORS) {
                return matchRowId;
            }
            if (indexKey >= keyFrames.length) {
                final int capacity = Math.max(indexKey + 1, keyFrames.length * 2);
                keyFrames = Arrays.copyOf(keyFrames, capacity);
                keyNextRows = Arrays.copyOf(keyNextRows, capacity);
                keyMatches = Arrays.copyOf(keyMatches, capacity);
                keyTimestamps = Arrays.copyOf(keyTimestamps, capacity);
                keyBases = Arrays.copyOf(keyBases, capacity);
            }
            if (indexKey >= keyCursors.size()) {
                keyCursors.extendAndSet(indexKey, null);
            }
            final RowCursor cursor = openKeyCursor(indexKey, positionFrame, positionRow + 1);
            keyCursors.setQuick(indexKey, cursor);
            keyCursorCount++;
            keyNextRows[indexKey] = cursor.hasNext() ? keyBases[indexKey] + cursor.next() : -1;
            keyFrames[indexKey] = positionFrame;
            keyMatches[indexKey] = matchRowId;
            keyTimestamps[indexKey] = masterTimestamp;
            return matchRowId;
        }

        // Moves the ASOF position, the last slave row at or below the master timestamp, to the
        // timestamp: forward from the last position by an exponential probe, or by a binary search when
        // the timestamp went back or moved into another frame. positionFrame -1: no row at or below it.
        private void updatePosition(long masterTimestamp) {
            if (positionFrame != -1 && masterTimestamp >= positionTimestamp) {
                int frame = positionFrame;
                while (frame + 1 < frameTsLos.size() && frameTsLos.getQuick(frame + 1) <= masterTimestamp) {
                    frame++;
                }
                if (frame != positionFrame) {
                    positionFrame = frame;
                    positionRow = findRow(frame, masterTimestamp);
                } else {
                    // rows above positionRow, up to the frame's end, by doubling steps
                    final long rowHi = frameRowHis.getQuick(frame) - 1;
                    long lo = positionRow;
                    long step = 1;
                    probeFrame(frame);
                    while (lo + step <= rowHi && probeTimestamp(lo + step) <= masterTimestamp) {
                        lo += step;
                        step <<= 1;
                    }
                    long hi = Math.min(lo + step, rowHi + 1);
                    // the last row at or below the timestamp lies in [lo, hi)
                    while (hi - lo > 1) {
                        final long mid = (lo + hi) >>> 1;
                        if (probeTimestamp(mid) <= masterTimestamp) {
                            lo = mid;
                        } else {
                            hi = mid;
                        }
                    }
                    positionRow = lo;
                }
            } else {
                positionFrame = findFrame(masterTimestamp);
                positionRow = positionFrame != -1 ? findRow(positionFrame, masterTimestamp) : -1;
            }
            positionTimestamp = masterTimestamp;
        }
    }
}
