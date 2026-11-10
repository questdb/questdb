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

import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

import java.util.Arrays;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * The index mode of Async AsOf Join for one worker slot, see
 * {@code AsyncAsOfJoinRecordCursorFactory#joinIndexed}: per master frame, the ASOF position of each
 * master row by a galloping search over the slave timestamps, and per join key a forward cursor over
 * the key's index rows that moves up to the position by row numbers, started by a backward lookup.
 * The index readers are the slot's own, see {@code ConcurrentTimeFrameCursor#enableIndexReaders}.
 * The keys' cursors carry on from one master frame of the slot to the next of the same execution,
 * and start over when the next one's master timestamps are lower; {@link #close()} frees them.
 */
public class AsyncAsOfJoinIndexState implements QuietCloseable {
    // a frame without native column memory, or without an index reader: the span scan joins it
    public static final long UNSUPPORTED = Long.MIN_VALUE + 1;
    // rows updatePosition() reads one by one before it gallops
    private static final int LINEAR_PROBE_ROWS = 8;
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
    private final ObjList<RowCursor> keyCursors = new ObjList<>();
    private final LongList frameRowHis = new LongList();
    private final LongList frameRowLos = new LongList();
    private final LongList frameTsLos = new LongList();
    // per joinable slot: the frame of the key's cursor, its next local row (-1: done), the local row
    // its values count from, and the key's latest row at or below the last position
    private long[] keyBases = new long[0];
    private int[] keyFrames = new int[0];
    private long[] keyMatches = new long[0];
    private long[] keyNextRows = new long[0];
    private TimeFrameCursor cursor;
    private int positionFrame;
    private long positionRow;
    private long positionTimestamp;
    private int slaveSymbolIndex;
    private PageFrameMemoryRecord slaveRecord;
    private int slaveTimestampIndex;
    private long slaveTsScale;
    private long tsAddress;
    private int tsAddressFrame;

    @Override
    public void close() {
        closeKeyCursors();
        cursor = null;
        slaveRecord = null;
    }

    /**
     * The slave's latest row of the slot's key at or below the master timestamp, Long.MIN_VALUE when
     * there is none, or {@link #UNSUPPORTED}. The master timestamps of a frame ascend.
     */
    public long match(int slot, int indexKey, long masterTimestamp, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!updatePosition(masterTimestamp)) {
            return UNSUPPORTED;
        }
        if (positionFrame == -1) {
            return Long.MIN_VALUE;
        }
        RowCursor keyCursor = keyCursors.getQuiet(slot);
        long matchRowId;
        if (keyCursor == null) {
            // the key's first master row of the frame: a backward lookup from the position
            matchRowId = findLatestBackward(indexKey, positionFrame, positionRow, 0, circuitBreaker);
            if (matchRowId == UNSUPPORTED) {
                return UNSUPPORTED;
            }
            keyCursor = openKeyCursor(slot, indexKey, positionFrame, positionRow + 1);
            if (keyCursor == null) {
                return UNSUPPORTED;
            }
            keyCursors.setQuick(slot, keyCursor);
            keyFrames[slot] = positionFrame;
            keyNextRows[slot] = keyCursor.hasNext() ? keyBases[slot] + keyCursor.next() : -1;
        } else {
            matchRowId = keyMatches[slot];
            if (keyFrames[slot] < positionFrame) {
                // the position moved past the key's frame: its latest row lies in the frames from the
                // key's frame to the one before the position's, or is the old match
                final long latest = findLatestBackward(indexKey, positionFrame - 1, frameRowHis.getQuick(positionFrame - 1) - 1, keyFrames[slot], circuitBreaker);
                if (latest == UNSUPPORTED) {
                    return UNSUPPORTED;
                }
                if (latest != Long.MIN_VALUE) {
                    matchRowId = latest;
                }
                keyCursor.close();
                keyCursor = openKeyCursor(slot, indexKey, positionFrame, frameRowLos.getQuick(positionFrame));
                keyCursors.setQuick(slot, keyCursor);
                if (keyCursor == null) {
                    return UNSUPPORTED;
                }
                keyFrames[slot] = positionFrame;
                keyNextRows[slot] = keyCursor.hasNext() ? keyBases[slot] + keyCursor.next() : -1;
            }
        }
        long nextRow = keyNextRows[slot];
        while (nextRow != -1 && nextRow <= positionRow) {
            matchRowId = Rows.toRowID(positionFrame, nextRow);
            nextRow = keyCursor.hasNext() ? keyBases[slot] + keyCursor.next() : -1;
        }
        keyNextRows[slot] = nextRow;
        keyMatches[slot] = matchRowId;
        return matchRowId;
    }

    /**
     * Starts a master frame: the slot's slave cursor and record, and the slave frames' bounds.
     * False when a slave frame has no native column memory: the span scan joins the master frame.
     */
    public boolean of(TimeFrameCursor cursor, PageFrameMemoryRecord slaveRecord, int slaveTimestampIndex, int slaveSymbolIndex, long slaveTsScale, int joinableCount) {
        if (this.cursor == cursor && this.slaveRecord == slaveRecord) {
            // the slot's next frame of the same execution: the keys' cursors carry on when its
            // master timestamps are not lower than the last position's, see updatePosition()
            return true;
        }
        closeKeyCursors();
        this.cursor = cursor;
        this.slaveRecord = slaveRecord;
        this.slaveTimestampIndex = slaveTimestampIndex;
        this.slaveSymbolIndex = slaveSymbolIndex;
        this.slaveTsScale = slaveTsScale;
        if (joinableCount > keyFrames.length) {
            keyFrames = Arrays.copyOf(keyFrames, joinableCount);
            keyNextRows = Arrays.copyOf(keyNextRows, joinableCount);
            keyMatches = Arrays.copyOf(keyMatches, joinableCount);
            keyBases = Arrays.copyOf(keyBases, joinableCount);
        }
        if (joinableCount > keyCursors.size()) {
            keyCursors.extendAndSet(joinableCount - 1, null);
        }
        frameTsLos.clear();
        frameRowLos.clear();
        frameRowHis.clear();
        final TimeFrame timeFrame = cursor.getTimeFrame();
        cursor.toTop();
        while (cursor.next()) {
            final long rowCount = cursor.open();
            // an empty frame keeps the frames' first timestamps ascending and is never searched
            final long previousTsLo = frameTsLos.size() > 0 ? frameTsLos.getLast() : Long.MIN_VALUE;
            frameTsLos.add(rowCount > 0 ? scaleTimestamp(timeFrame.getTimestampLo(), slaveTsScale) : previousTsLo);
            frameRowLos.add(timeFrame.getRowLo());
            frameRowHis.add(rowCount > 0 ? timeFrame.getRowHi() : timeFrame.getRowLo());
        }
        positionFrame = -1;
        positionTimestamp = Long.MIN_VALUE;
        tsAddressFrame = -1;
        return true;
    }

    // Closes the frame's key cursors.
    void closeKeyCursors() {
        for (int i = 0, n = keyCursors.size(); i < n; i++) {
            final RowCursor keyCursor = keyCursors.getQuick(i);
            if (keyCursor != null) {
                keyCursor.close();
                keyCursors.setQuick(i, null);
            }
        }
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

    // The key's latest row in the frames from frameHi down to frameLo, at or below rowHi in frameHi;
    // Long.MIN_VALUE when there is none, UNSUPPORTED without an index reader.
    private long findLatestBackward(int indexKey, int frameHi, long rowHi, int frameLo, SqlExecutionCircuitBreaker circuitBreaker) {
        long rowMax = rowHi;
        for (int f = frameHi; f >= frameLo; f--) {
            final long rowLo = frameRowLos.getQuick(f);
            if (f < frameHi) {
                rowMax = frameRowHis.getQuick(f) - 1;
            }
            if (rowMax < rowLo) {
                continue;
            }
            cursor.jumpTo(f);
            cursor.open();
            final IndexReader indexReader = cursor.getIndexReaderForCurrentFrame(slaveSymbolIndex, IndexReader.DIR_BACKWARD);
            if (indexReader == null) {
                return UNSUPPORTED;
            }
            final long indexRowLo = cursor.getIndexRowLoForCurrentFrame();
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
        while (lo <= hi) {
            final long mid = (lo + hi) >>> 1;
            if (timestampAt(mid) <= timestamp) {
                row = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return row;
    }

    // A forward cursor over the key's index rows of the frame from fromLocalRow on, null without an
    // index reader.
    private RowCursor openKeyCursor(int slot, int indexKey, int frameIndex, long fromLocalRow) {
        keyBases[slot] = fromLocalRow;
        if (fromLocalRow >= frameRowHis.getQuick(frameIndex)) {
            return NO_ROWS;
        }
        cursor.jumpTo(frameIndex);
        cursor.open();
        final IndexReader indexReader = cursor.getIndexReaderForCurrentFrame(slaveSymbolIndex, IndexReader.DIR_FORWARD);
        if (indexReader == null) {
            return null;
        }
        final long indexRowLo = cursor.getIndexRowLoForCurrentFrame();
        // the cursor returns rows relative to its minValue
        return indexReader.getCursor(indexKey, indexRowLo + fromLocalRow, indexRowLo + frameRowHis.getQuick(frameIndex) - 1);
    }

    // Points tsAddress at the frame's timestamp column: false without native column memory.
    private boolean timestampsOf(int frameIndex) {
        if (frameRowHis.getQuick(frameIndex) == frameRowLos.getQuick(frameIndex)) {
            // empty: no row to read
            return true;
        }
        if (tsAddressFrame != frameIndex) {
            cursor.recordAt(slaveRecord, Rows.toRowID(frameIndex, frameRowLos.getQuick(frameIndex)));
            tsAddress = slaveRecord.getPageAddress(slaveTimestampIndex);
            tsAddressFrame = frameIndex;
        }
        return tsAddress != 0;
    }

    private long timestampAt(long localRow) {
        return scaleTimestamp(Unsafe.getLong(tsAddress + (localRow << 3)), slaveTsScale);
    }

    // Moves the ASOF position, the last slave row at or below the master timestamp, to it: forward
    // from the last position by doubling steps, or by binary searches in a new frame. False when a
    // frame has no native column memory.
    private boolean updatePosition(long masterTimestamp) {
        if (positionFrame != -1 && masterTimestamp >= positionTimestamp) {
            int frame = positionFrame;
            while (frame + 1 < frameTsLos.size() && frameTsLos.getQuick(frame + 1) <= masterTimestamp) {
                frame++;
            }
            if (!timestampsOf(frame)) {
                return false;
            }
            if (frame != positionFrame) {
                positionFrame = frame;
                positionRow = findRow(frame, masterTimestamp);
            } else {
                final long rowHi = frameRowHis.getQuick(frame) - 1;
                long lo = positionRow;
                // most gaps are short: the next rows first, one after another
                final long linearHi = Math.min(lo + LINEAR_PROBE_ROWS, rowHi);
                while (lo < linearHi && timestampAt(lo + 1) <= masterTimestamp) {
                    lo++;
                }
                if (lo < linearHi) {
                    positionRow = lo;
                    positionTimestamp = masterTimestamp;
                    return true;
                }
                long step = 1;
                while (lo + step <= rowHi && timestampAt(lo + step) <= masterTimestamp) {
                    lo += step;
                    step <<= 1;
                }
                long hi = Math.min(lo + step, rowHi + 1);
                // the last row at or below the timestamp lies in [lo, hi)
                while (hi - lo > 1) {
                    final long mid = (lo + hi) >>> 1;
                    if (timestampAt(mid) <= masterTimestamp) {
                        lo = mid;
                    } else {
                        hi = mid;
                    }
                }
                positionRow = lo;
            }
        } else {
            positionFrame = findFrame(masterTimestamp);
            if (positionFrame != -1) {
                if (!timestampsOf(positionFrame)) {
                    return false;
                }
                positionRow = findRow(positionFrame, masterTimestamp);
            }
            // a lower master timestamp: the keys' cursors start over
            closeKeyCursors();
        }
        positionTimestamp = masterTimestamp;
        return true;
    }
}
