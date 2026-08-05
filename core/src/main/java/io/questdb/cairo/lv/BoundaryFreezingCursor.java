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

package io.questdb.cairo.lv;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps a repair replay's source cursor and freezes every logical checkpoint
 * boundary the replay crosses, one row before the window functions see that row.
 * <p>
 * A checkpoint root at boundary {@code B} must describe the window state after
 * every qualifying row with designated timestamp {@code <= B} has been folded,
 * and no row above it. The replay reads rows in ascending timestamp order, so
 * that moment is the instant before the first row with {@code ts > B} reaches the
 * window functions - which is exactly where this cursor sits.
 * <p>
 * Freezing from the replay's own row loop instead cannot reach that instant:
 * {@code WindowRecordCursorFactory.WindowRecordCursor.hasNext()} folds the row
 * into every function before it returns, so a boundary frozen from the loop body
 * carries the crossing row as well. For a bounded RANGE frame that row is
 * physically in the frozen ring, and a later resume that restores the root and
 * replays from {@code B + 1} folds it a second time - a partition's sum then
 * counts it twice. The same wrapping order the anchor dispatch relies on is what
 * makes the boundary land between two rows rather than inside one; see
 * {@link AnchorDispatchingCursor}, which must stay ABOVE this cursor so a row's
 * anchor reset also lands after the boundary it crosses.
 * <p>
 * The cursor carries the freeze cursor ({@link #getCaptured()}) across the turns
 * a yielding repair takes, so a resumed turn continues where the previous one
 * stopped.
 */
final class BoundaryFreezingCursor implements RecordCursor {
    private LiveViewWindow anchorWindow;
    private RecordCursor base;
    private ObjList<LiveViewCheckpointTimelineEntry> boundaries;
    private LiveViewCheckpointTimelineStoreWriter.RepairCapture capture;
    private int captured;
    private ObjList<WindowFunction> functions;
    // Per-boundary row positions, one per entry in boundaries; null when the
    // caller derives the position from its own running row count instead.
    private LongList positions;
    private long rowPosition;
    private LiveViewCheckpointRepairSession session;
    private int timestampIndex;

    @Override
    public void close() {
        // The window cursor frees its base on every incremental close, so this
        // runs per turn. It drops the delegate only: getCaptured() is read after
        // the cursor chain unwinds, and a yielding repair resumes on the same
        // freeze cursor.
        base = null;
    }

    /**
     * Releases everything the replay handed over. The freeze cursor survives so a
     * caller that clears mid-repair can still report its progress.
     */
    public void clear() {
        base = null;
        capture = null;
        boundaries = null;
        functions = null;
        anchorWindow = null;
        session = null;
        positions = null;
    }

    /**
     * Freezes every boundary left, whatever its timestamp. The replay calls this
     * once its scan is exhausted: no qualifying row sits between the last row read
     * and any remaining boundary, so the state the replay ends on is theirs.
     */
    public void freezeRemaining() {
        while (captured < boundaries.size()) {
            freezeOne();
        }
    }

    /**
     * @return how many boundaries this cursor has frozen
     */
    public int getCaptured() {
        return captured;
    }

    @Override
    public Record getRecord() {
        return base.getRecord();
    }

    @Override
    public Record getRecordB() {
        return base.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!base.hasNext()) {
            return false;
        }
        // Strictly below: a boundary AT this row's timestamp waits for the next
        // row, which is what admits the boundary's complete timestamp tie.
        final long timestamp = base.getRecord().getTimestamp(timestampIndex);
        while (captured < boundaries.size()
                && boundaries.getQuick(captured).maxTimestamp < timestamp) {
            freezeOne();
        }
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(columnIndex);
    }

    /**
     * @param base           the replay's source cursor, filter included
     * @param capture        the repair capture the frozen roots land in
     * @param boundaries     the logical boundaries to re-version, ascending
     * @param positions      per-boundary live-view row positions, or null when the
     *                       caller stamps a running position through
     *                       {@link #setRowPosition(long)}
     * @param functions      the live compiled window functions
     * @param anchorWindow   the live anchor window, or null
     * @param session        the repair session recording freeze progress, or null
     * @param captured       boundaries a previous turn of this repair already froze
     * @param timestampIndex designated timestamp index in the BASE cursor's
     *                       metadata - the column the scan is ordered and bounded on
     */
    public void of(
            RecordCursor base,
            LiveViewCheckpointTimelineStoreWriter.RepairCapture capture,
            ObjList<LiveViewCheckpointTimelineEntry> boundaries,
            @Nullable LongList positions,
            ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            @Nullable LiveViewCheckpointRepairSession session,
            int captured,
            int timestampIndex
    ) {
        this.base = base;
        this.capture = capture;
        this.boundaries = boundaries;
        this.positions = positions;
        this.functions = functions;
        this.anchorWindow = anchorWindow;
        this.session = session;
        this.captured = captured;
        this.timestampIndex = timestampIndex;
        this.rowPosition = 0;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        // Random access skips the freeze on purpose, exactly as the anchor
        // dispatch does: recordAt positions on an arbitrary row out of order, and
        // a boundary only means anything against forward iteration.
        base.recordAt(record, atRowId);
    }

    /**
     * Stamps the live-view row position the next freeze records. The replay keeps
     * it in step with the rows it has appended, so a boundary frozen before row
     * {@code k} carries the position of row {@code k - 1}.
     */
    public void setRowPosition(long rowPosition) {
        this.rowPosition = rowPosition;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        // The freeze cursor deliberately survives: the wrapping factory rewinds
        // the chain when it opens the incremental cursor, before any row is read.
        base.toTop();
    }

    private void freezeOne() {
        capture.capture(
                boundaries.getQuick(captured),
                functions,
                anchorWindow,
                positions != null ? positions.getQuick(captured) : rowPosition
        );
        captured++;
        if (session != null) {
            session.recordProgress(captured);
        }
    }
}
