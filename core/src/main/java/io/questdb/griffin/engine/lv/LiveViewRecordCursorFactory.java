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

package io.questdb.griffin.engine.lv;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

/**
 * Live-view read path. Wraps the standard
 * {@code PageFrameRecordCursorFactory} that {@code SqlCodeGenerator} builds
 * for the LV's WAL-backed table, and pins the LV's in-memory tier slot for
 * the cursor's lifetime so the refresh worker's slow-path
 * {@code tryAcquireWrite} sees the reader and trails rather than progressing
 * past it.
 * <p>
 * The returned cursor wires Mode B seam_ts routing: when the consistency fence
 * holds it serves disk rows with {@code ts < seamTs} and the pinned in-mem slot
 * for {@code ts >= seamTs}, skipping the hot tail partition(s) of the LV table.
 * The fence ({@code slot.lvSeqTxn == diskReader.seqTxn}) plus a full-schema,
 * ascending, unfiltered-scan requirement keep this safe; anything else falls
 * back to disk-only. See {@link LiveViewRecordCursor} for the routing details.
 * <p>
 * Each {@link #getCursor(SqlExecutionContext)} call allocates a fresh
 * {@link LiveViewRecordCursor}: the cursor pins a tier slot until
 * {@code close()}, so reusing a single cursor across consecutive
 * {@code getCursor} calls would release the previous reader's pin if both
 * cursors are still live (e.g. a plan-explain probe over the same factory).
 * Allocation here is once per query, not on the row hot path, so the cost
 * is negligible.
 */
public class LiveViewRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final CairoEngine engine;
    private final TableToken liveViewToken;
    private final int timestampColumnIndex;

    public LiveViewRecordCursorFactory(CairoEngine engine, TableToken liveViewToken, RecordCursorFactory base) {
        super(base.getMetadata());
        this.engine = engine;
        this.liveViewToken = liveViewToken;
        this.base = base;
        this.timestampColumnIndex = base.getMetadata().getTimestampIndex();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor diskCursor = base.getCursor(executionContext);
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(liveViewToken.getTableName());
        LiveViewRecordCursor cursor = new LiveViewRecordCursor();
        try {
            // Mode B seam routing assumes the disk scan yields rows in ascending
            // timestamp order. The LV table has a designated timestamp, so a
            // forward scan is ascending; backward / index scans are not, and the
            // cursor must fall back to disk-only for them.
            boolean diskScanAscending = base.getScanDirection() == SCAN_DIRECTION_FORWARD;
            cursor.of(diskCursor, base.getMetadata(), instance, timestampColumnIndex, diskScanAscending);
        } catch (Throwable t) {
            Misc.free(cursor);
            throw t;
        }
        return cursor;
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        // Every in-mem row is also durable on disk after the
        // inline apply, so ASOF JOIN-as-RHS reading via the base factory's
        // time-frame cursor sees the full dataset. Future phases (when in-mem
        // outpaces disk via the hand-off ring) will need a dedicated
        // time-frame cursor that bridges both tiers.
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LiveView");
        sink.optAttr("view", liveViewToken.getTableName());
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
        Misc.free(base);
    }
}
