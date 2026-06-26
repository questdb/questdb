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
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
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
 * {@link #toPlan} surfaces the static, query-shape part of this decision as the
 * {@code inMemory} EXPLAIN attribute (see {@link #isInMemRoutable}).
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
    // Static, query-shape eligibility for Mode B seam routing, surfaced as the
    // EXPLAIN "inMemory" attribute. True when the read's shape permits the
    // in-mem tier to serve the recent band (see isInMemRoutable). The runtime
    // seqTxn fence, the tier's population state, and a timestamp-interval filter
    // (not visible to a static plan) still make the final per-cursor call, so
    // this is a capability indicator, not a guarantee. See LiveViewRecordCursor.
    private final boolean inMemRoutable;
    private final TableToken liveViewToken;
    private final int timestampColumnIndex;

    public LiveViewRecordCursorFactory(CairoEngine engine, TableToken liveViewToken, RecordCursorFactory base) {
        super(base.getMetadata());
        this.engine = engine;
        this.liveViewToken = liveViewToken;
        this.base = base;
        this.timestampColumnIndex = base.getMetadata().getTimestampIndex();
        this.inMemRoutable = isInMemRoutable(base, timestampColumnIndex);
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
    public int getScanDirection() {
        // The cursor yields rows in the base scan's order: disk-only passes the
        // base order straight through, and Mode B routing only engages for a
        // forward (ascending) base, so it never reorders either. Delegating keeps
        // the optimizer's order reasoning correct - e.g. ORDER BY ts DESC over an
        // LV whose base is a backward scan no longer adds a redundant sort.
        return base.getScanDirection();
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
        // Surface whether the read's shape permits Mode B seam routing through
        // the in-mem tier. A capability flag, not a guarantee - see the field
        // doc and isInMemRoutable.
        sink.attr("inMemory").val(inMemRoutable);
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

    /**
     * Static, refresh-timing-independent eligibility for Mode B seam routing -
     * the read-shape preconditions {@link LiveViewRecordCursor} checks before the
     * runtime seqTxn fence. True only when:
     * <ul>
     *   <li>the base scan is forward (ascending timestamp) - Mode B's seam split
     *   assumes ascending disk rows, so a backward / index scan routes
     *   disk-only;</li>
     *   <li>the projection keeps the timestamp ({@code timestampColumnIndex >= 0})
     *   - a timestamp-pruned read (e.g. an aggregate over the LV) cannot seam;</li>
     *   <li>every projected column is a fixed-width type the tier can store - a
     *   var-length column means no tier, so it routes disk-only. SYMBOL columns
     *   are fine: the refresh worker stores LV-table-space ids the disk reader
     *   resolves on read.</li>
     * </ul>
     * A {@code true} result is a capability flag, not a guarantee: a static plan
     * cannot see the runtime seqTxn fence, the tier's population state, a
     * column-pruned (but timestamp-bearing) projection - the in-mem subset check
     * still rejects it - or a timestamp-interval filter pushed into the scan, all
     * of which can still route an individual cursor disk-only. A {@code false}
     * result, by contrast, is reliable: the read is always disk-only, since these
     * three preconditions are hard disqualifiers the cursor enforces too.
     */
    private static boolean isInMemRoutable(RecordCursorFactory base, int timestampColumnIndex) {
        if (base.getScanDirection() != SCAN_DIRECTION_FORWARD || timestampColumnIndex < 0) {
            return false;
        }
        final RecordMetadata metadata = base.getMetadata();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (!LiveViewInMemoryBuffer.isColumnTypeSupported(metadata.getColumnType(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void _close() {
        Misc.free(base);
    }
}
