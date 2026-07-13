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
import org.jetbrains.annotations.TestOnly;

/**
 * Live-view read path. Wraps the standard
 * {@code PageFrameRecordCursorFactory} that {@code SqlCodeGenerator} builds
 * for the LV's WAL-backed table, and pins the LV's in-memory tier slot for
 * the cursor's lifetime so the refresh worker's slow-path
 * {@code tryAcquireWrite} sees the reader and trails rather than progressing
 * past it.
 * <p>
 * The returned cursor wires seam_ts routing: when the consistency fence
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
    // Bound on the disk-open / slot-pin staleness retry (see getCursor). One
    // retry suffices in practice - a re-opened reader observes the already-applied
    // flush - so this is a wide safety margin against a pathological flush storm,
    // not an expected iteration count.
    private static final int MAX_STALE_DISK_RETRIES = 8;
    // Test-only, single-shot hook run right after the disk cursor is opened but
    // before the tier slot is pinned. Lets a test deterministically inject a flush
    // into that exact window - the disk-open / slot-pin race that leaves the disk
    // snapshot older than the republished slot - to exercise the staleness retry in
    // getCursor. Production never sets it; the null check is a single per-query read.
    @TestOnly
    private static volatile Runnable onDiskCursorOpenedHook;
    private final RecordCursorFactory base;
    private final CairoEngine engine;
    // Static, query-shape eligibility for lead routing, surfaced as the
    // EXPLAIN "inMemory" attribute. True when the read's shape permits the
    // in-mem tier to lead disk - serving the recent overlap band plus the
    // un-flushed lead (rows not yet on the LV's on-disk tier) from RAM (see
    // isInMemRoutable). The runtime seqTxn fence, the tier's population state,
    // and a timestamp-interval filter (not visible to a static plan) still make
    // the final per-cursor call, so this is a capability indicator, not a
    // guarantee. See LiveViewRecordCursor.
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
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(liveViewToken.getTableName());
        // seam routing assumes the disk scan yields rows in ascending timestamp
        // order. The LV table has a designated timestamp, so a forward scan is
        // ascending; backward / index scans are not, and the cursor must fall back
        // to disk-only for them.
        final boolean diskScanAscending = base.getScanDirection() == SCAN_DIRECTION_FORWARD;
        // Staleness retry against the disk-open / slot-pin race. The disk cursor is
        // opened before the slot is pinned; a flush landing in that window advances
        // the on-disk tier and republishes the slot with the newer seqTxn, leaving
        // the disk snapshot OLDER than the pinned slot. The fence then disengages
        // (slot seqTxn != disk seqTxn) and serves disk-only against the stale, smaller
        // snapshot, so a live view appears to shrink relative to an earlier read that
        // already reflected the flush. Re-open against a fresh disk snapshot when the
        // slot is newer than the disk; the slot's flush is already applied, so a fresh
        // reader observes at least the slot's seqTxn. Bounded so a pathological flush
        // storm still returns (at worst disk-only, one flush stale) instead of spinning.
        for (int attempt = 0; ; attempt++) {
            LiveViewRecordCursor cursor = openBoundCursor(executionContext, instance, diskScanAscending);
            if (attempt >= MAX_STALE_DISK_RETRIES || !cursor.isSlotNewerThanDisk()) {
                return cursor;
            }
            // Fully closes this attempt: releases the slot pin and closes the disk
            // cursor. The next attempt re-opens both against a fresh snapshot.
            Misc.free(cursor);
        }
    }

    /**
     * Test-only: arms a single-shot hook that fires inside {@link #getCursor}
     * right after the disk cursor is opened and before the tier slot is pinned.
     * Used to deterministically reproduce the disk-open / slot-pin flush race the
     * staleness retry guards against. Production never calls this.
     */
    @TestOnly
    public static void setOnDiskCursorOpenedHook(Runnable hook) {
        onDiskCursorOpenedHook = hook;
    }

    private LiveViewRecordCursor openBoundCursor(
            SqlExecutionContext executionContext,
            LiveViewInstance instance,
            boolean diskScanAscending
    ) throws SqlException {
        RecordCursor diskCursor = base.getCursor(executionContext);
        final LiveViewRecordCursor cursor;
        try {
            final Runnable hook = onDiskCursorOpenedHook;
            if (hook != null) {
                // Single-shot: clear before running so the staleness retry's second
                // disk open does not re-fire it.
                onDiskCursorOpenedHook = null;
                hook.run();
            }
            cursor = new LiveViewRecordCursor();
        } catch (Throwable t) {
            // Nothing owns diskCursor yet (of() has not run), so free it here.
            Misc.free(diskCursor);
            throw t;
        }
        try {
            // of() takes ownership of diskCursor first thing, so once it is
            // called the cursor closes diskCursor via close() on any failure.
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
        // base order straight through, and tier routing only engages for a
        // forward (ascending) base, so it never reorders either. Delegating keeps
        // the optimizer's order reasoning correct - e.g. ORDER BY ts DESC over an
        // LV whose base is a backward scan no longer adds a redundant sort.
        return base.getScanDirection();
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        // The time-frame cursor serves disk only: ASOF JOIN-as-RHS and interval
        // intrinsics see the applied prefix and trail the in-mem lead by at most
        // one flush cycle. A synthetic in-mem frame that bridges the lead is a
        // deferred enhancement; the disk-only frame stays correct, just not as
        // fresh as a record-cursor read that serves the lead.
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
        // Surface whether the read's shape permits lead routing through the
        // in-mem tier - serving the un-flushed lead (and the overlap) from RAM.
        // A capability flag, not a guarantee - see the field doc and
        // isInMemRoutable.
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
     * Static, refresh-timing-independent eligibility for lead routing - the
     * read-shape preconditions {@link LiveViewRecordCursor} checks before the
     * runtime seqTxn fence lets the in-mem tier lead disk (serve the un-flushed
     * lead, plus the overlap, from RAM). True only when:
     * <ul>
     *   <li>the base scan is forward (ascending timestamp) - the seam split
     *   assumes ascending disk rows, so a backward / index scan routes
     *   disk-only;</li>
     *   <li>the projection keeps the timestamp ({@code timestampColumnIndex >= 0})
     *   - a timestamp-pruned read (e.g. an aggregate over the LV) cannot seam;</li>
     *   <li>every projected column is a type the tier can store (fixed-width,
     *   SYMBOL, STRING, BINARY, VARCHAR, ARRAY) - an unsupported type (a non-persisted
     *   type such as INTERVAL) means no tier, so it routes disk-only. SYMBOL columns are fine: the
     *   refresh worker stores LV-table-space ids the disk reader resolves on
     *   read.</li>
     * </ul>
     * A {@code true} result is a capability flag, not a guarantee: a static plan
     * cannot see the runtime seqTxn fence, the tier's population state, a
     * column-pruned (but timestamp-bearing) projection - the in-mem subset check
     * still rejects it - a reordered full-schema projection (the optimiser fuses
     * the reorder into the scan as a non-identity column mapping; the cursor's
     * identity-mapping check rejects it), or a timestamp-interval filter pushed
     * into the scan, all of which can still route an individual cursor disk-only.
     * A {@code false} result, by contrast, is reliable: the read is always
     * disk-only, since these preconditions are hard disqualifiers the cursor
     * enforces too.
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
