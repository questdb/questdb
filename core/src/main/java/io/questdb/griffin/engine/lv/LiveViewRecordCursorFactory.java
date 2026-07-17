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
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

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
 * The fence ({@code slot.lvSeqTxn == diskReader.seqTxn}) plus a
 * tier-addressable-projection, ascending, unfiltered-scan requirement keep this
 * safe; anything else falls back to disk-only. See {@link LiveViewRecordCursor} for the routing details.
 * {@link #toPlan} surfaces the static, query-shape part of this decision as the
 * {@code inMemory} EXPLAIN attribute (see {@link #isInMemRoutable}).
 * <p>
 * The read gets the tier and the engine's page-frame machinery, not one or the
 * other. {@link #supportsPageFrameCursor} follows the base, so a filtered live-view
 * read runs the parallel filter, the JIT filter and LIMIT pushdown the way a plain
 * table read does - and {@link #getPageFrameCursor} still routes, handing back a
 * {@link LiveViewPageFrameCursor} whose synthetic frame over the pinned slot the
 * filter runs over exactly as it runs over a native partition. Routing evaluates the
 * same fence on both paths (see {@link LiveViewRouting}); a read that fails it gets
 * the base's frames unchanged.
 * <p>
 * Each {@link #getCursor(SqlExecutionContext)} / {@link #getPageFrameCursor} call
 * allocates a fresh cursor: it pins a tier slot until {@code close()}, so reusing a
 * single cursor across consecutive calls would release the previous reader's pin if
 * both cursors are still live (e.g. a plan-explain probe over the same factory).
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
    // before the tier slot is pinned - on either read path. Lets a test
    // deterministically inject a flush into that exact window - the disk-open /
    // slot-pin race that leaves the disk snapshot older than the republished slot -
    // to exercise the staleness retry. Production never sets it; the null check is a
    // single per-query read.
    @TestOnly
    private static volatile Runnable onDiskCursorOpenedHook;
    // Test-only, single-shot hook run right after a read pins its tier slot and before
    // anything adopts that pin - on either read path. Lets a test drive the error paths
    // that have to hand the slot back by themselves: bindFrameCursor's catch on the frame
    // path, and openBoundCursor's catch (an of() that throws with the slot already pinned)
    // on the record path. A pin those miss is not a stale read but a slot the refresh
    // worker can never reclaim again. Production never sets it; the null check is a single
    // per-query read.
    @TestOnly
    private static volatile Runnable onSlotPinnedHook;
    private final RecordCursorFactory base;
    private final CairoEngine engine;
    // Static, query-shape eligibility for lead routing, surfaced as the EXPLAIN
    // "inMemory" attribute. True when the read's shape leaves the in-mem tier some way to
    // lead disk - serving the un-flushed lead (rows not yet on the LV's on-disk tier), and
    // under the seam split the recent overlap band too, from RAM (see isInMemRoutable).
    // The runtime seqTxn fence, the tier's population state, a timestamp-interval filter
    // (none of them visible to a static plan) and the read path taken still make the final
    // per-cursor call, so this is a capability indicator, not a guarantee. See
    // LiveViewRecordCursor.
    private final boolean inMemRoutable;
    private final TableToken liveViewToken;
    private final int timestampColumnIndex;

    public LiveViewRecordCursorFactory(CairoEngine engine, TableToken liveViewToken, RecordCursorFactory base) {
        super(base.getMetadata());
        this.engine = engine;
        this.liveViewToken = liveViewToken;
        this.base = base;
        this.timestampColumnIndex = base.getMetadata().getTimestampIndex();
        this.inMemRoutable = isInMemRoutable(base);
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        base.changePageFrameSizes(minRows, maxRows);
    }

    @Override
    public boolean followedOrderByAdvice() {
        // The cursor yields rows in the base scan's order - disk-only passes the
        // base order straight through, and tier routing only engages for a forward
        // (ascending) base - so whatever advice the base scan followed, this
        // wrapper still honours. The default (false) claims the wrapper ignored
        // the advice, which costs a redundant sort on a parent model that reads
        // the flag: generateOrderBy's own scan-direction check only rescues the
        // single-column designated-timestamp case, so an LV read ordered by
        // anything else the base already satisfied sorted for nothing.
        return base.followedOrderByAdvice();
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
     * Test-only: arms a single-shot hook that fires inside {@link #getCursor} or
     * {@link #getPageFrameCursor} - whichever opens next - right after the disk cursor is
     * opened and before the tier slot is pinned. Used to deterministically reproduce the
     * disk-open / slot-pin flush race the staleness retry guards against. Production never
     * calls this.
     */
    @TestOnly
    public static void setOnDiskCursorOpenedHook(Runnable hook) {
        onDiskCursorOpenedHook = hook;
    }

    /**
     * Test-only: arms a single-shot hook that fires on whichever read path opens next, right
     * after it pins the tier slot and before anything adopts that pin. A hook that throws
     * reproduces a failure with the slot pinned, which is the only way to exercise the
     * releases that guard it. Production never calls this.
     */
    @TestOnly
    public static void setOnSlotPinnedHook(Runnable hook) {
        onSlotPinnedHook = hook;
    }

    /**
     * One attempt at the frame path: pins a tier slot and evaluates the routing fence over
     * it and the (already opened) disk scan. Returns a {@link LiveViewPageFrameCursor} when
     * the read routes, {@code diskCursor} itself when it does not, or {@code null} to ask
     * {@link #getPageFrameCursor} for another attempt against a fresh disk snapshot - having
     * already released the pin and closed {@code diskCursor}. Takes ownership of
     * {@code diskCursor}: every exit either hands it back or frees it.
     * <p>
     * Unlike {@link LiveViewRecordCursor#of}, every non-routing outcome releases the pin
     * before returning, including a version-fence miss. The record path holds that one so
     * {@link #getCursor}'s {@code isSlotNewerThanDisk()} retry can still read the slot; here
     * the retry decision is made in this method, against the slot it still holds, so once it
     * returns nothing downstream needs the pin. Sustained concurrent readers straddling a
     * tier swap can otherwise pin both slots, which fails the refresh worker's
     * publishToInMemoryTier and emergency-flushes the lead every cycle.
     */
    private PageFrameCursor bindFrameCursor(
            PageFrameCursor diskCursor,
            LiveViewInstance instance,
            boolean isLastAttempt
    ) {
        // Held here only until of() below adopts them; the catch releases whatever this
        // method still owns at the point of failure.
        LiveViewInMemoryTier tier = null;
        int slotIdx = -1;
        LiveViewPageFrameCursor cursor = null;
        try {
            runDiskCursorOpenedHook();
            // A non-table frame source carries no LV-table seqTxn to fence against, so it
            // can never route. Checked before the pin so the tier is left alone entirely.
            if (!(diskCursor instanceof TablePageFrameCursor tableDiskCursor)) {
                return diskCursor;
            }
            final LiveViewInMemoryTier candidate = instance.getInMemoryTier();
            if (candidate == null) {
                return diskCursor;
            }
            // A return of -1 means the tier was concurrently closed (LV dropped); we then
            // hold neither the global pin lease nor a per-slot rc and must not touch it again.
            final int pin = candidate.acquireRead();
            if (pin < 0) {
                return diskCursor;
            }
            tier = candidate;
            slotIdx = pin;
            runSlotPinnedHook();
            final LiveViewInMemoryBuffer slot = tier.getSlot(pin);
            final long diskSeqTxn = LiveViewRouting.diskReaderSeqTxn(tableDiskCursor);
            final IntList tierColumns = new IntList();
            if (diskSeqTxn != Numbers.LONG_NULL
                    && LiveViewRouting.buildTierColumnMapping(tableDiskCursor, base.getMetadata(), slot, tierColumns)) {
                if (!isLastAttempt && LiveViewRouting.isSlotNewerThanDisk(slot, diskSeqTxn)) {
                    tier.releaseRead(pin);
                    tier = null;
                    slotIdx = -1;
                    Misc.free(diskCursor);
                    return null;
                }
                if (LiveViewRouting.isFenced(slot, diskSeqTxn)) {
                    // Read the cache before the cursor exists: once `cursor` is assigned the
                    // catch below frees through it, and it can only free what of() adopted.
                    final LiveViewSymbolCache symbolCache = tier.getSymbolCache();
                    cursor = new LiveViewPageFrameCursor();
                    // of() adopts diskCursor and the pin before anything that can throw, so
                    // from here close() is what releases them - hence the catch's fork.
                    cursor.of(tableDiskCursor, tier, pin, slot, symbolCache, tierColumns);
                    return cursor;
                }
            }
            tier.releaseRead(pin);
            tier = null;
            slotIdx = -1;
            return diskCursor;
        } catch (Throwable th) {
            if (cursor != null) {
                Misc.free(cursor);
            } else {
                if (tier != null && slotIdx >= 0) {
                    tier.releaseRead(slotIdx);
                }
                Misc.free(diskCursor);
            }
            throw th;
        }
    }

    private LiveViewRecordCursor openBoundCursor(
            SqlExecutionContext executionContext,
            LiveViewInstance instance,
            boolean diskScanAscending
    ) throws SqlException {
        RecordCursor diskCursor = base.getCursor(executionContext);
        final LiveViewRecordCursor cursor;
        try {
            runDiskCursorOpenedHook();
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

    /**
     * The page-frame twin of {@link #getCursor}: the base scan's frames cut at the seam,
     * followed by a synthetic frame over the pinned in-mem slot, so a frame consumer sees
     * the un-flushed lead too. Falls back to the base scan's frames unchanged whenever
     * routing cannot engage - the shape rules it out, the tier is absent or empty, or the
     * seqTxn fence misses - which serves the applied prefix, correct and at worst one flush
     * cycle stale. See {@link LiveViewPageFrameCursor}.
     * <p>
     * Routing needs an ascending frame stream: the seam cut takes the disk band by ROW
     * COUNT ({@code base.size() - leadStart}) and then serves the slot, which is ascending
     * by construction. Over a descending stream that cut would take the newest rows instead
     * of the oldest and serve the slot on top of them, duplicating some rows and dropping
     * others.
     * <p>
     * The {@code order} ARGUMENT decides that, and it is the only thing that does - which
     * is worth stating because the base factory's own {@code getScanDirection()} looks like
     * it should have a say and does not. The base builds a forward page-frame cursor for
     * {@code ORDER_ASC} / {@code ORDER_ANY} and a backward one otherwise, whatever its
     * natural scan direction, so this consumer's request fully determines which way the
     * frames arrive. Checking the base's direction on top would only refuse reads whose
     * frames ascend anyway.
     * <p>
     * Unlike the record path, a backward read gets no lead-only fallback here; it serves
     * the applied prefix. Lead-only over frames means narrowing the slot frame to
     * {@code [leadStart, rowCount)}, which needs the per-sub-frame aux rebasing the single
     * whole-slot frame avoids by starting at 0 (see {@link LiveViewPageFrameCursor}).
     */
    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (!inMemRoutable || (order != ORDER_ASC && order != ORDER_ANY)) {
            return base.getPageFrameCursor(executionContext, order);
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(liveViewToken.getTableName());
        if (instance == null) {
            return base.getPageFrameCursor(executionContext, order);
        }
        // Staleness retry against the disk-open / slot-pin race; see getCursor for why a
        // slot newer than the disk snapshot must not be served as-is.
        for (int attempt = 0; ; attempt++) {
            final PageFrameCursor diskCursor = base.getPageFrameCursor(executionContext, order);
            if (diskCursor == null) {
                // The base does not frame this read at all, so there is nothing to route
                // over. Checked here rather than in bindFrameCursor, whose null means the
                // opposite - see below.
                return null;
            }
            final PageFrameCursor cursor = bindFrameCursor(diskCursor, instance, attempt >= MAX_STALE_DISK_RETRIES);
            if (cursor != null) {
                return cursor;
            }
            // null asks for a retry: the pinned slot was newer than the disk snapshot, and
            // bindFrameCursor has already released both. The next attempt re-opens the disk
            // side against a fresh snapshot, which observes at least the slot's flush.
        }
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

    /**
     * Serves disk only, for the same reason - and with the same consequence - as
     * {@link #getTimeFrameCursor}: the read sees the applied prefix and trails the in-mem
     * lead by at most one flush cycle.
     * <p>
     * Delegating is not optional. {@link #supportsTimeFrameCursor()} governs whether a
     * caller may call this at all, and this factory answers that with the base's verdict,
     * which holds for a plain ascending scan - very nearly the same shape
     * {@link #isInMemRoutable} accepts. The inherited default returns null, so a parallel
     * WINDOW / HORIZON JOIN over a live view NPE'd in its atom's constructor rather than
     * running.
     */
    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        return base.newTimeFrameCursor();
    }

    @Override
    public boolean producesMaterializedPageFrames() {
        // The frames come from the base scan (see getPageFrameCursor), so the base
        // is the authority. The inherited default routes through getBaseFactory(),
        // which this wrapper does not expose, and so would answer a blanket "true"
        // for a base that only produces metadata-only frames.
        return base.producesMaterializedPageFrames();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    /**
     * Follows the base scan, so a live-view read gets the engine's page-frame machinery
     * whenever a plain read of the same table would: the parallel filter, the JIT-compiled
     * filter, and LIMIT pushdown into that filter. Reporting the {@code false} default
     * instead sent every filtered live-view read down a single-threaded, interpreted
     * {@code Filter} with no limit pushdown - several times slower than the same read
     * through the base table.
     * <p>
     * This used to exclude a routable read ({@code !inMemRoutable && ...}), because frames
     * came from the base scan alone and would have dropped the un-flushed lead: a read could
     * be fresh or fast, never both, and a filtered scan over recent data - the read that
     * most wants freshness - was exactly the one the fork sent to disk. It no longer has to
     * choose. {@link #getPageFrameCursor} routes in its own right, and the filter runs over
     * the tier's frame like any other.
     */
    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
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
     * Static, refresh-timing-independent eligibility for lead routing: whether the read's
     * shape leaves the in-mem tier ANY way to lead disk (serve the un-flushed lead, and
     * possibly the overlap, from RAM). True when every projected column is a type the tier
     * can store (fixed-width, SYMBOL, STRING, BINARY, VARCHAR, ARRAY). An unsupported type
     * - a non-persisted one such as INTERVAL - means the LV has no tier at all, so the read
     * can only ever come from disk. SYMBOL columns are fine: the refresh worker stores
     * LV-table-space ids the disk reader resolves on read.
     * <p>
     * That is the whole of it, and the two preconditions that USED to be here are worth
     * naming because their absence is the point. Neither a backward scan nor a
     * timestamp-pruned projection disqualifies a read any more: both are seam-split
     * requirements, and a read that cannot seam now falls back to lead-only rather than to
     * disk-only (see {@link LiveViewRecordCursor}). Keeping them here would have made a
     * {@code false} result unreliable, which is the one property this flag has.
     * <p>
     * A {@code true} result is a capability flag, not a guarantee, and the gap is wider
     * than a static plan can show. It cannot see the runtime seqTxn fence, the tier's
     * population state, or a timestamp-interval filter pushed into the scan, any of which
     * still route an individual cursor disk-only. Nor does it distinguish the two read
     * paths: {@link #getPageFrameCursor} routes only an ascending scan, so a backward
     * FRAME read (e.g. a filtered {@code ORDER BY ts DESC}, which the parallel filter
     * takes) reports {@code true} and still serves disk alone, while the same read without
     * the filter takes the record path and routes lead-only.
     * <p>
     * A {@code false} result, by contrast, stays reliable: the read is always disk-only,
     * since an unsupported column type is a hard disqualifier on both paths.
     */
    private static boolean isInMemRoutable(RecordCursorFactory base) {
        final RecordMetadata metadata = base.getMetadata();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (!LiveViewInMemoryBuffer.isColumnTypeSupported(metadata.getColumnType(i))) {
                return false;
            }
        }
        return true;
    }

    // Single-shot: clears the hook before running it, so the staleness retry's second
    // disk open does not re-fire it. Production never arms one; the null check is a
    // single per-query read.
    private static void runDiskCursorOpenedHook() {
        final Runnable hook = onDiskCursorOpenedHook;
        if (hook != null) {
            onDiskCursorOpenedHook = null;
            hook.run();
        }
    }

    // Single-shot, for the same reason as runDiskCursorOpenedHook: the staleness retry
    // pins a slot again on its second attempt and must not re-fire the hook. Package-private
    // because the record path takes its pin inside LiveViewRecordCursor.of, so that is where
    // it fires from.
    static void runSlotPinnedHook() {
        final Runnable hook = onSlotPinnedHook;
        if (hook != null) {
            onSlotPinnedHook = null;
            hook.run();
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
    }
}
