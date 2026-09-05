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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One localized out-of-order repair in flight, and everything it has to keep
 * alive when its replay yields on the turn budget and continues in a later
 * refresh turn.
 * <p>
 * A repair with a finite convergence boundary reads and re-emits a bounded base
 * interval, freezes a root version per logical checkpoint boundary it crosses,
 * and publishes all of it at once. A large but finite interval can still be more
 * work than one refresh turn should hold, so the replay stops on its turn budget,
 * parks here, and picks the scan back up where it left it. Nothing durable moves
 * in between: the live-view WAL replacement is still uncommitted in the writer
 * this session holds, and no timeline generation names the staged roots.
 *
 * <h2>What the session owns, and when</h2>
 * The overlay, the descriptor, the boundary schedule, the plan and - for a repair that is
 * one segment of a multi-segment loop - the loop position belong to the
 * session for the whole repair - the executing turn reaches them through it. The
 * three resources a turn actively uses - the pinned base reader, the live-view
 * {@link WalWriter} carrying the uncommitted replacement, and the staged
 * {@link LiveViewCheckpointTimelineStoreWriter.RepairCapture} - are held here
 * <b>only while suspended</b>: {@link #suspend} takes them in and the
 * {@code take*} methods hand them back to the resuming turn. A session that is
 * closed while it holds them releases them, which rolls the uncommitted rows
 * back and unlinks the staged segment.
 *
 * <h2>Same-process resume only</h2>
 * The pinned {@code E} the bounds were derived against cannot be reopened -
 * QuestDB exposes no as-of reader - so a suspended repair is resumable only
 * within the process that pinned it. A crash instead leaves the descriptor
 * behind, and startup reconciliation discards the candidate and lets a later
 * turn replan at a freshly pinned {@code E} (see
 * {@link LiveViewCheckpointRepairState}).
 * <p>
 * For the same reason the session names the refresh job that suspended it.
 * Another worker must not continue it: the capture freezes through its owner's
 * timeline store writer, and the WAL writer and pinned reader were taken out of
 * the pools on the owner's thread. Foreign workers skip the view; the owner
 * drives it to completion on its own turns.
 */
public final class LiveViewCheckpointRepairSession implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointRepairSession.class);
    private final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
    private final LiveViewCheckpointRepairState descriptor;
    // Whether the qualifying output this repair has emitted so far holds two rows with
    // the same (timestamp, projected key) pair. It travels with the loop position for the
    // reason the domain does: a duplicate whose two rows sit on either side of a park is
    // still a duplicate, and the worker's own detector is re-armed by the next repair it
    // classifies.
    private final LiveViewCheckpointOutputUniqueness outputUniqueness = new LiveViewCheckpointOutputUniqueness();
    private final LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay();
    private final LiveViewCheckpointSealCarryover sealCarryover = new LiveViewCheckpointSealCarryover();
    private final LiveViewRefreshJob owner;
    private final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
    // Where the multi-segment loop that started this repair had got to, for a repair that
    // is one segment of one. Empty for a repair that stands on its own.
    private final LiveViewCheckpointSegmentLoop segmentLoop = new LiveViewCheckpointSegmentLoop();
    // The compiled factory whose window functions the replay is standing part-way
    // through. Identity only - the session never calls it - so a later turn can refuse
    // a runtime that drifted out from under the candidate. See getWindowFactory().
    private final WindowRecordCursorFactory windowFactory;
    private LiveViewWindow anchorWindow;
    private long appendedRows;
    private TableReader baseReader;
    private LiveViewCheckpointTimelineStoreWriter.RepairCapture capture;
    private int capturedBoundaries;
    private long durableRowsBeforeRepair;
    private long durableRowsBelowFloor;
    private long durableRowsReplaced;
    private ObjList<WindowFunction> functions;
    // Whether this repair has a durable LiveViewCheckpointRepairMarker on disk that the
    // turn finishing it owes a clear. It lives here rather than in the executing turn
    // because it outlives one: a repair that parks on its budget leaves the marker
    // behind, and the turn that resumes it is a different call with its own locals.
    private boolean isRepairMarkerLive;
    private boolean isSuspended;
    // Set when close() abandoned the repair but could not put the overlay back, leaving the
    // compiled factory holding neither the pre-repair state nor a settled one. Read by
    // LiveViewRefreshJob.endRepairSession after the free, so it must outlive close().
    private boolean isWindowStateRestoreFailed;
    private long replayMaxTs = Numbers.LONG_NULL;
    private long replayMinTs = Numbers.LONG_NULL;
    private long resumeFromTs = Numbers.LONG_NULL;
    private long resumeSkipRows;
    private long scanRows;
    private int turns;
    private WalWriter walWriter;

    public LiveViewCheckpointRepairSession(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewRefreshJob owner,
            @NotNull WindowRecordCursorFactory windowFactory
    ) {
        this.owner = owner;
        this.windowFactory = windowFactory;
        this.descriptor = new LiveViewCheckpointRepairState(configuration);
    }

    /**
     * Ends the repair and releases everything the session still holds. The
     * resources a turn borrows are null unless the repair is parked, so an
     * ordinary completion frees only the overlay buffer and the descriptor's
     * mapping, while abandoning a parked repair also rolls the uncommitted
     * replacement back, unlinks the staged data segment and returns the pinned
     * base snapshot.
     * <p>
     * A repair nobody settled also gets its runtime put back: the replay left the
     * compiled factory part-way through {@code [L, H)}, and the state the
     * untouched durable output belongs to is the one the overlay holds.
     * <p>
     * The durable descriptor is discarded rather than left behind: the candidate
     * it describes is gone with the session, so an intact record of it would read
     * as a crashed repair to the next startup sweep.
     */
    @Override
    public void close() {
        isWindowStateRestoreFailed = false;
        if (overlay.isCaptured() && functions != null) {
            // Nobody took the state back, so this repair is being abandoned rather
            // than settled: the replay left the compiled factory part-way through
            // [L, H) while the durable output is still the pre-repair one, and the
            // overlay holds exactly the state that output belongs to. Put it back.
            // A settled repair has already restored and cleared the overlay, so this
            // is a no-op on the ordinary path.
            try {
                overlay.restore(functions, anchorWindow);
            } catch (Throwable t) {
                // The runtime now holds neither the pre-repair state nor a settled one. Flag it so
                // the caller forces a rebuild; without that the next drain continues over
                // half-restored accumulators. settleRepairRuntime() takes the same position on the
                // settled path, where it can set windowStateDirty and rethrow directly.
                isWindowStateRestoreFailed = true;
                LOG.critical().$("could not restore the window state of an abandoned live view repair [error=")
                        .$(t).I$();
            }
        }
        functions = null;
        anchorWindow = null;
        capture = Misc.free(capture);
        walWriter = Misc.free(walWriter);
        baseReader = Misc.free(baseReader);
        descriptor.discard();
        Misc.free(descriptor);
        Misc.free(overlay);
        // Abandoned rather than settled, so nothing published a generation the parked
        // baselines could name. Dropping them leaves every target on the complete freeze
        // the wipe left it owing, which is the safe direction.
        Misc.free(sealCarryover);
        boundaries.clear();
        segmentLoop.clear();
        outputUniqueness.clear();
        isRepairMarkerLive = false;
        isSuspended = false;
    }

    /**
     * Copies the compiled factory's window state - and the anchor map, when the
     * view has one - aside before the replay runs over it, and remembers what it
     * came out of so {@link #close()} can put it back if the repair is abandoned.
     * <p>
     * {@code memoryTracker} is the view's own, so the copy counts against
     * {@code cairo.live.view.refresh.memory.limit.bytes} like the state it duplicates.
     * A repair holds it for the whole repair, across every turn the replay takes, and
     * it is as large as the window state itself - the one allocation on this path big
     * enough for the operator's ceiling to be the right instrument.
     */
    public void captureRuntime(
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            @Nullable MemoryTracker memoryTracker
    ) {
        overlay.capture(functions, anchorWindow, memoryTracker);
        this.functions = functions;
        this.anchorWindow = anchorWindow;
    }

    /**
     * Retires the durable descriptor of a repair that is ending - published,
     * abandoned, or unwinding - because by then it describes no file this process
     * still owns.
     */
    public void discardDescriptor() {
        descriptor.discard();
    }

    /**
     * Drops the captured runtime state without putting it back, for a caller that has
     * established the compiled factory it came out of is gone - a base-metadata
     * recompile freed it, say. Restoring into a factory rebuilt since the capture would
     * write one set of functions' bytes into another's, so a candidate whose runtime
     * drifted is discarded by forgetting it rather than by unwinding it; the recovery
     * that replaced the factory owns rebuilding the state.
     */
    public void forgetRuntime() {
        functions = null;
        anchorWindow = null;
        overlay.clear();
        // The dirty sets name keys of partition maps a recompile has already freed, and
        // the baseline names a root the rebuilt factory never froze. Both go with the
        // state they describe.
        sealCarryover.clear();
    }

    /**
     * @return live-view rows the replay has emitted across every turn so far
     */
    public long getAppendedRows() {
        return appendedRows;
    }

    /**
     * @return the logical checkpoint boundaries in {@code [C, H)} the repair
     * re-versions, ascending. Also the schedule the replay segments itself on.
     */
    public ObjList<LiveViewCheckpointTimelineEntry> getBoundaries() {
        return boundaries;
    }

    /**
     * @return how many of the {@link #getBoundaries() boundaries} the replay has
     * already frozen a root version for
     */
    public int getCapturedBoundaries() {
        return capturedBoundaries;
    }

    /**
     * @return the durable descriptor of this repair, the only record that the
     * staged temporary segments have an owner
     */
    public LiveViewCheckpointRepairState getDescriptor() {
        return descriptor;
    }

    /**
     * @return live-view rows the table held before the repair started
     */
    public long getDurableRowsBeforeRepair() {
        return durableRowsBeforeRepair;
    }

    /**
     * @return live-view rows below the output floor {@code R}, the prefix every
     * repaired root's cumulative position is anchored on
     */
    public long getDurableRowsBelowFloor() {
        return durableRowsBelowFloor;
    }

    /**
     * @return live-view rows the replacement is going to delete from
     * {@code [R, H)}
     */
    public long getDurableRowsReplaced() {
        return durableRowsReplaced;
    }

    /**
     * @return the in-RAM copy of the window state the repair took aside before
     * replaying over it
     */
    /**
     * @return the {@code (timestamp, projected key)} uniqueness of the output emitted
     * across every turn of this repair so far, for the turn that resumes it to continue
     * checking against
     */
    public LiveViewCheckpointOutputUniqueness getOutputUniqueness() {
        return outputUniqueness;
    }

    public LiveViewCheckpointScratchOverlay getOverlay() {
        return overlay;
    }

    /**
     * @return the refresh job that started this repair and is the only one that
     * may continue it
     */
    public LiveViewRefreshJob getOwner() {
        return owner;
    }

    /**
     * @return the bounds every turn of this repair works from, pinned to the
     * snapshot {@code E} the first turn opened
     */
    public LiveViewCheckpointRepairPlan getPlan() {
        return plan;
    }

    /**
     * @return the highest output timestamp the replay has produced so far, or
     * {@link Numbers#LONG_NULL} when it has produced none
     */
    public long getReplayMaxTs() {
        return replayMaxTs;
    }

    /**
     * @return the lowest output timestamp the replay has produced so far, or
     * {@link Numbers#LONG_NULL} when it has produced none
     */
    public long getReplayMinTs() {
        return replayMinTs;
    }

    /**
     * @return the inclusive timestamp the next turn re-opens its base scan at: the
     * designated timestamp of the last row the prior turn folded into the window
     * state. Read with {@link #getResumeSkipRows()}, which says how many rows of
     * that timestamp the resuming turn must skip past.
     */
    public long getResumeFromTs() {
        return resumeFromTs;
    }

    /**
     * @return how many rows at {@link #getResumeFromTs()} the prior turns already
     * folded into the window state. The resuming turn skips exactly that many
     * qualifying rows before it starts reading, so no row is folded or emitted
     * twice.
     */
    public long getResumeSkipRows() {
        return resumeSkipRows;
    }

    /**
     * @return base rows every turn of this repair has pulled, including rows the
     * view's {@code WHERE} dropped
     */
    public long getScanRows() {
        return scanRows;
    }

    /**
     * @return where the multi-segment loop that started this repair had got to: the
     * segments it has not reached, the coordinates they are planned against, and the
     * residual it still owes once they are done. The loop owns one pinned base snapshot
     * across every segment it takes, so a replay that parks parks the rest of the loop with
     * it, and the turn that resumes the replay is the turn that finishes the loop.
     * {@link LiveViewCheckpointSegmentLoop#isOpen()} is false for a repair that is not part
     * of one
     */
    public LiveViewCheckpointSegmentLoop getSegmentLoop() {
        return segmentLoop;
    }

    /**
     * @return the incremental-seal bookkeeping this repair holds aside while its replay
     * runs through the compiled factory. Captured before the retire that resets the
     * batch-minimum window, and handed back in the repair's single runtime exchange
     */
    public LiveViewCheckpointSealCarryover getSealCarryover() {
        return sealCarryover;
    }

    /**
     * @return how many turns this repair has spent; 1 for a repair that never
     * yielded
     */
    public int getTurns() {
        return turns;
    }

    /**
     * @return the compiled factory the replay is standing part-way through - the isolated
     * repair runtime's for a converging repair, the primary one for a repair that replays
     * through it. A turn that would replay through a different one is looking at a runtime
     * rebuilt since the capture, or at an operator who declined the isolated runtime
     * mid-repair, and must abandon the candidate rather than continue in it.
     */
    public WindowRecordCursorFactory getWindowFactory() {
        return windowFactory;
    }

    /**
     * @return true when a durable {@code LiveViewCheckpointRepairMarker} written for this
     * repair is still on disk, so the turn that finishes the repair owes either a clear
     * or a retire. See {@link #setRepairMarkerLive(boolean)}
     */
    public boolean isRepairMarkerLive() {
        return isRepairMarkerLive;
    }

    /**
     * @return true while the repair is parked between turns, holding the pinned
     * reader, the uncommitted replacement and the staged capture
     */
    public boolean isSuspended() {
        return isSuspended;
    }

    /**
     * @return true when {@link #close()} abandoned this repair but failed to put the captured
     * window state back, so the compiled factory is left half-restored and the caller must
     * rebuild rather than drain forward over it
     */
    public boolean isWindowStateRestoreFailed() {
        return isWindowStateRestoreFailed;
    }

    /**
     * Opens the session over one repair's plan. The plan is copied rather than
     * referenced: the refresh worker refills its own instance on every repair,
     * while a suspended one has to keep the bounds it derived against the
     * snapshot it pinned.
     */
    public void of(@NotNull LiveViewCheckpointRepairPlan plan) {
        this.plan.copyFrom(plan);
    }

    /**
     * Persists how far the replay got: the boundary it has just finished
     * reproducing, and the {@code checkpointId} of the next one it owes a root
     * version. One small descriptor write per boundary, against a capture that
     * has just frozen the whole runtime state into the data segment.
     */
    public void recordProgress(int capturedBoundaries) {
        descriptor.recordProgress(
                boundaries.getQuick(capturedBoundaries - 1).maxTimestamp,
                capturedBoundaries < boundaries.size()
                        ? boundaries.getQuick(capturedBoundaries).checkpointId
                        : Numbers.LONG_NULL
        );
    }

    /**
     * Records the live-view row counts read off the pre-repair table - the only
     * moment they exist. The prefix below the output floor anchors every repaired
     * root's cumulative position; the two totals prove after the fact that the
     * replacement moved exactly the rows the arithmetic says it did.
     */
    public void setDurableRowCounts(long rowsBeforeRepair, long rowsBelowFloor, long rowsReplaced) {
        this.durableRowsBeforeRepair = rowsBeforeRepair;
        this.durableRowsBelowFloor = rowsBelowFloor;
        this.durableRowsReplaced = rowsReplaced;
    }

    /**
     * Records whether this repair has a live durable repair marker: one is written before
     * a prefix truncate or a timeline splice, and cleared once the post-replay seal - or
     * the splice itself - has made the timeline consistent again. A repair that parks on
     * its turn budget leaves the marker on disk, so the flag travels with the session and
     * the turn that finishes the repair is the one that resolves it.
     */
    public void setRepairMarkerLive(boolean live) {
        this.isRepairMarkerLive = live;
    }

    /**
     * Parks a repair whose replay has spent its turn budget, taking ownership of
     * everything the next turn continues from. The caller has committed nothing,
     * so the durable state a reader sees is still the pre-repair one.
     *
     * @param baseReader         the pinned base snapshot {@code E}, which must stay
     *                           open: no as-of reader could reopen it
     * @param walWriter          the live-view writer carrying the replacement rows
     *                           emitted so far, still uncommitted
     * @param capture            the staged root versions, or null when this repair
     *                           has no timeline generation to splice into
     * @param resumeFromTs       inclusive timestamp the next turn re-opens its scan at
     * @param resumeSkipRows     rows at that timestamp the next turn must skip, having
     *                           already folded and emitted them
     * @param capturedBoundaries boundaries already frozen
     * @param appendedRows       output rows emitted so far
     * @param scanRows           base rows read so far
     * @param replayMinTs        lowest output timestamp so far, or {@link Numbers#LONG_NULL}
     * @param replayMaxTs        highest output timestamp so far, or {@link Numbers#LONG_NULL}
     * @param outputUniqueness   the pairs the replay has emitted so far, copied aside so
     *                           the turn that resumes compares against them rather than
     *                           starting the check over
     */
    public void suspend(
            @NotNull TableReader baseReader,
            @NotNull WalWriter walWriter,
            @Nullable LiveViewCheckpointTimelineStoreWriter.RepairCapture capture,
            long resumeFromTs,
            long resumeSkipRows,
            int capturedBoundaries,
            long appendedRows,
            long scanRows,
            long replayMinTs,
            long replayMaxTs,
            @NotNull LiveViewCheckpointOutputUniqueness outputUniqueness
    ) {
        this.baseReader = baseReader;
        this.walWriter = walWriter;
        this.capture = capture;
        this.resumeFromTs = resumeFromTs;
        this.resumeSkipRows = resumeSkipRows;
        this.capturedBoundaries = capturedBoundaries;
        this.appendedRows = appendedRows;
        this.scanRows = scanRows;
        this.replayMinTs = replayMinTs;
        this.replayMaxTs = replayMaxTs;
        this.outputUniqueness.copyFrom(outputUniqueness);
        this.isSuspended = true;
        this.turns++;
    }

    /**
     * Hands the pinned base snapshot back to the resuming turn, which owns it for
     * the turn's duration and either returns it here through {@link #suspend} or
     * closes it once the repair ends.
     */
    public TableReader takeBaseReader() {
        final TableReader reader = baseReader;
        baseReader = null;
        return reader;
    }

    /**
     * Hands the staged capture back to the resuming turn. Null when the repair
     * runs without one.
     */
    public LiveViewCheckpointTimelineStoreWriter.RepairCapture takeCapture() {
        final LiveViewCheckpointTimelineStoreWriter.RepairCapture staged = capture;
        capture = null;
        return staged;
    }

    /**
     * Hands the live-view writer holding the uncommitted replacement back to the
     * resuming turn.
     */
    public WalWriter takeWalWriter() {
        final WalWriter writer = walWriter;
        walWriter = null;
        isSuspended = false;
        return writer;
    }
}
