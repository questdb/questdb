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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoColumn;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.Lock;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

/**
 * Live-view refresh job.
 * <p>
 * The refresh worker walks the base table's sequencer log forward from
 * {@code lastProcessedSeqTxn + 1}, opens each WAL segment via
 * {@link WalSegmentPageFrameCursor}, and runs rows through the compiled
 * SELECT's filter + window cursor. Two cadences are decoupled:
 * <ul>
 *     <li><b>Refresh</b> appends the computed output rows to the N=2 in-memory
 *     tier as an <i>un-flushed lead</i> — rows in RAM that the LV's on-disk
 *     table does not yet hold. No WAL write happens on this path.</li>
 *     <li><b>Flush</b>, on the {@code FLUSH EVERY} cadence, re-serialises the
 *     lead into the live view's own WAL via {@link WalWriter}, then applies the
 *     just-written block inline on this worker via a dedicated
 *     {@link ApplyWal2TableJob} — the global apply job's {@code doRun} skips LV
 *     tokens so it never races the inline apply. Once apply commits,
 *     {@code lvConsumedSeqTxn} advances and {@code _lv.s} persists through
 *     {@code engine.advanceLiveViewConsumedSeqTxn}, and the worker writes a
 *     rolling head checkpoint under {@code _checkpoints/}.</li>
 * </ul>
 * The in-RAM lead carries no durability of its own: the base WAL purge floor
 * stays at the applied point, so a crash before flush recovers the lead by
 * replaying the retained base WAL forward on restart. Restart and out-of-order
 * (O3) base commits both fall through to the latest head checkpoint and replay
 * forward from it instead of from {@code viewLowerBoundTimestamp}; an O3 cycle
 * discards the in-RAM lead and recomputes it from the rewritten disk.
 * <p>
 * Other behaviours of the refresh path:
 * <ul>
 *     <li>FLUSH EVERY enforces a minimum interval between LV WAL commits: a flush
 *     that arrives within {@code flushEveryMicros} of the previous commit is
 *     deferred and the fallback scan retries on the next worker tick. Under
 *     high-rate base ingestion this batches many base notifications into one LV
 *     commit per FLUSH EVERY interval.</li>
 *     <li>Schema-change detection still routes through {@code ApplyWal2TableJob} on
 *     the base table — this job walks past non-DATA WAL events on the base
 *     without modifying state, while invalidation flows via
 *     {@link CairoEngine#invalidateLiveViewsForBaseTable}.</li>
 *     <li>The LV's WAL block carries {@code maxBaseSeqTxnInBlock} on a dedicated
 *     {@code WalTxnType#LIVE_VIEW_DATA} event; the inline apply on this worker
 *     reads it back and bumps {@code lvConsumedSeqTxn} after the rows are durable
 *     in the LV's own table.</li>
 * </ul>
 */
public class LiveViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    // Anti-spin floor (micros) between re-drains of a view deferred on base apply lag.
    // Bounds the retry rate without perceptibly delaying convergence (LV cadences are
    // >=100ms); the transient lag clears within a few apply-job ticks.
    private static final long APPLY_LAG_DEFER_BACKOFF_US = 5_000;
    // Upper bound on refresh tasks a single Job.run() drains from the notification queue
    // before yielding. A base table under sustained ingestion re-enqueues its task as soon
    // as the refresh finishes, so an unbounded drain would let one base table monopolize the
    // shared refresh pool and starve materialized-view jobs and timers. Leftover / re-enqueued
    // tasks are picked up on the next scheduler turn (run() still reports work, so the worker
    // is re-scheduled promptly), which bounds per-run latency without lowering throughput.
    private static final int MAX_REFRESH_TASKS_PER_RUN = 32;
    // Sentinel returned by replayToApplied when it detected an out-of-order base
    // commit mid-gap and handed off to o3Replay (which rebuilt disk + re-stamped
    // the watermarks). Distinct from the non-negative replayed-row counts.
    private static final long REPLAY_TO_APPLIED_O3 = -1L;
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final AnchorDispatchingCursor anchorDispatchingCursor = new AnchorDispatchingCursor();
    private final ApplyWal2TableJob applyJob;
    private final BlockFileWriter blockFileWriter;
    // Flyweight record over an in-mem tier buffer row, used by the flush path to
    // feed the compiled copier when materialising the un-flushed lead into the LV
    // WAL. Reused across rows; rebound via of() before each copy.
    private final LiveViewBufferRecord bufferRecord = new LiveViewBufferRecord();
    // Reusable manifest bean for the head-checkpoint write hook and the
    // restore path. Mutated only on the refresh-worker thread between clear()
    // and use.
    private final LiveViewCheckpointManifest checkpointManifest = new LiveViewCheckpointManifest();
    // Per-worker reusable checkpoint reader for the 2a.7 restart-restore
    // path. Lazily allocated on the first LV with a head .cp to restore;
    // reused for subsequent LVs by re-opening on a different file.
    private LiveViewCheckpointReader checkpointReader;
    // Test-only: number of trailing FUNCTION_SNAPSHOT blocks the head- and seed-
    // checkpoint writers omit, forging a CRC-valid-but-short checkpoint so a test can
    // drive restoreFromHead's missing-block validation (and, when only the last of
    // several is omitted, the partial-restore re-clear on the seed re-sweep). 0 in
    // production.
    @TestOnly
    private volatile int checkpointTrailingFunctionSnapshotBlocksToOmit;
    // Per-worker reusable checkpoint writer. Lazily allocated on the first
    // cycle that triggers a head write; reused across cycles via of() / commit().
    // Memory pages stay mmapped between writes so a frequently-checkpointed LV
    // does not pay reopen cost. Freed at job close.
    private LiveViewCheckpointWriter checkpointWriter;
    // Dedicated publisher for strictly in-order versioned-timeline cadence
    // entries. O3 forced checkpoints remain on the ring path until Phase 5 can
    // range-splice historical roots.
    private LiveViewCheckpointTimelineStoreWriter checkpointTimelineStoreWriter;
    @TestOnly
    private volatile int checkpointTimelineTestFailureStage;
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final IntList columnIndexes = new IntList();
    private final IntList columnSizeShifts = new IntList();
    // Reusable out-params from a base-WAL drain pass (drainBaseWal), shared by the
    // disk-subset cycle and the lead refresh. Mutated only on the refresh-worker
    // thread between reset() and use.
    private final DrainResult drainResult = new DrainResult();
    private final CairoEngine engine;
    // Scratch list of lvSeqTxns evicted from the retained-checkpoint ring by a
    // prune or a selective O3 invalidation, drained by unlinkCheckpointFiles.
    // Worker-owned; cleared before each use.
    private final LongList evictedCheckpoints = new LongList();
    private final LiveViewRefreshSqlExecutionContext executionContext;
    private final FilteringRecordCursor filteringCursor = new FilteringRecordCursor();
    private final PageFrameMemoryPool memoryPool = new PageFrameMemoryPool(0);
    private final Path path = new Path();
    private final LiveViewRefreshTask refreshTask = new LiveViewRefreshTask();
    // Positional cursor into windowFactory.getWindowFunctions() while a single
    // restoreFromHead walks the checkpoint's FUNCTION_SNAPSHOT blocks. The writer
    // emits one block per snapshot-capable function in window-function order, so
    // restore pairs the i-th block with the i-th snapshot-capable function. Reset
    // to 0 before each block walk; advanced by restoreFunctionBlock. Per-worker;
    // mutated only on the refresh-worker thread.
    private int restoreFunctionCursor;
    // Reusable holder for the values restoreFromHead reads out of the head .cp.
    // One instance per worker; mutated only on the refresh-worker thread between
    // restoreFromHead calls. Avoids per-call allocations on the restart and O3
    // head-hit paths.
    private final RestoredHeadState restoredHeadState = new RestoredHeadState();
    // Publishes _checkpoints/_ring. Lazily allocated on this worker's first
    // publication and held for the worker's life, so a per-cycle publication
    // costs the manifest rewrite plus one mmap/munmap and nothing else. Null
    // until the first view on this worker publishes.
    private LiveViewCheckpointRingManifestWriter ringManifestWriter;
    // Ring membership snapshot handed to a _checkpoints/_ring publication, packed
    // as LiveViewCheckpointRingManifest entry records. Worker-owned; cleared
    // before each use.
    private final LongList ringSnapshot = new LongList();
    // Reusable counter for the seed sweep's skipRows() resume positioning.
    private final RecordCursor.Counter seedSkipCounter = new RecordCursor.Counter();
    // Test-only: when armed, the refresh finally throws right where a real
    // LiveViewInMemoryBuffer.close() would (a native-memory / tracker-balance assert
    // under -ea), so a test can prove the refresh latch is still released on that path.
    // One-shot (self-clears on fire); always false in production.
    @TestOnly
    private boolean simulateStagingBufferCloseFaultForTest;
    // Reusable ARRAY read flyweight for the O3-rebuild disk stager
    // (copyReaderRowsToStaging): binds a view over the LV table reader's (data, aux)
    // column memory for one row, which is immediately re-appended into the staging
    // buffer, so a single instance is safe to reuse across rows and columns. Holds
    // no native memory of its own.
    private final BorrowedArray stagingArrayView = new BorrowedArray();
    // Per-worker staging buffer reused across cycles. Allocated lazily on the
    // first refresh of an LV whose output schema is fully supported by the
    // in-mem tier; reshaped (freed + reallocated) if the next LV's schema
    // differs. Null when no LV has driven a populate-the-tier path yet.
    // Memory-tagged NATIVE_LIVE_VIEW_IN_MEM via LiveViewInMemoryBuffer.
    private LiveViewInMemoryBuffer stagingBuffer;
    private final IntList stagingColumnTypes = new IntList();
    // Output-column indexes of the current LV's SYMBOL columns. The lead drain
    // eager-interns each into the tier's symbol cache (LV-table-consistent ids)
    // and overwrites the staging buffer's segment-local id with the interned id,
    // so the un-flushed lead resolves from RAM and agrees with disk after flush.
    // Empty for a SYMBOL-free schema. Recomputed each cycle in ensureStagingAndTier.
    private final IntList stagingSymbolColumnIndexes = new IntList();
    private int stagingTimestampColumnIndex = -1;
    private final LiveViewStateStore stateStore;
    // Reusable shape buffer for ensureStagingAndTier — alpha-ordered alongside
    // the other staging-related fields so the per-FLUSH-cycle code path can
    // mutate without per-call allocation.
    private final IntList tierColumnTypes = new IntList();
    // Wraps the page-frame cursor during O3 replay so pre-LB rows never reach
    // window.processRow. Single instance reused across cycles; rebound via
    // of() each replay.
    private final TimestampLowerBoundCursor tsLowerBoundCursor = new TimestampLowerBoundCursor();
    private final TimestampUpperBoundCursor tsUpperBoundCursor = new TimestampUpperBoundCursor();
    // Per-turn budget accounting. Reset on entry to refreshInstance; consulted
    // at the per-base-seqTxn boundary inside incrementalRefresh so a long
    // backlog does not monopolise the worker. The budget bounds (max commits
    // and max wall-clock duration) come from CairoConfiguration.
    private int turnCommitsProcessed;
    private long turnStartUs;
    private final ObjList<LiveViewInstance> viewInstanceSink = new ObjList<>();
    private final WalEventReader walEventReader;
    // Reusable WAL-segment cursors hoisted out of incrementalRefresh — each
    // refresh cycle rebinds them via of() instead of allocating fresh
    // instances. WalSegmentPageFrameCursor owns the WalReader + extracted-
    // timestamp scratch buffer; WalSegmentRecordCursor adapts the page frame
    // into a RecordCursor for the compiled SELECT's filter / window cursor.
    private final WalSegmentPageFrameCursor walFrameCursor;
    private final StringSink walNameSink = new StringSink();
    private final Path walPath = new Path();
    private final WalSegmentRecordCursor walRecordCursor;
    // True once the drain feeds a row to the incremental cursor; cleared on turn
    // entry and on every durable commit (fencedLiveViewCommit). If set at failure
    // time, the accumulators lead the last durable commit -> handleRefreshFailure
    // rebuilds so the retry does not double-advance them.
    private boolean windowStateDirty;
    // Number of refresh workers in the pool. The idle fallback scan is sharded by
    // live-view table id across [0, workerCount), so each view is scanned by exactly
    // one worker per sweep - O(views) across the pool instead of O(workers x views).
    private final int workerCount;
    private final int workerId;

    public LiveViewRefreshJob(int workerId, CairoEngine engine, int sharedQueryWorkerCount) {
        this(workerId, 1, engine, sharedQueryWorkerCount);
    }

    public LiveViewRefreshJob(int workerId, int workerCount, CairoEngine engine, int sharedQueryWorkerCount) {
        this.workerId = workerId;
        this.workerCount = workerCount;
        this.engine = engine;
        this.executionContext = new LiveViewRefreshSqlExecutionContext(engine, sharedQueryWorkerCount);
        this.walEventReader = new WalEventReader(engine.getConfiguration());
        this.stateStore = engine.getLiveViewStateStore();
        this.blockFileWriter = new BlockFileWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getCommitMode());
        // Each refresh worker owns a dedicated ApplyWal2TableJob so the inline LV
        // apply on this thread does not contend with global apply pool workers'
        // private state. The global ApplyWal2TableJob.doRun skips LV tokens; this
        // instance is invoked only via applyWalDirect from incrementalRefresh.
        this.applyJob = new ApplyWal2TableJob(engine, sharedQueryWorkerCount);
        this.walFrameCursor = new WalSegmentPageFrameCursor(engine.getConfiguration());
        this.walRecordCursor = new WalSegmentRecordCursor(addressCache, memoryPool);
    }

    @Override
    public void close() {
        LOG.debug().$("live view refresh job closing [workerId=").$(workerId).I$();
        executionContext.close();
        Misc.free(walEventReader);
        Misc.free(walPath);
        Misc.free(path);
        Misc.free(blockFileWriter);
        Misc.free(walFrameCursor);
        Misc.free(walRecordCursor);
        Misc.free(addressCache);
        Misc.free(memoryPool);
        Misc.free(applyJob);
        checkpointReader = Misc.free(checkpointReader);
        checkpointWriter = Misc.free(checkpointWriter);
        checkpointTimelineStoreWriter = Misc.free(checkpointTimelineStoreWriter);
        ringManifestWriter = Misc.free(ringManifestWriter);
        stagingBuffer = Misc.free(stagingBuffer);
    }

    /**
     * Test-only: the per-{@code Job.run()} notification-drain bound. See
     * {@link #MAX_REFRESH_TASKS_PER_RUN}.
     */
    @TestOnly
    public int maxRefreshTasksPerRun() {
        return MAX_REFRESH_TASKS_PER_RUN;
    }

    /**
     * Whether this worker owns the given live view in the idle-scan registry shard.
     * Views are sharded across the pool by table id so each is scanned by exactly one
     * worker per sweep. A single-worker pool ({@code workerCount <= 1}) owns every
     * view, keeping single-threaded and test behavior unchanged. Table ids are stable
     * per view, so a view's owner does not drift between sweeps or across workers.
     */
    public boolean ownsViewShard(int tableId) {
        return workerCount <= 1 || Math.floorMod(tableId, workerCount) == workerId;
    }

    /**
     * Test-only: runs one {@link #processNotifications()} pass (one {@code Job.run()} worth of
     * work) so a test can assert the per-run drain bound without constructing a WorkerContext or
     * looping like {@code drainJob}.
     */
    @TestOnly
    public boolean processNotificationsForTest() {
        return processNotifications();
    }

    /**
     * Test-only: drives {@link #retryPendingLiveViewApply(LiveViewInstance)} directly so a test can
     * assert its {@code finally} frees the runtime state of a view invalidated while the helper held
     * the refresh latch (the invalidator's own free lost the CAS). Production reaches the helper only
     * through {@link #scanForLaggingViews()}, which skips already-invalid views.
     */
    @TestOnly
    public boolean retryPendingLiveViewApplyForTest(LiveViewInstance instance) {
        return retryPendingLiveViewApply(instance);
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        // workerId is the fixed per-worker identity captured at assign(int, job)
        // time. The continuation framework may remount this job on a peer carrier,
        // so workerContext.carrierId() is not asserted against it here.
        return processNotifications();
    }

    /** Test-only failure injection for crash-ordering coverage of timeline publication. */
    @TestOnly
    public void setCheckpointTimelineTestFailureStage(int stage) {
        checkpointTimelineTestFailureStage = stage;
        if (checkpointTimelineStoreWriter != null) {
            checkpointTimelineStoreWriter.setTestFailureStage(stage);
        }
    }

    /**
     * Test-only: makes the head- and seed-checkpoint writers omit the last
     * {@code count} FUNCTION_SNAPSHOT blocks on subsequent writes, forging a
     * CRC-valid-but-short checkpoint so a test can drive {@link #restoreFromHead}'s
     * missing-block validation. Omitting fewer than all blocks leaves earlier
     * functions restored, exercising the seed re-sweep's partial-restore re-clear.
     * Production never calls this.
     */
    @TestOnly
    public void setCheckpointTrailingFunctionSnapshotBlocksToOmit(int count) {
        this.checkpointTrailingFunctionSnapshotBlocksToOmit = count;
    }

    /**
     * Test-only: arms a one-shot fault so the next refresh finally throws at the point where the
     * staging buffer is freed, modelling a native-memory / tracker-balance assert from
     * {@link LiveViewInMemoryBuffer#close()}. Lets a test prove the refresh latch is released even
     * on that throw. Production never calls this.
     */
    @TestOnly
    public void setSimulateStagingBufferCloseFaultForTest() {
        this.simulateStagingBufferCloseFaultForTest = true;
    }

    /**
     * Test-only: the worker's WAL page-frame cursor, so a test can assert its extracted-timestamp
     * scratch releases an outlier transaction's peak rather than retaining it for the worker's life.
     */
    @TestOnly
    public WalSegmentPageFrameCursor walFrameCursorForTest() {
        return walFrameCursor;
    }

    // Read-only-replica lead-reconstruction seam. The primary refresh loop maintains an un-flushed
    // in-RAM lead as a normal part of live-view operation; a read-only replica has no primary-side
    // WAL to drain, so it reconstructs that same lead off the applied base table for freshness parity.
    // That reconstruction is an enterprise concern: EntLiveViewRefreshJob (questdb-ent) overrides the
    // hooks below and composes over the accessors/helpers here. The defaults are primary behaviour --
    // isLeadReconstruction() is false, the drain/o3/publish-stall hooks decline (return false) so the
    // caller runs the primary path, and reconcileLeadWithDisk is a no-op. This job never runs a replica
    // path itself; only the enterprise subclass does.

    /**
     * Reports whether a read-only replica must hold this view's lead work back this tick -- an O3
     * symbol catch-up barrier, or a publish-stall / refresh-failure retry floor. Called twice per
     * tick, and the distinction matters:
     * <ul>
     *     <li>{@code authoritative == false} -- the {@link #scanForLaggingViews} pre-check, which runs
     *     OUTSIDE the refresh latch purely so a gated view costs a clock read instead of a re-drain.
     *     It must be side-effect free: with one job per live-view worker and every worker scanning
     *     every view, a pre-latch clear could erase a gate another worker armed under the latch
     *     microseconds earlier.</li>
     *     <li>{@code authoritative == true} -- the {@link #refreshInstance} check, which runs UNDER
     *     the refresh latch and is the one that decides. A gate satisfied here is cleared here, so
     *     the check-and-clear is atomic against the workers that arm gates (all of which arm under
     *     the same latch).</li>
     * </ul>
     * The primary default declines both. EntLiveViewRefreshJob overrides it.
     */
    protected boolean deferReplicaLeadWork(LiveViewInstance instance, boolean authoritative) {
        return false;
    }

    /**
     * Lets a read-only replica supply the lead rows from the applied base table instead of the raw
     * base WAL. Returns {@code true} when it handled the drain (result already in {@link #getDrainResult()});
     * the primary default returns {@code false}, so {@link #incrementalRefresh} runs {@code drainBaseWal}.
     */
    protected boolean drainLeadOverride(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            TableToken baseToken,
            int cursorTimestampIndex,
            long viewLowerBoundTimestamp,
            long fromSeqTxn,
            boolean populateTier
    ) throws SqlException {
        return false;
    }

    protected CairoEngine getEngine() {
        return engine;
    }

    protected DrainResult getDrainResult() {
        return drainResult;
    }

    protected LiveViewRefreshSqlExecutionContext getExecutionContext() {
        return executionContext;
    }

    protected LiveViewInMemoryBuffer getStagingBuffer() {
        return stagingBuffer;
    }

    protected IntList getStagingSymbolColumnIndexes() {
        return stagingSymbolColumnIndexes;
    }

    protected LiveViewStateStore getStateStore() {
        return stateStore;
    }

    protected WalEventReader getWalEventReader() {
        return walEventReader;
    }

    protected Path getWalPath() {
        return walPath;
    }

    /**
     * Reports whether this worker reconstructs the un-flushed lead in RAM without owning the durable
     * tier (the read-only-replica mode: refresh disabled, lead reconstruction enabled). The primary
     * default is {@code false}; EntLiveViewRefreshJob overrides it.
     */
    protected boolean isLeadReconstruction() {
        return false;
    }

    protected boolean isLeadRollbackSupported(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        return false;
    }

    protected boolean isLeadSlotStale(LiveViewInstance instance) {
        return false;
    }

    /**
     * Handles an out-of-order base commit detected while reconstructing the lead. A read-only replica
     * overrides this to reset the window state to cold-start and serve disk-only until the primary's
     * replicated O3 correction lands (it must not rewrite its own on-disk tier); the primary default
     * returns {@code false}, so {@link #finishLeadRefresh} rewrites disk via {@code o3Replay}.
     */
    protected boolean onLeadO3Detected(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            TableToken baseToken,
            long advanceTo
    ) {
        return false;
    }

    /**
     * Handles a lead publish that stalled with both in-mem tier slots reader-pinned. A read-only replica
     * overrides this to roll the window state back and arm a retry back-off (it cannot flush to disk);
     * the primary default returns {@code false}, so {@link #finishLeadRefresh} flushes the stall to disk.
     */
    protected boolean onLeadPublishStalled(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long advanceTo,
            long appendedRows
    ) throws SqlException {
        return false;
    }

    /**
     * Signals the end of one lead-refresh cycle -- drain plus publish, however it ended (published,
     * stalled, or threw). Runs under the refresh latch. A read-only replica overrides it to release
     * the native backing of the window-state snapshot it took for the publish-stall rollback: that
     * snapshot is scoped to exactly this cycle, so a worker that holds onto it retains its largest
     * ever snapshot for the process lifetime. No-op on a primary, which takes no such snapshot.
     */
    protected void onLeadRefreshCycleEnd() {
    }

    /**
     * Handles a refresh-cycle failure while a read-only replica reconstructs the lead. A replica must
     * never invalidate the view on such a failure -- invalidation is durable ({@link CairoEngine#invalidateLiveView}
     * rewrites {@code _lv.s} via a {@code BlockFileWriter} with no read-only gate) and sticky
     * ({@link CairoEngine#applyLiveViewData} preserves the local invalid flag as the in-band watermark
     * advances), so a transient lead-loop fault would leave the view invalid forever even against a
     * healthy primary, with no replica-side recovery (the documented DROP + CREATE cannot run on a
     * read-only node). {@link #handleRefreshFailure} routes here and returns {@code null} instead of an
     * invalidation reason. The base default is unreachable on a primary ({@link #isLeadReconstruction()}
     * is false there); EntLiveViewRefreshJob overrides it to arm a wall-clock back-off so
     * {@link #scanForLaggingViews} idles the view instead of re-draining into the same fault every tick.
     */
    protected void onReplicaLeadRefreshFailure(LiveViewInstance instance) {
    }

    /**
     * Builds the compiled scan pipeline (timestamp lower-bound + optional filter + optional anchor
     * dispatch + window) over {@code pageCursor}, returning the incremental window cursor. Hides the
     * package-private cursor helpers from the enterprise subclass, which composes over the returned
     * {@link RecordCursor}.
     */
    protected RecordCursor openLeadScanCursor(
            RecordCursor pageCursor,
            int baseTimestampIndex,
            long scanLowTs,
            Function filter,
            LiveViewWindow anchorWindow,
            WindowRecordCursorFactory windowFactory
    ) throws SqlException {
        tsLowerBoundCursor.of(pageCursor, baseTimestampIndex, scanLowTs);
        RecordCursor source = tsLowerBoundCursor;
        if (filter != null) {
            filteringCursor.of(source, filter, executionContext);
            source = filteringCursor;
        }
        if (anchorWindow != null) {
            anchorDispatchingCursor.of(source, anchorWindow, executionContext);
            source = anchorDispatchingCursor;
        }
        return windowFactory.getIncrementalCursor(source, executionContext);
    }

    /**
     * Reconciles a read-only replica's in-RAM lead against the on-disk tier the global apply job
     * advances asynchronously from replicated WAL. The primary default is a no-op (the primary owns
     * every disk advance, so its lead never trails an external flush); EntLiveViewRefreshJob overrides it.
     */
    protected void reconcileLeadWithDisk(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
    }

    /**
     * Publishes one logical checkpoint root into the versioned timeline for the seal
     * in progress, advancing the generation watermarks and the timeline WAL floor.
     * <p>
     * Runs for a forced seal too. A forced seal follows an O3 replay, which already
     * retired the timeline through {@link #retireCheckpointTimelineOnO3}, so the
     * append opens a fresh history whose single root describes the post-replay
     * state. Skipping it - as this did while the timeline was write-only on the
     * in-order path - leaves every replay-driven view with no timeline at all, so a
     * restart has nothing to restore from but a full rebuild from the view's
     * {@code START FROM} boundary.
     */
    private void appendCheckpointTimelineRoot(
            LiveViewInstance instance,
            ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long baseSeqTxn,
            long batchMaxTs
    ) {
        if (checkpointTimelineStoreWriter == null) {
            checkpointTimelineStoreWriter = new LiveViewCheckpointTimelineStoreWriter(engine.getConfiguration());
            checkpointTimelineStoreWriter.setTestFailureStage(checkpointTimelineTestFailureStage);
        }
        final long coveredLvSeqTxn = engine.getTableSequencerAPI()
                .getTxnTracker(instance.getLiveViewToken())
                .getWriterTxn();
        path.of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
        if (engine.isReadOnlyMode()) {
            throw CairoException.authorization().put(CairoException.READ_ONLY_ACCESS_MESSAGE);
        }
        final LiveViewCheckpointTimelineStoreWriter.Result timelineResult;
        final Lock roleLock = engine.getRoleSwitchReadLock();
        roleLock.lock();
        try {
            if (engine.isReadOnlyMode()) {
                throw CairoException.authorization().put(CairoException.READ_ONLY_ACCESS_MESSAGE);
            }
            timelineResult = checkpointTimelineStoreWriter.append(
                    path,
                    functions,
                    anchorWindow,
                    instance.getLiveViewToken().getTableId(),
                    coveredLvSeqTxn,
                    baseSeqTxn,
                    coveredLvSeqTxn,
                    0,
                    true,
                    batchMaxTs,
                    instance.getLvRowsTotal()
            );
        } finally {
            roleLock.unlock();
        }
        instance.recordCheckpointTimelineWalPurgeFloor(timelineResult.getWalPurgeFloor());
    }

    /**
     * Builds the base-column writer-index mapping for {@link WalSegmentPageFrameCursor}.
     */
    private void buildColumnMappings(RecordMetadata baseMetadata, TableToken baseToken) {
        columnIndexes.clear();
        columnSizeShifts.clear();
        try (MetadataCacheReader metaRO = engine.getMetadataCache().readLock()) {
            CairoTable baseTable = metaRO.getTable(baseToken);
            if (baseTable == null) {
                throw CairoException.tableDoesNotExist(baseToken.getTableName());
            }
            for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
                CharSequence colName = baseMetadata.getColumnName(i);
                CairoColumn col = baseTable.getColumnQuiet(colName);
                if (col == null) {
                    throw CairoException.critical(0)
                            .put("live view base column not found [view=").put(baseToken.getTableName())
                            .put(", column=").put(colName).put(']');
                }
                columnIndexes.add(col.getWriterIndex());
                int type = baseMetadata.getColumnType(i);
                if (ColumnType.isVarSize(type)) {
                    columnSizeShifts.add(0);
                } else {
                    columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(type)));
                }
            }
        }
    }

    private RecordCursorFactory ensureCompiledFactory(LiveViewInstance instance) throws SqlException {
        RecordCursorFactory factory = instance.getCompiledFactory();
        if (factory == null) {
            TableToken baseToken = instance.getDefinition().getBaseTableToken();
            boolean ownReader = !executionContext.hasReader();
            TableReader localReader = ownReader ? engine.getReader(baseToken) : null;
            boolean committed = false;
            try {
                if (ownReader) {
                    engine.detachReader(localReader);
                    executionContext.of(localReader);
                }
                executionContext.setLiveViewCompile(true);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    CompiledQuery cq = compiler.compile(instance.getDefinition().getViewSql(), executionContext);
                    factory = cq.getRecordCursorFactory();
                } finally {
                    executionContext.setLiveViewCompile(false);
                }
                // Build the anchor machinery (anchor Function + LiveViewWindow)
                // BEFORE caching the factory. Those are what dispatch the per-row
                // resetPartition; without them an anchored view cannot produce
                // correct output. Caching the factory first would skip this whole
                // block - and the anchor build - on every later refresh, so a
                // build failure would leave the view silently running with resets
                // never dispatched. Leaving the factory uncached makes the next
                // refresh recompile and retry; a persistent failure trips the
                // flush-retry budget and invalidates the view.
                // Acquire the per-view tracker before any window machinery exists, so the
                // anchor map and the window functions' partition maps (bound through the
                // execution context at cursor open) are both charged from their first byte.
                // This sits here, not in ensureAnchorFunction, because an UNANCHORED view has
                // no anchor window at all - and so no frontier compaction either - yet still
                // keeps one accumulator per partition key across cycles. It needs the cap most.
                if (instance.getMemoryTracker() == null) {
                    instance.setMemoryTracker(engine.getMemoryTrackerProvider().acquire(
                            executionContext.getSecurityContext(),
                            instance.getLiveViewToken().getTableId(),
                            MemoryTrackerWorkload.LIVE_VIEW_REFRESH
                    ));
                }
                ensureAnchorFunction(instance, factory);
                instance.setCompiledFactory(factory);
                committed = true;
            } finally {
                if (!committed) {
                    factory = Misc.free(factory);
                }
                if (ownReader) {
                    executionContext.clearReader();
                    engine.attachReader(localReader);
                    localReader.close();
                }
            }
        }
        return factory;
    }

    /**
     * Compiles the persisted anchor expression as a {@link Function} that evaluates
     * against records shaped by the live view's projected metadata (i.e. the same
     * shape as records emitted by {@link WalSegmentRecordCursor}). Stashed on the
     * {@link LiveViewInstance}; consumed by the runtime hookup that wraps the
     * source cursor with {@link LiveViewWindow#processRow(Record)}.
     */
    private void ensureAnchorFunction(LiveViewInstance instance, RecordCursorFactory compiledFactory) throws SqlException {
        if (instance.getAnchorFunction() != null) {
            return;
        }
        LiveViewDefinition.LvAnchorSpec spec = instance.getDefinition().getAnchorSpec();
        if (spec == null || spec.anchorExpressionSql == null) {
            return;
        }
        Function fn = null;
        LiveViewWindow window = null;
        boolean committed = false;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            // Re-parse just the anchor expression text into an ExpressionNode.
            // Going via generateExecutionModel does not work because the optimiser
            // strips named windows after copying their spec into referencing
            // SELECT-column WindowExpressions, and copySpecFrom does not carry the
            // anchor spec across.
            ExpressionNode anchorNode = compiler.parseExpression(spec.anchorExpressionSql);
            if (anchorNode == null) {
                throw CairoException.critical(0)
                        .put("live view anchor expression failed to parse [view=")
                        .put(instance.getDefinition().getViewName())
                        .put(", sql=").put(spec.anchorExpressionSql)
                        .put(']');
            }
            // Resolve against the LV's projected metadata (the page-frame factory's
            // metadata at the leaf of the compiled tree). That matches the records
            // WalSegmentRecordCursor emits at runtime.
            RecordMetadata projectedMeta = findLeafProjectedMetadata(compiledFactory);
            if (projectedMeta == null) {
                throw CairoException.critical(0)
                        .put("live view anchor compile could not resolve projected metadata [view=")
                        .put(instance.getDefinition().getViewName())
                        .put(']');
            }
            FunctionParser fp = new FunctionParser(engine.getConfiguration(), engine.getFunctionFactoryCache());
            executionContext.setLiveViewCompile(true);
            try {
                fn = fp.parseFunction(anchorNode, projectedMeta, executionContext);
            } finally {
                executionContext.setLiveViewCompile(false);
            }
            WindowRecordCursorFactory wf = unwrapWindowFactory(compiledFactory);
            // Reset only the anchored WINDOW's functions (UNBOUNDED PRECEDING ... CURRENT
            // ROW frames). A bounded ROWS/RANGE window declared alongside the anchored one
            // must keep sliding across anchor crossings -- dispatching resetPartition to it
            // would zero its frame at every bucket boundary and corrupt its output.
            ObjList<WindowFunction> anchoredFunctions = wf.getAnchorableWindowFunctions();
            if (anchoredFunctions == null || anchoredFunctions.size() == 0) {
                throw CairoException.critical(0)
                        .put("live view anchored window has no unbounded window functions [view=")
                        .put(instance.getDefinition().getViewName())
                        .put(']');
            }
            // The tracker was acquired by ensureCompiledFactory before it called us, so the
            // anchor map allocates against it from its first byte.
            window = LiveViewWindow.build(
                    engine.getConfiguration(),
                    compiler.getAsm(),
                    spec.windowName,
                    projectedMeta,
                    spec.partitionColumnNames,
                    fn,
                    anchoredFunctions,
                    isAnchorMonotoneWithBaseOrder(anchorNode, projectedMeta),
                    instance.getMemoryTracker()
            );
            // Commit the anchor Function and window together, only after the full
            // machinery builds. A failure before this point must not leave a
            // half-built anchor (function set, window null): the per-row reset
            // would never dispatch and the view would silently produce wrong
            // results. Propagating instead leaves the compiled factory uncached
            // (see ensureCompiledFactory) so the next refresh retries; a
            // persistent failure invalidates via the flush-retry budget.
            instance.setAnchorFunction(fn);
            instance.setAnchorWindow(window);
            committed = true;
        } finally {
            if (!committed) {
                Misc.free(window);
                Misc.free(fn);
            }
        }
    }

    /**
     * Walks the compiled SELECT factory chain down to the leaf
     * {@code PageFrameRecordCursorFactory} and returns its projected metadata.
     * That metadata matches the records {@link WalSegmentRecordCursor} emits at
     * runtime, so an anchor {@code Function} compiled against it will produce
     * correct results when invoked on the LV's source rows.
     */
    private static RecordMetadata findLeafProjectedMetadata(RecordCursorFactory factory) {
        WindowRecordCursorFactory wf = unwrapWindowFactory(factory);
        RecordCursorFactory base = wf.getBaseFactory();
        if (base.getFilter() != null) {
            base = base.getBaseFactory();
        }
        return base.getMetadata();
    }

    /**
     * Returns the ring index of the newest retained checkpoint whose
     * {@code maxTs} is strictly below {@code ceilTs}, or {@code -1} when the ring
     * holds no such entry (every retained anchor sits at or above the ceiling, so
     * the late row predates the whole ring and the caller must rebuild from the
     * view boundary).
     * <p>
     * The retained-checkpoint ring is held in strictly increasing {@code maxTs}
     * order (oldest at index 0), so the scan walks from the newest entry down and
     * returns the first one under the ceiling - the closest sealed anchor below
     * the late row, which yields the shortest resume replay {@code (maxTs, head]}.
     * A head-hit resumes from the newest entry; this picks an OLDER entry only
     * when the newest one (the head) sits at or above the late row - the
     * bounded-miss case.
     */
    private static int findResumeAnchorBelow(LiveViewInstance instance, long ceilTs) {
        for (int i = instance.getRetainedCheckpointCount() - 1; i >= 0; i--) {
            if (instance.getRetainedCheckpointMaxTs(i) < ceilTs) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Determines whether the anchor expression is provably monotone non-decreasing
     * with the base scan order, which is the enabling condition for frontier-gated
     * anchor-map compaction (see {@link LiveViewWindow}). During incremental refresh
     * the base page-frame cursor emits rows in ascending designated-timestamp order,
     * so an anchor that never dips below a value it has already produced advances with
     * the stream and the sweep may safely evict partitions two buckets behind the
     * frontier. An anchor that can dip back - into an already-evicted bucket - resets
     * the accumulator and silently undercounts; such anchors must keep every
     * partition.
     * <p>
     * The check is an allowlist of expression forms provably monotone non-decreasing
     * in the ascending designated timestamp (see {@link #isProvablyMonotoneAnchor}):
     * the bare designated timestamp column, a two-argument {@code timestamp_floor} /
     * {@code date_trunc} (the pure UTC floor), or a {@code dateadd} with a constant
     * stride and a constant fixed-duration-unit period (calendar {@code 'M'} / {@code 'y'}
     * periods are excluded - their day-of-month clamp is non-monotone), each composable
     * over another such form.
     * <p>
     * It is deliberately conservative because the failure modes are asymmetric: a
     * false negative only forgoes compaction (more resident memory, still correct),
     * whereas a false positive drops live state a later dip row revisits (silent
     * undercount). So it inspects the wrapping FUNCTIONS, not merely the column
     * references - a non-monotone function of the designated timestamp reads only that
     * column yet is not monotone (e.g. {@code dateadd('d', hour(ts), ts)} climbs
     * intra-day then dips ~23 days at each midnight; {@code to_timezone} dips an hour
     * at DST fall-back), so a column-identity check alone would wrongly enable
     * compaction. The three-argument (from-offset) and timezone-aware floor variants
     * are excluded even though some are monotone: a sub-day timezone floor dips at DST
     * fall-back, so they forgo compaction rather than risk a false positive, and
     * {@code instanceof MonotonicTimestampFunction} is not a sound gate either -
     * {@code to_timezone} implements it (for interval pruning) despite that dip. The
     * runtime latch in {@link LiveViewWindow} remains a backstop for a
     * monotone-looking anchor whose values decrease at runtime.
     */
    private static boolean isAnchorMonotoneWithBaseOrder(ExpressionNode anchorNode, RecordMetadata projectedMeta) {
        final int tsIndex = projectedMeta.getTimestampIndex();
        if (tsIndex < 0) {
            return false;
        }
        return isProvablyMonotoneAnchor(anchorNode, projectedMeta, tsIndex);
    }

    /**
     * Returns {@code true} when {@code node} and its entire subtree carry no reference
     * to a base column - every leaf is a constant, or a literal that does not resolve
     * to a projected column. Used to confirm that a monotone-preserving function's
     * non-timestamp arguments (the stride, unit, offset) are constant, so the function
     * applies a fixed transform to every row rather than a row-dependent one. A
     * null-token literal reads as a reference (returns {@code false}), the safe
     * direction: an unresolvable leaf forgoes compaction instead of assuming it is a
     * constant.
     */
    private static boolean containsNoColumnReference(ExpressionNode node, RecordMetadata projectedMeta) {
        if (node == null) {
            return true;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return node.token != null && projectedMeta.getColumnIndexQuiet(node.token) < 0;
        }
        // paramCount <= 2 stores children in lhs/rhs; > 2 stores them in args.
        if (node.paramCount > 2) {
            for (int i = 0, n = node.args.size(); i < n; i++) {
                if (!containsNoColumnReference(node.args.getQuick(i), projectedMeta)) {
                    return false;
                }
            }
            return true;
        }
        return containsNoColumnReference(node.lhs, projectedMeta)
                && containsNoColumnReference(node.rhs, projectedMeta);
    }

    /**
     * Confirms an allowlisted monotone-preserving call has exactly one argument that
     * is itself a monotone form (the timestamp carrier) while every other argument is
     * a constant (carries no column reference). This is order-agnostic across the
     * child slots, so it does not depend on which of {@code lhs}/{@code rhs} holds the
     * timestamp. A variable stride such as {@code hour(ts)} carries a column reference,
     * so it fails the constant check and the call is rejected.
     */
    private static boolean hasSingleMonotoneArgRestConstant(ExpressionNode node, RecordMetadata projectedMeta, int tsIndex) {
        int monotoneArgs = 0;
        // paramCount <= 2 stores children in lhs/rhs; > 2 stores them in args.
        if (node.paramCount > 2) {
            for (int i = 0, n = node.args.size(); i < n; i++) {
                final ExpressionNode arg = node.args.getQuick(i);
                if (isProvablyMonotoneAnchor(arg, projectedMeta, tsIndex)) {
                    monotoneArgs++;
                } else if (!containsNoColumnReference(arg, projectedMeta)) {
                    return false;
                }
            }
        } else {
            for (int i = 0; i < 2; i++) {
                final ExpressionNode arg = i == 0 ? node.lhs : node.rhs;
                if (arg == null) {
                    continue;
                }
                if (isProvablyMonotoneAnchor(arg, projectedMeta, tsIndex)) {
                    monotoneArgs++;
                } else if (!containsNoColumnReference(arg, projectedMeta)) {
                    return false;
                }
            }
        }
        return monotoneArgs == 1;
    }

    /**
     * Recursive worker for {@link #isAnchorMonotoneWithBaseOrder}. Returns
     * {@code true} only for expression forms provably monotone non-decreasing in the
     * ascending designated timestamp:
     * <ul>
     *     <li>the designated-timestamp column itself (strictly increasing);</li>
     *     <li>a two-argument {@code timestamp_floor} / {@code date_trunc} - the pure
     *     UTC floor, monotone non-decreasing - whose timestamp argument is itself a
     *     monotone form and whose stride argument is constant. The three- and
     *     five-argument (from-offset / timezone) overloads are excluded because a
     *     sub-day timezone floor dips at DST fall-back;</li>
     *     <li>a three-argument {@code dateadd} whose stride is constant and whose
     *     period is a constant fixed-duration unit (a fixed offset applied to every
     *     row) and whose timestamp argument is a monotone form. A variable stride is
     *     not monotone and is rejected because the stride argument then carries a
     *     column reference. A calendar-unit period ({@code 'M'} / {@code 'y'}) is
     *     rejected by {@link #hasConstFixedDurationPeriod} because day-of-month
     *     clamping makes it non-monotone by up to one unit.</li>
     * </ul>
     * Any other function, operator, or column reference returns {@code false}.
     */
    private static boolean isProvablyMonotoneAnchor(ExpressionNode node, RecordMetadata projectedMeta, int tsIndex) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            // The lone monotone leaf is the designated timestamp column, which the
            // incremental scan emits in ascending order. Any other column - a
            // non-designated TIMESTAMP is the dangerous case - can dip backward.
            return node.token != null && projectedMeta.getColumnIndexQuiet(node.token) == tsIndex;
        }
        if (node.type != ExpressionNode.FUNCTION || node.token == null) {
            return false;
        }
        if (node.paramCount == 2
                && (Chars.equalsIgnoreCase(node.token, "timestamp_floor")
                || Chars.equalsIgnoreCase(node.token, "date_trunc"))) {
            return hasSingleMonotoneArgRestConstant(node, projectedMeta, tsIndex);
        }
        if (node.paramCount == 3 && Chars.equalsIgnoreCase(node.token, "dateadd")
                && hasConstFixedDurationPeriod(node)) {
            return hasSingleMonotoneArgRestConstant(node, projectedMeta, tsIndex);
        }
        return false;
    }

    /**
     * Confirms the period argument of a three-argument {@code dateadd} is a constant
     * fixed-duration unit. A fixed-duration unit ({@code s}/{@code m}/{@code h}/{@code d}/
     * {@code w}/...) adds the same constant to every row, so the add is monotone
     * non-decreasing in the ascending timestamp. A calendar unit ({@code 'M'} month or
     * {@code 'y'} year) clamps day-of-month ({@link io.questdb.std.datetime.microtime.Micros#addMonths}),
     * so it is non-monotone by up to one unit: a later row can produce an anchor below one
     * an earlier row already produced (e.g. Jan 30 and Jan 31 both clamp to Feb 28), which
     * revisits an already-evicted bucket and silently undercounts. This is the same gate
     * {@code TimestampAddFunctionFactory} applies before treating {@code dateadd} as a
     * {@code MonotonicTimestampFunction} for interval pruning.
     * <p>
     * The period is {@code dateadd}'s FIRST SQL argument; args are stored inverted (see
     * {@code SqlParser}), so it is the last list item. Anything not provably a constant
     * single-character fixed-duration-unit literal is rejected, forgoing compaction, which
     * is the safe direction (still correct, just more resident memory).
     */
    private static boolean hasConstFixedDurationPeriod(ExpressionNode node) {
        final ExpressionNode period = node.args.getLast();
        if (period == null || period.type != ExpressionNode.CONSTANT || period.token == null) {
            return false;
        }
        // The period is a quoted single-character unit literal, e.g. 'M'; require exactly 'X'.
        final CharSequence token = period.token;
        if (token.length() != 3 || !Chars.isQuoted(token)) {
            return false;
        }
        return CommonUtils.isFixedDurationUnit(token.charAt(1));
    }

    /**
     * Cooperative apply-lag gate for the drain-triggered refresh paths that read
     * the applied base: the raw-WAL O3 replay ({@link #incrementalRefresh} /
     * {@link #finishLeadRefresh}) and the coupled dedup applied-base drain
     * ({@link #drainAppliedBase}). Peeks at the base table's applied seqTxn -- the
     * {@link SeqTxnTracker}'s writer txn, an O(1) in-memory read -- and throws
     * {@link LiveViewApplyLagException} when
     * {@code ApplyWal2TableJob} has not yet applied up to {@code advanceTo}.
     * <p>
     * The tracker's writer txn is the applied point: {@code ApplyWal2TableJob}
     * bumps it (updateWriterTxns) only AFTER the durable {@code _txn} commit, so it
     * never exceeds a freshly opened reader's {@code getSeqTxn()}. Reading it instead
     * of opening a {@code TableReader} keeps this hot, per-gated-cycle check off the
     * base's shared {@code TxnScoreboard} - a pooled reader checkout does an atomic
     * acquireTxn contended with the WAL-apply writer and every query reader, plus a
     * purge-schedule on close - at the cost of at most a benign extra defer while the
     * tracker momentarily lags the just-committed {@code _txn}. A cold tracker reads
     * {@code -1}, which defers (the safe direction) until apply warms it.
     * <p>
     * Callers invoke this BEFORE any destructive replay work (head {@code .cp}
     * retirement, window-state reset, the REPLACE_RANGE commit, discarding the
     * in-RAM lead) or before pinning the applied-base scan reader, so the throw
     * unwinds to {@link #refreshInstance} with no durable change and no
     * reader-visible tier shrink; the next fallback scan re-triggers the view once
     * apply catches up. The base applied seqTxn only advances on the global
     * {@code ApplyWal2TableJob}, so block-spinning inside {@link #waitForApply}
     * instead starves this refresh worker and, on the single-threaded
     * refresh/drain model the fuzz harness drives, deadlocks outright (the same
     * thread that must advance the apply is the one spinning).
     * <p>
     * The remaining {@link #waitForApply} callers (restart restore, seed
     * sweep, replay-to-applied) always target a seqTxn the base has already
     * applied - the LV consumed it before - so they never lag; the base
     * metadata-drift recovery keeps the blocking wait because its replay must
     * complete atomically within one recovery attempt.
     */
    private void ensureBaseApplied(TableToken baseToken, long advanceTo) {
        final long appliedSeqTxn = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
        if (appliedSeqTxn < advanceTo) {
            throw LiveViewApplyLagException.instance(baseToken, advanceTo, appliedSeqTxn);
        }
    }

    /**
     * Returns a {@link RecordToRowCopier} for the live view, compiling a fresh one when
     * the cached one's metadata version is out of sync with the WAL writer.
     */
    private RecordToRowCopier ensureCopier(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            WalWriter walWriter
    ) throws SqlException {
        long metadataVersion = walWriter.getMetadata().getMetadataVersion();
        RecordToRowCopier copier = instance.getRecordToRowCopier();
        if (copier == null || instance.getRecordRowCopierMetadataVersion() != metadataVersion) {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                columnFilter.of(windowFactory.getMetadata().getColumnCount());
                copier = RecordToRowCopierUtils.generateCopier(
                        compiler.getAsm(),
                        windowFactory.getMetadata(),
                        walWriter.getMetadata(),
                        columnFilter,
                        engine.getConfiguration()
                );
                instance.setRecordToRowCopier(copier, metadataVersion);
            }
        }
        return copier;
    }

    private WindowRecordCursorFactory getWindowFactory(LiveViewInstance instance) throws SqlException {
        RecordCursorFactory factory = ensureCompiledFactory(instance);
        return unwrapWindowFactory(factory);
    }

    /**
     * Walks the sequencer log forward and processes each DATA commit through the
     * compiled window cursor. For each output row, writes to both the LV's WAL (durable
     * tier) and the in-memory tier (read cache). Commits the WAL writer once at the end
     * of the cycle; advances {@code lastProcessedSeqTxn} / {@code lvConsumedSeqTxn} /
     * {@code appliedWatermark} on the instance and rewrites {@code _lv.s}.
     */
    private void incrementalRefresh(LiveViewInstance instance, long fromSeqTxn, long toSeqTxn, boolean leadMode) throws SqlException {
        WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
        final Function filter = filterFactory.getFilter();
        RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
        TableToken baseToken = instance.getDefinition().getBaseTableToken();
        RecordMetadata baseMetadata = pageFrameFactory.getMetadata();
        final int baseTimestampIndex = baseMetadata.getTimestampIndex();
        buildColumnMappings(baseMetadata, baseToken);

        // The view's resolved START FROM boundary, and the floor the forward-append path
        // takes. The initial seed and O3 head-miss replay bound their base scans at the very
        // same value, so the forward path must drop sub-floor rows as well - otherwise a
        // back-dated row would be kept when it arrives in order (forward) but dropped when
        // it arrives out of order (replay), making the view's contents depend on the arrival
        // path rather than on the data. START FROM BEGINNING has no floor (LONG_NULL), so
        // nothing is dropped there.
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();

        // Decide whether the in-memory tier can be populated for this LV. Every
        // output column must be a type the tier can store (fixed-width, SYMBOL,
        // STRING, BINARY, VARCHAR, ARRAY); an unsupported type (a non-persisted type
        // such as INTERVAL) falls back to disk-only. The staging buffer is reshaped on schema-mismatch;
        // the LV's tier is lazily allocated on first use.
        RecordMetadata outMetadata = windowFactory.getMetadata();
        int cursorTimestampIndex = outMetadata.getTimestampIndex();
        if (cursorTimestampIndex < 0) {
            throw CairoException.nonCritical()
                    .put("live view requires a designated timestamp [view=")
                    .put(instance.getDefinition().getViewName()).put(']');
        }
        boolean populateTier = ensureStagingAndTier(instance, outMetadata, cursorTimestampIndex);

        // Snapshot the LV's latestSeenTs at cycle entry. On O3 detect +
        // rollback any in-cycle bumps from the discarded rows must roll back
        // too, otherwise a later in-order commit whose ts sits between the
        // pre-cycle watermark and the inflated value gets misclassified as O3.
        final long latestSeenTsSnapshot = instance.getLatestSeenTs();
        drainResult.reset();

        if (leadMode) {
            // Lead refresh: drain the base WAL into the in-mem tier as the
            // un-flushed lead. No LV WAL commit and no apply here - the flush
            // (commit + apply) runs on the FLUSH EVERY cadence in refreshInstance
            // and materialises the lead out of the tier. The tier therefore leads
            // disk by the rows accumulated since the last flush; reads serve them
            // via the seam cut. The lead is in RAM only and recovered by replaying
            // base WAL forward on restart.
            //
            // A read-only replica overrides drainLeadOverride to compute the lead off the APPLIED
            // base table instead: its raw WAL segments race their own download/apply, so a raw read
            // can transiently return 0 rows for already-applied data and drop the batch, whereas the
            // applied reader is consistent. The primary default returns false, so the raw-WAL drain
            // below runs. Either source lands its result in drainResult, which finishLeadRefresh then
            // publishes as the un-flushed lead.
            try {
                if (!drainLeadOverride(
                        instance, windowFactory, baseToken, cursorTimestampIndex,
                        viewLowerBoundTimestamp, fromSeqTxn, populateTier
                )) {
                    drainBaseWal(
                            instance, windowFactory, baseToken, baseMetadata, baseTimestampIndex,
                            cursorTimestampIndex, viewLowerBoundTimestamp, Long.MAX_VALUE, filter, fromSeqTxn, toSeqTxn,
                            null, null, populateTier, latestSeenTsSnapshot
                    );
                }
                finishLeadRefresh(instance, windowFactory, baseToken, populateTier);
            } finally {
                // The publish decision is made; a replica's rollback snapshot is dead either way.
                onLeadRefreshCycleEnd();
            }
            return;
        }

        try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
            RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
            int lvTimestampIndex = walWriter.getMetadata().getTimestampIndex();
            if (lvTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(instance.getDefinition().getViewName()).put(']');
            }
            drainBaseWal(
                    instance, windowFactory, baseToken, baseMetadata, baseTimestampIndex,
                    cursorTimestampIndex, viewLowerBoundTimestamp, Long.MAX_VALUE, filter, fromSeqTxn, toSeqTxn,
                    walWriter, copier, populateTier, latestSeenTsSnapshot
            );
            // Publish ahead of the commit below and of the no-row branch's bare
            // watermark walk; both advance the floor to advanceTo. The o3Detected
            // guard is load-bearing, not defensive: drainBaseWal rolls its draft
            // back on detect but leaves advanceTo sitting ON the offending seqTxn,
            // so publishing here would claim the ring is sealed at the very commit
            // that unseals it. That cycle republishes through o3Replay's retire.
            if (!drainResult.o3Detected) {
                publishCheckpointRingOnAdvance(instance, drainResult.advanceTo);
            }
            if (drainResult.appendedRows > 0) {
                // The LV WAL block carries advanceTo as maxBaseSeqTxnInBlock. The
                // inline apply below makes the rows durable in the LV's on-disk
                // table; only then do we advance lvConsumedSeqTxn so base WAL
                // retention releases.
                fencedLiveViewCommit(() -> walWriter.commitLiveView(drainResult.advanceTo));
            }
        }

        long advanceTo = drainResult.advanceTo;
        long appendedRows = drainResult.appendedRows;
        long batchMaxTs = drainResult.batchMaxTs;
        long stagingMaxTs = drainResult.stagingMaxTs;
        boolean o3Detected = drainResult.o3Detected;
        long o3LateRowTs = drainResult.o3LateRowTs;
        long o3SeqTxn = drainResult.o3SeqTxn;
        if (o3Detected) {
            // Gate on base apply before the replay: o3Replay reads the applied
            // base at o3SeqTxn, so if ApplyWal2TableJob has not caught up,
            // ensureBaseApplied unwinds cooperatively (no watermark advance) and
            // the next tick retries once apply lands, rather than block-spinning in
            // the downstream waitForApply (which deadlocks the single-threaded
            // drain). drainBaseWal already rolled back this cycle's draft on O3
            // detect, so the unwind leaves no durable or reader-visible change.
            //
            // Defense-in-depth: unlike the lead-path twin (finishLeadRefresh, covered
            // by testO3ReplayDefersOnBaseApplyLagInsteadOfDeadlocking), the deferral
            // here is not deterministically reachable. The only routes into this
            // leadMode==false drain are a dedup-clean cycle - which isRangeProvablyClean
            // admits only when apply has already covered toSeqTxn (>= o3SeqTxn), so the
            // gate passes - and a tier-unstorable output type, which is INTERVAL alone
            // and cannot be persisted into an LV table. The gate stays for symmetry and
            // to cover a narrow apply-signal race; its logic is identical to the tested
            // lead path.
            ensureBaseApplied(baseToken, o3SeqTxn);
            // The replay path opens its own WalWriter and TableReader on the
            // base, drives the ts-sorted re-execution, commits a single
            // REPLACE_RANGE block, applies inline, and advances the LV
            // watermarks itself. Returning here keeps the in-WAL-order
            // post-cycle branch out of the picture; the next refresh tick
            // resumes from o3SeqTxn + 1.
            o3Replay(instance, windowFactory, o3LateRowTs, baseToken, o3SeqTxn);
            return;
        }

        if (advanceTo > instance.getLastProcessedSeqTxn()) {
            // Advance the in-memory lastProcessedSeqTxn before apply / persist.
            // A stranded LV WAL block (commit succeeded, apply or persist
            // failed) cannot drive a duplicate emit because the next cycle
            // reads the advanced in-memory value and resumes from
            // advanceTo + 1. testRefreshPersistFailureKeepsInMemoryAdvanced
            // pins this invariant. The remaining restart-edge-case where
            // _lv.s is stale on the next process boot is covered by the
            // forward-scan recovery in LiveViewRecovery.
            instance.setLastProcessedSeqTxn(advanceTo);
            // This path applies every cycle, so appliedWatermark tracks
            // lastProcessed and the in-mem tier stays a subset of disk (no
            // un-flushed lead). It serves lead-ineligible LVs (a non-persisted output
            // column type such as INTERVAL, or no designated timestamp);
            // lead-eligible LVs take the refresh/flush split instead.
            instance.setAppliedWatermark(advanceTo);
            boolean lvConsumedPersisted = false;
            // LV-table applied seqTxn for the fence stamp; LONG_NULL until apply.
            long lvAppliedSeqTxn = Numbers.LONG_NULL;
            if (appendedRows > 0) {
                // LV apply runs inline on this thread. The
                // global ApplyWal2TableJob.doRun skips LV tokens, so without
                // applyWalDirect here the LIVE_VIEW_DATA block would sit
                // unapplied and the on-disk tier would not catch up.
                applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
                // Capture the just-applied LV-table seqTxn (matches a query
                // reader's getSeqTxn()) to stamp the slot below.
                lvAppliedSeqTxn = engine.getTableSequencerAPI()
                        .getTxnTracker(instance.getLiveViewToken())
                        .getWriterTxn();
                // Apply has committed the _txn (the durability cut for the rows).
                // Now publish the new lvConsumedSeqTxn floor and persist _lv.s
                // through the refresh worker's reusable BlockFileWriter + Path so
                // base WAL retention can release the consumed segments.
                try {
                    engine.advanceLiveViewConsumedSeqTxn(
                            instance.getLiveViewToken(),
                            advanceTo,
                            blockFileWriter,
                            path
                    );
                    lvConsumedPersisted = true;
                } catch (CairoException e) {
                    LOG.critical().$("could not advance live view consumed seqTxn after apply [view=")
                            .$(instance.getDefinition().getViewName())
                            .$(", advanceTo=").$(advanceTo)
                            .$(", error=").$safe(e.getFlyweightMessage()).I$();
                }
            } else {
                // No LIVE_VIEW_DATA block was emitted (every base seqTxn was non-DATA —
                // schema change, DROP PARTITION, TRUNCATE, base TTL — or every row was
                // rejected by the WHERE filter). There is nothing to apply, but
                // lvConsumedSeqTxn must still advance or base WAL retention would
                // stall forever; non-DATA seqTxns still walk the watermark forward.
                try {
                    engine.advanceLiveViewConsumedSeqTxn(
                            instance.getLiveViewToken(),
                            advanceTo,
                            blockFileWriter,
                            path
                    );
                    lvConsumedPersisted = true;
                } catch (CairoException e) {
                    LOG.critical().$("could not advance live view consumed seqTxn on no-row cycle [view=")
                            .$(instance.getDefinition().getViewName())
                            .$(", advanceTo=").$(advanceTo)
                            .$(", error=").$safe(e.getFlyweightMessage()).I$();
                }
            }
            if (!lvConsumedPersisted) {
                // advanceLiveViewConsumedSeqTxn threw before publishing the new
                // floor. Persist lastProcessed + appliedWatermark anyway so the
                // next cycle does not redo the walked-past seqTxns. If this also
                // fails, the exception propagates to refreshInstance's
                // handleRefreshFailure which ticks the flush-retry budget.
                persistState(instance);
            }
            if (lvConsumedPersisted && populateTier && appendedRows > 0) {
                // Publish the just-applied rows into the tier as a subset of disk
                // (leadRowCount = 0). Failure to acquire a write slot is a
                // non-fatal stall: the on-disk tier still advanced, the in-mem
                // tier just trails this cycle. Any tier-populating output schema
                // (fixed-width, SYMBOL via eager interning, or var-length) is
                // lead-eligible and takes the refresh/flush split instead, so this
                // disk-subset publish is effectively unreachable; kept defensively.
                publishToInMemoryTier(instance, stagingMaxTs, lvAppliedSeqTxn, appendedRows, false);
            }
            if (lvConsumedPersisted && appendedRows > 0) {
                // 2a.4 head-checkpoint write hook. Ordered after the apply's
                // _txn advance and the lvConsumedSeqTxn publish so the .cp on
                // disk reflects state that is also durably committed in the
                // LV's own table. A failure here does not invalidate the view
                // (.cp is a derived artifact): the prior head remains addressable
                // and the next eligible cycle retries.
                //
                // O3 cycles never reach this branch: detect rolls back the
                // in-WAL-order draft and hands off to o3Replay, which writes
                // its own fresh head on completion (follow-up commit).
                maybeWriteHeadCheckpoint(instance, windowFactory, advanceTo, batchMaxTs, appendedRows, false);
            }
        }
    }

    /**
     * Coupled, applied-reader refresh cycle for a live view whose base table has
     * DEDUP keys. Sibling of the raw-WAL {@link #drainBaseWal} / {@link #incrementalRefresh}
     * pair: instead of appending the pre-dedup WAL stream, it reads the applied,
     * post-dedup base via a {@link TableReader} pinned behind the cooperative
     * {@link #ensureBaseApplied} apply-lag gate and routes any timestamp-overlap batch
     * through {@link #o3Replay}. The proven non-dedup {@code drainBaseWal} bytecode is
     * left untouched.
     * <p>
     * Each cycle:
     * <ol>
     *     <li>Gate on base apply ({@link #ensureBaseApplied}, defers cooperatively on
     *     apply lag) before pinning the applied base snapshot, then fix
     *     {@code effectiveSeqTxn = reader.getSeqTxn()} - the reader may sit past
     *     {@code toSeqTxn} if apply raced ahead. Both the overlap walk and the forward
     *     scan bound to this same point so no unexamined seqTxn slips past the cheap
     *     append.</li>
     *     <li>Compute {@code batchMinTs} by walking the base WAL-E events over
     *     {@code (fromSeqTxn, effectiveSeqTxn]}, taking {@code min(DataInfo.getMinTimestamp())}
     *     over DATA commits (structural {@code walId<=0} skipped, non-DATA excluded via
     *     {@link WalTxnType#isDataType}). Reads commit metadata only (type + min/max ts),
     *     never the pre-dedup data columns. Intra-commit out-of-order is not consulted:
     *     the applied reader is ts-sorted, so it is not an overlap trigger here (2A.6b).</li>
     *     <li>If {@code batchMinTs <= latestSeenTs} (a row at/below the frontier may have
     *     been added or dedup-replaced), release the reader and hand off to
     *     {@link #o3Replay} (correct whether the overlap was a replacement or an additive
     *     same-timestamp row). Otherwise do a strictly-forward cheap append of the
     *     post-dedup rows above the frontier, then commit / apply / advance watermarks to
     *     {@code effectiveSeqTxn} / checkpoint / publish the disk-subset tier.</li>
     * </ol>
     * The window functions retain accumulator state across cycles, so the forward scan
     * bounded to {@code ts > latestSeenTs} continues the accumulation exactly as
     * {@code drainBaseWal}'s per-commit continuation does - only the source is
     * post-dedup.
     */
    private void drainAppliedBase(LiveViewInstance instance, long fromSeqTxn, long toSeqTxn) throws SqlException {
        final WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        final String viewName = instance.getDefinition().getViewName();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();

        final RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
        final Function filter = filterFactory.getFilter();
        final PageFrameRecordCursorFactory pageFrameFactory = (PageFrameRecordCursorFactory) (filter != null ? filterFactory.getBaseFactory() : filterFactory);
        final RecordMetadata outMetadata = windowFactory.getMetadata();
        final int cursorTimestampIndex = outMetadata.getTimestampIndex();
        if (cursorTimestampIndex < 0) {
            throw CairoException.nonCritical()
                    .put("live view requires a designated timestamp [view=").put(viewName).put(']');
        }

        // Cooperative apply-lag gate before pinning the applied (post-dedup) base
        // snapshot (see ensureBaseApplied): peek the base's applied seqTxn and defer
        // this cycle by throwing LiveViewApplyLagException when ApplyWal2TableJob has
        // not reached toSeqTxn yet, rather than block-spinning in waitForApply. This
        // coupled dedup drain runs on the same single refresh/drain worker that has
        // to advance the base apply, so a blocking wait starves that worker for the
        // whole flush-retry budget and, on the single-threaded refresh/drain model,
        // deadlocks outright; a sustained-lag streak also drives the flush-retry
        // budget to exhaustion and spuriously invalidates the view - the exact
        // coupling the non-dedup raw-WAL O3 gate already avoids. (waitForApply threw
        // a plain CairoException that handleRefreshFailure counts as a refresh fault,
        // so the cooperative deferral never reached this path.) The gate sits before
        // any destructive work, so the unwind leaves the view untouched and the next
        // fallback scan retries once the apply lands. apply is monotone, so the
        // reader opened next observes at least the applied seqTxn just checked.
        ensureBaseApplied(baseToken, toSeqTxn);
        TableReader reader = engine.getReader(baseToken);
        try {
            // reader.getSeqTxn() may run past toSeqTxn if ApplyWal2TableJob raced
            // ahead. Fix the effective applied point from the reader and bound BOTH the
            // overlap walk and the forward scan to it, closing the apply-ahead hole the
            // head-hit guard also addresses.
            final long effectiveSeqTxn = reader.getSeqTxn();

            // Overlap trigger. The min-ts source is the base WAL-E event file, not
            // TransactionLogCursor.getTxnMinTimestamp() (V2-sequencer-only; throws on
            // the default V1). Reading WAL-E opens the per-segment event file (commit
            // metadata) but never the pre-dedup data columns.
            long batchMinTs = Numbers.LONG_NULL;
            try (TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn)) {
                while (txnCursor.hasNext()) {
                    final long txn = txnCursor.getTxn();
                    if (txn > effectiveSeqTxn) {
                        break;
                    }
                    final int walId = txnCursor.getWalId();
                    if (walId <= 0) {
                        // Compacted / structural entry (STRUCTURAL_CHANGE / DROP_TABLE):
                        // never a DATA commit, so it cannot dedup-replace a row.
                        continue;
                    }
                    final int segmentId = txnCursor.getSegmentId();
                    final int segmentTxn = txnCursor.getSegmentTxn();
                    walPath.of(engine.getConfiguration().getDbRoot())
                            .concat(baseToken)
                            .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                    final WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, walEventReader, segmentTxn, txn);
                    if (!WalTxnType.isDataType(eventCursor.getType())) {
                        // TRUNCATE / DROP PARTITION / UPDATE commit with walId>0 and a
                        // min ts, but no dedup-replaceable rows. Excluding them keeps a
                        // base row removal frozen (o3HeadMissReplay clamps to replayMinTs)
                        // rather than triggering a history-rewriting replay.
                        continue;
                    }
                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    long txnMinTs = dataInfo.getMinTimestamp();
                    // A REPLACE_RANGE commit deletes [rangeLo, rangeHi) beyond its own inserted
                    // rows, which may all sit above the frontier - or be absent entirely in a
                    // pure-delete commit whose min timestamp reads Long.MAX_VALUE. Its clamped
                    // range low is the commit's true overlap minimum. Mirrors drainBaseWal's O3
                    // detection; see effectiveReplaceRangeDeleteLo.
                    final long deleteLo = effectiveReplaceRangeDeleteLo(dataInfo, viewLowerBoundTimestamp);
                    if (deleteLo != Numbers.LONG_NULL) {
                        txnMinTs = deleteLo;
                    }
                    if (batchMinTs == Numbers.LONG_NULL || txnMinTs < batchMinTs) {
                        batchMinTs = txnMinTs;
                    }
                }
            }

            final long latestSeenTs = instance.getLatestSeenTs();
            // First cycle (latestSeenTs == LONG_NULL) never overlaps -> cheap full
            // build via the forward scan floored at viewLowerBoundTimestamp. A range
            // with no DATA commit (batchMinTs == LONG_NULL) has nothing to overlap.
            // Intra-commit out-of-order is deliberately NOT an overlap trigger here:
            // unlike drainBaseWal (which appends the raw stream in WAL row order, so an
            // OOO commit corrupts window state), the applied reader yields rows ts-sorted,
            // so an OOO commit entirely above the frontier appends correctly and an OOO
            // commit reaching at/below the frontier already has minTs <= latestSeenTs.
            final boolean overlap = latestSeenTs != Numbers.LONG_NULL
                    && batchMinTs != Numbers.LONG_NULL
                    && batchMinTs <= latestSeenTs;
            // Reshape the per-worker staging buffer to THIS view's output schema
            // before either branch touches it. stagingBuffer / stagingColumnTypes
            // are worker-wide fields carrying whatever view the worker last served,
            // and both branches stage through them: the overlap branch's o3Replay ->
            // rebuildInMemoryTier -> stageInMemoryWindowFromDisk reads this view's LV
            // disk columns but dispatches by stagingColumnTypes, so without this
            // up-front reshape it would read them through a different view's schema
            // and stamp the rebuilt slot with a matching disk seqTxn - corrupt rows
            // served under a passing read fence. Mirrors incrementalRefresh, which
            // calls ensureStagingAndTier up-front.
            final boolean populateTier = ensureStagingAndTier(instance, outMetadata, cursorTimestampIndex);
            if (overlap) {
                // Release the scan reader before o3Replay re-pins via its own
                // waitForApply (don't hold two base readers).
                reader.close();
                reader = null;
                // Drop any un-flushed lead before the rebuild. A view that just
                // flipped to dedup (ALTER ... DEDUP ENABLE) can still hold a RAM lead
                // built from the pre-dedup stream; o3Replay's rebuildInMemoryTier
                // rebuilds the tier as a pure disk subset, so the stale lead rows must
                // not be counted (mirrors finishLeadRefresh's o3 branch).
                instance.setLeadRowCount(0);
                o3Replay(instance, windowFactory, batchMinTs, baseToken, effectiveSeqTxn);
                // Coupled invariant: keep refreshedUpTo == lastProcessed so a later
                // ALTER DEDUP DISABLE flip back to the lead path resumes cleanly with
                // no stale un-flushed lead.
                instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
                return;
            }

            // Publish ahead of the forward append's commit. The overlap branch
            // above has handed every unsealing cycle to o3Replay, so what remains
            // is strictly forward and stays sealed at effectiveSeqTxn - the point
            // the watermarks advance to below. Ordered after the overlap decision,
            // not beside the effectiveSeqTxn read, so an overlap cycle never
            // publishes a membership its own retire is about to shrink.
            publishCheckpointRingOnAdvance(instance, effectiveSeqTxn);

            // Strictly-forward cheap append over the applied reader.
            // Inclusive lower bound: strictly above the frontier, floored at the view's
            // lower bound. On the first cycle latestSeenTs is LONG_NULL, so the floor
            // governs and the scan builds window state from empty. MAX has no strict
            // successor, so its forward range is empty rather than wrapping to MIN.
            final boolean emptyForwardRange = latestSeenTs == Long.MAX_VALUE;
            final long scanLowTs = latestSeenTs == Numbers.LONG_NULL
                    ? viewLowerBoundTimestamp
                    : emptyForwardRange
                      ? Long.MAX_VALUE
                      : Math.max(latestSeenTs + 1, viewLowerBoundTimestamp);

            final LiveViewSymbolCache symbolCache = populateTier ? instance.getInMemoryTier().getSymbolCache() : null;
            final boolean internSymbols = symbolCache != null && stagingSymbolColumnIndexes.size() > 0;

            long appendedRows = 0;
            long batchMaxTs = Numbers.LONG_NULL;
            long stagingMinTs = Numbers.LONG_NULL;
            long stagingMaxTs = Numbers.LONG_NULL;
            long lvAppliedSeqTxn = Numbers.LONG_NULL;
            boolean readerAttached = false;
            try (TableReader committedSymbolReader = internSymbols ? engine.getReader(instance.getLiveViewToken()) : null) {
                engine.detachReader(reader);
                executionContext.of(reader);
                readerAttached = true;
                if (internSymbols) {
                    // Re-anchor each SYMBOL column's next-new-id to the committed symbol
                    // count so a prior flush's advance moves new-id assignment past it.
                    for (int si = 0, sn = stagingSymbolColumnIndexes.size(); si < sn; si++) {
                        final int c = stagingSymbolColumnIndexes.getQuick(si);
                        symbolCache.anchor(c, committedSymbolReader.getSymbolMapReader(c).getSymbolCount());
                    }
                }
                try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                    final RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
                    try (RecordCursor pageCursor = emptyForwardRange
                            ? EmptyTableRecordCursor.INSTANCE
                            : pageFrameFactory.getCursorFromTimestamp(executionContext, scanLowTs)) {
                        RecordCursor source = pageCursor;
                        if (filter != null) {
                            filteringCursor.of(source, filter, executionContext);
                            source = filteringCursor;
                        }
                        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
                        if (anchorWindow != null) {
                            anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                            source = anchorDispatchingCursor;
                        }
                        try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                            final Record outRecord = windowCursor.getRecord();
                            while (windowCursor.hasNext()) {
                                // Accumulators advanced for this row; a failure before commit
                                // triggers a window-state rebuild (see handleRefreshFailure).
                                // The coupled forward scan feeds the same incremental cursor
                                // as drainBaseWal, so it raises the same flag. Belt and
                                // braces rather than a fix for a reachable bug: a partial
                                // feed leaves latestSeenTs at or above the pending range's
                                // min ts, so the retry's own overlap check already routes
                                // into o3Replay and recomputes. That recovery holds only
                                // while every LV is snapshot-capable (validateLiveViewWindowFunction
                                // rejects the rest at CREATE) - a non-capable view would take
                                // o3Replay's invalidateHeadOnO3 fallback, which force-advances
                                // the watermarks over rows this drain fed but never committed.
                                // Raising the flag keeps the invariant local to the drain that
                                // breaks it instead of resting on that gate.
                                windowStateDirty = true;
                                final long ts = outRecord.getTimestamp(cursorTimestampIndex);
                                if (batchMaxTs == Numbers.LONG_NULL || ts > batchMaxTs) {
                                    batchMaxTs = ts;
                                }
                                instance.setLatestSeenTs(ts);
                                final TableWriter.Row row = walWriter.newRow(ts);
                                copier.copy(executionContext, outRecord, row);
                                row.append();
                                if (populateTier) {
                                    stagingBuffer.copyRowFromRecord(outRecord, appendedRows);
                                    if (internSymbols) {
                                        // windowMapAuthoritative = !isLeadReconstruction(): the primary
                                        // resets the window map on every flush, so a live window entry is
                                        // authoritative and intern can skip the committed keyOf. A replica's
                                        // externally-flushed lead must stay committed-first.
                                        final boolean windowMapAuthoritative = !isLeadReconstruction();
                                        for (int si = 0, sn = stagingSymbolColumnIndexes.size(); si < sn; si++) {
                                            final int c = stagingSymbolColumnIndexes.getQuick(si);
                                            final int symId = symbolCache.intern(c, outRecord.getSymA(c), committedSymbolReader.getSymbolMapReader(c), windowMapAuthoritative);
                                            stagingBuffer.putInt(appendedRows, c, symId);
                                            if (symId == SymbolTable.VALUE_IS_NULL) {
                                                // The committed disk table may not know this NULL yet;
                                                // flag it so the read overlay reports containsNullValue().
                                                stagingBuffer.markSymbolNull(c);
                                            }
                                        }
                                    }
                                    if (stagingMinTs == Numbers.LONG_NULL) {
                                        stagingMinTs = ts;
                                    }
                                    if (stagingMaxTs == Numbers.LONG_NULL || ts > stagingMaxTs) {
                                        stagingMaxTs = ts;
                                    }
                                }
                                appendedRows++;
                            }
                        }
                        if (appendedRows > 0) {
                            fencedLiveViewCommit(() -> walWriter.commitLiveView(effectiveSeqTxn));
                        }
                    }
                }
            } finally {
                if (readerAttached) {
                    executionContext.clearReader();
                    engine.attachReader(reader);
                }
            }
            // The forward scan is done with the reader; close it before apply so the
            // base holds no lingering snapshot. The outer finally then no-ops.
            reader.close();
            reader = null;

            if (appendedRows > 0 && populateTier) {
                stagingBuffer.setRowCount(appendedRows);
                stagingBuffer.setSeamTs(stagingMinTs);
            }

            // Advance watermarks to the effective applied point (mirrors
            // o3HeadMissReplay): the scan covered every DATA seqTxn up to
            // effectiveSeqTxn, including any past toSeqTxn apply raced into.
            instance.setLastProcessedSeqTxn(effectiveSeqTxn);
            instance.setAppliedWatermark(effectiveSeqTxn);
            // Coupled invariant: no un-flushed lead.
            instance.setRefreshedUpToSeqTxn(effectiveSeqTxn);
            instance.setLeadRowCount(0);

            boolean lvConsumedPersisted = false;
            if (appendedRows > 0) {
                applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
                lvAppliedSeqTxn = engine.getTableSequencerAPI()
                        .getTxnTracker(instance.getLiveViewToken())
                        .getWriterTxn();
            }
            try {
                engine.advanceLiveViewConsumedSeqTxn(instance.getLiveViewToken(), effectiveSeqTxn, blockFileWriter, path);
                lvConsumedPersisted = true;
            } catch (CairoException e) {
                LOG.critical().$("could not advance live view consumed seqTxn after dedup forward append [view=")
                        .$(viewName)
                        .$(", effectiveSeqTxn=").$(effectiveSeqTxn)
                        .$(", error=").$safe(e.getFlyweightMessage()).I$();
            }
            if (!lvConsumedPersisted) {
                persistState(instance);
            }
            if (lvConsumedPersisted && populateTier && appendedRows > 0) {
                // Publish the just-applied rows into the tier as a subset of disk
                // (leadRowCount = 0). This disk-subset publish is the tier's only feed
                // for a dedup base (it has no un-flushed lead), so it is load-bearing.
                publishToInMemoryTier(instance, stagingMaxTs, lvAppliedSeqTxn, appendedRows, false);
            }
            if (lvConsumedPersisted && appendedRows > 0) {
                maybeWriteHeadCheckpoint(instance, windowFactory, effectiveSeqTxn, batchMaxTs, appendedRows, false);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    /**
     * Walks the base sequencer log forward over {@code (fromSeqTxn, toSeqTxn]} and
     * runs each in-order DATA commit through the compiled window cursor, mirroring
     * every output row into the worker-local staging buffer (when the tier is
     * populated) and, when {@code walWriter} is non-null, into the LV's WAL via
     * {@code copier}. The lead refresh passes a null {@code walWriter} (tier only,
     * no WAL); the disk-subset cycle passes a real one. Out-of-order arrival rolls
     * back any WAL writes, restores the latestSeenTs watermark, and stops the walk
     * so the caller can hand off to o3Replay. Results land in {@link #drainResult},
     * which the caller resets before the call.
     */
    private void drainBaseWal(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            TableToken baseToken,
            RecordMetadata baseMetadata,
            int baseTimestampIndex,
            int cursorTimestampIndex,
            long viewLowerBoundTimestamp,
            long viewUpperBoundTimestamp,
            Function filter,
            long fromSeqTxn,
            long toSeqTxn,
            WalWriter walWriter,
            RecordToRowCopier copier,
            boolean populateTier,
            long latestSeenTsSnapshot
    ) throws SqlException {
        long advanceTo = -1;
        long appendedRows = 0;
        long batchMaxTs = Numbers.LONG_NULL;
        // In-order rows the lower-bound cursor dropped this drain for falling
        // below viewLowerBoundTimestamp. Tallied per commit off the reused
        // tsLowerBoundCursor, folded into below_lower_bound_count after the walk.
        long belowLowerBoundSkipped = 0;
        // Base rows the skip-prefix cursor physically visited this walk. Counts VISITS (work),
        // not DROPS: a wholly sub-floor commit is dropped in O(1) above and contributes nothing,
        // a straddling commit contributes only its sub-floor prefix. Folded into
        // lowerBoundRowsScanned after the walk so a test can prove the O(1) skip.
        long lowerBoundRowsScanned = 0;
        boolean o3Detected = false;
        long o3LateRowTs = Numbers.LONG_NULL;
        long o3SeqTxn = Numbers.LONG_NULL;
        long stagingMaxTs = Numbers.LONG_NULL;
        long stagingMinTs = Numbers.LONG_NULL;

        final int turnMaxCommits = engine.getConfiguration().getLiveViewRefreshTurnMaxCommits();
        final long turnMaxDurationUs = engine.getConfiguration().getLiveViewRefreshTurnMaxDurationMicros();

        // Eager SYMBOL interning. When the tier holds SYMBOL columns, each output
        // row's symbol string is interned into the LV table's id space so the
        // un-flushed lead carries LV-table-consistent ids the read path resolves
        // from RAM. A committed value resolves to its existing id via the LV
        // table's symbol map (committedSymbolReader); a value new to the lead is
        // assigned the next id at or above the committed count, matching the id the
        // flush's apply will produce (in-order leads only - O3 is diverted to
        // o3Replay). The reader reflects the applied state, which is stable for the
        // duration of the drain (no apply runs on the lead path). Closed with the
        // txnCursor via the resource list (no-op when null).
        final LiveViewSymbolCache symbolCache = populateTier ? instance.getInMemoryTier().getSymbolCache() : null;
        final boolean internSymbols = symbolCache != null && stagingSymbolColumnIndexes.size() > 0;
        try (
                TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn);
                TableReader committedSymbolReader = internSymbols ? engine.getReader(instance.getLiveViewToken()) : null
        ) {
            if (internSymbols) {
                // Re-anchor each SYMBOL column's next-new-id to the committed symbol
                // count, so a flush (or O3) that advanced the count moves new-id
                // assignment past it while a within-window advance is preserved.
                for (int si = 0, sn = stagingSymbolColumnIndexes.size(); si < sn; si++) {
                    final int c = stagingSymbolColumnIndexes.getQuick(si);
                    symbolCache.anchor(c, committedSymbolReader.getSymbolMapReader(c).getSymbolCount());
                }
            }
            while (txnCursor.hasNext()) {
                long txn = txnCursor.getTxn();
                if (txn > toSeqTxn) {
                    break;
                }
                // Per-turn budget yield. Always make at least one commit
                // per turn so a slow first commit cannot starve forever;
                // the duration check therefore gates on
                // turnCommitsProcessed > 0. Yields land at the per-base-
                // seqTxn boundary - never mid-row. The next worker tick
                // resumes from advanceTo + 1.
                if (turnCommitsProcessed > 0
                        && (turnCommitsProcessed >= turnMaxCommits
                        || engine.getConfiguration().getMicrosecondClock().getTicks() - turnStartUs >= turnMaxDurationUs)) {
                    break;
                }
                advanceTo = txn;
                turnCommitsProcessed++;
                int walId = txnCursor.getWalId();
                int segmentId = txnCursor.getSegmentId();
                int segmentTxn = txnCursor.getSegmentTxn();

                if (walId <= 0) {
                    // Compacted seq entry / non-WAL: skip past, no data to consume.
                    continue;
                }

                walPath.of(engine.getConfiguration().getDbRoot())
                        .concat(baseToken)
                        .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, walEventReader, segmentTxn, txn);

                if (!WalTxnType.isDataType(eventCursor.getType())) {
                    if (eventCursor.getType() == WalTxnType.TRUNCATE && baseToken.isMatView()) {
                        // A mat view's TRUNCATE is never data retirement - it is the rebuild half
                        // of a full refresh, and the commits above it re-materialise rows this view
                        // already consumed. ApplyWal2TableJob's TRUNCATE arm invalidates the view
                        // for exactly that reason, but it is a different worker: this drain walks
                        // the base's raw sequencer log with no apply gate (unlike drainAppliedBase),
                        // so walking past the TRUNCATE here would emit those rows a second time -
                        // over accumulators still holding pre-rebuild state - while the view still
                        // reports ACTIVE, all before the apply job ever reaches this seqTxn.
                        //
                        // Stop the walk instead of skipping the commit. Rows drained from commits
                        // BELOW the TRUNCATE stay valid and are committed by the normal exit path;
                        // lastProcessedSeqTxn simply never crosses it. The view keeps serving its
                        // pre-rebuild rows until the apply job applies this same seqTxn and
                        // invalidates it. Until then each cycle re-drains to here and stops, which
                        // is the same benign no-progress hold the apply-lag gate takes.
                        break;
                    }
                    // Non-data commit (schema change / DROP PARTITION / TRUNCATE / TTL) —
                    // walked past, no rewrite to the in-memory tier or LV WAL. Schema
                    // changes that touch referenced columns invalidate via
                    // ApplyWal2TableJob.
                    continue;
                }

                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                // Out-of-order detection, two triggers, both handed to
                // o3Replay by the caller (which re-feeds base data in ts order via
                // a sorted TableReader and emits a single REPLACE_RANGE commit
                // from viewLowerBoundTimestamp (head-miss) or the head's
                // maxTimestamp (head-hit, follow-up commit) forward):
                //   - cross-commit: this commit's min ts sits below the LV's
                //     latestSeenTs watermark, so it lands before rows the LV
                //     already processed.
                //   - intra-commit: the commit's own rows are not in
                //     ts-ascending order. Raw WAL segments are unsorted, so
                //     DataInfo.isOutOfOrder() is set whenever a row lands
                //     below a preceding row in the same commit. Processing
                //     such a commit in WAL row order corrupts window state
                //     even when the whole commit sits above the watermark.
                // Either way, discard any rows queued earlier in this cycle,
                // break out of the loop, and hand off to o3Replay.
                final long latestSeen = instance.getLatestSeenTs();
                final long txnMinTs = dataInfo.getMinTimestamp();
                boolean crossCommitO3 = latestSeen != Numbers.LONG_NULL && txnMinTs < latestSeen;
                long o3TriggerTs = txnMinTs;
                // A REPLACE_RANGE data commit atomically deletes every base row in
                // [rangeLo, rangeHi) and inserts the commit's rows, all inside the
                // range (WalWriter and TableWriter both assert this). The raw WAL
                // carries only the inserted rows, so the deletion side is visible
                // solely through the commit's range metadata: a range reaching at
                // or below the frontier may have deleted rows the view already
                // emitted even when every inserted row sits above the frontier, or
                // when the commit carries no rows at all (a pure delete). Treat the
                // clamped range low as the commit's effective minimum, and compare
                // non-strictly: a range starting exactly at the frontier deletes the
                // frontier row itself. The o3Replay hand-off then re-reads the applied
                // (post-replace) base and rewrites the affected range, converging the
                // view instead of keeping ghost rows the base no longer holds. The
                // clamped range low also serves as the replay's lateRowTs so the
                // head-miss REPLACE_RANGE covers a deleted band even when the
                // recompute produces no output row at its bottom.
                final long deleteLo = effectiveReplaceRangeDeleteLo(dataInfo, viewLowerBoundTimestamp);
                if (deleteLo != Numbers.LONG_NULL) {
                    o3TriggerTs = deleteLo;
                    crossCommitO3 |= latestSeen != Numbers.LONG_NULL && deleteLo <= latestSeen;
                }
                if (crossCommitO3 || dataInfo.isOutOfOrder()) {
                    if (walWriter != null) {
                        walWriter.rollback();
                    }
                    // Roll back the in-cycle latestSeenTs bumps along with
                    // the WAL writes. The replay path re-stamps the
                    // watermark from the re-fed rows; without this restore
                    // a follow-up in-order commit at the inflated ts would
                    // be misclassified as O3.
                    instance.forceSetLatestSeenTs(latestSeenTsSnapshot);
                    o3Detected = true;
                    o3LateRowTs = o3TriggerTs;
                    o3SeqTxn = txn;
                    // Reset cycle-local accounting so the caller's post-loop
                    // branch does not see stale state (it diverts to o3Replay,
                    // but the explicit reset keeps the invariants narrow).
                    appendedRows = 0;
                    batchMaxTs = Numbers.LONG_NULL;
                    stagingMinTs = Numbers.LONG_NULL;
                    stagingMaxTs = Numbers.LONG_NULL;
                    // Count late rows that fall entirely below the view's
                    // lower bound as O3 rejections (surfaced via
                    // live_views().o3_rejected_count). The common case - the
                    // whole offending commit sits below the bound - is exact;
                    // a commit straddling the bound (minTs < bound <= maxTs)
                    // is not counted here, an accepted V1 under-count on a
                    // rare path. These rows are dropped by the replay's lower-
                    // bound cursor and never reach the on-disk tier.
                    if (dataInfo.getMaxTimestamp() < viewLowerBoundTimestamp) {
                        instance.bumpO3RejectedCount(dataInfo.getEndRowID() - dataInfo.getStartRowID());
                    }
                    break;
                }
                long startRow = dataInfo.getStartRowID();
                long endRow = dataInfo.getEndRowID();
                if (endRow <= startRow) {
                    continue;
                }
                // Fully sub-floor in-order commit: every row sits below the view's lower bound, so
                // the window would see nothing and TimestampLowerBoundCursor would linearly skip the
                // whole commit. dataInfo already carries the commit's max ts (event-file metadata,
                // independent of the column projection, so reliable even under schema drift), so drop
                // it in O(1) here - mirroring the O3 branch's sub-floor short-circuit above - instead
                // of opening the frame and walking every row. No row would pass the bound, so
                // setLatestSeenTs, the tier staging and the LV WAL writes are all no-ops for such a
                // commit anyway; only the below-lower-bound drop tally needs its rows, taken from the
                // frame row range. lowerBoundRowsScanned is deliberately NOT bumped: nothing is visited.
                if (dataInfo.getMaxTimestamp() < viewLowerBoundTimestamp) {
                    belowLowerBoundSkipped += endRow - startRow;
                    continue;
                }

                walNameSink.clear();
                walNameSink.put(WAL_NAME_BASE).put(walId);
                // walFrameCursor.of throws TableReferenceOutOfDateException when the
                // segment's schema has drifted from the compiled projection - a
                // referenced base column retyped/dropped/renamed by a structural commit
                // this raw-WAL drain walked past (the non-data continue above) but which
                // ApplyWal2TableJob has not applied yet, since the sequencer notifies
                // live views at COMMIT time. Reading the segment through the stale
                // columnSizeShifts strides would be an OOB native read (narrowing /
                // fixed<->var) or wrong values (widening). Let it propagate to the
                // refresh worker's recompile-and-recover path (recoverFromBaseMetadataDrift).
                walFrameCursor.of(
                        baseToken,
                        walNameSink,
                        segmentId,
                        endRow,
                        startRow,
                        endRow,
                        baseMetadata,
                        columnIndexes,
                        columnSizeShifts,
                        dataInfo
                );
                walRecordCursor.of(walFrameCursor, baseMetadata);

                // Drop rows below the view's lower bound before the window
                // sees them, matching the O3 head-miss replay's lower-bound
                // seed so both paths agree on sub-floor rows. This commit is
                // not out of order (intra/cross-commit O3 was diverted to the
                // replay above), so its rows are ts-ascending and the
                // skip-prefix cursor drops exactly the sub-floor prefix.
                tsLowerBoundCursor.of(walRecordCursor, baseTimestampIndex, viewLowerBoundTimestamp);
                RecordCursor source = tsLowerBoundCursor;
                if (viewUpperBoundTimestamp != Long.MAX_VALUE) {
                    tsUpperBoundCursor.of(source, baseTimestampIndex, viewUpperBoundTimestamp);
                    source = tsUpperBoundCursor;
                }
                if (filter != null) {
                    filteringCursor.of(source, filter, executionContext);
                    source = filteringCursor;
                }
                LiveViewWindow anchorWindow = instance.getAnchorWindow();
                if (anchorWindow != null) {
                    // Anchor dispatch sits between the filter (or lower-bound
                    // cursor) and the window cursor so window functions see
                    // resetPartition before pass1 evaluates the row.
                    anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                    source = anchorDispatchingCursor;
                }

                RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext);
                try {
                    Record outRecord = windowCursor.getRecord();
                    while (windowCursor.hasNext()) {
                        // Accumulators advanced for this row; a failure before commit
                        // triggers a window-state rebuild (see handleRefreshFailure).
                        windowStateDirty = true;
                        long ts = outRecord.getTimestamp(cursorTimestampIndex);
                        if (batchMaxTs == Numbers.LONG_NULL || ts > batchMaxTs) {
                            batchMaxTs = ts;
                        }
                        // Drive the O3 detection watermark from the post-
                        // window row loop so every LV - anchored or not -
                        // contributes. Monotonic clamp inside setLatestSeenTs
                        // guarantees the next O3 row cannot retroactively
                        // lower the watermark.
                        instance.setLatestSeenTs(ts);
                        if (walWriter != null) {
                            TableWriter.Row row = walWriter.newRow(ts);
                            copier.copy(executionContext, outRecord, row);
                            row.append();
                        }
                        if (populateTier) {
                            // Mirror the row into the worker-local staging buffer.
                            // The lead refresh publishes it into the tier as the
                            // un-flushed lead; the disk-subset cycle publishes it
                            // after apply as a subset of disk.
                            stagingBuffer.copyRowFromRecord(outRecord, appendedRows);
                            if (internSymbols) {
                                // Overwrite the segment-local symbol ids
                                // copyRowFromRecord stored with eager-interned,
                                // LV-table-consistent ids so the lead resolves from
                                // RAM and post-flush agrees with disk.
                                // windowMapAuthoritative = !isLeadReconstruction(): only the primary's
                                // reset-on-flush window map lets intern skip the committed keyOf; a
                                // replica's externally-flushed lead must stay committed-first.
                                final boolean windowMapAuthoritative = !isLeadReconstruction();
                                for (int si = 0, sn = stagingSymbolColumnIndexes.size(); si < sn; si++) {
                                    final int c = stagingSymbolColumnIndexes.getQuick(si);
                                    final int symId = symbolCache.intern(c, outRecord.getSymA(c), committedSymbolReader.getSymbolMapReader(c), windowMapAuthoritative);
                                    stagingBuffer.putInt(appendedRows, c, symId);
                                    if (symId == SymbolTable.VALUE_IS_NULL) {
                                        // The committed disk table may not know this NULL yet;
                                        // flag it so the read overlay reports containsNullValue().
                                        stagingBuffer.markSymbolNull(c);
                                    }
                                }
                            }
                            if (stagingMinTs == Numbers.LONG_NULL) {
                                stagingMinTs = ts;
                            }
                            if (stagingMaxTs == Numbers.LONG_NULL || ts > stagingMaxTs) {
                                stagingMaxTs = ts;
                            }
                        }
                        appendedRows++;
                    }
                    // Tally the sub-floor prefix this in-order commit dropped.
                    // Read before windowCursor.close(), which frees (and resets)
                    // the lower-bound cursor. The O3 path counts its own drops via
                    // bumpO3RejectedCount and breaks before reaching the cursor,
                    // so the two accounts never overlap.
                    final long visited = tsLowerBoundCursor.getSkippedCount();
                    belowLowerBoundSkipped += visited;
                    // These rows were physically visited by the cursor's skip loop (a straddling
                    // commit's sub-floor prefix). A wholly sub-floor commit never reaches here - it
                    // was dropped in O(1) above - so it does not inflate this visit count.
                    lowerBoundRowsScanned += visited;
                } finally {
                    windowCursor.close();
                }
            }
        }

        if (lowerBoundRowsScanned > 0) {
            instance.bumpLowerBoundRowsScanned(lowerBoundRowsScanned);
        }
        if (belowLowerBoundSkipped > 0) {
            instance.bumpBelowLowerBoundCount(belowLowerBoundSkipped);
            if (!instance.hasWarnedBelowLowerBoundDrop()) {
                // Advisory, once per process per view: a silent 100%-drop of
                // back-dated in-order data reads as a healthy view (active,
                // lag 0, no rejections), so surface the boundary once to point
                // the operator at a lower START FROM.
                instance.setWarnedBelowLowerBoundDrop();
                LOG.advisory().$("live view is dropping in-order rows below its START FROM boundary [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", bound=").$ts(
                                ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType()),
                                viewLowerBoundTimestamp
                        )
                        .$(", dropped=").$(belowLowerBoundSkipped)
                        .$("]; recreate the view with an earlier START FROM to include this data").$();
            }
        }

        if (appendedRows > 0 && populateTier) {
            stagingBuffer.setRowCount(appendedRows);
            stagingBuffer.setSeamTs(stagingMinTs);
        }

        drainResult.advanceTo = advanceTo;
        drainResult.appendedRows = appendedRows;
        drainResult.batchMaxTs = batchMaxTs;
        drainResult.o3Detected = o3Detected;
        drainResult.o3LateRowTs = o3LateRowTs;
        drainResult.o3SeqTxn = o3SeqTxn;
        drainResult.stagingMaxTs = stagingMaxTs;
        drainResult.stagingMinTs = stagingMinTs;
    }

    /**
     * The timestamp at or above which a REPLACE_RANGE commit can have removed a row the live view
     * emitted, or {@link Numbers#LONG_NULL} when it cannot have removed one. Both drains use it as
     * the commit's effective minimum timestamp (its inserted rows may all sit above the frontier,
     * or be absent entirely), and the replay uses it as the REPLACE_RANGE low boundary. The
     * enterprise replica's applied-base lead drain shares it too, so a read-only replica raises its
     * O3 hatch on exactly the commits the primary replays -- hence {@code protected}.
     * <p>
     * The range low is clamped up to the view's START FROM boundary: the view holds no row below
     * it, so a deletion down there removes nothing of the view's. That clamp is what keeps the
     * overlap trigger and the replay in the same coordinate space as the seed and the forward
     * drain - every path bottoms out at {@code viewLowerBoundTimestamp}.
     * <p>
     * The clamp cannot yield LONG_NULL, which the drains and the replay both read as "no trigger
     * timestamp" (the O3 detection would then miss the deletion outright, stranding the deleted
     * band's derived rows on disk as ghosts). It collides only for a BEGINNING view - whose
     * boundary IS LONG_NULL - taking a commit whose range low is Long.MIN_VALUE. No OSS producer
     * emits such a commit today (a mat view derives its replace range from real data timestamps),
     * but {@link WalWriter#commitWithParams} accepts any long, so pin the result at the lowest
     * non-null timestamp instead of resting on that. LONG_NULL is not a legal designated
     * timestamp, so its successor still sits at or below every row the base can hold.
     */
    protected static long effectiveReplaceRangeDeleteLo(WalEventCursor.DataInfo dataInfo, long viewLowerBoundTimestamp) {
        if (dataInfo.getDedupMode() != WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE) {
            return Numbers.LONG_NULL;
        }
        final long deleteLo = Math.max(dataInfo.getReplaceRangeTsLow(), viewLowerBoundTimestamp);
        if (deleteLo >= dataInfo.getReplaceRangeTsHi()) {
            // The range lies entirely below the view's boundary (or is empty): nothing the view
            // holds can have been deleted, so the commit's own row minimum stands.
            return Numbers.LONG_NULL;
        }
        return Math.max(deleteLo, Numbers.LONG_NULL + 1);
    }

    // The refresh job runs on a worker pool an in-place primary-to-replica demote never halts: it
    // acquires the LV WalWriter while PRIMARY (getWalWriter's eager read-only check passes), pumps the
    // window, then externalizes a replicated LV seqTxn with no in-lock read-only re-check between the
    // acquire and the commit. A demote flips the read-only flag at the front of the cascade
    // (prepareForRoleSwitch) but tears the uploader down only later, so a commit that lands in that window
    // mints a local-only LV seqTxn the closing uploader never ships -- the new primary never sees it, and
    // the ex-primary's on-disk tier / _lv.s advances past what replicated (silent loss). Route every LV
    // commit family (flushLead, the in-WAL-order and applied-base drains, the o3Replay REPLACE_RANGE
    // corrections, and the seed sweep) through this fence: hold the role-switch READ lock across an
    // authoritative in-lock isReadOnlyMode() re-check and the commit, so the mint is atomic against the
    // role flip. Either the flip ran first (refuse -- the commit throws the read-only authorization error,
    // which handleRefreshFailure treats as retry-later, never invalidate; a live view is derived state so
    // the new primary recomputes the lead forward) or the mint lands fully as PRIMARY while the flip's
    // WRITE acquire waits for this read hold and replicates. This fences the WAL externalization only; the
    // in-mem tier publish, the inline apply and the _lv.s watermark advance are local recovery state the
    // demote can safely leave behind. Mirrors MatViewRefreshJob.fencedMatViewCommit. A strict no-op for
    // non-replicating deployments: the read lock is uncontended and the read-only flag is static.
    private void fencedLiveViewCommit(Runnable commit) {
        if (engine.isReadOnlyMode()) {
            throw CairoException.authorization().put(CairoException.READ_ONLY_ACCESS_MESSAGE);
        }
        final Lock lock = engine.getRoleSwitchReadLock();
        lock.lock();
        try {
            if (engine.isReadOnlyMode()) {
                throw CairoException.authorization().put(CairoException.READ_ONLY_ACCESS_MESSAGE);
            }
            engine.fireRoleSwitchMintObserver();
            commit.run();
            // Rows are durable now, so the accumulators no longer lead durable state;
            // a later failure must not trigger a rebuild over the committed block.
            windowStateDirty = false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Post-drain step for a lead refresh: publishes the just-drained staging rows
     * into the in-mem tier as the un-flushed lead (no commit, no apply), advancing
     * the in-RAM refresh cursor. On out-of-order arrival it discards the lead and
     * hands off to o3Replay (which recomputes from base and rebuilds the tier from
     * the rewritten disk). When both tier slots are reader-pinned the lead cannot
     * enter RAM, so it falls back to flushing everything straight to disk so no row
     * is lost.
     */
    private void finishLeadRefresh(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            TableToken baseToken,
            boolean populateTier
    ) throws SqlException {
        final long advanceTo = drainResult.advanceTo;
        final long appendedRows = drainResult.appendedRows;
        final long stagingMaxTs = drainResult.stagingMaxTs;

        if (drainResult.o3Detected) {
            // The escape hatches below (o3Replay, emergency flush) both rewrite the LV's on-disk tier.
            // A read-only replica must never do that -- the primary owns those writes and replicates
            // the result -- so it overrides onLeadO3Detected to reset the window state to cold-start
            // and serve disk-only until the primary's replicated O3 correction lands. The primary
            // default returns false and rewrites disk via o3Replay below.
            if (onLeadO3Detected(instance, windowFactory, baseToken, advanceTo)) {
                return;
            }
            // Gate on base apply BEFORE discarding the lead. o3Replay reads the
            // applied base at o3SeqTxn; if ApplyWal2TableJob has not caught up,
            // ensureBaseApplied unwinds cooperatively with the lead still published
            // (no reader-visible shrink) and the next fallback tick retries once
            // apply lands. Block-spinning in the downstream waitForApply instead
            // would starve this worker and deadlock the single-threaded drain.
            ensureBaseApplied(baseToken, drainResult.o3SeqTxn);
            // Discard the in-RAM lead and recompute. o3Replay re-feeds base data
            // in ts order (the lead's base rows are retained because
            // lvConsumedSeqTxn only advances at flush), rewrites disk, and rebuilds
            // the tier from the rewritten disk as a pure subset. After it the
            // applied point covers the offending seqTxn, so resume the lead there.
            instance.setLeadRowCount(0);
            o3Replay(instance, windowFactory, drainResult.o3LateRowTs, baseToken, drainResult.o3SeqTxn);
            instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
            return;
        }

        if (advanceTo > instance.getRefreshedUpToSeqTxn()) {
            if (appendedRows > 0 && populateTier) {
                if (instance.isTierStale() && !isLeadReconstruction()) {
                    // The tier is stale: a prior both-slots-pinned O3 rebuild-skip
                    // (or an emergency flush) left the published slot inconsistent
                    // with the re-sequenced disk, and disk now holds every row up to
                    // the frontier. Publishing this cycle's lead through the
                    // dropRetained path would rebuild a pure-lead slot seamed at the
                    // lead's minimum timestamp; a disk row at exactly that timestamp
                    // (an additive same-ts row at the frontier - not diverted to O3,
                    // whose trigger is a strict below-frontier compare) would then be
                    // served by neither disk (the scan stops strictly below the seam)
                    // nor the slot (which holds only the lead) - silent row loss plus
                    // a size() overcount that breaks LIMIT. Flush this cycle's lead
                    // straight to disk and rebuild the tier as a clean disk subset
                    // instead: the seam lands at the IN MEMORY window's lower edge
                    // with the overlap present, and rebuildInMemoryTier clears the
                    // stale marking (or defers to the next cycle if both slots stay
                    // pinned).
                    //
                    // Gated to the primary via !isLeadReconstruction(): a read-only
                    // replica ALSO sets tierStale (its reconcileLeadWithDisk arms it on
                    // a Case B / cold-start reconcile to force a clean tier rebuild), but
                    // it must never flushLead - that opens a WalWriter on a read-only
                    // node. The replica instead falls through to publishToInMemoryTier's
                    // dropRetained path, which resets the slot and rebuilds the pure lead
                    // from this cycle's re-derived staging in RAM. It has no additive-
                    // same-ts gap to fix: its reconcile seam already drops the on-disk
                    // durable band (ts <= diskMaxTs) from staging, so the rebuilt slot is
                    // seamed strictly above disk and every durable row is served by disk.
                    // instance.leadRowCount is 0 here (both tierStale setters zero it), so
                    // flushLead materialises exactly this cycle's staging rows, not the
                    // stale slot rows. Pin that invariant explicitly rather than trusting
                    // upstream bookkeeping: the o3Replay non-capable resync only re-arms
                    // leadRowCount from a non-stale slot, but a from-scratch setLeadRowCount(0)
                    // here also keeps flushRows / lvRowPosition accounting exact against any
                    // future path that could leave a stale non-zero count while tierStale.
                    instance.setLeadRowCount(0);
                    flushLead(instance, windowFactory, advanceTo, appendedRows);
                    rebuildInMemoryTier(instance);
                    instance.setRefreshedUpToSeqTxn(advanceTo);
                    return;
                }
                // Stamp the slot with the last-flushed LV-table seqTxn (= disk's
                // current version, since nothing has applied since the last flush):
                // the overlap agrees with disk row-for-row and the lead sits on
                // top. A later flush re-stamps the slot once the lead lands on disk.
                long lastFlushedLvSeqTxn = engine.getTableSequencerAPI()
                        .getTxnTracker(instance.getLiveViewToken())
                        .getWriterTxn();
                boolean published;
                try {
                    published = publishToInMemoryTier(instance, stagingMaxTs, lastFlushedLvSeqTxn, appendedRows, true);
                } catch (Throwable t) {
                    // A publish error (e.g. a copy/swap failure mid-publish) left
                    // the lead out of the tier. The publish's own catch already
                    // released the writer sentinel, so the published slot is intact.
                    // Recover by flushing the lead straight to disk - otherwise a
                    // retry would re-drain and double-advance the window functions.
                    LOG.error().$("live view lead publish failed, flushing the lead to disk [view=")
                            .$(instance.getDefinition().getViewName())
                            .$(", error=").$(t).I$();
                    published = false;
                }
                if (!published) {
                    // A read-only replica cannot flush the stalled lead to disk. It overrides
                    // onLeadPublishStalled to roll the window state back to its pre-drain snapshot and
                    // arm a retry back-off, leaving refreshedUpToSeqTxn where it was so the next scan
                    // tick re-drains this exact range once a reader releases a slot; reads fall back to
                    // disk-only via the seqTxn fence meanwhile. The primary default returns false and
                    // flushes the stall straight to disk below.
                    if (onLeadPublishStalled(instance, windowFactory, advanceTo, appendedRows)) {
                        return;
                    }
                    // The lead could not enter RAM (both slots reader-pinned, or a
                    // publish error). Flush everything (the prior tier lead plus
                    // this batch's staging rows) straight to disk so no row is lost;
                    // the published slot is left stale and marked for rebuild, so
                    // the next refresh drops it and rebuilds a clean slot while disk
                    // (now current) serves reads in the meantime.
                    flushLead(instance, windowFactory, advanceTo, appendedRows);
                    instance.setRefreshedUpToSeqTxn(advanceTo);
                    return;
                }
            }
            instance.setRefreshedUpToSeqTxn(advanceTo);
        }
    }

    /**
     * Flushes the un-flushed lead to the LV's on-disk tier: materialises the
     * tier's lead rows (and, for an emergency flush, {@code stagingRowsToInclude}
     * un-published staging rows) into a fresh {@code WalWriter} via the compiled
     * copier, commits, applies inline, advances the applied / consumed watermarks,
     * re-stamps the published slot as a subset of disk, and writes the head
     * checkpoint. Runs on the FLUSH EVERY cadence (with {@code stagingRowsToInclude
     * == 0}); the emergency path passes the un-published staging count when the
     * tier publish stalled. When there are no output rows to flush (only non-data
     * or filtered base commits were drained) it still advances the watermarks so
     * base WAL retention releases.
     */
    private void flushLead(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long advanceTo,
            long stagingRowsToInclude
    ) throws SqlException {
        final TableToken token = instance.getLiveViewToken();
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        final long priorLead = instance.getLeadRowCount();
        final long flushRows = priorLead + stagingRowsToInclude;
        // Publish ahead of both branches below - the no-row one walks the floor
        // to advanceTo with no commit at all, the materialising one commits there
        // - and one call covers both because they advance to the same point. The
        // lead is in-order by construction: finishLeadRefresh hands every O3 cycle
        // to o3Replay before a flush can see it. This is the lead path's only
        // in-order publication; its drain half never advances the durable floor.
        publishCheckpointRingOnAdvance(instance, advanceTo);
        if (flushRows == 0 || tier == null) {
            // Nothing to materialise (only non-data / filtered base commits walked,
            // or no tier). Advance the watermarks anyway so base WAL retention
            // releases the consumed segments.
            instance.setLastProcessedSeqTxn(advanceTo);
            instance.setAppliedWatermark(advanceTo);
            try {
                engine.advanceLiveViewConsumedSeqTxn(token, advanceTo, blockFileWriter, path);
            } catch (CairoException e) {
                LOG.critical().$("could not advance live view consumed seqTxn on no-row flush [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", advanceTo=").$(advanceTo)
                        .$(", error=").$safe(e.getFlyweightMessage()).I$();
                persistState(instance);
            }
            instance.setLeadRowCount(0);
            return;
        }

        final RecordMetadata outMetadata = windowFactory.getMetadata();
        final int tsColIdx = outMetadata.getTimestampIndex();
        final int publishedIdx = tier.getPublishedIdx();
        final LiveViewInMemoryBuffer pubSlot = tier.getSlot(publishedIdx);
        final long overlapCount = pubSlot.rowCount() - priorLead;
        long flushedMaxTs = Numbers.LONG_NULL;
        // For a SYMBOL output schema the copier reads each lead row's symbol as a
        // string (getSymA) before re-interning it into the WAL, so the stored
        // LV-table-consistent id must resolve back to its string. symbolReader (the
        // pre-flush committed symbol map) plus the tier's symbol cache form the
        // overlay that does this; bufferRecord delegates getSymA to it. Closed with
        // the WalWriter (no-op when the schema has no SYMBOL column).
        final boolean hasSymbols = tier.getSymbolCache().hasSymbolColumns();
        try (WalWriter walWriter = engine.getWalWriter(token);
             TableReader symbolReader = hasSymbols ? engine.getReader(token) : null) {
            if (hasSymbols) {
                bufferRecord.setSymbolResolvers(buildFlushSymbolResolvers(outMetadata, symbolReader, tier.getSymbolCache()));
            }
            RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
            int lvTimestampIndex = walWriter.getMetadata().getTimestampIndex();
            if (lvTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(instance.getDefinition().getViewName()).put(']');
            }
            // Materialise the tier's lead rows (those above the overlap) into the
            // LV WAL via the compiled copier, reading them out of the pinned slot
            // through the buffer-record flyweight. The designated timestamp is set
            // by newRow; the copier copies the remaining columns.
            for (long r = overlapCount, rn = pubSlot.rowCount(); r < rn; r++) {
                long ts = pubSlot.getLong(r, tsColIdx);
                TableWriter.Row row = walWriter.newRow(ts);
                bufferRecord.of(pubSlot, r);
                copier.copy(executionContext, bufferRecord, row);
                row.append();
                flushedMaxTs = ts;
            }
            // Emergency flush: also materialise the staging rows the tier publish
            // could not absorb (both slots reader-pinned). They sit above the lead
            // in ts order.
            for (long r = 0; r < stagingRowsToInclude; r++) {
                long ts = stagingBuffer.getLong(r, tsColIdx);
                TableWriter.Row row = walWriter.newRow(ts);
                bufferRecord.of(stagingBuffer, r);
                copier.copy(executionContext, bufferRecord, row);
                row.append();
                flushedMaxTs = ts;
            }
            fencedLiveViewCommit(() -> walWriter.commitLiveView(advanceTo));
        } finally {
            // The overlays reference symbolReader, now closed; drop them so a later
            // flush of a non-SYMBOL view cannot reuse a stale resolver.
            bufferRecord.setSymbolResolvers(null);
        }

        instance.setLastProcessedSeqTxn(advanceTo);
        instance.setAppliedWatermark(advanceTo);
        // applyWalDirect never propagates: ApplyWal2TableJob.applyWal suspends the table via
        // handleWalApplyFailure and returns, and it can also no-op silently (the LV writer is
        // busy, or the table backed off under memory pressure). A failed inline apply thus
        // leaves the block committed-but-unapplied yet still runs the trailing
        // setLeadRowCount(0), so the next flush cannot re-materialise the slot rows; the view
        // serves disk-only behind the seqTxn fence, under-reporting the committed rows, until
        // the block lands once. Nothing else applies it - ApplyWal2TableJob.doRun drops
        // live-view notifications while refresh is enabled - so scanForLaggingViews re-drives
        // it (hasPendingLiveViewApply / retryPendingLiveViewApply) rather than leaving the view
        // stale until the next base commit happens to flush again.
        // See LiveViewSmokeTest.testFlushLeadInlineApplyFailureRecoversWithoutDuplication.
        final SeqTxnTracker lvTracker = engine.getTableSequencerAPI().getTxnTracker(token);
        // Captured before the apply so the restamp below can tell a real apply from a no-op /
        // suspend: only when the writer txn advanced did the flushed lead reach disk.
        final long lvAppliedBefore = lvTracker.getWriterTxn();
        applyJob.applyWalDirect(token, Job.RUNNING_STATUS);
        // Read the applied LV-table seqTxn only AFTER applyWalDirect: restampSlotAfterFlush
        // below stamps the slot with it, and the getCursor staleness retry depends on the
        // slot's seqTxn never exceeding what an applied-base reader can observe. Reading it
        // before the apply would under-stamp the slot: a racing cursor that re-opened disk
        // at the freshly applied seqTxn would see isSlotNewerThanDisk() false and disengage
        // the slot/disk seam, falling back to stale disk-only content (see
        // LiveViewRecordCursor.isSlotNewerThanDisk).
        final long lvAppliedSeqTxn = lvTracker.getWriterTxn();
        boolean lvConsumedPersisted = false;
        try {
            engine.advanceLiveViewConsumedSeqTxn(token, advanceTo, blockFileWriter, path);
            lvConsumedPersisted = true;
        } catch (CairoException e) {
            LOG.critical().$("could not advance live view consumed seqTxn after flush [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", advanceTo=").$(advanceTo)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
        }
        if (!lvConsumedPersisted) {
            persistState(instance);
        }
        // The lead is now on disk; reset the in-RAM lead count unconditionally.
        instance.setLeadRowCount(0);
        if (lvConsumedPersisted) {
            // The just-flushed lead's new symbols are now committed at the ids the
            // drain assigned, so the next window resolves them via the disk reader's
            // keyOf; drop the per-window intern maps. The id -> string lists stay
            // (a pinned pre-flush cursor still resolves its slot from them).
            tier.getSymbolCache().onFlush();
            if (stagingRowsToInclude > 0) {
                // Emergency flush: the published slot never received the staging
                // rows (the publish that would have added them failed), so it is an
                // incomplete subset of disk. Leave it stale-stamped and mark it for
                // rebuild - the fence routes reads disk-only (disk is now current)
                // until the next refresh drops the slot and rebuilds a clean one.
                instance.setTierStale(true);
            } else if (lvAppliedSeqTxn <= lvAppliedBefore) {
                // The inline apply no-opped or suspended (writer txn did not advance): the flushed
                // lead is in the LV WAL but never reached disk, so the slot still holds those rows
                // while disk lacks them. Unlike an emergency flush, disk did NOT move ahead, so the
                // fence (slot.lvSeqTxn == diskSeqTxn) stays engaged on the pre-flush stamp -
                // re-stamping as a leadRowCount=0 subset would make size() report disk-only while
                // the scan still serves the lead, so count() and a full scan disagree (the seam can
                // even double-serve an overlap row). Un-stamp so the fence disengages and reads are
                // disk-only and self-consistent until the block lands. Same appliedBefore/After
                // guard retryPendingLiveViewApply uses.
                restampSlot(instance, Numbers.LONG_NULL, 0);
                instance.setTierStale(true);
            } else {
                // Normal flush: the lead rows are now on disk and still in the slot,
                // so it is a complete subset of disk. Re-stamp it so reads regain
                // seam routing immediately.
                restampSlotAfterFlush(instance, lvAppliedSeqTxn);
            }
            maybeWriteHeadCheckpoint(instance, windowFactory, advanceTo, flushedMaxTs, flushRows, false);
        }
    }

    /**
     * Builds the per-column symbol resolvers the flush hands {@link #bufferRecord}
     * so the copier can turn a lead row's stored LV-table-consistent symbol id back
     * into its string before re-interning it into the WAL. Each SYMBOL column gets a
     * {@link LiveViewSymbolTable} overlaying {@code symbolReader}'s committed symbol
     * table (for already-flushed values) with {@code cache}'s lead symbols (for
     * values new to the un-flushed lead). Non-SYMBOL columns are left null. The
     * overlays borrow {@code symbolReader} (do not own it); the flush closes the
     * reader and drops the resolvers.
     */
    private ObjList<LiveViewSymbolTable> buildFlushSymbolResolvers(RecordMetadata outMetadata, TableReader symbolReader, LiveViewSymbolCache cache) {
        final int n = outMetadata.getColumnCount();
        final ObjList<LiveViewSymbolTable> resolvers = new ObjList<>(n);
        for (int c = 0; c < n; c++) {
            if (ColumnType.tagOf(outMetadata.getColumnType(c)) == ColumnType.SYMBOL) {
                // Writer-side resolver: the refresh worker builds this while flushing,
                // not interning, so the live horizon is exact and the whole lead band
                // is the correct bound (the flush re-serialises every lead id).
                // leadContainsNull is false here: the flush path only turns stored lead
                // ids back into strings and never calls containsNullValue().
                resolvers.add(new LiveViewSymbolTable().of(
                        symbolReader.getSymbolTable(c), cache, c, cache.newSymbolMaxIdExclusive(c), false, false));
            } else {
                resolvers.add(null);
            }
        }
        return resolvers;
    }

    /**
     * Re-stamps the published slot's LV-table seqTxn and lead count under the writer sentinel, so
     * reads regain seam routing (the fence holds) immediately with the correct
     * {@code size() = disk.size() + leadRowCount}. Best-effort: when a reader pins the slot the
     * {@code 0 -> -1} CAS fails and the slot keeps its prior (now stale) stamp, which the fence routes
     * disk-only until the next refresh re-publishes a fresh slot. Returns {@code true} when the slot
     * was re-stamped, {@code false} when the CAS failed (or there is no tier).
     */
    protected boolean restampSlot(LiveViewInstance instance, long lvSeqTxn, long leadRowCount) {
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            return false;
        }
        final int publishedIdx = tier.getPublishedIdx();
        final LiveViewInMemoryBuffer acquired = tier.tryAcquireWrite(publishedIdx);
        if (acquired == null) {
            return false;
        }
        try {
            acquired.setLvSeqTxn(lvSeqTxn);
            acquired.setLeadRowCount(leadRowCount);
        } finally {
            tier.releaseWriteWithoutPublish(publishedIdx);
        }
        return true;
    }

    /**
     * After a flush lands the whole lead on disk, re-stamps the published slot as a subset of disk:
     * the slot's stored seqTxn becomes the just-applied LV-table seqTxn and its lead count drops to
     * zero, so reads regain seam routing (fence holds) immediately. A thin {@code leadRowCount = 0}
     * specialisation of {@link #restampSlot(LiveViewInstance, long, long)}; see it for the
     * best-effort / reader-pinned semantics.
     */
    private void restampSlotAfterFlush(LiveViewInstance instance, long lvAppliedSeqTxn) {
        restampSlot(instance, lvAppliedSeqTxn, 0);
    }

    /**
     * Head invalidation on out-of-order arrival. The current cycle
     * still feeds the offending batch through the in-WAL-order pipeline (so
     * the live output for the affected partitions is wrong for this batch);
     * the value of this helper is
     * narrower: the on-disk head no longer reflects the rows the LV will
     * eventually need to replay, so it must be retired now to keep restart
     * recovery sound. The view falls through to head-miss replay on the
     * next restart, which restarts the window state from
     * {@code viewLowerBoundTimestamp}.
     * <p>
     * Best-effort: a removeQuiet failure is logged but does not invalidate
     * the view. Clearing the in-memory head metadata to {@code LONG_NULL}
     * stops the catalogue from advertising a head that may or may not still
     * be on disk.
     */
    private void invalidateHeadOnO3(LiveViewInstance instance, long seqTxn, long txnMinTs, long latestSeenTs) {
        final long headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
        LOG.critical().$("live view out-of-order base commit; invalidating head checkpoint [view=")
                .$(instance.getDefinition().getViewName())
                .$(", baseSeqTxn=").$(seqTxn)
                .$(", txnMinTs=").$(txnMinTs)
                .$(", latestSeenTs=").$(latestSeenTs)
                .$(", headLvSeqTxn=").$(headLvSeqTxn)
                .I$();
        if (headLvSeqTxn != Numbers.LONG_NULL) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                    .slash();
            LiveViewCheckpointWriter.appendCpFileName(path, headLvSeqTxn);
            try {
                engine.getConfiguration().getFilesFacade().removeQuiet(path.$());
            } catch (Throwable t) {
                LOG.error().$("could not unlink head checkpoint on O3 [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", error=").$(t).I$();
            }
        }
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
    }

    /**
     * Retires the retained checkpoints an out-of-order base commit at
     * {@code triggerLowTs} has unsealed, replacing the blanket head retire the
     * O3 replay paths used before retention. Entries with {@code maxTs <
     * triggerLowTs} stay sealed - the late row is above them - and survive as
     * resume anchors; entries at or above it (including the head) are dropped
     * from the ring and their {@code .cp} files unlinked best-effort. A non-DATA
     * / recovery trigger ({@code triggerLowTs == LONG_NULL}) drops the whole ring
     * and always clears the head.
     * <p>
     * When the head is among the unsealed set its metadata is cleared so the
     * post-replay write lands on its first-cp cadence path. The head is unlinked
     * explicitly in addition to the ring-driven unlinks: a restart restores head
     * metadata without repopulating the ring, so the head may not be a ring entry
     * yet ({@code removeQuiet} is idempotent, so a double unlink is harmless). The
     * in-memory drop is unconditional even if an unlink fails - a failed unlink
     * must never leave a live anchor.
     * <p>
     * The retire also publishes the survivors to {@code _ring} at
     * {@code coveredBaseSeqTxn} - the cycle's commit point - before it unlinks
     * anything, so the in-memory drop and its durable record stay one unit off
     * one {@code triggerLowTs}. The publication runs <em>ahead</em> of the commit
     * it names: the retire above has just proved that no row in the trigger
     * commit or the ahead range sits at or below any survivor's {@code maxTs},
     * which is exactly "sealed at {@code coveredBaseSeqTxn}", and that stays true
     * whether or not the replay below then succeeds.
     * <p>
     * Publishing before the unlink is what keeps a crash in between cheap: the
     * manifest is an allow-list, so a file it does not list is garbage whether or
     * not its unlink lands, whereas unlinking first would leave the prior
     * manifest naming files that no longer exist and a restart would reject it
     * whole over the referenced-file check. Failure never blocks the replay:
     * the helper logs and returns false, and the in-memory ring the resume
     * anchors come from is already correct.
     */
    private void invalidateRetainedCheckpointsOnO3(LiveViewInstance instance, long triggerLowTs, long coveredBaseSeqTxn) {
        evictedCheckpoints.clear();
        instance.invalidateRetainedCheckpointsFrom(triggerLowTs, evictedCheckpoints);
        final long headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        final boolean headUnsealed = headLvSeqTxn != Numbers.LONG_NULL
                && (triggerLowTs == Numbers.LONG_NULL || headMaxTs == Numbers.LONG_NULL || headMaxTs >= triggerLowTs);
        if (headUnsealed) {
            evictedCheckpoints.add(headLvSeqTxn);
        }
        publishCheckpointRing(instance, coveredBaseSeqTxn);
        unlinkCheckpointFiles(instance, evictedCheckpoints);
        if (headUnsealed) {
            instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
        }
        retireCheckpointTimelineOnO3(instance);
    }

    /**
     * Retires the whole checkpoint timeline when an out-of-order change invalidates
     * the retained checkpoints. Invariant 2 requires every current root in a
     * generation to be correct for one pinned base snapshot, and an O3 replay
     * rewrites live-view output below roots that were sealed before it - so those
     * roots no longer describe the materialization and must not survive into the
     * next generation. Retiring is the coarse form of that guarantee: Phase 5
     * replaces it with a range splice that re-versions only the roots in
     * {@code [C, H)} and keeps the prefix and converged suffix. Until then a
     * post-replay seal starts a fresh history, which mirrors what the legacy ring
     * already does by dropping its unsealed entries and clearing the head.
     * <p>
     * Failure is logged and swallowed: the replay owns correctness of the durable
     * output, and a timeline left behind is re-reconciled (and re-retired) on the
     * next seal or restart rather than blocking the refresh.
     */
    private void retireCheckpointTimelineOnO3(LiveViewInstance instance) {
        if (engine.isReadOnlyMode()) {
            return;
        }
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
            LiveViewCheckpointLifecycle.retireTimeline(
                    engine.getConfiguration(),
                    checkpointsDir,
                    null,
                    true
            );
        } catch (Throwable t) {
            LOG.error().$("could not retire live view checkpoint timeline after O3 [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
        }
        instance.clearCheckpointTimelineOwnership();
    }

    /**
     * Reports whether the live view's base table currently has DEDUP UPSERT keys
     * enabled. A dedup base makes the refresh worker route this view onto the
     * coupled, applied-reader path ({@link #drainAppliedBase}) instead of the raw-WAL
     * lead path, because the raw WAL holds the pre-dedup stream (see the class
     * javadoc).
     * <p>
     * Re-derived each cycle rather than cached, because base dedup config is mutable
     * via {@code ALTER TABLE ... DEDUP ENABLE/DISABLE}: a one-shot flag would freeze
     * the wrong cadence across a flip. The read is a MetadataCache read-lock plus a
     * map lookup on the memory-resident catalogue (no file open); it is once per
     * cycle, never per row, so the non-dedup per-row hot loop is unaffected. The
     * catalogue reflects the timestamp column's dedup flag, which is set exactly when
     * the table is dedup-enabled (mirrors {@code TableWriter.isDeduplicationEnabled()}).
     */
    /**
     * Reports whether the apply-lag back-off still holds this view back this tick. The wall-clock floor
     * is only an anti-spin bound; the real precondition is the base applying past the seqTxn that forced
     * the defer, so an O(1) tracker read short-circuits the floor the moment apply catches up -- which is
     * also what lets a frozen test clock, which never crosses the floor, converge.
     * <p>
     * {@code authoritative} callers run under the refresh latch and clear a floor they find satisfied;
     * pre-latch callers pass {@code false} and mutate nothing. Every arming site runs under the latch, so
     * the authoritative check-and-clear cannot erase an episode a peer worker armed concurrently.
     */
    private boolean isApplyLagDeferred(LiveViewInstance instance, boolean authoritative) {
        final long deferUntilUs = instance.getApplyLagDeferUntilUs();
        if (deferUntilUs == Numbers.LONG_NULL) {
            return false;
        }
        final LiveViewDefinition definition = instance.getDefinition();
        final TableToken baseToken = definition != null ? definition.getBaseTableToken() : null;
        final boolean applyCaughtUp = baseToken != null
                && engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn()
                >= instance.getApplyLagDeferTargetSeqTxn();
        if (!applyCaughtUp && engine.getConfiguration().getMicrosecondClock().getTicks() < deferUntilUs) {
            return true;
        }
        if (authoritative) {
            instance.setApplyLagDeferUntilUs(Numbers.LONG_NULL);
        }
        return false;
    }

    private boolean isDedupBase(LiveViewInstance instance) {
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        try (MetadataCacheReader metaRO = engine.getMetadataCache().readLock()) {
            final CairoTable baseTable = metaRO.getTable(baseToken);
            return baseTable != null && baseTable.hasDedup();
        }
    }

    /**
     * Reports whether the applied base over {@code (fromSeqTxn, toSeqTxn]} provably equals
     * the base's raw WAL stream, so a coupled dedup-base view can refresh through the proven
     * raw-WAL {@link #drainBaseWal} path instead of the applied-reader {@link #drainAppliedBase}.
     * The raw-WAL path appends additive same-ts rows
     * cheaply and its own O3 detection still routes a genuine below-frontier late row through
     * {@link #o3Replay}; it only diverges from the applied base when a seqTxn in range deduped,
     * skipped, or ran a data-shaped non-DATA op, which the signal rules out.
     * <p>
     * Reads the {@link SeqTxnTracker} the apply worker updates per applied batch. Fails safe
     * (returns false) on a cold signal (restart / first cycle), apply lag past the recorded
     * point, or any divergence in range -- the caller then falls back to {@link #drainAppliedBase}
     * with no correctness loss, only the raw-WAL fast-path benefit until the signal warms.
     */
    private boolean isRangeProvablyClean(TableToken baseToken, long fromSeqTxn, long toSeqTxn) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(baseToken);
        // Read covered first (acquire): recordApplied writes divergence and trackedFrom BEFORE
        // covered, so observing covered >= toSeqTxn guarantees the paired divergence/trackedFrom
        // are visible too. Reading them later than covered only over-reports (both monotone),
        // which is conservative.
        final long covered = tracker.getDedupSignalCoveredSeqTxn();
        if (covered < toSeqTxn) {
            // Apply has not yet recorded the whole range (cold signal or apply lag).
            return false;
        }
        final long trackedFrom = tracker.getDedupSignalTrackedFromSeqTxn();
        final long divergence = tracker.getDedupSignalDivergenceSeqTxn();
        return trackedFrom != Numbers.LONG_NULL
                && fromSeqTxn + 1 >= trackedFrom   // range lower bound within the tracked window
                && divergence <= fromSeqTxn;        // no dedup / skip / non-DATA op above fromSeqTxn
    }

    /**
     * Minimum in-view timestamp any DATA base commit in the apply-ahead range
     * {@code (fromSeqTxn, toSeqTxn]} would introduce, used to gate an apply-ahead
     * resume: when {@code ApplyWal2TableJob} has raced the base reader past the O3
     * trigger, a resume from a sealed anchor {@code C} is sound only if
     * {@code C.maxTs} sits strictly below every row those un-examined seqTxns hold
     * (else a back-dated row below {@code C.maxTs} would be dropped). This returns
     * that floor.
     * <p>
     * Returns {@link Numbers#LONG_NULL} when the range is <b>not safely
     * resumable</b> and the caller must rebuild from the boundary instead:
     * <ul>
     *     <li>a structural / compacted sequencer entry ({@code walId <= 0}), or a
     *     non-DATA commit (TRUNCATE / DROP PARTITION / UPDATE) is present - a
     *     bounded resume cannot reproduce whatever it changed below the anchor; or</li>
     *     <li>the range holds no DATA commit at all.</li>
     * </ul>
     * The min source mirrors {@link #drainAppliedBase}'s overlap walk exactly (the
     * WAL-E event file, corrected by {@link #effectiveReplaceRangeDeleteLo} so a
     * REPLACE_RANGE delete contributes its clamped range low rather than its
     * inserted-row minimum); it differs only by aborting to {@code LONG_NULL} on
     * the structural / non-DATA commits that walk merely skips - the drain is
     * still going to scan everything, whereas a resume must not.
     */
    private long computeApplyAheadMinTs(TableToken baseToken, long fromSeqTxn, long toSeqTxn, long viewLowerBoundTimestamp) {
        long minTs = Numbers.LONG_NULL;
        try (TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn)) {
            while (txnCursor.hasNext()) {
                final long txn = txnCursor.getTxn();
                if (txn > toSeqTxn) {
                    break;
                }
                final int walId = txnCursor.getWalId();
                if (walId <= 0) {
                    // Compacted / structural entry (STRUCTURAL_CHANGE / DROP_TABLE):
                    // a bounded resume cannot see whatever it changed, so refuse it.
                    return Numbers.LONG_NULL;
                }
                final int segmentId = txnCursor.getSegmentId();
                final int segmentTxn = txnCursor.getSegmentTxn();
                walPath.of(engine.getConfiguration().getDbRoot())
                        .concat(baseToken)
                        .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                final WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, walEventReader, segmentTxn, txn);
                if (!WalTxnType.isDataType(eventCursor.getType())) {
                    // TRUNCATE / DROP PARTITION / UPDATE: a non-DATA change whose
                    // effect a bounded resume cannot reproduce - force the rebuild.
                    return Numbers.LONG_NULL;
                }
                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                long txnMinTs = dataInfo.getMinTimestamp();
                final long deleteLo = effectiveReplaceRangeDeleteLo(dataInfo, viewLowerBoundTimestamp);
                if (deleteLo != Numbers.LONG_NULL) {
                    txnMinTs = deleteLo;
                }
                if (minTs == Numbers.LONG_NULL || txnMinTs < minTs) {
                    minTs = txnMinTs;
                }
            }
        }
        return minTs;
    }

    /**
     * Out-of-order replay. Called from {@code incrementalRefresh}
     * after detection rolls back the in-WAL-order draft for the offending
     * cycle. Picks the head-hit branch when an in-disk head exists and its
     * {@code maxTimestamp <= lateRowTs}; falls back to head-miss replay from
     * {@code viewLowerBoundTimestamp} otherwise. Either branch reads the base
     * table via {@code TableReader} in ts-ascending order through the
     * compiled SELECT's filter / anchor / window cursor stack, commits via
     * {@link WalWriter#commitLiveViewWithReplaceRange(long, long, long)},
     * applies inline, and writes a fresh head .cp post-replay.
     * <p>
     * Replay only fires for snapshot-capable LVs - the per-function state
     * resets used here rely on every WindowFunction exposing its partition
     * Map via {@link WindowFunction#getPartitionMap()}. Non-capable LVs fall
     * back to head invalidation only (the prior Option 1 disposition); their
     * live output for the O3 batch is wrong until the next refresh cycle,
     * matching the fallback behaviour for any LV whose SELECT contains a
     * function still on the default-throw snapshot path.
     *
     * @param instance      live view being replayed
     * @param windowFactory the LV's compiled SELECT (window cursor stack)
     * @param lateRowTs     {@code dataInfo.getMinTimestamp()} that triggered
     *                      O3 detection
     * @param baseToken     base table token (passed in so the replay path
     *                      doesn't re-look-it-up from the definition)
     * @param advanceTo     base seqTxn the replay must cover; also the value
     *                      passed to {@code commitLiveViewWithReplaceRange}
     *                      so the LV's lvConsumedSeqTxn advances after apply
     */
    private void o3Replay(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lateRowTs,
            TableToken baseToken,
            long advanceTo
    ) throws SqlException {
        final String viewName = instance.getDefinition().getViewName();
        // An intra-commit out-of-order FIRST commit can reach the replay path
        // before any in-order cycle computed snapshot capability (which normally
        // happens in maybeWriteHeadCheckpoint). Compute it here so the
        // not-capable disposition below is driven by the real value rather than
        // the default false, which would wrongly skip the replay.
        if (!instance.isSnapshotCapabilityComputed()) {
            instance.setSnapshotCapability(computeSnapshotCapability(instance, windowFactory));
        }
        if (!instance.isSnapshotCapability()) {
            // No clean per-function reset API for the unmigrated families;
            // recompiling the factory would wipe everything but is heavy.
            // Match the Option 1 disposition for these LVs: log critical,
            // retire the head .cp so restart cannot restore stale state,
            // accept that the live output for the O3 batch is wrong until
            // a non-O3 cycle naturally advances state.
            invalidateHeadOnO3(instance, advanceTo, lateRowTs, instance.getLatestSeenTs());
            // Retire the rest of the ring on the same terms (LONG_NULL drops it
            // whole) and publish the now-empty membership at the point the
            // watermarks below advance to. invalidateHeadOnO3 retires only the
            // head, which sufficed while nothing published the ring: this branch
            // feeds the O3 batch through the in-WAL-order pipeline and then walks
            // the watermarks over it, so every retained entry is unsealed - and
            // one left in the in-memory ring would be listed by the next in-order
            // publication at a covered equal to the floor, the one state a restart
            // trusts.
            //
            // Usually a no-op: a non-capable view seals no .cp, so the ring is
            // empty. The shape that is not is a view whose functions lost snapshot
            // support across a restart - capability is computed on first use here,
            // while promoteRestoredHeadIntoRing has already listed the restored
            // head.
            //
            // invalidateHeadOnO3 unlinking the head .cp before this publication
            // inverts the publish-then-unlink order, but costs nothing here: that
            // order exists so a crash between cannot leave the prior manifest
            // naming a missing file, which restart rejects whole, and the ring is
            // being dropped whole anyway - the same empty membership either way.
            invalidateRetainedCheckpointsOnO3(instance, Numbers.LONG_NULL, advanceTo);
            LOG.critical().$("live view O3 replay skipped, snapshot capability is false [view=")
                    .$(viewName)
                    .$(", advanceTo=").$(advanceTo)
                    .$(", lateRowTs=").$(lateRowTs).I$();
            // Advance the watermarks so the next cycle does not re-process
            // the O3 batch in WAL order again.
            instance.setLastProcessedSeqTxn(advanceTo);
            instance.setAppliedWatermark(advanceTo);
            try {
                engine.advanceLiveViewConsumedSeqTxn(
                        instance.getLiveViewToken(),
                        advanceTo,
                        blockFileWriter,
                        path
                );
            } catch (CairoException e) {
                LOG.critical().$("could not advance live view consumed seqTxn on skipped O3 replay [view=")
                        .$(viewName)
                        .$(", advanceTo=").$(advanceTo)
                        .$(", error=").$safe(e.getFlyweightMessage()).I$();
                persistState(instance);
            }
            // instance.leadRowCount is 0 on entry to o3Replay: finishLeadRefresh (the
            // lead path) and drainAppliedBase's overlap branch (the coupled dedup path,
            // where an ALTER ... DEDUP ENABLE flip can leave a pre-dedup RAM lead) zero
            // it explicitly first, and the remaining coupled-forward and replay-to-applied
            // callers carry no un-flushed lead so it is already 0. The capable path
            // rebuilds the tier as a pure disk subset (leadRowCount 0).
            // This branch rewrote nothing on disk and left the published slot
            // untouched, so a slot that is STILL a current un-flushed lead keeps its
            // stamped leadRowCount as the true lead. Resync instance.leadRowCount to it:
            // leaving it at 0 desyncs the two, so the next publish would reclassify those
            // L never-flushed rows as overlap (size() under-reports, iteration serves
            // them as phantoms) and flushLead's overlapCount would skip them entirely.
            //
            // But re-arm ONLY from a slot whose stamped LV-table seqTxn still matches the
            // applied disk seqTxn. A slot whose stamp has fallen behind disk holds rows
            // that are already durable, so its leadRowCount is NOT an un-flushed lead and
            // the correct value is the 0 the caller left. Two paths leave such a
            // stale-stamped slot, and both must be excluded:
            //   - an emergency flush wrote the lead to disk, set tierStale, and left the
            //     slot's now-durable leadRowCount stamped (isTierStale() would catch it); and
            //   - a normal flush wrote the lead to disk but its restampSlot 0 -> -1 CAS
            //     lost to a reader pin, so the slot kept its now-durable stamp while
            //     tierStale stayed FALSE (restampSlotAfterFlush ignores the CAS result) --
            //     an isTierStale() guard MISSES this one.
            // Re-arming from either would make the finishLeadRefresh flush path trust a
            // stale non-zero leadRowCount and re-flush the already-durable rows as on-disk
            // duplicates. The seqTxn-match check below subsumes both (both leave
            // slot.lvSeqTxn() != applied) and needs no reader open: the applied seqTxn is
            // the same coordinate flushLead / publishToInMemoryTier stamp the slot from,
            // and nothing has applied to the LV table since (this branch does not commit).
            //
            // Defensive: CREATE rejects every non-snapshot-capable window shape (each
            // WindowFunction.supportsCheckpointState() folds in the anchor key type check),
            // and o3Replay recomputes capability above, so a freshly-validated view
            // never reaches this branch. It fires only for a view that is
            // non-capable at runtime (e.g. a restored view whose function lost
            // snapshot support); the resync keeps its bookkeeping correct if so.
            final LiveViewInMemoryTier ncTier = instance.getInMemoryTier();
            if (ncTier != null) {
                final LiveViewInMemoryBuffer ncSlot = ncTier.getSlot(ncTier.getPublishedIdx());
                final long lvAppliedSeqTxn = engine.getTableSequencerAPI()
                        .getTxnTracker(instance.getLiveViewToken())
                        .getWriterTxn();
                if (ncSlot.lvSeqTxn() == lvAppliedSeqTxn) {
                    instance.setLeadRowCount(ncSlot.leadRowCount());
                }
            }
            return;
        }

        // Atomic snapshot of (lvSeqTxn, maxTs); without it a concurrent
        // setHeadCheckpoint could pair a fresh lvSeqTxn with the prior
        // maxTs and drive the head-hit decision off a torn read.
        final long[] headPair = instance.getHeadCheckpointSeqAndMaxTs();
        final long headLvSeqTxn = headPair[0];
        final long headMaxTs = headPair[1];
        // Strict comparison is load-bearing. The head's state covers every row
        // up to AND INCLUDING headMaxTs, and head-hit replay starts at
        // headMaxTs + 1 (TimestampLowerBoundCursor admits ts >= lowTs). A late
        // row at exactly headMaxTs is therefore not covered by the head yet also
        // excluded from the replay window, so it would be silently dropped. The
        // exact boundary routes to head-miss instead (full replay from the lower
        // bound), which re-reads and merges the late row in ts order.
        // A head with no maxTs cannot serve a head-hit: the replay floors at headMaxTs + 1,
        // and LONG_NULL + 1 would admit every base row - including rows below the view's
        // START FROM boundary, which this path (unlike head-miss) does not apply. Treat it as
        // no head at all and route to the full head-miss replay, which floors at the boundary.
        final boolean headHitEligible = headLvSeqTxn != Numbers.LONG_NULL
                && headMaxTs != Numbers.LONG_NULL
                && headMaxTs < lateRowTs;
        LOG.info().$("live view O3 replay [view=").$(viewName)
                .$(", lateRowTs=").$(lateRowTs)
                .$(", advanceTo=").$(advanceTo)
                .$(", headHitEligible=").$(headHitEligible)
                .$(", headLvSeqTxn=").$(headLvSeqTxn)
                .$(", headMaxTs=").$(headMaxTs).I$();

        if (headHitEligible) {
            // The head is simply the newest ring entry, so a head-hit is a
            // resume from that newest anchor.
            replayFromAnchor(instance, windowFactory, lateRowTs, baseToken, advanceTo, headLvSeqTxn, headMaxTs);
        } else {
            // Bounded-miss: the late row sits at or below the head, so the head
            // cannot anchor the replay. A DATA trigger (lateRowTs != LONG_NULL)
            // may still find an OLDER sealed ring entry whose maxTs is strictly
            // below the late row - resume from that anchor through the same helper
            // instead of rebuilding the whole view from the boundary. Replay cost
            // drops from O(view age) to O(head - anchor.maxTs), a few hundred rows
            // near the head. A non-DATA / recovery trigger (LONG_NULL) keeps the
            // frozen-prefix full rebuild, and a late row below every retained
            // anchor falls back to it too.
            //
            // Apply-ahead is NOT resumed here yet: replayFromAnchor bails to the
            // full head-miss rebuild whenever the base reader has advanced past
            // advanceTo, because a back-dated row hidden in an unexamined seqTxn
            // below the anchor would be dropped. Bounding that case to an anchor
            // below the ahead range's minimum is step 6 (the minAheadTs guard).
            final int anchorIdx = lateRowTs != Numbers.LONG_NULL
                    ? findResumeAnchorBelow(instance, lateRowTs)
                    : -1;
            if (anchorIdx >= 0) {
                replayFromAnchor(
                        instance,
                        windowFactory,
                        lateRowTs,
                        baseToken,
                        advanceTo,
                        instance.getRetainedCheckpointLvSeqTxn(anchorIdx),
                        instance.getRetainedCheckpointMaxTs(anchorIdx)
                );
            } else {
                o3HeadMissReplay(instance, windowFactory, lateRowTs, baseToken, advanceTo, false);
            }
        }

        // The replay rewrote the on-disk tier (REPLACE_RANGE); the in-mem tier
        // still holds the pre-replay output rows for the rewritten range. Rebuild
        // it atomically from the rewritten LV table so a post-O3 cursor regains
        // seam routing immediately instead of falling through to disk until the
        // next normal cycle republishes. The seqTxn fence keeps this safe either way.
        // See rebuildInMemoryTier.
        rebuildInMemoryTier(instance);
        // rebuildInMemoryTier zeros the rebuilt SLOT's leadRowCount but not
        // instance.leadRowCount. It is already 0 on entry (see the non-capable branch),
        // so this is a no-op today; pin it explicitly to match the resets in
        // finishLeadRefresh's stale-tier branch and the non-capable resync, keeping
        // instance.leadRowCount from desyncing with the rebuilt slot if a future path
        // ever reaches here non-zero.
        instance.setLeadRowCount(0);
    }

    /**
     * Resume replay from a sealed checkpoint anchor: rolls window state back to
     * the anchor {@code .cp}'s snapshot moment (clear per-function maps, then
     * restore from disk), scans the base table from {@code anchorMaxTs + 1}
     * forward (never below {@code viewLowerBoundTimestamp}), and emits a single
     * REPLACE_RANGE commit covering that same range through positive infinity.
     * Cheaper than the boundary rebuild because the anchor's state already
     * reflects everything in {@code [viewLowerBoundTimestamp, anchorMaxTs]} - the
     * replay only re-evaluates the tail above the anchor.
     * <p>
     * The head is simply the newest ring entry, so a head-hit is a resume from
     * the newest anchor; the bounded-miss path resumes from an older sealed anchor
     * through this same body. The caller has already verified the anchor is
     * hit-eligible (it exists and its {@code maxTimestamp < lateRowTs}).
     * <p>
     * Before restoring, this retires every retained checkpoint the O3 at
     * {@code lateRowTs} unsealed (entries with {@code maxTs >= lateRowTs},
     * including the prior head) so no later resume ever anchors on state that
     * predates this late row. For a head-hit the late row sits above the head, so
     * nothing is unsealed and the retire is a no-op; for a bounded-miss it drops
     * the poisoned entries between the anchor and the head. The anchor itself
     * ({@code anchorMaxTs < lateRowTs}) always survives.
     * <p>
     * Restore can still fail here (corrupt {@code .cp}, unsupported format
     * version). Structural corruption drives {@code restoreFromHead} ->
     * {@code handleCorruptHeadCheckpoint}, which unlinks the file and evicts the
     * anchor's ring entry, clearing the head metadata only when the anchor IS the
     * head - a non-head anchor leaves the newer, still-valid real head in place.
     * A compatibility break (version mismatch) instead stashes a pending
     * invalidation reason and neither unlinks nor evicts. Either way this method
     * abandons the replay without advancing the watermark, so the trigger re-fires
     * on a later cycle and recovers once a fresh head exists.
     * <p>
     * On success the post-replay state is sealed as a fresh head while the anchor
     * the replay was built on stays in the ring (the late row sat above it, so it
     * remains a valid resume anchor rather than garbage). A replay that produces no
     * row keeps its anchor as the head - the commit truncates the LV back to
     * exactly what that anchor covers.
     */
    private void replayFromAnchor(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lateRowTs,
            TableToken baseToken,
            long advanceTo,
            long anchorLvSeqTxn,
            long anchorMaxTs
    ) throws SqlException {
        final String viewName = instance.getDefinition().getViewName();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
        // The DATA trigger's authority to DELETE and to unseal retained checkpoints,
        // clamped up to the view's lower bound (same rule as o3HeadMissReplay).
        // replayFromAnchor never runs for a non-DATA trigger - the head-hit branch
        // needs headMaxTs < lateRowTs and the bounded-miss branch gates on
        // lateRowTs != LONG_NULL - so triggerLowTs is always a real timestamp here;
        // the LONG_NULL guard is kept for defensive symmetry with the replay paths.
        final long triggerLowTs = lateRowTs == Numbers.LONG_NULL
                ? Numbers.LONG_NULL
                : Math.max(lateRowTs, viewLowerBoundTimestamp);
        TableReader reader = waitForApply(baseToken, advanceTo);
        final long effectiveSeqTxn = reader.getSeqTxn();
        // Commit / watermark point for this replay. Normally the O3 trigger seqTxn;
        // an apply-ahead resume advances it to the base reader's effective seqTxn -
        // the snapshot the scan below materialises - exactly as o3HeadMissReplay does.
        long commitSeqTxn = advanceTo;
        // Threshold at or above which this O3 unseals retained checkpoints. Normally
        // the trigger ts; an apply-ahead resume lowers it to the ahead range's floor
        // so entries a back-dated ahead row unsealed are retired too (see below).
        long retireThreshold = triggerLowTs;
        if (effectiveSeqTxn != advanceTo) {
            // ApplyWal2TableJob has raced the base reader past the O3 trigger: the
            // snapshot already reflects seqTxns in (advanceTo, effectiveSeqTxn] the
            // forward drain has not examined. The anchor resume only re-reads base
            // above anchorMaxTs, so a back-dated row below anchorMaxTs hidden in one
            // of those unexamined seqTxns would be silently dropped, and advancing
            // the watermark over them would lose it permanently. Resume only from a
            // sealed anchor strictly below BOTH the trigger and the ahead range's
            // minimum in-view ts; otherwise rebuild the whole view from the boundary
            // (which sees the ahead rows and advances the watermark to exactly what
            // it materialised). waitForApply guarantees effectiveSeqTxn >= advanceTo,
            // so this is the strictly-ahead case.
            //
            // computeApplyAheadMinTs reads the base WAL-E off the pinned reader; a read
            // fault (a torn / purged base WAL-E - the "applied base outlived its WAL"
            // state) must return the pooled reader rather than leak it out of the shared
            // pool. The main try/finally below does not cover this pre-detach call, so
            // close and rethrow here. The no-anchor fallback further down already closes
            // the reader before it recurses, so it stays leak-safe without this guard.
            final long minAheadTs;
            try {
                minAheadTs = computeApplyAheadMinTs(baseToken, advanceTo, effectiveSeqTxn, viewLowerBoundTimestamp);
            } catch (Throwable th) {
                reader.close();
                throw th;
            }
            // A LONG_NULL minAheadTs means the ahead range is not safely resumable (a
            // structural / non-DATA commit, or no DATA commit at all) - force the rebuild.
            final long ceilTs = minAheadTs == Numbers.LONG_NULL || triggerLowTs == Numbers.LONG_NULL
                    ? Numbers.LONG_NULL
                    : Math.min(triggerLowTs, minAheadTs);
            final int aheadAnchorIdx = ceilTs == Numbers.LONG_NULL
                    ? -1
                    : findResumeAnchorBelow(instance, ceilTs);
            if (aheadAnchorIdx < 0) {
                // No sealed anchor below the ahead range's minimum (deep back-dating,
                // a structural change, or a non-DATA ahead commit). Fall back to the
                // full rebuild, forwarding this O3's own trigger ts so o3HeadMissReplay
                // keeps its DELETE authority; it recomputes the ahead floor itself to
                // retire the ahead-unsealed entries this resume would otherwise drop.
                reader.close();
                o3HeadMissReplay(instance, windowFactory, lateRowTs, baseToken, advanceTo, false);
                return;
            }
            // Re-anchor to the sealed entry below the ahead floor, commit at the
            // effective seqTxn the snapshot covers, and unseal every entry at or
            // above that floor (the prior head plus any entry a back-dated ahead row
            // invalidated). The replay stays bounded to (anchorMaxTs, effectiveSeqTxn].
            anchorLvSeqTxn = instance.getRetainedCheckpointLvSeqTxn(aheadAnchorIdx);
            anchorMaxTs = instance.getRetainedCheckpointMaxTs(aheadAnchorIdx);
            commitSeqTxn = effectiveSeqTxn;
            retireThreshold = ceilTs;
        }
        // Replay starts strictly above anchorMaxTs because the anchor's state
        // already covers rows up to and including anchorMaxTs. The same value
        // doubles as the REPLACE_RANGE low boundary so the apply step
        // rewrites only the affected partitions.
        //
        // Floored at the START FROM boundary so this path applies the same row
        // predicate as the seed, the forward drain and the head-miss replay -
        // the anchor resume is the one applied-base scan that does not start from
        // the boundary. The clamp is redundant under the anchor's own invariant
        // (a checkpoint is only ever written from seeded or drained output, which
        // already applied the boundary, so anchorMaxTs >= viewLowerBoundTimestamp
        // and anchorMaxTs + 1 is strictly above it), but that invariant is
        // implicit and lives four call sites away. Stating it here costs one
        // Math.max on a cold path and makes the property local: no anchor, however
        // it was written, can pull this scan below the boundary. An anchor with
        // maxTs LONG_NULL - the one that could, since LONG_NULL + 1 admits every
        // base row - is already refused hit-eligibility upstream.
        final long replayLowTs = Math.max(anchorMaxTs + 1, viewLowerBoundTimestamp);
        // Retire the checkpoints this O3 unsealed before restoring: entries with
        // maxTs >= retireThreshold (the prior head on the bounded-miss path, plus
        // any ahead-unsealed entry on an apply-ahead resume) predate the covered
        // rows and must never anchor a later resume. The anchor survives
        // (anchorMaxTs < retireThreshold), and for a head-hit nothing is unsealed
        // so this is a no-op. Mirrors o3HeadMissReplay's retire; the apply-ahead
        // rebuild fallback above routes through it too.
        //
        // The retire also publishes the survivors durably at commitSeqTxn, ahead
        // of the REPLACE_RANGE commit below: the same retireThreshold drives the
        // in-memory drop and the manifest, so the two can never disagree about
        // what this O3 unsealed. A publication failure does not abandon the
        // replay - covered only advances on success, so the manifest left on disk
        // is one a restart either trusts (its covered still equals the reconciled
        // floor, meaning this commit never landed and the survivors really are
        // still sealed there) or ignores.
        // Same reader-lifetime guard as the apply-ahead read above: the retire
        // publication runs off the pinned reader but before the main try/finally, so a
        // fault here must return the reader to the pool rather than leak it.
        try {
            invalidateRetainedCheckpointsOnO3(instance, retireThreshold, commitSeqTxn);
        } catch (Throwable th) {
            reader.close();
            throw th;
        }
        // Effectively-final snapshot of the commit / watermark point for the commit
        // lambda and the bookkeeping below (commitSeqTxn is reassigned above).
        final long committedSeqTxn = commitSeqTxn;
        boolean readerAttached = false;
        long appendedRows = 0;
        long o3ScanRows = 0;
        long replayMaxTs = Numbers.LONG_NULL;
        try {
            engine.detachReader(reader);
            executionContext.of(reader);
            readerAttached = true;

            RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
            final Function filter = filterFactory.getFilter();
            final PageFrameRecordCursorFactory pageFrameFactory =
                    (PageFrameRecordCursorFactory) (filter != null ? filterFactory.getBaseFactory() : filterFactory);
            RecordMetadata outMetadata = windowFactory.getMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();

            try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
                // Open the snapshot AT replayLowTs rather than scanning up to it: the
                // inclusive-lower-bound cursor culls whole partitions and binary-searches
                // into the first one. Head-hit exists to re-evaluate only the tail above
                // the head, so walking every partition below headMaxTs row by row - which
                // is what wrapping a full scan in TimestampLowerBoundCursor did - spent the
                // very cost the branch was built to avoid. Same cursor the seed and the
                // forward drain take, so all three agree on the boundary row for row.
                try (RecordCursor pageCursor = pageFrameFactory.getCursorFromTimestamp(executionContext, replayLowTs)) {
                    RecordCursor source = pageCursor;
                    if (filter != null) {
                        filteringCursor.of(source, filter, executionContext);
                        source = filteringCursor;
                    }
                    final LiveViewWindow anchorWindow = instance.getAnchorWindow();
                    if (anchorWindow != null) {
                        anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                        source = anchorDispatchingCursor;
                    }
                    try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                        // Drop pre-O3 drift before restoring from the head:
                        // clear each function's partition map so accumulator
                        // state that outran the head's snapshot moment is
                        // discarded. The anchor map gets the same treatment
                        // inside LiveViewWindow.restore() (it clears before
                        // reinserting), so no explicit wipe is needed here.
                        // Order matters: function maps clear -> restore from
                        // .cp.
                        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
                        for (int i = 0, n = functions.size(); i < n; i++) {
                            Map m = functions.getQuick(i).getPartitionMap();
                            if (m != null) {
                                m.clear();
                            }
                        }
                        if (!restoreFromHead(instance, windowFactory, anchorLvSeqTxn, restoredHeadState)) {
                            // restoreFromHead either unlinked the corrupt .cp and
                            // evicted this anchor's ring entry (clearing the head
                            // metadata only when the anchor IS the head), or stashed
                            // a version-mismatch invalidate reason. The O3 replay is
                            // abandoned here without advancing the watermark, so the
                            // same trigger re-fires on a later refresh cycle and
                            // recovers once a fresh head .cp exists (one cycle of
                            // stale pre-O3 rows in between). try-with-resources
                            // closes the cursor on return.
                            return;
                        }
                        // Snap the lifetime row counter back to the head's
                        // recorded value: the upcoming REPLACE_RANGE commit
                        // logically truncates rows above replayLowTs, so the
                        // counter rewinds in step with the table.
                        instance.setLvRowsTotal(restoredHeadState.lvRowsTotal);
                        Record outRecord = windowCursor.getRecord();
                        while (windowCursor.hasNext()) {
                            long ts = outRecord.getTimestamp(cursorTimestampIndex);
                            if (replayMaxTs == Numbers.LONG_NULL || ts > replayMaxTs) {
                                replayMaxTs = ts;
                            }
                            // Re-stamp the O3 detection watermark off the
                            // post-window output. The monotonic clamp on the
                            // setter means re-iterating rows the head already
                            // covered never lowers it.
                            instance.setLatestSeenTs(ts);
                            TableWriter.Row row = walWriter.newRow(ts);
                            copier.copy(executionContext, outRecord, row);
                            row.append();
                            appendedRows++;
                        }
                        // Capture base rows scanned before the cursor chain closes:
                        // FilteringRecordCursor.close() (cascaded from windowCursor)
                        // resets its counter. No filter -> scan equals emit; a filter
                        // makes scan exceed emit by the rows it dropped.
                        o3ScanRows = filter != null ? filteringCursor.getBaseRowsConsumed() : appendedRows;
                    }
                    // The REPLACE_RANGE is unconditional, including when the replay
                    // produced no row at all. Zero rows means the base no longer has
                    // anything above headMaxTs that survives the filter - a
                    // REPLACE_RANGE delete or a dedup replacement erased it - while
                    // the pre-O3 output for that range still sits on disk (head-hit
                    // eligibility implies latestSeenTs > headMaxTs, so the view did
                    // emit rows there). Skipping the commit would strand them as
                    // ghosts: size() over-reports, reads return stale rows, and
                    // rebuildInMemoryTier stages them back - all while the watermark
                    // advances past the commit that removed their base rows. Emitting
                    // the truncating range with no rows clears (headMaxTs, +inf) and
                    // leaves the LV exactly at the head's snapshot moment, which the
                    // restore above already reproduced in the window state. Mirrors
                    // the pure-delete branch in o3HeadMissReplay.
                    fencedLiveViewCommit(() -> walWriter.commitLiveViewWithReplaceRange(committedSeqTxn, replayLowTs, Long.MAX_VALUE));
                }
            }
        } finally {
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
            reader.close();
        }

        applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
        instance.setLastProcessedSeqTxn(committedSeqTxn);
        instance.setAppliedWatermark(committedSeqTxn);
        boolean lvConsumedPersisted = false;
        try {
            engine.advanceLiveViewConsumedSeqTxn(
                    instance.getLiveViewToken(),
                    committedSeqTxn,
                    blockFileWriter,
                    path
            );
            lvConsumedPersisted = true;
        } catch (CairoException e) {
            LOG.critical().$("could not advance live view consumed seqTxn after O3 resume replay [view=")
                    .$(viewName)
                    .$(", advanceTo=").$(committedSeqTxn)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            persistState(instance);
        }
        if (lvConsumedPersisted && appendedRows > 0) {
            // Seal the post-replay state as a fresh head and keep the anchor the
            // replay was built on in the retained-checkpoint ring: the late row sat
            // above it, so it stays sealed and serves as a valid resume anchor
            // rather than garbage. force writes past the cadence gate - on a head-hit
            // the anchor (prior head) is not cleared, so firstCp would be false; on a
            // bounded-miss the invalidate above cleared the head, so firstCp is
            // already true. Either way an O3 resume must advance the head or the next
            // replay re-scans from the stale maxTs.
            //
            // The zero-row replay keeps its anchor as the head instead: the
            // truncating commit above left the LV table holding exactly the rows the
            // anchor covers, and the restore left the window state at the anchor's
            // snapshot moment, so it still describes the view. There is nothing to
            // seal (replayMaxTs is LONG_NULL).
            maybeWriteHeadCheckpoint(instance, windowFactory, committedSeqTxn, replayMaxTs, appendedRows, true);
        }
        // The resume replay is "the win": bounded to the tail above the anchor.
        // Counted separately from the boundary rebuild so live_views() can show how
        // much O3 work stays cheap versus the residual unbounded fallbacks.
        instance.bumpO3ResumeReplayRows(appendedRows);
        // Baseline scan-cost signal: base rows this resume replay pulled (>= emit).
        instance.bumpO3ReplayScanRows(o3ScanRows);
        // applyAheadGap = the seqTxns ApplyWal2TableJob raced past the O3 trigger
        // (0 on the common path); the anchor fields record which sealed checkpoint the
        // resume rolled back to, so a wide gap or a distant anchor is diagnosable.
        LOG.info().$("live view O3 resume replay completed [view=")
                .$(viewName)
                .$(", advanceTo=").$(committedSeqTxn)
                .$(", anchorLvSeqTxn=").$(anchorLvSeqTxn)
                .$(", anchorMaxTs=").$(anchorMaxTs)
                .$(", applyAheadGap=").$(effectiveSeqTxn - advanceTo)
                .$(", rowsEmitted=").$(appendedRows).I$();
    }

    /**
     * Head-miss replay path: discards every window-function
     * partition map and the anchor map, opens the base table at applied
     * watermark &gt;= {@code advanceTo}, drives the compiled SELECT's
     * filter / anchor / window cursor stack over the {@code TableReader}'s
     * ts-sorted view starting from {@code viewLowerBoundTimestamp}, emits
     * a single REPLACE_RANGE commit covering everything from the lower
     * bound through positive infinity, and applies inline.
     * <p>
     * Cost is O(retained_rows x n_window_functions) of {@code computeNext}
     * plus the partition-rewrite I/O - acceptable for short-lived views
     * but several seconds to minutes for long-lived ones per the
     * cost model. The head-hit branch (follow-up commit) will avoid the
     * worst of this by starting from the head's {@code maxTimestamp}.
     * <p>
     * Apply-lag handling: a base-table {@code TableReader} obtained right
     * after detection may not yet reflect {@code advanceTo} because the
     * global {@code ApplyWal2TableJob} runs asynchronously. The replay
     * polls until the reader's {@code getSeqTxn() >= advanceTo}, bounded
     * by {@code cairo.live.view.flush.retry.max.duration} so a stalled
     * apply trips the flush-retry budget rather than spinning forever.
     * <p>
     * {@code fullRebuild} distinguishes a wholesale rebuild (restart restore,
     * corrupt-checkpoint restore, base-metadata-drift / mid-drain recovery, WAL-loss
     * re-derive) from an incremental O3 trigger. A full rebuild recomputes the entire
     * view from the applied base, so its REPLACE_RANGE must cover the whole view range
     * ({@code [viewLowerBoundTimestamp, +inf)}) to purge ANY stale on-disk row - notably
     * a below-frontier dedup replacement that dropped a row out of the {@code WHERE}
     * filter, whose stale pre-replacement row sits below the recompute's lowest surviving
     * ts and would otherwise survive the {@code replayMinTs}-floored replace. The
     * incremental path ({@code fullRebuild == false}) keeps the trigger-clamped floor so a
     * non-DATA removal (DROP PARTITION / TTL / TRUNCATE) still freezes its prefix.
     */
    private void o3HeadMissReplay(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lateRowTs,
            TableToken baseToken,
            long advanceTo,
            boolean fullRebuild
    ) throws SqlException {
        final String viewName = instance.getDefinition().getViewName();
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
        // The DATA trigger's authority to DELETE, expressed in the view's own coordinate space.
        // A DATA commit authorises the replay to erase output at or above its lowest touched
        // timestamp; a non-DATA / recovery trigger (LONG_NULL) authorises no deletion at all and
        // leaves the frozen-prefix rule to govern (DROP PARTITION / TTL / TRUNCATE / restart).
        //
        // Clamping UP to the view's lower bound is what makes the trigger usable once a finite
        // START FROM boundary is in play: a commit routinely reaches below the bound (its
        // sub-boundary rows are simply not the view's), and the raw lateRowTs is then below every
        // row the view could own. Abandoning the extension in that case - as this did - left the
        // deletion floored at replayMinTs, the recompute's lowest SURVIVING row, so a replacement
        // that dropped the lowest surviving row out of the view (a dedup upsert that fails the
        // WHERE, say) put replayMinTs ABOVE the row it removed and the stale output row lived on.
        // The bound is the lowest ts the view can hold, so it is the correct floor for the
        // trigger, and for a BEGINNING view (bound == LONG_NULL == Long.MIN_VALUE) the clamp is
        // an identity.
        final long triggerLowTs = lateRowTs == Numbers.LONG_NULL
                ? Numbers.LONG_NULL
                : Math.max(lateRowTs, viewLowerBoundTimestamp);
        TableReader reader = waitForApply(baseToken, advanceTo);
        // The replay recomputes the whole view from the base reader's snapshot,
        // which reflects every base row applied up to reader.getSeqTxn() - not
        // just the O3 trigger seqTxn (advanceTo). When ApplyWal2TableJob has run
        // ahead, that snapshot already incorporates seqTxns past advanceTo, and
        // the head-miss scan from viewLowerBoundTimestamp materialises all of
        // them. Advancing the watermarks to this effective seqTxn (rather than
        // advanceTo) keeps the LV's processed/consumed point in step with what
        // the replay actually wrote; otherwise the forward path re-reads those
        // already-materialised seqTxns and a trailing in-order commit (e.g. a
        // lone row at the global max) re-appends a duplicate row.
        final long effectiveSeqTxn = reader.getSeqTxn();
        boolean readerAttached = false;
        long appendedRows = 0;
        long o3ScanRows = 0;
        // True when the zero-surviving-row path issued a pure-delete
        // REPLACE_RANGE to clear ghost rows (appendedRows stays 0 there, but the
        // apply + on-disk row-count re-read below still have to run).
        boolean deletedGhostRange = false;
        long replayMaxTs = Numbers.LONG_NULL;
        // Minimum output ts the replay actually produced (rows arrive
        // ts-ascending, so the first appended row is the minimum). Base of the
        // REPLACE_RANGE low boundary decided at the commit site below.
        long replayMinTs = Numbers.LONG_NULL;
        // The reader is pinned above, but everything that can throw off it -
        // computeApplyAheadMinTs (opens the base WAL-E) and the retire publication -
        // runs INSIDE this try so the finally always returns it to the pool. A WAL-E
        // read fault on the apply-ahead floor (a torn / purged base WAL-E) would
        // otherwise unwind past the bare reader and leak one pooled reader per refresh
        // attempt, since an aborted replay never advances the watermark and the fault
        // re-fires every cycle until the flush-retry budget trips.
        try {
            // Retire the checkpoints this O3 has unsealed. A DATA trigger keeps every
            // entry with maxTs < the retire floor (still sealed - no un-incorporated
            // base row sits at or below them) and drops the rest, including the head; a
            // non-DATA / recovery trigger (LONG_NULL) drops the whole ring
            // conservatively. Clearing the head puts the post-replay write on its
            // first-cp path. The follow-up write below seals a fresh head; until then a
            // restart rebuilds from the boundary.
            //
            // The floor is derived only after pinning the reader so it can account for
            // apply-ahead: when ApplyWal2TableJob has raced past the trigger, the
            // snapshot this rebuild materialises includes seqTxns in (advanceTo,
            // effectiveSeqTxn] the ring's entries predate, so a back-dated row among
            // them at ts M un-seals every entry with maxTs >= M just as the trigger
            // does. Lower the floor to that ahead range's minimum in-view ts
            // (min(triggerLowTs, minAheadTs)); leaving it at triggerLowTs would strand a
            // poisoned survivor in [minAheadTs, triggerLowTs) that a later resume could
            // anchor on. An unresumable ahead range (structural / non-DATA commit) drops
            // the whole ring (LONG_NULL). A non-DATA trigger is already the whole-ring
            // case, so it needs no adjustment.
            long retireLowTs = triggerLowTs;
            if (effectiveSeqTxn != advanceTo && triggerLowTs != Numbers.LONG_NULL) {
                final long minAheadTs = computeApplyAheadMinTs(baseToken, advanceTo, effectiveSeqTxn, viewLowerBoundTimestamp);
                retireLowTs = minAheadTs == Numbers.LONG_NULL
                        ? Numbers.LONG_NULL
                        : Math.min(triggerLowTs, minAheadTs);
            }
            // The retire publishes the survivors durably at effectiveSeqTxn - this
            // rebuild's commit point - before the commit below, off the same
            // retireLowTs that drives the in-memory drop. A non-DATA / recovery
            // trigger empties the ring, so its publication is an empty manifest and a
            // crash mid-rebuild leaves no anchor to select.
            invalidateRetainedCheckpointsOnO3(instance, retireLowTs, effectiveSeqTxn);

            engine.detachReader(reader);
            executionContext.of(reader);
            readerAttached = true;

            RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
            final Function filter = filterFactory.getFilter();
            final PageFrameRecordCursorFactory pageFrameFactory =
                    (PageFrameRecordCursorFactory) (filter != null ? filterFactory.getBaseFactory() : filterFactory);
            RecordMetadata outMetadata = windowFactory.getMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();

            // Both scans below open the snapshot AT the START FROM boundary rather than
            // scanning up to it, the same inclusive-lower-bound cursor the seed and the
            // forward drain take: it culls whole partitions and binary-searches into the
            // first one instead of walking the sub-boundary history row by row. A view with
            // a finite boundary over a long-lived base has that history in front of it on
            // every rebuild - and a rebuild fires on any O3 commit, base metadata drift,
            // mid-drain failure, corrupt checkpoint or checkpoint-less restart - so the
            // walk was paid twice per rebuild (probe + recompute). BEGINNING persists
            // Numbers.LONG_NULL (= Long.MIN_VALUE), which the cursor turns into a full scan.

            // Probe pass: open a separate cursor over the same source + filter
            // chain and check whether any row survives. Skipping the wipe when
            // no rows pass the filter prevents a degenerate replay (e.g. WHERE
            // discards every row in the replay window) from permanently
            // erasing cumulative accumulator state for every partition.
            final boolean hasReplayRow;
            try (RecordCursor probeCursor = pageFrameFactory.getCursorFromTimestamp(executionContext, viewLowerBoundTimestamp)) {
                RecordCursor probeSource = probeCursor;
                if (filter != null) {
                    filteringCursor.of(probeSource, filter, executionContext);
                    probeSource = filteringCursor;
                }
                hasReplayRow = probeSource.hasNext();
            }

            if (hasReplayRow) {
                // Reset per-function accumulator state and the anchor map to
                // identity. The compiled factory's WindowFunction instances
                // stay live so the cursor chain below can reuse them; only
                // their accumulated state resets. clearWindowState rewinds via
                // toTop(), not a bare partition-map clear, so no-partition
                // ranking like row_number() OVER () - whose counter lives in a
                // scalar field with no map - also rewinds; otherwise it would
                // accumulate across head-miss replays.
                clearWindowState(windowFactory, anchorWindow);

                try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                    RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
                    try (RecordCursor pageCursor = pageFrameFactory.getCursorFromTimestamp(executionContext, viewLowerBoundTimestamp)) {
                        RecordCursor source = pageCursor;
                        if (filter != null) {
                            filteringCursor.of(source, filter, executionContext);
                            source = filteringCursor;
                        }
                        if (anchorWindow != null) {
                            anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                            source = anchorDispatchingCursor;
                        }
                        try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                            Record outRecord = windowCursor.getRecord();
                            while (windowCursor.hasNext()) {
                                long ts = outRecord.getTimestamp(cursorTimestampIndex);
                                if (replayMinTs == Numbers.LONG_NULL) {
                                    // First (= lowest) output row of the replay.
                                    replayMinTs = ts;
                                }
                                if (replayMaxTs == Numbers.LONG_NULL || ts > replayMaxTs) {
                                    replayMaxTs = ts;
                                }
                                // Re-stamp the O3 detection watermark off the
                                // post-window output so any subsequent O3 in
                                // the same worker cycle is caught against the
                                // just-rebuilt state.
                                instance.setLatestSeenTs(ts);
                                TableWriter.Row row = walWriter.newRow(ts);
                                copier.copy(executionContext, outRecord, row);
                                row.append();
                                appendedRows++;
                            }
                            // Capture base rows scanned before the cursor chain closes
                            // (FilteringRecordCursor.close() resets its counter). No
                            // filter -> scan equals emit; a filter makes scan exceed
                            // emit by the rows it dropped.
                            o3ScanRows = filter != null ? filteringCursor.getBaseRowsConsumed() : appendedRows;
                        }

                        if (appendedRows > 0) {
                            // REPLACE_RANGE low boundary. replayMinTs alone freezes the
                            // prefix when the base lost rows below it (DROP PARTITION /
                            // TTL / TRUNCATE - intended). But a below-frontier dedup
                            // replacement that drops the lowest result row via the filter
                            // leaves the base row present, so replayMinTs jumps above it
                            // and the stale LV row would survive. Extend down to the
                            // trigger ts (lowest triggering DATA-commit ts, clamped to the
                            // view's bound); removals are non-DATA and excluded from it, so
                            // frozen prefixes stay safe. A full rebuild has no single
                            // trigger - it recomputes the whole view - so it replaces the
                            // entire view range to purge any stale below-frontier row.
                            final long replaceLowTs = fullRebuild
                                    ? viewLowerBoundTimestamp
                                    : triggerLowTs != Numbers.LONG_NULL
                                      ? Math.min(replayMinTs, triggerLowTs)
                                      : replayMinTs;
                            fencedLiveViewCommit(() -> walWriter.commitLiveViewWithReplaceRange(
                                    effectiveSeqTxn,
                                    replaceLowTs,
                                    Long.MAX_VALUE
                            ));
                        }
                    }
                }
            } else if (fullRebuild || triggerLowTs != Numbers.LONG_NULL) {
                // The probe found no surviving row, but the view must still be cleared:
                //  - a convergent DATA trigger (a dedup/replacement whose lowest touched ts
                //    is triggerLowTs) genuinely empties the view from triggerLowTs upward; or
                //  - a full rebuild recomputed the whole view to empty, so every on-disk row
                //    is stale and the whole range [viewLowerBoundTimestamp, +inf) must go.
                // Leaving the block a no-op strands the pre-O3 output rows on disk as ghosts -
                // size() over-reports and reads return stale rows while the watermark advances
                // past the commit that removed their base rows. Reset the window accumulators to
                // identity (matching the from-scratch empty recompute) and emit a pure-delete
                // REPLACE_RANGE over [deleteLowTs, +inf) so the on-disk range is cleared. For the
                // DATA trigger, rows below triggerLowTs stay frozen, exactly as the surviving-row
                // boundary above treats them.
                //
                // A non-DATA / recovery trigger (lateRowTs == LONG_NULL) that is NOT a full
                // rebuild keeps the no-op: without a convergent trigger ts the emptiness is a
                // frozen prefix (DROP PARTITION / TTL / TRUNCATE), not a deletion to propagate,
                // and the pre-O3 accumulator state must survive.
                final long deleteLowTs = fullRebuild ? viewLowerBoundTimestamp : triggerLowTs;
                clearWindowState(windowFactory, anchorWindow);
                try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                    fencedLiveViewCommit(() -> walWriter.commitLiveViewWithReplaceRange(
                            effectiveSeqTxn,
                            deleteLowTs,
                            Long.MAX_VALUE
                    ));
                }
                deletedGhostRange = true;
                LOG.info().$("live view O3 head-miss replay cleared emptied range [view=")
                        .$(viewName)
                        .$(", deleteLowTs=").$(deleteLowTs)
                        .$(", effectiveSeqTxn=").$(effectiveSeqTxn).I$();
            }
        } finally {
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
            reader.close();
        }

        if (appendedRows > 0 || deletedGhostRange) {
            applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
            // Re-read the on-disk row count: the REPLACE_RANGE only rewrites the
            // band at or above its low boundary and may have preserved a frozen
            // prefix below it (or, on the pure-delete path, cleared the band
            // outright), so the head-miss output is no longer a pure
            // from-scratch rebuild. Sourcing the lifetime counter from the table
            // keeps the head checkpoint's lvRowPosition (written below)
            // consistent in both the intact-base and base-data-removed cases.
            try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
                instance.setLvRowsTotal(lvReader.size());
            }
        }
        instance.setLastProcessedSeqTxn(effectiveSeqTxn);
        instance.setAppliedWatermark(effectiveSeqTxn);
        boolean lvConsumedPersisted = false;
        try {
            engine.advanceLiveViewConsumedSeqTxn(
                    instance.getLiveViewToken(),
                    effectiveSeqTxn,
                    blockFileWriter,
                    path
            );
            lvConsumedPersisted = true;
        } catch (CairoException e) {
            LOG.critical().$("could not advance live view consumed seqTxn after O3 replay [view=")
                    .$(viewName)
                    .$(", advanceTo=").$(effectiveSeqTxn)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            persistState(instance);
        }
        if (lvConsumedPersisted && appendedRows > 0) {
            // Post-replay head: invalidateRetainedCheckpointsOnO3 cleared the head
            // metadata and dropped the unsealed ring entries above, so force
            // writes a fresh head reflecting the post-replay state (firstCp is
            // already true here; force keeps the intent explicit and robust).
            // Restart can then short-circuit to head-hit for a subsequent O3 in
            // the head's hit zone instead of paying for another full head-miss
            // replay.
            //
            // Pass 0 appendedRows: lvRowsTotal already includes them (sourced
            // from the on-disk size above), so adding them again would
            // double-count lvRowPosition. Mirrors the seed-completion path.
            maybeWriteHeadCheckpoint(instance, windowFactory, effectiveSeqTxn, replayMaxTs, 0L, true);
        }
        // The boundary rebuild is the residual O(view age) fallback (late row below
        // the whole retained ring, or a deep / unresumable apply-ahead range). Counted
        // separately from the resume path so a growing value in live_views() flags a
        // view the ring is failing to bound.
        instance.bumpO3BoundaryReplayRows(appendedRows);
        // Baseline scan-cost signal: base rows this boundary rebuild pulled (>= emit).
        instance.bumpO3ReplayScanRows(o3ScanRows);
        // applyAheadGap = the seqTxns ApplyWal2TableJob raced past the O3 trigger
        // (effectiveSeqTxn - advanceTo); a wide gap is what forces the rebuild when no
        // sealed anchor sits below the ahead range's minimum in-view ts.
        LOG.info().$("live view O3 head-miss replay completed [view=")
                .$(viewName)
                .$(", advanceTo=").$(effectiveSeqTxn)
                .$(", applyAheadGap=").$(effectiveSeqTxn - advanceTo)
                .$(", rowsEmitted=").$(appendedRows).I$();
    }

    /**
     * Resets all live-view window state in place to identity: every window
     * function and the anchor map. The compiled factory's
     * {@code WindowFunction} instances stay live so the cursor chain can reuse
     * them; only the accumulated state resets. Used before a from-scratch
     * re-sweep so a partial or failed checkpoint restore cannot leave drift.
     * <p>
     * Each function is rewound via {@link WindowFunction#toTop()} rather than a
     * bare {@link WindowFunction#getPartitionMap()} clear: a no-partition
     * ranking function such as {@code row_number() OVER ()} keeps its counter
     * in a scalar field and has no partition map, so clearing only the map
     * would leave that counter intact and it would accumulate across replays
     * (emitting row numbers above the row count). {@code toTop()} is the
     * canonical full reset every window function already implements for cursor
     * re-iteration; for partitioned functions it clears the map too.
     */
    protected static void clearWindowState(WindowRecordCursorFactory windowFactory, LiveViewWindow anchorWindow) {
        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).toTop();
        }
        if (anchorWindow != null) {
            anchorWindow.toTop();
        }
    }

    /**
     * Drives one turn of the initial seed sweep for a view in SEEDING state.
     * The sweep is resumable and yields on the turn budget so a long history
     * does not starve other views sharing the worker pool. One
     * {@code runSeedSweep} call is one turn; the fallback scan re-enqueues
     * the view while it stays SEEDING.
     * <p>
     * Every start mode seeds, and the seed is what makes the view's contents independent of
     * how a row arrived. It feeds the base snapshot rows whose designated timestamp sits at
     * or above {@code viewLowerBoundTimestamp} - the identical predicate the forward-append
     * path and every applied-base replay apply - so a pre-CREATE row above the boundary is in
     * the view from the moment the seed completes, rather than materialising later when an O3
     * commit or a restart happens to trigger a replay. START FROM BEGINNING has no boundary
     * and sweeps the whole history; START FROM NOW over a base holding only past data
     * qualifies nothing and completes in its first turn.
     * <ul>
     *     <li>The first turn of a process resumes window state + the data-cursor
     *     offset from the surviving {@code .scp} (restart mid-sweep), or starts
     *     from offset 0 with empty state (fresh CREATE, or no usable {@code
     *     .scp}). Later turns continue from the in-memory window state + offset
     *     ({@code getIncrementalCursor} preserves accumulated state across
     *     turns), so no per-turn restore is needed.</li>
     *     <li>The first turn pins ONE MVCC base snapshot (an
     *     {@link LiveViewInstance#getSeedBaseReader() instance-held reader}) at
     *     {@code sweepSeqTxn >= seedTargetSeqTxn} and every turn reads that same
     *     snapshot; re-opening at the latest applied seqTxn each turn would make the
     *     positional {@code skipRows()} resume unsound under concurrent out-of-order
     *     base commits (they reorder physical rows below the swept prefix). Each turn
     *     {@code skipRows()} past already-swept rows, feeds up to a row/duration
     *     budget, commits the batch, applies it, and writes a {@code .scp} on the
     *     checkpoint cadence.</li>
     *     <li>On cursor exhaustion the turn flips {@code seedState} to ACTIVE,
     *     writes a steady head {@code .cp} from the now-complete state, releases the
     *     pinned snapshot, and retires the {@code .scp}; the next tick begins the
     *     deferred drain from {@code sweepSeqTxn + 1}, where the ACTIVE phase's O3
     *     detection materialises anything the base committed after the snapshot.</li>
     * </ul>
     * Crash idempotency: the on-disk output is a deterministic prefix of the
     * eventual result, so a re-feed past the last {@code .scp} recomputes rows
     * already on disk to advance state but skips their WAL append
     * ({@code skipWriteUntil}). A crash before any {@code .scp} re-sweeps from
     * offset 0 and skip-writes the entire stale prefix. The resume applies any
     * committed-but-unapplied block first, so that prefix - and the floor read
     * off it - covers every block the sweep has already committed.
     */
    private void runSeedSweep(LiveViewInstance instance) throws SqlException {
        final long seedTargetSeqTxn = instance.getStateReader().getSeedTargetSeqTxn();
        final String viewName = instance.getDefinition().getViewName();
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        final WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        // The view's START FROM boundary. The seed feeds exactly the snapshot rows at or
        // above it - the same predicate the forward path and the replay paths apply.
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();

        // Defensive backstop. CREATE only lands a view in SEEDING when the base has a
        // committed transaction to seed from (baseHeadSeqTxn > 0), so a SEEDING view should
        // always carry a real target. A view that reaches here without one has nothing to
        // sweep; flip it ACTIVE and let the incremental drain take over on the next tick,
        // rather than handing a negative target to waitForApply.
        if (seedTargetSeqTxn < 0) {
            instance.setSeedState(LiveViewState.SEED_STATE_ACTIVE);
            instance.setSeedTargetSeqTxn(Numbers.LONG_NULL);
            persistState(instance);
            LOG.info().$("live view seed sweep skipped, no base history to seed [view=")
                    .$(viewName).I$();
            return;
        }

        // Resume setup, once per process on the first seed turn: establish
        // the in-memory data offset, window state, latestSeenTs, and the
        // persistent skip-write floor. Later turns inherit all of these in
        // memory (the per-turn budget can split the skip-write catch-up across
        // turns, so the floor must persist - it is an instance field).
        if (!instance.isSeedResumeAttempted()) {
            // Apply any LV WAL block that committed but never applied before reading the
            // row count below. A sweep turn commits its block and applies it as two steps,
            // so a crash in between leaves the block committed-but-unapplied; the global
            // apply path skips live-view tokens, and reconcileAppliedFloorAfterRestart
            // covers ACTIVE views only, so nothing else lands it. The unapplied rows sit
            // outside lvReader.size(), so the skip-write floor would fall below them: the
            // resumed sweep re-emits those rows and the pending block then applies on top
            // of them, duplicating (the seed append carries no dedup to collapse it).
            // Applying first folds the block into the count the floor derives from.
            // Idempotent on a healthy restart - applyWalDirect finds nothing pending.
            // Runs before the resume-attempted flag is stamped so a failure here re-enters
            // this block on the next turn rather than resuming off an under-read floor.
            applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
            // applyWalDirect is void and non-throwing: it silently no-ops when the LV writer is busy
            // (EntryUnavailableException) and suspends-then-swallows on an apply error, leaving the
            // committed block unapplied. Reading the skip-write floor off lvReader.size() below would
            // then under-count it, the resumed sweep would re-emit those rows, and they would
            // duplicate when the block later applies (the seed append carries no dedup to collapse
            // them). Only stamp the single-shot resume flag and derive the floor once the LV writer
            // has actually caught up to its committed seqTxn; otherwise leave the view SEEDING with
            // the flag unset so the fallback scan re-enqueues it and the next turn re-attempts the
            // apply (a genuinely suspended LV table then blocks the seed until RESUME - correct, and
            // strictly better than duplicating).
            final SeqTxnTracker lvTracker = engine.getTableSequencerAPI().getTxnTracker(instance.getLiveViewToken());
            if (lvTracker.isInitialised() && lvTracker.getSeqTxn() > lvTracker.getWriterTxn()) {
                return;
            }
            instance.setSeedResumeAttempted();
            long onDiskLvRows = 0;
            try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
                onDiskLvRows = lvReader.size();
            } catch (CairoException e) {
                // No readable LV table yet (fresh CREATE before first apply).
                onDiskLvRows = 0;
            }
            // Always start from a clean slate; restore (if any) repopulates on top.
            clearWindowState(windowFactory, anchorWindow);
            final long scpKey = instance.getHeadSeedCpKey();
            boolean restored = false;
            if (scpKey != Numbers.LONG_NULL
                    && restoreFromHead(instance, windowFactory, scpKey, true, restoredHeadState)
                    && restoredHeadState.resumeDataOffset != Numbers.LONG_NULL) {
                // A surviving .scp can be AHEAD of the on-disk LV output. A checkpoint
                // restore no longer produces one - TableSnapshotRestore wipes the live
                // _checkpoints/ dir and lays the snapshot's back down, so the restored
                // .scp matches the rolled-back _txn/partitions/_lv.s - but a backup that
                // omits the dir, or a crash between the .scp write and the LV commit,
                // still can: the live-ahead .scp (lvRowsTotal = R_bcp) outlives the disk
                // it describes (onDiskLvRows = R_cp < R_bcp). Resuming from it would jump
                // the data cursor past the base rows that produced R_cp..R_bcp while
                // lvRowsTotal starts at R_bcp, so those LV output rows would be neither on
                // disk nor re-swept - a permanent silent gap. Reject the ahead .scp and
                // fall through to the from-0 re-sweep below, where the skip-write floor
                // keeps the R_cp on-disk prefix and re-emits everything above it.
                if (restoredHeadState.lvRowsTotal <= onDiskLvRows) {
                    instance.setSeedDataOffset(restoredHeadState.resumeDataOffset);
                    instance.setLvRowsTotal(restoredHeadState.lvRowsTotal);
                    if (restoredHeadState.maxTimestamp != Numbers.LONG_NULL) {
                        instance.setLatestSeenTs(restoredHeadState.maxTimestamp);
                    }
                    restored = true;
                } else {
                    // restoreFromHead already wrote the ahead window state into
                    // the functions; wipe it back to identity for the from-0
                    // re-sweep, and unlink the ahead .scp so a later restart's
                    // highest-key sweepSeedCheckpoints does not re-select it
                    // (its data-offset key is larger than the re-sweep's fresh
                    // .scp keys). unlinkSeedCheckpoint also clears the
                    // in-memory head key.
                    clearWindowState(windowFactory, anchorWindow);
                    unlinkSeedCheckpoint(instance);
                    LOG.info().$("live view discarding seed checkpoint ahead of restored on-disk output [view=")
                            .$(viewName).$(", scpLvRows=").$(restoredHeadState.lvRowsTotal)
                            .$(", onDiskLvRows=").$(onDiskLvRows).I$();
                }
            }
            if (!restored) {
                // Fresh CREATE, no .scp, corrupt .scp, or a .scp rejected as
                // ahead of the restored disk: re-sweep from offset 0 with empty
                // state. The on-disk prefix (if any) is a deterministic match,
                // kept via skip-write below.
                //
                // Re-clear the window state unconditionally: a restoreFromHead that
                // threw partway (e.g. a short .scp missing a trailing FUNCTION_SNAPSHOT
                // block) has already written the anchor + some functions into the live
                // window before failing, and getIncrementalCursor keeps accumulator
                // state across the re-sweep, so feeding that half-restored state into a
                // from-0 sweep would double-count. The ahead-rejection branch above
                // already re-clears; this covers the throw path. Cheap and idempotent
                // for the fresh / no-.scp cases (nothing was restored).
                clearWindowState(windowFactory, anchorWindow);
                instance.setSeedDataOffset(0);
                instance.setLvRowsTotal(0);
                instance.setHeadSeedCpKey(Numbers.LONG_NULL);
            }
            // On-disk output is append-only (>= the restored row count), so the
            // skip-write floor is simply the on-disk row count: rows re-fed
            // below it are recomputed to advance state but not re-appended.
            instance.setSeedSkipWriteFloor(onDiskLvRows);
        }

        final long skipWriteUntil = instance.getSeedSkipWriteFloor();
        long dataOffset = instance.getSeedDataOffset();

        // Pin ONE stable base snapshot for the entire multi-turn sweep. Opened lazily
        // on the first turn (or after a fresh-snapshot re-arm) and held on the instance
        // across turns. Re-opening the base at the latest applied seqTxn each turn (as
        // this did before) makes the positional skipRows() resume unsound: an
        // out-of-order base commit landing below the swept prefix between turns
        // shifts physical row positions, so the next turn's skipRows(dataOffset) skips
        // a different set - silently dropping the back-dated row and re-feeding the old
        // boundary row (double-advancing the accumulators). Holding one snapshot keeps
        // the physical order stable across turns; everything committed after it is
        // handed to the ACTIVE phase's O3 detection from sweepSeqTxn + 1.
        //
        // Lazily null-guarded rather than folded into the isSeedResumeAttempted
        // block above: waitForApply can throw (apply-lag timeout), and the flag is
        // stamped before it. Gating the open on a null reader instead re-attempts it
        // on the next turn without re-running the window-state restore.
        TableReader reader = instance.getSeedBaseReader();
        if (reader == null) {
            reader = waitForApply(baseToken, seedTargetSeqTxn);
            instance.setSeedBaseReader(reader);
            // The reader may sit at a seqTxn strictly greater than the target if
            // ApplyWal2TableJob caught up further while waitForApply was running;
            // sweepSeqTxn pins the deferred drain to resume from after the snapshot.
            instance.setSeedSweepSeqTxn(Math.max(seedTargetSeqTxn, reader.getSeqTxn()));
        }
        final long sweepSeqTxn = instance.getSeedSweepSeqTxn();

        final long turnMaxRows = engine.getConfiguration().getLiveViewCheckpointRows();
        final long turnMaxDurationUs = engine.getConfiguration().getLiveViewRefreshTurnMaxDurationMicros();

        long batchMaxTs = Numbers.LONG_NULL;
        long lvRows = instance.getLvRowsTotal();
        long appendedThisTurn = 0;
        long processedThisTurn = 0;
        boolean yielded = false;
        boolean readerBound = false;
        try {
            // The pinned reader is borrowed (not detached), so the base SELECT reads a
            // copy at the reader's fixed snapshot txn via getReaderAtTxn's copy path.
            // It is NOT closed per turn: it stays pinned across turns and is released on
            // sweep completion (below) or by the drop/invalidate/shutdown hooks.
            executionContext.of(reader);
            readerBound = true;

            RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
            final Function filter = filterFactory.getFilter();
            final PageFrameRecordCursorFactory pageFrameFactory =
                    (PageFrameRecordCursorFactory) (filter != null ? filterFactory.getBaseFactory() : filterFactory);
            RecordMetadata outMetadata = windowFactory.getMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();
            if (cursorTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(viewName).put(']');
            }

            try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
                // Open the snapshot AT the START FROM boundary rather than scanning up to it:
                // the same inclusive-lower-bound cursor the forward path takes, which culls
                // whole partitions and binary-searches into the first one instead of walking
                // the sub-boundary history row by row. That matters because a seed is the
                // common case now - START FROM NOW over a base of past data qualifies nothing,
                // and a row-by-row walk to the boundary would scan the entire base inside a
                // single cursor call, with no turn budget able to interrupt it.
                //
                // BEGINNING persists Numbers.LONG_NULL (= Long.MIN_VALUE) as its boundary,
                // which cullPartitions special-cases into a full scan. dataOffset counts rows
                // of THIS cursor, and the bound plus the pinned snapshot are the same on every
                // turn, so the row numbering skipRows() resumes on is stable.
                try (RecordCursor pageCursor = pageFrameFactory.getCursorFromTimestamp(executionContext, viewLowerBoundTimestamp)) {
                    RecordCursor source = pageCursor;
                    if (filter != null) {
                        filteringCursor.of(source, filter, executionContext);
                        source = filteringCursor;
                    }
                    if (anchorWindow != null) {
                        anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                        source = anchorDispatchingCursor;
                    }
                    try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                        // getIncrementalCursor() rewinds the whole cursor chain
                        // (super.of() calls baseCursor.toTop()), so skip past the
                        // already-swept rows AFTER it is built, not before. The
                        // window functions already hold the state for those rows.
                        if (dataOffset > 0) {
                            seedSkipCounter.set(dataOffset);
                            pageCursor.skipRows(seedSkipCounter, RecordCursor.UNBOUNDED_ROW_COUNT);
                        }
                        Record outRecord = windowCursor.getRecord();
                        while (windowCursor.hasNext()) {
                            long ts = outRecord.getTimestamp(cursorTimestampIndex);
                            if (batchMaxTs == Numbers.LONG_NULL || ts > batchMaxTs) {
                                batchMaxTs = ts;
                            }
                            instance.setLatestSeenTs(ts);
                            // hasNext() already advanced the accumulators for this row, so the window
                            // state now leads the last durable commit. Mark it dirty like both
                            // active-drain loops do: without this, handleRefreshFailure's guard is
                            // false while seeding and its SEEDING recovery never runs, so a
                            // mid-sweep failure (uncommitted WAL rows roll back, the accumulator
                            // advance does not) re-feeds the same rows next turn from the unchanged
                            // dataOffset, double-advancing every window value from there on.
                            windowStateDirty = true;
                            // Skip-write: rows already on disk (outPos below the
                            // floor) are recomputed to advance window state but
                            // not re-appended; rows at/above it are emitted.
                            if (lvRows >= skipWriteUntil) {
                                TableWriter.Row row = walWriter.newRow(ts);
                                copier.copy(executionContext, outRecord, row);
                                row.append();
                                appendedThisTurn++;
                            }
                            lvRows++;
                            processedThisTurn++;
                            if (processedThisTurn >= turnMaxRows
                                    || engine.getConfiguration().getMicrosecondClock().getTicks() - turnStartUs >= turnMaxDurationUs) {
                                yielded = true;
                                break;
                            }
                        }
                        // Capture the base-cursor advance BEFORE the cursor
                        // chain closes: windowCursor.close() cascades to
                        // filteringCursor.close(), which resets its
                        // base-rows-consumed counter.
                        dataOffset += (filter != null ? filteringCursor.getBaseRowsConsumed() : processedThisTurn);
                    }
                    if (appendedThisTurn > 0) {
                        fencedLiveViewCommit(() -> walWriter.commitLiveView(sweepSeqTxn));
                    }
                }
            }
        } finally {
            if (readerBound) {
                executionContext.clearReader();
            }
        }

        instance.setLvRowsTotal(lvRows);
        instance.setSeedDataOffset(dataOffset);
        if (appendedThisTurn > 0) {
            applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
        }

        if (yielded) {
            // More to sweep: persist a resumable checkpoint on cadence, then
            // yield. The fallback scan re-enqueues (needsSeeding stays true).
            maybeWriteSeedCheckpoint(instance, windowFactory, dataOffset, batchMaxTs, sweepSeqTxn);
            return;
        }

        // Sweep complete. Materialise the steady head .cp from the now-complete
        // window state (maxTs = overall latestSeenTs, not this possibly-empty
        // final turn's batchMaxTs) so the ACTIVE phase's restart-restore + O3
        // head-hit have an anchor. lvRowsTotal is already maintained above, so
        // pass 0 appendedRows to avoid double-counting it.
        //
        // The head .cp is written BEFORE the _lv.s persist below - deliberately the
        // reverse of every steady-state site, which persists _lv.s first and writes
        // the .cp after. Do not "fix" this to match them. Those sites advance a
        // watermark over rows already on disk under an already-existing head, where a
        // head lagging the watermark is the routine cadence state and replayToApplied
        // closes the gap. This is where the FIRST head is born, and the _lv.s persist
        // is what flips the view durably ACTIVE at sweepSeqTxn. Persisting that first
        // would open a window where a crash leaves an ACTIVE view whose disk table
        // holds the whole swept output but which has no head .cp at all: the restart
        // then finds no head, and on a live primary (base WAL present) the applied-base
        // re-derive does not trigger, so the view drains forward from cold accumulators
        // and durably commits wrong cumulative results. Writing the head first makes
        // every crash window degrade safely - before the _lv.s persist the view is still
        // SEEDING on disk and simply resumes the sweep from its .scp (the orphan
        // .cp, being above the persisted watermark, is unlinked by the startup sweep and
        // was never load-bearing for that resume); after it, the head is already there.
        instance.setLastProcessedSeqTxn(sweepSeqTxn);
        instance.setAppliedWatermark(sweepSeqTxn);
        // Only when the seed actually emitted a row. A seed that qualified none - the normal
        // outcome for START FROM NOW over a base of past data, and for any boundary in the
        // future - has nothing to anchor a head on: latestSeenTs is only stamped per emitted
        // row, so it is still LONG_NULL, and the window accumulators are at identity. Writing
        // a head from that would persist maxTs = LONG_NULL, and the O3 head-hit path floors
        // its replay at headMaxTs + 1: Long.MIN_VALUE + 1 admits every base row, so the first
        // out-of-order commit would replay the whole base into the view, including the
        // sub-boundary rows the view exists to exclude. With no head, that commit routes to
        // the head-miss replay instead, which floors at viewLowerBoundTimestamp; the flush
        // cadence writes the first real head once rows land. An empty view rebuilt from cold
        // accumulators is correct by construction, so the "never leave an ACTIVE view without
        // a head" argument below does not apply here - there is no output to be wrong about.
        final long seedMaxTs = instance.getLatestSeenTs();
        if (seedMaxTs != Numbers.LONG_NULL) {
            maybeWriteHeadCheckpoint(instance, windowFactory, sweepSeqTxn, seedMaxTs, 0L, false);
        }
        instance.setSeedState(LiveViewState.SEED_STATE_ACTIVE);
        instance.setSeedTargetSeqTxn(Numbers.LONG_NULL);
        // Release the pinned base snapshot: the sweep is done and the ACTIVE phase
        // opens its own readers from sweepSeqTxn + 1. Runs under the refresh latch,
        // so no concurrent turn is reading from it.
        instance.freeSeedBaseReader();
        try {
            // Persists seedState=ACTIVE + watermarks durably before the .scp
            // is retired, so a crash between the two recovers as ACTIVE.
            engine.advanceLiveViewConsumedSeqTxn(
                    instance.getLiveViewToken(),
                    sweepSeqTxn,
                    blockFileWriter,
                    path
            );
        } catch (CairoException e) {
            LOG.critical().$("could not advance live view consumed seqTxn after seed sweep [view=")
                    .$(viewName)
                    .$(", sweepSeqTxn=").$(sweepSeqTxn)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            persistState(instance);
        }
        unlinkSeedCheckpoint(instance);
        LOG.info().$("live view seed sweep completed [view=")
                .$(viewName)
                .$(", seedTargetSeqTxn=").$(seedTargetSeqTxn)
                .$(", sweepSeqTxn=").$(sweepSeqTxn)
                .$(", lvRowsTotal=").$(instance.getLvRowsTotal()).I$();
    }

    /**
     * Best-effort unlinks the {@code <lvSeqTxn>.cp} file of every lvSeqTxn in
     * {@code lvSeqTxns} - the entries a prune or a selective O3 invalidation
     * evicted from the retained-checkpoint ring. A missing file is a no-op
     * ({@code removeQuiet}); the in-memory ring already dropped these entries, so
     * an unlink failure only leaks the file until the startup sweep retires it.
     */
    private void unlinkCheckpointFiles(LiveViewInstance instance, LongList lvSeqTxns) {
        final int n = lvSeqTxns.size();
        if (n == 0) {
            return;
        }
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        for (int i = 0; i < n; i++) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                    .slash();
            LiveViewCheckpointWriter.appendCpFileName(path, lvSeqTxns.getQuick(i));
            ff.removeQuiet(path.$());
        }
    }

    /**
     * Retires the rolling seed checkpoint {@code <key>.scp} (best-effort)
     * and clears {@code headSeedCpKey}. Called after the SEEDING ->
     * ACTIVE flip is durable. Leftovers from a crash in the tiny window before
     * this runs are swept at the next startup by {@code sweepSeedCheckpoints}
     * (the view is no longer SEEDING then).
     */
    private void unlinkSeedCheckpoint(LiveViewInstance instance) {
        final long scpKey = instance.getHeadSeedCpKey();
        if (scpKey == Numbers.LONG_NULL) {
            return;
        }
        path.of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .slash();
        LiveViewCheckpointWriter.appendScpFileName(path, scpKey);
        engine.getConfiguration().getFilesFacade().removeQuiet(path.$());
        instance.setHeadSeedCpKey(Numbers.LONG_NULL);
    }

    /**
     * Returns a base-table {@code TableReader} whose {@code getSeqTxn() >=
     * targetSeqTxn}, polling the reader pool until {@code ApplyWal2TableJob}
     * has caught up. Bounded by
     * {@code cairo.live.view.flush.retry.max.duration}; on timeout the
     * caller's flush-retry budget ticks and the view is eventually
     * invalidated via the unified path.
     */
    private TableReader waitForApply(TableToken baseToken, long targetSeqTxn) {
        final long maxWaitUs = engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros();
        final long startUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        TableReader reader = engine.getReader(baseToken);
        while (reader.getSeqTxn() < targetSeqTxn) {
            long elapsedUs = engine.getConfiguration().getMicrosecondClock().getTicks() - startUs;
            if (elapsedUs >= maxWaitUs) {
                long readerSeqTxn = reader.getSeqTxn();
                reader.close();
                throw CairoException.nonCritical()
                        .put("live view base reader apply lag exceeded retry budget [baseToken=")
                        .put(baseToken.getTableName())
                        .put(", targetSeqTxn=").put(targetSeqTxn)
                        .put(", readerSeqTxn=").put(readerSeqTxn)
                        .put(", elapsedUs=").put(elapsedUs)
                        .put(']');
            }
            reader.close();
            Os.sleep(20);
            reader = engine.getReader(baseToken);
        }
        return reader;
    }

    /**
     * Rewrites {@code <lvDir>/_checkpoints/_ring} with the instance's current
     * retained-checkpoint ring as the set of checkpoints proven sealed at
     * {@code coveredBaseSeqTxn} - the single durable-publication point for the
     * ring, so that ring membership, the generation counter and the dirty flag
     * only ever move together and only ever here.
     * <p>
     * {@code coveredBaseSeqTxn} is a claim about the listed entries, not about
     * what the view has consumed: every entry incorporates every base row at or
     * below its own {@code maxTs} from every base commit through
     * {@code coveredBaseSeqTxn}. Both replay paths prove exactly that before
     * they commit - each survivor of the retire sits strictly below
     * {@code min(triggerLowTs, minAheadTs)} - which is why a publication runs
     * <em>ahead</em> of the commit it names and needs no ordering relationship
     * to {@code _lv.s}.
     * <p>
     * Never unlinks: the manifest is an allow-list, so a caller retiring or
     * pruning {@code .cp} files orders its unlinks after the publication that
     * drops them, and a file the manifest does not list is garbage whether or
     * not its unlink lands.
     * <p>
     * Failure logs and returns {@code false}; it never blocks the cycle.
     * {@code coveredBaseSeqTxn} advances only on success, so the on-disk
     * manifest always holds a {@code (membership, covered)} pair that was valid
     * when written, and a restart either finds {@code covered} equal to the
     * reconciled applied floor (the membership really is sealed there, trust it)
     * or does not (fall back to the highest {@code .cp}). Refusing the replay
     * instead would buy nothing and would stall the view outright while
     * {@code _ring} is unwritable.
     *
     * @return {@code true} when the manifest is durable at
     * {@code coveredBaseSeqTxn}, {@code false} when the publication failed.
     */
    private boolean publishCheckpointRing(LiveViewInstance instance, long coveredBaseSeqTxn) {
        // Read-only replicas must not publish, for the reason maybeWriteHeadCheckpoint
        // spells out at its own copy of this assert: _ring is an allow-list over local
        // .cp files a replica never writes, so a replica reaching a publication means a
        // primary-only path lost its gate.
        assert !isLeadReconstruction() : "read-only replica must not publish the live view checkpoint ring";
        // The generation a successful publication will stamp. A failed one
        // leaves it unclaimed for the next attempt: nothing selects on
        // generation, so gaps would be harmless, but a monotone counter with no
        // gaps makes a publication countable from the logs.
        final long generation = instance.getLastPublishedRingGeneration() + 1;
        try {
            if (ringManifestWriter == null) {
                ringManifestWriter = new LiveViewCheckpointRingManifestWriter(engine.getConfiguration());
            }
            // Snapshot the ring rather than publish off the live list: the ring
            // is worker-private with no volatile publication, and the copy is
            // the retention count times ENTRY_SIZE longs (8 x 5 by default).
            ringSnapshot.clear();
            instance.copyRetainedCheckpointsTo(ringSnapshot);
            path.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken());
            ringManifestWriter.publish(path, generation, coveredBaseSeqTxn, ringSnapshot);
        } catch (Throwable t) {
            instance.recordCheckpointRingPublicationFailure();
            LOG.critical().$("could not publish live view checkpoint ring [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", generation=").$(generation)
                    .$(", coveredBaseSeqTxn=").$(coveredBaseSeqTxn)
                    .$(", entries=").$(instance.getRetainedCheckpointCount())
                    .$(", error=").$(t).I$();
            // The writer is reusable after a fault - publish() re-opens through
            // BlockFileWriter.of(), which closes and re-initialises whatever the
            // fault left behind - so it is kept rather than dropped the way the
            // half-open .cp writer is.
            return false;
        }
        // Read the floor off the snapshot that just reached disk rather than the
        // live ring: they are the same list here, but the durable one is what
        // restart resumes from, and a later change re-reading the ring instead
        // would silently start pinning the floor to entries the manifest does
        // not list. An empty manifest lists nothing to resume from, so it
        // releases the floor (LONG_NULL) - the retires that empty the ring
        // unlink every .cp with it, leaving restart to rebuild from the applied
        // base, which needs no raw base WAL.
        final int snapshotSize = ringSnapshot.size();
        final long newestBaseSeqTxn = snapshotSize == 0
                ? Numbers.LONG_NULL
                : ringSnapshot.getQuick(snapshotSize - LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_BASE_SEQ_TXN);
        instance.recordCheckpointRingPublication(generation, coveredBaseSeqTxn, newestBaseSeqTxn);
        return true;
    }

    /**
     * Publishes the ring ahead of an in-order cycle that walks the durable floor
     * to {@code advanceTo}, unsealing nothing. Membership is unchanged by
     * construction - an in-order cycle emits only rows above {@code latestSeenTs},
     * which is at or above every entry's {@code maxTs} - so only
     * {@code coveredBaseSeqTxn} moves.
     * <p>
     * Load-bearing rather than tidy. Otherwise the ring reaches disk only beside a
     * {@code .cp} write and on an O3 retire, and a view sealing a checkpoint once
     * per million rows while advancing its floor every base commit would leave
     * {@code covered} parked on the last checkpoint - so restart, which trusts the
     * manifest only when {@code covered} equals the reconciled applied floor,
     * would reject the ring on every steadily-ingesting view.
     * <p>
     * Ordered ahead of the cycle's commit so the window a crash can land in is the
     * commit rather than this write: once the commit lands, the floor is
     * {@code advanceTo} and the manifest already says so. A crash the other way
     * (published, commit lost) leaves {@code covered} above the floor, which reads
     * as the conservative fallback, not a wrong answer - the claim holds either
     * way, being about the listed entries rather than the cycle.
     * <p>
     * Mirrors the caller's advance guard rather than assuming it: publishing
     * {@code covered} below the floor would only turn a trustable manifest into an
     * untrustable one.
     */
    private void publishCheckpointRingOnAdvance(LiveViewInstance instance, long advanceTo) {
        if (advanceTo > instance.getAppliedWatermark()) {
            publishCheckpointRing(instance, advanceTo);
        }
    }

    /**
     * Head-checkpoint write hook. Computes the per-LV snapshot
     * capability on the first call, accumulates the cycle's row count into
     * the cadence counter, and writes a fresh {@code <lvSeqTxn>.cp} when
     * either trigger has fired (or this is the first commit and no head
     * exists yet).
     * <p>
     * Capability gate: AND of every compiled window function's
     * {@code supportsCheckpointState()} plus, when the LV has an anchored window,
     * codec support for the partition-key column shape. Computed once and
     * cached on the {@link LiveViewInstance}. A {@code false} cap stays false
     * for the LV's lifetime and the hook is a permanent no-op: the LV emits
     * no checkpoints and routes restart / O3 through the head-miss replay
     * path in 2a.7 / 2a.8.
     * <p>
     * Cadence triggers (whichever fires first):
     * <ul>
     *     <li>{@code rowsSinceLastCheckpointWritten >= cairo.live.view.checkpoint.rows}.</li>
     *     <li>Wall-clock distance from the prior head's commit time exceeds
     *     {@code cairo.live.view.checkpoint.max.duration.micros}.</li>
     *     <li>No head exists yet (first cp ever for this LV) and at least
     *     one row landed - guarantees a usable head ASAP for restart-replay
     *     bounding, with the duration trigger floor active from then on.</li>
     * </ul>
     * <p>
     * A failure here does not invalidate the view (.cp is a derived artifact).
     * The prior head, if any, remains addressable; we log critical and
     * continue. The writer is closed defensively so the next cycle reopens
     * cleanly.
     */
    private void maybeWriteHeadCheckpoint(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lvSeqTxn,
            long batchMaxTs,
            long appendedRows,
            boolean force
    ) {
        // A read-only replica must never seal a .cp. The primary owns the durable
        // tier and replicates the result, and neither .cp nor _ring ever ships
        // (WalEvents.reconstructLiveViewFiles carries _lv alone), so a replica that
        // wrote one would be minting local resume anchors for window state it does
        // not own. Every refresh-cycle route here is primary-only already:
        // incrementalRefresh and drainAppliedBase reach it past the leadMode early
        // return, flushLead is gated on !isLeadReconstruction(), and runSeedSweep is
        // skipped outright. But three of those gates are overridable hooks
        // (drainLeadOverride, onLeadO3Detected, onLeadPublishStalled) that deflect the
        // replica by dynamic dispatch rather than by structure, so pin the invariant
        // here rather than re-derive it per site.
        //
        // The one route that bypasses every gate is the single-shot restore:
        // tryRestoreFromHead runs before refreshInstance branches on the role, and its
        // replayToApplied / o3HeadMissReplay both reach this hook. It needs a local
        // .cp to enter (getHeadCheckpointLvSeqTxn() != LONG_NULL), which a node that
        // has only ever been a replica never has - .cp does not replicate. A node
        // restarted as a replica over an ex-primary's files does, and would trip this.
        // That is a static trace, not a reproduction: no test builds that shape, and
        // it is the assert doing its job if one ever does.
        assert !isLeadReconstruction() : "read-only replica must not write a live view checkpoint";
        if (!instance.isSnapshotCapabilityComputed()) {
            instance.setSnapshotCapability(computeSnapshotCapability(instance, windowFactory));
        }
        if (!instance.isSnapshotCapability()) {
            return;
        }
        // A head with no maxTs cannot anchor a head-hit or bound a findResumeAnchorBelow
        // ceiling - it is refused hit-eligibility upstream (see promoteRestoredHeadIntoRing) -
        // so sealing one only poisons the ring. Every caller reaches here with rows behind it
        // (appendedRows / flushRows > 0), so batchMaxTs is a real timestamp today; this guard
        // keeps a future force-caller from writing a poison head past the cadence gate below.
        if (batchMaxTs == Numbers.LONG_NULL) {
            return;
        }

        instance.addRowsSinceLastCheckpointWritten(appendedRows);

        final long rowsCadence = engine.getConfiguration().getLiveViewCheckpointRows();
        final long durationCadence = engine.getConfiguration().getLiveViewCheckpointMaxDurationMicros();
        final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        final long lastWrittenUs = instance.getLastCheckpointWrittenUs();
        final long priorLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
        final boolean firstCp = priorLvSeqTxn == Numbers.LONG_NULL;
        final boolean rowTrigger = instance.getRowsSinceLastCheckpointWritten() >= rowsCadence;
        final boolean durationTrigger = !firstCp
                && lastWrittenUs != Numbers.LONG_NULL
                && (nowUs - lastWrittenUs) >= durationCadence;
        // A head carrying no write time is one THIS process never wrote: either
        // the startup sweep stamped it from the highest surviving .cp, or
        // tryRestoreFromHead re-stamped it after restoring that .cp. Both leave
        // the cadence with no baseline - the duration trigger above disables
        // itself outright without a lastWrittenUs, and the row counter restarts
        // from zero - so the restored head would stay the ring's ONLY entry until
        // an O3 forces a write or a full rowsCadence accumulates. Densify above it
        // on the first flush instead: until a second entry exists, every O3 at
        // or below the restored head has no older anchor to resume from and
        // rebuilds from the view's lower bound, which is O(view age) on a
        // long-lived view.
        //
        // Gated on a real batchMaxTs because the write seals a ring entry: a
        // LONG_NULL maxTs anchor undercuts every findResumeAnchorBelow ceiling and
        // is refused hit-eligibility upstream (see promoteRestoredHeadIntoRing).
        // The non-force callers already only reach here with rows behind them
        // (appendedRows > 0 / flushRows > 0), so this holds by construction; state
        // it locally rather than rely on four call sites keeping it.
        final boolean restoredHeadFirstFlush = !firstCp
                && lastWrittenUs == Numbers.LONG_NULL
                && batchMaxTs != Numbers.LONG_NULL;
        // force fires the write past the row/duration cadence gate. The O3
        // replay paths pass it so an O3 always seals a fresh near-head anchor:
        // a head-hit keeps its (still sealed) prior head as a ring entry rather
        // than clearing it, so firstCp would otherwise stay false and cadence
        // could skip the write, stranding the head at the stale maxTs.
        if (!(force || firstCp || restoredHeadFirstFlush || rowTrigger || durationTrigger)) {
            return;
        }

        try {
            if (checkpointWriter == null) {
                checkpointWriter = new LiveViewCheckpointWriter(engine.getConfiguration());
            }
            path.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken());
            checkpointWriter.of(path.$(), lvSeqTxn);

            // The base commit this head covers. Stamped into the manifest and
            // mirrored onto the instance below so WalPurgeJob can hold the base
            // WAL purge floor here rather than at the applied point.
            final long baseSeqTxn = instance.getLastProcessedSeqTxn();
            checkpointManifest.clear();
            checkpointManifest.setLvSeqTxn(lvSeqTxn);
            checkpointManifest.setBaseSeqTxn(baseSeqTxn);
            checkpointManifest.setMaxTimestamp(batchMaxTs);
            checkpointManifest.setLvRowPosition(instance.getLvRowsTotal());
            checkpointManifest.setKind(LiveViewCheckpointManifest.KIND_STEADY);
            final LiveViewWindow anchorWindow = instance.getAnchorWindow();
            if (anchorWindow != null) {
                checkpointManifest.addWindowName(anchorWindow.getWindowName());
            }
            checkpointWriter.writeManifestBlock(checkpointManifest);

            if (anchorWindow != null) {
                MemoryA anchorSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_WINDOW_ANCHOR);
                anchorWindow.snapshot(anchorSink);
                checkpointWriter.endBlock();
            }

            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
            appendCheckpointTimelineRoot(instance, functions, anchorWindow, baseSeqTxn, batchMaxTs);
            final String windowName = anchorWindow != null ? anchorWindow.getWindowName() : "";
            // Test-only: omit the last N function-snapshot blocks to forge a
            // CRC-valid-but-short checkpoint. 0 in production, so the limit is
            // MAX_VALUE and every snapshot-capable function is written.
            int fnBlockWriteLimit = Integer.MAX_VALUE;
            final int fnBlocksToOmit = checkpointTrailingFunctionSnapshotBlocksToOmit;
            if (fnBlocksToOmit > 0) {
                int capable = 0;
                for (int i = 0, m = functions.size(); i < m; i++) {
                    if (functions.getQuick(i).supportsCheckpointState()) {
                        capable++;
                    }
                }
                fnBlockWriteLimit = Math.max(0, capable - fnBlocksToOmit);
            }
            int fnBlocksWritten = 0;
            for (int i = 0, n = functions.size(); i < n; i++) {
                final WindowFunction f = functions.getQuick(i);
                if (!f.supportsCheckpointState() || fnBlocksWritten >= fnBlockWriteLimit) {
                    continue;
                }
                final MemoryA fnSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                fnSink.putStr(windowName);
                fnSink.putStr(snapshotFactoryName(f));
                fnSink.putInt(f.checkpointStateFormatVersion());
                // LiveViewFunctionSnapshot frames every scalar or partition state as an
                // exact-length page; the enclosing block independently bounds the whole function.
                LiveViewFunctionSnapshot.write(fnSink, f);
                checkpointWriter.endBlock();
                fnBlocksWritten++;
            }

            // Capture before commit(): commit() truncates the mmap and resets
            // the writer for reuse.
            final long stateBytes = checkpointWriter.getAppendOffset();
            // Retain the prior head (LONG_NULL suppresses commit()'s unlink): it
            // is a sealed ring entry now, not garbage. The ring below governs
            // retirement.
            checkpointWriter.commit(Numbers.LONG_NULL);

            // Retain the freshly sealed head in the checkpoint ring. A
            // same-timestamp run can leave a prior entry at batchMaxTs, so drop
            // any entry the fresh head supersedes at or above its own maxTs
            // first - the ring is held in strictly increasing maxTs order - then
            // add and prune back within the count / bytes budget, unlinking
            // whatever falls out (the equal-maxTs prior and the pruned oldest).
            evictedCheckpoints.clear();
            instance.invalidateRetainedCheckpointsFrom(batchMaxTs, evictedCheckpoints);
            instance.addRetainedCheckpoint(lvSeqTxn, batchMaxTs, baseSeqTxn, instance.getLvRowsTotal(), stateBytes);
            // Prune back within the budget against the fresh head's maxTs.
            pruneRetainedCheckpointsToBudget(instance, batchMaxTs, evictedCheckpoints);
            // Publish the ring BEFORE the head advances. Every listed entry is
            // sealed at lvSeqTxn: the fresh one by construction, the survivors
            // because an O3 cycle already retired whatever this commit unsealed
            // and an in-order one unseals nothing (every maxTs is below the
            // commit's minTs). Callers reach here only after
            // the LV's own commit and the _lv.s persist, so lvSeqTxn is also the
            // applied watermark a restart reconciles against: covered == floor,
            // which is the trust rule.
            //
            // Publishing ahead of the head is load-bearing rather than cosmetic.
            // WalPurgeJob min-combines getHeadCheckpointBaseSeqTxn(), so the head
            // carries the base WAL purge floor, and restart's replayToApplied
            // re-feeds raw base WAL from the restored entry's baseSeqTxn. Let the
            // head advance onto an entry the durable manifest does not list and
            // the floor releases WAL that a restart trusting the manifest still
            // needs to replay from its older newest entry. Both the crash window
            // (crash between the .cp commit and the publish) and a failed publish
            // open exactly that gap, so the head waits on a successful
            // publication - the ordering covers the crash, the gate the failure.
            //
            // A failed publish therefore leaves the fresh .cp an orphan with the
            // head, the floor and the cadence counters all parked on the previous
            // entry, so the next cycle writes another .cp and re-lists the ring
            // from memory. The view keeps serving throughout: the in-memory ring
            // already holds the fresh entry, so resume anchors stay available even
            // while the manifest trails.
            if (publishCheckpointRing(instance, lvSeqTxn)) {
                instance.setHeadCheckpoint(lvSeqTxn, baseSeqTxn, batchMaxTs, stateBytes, nowUs);
            }
            // Unlink unconditionally, even when the publish failed and the stale
            // manifest still lists these files. Holding them back would keep that
            // manifest self-consistent, but a pinned head also pins the cadence
            // counters setHeadCheckpoint resets, so an unwritable _ring makes
            // every subsequent cycle seal another .cp - retaining every eviction
            // grows the directory without bound until the next restart. Bounded
            // disk wins: a manifest naming a missing .cp fails the referenced-file
            // check on restart and falls back to the highest .cp, which is the
            // outcome a run that never published one gets anyway.
            unlinkCheckpointFiles(instance, evictedCheckpoints);
            // Baseline observability: elapsed micros of this head-checkpoint write
            // (manifest + snapshots + commit + ring publish + evicted-file unlink),
            // measured from the cadence-gate clock read above. Surfaced via
            // live_views().head_checkpoint_write_micros.
            instance.recordCheckpointWriteMicros(engine.getConfiguration().getMicrosecondClock().getTicks() - nowUs);
        } catch (Throwable t) {
            LOG.critical().$("could not write live view head checkpoint [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", lvSeqTxn=").$(lvSeqTxn)
                    .$(", error=").$(t).I$();
            // Drop the half-open writer; the next cycle reallocates a fresh
            // one. The on-disk .cp.tmp (if any) is swept on next startup.
            checkpointWriter = Misc.free(checkpointWriter);
        }
    }

    /**
     * Restart replay-to-applied: re-feeds base WAL rows over
     * {@code (fromSeqTxn, toSeqTxn]} through the window pipeline to advance the
     * accumulators restored from the head {@code .cp} up to the persisted applied
     * watermark, WITHOUT emitting (no LV WAL write, no inline apply, no in-mem
     * tier append). The on-disk LV table already holds these rows - the checkpoint
     * cadence simply left the {@code .cp} short of the applied point - so only the
     * restored accumulators need to catch up before drain-forward rebuilds the
     * un-flushed lead lost on the crash.
     * <p>
     * The whole gap is processed in this single call: the per-turn yield budget is
     * reset before each drain pass so the replay never stops mid-gap and leaves the
     * accumulators short of disk (which would make drain-forward re-emit rows disk
     * already holds). On out-of-order arrival - only reachable when a prior post-O3
     * {@code .cp} write failed, so an unresolved O3 sits between the head and the
     * applied point - it hands off to {@link #o3Replay}, passing the applied point
     * (not the offending seqTxn) as {@code advanceTo} so the REPLACE_RANGE rewrite
     * covers everything disk already holds; {@code o3Replay} re-stamps the
     * watermarks and writes a fresh head {@code .cp}, and this returns
     * {@link #REPLAY_TO_APPLIED_O3}. Otherwise returns the number of rows re-fed.
     */
    private long replayToApplied(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long fromSeqTxn,
            long toSeqTxn
    ) throws SqlException {
        return replayToApplied(
                instance,
                windowFactory,
                fromSeqTxn,
                toSeqTxn,
                instance.getDefinition().getViewLowerBoundTimestamp(),
                Long.MAX_VALUE
        );
    }

    private long replayToApplied(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long fromSeqTxn,
            long toSeqTxn,
            long lowTimestampInclusive,
            long highTimestampInclusive
    ) throws SqlException {
        RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
        final Function filter = filterFactory.getFilter();
        RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        final RecordMetadata baseMetadata = pageFrameFactory.getMetadata();
        final int baseTimestampIndex = baseMetadata.getTimestampIndex();
        buildColumnMappings(baseMetadata, baseToken);
        final RecordMetadata outMetadata = windowFactory.getMetadata();
        final int cursorTimestampIndex = outMetadata.getTimestampIndex();

        long replayedRows = 0;
        long from = fromSeqTxn;
        while (from < toSeqTxn) {
            // Replay-to-applied must finish the gap inside this one restore call, so
            // it is not subject to the per-turn yield budget: reset the budget before
            // each pass and loop until the drain reaches the applied point.
            turnStartUs = engine.getConfiguration().getMicrosecondClock().getTicks();
            turnCommitsProcessed = 0;
            drainResult.reset();
            // walWriter == null and populateTier == false: drainBaseWal drives the
            // window cursor (advancing accumulators and latestSeenTs per row) but
            // skips every WAL write and every staging-buffer mirror.
            drainBaseWal(
                    instance, windowFactory, baseToken, baseMetadata, baseTimestampIndex,
                    cursorTimestampIndex, lowTimestampInclusive, highTimestampInclusive, filter, from, toSeqTxn,
                    null, null, false, instance.getLatestSeenTs()
            );
            if (drainResult.o3Detected) {
                o3Replay(instance, windowFactory, drainResult.o3LateRowTs, baseToken, toSeqTxn);
                return REPLAY_TO_APPLIED_O3;
            }
            replayedRows += drainResult.appendedRows;
            if (drainResult.advanceTo <= from) {
                // No forward progress (only compacted / non-WAL entries remain). Stop
                // to avoid spinning; the caller still advances the watermarks to the
                // applied point.
                break;
            }
            from = drainResult.advanceTo;
        }
        return replayedRows;
    }

    /**
     * Opens the head {@code .cp} at {@code headLvSeqTxn} and rehydrates the LV's
     * window state (anchor map + per-function maps) from the manifest + anchor
     * block + per-function blocks. Populates {@code out} with the manifest's
     * {@code baseSeqTxn}, {@code maxTimestamp}, and the file's byte length.
     * <p>
     * Callers (restart restore and the O3 anchor resume) decide what to do with
     * the restored watermarks and whether to refresh the head metadata trio on
     * the instance; this helper restricts itself to state restore + failure
     * cleanup so both call sites share the same disk read path. The anchor need
     * not be the head - {@code headLvSeqTxn} names whatever sealed checkpoint the
     * caller wants restored.
     * <p>
     * Failure handling: any structural error (CRC fail, magic mismatch, missing
     * function class, anchor type mismatch) is best-effort cleaned up in
     * {@link #handleCorruptHeadCheckpoint} - it logs critical, unlinks the corrupt
     * {@code .cp}, evicts the anchor's retained-ring entry, and clears the head
     * metadata only when the anchor IS the head, then returns {@code false}. The
     * LV is not invalidated; the caller falls through to the head-miss replay
     * path.
     */
    private boolean restoreFromHead(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long headLvSeqTxn,
            RestoredHeadState out
    ) {
        return restoreFromHead(instance, windowFactory, headLvSeqTxn, false, out);
    }

    /**
     * Opens a {@code .cp} (steady, {@code isSeed=false}) or {@code .scp}
     * (seed, {@code isSeed=true}) checkpoint and rehydrates window
     * state. The seed variant additionally surfaces the SEED_CURSOR's
     * data offset in {@code out.resumeDataOffset}.
     */
    private boolean restoreFromHead(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long headLvSeqTxn,
            boolean isSeed,
            RestoredHeadState out
    ) {
        out.reset();
        path.of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .slash();
        if (isSeed) {
            LiveViewCheckpointWriter.appendScpFileName(path, headLvSeqTxn);
        } else {
            LiveViewCheckpointWriter.appendCpFileName(path, headLvSeqTxn);
        }

        if (checkpointReader == null) {
            checkpointReader = new LiveViewCheckpointReader(engine.getConfiguration());
        }

        try {
            checkpointReader.of(path.$());
            checkpointReader.readManifestInto(checkpointManifest);
            out.manifestBaseSeqTxn = checkpointManifest.getBaseSeqTxn();
            out.maxTimestamp = checkpointManifest.getMaxTimestamp();
            out.lvRowsTotal = checkpointManifest.getLvRowPosition();
            out.stateBytes = engine.getConfiguration().getFilesFacade().length(path.$());

            final LiveViewWindow anchorWindow = instance.getAnchorWindow();
            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
            // Open the (lazy) window cursor before writing restored state into it:
            // allocates the per-partition maps and marks the cursor open so the
            // first post-restore incremental refresh preserves the restored state
            // rather than re-bootstrapping (which would clobber it).
            windowFactory.openForLiveViewRestore(executionContext);
            final LiveViewCheckpointReader.BlockCursor cursor = checkpointReader.getCursor();
            // Restart the positional pairing for this checkpoint's function blocks.
            restoreFunctionCursor = 0;
            // The MANIFEST is the first block; skip it - readManifestInto
            // already consumed it conceptually but resets the cursor.
            // Walk forward and dispatch by type.
            cursor.hasNext();
            cursor.next();
            boolean anchorRestored = false;
            while (cursor.hasNext()) {
                final LiveViewCheckpointReader.ReadableBlock block = cursor.next();
                switch (block.type()) {
                    case LiveViewCheckpointBlockType.BLOCK_SEED_CURSOR:
                        // Two LONGs: data-cursor row offset, then lvRowsTotal.
                        // lvRowsTotal is redundant with the manifest's
                        // lvRowPosition (already in out.lvRowsTotal); we read
                        // only the offset here.
                        out.resumeDataOffset = block.getLong(0L);
                        break;
                    case LiveViewCheckpointBlockType.BLOCK_WINDOW_ANCHOR:
                        if (anchorWindow == null) {
                            throw CairoException.critical(0)
                                    .put("checkpoint anchor block but LV has no anchored window");
                        }
                        anchorWindow.restore(block.memory(), block.payloadStart(), block.size());
                        anchorRestored = true;
                        break;
                    case LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT:
                        restoreFunctionBlock(block, functions);
                        break;
                    case LiveViewCheckpointBlockType.BLOCK_MANIFEST:
                        // Re-encountering manifest mid-file is malformed.
                        throw CairoException.critical(0)
                                .put("duplicate MANIFEST block in live view checkpoint");
                    default:
                        // Unknown block type: per the file-format contract
                        // (block types are content-defined, new types do not
                        // require a file-version bump), readers skip silently.
                        break;
                }
            }
            // Missing-block validation. The file-level CRC guards against bit-rot
            // but NOT against a CRC-valid-but-short checkpoint: a truncated tail,
            // or a format drift that adds a snapshot-capable function this .cp's
            // writer never emitted, simply ends the block walk early. Without this
            // check restoreFromHead would return success with a function (or the
            // anchor) left in default state, and the post-restore incremental
            // refresh would resume after the manifest txn from that wrong baseline,
            // durably diverging. restoreFunctionBlock already throws on the extra-
            // block direction; catch the missing-block direction here. Errno 0 =>
            // handleCorruptHeadCheckpoint unlinks the .cp / .scp and head-miss- or
            // seed-replays from a known-good boundary.
            if (isSeed && out.resumeDataOffset == Numbers.LONG_NULL) {
                throw CairoException.critical(0)
                        .put("live view seed checkpoint missing its SEED_CURSOR block");
            }
            if (anchorWindow != null && !anchorRestored) {
                throw CairoException.critical(0)
                        .put("live view checkpoint missing its WINDOW_ANCHOR block");
            }
            for (int i = restoreFunctionCursor, n = functions.size(); i < n; i++) {
                final WindowFunction unmatched = functions.getQuick(i);
                if (unmatched.supportsCheckpointState()) {
                    throw CairoException.critical(0)
                            .put("fewer live view function snapshot blocks than snapshot-capable functions [firstMissingPosition=")
                            .put(i)
                            .put(", factory=")
                            .put(snapshotFactoryName(unmatched))
                            .put(']');
                }
            }
            return true;
        } catch (CairoException ce) {
            final int errno = ce.getErrno();
            if (errno == CairoException.LV_FUNCTION_SNAPSHOT_VERSION_MISMATCH
                    || errno == CairoException.LV_CHECKPOINT_FILE_VERSION_MISMATCH) {
                // Version mismatch is a real compatibility break, not
                // corruption. Stash the reason on the instance so the caller
                // drives invalidation outside the refresh latch
                // (engine.invalidateLiveView parks on the instance monitor
                // when a checkpoint freeze is active, and the agent's
                // startCheckpoint cannot complete its latch handshake while
                // the worker still holds the refresh latch).
                LOG.critical().$("live view checkpoint version mismatch [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", lvSeqTxn=").$(headLvSeqTxn)
                        .$(", error=").$safe(ce.getFlyweightMessage()).I$();
                instance.setPendingInvalidationReason(Chars.toString(ce.getFlyweightMessage()));
                return false;
            }
            return handleCorruptHeadCheckpoint(instance, headLvSeqTxn, path, ce);
        } catch (Throwable t) {
            return handleCorruptHeadCheckpoint(instance, headLvSeqTxn, path, t);
        } finally {
            try {
                checkpointReader.close();
            } catch (Throwable closeErr) {
                LOG.error().$("could not close live view checkpoint reader [view=")
                        .$(instance.getLiveViewToken())
                        .$(", error=").$(closeErr).I$();
            }
        }
    }

    /**
     * Best-effort cleanup after a checkpoint restore fails on structural
     * corruption (CRC / magic / truncation / missing function class / anchor type
     * mismatch - all errno 0, distinct from a version mismatch, which
     * {@link #restoreFromHead} handles separately by stashing a pending
     * invalidation reason). Unlinks the corrupt {@code .cp} (unusable regardless of
     * which anchor it was) and drops the matching entry from the retained-checkpoint
     * ring so a later resume never re-selects it. Clears the head metadata trio ONLY
     * when the corrupt anchor IS the current head: a non-head anchor leaves the
     * newer, still-valid head in place, and clearing it would desync the head
     * metadata from the ring. Always returns {@code false} so the caller abandons
     * the restore and falls through to a from-boundary rebuild / trigger re-fire.
     */
    private boolean handleCorruptHeadCheckpoint(
            LiveViewInstance instance,
            long anchorLvSeqTxn,
            Path path,
            Throwable t
    ) {
        LOG.critical().$("could not restore live view from checkpoint [view=")
                .$(instance.getDefinition().getViewName())
                .$(", lvSeqTxn=").$(anchorLvSeqTxn)
                .$(", error=").$(t).I$();
        // Best-effort: unlink the corrupt .cp. It is unusable whether it was the
        // head or an older ring entry.
        try {
            engine.getConfiguration().getFilesFacade().removeQuiet(path.$());
        } catch (Throwable rmErr) {
            LOG.error().$("could not unlink corrupt checkpoint [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(rmErr).I$();
        }
        // Capture head membership BEFORE any head-clear so the eviction cannot
        // change the answer. removeRetainedCheckpoint is a no-op when the anchor is
        // not a ring entry (restart / seed restore run with an empty ring), so the
        // head-only callers behave exactly as before. Clearing the head trio for a
        // non-head anchor would strand the real head's metadata pointing above a
        // now-shorter ring.
        final boolean anchorIsHead = anchorLvSeqTxn == instance.getHeadCheckpointLvSeqTxn();
        instance.removeRetainedCheckpoint(anchorLvSeqTxn);
        if (anchorIsHead) {
            instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
        }
        return false;
    }

    /**
     * Restart-restore: opens the head {@code .cp}, rehydrates the LV's window
     * state from the manifest + anchor block + per-function blocks, replays the
     * base WAL forward to close the checkpoint-cadence gap between the head and
     * the applied point, then resumes the refresh worker at the applied point so
     * the next incremental refresh rebuilds only the un-flushed lead.
     * <p>
     * With <em>no</em> anchor to restore from - neither a sweep head nor one a
     * trusted manifest names - it rebuilds from the applied base instead of
     * returning. The caller only routes a view here headless once
     * {@link #needsHeadlessRestartRecovery} has established that it has
     * materialised rows, and cold accumulators would silently flush wrong
     * cumulative aggregates over those. So this method restores or rebuilds; it
     * never hands a caller back a view whose window state it could not account
     * for.
     * <p>
     * Which {@code .cp} is the head is {@link #rehydrateCheckpointRing}'s
     * decision, not the startup sweep's: a trusted {@code _ring} manifest
     * repopulates the whole retained-checkpoint ring and names its newest listed
     * entry, so a first post-restart O3 below the head can resume from an older
     * anchor instead of rebuilding from {@code viewLowerBoundTimestamp}. Without
     * one the sweep's highest surviving {@code .cp} stands, alone in the ring.
     * <p>
     * The head {@code .cp}'s {@code baseSeqTxn} can lag the persisted applied
     * watermark, because the checkpoint cadence does not write a fresh {@code .cp}
     * on every flush: the on-disk LV table holds every base commit up to the
     * applied point, but the restored accumulators stop at the (older) head. The
     * gap is closed by {@link #replayToApplied}, which re-feeds the base rows over
     * {@code (manifestBaseSeqTxn, appliedWatermark]} through the window pipeline to
     * advance the accumulators to the disk state without re-emitting.
     * <p>
     * Failure handling: a structural error opening the {@code .cp} (CRC fail,
     * magic mismatch, missing function class, anchor type mismatch) unlinks the
     * head .cp and clears the head metadata on the instance; the LV is not
     * invalidated - {@code .cp} is derived state, so this method rebuilds the
     * whole view inline via {@link #o3HeadMissReplay} over the applied base
     * snapshot (identical to the missing-.cp recovery) rather than bare-returning
     * into the caller's incremental drain from the applied watermark, which would
     * recompute post-watermark rows from cold accumulators and durably flush wrong
     * cumulative aggregates. A compatibility break (version-too-old / file-version
     * mismatch) instead stashes a pending-invalidation reason, and a
     * replay-to-applied error can leave the restored accumulators inconsistent
     * with disk; both invalidate the view (operator recovers with DROP + CREATE)
     * via the pending-invalidation hook rather than serving wrong results.
     */
    private void tryRestoreFromHead(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        // The ring is never rebuilt by scanning the surviving on-disk .cp files:
        // a stale .cp whose retirement unlink failed is indistinguishable from a
        // sealed one and would poison a later O3 resume. It is repopulated only
        // from the durable _ring manifest's allow-list (rehydrateCheckpointRing
        // below) or, absent one, from the single restored head
        // (promoteRestoredHeadIntoRing at the tail). Both run inside this
        // single-shot restore, and the catalogue load stashes a manifest
        // CANDIDATE on the instance rather than ring entries, so the ring must
        // still be empty on entry. If it is not, some path has started
        // resurrecting on-disk entries as anchors - fail loudly in tests.
        assert instance.getRetainedCheckpointCount() == 0
                : "retained-checkpoint ring must be empty on restart restore, was "
                + instance.getRetainedCheckpointCount();
        // The persisted applied watermark (base seqTxn) is disk truth: the LV's
        // on-disk table holds every base commit up to it, and
        // reconcileAppliedFloorAfterRestart has already clamped it up from the LV
        // table, so this IS the reconciled floor the manifest is judged against.
        // Snapshot it before the restore below overwrites the in-memory
        // watermarks with the head's (potentially older) base seqTxn.
        final long diskAppliedSeqTxn = instance.getAppliedWatermark();
        final long headLvSeqTxn = rehydrateCheckpointRing(instance, diskAppliedSeqTxn);
        if (headLvSeqTxn == Numbers.LONG_NULL) {
            // No anchor at all: the sweep found no .cp at or below the RAW _lv.s
            // watermark, and no trusted manifest named one either. The caller has
            // already established that this view has materialised rows, so its
            // accumulators are NOT at identity and a bare return would drain the
            // post-watermark base commits from cold state and durably flush wrong
            // cumulative aggregates - the same silent corruption the corrupt-.cp
            // branch below rebuilds to avoid, reached with no .cp to be corrupt.
            //
            // Reachable through a LOST (not merely trailing) _lv.s persist: the
            // sweep gates the head on the raw watermark, which
            // reconcileAppliedFloorAfterRestart is about to clamp up, so every
            // legitimately sealed .cp above the lost value is declined (and, with
            // no manifest exempting it, unlinked). Rebuild from the applied base
            // exactly as the corrupt-.cp path does: unconditionally correct and
            // idempotent, it re-seeds the window from identity, rewrites the tier
            // with a single REPLACE_RANGE, advances the watermarks and seals a
            // fresh head.
            //
            // This is NOT the rebuild rehydrateCheckpointRing's javadoc tells you
            // not to re-add. That one refuses a head the sweep DID find, because a
            // trusted manifest listed nothing; it gates on the verdict. This one
            // gates on the value rehydrateCheckpointRing returned, so a trusted
            // empty manifest still restores from the fallback head - there is
            // simply no head here to refuse.
            LOG.info().$("live view restart found no checkpoint to restore, rebuilding from the applied base [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", appliedWatermark=").$(diskAppliedSeqTxn).I$();
            try {
                o3HeadMissReplay(instance, windowFactory, Numbers.LONG_NULL, instance.getDefinition().getBaseTableToken(), diskAppliedSeqTxn, true);
            } catch (Throwable t) {
                LOG.critical().$("live view restart head-miss replay failed with no checkpoint [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", appliedWatermark=").$(diskAppliedSeqTxn)
                        .$(", error=").$(t).I$();
                instance.setPendingInvalidationReason("live view restart head-miss replay without a checkpoint failed");
                return;
            }
            instance.setCheckpointRestoreSucceeded();
            return;
        }
        if (!restoreFromHead(instance, windowFactory, headLvSeqTxn, restoredHeadState)) {
            // restoreFromHead failed in one of two distinct ways:
            //  - Compatibility break (LV_*_VERSION_* errno): it stashed a
            //    pending-invalidation reason. Return so the caller drives
            //    invalidation out of the refresh latch - a format we can no
            //    longer read must not be served or rebuilt from.
            //  - Structural corruption (CRC / magic / truncation / missing
            //    function class, all errno 0): it unlinked the corrupt .cp and
            //    cleared head metadata but left NO pending reason. A bare return
            //    here falls through to the caller's incremental drain from the
            //    applied watermark with COLD accumulators, which recomputes the
            //    post-watermark rows from zero and commits + flushes wrong
            //    cumulative aggregates (sum() OVER (ORDER BY ts), row_number(),
            //    partitioned cumulatives) durably - silent, no crash, no
            //    invalidation. A *missing* .cp recovers correctly via a full
            //    rebuild; a *corrupt* one must not fare worse. Recover the same
            //    way the O3 head-hit and dedup-restart paths do: rebuild the
            //    whole view from the applied base snapshot, which re-seeds the
            //    window from identity, rewrites the tier with a single
            //    REPLACE_RANGE, advances the watermarks, and writes a fresh head.
            if (instance.hasPendingInvalidationReason()) {
                return;
            }
            try {
                o3HeadMissReplay(instance, windowFactory, Numbers.LONG_NULL, instance.getDefinition().getBaseTableToken(), diskAppliedSeqTxn, true);
            } catch (Throwable t) {
                LOG.critical().$("live view restart head-miss replay failed after corrupt checkpoint [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", appliedWatermark=").$(diskAppliedSeqTxn)
                        .$(", error=").$(t).I$();
                instance.setPendingInvalidationReason("live view restart head-miss replay after corrupt checkpoint failed");
                return;
            }
            instance.setCheckpointRestoreSucceeded();
            return;
        }
        final long manifestBaseSeqTxn = restoredHeadState.manifestBaseSeqTxn;
        // Re-seed the O3 detection watermark from the head before any replay -
        // latestSeenTs is an in-memory volatile reset to LONG_NULL on rebuild.
        // Without it the first post-restart commit (or the replay below) is not
        // compared against already-materialized rows, so a late row arriving
        // first slips past O3 detection and gets forward-appended in arrival
        // order. The monotonic setter lets the replay advance it further.
        if (restoredHeadState.maxTimestamp != Numbers.LONG_NULL) {
            instance.setLatestSeenTs(restoredHeadState.maxTimestamp);
        }
        // Refresh the head metadata trio with the real maxTs + stateBytes we just
        // read; the startup sweep stamped placeholders. Done before the replay so
        // that if replayToApplied hands off to o3Replay, its head-hit / head-miss
        // decision reads the real materialized maxTs rather than the placeholder.
        // writtenUs stays LONG_NULL: it marks the head as one this process never
        // wrote, which is exactly what maybeWriteHeadCheckpoint's restored-head
        // trigger keys off to seal a fresh .cp on the first post-restart flush.
        instance.setHeadCheckpoint(
                headLvSeqTxn,
                manifestBaseSeqTxn,
                restoredHeadState.maxTimestamp,
                restoredHeadState.stateBytes,
                Numbers.LONG_NULL
        );
        // Replaces the entry-time "the ring is always empty after restart"
        // contract. A rehydrated ring ends on the entry we just restored from,
        // and the manifest's claim about it must match the .cp's own manifest -
        // maybeWriteHeadCheckpoint stamps batchMaxTs into both. A mismatch means
        // the allow-list and the checkpoints have drifted, which would let an
        // anchor search select on a maxTs the window state does not hold.
        assert instance.getRetainedCheckpointCount() == 0
                || (instance.getRetainedCheckpointLvSeqTxn(instance.getRetainedCheckpointCount() - 1) == headLvSeqTxn
                && instance.getRetainedCheckpointMaxTs(instance.getRetainedCheckpointCount() - 1) == restoredHeadState.maxTimestamp)
                : "rehydrated checkpoint ring must end on the restored head, was lvSeqTxn="
                + instance.getRetainedCheckpointLvSeqTxn(instance.getRetainedCheckpointCount() - 1)
                + ", maxTs=" + instance.getRetainedCheckpointMaxTs(instance.getRetainedCheckpointCount() - 1)
                + " against head lvSeqTxn=" + headLvSeqTxn + ", maxTs=" + restoredHeadState.maxTimestamp;
        long resumeSeqTxn = manifestBaseSeqTxn;
        long replayedRows = 0;
        if (diskAppliedSeqTxn > manifestBaseSeqTxn && isDedupBase(instance)) {
            // Dedup base: the checkpoint-to-applied gap must be closed over the
            // applied (post-dedup) base, not raw WAL. replayToApplied re-feeds raw
            // WAL via drainBaseWal, which would advance the restored accumulators over
            // the pre-dedup stream and silently diverge from the post-dedup disk state
            // (Gap A / Gap B are invisible to the raw O3 triggers). Route straight to a
            // full head-miss rebuild from viewLowerBoundTimestamp over the applied
            // snapshot: unconditionally correct, and idempotent with an intact base (its
            // REPLACE_RANGE reproduces the rows disk already holds). o3HeadMissReplay
            // advances the watermarks and writes a fresh head, so restore is complete.
            try {
                o3HeadMissReplay(instance, windowFactory, Numbers.LONG_NULL, instance.getDefinition().getBaseTableToken(), diskAppliedSeqTxn, true);
            } catch (Throwable t) {
                LOG.critical().$("live view dedup restart head-miss replay failed [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", manifestBaseSeqTxn=").$(manifestBaseSeqTxn)
                        .$(", appliedWatermark=").$(diskAppliedSeqTxn)
                        .$(", error=").$(t).I$();
                instance.setPendingInvalidationReason("live view restart dedup replay-to-applied failed");
                return;
            }
            instance.setCheckpointRestoreSucceeded();
            return;
        }
        if (diskAppliedSeqTxn > manifestBaseSeqTxn) {
            // The checkpoint cadence left the head short of the applied point.
            // Advance the restored accumulators over the gap without re-emitting
            // (disk already holds these rows), then resume at the applied point.
            try {
                replayedRows = replayToApplied(instance, windowFactory, manifestBaseSeqTxn, diskAppliedSeqTxn);
            } catch (Throwable t) {
                LOG.critical().$("live view replay-to-applied failed on restart [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", manifestBaseSeqTxn=").$(manifestBaseSeqTxn)
                        .$(", appliedWatermark=").$(diskAppliedSeqTxn)
                        .$(", error=").$(t).I$();
                // Recovery integrity is compromised (accumulators may be a partial
                // advance over disk). Invalidate out of the refresh latch via the
                // pending-reason hook rather than serve wrong results.
                instance.setPendingInvalidationReason("live view restart replay-to-applied failed");
                return;
            }
            if (replayedRows == REPLAY_TO_APPLIED_O3) {
                // replayToApplied hit an out-of-order base commit mid-gap and handed
                // off to o3Replay, which rebuilt the on-disk tier from base in ts
                // order over the applied range, re-stamped the watermarks, and wrote
                // a fresh head .cp. Restore is complete.
                instance.setCheckpointRestoreSucceeded();
                return;
            }
            resumeSeqTxn = diskAppliedSeqTxn;
        }
        // Resume the refresh worker at the applied point; the next incremental
        // refresh drains forward from here to rebuild the un-flushed lead. The
        // seam_ts is anchored at the WAL commit boundary (see incrementalRefresh),
        // so appliedWatermark mirrors lastProcessed.
        instance.setLastProcessedSeqTxn(resumeSeqTxn);
        instance.setAppliedWatermark(resumeSeqTxn);
        // Re-seed the lifetime row counter from the manifest plus the rows the
        // replay re-fed, so subsequent addRowsSinceLastCheckpointWritten calls
        // accumulate against the disk total rather than the (older) head total.
        instance.setLvRowsTotal(restoredHeadState.lvRowsTotal + replayedRows);
        promoteRestoredHeadIntoRing(instance, headLvSeqTxn, manifestBaseSeqTxn);
        instance.setCheckpointRestoreSucceeded();
    }

    /**
     * ACTIVE-view restart recovery from the versioned checkpoint timeline. The
     * durable live-view table supplies the materialization frontier, row count,
     * and live-view writer txn; its in-band max-base-seqTxn supplies the
     * transaction inclusion boundary. Root selection and restore remain under
     * one generation pin inside the timeline reader.
     */
    private void tryRestoreFromTimeline(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        final long durableBaseSeqTxn = instance.getAppliedWatermark();
        final long durableFrontierTimestamp;
        final long durableLvRowCount;
        final long durableLvSeqTxn;
        try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
            durableLvRowCount = lvReader.size();
            durableFrontierTimestamp = durableLvRowCount == 0 ? Numbers.LONG_NULL : lvReader.getMaxTimestamp();
            durableLvSeqTxn = lvReader.getSeqTxn();
        }

        if (durableLvRowCount == 0
                && instance.getStateReader().getLastProcessedSeqTxn()
                < instance.getStateReader().getSubscribeFromSeqTxn()) {
            // Identity state is already the exact runtime for a never-materialized
            // ACTIVE view. Avoid creating an empty timeline file merely to prove it.
            return;
        }

        try (
                Path checkpointsDir = new Path();
                Path timelinePath = new Path()
        ) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
            if (!engine.getConfiguration().getFilesFacade().exists(timelinePath.$())) {
                rebuildTimelineRecoveryFromAppliedBase(
                        instance,
                        windowFactory,
                        durableBaseSeqTxn,
                        "timeline is absent"
                );
                return;
            }

            final LiveViewCheckpointTimelineStoreReader.Result restored;
            try (LiveViewCheckpointTimelineStoreReader timelineReader =
                         new LiveViewCheckpointTimelineStoreReader(engine.getConfiguration())) {
                timelineReader.of(checkpointsDir);
                // Allocate/open the caller-owned maps before the page reader
                // validates and restores into them, matching legacy restore.
                windowFactory.openForLiveViewRestore(executionContext);
                restored = timelineReader.restoreLatestCompatible(
                        durableFrontierTimestamp,
                        durableBaseSeqTxn,
                        durableLvSeqTxn,
                        durableLvRowCount,
                        instance.getLiveViewToken().getTableId(),
                        windowFactory.getWindowFunctions(),
                        instance.getAnchorWindow()
                );
            }

            instance.forceSetLatestSeenTs(restored.maxTimestamp);
            instance.setLvRowsTotal(restored.effectiveLvRowPosition);

            long replayedRows = 0;
            if (restored.maxTimestamp != Long.MAX_VALUE) {
                final long lowTimestamp = Math.max(
                        instance.getDefinition().getViewLowerBoundTimestamp(),
                        restored.maxTimestamp + 1
                );
                replayedRows = replayToApplied(
                        instance,
                        windowFactory,
                        restored.normalizedBaseSeqTxn,
                        durableBaseSeqTxn,
                        lowTimestamp,
                        durableFrontierTimestamp
                );
            }
            if (replayedRows == REPLAY_TO_APPLIED_O3) {
                // The legacy O3 path completed a full, timestamp-ordered rewrite.
                // Phase 5 replaces this hand-off with bounded timeline repair.
                instance.setCheckpointRestoreSucceeded();
                return;
            }

            final long rebuiltLvRows = restored.effectiveLvRowPosition + replayedRows;
            if (rebuiltLvRows != durableLvRowCount
                    || instance.getLatestSeenTs() != durableFrontierTimestamp) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint timeline rebuild does not match durable materialization")
                        .put(" [rootRows=").put(restored.effectiveLvRowPosition)
                        .put(", replayedRows=").put(replayedRows)
                        .put(", durableRows=").put(durableLvRowCount)
                        .put(", rebuiltFrontier=").put(instance.getLatestSeenTs())
                        .put(", durableFrontier=").put(durableFrontierTimestamp).put(']');
            }

            instance.setLastProcessedSeqTxn(durableBaseSeqTxn);
            instance.setAppliedWatermark(durableBaseSeqTxn);
            instance.setLvRowsTotal(rebuiltLvRows);
            // Publish the restored root as the checkpoint head, replacing the
            // placeholder maxTs/stateBytes startup stamped from the superblock
            // alone. writtenUs stays LONG_NULL: it marks a head this process
            // restored rather than wrote, which is what maybeWriteHeadCheckpoint's
            // restored-head trigger keys off to seal on the first post-restart
            // flush.
            instance.setHeadCheckpoint(
                    restored.normalizedBaseSeqTxn,
                    restored.normalizedBaseSeqTxn,
                    restored.maxTimestamp,
                    restored.logicalStateBytes,
                    Numbers.LONG_NULL
            );
            instance.setCheckpointRestoreSucceeded();
            LOG.info().$("restored live view from checkpoint timeline [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", generation=").$(restored.generation)
                    .$(", checkpointId=").$(restored.checkpointId)
                    .$(", boundary=").$ts(restored.maxTimestamp)
                    .$(", frontier=").$ts(durableFrontierTimestamp)
                    .$(", baseSeqTxn=").$(durableBaseSeqTxn)
                    .$(", replayedRows=").$(replayedRows).I$();
        } catch (Throwable t) {
            LOG.error().$("could not restore live view from checkpoint timeline, rebuilding derived state [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
            rebuildTimelineRecoveryFromAppliedBase(
                    instance,
                    windowFactory,
                    durableBaseSeqTxn,
                    "timeline restore failed"
            );
        }
    }

    private void rebuildTimelineRecoveryFromAppliedBase(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long durableBaseSeqTxn,
            CharSequence cause
    ) {
        LOG.info().$("live view restart rebuilding from applied base [view=")
                .$(instance.getDefinition().getViewName())
                .$(", cause=").$(cause)
                .$(", appliedWatermark=").$(durableBaseSeqTxn).I$();
        try {
            o3HeadMissReplay(
                    instance,
                    windowFactory,
                    Numbers.LONG_NULL,
                    instance.getDefinition().getBaseTableToken(),
                    durableBaseSeqTxn,
                    true
            );
            instance.setCheckpointRestoreSucceeded();
        } catch (Throwable t) {
            LOG.critical().$("live view restart applied-base rebuild failed [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
            instance.setPendingInvalidationReason("live view restart timeline recovery failed");
        }
    }

    /**
     * Whether a restart that found <b>no</b> head {@code .cp} must still route
     * through {@link #tryRestoreFromHead} - which rebuilds - rather than fall
     * through to the caller's incremental drain.
     * <p>
     * The drain resumes from the applied watermark with the accumulators at
     * identity, so it is correct only for a view whose window state really is at
     * identity. Over a view that has already materialised rows it recomputes the
     * post-watermark rows from zero and durably flushes wrong cumulative
     * aggregates ({@code row_number()}, {@code sum() OVER (ORDER BY ts)}) with no
     * crash and no invalidation. Every ACTIVE view that has emitted a row also
     * sealed a {@code .cp} for it, so the two normally coincide and this predicate
     * is false; it separates them on the paths where the {@code .cp} is gone but
     * the rows are not:
     * <ul>
     *     <li>a <b>lost</b> {@code _lv.s} persist - not merely a trailing one -
     *     puts every sealed {@code .cp} above the raw watermark the startup sweep
     *     gates the head on, and {@code reconcileAppliedFloorAfterRestart} clamps
     *     the floor back up only after the sweep has already declined (and,
     *     without a manifest exempting them, unlinked) the lot;</li>
     *     <li>a run whose {@code .cp} writes all failed - derived state, non-fatal
     *     by design.</li>
     * </ul>
     * The three exclusions are not defensive:
     * <ul>
     *     <li><b>Lead reconstruction</b> - a read-only replica must never write
     *     disk, and {@code .cp} state does not replicate, so "no head" is its
     *     resting shape and a rebuild would both corrupt the contract and trip
     *     {@code o3Replay}'s replica assertion.</li>
     *     <li><b>SEEDING</b> - the seed sweep owns the resume, from its own
     *     {@code .scp} namespace and its own floor. Its rows are mid-sweep, not
     *     abandoned.</li>
     *     <li><b>Nothing materialised</b> - identity accumulators over a view that
     *     has emitted nothing and consumed no base commit it owns are simply
     *     correct, and this is the resting state of an idle view seeded over an
     *     empty base. Rebuilding it every restart would cost a base scan to
     *     recompute nothing.</li>
     * </ul>
     * The two materialisation probes are deliberately OR'd, and each covers what
     * the other misses: the row count alone misses a view whose rows a TTL or DROP
     * PARTITION has since removed while its accumulators stayed advanced, and the
     * seqTxn comparison alone misses a view whose rows came from the seed, which
     * completes <em>at</em> {@code subscribeFromSeqTxn - 1}. A false positive costs
     * one rebuild the view did not need; a false negative is silent corruption.
     */
    private boolean needsHeadlessRestartRecovery(LiveViewInstance instance, boolean leadReconstruction) {
        if (leadReconstruction || instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE) {
            return false;
        }
        if (instance.getStateReader().getLastProcessedSeqTxn() >= instance.getStateReader().getSubscribeFromSeqTxn()) {
            return true;
        }
        try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
            return lvReader.size() > 0;
        }
    }

    /**
     * Decides whether the {@code _checkpoints/_ring} manifest the startup sweep
     * stashed may be trusted, rehydrates the retained-checkpoint ring from it
     * when it may, and names the checkpoint {@link #tryRestoreFromHead} restores
     * window state from.
     * <p>
     * The trust rule is one comparison:
     * <pre>
     *     trust the ring  iff  ring.coveredBaseSeqTxn == reconciled applied floor
     * </pre>
     * At equality every listed entry is sealed at the view's true durable
     * position, which is exactly what an anchor must be. The floor has to be the
     * <em>reconciled</em> one - {@code _lv.s} is a stale lower bound by design,
     * because {@code persistState} cannot persist-then-publish, and
     * {@code reconcileAppliedFloorAfterRestart} clamps it back up from the LV
     * table. Comparing against the raw {@code _lv.s} value instead would read the
     * routine crash window (manifest published, {@code _lv.s} not yet) as a
     * mismatch and discard the ring on precisely the restarts it exists for.
     * That is why this runs on the refresh worker: the reconciled floor does not
     * exist on the startup thread the sweep runs on.
     * <p>
     * Everything else falls back, conservatively and non-fatally: no manifest, an
     * unreadable one, a version-skewed one, one naming a checkpoint that is gone,
     * a {@code covered} that does not match, or an entry this code could not have
     * written. Ring state is derived, so a fallback costs one boundary rebuild
     * and never invalidates the view.
     * <p>
     * <b>Under trust the manifest, not the directory, defines the anchors.</b> The
     * sweep's highest surviving {@code .cp} is the <em>fallback</em> head, and a
     * trusted manifest that lists an entry overrides it: the newest listed entry
     * becomes the head even when it sits above the raw watermark the sweep gated
     * on - the manifest vouches for it, and the sweep exempted the file for
     * exactly this - and a higher unlisted {@code .cp} is ignored rather than
     * restored, whatever its filename says.
     * <p>
     * A trusted manifest that lists <em>nothing</em> takes the fallback head all
     * the same, and deliberately: it withholds anchors, it does not condemn the
     * directory. Restoring an unlisted head cannot resurrect stale window state,
     * because {@link #tryRestoreFromHead} re-seeds {@code latestSeenTs} from the
     * head's own {@code maxTs} and then replays the checkpoint-to-applied gap - a
     * head that a consumed commit unsealed is unsealed by a commit inside that
     * gap, by construction, so {@code replayToApplied} detects the O3 and rebuilds.
     * The ring therefore only ever gains an entry that replay proved sealed, which
     * is the property the allow-list protects, and refusing the head here would
     * buy a full scan on a doubly-degraded path (a retirement whose unlink AND
     * whose post-replay {@code .cp} write both failed) that recovers correctly
     * without one. Do not "harden" this into a rebuild without a reproduction.
     *
     * @param reconciledFloor the applied watermark after
     *                        {@code reconcileAppliedFloorAfterRestart}
     * @return the {@code lvSeqTxn} to restore window state from: the ring's newest
     * listed entry when the manifest is trusted and lists one, the sweep's
     * fallback head otherwise.
     */
    private long rehydrateCheckpointRing(LiveViewInstance instance, long reconciledFloor) {
        final long fallbackHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
        final LiveViewCheckpointRingCandidate candidate = instance.getCheckpointRingCandidate();
        // Single-shot, whatever the verdict: the sweep that produced the
        // candidate runs once per process and this restore runs once per LV
        // lifetime, so nothing downstream may read startup state as live.
        instance.setCheckpointRingCandidate(null);
        if (candidate == null || !candidate.isStructurallyValid()) {
            // No manifest on disk, or one the sweep could not read. The absent
            // and corrupt cases are indistinguishable here - the sweep nulls the
            // candidate for both - and both count, being equally a fallback the
            // first post-restart O3 pays for. The read logs which at its own site.
            // A view whose first ever restart predates its first publication
            // counts one too, and that is the honest reading: it recovers no ring
            // and pays the same scan as a view whose manifest went missing.
            instance.recordCheckpointRingRecoveryFallback();
            return fallbackHeadLvSeqTxn;
        }
        final long covered = candidate.getCoveredBaseSeqTxn();
        final int entryCount = candidate.getEntryCount();
        if (covered != reconciledFloor || !isCheckpointRingRehydratable(instance, candidate)) {
            LOG.info().$("live view checkpoint ring recovery fallback [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", reason=").$(covered != reconciledFloor ? "covered does not match the reconciled floor" : "entry not rehydratable")
                    .$(", generation=").$(candidate.getGeneration())
                    .$(", ringCovered=").$(covered)
                    .$(", reconciledFloor=").$(reconciledFloor)
                    .$(", entries=").$(entryCount)
                    .$(", fallbackHeadLvSeqTxn=").$(fallbackHeadLvSeqTxn).I$();
            instance.recordCheckpointRingRecoveryFallback();
            discardCheckpointRingManifest(instance);
            return fallbackHeadLvSeqTxn;
        }
        for (int i = 0; i < entryCount; i++) {
            instance.addRetainedCheckpoint(
                    candidate.getEntryLvSeqTxn(i),
                    candidate.getEntryMaxTs(i),
                    candidate.getEntryBaseSeqTxn(i),
                    candidate.getEntryLvRowsTotal(i),
                    candidate.getEntryStateBytes(i)
            );
        }
        // Adopt the manifest as this process's durable ring state before anything
        // republishes over it. The generation must continue the on-disk counter
        // rather than restart at 1, or a run's publications reuse generations the
        // manifest already burned and stop being countable from the logs. And
        // lastPublishedRingNewestBaseSeqTxn is the durable arm of WalPurgeJob's
        // base WAL floor, which only a publication otherwise stamps - rehydration
        // is the one path that adopts a manifest this process did not publish, so
        // leaving it LONG_NULL would make the two arms disagree for no reason.
        instance.recordCheckpointRingPublication(
                candidate.getGeneration(),
                covered,
                entryCount == 0 ? Numbers.LONG_NULL : candidate.getEntryBaseSeqTxn(entryCount - 1)
        );
        pruneRehydratedCheckpointRing(instance, covered);
        final int retainedCount = instance.getRetainedCheckpointCount();
        // The trust verdict, for live_views(). Post-prune rather than the
        // manifest's entryCount: what an operator wants to know is how many
        // anchors this process came back with, and a lowered retention budget
        // drops some of what the manifest listed. Recorded on the empty path too -
        // a trusted manifest listing nothing is not a fallback, and only the pair
        // (entries=0, fallbacks=0) tells the two apart.
        instance.recordCheckpointRingRecovery(retainedCount);
        if (retainedCount == 0) {
            // Trusted, but it offers no anchor - so the sweep's head stands, and
            // the restore validates it the way it does without any manifest.
            LOG.info().$("live view checkpoint ring restored empty [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", generation=").$(candidate.getGeneration())
                    .$(", coveredBaseSeqTxn=").$(covered)
                    .$(", fallbackHeadLvSeqTxn=").$(fallbackHeadLvSeqTxn).I$();
            return fallbackHeadLvSeqTxn;
        }
        final long newestLvSeqTxn = instance.getRetainedCheckpointLvSeqTxn(retainedCount - 1);
        // Stamp the newest listed entry as the head, replacing the placeholders
        // the startup sweep left (subscribeFromSeqTxn as a safe purge-floor lower
        // bound, a LONG_NULL maxTs and zero stateBytes) - the manifest knows the
        // real values. Ahead of the restore, so that a corrupt anchor sends
        // handleCorruptHeadCheckpoint at the .cp the head actually names and it
        // clears the head trio rather than stranding it on the sweep's pick.
        instance.setHeadCheckpoint(
                newestLvSeqTxn,
                instance.getRetainedCheckpointBaseSeqTxn(retainedCount - 1),
                instance.getRetainedCheckpointMaxTs(retainedCount - 1),
                instance.getRetainedCheckpointStateBytes(retainedCount - 1),
                Numbers.LONG_NULL
        );
        LOG.info().$("live view checkpoint ring restored [view=")
                .$(instance.getDefinition().getViewName())
                .$(", generation=").$(candidate.getGeneration())
                .$(", coveredBaseSeqTxn=").$(covered)
                .$(", entries=").$(retainedCount)
                .$(", oldestMaxTs=").$(instance.getRetainedCheckpointMaxTs(0))
                .$(", headLvSeqTxn=").$(newestLvSeqTxn)
                .$(", headMaxTs=").$(instance.getRetainedCheckpointMaxTs(retainedCount - 1)).I$();
        return newestLvSeqTxn;
    }

    /**
     * Whether every entry of a trusted manifest describes a checkpoint this code
     * could have written. Rejects the manifest <b>whole</b> rather than entry by
     * entry, the way the sweep's missing-{@code .cp} rule does: a partial ring is
     * a claim nothing backs, and membership is what makes the survivors
     * meaningful.
     * <p>
     * The codec cannot enforce this. It validates ordering and the
     * {@code coveredBaseSeqTxn} bounds, and {@link Numbers#LONG_NULL} is
     * {@code Long.MIN_VALUE}, so a LONG_NULL field passes every one of them by
     * being smaller than whatever it is compared against. No such entry is
     * reachable from either {@code addRetainedCheckpoint} call site - both stamp
     * real values - but rehydration is what first turns manifest bytes into ring
     * records, so the check belongs here:
     * <ul>
     *     <li>a LONG_NULL {@code baseSeqTxn} restamps the ring's purge-floor
     *     mirror to LONG_NULL, which {@code WalPurgeJob} reads as "no floor" and
     *     which would release the base WAL of a ring about to be trusted;</li>
     *     <li>a LONG_NULL {@code maxTs} undercuts every
     *     {@link #findResumeAnchorBelow} ceiling and would anchor a replay at
     *     {@code LONG_NULL + 1}, admitting every base row including those below
     *     the START FROM boundary - the same reason
     *     {@link #promoteRestoredHeadIntoRing} refuses one;</li>
     *     <li>a LONG_NULL {@code lvSeqTxn} names no {@code .cp} the sweep's
     *     {@code exists()} check could have passed, so a manifest carrying one
     *     describes a directory that cannot exist.</li>
     * </ul>
     */
    private boolean isCheckpointRingRehydratable(LiveViewInstance instance, LiveViewCheckpointRingCandidate candidate) {
        for (int i = 0, n = candidate.getEntryCount(); i < n; i++) {
            if (candidate.getEntryLvSeqTxn(i) == Numbers.LONG_NULL
                    || candidate.getEntryMaxTs(i) == Numbers.LONG_NULL
                    || candidate.getEntryBaseSeqTxn(i) == Numbers.LONG_NULL) {
                LOG.error().$("live view checkpoint ring manifest entry has a null coordinate [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", entryIndex=").$(i)
                        .$(", lvSeqTxn=").$(candidate.getEntryLvSeqTxn(i))
                        .$(", maxTs=").$(candidate.getEntryMaxTs(i))
                        .$(", baseSeqTxn=").$(candidate.getEntryBaseSeqTxn(i)).I$();
                return false;
            }
        }
        return true;
    }

    /**
     * Trims a rehydrated ring back inside the running retention budget and
     * unlinks whatever falls out.
     * <p>
     * The codec deliberately does not enforce the count / byte bounds - it has no
     * configuration - and rejecting a manifest over budget would be the wrong
     * answer anyway: an operator <em>lowering</em> a budget between restarts
     * should cost a prune, which satisfies the bound for free, not a full scan.
     * The event-time horizon keys off the newest entry, exactly as the add path
     * keys it off the fresh head's {@code batchMaxTs}.
     * <p>
     * Republishes before unlinking, the way {@code maybeWriteHeadCheckpoint}
     * does: the manifest on disk still lists what the prune dropped, and
     * unlinking first would leave it naming missing files, which the next
     * restart's {@code exists()} check rejects whole. The unlink then runs
     * regardless of the publication's outcome - a stale manifest costs one
     * fallback, while retaining the files leaks the disk the prune exists to
     * bound, and they are unlisted garbage the moment the next publication lands.
     */
    private void pruneRehydratedCheckpointRing(LiveViewInstance instance, long coveredBaseSeqTxn) {
        final int entryCount = instance.getRetainedCheckpointCount();
        if (entryCount == 0) {
            return;
        }
        evictedCheckpoints.clear();
        pruneRetainedCheckpointsToBudget(instance, instance.getRetainedCheckpointMaxTs(entryCount - 1), evictedCheckpoints);
        if (evictedCheckpoints.size() == 0) {
            return;
        }
        LOG.info().$("live view pruned rehydrated checkpoint ring [view=")
                .$(instance.getDefinition().getViewName())
                .$(", evicted=").$(evictedCheckpoints.size())
                .$(", retained=").$(instance.getRetainedCheckpointCount()).I$();
        publishCheckpointRing(instance, coveredBaseSeqTxn);
        unlinkCheckpointFiles(instance, evictedCheckpoints);
    }

    /**
     * Prunes the retained-checkpoint ring back within the configured retention
     * budget, measuring the event-time horizon from {@code referenceMaxTs} - the
     * newest entry the ring is meant to keep, which is the fresh head on the add
     * path and the manifest's last entry on the rehydrate path.
     * <p>
     * The single place that says which knobs bound the ring, so the two callers
     * cannot drift onto different budgets. Count and bytes are the primary
     * bounds. The event-time horizon is a loose upper safety, disabled by
     * default: when enabled, an entry older than {@code retentionMicros} below
     * {@code referenceMaxTs} is pruned. At real ingest rates count/bytes bind
     * first - near-head checkpoint spacing covers many times the observed base
     * lateness - and a {@code retentionMicros <= 0} (the default) disables the
     * horizon so low-rate views keep their older, event-time-distant anchors
     * instead of collapsing the ring to a single entry.
     * <p>
     * {@code pruneRetainedCheckpoints} always keeps the newest entry, and appends
     * each evicted {@code lvSeqTxn} to {@code evictedOut} for the caller to
     * unlink - this touches no files.
     */
    private void pruneRetainedCheckpointsToBudget(LiveViewInstance instance, long referenceMaxTs, LongList evictedOut) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final long retentionMicros = configuration.getLiveViewCheckpointRetentionMicros();
        instance.pruneRetainedCheckpoints(
                configuration.getLiveViewCheckpointRetentionCount(),
                configuration.getLiveViewCheckpointRetentionMaxBytes(),
                retentionMicros > 0 ? referenceMaxTs - retentionMicros : Numbers.LONG_NULL,
                evictedOut
        );
    }

    /**
     * Removes {@code _checkpoints/_ring} after the trust decision fell back.
     * <p>
     * Best-effort and non-fatal: a failed removal leaves a manifest whose
     * {@code covered} still does not match the floor, so it stays untrusted, and
     * the next publication overwrites it. Removing it matters for the {@code .cp}
     * files the sweep exempted on its behalf: they survived as an allow-list that
     * turned out not to be trustworthy, and with the manifest gone the next
     * restart's sweep retires them with no allow-list at all.
     */
    private void discardCheckpointRingManifest(LiveViewInstance instance) {
        path.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken());
        // Self-base: Path.of(this) is a no-op, so this addresses _ring off the LV
        // directory just built - through the one path builder every other site
        // uses, so the two cannot drift.
        LiveViewCheckpointRingManifest.ringManifestPath(path, path);
        engine.getConfiguration().getFilesFacade().removeQuiet(path.$());
        // The manifest listed the entry the startup read pinned the base WAL
        // floor at, and it lists nothing now. Release the arm so the floor
        // follows the head this fallback actually resumes from.
        instance.releaseCheckpointRingPurgeFloor();
    }

    /**
     * Seeds the retained-checkpoint ring with the head {@link #tryRestoreFromHead}
     * just restored, making it the ring's sole entry.
     * <p>
     * This grants the head no trust it did not already hold: the head-hit branch of
     * {@link #o3Replay} resumes from it directly, off the head metadata, without
     * consulting the ring at all. Listing it only lets the ring SEARCH
     * ({@link #findResumeAnchorBelow}) find it. That search is what an apply-ahead
     * O3 falls back on when {@link ApplyWal2TableJob} has raced the base reader past
     * the trigger: {@link #replayFromAnchor} then needs an anchor strictly below
     * {@code min(triggerLowTs, minAheadTs)}, and against an empty ring that lookup
     * fails and forces an O(view age) rebuild from the view's lower bound - even
     * when the restored head sits below the ahead floor and would have served.
     * <p>
     * The ring's newest entry stays the head, which WalPurgeJob's base WAL
     * purge floor depends on: it holds the floor at
     * {@code getHeadCheckpointBaseSeqTxn()}, so an entry the floor does not cover
     * could not be resumed from. One entry, equal to the head, cannot violate that.
     * <p>
     * A LONG_NULL maxTs head must never enter the ring. findResumeAnchorBelow
     * selects on {@code maxTs < ceilTs}, so LONG_NULL would undercut every ceiling
     * and anchor a replay at {@code LONG_NULL + 1} - admitting every base row,
     * including rows below the START FROM boundary this path does not re-apply. The
     * same value is already refused hit-eligibility in {@link #o3Replay}; refuse it
     * here for the same reason.
     */
    private void promoteRestoredHeadIntoRing(LiveViewInstance instance, long headLvSeqTxn, long manifestBaseSeqTxn) {
        if (restoredHeadState.maxTimestamp == Numbers.LONG_NULL) {
            return;
        }
        if (instance.getRetainedCheckpointCount() > 0) {
            // rehydrateCheckpointRing trusted the manifest, so the ring already
            // ends on this head (the assert in tryRestoreFromHead pins that) and
            // carries the older anchors the manifest vouched for. Adding it again
            // would trip addRetainedCheckpoint's strictly-increasing-maxTs
            // contract. Promotion is the no-manifest fallback's way of reaching
            // the same place with one entry.
            return;
        }
        // The .cp this entry names is the one restoreFromHead just read, and
        // nothing unlinks it: maybeWriteHeadCheckpoint commits the next head with
        // LONG_NULL, which suppresses commit()'s prior-head unlink, and the ring
        // governs retirement from here on.
        instance.addRetainedCheckpoint(
                headLvSeqTxn,
                restoredHeadState.maxTimestamp,
                manifestBaseSeqTxn,
                restoredHeadState.lvRowsTotal,
                restoredHeadState.stateBytes
        );
        LOG.info().$("live view promoted restored head into checkpoint ring [view=")
                .$(instance.getDefinition().getViewName())
                .$(", lvSeqTxn=").$(headLvSeqTxn)
                .$(", baseSeqTxn=").$(manifestBaseSeqTxn)
                .$(", maxTs=").$(restoredHeadState.maxTimestamp).I$();
    }

    /**
     * Decodes a single FUNCTION_SNAPSHOT block:
     * <pre>
     *     STR windowName
     *     STR factoryName     (matches snapshotFactoryName(f) - factory class,
     *                          not the function impl, so impl renames survive)
     *     INT formatVersion
     *     ...key-shape header + per-partition state (consumed by
     *        {@link LiveViewFunctionSnapshot#restore})
     * </pre>
     * Then decodes the trailing payload directly from the mapped checkpoint and
     * pairs the block with a running window function positionally: the
     * writer emits one block per snapshot-capable function in
     * {@code getWindowFunctions()} order, so the i-th block restores into the
     * i-th snapshot-capable function. Matching by factory name alone is
     * ambiguous - a view can hold several functions from one factory (e.g.
     * {@code min(x)} and {@code max(x)} share {@code MaxDoubleWindowFunctionFactory},
     * as do bounded and unbounded RANGE frames of the same function), so a
     * name-only first-match would route every block to the first such function
     * and either overflow on a layout mismatch or silently restore crossed
     * state. The stored factory name is still validated against the paired
     * function to catch a window-function-order drift.
     */
    private void restoreFunctionBlock(LiveViewCheckpointReader.ReadableBlock block, ObjList<WindowFunction> functions) {
        long offset = 0;
        // windowName: STR. We only need to skip past it - the manifest
        // already captured window names, and the writer stamps the anchor
        // window name (shared across all blocks), so it is not a per-function
        // discriminator; positional pairing below resolves the function.
        offset += strByteSize(block, offset);
        final CharSequence storedFactoryName = block.getStr(offset);
        final long factoryNameByteSize = strByteSize(block, offset);
        offset += factoryNameByteSize;
        final int formatVersion = block.getInt(offset);
        offset += Integer.BYTES;

        // Advance to the next snapshot-capable function, mirroring the writer's
        // !supportsCheckpointState() skip so the positional pairing stays aligned.
        WindowFunction match = null;
        while (restoreFunctionCursor < functions.size()) {
            final WindowFunction candidate = functions.getQuick(restoreFunctionCursor++);
            if (candidate.supportsCheckpointState()) {
                match = candidate;
                break;
            }
        }
        if (match == null) {
            throw CairoException.critical(0)
                    .put("more live view function snapshot blocks than snapshot-capable functions, factory=")
                    .put(storedFactoryName);
        }
        if (!Chars.equals(storedFactoryName, snapshotFactoryName(match))) {
            // Window-function order drifted vs the writer (e.g. a definition
            // change across an upgrade). Errno 0 unlinks the head .cp and
            // head-miss-replays rather than restoring crossed state.
            throw CairoException.critical(0)
                    .put("live view function snapshot factory mismatch [position=")
                    .put(restoreFunctionCursor - 1)
                    .put(", expected=")
                    .put(snapshotFactoryName(match))
                    .put(", got=")
                    .put(storedFactoryName)
                    .put(']');
        }
        // A version outside [checkpointStateMinSupportedVersion(), checkpointStateFormatVersion()]
        // signals a real compatibility break (operator DROP+CREATE is the
        // recovery), not structural corruption. Tag the throws so the catch site
        // invalidates the LV rather than unlinking and replaying from head-miss.
        // Mirrors the file-level range check in LiveViewCheckpointReader.of().
        if (formatVersion < match.checkpointStateMinSupportedVersion()) {
            throw CairoException.critical(CairoException.LV_FUNCTION_SNAPSHOT_VERSION_MISMATCH)
                    .put("live view function snapshot version too old, factory=")
                    .put(storedFactoryName)
                    .put(", read=")
                    .put(formatVersion)
                    .put(", minSupported=")
                    .put(match.checkpointStateMinSupportedVersion());
        }
        if (formatVersion > match.checkpointStateFormatVersion()) {
            // A newer writer laid this state out to a shape this build has no
            // decoder for - restoreCheckpointState() reads the current fixed
            // layout and never dispatches on a higher version. Accepting the
            // block would silently rehydrate the accumulators from foreign
            // bytes. A downgraded binary reaches exactly this: the newer
            // binary's CRC-valid .cp is still the head on disk.
            throw CairoException.critical(CairoException.LV_FUNCTION_SNAPSHOT_VERSION_MISMATCH)
                    .put("live view function snapshot version too new, factory=")
                    .put(storedFactoryName)
                    .put(", read=")
                    .put(formatVersion)
                    .put(", maxSupported=")
                    .put(match.checkpointStateFormatVersion());
        }

        final long payloadStart = offset;
        final long payloadLength = block.size() - payloadStart;
        LiveViewFunctionSnapshot.restore(block.memory(), block.payloadStart() + payloadStart, payloadLength, match, formatVersion);
    }


    private static long strByteSize(LiveViewCheckpointReader.ReadableBlock block, long offset) {
        // STR encoding: INT length prefix + length * CHAR (2 bytes each). A null
        // STR encodes length -1 with no char bytes; windowName/factoryName are
        // never null today, but mis-sizing a null as prefix-minus-2 would
        // misalign every field after it, so guard.
        final int len = block.getInt(offset);
        if (len < 0) {
            return Integer.BYTES;
        }
        return Integer.BYTES + (long) len * Character.BYTES;
    }

    /**
     * Computes the AND of (a) anchor-map key codec support and (b) every
     * compiled window function's {@code supportsCheckpointState()}. Called once
     * per LV lifetime on the first refresh after the compiled factory is
     * available; subsequent calls short-circuit on the cached flag.
     */
    protected static boolean computeSnapshotCapability(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        if (anchorWindow != null && !LiveViewSnapshotKeyCodec.isAllTypesSupported(anchorWindow.getPartitionKeyTypes())) {
            return false;
        }
        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).supportsCheckpointState()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Prepares the worker-local staging buffer and the LV's own in-memory tier
     * for the upcoming cycle. Returns {@code true} when both are usable for
     * this LV (every output column type is one the tier can store - fixed-width,
     * SYMBOL, STRING, BINARY, VARCHAR, ARRAY); {@code false} when any column type
     * is unsupported (a non-persisted type such as INTERVAL), in which case the cycle still writes to
     * the on-disk tier but the in-mem tier stays empty / unallocated and reads fall
     * back to {@code TableReader}.
     * <p>
     * The reusable {@code tierColumnTypes} member captures the output schema for
     * this cycle, doubling as the staging buffer's shape and the LV tier's
     * shape on first allocation. No per-cycle {@code IntList} is allocated.
     */
    /**
     * Computes (once, then caches) whether the LV's in-mem tier may serve an
     * un-flushed lead ahead of disk. Eligible when the output schema has a
     * designated timestamp and every column is a type the tier can store
     * (fixed-width, SYMBOL, STRING, BINARY, VARCHAR, ARRAY). SYMBOL columns are
     * eligible: the lead drain eager-interns them into the tier's symbol cache
     * (LV-table-consistent ids), so the read path resolves the lead's symbols from
     * RAM. Ineligible LVs (a non-persisted output column type such as INTERVAL, or no
     * designated timestamp) keep the tier a strict subset of disk: the refresh worker
     * applies every cycle for them. Compiles the SELECT on the first call if needed
     * (cached for the LV's lifetime).
     */
    private boolean ensureLeadEligible(LiveViewInstance instance) throws SqlException {
        if (instance.isLeadEligibilityComputed()) {
            return instance.isLeadEligible();
        }
        WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        RecordMetadata outMetadata = windowFactory.getMetadata();
        boolean eligible = outMetadata.getTimestampIndex() >= 0;
        for (int i = 0, n = outMetadata.getColumnCount(); eligible && i < n; i++) {
            if (!LiveViewInMemoryBuffer.isColumnTypeSupported(outMetadata.getColumnType(i))) {
                eligible = false;
            }
        }
        instance.setLeadEligible(eligible);
        return eligible;
    }

    private boolean ensureStagingAndTier(LiveViewInstance instance, RecordMetadata outMetadata, int tsColIdx) {
        // Capture the output column types into the reusable IntList; this
        // doubles as the unsupported-type probe and the shape-mismatch check
        // against the cached staging buffer. Member-resident so the per-FLUSH-
        // cycle path stays allocation-free.
        tierColumnTypes.clear();
        for (int i = 0, n = outMetadata.getColumnCount(); i < n; i++) {
            tierColumnTypes.add(outMetadata.getColumnType(i));
        }
        if (!LiveViewInMemoryBuffer.areColumnTypesSupported(tierColumnTypes)) {
            // LV output schema contains a column type the tier cannot store (a
            // non-persisted type such as INTERVAL); skip the in-mem tier population
            // for this LV. The cursor reads disk-only via TableReader.
            return false;
        }
        long pageSize = engine.getConfiguration().getLiveViewInMemoryBufferInitialBytes();
        // The view's refresh tracker charges the tier's data-scaled native memory against
        // cairo.live.view.refresh.memory.limit.bytes. Acquired by ensureCompiledFactory before
        // this runs; null only on a defensive path, which degrades to global-only accounting.
        final MemoryTracker viewTracker = instance.getMemoryTracker();
        if (stagingBuffer == null || !stagingColumnTypes.equals(tierColumnTypes)) {
            // Shape changed, or first use this cycle. The staging buffer is bound to the view's refresh
            // tracker so a large drain's staged output is capped by cairo.live.view.refresh.memory.limit
            // .bytes like the rest of the cycle's query state (one base commit is staged in a single
            // batch, so an unbounded commit would otherwise balloon it). refreshInstance's finally frees
            // it at the end of EVERY cycle, so it is normally null here and is never retained across a
            // cycle boundary: that cycle-scoped ownership is what makes the tracker binding safe, since
            // the buffer is worker-owned and keeping its tracker-charged pages past the cycle would let
            // an async invalidation (e.g. a base recreate on the DDL thread) free the tracker dirty.
            stagingBuffer = Misc.free(stagingBuffer);
            stagingColumnTypes.clear();
            for (int i = 0, n = tierColumnTypes.size(); i < n; i++) {
                stagingColumnTypes.add(tierColumnTypes.getQuick(i));
            }
            stagingBuffer = new LiveViewInMemoryBuffer(stagingColumnTypes, tsColIdx, pageSize, viewTracker);
            stagingTimestampColumnIndex = tsColIdx;
        }
        stagingBuffer.reset();
        // Allocate the per-LV in-mem tier on first use; subsequent cycles reuse
        // it. The tier's shape is fixed at allocation — if a downstream commit
        // changes the LV's _meta (reserved for ALTER LIVE VIEW later) the tier
        // would need to be reshaped too. Today _meta is immutable post-CREATE.
        if (instance.getInMemoryTier() == null) {
            instance.setInMemoryTier(new LiveViewInMemoryTier(tierColumnTypes, tsColIdx, pageSize, viewTracker));
        }
        // Capture the SYMBOL output-column indexes so the lead drain can
        // eager-intern them into the tier's symbol cache.
        stagingSymbolColumnIndexes.clear();
        for (int i = 0, n = tierColumnTypes.size(); i < n; i++) {
            if (ColumnType.tagOf(tierColumnTypes.getQuick(i)) == ColumnType.SYMBOL) {
                stagingSymbolColumnIndexes.add(i);
            }
        }
        return true;
    }

    /**
     * Decides whether a slow-path evict-and-swap is worth doing on this lead/subset
     * publish. The slow path copies the whole retained set (the IN MEMORY overlap
     * still resident plus the un-flushed lead) into the other slot before swapping,
     * so it is O(retained). Only AGED overlap rows - already durable on disk and
     * older than the IN MEMORY horizon - can be reclaimed; the un-flushed lead has no
     * disk copy and is never evicted. So a swap is worthwhile only once at least
     * {@code growthBudget} bytes of such rows have accumulated: that bounds the extra
     * RAM held above the IN MEMORY window to the growth budget and amortises the copy
     * across the many in-place appends that happen in between.
     * <p>
     * When it returns {@code false} the caller appends in place (fast path) if no
     * reader pins the published slot; a reader pin still forces the slow path via the
     * failed {@code tryAcquireWrite}. Returning {@code true} for {@code growthBudget <= 0}
     * preserves the "compact every publish" behaviour some tests rely on.
     */
    private boolean isCompactionWorthwhile(
            LiveViewInMemoryBuffer pubSlot,
            long stagingMaxTs,
            LiveViewInstance instance,
            long growthBudget
    ) {
        if (growthBudget <= 0) {
            // No slack configured: compact on every publish (aggressive eviction).
            return true;
        }
        if (stagingMaxTs == Numbers.LONG_NULL) {
            return false;
        }
        // Only the overlap prefix [0, overlapCount) can age out; the trailing lead
        // never does.
        final long overlapCount = pubSlot.rowCount() - pubSlot.leadRowCount();
        final long budgetRows = growthBudget / Math.max(1, pubSlot.approxRowSizeBytes());
        if (overlapCount <= budgetRows) {
            // Fewer than a budget's worth of reclaimable rows even if all overlap
            // aged out - not worth an O(retained) copy yet.
            return false;
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType());
        final long retainThreshold = stagingMaxTs - driver.fromMicros(instance.getDefinition().getInMemoryMicros());
        // Rows are ts-ascending, so at least budgetRows overlap rows have aged out
        // iff the overlap row at index budgetRows still sits below the horizon.
        return pubSlot.getLong(budgetRows, pubSlot.getTimestampColumnIndex()) < retainThreshold;
    }

    /**
     * Publishes this cycle's staging rows into the LV's in-memory tier
     * (fast-path + slow-path swap), returning {@code true} on success and
     * {@code false} only when both slots are reader-pinned in lead mode.
     * <p>
     * In lead mode ({@code leadMode == true}) the staged rows are the un-flushed
     * lead: the slot's {@code leadRowCount} grows by {@code appendedRows} and the
     * slot is stamped with {@code lvSeqTxn} = the last-flushed LV-table seqTxn, so
     * the overlap agrees with disk while the lead sits on top. In subset mode the
     * staged rows are already on disk (apply preceded this publish), so
     * {@code leadRowCount} stays 0.
     * <p>
     * Two paths share the same {@code 0 -> -1} CAS primitive on a slot's
     * refcount:
     * <ul>
     *   <li><b>Fast-path</b> — try to acquire the writer sentinel on the
     *     <em>published</em> slot. On success (no readers currently pin it
     *     and the slot's footprint is still under the growth budget),
     *     append staging rows in place and release the sentinel via
     *     {@link LiveViewInMemoryTier#releaseWriteWithoutPublish(int)}
     *     without flipping the published index. No per-cycle memcpy of
     *     retained rows; {@code seamTs} stays at the published slot's
     *     existing minimum.</li>
     *   <li><b>Slow-path</b> — acquire the non-published slot, copy
     *     retained rows (those still inside the {@code IN MEMORY} window
     *     relative to {@code stagingMaxTs}), append staging on top, and
     *     publish the swap. The {@code IN MEMORY} eviction runs here; the
     *     fast-path defers it to the next slow-path edge.</li>
     * </ul>
     * Slow-path triggers when (a) a reader holds a pin on the published
     * slot (fast-path CAS fails), or (b) the published slot's footprint
     * already meets or exceeds
     * {@code cairo.live.view.in.memory.buffer.growth.bytes} (growth
     * backstop), or (c) the fast-path acquire fails despite no reader pin
     * — which can only happen if the writer sentinel was somehow left
     * dangling, a contract violation that falls through cleanly.
     * <p>
     * If both slow-path acquire attempts fail (both slots reader-pinned),
     * {@code writerStallStartUs} is set so {@code live_views().writer_stall_micros}
     * surfaces the stall. In subset mode the disk tier is current so the trail is
     * harmless and the method returns {@code true}; in lead mode the lead has
     * nowhere durable to live, so it returns {@code false} and the caller flushes
     * the lead straight to disk.
     */
    private boolean publishToInMemoryTier(LiveViewInstance instance, long stagingMaxTs, long lvSeqTxn, long appendedRows, boolean leadMode) {
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            return true;
        }
        int publishedIdx = tier.getPublishedIdx();
        LiveViewInMemoryBuffer pubSlot = tier.getSlot(publishedIdx);

        // The lead grows by this cycle's staging rows; subset publishes carry no
        // lead. After a both-slots-pinned O3 rebuild skip (dropRetained) the prior
        // lead was already reset to 0, so this expression still yields appendedRows.
        long newLeadRowCount = leadMode ? instance.getLeadRowCount() + appendedRows : 0;

        // A both-slots-pinned O3 rebuild skip left the published slot carrying
        // pre-O3 rows the replay re-sequenced on disk. Drop those retained rows
        // on this publish instead of carrying them forward, so a read never serves
        // stale rows re-stamped with a matching seqTxn. Disk still holds every
        // dropped row, so the slot just rebuilds from this cycle's staging rows.
        boolean dropRetained = instance.isTierStale();

        // Fast-path: append in place when no reader pins the published slot and a
        // slow-path compaction is not yet worthwhile. The slow path copies the
        // entire retained set (up to the IN MEMORY window plus the un-flushed lead)
        // into the other slot, so it must not run on every publish: it is worth
        // paying only once enough AGED overlap rows have piled up for a swap to
        // reclaim a meaningful amount (isCompactionWorthwhile), which amortises the
        // O(retained) copy and keeps the tier from accumulating indefinitely even
        // when readers never pin (an idle LV with steady ingestion). The un-flushed
        // lead never ages out, so a slot dominated by the lead (a long FLUSH EVERY
        // window at high ingest) keeps appending in place instead of copying the
        // whole lead every cycle - the copy the old absolute-footprint gate forced
        // for any tier larger than the growth budget, which O3-throttled the drain
        // and made the view fall behind.
        long growthBudget = engine.getConfiguration().getLiveViewInMemoryBufferGrowthBytes();
        if (!isCompactionWorthwhile(pubSlot, stagingMaxTs, instance, growthBudget)) {
            LiveViewInMemoryBuffer acquired = tier.tryAcquireWrite(publishedIdx);
            if (acquired != null) {
                try {
                    if (dropRetained) {
                        // Reset under the writer sentinel (no reader can observe
                        // it) so the published slot reflects only this cycle's
                        // disk-consistent staging rows; seamTs re-initialises from
                        // the first staged row in appendStagingInPlace.
                        acquired.reset();
                    }
                    acquired.appendStaging(stagingBuffer, stagingBuffer.seamTs());
                    acquired.setLvSeqTxn(lvSeqTxn);
                    acquired.setLeadRowCount(newLeadRowCount);
                } catch (Throwable t) {
                    // Fast-path append cannot leave the slot partially
                    // populated visibly to readers: rowCount only advances
                    // once at the end of appendStaging, after all column
                    // writes have completed, and appendStaging rewinds any
                    // partially-advanced var-size append cursors on
                    // failure, so the slot is byte-identical to its pre-append
                    // state. The writer sentinel (rc = -1) keeps readers
                    // spinning until release. Drop the sentinel and let the
                    // flush-retry budget tick.
                    tier.releaseWriteWithoutPublish(publishedIdx);
                    throw t;
                }
                tier.releaseWriteWithoutPublish(publishedIdx);
                if (leadMode) {
                    instance.setLeadRowCount(newLeadRowCount);
                }
                instance.setWriterStallStartUs(Numbers.LONG_NULL);
                instance.setTierStale(false);
                return true;
            }
        }

        // Slow-path: take the non-published slot, copy retained rows, append
        // staging, swap published index.
        int writeIdx = 1 - publishedIdx;
        LiveViewInMemoryBuffer writeSlot = tier.tryAcquireWrite(writeIdx);
        if (writeSlot == null) {
            // Both slots reader-pinned. Record the start of the stall streak; a
            // subsequent successful acquire clears it.
            if (instance.getWriterStallStartUs() == Numbers.LONG_NULL) {
                instance.setWriterStallStartUs(engine.getConfiguration().getMicrosecondClock().getTicks());
            }
            LOG.info().$("live view in-mem tier stalled, both slots pinned [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            // Subset mode: disk is current, the tier just trails. Lead mode: the
            // lead is not on disk, so the caller must flush it.
            return !leadMode;
        }
        try {
            writeSlot.reset();
            int tsCol = pubSlot.getTimestampColumnIndex();
            long writeRow = 0;
            long writeSeamTs = Numbers.LONG_NULL;
            // Copy retained rows from the currently-published slot, unless the
            // tier is stale (a prior both-pinned O3 rebuild skip): then those
            // rows are pre-O3 and disk has re-sequenced them, so dropping them
            // and rebuilding from staging is the fix - disk still holds every
            // dropped row.
            if (!dropRetained) {
                // Compute the eviction threshold in the base table's timestamp
                // units. IN MEMORY is stored in micros; scale to base units once.
                TimestampDriver driver = ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType());
                long inMemoryInBaseUnits = driver.fromMicros(instance.getDefinition().getInMemoryMicros());
                long retainThreshold = stagingMaxTs - inMemoryInBaseUnits;

                // Eviction. A slot's trailing leadRowCount rows are the un-flushed
                // lead (no durable disk copy) and must never age out; the leading
                // overlap rows are on disk, so they age out once they fall below
                // latest - IN MEMORY. The lead suffix bounds the tier at the IN
                // MEMORY window plus the un-flushed lead, and forces a flush before
                // the lead could span the whole window. In subset mode leadRowCount
                // is 0, so every row is overlap and ages normally.
                //
                // Slot rows are ts-ascending, so the evicted overlap rows form a
                // prefix [0, k) and the retained rows a contiguous suffix
                // [k, rowCount) - retained overlap plus the always-kept lead. Binary
                // search k (lower bound of the eviction threshold) over the overlap
                // region, then bulk-copy the suffix with a single copyRowsFrom - a
                // per-column memcpy for fixed-width / SYMBOL columns - instead of a
                // scalar per-row, per-column copy.
                long leadCount = pubSlot.leadRowCount();
                long overlapCount = pubSlot.rowCount() - leadCount;
                // Clamp the eviction threshold to the lead's minimum timestamp so
                // an overlap group sharing that timestamp stays resident. When the
                // whole overlap ages out (lo == overlapCount) the seam lands at the
                // lead minimum (lead_min); a disk-backed overlap row at exactly
                // lead_min - an additive same-ts row at the frontier, admitted
                // because the O3 trigger is a strict below-frontier compare - would
                // then be served by neither disk (the reader's scan stops strictly
                // below the seam) nor the lead-only slot: silent row loss plus a
                // size() overcount that breaks LIMIT. Retaining every overlap row
                // with ts >= lead_min keeps that group in the slot at the seam,
                // where the overlap band still agrees with disk row-for-row. This
                // mirrors the tierStale rebuild guard in finishLeadRefresh that
                // avoids the same additive-same-ts gap. In the common unique-ts
                // case lead_min is strictly above every overlap ts, so the clamp
                // retains nothing extra and eviction is unchanged.
                long evictionThreshold = retainThreshold;
                if (leadCount > 0) {
                    long leadMinTs = pubSlot.getLong(overlapCount, tsCol);
                    if (leadMinTs < evictionThreshold) {
                        evictionThreshold = leadMinTs;
                    }
                }
                long lo = 0;
                long hi = overlapCount;
                while (lo < hi) {
                    long mid = (lo + hi) >>> 1;
                    if (pubSlot.getLong(mid, tsCol) < evictionThreshold) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                // lo is the first retained overlap row, or overlapCount when the
                // whole overlap aged out and only the lead survives.
                long retainedCount = pubSlot.rowCount() - lo;
                if (retainedCount > 0) {
                    writeSeamTs = pubSlot.getLong(lo, tsCol);
                    writeSlot.copyRowsFrom(pubSlot, lo, pubSlot.rowCount(), 0);
                    writeRow = retainedCount;
                }
            }
            // Append staging rows on top. Staging is ts-ascending, so its first row
            // carries the minimum; it seeds writeSeamTs only when no retained row did.
            final long stagingRows = stagingBuffer.rowCount();
            if (stagingRows > 0 && writeSeamTs == Numbers.LONG_NULL) {
                writeSeamTs = stagingBuffer.getLong(0, tsCol);
            }
            writeSlot.copyRowsFrom(stagingBuffer, 0, stagingRows, writeRow);
            writeRow += stagingRows;
            writeSlot.setRowCount(writeRow);
            writeSlot.setSeamTs(writeSeamTs);
            writeSlot.setLvSeqTxn(lvSeqTxn);
            writeSlot.setLeadRowCount(newLeadRowCount);
            tier.publishSwap(writeIdx);
            if (leadMode) {
                instance.setLeadRowCount(newLeadRowCount);
            }
            // Clear any prior stall streak — this cycle made progress.
            instance.setWriterStallStartUs(Numbers.LONG_NULL);
            // The published slot now reflects this cycle's disk-consistent rows
            // (retained rows dropped when stale); the stale marking is resolved.
            instance.setTierStale(false);
        } catch (Throwable t) {
            // Release the writer sentinel without flipping publishedIdx so
            // readers continue to see the previously-published slot. Flipping
            // here would expose a half-populated slot (rowCount=0 since
            // setRowCount runs only on the success path) and silently regress
            // queries that previously saw N rows to seeing 0 rows. Propagate
            // the failure so the flush-retry budget ticks.
            tier.releaseWriteWithoutPublish(writeIdx);
            throw t;
        }
        return true;
    }

    /**
     * Rebuilds an ACTIVE primary view's window state from the applied base via
     * {@link #o3HeadMissReplay} (clearWindowState + full recompute + REPLACE_RANGE +
     * watermark advance) and restages the in-mem tier. Idempotent on the written
     * prefix. Shared by the base-metadata-drift and mid-drain-failure recoveries;
     * the caller has already handled the leadReconstruction / SEEDING states.
     * Returns {@code null} on success (records a refresh success), else the replay
     * error for the caller's flush-retry accounting.
     */
    private Throwable rebuildActiveWindowStateFromAppliedBase(LiveViewInstance instance, String cause) {
        final String viewName = instance.getDefinition().getViewName();
        try {
            final TableToken baseToken = instance.getDefinition().getBaseTableToken();
            final long writerTxn = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
            instance.setLeadRowCount(0);
            o3HeadMissReplay(instance, getWindowFactory(instance), Numbers.LONG_NULL, baseToken, writerTxn, true);
            // REPLACE_RANGE rewrote disk, so the published slot is stale; rebuild it
            // from the rewritten LV table or reads keep serving pre-recompute rows.
            rebuildInMemoryTier(instance);
            instance.setLeadRowCount(0);
            instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
            instance.recordRefreshSuccess();
            LOG.info().$("live view recomputed window state from applied base [view=")
                    .$(viewName).$(", cause=").$(cause).I$();
            return null;
        } catch (Throwable t) {
            LOG.error().$("live view window-state recompute failed [view=")
                    .$(viewName)
                    .$(", cause=").$(cause)
                    .$(", error=").$(t).I$();
            return t;
        }
    }

    /**
     * Atomic O3 in-mem tier rebuild. Runs after an O3 replay has rewritten the
     * on-disk tier (REPLACE_RANGE) and applied it inline. Instead of emptying
     * the tier - which would drop seam routing until a later normal cycle
     * refills it - this repopulates the recent {@code IN MEMORY} window directly
     * from the rewritten LV table and publishes it stamped with the post-O3
     * LV-table seqTxn. A cursor opened right after the O3 cycle therefore regains
     * the tier immediately.
     * <p>
     * This is a performance restoration, not a correctness requirement. The
     * seqTxn fence ({@code slot.lvSeqTxn == diskReader.seqTxn}) already routes any
     * cursor whose slot disagrees with the disk snapshot to disk-only, so an
     * empty or stale tier after O3 is always safe; the rebuild only shortens the
     * disk-only window.
     * <p>
     * The read is bounded to the tail. {@link #stageInMemoryWindowFromDisk} walks
     * partitions, skips any whose newest row falls below the retain threshold
     * ({@code maxTs - IN_MEMORY}), and copies only the window suffix into the
     * worker-local staging buffer in ts-ascending order. A head-hit replay that
     * rewrote only the recent partition(s) therefore does not pay a full-table
     * scan here.
     * <p>
     * The acquire protocol mirrors {@link #publishToInMemoryTier}: fast-path
     * replaces the published slot in place when no reader pins it; slow-path
     * fills the non-published slot and swaps. When both slots are reader-pinned
     * the rebuild is skipped this cycle - those pinned readers hold a frozen
     * snapshot whose pre-O3 seqTxn no longer matches the rewritten disk, so the
     * fence already routes them disk-only, and the next eligible cycle
     * republishes.
     */
    private void rebuildInMemoryTier(LiveViewInstance instance) {
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            // An unsupported output column type (a non-persisted type such as
            // INTERVAL) never allocates the tier.
            return;
        }
        // The O3 replay re-sequenced the on-disk symbol ids; the failed in-order
        // drain's window intern entries are now stale. Drop them - the next drain
        // re-anchors nextNewId to the post-replay committed count, and the rebuilt
        // slot stores disk-resolved committed ids that the overlay reads via the
        // disk reader (no intern). The id -> string lists stay for any pinned
        // pre-O3 cursor.
        tier.getSymbolCache().onO3();
        // Stage the recent IN MEMORY window from the rewritten, applied LV table.
        // The reader's getSeqTxn() is the same coordinate a query's disk reader
        // reports, so stamping the slot with it makes the fence pass for an
        // immediately-following cursor (no intervening apply).
        final long lvSeqTxn;
        try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
            lvSeqTxn = lvReader.getSeqTxn();
            stageInMemoryWindowFromDisk(instance, lvReader);
        }

        int publishedIdx = tier.getPublishedIdx();
        // Fast-path: replace the published slot in place when no reader pins it.
        // A successful 0 -> -1 CAS proves there are no active read pins, and the
        // writer sentinel keeps new readers spinning until the fill + release.
        LiveViewInMemoryBuffer acquired = tier.tryAcquireWrite(publishedIdx);
        if (acquired != null) {
            try {
                fillSlotFromStaging(acquired, lvSeqTxn);
            } catch (Throwable t) {
                tier.releaseWriteWithoutPublish(publishedIdx);
                throw t;
            }
            tier.releaseWriteWithoutPublish(publishedIdx);
            // Published slot now mirrors the rewritten disk tail; any prior
            // stale-row marking is resolved.
            instance.setTierStale(false);
            return;
        }
        // Slow-path: a reader pins the published slot. Fill the non-published
        // slot and swap to it; the old slot's pinned readers keep their frozen
        // (pre-O3) rows until they release, and the fence routes them disk-only.
        int writeIdx = 1 - publishedIdx;
        LiveViewInMemoryBuffer writeSlot = tier.tryAcquireWrite(writeIdx);
        if (writeSlot == null) {
            // Both slots reader-pinned: the rebuild is skipped, so the published
            // slot keeps its pre-O3 rows (the replay re-sequenced them on disk).
            // Mark the tier stale so the next normal publish drops those retained
            // rows instead of re-stamping them with a matching seqTxn - otherwise
            // a read would serve the stale pre-O3 rows. The fence keeps reads
            // correct until then (the stale slot's seqTxn no longer matches disk).
            instance.setTierStale(true);
            LOG.info().$("live view in-mem tier rebuild skipped, both slots pinned [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return;
        }
        try {
            fillSlotFromStaging(writeSlot, lvSeqTxn);
            tier.publishSwap(writeIdx);
        } catch (Throwable t) {
            tier.releaseWriteWithoutPublish(writeIdx);
            throw t;
        }
        // Published a fresh disk-staged slot; the stale marking (if any) is resolved.
        instance.setTierStale(false);
    }

    /**
     * Recovers a turn that advanced the accumulators (windowStateDirty) but failed
     * before any durable commit - a mid-drain fault (map/staging OOM, bad segment
     * read). The retry would re-drain and double-advance them; rebuild from the
     * applied base so it starts clean. Returns {@code null} on success/re-arm, else
     * the rebuild error.
     */
    private Throwable rebuildWindowStateAfterMidDrainFailure(LiveViewInstance instance) {
        if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING) {
            // Mid-seed: re-arm the sweep resume, which rebuilds from the surviving
            // .scp (or re-sweeps from 0 behind the skip-write floor). Idempotent.
            // Deliberately KEEP the pinned base snapshot (do not freeSeedBaseReader):
            // the fault is transient (map/staging OOM, bad read), the snapshot is intact,
            // and resuming the .scp data offset against the SAME snapshot stays sound. A
            // fresh snapshot would reintroduce the positional-resume hazard this fix closes.
            instance.resetSeedResumeAttempted();
            LOG.info().$("live view mid-seed refresh failure, sweep will resume [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return null;
        }
        return rebuildActiveWindowStateFromAppliedBase(instance, "mid-drain refresh failure");
    }

    /**
     * Stages the LV table's recent {@code IN MEMORY} window suffix into the
     * worker-local {@code stagingBuffer} in ts-ascending order. Partitions whose
     * newest row sits below {@code maxTs - IN_MEMORY} are skipped entirely; the
     * first partition that crosses the threshold is binary-searched for the
     * boundary row so the copy starts exactly at the window's lower edge. The
     * staging buffer's {@code seamTs} is set to the lowest copied timestamp (or
     * {@code LONG_NULL} when the table is empty).
     */
    private void stageInMemoryWindowFromDisk(LiveViewInstance instance, TableReader lvReader) {
        stagingBuffer.reset();
        final int pc = lvReader.getPartitionCount();
        if (pc == 0) {
            // Empty LV table - the slot publishes empty (equivalent to a reset).
            stagingBuffer.setRowCount(0);
            stagingBuffer.setSeamTs(Numbers.LONG_NULL);
            return;
        }
        final int tsIdx = lvReader.getMetadata().getTimestampIndex();
        final long maxTs = lvReader.getMaxTimestamp();
        final TimestampDriver driver = ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType());
        final long inMemoryInBaseUnits = driver.fromMicros(instance.getDefinition().getInMemoryMicros());
        final long retainThreshold = maxTs - inMemoryInBaseUnits;
        final int partitionLo = PartitionBy.isPartitioned(lvReader.getPartitionedBy())
                ? Math.max(0, lvReader.getPartitionIndexByTimestamp(retainThreshold))
                : 0;

        long dstRow = 0;
        long seamTs = Numbers.LONG_NULL;
        for (int p = partitionLo; p < pc; p++) {
            final long size = lvReader.openPartition(p);
            if (size <= 0) {
                continue;
            }
            final int columnBase = lvReader.getColumnBase(p);
            final MemoryCR tsCol = lvReader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, tsIdx));
            // Skip whole partitions whose newest row is still below the window.
            if (tsCol.getLong((size - 1) << 3) < retainThreshold) {
                continue;
            }
            // Rows within a partition are ts-ascending: find the first one at or
            // above the threshold, then copy the suffix.
            final long rowLo = firstRowAtOrAbove(tsCol, size, retainThreshold);
            if (rowLo >= size) {
                continue;
            }
            if (seamTs == Numbers.LONG_NULL) {
                seamTs = tsCol.getLong(rowLo << 3);
            }
            copyReaderRowsToStaging(lvReader, columnBase, rowLo, size, dstRow);
            dstRow += size - rowLo;
        }
        stagingBuffer.setRowCount(dstRow);
        stagingBuffer.setSeamTs(seamTs);
    }

    /**
     * Copies the LV-table row range {@code [rowLo, rowHi)} of one partition into
     * {@code stagingBuffer} starting at {@code dstRow}, reading directly from the reader's
     * mapped column memory. Walks column-major, mirroring
     * {@link LiveViewInMemoryBuffer#copyRowsFrom}: a fixed-width / SYMBOL column moves its
     * whole row range with a single {@code Vect.memcpy} - a native column file stores its
     * values at the same {@code row * size} offsets the staging buffer writes them to, for
     * every fixed-width type the tier stores - which hoists the per-cell type switch and the
     * per-cell column lookup out of the row loop. A variable-length column (STRING / BINARY /
     * VARCHAR / ARRAY) still decodes each row from the reader's (data, aux) column pair and
     * re-appends it, because its aux offsets are relative to the staging buffer's own payload
     * cursor. Those appends make the buffer fill in dense row order, which the caller
     * satisfies by walking the window suffix ascending. The decoded var-length value is
     * copied into {@code stagingBuffer} before the next read reuses the reader's flyweight,
     * so reusing one {@link #stagingArrayView} is safe.
     */
    private void copyReaderRowsToStaging(TableReader reader, int columnBase, long rowLo, long rowHi, long dstRow) {
        final long count = rowHi - rowLo;
        for (int c = 0, n = stagingColumnTypes.size(); c < n; c++) {
            final int columnType = stagingColumnTypes.getQuick(c);
            final int primaryIndex = TableReader.getPrimaryColumnIndex(columnBase, c);
            final MemoryCR data = reader.getColumn(primaryIndex);
            if (!ColumnType.isVarSize(columnType)) {
                // Fixed-width / SYMBOL column: one memcpy over the contiguous byte range. The
                // staging buffer owns the stride, so the source offset cannot drift from it.
                // ensureStagingAndTier admits only tier-supported types, and every fixed-width
                // one of those reads back byte-identically from this layout.
                stagingBuffer.copyFixedColumnFrom(c, data.addressOf(0), rowLo, count, dstRow);
                continue;
            }
            final MemoryCR aux = reader.getColumn(primaryIndex + 1);
            long dstRowInBuffer = dstRow;
            for (long r = rowLo; r < rowHi; r++, dstRowInBuffer++) {
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.STRING:
                        // STRING .d/.i layout: aux holds the per-row 8-byte start offset into
                        // the data payload. getStrA returns null for a null marker.
                        stagingBuffer.appendStr(c, dstRowInBuffer, data.getStrA(aux.getLong(r << 3)));
                        break;
                    case ColumnType.BINARY:
                        // BINARY .d/.i layout: aux holds the per-row 8-byte start offset.
                        // getBin returns null for a null marker; len == 0 is a real empty.
                        stagingBuffer.appendBin(c, dstRowInBuffer, data.getBin(aux.getLong(r << 3)));
                        break;
                    case ColumnType.VARCHAR:
                        // VARCHAR (aux header + split data) decoded by VarcharTypeDriver;
                        // getSplitValue returns null for a null marker and carries the ascii
                        // flag.
                        stagingBuffer.appendVarchar(c, dstRowInBuffer, VarcharTypeDriver.getSplitValue(aux, data, r, 1));
                        break;
                    case ColumnType.ARRAY:
                        // ARRAY (aux header + shape/payload data) bound by BorrowedArray over
                        // the reader's column pair, mirroring PageFrameMemoryRecord; a
                        // zero-size aux entry decodes to a null ArrayView.
                        stagingArrayView.of(
                                columnType,
                                aux.addressOf(0),
                                aux.addressOf(0) + aux.size(),
                                data.addressOf(0),
                                data.addressOf(0) + data.size(),
                                r
                        );
                        stagingBuffer.appendArray(c, dstRowInBuffer, stagingArrayView);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "live view in-memory tier does not support column type: "
                                        + ColumnType.nameOf(columnType));
                }
            }
        }
    }

    /**
     * Binary-searches a ts-ascending timestamp column for the first row index in
     * {@code [0, size)} whose value is at or above {@code threshold}, returning
     * {@code size} when every row is below it.
     */
    private static long firstRowAtOrAbove(MemoryCR tsCol, long size, long threshold) {
        long lo = 0;
        long hi = size;
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (tsCol.getLong(mid << 3) < threshold) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * True when {@code e} is a file-does-not-exist failure raised while {@code drainBaseWal}
     * reads the base WAL segments the lead re-derive wants. The base WAL is absent whenever the
     * applied base TABLE outlived its WAL segments: an enterprise backup/restore captures only the
     * TABLE, and a role migration onto a partially-uploaded object store leaves a lagging live view
     * needing base commits whose WAL a replica purged (a replica does not hold base WAL for its live
     * view -- it follows the replicated on-disk tier, not the base WAL) or whose upload was cut
     * mid-segment. A missing segment can surface as any of its files -- the WAL event file
     * ({@link WalTxnDetails#openWalEFile} "cannot read WAL event file"), a symbol map
     * ("SymbolMap does not exist"), or a column file -- so match on the errno (file-does-not-exist)
     * rather than a single message. Gating on {@link CairoException#isFileCannotRead()} keeps a
     * genuinely corrupt WAL file (read with errno 0) on the invalidating path rather than silently
     * rebuilding over it.
     */
    private static boolean isBaseWalSegmentFileMissing(CairoException e) {
        return e.isFileCannotRead();
    }

    /**
     * Replaces {@code slot}'s contents with the current {@code stagingBuffer}
     * rows and stamps the slot. The slot is reset first (full replace, not an
     * append) - the rebuild slot reflects the disk tail exactly, with no carry
     * over of pre-O3 rows. Runs under the writer sentinel, so no reader observes
     * the intermediate state.
     */
    private void fillSlotFromStaging(LiveViewInMemoryBuffer slot, long lvSeqTxn) {
        slot.reset();
        final long rows = stagingBuffer.rowCount();
        // Rollback-safe: copyRowsFrom can throw mid-copy (native OOM growing a var-size column). On
        // rebuildInMemoryTier's fast path the slot being filled IS the published slot, and the
        // caller republishes it in its catch - a plain copyRowsFrom would leave rowCount 0 but a
        // var-size cursor stranded at k*width, misaligning the aux vector so every later
        // STRING/BINARY/VARCHAR/ARRAY read dereferences a garbage offset. The rollback rewinds every
        // var-size cursor, so a failed fill republishes a clean, empty slot (lvSeqTxn stays
        // LONG_NULL from reset(), so the fence routes reads disk-only).
        slot.copyRowsFromWithRollback(stagingBuffer, 0, rows, 0);
        slot.setRowCount(rows);
        slot.setSeamTs(stagingBuffer.seamTs());
        slot.setLvSeqTxn(lvSeqTxn);
        // Rebuilt straight from the rewritten disk, so every row is on disk: the
        // slot is a pure subset of disk with no un-flushed lead.
        slot.setLeadRowCount(0);
    }

    /**
     * Seed-checkpoint write hook. Writes a {@code <dataOffset>.scp}
     * capturing the sweep's resume position (a SEED_CURSOR block holding
     * the data-cursor row offset + lvRowsTotal) plus the same WINDOW_ANCHOR /
     * FUNCTION_SNAPSHOT state blocks the steady head writes, then unlinks the
     * prior {@code .scp} and stamps {@code headSeedCpKey} on the instance.
     * <p>
     * Cadence-gated by the same {@code cairo.live.view.checkpoint.rows} /
     * {@code .max.duration} triggers as the steady head, plus a
     * first-checkpoint trigger so a restart early in the sweep resumes rather
     * than re-sweeping. The intervening per-turn yields rely on in-memory
     * window state; the {@code .scp} only has to be recent enough that a
     * restart's skip-write re-feed (bounded by the cadence) is cheap.
     * <p>
     * No-op when the LV is not snapshot-capable: such a view cannot persist
     * window state, so a crash mid-sweep re-sweeps from the beginning (the
     * wipe path in {@link #runSeedSweep}).
     * <p>
     * A failure here does not invalidate the view ({@code .scp} is derived
     * state). The prior {@code .scp}, if any, stays addressable; we log
     * critical, drop the half-open writer, and continue.
     */
    private void maybeWriteSeedCheckpoint(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long dataOffset,
            long batchMaxTs,
            long sweepSeqTxn
    ) {
        if (!instance.isSnapshotCapabilityComputed()) {
            instance.setSnapshotCapability(computeSnapshotCapability(instance, windowFactory));
        }
        if (!instance.isSnapshotCapability()) {
            return;
        }

        // Cadence keys off the data-offset delta since the prior .scp (its key
        // is the data offset at that write). firstScp forces a write so a crash
        // early in the sweep resumes rather than re-sweeping from scratch.
        final long rowsCadence = engine.getConfiguration().getLiveViewCheckpointRows();
        final long durationCadence = engine.getConfiguration().getLiveViewCheckpointMaxDurationMicros();
        final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        final long lastWrittenUs = instance.getLastCheckpointWrittenUs();
        final long priorKey = instance.getHeadSeedCpKey();
        final boolean firstScp = priorKey == Numbers.LONG_NULL;
        final boolean rowTrigger = !firstScp && (dataOffset - priorKey) >= rowsCadence;
        final boolean durationTrigger = !firstScp
                && lastWrittenUs != Numbers.LONG_NULL
                && (nowUs - lastWrittenUs) >= durationCadence;
        if (!(firstScp || rowTrigger || durationTrigger)) {
            return;
        }

        try {
            if (checkpointWriter == null) {
                checkpointWriter = new LiveViewCheckpointWriter(engine.getConfiguration());
            }
            path.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken());
            checkpointWriter.of(path.$(), dataOffset, true);

            checkpointManifest.clear();
            checkpointManifest.setLvSeqTxn(dataOffset);
            checkpointManifest.setBaseSeqTxn(sweepSeqTxn);
            checkpointManifest.setMaxTimestamp(batchMaxTs);
            checkpointManifest.setLvRowPosition(instance.getLvRowsTotal());
            checkpointManifest.setKind(LiveViewCheckpointManifest.KIND_SEED);
            final LiveViewWindow anchorWindow = instance.getAnchorWindow();
            if (anchorWindow != null) {
                checkpointManifest.addWindowName(anchorWindow.getWindowName());
            }
            checkpointWriter.writeManifestBlock(checkpointManifest);

            final MemoryA cursorSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_SEED_CURSOR);
            cursorSink.putLong(dataOffset);
            cursorSink.putLong(instance.getLvRowsTotal());
            checkpointWriter.endBlock();

            if (anchorWindow != null) {
                MemoryA anchorSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_WINDOW_ANCHOR);
                anchorWindow.snapshot(anchorSink);
                checkpointWriter.endBlock();
            }

            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
            final String windowName = anchorWindow != null ? anchorWindow.getWindowName() : "";
            // Test-only: omit the last N function-snapshot blocks to forge a
            // CRC-valid-but-short .scp. 0 in production, so the limit is MAX_VALUE and
            // every snapshot-capable function is written.
            int fnBlockWriteLimit = Integer.MAX_VALUE;
            final int fnBlocksToOmit = checkpointTrailingFunctionSnapshotBlocksToOmit;
            if (fnBlocksToOmit > 0) {
                int capable = 0;
                for (int i = 0, m = functions.size(); i < m; i++) {
                    if (functions.getQuick(i).supportsCheckpointState()) {
                        capable++;
                    }
                }
                fnBlockWriteLimit = Math.max(0, capable - fnBlocksToOmit);
            }
            int fnBlocksWritten = 0;
            for (int i = 0, n = functions.size(); i < n; i++) {
                final WindowFunction f = functions.getQuick(i);
                if (!f.supportsCheckpointState() || fnBlocksWritten >= fnBlockWriteLimit) {
                    continue;
                }
                final MemoryA fnSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                fnSink.putStr(windowName);
                fnSink.putStr(snapshotFactoryName(f));
                fnSink.putInt(f.checkpointStateFormatVersion());
                LiveViewFunctionSnapshot.write(fnSink, f);
                checkpointWriter.endBlock();
                fnBlocksWritten++;
            }

            checkpointWriter.commit(firstScp ? Numbers.LONG_NULL : priorKey);
            instance.recordSeedCheckpointWritten(dataOffset, nowUs);
        } catch (Throwable t) {
            LOG.critical().$("could not write live view seed checkpoint [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", dataOffset=").$(dataOffset)
                    .$(", error=").$(t).I$();
            checkpointWriter = Misc.free(checkpointWriter);
        }
    }

    /**
     * Rewrites {@code _lv.s} from the in-memory state mirror. Called after each
     * cycle's advance so restart sees the latest {@code lastProcessedSeqTxn}.
     * <p>
     * Unlike {@code CairoEngine.advanceLiveViewConsumedSeqTxn}, this call cannot
     * persist-then-publish: the live view's WAL block has already been committed
     * upstream, so the in-memory advance is necessary to prevent the next cycle
     * from re-processing the same base seqTxns and writing duplicate output rows.
     * The order is therefore: in-memory advance, then persist. On persist failure
     * the exception propagates to the {@code refreshInstance} top-level catch,
     * which logs at LOG.critical level. Subsequent cycles re-attempt the persist
     * the next time the in-memory state advances.
     */
    private void persistState(LiveViewInstance instance) {
        TableToken token = instance.getLiveViewToken();
        // Synchronize on the instance: ApplyWal2TableJob also rewrites _lv.s when it
        // applies an LV-data block (advanceLiveViewConsumedSeqTxn), and the two
        // workers can otherwise race on the same file.
        synchronized (instance) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
            blockFileWriter.of(path.$());
            LiveViewState.append(instance.getStateReader(), blockFileWriter);
        }
    }

    private boolean processNotifications() {
        if (!stateStore.isRefreshEnabled()) {
            // Lead-reconstruction (read-only replica, freshness parity): run only the registry
            // scan, which drives the compute-lead-only path in refreshInstance. Skip the
            // notification-queue drain -- notifications are a primary-side, in-process signal (the
            // sequencer fans them out at commit time), never populated on a replica; the replica's
            // lead follows the applied on-disk watermark via the scan, not a queue.
            if (stateStore.isLeadReconstructionEnabled()) {
                return scanForLaggingViews();
            }
            // Quiesced store (live views disabled, or a replica before a promote without lead
            // reconstruction): skip the whole pass, including the registry fallback scan, so refresh
            // workers never touch a live view. A promote swaps in a real store (see
            // ForwardingLiveViewStateStore) and this gate reopens.
            return false;
        }
        boolean didWork = false;
        // Bounded drain: leave any leftover / re-enqueued tasks for the next scheduler turn so
        // one busy base table cannot starve the pool. See MAX_REFRESH_TASKS_PER_RUN.
        int drained = 0;
        while (drained < MAX_REFRESH_TASKS_PER_RUN && stateStore.tryDequeueRefreshTask(refreshTask)) {
            refreshViewsForBaseTable(refreshTask.baseTableToken, refreshTask.seqTxn);
            stateStore.notifyBaseRefreshed(refreshTask, refreshTask.seqTxn);
            didWork = true;
            drained++;
        }
        if (!didWork) {
            // Notification queue empty: scan all registered views and refresh any whose
            // base sequencer head is past their last-processed seqTxn. Catches missed /
            // coalesced commit notifications (e.g., a CREATE that races a writer or a
            // notification dropped while the worker was busy on another task) and serves
            // as the periodic FLUSH-EVERY tick this build doesn't yet have a dedicated
            // timer for.
            didWork = scanForLaggingViews();
        }
        return didWork;
    }

    /**
     * Iterates the live-view registry and refreshes any view whose base sequencer head
     * is ahead of its last-processed seqTxn. Returns {@code true} if any view advanced.
     */
    private boolean scanForLaggingViews() {
        // Lead-reconstruction mode (read-only replica): the compute-lead-only path never advances
        // lastProcessedSeqTxn (that tracks the flushed disk tier, owned by the apply job), so the
        // "caught up" mark is refreshedUpToSeqTxn -- how far the in-RAM lead has been computed.
        // Using lastProcessedSeqTxn here would keep the scan reporting work while the base leads
        // disk, spinning the worker; refreshedUpToSeqTxn goes quiet once the lead reaches the base
        // head and reopens when new base commits land.
        final boolean leadOnly = isLeadReconstruction();
        LiveViewRegistry registry = engine.getLiveViewRegistry();
        // Registry sharding: each worker copies (and scans) ONLY the views it owns (by table id),
        // so the idle fallback scan copies and processes each view once across the pool (O(views))
        // rather than copying every view on every worker and discarding the non-owned ones
        // afterwards (O(workers x views) copies). The notification-driven path is unaffected (any
        // worker handles any base-table task); this only splits the periodic catch-up scan. A
        // single-worker pool owns everything, so behavior there is unchanged.
        registry.getShardedViews(viewInstanceSink, workerId, workerCount);
        boolean didWork = false;
        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            // A definition-less stub (torn / too-new _lv or _lv.s, registered by
            // buildViewGraphs so DROP LIVE VIEW can still remove it) lives in the
            // registry that getViews iterates, so it reaches this scan even though it
            // must never refresh. Skip it before the getDefinition() deref below, which
            // would NPE on its null definition (getDefinition() == null, and a stub is
            // neither dropped nor invalid). The catalogue reader guards it the same way.
            if (instance.isStub() || instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            // CREATE deferred-name transient: createLiveView registers the refresh instance before it
            // commits the LV table name, so a view firing this scan at CREATE would busy-loop
            // refreshInstance (didWork stays true) on getWalWriter's "table does not exist", flooding the
            // log ring and starving the name-commit thread on a few-core box. Skip until the name
            // resolves; the pool's idle sleep throttles the retry, and gating on resolution (not a wall
            // clock) is frozen-test-clock safe. getTableTokenIfExists returns null for a locked/absent
            // name -- exactly the transient verifyTableToken rejects.
            if (engine.getTableTokenIfExists(instance.getLiveViewToken().getTableName()) == null) {
                continue;
            }
            TableToken baseToken = instance.getDefinition().getBaseTableToken();
            if (baseToken == null) {
                // Unresolved base token: on a read-only replica the LV's files can download and
                // register BEFORE its base table's (object-store ordering), so the registration-time
                // lookup froze null into the definition. Re-resolve by name each tick; until the
                // base lands the view serves disk-only, and once it registers the heal below lets
                // this same tick proceed to reconstruct the lead.
                baseToken = engine.getTableTokenIfExists(instance.getDefinition().getBaseTableName());
                if (baseToken == null) {
                    continue;
                }
                instance.getDefinition().resolveBaseTableToken(baseToken);
                LOG.info().$("resolved live view base table token after registration [view=")
                        .$(instance.getLiveViewToken())
                        .$(", base=").$(baseToken)
                        .I$();
            }
            // Replica anti-spin: skip a view whose lead loop armed a publish-stall back-off that has not
            // elapsed, so the worker idles instead of re-draining into the same stall every tick. This is
            // a side-effect-free pre-check only -- the gate that decides is the authoritative one
            // refreshInstance runs under the refresh latch.
            if (leadOnly && deferReplicaLeadWork(instance, false)) {
                continue;
            }
            // Promote-hydrate consistency guard (primary only). A role migration onto a
            // partially-uploaded object store can leave a live view's durable watermark ahead of the
            // base seqTxn that actually replicated: the ex-primary flushed + uploaded derived rows for
            // base commits whose own base-table WAL upload lagged and was cut, and a replica applies that
            // LIVE_VIEW_DATA -- advancing _lv.s via applyLiveViewData -- without ever holding the base
            // beyond what replicated. The base can never reach that seqTxn (it is not in the downloaded
            // WAL), so the view would otherwise sit active forever serving derived rows for base commits
            // the promoted primary no longer holds. Invalidate it durably, mirroring the mat-view "view
            // is ahead of base table and cannot be synchronized" guard in CairoEngine.loadMatViewIntoStore.
            // Read-only replicas never invalidate (they defer forever; see onReplicaLeadRefreshFailure),
            // and this is a strict no-op on a healthy primary, where a view never outruns the base it
            // derives from.
            if (!leadOnly) {
                final long baseSeqLastTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                if (instance.getLastProcessedSeqTxn() > baseSeqLastTxn) {
                    LOG.error().$("live view is ahead of base table and cannot be synchronized [view=")
                            .$(instance.getLiveViewToken())
                            .$(", lastProcessedSeqTxn=").$(instance.getLastProcessedSeqTxn())
                            .$(", baseTableTxn=").$(baseSeqLastTxn)
                            .I$();
                    engine.invalidateLiveView(instance, "live view is ahead of base table and cannot be synchronized");
                    didWork = true;
                    continue;
                }
            }
            // Primary-only: re-drive an LV WAL block whose inline apply never landed. On a
            // primary the refresh worker owns the LV's TableWriter, so ApplyWal2TableJob.doRun
            // drops every live-view notification and flushLead's inline applyWalDirect is the
            // view's ONLY applier. When that apply silently no-ops (the LV writer was busy, or
            // its memory-pressure control backed off) or fails and suspends the table, the
            // republish it falls back on goes nowhere: the committed rows stay off disk, and
            // the flush already re-stamped the slot with a zero lead, so neither tier serves
            // them - the view under-reports until a later base commit drives another flush. On
            // a quiescent base that never comes. Retry the apply here instead; it is idempotent
            // (the block is committed, so it lands each row exactly once) and it is what lets
            // ALTER LIVE VIEW ... RESUME WAL on a suspended live view take effect without waiting
            // for the base to move.
            if (!leadOnly && hasPendingLiveViewApply(instance)) {
                didWork |= retryPendingLiveViewApply(instance);
            }
            long head = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
            // Timeline recovery runs inside refreshInstance on the first cycle
            // of every ACTIVE primary, even when there are no new base commits.
            // The recovery itself cheaply recognizes an empty identity view;
            // scheduling it once avoids reopening the LV table on every fallback
            // scan merely to decide that no recovery is needed.
            final boolean needsRestore = !instance.isCheckpointRestoreAttempted()
                    && !leadOnly
                    && instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE;
            // A SEEDING view needs a refresh tick to drive its sweep even when
            // no new base commits have arrived since CREATE - the sweep
            // covers existing history, not future commits.
            final boolean needsSeeding = instance.getStateReader().getSeedState()
                    == LiveViewState.SEED_STATE_SEEDING;
            final long processedTo = leadOnly ? instance.getRefreshedUpToSeqTxn() : instance.getLastProcessedSeqTxn();
            // Replica-only: the global apply job advances the on-disk tier (and its seqTxn)
            // independently of new base commits, so a lead built before a replicated flush landed must
            // be reconciled even when the base head has not moved. isLeadSlotStale fires when the
            // on-disk seqTxn has advanced past the slot's stamp -- covering both the fully-subsumed
            // flush (whole lead now on disk) and the partial-overlap flush (a prefix on disk, a
            // remainder still lead) -- so reconcileLeadWithDisk re-stamps the slot and trims the
            // now-durable prefix instead of leaving reads stuck disk-only.
            //
            // The slot stamp alone is not enough: the drain reads the on-disk state at the start of a
            // tick, stages the lead, then publishes it. If the apply job advances the disk past the
            // loop's frontier BETWEEN the read and the publish, publishToInMemoryTier stamps the slot
            // with the already-advanced seqTxn, so isLeadSlotStale reads false even though the staged
            // rows are now fully on disk -- and with the base head not moving either, no trigger fires
            // and the subsumed lead lingers, over-counting size()/count() by the durable rows. Fire the
            // reconcile whenever a non-empty lead sits at or below the applied watermark (exact-boundary
            // or Case B), which reconcileLeadWithDisk resolves by dropping it; the partial-overlap case
            // (a genuine remainder above disk, applied < refreshedUpTo) is left to isLeadSlotStale.
            final boolean leadSubsumedByDisk = leadOnly
                    && instance.getLeadRowCount() > 0
                    && instance.getAppliedWatermark() >= instance.getRefreshedUpToSeqTxn();
            final boolean needsLeadReconcile = leadOnly && (isLeadSlotStale(instance) || leadSubsumedByDisk);
            if (head > processedTo || needsLeadReconcile || needsRestore || needsSeeding) {
                // Only count a turn that actually refreshed. refreshInstance returns false
                // when it lost the refresh latch to another worker (or backed off), so the
                // losing workers fall through to the idle backoff instead of rescanning the
                // whole registry at full tilt while one worker holds the latch.
                didWork |= refreshInstance(instance, head);
            }
        }
        return didWork;
    }

    /**
     * True when the live view's own WAL carries transactions its inline apply never landed
     * ({@code seqTxn > writerTxn}) and a retry can make progress. Excludes the states a retry
     * cannot move: a suspended table (only an operator RESUME clears it), a memory-pressure
     * back-off ({@code applyWal} returns at its own readiness gate without advancing), and a
     * view whose tracker is not initialised yet. Excludes a view with an un-flushed lead too -
     * its next FLUSH EVERY tick calls {@code flushLead}, whose {@code applyWalDirect} re-drives
     * the outstanding block anyway, so only a view the scan would otherwise leave idle needs
     * the retry.
     */
    private boolean hasPendingLiveViewApply(LiveViewInstance instance) {
        if (instance.getLeadRowCount() > 0) {
            return false;
        }
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(instance.getLiveViewToken());
        return tracker.isInitialised()
                && !tracker.isSuspended()
                && tracker.getMemPressureControl().isReadyToProcess()
                && tracker.getSeqTxn() > tracker.getWriterTxn();
    }

    /**
     * Re-drives the live view's own WAL apply for a block {@code flushLead} committed but could
     * not apply inline, then repairs the in-mem tier's stamp so reads regain seam routing
     * instead of staying disk-only. Runs under the refresh latch: the apply advances the LV's
     * on-disk tier, which neither a concurrent refresh cycle nor the checkpoint agent's freeze
     * may race. Returns {@code true} only when the applied seqTxn actually advanced, so a retry
     * that no-ops again reports no work and lets the worker idle rather than spin.
     */
    private boolean retryPendingLiveViewApply(LiveViewInstance instance) {
        if (!instance.tryLockForRefresh()) {
            return false;
        }
        try {
            // Re-check under the latch: a concurrent flush may have applied the block already,
            // and a stub / dropped / invalid / frozen view must never advance its disk tier.
            if (instance.isStub()
                    || instance.isDropped()
                    || instance.isInvalid()
                    || instance.isFreezeInProgress()
                    || !hasPendingLiveViewApply(instance)) {
                return false;
            }
            final TableToken token = instance.getLiveViewToken();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long appliedBefore = tracker.getWriterTxn();
            LOG.info().$("live view has committed but unapplied WAL, retrying apply [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", appliedSeqTxn=").$(appliedBefore)
                    .$(", committedSeqTxn=").$(tracker.getSeqTxn()).I$();
            applyJob.applyWalDirect(token, Job.RUNNING_STATUS);
            final long appliedAfter = tracker.getWriterTxn();
            if (appliedAfter <= appliedBefore) {
                // The apply no-opped again (the LV writer is busy) or failed and suspended the
                // table. Reads stay correct - the fence routes them to disk, which simply lacks
                // the committed rows - and the next tick retries a transient block. A suspend
                // waits for the operator; hasPendingLiveViewApply skips it meanwhile.
                return false;
            }
            if (instance.isTierStale()) {
                // The slot is an incomplete subset of the now-current disk (an emergency flush
                // left it un-published). Rebuild it from disk rather than re-stamping content
                // that never received the flushed rows.
                rebuildInMemoryTier(instance);
            } else {
                // The flushed rows reached disk and are still in the slot, which is a complete
                // subset of it again. Re-stamp so the seam routes reads back through RAM.
                restampSlotAfterFlush(instance, appliedAfter);
            }
            return true;
        } catch (Throwable t) {
            // A tier rebuild / re-stamp failure is not fatal and must not invalidate the view:
            // the apply landed, and the seqTxn fence keeps reads correct (disk-only) until the
            // next refresh rebuilds the slot.
            LOG.error().$("live view apply retry failed [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
            return false;
        } finally {
            instance.unlockAfterRefresh();
            instance.tryCloseIfDropped();
            // Mirror the main refresh finally: if the view was invalidated concurrently while
            // this helper held the refresh latch, the invalidator's own free lost the CAS and
            // relies on the latch holder to free the runtime state (factory, maps, tier,
            // tracker) once the latch is released. Without this, an invalid view strands that
            // state until DROP or shutdown.
            instance.tryFreeRuntimeStateIfInvalid();
        }
    }

    /**
     * Repairs a durable-floor lag on the first refresh cycle after a restart. A flush
     * commits the LV WAL block, inline-applies it (rows become durable in the LV
     * table's own {@code _txn}), then persists {@code _lv.s} last; a crash between the
     * apply and that persist leaves {@code _lv.s} behind the LV table's applied state.
     * On restart the stale {@code lastProcessedSeqTxn} would make the first drain
     * re-derive and re-append the already-materialised base range - a forward-append
     * commit carries no dedup to collapse the duplicate, so the rows would double
     * permanently.
     * <p>
     * Recovery restores the floor from disk truth: first apply any LV WAL block that
     * committed but never applied (a crash in the narrower commit-to-apply window), so
     * the LV table reflects every committed block, then clamp
     * {@code lastProcessedSeqTxn} / {@code appliedWatermark} / {@code lvConsumedSeqTxn}
     * up to the last applied block's in-band {@code maxBaseSeqTxn}. ACTIVE views only:
     * a SEEDING view resumes through its own {@code .scp} sweep, which owns its
     * distinct floor. Idempotent on a healthy restart - {@code applyWalDirect} finds
     * nothing pending and the clamp is a no-op because {@code _lv.s} already matches
     * disk. When the WAL-e cannot be read the recovery no-ops, leaving the prior
     * (worst-case duplicating, never lossy) behaviour.
     */
    private void reconcileAppliedFloorAfterRestart(LiveViewInstance instance) {
        if (instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE) {
            return;
        }
        final TableToken token = instance.getLiveViewToken();
        try {
            applyJob.applyWalDirect(token, Job.RUNNING_STATUS);
        } catch (Throwable t) {
            LOG.error().$("could not apply pending live view WAL on restart [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
            return;
        }
        final long appliedMaxBaseSeqTxn = engine.readLiveViewAppliedMaxBaseSeqTxn(token);
        if (appliedMaxBaseSeqTxn >= 0
                && appliedMaxBaseSeqTxn != instance.getStateReader().getLastProcessedSeqTxn()) {
            try {
                // waitForUnfrozen=false: this runs on the refresh worker while it holds the
                // refresh latch, so the startCheckpoint handshake already serialises the
                // rewrite against the agent's copy. Parking here would deadlock the worker
                // against a concurrent checkpoint freeze.
                engine.applyLiveViewData(token, appliedMaxBaseSeqTxn, blockFileWriter, path, false);
                LOG.info().$("reconciled live view floor to applied state on restart [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", maxBaseSeqTxn=").$(appliedMaxBaseSeqTxn).I$();
            } catch (CairoException e) {
                LOG.critical().$("could not reconcile live view floor on restart [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", maxBaseSeqTxn=").$(appliedMaxBaseSeqTxn)
                        .$(", error=").$safe(e.getFlyweightMessage()).I$();
            }
        }
    }

    /**
     * Recovery for a refresh cycle that failed with
     * {@link TableReferenceOutOfDateException}: the base table's metadata version
     * drifted from the cached compiled factory. Frees the compiled artifacts so
     * the next factory use recompiles against current metadata, then rebuilds the
     * window state that was lost with the old factory's function instances:
     * <ul>
     *     <li>SEEDING: re-arms the sweep's single-shot resume setup; the next
     *     turn restores window state and the data offset from the surviving
     *     {@code .scp} against the recompiled factory (same SQL, so the snapshot
     *     blocks stay shape-compatible), or re-sweeps from offset 0 behind the
     *     skip-write floor. Both are idempotent on the already-written prefix.</li>
     *     <li>ACTIVE on the primary: full head-miss replay over the applied base -
     *     unconditionally correct and idempotent (mirrors the dedup restart path
     *     and the checkpoint-less restore fallback). The replay resets window
     *     state, recomputes every retained row through the recompiled factory,
     *     rewrites the on-disk tier with a single REPLACE_RANGE, advances the
     *     watermarks, and writes a fresh head {@code .cp}. Any un-flushed lead is
     *     dropped first (its rows were computed by the old factory's state) and
     *     {@code refreshedUpToSeqTxn} is pinned back to {@code lastProcessedSeqTxn}
     *     so no phantom lead survives.</li>
     *     <li>Read-only replica lead reconstruction: cannot rewrite the tier, and
     *     has no head {@code .cp} to restore from (checkpoints do not replicate).
     *     Mirrors {@code onLeadO3Detected}'s cold-start reset: clearing
     *     {@code latestSeenTs} routes the next {@code reconcileLeadWithDisk} tick
     *     through its unseeded cold-start branch, which arms the catch-up seam at
     *     the on-disk max ts and re-derives the whole applied history through the
     *     recompiled factory without staging the durable band.</li>
     * </ul>
     * Returns {@code null} when recovery completed (or was re-armed for the next
     * tick); otherwise the error the recovery replay failed with, which the caller
     * feeds into the standard flush-retry accounting.
     */
    private Throwable recoverFromBaseMetadataDrift(LiveViewInstance instance, boolean leadReconstruction) {
        final String viewName = instance.getDefinition().getViewName();
        instance.prepareForBaseSchemaRecompile();
        if (leadReconstruction) {
            instance.forceSetLatestSeenTs(Numbers.LONG_NULL);
            instance.setLeadRowCount(0);
            instance.setTierStale(true);
            // Rewind so the promised next tick actually happens: if this cycle was triggered by a
            // slot-stale reconcile (head == refreshedUpTo), leaving refreshedUpTo in place closes
            // the scanForLaggingViews gate (every other trigger needs the leadRowCount just zeroed)
            // and the re-derive never runs. Rewinding to lastProcessed reopens it exactly when an
            // un-flushed lead can exist; the cold-start drain cannot O3 (latestSeenTs is unset).
            instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
            LOG.info().$("live view base table metadata changed, lead re-derives on next tick [view=")
                    .$(viewName).I$();
            return null;
        }
        if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING) {
            // The recompiled factory expects the base's NEW metadata; the pinned base
            // snapshot is at the OLD metadata version. Drop it so the next sweep turn
            // re-pins a fresh snapshot consistent with the recompiled factory. A
            // metadata-only change preserves physical row order, so the .scp data
            // offset still resumes correctly against the re-pinned snapshot.
            instance.freeSeedBaseReader();
            instance.resetSeedResumeAttempted();
            LOG.info().$("live view base table metadata changed mid-seed, sweep will resume recompiled [view=")
                    .$(viewName).I$();
            return null;
        }
        return rebuildActiveWindowStateFromAppliedBase(instance, "base table metadata change");
    }

    /**
     * Rebuilds the view from the applied base TABLE after a base WAL segment it needs turned out to
     * be gone for good - an enterprise backup captures the applied base, not its WAL, so a restored
     * view that still owed itself commits cannot read them anywhere else. {@link #o3HeadMissReplay}
     * re-seeds the window from identity (so {@code row_number()} stays 1..N), rewrites the tier with
     * one REPLACE_RANGE and advances the watermarks: self-contained on the base table, needing
     * neither the WAL nor a checkpoint. The primary-side analog of the replica's applied-base lead
     * reconstruction. Runs under the refresh latch, like every other refresh path.
     * <p>
     * Only carries the view as far as the base has APPLIED, because that is all the base table
     * holds; a base still applying WAL of its own keeps its remaining commits for the next drain,
     * which reads them from WAL if it can and comes back here if it cannot.
     *
     * @return true when the view recovered and must not be invalidated
     */
    private boolean rederiveFromAppliedBaseAfterWalLoss(LiveViewInstance instance, CairoException cause) {
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        if (baseToken == null) {
            return false;
        }
        final long baseAppliedSeqTxn = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
        if (baseAppliedSeqTxn <= instance.getLastProcessedSeqTxn()) {
            return false;
        }
        try {
            // Drop any un-flushed lead the failed cycles left published (a lead-eligible view
            // between flushes carries a non-zero leadRowCount and a slot holding those rows). The
            // replay recomputes the whole view and rewrites the on-disk tier via REPLACE_RANGE, so
            // those RAM rows are about to become durable on disk; a surviving leadRowCount would
            // make the next publishToInMemoryTier / flushLead re-append them as duplicates. Mirror
            // rebuildActiveWindowStateFromAppliedBase, the sibling recovery: reset before the replay
            // (defensive - o3HeadMissReplay does not read the lead) and rebuild the tier + resync the
            // counter after it, so the published slot becomes a clean disk subset stamped at the new
            // seqTxn with leadRowCount 0.
            instance.setLeadRowCount(0);
            o3HeadMissReplay(instance, getWindowFactory(instance), Numbers.LONG_NULL, baseToken, baseAppliedSeqTxn, true);
            // REPLACE_RANGE rewrote disk, so the published slot is stale; rebuild it from the
            // rewritten LV table (read via the LV token - the missing base WAL is irrelevant here)
            // or reads keep serving the pre-replay rows and the next flush re-appends the stale lead.
            rebuildInMemoryTier(instance);
            instance.setLeadRowCount(0);
            // The replay flushed the whole tier to disk and advanced lastProcessed / the applied
            // watermark, so no un-flushed lead remains. Keep refreshedUpTo == lastProcessed so a
            // later ALTER cannot see a phantom lead.
            instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
            instance.recordRefreshSuccess();
            LOG.info().$("live view re-derived from the applied base after base WAL loss [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", head=").$(baseAppliedSeqTxn)
                    .$(", reason=").$safe(cause.getFlyweightMessage()).I$();
            return true;
        } catch (Throwable e) {
            LOG.error().$("live view could not re-derive from the applied base after base WAL loss [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(e).I$();
            return false;
        }
    }

    private boolean refreshInstance(LiveViewInstance instance, long seqTxn) {
        // Tracks whether this call did real refresh work (seed sweep, drain, flush,
        // reconcile). Returned to the fallback scan so a call that did nothing - most
        // importantly one that lost the refresh latch to another worker - does NOT count
        // as work; otherwise the losing workers keep rescanning the whole registry at full
        // tilt (Worker.runAsap, no nap) while one worker refreshes, an O(workers x views)
        // busy-spin. The notification-driven caller ignores the result.
        boolean attempted = false;
        // Apply-lag back-off: a prior cycle deferred this view (raw-WAL O3 or coupled dedup
        // drain) because ApplyWal2TableJob had not applied the base to the seqTxn the replay
        // reads. Skip re-entering the full window recompute until the floor elapses so the
        // worker does not hot-spin the drain every tick; apply advances on its own, so a tick
        // past the floor converges. Cheap guard before the latch - a deferred view costs a
        // clock read, not a re-drain. Covers both refresh entry paths. Side-effect free: the
        // floor is cleared only by the authoritative under-latch check below.
        if (isApplyLagDeferred(instance, false)) {
            return false;
        }
        // Live-view WAL apply back-off: the refresh worker drives the view's OWN WAL apply
        // inline (applyWalDirect) after committing a flushed lead or a coupled-drain batch.
        // When that apply is backed off under memory pressure it silently no-ops at
        // ApplyWal2TableJob's isReadyToProcess gate (returning without advancing the applied
        // seqTxn), so committing this cycle would land rows in the LV WAL that never reach
        // disk while the tier gets stamped as if they had - size()/count()/LIMIT would then
        // undercount them until the pressure eased. Skip the whole cycle instead: the
        // already-published lead stays in RAM under its valid stamp and reads stay correct,
        // and the next tick retries once apply readiness returns (the back-off self-clears on
        // its timeout). A healthy view - the common case - reads ready and is unaffected.
        // Cheap guard before the latch, like the apply-lag defer above.
        if (!engine.getTableSequencerAPI().getTxnTracker(instance.getLiveViewToken())
                .getMemPressureControl().isReadyToProcess()) {
            return false;
        }
        // Another worker already holds this view's refresh latch. Report no work so this
        // worker backs off instead of busy-rescanning the registry while the holder runs.
        if (!instance.tryLockForRefresh()) {
            return false;
        }
        String invalidationReason = null;
        // Bound each refresh turn (one refreshInstance call) by max commits
        // and max duration so a long backlog does not monopolise the worker.
        // The yield itself lives at the per-base-seqTxn boundary inside
        // incrementalRefresh; the budget snapshot resets per turn.
        turnStartUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        turnCommitsProcessed = 0;
        // No rows fed yet, so the accumulators match the last durable commit.
        windowStateDirty = false;
        // Lead-reconstruction mode: a read-only replica computes the un-flushed lead into RAM for
        // freshness parity but must never flush, apply, seed, or advance a durable watermark --
        // the on-disk tier is fed by the global apply job from replicated WAL. The enterprise
        // subclass overrides isLeadReconstruction() to select it; the primary default is false.
        final boolean leadReconstruction = isLeadReconstruction();
        // Bind the view so the shared context's getMemoryTracker() resolves to THIS view's
        // tracker; the window cursor reads it at open() to charge the functions' partition
        // maps. The finally clears it, so the worker's next view cannot charge this one.
        executionContext.ofRefreshingInstance(instance);
        try {
            if (engine.isReadOnlyMode()) {
                instance.clearCheckpointTimelineOwnership();
            }

            // A definition-less stub (torn / too-new _lv or _lv.s) must never refresh -
            // it has no definition to drive from. Both refresh entry paths already
            // filter it (the fallback scan via isStub(), the by-base-table map
            // structurally excludes it), so this is defense-in-depth against a future
            // third caller reaching a stub here and NPEing on getDefinition().
            if (instance.isStub() || instance.isDropped() || instance.isInvalid()) {
                return false;
            }
            // Snapshot freeze: DatabaseCheckpointAgent is mid-copy of this LV's
            // files. Skip this turn so _lv.s and the on-disk tier do not
            // advance while the agent is reading them. The agent clears the
            // flag via endCheckpoint() once the per-LV copy completes; the
            // next fallback or notification tick picks the worker back up.
            // This check is load-bearing for the checkpoint deadlock fix: it runs
            // under the refresh latch acquired above, so a freeze armed AFTER this
            // turn took the latch is observed here and skips the turn, while a freeze
            // armed BEFORE is serialised by startCheckpoint's latch take-and-release.
            // That handshake is what lets the in-band _lv.s rewrites drop
            // waitForUnfrozen() without racing the agent's copy - do not move a rewrite
            // ahead of this guard or out of the latch hold.
            if (instance.isFreezeInProgress()) {
                return false;
            }
            // Authoritative apply-lag gate, under the refresh latch, and the only place the floor is
            // cleared. The pre-latch check above races: a worker that reads a satisfied floor there can
            // be descheduled, and by the time it clears the field another worker has already run a full
            // cycle under the latch, hit the lag again, and armed a NEWER floor -- which the stale clear
            // then erases, dropping this view back into a re-drain-every-tick loop. Arming happens under
            // this latch too (the LiveViewApplyLagException catch below), so checking and clearing here
            // is atomic against it.
            if (isApplyLagDeferred(instance, true)) {
                return false;
            }
            // Labels the refresh body so a compromised timeline recovery
            // can break straight to the out-of-latch invalidation below, skipping
            // the refresh + flush that would otherwise materialise the
            // inconsistent accumulators to disk.
            refreshBody:
            try {
                // First cycle after restart restores the newest compatible
                // timeline root, or rebuilds derived state when the timeline is
                // absent/unusable. The drain below must never start over durable
                // output with cold accumulators.
                // Single-shot per LV lifetime - the flag flips true whether the
                // restore succeeded, missed, or failed.
                // An in-process promote keeps the same LiveViewInstance but flips it from replica
                // lead reconstruction (which already burned checkpointRestoreAttempted) to a writable
                // primary, so the single-shot restart block below would otherwise be skipped on the
                // first primary cycle. Detect that role edge here, at the same gate the flag is managed
                // (both mutate only after the early-return checks above), so a replica cycle that burned
                // the flag also recorded leadReconstruction=true; the edge survives an intervening early
                // return because the previous role is not updated until a cycle reaches this point.
                final boolean promotedSinceLastRefresh =
                        instance.isLastRefreshLeadReconstruction() && !leadReconstruction;
                instance.setLastRefreshLeadReconstruction(leadReconstruction);
                // On that promote edge, re-arm the single-shot restart recovery WHEN (and only when) the
                // applied floor lags the LV table's applied state -- i.e. the replica's last _lv.s persist
                // failed and ApplyWal2TableJob swallowed it. The recovery then reconciles the floor to disk
                // truth AND rebuilds the window accumulators / lead frontier from the applied tier (a
                // head-miss replay that REPLACE_RANGE-rewrites the tier), so the promoted primary does not
                // resume from the stale floor/frontier and re-derive (forward-append duplicating) an
                // already-materialised base range. A clean promote has a consistent floor, so this is a
                // no-op and never forces a replay.
                if (promotedSinceLastRefresh
                        && engine.readLiveViewAppliedMaxBaseSeqTxn(instance.getLiveViewToken())
                        > instance.getStateReader().getLastProcessedSeqTxn()) {
                    instance.resetCheckpointRestoreAttempted();
                }
                if (!instance.isCheckpointRestoreAttempted()) {
                    instance.setCheckpointRestoreAttempted();
                    // Durable restart recovery is PRIMARY-ONLY. A read-only replica reconstructs its lead
                    // purely in RAM from replicated disk (the leadReconstruction branches below) and must
                    // never do durable recovery here: reconcileAppliedFloorAfterRestart would rewrite
                    // _lv.s -- which the global apply job owns on a replica, so it would race that write --
                    // and timeline fallback can REPLACE_RANGE-rewrite the on-disk tier. A node restarted
                    // read-only over an ex-primary's files may retain a local timeline, but it must not
                    // consume it until promotion. The flag is still burned above, so a later in-process
                    // promote re-arms this recovery through the promote-edge branch just above.
                    if (!leadReconstruction) {
                        // Reconcile a durable floor left behind by a crash between the
                        // inline apply and the trailing _lv.s persist, before timeline
                        // selection reconciles its generation coordinates.
                        reconcileAppliedFloorAfterRestart(instance);
                        if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
                            // Baseline observability: time bounded generation selection,
                            // root restore, and the (B,F] replay. Recorded once
                            // per LV lifetime regardless of outcome. Surfaced via
                            // live_views().head_checkpoint_restore_micros.
                            final long restoreStartUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                            tryRestoreFromTimeline(instance, getWindowFactory(instance));
                            instance.recordCheckpointRestoreMicros(
                                    engine.getConfiguration().getMicrosecondClock().getTicks() - restoreStartUs
                            );
                            if (instance.hasPendingInvalidationReason()) {
                                // The restore could not rebuild a consistent window
                                // state (replay-to-applied failed mid-gap leaving the
                                // accumulators a partial advance over disk, a dedup
                                // replay failed, or no safe derived-state rebuild was
                                // possible). Do NOT run the incremental refresh + flush
                                // below: they would advance and flush the inconsistent
                                // accumulators, leaving the (about-to-be-invalidated)
                                // view serving corrupted content off its own on-disk
                                // tier - an invalid view stays queryable. Break to the
                                // out-of-latch invalidation, which drains the stashed
                                // reason and marks the view invalid without a partial
                                // advance ever reaching disk.
                                break refreshBody;
                            }
                        }
                    }
                }
                // Seed phase: every view CREATEs in SEEDING state and stays there until the
                // sweep has covered everything <= seedTargetSeqTxn, feeding the base rows
                // that satisfy its START FROM boundary. The sweep takes priority over
                // incremental drain; once it completes, the next refresh tick resumes normal
                // incremental processing from seedTargetSeqTxn + 1.
                //
                // The sweep does not bump lastFlushTimeUs - the FLUSH EVERY rate limit
                // governs steady-state publish cadence, and a view should resume incremental
                // drain immediately after the sweep without an artificial 100ms+ stall.
                if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING) {
                    if (leadReconstruction) {
                        // A replica never runs the seed sweep (it writes disk). Serve disk-only
                        // while SEEDING. This is NOT the common replica path: _lv.s never
                        // replicates (only _lv, the definition, ships to the sequencer dir), so
                        // WalEvents.reconstructLiveViewFiles synthesizes a default _lv.s whose
                        // seedState is ACTIVE, and CairoEngine.applyLiveViewData preserves that
                        // local state as it advances the in-band watermark. A replica fed purely by
                        // replication therefore never sees SEEDING here -- it runs the ordinary
                        // lead-reconstruction path below, serving the primary's seeded rows off the
                        // replicated on-disk tier and reconstructing the un-flushed lead on top (see the
                        // enterprise test testReplicateSeedLiveViewReconstructsLead).
                        //
                        // This branch is reachable only for a node whose OWN _lv.s carries SEEDING:
                        // a primary demoted, or restarted, mid-sweep. Disk-only is the safe choice there
                        // -- the node cannot reliably detect the sweep's completion from replicated
                        // state (neither _lv.s nor the .cp replicate, and every sweep commit carries the
                        // same seedTargetSeqTxn watermark), and clearing SEEDING early would
                        // skip the sweep resume on a later promote and leave pre-CREATE history
                        // unmaterialised. The state does NOT self-clear from the in-band watermark
                        // (applyLiveViewData preserves the local seedState), so this view stays
                        // disk-only until the node promotes (and completes/resumes the sweep) or the
                        // view is recreated.
                        return attempted;
                    }
                    attempted = true;
                    runSeedSweep(instance);
                    instance.setLastRefreshTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                    instance.recordRefreshSuccess();
                    return attempted;
                }
                // Decide the cadence. A lead-eligible LV decouples refresh (drain
                // into the in-mem tier as the un-flushed lead, every tick with new
                // base commits) from flush (commit + apply + checkpoint, on the
                // FLUSH EVERY cadence). A coupled LV keeps the coupled cycle, gated by
                // FLUSH EVERY, applying every cycle so the tier stays a subset of disk.
                // Two things force the coupled cadence: a tier-unstorable output type
                // (a non-persisted type such as INTERVAL, or no designated timestamp),
                // which ensureLeadEligible rejects one-shot; and a DEDUP base, which
                // isDedupBase re-derives each cycle (mutable via ALTER). A dedup base
                // additionally reads the applied (post-dedup) base instead of raw WAL.
                final boolean dedupBase = isDedupBase(instance);
                final boolean leadEligible = ensureLeadEligible(instance) && !dedupBase;
                final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                final long lastFlushUs = instance.getLastFlushTimeUs();
                final long flushEveryMicros = instance.getDefinition().getFlushEveryMicros();
                final boolean flushDue = lastFlushUs == Numbers.LONG_NULL || nowUs - lastFlushUs >= flushEveryMicros;
                if (leadEligible) {
                    if (leadReconstruction) {
                        // Authoritative replica lead gate, under the refresh latch. The pre-latch check in
                        // scanForLaggingViews is a cheap skip only: with one job per live-view worker and
                        // every worker scanning every view, worker B can clear that check while worker A
                        // still holds the latch, and A can arm a gate (an O3 symbol catch-up barrier, say)
                        // before it releases. B would then drain straight through the barrier A just
                        // raised. Re-check here, where arming and checking serialise on the same latch.
                        if (deferReplicaLeadWork(instance, true)) {
                            return attempted;
                        }
                        final WindowRecordCursorFactory leadWindowFactory = getWindowFactory(instance);
                        if (!isLeadRollbackSupported(instance, leadWindowFactory)) {
                            // The replica cannot safely reconstruct this view's lead: a stalled publish
                            // would leave the window state advanced with no way to roll it back (the
                            // primary flushes such a stall to disk; a read-only replica cannot). Serve
                            // disk-only instead -- correct, at worst one flush cycle stale. Snapshot-capable
                            // views -- including partitioned and anchored shapes -- round-trip their window
                            // state through the in-RAM rollback; only non-snapshot-capable windows take this
                            // branch.
                            return attempted;
                        }
                        // Reconcile the in-RAM lead with the on-disk tier the global apply job
                        // advances asynchronously (as the primary's flushes replicate). Without this,
                        // a lead computed while the applied watermark lagged the base would keep rows
                        // that later landed on disk, double-counting them in size(). The window factory
                        // lets the reconcile drop and cold re-derive the lead when a replicated flush
                        // re-sequenced the on-disk symbol id space out from under a kept remainder.
                        reconcileLeadWithDisk(instance, leadWindowFactory);
                        attempted = true;
                    }
                    long refreshFrom = instance.getRefreshedUpToSeqTxn();
                    if (seqTxn > refreshFrom) {
                        // Refresh runs every tick with new base commits, ungated by
                        // FLUSH EVERY, so the tier leads disk by the rows refreshed
                        // since the last flush. TransactionLogCursor treats txnLo as
                        // exclusive, so pass refreshFrom directly.
                        attempted = true;
                        // A drain that cannot read a base WAL segment propagates: handleRefreshFailure
                        // retries it, and a segment that is genuinely gone (a restore keeps the applied
                        // base TABLE, not its WAL) lands on the applied-base re-derive there.
                        incrementalRefresh(instance, refreshFrom, seqTxn, true);
                    }
                    // Flush the accumulated lead on the FLUSH EVERY cadence -- primary only. A
                    // read-only replica never flushes: its on-disk tier is materialised by the
                    // global apply job from replicated WAL, and the lead above it stays in RAM,
                    // rebuilt by the incrementalRefresh above. The refresh may also have flushed
                    // (emergency, on a tier stall) on the primary, in which case refreshedUpTo ==
                    // lastProcessed and this is skipped.
                    if (!leadReconstruction && flushDue && instance.getRefreshedUpToSeqTxn() > instance.getLastProcessedSeqTxn()) {
                        attempted = true;
                        flushLead(instance, getWindowFactory(instance), instance.getRefreshedUpToSeqTxn(), 0);
                        instance.setLastFlushTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                    }
                } else {
                    if (leadReconstruction) {
                        // A non-lead-eligible LV (coupled cadence: a DEDUP base, or a tier-unstorable
                        // output type) keeps its in-mem tier a strict subset of disk -- no un-flushed
                        // lead exists, so the replica serves it correctly off the replicated on-disk
                        // tier. Nothing to reconstruct.
                        return attempted;
                    }
                    long lastSeqTxn = instance.getLastProcessedSeqTxn();
                    if (seqTxn > lastSeqTxn) {
                        // FLUSH EVERY rate-limit: skip if the previous commit was within
                        // flushEveryMicros. The fallback scan retries each worker tick, so
                        // this view's catch-up resumes naturally once the interval elapses.
                        // We bump lastFlushTimeUs to nowUs only after a successful refresh,
                        // so a long-running first commit does not double-charge the budget.
                        if (!flushDue) {
                            return attempted;
                        }
                        // TransactionLogCursor treats txnLo as exclusive (lastApplied), so we
                        // pass lastSeqTxn directly. The cursor's getTxn() returns entries with
                        // seqTxn > lastSeqTxn.
                        attempted = true;
                        if (dedupBase) {
                            if (isRangeProvablyClean(instance.getDefinition().getBaseTableToken(), lastSeqTxn, seqTxn)) {
                                // The applied base provably equals the raw WAL over this
                                // range (nothing deduped / skipped / removed). Take the
                                // proven raw-WAL path -- it appends additive same-ts rows without
                                // the applied-reader over-trigger, and drainBaseWal's own O3
                                // detection still routes a genuine below-frontier late row through
                                // o3Replay over the applied base.
                                incrementalRefresh(instance, lastSeqTxn, seqTxn, false);
                                // Coupled invariant: keep refreshedUpTo == lastProcessed (which
                                // incrementalRefresh, unlike drainAppliedBase, does not set) so a
                                // later ALTER DEDUP DISABLE flip back to the lead path resumes
                                // cleanly with no stale un-flushed lead / double emit. Uses
                                // getLastProcessedSeqTxn() so a partial or internal-o3Replay cycle
                                // stays consistent.
                                instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
                                // Coupled invariant: no un-flushed lead on the clean raw-WAL path.
                                instance.setLeadRowCount(0);
                                instance.bumpDedupRawWalCleanCycles();
                            } else {
                                // Cold signal, apply lag, or a divergence (dedup / skip / non-DATA
                                // op) in range: read the applied, post-dedup base via a TableReader
                                // and route any timestamp-overlap batch through o3Replay.
                                drainAppliedBase(instance, lastSeqTxn, seqTxn);
                            }
                        } else {
                            incrementalRefresh(instance, lastSeqTxn, seqTxn, false);
                        }
                        instance.setLastFlushTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                    }
                }
                instance.setLastRefreshTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                if (attempted) {
                    instance.recordRefreshSuccess();
                }
            } catch (LiveViewApplyLagException e) {
                // Cooperative apply-lag handoff: this cycle's O3 replay needs the
                // base applied to a seqTxn ApplyWal2TableJob has not reached yet.
                // ensureBaseApplied threw before any destructive replay work, so
                // the view is untouched - no watermark advance, no failure
                // accounting, no invalidation. Leave invalidationReason null and
                // return through the finally; the next fallback scan retries this
                // view (head > processedTo still holds) once the apply catches up.
                // Not counting this toward the flush-retry budget is deliberate:
                // apply lag is transient and self-heals, unlike a refresh fault.
                // Arm a short back-off so the next scans skip this view instead of
                // re-draining the whole window every tick until apply lands. Record the
                // target seqTxn first so the pre-latch guard, which reads it once it sees
                // the floor, can clear the floor early the moment the base applies past it.
                instance.setApplyLagDeferTargetSeqTxn(e.getTargetSeqTxn());
                instance.setApplyLagDeferUntilUs(
                        engine.getConfiguration().getMicrosecondClock().getTicks() + APPLY_LAG_DEFER_BACKOFF_US);
                LOG.debug().$("live view O3 replay deferred, base apply lag [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", base=").$safe(e.getBaseTableName())
                        .$(", advanceTo=").$(e.getTargetSeqTxn())
                        .$(", appliedSeqTxn=").$(e.getAppliedSeqTxn()).I$();
            } catch (Throwable t) {
                invalidationReason = handleRefreshFailure(instance, t, leadReconstruction);
            }
        } finally {
            // Release the worker's staging buffer under the refresh latch (before unlockAfterRefresh),
            // so its per-view-tracker-charged pages are freed while THIS view's tracker is still alive
            // and before tryCloseIfDropped / tryFreeRuntimeStateIfInvalid (below) can recycle it. The
            // buffer is worker-owned and reused across views, so scoping its charge to the cycle is what
            // keeps the tracker binding safe; the next populate-the-tier cycle rebuilds it.
            // Free the buffer inside a nested try so the release below always runs even if
            // close() throws. Misc.free catches only IOException, so an AssertionError (under
            // -ea) or a CairoException from a native-memory / tracker-balance assert in
            // LiveViewInMemoryBuffer.close() would otherwise skip unlockAfterRefresh and wedge
            // the view's refresh latch forever (and leave the exec-context tracker bound to it).
            // The nested finally preserves the deliberate "free under the latch" ordering.
            try {
                stagingBuffer = Misc.free(stagingBuffer);
                if (simulateStagingBufferCloseFaultForTest) { // @TestOnly, always false in production
                    simulateStagingBufferCloseFaultForTest = false;
                    throw new AssertionError("injected staging-buffer close fault");
                }
            } finally {
                executionContext.ofRefreshingInstance(null);
                instance.unlockAfterRefresh();
                instance.tryCloseIfDropped();
                // If this view was invalidated concurrently while this cycle held
                // the refresh latch (so the invalidator's own free lost the CAS),
                // free its runtime state now that the latch is released.
                instance.tryFreeRuntimeStateIfInvalid();
            }
        }
        // Invalidate outside the refresh latch: invalidateLiveView's
        // freeze-aware synchronized block parks on the instance monitor when a
        // checkpoint freeze is active, and the agent's startCheckpoint cannot
        // complete its latch handshake while the worker still holds the
        // refresh latch. Running the invalidate after unlockAfterRefresh
        // avoids that deadlock.
        if (invalidationReason == null) {
            // The restore path may have stashed its own invalidate reason
            // (e.g. version-too-old function snapshot in the head .cp). Drain
            // and run it on the same out-of-latch path.
            invalidationReason = instance.takePendingInvalidationReason();
        }
        if (invalidationReason != null) {
            engine.invalidateLiveView(instance, invalidationReason);
        }
        return attempted;
    }

    /**
     * Flush retry budget (primary path): count consecutive failures and the
     * elapsed wall-clock time since the streak began. On budget exhaustion,
     * returns the reason string so the caller can drive the invalidation
     * outside the refresh latch; otherwise returns null. The view stops
     * refreshing but stays queryable; recovery is operator-driven (DROP +
     * CREATE). A {@code leadReconstruction} (read-only-replica) failure takes a
     * separate branch that never invalidates -- see {@link #onReplicaLeadRefreshFailure}.
     */
    private String handleRefreshFailure(LiveViewInstance instance, Throwable t, boolean leadReconstruction) {
        // Count the fault before any of the branches below decide to swallow it. Most of them do:
        // the read-only-gate refusal, the metadata-drift recompile and the mid-drain rebuild all
        // return null, and the rebuild even calls recordRefreshSuccess(), so nothing else survives to
        // tell a test that the incremental path faulted at all.
        instance.recordRefreshFault();
        // Captured before the metadata-drift block reassigns t: that path already
        // rebuilds, so the mid-drain rebuild below must not fire a second time.
        final boolean wasMetadataDrift = t instanceof TableReferenceOutOfDateException;
        if (t instanceof CairoException ce && ce.isAuthorizationError()) {
            // A demote flipped the read-only flag after this cycle acquired its WalWriter but before the
            // commit; fencedLiveViewCommit re-checked isReadOnlyMode() under the role-switch read lock and
            // refused the mint (or getWalWriter's own eager check refused the acquire). A role-switch
            // refusal is NOT a refresh failure: never invalidate. Invalidation is durable and sticky with
            // no replica-side recovery, so counting a demote refusal toward the flush-retry budget could
            // brick a view locally while the primary stays healthy. Retry later instead -- the node is
            // becoming a replica and the next tick runs lead reconstruction; a live view is derived state,
            // so the new primary recomputes the lead forward. The refresh job runs under the internal
            // all-access context, so an authorization error here can only be the read-only gate. Mirrors
            // MatViewRefreshJob.rethrowReadOnlyRefusal + handleErrorRetryRefresh.
            LOG.info().$("live view refresh refused by read-only gate, retrying later [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return null;
        }
        if (t instanceof TableReferenceOutOfDateException) {
            // The base table's metadata version drifted from the cached compiled factory:
            // a schema change that does not touch the view's referenced columns keeps the
            // view valid by design (invalidateLiveViewsForBaseSchemaChange leaves it
            // alone), but the factory's page-frame column mapping no longer matches the
            // reader's column layout, so LiveViewRefreshSqlExecutionContext.getReader
            // refused to serve the mismatched reader. Not a refresh failure: recompile
            // and rebuild instead of counting toward the invalidation budget.
            t = recoverFromBaseMetadataDrift(instance, leadReconstruction);
            if (t == null) {
                return null;
            }
            // The recovery replay itself failed; account for THAT error below.
        }
        // Mid-drain fault with the accumulators advanced past the last durable commit:
        // rebuild from the applied base so the retry does not double-advance them. Skip
        // when the drift path already rebuilt, when nothing was fed (windowStateDirty
        // false - includes a transient table-absent during CREATE / DROP), or for a
        // read-only replica (its lead is rebuilt every tick; it backs off below).
        if (windowStateDirty
                && !wasMetadataDrift
                && !leadReconstruction
                && !(t instanceof CairoException dce && dce.isTableDoesNotExist())) {
            Throwable rebuildErr = rebuildWindowStateAfterMidDrainFailure(instance);
            if (rebuildErr == null) {
                return null;
            }
            // The rebuild replay itself failed; account for THAT error below.
            t = rebuildErr;
        }
        long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        instance.recordRefreshFailure(nowUs);
        if (leadReconstruction) {
            // Read-only-replica lead reconstruction: NEVER invalidate. The lead is derived, in-RAM state
            // rebuilt every tick off the applied base, and the failure is typically transient (e.g. a
            // reader failure storm while a large replicated ALTER applies to the base). Invalidation, by
            // contrast, is durable and sticky with no replica-side recovery (see onReplicaLeadRefreshFailure),
            // so a transient lead-loop fault must not brick the view locally while the primary stays
            // healthy. Arm a back-off (the enterprise subclass mirrors the publish-stall floor) so the scan
            // idles instead of re-draining into the same fault every tick; the view stays active and serves
            // disk-only via the seqTxn fence, and a later tick past the floor re-drains and resumes once the
            // fault clears.
            onReplicaLeadRefreshFailure(instance);
            LOG.error().$("live view lead reconstruction failed, backing off [view=").$(instance.getDefinition().getViewName())
                    .$(", retryCount=").$(instance.getFlushRetryCount())
                    .$(", error=").$(t).I$();
            return null;
        }
        // A breach of THIS view's configured limit means its working set does not fit the
        // budget the operator set. Retrying re-allocates into the same ceiling and ends at
        // the generic budget message anyway, throwing away the one diagnostic that says why.
        // Invalidate now, carrying the tracker's own message (limit, usage, workload).
        //
        // Match the breach PHRASE, not the workload name: Unsafe stamps the workload into the
        // native-OOM message too ("sun.misc.Unsafe.allocateMemory() OutOfMemoryError
        // [workload=...]"), which fires whatever the limit is. Treating that as a limit breach
        // would permanently invalidate a view on a transient host OOM - a behaviour change for
        // every user on the default limit of 0. "query memory limit exceeded" is emitted only
        // by the per-query limit check (Java and Rust alike); the native-OOM and global-RSS
        // messages do not carry it, so both keep the retry budget. The limit > 0 gate makes
        // the default config structurally unable to reach this path.
        if (t instanceof CairoException ce
                && ce.isOutOfMemory()
                && engine.getConfiguration().getLiveViewRefreshMemoryLimitBytes() > 0
                && Chars.contains(ce.getFlyweightMessage(), "query memory limit exceeded")) {
            LOG.critical().$("live view exceeded its refresh memory limit, invalidating [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$safe(ce.getFlyweightMessage()).I$();
            return Chars.toString(ce.getFlyweightMessage());
        }
        int retryCount = instance.getFlushRetryCount();
        long retryStartUs = instance.getFlushRetryStartUs();
        int maxRetry = engine.getConfiguration().getLiveViewFlushRetryMax();
        long maxDurationMicros = engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros();
        long elapsedUs = retryStartUs == Numbers.LONG_NULL ? 0 : nowUs - retryStartUs;
        // A transiently unresolvable table is not a refresh error the COUNT budget should
        // invalidate on. It happens when the refresh worker scans a newly registered instance
        // during the CREATE deferred-name window -- createLiveView registers the instance in the
        // refresh registry before commitDeferredTableNameAndRelease flips the table name, so
        // getWalWriter throws "table does not exist" until the name commits -- or when a concurrent
        // DROP is mid-flight. A new view fires the refresh scan immediately at CREATE (its
        // sweep covers existing history, not a future commit), so a fast worker can spin the count
        // budget to exhaustion inside that sub-millisecond window and brick a freshly created view.
        // Gate this case on the wall-clock duration budget only: the CREATE transient clears within
        // the same CREATE and the retry succeeds, while a genuinely never-resolving table still
        // invalidates after the duration cap. A dropped/invalidated view is short-circuited by the
        // isDropped()/isInvalid() gate in refreshInstance, so it never spins here.
        boolean tableTransient = t instanceof CairoException ce && ce.isTableDoesNotExist();
        boolean budgetExhausted = elapsedUs >= maxDurationMicros || (!tableTransient && retryCount >= maxRetry);
        if (budgetExhausted) {
            // Last resort before a permanent invalidation: a base WAL segment the drain needs has
            // been missing for the whole budget, so it is not coming back. That is what a restore
            // leaves behind - a backup captures the applied base TABLE, not its WAL segments - and
            // the view's rows are all in that table, so re-derive from it rather than brick a view
            // whose data is right there. Spending the budget first is what separates this from a
            // transient read fault, which clears on a retry and never reaches here.
            if (!leadReconstruction
                    && t instanceof CairoException walMissing
                    && isBaseWalSegmentFileMissing(walMissing)
                    && rederiveFromAppliedBaseAfterWalLoss(instance, walMissing)) {
                return null;
            }
            LOG.critical().$("live view refresh budget exhausted, invalidating [view=").$(instance.getDefinition().getViewName())
                    .$(", retryCount=").$(retryCount)
                    .$(", elapsedUs=").$(elapsedUs)
                    .$(", error=").$(t).I$();
            return "flush retry budget exhausted";
        }
        LOG.critical().$("live view refresh failed [view=").$(instance.getDefinition().getViewName())
                .$(", retryCount=").$(retryCount)
                .$(", error=").$(t).I$();
        return null;
    }

    private void refreshViewsForBaseTable(TableToken baseTableToken, long seqTxn) {
        LiveViewRegistry registry = engine.getLiveViewRegistry();
        registry.getViewsForBaseTable(baseTableToken.getTableName(), viewInstanceSink);
        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            if (instance.getDefinition().getBaseTableToken() == null) {
                // A definition registered before its base table resolved (replica download-order
                // race) can reach this path after a promote. The notification carries the base
                // token, so heal the definition before refreshInstance dereferences it.
                instance.getDefinition().resolveBaseTableToken(baseTableToken);
            }
            if (seqTxn > instance.getLastProcessedSeqTxn()) {
                refreshInstance(instance, seqTxn);
            }
        }
    }

    /**
     * Returns the stable identifier for a window function's enclosing
     * factory. Window function impls live as static inner classes of their
     * factory (e.g. {@code AvgDoubleWindowFunctionFactory$AvgOverPartition...}),
     * so the enclosing class name survives an impl rename while the
     * function class name does not. Top-level WindowFunction impls (none
     * today) fall back to their own class name.
     */
    private static String snapshotFactoryName(WindowFunction f) {
        Class<?> enclosing = f.getClass().getEnclosingClass();
        return (enclosing != null ? enclosing : f.getClass()).getName();
    }

    private static WindowRecordCursorFactory unwrapWindowFactory(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (f instanceof WindowRecordCursorFactory wf) {
                return wf;
            }
            if (f instanceof QueryProgress) {
                f = f.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    /**
     * Output bundle for {@link #drainBaseWal}. Captures everything a drain pass
     * over the base WAL produces: how far it advanced, how many output rows it
     * emitted, the timestamp range, and any out-of-order detection. The
     * disk-subset cycle and the lead refresh both read it after the call.
     */
    protected static final class DrainResult {
        // Highest base seqTxn processed this pass (-1 if none).
        public long advanceTo;
        // Output rows emitted this pass (mirrored to the staging buffer when the
        // tier is populated; written to the LV WAL when a walWriter was supplied).
        public long appendedRows;
        // Max output timestamp across the pass, in base-table units (LONG_NULL if none).
        public long batchMaxTs;
        // The offending late-row timestamp when o3Detected.
        public long o3LateRowTs;
        // True when a base commit arrived out of order; the caller hands off to o3Replay.
        public boolean o3Detected;
        // The base seqTxn of the out-of-order commit when o3Detected.
        public long o3SeqTxn;
        // Max output timestamp mirrored to the staging buffer (LONG_NULL if none).
        public long stagingMaxTs;
        // Min output timestamp mirrored to the staging buffer (LONG_NULL if none).
        public long stagingMinTs;

        void reset() {
            advanceTo = -1;
            appendedRows = 0;
            batchMaxTs = Numbers.LONG_NULL;
            o3Detected = false;
            o3LateRowTs = Numbers.LONG_NULL;
            o3SeqTxn = Numbers.LONG_NULL;
            stagingMaxTs = Numbers.LONG_NULL;
            stagingMinTs = Numbers.LONG_NULL;
        }
    }

    /**
     * Output bundle for {@link #restoreFromHead(LiveViewInstance, WindowRecordCursorFactory, long, RestoredHeadState)}.
     * The fields capture the values restart-restore and O3 head-hit replay
     * both need after the disk read completes; the helper rewrites them on
     * each successful call and the caller reads them immediately.
     */
    private static final class RestoredHeadState {
        long lvRowsTotal;
        long manifestBaseSeqTxn;
        long maxTimestamp;
        // Seed sweep's data-cursor row offset read from a SEED_CURSOR
        // block. Numbers.LONG_NULL when the restored checkpoint carries no such
        // block (any steady .cp), signalling "not a resumable seed head".
        long resumeDataOffset;
        long stateBytes;

        void reset() {
            lvRowsTotal = 0L;
            manifestBaseSeqTxn = Numbers.LONG_NULL;
            maxTimestamp = Numbers.LONG_NULL;
            resumeDataOffset = Numbers.LONG_NULL;
            stateBytes = 0L;
        }
    }
}
