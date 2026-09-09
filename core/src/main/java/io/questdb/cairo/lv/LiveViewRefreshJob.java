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
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableMetadata;
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
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.window.LiveViewCheckpointFunctionCompiler;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
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
 *     {@code engine.advanceLiveViewConsumedSeqTxn}, and the worker seals a
 *     checkpoint root into the timeline under {@code _checkpoints/}.</li>
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
    // Fixed policy for the config-gated checkpoint compaction pass: a data segment
    // is a compaction source when at most half its bytes are still live, and a pass
    // drains at least two and at most this many such segments so a lone sparse
    // segment does not trigger a whole-timeline rewrite and one pass stays bounded.
    private static final int COMPACTION_MAX_LIVE_FRACTION_PERCENT = 50;
    private static final int COMPACTION_MAX_SOURCE_SEGMENTS = 8;
    private static final int COMPACTION_MIN_SOURCE_SEGMENTS = 2;
    // Consecutive failed cadence seals that prove the fault is deterministic rather
    // than transient. Below this the seal simply retries at its cadence, which is
    // right for a held writer or a momentarily full disk. At it, the view releases
    // both WAL purge floor arms: a fault that survived this many attempts will not
    // produce the recovery state those arms are held for, and holding them retains
    // the base WAL indefinitely while the base keeps ingesting.
    private static final int MAX_CONSECUTIVE_SEAL_FAILURES = 3;
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
    // Backoff arming the seal cooldown once MAX_CONSECUTIVE_SEAL_FAILURES is spent,
    // doubling per further failure and capped. A deterministically failing seal
    // re-streams the whole ring before it throws - up to gigabytes of encode for a
    // ring at the state-page reference ceiling - so retrying at the cadence burns
    // that work every tick forever. The cap keeps a view whose fault later clears (a
    // ring that shrank back under the bound, a disk that freed) recovering within
    // the hour.
    private static final long SEAL_COOLDOWN_BASE_MICROS = Micros.MINUTE_MICROS;
    // Bounds the shift the backoff takes. Sixteen doublings of the base already run
    // to weeks, so the cap below always wins; the bound exists so an unbounded
    // failure streak cannot shift past 63 and wrap.
    private static final int SEAL_COOLDOWN_MAX_DOUBLINGS = 16;
    private static final long SEAL_COOLDOWN_MAX_MICROS = 60 * Micros.MINUTE_MICROS;
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final AnchorDispatchingCursor anchorDispatchingCursor = new AnchorDispatchingCursor();
    // Reusable {minTs, maxTs} out-pair from computeApplyAheadBounds. Worker-owned;
    // rewritten in full on every call, read out immediately by planO3Repair.
    private final long[] applyAheadBounds = new long[2];
    // Second out-value of the same computeApplyAheadBounds call: whether every commit in
    // the apply-ahead range only ADDED rows. Separate from the pair above because the two
    // are independent - a classifiable range whose timestamps bound the repair may still
    // hold a REPLACE_RANGE delete band that denies the ROWS discovery its key domain.
    private boolean applyAheadInsertOnly;
    private final ApplyWal2TableJob applyJob;
    // Test-only observability for the WAL-loss re-derive's drift branch. Counts the
    // re-derives this worker completed only after recompiling against changed base
    // metadata, so the plain success path leaves it at 0. Kept on the worker rather
    // than the view because it is not a production metric; a test reads it to prove
    // which branch ran - the two branches differ in nothing else a test can observe.
    private long baseMetadataDriftRecompileCount;
    private final BlockFileWriter blockFileWriter;
    // Sits directly under the window cursor on the two repair replays that
    // re-version logical roots, so each boundary freezes between two rows rather
    // than inside one. See BoundaryFreezingCursor for why the replay's own row
    // loop is one row too late to freeze from.
    private final BoundaryFreezingCursor boundaryFreezingCursor = new BoundaryFreezingCursor();
    // Flyweight record over an in-mem tier buffer row, used by the flush path to
    // feed the compiled copier when materialising the un-flushed lead into the LV
    // WAL. Reused across rows; rebound via of() before each copy.
    private final LiveViewBufferRecord bufferRecord = new LiveViewBufferRecord();
    // Restores of versioned-timeline roots: the out-of-order resume anchor, the
    // seed resume, restart recovery and the repair plan's anchor search. Lazily
    // allocated on this worker's first restore and rebound per call, so a worker
    // builds the reader tree once rather than once per restored root. Bound only
    // for the duration of one restore - see borrowCheckpointTimelineStoreReader.
    private LiveViewCheckpointTimelineStoreReader checkpointTimelineStoreReader;
    // Publisher for versioned-timeline roots: the in-order cadence append and the
    // out-of-order range splice. Lazily allocated on this worker's first seal.
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
    private final LiveViewRefreshSqlExecutionContext executionContext;
    // Stand-in boundary schedule for a rebuild that runs without a repair session -
    // the unlocalized one, which versions no logical root. Always empty and never
    // written, so the replay's segmentation is a dead branch rather than a null check
    // per row.
    private final ObjList<LiveViewCheckpointTimelineEntry> emptyRepairBoundaries = new ObjList<>();
    private final FilteringRecordCursor filteringCursor = new FilteringRecordCursor();
    private final PageFrameMemoryPool memoryPool;
    private final Path path = new Path();
    private final LiveViewRefreshTask refreshTask = new LiveViewRefreshTask();
    // Coordinates of the out-of-order repair currently executing: the pinned base
    // snapshot, the correction floor, the retire floor and the chosen executor.
    // One instance per worker, refilled by planO3Repair at the start of each
    // repair; the two executors only read it. Repairs never nest, so the single
    // instance cannot be observed mid-refill.
    private final LiveViewCheckpointRepairPlan repairPlan = new LiveViewCheckpointRepairPlan();
    // Publication ordering of the out-of-order repair currently executing: which
    // stage it has reached, the live-view seqTxn its replacement minted, and what
    // it does with the runtime once it publishes. One instance per worker, cleared
    // at the start of each repair; repairs never nest, so it cannot be observed
    // mid-walk.
    private final LiveViewCheckpointRepairPublication repairPublication = new LiveViewCheckpointRepairPublication();
    // Reusable counter for the skip a resumed localized repair takes over the rows of
    // its resume group that a prior turn already folded.
    private final RecordCursor.Counter repairSkipCounter = new RecordCursor.Counter();
    // Reusable holder for the values the seed resume reads out of the timeline's
    // newest root. One instance per worker; mutated only on the refresh-worker
    // thread between restore calls. Avoids a per-call allocation on the resume.
    private final RestoredSeedState restoredSeedState = new RestoredSeedState();
    // Per-key ROWS repair-bound discovery, and the adapter the repair plan calls it
    // through. One of each per worker: the discovery owns a native counter map it
    // rebuilds per call, and the adapter carries the one repair's cursor factory and
    // pinned reader. Both are idle outside a repair, which never nests.
    private final LiveViewCheckpointRowsBounds rowsBounds;
    private final RowsBoundDiscovery rowsBoundDiscovery = new RowsBoundDiscovery();
    // Test-only observability for the O3 resume's runtime-anchor reuse. Counts the
    // replays this worker served from the live window state instead of restoring
    // the same root off disk. Kept on the worker rather than the view because it
    // is not a production metric; a test reads it to prove which branch ran.
    private long runtimeAnchorReuseCount;
    // Prices a repair's two candidate scan intervals off the pinned reader's partition
    // metadata, so the plan chooses between an anchor resume and a localized rebuild on
    // what each would read. One per worker, bound to the repair's reader per plan.
    private final LiveViewCheckpointScanCost scanCost = new LiveViewCheckpointScanCost();
    // Reusable counter for the seed sweep's skipRows() resume positioning.
    private final RecordCursor.Counter seedSkipCounter = new RecordCursor.Counter();
    // Test-only: when armed, the WAL-loss re-derive runs this action after its entry
    // broken-dependency check and before the replay, modelling the base apply that lands
    // mid-method - the window ApplyWal2TableJob opens between changing the base writer and
    // invalidating the dependent views. Lets a test drive the second, in-catch refusal
    // deterministically instead of racing a thread against the drive.
    // One-shot (self-clears on fire); always null in production.
    @TestOnly
    private Runnable simulateBaseApplyDuringRederiveForTest;
    // Test-only: when armed, the fallback scan runs this action inside the ahead guard - after
    // the view watermark read, before the two base head reads (cached sequencer head and tracker
    // writerTxn). That interval is the contract, not a convenience: it is the window a concurrent
    // refresh worker used to race, so a test can publish and refresh the next base commit there
    // instead of relying on thread timing. Keep the call between the watermark read and the base
    // reads. Moving it outside them, or reordering the reads across it, makes the guard sample a
    // coherent set either way and silently strips
    // testConcurrentRefreshCannotInvalidateFromStaleBaseHead of its power to tell the orders apart.
    // One-shot (self-clears on fire); always null in production.
    @TestOnly
    private Runnable simulateBaseCommitBetweenAheadGuardReadsForTest;
    // Test-only: an extra closeable whose close() throws. consumeBaseMetadataCloseFaultForTest
    // closes it and returns the resulting throwable as the primary of the very freeBestEffort call
    // that closes the pooled base metadata, so the fault lands in closeFailure exactly where a real
    // TableMetadata.close() failure would. Consuming it inside the close statement is what pins the
    // close's position: relocating that statement into a try-with-resources takes the fault with it,
    // and the test's "the fault fired" assertion goes red.
    // One-shot (self-clears on fire); always null in production.
    @TestOnly
    private QuietCloseable simulateBaseMetadataCloseFailureForTest;
    // Test-only: when armed, an out-of-order repair skips the inline apply of its
    // own REPLACE_RANGE block, modelling the apply silently no-opping (the LV writer
    // was busy, or its memory-pressure control backed off). Lets a test drive the
    // committed-but-unapplied branch of the post-commit reconciliation. Sticky until
    // cleared; always false in production.
    @TestOnly
    private boolean simulateRepairApplyFailureForTest;
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
    private final LiveViewStateStore stateStore;
    // Views whose localized out-of-order repair this worker parked on its turn
    // budget. Only the worker that suspended a repair can continue it - it holds
    // the pinned base snapshot, the live-view writer carrying the uncommitted
    // replacement and the capture that freezes through this worker's timeline
    // store writer - and neither the sharded idle scan nor the notification queue
    // guarantees the view comes back to this worker, so it drives them itself at
    // the top of every run. Pruned lazily: an entry whose repair has ended is
    // dropped on the next pass.
    private final ObjList<LiveViewInstance> suspendedRepairViews = new ObjList<>();
    // Reusable shape buffer for ensureStagingAndTier — alpha-ordered alongside
    // the other staging-related fields so the per-FLUSH-cycle code path can
    // mutate without per-call allocation.
    private final IntList tierColumnTypes = new IntList();
    // The versioned timeline's predecessor lookup, in the shape the repair plan
    // searches resume anchors through. One per worker, bound to the repair's view
    // per plan; idle outside a repair, which never nests.
    private final TimelineAnchorSource timelineAnchors = new TimelineAnchorSource();
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
    // Reader for the base's per-segment WAL-E event file (commit metadata). Held as a
    // field so one walk reuses it across the commits it visits, but every walk binds it
    // inside a try-with-resources: WalEventReader.close() is idempotent and leaves the
    // instance re-usable, and closing unmaps the event file. Without that a worker parks
    // the last segment it read mapped until the job closes, which on Windows keeps an
    // open handle on the segment directory and makes WalPurgeJob's rmdir fail with
    // ACCESS_DENIED for as long as the view stays idle - the segment is then never
    // reaped, even though the view has long consumed past it. Mirrors how
    // ApplyWal2TableJob and WalTxnDetails scope the same reader.
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
        this.memoryPool = new PageFrameMemoryPool(engine.getConfiguration(), 0L);
        this.walRecordCursor = new WalSegmentRecordCursor(addressCache, memoryPool);
        this.rowsBounds = new LiveViewCheckpointRowsBounds(engine.getConfiguration());
    }

    /**
     * Test-only: number of WAL-loss re-derives this worker completed through the base-metadata drift
     * branch of {@link #rederiveFromAppliedBaseAfterWalLoss} - the ones that recompiled through
     * {@link #recoverFromBaseMetadataDrift} and retried. A re-derive that succeeded on the plain path
     * does not count.
     */
    @TestOnly
    public long baseMetadataDriftRecompileCountForTest() {
        return baseMetadataDriftRecompileCount;
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
        checkpointTimelineStoreReader = Misc.free(checkpointTimelineStoreReader);
        checkpointTimelineStoreWriter = Misc.free(checkpointTimelineStoreWriter);
        stagingBuffer = Misc.free(stagingBuffer);
        Misc.free(rowsBounds);
        // A repair this worker parked between turns can only be continued by this
        // worker, so a closing worker abandons it rather than leaving its pinned
        // reader, uncommitted replacement and staged segment for nobody.
        for (int i = 0, n = suspendedRepairViews.size(); i < n; i++) {
            final LiveViewInstance instance = suspendedRepairViews.getQuick(i);
            final LiveViewCheckpointRepairSession session = instance.getSuspendedRepair();
            if (session != null && session.getOwner() == this) {
                instance.discardSuspendedRepair();
            }
        }
        suspendedRepairViews.clear();
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
        return processNotifications();
    }

    /**
     * Test-only: number of O3 resume replays this worker served from the live
     * window state instead of restoring the same logical root from disk. See
     * {@link #canReuseRuntimeAnchor}.
     */
    @TestOnly
    public long runtimeAnchorReuseCountForTest() {
        return runtimeAnchorReuseCount;
    }

    /**
     * Test-only failure injection for crash-ordering coverage of timeline publication.
     */
    @TestOnly
    public void setCheckpointTimelineTestFailureStage(int stage) {
        checkpointTimelineTestFailureStage = stage;
        if (checkpointTimelineStoreWriter != null) {
            checkpointTimelineStoreWriter.setTestFailureStage(stage);
        }
    }

    /**
     * Test-only: arms a one-shot action that {@link #rederiveFromAppliedBaseAfterWalLoss} runs after
     * its entry broken-dependency check and before the replay, so a test can land a base schema
     * change in exactly the window {@code ApplyWal2TableJob} opens between applying a structural
     * change to the base writer and invalidating the dependent views. That is the only way to reach
     * the re-derive's second, in-catch refusal deterministically: both checks read the same applied
     * base metadata, so nothing but a concurrent apply separates them. Production never calls this.
     * <p>
     * Two constraints bind the action. First, it runs inside the same {@code try} that
     * {@link #rederiveFromAppliedBaseAfterWalLoss}'s trailing {@code catch (Throwable)} closes, and
     * that clause logs and returns false - the very refusal such a test asserts - so an action that
     * lets a throwable escape turns a broken fixture into a passing test. The action must catch its
     * own throwables and hand them back to the test thread. Second, {@code refreshInstance} holds
     * the instance's refresh latch across this call, so the action must not reach a path that waits
     * for that latch: {@code DROP LIVE VIEW} on this view ({@code LiveViewInstance.fenceRefresh})
     * and a checkpoint freeze ({@code LiveViewInstance.startCheckpoint}) both spin-then-sleep on it
     * with no timeout, and would hang this thread against a latch only this thread can release.
     */
    @TestOnly
    public void setSimulateBaseApplyDuringRederiveForTest(Runnable action) {
        this.simulateBaseApplyDuringRederiveForTest = action;
    }

    /**
     * Test-only: arms a one-shot action that the fallback scan runs inside the live-view-ahead
     * guard - after the view watermark read, after which it blocks the scan, and before the two
     * base head reads (cached sequencer head and tracker writerTxn). The interval is what the
     * arming test asserts on: a commit published there is visible to the base reads but not to
     * the already-captured watermark, which is exactly the asymmetry that distinguishes the
     * correct read order from the racy one. Relocating this call outside the reads leaves
     * {@code testConcurrentRefreshCannotInvalidateFromStaleBaseHead} green under a read-order
     * swap that fully restores the race.
     * <p>
     * Unlike {@link #setSimulateBaseApplyDuringRederiveForTest}, the action runs with no refresh
     * latch held and no enclosing {@code catch}, so it may block and a throwable it raises
     * propagates out of {@code Job.run()} to the caller rather than being swallowed. Production
     * never calls this.
     */
    @TestOnly
    public void setSimulateBaseCommitBetweenAheadGuardReadsForTest(Runnable action) {
        this.simulateBaseCommitBetweenAheadGuardReadsForTest = action;
    }

    /**
     * Test-only: arms a one-shot closeable that
     * {@link #isRederiveRefusedForBrokenDependency} closes as part of the same statement that closes
     * the pooled base metadata (see {@link #consumeBaseMetadataCloseFaultForTest}), so the metadata
     * tenant still returns to the pool and {@code assertMemoryLeak} keeps its force. A
     * {@code close()} that throws is the only way a test can produce a close failure there:
     * {@code TableMetadataPool} has no reachable path that makes its tenant's {@code close()} throw
     * (see the comment on the close itself). The read-failure {@code catch} releases the armed
     * closeable too, so arming it never leaks when the read throws first.
     * <p>
     * Because the close statement itself consumes the fault, that statement is what the test pins:
     * moving the close back inside a try-with-resources deletes the statement, the fault is never
     * consumed, and the test's "the injected close fault must have run" assertion goes red.
     * Production never calls this.
     */
    @TestOnly
    public void setSimulateBaseMetadataCloseFailureForTest(QuietCloseable closeFault) {
        this.simulateBaseMetadataCloseFailureForTest = closeFault;
    }

    /**
     * Test-only: makes every out-of-order repair skip the inline apply of its own
     * REPLACE_RANGE block, so the block stays committed-but-unapplied and the repair
     * takes its unreconciled branch. Production never calls this.
     */
    @TestOnly
    public void setSimulateRepairApplyFailureForTest(boolean simulate) {
        this.simulateRepairApplyFailureForTest = simulate;
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
     * Test-only: the worker's WAL page-frame cursor, for assertions on state the cursor keeps
     * across a drain - that its extracted-timestamp scratch releases an outlier transaction's
     * peak rather than retaining it for the worker's life, and that the ColumnMapping it
     * publishes matches the view's projection.
     */
    @TestOnly
    public WalSegmentPageFrameCursor walFrameCursorForTest() {
        return walFrameCursor;
    }

    protected CairoEngine getEngine() {
        return engine;
    }

    /**
     * Reports whether this node refreshes off the APPLIED base table rather than the raw base WAL.
     * <p>
     * The primary default is {@code false}: a primary owns its base WAL, so the raw-WAL drain
     * ({@link #drainBaseWal}) is the fresh, settled source. A read-only replica overrides this to
     * {@code true}: it downloads and applies its base WAL asynchronously, so the raw segments can still
     * be settling (mid-download / post-apply purge) when the sequencer head already advertises the
     * commit -- a raw read would transiently miss applied rows. Under symmetric local refresh the
     * replica runs the full refresh + flush locally, so
     * {@link #refreshInstance} routes it through the coupled applied-base drain
     * ({@link #drainAppliedBase}), which pins the applied base reader behind the cooperative apply-lag
     * gate and routes any timestamp overlap through {@code o3Replay} -- the same well-tested path a
     * DEDUP base already uses, just selected by node role instead of by dedup.
     */
    protected boolean prefersAppliedBaseRefresh() {
        return false;
    }

    /**
     * Publishes one logical checkpoint root into the versioned timeline for the seal
     * in progress, advancing the generation watermarks and the timeline WAL floor.
     * <p>
     * Runs for a forced seal too. A forced seal follows an O3 replay, which already
     * retired the timeline through {@link #retireCheckpointTimeline}, so the
     * append opens a fresh history whose single root describes the post-replay
     * state. Skipping it - as this did while the timeline was write-only on the
     * in-order path - leaves every replay-driven view with no timeline at all, so a
     * restart has nothing to restore from but a full rebuild from the view's
     * {@code START FROM} boundary.
     * <p>
     * {@code seedCursorOffset} carries the seed sweep's base-cursor row offset
     * when a mid-sweep cadence event drives the append, so a restart can resume
     * the sweep from the root this publishes; a steady seal passes
     * {@link Numbers#LONG_NULL}.
     *
     * @return the append's result, carrying the appended root's
     * {@code checkpointId} and the logical state byte size attributed to it,
     * both of which a steady seal mirrors onto the head metadata
     */
    private LiveViewCheckpointTimelineStoreWriter.Result appendCheckpointTimelineRoot(
            LiveViewInstance instance,
            ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long baseSeqTxn,
            long batchMaxTs,
            long seedCursorOffset
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
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
        final LiveViewCheckpointTimelineStoreWriter.Result timelineResult;
        // The role-switch read lock stays, the read-only refusal does not. Under symmetric local
        // refresh every node seals its own node-local timeline over its own durable output, so a
        // demote landing mid-append no longer makes the append illegal - it changes nothing this
        // root describes. The lock keeps the append serialised against the switch itself, matching
        // fencedLiveViewCommit.
        final Lock roleLock = engine.getRoleSwitchReadLock();
        roleLock.lock();
        try {
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
                    instance.getLvRowsTotal(),
                    instance.getMinSeenTsSinceCheckpoint(),
                    seedCursorOffset,
                    instance.getMemoryTracker()
            );
        } finally {
            roleLock.unlock();
        }
        // Only a published root moves the window the next seal measures over. A
        // failed append leaves it open, so the next attempt still sees every row
        // fed since the root that is actually on disk.
        instance.resetMinSeenTsSinceCheckpoint();
        instance.recordCheckpointTimelineWalPurgeFloor(timelineResult.getWalPurgeFloor());
        instance.recordCheckpointTimelineStats(timelineResult.getStats());
        if (timelineResult.getLiveSegmentCount() != Numbers.LONG_NULL) {
            instance.recordCheckpointGcSweep(
                    timelineResult.getLiveSegmentCount(),
                    timelineResult.getObsoleteSegmentBytes()
            );
        }
        return timelineResult;
    }

    /**
     * Records that the compiled factory's window accumulators no longer agree with the
     * view's durable output, both for this turn and - on {@code instance} - for later
     * ones. The per-turn field alone is not enough: {@link #refreshInstance} re-seeds it
     * at every turn entry, so before this fix a rebuild that itself failed lost the fact
     * that a rebuild was still owed, and the next turn drained forward over a wiped or
     * half-advanced runtime.
     */
    private void markWindowStateDirty(LiveViewInstance instance) {
        windowStateDirty = true;
        instance.setWindowStateDirty(true);
    }

    /**
     * Runs one physical compaction pass over the instance's checkpoint timeline when
     * the {@code cairo.live.view.checkpoint.compaction.interval} cadence is reached.
     * Disabled by default (interval zero); when set, it repacks the still-live pages
     * of sparse data segments into a fresh segment and redirects the roots, so the
     * drained segments retire for the purge job. Best-effort: a fault abandons the
     * candidate and leaves the published generation byte-identical.
     */
    private void maybeCompactCheckpointTimeline(LiveViewInstance instance) {
        final long interval = engine.getConfiguration().getLiveViewCheckpointCompactionInterval();
        if (interval <= 0) {
            return;
        }
        if (checkpointTimelineStoreWriter == null) {
            // Nothing to compact through. Return before the cadence counter advances, so a
            // stretch of seals reached without a writer does not silently burn the interval.
            return;
        }
        // Count seals. The cadence config is documented in seals, and lvSeqTxn is the BASE
        // table's sequencer txn: it advances on the base's schedule, and this hook is only
        // reached past the seal cadence gate, so under a steady ingest rate consecutive seals
        // land on a near-constant seqTxn stride. A modulo test against that stride fires at
        // every seal or at no seal at all, decided by an arbitrary phase offset rather than by
        // the configured interval - and the "never" case leaves the dead bytes that repairs
        // strand in ring-shared segments unreclaimed, growing _checkpoints without bound while
        // compaction is nominally enabled.
        if (instance.incrementAndGetSealsSinceCompaction() < interval) {
            return;
        }
        instance.resetSealsSinceCompaction();
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            // Compaction repacks a timeline this node published, so it runs on whichever role
            // that node currently holds - see appendCheckpointTimelineRoot for why the role
            // read lock outlives the read-only refusal it used to carry.
            final Lock roleLock = engine.getRoleSwitchReadLock();
            roleLock.lock();
            try {
                LiveViewCheckpointCompaction.compact(
                        engine.getConfiguration(),
                        checkpointsDir,
                        checkpointTimelineStoreWriter,
                        instance.getLiveViewToken().getTableId(),
                        0,
                        true,
                        COMPACTION_MAX_LIVE_FRACTION_PERCENT,
                        COMPACTION_MIN_SOURCE_SEGMENTS,
                        COMPACTION_MAX_SOURCE_SEGMENTS
                );
            } finally {
                roleLock.unlock();
            }
        } catch (Throwable t) {
            LOG.error().$("could not compact live view checkpoint timeline [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
        }
    }

    /**
     * Runs one purge sweep over the instance's checkpoint directory when the
     * {@code cairo.live.view.checkpoint.purge.interval} cadence is reached. The
     * sweep unlinks every segment no generation reaches any more, every
     * final-name file no generation ever catalogued, and stages the catalogue
     * entries it leaves naming nothing, which the next seal removes.
     * <p>
     * Without the cadence a sweep runs only where {@link LiveViewCheckpointLifecycle#reconcile}
     * does - once per worker per directory - so everything a seal, a repair, a
     * compaction supersedes after that first seal waits for a restart before its
     * bytes come back, and so do the files a failed one renamed into place. Both
     * halves wait exactly that long, because reconciliation applies the same two
     * rules; the cadence is what stops either of them from waiting. Best-effort,
     * like the compaction pass ahead of it:
     * the sweep publishes no generation, so a fault costs one deferred collection
     * and leaves the checkpoint store byte-identical.
     */
    private void maybeSweepCheckpointSegments(LiveViewInstance instance) {
        final long interval = engine.getConfiguration().getLiveViewCheckpointPurgeInterval();
        if (interval <= 0) {
            return;
        }
        if (checkpointTimelineStoreWriter == null) {
            // Nothing to sweep through, and nothing to hand a retirement proposal to.
            // Return before the cadence counter advances, as compaction does.
            return;
        }
        if (instance.incrementAndGetSealsSincePurge() < interval) {
            return;
        }
        instance.resetSealsSincePurge();
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            // The sweep collects under a timeline this node published, so it runs on
            // whichever role that node currently holds - see appendCheckpointTimelineRoot
            // for why the role read lock outlives the read-only refusal it used to carry.
            final Lock roleLock = engine.getRoleSwitchReadLock();
            roleLock.lock();
            final LiveViewCheckpointTimelineStoreWriter.SweepResult result;
            try {
                result = checkpointTimelineStoreWriter.sweep(
                        checkpointsDir,
                        instance.getLiveViewToken().getTableId(),
                        0,
                        true
                );
            } finally {
                roleLock.unlock();
            }
            if (result.isSwept()) {
                instance.recordCheckpointGcSweep(result.getLiveSegmentCount(), result.getObsoleteBytes());
                LOG.debug().$("swept live view checkpoint segments [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", purged=").$(result.getPurgedSegmentCount())
                        .$(", purgedBytes=").$(result.getPurgedBytes())
                        .$(", failed=").$(result.getFailedSegmentCount())
                        .$(", orphans=").$(result.getRemovedOrphanCount())
                        .$(", failedOrphans=").$(result.getFailedOrphanCount())
                        .$(", retirableEntries=").$(result.getRetirableEntryCount()).I$();
            }
        } catch (Throwable t) {
            LOG.error().$("could not sweep live view checkpoint segments [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
        }
    }

    /**
     * Opens the repair capture one localized, finitely converging rebuild publishes
     * its range splice through, and fills the session's boundary schedule with the
     * logical boundaries in {@code [C, H)} that rebuild has to re-version.
     * <p>
     * The capture pins the generation it reads that list from, so it must be opened
     * before anything else touches the timeline and held until publication. Nothing
     * it writes is reachable until {@link #publishCheckpointTimelineRepair} commits
     * the superblock, so abandoning it costs one temporary data segment.
     * <p>
     * That temporary segment is also the reason the repair opens a durable
     * {@link LiveViewCheckpointRepairState} descriptor here: no metadata names the
     * segment until the splice publishes, so the descriptor is the only record that
     * this repair owns it. The descriptor carries the plan's bounds and pinned
     * snapshot alongside, and the replay and the publication stamp their progress
     * into it, which is what lets startup discard a crashed candidate and replan
     * instead of guessing at the files it left behind.
     *
     * @return the open capture, or null when this repair cannot splice - which is
     * not a failure of the repair, only of its ability to keep the timeline. The
     * caller then retires the timeline as an unlocalized repair does.
     */
    private @Nullable LiveViewCheckpointTimelineStoreWriter.RepairCapture beginCheckpointTimelineRepair(
            LiveViewInstance instance,
            LiveViewCheckpointRepairPlan plan,
            LiveViewCheckpointRepairSession session
    ) {
        final ObjList<LiveViewCheckpointTimelineEntry> repairBoundaries = session.getBoundaries();
        final LiveViewCheckpointRepairState repairState = session.getDescriptor();
        repairBoundaries.clear();
        if (checkpointTimelineStoreWriter == null) {
            checkpointTimelineStoreWriter = new LiveViewCheckpointTimelineStoreWriter(engine.getConfiguration());
            checkpointTimelineStoreWriter.setTestFailureStage(checkpointTimelineTestFailureStage);
        }
        LiveViewCheckpointTimelineStoreWriter.RepairCapture capture = null;
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            // Null for a replay that reconstructs every live key, and Q for one that
            // describes only the keys the replacement re-emits - which is what lets a
            // ROWS repair splice at all. The gate in o3HeadMissReplay has already
            // refused a repair carrying neither.
            capture = checkpointTimelineStoreWriter.beginRepair(
                    checkpointsDir,
                    plan.isReplayStateKeyComplete() ? null : plan.getOutputKeyDomain(),
                    instance.getMemoryTracker()
            );
            // C, not R: a root in [R, C) keeps its state - nothing it holds
            // changed - and its output is re-emitted identically, so the splice
            // reuses it. Only [C, H) receives new payload versions.
            capture.collectBoundaries(plan.getRetireLowTs(), plan.getHighTsExclusive(), repairBoundaries);
            // The repair is named after the snapshot it pinned, so a repair that is
            // repeated against the same E - a deferred replacement the next turn
            // re-materialises - rewrites its own descriptor rather than leaving a
            // second one behind.
            repairState.begin(
                    checkpointsDir,
                    plan.getPinnedSeqTxn(),
                    instance.getLiveViewToken().getTableId(),
                    0,
                    capture.getGeneration(),
                    plan.getPinnedSeqTxn(),
                    plan.getTriggerSeqTxn(),
                    plan.getRetireLowTs(),
                    plan.getReplayLowTs(),
                    plan.getOutputLowTs(),
                    plan.getHighTsExclusive(),
                    plan.getHighBoundTag()
            );
            repairState.addOwnedSegmentId(capture.getDataSegmentId());
            repairState.recordProgress(
                    Numbers.LONG_NULL,
                    repairBoundaries.size() > 0 ? repairBoundaries.getQuick(0).checkpointId : Numbers.LONG_NULL
            );
            // Armed: the publication mirrors every stage it records from here on.
            repairPublication.of(repairState);
            return capture;
        } catch (Throwable t) {
            // Most often "no valid generation": a view whose timeline was retired by
            // an earlier repair and not yet re-sealed has nothing to splice into.
            LOG.info().$("live view checkpoint timeline repair capture unavailable, retiring instead [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", correctionTs=").$(plan.getRetireLowTs())
                    .$(", highTsExclusive=").$(plan.getHighTsExclusive())
                    .$(", error=").$(t).I$();
            // A failed unlink logs its own path and leaves the descriptor to the next
            // reconciliation, which discards it as a crashed candidate - correct, since
            // by then it describes nothing this process still owns.
            session.discardDescriptor();
            Misc.free(capture);
            repairBoundaries.clear();
            return null;
        }
    }

    /**
     * Opens the session one localized repair carries across however many refresh
     * turns it takes: the overlay it puts the published window state into, the
     * durable descriptor that owns its staged files, the boundary schedule its
     * replay segments on, and a private copy of the bounds it derived. A repair
     * that never yields disposes of it on the way out; one that yields leaves it
     * on the instance for its next turn.
     * <p>
     * The session also names the compiled factory it is replaying through, so a later
     * turn can prove the runtime it is about to continue in is still the one the
     * capture came out of. See {@link #resumeSuspendedRepair}.
     */
    private LiveViewCheckpointRepairSession openRepairSession(
            LiveViewCheckpointRepairPlan plan,
            WindowRecordCursorFactory windowFactory
    ) {
        final LiveViewCheckpointRepairSession session =
                new LiveViewCheckpointRepairSession(engine.getConfiguration(), this, windowFactory);
        session.of(plan);
        return session;
    }

    /**
     * Ends a repair: retires its durable descriptor, releases whatever the session
     * still holds, and unblocks refresh for the view. Called from the executor's
     * exit path, so it covers a repair that published, one that abandoned its
     * candidate, and one that unwound out of a turn - by then none of them owns a
     * file a startup sweep would have to discard. A no-op for the unlocalized
     * rebuild, which opens no session.
     */
    private void endRepairSession(LiveViewInstance instance, @Nullable LiveViewCheckpointRepairSession session) {
        if (session == null) {
            return;
        }
        if (instance.getSuspendedRepair() == session) {
            instance.setSuspendedRepair(null);
        }
        Misc.free(session);
        if (session.isWindowStateRestoreFailed()) {
            // The abandoned repair could not put its overlay back, so the compiled factory holds
            // neither the pre-repair state nor a settled one. Mark the runtime dirty so
            // handleRefreshFailure rebuilds it instead of letting the next drain continue over
            // half-restored accumulators. Mirrors settleRepairRuntime's handling on the
            // settled path.
            markWindowStateDirty(instance);
        }
    }

    /**
     * Whether the localized replay has spent this refresh turn. Two budgets, read
     * per turn: the base rows one turn of a repair may replay
     * ({@code cairo.live.view.checkpoint.repair.replay.max.rows}, {@code <= 0}
     * disables it), and the wall-clock ceiling every refresh turn runs under.
     * <p>
     * The duration budget only ends a turn that made progress. A turn already over
     * the wall-clock bound on entry - the drain that triggered the repair spent it
     * - would otherwise yield having replayed nothing, and a repair whose every
     * turn arrived late would never converge.
     *
     * @param replayedThisTurn rows this turn has folded into the window state
     */
    private boolean isRepairReplayBudgetSpent(long replayedThisTurn) {
        final long maxRows = engine.getConfiguration().getLiveViewCheckpointRepairReplayMaxRows();
        if (maxRows > 0 && replayedThisTurn >= maxRows) {
            return true;
        }
        return replayedThisTurn > 0
                && engine.getConfiguration().getMicrosecondClock().getTicks() - turnStartUs
                >= engine.getConfiguration().getLiveViewRefreshTurnMaxDurationMicros();
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
                // Decompose once, here, and hand the same plan to everything below: the
                // anchor build, the repair planner and every refresh cycle read the
                // window factory, the two projections and the base scan off it rather
                // than re-walking the tree and risking a different answer. CREATE already
                // accepted this shape, so a reject here means the recompile after a base
                // DDL produced a shape the refresh path cannot drive - which must fail
                // loudly rather than run on a mismatched chain.
                final LiveViewCompiledPlan plan = LiveViewCompiledPlan.of(factory, 0);
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
                ensureAnchorFunction(instance, plan);
                // Which plans bound this view's repair follows from the factory alone, so
                // it settles here rather than at the first out-of-order row. That is what
                // lets live_views() report the answer for a view no late row has reached
                // yet, which is the one whose latency cliff is still invisible.
                instance.setCheckpointRepairDependencyPlans(
                        repairDependencyPlans(instance, plan.getWindowFactory())
                );
                instance.setCompiledFactory(factory, plan);
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
    private void ensureAnchorFunction(LiveViewInstance instance, LiveViewCompiledPlan plan) throws SqlException {
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
            // Resolve against the window's input metadata. That is what the window
            // functions and their PARTITION BY keys resolve against, and it is the shape
            // of the records reaching the anchor dispatch at runtime - which sits above
            // any alias or pre-window projection, precisely so a window partitioned by
            // `sym AS s` finds `s` here. With no projection in the tree it is the leaf
            // scan's metadata, which is what WalSegmentRecordCursor emits directly.
            RecordMetadata projectedMeta = plan.getWindowInputMetadata();
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
            WindowRecordCursorFactory wf = plan.getWindowFactory();
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
                    LiveViewCheckpointFunctionCompiler.anchorPlan(
                            spec,
                            anchorNode,
                            projectedMeta,
                            wf.getWindowFunctions(),
                            anchoredFunctions
                    ),
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
     * Reports which dependency plans cover the compiled factory's window functions, as
     * the {@code LiveViewInstance.REPAIR_PLAN_*} bit mask {@code live_views()} surfaces.
     * <p>
     * {@link #planO3Repair} asks the same question of the same three plans every time a
     * late row arrives, and this answers it once, when the factory compiles. The two
     * agree by construction - both read the plans off the cached factory and the anchor
     * window built beside it - but they are not the same statement. This one describes
     * the view's SQL: which bounds a localized repair would union. That one decides a
     * particular repair, and can still deny it on grounds the SQL does not carry, such
     * as a ROWS plan over a base that deduplicates.
     * <p>
     * The mask is {@code REPAIR_PLAN_NONE} whenever one window function sits outside the
     * union, because a repair the plans cover only in part is a repair declined outright:
     * the replacement it publishes is timestamp-global, so it re-emits the uncovered
     * function's output from a replay that cannot reconstruct it. Reporting the plans
     * that do exist would name bounds nothing takes.
     */
    private static int repairDependencyPlans(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        final boolean hasAnchorPlan = anchorWindow != null && anchorWindow.getCheckpointAnchorPlan() != null;
        final boolean hasRangePlan = windowFactory.getCheckpointRangePlan() != null;
        final boolean hasRowsPlan = windowFactory.getCheckpointRowsPlan() != null;
        if (!LiveViewCheckpointFunctionCompiler.isDependencyComplete(
                windowFactory.getWindowFunctions(),
                hasRangePlan,
                hasRowsPlan,
                hasAnchorPlan
        )) {
            return LiveViewInstance.REPAIR_PLAN_NONE;
        }
        int plans = LiveViewInstance.REPAIR_PLAN_NONE;
        if (hasRangePlan) {
            plans |= LiveViewInstance.REPAIR_PLAN_RANGE;
        }
        if (hasRowsPlan) {
            plans |= LiveViewInstance.REPAIR_PLAN_ROWS;
        }
        if (hasAnchorPlan) {
            plans |= LiveViewInstance.REPAIR_PLAN_ANCHOR;
        }
        return plans;
    }

    /**
     * Walks the compiled SELECT factory chain down to the leaf
     * {@code PageFrameRecordCursorFactory} and returns its projected metadata.
     * That metadata matches the records {@link WalSegmentRecordCursor} emits at
     * runtime, so an anchor {@code Function} compiled against it will produce
     * correct results when invoked on the LV's source rows.
     */
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
     * Clears the prefix-preservation repair marker once a repair's post-replay
     * seal has re-anchored the timeline head, so a later restart restores
     * normally rather than rebuilding from the applied base. Best effort: a
     * lingering marker only forces one extra rebuild, which a superblock
     * generation past the recorded base generation makes stale anyway.
     */
    private void clearCheckpointRepairMarker(LiveViewInstance instance) {
        path.of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
        LiveViewCheckpointRepairMarker.clear(engine.getConfiguration().getFilesFacade(), path);
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
     * Callers invoke this BEFORE any destructive replay work (checkpoint
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
            WalWriter walWriter
    ) throws SqlException {
        long metadataVersion = walWriter.getMetadata().getMetadataVersion();
        RecordToRowCopier copier = instance.getRecordToRowCopier();
        if (copier == null || instance.getRecordRowCopierMetadataVersion() != metadataVersion) {
            // The view's own schema, which is the output projection's when the SELECT
            // wraps a window function in an expression and the window factory's when it
            // does not. Reading the window factory's here instead would generate a copier
            // for the wrong column count and mis-write every row.
            final RecordMetadata outMetadata = instance.getCompiledPlan().getOutputMetadata();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                columnFilter.of(outMetadata.getColumnCount());
                copier = RecordToRowCopierUtils.generateCopier(
                        compiler.getAsm(),
                        outMetadata,
                        walWriter.getMetadata(),
                        columnFilter,
                        engine.getConfiguration()
                );
                instance.setRecordToRowCopier(copier, metadataVersion);
            }
        }
        return copier;
    }

    /**
     * The decomposed plan for {@code instance}, compiling the view's SELECT first when no
     * factory is cached. Every refresh path reads the window factory, the projections, the
     * filter and the base scan off this rather than re-walking the factory tree.
     */
    private LiveViewCompiledPlan getPlan(LiveViewInstance instance) throws SqlException {
        ensureCompiledFactory(instance);
        return instance.getCompiledPlan();
    }

    private WindowRecordCursorFactory getWindowFactory(LiveViewInstance instance) throws SqlException {
        return getPlan(instance).getWindowFactory();
    }

    /**
     * Walks the sequencer log forward and processes each DATA commit through the
     * compiled window cursor. For each output row, writes to both the LV's WAL (durable
     * tier) and the in-memory tier (read cache). Commits the WAL writer once at the end
     * of the cycle; advances {@code lastProcessedSeqTxn} / {@code lvConsumedSeqTxn} /
     * {@code appliedWatermark} on the instance and rewrites {@code _lv.s}.
     */
    private void incrementalRefresh(LiveViewInstance instance, long fromSeqTxn, long toSeqTxn, boolean leadMode) throws SqlException {
        final LiveViewCompiledPlan compiledPlan = getPlan(instance);
        WindowRecordCursorFactory windowFactory = compiledPlan.getWindowFactory();
        final Function filter = compiledPlan.getFilter();
        TableToken baseToken = instance.getDefinition().getBaseTableToken();
        RecordMetadata baseMetadata = compiledPlan.getBaseScanMetadata();
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
        RecordMetadata outMetadata = compiledPlan.getOutputMetadata();
        int cursorTimestampIndex = outMetadata.getTimestampIndex();
        if (cursorTimestampIndex < 0) {
            throw CairoException.nonCritical()
                    .put("live view requires a designated timestamp [view=")
                    .put(instance.getDefinition().getViewName()).put(']');
        }
        boolean populateTier = ensureStagingAndTier(instance, outMetadata, cursorTimestampIndex);
        if (populateTier) {
            // Seed an empty published slot from the LV table before the drain can
            // publish into it; see stageInMemoryTierWhenEmpty for why an empty slot
            // cannot seed its own seam. Runs here, where the tier and the staging
            // buffer exist but the drain has not staged a row yet.
            stageInMemoryTierWhenEmpty(instance);
        }

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
            // base WAL forward on restart. The drain lands its result in drainResult,
            // which finishLeadRefresh then publishes as the un-flushed lead.
            drainBaseWal(
                    instance, windowFactory, baseToken, baseMetadata, baseTimestampIndex,
                    cursorTimestampIndex, viewLowerBoundTimestamp, Long.MAX_VALUE, filter, fromSeqTxn, toSeqTxn,
                    null, null, populateTier, latestSeenTsSnapshot
            );
            finishLeadRefresh(instance, windowFactory, baseToken, populateTier);
            return;
        }

        try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
            RecordToRowCopier copier = ensureCopier(instance, walWriter);
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
            if (drainResult.appendedRows > 0) {
                // The LV WAL block carries advanceTo as maxBaseSeqTxnInBlock. The
                // inline apply below makes the rows durable in the LV's on-disk
                // table; only then do we advance lvConsumedSeqTxn so base WAL
                // retention releases.
                fencedLiveViewCommit(instance, () -> walWriter.commitLiveView(drainResult.advanceTo));
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
            // The drain rolled back exactly the commits it walked, so its change
            // ceiling is this repair's: advanceTo is the offending seqTxn, the top of
            // the range the walk covered.
            o3Replay(instance, windowFactory, o3LateRowTs, drainResult.o3ChangeMaxTs, drainResult.o3ChangeInsertOnly, baseToken, o3SeqTxn);
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
            // forward-scan recovery of the consumed floor from the live-view WAL.
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
                // Head-checkpoint write hook. Ordered after the apply's _txn
                // advance and the lvConsumedSeqTxn publish so the root on disk
                // reflects state that is also durably committed in the LV's own
                // table. A failure here does not invalidate the view (the timeline
                // is derived state): the previously published generation stays
                // authoritative and the next eligible cycle retries.
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

        final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
        final Function filter = compiledPlan.getFilter();
        final PageFrameRecordCursorFactory pageFrameFactory = compiledPlan.getPageFrameFactory();
        final RecordMetadata outMetadata = compiledPlan.getOutputMetadata();
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
            // metadata) but never the pre-dedup data columns. The reader joins the
            // resource list so the walk unmaps the last segment's event file when it
            // ends - see the note on walEventReader.
            long batchMinTs = Numbers.LONG_NULL;
            try (
                    TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn);
                    WalEventReader eventReader = walEventReader
            ) {
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
                    final WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, eventReader, segmentTxn, txn);
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
            if (populateTier) {
                // Same empty-slot seed the lead path runs, for the same reason: this
                // path's publish is a disk subset, and an empty slot would still cut
                // the seam at its own staging minimum. See stageInMemoryTierWhenEmpty.
                stageInMemoryTierWhenEmpty(instance);
            }
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
                // No change ceiling from this walk: it skips past the structural and
                // non-DATA entries that would deny one rather than aborting on them
                // (the applied-reader rebuild is going to read everything anyway), so
                // the maximum it could report would not be an upper bound on what the
                // range changed. The rebuild reads to the end of the base table, as it
                // did before the bound existed.
                o3Replay(instance, windowFactory, batchMinTs, Numbers.LONG_NULL, false, baseToken, effectiveSeqTxn);
                // Coupled invariant: keep refreshedUpTo == lastProcessed so a later
                // ALTER DEDUP DISABLE flip back to the lead path resumes cleanly with
                // no stale un-flushed lead.
                instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
                return;
            }

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
                    final RecordToRowCopier copier = ensureCopier(instance, walWriter);
                    try (RecordCursor pageCursor = emptyForwardRange
                            ? EmptyTableRecordCursor.INSTANCE
                            : pageFrameFactory.getCursorFromTimestamp(executionContext, scanLowTs)) {
                        RecordCursor source = pageCursor;
                        if (filter != null) {
                            filteringCursor.of(source, filter, executionContext);
                            source = filteringCursor;
                        }
                        // Rebuild the compiled nodes between the base scan and the window -
                        // an alias, a column drop, a pre-window scalar - so the window
                        // functions see the shape they were compiled against. Above the
                        // filter, which resolves against the base scan, and below the anchor
                        // dispatch, which resolves against the window's input.
                        source = compiledPlan.wrapWindowInput(source, executionContext);
                        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
                        if (anchorWindow != null) {
                            anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                            source = anchorDispatchingCursor;
                        }
                        try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                            // Rows leave the window in the window factory's shape; the output
                            // projection turns them into the view's own schema, which is what
                            // the copier writes and the tier stores. Drive the projected
                            // cursor rather than the window one - it is what advances the
                            // projection's per-row memoization before the record is read.
                            final RecordCursor outCursor = compiledPlan.wrapWindowOutput(windowCursor, executionContext);
                            final Record outRecord = outCursor.getRecord();
                            while (outCursor.hasNext()) {
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
                                        // windowMapAuthoritative = true: this node is the sole writer of its
                                        // own LV table and resets the window map on every flush (onFlush/onO3),
                                        // so a live window entry is authoritative and intern can skip the
                                        // committed keyOf.
                                        for (int si = 0, sn = stagingSymbolColumnIndexes.size(); si < sn; si++) {
                                            final int c = stagingSymbolColumnIndexes.getQuick(si);
                                            final int symId = symbolCache.intern(c, outRecord.getSymA(c), committedSymbolReader.getSymbolMapReader(c), true);
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
                            fencedLiveViewCommit(instance, () -> walWriter.commitLiveView(effectiveSeqTxn));
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
        // Upper bound on what this pass's commits changed, accumulated across every
        // entry the walk visits rather than only the offending one: an O3 hand-off
        // rolls the whole pass back and re-materialises all of it, so the repair's
        // change set is the pass, not the trigger. That breadth is also what keeps the
        // repair's runtime-frontier test honest. This pass fed its in-order commits
        // through the window cursor before the hand-off rolled the WAL writes back and
        // rewound latestSeenTs, so the window state stands ABOVE that watermark; a
        // ceiling taken from the trigger alone could sit under the watermark and let a
        // repair claim it converged below state the pass had already advanced.
        // Counting those commits puts the ceiling - and therefore H - above them, which
        // is what makes the frontier comparison reject the case.
        // changeMaxTsKnown goes false on an entry whose reach the commit metadata does
        // not describe - a compacted or structural sequencer entry, or a non-DATA
        // commit - after which no timestamp arithmetic bounds the change and the repair
        // must read to the end of the base table.
        long changeMaxTs = Numbers.LONG_NULL;
        boolean changeMaxTsKnown = true;
        // Whether every entry the walk visits only ADDED rows. Cleared by the same three
        // entries that deny a change ceiling - a compacted / structural entry, a non-DATA
        // commit, a REPLACE_RANGE whose delete band reaches into the view - because each
        // of them can retire a base row, and a ROWS repair discovers its affected key
        // domain by looking for those rows. See DrainResult.o3ChangeInsertOnly.
        boolean changeInsertOnly = true;
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
                TableReader committedSymbolReader = internSymbols ? engine.getReader(instance.getLiveViewToken()) : null;
                // Closed with the drain so the turn does not park the last segment's
                // event file mapped between turns - see the note on walEventReader.
                WalEventReader eventReader = walEventReader;
                // Same reason, over a strictly larger mapping set: the frame cursor holds the
                // segment's _meta, its nested _event and every projected .d/.i. On Windows those
                // mappings are open handles on the segment directory, so WalPurgeJob's rmdir
                // fails ACCESS_DENIED and an otherwise idle view pins the segment indefinitely.
                // releaseSegment() rather than close(): it drops the mappings but keeps the
                // per-worker scratch, whose retained capacity is what stops a steady sub-cap load
                // reallocating every turn.
                QuietCloseable segmentRelease = walFrameCursor::releaseSegment
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
                // Snapshot the prior value: advanceTo is claimed before this commit's type is
                // known, and the mat-view TRUNCATE arm below has to give it back.
                final long advanceToBeforeCommit = advanceTo;
                advanceTo = txn;
                turnCommitsProcessed++;
                int walId = txnCursor.getWalId();
                int segmentId = txnCursor.getSegmentId();
                int segmentTxn = txnCursor.getSegmentTxn();

                if (walId <= 0) {
                    // Compacted seq entry / non-WAL: skip past, no data to consume.
                    // Nothing here says what it touched, so a repair over this pass
                    // cannot claim a convergence boundary, nor that nothing was removed.
                    changeMaxTsKnown = false;
                    changeInsertOnly = false;
                    continue;
                }

                walPath.of(engine.getConfiguration().getDbRoot())
                        .concat(baseToken)
                        .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, eventReader, segmentTxn, txn);

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
                        //
                        // Give advanceTo back. It was claimed above before this commit's type was
                        // known, so leaving it would commit the TRUNCATE as consumed and the next
                        // drain would resume ABOVE it - the exact crossing the paragraph above
                        // says never happens, and the rows it would then re-emit are the ones this
                        // arm exists to refuse.
                        advanceTo = advanceToBeforeCommit;
                        break;
                    }
                    // Non-data commit (schema change / DROP PARTITION / TRUNCATE / TTL) —
                    // walked past, no rewrite to the in-memory tier or LV WAL. Schema
                    // changes that touch referenced columns invalidate via
                    // ApplyWal2TableJob. A removal can retire base rows anywhere in the
                    // table, so nothing in the inserted-row timestamps bounds what a
                    // repair over this pass has to re-evaluate, and the pass is not
                    // insert-only either.
                    changeMaxTsKnown = false;
                    changeInsertOnly = false;
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
                    // The delete band reaches into the view, so this commit can have
                    // retired a base row the view holds.
                    changeInsertOnly = false;
                }
                // Raise the pass's change ceiling before the O3 branch: the offending
                // commit is part of the change set the hand-off re-materialises, so it
                // has to contribute even though the walk stops on it.
                changeMaxTs = Math.max(changeMaxTs, effectiveCommitMaxTs(dataInfo, deleteLo));
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
                final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
                source = compiledPlan.wrapWindowInput(source, executionContext);
                LiveViewWindow anchorWindow = instance.getAnchorWindow();
                if (anchorWindow != null) {
                    // Anchor dispatch sits between the window's input projection (or the
                    // filter, or the lower-bound cursor) and the window cursor so window
                    // functions see resetPartition before pass1 evaluates the row.
                    anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                    source = anchorDispatchingCursor;
                }

                RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext);
                try {
                    RecordCursor outCursor = compiledPlan.wrapWindowOutput(windowCursor, executionContext);
                    Record outRecord = outCursor.getRecord();
                    while (outCursor.hasNext()) {
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
                                // windowMapAuthoritative = true: this node is the sole writer of its own
                                // LV table and its reset-on-flush window map lets intern skip the committed
                                // keyOf for a live (not-yet-committed) window entry.
                                for (int si = 0, sn = stagingSymbolColumnIndexes.size(); si < sn; si++) {
                                    final int c = stagingSymbolColumnIndexes.getQuick(si);
                                    final int symId = symbolCache.intern(c, outRecord.getSymA(c), committedSymbolReader.getSymbolMapReader(c), true);
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
        drainResult.o3ChangeInsertOnly = changeInsertOnly;
        drainResult.o3ChangeMaxTs = changeMaxTsKnown ? changeMaxTs : Numbers.LONG_NULL;
        drainResult.o3Detected = o3Detected;
        drainResult.o3LateRowTs = o3LateRowTs;
        drainResult.o3SeqTxn = o3SeqTxn;
        drainResult.stagingMaxTs = stagingMaxTs;
        drainResult.stagingMinTs = stagingMinTs;
    }

    /**
     * Highest designated timestamp one base DATA commit can have changed, or
     * {@link Numbers#LONG_NULL} when it changed nothing. The mirror of
     * {@link #effectiveReplaceRangeDeleteLo}: that method reports how far down a
     * commit reaches, this one how far up, and a localized repair needs both to
     * enclose the change set in a finite interval.
     * <p>
     * Two sources. The commit's own rows top out at its recorded maximum timestamp,
     * which is only meaningful when it carries rows at all - a pure-delete
     * REPLACE_RANGE carries none and its recorded extremes are the empty-range
     * sentinels. And a REPLACE_RANGE deletes {@code [rangeLo, rangeHi)} beyond its
     * inserted rows, whose topmost removed row sits at {@code rangeHi - 1}; a
     * non-null {@code deleteLo} already proves that range non-empty, so the
     * decrement cannot wrap.
     * <p>
     * The result is not clamped to the view's boundary the way the delete low is.
     * The clamp down exists because a view holds no row below its {@code START
     * FROM}, so a deletion there removes nothing of the view's; there is no
     * equivalent ceiling above, and clamping would understate how far the change
     * reaches.
     *
     * @param dataInfo the commit's WAL-E metadata
     * @param deleteLo this commit's {@link #effectiveReplaceRangeDeleteLo}, so the
     *                 caller's single evaluation serves both bounds
     */
    private static long effectiveCommitMaxTs(WalEventCursor.DataInfo dataInfo, long deleteLo) {
        long maxTs = Numbers.LONG_NULL;
        if (dataInfo.getEndRowID() > dataInfo.getStartRowID()) {
            maxTs = dataInfo.getMaxTimestamp();
        }
        if (deleteLo != Numbers.LONG_NULL) {
            maxTs = Math.max(maxTs, dataInfo.getReplaceRangeTsHi() - 1);
        }
        return maxTs;
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

    // Under symmetric local refresh the live-view table is
    // node-local derived data: every node -- primary AND replica -- refreshes and flushes its own LV
    // table locally, and LV WAL is never uploaded or downloaded. So the read-only fence this method once
    // held (refuse an LV mint on a read-only node, to stop a demoting primary externalizing a local-only
    // LV seqTxn the closing uploader never ships) is obsolete: a node-local LV mint has no upstream to
    // lose to, and a read-only replica legitimately originates its own LV WAL. The read lock + mint
    // observer are retained (uncontended, harmless) so the seam stays a single choke point for every LV
    // commit family -- flushLead, the in-WAL-order and applied-base drains, the o3Replay REPLACE_RANGE
    // corrections, and the seed sweep -- pending the Phase 5 cleanup that folds it away entirely.
    private void fencedLiveViewCommit(LiveViewInstance instance, Runnable commit) {
        final Lock lock = engine.getRoleSwitchReadLock();
        lock.lock();
        try {
            engine.fireRoleSwitchMintObserver();
            commit.run();
            // Rows are durable now, so the accumulators no longer lead durable state;
            // a later failure must not trigger a rebuild over the committed block.
            // This is also the single point that resolves a carried-over wipe: the
            // runtime that produced the committed rows is by definition consistent
            // with them.
            windowStateDirty = false;
            instance.setWindowStateDirty(false);
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
            o3Replay(instance, windowFactory, drainResult.o3LateRowTs, drainResult.o3ChangeMaxTs, drainResult.o3ChangeInsertOnly, baseToken, drainResult.o3SeqTxn);
            instance.setRefreshedUpToSeqTxn(instance.getLastProcessedSeqTxn());
            return;
        }

        if (advanceTo > instance.getRefreshedUpToSeqTxn()) {
            if (appendedRows > 0 && populateTier) {
                if (instance.isTierStale()) {
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

        final RecordMetadata outMetadata = instance.getCompiledPlan().getOutputMetadata();
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
            RecordToRowCopier copier = ensureCopier(instance, walWriter);
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
            fencedLiveViewCommit(instance, () -> walWriter.commitLiveView(advanceTo));
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
        } finally {
            // The lead is now on disk; reset the in-RAM lead count unconditionally. This
            // sits in a finally because persistState writes the very _lv.s file whose
            // failure routed us here, so it commonly throws too - and that throw used to
            // skip the reset, leaving rows that are ALREADY durable still counted as
            // un-flushed lead. The next flush then materialised them a second time and
            // the LV table durably held duplicate rows, which the record path's seam hid
            // (seamTs sat at or below them, so disk served nothing) while count(*) and
            // every page-frame read exposed it.
            // See LiveViewSmokeTest.testFlushPersistFailureDoesNotReflushDurableLead.
            instance.setLeadRowCount(0);
        }
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
     * Head invalidation on out-of-order arrival for a view the replay cannot
     * repair. The current cycle still feeds the offending batch through the
     * in-WAL-order pipeline (so the live output for the affected partitions is
     * wrong for this batch); the value of this helper is narrower: the head no
     * longer reflects the rows the LV will eventually need to replay, so it must
     * be retired now to keep restart recovery sound. The view falls through to a
     * from-base rebuild on the next restart, which restarts the window state from
     * {@code viewLowerBoundTimestamp}.
     * <p>
     * Clearing the in-memory head metadata to {@code LONG_NULL} stops the
     * catalogue from advertising a boundary the durable output no longer matches.
     */
    private void invalidateHeadOnO3(LiveViewInstance instance, long seqTxn, long txnMinTs, long latestSeenTs) {
        LOG.critical().$("live view out-of-order base commit; invalidating head checkpoint [view=")
                .$(instance.getDefinition().getViewName())
                .$(", baseSeqTxn=").$(seqTxn)
                .$(", txnMinTs=").$(txnMinTs)
                .$(", latestSeenTs=").$(latestSeenTs)
                .$(", headLvSeqTxn=").$(instance.getHeadCheckpointLvSeqTxn())
                .I$();
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
    }

    /**
     * Retires the checkpoint state of a view whose seal keeps failing, and arms a
     * backoff before the next attempt.
     * <p>
     * Both {@code WalPurgeJob} floor arms this view publishes -
     * {@code getHeadCheckpointBaseSeqTxn()} and
     * {@code getCheckpointTimelineWalPurgeFloor()} - are held so a restart can replay
     * the base WAL above the newest durable root. A seal that has failed
     * {@link #MAX_CONSECUTIVE_SEAL_FAILURES} times running will not produce that root,
     * so the arms pin the base table's WAL indefinitely while it keeps ingesting, for
     * a recovery that is never going to happen. Releasing them costs the fast restart
     * path only: with no timeline the view rebuilds from the applied base table, which
     * reads no raw base WAL at all.
     * <p>
     * Retire rather than invalidate. The timeline is derived state, so losing it
     * leaves the view serving correct results; invalidation is terminal and needs DROP
     * plus CREATE. The two calls must stay in this order - the timeline arm keeps the
     * floor pinned until {@code clearCheckpointTimelineOwnership()} runs after the
     * on-disk retire, so no purge can outrun a root a restart could still restore.
     */
    private void retireCheckpointStateAfterRepeatedSealFailure(LiveViewInstance instance, long nowUs, int streak) {
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
        retireCheckpointTimeline(instance);
        // Doubling per failure past the budget, capped. The shift count is bounded
        // before it is taken: a streak on a long-lived view grows without limit and
        // would otherwise shift past 63 and wrap to a cooldown in the past.
        final int doublings = Math.min(streak - MAX_CONSECUTIVE_SEAL_FAILURES, SEAL_COOLDOWN_MAX_DOUBLINGS);
        final long backoffUs = Math.min(SEAL_COOLDOWN_BASE_MICROS << doublings, SEAL_COOLDOWN_MAX_MICROS);
        instance.setSealCooldownUntilUs(nowUs + backoffUs);
        LOG.critical().$("live view checkpoint seal keeps failing, retiring the timeline and backing off [view=")
                .$(instance.getDefinition().getViewName())
                .$(", consecutiveFailures=").$(streak)
                .$(", backoffMicros=").$(backoffUs).I$();
    }

    /**
     * Retires the checkpoint state an out-of-order change has unsealed: the whole
     * versioned timeline plus the head metadata trio the catalogue and the
     * post-replay cadence read.
     * <p>
     * Clearing the head puts the post-replay seal on its first-checkpoint cadence
     * path, so an O3 repair always advances the boundary rather than leaving it
     * parked at the stale {@code maxTs} it found.
     * <p>
     * {@code retireTimeline} decides whether the timeline goes with it. A repair
     * that publishes its own range splice passes {@code false}: the splice
     * re-versions the roots in {@code [C, H)} and keeps the prefix and converged
     * suffix, which is the whole point of the timeline, so retiring them here
     * would throw away exactly what the splice is about to correct. Every other
     * repair passes {@code true} - see {@link #retireCheckpointTimeline}.
     */
    private void retireCheckpointStateOnO3(LiveViewInstance instance, boolean retireTimeline) {
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
        if (retireTimeline) {
            retireCheckpointTimeline(instance);
        }
    }

    /**
     * Retires the whole checkpoint timeline: its superblock, metadata segments,
     * data segments, and repair descriptors.
     * <p>
     * The out-of-order replay paths are the main callers. Invariant 2 requires
     * every current root in a generation to be correct for one pinned base
     * snapshot, and an O3 replay rewrites live-view output below roots that were
     * sealed before it - so those roots no longer describe the materialization
     * and must not survive into the next generation. Retiring is the coarse form
     * of that guarantee, and a post-replay seal then starts a fresh history. The
     * seed sweep calls it too, through
     * {@link #retireSeedCheckpointTimeline(LiveViewInstance)}.
     * <p>
     * The precise form is the range splice
     * ({@link #publishCheckpointTimelineRepair}), which re-versions only the roots
     * in {@code [C, H)} and keeps the prefix and the converged suffix. A repair
     * takes it when it localized <b>and</b> converged at a finite {@code H}: only
     * then is there a suffix whose state provably did not change, and only then
     * does the repair leave the runtime standing where it found it. The splice
     * appends no boundary of its own; the post-replay seal adds one at the
     * runtime frontier when the frontier has run past the newest root the splice
     * kept, so the generation's base coverage never outruns its roots. A repair
     * that replaces through positive
     * infinity - an unlocalized rebuild, or a localized one whose change set has no
     * proven ceiling - has no converged suffix to keep and still retires here.
     * This also catches a splice that failed after its replacement committed: the
     * durable output has moved under every root, so the timeline goes.
     * <p>
     * Failure is logged and swallowed: the replay owns correctness of the durable
     * output, and a timeline left behind is re-reconciled (and re-retired) on the
     * next seal or restart rather than blocking the refresh.
     */
    private void retireCheckpointTimeline(LiveViewInstance instance) {
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            LiveViewCheckpointLifecycle.retireTimeline(
                    engine.getConfiguration(),
                    checkpointsDir,
                    null,
                    true
            );
        } catch (Throwable t) {
            LOG.error().$("could not retire live view checkpoint timeline [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
        }
        instance.clearCheckpointTimelineOwnership();
    }

    /**
     * Prefix-preserving alternative to {@link #retireCheckpointStateOnO3} for an
     * out-of-order repair whose influence reaches the runtime frontier (EOF) or
     * that resumes from a predecessor: rather than retiring the whole timeline
     * and losing every long-term anchor because of one near-head correction, it
     * keeps the roots below {@code floorTs} and drops only the tail the repair is
     * about to rewrite.
     * <p>
     * It always clears the in-memory head so the post-replay seal takes its
     * first-checkpoint path. When a prefix survives below the floor it writes the
     * durable {@link LiveViewCheckpointRepairMarker} - which forces a mid-repair
     * crash restart to rebuild from the applied base rather than trust the
     * truncated head - and truncates the on-disk timeline to that prefix; the
     * caller clears the marker once the post-replay seal re-anchors the head.
     * When no prefix survives (or there is no valid timeline) it retires outright,
     * exactly as before.
     *
     * @return true when the prefix was preserved and a marker is now live; false
     * when the timeline was retired
     */
    private boolean truncateOrRetireTimelineOnO3(LiveViewInstance instance, long floorTs) {
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
        try {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            final long baseGeneration;
            final long definitionTxn;
            final long historyEpoch;
            try (LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(engine.getConfiguration())) {
                superblock.of(path);
                if (!superblock.isValid()) {
                    // Nothing to preserve - no valid timeline. Retire (which also
                    // removes any stale marker) exactly as the old path did.
                    retireCheckpointTimeline(instance);
                    return false;
                }
                baseGeneration = superblock.generation;
                definitionTxn = superblock.definitionTxn;
                historyEpoch = superblock.historyEpoch;
            }
            if (checkpointTimelineStoreWriter == null) {
                checkpointTimelineStoreWriter = new LiveViewCheckpointTimelineStoreWriter(engine.getConfiguration());
                checkpointTimelineStoreWriter.setTestFailureStage(checkpointTimelineTestFailureStage);
            }
            boolean preserved = false;
            final Lock roleLock = engine.getRoleSwitchReadLock();
            roleLock.lock();
            try {
                // Ordered before the truncate: a crash between the marker and the
                // post-replay seal must rebuild from the applied base.
                LiveViewCheckpointRepairMarker.write(
                        engine.getConfiguration(),
                        path,
                        definitionTxn,
                        historyEpoch,
                        baseGeneration,
                        floorTs
                );
                final LiveViewCheckpointTimelineStoreWriter.TruncateResult result =
                        checkpointTimelineStoreWriter.publishTruncate(
                                path,
                                definitionTxn,
                                historyEpoch,
                                floorTs,
                                true
                        );
                preserved = result.isPublished();
                if (preserved) {
                    instance.recordCheckpointTimelineWalPurgeFloor(result.getWalPurgeFloor());
                    instance.recordCheckpointTimelineStats(result.getStats());
                } else {
                    // No prefix survived after all - drop the marker before retiring.
                    LiveViewCheckpointRepairMarker.clear(engine.getConfiguration().getFilesFacade(), path);
                }
            } finally {
                roleLock.unlock();
            }
            if (!preserved) {
                retireCheckpointTimeline(instance);
            }
            return preserved;
        } catch (Throwable t) {
            LOG.error().$("could not preserve live view checkpoint timeline prefix, retiring [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", floorTs=").$(floorTs)
                    .$(", error=").$(t).I$();
            retireCheckpointTimeline(instance);
            return false;
        }
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
     * Reports whether the live view's table holds every block its own WAL has already
     * committed, from disk truth: the sequencer log's last committed seqTxn against the
     * applied seqTxn the LV table records in its {@code _txn}.
     * <p>
     * Deliberately does not consult the {@link SeqTxnTracker}. That tracker is memory-only
     * and both of its txns default to {@code UNINITIALIZED_TXN}, so on a restart path -
     * where nothing has initialised it yet - a tracker comparison answers "fully applied"
     * for a view that has applied nothing, which is the wrong way to be wrong here.
     * <p>
     * Fails closed: any read failure reports {@code false}. The caller clamps a base-WAL
     * purge floor on the answer, so "cannot tell" has to mean "do not release".
     */
    private boolean isLiveViewWalFullyApplied(LiveViewInstance instance) {
        final TableToken token = instance.getLiveViewToken();
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        if (tracker.isInitialised()) {
            // An initialised tracker already holds both numbers - the apply job feeds it
            // writerTxn from the LV writer and seqTxn from the sequencer - so answer from
            // memory. Only a cold tracker needs the disk read below, which matters because
            // a deferred reconcile is re-entered on every base commit until the block
            // lands, and paying a sequencer read lock plus a reader open per commit for an
            // answer already in memory would be pure waste.
            return tracker.getWriterTxn() >= tracker.getSeqTxn();
        }
        try {
            final long committedSeqTxn = engine.getTableSequencerAPI().lastTxn(token);
            final long appliedSeqTxn;
            try (TableReader lvReader = engine.getReader(token)) {
                appliedSeqTxn = lvReader.getSeqTxn();
            }
            if (appliedSeqTxn >= committedSeqTxn) {
                return true;
            }
            // Debug, not info: the caller re-enters per base commit while the block is
            // outstanding, and scanForLaggingViews already reports the same condition.
            LOG.debug().$("live view has committed but unapplied WAL, deferring restart floor reconcile [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", appliedSeqTxn=").$(appliedSeqTxn)
                    .$(", committedSeqTxn=").$(committedSeqTxn).I$();
            return false;
        } catch (Throwable t) {
            LOG.error().$("could not read live view apply state on restart [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
            return false;
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
     * Timestamp span the apply-ahead range {@code (fromSeqTxn, toSeqTxn]} touches,
     * returned through the reusable {@link #applyAheadBounds} pair as
     * {@code [minTs, maxTs]}.
     * <p>
     * The minimum gates an apply-ahead resume: when {@code ApplyWal2TableJob} has
     * raced the base reader past the O3 trigger, a resume from a sealed anchor
     * {@code C} is sound only if {@code C.maxTs} sits strictly below every row those
     * un-examined seqTxns hold (else a back-dated row below {@code C.maxTs} would be
     * dropped). The maximum feeds the other end of the same reasoning: the repair
     * re-materialises this range too, so the convergence boundary {@code H} has to
     * clear it as well as the trigger's own rows.
     * <p>
     * Both are {@link Numbers#LONG_NULL} when the range is <b>not safely
     * resumable</b> and the caller must rebuild from the boundary instead:
     * <ul>
     *     <li>a structural / compacted sequencer entry ({@code walId <= 0}), or a
     *     non-DATA commit (TRUNCATE / DROP PARTITION / UPDATE) is present - a
     *     bounded resume cannot reproduce whatever it changed below the anchor, and
     *     no arithmetic over the inserted timestamps bounds it above either; or</li>
     *     <li>the range holds no DATA commit at all.</li>
     * </ul>
     * The min source mirrors {@link #drainAppliedBase}'s overlap walk exactly (the
     * WAL-E event file, corrected by {@link #effectiveReplaceRangeDeleteLo} so a
     * REPLACE_RANGE delete contributes its clamped range low rather than its
     * inserted-row minimum); it differs only by aborting to {@code LONG_NULL} on
     * the structural / non-DATA commits that walk merely skips - the drain is
     * still going to scan everything, whereas a resume must not. The max comes from
     * {@link #effectiveCommitMaxTs} over the same commits.
     * <p>
     * The same walk fills {@link #applyAheadInsertOnly}, the ahead half of the
     * insert-only proof a localized ROWS repair needs. It is a separate verdict because
     * a range can be perfectly classifiable - every commit DATA, every timestamp
     * readable - and still have deleted rows through a REPLACE_RANGE band.
     */
    private long[] computeApplyAheadBounds(TableToken baseToken, long fromSeqTxn, long toSeqTxn, long viewLowerBoundTimestamp) {
        applyAheadBounds[0] = Numbers.LONG_NULL;
        applyAheadBounds[1] = Numbers.LONG_NULL;
        applyAheadInsertOnly = false;
        long minTs = Numbers.LONG_NULL;
        long maxTs = Numbers.LONG_NULL;
        boolean insertOnly = true;
        try (
                TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn);
                // Every arm out of this walk is a return, and each closes the reader with
                // the cursor - see the note on walEventReader.
                WalEventReader eventReader = walEventReader
        ) {
            while (txnCursor.hasNext()) {
                final long txn = txnCursor.getTxn();
                if (txn > toSeqTxn) {
                    break;
                }
                final int walId = txnCursor.getWalId();
                if (walId <= 0) {
                    // Compacted / structural entry (STRUCTURAL_CHANGE / DROP_TABLE):
                    // a bounded resume cannot see whatever it changed, so refuse it.
                    return applyAheadBounds;
                }
                final int segmentId = txnCursor.getSegmentId();
                final int segmentTxn = txnCursor.getSegmentTxn();
                walPath.of(engine.getConfiguration().getDbRoot())
                        .concat(baseToken)
                        .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                final WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, eventReader, segmentTxn, txn);
                if (!WalTxnType.isDataType(eventCursor.getType())) {
                    // TRUNCATE / DROP PARTITION / UPDATE: a non-DATA change whose
                    // effect a bounded resume cannot reproduce - force the rebuild.
                    return applyAheadBounds;
                }
                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                long txnMinTs = dataInfo.getMinTimestamp();
                final long deleteLo = effectiveReplaceRangeDeleteLo(dataInfo, viewLowerBoundTimestamp);
                if (deleteLo != Numbers.LONG_NULL) {
                    txnMinTs = deleteLo;
                    insertOnly = false;
                }
                if (minTs == Numbers.LONG_NULL || txnMinTs < minTs) {
                    minTs = txnMinTs;
                }
                maxTs = Math.max(maxTs, effectiveCommitMaxTs(dataInfo, deleteLo));
            }
        }
        applyAheadBounds[0] = minTs;
        // A classifiable range with no minimum holds no DATA commit, which the
        // caller reads as unresumable; pin the maximum to the same verdict rather
        // than publishing half a span.
        applyAheadBounds[1] = minTs == Numbers.LONG_NULL ? Numbers.LONG_NULL : maxTs;
        applyAheadInsertOnly = insertOnly;
        return applyAheadBounds;
    }

    /**
     * Out-of-order replay. Called from {@code incrementalRefresh}
     * after detection rolls back the in-WAL-order draft for the offending
     * cycle. Pins one applied base reader, plans the repair against that single
     * snapshot ({@link #planO3Repair}), and hands the plan to one of the two
     * executors: {@link #replayFromAnchor} when a sealed checkpoint sits strictly
     * below the change, {@link #o3HeadMissReplay} otherwise. Either executor reads
     * the base table via the pinned {@code TableReader} in ts-ascending order
     * through the compiled SELECT's filter / anchor / window cursor stack, commits
     * via {@link WalWriter#commitLiveViewWithReplaceRange(long, long, long)},
     * applies inline, and seals a fresh boundary post-replay.
     * <p>
     * Planning and replay share one pinned reader. The executors neither open
     * nor close it: this method owns it for the whole repair, so a plan that
     * rejects a resume can rebuild from the same snapshot its bounds were
     * derived against instead of reopening at a newer one.
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
     * @param changeMaxTs   highest designated timestamp the commits this replay
     *                      re-materialises touched, or {@link Numbers#LONG_NULL}
     *                      when the caller cannot bound it. Only an upper bound on
     *                      the change lets the repair converge below the end of the
     *                      base table; without one it reads the whole tail, which is
     *                      what every caller did before the bound existed
     * @param insertOnly    whether every commit the caller walked only ADDED base rows.
     *                      A ROWS repair discovers which partition keys the change
     *                      touched by reading them back out of the post-change snapshot,
     *                      so a deletion anywhere in the change set denies it that bound;
     *                      a caller that does not track the question passes false
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
            long changeMaxTs,
            boolean insertOnly,
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
            // retire the head so restart cannot restore stale state,
            // accept that the live output for the O3 batch is wrong until
            // a non-O3 cycle naturally advances state.
            invalidateHeadOnO3(instance, advanceTo, lateRowTs, instance.getLatestSeenTs());
            // Retire the timeline on the same terms. This branch feeds the O3
            // batch through the in-WAL-order pipeline and then walks the
            // watermarks over it, so every root this view holds now describes
            // output the replay never corrected. Usually a no-op: a non-capable
            // view seals no root at all. The shape that is not is a view whose
            // functions lost snapshot support across a restart - capability is
            // computed on first use here, after the restore already selected a
            // root.
            retireCheckpointTimeline(instance);
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
            // instance.leadRowCount is 0 on entry to o3Replay: finishLeadRefresh and
            // drainAppliedBase's overlap branch (where an ALTER ... DEDUP ENABLE flip can
            // leave a pre-dedup RAM lead) zero it explicitly, and the coupled-forward and
            // replay-to-applied callers carry no un-flushed lead. The capable path
            // rebuilds the tier as a pure disk subset (leadRowCount 0).
            // This branch rewrote nothing on disk and left the published slot untouched,
            // so a slot that is STILL a current un-flushed lead keeps its stamped
            // leadRowCount as the true lead. Resync instance.leadRowCount to it: leaving
            // it at 0 would reclassify those never-flushed rows as overlap, so size()
            // under-reports, iteration serves them as phantoms, and flushLead's
            // overlapCount skips them entirely.
            //
            // But re-arm ONLY from a slot whose stamped LV-table seqTxn still matches the
            // applied disk seqTxn. A slot whose stamp has fallen behind disk holds rows
            // that are already durable, so the 0 the caller left is correct. Two paths
            // leave such a stale stamp: an emergency flush that set tierStale
            // (isTierStale() would catch it), and a normal flush whose restampSlot
            // 0 -> -1 CAS lost to a reader pin, leaving tierStale FALSE because
            // restampSlotAfterFlush ignores the CAS result -- an isTierStale() guard
            // MISSES that one. Re-arming from either would make finishLeadRefresh trust a
            // stale non-zero leadRowCount and re-flush already-durable rows as on-disk
            // duplicates. The seqTxn-match check below subsumes both (both leave
            // slot.lvSeqTxn() != applied) and needs no reader open: the applied seqTxn is
            // the same coordinate flushLead / publishToInMemoryTier stamp the slot from,
            // and nothing has applied to the LV table since (this branch does not commit).
            //
            // Defensive: CREATE rejects every non-snapshot-capable window shape (each
            // WindowFunction.supportsCheckpointState() folds in the anchor key type check)
            // and o3Replay recomputes capability above, so a freshly-validated view never
            // reaches this branch. It fires only for a view that is non-capable at runtime
            // (e.g. a restored view whose function lost snapshot support); the resync
            // keeps its bookkeeping correct if so.
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

        // Pin one applied base reader for the whole repair, then plan against it.
        // The executors run off this same reader and this same plan: a resume the
        // plan rejects rebuilds from the snapshot its bounds were derived against,
        // rather than reopening at whatever apply has reached by then.
        final TableReader reader = waitForApply(baseToken, advanceTo);
        // True when the localized rebuild spent its turn budget and parked: it owns
        // the pinned reader from here on, and nothing this method does after the
        // repair applies to a repair that has not finished.
        boolean suspended = false;
        try {
            planO3Repair(instance, windowFactory, lateRowTs, changeMaxTs, insertOnly, baseToken, advanceTo, reader);
            LOG.info().$("live view O3 replay [view=").$(viewName)
                    .$(", lateRowTs=").$(lateRowTs)
                    .$(", advanceTo=").$(advanceTo)
                    .$(", pinnedSeqTxn=").$(repairPlan.getPinnedSeqTxn())
                    .$(", correctionTs=").$(repairPlan.getCorrectionTs())
                    .$(", changeMaxTs=").$(repairPlan.getChangeMaxTs())
                    .$(", highTsExclusive=").$(repairPlan.getHighTsExclusive())
                    .$(", resumeFromAnchor=").$(repairPlan.isResumeFromAnchor())
                    // Why this repair reads more than a localized rebuild would, as
                    // live_views().checkpoint_repair_last_denial reports it. Absent for a
                    // repair that read exactly its localized interval.
                    .$(", denial=").$(LiveViewCheckpointRepairPlan.denialReasonName(
                            instance.getCheckpointRepairLastDenialReason()))
                    .$(", anchorCheckpointId=").$(repairPlan.getAnchorCheckpointId())
                    .$(", anchorMaxTs=").$(repairPlan.getAnchorMaxTs())
                    // The two estimates the disposition above was chosen on, so a repair
                    // that took the more expensive-looking route is diagnosable. Both are
                    // LONG_NULL when no anchor competed and nothing needed pricing.
                    .$(", resumeScanRows=").$(repairPlan.getResumeScanRows())
                    .$(", rebuildScanRows=").$(repairPlan.getRebuildScanRows()).I$();
            if (repairPlan.isResumeFromAnchor()) {
                replayFromAnchor(instance, windowFactory, repairPlan, reader);
            } else {
                // Either no logical boundary sits below the change (the whole
                // timeline is above it, the trigger carries no timestamp to search
                // with, the timeline is unreadable, or apply raced ahead over an
                // unclassifiable range), in which case this
                // is the O(view age) rebuild from the view boundary; or one does and
                // the plan priced its resume above the localized rebuild, in which
                // case this reads only [L, H).
                suspended = o3HeadMissReplay(instance, windowFactory, repairPlan, reader, false, null, true);
            }
        } finally {
            if (!suspended) {
                reader.close();
            }
        }
        if (suspended) {
            // Nothing durable moved, so there is no rewritten tier to rebuild from
            // and no watermark to walk. The next turn on this worker continues the
            // replay against the same pinned snapshot.
            return;
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
     * Runs one more turn of a localized repair a prior turn parked on its turn
     * budget. The session hands back the pinned snapshot {@code E} the repair was
     * planned against, the live-view writer holding the replacement rows emitted so
     * far and the staged root versions; the compiled factory still holds the window
     * state the replay had reached. So the turn is the same replay continuing, not
     * a new repair: nothing is re-planned, nothing durable has moved, and the
     * bounds stay the ones derived against {@code E}.
     * <p>
     * Only the worker that suspended the repair calls this - the resources came out
     * of this worker's pools and its capture freezes through this worker's timeline
     * store writer. A crash instead loses {@code E} for good, and startup
     * reconciliation discards the candidate so a later turn replans at a freshly
     * pinned one.
     * <p>
     * What it will not do is continue in a runtime that drifted. The whole premise is
     * that the compiled factory still stands where the last turn left it, so a factory
     * rebuilt since the capture - a base-metadata recompile is the one path that does
     * that - takes the candidate away rather than the replay: its bounds and its staged
     * roots describe a state those functions no longer hold, and its overlay holds
     * bytes that belong to functions now freed. Discarding is bounded and cheap, and
     * the change that triggered the repair is still unconsumed in the base, so the next
     * tick replans it at a freshly pinned snapshot. {@code prepareForBaseSchemaRecompile}
     * discards on that path already; this is the guard that keeps a future one honest.
     */
    private void resumeSuspendedRepair(LiveViewInstance instance, LiveViewCheckpointRepairSession session)
            throws SqlException {
        final RecordCursorFactory compiledFactory = instance.getCompiledFactory();
        final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
        if (compiledFactory == null || compiledPlan == null || compiledPlan.getWindowFactory() != session.getWindowFactory()) {
            LOG.info().$("live view runtime changed under a parked O3 repair, discarding the candidate [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", turns=").$(session.getTurns())
                    .$(", highTsExclusive=").$(session.getPlan().getHighTsExclusive()).I$();
            // Forget rather than restore: close() would otherwise put the capture's bytes
            // back into whichever functions the view holds now, which are not the ones
            // they came out of.
            session.forgetRuntime();
            instance.discardSuspendedRepair();
            return;
        }
        final WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        final TableReader reader = session.takeBaseReader();
        instance.recordCheckpointRepairResume();
        boolean suspended = false;
        try {
            suspended = o3HeadMissReplay(instance, windowFactory, session.getPlan(), reader, false, session, true);
        } finally {
            if (!suspended) {
                reader.close();
            }
        }
        if (suspended) {
            return;
        }
        // The repair published, so its replacement rewrote the on-disk tier and the
        // in-mem tier still holds the pre-repair rows for that range. Same tail as a
        // single-turn repair takes - see o3Replay.
        rebuildInMemoryTier(instance);
        instance.setLeadRowCount(0);
    }

    /**
     * Fills {@link #repairPlan} with the coordinates one out-of-order repair
     * works from: the pinned snapshot's {@code seqTxn}, the correction floor
     * {@code C}, the retire floor, and the executor the change qualifies for.
     * This is the pin-and-classify half of a repair, with the pin itself owned
     * by the caller.
     * <p>
     * The one classification this needs from disk is the apply-ahead range: when
     * {@code ApplyWal2TableJob} raced the reader past {@code advanceTo}, the pinned
     * snapshot already holds transactions the forward drain never examined, and the
     * lowest in-view timestamp among them decides both how far the retire floor
     * drops and whether any sealed anchor can still be trusted.
     * {@link #computeApplyAheadBounds} reads the base WAL-E files to answer that, so
     * it runs only when {@link LiveViewCheckpointRepairPlan#isApplyAheadClassificationRequired}
     * says the answer can change the plan. It can throw on a torn or purged WAL-E,
     * which the caller's pin ownership turns into a returned reader rather than a
     * leaked one. Its upper bound joins the caller's {@code changeMaxTs} for the same
     * reason the lower one joins the correction floor: the repair re-materialises the
     * ahead range too, so a convergence boundary that does not clear it would leave
     * those rows unemitted while the watermark walked past them.
     * <p>
     * The head pair is read atomically: without that, a concurrent
     * {@code setHeadCheckpoint} could pair a fresh {@code lvSeqTxn} with the prior
     * {@code maxTs} and drive the anchor decision off a torn read.
     * <p>
     * The second thing it reads from disk is the live-view table's own frontier, the
     * lower bound on {@code D} the output floor {@code R} is clamped to. Only a
     * DATA-triggered repair over a view whose window functions are <b>all</b> covered by
     * a finite RANGE, ROWS or anchor-segment dependency can localize, so the reader opens
     * only for that case: a non-DATA trigger rebuilds the whole view, and one uncovered
     * function leaves the repair with no floor it may raise. A view carrying more than one
     * shape - a bounded ROWS window declared beside an anchored one, say - hands all of
     * them over together and the plan takes their union.
     * <p>
     * A finite ROWS dependency costs one more read than a RANGE one. Its bounds are
     * per-key row counts rather than timestamp arithmetic, so the plan calls back into
     * {@link RowsBoundDiscovery} to find them in the base data - which is why the
     * discovery is prepared here, against the same pinned reader, and why this method
     * can now throw the way a scan can. The insert-only proof the ROWS path needs is
     * assembled from three independent observations: the caller's walk over the commits
     * it drained, this method's walk over the apply-ahead range, and the base table's
     * dedup configuration, which is the one deletion source neither walk can see (both
     * read the raw WAL, and dedup happens at apply time).
     * <p>
     * The runtime frontier it passes is the view's own {@code latestSeenTs}, but only
     * when the repair could actually put the runtime window state back: the state
     * travels through the checkpoint freeze/restore contract, so a view without
     * checkpoint-state support has no way to save it. An anchored view's anchor map
     * rides on that same contract - {@link LiveViewCheckpointScratchOverlay} carries it
     * beside the function state - so an anchored view quotes its frontier here too.
     * <p>
     * The third disk read is {@link LiveViewCheckpointScanCost}, which prices the two
     * dispositions off the pinned reader's partition metadata so the plan takes the
     * cheaper rather than whichever exists. It opens no partition and reads no column,
     * and it is what stops a sealed anchor sitting just below an old correction from
     * turning a bounded repair back into a replay of the whole view above it. The cost
     * it does add is the ROWS discovery: an anchored repair over a view holding a
     * bounded ROWS function discovers its bounds even when the resume goes on to win,
     * because those bounds are the only thing that could answer the question. The
     * discovery's own scan budget bounds what that costs.
     */
    private void planO3Repair(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lateRowTs,
            long changeMaxTs,
            boolean insertOnly,
            TableToken baseToken,
            long advanceTo,
            TableReader reader
    ) throws SqlException {
        final long pinnedSeqTxn = reader.getSeqTxn();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
        long applyAheadMinTs = Numbers.LONG_NULL;
        long effectiveChangeMaxTs = changeMaxTs;
        boolean effectiveInsertOnly = insertOnly;
        if (LiveViewCheckpointRepairPlan.isApplyAheadClassificationRequired(lateRowTs, advanceTo, pinnedSeqTxn)) {
            final long[] aheadBounds = computeApplyAheadBounds(baseToken, advanceTo, pinnedSeqTxn, viewLowerBoundTimestamp);
            applyAheadMinTs = aheadBounds[0];
            // An unclassifiable ahead range already denies every anchor through the
            // retire floor; deny the convergence boundary on the same terms, since
            // nothing then says how far up those un-examined seqTxns reach.
            effectiveChangeMaxTs = applyAheadMinTs == Numbers.LONG_NULL
                    ? Numbers.LONG_NULL
                    : Math.max(effectiveChangeMaxTs, aheadBounds[1]);
            effectiveInsertOnly &= applyAheadInsertOnly;
        }
        long rangeFrameWidth = Numbers.LONG_NULL;
        long durableOutputMaxTs = Numbers.LONG_NULL;
        LiveViewCheckpointRepairPlan.RowsBoundSource rowsBoundSource = null;
        LiveViewCheckpointAnchorPlan anchorPlan = null;
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        final LiveViewCheckpointRangePlan rangePlan = windowFactory.getCheckpointRangePlan();
        final LiveViewCheckpointRowsPlan rowsPlan = windowFactory.getCheckpointRowsPlan();
        // Null unless the compiler proved both halves of the anchor contract: a
        // closed-form segment boundary, and every anchored window function reset by it.
        final LiveViewCheckpointAnchorPlan viewAnchorPlan = anchorWindow == null
                ? null
                : anchorWindow.getCheckpointAnchorPlan();
        // Each plan describes the window functions of its own kind, so a mixed factory
        // carries several and the repair bounds them together. What it may not do is bound
        // some of them: a function outside the union is one the replay cannot reconstruct,
        // and the timestamp-global replacement re-emits its output all the same.
        final boolean isDependencyComplete = LiveViewCheckpointFunctionCompiler.isDependencyComplete(
                windowFactory.getWindowFunctions(),
                rangePlan != null,
                rowsPlan != null,
                viewAnchorPlan != null
        );
        // Dedup denies a ROWS plan its answer on separate grounds: it replaces a row with
        // the incoming one, so it can drop a partition key out of the change interval -
        // either by rewriting the key column of a row that is not itself a dedup key, or by
        // replacing a row the view's WHERE accepted with one it rejects. Neither raw-WAL
        // walk sees that, and a table without dedup keys cannot do it at all.
        //
        // Each gate withholds every dependency input, so the plan below sees only their
        // absence. The code names which one fired, because that is what an operator acts
        // on. Dedup is tested ahead of the insert-only claim: a dedup base is routed to
        // drainAppliedBase, which claims none, so the ordering reports the root cause
        // rather than the routing's consequence.
        final int dependencyDenialReason;
        if (lateRowTs == Numbers.LONG_NULL) {
            dependencyDenialReason = LiveViewCheckpointRepairPlan.DENIAL_NON_DATA_TRIGGER;
        } else if (!isDependencyComplete) {
            dependencyDenialReason = LiveViewCheckpointRepairPlan.DENIAL_INCOMPLETE_DEPENDENCY;
        } else if (rowsPlan != null && hasDedupKeys(reader.getMetadata())) {
            dependencyDenialReason = LiveViewCheckpointRepairPlan.DENIAL_DEDUP;
        } else if (rowsPlan != null && !effectiveInsertOnly) {
            dependencyDenialReason = LiveViewCheckpointRepairPlan.DENIAL_NOT_INSERT_ONLY;
        } else {
            dependencyDenialReason = LiveViewCheckpointRepairPlan.DENIAL_NONE;
        }
        if (dependencyDenialReason == LiveViewCheckpointRepairPlan.DENIAL_NONE) {
            if (rangePlan != null) {
                rangeFrameWidth = rangePlan.getMaxFrameWidth();
            }
            if (rowsPlan != null) {
                rowsBoundDiscovery.of(rowsPlan, instance.getCompiledPlan(), reader);
                rowsBoundSource = rowsBoundDiscovery;
            }
            // The segment bounds the repair from both sides without reading a base row, so
            // unlike the ROWS path this arm costs only the live-view frontier read.
            anchorPlan = viewAnchorPlan;
            durableOutputMaxTs = readDurableOutputMaxTs(instance);
        }
        // The runtime state a converging repair puts back travels through the checkpoint
        // freeze/restore contract, so a view without checkpoint-state support cannot save
        // it. An anchored view's anchor map rides along on the same contract, which is
        // what lets its frontier be quoted here at all.
        final long runtimeFrontierTs = instance.isSnapshotCapability()
                ? instance.getLatestSeenTs()
                : Numbers.LONG_NULL;
        timelineAnchors.of(instance);
        scanCost.of(reader);
        repairPlan.of(
                timelineAnchors,
                lateRowTs,
                viewLowerBoundTimestamp,
                advanceTo,
                pinnedSeqTxn,
                applyAheadMinTs,
                rangeFrameWidth,
                rowsBoundSource,
                anchorPlan,
                effectiveInsertOnly,
                durableOutputMaxTs,
                effectiveChangeMaxTs,
                runtimeFrontierTs,
                scanCost
        );
        // The disposition this repair takes, and the reason it reads more than a
        // localized rebuild would - the runtime counterpart to the static
        // checkpoint_repair_plan, which names only what the SQL admits. A gate above
        // withheld the dependency inputs on grounds the plan cannot see, so its generic
        // no-dependency verdict gives way to the specific one.
        instance.recordCheckpointRepairOutcome(
                repairPlan.getDisposition(),
                repairPlan.getDenialReason() == LiveViewCheckpointRepairPlan.DENIAL_NO_DEPENDENCY
                        && dependencyDenialReason != LiveViewCheckpointRepairPlan.DENIAL_NONE
                        ? dependencyDenialReason
                        : repairPlan.getDenialReason()
        );
        if (rowsBoundSource != null && rowsBoundDiscovery.hasDiscovered()) {
            // The discovery's reads are this repair's reads, so they join the same
            // base-rows-scanned counter the replay reports through - planning and replay
            // cost are then readable in one unit.
            instance.bumpO3ReplayScanRows(rowsBounds.getScanRows());
            LOG.info().$("live view O3 repair ROWS bounds [view=").$(instance.getDefinition().getViewName())
                    .$(", localized=").$(repairPlan.isLocalized())
                    .$(", replayLowTs=").$(repairPlan.getReplayLowTs())
                    .$(", outputLowTs=").$(repairPlan.getOutputLowTs())
                    .$(", highTsExclusive=").$(repairPlan.getHighTsExclusive())
                    .$(", affectedKeys=").$(rowsBounds.getAffectedKeyCount())
                    .$(", outputKeys=").$(rowsBounds.getOutputKeyCount())
                    .$(", indexedKeyLookups=").$(rowsBounds.getIndexedKeyLookups())
                    .$(", scanRows=").$(rowsBounds.getScanRows())
                    .$(", scanBudget=").$(rowsBounds.getScanBudgetStatus().name()).I$();
        }
    }

    /**
     * Whether the base table deduplicates on commit, the one way a change set the raw
     * WAL walks read as insert-only can still have removed a base row: dedup drops the
     * existing row and keeps the incoming one, at apply time and therefore out of sight
     * of both walks. A table with no dedup key column cannot do it at all.
     * <p>
     * {@link #isDedupBase} answers the same question off the metadata cache, and answers
     * it earlier: a dedup base is routed to {@link #drainAppliedBase}, which hands the
     * repair no change ceiling and no insert-only claim, so a repair that reaches here
     * with dedup enabled is one whose routing already changed under it (an
     * {@code ALTER ... DEDUP ENABLE} between cycles). This reads the pinned reader's own
     * metadata rather than the cache for the same reason the bounds read the pinned
     * reader's rows - it is the snapshot the discovery is about to search.
     */
    private static boolean hasDedupKeys(TableReaderMetadata metadata) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.isDedupKey(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads the highest designated timestamp the live-view table durably holds, or
     * {@link Numbers#LONG_NULL} when it holds no row. This is the lower bound on
     * {@code D}: every output row the runtime has incorporated but not made durable -
     * a discarded in-RAM lead, a rolled-back current-turn draft - was produced after
     * the last flush and therefore sits at or above this frontier.
     * <p>
     * A live-view WAL block that is committed but not yet applied leaves the frontier
     * behind the true one, which only lowers {@code R} and makes the repair re-emit
     * more. There is no error in that direction.
     */
    private long readDurableOutputMaxTs(LiveViewInstance instance) {
        try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
            return lvReader.size() > 0 ? lvReader.getMaxTimestamp() : Numbers.LONG_NULL;
        }
    }

    /**
     * Resume replay from a logical checkpoint boundary: rolls window state back to
     * that root's snapshot moment (clear per-function maps, then restore the root),
     * scans the base table from {@code anchorMaxTs + 1} forward (never below
     * {@code viewLowerBoundTimestamp}), and emits a single REPLACE_RANGE commit
     * covering that same range through positive infinity. Cheaper than the
     * whole-history rebuild because the root's state already reflects everything in
     * {@code [viewLowerBoundTimestamp, anchorMaxTs]} - the replay only re-evaluates
     * the tail above it. Not cheaper than a <i>localized</i> rebuild by
     * construction, though: that one stops at a finite convergence boundary while
     * this reads to the end of the base table, so the plan prices the two and this
     * path runs only when it wins.
     * <p>
     * Pure execution: {@link LiveViewCheckpointRepairPlan} has already chosen the
     * anchor through the timeline's predecessor lookup, along with the commit point
     * and the retire floor, all against the pinned snapshot; it has proven the
     * anchor sits strictly below both the change and anything apply raced past the
     * trigger.
     * <p>
     * The caller owns {@code reader} and closes it; this method only detaches it
     * for the execution context and re-attaches it on the way out.
     * <p>
     * The restore runs before the timeline is retired, because the timeline is what
     * holds the state being restored. Once the state is in memory the whole
     * timeline goes: this replay rewrites durable output above the anchor, so every
     * root at or above it describes a materialization that no longer exists, and
     * invariant 2 admits no generation mixing corrected and stale roots. Retiring
     * ahead of the scan also keeps a crash mid-replay cheap - a restart then finds
     * no timeline and rebuilds from the applied base, which is what it would do
     * with a poisoned one anyway. The post-replay seal opens a fresh history.
     * <p>
     * A restore failure retires the timeline too and abandons the replay without
     * advancing the watermark, so the trigger re-fires on a later cycle and takes
     * the rebuild rather than re-selecting the root it could not read. A
     * compatibility break (version mismatch) instead stashes a pending invalidation
     * reason.
     * <p>
     * A replay that produces no row seals nothing: the truncating commit leaves the
     * LV holding exactly the rows the anchor covered, and the restore left the
     * window state at that same moment, so the retired timeline is re-opened by the
     * next in-order seal.
     */
    private void replayFromAnchor(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            LiveViewCheckpointRepairPlan plan,
            TableReader reader
    ) throws SqlException {
        final String viewName = instance.getDefinition().getViewName();
        final long anchorCheckpointId = plan.getAnchorCheckpointId();
        final long anchorMaxTs = plan.getAnchorMaxTs();
        // Replay starts strictly above anchorMaxTs because the anchor's state
        // already covers rows up to and including anchorMaxTs. The same value
        // doubles as the REPLACE_RANGE low boundary so the apply step
        // rewrites only the affected partitions. The plan floors it at the START
        // FROM boundary so this path applies the same row predicate as the seed,
        // the forward drain and the head-miss replay.
        final long replayLowTs = plan.getReplayLowTs();
        // Commit / watermark point for this replay: the pinned snapshot's seqTxn.
        // Equal to the O3 trigger seqTxn unless apply raced ahead of it, in which
        // case the scan below materialises the ahead range too - exactly as
        // o3HeadMissReplay does.
        final long committedSeqTxn = plan.getCommitSeqTxn();
        boolean readerAttached = false;
        long appendedRows = 0;
        long o3ScanRows = 0;
        long replayMaxTs = Numbers.LONG_NULL;
        // Set when this resume preserved the timeline prefix (the anchor and every
        // earlier root) behind a live marker instead of retiring; the seal below
        // resolves it.
        boolean prefixMarkerLive = false;
        try {
            engine.detachReader(reader);
            executionContext.of(reader);
            readerAttached = true;

            final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
            final Function filter = compiledPlan.getFilter();
            final PageFrameRecordCursorFactory pageFrameFactory = compiledPlan.getPageFrameFactory();
            RecordMetadata outMetadata = compiledPlan.getOutputMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();

            try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                RecordToRowCopier copier = ensureCopier(instance, walWriter);
                // Open the snapshot AT replayLowTs rather than scanning up to it: the
                // inclusive-lower-bound cursor culls whole partitions and binary-searches
                // into the first one. Head-hit exists to re-evaluate only the tail above
                // the head, so walking every partition below headMaxTs row by row - which
                // is what wrapping a full scan in TimestampLowerBoundCursor did - spent the
                // very cost the branch was built to avoid. Same cursor the seed and the
                // forward drain take, so all three agree on the boundary row for row.
                //
                // The high bound comes from the plan's tagged H, so a repair that proves a
                // finite convergence boundary reads no partition above it. Today every plan
                // tags EOF, which is Long.MAX_VALUE inclusive - the same unbounded tail this
                // scan always read.
                try (RecordCursor pageCursor = pageFrameFactory.getCursorInTimestampRange(
                        executionContext,
                        replayLowTs,
                        plan.getScanHighTsInclusive()
                )) {
                    RecordCursor source = pageCursor;
                    if (filter != null) {
                        filteringCursor.of(source, filter, executionContext);
                        source = filteringCursor;
                    }
                    source = compiledPlan.wrapWindowInput(source, executionContext);
                    final LiveViewWindow anchorWindow = instance.getAnchorWindow();
                    if (anchorWindow != null) {
                        anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                        source = anchorDispatchingCursor;
                    }
                    try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                        final long anchorLvRowPosition;
                        if (canReuseRuntimeAnchor(instance, windowFactory, plan)) {
                            // The selected anchor is the root the current head
                            // mirrors, this runtime is the one that froze it, and no
                            // row has entered the window pipeline since. The live
                            // maps and arenas therefore already are the anchor's
                            // state, and the lifetime row counter is still the
                            // position the root recorded. Avoid decoding the same
                            // immutable pages to write that state back over itself.
                            anchorLvRowPosition = instance.getLvRowsTotal();
                            runtimeAnchorReuseCount++;
                        } else {
                            // Drop pre-O3 drift before restoring the anchor root:
                            // clear each function's partition map so accumulator
                            // state that outran the root's snapshot moment is
                            // discarded. The anchor map gets the same treatment
                            // inside LiveViewWindow.restore() (it clears before
                            // reinserting), so no explicit wipe is needed here.
                            // Order matters: function maps clear -> restore root.
                            // isOpen() rather than a null test: a function whose state the
                            // window owns keeps a closed map, and its accumulator is
                            // cleared with the anchor map's own entry instead.
                            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
                            for (int i = 0, n = functions.size(); i < n; i++) {
                                Map m = functions.getQuick(i).getPartitionMap();
                                if (m != null && m.isOpen()) {
                                    m.clear();
                                }
                            }
                            // Wiped, and restoreAnchorRoot below can fail or come back
                            // empty, so the runtime is inconsistent until the replay
                            // commits.
                            markWindowStateDirty(instance);
                            anchorLvRowPosition = restoreAnchorRoot(
                                    instance,
                                    windowFactory,
                                    anchorMaxTs,
                                    anchorCheckpointId
                            );
                        }
                        if (anchorLvRowPosition == Numbers.LONG_NULL) {
                            // The root could not be read, or its format is one this
                            // build cannot restore (which stashed a pending
                            // invalidation reason). Nothing is in memory, so retire
                            // the whole timeline: the O3 replay is abandoned here
                            // without advancing the watermark, so the same trigger
                            // re-fires on a later refresh cycle and rebuilds against
                            // the now-retired timeline (one cycle of stale pre-O3
                            // rows in between). try-with-resources closes the cursor
                            // on return.
                            retireCheckpointStateOnO3(instance, true);
                            return;
                        }
                        // The state is in memory now - the timeline is what held it,
                        // and from here the replay owns correctness of the durable
                        // output. Preserve the roots below the output floor (the
                        // anchor among them) instead of retiring the whole timeline
                        // for one predecessor-resume repair; the marker this writes
                        // forces a mid-repair crash to rebuild from the applied base.
                        prefixMarkerLive = truncateOrRetireTimelineOnO3(instance, plan.getOutputLowTs());
                        // Snap the lifetime row counter back to the root's
                        // recorded position: the upcoming REPLACE_RANGE commit
                        // logically truncates rows above replayLowTs, so the
                        // counter rewinds in step with the table.
                        instance.setLvRowsTotal(anchorLvRowPosition);
                        // Rows leave the window in the window factory's shape; the
                        // output projection turns them into the view's own schema,
                        // which is what the copier was generated for. Drive the
                        // projected cursor rather than the window one - it is what
                        // advances the projection's per-row memoization before the
                        // record is read. Without the wrap the copier reads the
                        // window's own columns positionally, so a projected view
                        // silently stores a different window column in each of its
                        // computed columns. wrapWindowOutput does not rewind and
                        // returns windowCursor itself when the view has no
                        // projection, so the unprojected replay is unchanged.
                        final RecordCursor outCursor = compiledPlan.wrapWindowOutput(windowCursor, executionContext);
                        Record outRecord = outCursor.getRecord();
                        while (outCursor.hasNext()) {
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
                    // anything above anchorMaxTs that survives the filter - a
                    // REPLACE_RANGE delete or a dedup replacement erased it - while
                    // the pre-O3 output for that range still sits on disk (the plan
                    // picked the anchor strictly below the change, so the view did
                    // emit rows there). Skipping the commit would strand them as
                    // ghosts: size() over-reports, reads return stale rows, and
                    // rebuildInMemoryTier stages them back - all while the watermark
                    // advances past the commit that removed their base rows. Emitting
                    // the truncating range with no rows clears (anchorMaxTs, +inf) and
                    // leaves the LV exactly at the anchor's snapshot moment, which the
                    // restore above already reproduced in the window state. Mirrors
                    // the pure-delete branch in o3HeadMissReplay.
                    //
                    // The output floor R equals the scan floor L on a resume - the
                    // restored anchor state IS the warm-up, so every row read is a row
                    // emitted - but the commit takes R to keep the two roles distinct.
                    final long replaceLowTs = plan.getOutputLowTs();
                    fencedLiveViewCommit(instance, () -> walWriter.commitLiveViewWithReplaceRange(committedSeqTxn, replaceLowTs, Long.MAX_VALUE));
                }
            }
        } finally {
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
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
        boolean headSealed = false;
        if (lvConsumedPersisted && appendedRows > 0) {
            // Seal the post-replay state, appending a fresh head onto the preserved
            // prefix (or opening a fresh history when the prefix was retired). force
            // writes past the cadence gate, though the cleared head already puts this
            // on the first-checkpoint path: an O3 resume must advance the boundary or
            // the next replay re-scans from the stale maxTs.
            //
            // A zero-row replay seals nothing: the truncating commit above left the
            // LV table holding exactly the rows the anchor covered, and the restore
            // left the window state at that same moment, so the next in-order seal
            // re-opens the history from there. There is also nothing to seal
            // (replayMaxTs is LONG_NULL).
            // Take the seal's own answer. maybeWriteHeadCheckpoint swallows every Throwable and
            // also declines a boundary that does not clear the head, so assuming success here
            // would clear the durable repair marker over a head that was never written - and the
            // next restart would take the incremental path against a head-truncated timeline.
            headSealed = maybeWriteHeadCheckpoint(instance, windowFactory, committedSeqTxn, replayMaxTs, appendedRows, true);
        }
        if (prefixMarkerLive) {
            // Resolve the truncate's live marker: a fresh head now anchors the
            // preserved prefix, so clear it; or, on a zero-row resume, the truncated
            // timeline has no head, so retire it (which removes the marker) and let a
            // restart rebuild.
            if (headSealed) {
                clearCheckpointRepairMarker(instance);
            } else {
                retireCheckpointTimeline(instance);
            }
        }
        // The resume replay is "the win": bounded to the tail above the anchor.
        // Counted separately from the boundary rebuild so live_views() can show how
        // much O3 work stays cheap versus the residual unbounded fallbacks.
        instance.bumpO3ResumeReplayRows(appendedRows);
        // Baseline scan-cost signal: base rows this resume replay pulled (>= emit).
        instance.bumpO3ReplayScanRows(o3ScanRows);
        // applyAheadGap = the seqTxns ApplyWal2TableJob raced past the O3 trigger
        // (0 on the common path); the anchor fields record which logical boundary the
        // resume rolled back to, so a wide gap or a distant anchor is diagnosable.
        LOG.info().$("live view O3 resume replay completed [view=")
                .$(viewName)
                .$(", advanceTo=").$(committedSeqTxn)
                .$(", anchorCheckpointId=").$(anchorCheckpointId)
                .$(", anchorMaxTs=").$(anchorMaxTs)
                .$(", applyAheadGap=").$(plan.getPinnedSeqTxn() - plan.getTriggerSeqTxn())
                .$(", rowsEmitted=").$(appendedRows).I$();
    }

    /**
     * Pins a base reader, plans against it, and runs the head-miss rebuild - the
     * entry point for callers that already know they need a rebuild and hold no
     * pinned snapshot: restart restore, corrupt-checkpoint restore, base-metadata
     * drift, mid-drain recovery and WAL-loss re-derive. They all pass a non-DATA
     * trigger ({@code lateRowTs == LONG_NULL}), which authorises no deletion, so
     * the plan reduces to the pinned snapshot's {@code seqTxn} and a wholesale
     * retire. The same non-DATA trigger denies localization, so the change ceiling
     * these callers cannot supply would not be read anyway.
     * <p>
     * Apply-lag handling: a base-table {@code TableReader} obtained right after
     * detection may not yet reflect {@code advanceTo}, because the global
     * {@code ApplyWal2TableJob} runs asynchronously. {@link #waitForApply} polls
     * until the reader's {@code getSeqTxn() >= advanceTo}, bounded by
     * {@code cairo.live.view.flush.retry.max.duration} so a stalled apply trips the
     * flush-retry budget rather than spinning forever.
     * <p>
     * {@link #o3Replay} does not come through here: it pins and plans once for both
     * executors, and calls the plan-taking overload directly.
     */
    private void o3HeadMissReplay(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lateRowTs,
            TableToken baseToken,
            long advanceTo,
            boolean fullRebuild
    ) throws SqlException {
        final TableReader reader = waitForApply(baseToken, advanceTo);
        try {
            planO3Repair(instance, windowFactory, lateRowTs, Numbers.LONG_NULL, false, baseToken, advanceTo, reader);
            // These callers own the pinned reader for one call and close it below, so
            // the rebuild may not park a repair on it. It never would: a non-DATA
            // trigger denies localization, and only a localized rebuild yields.
            o3HeadMissReplay(instance, windowFactory, repairPlan, reader, fullRebuild, null, false);
        } finally {
            reader.close();
        }
    }

    /**
     * Head-miss replay path: discards every window-function
     * partition map and the anchor map, drives the compiled SELECT's
     * filter / anchor / window cursor stack over the pinned {@code TableReader}'s
     * ts-sorted view starting from the plan's scan floor {@code L}, emits
     * a single REPLACE_RANGE commit covering the plan's replacement interval
     * {@code [R, H)}, and applies inline.
     * <p>
     * {@code L} and {@code R} are both {@code viewLowerBoundTimestamp} unless
     * the plan localized the rebuild, in which case the two split: rows in
     * {@code [L, R)} are fed through the window stack to warm its state up and
     * emit nothing, because their durable output is already correct, and the
     * replacement starts at {@code R}. A localized rebuild reads no base row
     * below {@code L} at all, which is the whole point - its cost stops
     * tracking the view's age. See
     * {@link LiveViewCheckpointRepairPlan#isLocalized()} for when that applies.
     * <p>
     * {@code H} closes the interval from above. It is end-of-frame - the whole
     * tail, as every rebuild read before the bound existed - unless the plan
     * proved a finite convergence boundary, which it does only for a localized
     * rebuild whose change set has a known ceiling and whose runtime state
     * provably survives the repair. A finite {@code H} changes three things at
     * once, and they stand or fall together: the scan stops there, the
     * replacement's high bound is that value rather than positive infinity so
     * the durable output above it is left alone, and the runtime window state -
     * correct on entry, and describing {@code H - 1} rather than the frontier
     * once the replay has run over it - is taken out of the way beforehand and
     * put back after. See
     * {@link LiveViewCheckpointRepairPlan#isRuntimeStatePreserved()}.
     * <p>
     * Cost of the unlocalized rebuild is O(retained_rows x n_window_functions) of
     * {@code computeNext} plus the partition-rewrite I/O - acceptable for short-lived
     * views but several seconds to minutes for long-lived ones per the cost model.
     * {@link #replayFromAnchor} avoids the worst of this when a sealed anchor below the
     * change reads fewer base rows than the interval this rebuild would localize to;
     * the plan compares the two and routes here when it does not.
     * <p>
     * Pure execution: {@code plan} carries the pinned snapshot's {@code seqTxn}
     * (the commit and watermark point), the correction floor {@code C}, the scan and
     * output floors {@code L}/{@code R}, and the retire floor, all derived once in
     * {@link #planO3Repair}. The caller owns {@code reader} and closes it; this
     * method only detaches it for the execution context and re-attaches it on the way
     * out.
     * <p>
     * The rebuild commits at the pinned {@code seqTxn} rather than at the trigger.
     * The scan materialises everything the snapshot holds at or above {@code L},
     * including transactions {@code ApplyWal2TableJob} raced past the trigger;
     * leaving the watermarks at the trigger would make the forward path re-read those
     * already-materialised seqTxns, and a trailing in-order commit (a lone row at the
     * global max, say) would re-append a duplicate row. That is also why the plan
     * derives {@code L}/{@code R} from the retire floor rather than from {@code C}:
     * the watermark advances past the whole snapshot, so a floor above a back-dated
     * apply-ahead row would drop it permanently.
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
     * non-DATA removal (DROP PARTITION / TTL / TRUNCATE) still freezes its prefix. A full
     * rebuild never localizes - every one of its callers passes a non-DATA trigger, which
     * the plan refuses to derive floors from.
     * <p>
     * A localized rebuild runs one turn at a time. Its interval is finite but can
     * still be dense enough to hold more rows than one refresh turn should carry,
     * so the replay stops once it crosses the per-turn budget, parks everything the
     * next turn continues from in a {@link LiveViewCheckpointRepairSession}, and
     * returns true. It stops after a row rather than before one - the window cursor
     * folds a row into the state as it yields it - so the resume point is that
     * row's timestamp plus the count of its timestamp group already folded, which
     * the next turn skips past. Nothing durable moves at that point: the
     * replacement is still uncommitted in the writer the session holds and no
     * generation names the roots it has staged, so a reader sees the pre-repair
     * view until the final turn publishes the lot. Only the worker that suspended a
     * repair resumes it, through {@code resumed}; the pinned snapshot {@code E} it
     * holds cannot be reopened once lost, which is why a crash discards the
     * candidate and replans instead.
     *
     * @param resumed  the session a prior turn parked, or null to start a repair
     * @param mayYield whether this caller can hand the pinned reader over to a
     *                 parked repair. False for the callers that own the reader for
     *                 one call and close it on the way out
     * @return true when the replay yielded and the repair is parked on the instance
     */
    private boolean o3HeadMissReplay(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            LiveViewCheckpointRepairPlan plan,
            TableReader reader,
            boolean fullRebuild,
            @Nullable LiveViewCheckpointRepairSession resumed,
            boolean mayYield
    ) throws SqlException {
        final String viewName = instance.getDefinition().getViewName();
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
        // C, the DATA trigger's authority to DELETE, expressed in the view's own
        // coordinate space: the lowest timestamp the triggering commit touched, clamped up
        // to the view's lower bound. A non-DATA / recovery trigger (LONG_NULL) authorises
        // no deletion at all and leaves the frozen-prefix rule to govern (DROP PARTITION /
        // TTL / TRUNCATE / restart). See LiveViewCheckpointRepairPlan for why the clamp is
        // what makes the trigger usable once a finite START FROM boundary is in play.
        final long triggerLowTs = plan.getCorrectionTs();
        final long effectiveSeqTxn = plan.getCommitSeqTxn();
        // A finite dependency raised the floors above the view boundary, so this
        // rebuild is bounded below. fullRebuild is redundant here - its callers all
        // pass a non-DATA trigger, which the plan refuses to localize - and is kept
        // only so a future full-rebuild caller with a timestamp cannot silently
        // inherit a localized floor.
        final boolean localized = plan.isLocalized() && !fullRebuild;
        // L: the lowest base row the replay reads. Everything below it is provably
        // outside every frame the replay evaluates.
        final long scanLowTs = localized ? plan.getReplayLowTs() : viewLowerBoundTimestamp;
        // R: the lowest output row the replay emits, and the REPLACE_RANGE floor.
        // Rows scanned below it warm the window state up and produce nothing.
        final long emitLowTs = localized ? plan.getOutputLowTs() : viewLowerBoundTimestamp;
        // H: finite only for a localized rebuild, and only when the plan proved the
        // repair converges below the runtime frontier. It governs three things that
        // have to move together - where the scan stops, where the replacement stops,
        // and whether the runtime keeps the state it entered with - so they all read
        // this one flag rather than the plan directly. Routed through the same
        // fullRebuild veto as the floors: a rebuild that must recompute the whole view
        // may not stop early, whatever the plan derived.
        final boolean finiteHighBound = localized && plan.isRuntimeStatePreserved();
        // Whether this repair may re-version the logical boundaries it crosses instead
        // of truncating the timeline at R. It needs the finite H every splice needs,
        // and one thing more: the publication has to be able to describe every key the
        // boundary held. Two ways to get there. A time-expiring dependency reconstructs
        // every key outright, which is
        // LiveViewCheckpointRepairPlan.isReplayStateKeyComplete(). A ROWS dependency
        // does not - a root frozen from such a replay would describe a narrower key set
        // than the boundary it replaces, which a later resume or restore then reads as
        // the whole truth - so it instead names the keys it does describe, and the
        // publication leaves every other key's entry exactly as the old root wrote it.
        // With neither, the repair truncates at R: the runtime survives a narrowed
        // state because the overlay puts it back, and a published root has nothing to
        // put it back from.
        final boolean isTimelineSpliceable = finiteHighBound
                && (plan.isReplayStateKeyComplete() || plan.getOutputKeyDomain() != null);
        // The publication ordering this rebuild walks. It owns the two decisions the
        // rest of the method used to spread across local flags: what happens to the
        // runtime once the repair publishes, and whether the replacement is
        // materialised enough for a generation, a watermark or a head seal to
        // describe it.
        repairPublication.clear();
        repairPublication.plan();
        // Everything one localized repair carries across the turns it may take. A
        // repair that never yields uses it as plain scratch and disposes of it on
        // the way out; only a repair that parks leaves it on the instance. The
        // unlocalized rebuild has none - it stages no roots, keeps no overlay and
        // may not yield.
        final boolean resuming = resumed != null;
        final LiveViewCheckpointRepairSession session = resuming
                ? resumed
                : finiteHighBound ? openRepairSession(plan, windowFactory) : null;
        boolean readerAttached = false;
        // The scratch overlay is captured once, by the first turn, before the wipe
        // reaches the published state.
        boolean overlayCaptured = session != null && session.getOverlay().isCaptured();
        // Cumulative across every turn of this repair; a resumed turn continues the
        // counts the prior ones left.
        long appendedRows = resuming ? resumed.getAppendedRows() : 0;
        long o3ScanRows = resuming ? resumed.getScanRows() : 0;
        long replayMaxTs = resuming ? resumed.getReplayMaxTs() : Numbers.LONG_NULL;
        // Minimum output ts the replay actually produced (rows arrive
        // ts-ascending, so the first appended row is the minimum). Base of the
        // REPLACE_RANGE low boundary decided at the commit site below.
        long replayMinTs = resuming ? resumed.getReplayMinTs() : Numbers.LONG_NULL;
        // Rows this turn's window cursor produced, emitted or suppressed. The
        // scan-cost counter is sourced from it when no filter is present to count
        // base rows itself.
        long scannedRows = 0;
        // The timeline range splice this repair publishes instead of retiring
        // the whole timeline. Taken only by a repair that stopped at a finite
        // H whose replay reconstructs every key: that is the case with a converged
        // suffix to keep, and the case whose runtime is restored rather than
        // promoted, so it creates no new logical boundary either. Null leaves the
        // retire - or, for a localized repair, the prefix truncate - in place, and
        // the boundary list stays empty so the replay's segmentation is a dead
        // branch.
        LiveViewCheckpointTimelineStoreWriter.RepairCapture timelineCapture = resuming
                ? resumed.takeCapture()
                : isTimelineSpliceable ? beginCheckpointTimelineRepair(instance, plan, session) : null;
        if (session != null) {
            // The publication mirrors every stage it records into the descriptor, and
            // a resumed turn walks the stages from PLAN again over the same record.
            repairPublication.of(session.getDescriptor());
        }
        // Live-view rows below R, and the rows the replacement is about to delete
        // from [R, H). Both are read from the pre-repair table, which is the only
        // moment they exist: the first anchors every repaired root's position, the
        // second proves after the fact that the replacement moved exactly the rows
        // the arithmetic says it did. A resumed turn inherits them - the table has
        // not moved since, because nothing was committed.
        long durableRowsBelowFloor = resuming ? resumed.getDurableRowsBelowFloor() : 0;
        long durableRowsBeforeRepair = resuming ? resumed.getDurableRowsBeforeRepair() : 0;
        long durableRowsReplaced = resuming ? resumed.getDurableRowsReplaced() : 0;
        if (!resuming && timelineCapture != null) {
            try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
                durableRowsBeforeRepair = lvReader.size();
                durableRowsBelowFloor = countDurableRowsBelow(lvReader, emitLowTs);
                final long rowsBelowHighBound = countDurableRowsBelow(lvReader, plan.getHighTsExclusive());
                if (durableRowsBelowFloor < 0 || rowsBelowHighBound < 0) {
                    throw CairoException.critical(0)
                            .put("live view table has no searchable prefix for a checkpoint timeline repair");
                }
                durableRowsReplaced = rowsBelowHighBound - durableRowsBelowFloor;
                session.setDurableRowCounts(durableRowsBeforeRepair, durableRowsBelowFloor, durableRowsReplaced);
            } catch (Throwable t) {
                LOG.error().$("could not measure live view durable prefix for a checkpoint timeline repair [view=")
                        .$(viewName).$(", error=").$(t).I$();
                timelineCapture = Misc.free(timelineCapture);
                session.discardDescriptor();
                session.getBoundaries().clear();
                durableRowsBelowFloor = 0;
                durableRowsBeforeRepair = 0;
                durableRowsReplaced = 0;
            }
        }
        // The logical boundaries this repair re-versions, and the cursor into them:
        // the ones the replay has already frozen. Empty for an unlocalized rebuild,
        // which freezes none.
        final ObjList<LiveViewCheckpointTimelineEntry> repairBoundaries =
                session != null ? session.getBoundaries() : emptyRepairBoundaries;
        int capturedBoundaries = resuming ? resumed.getCapturedBoundaries() : 0;
        boolean replayCompleted = false;
        // The range splice this repair published, null until it does (and if it
        // never does). Carries the newest logical key the spliced timeline holds,
        // which the post-replay seal below needs: a splice appends no root, so a
        // frontier that has run past that key leaves the generation claiming base
        // coverage no root has.
        LiveViewCheckpointTimelineStoreWriter.RepairResult timelineSplice = null;
        // Set when this turn preserved the timeline prefix instead of retiring it
        // (an EOF-reaching localized repair). A durable marker is then live and the
        // post-replay seal must resolve it: clear it once a fresh head is sealed, or
        // retire the truncated timeline when the repair emitted no rows to seal.
        boolean prefixMarkerLive = false;
        // Set when the replay stops on its turn budget with the repair unfinished,
        // together with the inclusive timestamp the next turn re-opens the scan at.
        boolean yielded = false;
        long resumeFromTs = Numbers.LONG_NULL;
        long resumeSkipRows = 0;
        if (resuming) {
            // The accumulators already lead the last durable commit - the prior turns
            // put them there - so a fault anywhere in this turn has to rebuild them from
            // the applied base rather than let the next cycle drain over a half-replayed
            // runtime. handleRefreshFailure reads this flag to decide that.
            windowStateDirty = true;
        }
        try {
            // Retire the checkpoint state this O3 has unsealed. Clearing the head
            // puts the post-replay seal on its first-checkpoint path; the follow-up
            // seal below opens a fresh history, and until then a restart rebuilds
            // from the view boundary.
            //
            // The versioned timeline goes with it unless this repair holds a splice
            // capture, which corrects the same roots precisely instead of dropping
            // them all.
            //
            // First turn only: a repair that yielded already retired what its change
            // unsealed, and the timeline it may still splice into is the one its
            // capture pinned.
            if (!resuming) {
                if (timelineCapture == null && localized) {
                    // Localized repair whose influence reaches the runtime frontier:
                    // there is no converged suffix to keep, but the roots below R are
                    // still correct. Preserve them - keeping the long-term anchors and
                    // the checkpoint id space - instead of retiring the whole timeline
                    // for one near-head correction. The durable marker this writes
                    // forces a mid-repair crash to rebuild from the applied base.
                    prefixMarkerLive = truncateOrRetireTimelineOnO3(instance, emitLowTs);
                } else {
                    retireCheckpointStateOnO3(instance, timelineCapture == null);
                }
            }

            engine.detachReader(reader);
            executionContext.of(reader);
            readerAttached = true;

            final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
            final Function filter = compiledPlan.getFilter();
            final PageFrameRecordCursorFactory pageFrameFactory = compiledPlan.getPageFrameFactory();
            RecordMetadata outMetadata = compiledPlan.getOutputMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();

            // Both scans below open the snapshot AT the scan floor rather than scanning up
            // to it, the same inclusive-lower-bound cursor the seed and the forward drain
            // take: it culls whole partitions and binary-searches into the first one instead
            // of walking the sub-floor history row by row. A view with a finite boundary over
            // a long-lived base has that history in front of it on every rebuild - and a
            // rebuild fires on any O3 commit, base metadata drift, mid-drain failure, corrupt
            // checkpoint or checkpoint-less restart - so the walk was paid twice per rebuild
            // (probe + recompute). BEGINNING persists Numbers.LONG_NULL (= Long.MIN_VALUE),
            // which the cursor turns into a full scan; a localized rebuild replaces that with
            // its dependency floor L and culls the history below it as well.
            // Both take their high bound from the plan's tagged H, so probe and recompute
            // agree on the read interval. Long.MAX_VALUE as an INCLUSIVE bound admits the
            // whole tail exactly as an unbounded scan did; a finite H culls the partitions
            // above the convergence boundary as well.
            final long scanHighTs = finiteHighBound ? plan.getScanHighTsInclusive() : Long.MAX_VALUE;
            // Where this turn's scan starts. A resumed turn re-opens at the timestamp
            // the prior one stopped on: everything below it is already in the window
            // state the compiled functions still hold, and the rows AT it that the
            // prior turn folded are skipped once the cursor chain is up.
            final long turnLowTs = resuming ? resumed.getResumeFromTs() : scanLowTs;

            // Probe pass: open a separate cursor over the same source + filter
            // chain and check whether any row survives. Skipping the wipe when
            // no rows pass the filter prevents a degenerate replay (e.g. WHERE
            // discards every row in the replay window) from permanently
            // erasing cumulative accumulator state for every partition.
            //
            // A localized rebuild needs no probe and must not take one: it reconstructs the
            // window state from [L, R) whatever the emit range holds, so the wipe it does is
            // never the permanent erasure the probe guards against, and an empty [R, H)
            // must still commit a truncating replacement to clear the ghost rows sitting
            // there. Skipping the probe also saves it the second pass over [L, H).
            final boolean hasReplayRow;
            if (localized) {
                hasReplayRow = true;
            } else {
                try (RecordCursor probeCursor = pageFrameFactory.getCursorInTimestampRange(
                        executionContext,
                        scanLowTs,
                        scanHighTs
                )) {
                    RecordCursor probeSource = probeCursor;
                    if (filter != null) {
                        filteringCursor.of(probeSource, filter, executionContext);
                        probeSource = filteringCursor;
                    }
                    hasReplayRow = probeSource.hasNext();
                }
            }

            if (hasReplayRow) {
                if (!resuming) {
                    if (finiteHighBound) {
                        // Copy the published runtime state aside before the wipe below
                        // reaches it. The replay has to run through these same function
                        // instances - the compiled cursor stack owns them and there is only
                        // one of it - so the overlay is what keeps the repair from
                        // overwriting state it has already proved correct.
                        session.captureRuntime(
                                windowFactory.getWindowFunctions(),
                                anchorWindow,
                                instance.getMemoryTracker()
                        );
                        overlayCaptured = true;
                    }
                    // Reset per-function accumulator state and the anchor map to
                    // identity. The compiled factory's WindowFunction instances
                    // stay live so the cursor chain below can reuse them; only
                    // their accumulated state resets. clearWindowState rewinds via
                    // toTop(), not a bare partition-map clear, so no-partition
                    // ranking like row_number() OVER () - whose counter lives in a
                    // scalar field with no map - also rewinds; otherwise it would
                    // accumulate across head-miss replays.
                    //
                    // A resumed turn skips both: the state it continues from is the one
                    // the prior turn built, and the overlay already holds what the repair
                    // took aside.
                    clearWindowState(windowFactory, anchorWindow);
                    if (!finiteHighBound) {
                        // The runtime is now identity while the durable tier still holds
                        // the full history, and everything that rebuilds it can throw.
                        // Mark before the scan, not after, so an unwind leaves the view
                        // knowing it must rebuild before a later turn drains over these
                        // accumulators.
                        //
                        // The predicate is "no overlay was captured", which finiteHighBound
                        // decides: the capture just above runs under it, so when it holds
                        // the session's close() puts the pre-repair runtime back as the
                        // turn unwinds and marking here would escalate a recoverable fault
                        // into a full recompute that also discards the checkpoint timeline.
                        // It is NOT the same as "unlocalized" - a localized repair whose
                        // plan keeps an EOF high bound has finiteHighBound false, captures
                        // nothing, and does need the mark. A restore that itself fails
                        // raises the flag through endRepairSession and settleRepairRuntime.
                        markWindowStateDirty(instance);
                    }
                }

                // Opened once per repair, not once per turn: the rows emitted so far sit
                // uncommitted in this writer, so a repair that yields hands it to the
                // session rather than closing it - closing rolls them back.
                WalWriter walWriter = resuming ? resumed.takeWalWriter() : engine.getWalWriter(instance.getLiveViewToken());
                boolean walWriterRetained = false;
                try {
                    RecordToRowCopier copier = ensureCopier(instance, walWriter);
                    try (RecordCursor pageCursor = pageFrameFactory.getCursorInTimestampRange(
                            executionContext,
                            turnLowTs,
                            scanHighTs
                    )) {
                        RecordCursor source = pageCursor;
                        if (filter != null) {
                            filteringCursor.of(source, filter, executionContext);
                            source = filteringCursor;
                        }
                        if (timelineCapture != null) {
                            // Below the anchor dispatch on purpose: a boundary this
                            // replay crosses must freeze before the crossing row
                            // resets any partition, not after.
                            boundaryFreezingCursor.of(
                                    source,
                                    timelineCapture,
                                    repairBoundaries,
                                    null,
                                    windowFactory.getWindowFunctions(),
                                    anchorWindow,
                                    session,
                                    capturedBoundaries,
                                    pageFrameFactory.getMetadata().getTimestampIndex()
                            );
                            boundaryFreezingCursor.setRowPosition(durableRowsBelowFloor + appendedRows);
                            source = boundaryFreezingCursor;
                        }
                        source = compiledPlan.wrapWindowInput(source, executionContext);
                        if (anchorWindow != null) {
                            anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                            source = anchorDispatchingCursor;
                        }
                        try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                            RecordCursor outCursor = compiledPlan.wrapWindowOutput(windowCursor, executionContext);
                            Record outRecord = outCursor.getRecord();
                            // Designated timestamp of the group the replay is inside, and
                            // how many of its rows are already folded into the window
                            // state. A turn may stop anywhere, including mid-group, so this
                            // pair is what the next turn re-enters on. A resumed turn starts
                            // holding the pair the prior one left.
                            long groupTs = resuming ? turnLowTs : Numbers.LONG_NULL;
                            long groupFoldedRows = resuming ? resumed.getResumeSkipRows() : 0;
                            if (resuming && groupFoldedRows > 0) {
                                // Those rows are in the window state already - the prior turn
                                // folded them and emitted them - so they must not reach the
                                // window cursor again. Skip below the anchor dispatch too, or
                                // an anchored view would re-reset the partitions they opened.
                                // Skipping after the cursor chain is built, not before, because
                                // getIncrementalCursor rewinds it.
                                repairSkipCounter.set(groupFoldedRows);
                                (filter != null ? filteringCursor : pageCursor)
                                        .skipRows(repairSkipCounter, RecordCursor.UNBOUNDED_ROW_COUNT);
                            }
                            final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
                            // Drive the projected cursor rather than the window one - it is
                            // what advances the projection's per-row memoization before the
                            // record is read. Nothing else invalidates a memoizer's cache:
                            // wrapWindowOutput's of() re-inits the functions but leaves the
                            // cached value valid, so the replay would emit the value the
                            // preceding drain left behind for every row it re-emits.
                            while (outCursor.hasNext()) {
                                // The turn budget below ends a localized repair, but only a
                                // localized one: an unlocalized rebuild recomputes the whole
                                // view in this loop and may not yield, so the breaker is the
                                // only thing that stops it early. It answers to DROP,
                                // invalidation and engine shutdown - none of which is worth
                                // finishing a rebuild for, and all of which otherwise wait it
                                // out.
                                circuitBreaker.statefulThrowExceptionIfTripped();
                                long ts = outRecord.getTimestamp(cursorTimestampIndex);
                                // Segmenting the replay at the logical boundaries it
                                // crosses happens one level down, in
                                // boundaryFreezingCursor: hasNext() above has already
                                // folded this row into the window state, so freezing a
                                // boundary below it from here would carry this row into
                                // a root that must not hold it.
                                if (ts == groupTs) {
                                    groupFoldedRows++;
                                } else {
                                    groupTs = ts;
                                    groupFoldedRows = 1;
                                }
                                scannedRows++;
                                // Anything below R is a warm-up row: the window functions
                                // advanced over it, which is the only reason it was read. Its
                                // durable output is already correct and the replacement does
                                // not reach it, so emitting it would duplicate a row the LV
                                // table still holds.
                                if (ts >= emitLowTs) {
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
                                    if (timelineCapture != null) {
                                        // Keep the freeze cursor's row position in step:
                                        // the next boundary it freezes sits below the row
                                        // after this one, so it carries this row's position.
                                        boundaryFreezingCursor.setRowPosition(durableRowsBelowFloor + appendedRows);
                                    }
                                }
                                if (mayYield && session != null && isRepairReplayBudgetSpent(scannedRows)) {
                                    // Out of budget. This row is folded and, if it qualified,
                                    // emitted, so the next turn re-opens at its timestamp and
                                    // skips the rows of that group it has already seen.
                                    // Nothing is committed or published here, so the durable
                                    // view stays the pre-repair one until the final turn.
                                    yielded = true;
                                    resumeFromTs = ts;
                                    resumeSkipRows = groupFoldedRows;
                                    break;
                                }
                            }
                            // Boundaries above the last row the replay saw. No qualifying
                            // row sits between them and that row, so the state the replay
                            // ends on is their state too - and it is bounded above by H,
                            // which every one of them is below. A turn that yielded owes
                            // them the rows it has not read yet, so it freezes none.
                            if (!yielded && timelineCapture != null) {
                                boundaryFreezingCursor.freezeRemaining();
                            }
                            if (timelineCapture != null) {
                                capturedBoundaries = boundaryFreezingCursor.getCaptured();
                            }
                            // Capture base rows scanned before the cursor chain closes
                            // (FilteringRecordCursor.close() resets its counter). No
                            // filter -> scan equals the rows the window cursor produced;
                            // a filter makes scan exceed it by the rows it dropped. A
                            // yielding turn counts the row it stopped on, which the next
                            // turn reads again - the only double-count, and one row wide.
                            o3ScanRows += filter != null ? filteringCursor.getBaseRowsConsumed() : scannedRows;
                        }

                        // Every candidate root the repair owed is frozen and the runtime
                        // disposition is fixed. The replacement commits only from here,
                        // never before: a commit the roots do not describe leaves durable
                        // output with no state version to recover it from. A turn that
                        // yielded owes roots it has not read the rows for, so it parks
                        // instead - and commits nothing, which is what leaves the durable
                        // view as the repair found it.
                        if (yielded) {
                            session.suspend(
                                    reader,
                                    walWriter,
                                    timelineCapture,
                                    resumeFromTs,
                                    resumeSkipRows,
                                    capturedBoundaries,
                                    appendedRows,
                                    o3ScanRows,
                                    replayMinTs,
                                    replayMaxTs
                            );
                            walWriterRetained = true;
                            timelineCapture = null;
                        } else {
                            repairPublication.candidateReady(runtimeDisposition(overlayCaptured));
                        }
                        if (!yielded && (appendedRows > 0 || localized)) {
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
                            //
                            // A localized rebuild answers all of this with R directly: it
                            // re-emitted every qualifying row in [R, H), so anything the
                            // LV table still holds in there is stale whatever produced it -
                            // a dropped filter row, a dedup replacement or a base removal.
                            // R already sits at or below the trigger ts, so the clamp above
                            // could only raise it. The commit is unconditional here: an empty
                            // emit range means the base no longer has a qualifying row in
                            // [R, H), and the rows the LV table still holds there are ghosts
                            // that the truncating replacement has to clear.
                            final long replaceLowTs = localized
                                    ? emitLowTs
                                    : fullRebuild
                                      ? viewLowerBoundTimestamp
                                      : triggerLowTs != Numbers.LONG_NULL
                                        ? Math.min(replayMinTs, triggerLowTs)
                                        : replayMinTs;
                            // The replacement's high bound is the same H the scan stopped at,
                            // so what the replay did not re-evaluate it also does not delete.
                            // Positive infinity otherwise, which is the truncating
                            // replacement every rebuild issued before the bound existed.
                            final long replaceHighTs = finiteHighBound
                                    ? plan.getHighTsExclusive()
                                    : Long.MAX_VALUE;
                            fencedLiveViewCommit(instance, () -> walWriter.commitLiveViewWithReplaceRange(
                                    effectiveSeqTxn,
                                    replaceLowTs,
                                    replaceHighTs
                            ));
                            repairPublication.replacementCommitted(walWriter.getLastSeqTxn());
                        }
                    }
                } finally {
                    if (!walWriterRetained) {
                        // Not parked, so nothing else owns the writer. Closing it rolls
                        // back anything the commit above did not take, which is what an
                        // unwinding turn wants.
                        walWriter.close();
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
                markWindowStateDirty(instance);
                repairPublication.candidateReady(runtimeDisposition(overlayCaptured));
                try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                    fencedLiveViewCommit(instance, () -> walWriter.commitLiveViewWithReplaceRange(
                            effectiveSeqTxn,
                            deleteLowTs,
                            Long.MAX_VALUE
                    ));
                    repairPublication.replacementCommitted(walWriter.getLastSeqTxn());
                }
                LOG.info().$("live view O3 head-miss replay cleared emptied range [view=")
                        .$(viewName)
                        .$(", deleteLowTs=").$(deleteLowTs)
                        .$(", effectiveSeqTxn=").$(effectiveSeqTxn).I$();
            }
            if (!yielded && !repairPublication.isAtOrAfter(RepairPublicationStage.CANDIDATE_ROOTS_AND_RUNTIME_READY)) {
                // A rebuild that replaced nothing: the probe found no surviving row and
                // the trigger authorised no deletion, so the empty candidate set is
                // still this repair's candidate set and the runtime still has to be
                // settled below.
                repairPublication.candidateReady(runtimeDisposition(overlayCaptured));
            }
            replayCompleted = true;
        } finally {
            // Drops the boundary schedule, the capture and the runtime this turn
            // handed the freeze cursor. Its freeze counter is already read back
            // into capturedBoundaries, and a resumed turn re-arms it from there.
            boundaryFreezingCursor.clear();
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
            if (!yielded
                    && timelineCapture != null
                    && (!replayCompleted || capturedBoundaries < repairBoundaries.size())) {
                // The replay is unwinding, or stopped short of a boundary it owed a
                // root version, so the splice below never publishes.
                //
                // A parked repair owes those boundaries by design and has handed the
                // capture to its session, so it keeps the timeline it is going to
                // splice into.
                timelineCapture = Misc.free(timelineCapture);
                session.discardDescriptor();
                repairBoundaries.clear();
                if (repairPublication.hasCommittedReplacement()) {
                    // The durable output has moved under every root and no splice
                    // corrected them, so the retire this repair displaced on its first
                    // turn has to happen after all: a timeline nothing corrects must not
                    // outlive the output it describes.
                    retireCheckpointTimeline(instance);
                }
                // Otherwise the candidate is discarded having changed nothing durable -
                // a cancelled turn is the ordinary case - and the generation the capture
                // pinned still describes exactly the output on disk: it was never
                // advanced, the replacement never committed, and the watermarks the
                // publication tail moves are untouched, so the change stays unconsumed
                // and a later turn replans it. Retiring here instead would delete every
                // historical root and leave that replan with no anchor below the
                // correction, which is the age-unbounded rebuild the timeline exists to
                // avoid.
            }
            if (!replayCompleted) {
                // The turn is unwinding, so the publication tail below never runs and
                // nothing else would end the repair. Release the session here instead,
                // which also unblocks refresh for the view: a resumed turn that failed
                // must not leave the instance pointing at a candidate whose resources
                // the unwind has already taken apart.
                endRepairSession(instance, session);
            }
        }

        if (yielded) {
            // Parked with the pinned reader, the uncommitted replacement and the
            // staged roots in the session, and the runtime standing where the replay
            // left it. Refresh for this view is blocked until a later turn on this
            // worker finishes the repair.
            instance.setSuspendedRepair(session);
            if (suspendedRepairViews.indexOf(instance) < 0) {
                suspendedRepairViews.add(instance);
            }
            LOG.info().$("live view O3 repair yielded on its turn budget [view=")
                    .$(viewName)
                    .$(", turns=").$(session.getTurns())
                    .$(", resumeFromTs=").$(resumeFromTs)
                    .$(", highTsExclusive=").$(plan.getHighTsExclusive())
                    .$(", rootsVersioned=").$(capturedBoundaries)
                    .$(", rootsOwed=").$(repairBoundaries.size() - capturedBoundaries)
                    .$(", rowsScanned=").$(o3ScanRows)
                    .$(", rowsEmitted=").$(appendedRows).I$();
            return true;
        }

        try {
            if (repairPublication.hasCommittedReplacement()) {
                // Post-commit reconciliation. The replacement is durable in the live
                // view's own WAL, but every coordinate the rest of this method derives -
                // the repaired roots' positions, the suffix range-add, the head seal's
                // lvRowPosition - is read off the materialised table, and the consumed
                // watermark declares base transactions the table is meant to hold. So
                // the repair finds out whether the block landed before it commits to any
                // of them, rather than reading a table that does not have the output yet.
                if (reconcileLiveViewReplacement(instance, repairPublication.getCommittedLvSeqTxn())) {
                    repairPublication.replacementApplied();
                }
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
            final boolean replacementReconciled = repairPublication.isReplacementReconciled();
            if (timelineCapture != null && replacementReconciled) {
                // The replacement is applied, so the repaired roots now describe real
                // output and the splice can commit.
                //
                // Every row that moved moved inside [R, H), so the table's total change
                // IS the shift every suffix root's cumulative position owes. Proving
                // that against the two counts read from the pre-repair table is what
                // makes the repaired positions - anchored on the same prefix count -
                // trustworthy: a mismatch means the durable table did not change the way
                // the replacement says, and a wrong lvRowPosition is not something a
                // later restart can detect, only fail on.
                final long durableRowsAfterRepair = instance.getLvRowsTotal();
                final long suffixRowDelta = durableRowsAfterRepair - durableRowsBeforeRepair;
                if (durableRowsBeforeRepair - durableRowsReplaced + appendedRows != durableRowsAfterRepair) {
                    LOG.critical().$("live view replacement row count does not match the repair plan [view=")
                            .$(viewName)
                            .$(", rowsBefore=").$(durableRowsBeforeRepair)
                            .$(", rowsReplaced=").$(durableRowsReplaced)
                            .$(", rowsEmitted=").$(appendedRows)
                            .$(", rowsAfter=").$(durableRowsAfterRepair).I$();
                } else {
                    timelineSplice = publishCheckpointTimelineRepair(
                            instance,
                            timelineCapture,
                            effectiveSeqTxn,
                            plan.getHighTsExclusive(),
                            suffixRowDelta
                    );
                    if (timelineSplice != null) {
                        repairPublication.timelinePublished();
                    }
                }
            }
            // The one runtime exchange, and the first point at which it is safe: the
            // generation that describes the state the primary is about to hold is
            // already published, so a crash from here on restores that generation
            // rather than a runtime nothing recorded.
            settleRepairRuntime(instance, session, windowFactory, anchorWindow);
            if (!replacementReconciled) {
                // The replacement is in the live view's WAL but not in its table. No
                // watermark may walk past output the table does not hold, so this turn
                // stops short and leaves the repair to be repeated: the base range stays
                // unconsumed, the retire below leaves nothing describing superseded
                // output, and the next turn blocks on this same seqTxn until the block
                // lands.
                instance.setPendingReplacementLvSeqTxn(repairPublication.getCommittedLvSeqTxn());
                LOG.critical().$("live view O3 replacement committed but did not apply, deferring repair [view=")
                        .$(viewName)
                        .$(", lvSeqTxn=").$(repairPublication.getCommittedLvSeqTxn())
                        .$(", advanceTo=").$(effectiveSeqTxn).I$();
            } else {
                instance.setLastProcessedSeqTxn(effectiveSeqTxn);
                instance.setAppliedWatermark(effectiveSeqTxn);
                boolean lvConsumedPersisted = false;
                boolean headSealed = false;
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
                repairPublication.watermarkAdvanced();
                if (lvConsumedPersisted && (appendedRows > 0 || repairPublication.isKeepPrimaryRuntime())) {
                    // Post-replay head: retireCheckpointStateOnO3 cleared the head metadata
                    // above, so force seals a fresh boundary reflecting the post-replay state
                    // (firstCp is already true here; force keeps the intent explicit). A
                    // subsequent O3 above it resumes from there instead of rebuilding in full.
                    //
                    // The head's maxTs has to describe the state the checkpoint is about to
                    // serialise: replayMaxTs for a rebuild that ran to the end of the base
                    // table, but the runtime frontier for one that stopped at a finite H and
                    // put its own state back - the restore just rewound the functions past
                    // replayMaxTs, so sealing them under it would claim a boundary the state
                    // does not sit at, and the next O3 would resume from it and re-read rows
                    // the state already holds. The frontier is a real timestamp whenever the
                    // plan tagged a finite H (it had to be at or above H to do so), so this
                    // seals even when the replacement emitted nothing at all - the retire
                    // dropped every boundary, and a view left with none rebuilds from scratch
                    // on the next restart.
                    //
                    // Pass 0 appendedRows: lvRowsTotal already includes them (sourced from the
                    // on-disk size above), so adding them again would double-count
                    // lvRowPosition. Mirrors the seed-completion path.
                    //
                    // A published splice already IS this repair's timeline publication and
                    // appended no root, which is enough only while the newest root it kept
                    // still sits at the frontier: the splice moved the generation's
                    // normalizedBaseSeqTxn up to E, and restart replays (E, durableBase]
                    // alone, so any row above that root came from a base transaction the
                    // replay will not walk and the restored state would never see it. Seal
                    // the frontier as a root of its own whenever it has run past the splice's
                    // head key - the convergence that let the repair keep the primary runtime
                    // is exactly what makes that runtime the correct state there - and leave
                    // the seal to re-stamp the head metadata alone when the two agree.
                    final long headMaxTs = repairPublication.isKeepPrimaryRuntime()
                            ? instance.getLatestSeenTs()
                            : replayMaxTs;
                    // Take the seal's own answer: it swallows every Throwable and also declines a
                    // boundary that does not clear the head, so assuming success would clear the
                    // durable repair marker over a head that was never written.
                    headSealed = maybeWriteHeadCheckpoint(
                            instance,
                            windowFactory,
                            effectiveSeqTxn,
                            headMaxTs,
                            0L,
                            true,
                            timelineSplice == null || headMaxTs > timelineSplice.getHeadRootMaxTimestamp()
                    );
                }
                if (prefixMarkerLive) {
                    // The truncate preserved the prefix behind a live marker. If a
                    // fresh head was just sealed above it the timeline is consistent
                    // again, so drop the marker and let a restart restore normally.
                    // Otherwise the repair emitted nothing to seal (a pure delete to
                    // EOF), leaving a headless truncated timeline: retire it - which
                    // removes the marker - and let a restart rebuild.
                    if (headSealed) {
                        clearCheckpointRepairMarker(instance);
                    } else {
                        retireCheckpointTimeline(instance);
                    }
                }
            }
        } finally {
            if (!repairPublication.isRuntimeSettled()) {
                // The block above unwound before the exchange. Settle anyway: the
                // disposition was fixed before the replacement committed, and a runtime
                // left half in the replay's state and half in the pre-repair state is
                // worse than either. A settle that fails here has already marked the
                // window state for rebuild, so let the original failure propagate.
                try {
                    settleRepairRuntime(instance, session, windowFactory, anchorWindow);
                } catch (Throwable t) {
                    LOG.critical().$("could not settle live view repair runtime [view=")
                            .$(viewName)
                            .$(", error=").$(t).I$();
                }
            }
            if (timelineCapture != null && timelineSplice == null) {
                // Either the splice could not publish, or it was never allowed to try
                // because the replacement has not applied. The output the timeline's
                // roots describe has moved either way, so it must not survive them.
                retireCheckpointTimeline(instance);
            }
            Misc.free(timelineCapture);
            // The candidate is either published - its segments reachable from the new
            // generation - or gone. Either way nothing is left for a startup sweep to
            // discard, so the descriptor's ownership claim retires with it, together
            // with the session that carried the repair across its turns.
            endRepairSession(instance, session);
        }
        // The boundary rebuild is the residual O(view age) fallback (late row below
        // every logical boundary, or a deep / unresumable apply-ahead range). Counted
        // separately from the resume path so a growing value in live_views() flags a
        // view the timeline is failing to bound. A localized rebuild is bounded by the
        // dependency floor instead, so it is not that residual - but it is still the
        // same executor and still counted here.
        instance.bumpO3BoundaryReplayRows(appendedRows);
        // Baseline scan-cost signal: base rows this boundary rebuild pulled (>= emit).
        instance.bumpO3ReplayScanRows(o3ScanRows);
        // applyAheadGap = the seqTxns ApplyWal2TableJob raced past the O3 trigger
        // (effectiveSeqTxn - advanceTo); a wide gap is what forces the rebuild when no
        // sealed anchor sits below the ahead range's minimum in-view ts. scanLowTs /
        // emitLowTs are L and R: equal to the view boundary on an unlocalized rebuild,
        // and the proof of what a localized one did not read when they are not.
        // highTsExclusive is H, LONG_NULL when the rebuild ran to the end of the base
        // table; runtimeStatePreserved says whether the primary runtime kept the state
        // it entered with rather than the state the replay produced.
        LOG.info().$("live view O3 head-miss replay completed [view=")
                .$(viewName)
                .$(", advanceTo=").$(effectiveSeqTxn)
                .$(", applyAheadGap=").$(plan.getPinnedSeqTxn() - plan.getTriggerSeqTxn())
                .$(", localized=").$(localized)
                .$(", scanLowTs=").$(scanLowTs)
                .$(", emitLowTs=").$(emitLowTs)
                .$(", highTsExclusive=").$(finiteHighBound ? plan.getHighTsExclusive() : Numbers.LONG_NULL)
                .$(", runtimeStatePreserved=").$(repairPublication.isKeepPrimaryRuntime())
                .$(", replacementApplied=").$(repairPublication.isReplacementReconciled())
                .$(", turns=").$(session != null ? session.getTurns() + 1 : 1)
                .$(", rowsScanned=").$(o3ScanRows)
                .$(", rowsEmitted=").$(appendedRows).I$();
        return false;
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
     * Which state the compiled factory ends the repair holding. A repair that
     * captured the scratch overlay proved its convergence boundary lands at or
     * below the runtime frontier, so the state it took aside is still correct and
     * goes back; one that did not replaced through the frontier, so the state its
     * replay produced <i>is</i> the runtime.
     */
    private static LiveViewCheckpointRepairPublication.RuntimeDisposition runtimeDisposition(boolean overlayCaptured) {
        return overlayCaptured
                ? LiveViewCheckpointRepairPublication.RuntimeDisposition.KEEP_PRIMARY
                : LiveViewCheckpointRepairPublication.RuntimeDisposition.PROMOTE_REPLAY;
    }

    /**
     * Drives the live view's own WAL apply for the replacement a repair just
     * committed and reports whether the live-view table now holds it. The refresh
     * worker owns the live view's {@code TableWriter} on a primary, so this inline
     * apply is the view's only applier - but it can silently no-op (the writer is
     * busy, or the table backed off under memory pressure) or suspend the table,
     * and neither raises. Comparing the applied writer txn against the seqTxn the
     * commit minted is what turns "the apply ran" into "the replacement landed".
     * <p>
     * Idempotent: a block that is already applied short-circuits without reopening
     * the writer, which is what lets the next refresh turn re-drive the same
     * committed replacement.
     */
    private boolean reconcileLiveViewReplacement(LiveViewInstance instance, long committedLvSeqTxn) {
        final TableToken token = instance.getLiveViewToken();
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        if (tracker.getWriterTxn() >= committedLvSeqTxn) {
            return true;
        }
        if (!simulateRepairApplyFailureForTest) {
            try {
                applyJob.applyWalDirect(token, Job.RUNNING_STATUS);
            } catch (Throwable t) {
                // applyWal2Table suspends the table and returns rather than throwing, so
                // this is defence against a future path that does raise: the check below
                // decides either way and the caller handles an unapplied replacement.
                LOG.critical().$("live view replacement apply failed [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", lvSeqTxn=").$(committedLvSeqTxn)
                        .$(", error=").$(t).I$();
            }
        }
        return tracker.getWriterTxn() >= committedLvSeqTxn;
    }

    /**
     * Re-drives an out-of-order repair's replacement that committed without
     * applying, and reports whether refresh may proceed. A turn that ran over an
     * unapplied replacement would read its own coordinates - the lifetime row
     * count, a head checkpoint's {@code lvRowPosition}, a repaired root's position
     * - off a table that does not hold the output, and would consume base
     * transactions nothing materialised. So the view stays blocked here until the
     * block lands, at which point the deferred repair simply runs again from the
     * base range it never consumed.
     * <p>
     * Called under the refresh latch, before any other work in the turn.
     */
    private boolean reconcilePendingReplacement(LiveViewInstance instance) {
        final long pendingLvSeqTxn = instance.getPendingReplacementLvSeqTxn();
        if (pendingLvSeqTxn == Numbers.LONG_NULL) {
            return true;
        }
        if (!reconcileLiveViewReplacement(instance, pendingLvSeqTxn)) {
            return false;
        }
        instance.setPendingReplacementLvSeqTxn(Numbers.LONG_NULL);
        LOG.info().$("live view deferred O3 replacement applied, resuming refresh [view=")
                .$(instance.getDefinition().getViewName())
                .$(", lvSeqTxn=").$(pendingLvSeqTxn).I$();
        return true;
    }

    /**
     * Performs the repair's single runtime exchange. A {@code KEEP_PRIMARY}
     * disposition hands the scratch overlay - the window-function state, plus the
     * anchor map for an anchored view - back to the compiled factory; a
     * {@code PROMOTE_REPLAY} one leaves the state the replay produced standing,
     * which is already the runtime. Either way it runs at most once per repair:
     * the publication records the exchange before it happens, so an unwinding
     * caller cannot restore twice or restore into a half-rebuilt runtime.
     * <p>
     * A failed exchange leaves the factory holding neither state consistently, so
     * it marks the window state dirty on the way out and the refresh failure
     * handler recomputes it from the applied base rather than continuing over it.
     */
    private void settleRepairRuntime(
            LiveViewInstance instance,
            @Nullable LiveViewCheckpointRepairSession session,
            WindowRecordCursorFactory windowFactory,
            LiveViewWindow anchorWindow
    ) {
        if (repairPublication.isRuntimeSettled()) {
            return;
        }
        final boolean keepPrimary = repairPublication.isKeepPrimaryRuntime();
        repairPublication.runtimePromoted();
        if (keepPrimary) {
            try {
                session.getOverlay().restore(windowFactory.getWindowFunctions(), anchorWindow);
            } catch (Throwable t) {
                markWindowStateDirty(instance);
                throw t;
            }
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
     *     offset from the checkpoint timeline's newest root (restart mid-sweep),
     *     or starts from offset 0 with empty state (fresh CREATE, or no usable
     *     timeline). Later turns continue from the in-memory window state +
     *     offset ({@code getIncrementalCursor} preserves accumulated state
     *     across turns), so no per-turn restore is needed.</li>
     *     <li>The first turn pins ONE MVCC base snapshot (an
     *     {@link LiveViewInstance#getSeedBaseReader() instance-held reader}) at
     *     {@code sweepSeqTxn >= seedTargetSeqTxn} and every turn reads that same
     *     snapshot; re-opening at the latest applied seqTxn each turn would make the
     *     positional {@code skipRows()} resume unsound under concurrent out-of-order
     *     base commits (they reorder physical rows below the swept prefix). Each turn
     *     {@code skipRows()} past already-swept rows, feeds up to a row/duration
     *     budget, commits the batch, applies it, and seals a boundary on the
     *     checkpoint cadence.</li>
     *     <li>On cursor exhaustion the turn retires the sweep's own boundaries,
     *     seals one steady boundary from the now-complete state, flips
     *     {@code seedState} to ACTIVE and releases the pinned snapshot; the next
     *     tick begins the deferred drain from {@code sweepSeqTxn + 1}, where the
     *     ACTIVE phase's O3 detection materialises anything the base committed
     *     after the snapshot.</li>
     * </ul>
     * Crash idempotency: the on-disk output is a deterministic prefix of the
     * eventual result, so a re-feed past the last sealed boundary recomputes
     * rows already on disk to advance state but skips their WAL append
     * ({@code skipWriteUntil}). A crash before any boundary re-sweeps from
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
            boolean restored = false;
            if (restoreSeedFromTimeline(instance, windowFactory, restoredSeedState)) {
                // A surviving seed root can be AHEAD of the on-disk LV output. A
                // checkpoint restore no longer produces one - TableSnapshotRestore wipes
                // the live _checkpoints/ dir and lays the snapshot's back down, so the
                // restored timeline matches the rolled-back _txn/partitions/_lv.s - but a
                // backup that omits the dir, or a crash between the seal and the LV
                // commit, still can: the live-ahead root (lvRowsTotal = R_bcp) outlives
                // the disk it describes (onDiskLvRows = R_cp < R_bcp). Resuming from it
                // would jump the data cursor past the base rows that produced
                // R_cp..R_bcp while lvRowsTotal starts at R_bcp, so those LV output rows
                // would be neither on disk nor re-swept - a permanent silent gap. Reject
                // the ahead root and fall through to the from-0 re-sweep below, where the
                // skip-write floor keeps the R_cp on-disk prefix and re-emits everything
                // above it.
                if (restoredSeedState.lvRowsTotal <= onDiskLvRows) {
                    instance.setSeedDataOffset(restoredSeedState.resumeDataOffset);
                    instance.setLvRowsTotal(restoredSeedState.lvRowsTotal);
                    if (restoredSeedState.maxTimestamp != Numbers.LONG_NULL) {
                        instance.setLatestSeenTs(restoredSeedState.maxTimestamp);
                    }
                    instance.recordSeedCheckpointWritten(
                            restoredSeedState.resumeDataOffset,
                            restoredSeedState.maxTimestamp,
                            Numbers.LONG_NULL
                    );
                    restored = true;
                } else {
                    // restoreSeedFromTimeline already wrote the ahead window state into
                    // the functions; wipe it back to identity for the from-0 re-sweep.
                    // The retire below takes the ahead root with it, so the re-sweep's
                    // own boundaries do not have to climb past its maxTimestamp.
                    clearWindowState(windowFactory, anchorWindow);
                    LOG.info().$("live view discarding seed checkpoint ahead of restored on-disk output [view=")
                            .$(viewName).$(", checkpointLvRows=").$(restoredSeedState.lvRowsTotal)
                            .$(", onDiskLvRows=").$(onDiskLvRows).I$();
                }
            }
            if (!restored) {
                // Fresh CREATE, no timeline, an unreadable one, one holding no seed
                // resume point, or one rejected as ahead of the restored disk: re-sweep
                // from offset 0 with empty state. The on-disk prefix (if any) is a
                // deterministic match, kept via skip-write below.
                //
                // Re-clear the window state unconditionally: a seed restore that threw
                // partway has already written the anchor + some functions into the live
                // window before failing, and getIncrementalCursor keeps accumulator state
                // across the re-sweep, so feeding that half-restored state into a from-0
                // sweep would double-count. The ahead-rejection branch above already
                // re-clears; this covers the throw path. Cheap and idempotent for the
                // fresh / no-timeline cases (nothing was restored).
                clearWindowState(windowFactory, anchorWindow);
                // Retire whatever the timeline holds. Every root in it describes a
                // sweep prefix this re-sweep is about to recompute from scratch, and
                // the append refuses a boundary at or below the current head, so
                // leaving them would silently starve the re-sweep of resume points.
                retireSeedCheckpointTimeline(instance);
                instance.setSeedDataOffset(0);
                instance.setLvRowsTotal(0);
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

            final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
            final Function filter = compiledPlan.getFilter();
            final PageFrameRecordCursorFactory pageFrameFactory = compiledPlan.getPageFrameFactory();
            RecordMetadata outMetadata = compiledPlan.getOutputMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();
            if (cursorTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(viewName).put(']');
            }

            try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                RecordToRowCopier copier = ensureCopier(instance, walWriter);
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
                    source = compiledPlan.wrapWindowInput(source, executionContext);
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
                        // Bound after the skip above, not before: wrapWindowOutput does not
                        // rewind, so it neither undoes the skip nor cares that it happened.
                        RecordCursor outCursor = compiledPlan.wrapWindowOutput(windowCursor, executionContext);
                        Record outRecord = outCursor.getRecord();
                        while (outCursor.hasNext()) {
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
                        fencedLiveViewCommit(instance, () -> walWriter.commitLiveView(sweepSeqTxn));
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

        // Sweep complete. Seal the steady boundary from the now-complete window
        // state (maxTs = overall latestSeenTs, not this possibly-empty final
        // turn's batchMaxTs) so the ACTIVE phase's restart-restore and O3 resume
        // have a root. lvRowsTotal is already maintained above, so pass 0
        // appendedRows to avoid double-counting it.
        //
        // The seal runs BEFORE the _lv.s persist below - deliberately the reverse
        // of every steady-state site, which persists _lv.s first and seals after.
        // Do not "fix" this to match them. Those sites advance a watermark over
        // rows already on disk under an already-existing boundary, where a head
        // lagging the watermark is the routine cadence state and replayToApplied
        // closes the gap. This is where the FIRST boundary is born, and the _lv.s
        // persist is what flips the view durably ACTIVE at sweepSeqTxn. Persisting
        // that first would open a window where a crash leaves an ACTIVE view whose
        // disk table holds the whole swept output but which has no timeline at all:
        // the restart then finds nothing to restore, and on a live primary (base WAL
        // present) the applied-base re-derive does not trigger, so the view drains
        // forward from cold accumulators and durably commits wrong cumulative
        // results. Sealing first makes every crash window degrade safely - before
        // the _lv.s persist the view is still SEEDING on disk and simply re-sweeps
        // from offset zero over the deterministic prefix already on disk; after it,
        // the boundary is already there.
        instance.setLastProcessedSeqTxn(sweepSeqTxn);
        instance.setAppliedWatermark(sweepSeqTxn);
        // Retire the sweep's own boundaries first. They resume a cursor, not a
        // forward replay, so none of them may outlive the sweep as an O3 anchor;
        // the seal below replaces the lot with one boundary over the finished
        // state. A crash in between leaves a SEEDING view with no timeline, which
        // re-sweeps from offset zero - the same disposition a crash before the
        // first cadence event already had.
        retireSeedCheckpointTimeline(instance);
        // Only when the seed actually emitted a row. A seed that qualified none - the normal
        // outcome for START FROM NOW over a base of past data, and for any boundary in the
        // future - has nothing to anchor a head on: latestSeenTs is only stamped per emitted
        // row, so it is still LONG_NULL, and the window accumulators are at identity. Writing
        // a head from that would persist maxTs = LONG_NULL, and the O3 head-hit path floors
        // its replay at headMaxTs + 1: Long.MIN_VALUE + 1 admits every base row, so the first
        // out-of-order commit would replay the whole base into the view, including the
        // sub-boundary rows the view exists to exclude. With no boundary, that commit routes
        // to the rebuild instead, which floors at viewLowerBoundTimestamp; the flush cadence
        // seals the first real boundary once rows land. An empty view rebuilt from cold
        // accumulators is correct by construction, so the "never leave an ACTIVE view without
        // a boundary" argument below does not apply here - there is no output to be wrong
        // about.
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
            // Persists seedState=ACTIVE + watermarks durably, over a timeline that
            // already holds the finished boundary alone.
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
        LOG.info().$("live view seed sweep completed [view=")
                .$(viewName)
                .$(", seedTargetSeqTxn=").$(seedTargetSeqTxn)
                .$(", sweepSeqTxn=").$(sweepSeqTxn)
                .$(", lvRowsTotal=").$(instance.getLvRowsTotal()).I$();
    }

    /**
     * Retires the boundaries a seed sweep sealed and clears the in-memory seed
     * cadence markers. Called when the sweep completes, and whenever a resume is
     * abandoned for a re-sweep from offset zero.
     * <p>
     * A sweep turn ends wherever its row/duration budget runs out, which can cut
     * a timestamp tie in half: the root it seals then holds state for only part
     * of the rows at its {@code maxTimestamp}. That is sound for the sweep's own
     * positional resume, which continues from the cursor offset rather than from
     * the boundary timestamp, but it is not a boundary an O3 repair may anchor a
     * forward replay at. The sweep's roots therefore do not outlive it: the
     * completion path retires them and seals one steady boundary over the
     * finished state, and the ACTIVE view starts from that alone.
     */
    private void retireSeedCheckpointTimeline(LiveViewInstance instance) {
        retireCheckpointTimeline(instance);
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
        instance.clearSeedCheckpoint();
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
            try {
                // The job's breaker trips on engine shutdown and on the per-view cancel
                // flag a DROP or an invalidation sets, so neither has to wait out the
                // remaining budget. It has to leave through the breaker rather than as a
                // plain failure: handleRefreshFailure skips the flush-retry budget only
                // for a cancellation, and counting one would invalidate the view durably
                // on the way down - the exact hazard its comment describes.
                executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
            } catch (Throwable th) {
                reader.close();
                throw th;
            }
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
     * Commits one localized repair's timeline range splice: the roots the
     * replay froze into {@code capture} take new payload versions under their
     * existing {@code checkpointId}s, the prefix below {@code C} and the
     * converged suffix at or above {@code highTsExclusive} keep theirs, and one
     * persistent range-add shifts every suffix root's cumulative
     * {@code lvRowPosition} by {@code suffixRowDelta}.
     * <p>
     * Ordered after the replacement is committed and applied, so the generation this
     * publishes is valid against durable output rather than a candidate one
     * ({@code LV_REPLACEMENT_APPLIED -> TIMELINE_GENERATION_PUBLISHED}). A crash in
     * between leaves the previous generation authoritative and the repair repeatable;
     * a failure returns null and the caller retires the timeline instead, because
     * the durable output has already moved under every root it holds.
     *
     * @return the splice's result when the superblock committed the new
     * generation, null when it did not
     */
    private LiveViewCheckpointTimelineStoreWriter.RepairResult publishCheckpointTimelineRepair(
            LiveViewInstance instance,
            LiveViewCheckpointTimelineStoreWriter.RepairCapture capture,
            long normalizedBaseSeqTxn,
            long highTsExclusive,
            long suffixRowDelta
    ) {
        try {
            final long coveredLvSeqTxn = engine.getTableSequencerAPI()
                    .getTxnTracker(instance.getLiveViewToken())
                    .getWriterTxn();
            final LiveViewCheckpointTimelineStoreWriter.RepairResult result;
            // Node-local splice over node-local output, on either role - see
            // appendCheckpointTimelineRoot for why only the role read lock survives here.
            final Lock roleLock = engine.getRoleSwitchReadLock();
            roleLock.lock();
            try {
                result = checkpointTimelineStoreWriter.publishRepair(
                        capture,
                        instance.getLiveViewToken().getTableId(),
                        normalizedBaseSeqTxn,
                        coveredLvSeqTxn,
                        0,
                        true,
                        highTsExclusive,
                        suffixRowDelta
                );
            } finally {
                roleLock.unlock();
            }
            instance.recordCheckpointTimelineWalPurgeFloor(result.getWalPurgeFloor());
            instance.recordCheckpointTimelineStats(result.getStats());
            instance.recordCheckpointRepairSplice(
                    result.getRootsVersioned(),
                    result.getDataBytesAdded() + result.getMetadataBytesAdded()
            );
            LOG.info().$("live view checkpoint timeline repair published [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", generation=").$(result.getGeneration())
                    .$(", rootsVersioned=").$(result.getRootsVersioned())
                    .$(", highTsExclusive=").$(highTsExclusive)
                    .$(", suffixRowDelta=").$(result.getSuffixRowDelta())
                    .$(", suffixBreakpointTs=").$(result.getSuffixBreakpointTimestamp())
                    .$(", newBytes=").$(result.getDataBytesAdded() + result.getMetadataBytesAdded()).I$();
            return result;
        } catch (Throwable t) {
            instance.recordCheckpointRepairFailure();
            LOG.critical().$("could not publish live view checkpoint timeline repair [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", highTsExclusive=").$(highTsExclusive)
                    .$(", suffixRowDelta=").$(suffixRowDelta)
                    .$(", error=").$(t).I$();
            return null;
        }
    }

    /**
     * Head-checkpoint write hook. Computes the per-LV snapshot capability on the
     * first call, accumulates the cycle's row count into the cadence counter, and
     * seals a fresh logical checkpoint boundary into the versioned timeline when
     * either trigger has fired (or this is the first commit and no boundary
     * exists yet).
     * <p>
     * Capability gate: AND of every compiled window function's
     * {@code supportsCheckpointState()} plus, when the LV has an anchored window,
     * codec support for the partition-key column shape. Computed once and
     * cached on the {@link LiveViewInstance}. A {@code false} cap stays false
     * for the LV's lifetime and the hook is a permanent no-op: the LV seals
     * no boundary and routes restart / O3 through the from-base rebuild.
     * <p>
     * Cadence triggers (whichever fires first):
     * <ul>
     *     <li>{@code rowsSinceLastCheckpointWritten >= cairo.live.view.checkpoint.rows}.</li>
     *     <li>Wall-clock distance from the prior seal exceeds
     *     {@code cairo.live.view.checkpoint.max.duration.micros}.</li>
     *     <li>No boundary exists yet (first seal ever for this LV) and at least
     *     one row landed - guarantees a usable root ASAP for restart-replay
     *     bounding, with the duration trigger floor active from then on.</li>
     * </ul>
     * <p>
     * A failure here does not invalidate the view: the timeline is derived state,
     * the previously published generation stays authoritative, and the next
     * eligible cycle seals again.
     */
    /**
     * @return true when a head checkpoint was actually sealed. False covers every skip - the
     * capability and cadence gates, a boundary that did not clear the head, and a swallowed
     * write failure - so a caller must not treat the call as having sealed unconditionally.
     */
    private boolean maybeWriteHeadCheckpoint(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lvSeqTxn,
            long batchMaxTs,
            long appendedRows,
            boolean force
    ) {
        return maybeWriteHeadCheckpoint(instance, windowFactory, lvSeqTxn, batchMaxTs, appendedRows, force, true);
    }

    /**
     * As above, with {@code appendTimelineRoot} deciding whether the seal adds a
     * logical boundary or only re-stamps the head metadata.
     * <p>
     * Only a repair that published a timeline range splice passes
     * {@code false}, and only when the splice's newest root already sits at the
     * frontier the seal is about to stamp: that generation is published, the
     * runtime stands exactly where the repair found it, and appending would
     * claim a boundary duplicating the head root. A splice whose frontier ran
     * past its newest root passes {@code true} instead - see the call site for
     * why a root there is what keeps the generation's base coverage honest.
     */
    private boolean maybeWriteHeadCheckpoint(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long lvSeqTxn,
            long batchMaxTs,
            long appendedRows,
            boolean force,
            boolean appendTimelineRoot
    ) {
        // Under symmetric local refresh every node --
        // primary or replica -- owns and seals its own node-local checkpoint timeline for restart
        // recovery; nothing replicates. So this seal runs on every role.
        if (!instance.isSnapshotCapabilityComputed()) {
            instance.setSnapshotCapability(computeSnapshotCapability(instance, windowFactory));
        }
        if (!instance.isSnapshotCapability()) {
            return false;
        }
        // A boundary with no maxTs has no place in a timeline keyed on
        // (maxTimestamp, checkpointId): the resume floors at maxTs + 1, and
        // LONG_NULL + 1 would admit every base row. Every caller reaches here with
        // rows behind it (appendedRows / flushRows > 0), so batchMaxTs is a real
        // timestamp today; this guard keeps a future force-caller from sealing a
        // poison boundary past the cadence gate below.
        if (batchMaxTs == Numbers.LONG_NULL) {
            return false;
        }

        instance.addRowsSinceLastCheckpointWritten(appendedRows);

        final long rowsCadence = engine.getConfiguration().getLiveViewCheckpointRows();
        final long durationCadence = engine.getConfiguration().getLiveViewCheckpointMaxDurationMicros();
        final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        // A cooldown is armed only after MAX_CONSECUTIVE_SEAL_FAILURES proved the fault
        // deterministic, so suppress the seal here whatever triggered it - force
        // included. A forced O3 seal would fail the same way and be swallowed the same
        // way, leaving the same cleared head it leaves now, so letting it through buys
        // nothing and pays the whole re-stream for it. The row counter above keeps
        // accruing, so the first cycle past the cooldown seals at once rather than
        // waiting out a fresh cadence.
        if (instance.isSealOnCooldown(nowUs)) {
            return false;
        }
        final long lastWrittenUs = instance.getLastCheckpointWrittenUs();
        final long priorLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
        final boolean firstCp = priorLvSeqTxn == Numbers.LONG_NULL;
        final boolean rowTrigger = instance.getRowsSinceLastCheckpointWritten() >= rowsCadence;
        final boolean durationTrigger = !firstCp
                && lastWrittenUs != Numbers.LONG_NULL
                && (nowUs - lastWrittenUs) >= durationCadence;
        // A head carrying no write time is one THIS process never sealed: startup
        // stamped it from the selected generation, or the restart restore stamped
        // it from the root it restored. Either leaves the cadence with no baseline
        // - the duration trigger above disables itself outright without a
        // lastWrittenUs, and the row counter restarts from zero - so the restored
        // boundary would stay the newest one until an O3 forces a seal or a full
        // rowsCadence accumulates. Densify above it on the first flush instead, so
        // a post-restart O3 resumes from a near boundary rather than the restored
        // one.
        //
        // Gated on a real batchMaxTs for the reason the guard above states. The
        // non-force callers already only reach here with rows behind them
        // (appendedRows > 0 / flushRows > 0), so this holds by construction; state
        // it locally rather than rely on four call sites keeping it.
        final boolean restoredHeadFirstFlush = !firstCp
                && lastWrittenUs == Numbers.LONG_NULL
                && batchMaxTs != Numbers.LONG_NULL;
        // force fires the seal past the row/duration cadence gate. The O3 replay
        // paths pass it so an O3 always seals a fresh near-head boundary: the
        // repair clears the head first, so firstCp is normally true anyway, but a
        // splice that keeps its timeline does not, and cadence could then skip the
        // seal and strand the head at the stale maxTs.
        if (!(force || firstCp || restoredHeadFirstFlush || rowTrigger || durationTrigger)) {
            return false;
        }

        boolean sealed = false;
        try {
            // The base commit this root covers. Mirrored onto the instance below
            // so WalPurgeJob can hold the base WAL purge floor here rather than at
            // the applied point.
            final long baseSeqTxn = instance.getLastProcessedSeqTxn();
            final LiveViewWindow anchorWindow = instance.getAnchorWindow();
            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
            // 0 for a splice, which appends no root: the newest boundary is one the
            // splice reused, and its byte figure is restamped by the next cadence
            // seal (or by a restart, from the root it restores). The column is
            // diagnostic, so a transient 0 there costs nothing.
            long stateBytes = 0L;
            long rootCheckpointId = Numbers.LONG_NULL;
            if (appendTimelineRoot) {
                final LiveViewCheckpointTimelineStoreWriter.Result appended = appendCheckpointTimelineRoot(
                        instance,
                        functions,
                        anchorWindow,
                        baseSeqTxn,
                        batchMaxTs,
                        Numbers.LONG_NULL
                );
                stateBytes = appended.getLogicalStateBytes();
                rootCheckpointId = appended.getCheckpointId();
            }
            // Advance the head only after the generation carrying this root is
            // durable. WalPurgeJob min-combines getHeadCheckpointBaseSeqTxn(), so
            // the head carries the base WAL purge floor, and restart's
            // replayToApplied re-feeds raw base WAL from the restored root's base
            // seqTxn: a head that ran ahead of the published generation would
            // release WAL a restart still needs.
            instance.setHeadCheckpoint(lvSeqTxn, baseSeqTxn, batchMaxTs, stateBytes, nowUs);
            if (rootCheckpointId != Numbers.LONG_NULL) {
                // This head mirrors the root just appended, frozen from the state
                // windowFactory's functions hold right now. A splice appends none
                // and leaves the identity cleared, which denies the O3 resume's
                // runtime-anchor reuse rather than letting it match on maxTs alone.
                instance.setHeadCheckpointRoot(rootCheckpointId, windowFactory);
            }
            // Baseline observability: elapsed micros of this head-checkpoint write
            // (state freeze + root append + generation publish), measured from the
            // cadence-gate clock read above. Surfaced via
            // live_views().checkpoint_last_write_micros.
            instance.recordCheckpointWriteMicros(engine.getConfiguration().getMicrosecondClock().getTicks() - nowUs);
            // Clears the failure streak and any armed cooldown. A seal that got
            // through proves whatever rejected the previous ones has passed.
            instance.recordSealSuccess();
            sealed = true;
        } catch (LiveViewCheckpointTimelineStoreWriter.BoundaryNotAboveHeadException e) {
            // Every row this cycle emitted sat on the head boundary's own designated
            // timestamp, so the group that boundary covers grew instead of a new one
            // opening above it. A normal root only ever extends the timeline upwards,
            // which leaves nothing to seal. Ordinary data reaches here - a timestamp
            // that spans two refresh cycles - so it is a skipped cadence, not a failed
            // one: the head, the cadence counters and the batch-minimum the next seal
            // shares chunks against all stay where they are, and the next cycle to
            // reach a higher timestamp seals this cycle's rows along with its own.
            LOG.debug().$("live view head checkpoint boundary not above head, seal skipped [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", head=").$ts(instance.getHeadCheckpointMaxTs())
                    .$(", candidate=").$ts(batchMaxTs).I$();
        } catch (Throwable t) {
            // Derived state: the seal failed, so the head and the cadence counters
            // stay parked on the previous root and the next eligible cycle seals
            // again. Any temporary segment the failed append staged is reclaimed by
            // the next lifecycle reconciliation.
            final int streak = instance.recordSealFailure();
            LOG.critical().$("could not write live view head checkpoint [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", lvSeqTxn=").$(lvSeqTxn)
                    .$(", consecutiveFailures=").$(streak)
                    .$(", error=").$(t).I$();
            if (streak >= MAX_CONSECUTIVE_SEAL_FAILURES) {
                retireCheckpointStateAfterRepeatedSealFailure(instance, nowUs, streak);
            }
        }
        // Best-effort maintenance, kept off the seal's own try so a compaction fault never reads
        // as a failed head write. It publishes its own generation after the seal's is durable, so
        // it sits outside any reconcile orphan window and a fault leaves the just-sealed
        // generation untouched. Gated on an actual seal because its cadence is configured in
        // seals: a skipped boundary or a failed write added no roots and left nothing to repack.
        if (sealed) {
            // The sweep runs last so it walks a catalogue compaction has finished
            // writing, and collects whatever that pass superseded in the same turn.
            maybeCompactCheckpointTimeline(instance);
            maybeSweepCheckpointSegments(instance);
        }
        return sealed;
    }

    /**
     * Reconstructs the logical checkpoint roots the restore reader had to skip
     * because their data pages were structurally invalid, re-versioning each one in
     * place: the same {@code (maxTimestamp, checkpointId)} logical key, fresh state
     * derived from the current base, and the identical row position it always had.
     * The corrupt roots are healed rather than deleted, so every unrelated root
     * survives and a later restore or historical repair addresses the repaired ids
     * directly.
     * <p>
     * The heal restores the safe predecessor the reader landed on, folds the base
     * rows above it back through the window pipeline - writing nothing to the
     * live-view table, which is already correct - freezes each corrupt boundary as
     * the replay crosses it, and publishes the lot as one timeline range splice with
     * no row-count change. It is best-effort: any failure leaves the timeline
     * untouched (the temporary capture segment removes itself) and returns
     * {@code false}, and the caller rebuilds the derived state from the applied base
     * instead.
     *
     * @return true when the corrupt roots were reconstructed and republished
     */
    private boolean reconstructCorruptCheckpointRoots(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            LiveViewCheckpointTimelineStoreReader.Result restored,
            long durableBaseSeqTxn
    ) {
        final String viewName = instance.getDefinition().getViewName();
        final long predecessorMaxTs = restored.maxTimestamp;
        final long predecessorCheckpointId = restored.checkpointId;
        final long corruptCeilingMaxTs = restored.corruptCeilingMaxTs;
        // H, the exclusive convergence bound the suffix starts at. A boundary
        // timestamp is a real designated timestamp, so the +1 never overflows in
        // practice; the guard keeps a hypothetical unbounded ceiling from wrapping.
        final long highTsExclusive = corruptCeilingMaxTs == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : corruptCeilingMaxTs + 1;
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        final long definitionTxn = instance.getLiveViewToken().getTableId();
        if (checkpointTimelineStoreWriter == null) {
            checkpointTimelineStoreWriter = new LiveViewCheckpointTimelineStoreWriter(engine.getConfiguration());
            checkpointTimelineStoreWriter.setTestFailureStage(checkpointTimelineTestFailureStage);
        }
        LiveViewCheckpointTimelineStoreWriter.RepairCapture capture = null;
        TableReader baseReader = null;
        boolean readerAttached = false;
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            // The heal replays every base row above the predecessor it restored, so its
            // state describes every live key and no key domain narrows it.
            capture = checkpointTimelineStoreWriter.beginRepair(checkpointsDir, null, instance.getMemoryTracker());
            // (predecessorMaxTs, corruptCeilingMaxTs] in key space: the predecessor's
            // own boundary is kept, and every corrupt root above it up to and including
            // the ceiling is re-versioned. A non-corrupt boundary caught in the range
            // (a same-timestamp tie the reader stepped over) re-versions to identical
            // state, which is harmless.
            final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
            capture.collectBoundaries(predecessorMaxTs + 1, highTsExclusive, boundaries);
            if (boundaries.size() == 0) {
                return false;
            }
            // The durable live-view table is authoritative for each repaired root's
            // position - its rows at or below the boundary's timestamp. A non-native
            // boundary partition has no searchable prefix, so the heal cannot position
            // its root and defers to the full rebuild.
            final LongList positions = new LongList();
            try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
                for (int i = 0, n = boundaries.size(); i < n; i++) {
                    final long boundaryMaxTs = boundaries.getQuick(i).maxTimestamp;
                    final long position = countDurableRowsBelow(
                            lvReader,
                            boundaryMaxTs == Long.MAX_VALUE ? Long.MAX_VALUE : boundaryMaxTs + 1
                    );
                    if (position < 0) {
                        return false;
                    }
                    positions.add(position);
                }
            }
            baseReader = waitForApply(baseToken, durableBaseSeqTxn);
            // The predecessor's state is the replay's warm start. Clear the maps the
            // failed floor restore may have partially filled, then restore it: the
            // reader already proved this boundary reads cleanly.
            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
            for (int i = 0, n = functions.size(); i < n; i++) {
                final Map map = functions.getQuick(i).getPartitionMap();
                if (map != null && map.isOpen()) {
                    map.clear();
                }
            }
            if (restoreAnchorRoot(instance, windowFactory, predecessorMaxTs, predecessorCheckpointId)
                    == Numbers.LONG_NULL) {
                return false;
            }
            engine.detachReader(baseReader);
            executionContext.of(baseReader);
            readerAttached = true;
            final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
            final Function filter = compiledPlan.getFilter();
            final PageFrameRecordCursorFactory pageFrameFactory = compiledPlan.getPageFrameFactory();
            // Start strictly above the predecessor: its restored state already covers
            // every row at or below its timestamp, so the frame it holds is the warm-up
            // the replay resumes from. Stop at the ceiling - every corrupt boundary is
            // at or below it.
            final long scanLowTs = Math.max(viewLowerBoundTimestamp, predecessorMaxTs + 1);
            try (RecordCursor pageCursor = pageFrameFactory.getCursorInTimestampRange(
                    executionContext,
                    scanLowTs,
                    corruptCeilingMaxTs
            )) {
                RecordCursor source = pageCursor;
                if (filter != null) {
                    filteringCursor.of(source, filter, executionContext);
                    source = filteringCursor;
                }
                // Below the anchor dispatch, and below the window cursor whose
                // hasNext() folds a row before the loop below ever sees it: a
                // boundary has to freeze between two rows, never inside one.
                boundaryFreezingCursor.of(
                        source,
                        capture,
                        boundaries,
                        positions,
                        functions,
                        anchorWindow,
                        null,
                        0,
                        pageFrameFactory.getMetadata().getTimestampIndex()
                );
                source = boundaryFreezingCursor;
                source = compiledPlan.wrapWindowInput(source, executionContext);
                if (anchorWindow != null) {
                    anchorDispatchingCursor.of(source, anchorWindow, executionContext);
                    source = anchorDispatchingCursor;
                }
                // No output projection here: the heal emits nothing, so no row is ever read
                // out of this cursor and there is nothing for a projection to compute.
                try (RecordCursor windowCursor = windowFactory.getIncrementalCursor(source, executionContext)) {
                    while (windowCursor.hasNext()) {
                        // The heal emits nothing - the live-view table is already
                        // correct - so every row here is warm-up. The freeze cursor
                        // under this one has already segmented the window state at
                        // any boundary the row crossed, which is the whole point of
                        // walking them.
                    }
                    // Boundaries at or above the last row the replay saw: no qualifying
                    // row sits between them and it, so the state the replay ends on is
                    // theirs. The ceiling is the highest, so this drains the rest.
                    boundaryFreezingCursor.freezeRemaining();
                }
            }
            executionContext.clearReader();
            engine.attachReader(baseReader);
            readerAttached = false;
            final long coveredLvSeqTxn = engine.getTableSequencerAPI()
                    .getTxnTracker(instance.getLiveViewToken())
                    .getWriterTxn();
            // Same seam as appendCheckpointTimelineRoot: the role read lock stays, the
            // read-only refusal does not.
            final Lock roleLock = engine.getRoleSwitchReadLock();
            roleLock.lock();
            try {
                // suffixRowDelta is 0: the base did not change, so the repaired roots
                // hold the same rows at the same positions - only their damaged state
                // pages are replaced.
                checkpointTimelineStoreWriter.publishRepair(
                        capture,
                        definitionTxn,
                        restored.normalizedBaseSeqTxn,
                        coveredLvSeqTxn,
                        0,
                        true,
                        highTsExclusive,
                        0
                );
            } finally {
                roleLock.unlock();
            }
            capture = Misc.free(capture);
            LOG.info().$("reconstructed corrupt live view checkpoint roots [view=")
                    .$(viewName)
                    .$(", predecessorMaxTs=").$ts(predecessorMaxTs)
                    .$(", corruptCeilingMaxTs=").$ts(corruptCeilingMaxTs)
                    .$(", roots=").$(boundaries.size()).I$();
            return true;
        } catch (Throwable t) {
            LOG.critical().$("could not reconstruct corrupt live view checkpoint roots [view=")
                    .$(viewName)
                    .$(", corruptCeilingMaxTs=").$ts(corruptCeilingMaxTs)
                    .$(", error=").$(t).I$();
            return false;
        } finally {
            boundaryFreezingCursor.clear();
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(baseReader);
            }
            Misc.free(capture);
            if (baseReader != null) {
                baseReader.close();
            }
        }
    }

    /**
     * Restart replay-to-applied: re-feeds base WAL rows over
     * {@code (fromSeqTxn, toSeqTxn]} through the window pipeline to advance the
     * accumulators restored from a checkpoint root up to the persisted applied
     * watermark, WITHOUT emitting (no LV WAL write, no inline apply, no in-mem
     * tier append). The on-disk LV table already holds these rows - the checkpoint
     * cadence simply left the newest root short of the applied point - so only the
     * restored accumulators need to catch up before drain-forward rebuilds the
     * un-flushed lead lost on the crash.
     * <p>
     * The whole gap is processed in this single call: the per-turn yield budget is
     * reset before each drain pass so the replay never stops mid-gap and leaves the
     * accumulators short of disk (which would make drain-forward re-emit rows disk
     * already holds). On out-of-order arrival - only reachable when a prior post-O3
     * seal failed, so an unresolved O3 sits between the head and the
     * applied point - it hands off to {@link #o3Replay}, passing the applied point
     * (not the offending seqTxn) as {@code advanceTo} so the REPLACE_RANGE rewrite
     * covers everything disk already holds; {@code o3Replay} re-stamps the
     * watermarks and seals a fresh boundary, and this returns
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
        final LiveViewCompiledPlan compiledPlan = instance.getCompiledPlan();
        final Function filter = compiledPlan.getFilter();
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        final RecordMetadata baseMetadata = compiledPlan.getBaseScanMetadata();
        final int baseTimestampIndex = baseMetadata.getTimestampIndex();
        buildColumnMappings(baseMetadata, baseToken);
        final RecordMetadata outMetadata = compiledPlan.getOutputMetadata();
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
                // No change ceiling: the hand-off advances to the applied point, not
                // to the offending seqTxn, so it re-materialises commits above the
                // ones this drain pass walked and the pass's ceiling would not bound
                // them.
                o3Replay(instance, windowFactory, drainResult.o3LateRowTs, Numbers.LONG_NULL, false, baseToken, toSeqTxn);
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
     * Binds this worker's checkpoint timeline store reader to {@code checkpointsDir}
     * and hands it to the caller for exactly one restore.
     * <p>
     * The reader is built on first use and rebound per call rather than rebuilt,
     * which is what keeps a per-commit resume off the allocation path: the tree
     * behind it - meta store, delta reader, partition map reader, ring state
     * reader, per-segment readers - is the same one every restore of every view
     * this worker drives goes through.
     * <p>
     * The caller must {@link LiveViewCheckpointTimelineStoreReader#detach()} on
     * every exit path. Detaching drops every mapping and forgets the generation,
     * so the reader holds nothing between restores and the next bind re-reads what
     * a retire, repair or compaction may have changed in between. A bind that
     * meets a reader still attached is a caller that lost its {@code finally};
     * that raises rather than silently restoring against the wrong view.
     */
    private LiveViewCheckpointTimelineStoreReader borrowCheckpointTimelineStoreReader(
            @Transient Path checkpointsDir
    ) {
        if (checkpointTimelineStoreReader == null) {
            checkpointTimelineStoreReader = new LiveViewCheckpointTimelineStoreReader(engine.getConfiguration());
        }
        checkpointTimelineStoreReader.of(checkpointsDir);
        return checkpointTimelineStoreReader;
    }

    /**
     * Restores the logical checkpoint root the repair plan chose as its resume
     * anchor, identified by the timeline's composite {@code (maxTimestamp,
     * checkpointId)} key. The caller has already cleared the per-function
     * partition maps, so this writes the root's state into an empty runtime.
     * <p>
     * Selection and restore run under separate generation pins - the plan searched
     * during planning, this restores at replay time - so the exact-key lookup here
     * is what proves the boundary survived in between. Only this worker publishes
     * for this view and it publishes nothing between the two, so a miss means the
     * timeline is gone or unreadable rather than merely re-versioned.
     *
     * @return the root's effective {@code lvRowPosition}, or
     * {@link Numbers#LONG_NULL} when the root could not be restored
     */
    private long restoreAnchorRoot(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long anchorMaxTs,
            long anchorCheckpointId
    ) {
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            final LiveViewCheckpointTimelineStoreReader reader =
                    borrowCheckpointTimelineStoreReader(checkpointsDir);
            try {
                return reader.restore(
                        anchorMaxTs,
                        anchorCheckpointId,
                        instance.getLiveViewToken().getTableId(),
                        windowFactory.getWindowFunctions(),
                        instance.getAnchorWindow()
                ).effectiveLvRowPosition;
            } finally {
                reader.detach();
            }
        } catch (CairoException ce) {
            LOG.critical().$("could not restore live view O3 resume anchor [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", anchorMaxTs=").$ts(anchorMaxTs)
                    .$(", anchorCheckpointId=").$(anchorCheckpointId)
                    .$(", error=").$safe(ce.getFlyweightMessage()).I$();
            return Numbers.LONG_NULL;
        } catch (Throwable t) {
            LOG.critical().$("could not restore live view O3 resume anchor [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", anchorMaxTs=").$ts(anchorMaxTs)
                    .$(", anchorCheckpointId=").$(anchorCheckpointId)
                    .$(", error=").$(t).I$();
            return Numbers.LONG_NULL;
        }
    }

    /**
     * Whether the runtime already holds the exact state {@link #restoreAnchorRoot}
     * would reconstruct for the anchor a resume replay selected.
     * <p>
     * The measured workload makes this the common case rather than a corner: the
     * load generator groups each ingest slice by symbol, so a commit is internally
     * out of order while sitting wholly above the sealed head. O3 detection fires
     * before either row reaches the incremental cursor, which leaves the maps,
     * ring arenas, anchor window and lifetime row position exactly where the head
     * seal left them - and the restore then decodes the same immutable pages to
     * write that identical state back.
     * <p>
     * Three independent facts have to hold, and the plan takes the original
     * restore whenever any of them cannot be established:
     * <ul>
     *     <li><b>The anchor IS the head's root.</b> {@code headCheckpointRootId}
     *     is stamped only by the caller that published (or restored) the root the
     *     head mirrors, and {@link LiveViewInstance#setHeadCheckpoint} clears it
     *     on every head transition, so matching it against
     *     {@code plan.anchorCheckpointId} identifies the root outright instead of
     *     inferring identity from {@code maxTimestamp} alone. The
     *     {@code maxTimestamp} comparison stays alongside it: the composite key is
     *     the pair, and the head's own {@code maxTs} is what the frontier test
     *     below is written against.</li>
     *     <li><b>The runtime still belongs to that root.</b> The stamp carries the
     *     compiled factory whose functions were frozen into it. A base-metadata
     *     recompile drops the window state (and clears the stamp with it), so the
     *     identity check refuses a runtime whose maps are a different, possibly
     *     empty, generation of the same view.</li>
     *     <li><b>Nothing has entered the window pipeline since.</b>
     *     {@code minSeenTsSinceCheckpoint == Long.MAX_VALUE} is the load-bearing
     *     one - the seal resets it, and every row the runtime consumes lowers it,
     *     including a row at the head's own timestamp, which would not advance
     *     {@code latestSeenTs}. {@code latestSeenTs == headMaxTs} pins the frontier
     *     to the boundary, and {@code windowStateDirty} covers the current drain
     *     explicitly.</li>
     * </ul>
     * With all three, the live maps, rings, arenas and
     * {@link LiveViewInstance#getLvRowsTotal()} are that root's state, because the
     * seal froze them from it and nothing has touched them since.
     */
    private boolean canReuseRuntimeAnchor(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            LiveViewCheckpointRepairPlan plan
    ) {
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        final long headRootId = instance.getHeadCheckpointRootId();
        return !windowStateDirty
                && headMaxTs != Numbers.LONG_NULL
                && headRootId != Numbers.LONG_NULL
                && instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL
                && instance.getHeadCheckpointRootWindowFactory() == windowFactory
                && plan.getAnchorCheckpointId() == headRootId
                && plan.getAnchorMaxTs() == headMaxTs
                && instance.getMinSeenTsSinceCheckpoint() == Long.MAX_VALUE
                && instance.getLatestSeenTs() == headMaxTs;
    }

    /**
     * Restores the newest logical root the timeline holds and rehydrates the
     * LV's mid-sweep window state (anchor map + per-function maps) from it,
     * surfacing the generation's seed cursor in {@code out.resumeDataOffset}
     * alongside the root's {@code maxTimestamp} and lifetime row position.
     * <p>
     * A view with no valid generation - a fresh CREATE, or one whose timeline an
     * earlier turn retired - is an ordinary miss, not a failure: it returns
     * {@code false} without touching the runtime, and the caller sweeps from
     * offset zero. So is a generation whose seed cursor is
     * {@link Numbers#LONG_NULL}, which a steady seal or a repair published rather
     * than a mid-sweep cadence event, and whose newest root therefore names no
     * cursor position to resume from.
     * <p>
     * The caller decides what to do with the restored coordinates; this helper
     * restricts itself to state restore + failure cleanup.
     * <p>
     * Failure handling: any structural error (page checksum, missing function,
     * anchor shape mismatch) is best-effort cleaned up in
     * {@link #handleCorruptSeedTimeline} - it logs critical, retires the
     * unreadable timeline and returns {@code false}. The LV is not invalidated;
     * the caller re-sweeps from the beginning.
     */
    private boolean restoreSeedFromTimeline(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            RestoredSeedState out
    ) {
        out.reset();
        try (Path checkpointsDir = new Path()) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            if (!hasValidCheckpointGeneration(checkpointsDir)) {
                // The common case on the sweep's very first turn. Separated from the
                // open below so it reads as a miss rather than as the corruption the
                // reader raises when it is asked to restore from nothing.
                return false;
            }
            final LiveViewCheckpointTimelineStoreReader reader =
                    borrowCheckpointTimelineStoreReader(checkpointsDir);
            try {
                // Open the (lazy) window cursor before writing restored state into it:
                // allocates the per-partition maps and marks the cursor open so the
                // first post-restore incremental refresh preserves the restored state
                // rather than re-bootstrapping (which would clobber it).
                windowFactory.openForLiveViewRestore(executionContext);
                final LiveViewCheckpointTimelineStoreReader.Result restored = reader.restoreLatest(
                        instance.getLiveViewToken().getTableId(),
                        windowFactory.getWindowFunctions(),
                        instance.getAnchorWindow()
                );
                if (restored.seedCursorOffset == Numbers.LONG_NULL) {
                    LOG.info().$("live view timeline holds no seed resume point [view=")
                            .$(instance.getDefinition().getViewName())
                            .$(", generation=").$(restored.generation).I$();
                    return false;
                }
                out.resumeDataOffset = restored.seedCursorOffset;
                out.maxTimestamp = restored.maxTimestamp;
                out.lvRowsTotal = restored.effectiveLvRowPosition;
                out.stateBytes = restored.logicalStateBytes;
                return true;
            } finally {
                reader.detach();
            }
        } catch (Throwable t) {
            return handleCorruptSeedTimeline(instance, t);
        }
    }

    /**
     * Best-effort cleanup after a seed resume fails on structural corruption
     * (page checksum, truncation, missing function root, anchor shape mismatch).
     * Retires the unreadable timeline so the next sweep turn cannot re-select it,
     * and so the from-zero re-sweep starts against an empty one. Always returns
     * {@code false} so the caller abandons the resume.
     */
    private boolean handleCorruptSeedTimeline(LiveViewInstance instance, Throwable t) {
        LOG.critical().$("could not restore live view from seed checkpoint [view=")
                .$(instance.getDefinition().getViewName())
                .$(", error=").$(t).I$();
        retireSeedCheckpointTimeline(instance);
        return false;
    }

    /**
     * Reports whether {@code checkpointsDir} publishes a generation a reader can
     * open, without creating the superblock file for a view that has never sealed
     * one.
     */
    private boolean hasValidCheckpointGeneration(@Transient Path checkpointsDir) {
        try (Path timelinePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
            if (!engine.getConfiguration().getFilesFacade().exists(timelinePath.$())) {
                return false;
            }
        }
        try (LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(engine.getConfiguration())) {
            metaStore.of(checkpointsDir);
            return metaStore.isValid();
        }
    }

    /**
     * Appends this cycle's staging rows onto the <em>published</em> slot in place, skipping the
     * retained-row copy and index swap the slow path performs. Returns {@code false} when a
     * reader pins that slot, leaving the tier byte-identical to its prior state.
     * <p>
     * Both the fast path and the slow path's one-slot-pinned fallback route through here, so a
     * reader pinning only the non-published slot defers compaction by a cycle instead of forcing
     * the caller to flush.
     */
    private boolean tryAppendStagingInPlace(
            LiveViewInMemoryTier tier,
            LiveViewInstance instance,
            int publishedIdx,
            boolean dropRetained,
            long lvSeqTxn,
            long newLeadRowCount,
            boolean leadMode
    ) {
        LiveViewInMemoryBuffer acquired = tier.tryAcquireWrite(publishedIdx);
        if (acquired == null) {
            return false;
        }
        try {
            if (dropRetained) {
                // Reset under the writer sentinel (no reader can observe it) so the published
                // slot reflects only this cycle's disk-consistent staging rows; seamTs
                // re-initialises from the first staged row.
                acquired.reset();
            }
            acquired.appendStaging(stagingBuffer, stagingBuffer.seamTs());
            acquired.setLvSeqTxn(lvSeqTxn);
            acquired.setLeadRowCount(newLeadRowCount);
        } catch (Throwable t) {
            // An in-place append cannot leave the slot partially populated visibly to readers:
            // rowCount only advances once at the end of appendStaging, after all column writes
            // have completed, and appendStaging rewinds any partially-advanced var-size append
            // cursors on failure, so the slot is byte-identical to its pre-append state. The
            // writer sentinel (rc = -1) keeps readers spinning until release. Drop the sentinel
            // and let the flush-retry budget tick.
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
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);

            // A prefix-preserving repair truncated the timeline head but a crash
            // reached here before it re-sealed a fresh head: the superblock's
            // watermark still names the discarded head, so an incremental restore
            // would rehydrate wrong state. The live marker forces the deterministic
            // applied-base rebuild instead. It is stale - a harmless leftover a crash
            // left between the seal and its clear - only when a generation strictly
            // past the truncate's own was sealed over it, which the base generation
            // it records lets a restart tell from a live repair.
            if (LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), checkpointsDir)) {
                final long markerBaseGeneration = LiveViewCheckpointRepairMarker.readBaseGeneration(
                        engine.getConfiguration(),
                        checkpointsDir
                );
                long currentGeneration = Numbers.LONG_NULL;
                try (LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(engine.getConfiguration())) {
                    superblock.of(checkpointsDir);
                    if (superblock.isValid()) {
                        currentGeneration = superblock.generation;
                    }
                }
                final boolean stale = markerBaseGeneration != Numbers.LONG_NULL
                        && currentGeneration != Numbers.LONG_NULL
                        && currentGeneration > markerBaseGeneration + 1;
                if (!stale) {
                    // Live repair, torn marker, or an unreadable superblock: rebuild.
                    // The rebuild retires the timeline, which removes the marker.
                    rebuildTimelineRecoveryFromAppliedBase(
                            instance,
                            windowFactory,
                            durableBaseSeqTxn,
                            "prefix preservation repair marker present"
                    );
                    return;
                }
                // The repair completed; the marker is a leftover. Clear it and
                // restore from the sealed timeline as usual.
                LiveViewCheckpointRepairMarker.clear(engine.getConfiguration().getFilesFacade(), checkpointsDir);
            }

            if (!engine.getConfiguration().getFilesFacade().exists(timelinePath.$())) {
                rebuildTimelineRecoveryFromAppliedBase(
                        instance,
                        windowFactory,
                        durableBaseSeqTxn,
                        "timeline is absent"
                );
                return;
            }

            LiveViewCheckpointTimelineStoreReader.Result restored;
            final LiveViewCheckpointTimelineStoreReader timelineReader =
                    borrowCheckpointTimelineStoreReader(checkpointsDir);
            try {
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
            } finally {
                timelineReader.detach();
            }

            if (restored.corruptCeilingMaxTs != Numbers.LONG_NULL) {
                // The floor root's data page was corrupt and the reader fell back to a
                // predecessor. Heal the skipped boundaries in place - same logical ids,
                // fresh state - so a later restore or historical repair addresses them
                // directly instead of falling back again, then restore cleanly from the
                // healed generation. A heal that cannot complete throws to the rebuild
                // below, which still derives a correct view from the applied base.
                LOG.error().$("live view checkpoint restore fell back past corrupt roots, reconstructing [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", predecessorMaxTs=").$ts(restored.maxTimestamp)
                        .$(", corruptCeilingMaxTs=").$ts(restored.corruptCeilingMaxTs).I$();
                if (!reconstructCorruptCheckpointRoots(instance, windowFactory, restored, durableBaseSeqTxn)) {
                    throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                            .put("live view checkpoint corrupt-root reconstruction failed");
                }
                final LiveViewCheckpointTimelineStoreReader healedReader =
                        borrowCheckpointTimelineStoreReader(checkpointsDir);
                try {
                    restored = healedReader.restoreLatestCompatible(
                            durableFrontierTimestamp,
                            durableBaseSeqTxn,
                            durableLvSeqTxn,
                            durableLvRowCount,
                            instance.getLiveViewToken().getTableId(),
                            windowFactory.getWindowFunctions(),
                            instance.getAnchorWindow()
                    );
                } finally {
                    healedReader.detach();
                }
                if (restored.corruptCeilingMaxTs != Numbers.LONG_NULL) {
                    throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                            .put("live view checkpoint reconstruction did not heal the corrupt roots");
                }
            }

            instance.forceSetLatestSeenTs(restored.maxTimestamp);
            instance.setLvRowsTotal(restored.effectiveLvRowPosition);
            instance.recordCheckpointLookupDepth(restored.lookupDepth);

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
            // The head mirrors the root this restore rehydrated windowFactory's
            // functions from. replayToApplied above may have fed rows past it, but
            // that shows up as a runtime frontier beyond the head's maxTs, which is
            // what canReuseRuntimeAnchor tests separately.
            instance.setHeadCheckpointRoot(restored.checkpointId, windowFactory);
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
     * Computes the AND of (a) anchor-map key codec support and (b) every
     * compiled window function either supporting checkpoint state or declaring
     * it holds none. Called once per LV lifetime on the first refresh after the
     * compiled factory is available; subsequent calls short-circuit on the
     * cached flag.
     */
    protected static boolean computeSnapshotCapability(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        if (anchorWindow != null && !LiveViewSnapshotKeyCodec.isAllTypesSupported(anchorWindow.getPartitionKeyTypes())) {
            return false;
        }
        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            // A stateless function contributes nothing to the image, so it is no obstacle to
            // one: every capture and restore site skips it and the round trip is complete
            // without it.
            if (!function.supportsCheckpointState() && !function.isCheckpointStateless()) {
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
        RecordMetadata outMetadata = getPlan(instance).getOutputMetadata();
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
        if (!isCompactionWorthwhile(pubSlot, stagingMaxTs, instance, growthBudget)
                && tryAppendStagingInPlace(tier, instance, publishedIdx, dropRetained, lvSeqTxn, newLeadRowCount, leadMode)) {
            return true;
        }

        // Slow-path: take the non-published slot, copy retained rows, append
        // staging, swap published index.
        int writeIdx = 1 - publishedIdx;
        LiveViewInMemoryBuffer writeSlot = tier.tryAcquireWrite(writeIdx);
        if (writeSlot == null) {
            // The non-published slot is reader-pinned, so the retained-row copy and the index
            // swap cannot run this cycle. Compaction is an optimisation, not a correctness
            // requirement, so defer it one cycle and append in place rather than treating this
            // as a stall: a single long-lived cursor (an idle PGWire connection, a slow ASOF
            // slave) pins exactly ONE slot, and giving up here degrades the view to a disk
            // flush every cycle for as long as that cursor lives. The stall accounting below
            // is for BOTH slots pinned, which is what this method's contract already states.
            if (tryAppendStagingInPlace(tier, instance, publishedIdx, dropRetained, lvSeqTxn, newLeadRowCount, leadMode)) {
                return true;
            }
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
                // Clamp the eviction threshold to the minimum timestamp of whatever
                // will sit above the retained overlap - the lead's minimum when the
                // slot carries one, else this cycle's staging minimum - so an overlap
                // group sharing that timestamp stays resident. When the whole overlap
                // ages out (lo == overlapCount) the seam lands on that minimum; a
                // disk-backed overlap row at exactly it - an additive same-ts row at
                // the frontier, admitted because the O3 trigger is a strict
                // below-frontier compare - would then be served by neither disk (the
                // reader's scan stops strictly below the seam) nor the slot: silent
                // row loss plus a size() overcount that breaks LIMIT. Retaining every
                // overlap row at or above that minimum keeps the group in the slot at
                // the seam, where the overlap band still agrees with disk row-for-row.
                // This mirrors the tierStale rebuild guard in finishLeadRefresh and
                // the empty-slot seed in stageInMemoryTierWhenEmpty, which close the
                // same additive-same-ts gap on their own paths. In the common
                // unique-ts case that minimum sits strictly above every overlap ts,
                // so the clamp retains nothing extra and eviction is unchanged.
                //
                // A lead-carrying slot clamps at the lead rather than at staging
                // because the lead is the older of the two (staging appends on top of
                // it) and the whole band above the retained overlap has to survive.
                long evictionThreshold = retainThreshold;
                long aboveOverlapMinTs = Numbers.LONG_NULL;
                if (leadCount > 0) {
                    aboveOverlapMinTs = pubSlot.getLong(overlapCount, tsCol);
                } else if (stagingBuffer.rowCount() > 0) {
                    aboveOverlapMinTs = stagingBuffer.getLong(0, tsCol);
                }
                if (aboveOverlapMinTs != Numbers.LONG_NULL && aboveOverlapMinTs < evictionThreshold) {
                    evictionThreshold = aboveOverlapMinTs;
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
     * Rebuilds an ACTIVE view's window state from the applied base via
     * {@link #o3HeadMissReplay} (clearWindowState + full recompute + REPLACE_RANGE +
     * watermark advance) and restages the in-mem tier. Idempotent on the written
     * prefix. Shared by the base-metadata-drift and mid-drain-failure recoveries;
     * the caller has already handled the SEEDING state.
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
        restageInMemoryTierFromDisk(instance, tier);
    }

    /**
     * Replaces the published slot with the LV table's current {@code IN MEMORY}
     * window, stamped with the table's applied seqTxn and carrying no un-flushed
     * lead. The staging half of {@link #rebuildInMemoryTier} (which adds the
     * post-O3 symbol-cache drop) and of {@link #stageInMemoryTierWhenEmpty}; see
     * {@link #rebuildInMemoryTier} for the read bound and the acquire protocol.
     * <p>
     * Every caller must have zeroed {@code instance.leadRowCount} first: the
     * staged rows are a pure disk subset, so a surviving lead count would
     * reclassify durable rows as un-flushed and the next flush would re-append
     * them as duplicates.
     */
    private void restageInMemoryTierFromDisk(LiveViewInstance instance, LiveViewInMemoryTier tier) {
        // Reshape the worker-local staging buffer to THIS view before staging through it.
        // Only the two drain entry points (incrementalRefresh, drainAppliedBase) allocate
        // one, so the three rebuild paths that reach here outside a drain -
        // resumeSuspendedRepair, retryPendingLiveViewApply and
        // rederiveFromAppliedBaseAfterWalLoss - would otherwise stage through a null
        // buffer. Allocating one unconditionally is not enough either: stagingColumnTypes
        // is worker-wide and still describes whichever view this worker served last, and
        // copyReaderRowsToStaging dispatches off it, so this view's disk columns would be
        // read through another view's strides. ensureStagingAndTier reshapes both, and is
        // a cheap shape-match no-op for the callers that already ran it this cycle.
        //
        // What decides who releases the buffer is not "outside a drain" but "outside
        // refreshInstance", whose finally frees it at the end of every cycle.
        // resumeSuspendedRepair and rederiveFromAppliedBaseAfterWalLoss still run inside
        // refreshInstance and are covered by it; retryPendingLiveViewApply is a top-level
        // helper reached from scanForLaggingViews, so it mirrors that free in its own
        // finally - otherwise the buffer would outlive the latch that allocated it and
        // strand this view's tracker charge past tryFreeRuntimeStateIfInvalid.
        // A skipped restage marks the tier stale, exactly like the both-slots-pinned skip
        // below: the published slot keeps rows this rebuild was supposed to replace, so a
        // later publish must drop them rather than append onto them and re-stamp the slot
        // with a matching seqTxn. The fence keeps reads correct until then.
        final RecordCursorFactory compiledFactory = instance.getCompiledFactory();
        if (compiledFactory == null) {
            // A base-schema recompile frees the factory and deliberately keeps the tier,
            // so this is reachable with a live tier. The next cycle recompiles and rebuilds.
            instance.setTierStale(true);
            return;
        }
        // The plan is total here: ensureCompiledFactory decomposes the same object before
        // caching it, so a non-null compiled factory has already survived that walk.
        final RecordMetadata outMetadata = instance.getCompiledPlan().getOutputMetadata();
        final int tsColIdx = outMetadata.getTimestampIndex();
        if (tsColIdx < 0 || !ensureStagingAndTier(instance, outMetadata, tsColIdx)) {
            // An output column type the tier cannot store: this view never populates the
            // tier and its cursors read disk-only.
            instance.setTierStale(true);
            return;
        }

        // Stage the recent IN MEMORY window from the applied LV table.
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
            // Published slot now mirrors the disk tail; any prior stale-row marking
            // is resolved.
            instance.setTierStale(false);
            return;
        }
        // Slow-path: a reader pins the published slot. Fill the non-published
        // slot and swap to it; the old slot's pinned readers keep their frozen
        // (pre-restage) rows until they release, and the fence routes them disk-only.
        int writeIdx = 1 - publishedIdx;
        LiveViewInMemoryBuffer writeSlot = tier.tryAcquireWrite(writeIdx);
        if (writeSlot == null) {
            // Both slots reader-pinned: the restage is skipped, so the published slot
            // keeps whatever it held - the pre-O3 rows the replay re-sequenced on disk
            // for the rebuild caller, or nothing at all for the empty-slot seed. Mark
            // the tier stale so the next normal publish drops those retained rows
            // instead of re-stamping them with a matching seqTxn - otherwise a read
            // would serve the stale rows - and so the lead path flushes to disk rather
            // than seaming a slot that does not mirror the disk tail. The fence keeps
            // reads correct until then (the stale slot's seqTxn no longer matches disk).
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
            // timeline (or re-sweeps from 0 behind the skip-write floor). Idempotent.
            // Deliberately KEEP the pinned base snapshot (do not freeSeedBaseReader):
            // the fault is transient (map/staging OOM, bad read), the snapshot is intact,
            // and resuming the sealed data offset against the SAME snapshot stays sound. A
            // fresh snapshot would reintroduce the positional-resume hazard this fix closes.
            instance.resetSeedResumeAttempted();
            LOG.info().$("live view mid-seed refresh failure, sweep will resume [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return null;
        }
        return rebuildActiveWindowStateFromAppliedBase(instance, "mid-drain refresh failure");
    }

    /**
     * Seeds an empty published slot from the LV table before the cycle's drain can
     * publish into it. A restart (and the seed sweep, which appends straight to the
     * LV WAL) leaves the tier empty over an LV table that already holds rows, and
     * the first publish into an empty slot seeds {@code seamTs} from the staged
     * rows' own minimum timestamp.
     * <p>
     * That seam is only sound when no disk row below the staged rows shares their
     * minimum timestamp, and an additive commit that extends the durable frontier's
     * own timestamp group breaks it: the O3 trigger is a strict below-frontier
     * compare, so such a commit is an ordinary forward append. The reader then
     * serves neither side of the disk rows at that timestamp - the disk scan stops
     * strictly below the seam and the slot never held them - while {@code size()}
     * still counts them, so the stream and the count disagree and a LIMIT near the
     * seam reads short. The pure-lead slot the first publish produces is covered by
     * {@code LiveViewRecordCursor.hasNext}'s {@code leadStart == 0} branch, but the
     * flush that follows drops the slot's lead count to zero and re-engages the
     * seam cut over exactly those rows.
     * <p>
     * Staging from disk closes it because {@link #stageInMemoryWindowFromDisk} cuts
     * the window at the first row at or above {@code maxTs - IN_MEMORY}, which is
     * the first row of its timestamp group: every disk row at or above the seam is
     * then in the slot, and the drain appends on top without moving it.
     * <p>
     * Self-limiting: a publish never empties a populated slot (it always appends
     * this cycle's staging rows), so this runs at most once per view per process.
     * When both slots are reader-pinned the stage is skipped and marks the tier
     * stale, which routes this cycle's lead straight to disk and rebuilds the slot
     * as a clean disk subset - the same fallback the O3 rebuild-skip takes.
     * <p>
     * Callers run this after {@link #ensureStagingAndTier} (which allocates both the
     * tier and the worker-local staging buffer this borrows) and before the drain
     * stages its first row, so it hands the buffer back reset.
     */
    private void stageInMemoryTierWhenEmpty(LiveViewInstance instance) {
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null || tier.getSlot(tier.getPublishedIdx()).rowCount() > 0) {
            return;
        }
        // An empty slot carries no un-flushed lead, so the staged disk subset cannot
        // orphan one. Guard anyway: replacing the slot under a non-zero count would
        // make the next flush re-append rows that are already durable.
        if (instance.getLeadRowCount() != 0) {
            return;
        }
        try {
            restageInMemoryTierFromDisk(instance, tier);
        } finally {
            // The stage filled the staging buffer with the disk window; the drain
            // about to run appends from row 0, so give it back empty.
            stagingBuffer.reset();
        }
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
     * Number of rows the live-view table durably holds strictly below {@code ts}.
     * This is the one absolute the repaired root positions are measured from: a
     * localized replacement rewrites {@code [R, H)} and leaves everything below
     * {@code R} alone, so a boundary {@code B} in that interval sits at
     * {@code countDurableRowsBelow(R) + <rows the replay emitted at or below B>}.
     * <p>
     * Costed like the bound it serves rather than like the view's age. Partitions
     * whose metadata upper bound is already below {@code ts} contribute their
     * recorded size with no file opened at all; only the one partition the boundary
     * falls inside is opened and binary-searched, and the walk stops there.
     *
     * @return the row count, or {@code -1} when the boundary partition is not
     * native and cannot be searched through the reader's mapped columns - the
     * caller then has no exact prefix and must not splice
     */
    private static long countDurableRowsBelow(TableReader reader, long ts) {
        final int partitionCount = reader.getPartitionCount();
        final int timestampIndex = reader.getMetadata().getTimestampIndex();
        long count = 0;
        for (int p = 0; p < partitionCount; p++) {
            final long partitionRows = reader.getPartitionRowCountFromMetadata(p);
            if (partitionRows <= 0) {
                continue;
            }
            if (reader.getPartitionMaxTimestampFromMetadata(p) < ts) {
                // Every row this partition can hold is below the boundary.
                count += partitionRows;
                continue;
            }
            if (reader.getPartitionFormatFromMetadata(p) != PartitionFormat.NATIVE) {
                return -1;
            }
            final long size = reader.openPartition(p);
            final MemoryCR tsCol = reader.getColumn(
                    TableReader.getPrimaryColumnIndex(reader.getColumnBase(p), timestampIndex)
            );
            final long below = firstRowAtOrAbove(tsCol, size, ts);
            count += below;
            if (below < size) {
                // The first row at or above the boundary is in this partition, so
                // every later partition is above it too.
                break;
            }
        }
        return count;
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
     * Seed-checkpoint write hook. Appends one logical boundary to the versioned
     * timeline over the sweep's current window state, and stamps the sweep's
     * base-cursor row offset into the generation the append publishes, so a
     * restart mid-sweep restores that root and continues the cursor from where
     * it stopped.
     * <p>
     * Cadence-gated by the same {@code cairo.live.view.checkpoint.rows} /
     * {@code .max.duration} triggers as the steady head, plus a
     * first-checkpoint trigger so a restart early in the sweep resumes rather
     * than re-sweeping. The intervening per-turn yields rely on in-memory
     * window state; the sealed root only has to be recent enough that a
     * restart's skip-write re-feed (bounded by the cadence) is cheap.
     * <p>
     * The boundary must sit strictly above the previous one, which a turn
     * ending on a timestamp the previous turn already reached does not: the
     * timeline is keyed on {@code (maxTimestamp, checkpointId)} and the append
     * refuses an overlap. Such a turn writes nothing and the next one carries
     * the sweep past it.
     * <p>
     * No-op when the LV is not snapshot-capable: such a view cannot persist
     * window state, so a crash mid-sweep re-sweeps from the beginning (the
     * wipe path in {@link #runSeedSweep}).
     * <p>
     * A failure here does not invalidate the view - the timeline is derived
     * state. The previously published generation stays authoritative, so the
     * sweep keeps its older resume point; we log critical and continue.
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
        // A boundary with no maxTs has no place in a timeline keyed on
        // (maxTimestamp, checkpointId). A yielding turn always has rows behind
        // it, so batchMaxTs is a real timestamp here; the guard keeps a future
        // caller from sealing a poison boundary.
        if (batchMaxTs == Numbers.LONG_NULL) {
            return;
        }
        final long sealedMaxTs = instance.getSeedCheckpointMaxTs();
        if (sealedMaxTs != Numbers.LONG_NULL && batchMaxTs <= sealedMaxTs) {
            return;
        }

        // Cadence keys off the data-offset delta since the prior seed root.
        // firstSeedRoot forces a write so a crash early in the sweep resumes
        // rather than re-sweeping from scratch.
        final long rowsCadence = engine.getConfiguration().getLiveViewCheckpointRows();
        final long durationCadence = engine.getConfiguration().getLiveViewCheckpointMaxDurationMicros();
        final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        final long lastWrittenUs = instance.getLastCheckpointWrittenUs();
        final long priorOffset = instance.getSeedCheckpointDataOffset();
        final boolean firstSeedRoot = priorOffset == Numbers.LONG_NULL;
        final boolean rowTrigger = !firstSeedRoot && (dataOffset - priorOffset) >= rowsCadence;
        final boolean durationTrigger = !firstSeedRoot
                && lastWrittenUs != Numbers.LONG_NULL
                && (nowUs - lastWrittenUs) >= durationCadence;
        if (!(firstSeedRoot || rowTrigger || durationTrigger)) {
            return;
        }

        try {
            appendCheckpointTimelineRoot(
                    instance,
                    windowFactory.getWindowFunctions(),
                    instance.getAnchorWindow(),
                    sweepSeqTxn,
                    batchMaxTs,
                    dataOffset
            );
            instance.recordSeedCheckpointWritten(dataOffset, batchMaxTs, nowUs);
        } catch (LiveViewCheckpointTimelineStoreWriter.BoundaryNotAboveHeadException e) {
            // The turn ended on the timestamp the head boundary already covers. The
            // sealedMaxTs gate above normally catches that, so reaching here means the
            // mirror trails the timeline; either way there is nothing to seal and the
            // next turn that reaches a higher timestamp seals this turn's rows too.
            LOG.debug().$("live view seed checkpoint boundary not above head, seal skipped [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", candidate=").$ts(batchMaxTs).I$();
        } catch (Throwable t) {
            LOG.critical().$("could not write live view seed checkpoint [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", dataOffset=").$(dataOffset)
                    .$(", error=").$(t).I$();
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

    /**
     * Continues every localized repair this worker parked on its turn budget.
     * <p>
     * A parked repair holds a pinned base snapshot and an uncommitted live-view
     * replacement, and only this worker can continue it, so it must not wait for
     * the view to come back around: the idle scan is sharded by table id and the
     * notification queue hands a base table to whichever worker dequeues it, so
     * neither route promises this worker the view. Driving the list here does.
     * <p>
     * The list is pruned lazily - an entry whose repair has ended (published,
     * abandoned, or discarded by DROP / invalidation) is dropped on the pass that
     * finds it that way.
     *
     * @return true when a turn ran, so the worker reports work and is rescheduled
     */
    private boolean driveSuspendedRepairs() {
        boolean didWork = false;
        for (int i = suspendedRepairViews.size() - 1; i >= 0; i--) {
            final LiveViewInstance instance = suspendedRepairViews.getQuick(i);
            final LiveViewCheckpointRepairSession session = instance.getSuspendedRepair();
            if (session == null || session.getOwner() != this) {
                suspendedRepairViews.remove(i);
                continue;
            }
            didWork |= refreshInstance(instance, instance.getLastProcessedSeqTxn());
        }
        return didWork;
    }

    private boolean processNotifications() {
        if (!stateStore.isRefreshEnabled()) {
            // Quiesced store (live views disabled): skip the whole pass, including the registry
            // fallback scan, so refresh workers never touch a live view. Enabling live views swaps in
            // a real store (see ForwardingLiveViewStateStore) and this gate reopens. Under symmetric
            // local refresh every role runs the refresh-enabled store, so a read-only replica no
            // longer takes this branch.
            return false;
        }
        // Ahead of the queue drain: a repair parked between turns blocks its view's
        // refresh entirely, and only this worker can finish it.
        boolean didWork = driveSuspendedRepairs();
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
                // this same tick proceed to refresh.
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
            // Promote-hydrate consistency guard. A role migration onto a partially-uploaded object
            // store can leave a live view's durable watermark ahead of the base seqTxn that actually
            // replicated: an ex-primary flushed + uploaded derived rows for base commits whose own
            // base-table WAL upload lagged and was cut. The base can never reach that seqTxn (it is not
            // in the downloaded WAL), so the view would otherwise sit active forever serving derived
            // rows for base commits the promoted primary no longer holds. Invalidate it durably,
            // mirroring the mat-view "view is ahead of base table and cannot be synchronized" guard in
            // CairoEngine.loadMatViewIntoStore. A strict no-op on a healthy node, where a view never
            // outruns the base it derives from.
            // Read the view watermark first. A refresh publishes it only after consuming the
            // corresponding sequencer transaction, so observing that volatile write before reading
            // the monotonic base head yields a coherent pair. Reading base first lets another worker
            // advance the view between the reads and manufactures a false ahead state from two
            // different points in time. Monotonicity is what makes the order sufficient rather than
            // merely narrower: TableTransactionLog.lastTxn is a volatile field over an append-only
            // log, and lastTxn() reads it under the sequencer READ lock, so it cannot observe the
            // appender's half-published state.
            final long lastProcessedSeqTxn = instance.getLastProcessedSeqTxn();
            final Runnable aheadGuardAction = simulateBaseCommitBetweenAheadGuardReadsForTest;
            if (aheadGuardAction != null) {
                simulateBaseCommitBetweenAheadGuardReadsForTest = null;
                aheadGuardAction.run();
            }
            // Compare against the higher of the two base heads. On a primary the cached
            // sequencer head is authoritative: the sole appender keeps it current, and the
            // watermark can sit above writerTxn because the notification-driven refresh
            // consumes a commit before the apply job lands it. On an enterprise replica the
            // downloader appends the on-disk txnlog behind the cached sequencer and reconciles
            // it later, while WAL apply and this refresh consume the new txns from the file;
            // there writerTxn covers the stale window. writerTxn is not monotonic
            // (notifyWalTxnRepublisher resets it to -1; a late apply can publish an older head),
            // so the max never drops below the cached head. Neither head can exceed the durable
            // txnlog, so a hydrate-restored watermark past it still trips the guard.
            final long baseSeqLastTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
            final long baseWriterTxn = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
            if (lastProcessedSeqTxn > Math.max(baseSeqLastTxn, baseWriterTxn)) {
                LOG.error().$("live view is ahead of base table and cannot be synchronized [view=")
                        .$(instance.getLiveViewToken())
                        .$(", lastProcessedSeqTxn=").$(lastProcessedSeqTxn)
                        .$(", baseTableTxn=").$(baseSeqLastTxn)
                        .$(", baseWriterTxn=").$(baseWriterTxn)
                        .I$();
                engine.invalidateLiveView(instance, "live view is ahead of base table and cannot be synchronized");
                didWork = true;
                continue;
            }
            // Re-drive an LV WAL block whose inline apply never landed. The refresh worker owns the
            // LV's TableWriter, so ApplyWal2TableJob.doRun drops every live-view notification and
            // flushLead's inline applyWalDirect is the view's ONLY applier. When that apply silently
            // no-ops (the LV writer was busy, or its memory-pressure control backed off) or fails and
            // suspends the table, the republish it falls back on goes nowhere: the committed rows stay
            // off disk, and the flush already re-stamped the slot with a zero lead, so neither tier
            // serves them - the view under-reports until a later base commit drives another flush. On
            // a quiescent base that never comes. Retry the apply here instead; it is idempotent
            // (the block is committed, so it lands each row exactly once) and it is what lets
            // ALTER LIVE VIEW ... RESUME WAL on a suspended live view take effect without waiting
            // for the base to move.
            if (hasPendingLiveViewApply(instance)) {
                didWork |= retryPendingLiveViewApply(instance);
            }
            long head = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
            // Timeline recovery runs inside refreshInstance on the first cycle
            // of every ACTIVE view, even when there are no new base commits.
            // The recovery itself cheaply recognizes an empty identity view;
            // scheduling it once avoids reopening the LV table on every fallback
            // scan merely to decide that no recovery is needed.
            final boolean needsRestore = !instance.isCheckpointRestoreAttempted()
                    && instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE;
            // A SEEDING view needs a refresh tick to drive its sweep even when
            // no new base commits have arrived since CREATE - the sweep
            // covers existing history, not future commits.
            final boolean needsSeeding = instance.getStateReader().getSeedState()
                    == LiveViewState.SEED_STATE_SEEDING;
            final long processedTo = instance.getLastProcessedSeqTxn();
            if (head > processedTo || needsRestore || needsSeeding) {
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
                    || instance.isFreezeArmed()
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
            // Mirror refreshInstance's cycle-scoped free. The tier rebuild above reaches
            // restageInMemoryTierFromDisk, which reshapes the worker's staging buffer for
            // this view - and this helper runs outside refreshInstance, so nothing else
            // would ever release it. Leaving it bound keeps this view's refresh tracker
            // charged past the latch, and lets tryFreeRuntimeStateIfInvalid below return
            // that tracker to the pool with the buffer's pages still outstanding. Nested
            // try for the same reason as the main cycle: Misc.free catches only
            // IOException, so a close() assert must not skip the releases below and wedge
            // the view's refresh latch.
            try {
                stagingBuffer = Misc.free(stagingBuffer);
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
     * a SEEDING view resumes through its own sweep, which owns its distinct
     * floor. Idempotent on a healthy restart - {@code applyWalDirect} finds
     * nothing pending and the clamp is a no-op because {@code _lv.s} already matches
     * disk. When the WAL-e cannot be read the recovery no-ops, leaving the prior
     * (worst-case duplicating, never lossy) behaviour.
     * <p>
     * Returns {@code false} when the LV table still trails its own WAL, i.e. the
     * re-apply above did not land every committed block. {@code applyWalDirect}
     * reports nothing: it returns without applying under memory pressure, on a busy
     * writer ({@code EntryUnavailableException}), and after {@code handleWalApplyFailure}
     * suspended the table. The caller must then leave the whole restore for a later
     * turn rather than clamp, because {@code readLiveViewAppliedMaxBaseSeqTxn} reports
     * the last COMMITTED block: clamping onto it would release the base-WAL purge floor
     * ({@code lvConsumedSeqTxn}, see {@code WalPurgeJob}) and hand
     * {@link #tryRestoreFromTimeline} an {@code appliedWatermark} that names rows the
     * LV table does not hold, while it reads that same table's row count and frontier.
     * Blocking rather than clamping-low is what keeps this idempotent - the deferred
     * block lands exactly once when the fault clears, where re-deriving its base range
     * would duplicate it (a forward-append commit carries no dedup). Same rule, and the
     * same reasoning, as the pending-replacement gate in {@link #refreshInstance}.
     */
    private boolean reconcileAppliedFloorAfterRestart(LiveViewInstance instance) {
        if (instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE) {
            return true;
        }
        final TableToken token = instance.getLiveViewToken();
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        if (tracker.isSuspended() || !tracker.getMemPressureControl().isReadyToProcess()) {
            // Same exclusions hasPendingLiveViewApply carries, and for the same reason:
            // a retry cannot make progress from either state, so re-driving the apply
            // only burns a writer open and - on a suspended table, whose applyWal has no
            // suspension gate of its own - another markDistressed plus a CRITICAL
            // stacktrace on every idle pass. Defer cheaply instead. An operator RESUME
            // WAL clears the suspension and the memory-pressure gate eases on its own.
            return false;
        }
        try {
            applyJob.applyWalDirect(token, Job.RUNNING_STATUS);
        } catch (Throwable t) {
            // applyWal2Table suspends the table and returns rather than throwing, so this
            // guards a future path that does raise. The apply check below decides either way.
            LOG.error().$("could not apply pending live view WAL on restart [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
        }
        if (!isLiveViewWalFullyApplied(instance)) {
            return false;
        }
        final long appliedMaxBaseSeqTxn = engine.readLiveViewAppliedMaxBaseSeqTxn(token);
        if (appliedMaxBaseSeqTxn >= 0
                && appliedMaxBaseSeqTxn != instance.getStateReader().getLastProcessedSeqTxn()) {
            try {
                // This runs on the refresh worker while it holds the refresh latch, so the
                // startCheckpoint handshake already serialises the rewrite against the agent's
                // copy. Parking on waitForUnfrozen() here would deadlock the worker against a
                // concurrent checkpoint freeze, so applyLiveViewData does not.
                engine.applyLiveViewData(token, appliedMaxBaseSeqTxn, blockFileWriter, path);
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
        return true;
    }

    /**
     * Test-only seam: closes the armed close fault, if any, and returns the throwable its
     * {@code close()} raised, so a caller can hand it to {@code Misc.freeBestEffort} as the primary
     * of the same call that closes the real resource. Disarms one-shot. Returns null in production,
     * where the field is never set, at a cost of one null check.
     *
     * @see #setSimulateBaseMetadataCloseFailureForTest
     */
    @TestOnly
    private @Nullable Throwable consumeBaseMetadataCloseFaultForTest() {
        if (simulateBaseMetadataCloseFailureForTest == null) {
            return null;
        }
        final QuietCloseable closeFault = simulateBaseMetadataCloseFailureForTest;
        simulateBaseMetadataCloseFailureForTest = null;
        return Misc.freeBestEffort(null, closeFault);
    }

    /**
     * Decides whether the WAL-loss re-derive must refuse outright because the base's applied
     * metadata no longer resolves every column the view REFERENCES under the same name AND type
     * ({@link LiveViewInstance#findFirstMissingOrRetypedColumn}, the same predicate
     * {@code invalidateLiveViewsForBaseSchemaChange} invalidates on). Rebuilding across a dropped,
     * renamed or retyped referenced column would recompute the view over the NEW schema and commit
     * the result as if nothing happened, converting a loud, correct invalidation into silently wrong
     * output.
     * <p>
     * Reads the base metadata FRESH on every call, so
     * {@link #rederiveFromAppliedBaseAfterWalLoss} can ask both before the replay and again after a
     * {@link TableReferenceOutOfDateException} - the drift proves the base metadata moved since the
     * first read, and the recompile that follows would adopt whatever it moved to. The answer is
     * only as fresh as the read that produced it: this method closes the metadata before it returns,
     * and the caller's replay and recompile each open their own base reader afterwards, so asking
     * again NARROWS the window in which a structural change slips past unseen - it does not close
     * it.
     * <p>
     * A metadata read that FAILS is not a refusal. "Cannot read the metadata" and "the metadata
     * says a referenced column broke" are different answers: the read can fail for reasons that have
     * nothing to do with the view's health - the metadata pool refusing while a concurrent DDL or
     * checkpoint holds the entry, {@code verifyTableToken} racing a rename, pool exhaustion - and
     * this runs when the flush-retry budget is already spent, so refusing on a doubt would brick a
     * view over a healthy base with no second chance. An unreadable base falls through to the
     * replay, whose own {@code getReader} opens the base for real and faults loudly if it truly is
     * unreadable. A read that SUCCEEDS and names a broken dependency is a decision, and refuses -
     * including when the metadata close that follows then fails, which is why the close sits
     * outside the guarded region rather than inside a try-with-resources.
     * <p>
     * On refusal, stashes the offending column name as the pending invalidation reason, which
     * {@link #handleRefreshFailure} invalidates with, so {@code live_views().invalidation_reason}
     * names the broken dependency exactly as the apply-side invalidation does.
     *
     * @return true when the caller must abandon the re-derive and let the view invalidate
     */
    private boolean isRederiveRefusedForBrokenDependency(LiveViewInstance instance, TableToken baseToken, CairoException cause) {
        final String viewName = instance.getDefinition().getViewName();
        final String brokenColumn;
        TableMetadata baseMetadata = null;
        try {
            baseMetadata = engine.getTableMetadata(baseToken);
            brokenColumn = instance.findFirstMissingOrRetypedColumn(baseMetadata);
        } catch (Throwable e) {
            Misc.free(baseMetadata, e);
            consumeBaseMetadataCloseFaultForTest(); // @TestOnly no-op in production; releases an armed fault the close below never reaches
            LOG.error().$("live view could not read the applied base metadata before re-deriving, proceeding [view=")
                    .$(viewName)
                    .$(", error=").$(e).I$();
            return false;
        }
        // The close sits OUTSIDE the guard above on purpose: a close that fails cannot unmake the
        // answer the read already produced. A try-with-resources runs its implicit close BEFORE the
        // catch clause of the same statement, so any close fault would route an already-named broken
        // column into the "proceeding" branch and let the re-derive adopt the new schema. That is a
        // language-level guarantee rather than a defence against a known fault: TableMetadataPool
        // has no reachable path that makes this tenant's close() throw today - AbstractMultiTenantPool
        // only ever claims an UNALLOCATED slot (releaseAll's idle sweep, notifyDropped, lock), and the
        // one branch that erases a LIVE borrower's slot, releaseAll at pool shutdown, calls goodbye()
        // first, which nulls the tenant's pool and entry so close() never reaches returnToPool at all.
        // Log the close failure and drop it: the caller's contract is that this check answers rather
        // than throws.
        //
        // consumeBaseMetadataCloseFaultForTest() is null in production and costs one null check; a
        // test arms it to make this very close report a failure. It sits INSIDE this statement so
        // that the statement is what a mutation has to delete or move, and deleting it deletes the
        // fault's consumption too.
        final Throwable closeFailure = Misc.freeBestEffort(consumeBaseMetadataCloseFaultForTest(), baseMetadata);
        if (closeFailure != null) {
            LOG.error().$("live view could not close the applied base metadata after the dependency check [view=")
                    .$(viewName)
                    .$(", error=").$(closeFailure).I$();
        }
        if (brokenColumn == null) {
            return false;
        }
        instance.setPendingInvalidationReason("base schema change to a referenced column [column=" + brokenColumn + ']');
        LOG.critical().$("live view cannot re-derive from the applied base across a base schema change to a referenced column [view=")
                .$(viewName)
                .$(", column=").$safe(brokenColumn)
                .$(", reason=").$safe(cause.getFlyweightMessage()).I$();
        return true;
    }

    /**
     * Recovery for a refresh cycle that failed with
     * {@link TableReferenceOutOfDateException}: the base table's metadata version
     * drifted from the cached compiled factory. Frees the compiled artifacts so
     * the next factory use recompiles against current metadata, then rebuilds the
     * window state that was lost with the old factory's function instances:
     * <ul>
     *     <li>SEEDING: re-arms the sweep's single-shot resume setup; the next
     *     turn restores window state and the data offset from the timeline's
     *     newest root against the recompiled factory (same SQL, so the stored
     *     state stays shape-compatible), or re-sweeps from offset 0 behind the
     *     skip-write floor. Both are idempotent on the already-written prefix.</li>
     *     <li>ACTIVE: full head-miss replay over the applied base -
     *     unconditionally correct and idempotent (mirrors the dedup restart path
     *     and the checkpoint-less restore fallback). The replay resets window
     *     state, recomputes every retained row through the recompiled factory,
     *     rewrites the on-disk tier with a single REPLACE_RANGE, advances the
     *     watermarks, and seals a fresh boundary. Any un-flushed lead is
     *     dropped first (its rows were computed by the old factory's state) and
     *     {@code refreshedUpToSeqTxn} is pinned back to {@code lastProcessedSeqTxn}
     *     so no phantom lead survives.</li>
     * </ul>
     * Returns {@code null} when recovery completed (or was re-armed for the next
     * tick); otherwise the error the recovery replay failed with, which the caller
     * feeds into the standard flush-retry accounting.
     */
    private Throwable recoverFromBaseMetadataDrift(LiveViewInstance instance) {
        final String viewName = instance.getDefinition().getViewName();
        instance.prepareForBaseSchemaRecompile();
        if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING) {
            // The recompiled factory expects the base's NEW metadata; the pinned base
            // snapshot is at the OLD metadata version. Drop it so the next sweep turn
            // re-pins a fresh snapshot consistent with the recompiled factory. A
            // metadata-only change preserves physical row order, so the sealed data
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
     * <p>
     * Two guards bracket the rebuild, because it is the last thing standing between a recoverable
     * fault and a permanent invalidation - and, for the same reason, the last place a silent schema
     * adoption could hide.
     * <p>
     * The rebuild refuses outright when the base's applied metadata no longer resolves every column
     * the view REFERENCES under the same name AND type
     * ({@link #isRederiveRefusedForBrokenDependency}). A restart makes that reachable with no drift
     * exception to stop it: a reloaded view has no compiled factory, so
     * {@code ensureCompiledFactory} compiles it against the base's CURRENT metadata and the replay
     * runs clean.
     * <p>
     * That question is asked TWICE, against freshly read metadata each time: once before the replay,
     * and again inside the drift catch below, before the recompile. The second ask earns its keep
     * because {@code ApplyWal2TableJob} applies a structural change to the base writer BEFORE it
     * calls {@code invalidateLiveViewsForBaseSchemaChange}, so an {@code ALTER TABLE base ALTER
     * COLUMN <referenced> TYPE ...} landing between the first read and the replay's reader open
     * passes the entry check on the OLD metadata and surfaces only as the drift - by which point the
     * first answer has decided nothing about the schema the recompile is about to adopt.
     * <p>
     * Neither ask CLOSES that window; both only narrow it. The check reads the base metadata under
     * its own guarded read and closes it before returning, and the recompile re-reads the
     * metadata independently - {@code ensureCompiledFactory} opens its own base reader - so nothing
     * pins a metadata version across the gap. A structural change landing inside that gap (one
     * {@code prepareForBaseSchemaRecompile} plus one compile wide) still passes the second check,
     * and the recompile still adopts the NEW schema and republishes the whole tier from it. Pinning
     * the metadata open across the whole recovery would close the gap, at the cost of holding a
     * pooled metadata tenant across a long call. What bounds the residue today is the apply side:
     * {@code invalidateLiveViewsForBaseSchemaChange} marks the registered instance invalid
     * microseconds later, and an invalid view stays queryable - so the worst surviving outcome is an
     * already-invalid view holding rows recomputed from the new schema rather than the stale
     * old-schema ones.
     * <p>
     * When every referenced column survives but the base metadata version moved anyway, the cached
     * plan is stale and {@code LiveViewRefreshSqlExecutionContext.getReader} refuses the reader with
     * {@link TableReferenceOutOfDateException}. A symbol-capacity rebuild is the canonical case, and
     * it is also what strands the WAL symbol dictionary that brings a lagging view here in the first
     * place, so this is not a corner: the recovery is recompiled through
     * {@link #recoverFromBaseMetadataDrift} and retried EXACTLY ONCE. Once, because the flush-retry
     * budget is already exhausted by the time this method runs, so nothing outside it would bound a
     * loop; and the retry cannot re-enter here, because
     * {@code rebuildActiveWindowStateFromAppliedBase} catches its own throwables and returns them.
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
        final String viewName = instance.getDefinition().getViewName();
        // The rebuild would read a broken dependency through the base's NEW schema and commit the
        // result, so refuse before touching anything.
        if (isRederiveRefusedForBrokenDependency(instance, baseToken, cause)) {
            return false;
        }
        try {
            if (simulateBaseApplyDuringRederiveForTest != null) { // @TestOnly, always null in production
                final Runnable baseApply = simulateBaseApplyDuringRederiveForTest;
                simulateBaseApplyDuringRederiveForTest = null;
                baseApply.run();
            }
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
                    .$(viewName)
                    .$(", head=").$(baseAppliedSeqTxn)
                    .$(", reason=").$safe(cause.getFlyweightMessage()).I$();
            return true;
        } catch (TableReferenceOutOfDateException drift) {
            // recoverFromBaseMetadataDrift only re-derives an ACTIVE view; for a SEEDING one it
            // re-arms the sweep and returns null WITHOUT rebuilding, so reporting that as a
            // recovery would claim a re-derive that did not happen - and this method has already
            // dropped the un-flushed lead via setLeadRowCount(0). Refuse instead: the caller
            // invalidates, which is the loud, correct outcome for a view whose data it cannot
            // reconstruct here.
            if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING) {
                return false;
            }
            // The drift proves the base metadata moved AFTER the entry check read it, so that read
            // decided nothing about the schema the recompile is about to adopt. Ask again, against
            // metadata read now. That narrows the window to the gap between this read and the
            // recompile's own reader open; it does not close it (see the method javadoc).
            if (isRederiveRefusedForBrokenDependency(instance, baseToken, cause)) {
                return false;
            }
            // The cached plan predates a base metadata change that left every referenced column
            // intact (re-checked just above), so the plan is stale rather than wrong. Recompile
            // through the sibling recovery and take the one retry the exhausted budget cannot give
            // us.
            LOG.info().$("live view re-derive found the base metadata changed, recompiling once [view=")
                    .$(viewName)
                    .$(", error=").$safe(drift.getFlyweightMessage()).I$();
            final Throwable recompiledError;
            try {
                // Java routes nothing thrown in one catch clause to a later clause of the same try,
                // and the recovery's prepareForBaseSchemaRecompile() closes artifacts, which this
                // file documents can throw. Catch it here so the refusal outcome is the same one the
                // trailing catch (Throwable) would have produced.
                recompiledError = recoverFromBaseMetadataDrift(instance);
            } catch (Throwable recoveryFailure) {
                LOG.error().$("live view could not re-derive from the applied base after base WAL loss [view=")
                        .$(viewName)
                        .$(", error=").$(recoveryFailure).I$();
                return false;
            }
            if (recompiledError != null) {
                LOG.error().$("live view could not re-derive from the applied base after base WAL loss [view=")
                        .$(viewName)
                        .$(", error=").$(recompiledError).I$();
                return false;
            }
            // The recompiled retry is the only thing separating this outcome from the plain success
            // path above - both leave the view valid over the same rows - so a test asserting the
            // outcome alone cannot tell them apart. Count the branch here, where it succeeded, and
            // the plain path stays at 0. @TestOnly, read by LiveViewSmokeTest.
            baseMetadataDriftRecompileCount++;
            LOG.info().$("live view re-derived from the applied base after base WAL loss and a base metadata change [view=")
                    .$(viewName)
                    // Not baseAppliedSeqTxn: the recompiled recovery re-reads the base writer txn
                    // itself and can carry the view further, so report where the view actually landed.
                    .$(", head=").$(instance.getLastProcessedSeqTxn())
                    .$(", reason=").$safe(cause.getFlyweightMessage()).I$();
            return true;
        } catch (Throwable e) {
            LOG.error().$("live view could not re-derive from the applied base after base WAL loss [view=")
                    .$(viewName)
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
        // No rows fed yet, so the accumulators match the last durable commit - unless a
        // previous turn left them wiped or half-advanced and could not rebuild them, in
        // which case the debt carries on the instance. The gate below reads the instance
        // flag directly and is what settles the debt; seeding the per-turn field here is
        // defence-in-depth, so a reader added ABOVE the gate cannot inherit the very
        // defect this fixes. Note the gate itself then clears the per-turn field while
        // leaving the instance debt standing, so the two are not equal at every point -
        // read the instance flag when the question is "does this view owe a rebuild".
        windowStateDirty = instance.isWindowStateDirty();
        // Bind the view so the shared context's getMemoryTracker() resolves to THIS view's
        // tracker; the window cursor reads it at open() to charge the functions' partition
        // maps. The finally clears it, so the worker's next view cannot charge this one.
        executionContext.ofRefreshingInstance(instance);
        try {
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
            // This check is load-bearing for the checkpoint handshake: it runs under
            // the refresh latch acquired above, so a freeze armed AFTER this turn took
            // the latch is observed here and skips the turn, while a freeze armed
            // BEFORE is serialised by startCheckpoint's latch take-and-release. That
            // handshake is what lets the in-band _lv.s rewrites drop waitForUnfrozen()
            // without racing the agent's copy - do not move a rewrite ahead of this
            // guard or out of the latch hold.
            // It tests isFreezeArmed(), not isFreezeInProgress(): startCheckpoint
            // publishes the copy flag only once it holds the latch, so a busy view
            // would otherwise keep retaking the latch ahead of the waiting agent.
            if (instance.isFreezeArmed()) {
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
            // Reconciliation gate. A prior turn's out-of-order repair committed a
            // REPLACE_RANGE whose inline apply never landed, so the live view's table
            // does not yet hold the output its WAL carries. Every coordinate this turn
            // would derive - the lifetime row count, a head checkpoint's lvRowPosition,
            // a repaired root's position, the consumed watermark - reads that table, so
            // refresh stays blocked until the block is known applied. Reporting no work
            // idles the worker instead of spinning a repair that would derive its
            // numbers from a table missing the rows; scanForLaggingViews re-drives the
            // apply on each sweep, and a suspended live view waits for an operator
            // RESUME WAL, serving disk-only behind the seqTxn fence meanwhile.
            if (!reconcilePendingReplacement(instance)) {
                return false;
            }
            // Labels the refresh body so a compromised timeline recovery
            // can break straight to the out-of-latch invalidation below, skipping
            // the refresh + flush that would otherwise materialise the
            // inconsistent accumulators to disk.
            refreshBody:
            try {
                // A localized out-of-order repair parked on its turn budget. It holds
                // the pinned base snapshot its bounds were derived against, an
                // uncommitted replacement and a runtime half-way through the replay, so
                // no other work may run over this view until it finishes: every
                // coordinate a turn would derive reads that runtime, and re-planning
                // would abandon a candidate that is still good. Continue it and return -
                // the next tick picks the ordinary cadence back up.
                final LiveViewCheckpointRepairSession suspendedRepair = instance.getSuspendedRepair();
                if (suspendedRepair != null) {
                    if (suspendedRepair.getOwner() != this) {
                        // Another worker's continuation: its pools hold the reader and the
                        // writer, and its timeline store writer owns the capture. Report no
                        // work so this worker backs off rather than rescanning the view.
                        return attempted;
                    }
                    attempted = true;
                    resumeSuspendedRepair(instance, suspendedRepair);
                    instance.recordRefreshSuccess();
                    return attempted;
                }
                // First cycle after restart restores the newest compatible
                // timeline root, or rebuilds derived state when the timeline is
                // absent/unusable. The drain below must never start over durable
                // output with cold accumulators.
                // Single-shot per LV lifetime - the flag flips true whether the
                // restore succeeded, missed, or failed.
                if (!instance.isCheckpointRestoreAttempted()) {
                    // Reconcile a durable floor left behind by a crash between the
                    // inline apply and the trailing _lv.s persist, before timeline
                    // selection reconciles its generation coordinates.
                    if (!reconcileAppliedFloorAfterRestart(instance)) {
                        // The view's own WAL holds a block its table has not applied, so
                        // every coordinate the restore below derives - the floor it clamps,
                        // the row count and frontier tryRestoreFromTimeline reads - would
                        // describe rows that are not there. Report no work and leave the
                        // flag unset so the whole restore retries once the block lands;
                        // burning it here would make the miss permanent for this view's
                        // lifetime. A suspended view waits for an operator RESUME WAL,
                        // serving disk-only meanwhile.
                        return false;
                    }
                    instance.setCheckpointRestoreAttempted();
                    if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
                        // Baseline observability: time bounded generation selection,
                        // root restore, and the (B,F] replay. Recorded once
                        // per LV lifetime regardless of outcome. Surfaced via
                        // live_views().checkpoint_last_restore_micros.
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
                    attempted = true;
                    runSeedSweep(instance);
                    instance.recordRefreshSuccess();
                    return attempted;
                }
                // A previous turn wiped or half-advanced the accumulators and its own
                // rebuild failed, so the runtime still disagrees with the durable tier.
                // Draining forward from here would commit cumulative output derived from
                // that runtime and then call recordRefreshSuccess(), which resets the
                // flush-retry budget - so the view would serve wrong totals and never
                // invalidate itself out of them. Rebuild from the applied base first. A
                // rebuild that fails again charges the budget through handleRefreshFailure
                // until it exhausts and the view invalidates honestly, so this terminates
                // either way; a rebuild that succeeds commits, which clears the debt.
                if (instance.isWindowStateDirty()) {
                    attempted = true;
                    final Throwable rebuildErr = rebuildWindowStateAfterMidDrainFailure(instance);
                    if (rebuildErr != null) {
                        // Already rebuilt-and-failed here, so stop handleRefreshFailure
                        // repeating it for this turn; the debt stays on the instance.
                        windowStateDirty = false;
                        invalidationReason = handleRefreshFailure(instance, rebuildErr);
                        break refreshBody;
                    }
                    windowStateDirty = instance.isWindowStateDirty();
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
                // A read-only replica reads the applied base (see prefersAppliedBaseRefresh) via the
                // coupled drainAppliedBase path, exactly like a DEDUP base: no un-flushed in-RAM lead,
                // the tier stays a subset of disk. So it is never lead-eligible.
                final boolean appliedBase = prefersAppliedBaseRefresh();
                final boolean leadEligible = ensureLeadEligible(instance) && !dedupBase && !appliedBase;
                final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                final long lastFlushUs = instance.getLastFlushTimeUs();
                final long flushEveryMicros = instance.getDefinition().getFlushEveryMicros();
                final boolean flushDue = lastFlushUs == Numbers.LONG_NULL || nowUs - lastFlushUs >= flushEveryMicros;
                if (leadEligible) {
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
                    // Flush the accumulated lead on the FLUSH EVERY cadence. The refresh may also have
                    // flushed (emergency, on a tier stall), in which case refreshedUpTo == lastProcessed
                    // and this is skipped.
                    if (flushDue && instance.getRefreshedUpToSeqTxn() > instance.getLastProcessedSeqTxn()) {
                        attempted = true;
                        flushLead(instance, getWindowFactory(instance), instance.getRefreshedUpToSeqTxn(), 0);
                        instance.setLastFlushTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                    }
                } else {
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
                        if (appliedBase) {
                            // Read-only replica: the raw base WAL races its own async download/apply, so
                            // always read the applied, post-apply base table. drainAppliedBase pins it
                            // behind the cooperative apply-lag gate and routes any timestamp overlap
                            // through o3Replay -- the replica owns and rewrites its own LV disk under
                            // symmetric refresh. Deliberately bypasses the dedup isRangeProvablyClean
                            // raw-WAL shortcut: a replica has no settled raw WAL to fast-path against.
                            drainAppliedBase(instance, lastSeqTxn, seqTxn);
                        } else if (dedupBase) {
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
                if (attempted) {
                    instance.recordRefreshSuccess();
                }
            } catch (LiveViewApplyLagException e) {
                // Cooperative apply-lag handoff: this cycle's O3 replay needs the
                // base applied to a seqTxn ApplyWal2TableJob has not reached yet.
                // ensureBaseApplied threw before any destructive replay work, so
                // the view's DURABLE output is untouched - no watermark advance, no
                // failure accounting, no invalidation. Leave invalidationReason null and
                // return through the finally; the next fallback scan retries this
                // view (head > processedTo still holds) once the apply catches up.
                // Not counting this toward the flush-retry budget is deliberate:
                // apply lag is transient and self-heals, unlike a refresh fault.
                //
                // The compiled factory's accumulators are NOT untouched. The drain that
                // raised this had already fed every commit below the offending one through
                // the window cursor, and its O3 detect rolled back only the WAL draft and
                // latestSeenTs - the accumulators keep every row they counted. So carry the
                // debt onto the instance: windowStateDirty is a per-turn field that
                // refreshInstance re-seeds from the instance at every entry, so without this
                // the next turn starts from a clean slate and drains those same commits
                // again over accumulators that already counted them. The re-fed rows then
                // carry cumulative values continuing from the abandoned cycle - a running
                // count(*) emits N+1.. for what is the view's FIRST row - and the lead
                // publish makes them reader-visible without any commit at all. The
                // instance.isWindowStateDirty() gate rebuilds from the applied base before
                // that drain instead. Gated on the flag rather than raised unconditionally:
                // a cycle that deferred before feeding a single row owes no rebuild.
                // See LiveViewConcurrencyTest.testApplyLagDeferralRebuildsAdvancedWindowState.
                if (windowStateDirty) {
                    markWindowStateDirty(instance);
                }
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
                invalidationReason = handleRefreshFailure(instance, t);
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
            // (e.g. a version-too-old function snapshot). Drain and run it on
            // the same out-of-latch path.
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
     * CREATE).
     */
    private String handleRefreshFailure(LiveViewInstance instance, Throwable t) {
        // Count the fault before any of the branches below decide to swallow it. Most of them do:
        // the read-only-gate refusal, the metadata-drift recompile and the mid-drain rebuild all
        // return null, and the rebuild even calls recordRefreshSuccess(), so nothing else survives to
        // tell a test that the incremental path faulted at all.
        instance.recordRefreshFault();
        // A parked repair cannot survive a fault on this view, whichever branch below
        // takes it. Every recovery here rebuilds the window state the candidate's replay
        // was standing in and rewrites the durable output its staged roots describe; a
        // back-off leaves a pinned base snapshot and an uncommitted replacement held over
        // a view that just failed. Discarding rolls the replacement back, unlinks the
        // staged segment, retires the descriptor and puts the pre-repair state back, which
        // is also what makes the mid-drain rebuild below correct rather than merely safe.
        // Idempotent: a repair that faulted inside its own turn was already released by
        // the executor's unwind. Runs under the refresh latch, as discardSuspendedRepair
        // requires.
        instance.discardSuspendedRepair();
        // Captured before the metadata-drift block reassigns t: that path already
        // rebuilds, so the mid-drain rebuild below must not fire a second time.
        final boolean wasMetadataDrift = t instanceof TableReferenceOutOfDateException;
        if (t instanceof CairoException cancelled && cancelled.isCancellation()) {
            // The circuit breaker tripped. Only three things trip it, and none is a refresh
            // fault: DROP and invalidation, both of which end this view's refreshing life
            // outright (the guards at the top of refreshInstance refuse it from here on),
            // and engine shutdown, where restart recovery rebuilds the runtime from _lv.s
            // and the checkpoint timeline anyway. So the accumulators being left ahead of
            // the last durable commit costs nothing: no later turn drains over them.
            // Counting a cancellation toward the flush-retry budget would let a shutdown
            // that caught several views mid-scan invalidate them durably on the way down,
            // and running the mid-drain rebuild below would re-enter a scan the same
            // breaker is still tripping.
            LOG.info().$("live view refresh cancelled [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", dropped=").$(instance.isDropped())
                    .$(", invalid=").$(instance.isInvalid()).I$();
            return null;
        }
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
            t = recoverFromBaseMetadataDrift(instance);
            if (t == null) {
                return null;
            }
            // The recovery replay itself failed; account for THAT error below.
        }
        // Mid-drain fault with the accumulators advanced past the last durable commit:
        // rebuild from the applied base so the retry does not double-advance them. Skip
        // when the drift path already rebuilt, or when nothing was fed (windowStateDirty
        // false - includes a transient table-absent during CREATE / DROP).
        if (windowStateDirty
                && !wasMetadataDrift
                && !(t instanceof CairoException dce && dce.isTableDoesNotExist())) {
            Throwable rebuildErr = rebuildWindowStateAfterMidDrainFailure(instance);
            if (rebuildErr == null) {
                return null;
            }
            // The rebuild replay itself failed, so the runtime is still wiped or
            // half-advanced. Carry the debt onto the instance: this turn's field is about
            // to go out of scope and the next turn's entry would read a clean slate,
            // which is what lets a drain start over durable output with cold accumulators.
            instance.setWindowStateDirty(true);
            // The rebuild replay itself failed; account for THAT error below.
            t = rebuildErr;
            if (t instanceof CairoException rebuildCancelled && rebuildCancelled.isCancellation()) {
                // Re-test after the reassignment. The replay consults the same breaker, so
                // a shutdown or a DROP that arrived mid-rebuild surfaces here rather than at
                // the guard above - and counting it toward the flush-retry budget is exactly
                // what that guard exists to prevent.
                LOG.info().$("live view refresh cancelled during mid-drain rebuild [view=")
                        .$(instance.getDefinition().getViewName()).I$();
                return null;
            }
        }
        long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        instance.recordRefreshFailure(nowUs);
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
        // The duration term needs an actual retry window to measure. On the first failure
        // retryStartUs is unset, so elapsedUs is 0, and a configured budget of 0 would make
        // "0 >= 0" exhaust the budget before !tableTransient is ever consulted - permanently
        // invalidating a view on its first fault and defeating the CREATE-window carve-out
        // documented above. For every positive budget this is behaviour-neutral: elapsedUs is 0
        // on the first failure, which is already below the cap.
        boolean durationExhausted = retryStartUs != Numbers.LONG_NULL && elapsedUs >= maxDurationMicros;
        boolean budgetExhausted = durationExhausted || (!tableTransient && retryCount >= maxRetry);
        if (budgetExhausted) {
            // Last resort before a permanent invalidation: a base WAL segment the drain needs has
            // been missing for the whole budget, so it is not coming back. That is what a restore
            // leaves behind - a backup captures the applied base TABLE, not its WAL segments - and
            // the view's rows are all in that table, so re-derive from it rather than brick a view
            // whose data is right there. Spending the budget first is what separates this from a
            // transient read fault, which clears on a retry and never reaches here.
            if (t instanceof CairoException walMissing
                    && isBaseWalSegmentFileMissing(walMissing)
                    && rederiveFromAppliedBaseAfterWalLoss(instance, walMissing)) {
                return null;
            }
            LOG.critical().$("live view refresh budget exhausted, invalidating [view=").$(instance.getDefinition().getViewName())
                    .$(", retryCount=").$(retryCount)
                    .$(", elapsedUs=").$(elapsedUs)
                    .$(", error=").$(t).I$();
            // The re-derive refuses a base schema change that broke a column the view references,
            // and stashes a reason naming that column. Prefer it: the generic budget message would
            // hide the one diagnostic an operator can act on, and leaving the stash undrained would
            // carry it onto an unrelated later invalidation (refreshInstance only drains it when no
            // reason was returned here).
            final String rederiveRefusal = instance.takePendingInvalidationReason();
            return rederiveRefusal != null ? rederiveRefusal : "flush retry budget exhausted";
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
            // Deferred-name transient, the same one scanForLaggingViews skips on. Both loaders publish
            // the refresh instance BEFORE they commit the view's table name - createLiveView through
            // commitDeferredTableNameAndRelease, and a replica's registration path through the token
            // commit that follows its sidecar rebuild - so a base-table commit landing inside that
            // window reaches a view whose name does not resolve yet. Refreshing it anyway costs more
            // than the failed turn: refreshInstance burns the SINGLE-SHOT checkpoint-restore flag
            // before getWalWriter throws "table does not exist", and the flag flips true whether the
            // restore ran or not - so a view with a real _checkpoints/ timeline (a re-registration, or
            // a restored data directory) permanently loses its resume and drains over durable output
            // with cold accumulators. Skipping costs nothing: the fallback scan re-drives the view by
            // comparing its watermark against the base head, so no notification is lost, only delayed.
            // getTableTokenIfExists returns null for a locked/absent name - exactly the transient.
            if (engine.getTableTokenIfExists(instance.getLiveViewToken().getTableName()) == null) {
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
        // Highest designated timestamp any commit this pass walked touched - inserted
        // rows plus the top of a REPLACE_RANGE delete band - or LONG_NULL when the
        // pass walked something whose reach cannot be read off the commit metadata (a
        // compacted / structural sequencer entry, or a non-DATA commit such as DROP
        // PARTITION, TTL or TRUNCATE). Meaningful when o3Detected: the hand-off
        // re-materialises everything this pass rolled back, so it is that repair's
        // change-set upper bound and the input its convergence boundary H is derived
        // from.
        public long o3ChangeMaxTs;
        // Whether every commit this pass walked only ADDED base rows: no REPLACE_RANGE
        // delete band reaching into the view, no non-DATA commit (DROP PARTITION / TTL /
        // TRUNCATE) and no compacted / structural sequencer entry. Meaningful when
        // o3Detected: the ROWS repair bounds are discovered by reading the post-change
        // snapshot, so a deletion that emptied a partition key out of the change interval
        // would leave it invisible to that search. Says nothing about apply-time dedup,
        // which this raw-WAL walk cannot see - the repair proves that separately.
        public boolean o3ChangeInsertOnly;
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
            o3ChangeInsertOnly = false;
            o3ChangeMaxTs = Numbers.LONG_NULL;
            o3Detected = false;
            o3LateRowTs = Numbers.LONG_NULL;
            o3SeqTxn = Numbers.LONG_NULL;
            stagingMaxTs = Numbers.LONG_NULL;
            stagingMinTs = Numbers.LONG_NULL;
        }
    }

    /**
     * Runs the bounded ROWS repair-bound discovery on behalf of
     * {@link LiveViewCheckpointRepairPlan}, which calls back into it once it has computed
     * the output floor {@code R} both searches start from.
     * <p>
     * The scans read through the same source-plus-filter stack the replay does - the base
     * factory's page-frame cursors under the view's own {@code WHERE} - so "qualifying"
     * means at planning time exactly what it means at replay time. They read it through
     * the same pinned reader for the same reason: a second reader opened later could sit
     * at a different {@code seqTxn}, and bounds derived against it would not describe the
     * data the replay is about to see. Binding that reader to the execution context is
     * therefore borrowed for one discovery and handed straight back, because the
     * executors bind it again for the replay itself.
     * <p>
     * An anchored view never gets here: its windows carry a fixed-anchor dependency
     * rather than a finite ROWS one, so no ROWS plan exists to discover bounds for, and
     * the anchor cursor the executors splice in has no counterpart here.
     */
    private final class RowsBoundDiscovery implements LiveViewCheckpointRepairPlan.RowsBoundSource {
        private boolean discovered;
        private Function filter;
        private PageFrameRecordCursorFactory pageFrameFactory;
        private LiveViewCheckpointRowsPlan plan;
        private TableReader reader;

        @Override
        public void collectRowsOutputKeys(@NotNull LiveViewCheckpointOutputKeyDomain out) {
            rowsBounds.collectOutputKeys(out);
        }

        @Override
        public void discoverRowsBounds(
                long viewLowerBoundTs,
                long outputLowTs,
                long changeLowTs,
                long changeMaxTs
        ) throws SqlException {
            engine.detachReader(reader);
            try {
                executionContext.of(reader);
                rowsBounds.discover(
                        plan,
                        pageFrameFactory,
                        executionContext,
                        filter,
                        viewLowerBoundTs,
                        outputLowTs,
                        changeLowTs,
                        changeMaxTs
                );
                discovered = true;
            } finally {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
        }

        @Override
        public long getRowsDependencyLowTs() {
            return rowsBounds.getDependencyLowTs();
        }

        @Override
        public HighBoundTag getRowsHighBoundTag() {
            return rowsBounds.getHighBoundTag();
        }

        @Override
        public long getRowsHighTsExclusive() {
            return rowsBounds.getHighTsExclusive();
        }

        @Override
        public boolean isRowsOutputKeyDomainComplete() {
            return rowsBounds.isOutputKeyDomainComplete();
        }

        @Override
        public boolean isRowsScanBudgetExceeded() {
            return rowsBounds.isScanBudgetExceeded();
        }

        /**
         * @return whether the plan went on to call {@link #discoverRowsBounds}, and so
         * whether the counters the discovery carries describe this repair. It declines
         * the call for a repair that could not have used the answer, and the counters
         * would otherwise still hold the previous repair's reads.
         */
        boolean hasDiscovered() {
            return discovered;
        }

        void of(LiveViewCheckpointRowsPlan plan, LiveViewCompiledPlan compiledPlan, TableReader reader) {
            this.discovered = false;
            this.plan = plan;
            this.reader = reader;
            this.filter = compiledPlan.getFilter();
            this.pageFrameFactory = compiledPlan.getPageFrameFactory();
        }
    }

    /**
     * The versioned timeline's predecessor lookup, in the shape
     * {@link LiveViewCheckpointRepairPlan} plans a resume through. Every logical
     * boundary that still covers its own timestamp group is a candidate, so
     * however old a correction is, the search still answers with the newest such
     * boundary below it.
     * <p>
     * The lookup binds the worker's timeline store reader per search rather than
     * holding it bound across the repair. A repair runs at most two searches, both
     * during planning and both before anything is staged, so a bind costs one
     * superblock read and the root metadata pages on the search path. The
     * generation each search pins is released before it returns, and the bind
     * itself ends with it, which is what lets the repair's own capture pin the
     * generation it splices into.
     * <p>
     * A view with no readable timeline - never sealed, retired by an earlier
     * repair, or corrupt - reports no anchor rather than raising, so the plan
     * takes the rebuild it would take for a change below every boundary.
     */
    private final class TimelineAnchorSource implements LiveViewCheckpointRepairPlan.AnchorSource {
        private LiveViewInstance instance;

        @Override
        public boolean findAnchorBelow(long ceilTs, @NotNull LiveViewCheckpointTimelineEntry out) {
            long ceiling = ceilTs;
            try (Path checkpointsDir = new Path()) {
                checkpointsDir.of(engine.getConfiguration().getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                final LiveViewCheckpointTimelineStoreReader reader =
                        borrowCheckpointTimelineStoreReader(checkpointsDir);
                try {
                    while (true) {
                        final long lvRowPosition = reader.predecessorLvRowPosition(ceiling, out);
                        if (lvRowPosition == Numbers.LONG_NULL) {
                            return false;
                        }
                        if (coversOwnTimestampGroup(out, lvRowPosition)) {
                            return true;
                        }
                        LOG.info().$("live view resume anchor no longer covers its timestamp group, re-anchoring below it [view=")
                                .$(instance.getDefinition().getViewName())
                                .$(", anchorMaxTs=").$ts(out.maxTimestamp)
                                .$(", anchorCheckpointId=").$(out.checkpointId)
                                .$(", lvRowPosition=").$(lvRowPosition).I$();
                        // Strictly below the boundary just rejected, which is the
                        // next-older root - the rejected one only under-covers its
                        // own group, so everything below it is still sealed against
                        // this correction.
                        ceiling = out.maxTimestamp;
                    }
                } finally {
                    reader.detach();
                }
            } catch (Throwable t) {
                LOG.info().$("live view checkpoint timeline holds no resume anchor [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", ceilTs=").$ts(ceilTs)
                        .$(", reason=").$(t).I$();
                return false;
            }
        }

        /**
         * Whether the candidate boundary still means what a resume reads it to mean:
         * the window state after <i>every</i> qualifying row at or below its
         * {@code maxTimestamp}.
         * <p>
         * A root can stop meaning that after it is written. The cadence seal refuses
         * to append a second boundary at the head's own timestamp, so when a later
         * in-order commit adds more rows at that timestamp the existing root is left
         * describing only part of the group. A resume anchored there restores the
         * partial state and replays from {@code maxTimestamp + 1}, which reads back
         * neither the missing rows nor their contribution - every value it computes
         * from then on is short by them, durably.
         * <p>
         * Two checks, because the evidence differs either side of a head transition:
         * <ul>
         *     <li><b>The head's own root</b> answers from the runtime.
         *     {@code minSeenTsSinceCheckpoint} is reset by the seal and lowered by
         *     every row the runtime consumes since, and immediately after a seal the
         *     frontier sits at the boundary - so a row at or below it is exactly the
         *     tie growing. This costs nothing and covers the common resume, whose
         *     anchor is the head.</li>
         *     <li><b>Every older root</b> answers from the durable live-view table,
         *     which is authoritative for a root's position (the corrupt-root heal
         *     positions its repaired roots the same way). The table can lag a root -
         *     a live-view block committed but not yet applied - and never leads it,
         *     so holding strictly more rows at or below the boundary than the root
         *     claims as its whole prefix proves rows landed there after it was
         *     sealed. The head's in-memory answer does not survive the head moving
         *     on, and this one does, including across a restart.</li>
         * </ul>
         * A boundary partition the live-view table does not hold natively has no
         * searchable prefix, so it yields no evidence either way and the anchor
         * stands.
         */
        private boolean coversOwnTimestampGroup(LiveViewCheckpointTimelineEntry entry, long lvRowPosition) {
            if (entry.maxTimestamp == instance.getHeadCheckpointMaxTs()
                    && entry.checkpointId == instance.getHeadCheckpointRootId()) {
                return instance.getMinSeenTsSinceCheckpoint() > entry.maxTimestamp;
            }
            try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
                final long durableRowsBelow = countDurableRowsBelow(
                        lvReader,
                        entry.maxTimestamp == Long.MAX_VALUE ? Long.MAX_VALUE : entry.maxTimestamp + 1
                );
                return durableRowsBelow < 0 || durableRowsBelow <= lvRowPosition;
            }
        }

        private void of(LiveViewInstance instance) {
            this.instance = instance;
        }
    }

    /**
     * Output bundle for
     * {@link #restoreSeedFromTimeline(LiveViewInstance, WindowRecordCursorFactory, RestoredSeedState)}.
     * The fields capture the values the seed resume needs after the disk read
     * completes; the helper rewrites them on each successful call and the caller
     * reads them immediately.
     */
    private static final class RestoredSeedState {
        long lvRowsTotal;
        long maxTimestamp;
        // Seed sweep's data-cursor row offset, read from the seed cursor the
        // restored generation carries.
        long resumeDataOffset;
        long stateBytes;

        void reset() {
            lvRowsTotal = 0L;
            maxTimestamp = Numbers.LONG_NULL;
            resumeDataOffset = Numbers.LONG_NULL;
            stateBytes = 0L;
        }
    }
}
