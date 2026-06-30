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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

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
    // Sentinel returned by replayToApplied when it detected an out-of-order base
    // commit mid-gap and handed off to o3Replay (which rebuilt disk + re-stamped
    // the watermarks). Distinct from the non-negative replayed-row counts.
    private static final long REPLAY_TO_APPLIED_O3 = -1L;
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final AnchorDispatchingCursor anchorDispatchingCursor = new AnchorDispatchingCursor();
    private final ApplyWal2TableJob applyJob;
    // Reusable counter for the backfill sweep's skipRows() resume positioning.
    private final RecordCursor.Counter backfillSkipCounter = new RecordCursor.Counter();
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
    // Bulk-copy buffer for restoring per-function snapshot blocks. The
    // snapshot framework reads a function's payload from offset 0, so it
    // copies the payload bytes here from the checkpoint file and hands the
    // scratch to LiveViewFunctionSnapshot.restore. Lazily
    // allocated; reused across functions and across cycles. Freed at
    // job close.
    private MemoryCARW checkpointRestoreScratch;
    // Per-worker reusable checkpoint writer. Lazily allocated on the first
    // cycle that triggers a head write; reused across cycles via of() / commit().
    // Memory pages stay mmapped between writes so a frequently-checkpointed LV
    // does not pay reopen cost. Freed at job close.
    private LiveViewCheckpointWriter checkpointWriter;
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final IntList columnIndexes = new IntList();
    private final IntList columnSizeShifts = new IntList();
    // Reusable out-params from a base-WAL drain pass (drainBaseWal), shared by the
    // disk-subset cycle and the lead refresh. Mutated only on the refresh-worker
    // thread between reset() and use.
    private final DrainResult drainResult = new DrainResult();
    private final CairoEngine engine;
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
    private final int workerId;

    public LiveViewRefreshJob(int workerId, CairoEngine engine, int sharedQueryWorkerCount) {
        this.workerId = workerId;
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
        checkpointRestoreScratch = Misc.free(checkpointRestoreScratch);
        checkpointWriter = Misc.free(checkpointWriter);
        stagingBuffer = Misc.free(stagingBuffer);
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        // workerId is the fixed per-worker identity captured at assign(int, job)
        // time. The continuation framework may remount this job on a peer carrier,
        // so workerContext.carrierId() is not asserted against it here.
        return processNotifications();
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
            window = LiveViewWindow.build(
                    engine.getConfiguration(),
                    compiler.getAsm(),
                    spec.windowName,
                    projectedMeta,
                    spec.partitionColumnNames,
                    fn,
                    anchoredFunctions
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

        // Floor the forward-append path takes too. A non-BACKFILL view's lower
        // bound is the CREATE wall-clock moment; a BACKFILL view's is the
        // earliest visible base row. O3 head-miss replay already seeds its scan
        // at this floor and drops everything below it, so the forward-append
        // path must drop sub-floor rows as well - otherwise a back-dated row
        // would be kept when it arrives in order (forward) but dropped when it
        // arrives out of order (replay), making the view's contents depend on
        // the arrival path rather than the data. Only reachable for data
        // ingested before CREATE, which production does not do.
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();

        // Decide whether the in-memory tier can be populated
        // for this LV. Only LVs whose output schema is fully fixed-width are
        // supported in this phase; var-length columns fall back to disk-only.
        // The staging buffer is reshaped on schema-mismatch; the LV's tier is
        // lazily allocated on first use.
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
            drainBaseWal(
                    instance, windowFactory, baseToken, baseMetadata, baseTimestampIndex,
                    cursorTimestampIndex, viewLowerBoundTimestamp, filter, fromSeqTxn, toSeqTxn,
                    null, null, populateTier, latestSeenTsSnapshot
            );
            finishLeadRefresh(instance, windowFactory, baseToken, populateTier);
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
                    cursorTimestampIndex, viewLowerBoundTimestamp, filter, fromSeqTxn, toSeqTxn,
                    walWriter, copier, populateTier, latestSeenTsSnapshot
            );
            if (drainResult.appendedRows > 0) {
                // The LV WAL block carries advanceTo as maxBaseSeqTxnInBlock. The
                // inline apply below makes the rows durable in the LV's on-disk
                // table; only then do we advance lvConsumedSeqTxn so base WAL
                // retention releases.
                walWriter.commitLiveView(drainResult.advanceTo);
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
            // un-flushed lead). It serves SYMBOL-output and var-length-output LVs;
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
                // tier just trails this cycle. A fixed-width output schema (the only
                // tier-populating shape, SYMBOL included since eager interning) is
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
                maybeWriteHeadCheckpoint(instance, windowFactory, advanceTo, batchMaxTs, appendedRows);
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
        final TableReader committedSymbolReader = internSymbols ? engine.getReader(instance.getLiveViewToken()) : null;
        try (TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn); committedSymbolReader) {
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
                final boolean crossCommitO3 = latestSeen != Numbers.LONG_NULL && txnMinTs < latestSeen;
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
                    o3LateRowTs = txnMinTs;
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

                walNameSink.clear();
                walNameSink.put(WAL_NAME_BASE).put(walId);
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
                                for (int si = 0, sn = stagingSymbolColumnIndexes.size(); si < sn; si++) {
                                    final int c = stagingSymbolColumnIndexes.getQuick(si);
                                    stagingBuffer.putInt(appendedRows, c,
                                            symbolCache.intern(c, outRecord.getSymA(c), committedSymbolReader.getSymbolMapReader(c)));
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
                } finally {
                    windowCursor.close();
                }
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
            walWriter.commitLiveView(advanceTo);
        } finally {
            // The overlays reference symbolReader, now closed; drop them so a later
            // flush of a non-SYMBOL view cannot reuse a stale resolver.
            bufferRecord.setSymbolResolvers(null);
        }

        instance.setLastProcessedSeqTxn(advanceTo);
        instance.setAppliedWatermark(advanceTo);
        applyJob.applyWalDirect(token, Job.RUNNING_STATUS);
        final long lvAppliedSeqTxn = engine.getTableSequencerAPI().getTxnTracker(token).getWriterTxn();
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
            } else {
                // Normal flush: the lead rows are now on disk and still in the slot,
                // so it is a complete subset of disk. Re-stamp it so reads regain
                // seam routing immediately.
                restampSlotAfterFlush(instance, lvAppliedSeqTxn);
            }
            maybeWriteHeadCheckpoint(instance, windowFactory, advanceTo, flushedMaxTs, flushRows);
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
                resolvers.add(new LiveViewSymbolTable().of(symbolReader.getSymbolTable(c), cache, c, false));
            } else {
                resolvers.add(null);
            }
        }
        return resolvers;
    }

    /**
     * After a flush lands the lead on disk, re-stamps the published slot as a
     * subset of disk: the slot's stored seqTxn becomes the just-applied LV-table
     * seqTxn and its lead count drops to zero, so reads regain seam routing
     * (fence holds) immediately. Best-effort fast-path: when a reader pins the
     * slot the {@code 0 -> -1} CAS fails and the slot keeps its prior (now stale)
     * stamp, which the fence routes disk-only - correct, since disk holds the
     * just-flushed rows - until the next refresh re-publishes a fresh slot.
     */
    private void restampSlotAfterFlush(LiveViewInstance instance, long lvAppliedSeqTxn) {
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            return;
        }
        final int publishedIdx = tier.getPublishedIdx();
        final LiveViewInMemoryBuffer acquired = tier.tryAcquireWrite(publishedIdx);
        if (acquired == null) {
            return;
        }
        try {
            acquired.setLvSeqTxn(lvAppliedSeqTxn);
            acquired.setLeadRowCount(0);
        } finally {
            tier.releaseWriteWithoutPublish(publishedIdx);
        }
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
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
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
        final boolean headHitEligible = headLvSeqTxn != Numbers.LONG_NULL && headMaxTs < lateRowTs;
        LOG.info().$("live view O3 replay [view=").$(viewName)
                .$(", lateRowTs=").$(lateRowTs)
                .$(", advanceTo=").$(advanceTo)
                .$(", headHitEligible=").$(headHitEligible)
                .$(", headLvSeqTxn=").$(headLvSeqTxn)
                .$(", headMaxTs=").$(headMaxTs).I$();

        if (headHitEligible) {
            o3HeadHitReplay(instance, windowFactory, baseToken, advanceTo, headLvSeqTxn, headMaxTs);
        } else {
            o3HeadMissReplay(instance, windowFactory, baseToken, advanceTo);
        }

        // The replay rewrote the on-disk tier (REPLACE_RANGE); the in-mem tier
        // still holds the pre-replay output rows for the rewritten range. Rebuild
        // it atomically from the rewritten LV table so a post-O3 cursor regains
        // seam routing immediately instead of falling through to disk until the
        // next normal cycle republishes. The seqTxn fence keeps this safe either way.
        // See rebuildInMemoryTier.
        rebuildInMemoryTier(instance);
    }

    /**
     * Head-hit replay: rolls window state back to the head .cp's
     * snapshot moment (clear per-function maps, then restore from disk),
     * scans the base table from {@code headMaxTs + 1} forward, and emits
     * a single REPLACE_RANGE commit covering {@code [headMaxTs + 1, +inf)}.
     * Cheaper than head-miss because the head's state already reflects
     * everything in {@code [viewLowerBoundTimestamp, headMaxTs]} - the
     * replay only re-evaluates the small tail.
     * <p>
     * Caller has already verified that the head is hit-eligible (a head
     * exists and its {@code maxTimestamp <= lateRowTs}). The restoration
     * itself can still fail at this point (corrupt .cp, unsupported
     * format version): when that happens, {@code restoreFromHead} unlinks
     * the .cp + clears head metadata, and this method falls through to
     * {@code o3HeadMissReplay} so the LV ends in a consistent state.
     * <p>
     * The head .cp is retired after the apply commit succeeds (its state
     * underwrites the replay we just wrote). The follow-up commit writes
     * a fresh post-replay head; until then the next refresh cycle's
     * first-commit cadence trigger picks up the slack.
     */
    private void o3HeadHitReplay(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            TableToken baseToken,
            long advanceTo,
            long headLvSeqTxn,
            long headMaxTs
    ) throws SqlException {
        final String viewName = instance.getDefinition().getViewName();
        // Replay starts strictly above headMaxTs because the head's state
        // already covers rows up to and including headMaxTs. The same value
        // doubles as the REPLACE_RANGE low boundary so the apply step
        // rewrites only the affected partitions.
        final long replayLowTs = headMaxTs + 1;
        TableReader reader = waitForApply(baseToken, advanceTo);
        if (reader.getSeqTxn() != advanceTo) {
            // ApplyWal2TableJob has run ahead of the O3 trigger: the base reader
            // snapshot already reflects seqTxns past advanceTo that the forward
            // drain has not examined. Head-hit only re-reads base above headMaxTs,
            // so a back-dated row below headMaxTs sitting in one of those
            // unexamined seqTxns would be missed - and advancing the watermark to
            // cover them would lose it permanently. Fall back to head-miss, which
            // recomputes the whole view from the full snapshot and advances the
            // watermark to exactly what it materialised. waitForApply guarantees
            // reader.getSeqTxn() >= advanceTo, so this branch is the strictly-ahead
            // case; head-hit proceeds only when the snapshot is exactly at the
            // trigger (no unexamined seqTxns, so no gap and no duplicate).
            reader.close();
            o3HeadMissReplay(instance, windowFactory, baseToken, advanceTo);
            return;
        }
        boolean readerAttached = false;
        boolean restoredOk = false;
        long appendedRows = 0;
        long replayMaxTs = Numbers.LONG_NULL;
        try {
            engine.detachReader(reader);
            executionContext.of(reader);
            readerAttached = true;

            RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
            final Function filter = filterFactory.getFilter();
            RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
            RecordMetadata baseMetadata = pageFrameFactory.getMetadata();
            final int baseTimestampIndex = baseMetadata.getTimestampIndex();
            RecordMetadata outMetadata = windowFactory.getMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();

            try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
                try (RecordCursor pageCursor = pageFrameFactory.getCursor(executionContext)) {
                    tsLowerBoundCursor.of(pageCursor, baseTimestampIndex, replayLowTs);
                    RecordCursor source = tsLowerBoundCursor;
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
                        if (!restoreFromHead(instance, windowFactory, headLvSeqTxn, restoredHeadState)) {
                            // restoreFromHead retired the corrupt .cp + cleared
                            // head metadata (or stashed an invalidate reason).
                            // State is now empty across the board - the same
                            // starting condition head-miss replay expects.
                            // try-with-resources closes the cursor on return.
                            return;
                        }
                        // Snap the lifetime row counter back to the head's
                        // recorded value: the upcoming REPLACE_RANGE commit
                        // logically truncates rows above replayLowTs, so the
                        // counter rewinds in step with the table.
                        instance.setLvRowsTotal(restoredHeadState.lvRowsTotal);
                        restoredOk = true;
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
                    }
                    if (appendedRows > 0) {
                        walWriter.commitLiveViewWithReplaceRange(advanceTo, replayLowTs, Long.MAX_VALUE);
                    }
                }
            }
        } finally {
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
            reader.close();
        }

        if (!restoredOk) {
            // Falling through to the head-miss path. The reader/walWriter
            // we held are released by the finally above; head-miss opens
            // its own. The head .cp is already gone (restoreFromHead did
            // it) so head-miss's invalidateHeadOnO3 no-ops.
            LOG.info().$("live view O3 head-hit replay falling through to head-miss [view=")
                    .$(viewName).I$();
            o3HeadMissReplay(instance, windowFactory, baseToken, advanceTo);
            return;
        }

        if (appendedRows > 0) {
            applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
        }
        instance.setLastProcessedSeqTxn(advanceTo);
        instance.setAppliedWatermark(advanceTo);
        boolean lvConsumedPersisted = false;
        try {
            engine.advanceLiveViewConsumedSeqTxn(
                    instance.getLiveViewToken(),
                    advanceTo,
                    blockFileWriter,
                    path
            );
            lvConsumedPersisted = true;
        } catch (CairoException e) {
            LOG.critical().$("could not advance live view consumed seqTxn after O3 head-hit replay [view=")
                    .$(viewName)
                    .$(", advanceTo=").$(advanceTo)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            persistState(instance);
        }
        if (lvConsumedPersisted && appendedRows > 0) {
            // Retire the head .cp the replay was built on (its state is
            // strictly older than what we just wrote) then write a fresh
            // post-replay head. The retire-then-write ordering puts
            // maybeWriteHeadCheckpoint on its first-cp cadence path, which
            // unconditionally writes regardless of row/duration cadence.
            if (instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL) {
                invalidateHeadOnO3(instance, advanceTo, Numbers.LONG_NULL, Numbers.LONG_NULL);
            }
            maybeWriteHeadCheckpoint(instance, windowFactory, advanceTo, replayMaxTs, appendedRows);
        }
        LOG.info().$("live view O3 head-hit replay completed [view=")
                .$(viewName)
                .$(", advanceTo=").$(advanceTo)
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
     */
    private void o3HeadMissReplay(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            TableToken baseToken,
            long advanceTo
    ) throws SqlException {
        final String viewName = instance.getDefinition().getViewName();
        // Retire any existing head .cp. The follow-up commit writes a fresh
        // one post-replay; until then the next refresh cycle's first-commit
        // cadence trigger will write it.
        if (instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL) {
            invalidateHeadOnO3(instance, advanceTo, Numbers.LONG_NULL, Numbers.LONG_NULL);
        }

        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
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
        long replayMaxTs = Numbers.LONG_NULL;
        // Minimum output ts the replay actually produced (rows arrive in
        // ts-ascending order, so the first appended row is the minimum). Used
        // as the REPLACE_RANGE low boundary instead of viewLowerBoundTimestamp:
        // when the base has lost rows below this point (base DROP PARTITION /
        // TTL / TRUNCATE), the LV's own derived rows below it are preserved
        // (frozen) rather than deleted by a range that the base can no longer
        // refill. With an intact base the filter is deterministic, so every
        // existing LV row regenerates and replayMinTs sits at or below the
        // oldest LV row - the range still covers everything and behaviour is
        // unchanged.
        long replayMinTs = Numbers.LONG_NULL;
        try {
            engine.detachReader(reader);
            executionContext.of(reader);
            readerAttached = true;

            RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
            final Function filter = filterFactory.getFilter();
            RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
            RecordMetadata baseMetadata = pageFrameFactory.getMetadata();
            final int baseTimestampIndex = baseMetadata.getTimestampIndex();
            RecordMetadata outMetadata = windowFactory.getMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();

            // Probe pass: open a separate cursor over the same source + filter
            // chain and check whether any row survives. Skipping the wipe when
            // no rows pass the filter prevents a degenerate replay (e.g. WHERE
            // discards every row in the replay window) from permanently
            // erasing cumulative accumulator state for every partition.
            final boolean hasReplayRow;
            try (RecordCursor probeCursor = pageFrameFactory.getCursor(executionContext)) {
                tsLowerBoundCursor.of(probeCursor, baseTimestampIndex, viewLowerBoundTimestamp);
                RecordCursor probeSource = tsLowerBoundCursor;
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
                    try (RecordCursor pageCursor = pageFrameFactory.getCursor(executionContext)) {
                        tsLowerBoundCursor.of(pageCursor, baseTimestampIndex, viewLowerBoundTimestamp);
                        RecordCursor source = tsLowerBoundCursor;
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
                        }

                        if (appendedRows > 0) {
                            // Clamp the rewrite to the rows the base actually
                            // produced. With an intact base replayMinTs sits at
                            // or below the oldest LV row, so this still rewrites
                            // the whole view; with base data removed below
                            // replayMinTs it leaves the unrefreshable prefix in
                            // place rather than deleting it. See the replayMinTs
                            // declaration.
                            walWriter.commitLiveViewWithReplaceRange(
                                    effectiveSeqTxn,
                                    replayMinTs,
                                    Long.MAX_VALUE
                            );
                        }
                    }
                }
            }
        } finally {
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
            reader.close();
        }

        if (appendedRows > 0) {
            applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
            // Re-read the on-disk row count: the clamped REPLACE_RANGE may have
            // preserved an unrefreshable prefix below replayMinTs, so the head-
            // miss output is no longer a pure from-scratch rebuild. Sourcing the
            // lifetime counter from the table keeps the head checkpoint's
            // lvRowPosition (written below) consistent in both the intact-base
            // and base-data-removed cases.
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
            // Post-replay head: head metadata was cleared at the top of this
            // function, so maybeWriteHeadCheckpoint's first-cp cadence path
            // fires unconditionally and writes a fresh head reflecting the
            // post-replay state. Restart can then short-circuit to head-hit
            // for a subsequent O3 in the head's hit zone instead of paying
            // for another full head-miss replay.
            //
            // Pass 0 appendedRows: lvRowsTotal already includes them (sourced
            // from the on-disk size above), so adding them again would
            // double-count lvRowPosition. Mirrors the backfill-completion path.
            maybeWriteHeadCheckpoint(instance, windowFactory, effectiveSeqTxn, replayMaxTs, 0L);
        }
        LOG.info().$("live view O3 head-miss replay completed [view=")
                .$(viewName)
                .$(", advanceTo=").$(effectiveSeqTxn)
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
    private static void clearWindowState(WindowRecordCursorFactory windowFactory, LiveViewWindow anchorWindow) {
        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).toTop();
        }
        if (anchorWindow != null) {
            anchorWindow.toTop();
        }
    }

    /**
     * Drives one turn of the BACKFILL sweep for a view in BACKFILLING state.
     * The sweep is resumable and yields on the turn budget so a long history
     * does not starve other views sharing the worker pool. One
     * {@code runBackfillSweep} call is one turn; the fallback scan re-enqueues
     * the view while it stays BACKFILLING.
     * <ul>
     *     <li>The first turn of a process resumes window state + the data-cursor
     *     offset from the surviving {@code .bcp} (restart mid-sweep), or starts
     *     from offset 0 with empty state (fresh CREATE, or no usable {@code
     *     .bcp}). Later turns continue from the in-memory window state + offset
     *     ({@code getIncrementalCursor} preserves accumulated state across
     *     turns), so no per-turn restore is needed.</li>
     *     <li>Each turn re-opens the MVCC base reader at {@code backfillTargetSeqTxn},
     *     {@code skipRows()} past already-swept rows, feeds up to a row/duration
     *     budget, commits the batch, applies it, and writes a {@code .bcp} on
     *     the checkpoint cadence.</li>
     *     <li>On cursor exhaustion the turn flips {@code backfillState} to ACTIVE,
     *     writes a steady head {@code .cp} from the now-complete state, and
     *     retires the {@code .bcp}; the next tick begins the deferred drain from
     *     {@code sweepSeqTxn + 1}.</li>
     * </ul>
     * Crash idempotency: the on-disk output is a deterministic prefix of the
     * eventual result, so a re-feed past the last {@code .bcp} recomputes rows
     * already on disk to advance state but skips their WAL append
     * ({@code skipWriteUntil}). A crash before any {@code .bcp} re-sweeps from
     * offset 0 and skip-writes the entire stale prefix.
     */
    private void runBackfillSweep(LiveViewInstance instance) throws SqlException {
        final long backfillTargetSeqTxn = instance.getStateReader().getBackfillTargetSeqTxn();
        final String viewName = instance.getDefinition().getViewName();
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        final WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();

        // Empty base or pre-CREATE base with no committed seqTxn: nothing to
        // sweep. Skip straight to the ACTIVE flip so incremental drain takes
        // over on the next refresh tick.
        if (backfillTargetSeqTxn < 0) {
            instance.setBackfillState(LiveViewState.BACKFILL_STATE_ACTIVE);
            instance.setBackfillTargetSeqTxn(Numbers.LONG_NULL);
            persistState(instance);
            LOG.info().$("live view backfill sweep skipped on empty base [view=")
                    .$(viewName).I$();
            return;
        }

        // Resume setup, once per process on the first backfill turn: establish
        // the in-memory data offset, window state, latestSeenTs, and the
        // persistent skip-write floor. Later turns inherit all of these in
        // memory (the per-turn budget can split the skip-write catch-up across
        // turns, so the floor must persist - it is an instance field).
        if (!instance.isBackfillResumeAttempted()) {
            instance.setBackfillResumeAttempted();
            long onDiskLvRows = 0;
            try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
                onDiskLvRows = lvReader.size();
            } catch (CairoException e) {
                // No readable LV table yet (fresh CREATE before first apply).
                onDiskLvRows = 0;
            }
            // Always start from a clean slate; restore (if any) repopulates on top.
            clearWindowState(windowFactory, anchorWindow);
            final long bcpKey = instance.getHeadBackfillCpKey();
            boolean restored = false;
            if (bcpKey != Numbers.LONG_NULL
                    && restoreFromHead(instance, windowFactory, bcpKey, true, restoredHeadState)
                    && restoredHeadState.resumeDataOffset != Numbers.LONG_NULL) {
                instance.setBackfillDataOffset(restoredHeadState.resumeDataOffset);
                instance.setLvRowsTotal(restoredHeadState.lvRowsTotal);
                if (restoredHeadState.maxTimestamp != Numbers.LONG_NULL) {
                    instance.setLatestSeenTs(restoredHeadState.maxTimestamp);
                }
                restored = true;
            }
            if (!restored) {
                // Fresh CREATE, no .bcp, or corrupt .bcp: re-sweep from offset 0
                // with empty state. The on-disk prefix (if any) is a
                // deterministic match, kept via skip-write below.
                instance.setBackfillDataOffset(0);
                instance.setLvRowsTotal(0);
                instance.setHeadBackfillCpKey(Numbers.LONG_NULL);
            }
            // On-disk output is append-only (>= the restored row count), so the
            // skip-write floor is simply the on-disk row count: rows re-fed
            // below it are recomputed to advance state but not re-appended.
            instance.setBackfillSkipWriteFloor(onDiskLvRows);
        }

        final long skipWriteUntil = instance.getBackfillSkipWriteFloor();
        long dataOffset = instance.getBackfillDataOffset();

        TableReader reader = waitForApply(baseToken, backfillTargetSeqTxn);
        // The reader may sit at a seqTxn strictly greater than the target if
        // ApplyWal2TableJob caught up further while waitForApply was running;
        // sweepSeqTxn pins the deferred drain to resume from after the snapshot.
        final long sweepSeqTxn = Math.max(backfillTargetSeqTxn, reader.getSeqTxn());

        final long turnMaxRows = engine.getConfiguration().getLiveViewCheckpointRows();
        final long turnMaxDurationUs = engine.getConfiguration().getLiveViewRefreshTurnMaxDurationMicros();

        long batchMaxTs = Numbers.LONG_NULL;
        long lvRows = instance.getLvRowsTotal();
        long appendedThisTurn = 0;
        long processedThisTurn = 0;
        boolean yielded = false;
        boolean readerAttached = false;
        try {
            engine.detachReader(reader);
            executionContext.of(reader);
            readerAttached = true;

            RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
            final Function filter = filterFactory.getFilter();
            RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
            RecordMetadata outMetadata = windowFactory.getMetadata();
            final int cursorTimestampIndex = outMetadata.getTimestampIndex();
            if (cursorTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(viewName).put(']');
            }

            try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
                RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
                try (RecordCursor pageCursor = pageFrameFactory.getCursor(executionContext)) {
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
                            backfillSkipCounter.set(dataOffset);
                            pageCursor.skipRows(backfillSkipCounter, RecordCursor.UNBOUNDED_ROW_COUNT);
                        }
                        Record outRecord = windowCursor.getRecord();
                        while (windowCursor.hasNext()) {
                            long ts = outRecord.getTimestamp(cursorTimestampIndex);
                            if (batchMaxTs == Numbers.LONG_NULL || ts > batchMaxTs) {
                                batchMaxTs = ts;
                            }
                            instance.setLatestSeenTs(ts);
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
                        walWriter.commitLiveView(sweepSeqTxn);
                    }
                }
            }
        } finally {
            if (readerAttached) {
                executionContext.clearReader();
                engine.attachReader(reader);
            }
            reader.close();
        }

        instance.setLvRowsTotal(lvRows);
        instance.setBackfillDataOffset(dataOffset);
        if (appendedThisTurn > 0) {
            applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
        }

        if (yielded) {
            // More to sweep: persist a resumable checkpoint on cadence, then
            // yield. The fallback scan re-enqueues (needsBackfill stays true).
            maybeWriteBackfillCheckpoint(instance, windowFactory, dataOffset, batchMaxTs, sweepSeqTxn);
            return;
        }

        // Sweep complete. Materialise the steady head .cp from the now-complete
        // window state (maxTs = overall latestSeenTs, not this possibly-empty
        // final turn's batchMaxTs) so the ACTIVE phase's restart-restore + O3
        // head-hit have an anchor. lvRowsTotal is already maintained above, so
        // pass 0 appendedRows to avoid double-counting it.
        instance.setLastProcessedSeqTxn(sweepSeqTxn);
        instance.setAppliedWatermark(sweepSeqTxn);
        maybeWriteHeadCheckpoint(instance, windowFactory, sweepSeqTxn, instance.getLatestSeenTs(), 0L);
        instance.setBackfillState(LiveViewState.BACKFILL_STATE_ACTIVE);
        instance.setBackfillTargetSeqTxn(Numbers.LONG_NULL);
        try {
            // Persists backfillState=ACTIVE + watermarks durably before the .bcp
            // is retired, so a crash between the two recovers as ACTIVE.
            engine.advanceLiveViewConsumedSeqTxn(
                    instance.getLiveViewToken(),
                    sweepSeqTxn,
                    blockFileWriter,
                    path
            );
        } catch (CairoException e) {
            LOG.critical().$("could not advance live view consumed seqTxn after backfill sweep [view=")
                    .$(viewName)
                    .$(", sweepSeqTxn=").$(sweepSeqTxn)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            persistState(instance);
        }
        unlinkBackfillCheckpoint(instance);
        LOG.info().$("live view backfill sweep completed [view=")
                .$(viewName)
                .$(", backfillTargetSeqTxn=").$(backfillTargetSeqTxn)
                .$(", sweepSeqTxn=").$(sweepSeqTxn)
                .$(", lvRowsTotal=").$(instance.getLvRowsTotal()).I$();
    }

    /**
     * Retires the rolling backfill checkpoint {@code <key>.bcp} (best-effort)
     * and clears {@code headBackfillCpKey}. Called after the BACKFILLING ->
     * ACTIVE flip is durable. Leftovers from a crash in the tiny window before
     * this runs are swept at the next startup by {@code sweepBackfillCheckpoints}
     * (the view is no longer BACKFILLING then).
     */
    private void unlinkBackfillCheckpoint(LiveViewInstance instance) {
        final long bcpKey = instance.getHeadBackfillCpKey();
        if (bcpKey == Numbers.LONG_NULL) {
            return;
        }
        path.of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .slash();
        LiveViewCheckpointWriter.appendBcpFileName(path, bcpKey);
        engine.getConfiguration().getFilesFacade().removeQuiet(path.$());
        instance.setHeadBackfillCpKey(Numbers.LONG_NULL);
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
     * Backfill-checkpoint write hook. Writes a {@code <dataOffset>.bcp}
     * capturing the sweep's resume position (a BACKFILL_CURSOR block holding
     * the data-cursor row offset + lvRowsTotal) plus the same WINDOW_ANCHOR /
     * FUNCTION_SNAPSHOT state blocks the steady head writes, then unlinks the
     * prior {@code .bcp} and stamps {@code headBackfillCpKey} on the instance.
     * <p>
     * Cadence-gated by the same {@code cairo.live.view.checkpoint.rows} /
     * {@code .max.duration} triggers as the steady head, plus a
     * first-checkpoint trigger so a restart early in the sweep resumes rather
     * than re-sweeping. The intervening per-turn yields rely on in-memory
     * window state; the {@code .bcp} only has to be recent enough that a
     * restart's skip-write re-feed (bounded by the cadence) is cheap.
     * <p>
     * No-op when the LV is not snapshot-capable: such a view cannot persist
     * window state, so a crash mid-sweep re-sweeps from the beginning (the
     * wipe path in {@link #runBackfillSweep}).
     * <p>
     * A failure here does not invalidate the view ({@code .bcp} is derived
     * state). The prior {@code .bcp}, if any, stays addressable; we log
     * critical, drop the half-open writer, and continue.
     */
    private void maybeWriteBackfillCheckpoint(
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

        // Cadence keys off the data-offset delta since the prior .bcp (its key
        // is the data offset at that write). firstBcp forces a write so a crash
        // early in the sweep resumes rather than re-sweeping from scratch.
        final long rowsCadence = engine.getConfiguration().getLiveViewCheckpointRows();
        final long durationCadence = engine.getConfiguration().getLiveViewCheckpointMaxDurationMicros();
        final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        final long lastWrittenUs = instance.getLastCheckpointWrittenUs();
        final long priorKey = instance.getHeadBackfillCpKey();
        final boolean firstBcp = priorKey == Numbers.LONG_NULL;
        final boolean rowTrigger = !firstBcp && (dataOffset - priorKey) >= rowsCadence;
        final boolean durationTrigger = !firstBcp
                && lastWrittenUs != Numbers.LONG_NULL
                && (nowUs - lastWrittenUs) >= durationCadence;
        if (!(firstBcp || rowTrigger || durationTrigger)) {
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
            checkpointManifest.setKind(LiveViewCheckpointManifest.KIND_BACKFILL);
            final LiveViewWindow anchorWindow = instance.getAnchorWindow();
            if (anchorWindow != null) {
                checkpointManifest.addWindowName(anchorWindow.getWindowName());
            }
            checkpointWriter.writeManifestBlock(checkpointManifest);

            final MemoryA cursorSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_BACKFILL_CURSOR);
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
            for (int i = 0, n = functions.size(); i < n; i++) {
                final WindowFunction f = functions.getQuick(i);
                if (!f.supportsSnapshot()) {
                    continue;
                }
                final MemoryA fnSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                fnSink.putStr(windowName);
                fnSink.putStr(snapshotFactoryName(f));
                fnSink.putInt(f.snapshotFormatVersion());
                LiveViewFunctionSnapshot.write(fnSink, f);
                checkpointWriter.endBlock();
            }

            checkpointWriter.commit(firstBcp ? Numbers.LONG_NULL : priorKey);
            instance.recordBackfillCheckpointWritten(dataOffset, nowUs);
        } catch (Throwable t) {
            LOG.critical().$("could not write live view backfill checkpoint [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", dataOffset=").$(dataOffset)
                    .$(", error=").$(t).I$();
            checkpointWriter = Misc.free(checkpointWriter);
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
     * {@code supportsSnapshot()} plus, when the LV has an anchored window,
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
            long appendedRows
    ) {
        if (!instance.isSnapshotCapabilityComputed()) {
            instance.setSnapshotCapability(computeSnapshotCapability(instance, windowFactory));
        }
        if (!instance.isSnapshotCapability()) {
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
        if (!(firstCp || rowTrigger || durationTrigger)) {
            return;
        }

        try {
            if (checkpointWriter == null) {
                checkpointWriter = new LiveViewCheckpointWriter(engine.getConfiguration());
            }
            path.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken());
            checkpointWriter.of(path.$(), lvSeqTxn);

            checkpointManifest.clear();
            checkpointManifest.setLvSeqTxn(lvSeqTxn);
            checkpointManifest.setBaseSeqTxn(instance.getLastProcessedSeqTxn());
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
            final String windowName = anchorWindow != null ? anchorWindow.getWindowName() : "";
            for (int i = 0, n = functions.size(); i < n; i++) {
                final WindowFunction f = functions.getQuick(i);
                if (!f.supportsSnapshot()) {
                    continue;
                }
                final MemoryA fnSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                fnSink.putStr(windowName);
                fnSink.putStr(snapshotFactoryName(f));
                fnSink.putInt(f.snapshotFormatVersion());
                // The snapshot payload is implicitly length-bounded by the
                // enclosing FUNCTION_SNAPSHOT block. A future change that
                // packs multiple payloads into a single block would need an
                // explicit LONG length prefix here.
                LiveViewFunctionSnapshot.write(fnSink, f);
                checkpointWriter.endBlock();
            }

            // Capture before commit(): commit() truncates the mmap and resets
            // the writer for reuse.
            final long stateBytes = checkpointWriter.getAppendOffset();
            checkpointWriter.commit(firstCp ? Numbers.LONG_NULL : priorLvSeqTxn);

            instance.setHeadCheckpoint(lvSeqTxn, batchMaxTs, stateBytes, nowUs);
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
        RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
        final Function filter = filterFactory.getFilter();
        RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
        final TableToken baseToken = instance.getDefinition().getBaseTableToken();
        final RecordMetadata baseMetadata = pageFrameFactory.getMetadata();
        final int baseTimestampIndex = baseMetadata.getTimestampIndex();
        buildColumnMappings(baseMetadata, baseToken);
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
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
                    cursorTimestampIndex, viewLowerBoundTimestamp, filter, from, toSeqTxn,
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
     * Callers (restart restore and the 2a.8 O3 head-hit replay) decide what to
     * do with the restored watermarks and whether to refresh the head metadata
     * trio on the instance; this helper restricts itself to state restore +
     * failure cleanup so both call sites share the same disk read path.
     * <p>
     * Failure handling: any structural error (CRC fail, magic mismatch, missing
     * function class, anchor type mismatch) is best-effort cleaned up here -
     * the helper logs critical, unlinks the corrupt {@code .cp}, clears the
     * head metadata on the instance, and returns {@code false}. The LV is not
     * invalidated; the caller falls through to the head-miss replay path.
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
     * Opens a {@code .cp} (steady, {@code isBackfill=false}) or {@code .bcp}
     * (backfill, {@code isBackfill=true}) checkpoint and rehydrates window
     * state. The backfill variant additionally surfaces the BACKFILL_CURSOR's
     * data offset in {@code out.resumeDataOffset}.
     */
    private boolean restoreFromHead(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long headLvSeqTxn,
            boolean isBackfill,
            RestoredHeadState out
    ) {
        out.reset();
        path.of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .slash();
        if (isBackfill) {
            LiveViewCheckpointWriter.appendBcpFileName(path, headLvSeqTxn);
        } else {
            LiveViewCheckpointWriter.appendCpFileName(path, headLvSeqTxn);
        }

        if (checkpointReader == null) {
            checkpointReader = new LiveViewCheckpointReader(engine.getConfiguration());
        }
        if (checkpointRestoreScratch == null) {
            checkpointRestoreScratch = Vm.getCARWInstance(
                    64 * 1024L,
                    Integer.MAX_VALUE,
                    MemoryTag.NATIVE_DEFAULT
            );
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
            while (cursor.hasNext()) {
                final LiveViewCheckpointReader.ReadableBlock block = cursor.next();
                switch (block.type()) {
                    case LiveViewCheckpointBlockType.BLOCK_BACKFILL_CURSOR:
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
                        copyBlockToScratch(block, 0L, block.size());
                        anchorWindow.restore(checkpointRestoreScratch);
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
            return true;
        } catch (CairoException ce) {
            final int errno = ce.getErrno();
            if (errno == CairoException.LV_FUNCTION_SNAPSHOT_VERSION_TOO_OLD
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

    private boolean handleCorruptHeadCheckpoint(
            LiveViewInstance instance,
            long headLvSeqTxn,
            Path path,
            Throwable t
    ) {
        LOG.critical().$("could not restore live view from head checkpoint [view=")
                .$(instance.getDefinition().getViewName())
                .$(", lvSeqTxn=").$(headLvSeqTxn)
                .$(", error=").$(t).I$();
        // Best-effort: unlink the corrupt .cp and clear head metadata.
        // The next refresh cycle falls through to the head-miss replay
        // path (which restarts from viewLowerBoundTimestamp).
        try {
            engine.getConfiguration().getFilesFacade().removeQuiet(path.$());
        } catch (Throwable rmErr) {
            LOG.error().$("could not unlink corrupt head checkpoint [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(rmErr).I$();
        }
        instance.setHeadCheckpoint(Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
        return false;
    }

    /**
     * Restart-restore: opens the head {@code .cp} (stamped on the
     * instance by the startup sweep), rehydrates the LV's window state from
     * the manifest + anchor block + per-function blocks, replays the base WAL
     * forward to close the checkpoint-cadence gap between the head and the
     * applied point, then resumes the refresh worker at the applied point so
     * the next incremental refresh rebuilds only the un-flushed lead.
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
     * invalidated - {@code .cp} is derived state, and the upcoming refresh cycle
     * falls through to the head-miss replay path. A replay-to-applied error,
     * however, can leave the restored accumulators inconsistent with disk, so it
     * invalidates the view (operator recovers with DROP + CREATE) via the
     * pending-invalidation hook rather than serving wrong results.
     */
    private void tryRestoreFromHead(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        final long headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
        // The persisted applied watermark (base seqTxn) is disk truth: the LV's
        // on-disk table holds every base commit up to it. Snapshot it before the
        // restore below overwrites the in-memory watermarks with the head's
        // (potentially older) base seqTxn.
        final long diskAppliedSeqTxn = instance.getAppliedWatermark();
        if (!restoreFromHead(instance, windowFactory, headLvSeqTxn, restoredHeadState)) {
            // restoreFromHead has already unlinked the corrupt .cp and
            // cleared head metadata; nothing more to do here.
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
        // writtenUs stays LONG_NULL so the next flush's cadence check treats this
        // as "first commit" and writes a fresh head soon after.
        instance.setHeadCheckpoint(
                headLvSeqTxn,
                restoredHeadState.maxTimestamp,
                restoredHeadState.stateBytes,
                Numbers.LONG_NULL
        );
        long resumeSeqTxn = manifestBaseSeqTxn;
        long replayedRows = 0;
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
        instance.setCheckpointRestoreSucceeded();
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
     * Then bulk-copies the trailing payload bytes into the per-worker scratch
     * buffer (so {@link LiveViewFunctionSnapshot#restore} reads from offset 0)
     * and pairs the block with a running window function positionally: the
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
        // !supportsSnapshot() skip so the positional pairing stays aligned.
        WindowFunction match = null;
        while (restoreFunctionCursor < functions.size()) {
            final WindowFunction candidate = functions.getQuick(restoreFunctionCursor++);
            if (candidate.supportsSnapshot()) {
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
        if (formatVersion < match.snapshotMinSupportedVersion()) {
            // Below-min versions signal a real compatibility break (operator
            // DROP+CREATE is the recovery), not structural corruption. Tag
            // the throw so the catch site invalidates the LV rather than
            // unlinking and replaying from head-miss. Above-current versions
            // are tolerated: future writers may emit blocks readers do not
            // understand yet.
            throw CairoException.critical(CairoException.LV_FUNCTION_SNAPSHOT_VERSION_TOO_OLD)
                    .put("live view function snapshot version too old, factory=")
                    .put(storedFactoryName)
                    .put(", read=")
                    .put(formatVersion)
                    .put(", minSupported=")
                    .put(match.snapshotMinSupportedVersion());
        }

        final long payloadStart = offset;
        final long payloadLength = block.size() - payloadStart;
        copyBlockToScratch(block, payloadStart, payloadLength);
        LiveViewFunctionSnapshot.restore(checkpointRestoreScratch, 0L, match, formatVersion);
    }

    private void copyBlockToScratch(LiveViewCheckpointReader.ReadableBlock block, long offsetInBlock, long length) {
        checkpointRestoreScratch.jumpTo(0);
        if (length == 0) {
            return;
        }
        checkpointRestoreScratch.putBlockOfBytes(block.addressOf(offsetInBlock), length);
        checkpointRestoreScratch.jumpTo(0);
    }

    private static long strByteSize(LiveViewCheckpointReader.ReadableBlock block, long offset) {
        // STR encoding: INT length prefix + length * CHAR (2 bytes each).
        final int len = block.getInt(offset);
        return Integer.BYTES + (long) len * Character.BYTES;
    }

    /**
     * Computes the AND of (a) anchor-map key codec support and (b) every
     * compiled window function's {@code supportsSnapshot()}. Called once
     * per LV lifetime on the first refresh after the compiled factory is
     * available; subsequent calls short-circuit on the cached flag.
     */
    private static boolean computeSnapshotCapability(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        if (anchorWindow != null && !LiveViewSnapshotKeyCodec.isAllTypesSupported(anchorWindow.getPartitionKeyTypes())) {
            return false;
        }
        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).supportsSnapshot()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Prepares the worker-local staging buffer and the LV's own in-memory tier
     * for the upcoming cycle. Returns {@code true} when both are usable for
     * this LV (all output column types are fixed-width-supported); {@code false}
     * when any column type is var-length or otherwise unsupported, in which case
     * the cycle still writes to the on-disk tier but the in-mem tier stays
     * empty / unallocated and reads fall back to {@code TableReader}.
     * <p>
     * The reusable {@code tierColumnTypes} member captures the output schema for
     * this cycle, doubling as the staging buffer's shape and the LV tier's
     * shape on first allocation. No per-cycle {@code IntList} is allocated.
     */
    /**
     * Computes (once, then caches) whether the LV's in-mem tier may serve an
     * un-flushed lead ahead of disk. Eligible when the output schema has a
     * designated timestamp and is fully fixed-width (so the tier can store it).
     * SYMBOL columns are eligible: the lead drain eager-interns them into the
     * tier's symbol cache (LV-table-consistent ids), so the read path resolves the
     * lead's symbols from RAM. Ineligible LVs (var-length output, or no designated
     * timestamp) keep the tier a strict subset of disk: the refresh worker applies
     * every cycle for them. Compiles the SELECT on the first call if needed (cached
     * for the LV's lifetime).
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
            // LV output schema contains a var-length / unsupported column type;
            // skip the in-mem tier population for this LV. The cursor reads
            // disk-only via TableReader.
            return false;
        }
        long pageSize = engine.getConfiguration().getLiveViewInMemoryBufferInitialBytes();
        if (stagingBuffer == null || !stagingColumnTypes.equals(tierColumnTypes)) {
            // Shape changed (or first use on this worker) — reshape the
            // staging buffer. The previous buffer's allocations are released
            // by Misc.free before the new one is constructed.
            stagingBuffer = Misc.free(stagingBuffer);
            stagingColumnTypes.clear();
            for (int i = 0, n = tierColumnTypes.size(); i < n; i++) {
                stagingColumnTypes.add(tierColumnTypes.getQuick(i));
            }
            stagingBuffer = new LiveViewInMemoryBuffer(stagingColumnTypes, tsColIdx, pageSize);
            stagingTimestampColumnIndex = tsColIdx;
        }
        stagingBuffer.reset();
        // Allocate the per-LV in-mem tier on first use; subsequent cycles reuse
        // it. The tier's shape is fixed at allocation — if a downstream commit
        // changes the LV's _meta (reserved for ALTER LIVE VIEW later) the tier
        // would need to be reshaped too. Today _meta is immutable post-CREATE.
        if (instance.getInMemoryTier() == null) {
            instance.setInMemoryTier(new LiveViewInMemoryTier(tierColumnTypes, tsColIdx, pageSize));
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

        // Fast-path: append in place when no reader pins the published slot
        // and the slot's footprint is still under the growth budget. Growth
        // backstop ensures the in-mem tier cannot accumulate indefinitely
        // even when readers never pin (e.g. an idle LV with steady
        // ingestion); the IN MEMORY eviction runs on the slow-path edge
        // that fires when the threshold trips.
        long growthBudget = engine.getConfiguration().getLiveViewInMemoryBufferGrowthBytes();
        if (pubSlot.footprintBytes() < growthBudget) {
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
                    appendStagingInPlace(acquired, stagingBuffer.seamTs());
                    acquired.setLvSeqTxn(lvSeqTxn);
                    acquired.setLeadRowCount(newLeadRowCount);
                } catch (Throwable t) {
                    // Fast-path append cannot leave the slot partially
                    // populated visibly to readers: rowCount only advances
                    // once at the end of appendStagingInPlace, after all
                    // column writes have completed, and the writer sentinel
                    // (rc = -1) keeps readers spinning until release. Drop
                    // the sentinel and let the flush-retry budget tick.
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

                // Per-row eviction. A slot's trailing leadRowCount rows are the
                // un-flushed lead (no durable disk copy) and must never age out;
                // the leading overlap rows are on disk, so they age out once they
                // fall below latest - IN MEMORY. The lead suffix bounds the tier at
                // the IN MEMORY window plus the un-flushed lead, and forces a flush
                // before the lead could span the whole window. In subset mode
                // leadRowCount is 0, so every row is overlap and ages normally.
                long leadCount = pubSlot.leadRowCount();
                long overlapCount = pubSlot.rowCount() - leadCount;
                for (long r = 0, rn = pubSlot.rowCount(); r < rn; r++) {
                    long srcTs = pubSlot.getLong(r, tsCol);
                    boolean isLead = r >= overlapCount;
                    if (!isLead && srcTs < retainThreshold) {
                        continue;
                    }
                    if (writeSeamTs == Numbers.LONG_NULL) {
                        writeSeamTs = srcTs;
                    }
                    writeSlot.copyRowFrom(pubSlot, r, writeRow);
                    writeRow++;
                }
            }
            // Append staging rows on top.
            for (long r = 0, rn = stagingBuffer.rowCount(); r < rn; r++) {
                long srcTs = stagingBuffer.getLong(r, tsCol);
                if (writeSeamTs == Numbers.LONG_NULL) {
                    writeSeamTs = srcTs;
                }
                writeSlot.copyRowFrom(stagingBuffer, r, writeRow);
                writeRow++;
            }
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
     * Fast-path append helper: copies staging rows onto the tail of
     * {@code slot}, bumping {@code rowCount} once at the end. Runs under the
     * writer sentinel ({@code rc = -1}) so no reader observes intermediate
     * state.
     * <p>
     * {@code seamTs} bookkeeping: when the slot is empty at fast-path entry,
     * the first appended row's ts becomes the slot's minimum; the helper
     * initialises {@code seamTs} accordingly. When the slot already holds
     * rows, the existing {@code seamTs} is already the slot's minimum and
     * staging rows are strictly newer (ts-ascending append), so no update is
     * needed. The fast-path defers {@code IN MEMORY} eviction to the next
     * slow-path edge — {@code seamTs} therefore never grows on the
     * fast-path.
     */
    private void appendStagingInPlace(LiveViewInMemoryBuffer slot, long stagingMinTs) {
        long writeRow = slot.rowCount();
        if (writeRow == 0 && stagingBuffer.rowCount() > 0) {
            slot.setSeamTs(stagingMinTs);
        }
        for (long r = 0, rn = stagingBuffer.rowCount(); r < rn; r++) {
            slot.copyRowFrom(stagingBuffer, r, writeRow);
            writeRow++;
        }
        slot.setRowCount(writeRow);
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
            // Unsupported (var-length) output schema never allocates the tier.
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

        long dstRow = 0;
        long seamTs = Numbers.LONG_NULL;
        for (int p = 0; p < pc; p++) {
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
            for (long r = firstRowAtOrAbove(tsCol, size, retainThreshold); r < size; r++) {
                if (seamTs == Numbers.LONG_NULL) {
                    seamTs = tsCol.getLong(r << 3);
                }
                copyReaderRowToStaging(lvReader, columnBase, r, dstRow);
                dstRow++;
            }
        }
        stagingBuffer.setRowCount(dstRow);
        stagingBuffer.setSeamTs(seamTs);
    }

    /**
     * Copies one LV-table row's fixed-width columns into {@code stagingBuffer}
     * at {@code dstRow}, reading directly from the reader's mapped column memory.
     * Dispatches by the staging buffer's column type (which equals the LV output
     * schema, 1:1 with the LV table column order). Only the in-mem tier's
     * supported fixed-width types reach here - {@code stagingBuffer} is allocated
     * only for such schemas.
     */
    private void copyReaderRowToStaging(TableReader reader, int columnBase, long partitionRow, long dstRow) {
        for (int c = 0, n = stagingColumnTypes.size(); c < n; c++) {
            final int type = ColumnType.tagOf(stagingColumnTypes.getQuick(c));
            final MemoryCR col = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, c));
            switch (type) {
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                case ColumnType.GEOLONG:
                    stagingBuffer.putLong(dstRow, c, col.getLong(partitionRow << 3));
                    break;
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                case ColumnType.GEOINT:
                case ColumnType.IPv4:
                    stagingBuffer.putInt(dstRow, c, col.getInt(partitionRow << 2));
                    break;
                case ColumnType.DOUBLE:
                    stagingBuffer.putDouble(dstRow, c, col.getDouble(partitionRow << 3));
                    break;
                case ColumnType.FLOAT:
                    stagingBuffer.putFloat(dstRow, c, col.getFloat(partitionRow << 2));
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    stagingBuffer.putShort(dstRow, c, col.getShort(partitionRow << 1));
                    break;
                case ColumnType.CHAR:
                    stagingBuffer.putShort(dstRow, c, (short) col.getChar(partitionRow << 1));
                    break;
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    stagingBuffer.putByte(dstRow, c, col.getByte(partitionRow));
                    break;
                case ColumnType.BOOLEAN:
                    stagingBuffer.putBool(dstRow, c, col.getBool(partitionRow));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "live view in-memory tier does not support column type: "
                                    + ColumnType.nameOf(stagingColumnTypes.getQuick(c)));
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
     * Replaces {@code slot}'s contents with the current {@code stagingBuffer}
     * rows and stamps the slot. The slot is reset first (full replace, not an
     * append) - the rebuild slot reflects the disk tail exactly, with no carry
     * over of pre-O3 rows. Runs under the writer sentinel, so no reader observes
     * the intermediate state.
     */
    private void fillSlotFromStaging(LiveViewInMemoryBuffer slot, long lvSeqTxn) {
        slot.reset();
        final long rows = stagingBuffer.rowCount();
        for (long r = 0; r < rows; r++) {
            slot.copyRowFrom(stagingBuffer, r, r);
        }
        slot.setRowCount(rows);
        slot.setSeamTs(stagingBuffer.seamTs());
        slot.setLvSeqTxn(lvSeqTxn);
        // Rebuilt straight from the rewritten disk, so every row is on disk: the
        // slot is a pure subset of disk with no un-flushed lead.
        slot.setLeadRowCount(0);
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
        // Quiesced store (read-only replica before a promote): skip the whole pass, including the
        // registry fallback scan, so refresh workers never touch a replica's live views. A promote
        // swaps in a real store (see ForwardingLiveViewStateStore) and this gate reopens.
        if (!stateStore.isRefreshEnabled()) {
            return false;
        }
        boolean didWork = false;
        while (stateStore.tryDequeueRefreshTask(refreshTask)) {
            refreshViewsForBaseTable(refreshTask.baseTableToken, refreshTask.seqTxn);
            stateStore.notifyBaseRefreshed(refreshTask, refreshTask.seqTxn);
            didWork = true;
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
        registry.getViews(viewInstanceSink);
        boolean didWork = false;
        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            TableToken baseToken = instance.getDefinition().getBaseTableToken();
            if (baseToken == null) {
                continue;
            }
            long head = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
            // The 2a.7 restart-restore runs inside refreshInstance on the
            // first cycle for any LV with a stamped head .cp - even when
            // there are no new base commits to consume. Letting the worker
            // skip such LVs would defer restore to the next inbound commit,
            // which for a quiescent base could be hours away.
            final boolean needsRestore = !instance.isCheckpointRestoreAttempted()
                    && instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL;
            // BACKFILL views need a refresh tick to drive the sweep even when
            // no new base commits have arrived since CREATE - the sweep
            // covers existing history, not future commits.
            final boolean needsBackfill = instance.getStateReader().getBackfillState()
                    == LiveViewState.BACKFILL_STATE_BACKFILLING;
            if (head > instance.getLastProcessedSeqTxn() || needsRestore || needsBackfill) {
                refreshInstance(instance, head);
                didWork = true;
            }
        }
        return didWork;
    }

    private void refreshInstance(LiveViewInstance instance, long seqTxn) {
        if (!instance.tryLockForRefresh()) {
            return;
        }
        String invalidationReason = null;
        // Bound each refresh turn (one refreshInstance call) by max commits
        // and max duration so a long backlog does not monopolise the worker.
        // The yield itself lives at the per-base-seqTxn boundary inside
        // incrementalRefresh; the budget snapshot resets per turn.
        turnStartUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        turnCommitsProcessed = 0;
        try {
            if (instance.isDropped() || instance.isInvalid()) {
                return;
            }
            // Snapshot freeze: DatabaseCheckpointAgent is mid-copy of this LV's
            // files. Skip this turn so _lv.s and the on-disk tier do not
            // advance while the agent is reading them. The agent clears the
            // flag via endCheckpoint() once the per-LV copy completes; the
            // next fallback or notification tick picks the worker back up.
            if (instance.isFreezeInProgress()) {
                return;
            }
            boolean attempted = false;
            try {
                // First cycle after restart restores from the head
                // .cp (if any). Single-shot per LV lifetime - the flag flips
                // true whether the restore succeeded, missed, or failed.
                if (!instance.isCheckpointRestoreAttempted()) {
                    instance.setCheckpointRestoreAttempted();
                    if (instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL) {
                        tryRestoreFromHead(instance, getWindowFactory(instance));
                    }
                }
                // BACKFILL phase: a view created with the BACKFILL clause sits
                // in BACKFILLING state until the sweep covers everything <=
                // backfillTargetSeqTxn. The sweep takes priority over
                // incremental drain; once it completes, the next refresh tick
                // resumes normal incremental processing from
                // backfillTargetSeqTxn + 1.
                //
                // The sweep does not bump lastFlushTimeUs - the FLUSH EVERY
                // rate limit governs steady-state publish cadence, and a
                // BACKFILL view should resume incremental drain immediately
                // after the sweep without an artificial 100ms+ stall.
                if (instance.getStateReader().getBackfillState() == LiveViewState.BACKFILL_STATE_BACKFILLING) {
                    attempted = true;
                    runBackfillSweep(instance);
                    instance.setLastRefreshTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                    instance.recordRefreshSuccess();
                    return;
                }
                // Decide the cadence. A lead-eligible LV decouples refresh (drain
                // into the in-mem tier as the un-flushed lead, every tick with new
                // base commits) from flush (commit + apply + checkpoint, on the
                // FLUSH EVERY cadence). An ineligible LV (SYMBOL or var-length
                // output) keeps the coupled cycle, gated by FLUSH EVERY, applying
                // every cycle so the tier stays a subset of disk.
                final boolean leadEligible = ensureLeadEligible(instance);
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
                        incrementalRefresh(instance, refreshFrom, seqTxn, true);
                    }
                    // Flush the accumulated lead on the FLUSH EVERY cadence. The
                    // refresh above may have already flushed (emergency, on a tier
                    // stall), in which case refreshedUpTo == lastProcessed and this
                    // is skipped.
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
                            return;
                        }
                        // TransactionLogCursor treats txnLo as exclusive (lastApplied), so we
                        // pass lastSeqTxn directly. The cursor's getTxn() returns entries with
                        // seqTxn > lastSeqTxn.
                        attempted = true;
                        incrementalRefresh(instance, lastSeqTxn, seqTxn, false);
                        instance.setLastFlushTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                    }
                }
                instance.setLastRefreshTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
                if (attempted) {
                    instance.recordRefreshSuccess();
                }
            } catch (Throwable t) {
                invalidationReason = handleRefreshFailure(instance, t);
            }
        } finally {
            instance.unlockAfterRefresh();
            instance.tryCloseIfDropped();
            // If this view was invalidated concurrently while this cycle held
            // the refresh latch (so the invalidator's own free lost the CAS),
            // free its runtime state now that the latch is released.
            instance.tryFreeRuntimeStateIfInvalid();
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
    }

    /**
     * Flush retry budget: count consecutive failures and the elapsed
     * wall-clock time since the streak began. On budget exhaustion, returns
     * the reason string so the caller can drive the invalidation outside the
     * refresh latch; otherwise returns null. The view stops refreshing but
     * stays queryable; recovery is operator-driven (DROP + CREATE).
     */
    private String handleRefreshFailure(LiveViewInstance instance, Throwable t) {
        long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        instance.recordRefreshFailure(nowUs);
        int retryCount = instance.getFlushRetryCount();
        long retryStartUs = instance.getFlushRetryStartUs();
        int maxRetry = engine.getConfiguration().getLiveViewFlushRetryMax();
        long maxDurationMicros = engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros();
        long elapsedUs = retryStartUs == Numbers.LONG_NULL ? 0 : nowUs - retryStartUs;
        boolean budgetExhausted = retryCount >= maxRetry || elapsedUs >= maxDurationMicros;
        if (budgetExhausted) {
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
    private static final class DrainResult {
        // Highest base seqTxn processed this pass (-1 if none).
        long advanceTo;
        // Output rows emitted this pass (mirrored to the staging buffer when the
        // tier is populated; written to the LV WAL when a walWriter was supplied).
        long appendedRows;
        // Max output timestamp across the pass, in base-table units (LONG_NULL if none).
        long batchMaxTs;
        // The offending late-row timestamp when o3Detected.
        long o3LateRowTs;
        // True when a base commit arrived out of order; the caller hands off to o3Replay.
        boolean o3Detected;
        // The base seqTxn of the out-of-order commit when o3Detected.
        long o3SeqTxn;
        // Max output timestamp mirrored to the staging buffer (LONG_NULL if none).
        long stagingMaxTs;
        // Min output timestamp mirrored to the staging buffer (LONG_NULL if none).
        long stagingMinTs;

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
        // Backfill sweep's data-cursor row offset read from a BACKFILL_CURSOR
        // block. Numbers.LONG_NULL when the restored checkpoint carries no such
        // block (any steady .cp), signalling "not a resumable backfill head".
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
