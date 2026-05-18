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
 * Phase 1 live-view refresh job.
 * <p>
 * Disk-only refresh path: walks the base table's sequencer log forward from
 * {@code lastProcessedSeqTxn + 1}, opens each WAL segment via
 * {@link WalSegmentPageFrameCursor}, runs rows through the compiled SELECT's
 * filter + window cursor, and writes outputs to the live view's own WAL via
 * {@link WalWriter}. Phase 1b applies the just-written WAL block inline on
 * this worker via a dedicated {@link ApplyWal2TableJob} — the global apply
 * job's {@code doRun} skips LV tokens so it never races the inline apply.
 * Once apply commits, {@code lvConsumedSeqTxn} advances and {@code _lv.s}
 * persists atomically through {@code engine.advanceLiveViewConsumedSeqTxn}.
 * <p>
 * Phase 1 sharp edges (per delta plan §"Phase 1 — disk-only end-to-end"):
 * <ul>
 *     <li>No checkpoints, no JIT filter path, no cold-skip / warm-replay branching.</li>
 *     <li>FLUSH EVERY enforces a minimum interval between LV WAL commits: a refresh
 *     that arrives within {@code flushEveryMicros} of the previous commit is skipped
 *     and the fallback scan retries on the next worker tick. Under high-rate base
 *     ingestion this batches many base notifications into one LV commit per FLUSH
 *     EVERY interval.</li>
 *     <li>Schema-change detection still routes through {@code ApplyWal2TableJob} on
 *     the base table — non-DATA WAL events on the base are walked past by this job
 *     without modifying state, while invalidation flows via
 *     {@link CairoEngine#invalidateLiveViewsForBaseTable}.</li>
 *     <li>The LV's WAL block carries {@code maxBaseSeqTxnInBlock} on a dedicated
 *     {@code WalTxnType#LIVE_VIEW_DATA} event; the inline apply on this worker
 *     reads it back and bumps {@code lvConsumedSeqTxn} after the rows are durable
 *     in the LV's own table (RFC 123 §Flush).</li>
 * </ul>
 */
public class LiveViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final AnchorDispatchingCursor anchorDispatchingCursor = new AnchorDispatchingCursor();
    private final ApplyWal2TableJob applyJob;
    private final BlockFileWriter blockFileWriter;
    // Reusable manifest bean for the head-checkpoint write hook and the
    // restore path. Mutated only on the refresh-worker thread between clear()
    // and use.
    private final LiveViewCheckpointManifest checkpointManifest = new LiveViewCheckpointManifest();
    // Per-worker reusable checkpoint reader for the 2a.7 restart-restore
    // path. Lazily allocated on the first LV with a head .cp to restore;
    // reused for subsequent LVs by re-opening on a different file.
    private LiveViewCheckpointReader checkpointReader;
    // Bulk-copy buffer for restoring per-function snapshot blocks. The
    // WindowFunction.restore(MemoryR, int) contract reads from offset 0,
    // so the framework copies a function's payload bytes here from the
    // checkpoint file and hands the scratch to the function. Lazily
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
    private final CairoEngine engine;
    private final LiveViewRefreshSqlExecutionContext executionContext;
    private final FilteringRecordCursor filteringCursor = new FilteringRecordCursor();
    private final PageFrameMemoryPool memoryPool = new PageFrameMemoryPool(0);
    private final Path path = new Path();
    private final LiveViewRefreshTask refreshTask = new LiveViewRefreshTask();
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
    public boolean run(int workerId, @NotNull Job.RunStatus runStatus) {
        assert this.workerId == workerId;
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
                instance.setCompiledFactory(factory);
                // Lazily compile the anchor expression as a Function so the
                // ANCHOR runtime can evaluate it per-row. Only fires when the
                // LV has an anchored named WINDOW persisted in _lv.
                ensureAnchorFunction(instance, factory);
            } finally {
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
    private void ensureAnchorFunction(LiveViewInstance instance, RecordCursorFactory compiledFactory) {
        if (instance.getAnchorFunction() != null) {
            return;
        }
        LiveViewDefinition.LvAnchorSpec spec = instance.getDefinition().getAnchorSpec();
        if (spec == null || spec.anchorExpressionSql == null) {
            return;
        }
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            // Re-parse just the anchor expression text into an ExpressionNode.
            // Going via generateExecutionModel does not work because the optimiser
            // strips named windows after copying their spec into referencing
            // SELECT-column WindowExpressions, and copySpecFrom does not carry the
            // anchor spec across.
            ExpressionNode anchorNode = compiler.parseExpression(spec.anchorExpressionSql);
            if (anchorNode == null) {
                LOG.error().$("anchor compile: parser returned null node [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", sql=").$safe(spec.anchorExpressionSql).I$();
                return;
            }
            // Resolve against the LV's projected metadata (the page-frame factory's
            // metadata at the leaf of the compiled tree). That matches the records
            // WalSegmentRecordCursor emits at runtime.
            RecordMetadata projectedMeta = findLeafProjectedMetadata(compiledFactory);
            if (projectedMeta == null) {
                LOG.error().$("anchor compile: could not find leaf projected metadata [view=")
                        .$(instance.getDefinition().getViewName()).I$();
                return;
            }
            FunctionParser fp = new FunctionParser(engine.getConfiguration(), engine.getFunctionFactoryCache());
            executionContext.setLiveViewCompile(true);
            Function fn;
            try {
                fn = fp.parseFunction(anchorNode, projectedMeta, executionContext);
            } finally {
                executionContext.setLiveViewCompile(false);
            }
            instance.setAnchorFunction(fn);

            WindowRecordCursorFactory wf = unwrapWindowFactory(compiledFactory);
            LiveViewWindow window = LiveViewWindow.build(
                    engine.getConfiguration(),
                    compiler.getAsm(),
                    spec.windowName,
                    projectedMeta,
                    spec.partitionColumnNames,
                    fn,
                    wf.getWindowFunctions()
            );
            instance.setAnchorWindow(window);
        } catch (SqlException e) {
            LOG.error().$("could not compile live-view anchor function [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$safe(e.getFlyweightMessage())
                    .I$();
        } catch (Throwable t) {
            LOG.error().$("could not compile live-view anchor function [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", error=").$(t).I$();
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
    private void incrementalRefresh(LiveViewInstance instance, long fromSeqTxn, long toSeqTxn) throws SqlException {
        WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
        final Function filter = filterFactory.getFilter();
        RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
        TableToken baseToken = instance.getDefinition().getBaseTableToken();
        RecordMetadata baseMetadata = pageFrameFactory.getMetadata();
        buildColumnMappings(baseMetadata, baseToken);

        // RFC 123 Phase 1b: decide whether the in-memory tier can be populated
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

        long advanceTo = -1;
        // Hoisted out of the try-with-resources so the post-loop branch can decide
        // whether the apply path will publish the new lvConsumedSeqTxn (rows emitted)
        // or whether we must publish it directly (no LIVE_VIEW_DATA block written).
        long appendedRows = 0;
        // Max ts observed across every applied row in this cycle, in base-table
        // timestamp units. Drives the {@code .cp} manifest's {@code maxTimestamp}
        // field which the 2a.8 O3 path consults to decide head-hit vs head-miss.
        // Tracked here unconditionally (the staging path tracks stagingMaxTs only
        // when populateTier is true, so we cannot reuse it).
        long batchMaxTs = Numbers.LONG_NULL;
        // True when a base txn in this cycle arrived with min ts below the LV's
        // latestSeenTs watermark. On detect, the WAL writer is rolled back, the
        // txn loop breaks, and the post-loop replay path takes over.
        boolean o3Detected = false;
        long o3LateRowTs = Numbers.LONG_NULL;
        long o3SeqTxn = Numbers.LONG_NULL;
        long stagingMaxTs = Numbers.LONG_NULL;
        long stagingMinTs = Numbers.LONG_NULL;
        // Snapshot the LV's latestSeenTs at cycle entry. On O3 detect +
        // rollback any in-cycle bumps from the discarded rows must roll back
        // too, otherwise a later in-order commit whose ts sits between the
        // pre-cycle watermark and the inflated value gets misclassified as O3.
        final long latestSeenTsSnapshot = instance.getLatestSeenTs();
        try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
            RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
            int lvTimestampIndex = walWriter.getMetadata().getTimestampIndex();
            if (lvTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(instance.getDefinition().getViewName()).put(']');
            }

            final int turnMaxCommits = engine.getConfiguration().getLiveViewRefreshTurnMaxCommits();
            final long turnMaxDurationUs = engine.getConfiguration().getLiveViewRefreshTurnMaxDurationMicros();
            try (TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn)) {
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
                    // Out-of-order detection. A commit whose min ts sits below
                    // the LV's latestSeenTs watermark cannot be processed in
                    // WAL order without corrupting window-function state.
                    // Discard any rows queued earlier in this cycle, break
                    // out of the loop, and hand off to o3Replay below, which
                    // re-feeds base data in ts order via TableReader and
                    // emits a single REPLACE_RANGE commit covering everything
                    // from viewLowerBoundTimestamp (head-miss) or the head's
                    // maxTimestamp (head-hit, follow-up commit) forward.
                    final long latestSeen = instance.getLatestSeenTs();
                    final long txnMinTs = dataInfo.getMinTimestamp();
                    if (latestSeen != Numbers.LONG_NULL && txnMinTs < latestSeen) {
                        walWriter.rollback();
                        // Roll back the in-cycle latestSeenTs bumps along with
                        // the WAL writes. The replay path below re-stamps the
                        // watermark from the re-fed rows; without this restore
                        // a follow-up in-order commit at the inflated ts would
                        // be misclassified as O3.
                        instance.forceSetLatestSeenTs(latestSeenTsSnapshot);
                        o3Detected = true;
                        o3LateRowTs = txnMinTs;
                        o3SeqTxn = txn;
                        // Reset cycle-local accounting so the post-loop apply
                        // branch does not see stale state (it never runs in
                        // this cycle, but the explicit reset keeps the
                        // invariants narrow).
                        appendedRows = 0;
                        batchMaxTs = Numbers.LONG_NULL;
                        stagingMinTs = Numbers.LONG_NULL;
                        stagingMaxTs = Numbers.LONG_NULL;
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

                    RecordCursor source = walRecordCursor;
                    if (filter != null) {
                        filteringCursor.of(walRecordCursor, filter, executionContext);
                        source = filteringCursor;
                    }
                    LiveViewWindow anchorWindow = instance.getAnchorWindow();
                    if (anchorWindow != null) {
                        // Anchor dispatch sits between the filter (or raw WAL cursor)
                        // and the window cursor so window functions see resetPartition
                        // before pass1 evaluates the row.
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
                            TableWriter.Row row = walWriter.newRow(ts);
                            copier.copy(executionContext, outRecord, row);
                            row.append();
                            if (populateTier) {
                                // Mirror the row into the worker-local staging
                                // buffer. The slow-path swap below (after apply
                                // commits) copies retained published-slot rows
                                // and appends staging on top.
                                stagingBuffer.copyRowFromRecord(outRecord, appendedRows);
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

            if (appendedRows > 0) {
                // RFC 123 Phase 1b: the LV WAL block carries advanceTo as
                // maxBaseSeqTxnInBlock. The inline apply below makes the rows
                // durable in the LV's on-disk table; only then do we advance
                // lvConsumedSeqTxn so base WAL retention releases.
                walWriter.commitLiveView(advanceTo);
                if (populateTier) {
                    stagingBuffer.setRowCount(appendedRows);
                    stagingBuffer.setSeamTs(stagingMinTs);
                }
            }
        }

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
            // appliedWatermark mirrors lastProcessed for now; sub-FLUSH-cycle freshness via
            // the in-mem tier is parked, so the seam_ts is anchored at the WAL commit boundary.
            instance.setAppliedWatermark(advanceTo);
            boolean lvConsumedPersisted = false;
            if (appendedRows > 0) {
                // RFC 123 Phase 1b: LV apply runs inline on this thread. The
                // global ApplyWal2TableJob.doRun skips LV tokens, so without
                // applyWalDirect here the LIVE_VIEW_DATA block would sit
                // unapplied and the on-disk tier would not catch up.
                applyJob.applyWalDirect(instance.getLiveViewToken(), Job.RUNNING_STATUS);
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
                // stall forever (RFC 123 §"Lifecycle / Invalidation - Base-table
                // data removal").
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
                // handleRefreshFailure which ticks the flush-retry budget
                // (RFC 123 §Flush).
                persistState(instance);
            }
            if (lvConsumedPersisted && populateTier && appendedRows > 0) {
                // Phase 1b slow-path swap: copy retained rows from the published
                // slot, append staging on top, publishSwap. Failure to acquire
                // the write slot is a non-fatal stall — the on-disk tier still
                // advanced, the in-mem tier just trails for this cycle.
                publishToInMemoryTier(instance, stagingMaxTs);
            }
            if (lvConsumedPersisted && appendedRows > 0) {
                // 2a.4 head-checkpoint write hook. Ordered after the apply's
                // _txn advance and the lvConsumedSeqTxn publish so the .cp on
                // disk reflects state that is also durably committed in the
                // LV's own table. A failure here does not invalidate the view
                // (RFC 123 "Flush" step 4): the prior head remains addressable
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
     * Phase 2a.8 head invalidation on out-of-order arrival. The current cycle
     * still feeds the offending batch through the in-WAL-order pipeline (so
     * the live output for the affected partitions is wrong for this batch -
     * same disposition Phase 1b shipped with); the value of this helper is
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
     * Phase 2a.8 out-of-order replay. Called from {@code incrementalRefresh}
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
     * matching the Phase 1b behaviour for any LV whose SELECT contains a
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
        final boolean headHitEligible = headLvSeqTxn != Numbers.LONG_NULL && headMaxTs <= lateRowTs;
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
    }

    /**
     * Phase 2a.8 head-hit replay: rolls window state back to the head .cp's
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
     * Phase 2a.8 head-miss replay path: discards every window-function
     * partition map and the anchor map, opens the base table at applied
     * watermark &gt;= {@code advanceTo}, drives the compiled SELECT's
     * filter / anchor / window cursor stack over the {@code TableReader}'s
     * ts-sorted view starting from {@code viewLowerBoundTimestamp}, emits
     * a single REPLACE_RANGE commit covering everything from the lower
     * bound through positive infinity, and applies inline.
     * <p>
     * Cost is O(retained_rows x n_window_functions) of {@code computeNext}
     * plus the partition-rewrite I/O - acceptable for short-lived views
     * but several seconds to minutes for long-lived ones, per the RFC
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

        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        final LiveViewWindow anchorWindow = instance.getAnchorWindow();
        final long viewLowerBoundTimestamp = instance.getDefinition().getViewLowerBoundTimestamp();
        TableReader reader = waitForApply(baseToken, advanceTo);
        boolean readerAttached = false;
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
                // Wipe per-function partition state and the anchor map. The
                // compiled factory's WindowFunction instances stay live so
                // the cursor chain below can reuse them; only their
                // accumulated state resets.
                for (int i = 0, n = functions.size(); i < n; i++) {
                    Map m = functions.getQuick(i).getPartitionMap();
                    if (m != null) {
                        m.clear();
                    }
                }
                if (anchorWindow != null) {
                    anchorWindow.toTop();
                }

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
                            walWriter.commitLiveViewWithReplaceRange(
                                    advanceTo,
                                    viewLowerBoundTimestamp,
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
            LOG.critical().$("could not advance live view consumed seqTxn after O3 replay [view=")
                    .$(viewName)
                    .$(", advanceTo=").$(advanceTo)
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
            maybeWriteHeadCheckpoint(instance, windowFactory, advanceTo, replayMaxTs, appendedRows);
        }
        LOG.info().$("live view O3 head-miss replay completed [view=")
                .$(viewName)
                .$(", advanceTo=").$(advanceTo)
                .$(", rowsEmitted=").$(appendedRows).I$();
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
     * Phase 2a.4 head-checkpoint write hook. Computes the per-LV snapshot
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
     * A failure here does not invalidate the view (RFC 123 "Flush" step 4).
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
            checkpointManifest.setLvRowPosition(0L);
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
                f.snapshot(fnSink);
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
     * invalidated; the caller falls through to the head-miss replay path
     * (RFC 123 §"Checkpoint stream", §"Corruption handling").
     */
    private boolean restoreFromHead(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            long headLvSeqTxn,
            RestoredHeadState out
    ) {
        out.reset();
        path.of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .slash();
        LiveViewCheckpointWriter.appendCpFileName(path, headLvSeqTxn);

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
            out.stateBytes = engine.getConfiguration().getFilesFacade().length(path.$());

            final LiveViewWindow anchorWindow = instance.getAnchorWindow();
            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
            final LiveViewCheckpointReader.BlockCursor cursor = checkpointReader.getCursor();
            // The MANIFEST is the first block; skip it - readManifestInto
            // already consumed it conceptually but resets the cursor.
            // Walk forward and dispatch by type.
            cursor.hasNext();
            cursor.next();
            while (cursor.hasNext()) {
                final LiveViewCheckpointReader.ReadableBlock block = cursor.next();
                switch (block.type()) {
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
            if (ce.getErrno() == CairoException.LV_FUNCTION_SNAPSHOT_VERSION_TOO_OLD) {
                // Version mismatch is a real compatibility break, not
                // corruption. Stash the reason on the instance so the caller
                // drives invalidation outside the refresh latch
                // (engine.invalidateLiveView parks on the instance monitor
                // when a checkpoint freeze is active, and the agent's
                // startCheckpoint cannot complete its latch handshake while
                // the worker still holds the refresh latch).
                LOG.critical().$("live view function snapshot version mismatch [view=")
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
     * Phase 2a.7 restart-restore: opens the head {@code .cp} (stamped on the
     * instance by the startup sweep), rehydrates the LV's window state from
     * the manifest + anchor block + per-function blocks, then advances
     * {@code lastProcessedSeqTxn} to the manifest's {@code baseSeqTxn} so the
     * upcoming incremental refresh resumes one step past the head.
     * <p>
     * Failure handling: any structural error (CRC fail, magic mismatch,
     * missing function class, anchor type mismatch) unlinks the head .cp
     * and clears the head metadata on the instance. The LV is not
     * invalidated - {@code .cp} is derived state, and the upcoming refresh
     * cycle falls through to the head-miss replay path
     * (RFC 123 §"Checkpoint stream", §"Corruption handling"). A fresh head
     * is written by the same cycle once it lands rows.
     */
    private void tryRestoreFromHead(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        final long headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
        if (!restoreFromHead(instance, windowFactory, headLvSeqTxn, restoredHeadState)) {
            // restoreFromHead has already unlinked the corrupt .cp and
            // cleared head metadata; nothing more to do here.
            return;
        }
        // Advance the refresh worker's view of the base position to the
        // manifest's baseSeqTxn; the next incrementalRefresh resumes one
        // step past it. appliedWatermark mirrors lastProcessed in
        // Phase 1b (the seam_ts is anchored at the WAL commit boundary,
        // see incrementalRefresh).
        instance.setLastProcessedSeqTxn(restoredHeadState.manifestBaseSeqTxn);
        instance.setAppliedWatermark(restoredHeadState.manifestBaseSeqTxn);
        // Refresh the head metadata trio with the real maxTs + stateBytes
        // we just read; the startup sweep stamped placeholder values.
        // writtenUs stays LONG_NULL so the next cycle's cadence check
        // treats this as "first commit" and writes a fresh head soon
        // after - RFC 123 §"Restart recovery": re-emit a fresh .cp after
        // the first post-restart refresh cycle.
        instance.setHeadCheckpoint(
                headLvSeqTxn,
                restoredHeadState.maxTimestamp,
                restoredHeadState.stateBytes,
                Numbers.LONG_NULL
        );
    }

    /**
     * Decodes a single FUNCTION_SNAPSHOT block:
     * <pre>
     *     STR windowName
     *     STR factoryName     (matches snapshotFactoryName(f) - factory class,
     *                          not the function impl, so impl renames survive)
     *     INT formatVersion
     *     ...function-private state bytes (consumed by WindowFunction.restore)
     * </pre>
     * Then bulk-copies the trailing state bytes into the per-worker scratch
     * buffer (so {@link WindowFunction#restore(MemoryR, int)} reads from
     * offset 0 as the contract requires) and dispatches by matching the
     * factory name against the compiled SELECT's window functions.
     */
    private void restoreFunctionBlock(LiveViewCheckpointReader.ReadableBlock block, ObjList<WindowFunction> functions) {
        long offset = 0;
        // windowName: STR. We only need to skip past it - the manifest
        // already captured window names; cross-validation belongs in a
        // later commit if needed.
        offset += strByteSize(block, offset);
        final CharSequence storedFactoryName = block.getStr(offset);
        final long factoryNameByteSize = strByteSize(block, offset);
        offset += factoryNameByteSize;
        final int formatVersion = block.getInt(offset);
        offset += Integer.BYTES;

        WindowFunction match = null;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction candidate = functions.getQuick(i);
            if (Chars.equals(storedFactoryName, snapshotFactoryName(candidate))) {
                match = candidate;
                break;
            }
        }
        if (match == null) {
            throw CairoException.critical(0)
                    .put("function not found in compiled live view SELECT, factory=")
                    .put(storedFactoryName);
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
        match.restore(checkpointRestoreScratch, formatVersion);
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
            // disk-only via TableReader (Phase 1a behaviour).
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
        // changes the LV's _meta (not in V1, but reserved for ALTER LIVE VIEW
        // later) the tier would need to be reshaped too. For Phase 1b _meta is
        // immutable post-CREATE.
        if (instance.getInMemoryTier() == null) {
            instance.setInMemoryTier(new LiveViewInMemoryTier(tierColumnTypes, tsColIdx, pageSize));
        }
        return true;
    }

    /**
     * Publishes this cycle's staging rows into the LV's in-memory tier
     * (RFC 123 Phase 3a).
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
     * the writer stalls: the in-mem tier trails for this cycle, the disk
     * tier is still up to date, and {@code writerStallStartUs} is set so
     * {@code live_views().writer_stall_micros} surfaces the stall duration
     * (RFC 123 §"Stall behavior").
     */
    private void publishToInMemoryTier(LiveViewInstance instance, long stagingMaxTs) {
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            return;
        }
        int publishedIdx = tier.getPublishedIdx();
        LiveViewInMemoryBuffer pubSlot = tier.getSlot(publishedIdx);

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
                    appendStagingInPlace(acquired, stagingBuffer.seamTs());
                } catch (Throwable t) {
                    // Fast-path append cannot leave the slot partially
                    // populated visibly to readers: rowCount only advances
                    // after each row's column writes succeed. Drop the
                    // sentinel and let the flush-retry budget tick.
                    tier.releaseWriteWithoutPublish(publishedIdx);
                    throw t;
                }
                tier.releaseWriteWithoutPublish(publishedIdx);
                instance.setWriterStallStartUs(Numbers.LONG_NULL);
                return;
            }
        }

        // Slow-path: take the non-published slot, copy retained rows, append
        // staging, swap published index.
        int writeIdx = 1 - publishedIdx;
        LiveViewInMemoryBuffer writeSlot = tier.tryAcquireWrite(writeIdx);
        if (writeSlot == null) {
            // Both slots reader-pinned: the view trails this cycle. Record
            // the start of the stall streak; a subsequent successful
            // acquire clears it.
            if (instance.getWriterStallStartUs() == Numbers.LONG_NULL) {
                instance.setWriterStallStartUs(engine.getConfiguration().getMicrosecondClock().getTicks());
            }
            LOG.info().$("live view in-mem tier stalled, both slots pinned [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return;
        }
        try {
            writeSlot.reset();
            int tsCol = stagingTimestampColumnIndex;
            // Compute the eviction threshold in the base table's timestamp
            // units. IN MEMORY is stored in micros; scale to base units once.
            TimestampDriver driver = ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType());
            long inMemoryInBaseUnits = driver.fromMicros(instance.getDefinition().getInMemoryMicros());
            long retainThreshold = stagingMaxTs - inMemoryInBaseUnits;

            // Copy retained rows from the currently-published slot (those
            // with ts >= retainThreshold). Rows in the slot are stored in
            // ts-ascending order, so we can simply skip leading rows until
            // the first retained one is found.
            long writeRow = 0;
            long writeSeamTs = Numbers.LONG_NULL;
            for (long r = 0, rn = pubSlot.rowCount(); r < rn; r++) {
                long srcTs = pubSlot.getLong(r, tsCol);
                if (srcTs < retainThreshold) {
                    continue;
                }
                if (writeSeamTs == Numbers.LONG_NULL) {
                    writeSeamTs = srcTs;
                }
                writeSlot.copyRowFrom(pubSlot, r, writeRow);
                writeRow++;
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
            tier.publishSwap(writeIdx);
            // Clear any prior stall streak — this cycle made progress.
            instance.setWriterStallStartUs(Numbers.LONG_NULL);
        } catch (Throwable t) {
            // Release the writer sentinel without flipping publishedIdx so
            // readers continue to see the previously-published slot. Flipping
            // here would expose a half-populated slot (rowCount=0 since
            // setRowCount runs only on the success path) and silently regress
            // queries that previously saw N rows to seeing 0 rows. Propagate
            // the failure so the flush-retry budget ticks (RFC 123 §Flush).
            tier.releaseWriteWithoutPublish(writeIdx);
            throw t;
        }
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
            if (head > instance.getLastProcessedSeqTxn() || needsRestore) {
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
                // Phase 2a.7: first cycle after restart restores from the head
                // .cp (if any). Single-shot per LV lifetime - the flag flips
                // true whether the restore succeeded, missed, or failed.
                if (!instance.isCheckpointRestoreAttempted()) {
                    instance.setCheckpointRestoreAttempted();
                    if (instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL) {
                        tryRestoreFromHead(instance, getWindowFactory(instance));
                    }
                }
                long lastSeqTxn = instance.getLastProcessedSeqTxn();
                if (seqTxn > lastSeqTxn) {
                    // FLUSH EVERY rate-limit: skip if the previous commit was within
                    // flushEveryMicros. The fallback scan retries each worker tick, so
                    // this view's catch-up resumes naturally once the interval elapses.
                    // We bump lastFlushTimeUs to nowUs only after a successful refresh,
                    // so a long-running first commit does not double-charge the budget.
                    long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                    long lastFlushUs = instance.getLastFlushTimeUs();
                    long flushEveryMicros = instance.getDefinition().getFlushEveryMicros();
                    if (lastFlushUs != Numbers.LONG_NULL && nowUs - lastFlushUs < flushEveryMicros) {
                        return;
                    }
                    // TransactionLogCursor treats txnLo as exclusive (lastApplied), so we
                    // pass lastSeqTxn directly. The cursor's getTxn() returns entries with
                    // seqTxn > lastSeqTxn.
                    attempted = true;
                    incrementalRefresh(instance, lastSeqTxn, seqTxn);
                    instance.setLastFlushTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
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
     * RFC 123 §"Flush" retry budget: count consecutive failures and the elapsed
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
     * Output bundle for {@link #restoreFromHead(LiveViewInstance, WindowRecordCursorFactory, long, RestoredHeadState)}.
     * The fields capture the values restart-restore and O3 head-hit replay
     * both need after the disk read completes; the helper rewrites them on
     * each successful call and the caller reads them immediately.
     */
    private static final class RestoredHeadState {
        long manifestBaseSeqTxn;
        long maxTimestamp;
        long stateBytes;

        void reset() {
            manifestBaseSeqTxn = Numbers.LONG_NULL;
            maxTimestamp = Numbers.LONG_NULL;
            stateBytes = 0L;
        }
    }
}
