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
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
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
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
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
 * {@link WalWriter} for durability. Once committed, the global
 * {@code ApplyWal2TableJob} applies the rows into the LV's on-disk tier;
 * {@code lvConsumedSeqTxn} advances at apply time so retention only releases
 * once the rows are durable.
 * <p>
 * Phase 1 sharp edges (per delta plan §"Phase 1 — disk-only end-to-end"):
 * <ul>
 *     <li>No checkpoints, no JIT filter path, no cold-skip / warm-replay branching.</li>
 *     <li>FLUSH EVERY enforces a minimum interval between LV WAL commits: a refresh
 *     that arrives within {@code flushEveryMicros} of the previous commit is skipped
 *     and the fallback scan retries on the next worker tick. Under high-rate base
 *     ingestion this batches many base notifications into one LV commit per FLUSH
 *     EVERY interval.</li>
 *     <li>Schema-change detection still routes through {@code ApplyWal2TableJob} —
 *     non-DATA WAL events on the base are walked past by this job without modifying
 *     state, while invalidation flows via
 *     {@link CairoEngine#invalidateLiveViewsForBaseTable}.</li>
 *     <li>The LV's WAL block carries {@code maxBaseSeqTxnInBlock} on a dedicated
 *     {@code WalTxnType#LIVE_VIEW_DATA} event; {@code ApplyWal2TableJob} reads it back
 *     and bumps {@code lvConsumedSeqTxn} at apply time so retention only releases once
 *     the rows are durable in the LV's own table (RFC 123 §Flush).</li>
 * </ul>
 */
public class LiveViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final BlockFileWriter blockFileWriter;
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    private final IntList columnIndexes = new IntList();
    private final IntList columnSizeShifts = new IntList();
    private final CairoEngine engine;
    private final LiveViewRefreshSqlExecutionContext executionContext;
    private final AnchorDispatchingCursor anchorDispatchingCursor = new AnchorDispatchingCursor();
    private final FilteringRecordCursor filteringCursor = new FilteringRecordCursor();
    private final PageFrameMemoryPool memoryPool = new PageFrameMemoryPool(0);
    private final Path path = new Path();
    private final LiveViewRefreshTask refreshTask = new LiveViewRefreshTask();
    private final LiveViewStateStore stateStore;
    private final ObjList<LiveViewInstance> viewInstanceSink = new ObjList<>();
    private final WalEventReader walEventReader;
    private final StringSink walNameSink = new StringSink();
    private final Path walPath = new Path();
    private final int workerId;

    public LiveViewRefreshJob(int workerId, CairoEngine engine, int sharedQueryWorkerCount) {
        this.workerId = workerId;
        this.engine = engine;
        this.executionContext = new LiveViewRefreshSqlExecutionContext(engine, sharedQueryWorkerCount);
        this.walEventReader = new WalEventReader(engine.getConfiguration());
        this.stateStore = engine.getLiveViewStateStore();
        this.blockFileWriter = new BlockFileWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getCommitMode());
    }

    @Override
    public void close() {
        LOG.debug().$("live view refresh job closing [workerId=").$(workerId).I$();
        executionContext.close();
        Misc.free(walEventReader);
        Misc.free(walPath);
        Misc.free(path);
        Misc.free(blockFileWriter);
        Misc.free(addressCache);
        Misc.free(memoryPool);
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

        long advanceTo = -1;
        WalSegmentPageFrameCursor frameCursor = new WalSegmentPageFrameCursor(
                engine.getConfiguration(), columnIndexes, columnSizeShifts
        );
        WalSegmentRecordCursor walRecordCursor = new WalSegmentRecordCursor(addressCache, memoryPool);
        try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
            RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
            int lvTimestampIndex = walWriter.getMetadata().getTimestampIndex();
            // Window cursor metadata mirrors the LV's _meta, so the timestamp index is
            // the same in both sources.
            int cursorTimestampIndex = windowFactory.getMetadata().getTimestampIndex();
            if (lvTimestampIndex < 0 || cursorTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(instance.getDefinition().getViewName()).put(']');
            }

            long appendedRows = 0;
            try (TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn)) {
                while (txnCursor.hasNext()) {
                    long txn = txnCursor.getTxn();
                    if (txn > toSeqTxn) {
                        break;
                    }
                    advanceTo = txn;
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
                    long startRow = dataInfo.getStartRowID();
                    long endRow = dataInfo.getEndRowID();
                    if (endRow <= startRow) {
                        continue;
                    }

                    walNameSink.clear();
                    walNameSink.put(WAL_NAME_BASE).put(walId);
                    frameCursor.of(baseToken, walNameSink, segmentId, endRow, startRow, endRow, baseMetadata, dataInfo);
                    walRecordCursor.of(frameCursor, baseMetadata);

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
                            TableWriter.Row row = walWriter.newRow(ts);
                            copier.copy(executionContext, outRecord, row);
                            row.append();
                            appendedRows++;
                        }
                    } finally {
                        windowCursor.close();
                    }
                }
            }

            if (appendedRows > 0) {
                // The LV-data block carries advanceTo as maxBaseSeqTxnInBlock. ApplyWal2TableJob
                // reads it back at apply time and bumps lvConsumedSeqTxn / persists _lv.s, so base
                // WAL retention only releases once the rows are durable in the LV's own table
                // (RFC 123 strict semantics).
                walWriter.commitLiveView(advanceTo);
            }
        } finally {
            Misc.free(frameCursor);
        }

        if (advanceTo > instance.getLastProcessedSeqTxn()) {
            instance.setLastProcessedSeqTxn(advanceTo);
            // appliedWatermark mirrors lastProcessed for now; sub-FLUSH-cycle freshness via
            // the in-mem tier is parked, so the seam_ts is anchored at the WAL commit boundary.
            instance.setAppliedWatermark(advanceTo);
            // lvConsumedSeqTxn is advanced from the apply path (CairoEngine.advanceLiveViewConsumedSeqTxn).
            persistState(instance);
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
            if (head > instance.getLastProcessedSeqTxn()) {
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
        try {
            if (instance.isDropped() || instance.isInvalid()) {
                return;
            }
            boolean attempted = false;
            try {
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
                handleRefreshFailure(instance, t);
            }
        } finally {
            instance.unlockAfterRefresh();
            instance.tryCloseIfDropped();
        }
    }

    /**
     * RFC 123 §"Flush" retry budget: count consecutive failures and the elapsed
     * wall-clock time since the streak began. On budget exhaustion, invalidate
     * the view via the unified path. The view stops refreshing but stays
     * queryable; recovery is operator-driven (DROP + CREATE).
     */
    private void handleRefreshFailure(LiveViewInstance instance, Throwable t) {
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
            engine.invalidateLiveView(instance, "flush retry budget exhausted");
        } else {
            LOG.critical().$("live view refresh failed [view=").$(instance.getDefinition().getViewName())
                    .$(", retryCount=").$(retryCount)
                    .$(", error=").$(t).I$();
        }
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
}
