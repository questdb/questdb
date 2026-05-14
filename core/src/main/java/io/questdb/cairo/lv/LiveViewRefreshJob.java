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
        long stagingMaxTs = Numbers.LONG_NULL;
        long stagingMinTs = Numbers.LONG_NULL;
        try (WalWriter walWriter = engine.getWalWriter(instance.getLiveViewToken())) {
            RecordToRowCopier copier = ensureCopier(instance, windowFactory, walWriter);
            int lvTimestampIndex = walWriter.getMetadata().getTimestampIndex();
            if (lvTimestampIndex < 0) {
                throw CairoException.nonCritical()
                        .put("live view requires a designated timestamp [view=")
                        .put(instance.getDefinition().getViewName()).put(']');
            }

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

        if (advanceTo > instance.getLastProcessedSeqTxn()) {
            // Advance the in-memory watermarks first; both advanceLiveViewConsumedSeqTxn
            // and persistState below read these to write a single, consistent _lv.s.
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
                maybeWriteHeadCheckpoint(instance, windowFactory, advanceTo, batchMaxTs, appendedRows);
            }
        }
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
                MemoryA anchorSink = checkpointWriter.beginBlock(LiveViewCheckpointBlockType.BLOCK_ANCHOR);
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
                fnSink.putStr(f.getClass().getName());
                fnSink.putInt(f.snapshotFormatVersion());
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

        long restoredMaxTs = Numbers.LONG_NULL;
        long restoredStateBytes = 0L;
        try {
            checkpointReader.of(path.$());
            checkpointReader.readManifestInto(checkpointManifest);
            final long manifestBaseSeqTxn = checkpointManifest.getBaseSeqTxn();
            restoredMaxTs = checkpointManifest.getMaxTimestamp();
            restoredStateBytes = engine.getConfiguration().getFilesFacade().length(path.$());

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
                    case LiveViewCheckpointBlockType.BLOCK_ANCHOR:
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

            // Advance the refresh worker's view of the base position to the
            // manifest's baseSeqTxn; the next incrementalRefresh resumes one
            // step past it. appliedWatermark mirrors lastProcessed in
            // Phase 1b (the seam_ts is anchored at the WAL commit boundary,
            // see incrementalRefresh).
            instance.setLastProcessedSeqTxn(manifestBaseSeqTxn);
            instance.setAppliedWatermark(manifestBaseSeqTxn);
            // Refresh the head metadata trio with the real maxTs + stateBytes
            // we just read; the startup sweep stamped placeholder values.
            // writtenUs stays LONG_NULL so the next cycle's cadence check
            // treats this as "first commit" and writes a fresh head soon
            // after - RFC 123 §"Restart recovery": re-emit a fresh .cp after
            // the first post-restart refresh cycle.
            instance.setHeadCheckpoint(headLvSeqTxn, restoredMaxTs, restoredStateBytes, Numbers.LONG_NULL);
        } catch (Throwable t) {
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
        } finally {
            try {
                checkpointReader.close();
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Decodes a single FUNCTION_SNAPSHOT block:
     * <pre>
     *     STR windowName
     *     STR functionClassName  (matches f.getClass().getName())
     *     INT formatVersion
     *     ...function-private state bytes (consumed by WindowFunction.restore)
     * </pre>
     * Then bulk-copies the trailing state bytes into the per-worker scratch
     * buffer (so {@link WindowFunction#restore(MemoryR, int)} reads from
     * offset 0 as the contract requires) and dispatches by matching the
     * function class name against the compiled SELECT's window functions.
     */
    private void restoreFunctionBlock(LiveViewCheckpointReader.ReadableBlock block, ObjList<WindowFunction> functions) {
        long offset = 0;
        // windowName: STR. We only need to skip past it - the manifest
        // already captured window names; cross-validation belongs in a
        // later commit if needed.
        offset += strByteSize(block, offset);
        final CharSequence storedClassName = block.getStr(offset);
        final long classNameByteSize = strByteSize(block, offset);
        offset += classNameByteSize;
        final int formatVersion = block.getInt(offset);
        offset += Integer.BYTES;

        WindowFunction match = null;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction candidate = functions.getQuick(i);
            final String candidateName = candidate.getClass().getName();
            if (Chars.equals(storedClassName, candidateName)) {
                match = candidate;
                break;
            }
        }
        if (match == null) {
            throw CairoException.critical(0)
                    .put("function not found in compiled live view SELECT, name=")
                    .put(storedClassName);
        }
        if (formatVersion < match.snapshotMinSupportedVersion()
                || formatVersion > match.snapshotFormatVersion()) {
            throw CairoException.critical(0)
                    .put("function snapshot format version out of range, function=")
                    .put(storedClassName)
                    .put(", got=")
                    .put(formatVersion)
                    .put(", minSupported=")
                    .put(match.snapshotMinSupportedVersion())
                    .put(", current=")
                    .put(match.snapshotFormatVersion());
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
     * Runs the slow-path swap into the LV's in-memory tier. After the inline
     * apply commits the WAL block to the on-disk tier, this method takes the
     * non-published slot via {@code tryAcquireWrite}, copies retained rows
     * from the published slot (those still within the {@code IN MEMORY}
     * window relative to {@code stagingMaxTs}), appends the staging rows on
     * top, and publishes the swap.
     * <p>
     * On {@code tryAcquireWrite} failure (both slots reader-pinned) the
     * writer stalls: the in-mem tier trails for this cycle, the disk tier is
     * still up to date, and {@code writerStallStartUs} is set so
     * {@code live_views().writer_stall_micros} surfaces the stall duration.
     */
    private void publishToInMemoryTier(LiveViewInstance instance, long stagingMaxTs) {
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            return;
        }
        int publishedIdx = tier.getPublishedIdx();
        int writeIdx = 1 - publishedIdx;
        LiveViewInMemoryBuffer writeSlot = tier.tryAcquireWrite(writeIdx);
        if (writeSlot == null) {
            // Both slots reader-pinned: the view trails this cycle. Record the
            // start of the stall streak; a subsequent successful acquire clears
            // it. RFC 123 §"Stall behavior".
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

            // Copy retained rows from the currently-published slot (those with
            // ts >= retainThreshold). Rows in the slot are stored in
            // ts-ascending order, so we can simply skip leading rows until the
            // first retained one is found.
            LiveViewInMemoryBuffer pubSlot = tier.getSlot(publishedIdx);
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
