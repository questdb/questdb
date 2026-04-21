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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
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
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
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
 * Job that processes WAL commit notifications and refreshes live view instances.
 * <p>
 * On the first refresh (bootstrap), compiles the live view's SELECT query, caches
 * the factory on the instance, and populates the InMemoryTable via the full factory
 * cursor. Window function state accumulates during bootstrap and carries over into
 * subsequent incremental refreshes.
 * <p>
 * Incremental refreshes walk the {@link TransactionLogCursor} from
 * {@code lastProcessedSeqTxn + 1}, open each WAL segment's event file to check
 * the transaction type, and for DATA events read the raw rows through
 * {@link WalSegmentPageFrameCursor} wrapped in the factory's incremental cursor.
 * Non-DATA events (TRUNCATE, SQL, schema changes) trigger a full recompute that
 * clears the table, resets window functions, and re-bootstraps.
 */
public class LiveViewRefreshJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final IntList columnIndexes = new IntList();
    private final IntList columnSizeShifts = new IntList();
    private final CairoEngine engine;
    private final SqlExecutionContextImpl executionContext;
    private final FilteringRecordCursor filteringCursor = new FilteringRecordCursor();
    private final PageFrameMemoryPool memoryPool = new PageFrameMemoryPool(0);
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
        this.executionContext = new SqlExecutionContextImpl(engine, sharedQueryWorkerCount).with(AllowAllSecurityContext.INSTANCE);
        this.walEventReader = new WalEventReader(engine.getConfiguration());
        this.stateStore = engine.getLiveViewStateStore();
    }

    @Override
    public void close() {
        LOG.debug().$("live view refresh job closing [workerId=").$(workerId).I$();
        executionContext.close();
        Misc.free(walEventReader);
        Misc.free(walPath);
        Misc.free(addressCache);
        Misc.free(memoryPool);
    }

    /**
     * Forces a drain of every live view's merge buffer, regardless of whether new
     * WAL commits have arrived. Each view is processed with {@code forceDrain=true},
     * so the drain watermark equals the max timestamp currently held in the buffer.
     * <p>
     * Used by {@code LiveViewTimerJob} to flush idle buffers and by tests to observe
     * the full view state without waiting for the LAG window to advance.
     */
    public void forceFlushAllViews() {
        LiveViewRegistry registry = engine.getLiveViewRegistry();
        registry.getViews(viewInstanceSink);
        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            refreshInstance(instance, instance.getLastProcessedSeqTxn(), true);
        }
    }

    @Override
    public boolean run(int workerId, @NotNull Job.RunStatus runStatus) {
        assert this.workerId == workerId;
        return processNotifications();
    }

    /**
     * Rebuilds the InMemoryTable from scratch. Feeds every base-table row through the
     * merge buffer, drains rows older than the LAG watermark into the bootstrap window
     * cursor, and writes the result to the InMemoryTable. Rows within the LAG window
     * remain in the merge buffer for the next incremental refresh to pick up.
     * <p>
     * When {@code forceDrain} is true the watermark equals the max timestamp observed,
     * so every buffered row is emitted — used for idle-timer flushes where we want
     * to publish everything currently held back.
     */
    private void bootstrap(LiveViewInstance instance, boolean forceDrain) throws SqlException {
        WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        RecordCursorFactory baseFactory = windowFactory.getBaseFactory();

        // Claim the write buffer before any destructive work on the merge buffer or
        // window state: a readers-are-still-pinned failure later would lose rows.
        InMemoryTable writeBuffer = instance.tryAcquireWriteBuffer();
        if (writeBuffer == null) {
            LOG.debug().$("live view bootstrap deferred, write buffer pinned by readers [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return;
        }

        try {
            windowFactory.resetWindowFunctions();
            MergeBuffer mergeBuffer = ensureMergeBuffer(instance, baseFactory.getMetadata());
            mergeBuffer.reset();

            RecordCursor baseCursor = baseFactory.getCursor(executionContext);
            try {
                Record record = baseCursor.getRecord();
                while (baseCursor.hasNext()) {
                    mergeBuffer.addRow(record);
                }
            } finally {
                baseCursor.close();
            }

            long lagMicros = instance.getDefinition().getLagMicros();
            long watermark = forceDrain ? mergeBuffer.getMaxTsSeen() : mergeBuffer.getMaxTsSeen() - lagMicros;

            writeBuffer.clear();
            RecordCursor drainCursor = mergeBuffer.drain(watermark);
            try {
                RecordCursor windowCursor = windowFactory.getBootstrapCursor(drainCursor, executionContext);
                try {
                    Record record = windowCursor.getRecord();
                    while (windowCursor.hasNext()) {
                        writeBuffer.appendRow(record);
                    }
                } finally {
                    windowCursor.close();
                }
            } catch (Throwable t) {
                Misc.free(drainCursor);
                throw t;
            }
            writeBuffer.applyRetention(instance.getDefinition().getRetentionMicros());
            instance.publishWriteBuffer();
        } catch (Throwable t) {
            instance.abortWriteBuffer(writeBuffer);
            throw t;
        }
    }

    private void buildColumnMappings(RecordMetadata baseMetadata) {
        columnIndexes.clear();
        columnSizeShifts.clear();
        for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
            int writerIndex = baseMetadata.getWriterIndex(i);
            columnIndexes.add(writerIndex);
            int type = baseMetadata.getColumnType(i);
            if (ColumnType.isVarSize(type)) {
                columnSizeShifts.add(0);
            } else {
                columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(type)));
            }
        }
    }

    /**
     * Computes the drain watermark, drains the merge buffer, and feeds the drained rows
     * through the window cursor into the InMemoryTable. No-op when the buffer is empty.
     * When {@code forceDrain} is true, the watermark is the max observed timestamp so
     * every buffered row is emitted — used for idle-timer flushes.
     */
    private void drainAndCommit(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            MergeBuffer mergeBuffer,
            boolean forceDrain
    ) throws SqlException {
        if (mergeBuffer.isEmpty()) {
            return;
        }
        // Acquire the write buffer before draining — drain is destructive on the merge
        // buffer (rows with ts <= watermark are dropped on cursor close). A late failure
        // to acquire would lose rows with no rollback path.
        InMemoryTable writeBuffer = instance.tryAcquireWriteBuffer();
        if (writeBuffer == null) {
            LOG.debug().$("live view incremental refresh deferred, write buffer pinned by readers [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return;
        }

        try {
            // Sync the write buffer from the currently-published state; appending the
            // delta on top yields the next publishable snapshot.
            writeBuffer.copyFrom(instance.peekPublishedBuffer());

            long lagMicros = instance.getDefinition().getLagMicros();
            long watermark = forceDrain ? mergeBuffer.getMaxTsSeen() : mergeBuffer.getMaxTsSeen() - lagMicros;
            RecordCursor drainCursor = mergeBuffer.drain(watermark);
            try {
                RecordCursor windowCursor = windowFactory.getIncrementalCursor(drainCursor, executionContext);
                try {
                    Record record = windowCursor.getRecord();
                    while (windowCursor.hasNext()) {
                        writeBuffer.appendRow(record);
                    }
                } finally {
                    windowCursor.close();
                }
            } catch (Throwable t) {
                Misc.free(drainCursor);
                throw t;
            }
            writeBuffer.applyRetention(instance.getDefinition().getRetentionMicros());
            instance.publishWriteBuffer();
        } catch (Throwable t) {
            instance.abortWriteBuffer(writeBuffer);
            throw t;
        }
    }

    private RecordCursorFactory ensureCompiledFactory(LiveViewInstance instance) throws SqlException {
        RecordCursorFactory factory = instance.getCompiledFactory();
        if (factory == null) {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile(instance.getDefinition().getViewSql(), executionContext);
                factory = cq.getRecordCursorFactory();
            }
            instance.setCompiledFactory(factory);
        }
        return factory;
    }

    private MergeBuffer ensureMergeBuffer(LiveViewInstance instance, RecordMetadata baseMetadata) {
        MergeBuffer mergeBuffer = instance.getMergeBuffer();
        if (mergeBuffer == null) {
            mergeBuffer = new MergeBuffer(baseMetadata);
            instance.setMergeBuffer(mergeBuffer);
        }
        return mergeBuffer;
    }

    /**
     * Resets window state and re-bootstraps from the base table. Called when
     * a non-DATA WAL event (TRUNCATE, SQL, schema change) makes incremental
     * refresh impossible.
     */
    private void fullRecompute(LiveViewInstance instance, boolean forceDrain) throws SqlException {
        bootstrap(instance, forceDrain);
    }

    private WindowRecordCursorFactory getWindowFactory(LiveViewInstance instance) throws SqlException {
        RecordCursorFactory factory = ensureCompiledFactory(instance);
        return unwrapWindowFactory(factory);
    }

    /**
     * Walks the {@link TransactionLogCursor} from {@code fromSeqTxn} through
     * {@code toSeqTxn}. For each DATA transaction, reads the corresponding WAL segment
     * rows through the merge buffer; after all segments have been buffered, drains rows
     * older than the LAG watermark through the window cursor and appends them to the
     * InMemoryTable. Falls back to a full recompute on any non-DATA event or when WAL
     * segment data is unavailable (e.g. clean symbol files were never hardlinked by the
     * WAL writer).
     */
    private void incrementalRefresh(
            LiveViewInstance instance,
            long fromSeqTxn,
            long toSeqTxn,
            boolean forceDrain
    ) throws SqlException {
        try {
            incrementalRefresh0(instance, fromSeqTxn, toSeqTxn, forceDrain);
        } catch (CairoException e) {
            LOG.info().$("WAL segment data unavailable, falling back to full recompute [error=").$(e.getMessage()).I$();
            fullRecompute(instance, forceDrain);
        }
    }

    private void incrementalRefresh0(
            LiveViewInstance instance,
            long fromSeqTxn,
            long toSeqTxn,
            boolean forceDrain
    ) throws SqlException {
        WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
        // When the view's SELECT has a WHERE clause, the planner inserts a filter factory
        // between the window and the page-frame factory. We apply its filter row-by-row
        // against WAL segment rows so the window functions never observe filtered-out rows.
        // TODO(live-view): hook in the JIT-compiled filter from AsyncJitFilteredRecordCursorFactory.
        //  The Java Function path below is always correct but ignores the JIT fast-path on the
        //  WAL refresh hot loop. Supporting it requires invoking CompiledFilter.call() on the
        //  single NATIVE WAL page frame and iterating the resulting row-id bitmap.
        RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
        final Function filter = filterFactory.getFilter();
        RecordCursorFactory pageFrameFactory = filter != null ? filterFactory.getBaseFactory() : filterFactory;
        TableToken baseToken = instance.getDefinition().getBaseTableToken();
        RecordMetadata baseMetadata = pageFrameFactory.getMetadata();

        buildColumnMappings(baseMetadata);
        MergeBuffer mergeBuffer = ensureMergeBuffer(instance, baseMetadata);

        WalSegmentPageFrameCursor frameCursor = new WalSegmentPageFrameCursor(
                engine.getConfiguration(), columnIndexes, columnSizeShifts
        );
        WalSegmentRecordCursor walRecordCursor = new WalSegmentRecordCursor(addressCache, memoryPool);
        try {
            try (TransactionLogCursor txnCursor = engine.getTableSequencerAPI().getCursor(baseToken, fromSeqTxn)) {
                while (txnCursor.hasNext()) {
                    long txn = txnCursor.getTxn();
                    if (txn > toSeqTxn) {
                        break;
                    }
                    int walId = txnCursor.getWalId();
                    int segmentId = txnCursor.getSegmentId();
                    int segmentTxn = txnCursor.getSegmentTxn();

                    if (walId <= 0) {
                        fullRecompute(instance, forceDrain);
                        return;
                    }

                    walPath.of(engine.getConfiguration().getDbRoot())
                            .concat(baseToken)
                            .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                    WalEventCursor eventCursor = WalTxnDetails.openWalEFile(walPath, walEventReader, segmentTxn, txn);

                    if (!WalTxnType.isDataType(eventCursor.getType())) {
                        fullRecompute(instance, forceDrain);
                        return;
                    }

                    WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    long startRow = dataInfo.getStartRowID();
                    long endRow = dataInfo.getEndRowID();
                    if (endRow <= startRow) {
                        continue;
                    }

                    walNameSink.clear();
                    walNameSink.put(WAL_NAME_BASE).put(walId);
                    frameCursor.of(baseToken, walNameSink, segmentId, endRow, startRow, endRow, baseMetadata);
                    walRecordCursor.of(frameCursor, baseMetadata);

                    RecordCursor source = walRecordCursor;
                    if (filter != null) {
                        // Re-init bind variables and symbol-table caches against the current segment's
                        // cursor; symbol keys may differ between segments produced by different WAL writers.
                        filteringCursor.of(walRecordCursor, filter, executionContext);
                        source = filteringCursor;
                    }

                    Record record = source.getRecord();
                    while (source.hasNext()) {
                        mergeBuffer.addRow(record);
                    }
                }
            }
        } finally {
            Misc.free(frameCursor);
        }

        drainAndCommit(instance, windowFactory, mergeBuffer, forceDrain);
    }

    private boolean processNotifications() {
        boolean didWork = false;
        while (stateStore.tryDequeueRefreshTask(refreshTask)) {
            refreshViewsForBaseTable(refreshTask.baseTableToken, refreshTask.seqTxn, refreshTask.forceDrain);
            if (!refreshTask.forceDrain) {
                // Reopen the dedup gate and re-enqueue if a newer commit landed while we were busy.
                // Force-drain tasks don't touch the gate - they run alongside normal WAL-driven refreshes.
                stateStore.notifyBaseRefreshed(refreshTask, refreshTask.seqTxn);
            }
            didWork = true;
        }
        return didWork;
    }

    private void refreshInstance(LiveViewInstance instance, long seqTxn, boolean forceDrain) {
        if (!instance.tryLockForRefresh()) {
            return;
        }
        try {
            if (instance.isDropped() || instance.isInvalid()) {
                return;
            }
            try {
                long lastSeqTxn = instance.getLastProcessedSeqTxn();
                boolean bootstrapAttempted = false;
                if (lastSeqTxn < 0) {
                    bootstrapAttempted = true;
                    bootstrap(instance, forceDrain);
                } else if (seqTxn > lastSeqTxn) {
                    incrementalRefresh(instance, lastSeqTxn, seqTxn, forceDrain);
                } else if (forceDrain) {
                    // Idle flush: no new WAL transactions, just drain whatever the merge
                    // buffer is still holding through the window cursor.
                    WindowRecordCursorFactory windowFactory = getWindowFactory(instance);
                    MergeBuffer mergeBuffer = instance.getMergeBuffer();
                    if (mergeBuffer != null) {
                        drainAndCommit(instance, windowFactory, mergeBuffer, true);
                    }
                }
                // Advance lastProcessedSeqTxn for an incremental refresh even when the final
                // drainAndCommit bailed on write buffer contention: WAL rows were already
                // drained into the merge buffer, so the retry path (force-drain via the
                // timer) just needs to flush the merge buffer into the write buffer, not
                // re-read the same WAL segments. Bootstrap is different — it never touches
                // WAL state on a pending-refresh bail, so do not advance or the retry would
                // take the incremental branch on an un-bootstrapped view.
                if (seqTxn > lastSeqTxn && !(bootstrapAttempted && instance.isPendingRefresh())) {
                    instance.setLastProcessedSeqTxn(seqTxn);
                }
                instance.setLastRefreshTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks());
            } catch (Throwable t) {
                LOG.critical().$("live view refresh failed [view=").$(instance.getDefinition().getViewName())
                        .$(", error=").$(t)
                        .I$();
            }
        } finally {
            instance.unlockAfterRefresh();
            instance.tryCloseIfDropped();
        }
    }

    private void refreshViewsForBaseTable(TableToken baseTableToken, long seqTxn, boolean forceDrain) {
        LiveViewRegistry registry = engine.getLiveViewRegistry();
        registry.getViewsForBaseTable(baseTableToken.getTableName(), viewInstanceSink);

        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            if (seqTxn > instance.getLastProcessedSeqTxn() || forceDrain) {
                refreshInstance(instance, seqTxn, forceDrain);
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
