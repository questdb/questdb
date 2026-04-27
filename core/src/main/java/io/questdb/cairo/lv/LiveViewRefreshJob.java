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
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
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
    private final LiveViewRefreshSqlExecutionContext executionContext;
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
        this.executionContext = new LiveViewRefreshSqlExecutionContext(engine, sharedQueryWorkerCount);
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
     * <p>
     * Also the disk-read replay entry point for any-unbounded views: pass a
     * {@code lowerBoundOverride} older than {@code maxTs - retention} to expand the
     * backfill window and rebuild the accumulator so it reflects a cold row (one
     * that arrived past retention and cannot pass through the merge buffer). Pass
     * {@link Long#MAX_VALUE} for the normal bounded backfill. The effective lower
     * bound becomes the view's {@code stateBackfillTimestamp}, which {@link #drainAndCommit}
     * consults on subsequent warm paths to decide between merge-buffer replay and
     * another disk-read replay.
     */
    private void bootstrap(LiveViewInstance instance, boolean forceDrain, long lowerBoundOverride) throws SqlException {
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
            RecordMetadata baseMetadata = baseFactory.getMetadata();
            MergeBuffer mergeBuffer = ensureMergeBuffer(instance, baseMetadata);
            mergeBuffer.reset();

            // Bounded backfill: window functions see only the last RETENTION worth of
            // data. The lower bound is pushed into the base scan as a BETWEEN intrinsic
            // via LiveViewRefreshSqlExecutionContext.setRange, so the partition-frame
            // cursor prunes partitions outside the retained window — bootstrap cost is
            // O(retained rows) rather than O(base rows). The reader's seqTxn pins the
            // high watermark of WAL transactions visible to bootstrap, so the next
            // incremental refresh resumes from there.
            TableToken baseToken = instance.getDefinition().getBaseTableToken();
            long retentionMicros = instance.getDefinition().getRetentionMicros();
            long lagMicros = instance.getDefinition().getLagMicros();
            long lowerBound;
            long bootstrapSeqTxn;
            long watermark;

            try (TableReader reader = engine.getReader(baseToken)) {
                engine.detachReader(reader);
                try {
                    executionContext.of(reader);
                    try {
                        long maxTs = reader.getMaxTimestamp();
                        lowerBound = Long.MIN_VALUE;
                        if (maxTs != Long.MIN_VALUE) {
                            lowerBound = maxTs - retentionMicros;
                            if (lowerBoundOverride != Long.MAX_VALUE && lowerBoundOverride < lowerBound) {
                                lowerBound = lowerBoundOverride;
                            }
                        }
                        bootstrapSeqTxn = reader.getSeqTxn();

                        // The cursor's strict-greater-than semantics translate to a
                        // BETWEEN whose inclusive lo is shifted by one timestamp unit.
                        // timestampHi is pinned to MAX so the upper end of the scan is
                        // unbounded.
                        int timestampType = baseMetadata.getTimestampType();
                        executionContext.setRange(lowerBound + 1, Long.MAX_VALUE, timestampType);

                        RecordCursor baseCursor = baseFactory.getCursor(executionContext);
                        try {
                            Record record = baseCursor.getRecord();
                            while (baseCursor.hasNext()) {
                                mergeBuffer.addRow(record);
                            }
                        } finally {
                            baseCursor.close();
                        }
                    } finally {
                        executionContext.clearReader();
                    }
                } finally {
                    engine.attachReader(reader);
                }
            }

            watermark = forceDrain ? mergeBuffer.getMaxTimestampSeen() : mergeBuffer.getMaxTimestampSeen() - lagMicros;

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
            writeBuffer.applyRetention(retentionMicros);
            mergeBuffer.applyRetention(retentionMicros);
            mergeBuffer.compactIfNeeded();
            // Phase 5 partition-state eviction is deliberately not run here. Bootstrap
            // has just (re)populated the accumulator from scratch over the backfill
            // window, so evicting immediately would discard work we just paid for. The
            // next drainAndCommit cycle runs eviction once new hot-path rows have made
            // stale keys identifiable.
            instance.publishWriteBuffer();
            // State now covers rows with ts > lowerBound (bootstrap's bounded backfill
            // is strict-greater-than). drainAndCommit uses this to decide whether a
            // subsequent warm path can replay from the merge buffer alone or must
            // route back through disk-read replay.
            instance.setStateBackfillTimestamp(lowerBound);
            // Advance the high watermark even when the inbound notification carried
            // seqTxn=-1 (initial flush triggered by forceFlushAllViews). Without this,
            // the next refresh re-enters the bootstrap branch and calls
            // resetWindowFunctions(), clearing window state.
            if (bootstrapSeqTxn > instance.getLastProcessedSeqTxn()) {
                instance.setLastProcessedSeqTxn(bootstrapSeqTxn);
            }
        } catch (Throwable t) {
            instance.abortWriteBuffer(writeBuffer);
            throw t;
        }
    }

    /**
     * Maps each SQL output column to its writer-index slot in the base table's WAL
     * segments. The SQL cursor's {@link RecordMetadata} carries column names and types
     * but not writer indexes (they default to -1 for non-table-reader metadata), so
     * we resolve names through the engine's metadata cache. Live views invalidate on
     * any base-table schema change, so this mapping is stable for the lifetime of one
     * incremental refresh.
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

    /**
     * Computes the ts below which an incoming WAL row is cold for an any-unbounded
     * view and must divert to disk-read replay. The cutoff is
     * {@code mergeBuffer.maxTimestampSeen - retention} — rows older than this have already
     * fallen outside what the merge buffer can carry across a warm-path replay, so
     * for a view with at least one unbounded window function they cannot be routed
     * through the normal merge-buffer path without corrupting the accumulator.
     * Returns {@code Long.MIN_VALUE} when:
     * <ul>
     *     <li>the view is all-bounded — the regular {@link #computeColdCutoff}
     *         drives the skip path in that case;</li>
     *     <li>the merge buffer has never seen a row (empty) — there is no meaningful
     *         coverage boundary yet;</li>
     *     <li>the subtraction would underflow a signed long.</li>
     * </ul>
     */
    private long computeAnyUnboundedColdCutoff(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            MergeBuffer mergeBuffer
    ) {
        if (windowFactory.getMaxLookbackMicros() >= 0) {
            return Long.MIN_VALUE;
        }
        long maxTimestampSeen = mergeBuffer.getMaxTimestampSeen();
        if (maxTimestampSeen == Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        }
        long retentionMicros = instance.getDefinition().getRetentionMicros();
        if (retentionMicros <= 0 || maxTimestampSeen < Long.MIN_VALUE + retentionMicros) {
            return Long.MIN_VALUE;
        }
        return maxTimestampSeen - retentionMicros;
    }

    /**
     * Computes the ts below which an incoming WAL row is cold relative to the
     * live view's currently-published output. Returns {@code Long.MIN_VALUE} to
     * disable skipping when:
     * <ul>
     *     <li>any window function reports an unbounded (or ts-inexpressible)
     *         lookback — a cold row cannot be safely skipped because it would
     *         change the accumulator; the disk-read replay path
     *         ({@link #computeAnyUnboundedColdCutoff}) handles those views;</li>
     *     <li>the published buffer is empty — no visible rows to protect, so
     *         the row must flow through the normal hot/warm path;</li>
     *     <li>the subtraction would underflow a signed long.</li>
     * </ul>
     */
    private long computeColdCutoff(LiveViewInstance instance, WindowRecordCursorFactory windowFactory) {
        long lookback = windowFactory.getMaxLookbackMicros();
        if (lookback < 0) {
            return Long.MIN_VALUE;
        }
        InMemoryTable published = instance.peekPublishedBuffer();
        if (published == null || published.getRowCount() == 0) {
            return Long.MIN_VALUE;
        }
        long oldestVisibleTs = published.getTimestampAt(0);
        if (oldestVisibleTs == Long.MIN_VALUE || oldestVisibleTs < Long.MIN_VALUE + lookback) {
            return Long.MIN_VALUE;
        }
        return oldestVisibleTs - lookback;
    }

    /**
     * Computes the drain watermark, drains the merge buffer, and feeds the drained rows
     * through the window cursor into the InMemoryTable. No-op when the buffer is empty.
     * When {@code forceDrain} is true, the watermark is the max observed timestamp so
     * every buffered row is emitted — used for idle-timer flushes.
     * <p>
     * Hot path ({@code pendingLateCount == 0}): copy the published snapshot into the
     * write buffer and append the delta rows on top, reusing accumulated window state.
     * <p>
     * Warm path ({@code pendingLateCount > 0}): a row added since the last drain has
     * {@code ts <= lastDrainedWatermark}, so the window functions have already emitted
     * state for that time range out of order. Reset window state, clear the write
     * buffer, and re-emit every retained row in sort order through
     * {@link MergeBuffer#replay} so the accumulator rebuilds from the retained range.
     * <p>
     * Any-unbounded disk-read branch: when the view has at least one unbounded window
     * function and its {@code stateBackfillTimestamp} is older than the merge buffer's retention
     * coverage, warm-path replay from the merge buffer alone would shrink the
     * accumulator's coverage (merge buffer has already evicted rows the accumulator
     * still reflects). The warm path therefore routes back through
     * {@link #bootstrap} with the backfill timestamp as the lower-bound override,
     * re-reading the base table and rebuilding the accumulator over the wider window.
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
        if (needsDiskReadWarmPath(instance, windowFactory, mergeBuffer)) {
            LOG.info().$("live view warm-path disk-read replay [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", backfillTimestampMicros=").$(instance.getStateBackfillTimestamp())
                    .$(", maxTimestampSeenMicros=").$(mergeBuffer.getMaxTimestampSeen()).I$();
            bootstrap(instance, forceDrain, instance.getStateBackfillTimestamp());
            return;
        }
        InMemoryTable writeBuffer = instance.tryAcquireWriteBuffer();
        if (writeBuffer == null) {
            LOG.debug().$("live view incremental refresh deferred, write buffer pinned by readers [view=")
                    .$(instance.getDefinition().getViewName()).I$();
            return;
        }

        try {
            long lagMicros = instance.getDefinition().getLagMicros();
            long retentionMicros = instance.getDefinition().getRetentionMicros();
            long maxTimestampSeen = mergeBuffer.getMaxTimestampSeen();
            long hotWatermark = forceDrain ? maxTimestampSeen : maxTimestampSeen - lagMicros;
            boolean isWarmPath = mergeBuffer.getPendingLateCount() > 0;
            // Warm path rebuilds the write buffer from scratch; it must emit at least
            // every row previously emitted (up to lastDrainedWatermark) so that rows
            // that slipped past LAG on an earlier force-flush still appear. Otherwise
            // the rebuild would silently drop them.
            long watermark = isWarmPath
                    ? Math.max(hotWatermark, mergeBuffer.getLastDrainedWatermark())
                    : hotWatermark;

            RecordCursor drainCursor;
            if (isWarmPath) {
                windowFactory.resetWindowFunctions();
                writeBuffer.clear();
                drainCursor = mergeBuffer.replay(watermark);
            } else {
                // Sync the write buffer from the currently-published state; appending the
                // delta on top yields the next publishable snapshot.
                writeBuffer.copyFrom(instance.peekPublishedBuffer());
                drainCursor = mergeBuffer.drain(watermark);
            }
            RecordCursor windowCursor;
            try {
                windowCursor = isWarmPath
                        ? windowFactory.getBootstrapCursor(drainCursor, executionContext)
                        : windowFactory.getIncrementalCursor(drainCursor, executionContext);
            } catch (Throwable t) {
                Misc.free(drainCursor);
                throw t;
            }
            try {
                Record record = windowCursor.getRecord();
                while (windowCursor.hasNext()) {
                    writeBuffer.appendRow(record);
                }
            } finally {
                windowCursor.close();
            }

            writeBuffer.applyRetention(retentionMicros);
            mergeBuffer.applyRetention(retentionMicros);
            mergeBuffer.compactIfNeeded();
            evictStalePartitionState(windowFactory, mergeBuffer, retentionMicros);
            instance.publishWriteBuffer();
        } catch (Throwable t) {
            instance.abortWriteBuffer(writeBuffer);
            throw t;
        }
    }

    private RecordCursorFactory ensureCompiledFactory(LiveViewInstance instance) throws SqlException {
        RecordCursorFactory factory = instance.getCompiledFactory();
        if (factory == null) {
            // The compile path needs a base table reader pinned on the execution
            // context so the BETWEEN intrinsic injected by overrideWhereIntrinsics
            // resolves against the right TableToken. Compile is one-time per view,
            // so just own a reader locally when the context has none. Callers that
            // already pinned one (none today) are honored via the hasReader gate.
            TableToken baseToken = instance.getDefinition().getBaseTableToken();
            boolean ownReader = !executionContext.hasReader();
            TableReader localReader = ownReader ? engine.getReader(baseToken) : null;
            try {
                if (ownReader) {
                    engine.detachReader(localReader);
                    executionContext.of(localReader);
                }
                // Flags the compile so window function factories can tell they are
                // being compiled inside a live view and opt into live-view-only
                // machinery (e.g. the lastActivityTs value-layout slot that drives
                // Phase 5 partition-state eviction). Regular queries leave the flag
                // clear and skip the extra slot.
                executionContext.setLiveViewCompile(true);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    CompiledQuery cq = compiler.compile(instance.getDefinition().getViewSql(), executionContext);
                    factory = cq.getRecordCursorFactory();
                } finally {
                    executionContext.setLiveViewCompile(false);
                }
                instance.setCompiledFactory(factory);
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

    private MergeBuffer ensureMergeBuffer(LiveViewInstance instance, RecordMetadata baseMetadata) {
        MergeBuffer mergeBuffer = instance.getMergeBuffer();
        if (mergeBuffer == null) {
            mergeBuffer = new MergeBuffer(baseMetadata);
            instance.setMergeBuffer(mergeBuffer);
        }
        return mergeBuffer;
    }

    /**
     * Drives live view Phase 5 partition-state eviction. Sheds partition-keyed
     * accumulator entries whose last-seen row timestamp has fallen below the
     * retention cutoff ({@code maxTimestampSeen - retentionMicros}), so long-lived views
     * with churning keys do not accumulate unbounded state.
     * <p>
     * Called only from the hot/warm path in {@link #drainAndCommit} after
     * {@code applyRetention}. The disk-read warm-path branch returns early from
     * {@link #drainAndCommit} before reaching this site, so an accumulator
     * rebuilt via cold-path replay is not immediately evicted. Eviction does
     * still fire on subsequent hot-path refreshes, which means the expanded
     * backfill coverage can be partially re-discarded over time; the deferred
     * "cap backfill expansion" policy is the long-term fix. For the V1
     * any-unbounded views in scope today, this trade-off matches the bounded-
     * backfill semantics: once a partition key has no rows in the retained
     * window, its accumulator state is treated as stale.
     */
    private void evictStalePartitionState(
            WindowRecordCursorFactory windowFactory,
            MergeBuffer mergeBuffer,
            long retentionMicros
    ) {
        long maxTimestampSeen = mergeBuffer.getMaxTimestampSeen();
        if (maxTimestampSeen == Long.MIN_VALUE || retentionMicros <= 0) {
            return;
        }
        if (maxTimestampSeen < Long.MIN_VALUE + retentionMicros) {
            return;
        }
        windowFactory.evictStalePartitionState(maxTimestampSeen - retentionMicros);
    }

    /**
     * Resets window state and re-bootstraps from the base table. Called when
     * a non-DATA WAL event (TRUNCATE, SQL, schema change) makes incremental
     * refresh impossible.
     */
    private void fullRecompute(LiveViewInstance instance, boolean forceDrain) throws SqlException {
        bootstrap(instance, forceDrain, Long.MAX_VALUE);
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

        buildColumnMappings(baseMetadata, baseToken);
        MergeBuffer mergeBuffer = ensureMergeBuffer(instance, baseMetadata);

        // Cold-path skip threshold: rows with ts below this cutoff cannot affect any
        // visible output and are dropped before entering the merge buffer. The cutoff
        // is {@code Long.MIN_VALUE} (skip disabled) when the view has any unbounded
        // window function (max lookback < 0), when the published buffer is empty
        // (nothing visible to protect), or when the subtraction would underflow.
        final int baseTimestampIndex = baseMetadata.getTimestampIndex();
        final long coldCutoff = computeColdCutoff(instance, windowFactory);
        // Any-unbounded cold threshold: rows older than the merge buffer's retention
        // coverage cannot pass through merge-buffer replay (they would be evicted
        // before a warm path could process them) and, because at least one window
        // function is unbounded, changing them silently corrupts the accumulator. A
        // row below this threshold is diverted: it skips the merge buffer and
        // triggers a disk-read replay after the WAL loop finishes. {@code
        // Long.MIN_VALUE} means the check is off (merge buffer is empty or the view
        // is all-bounded).
        final long anyUnboundedColdCutoff = computeAnyUnboundedColdCutoff(instance, windowFactory, mergeBuffer);
        long skippedColdRows = 0;
        long batchMinColdTimestamp = Long.MAX_VALUE;

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
                    // dataInfo doubles as the txn's SymbolMapDiffCursor. The cursor
                    // consumes it into a per-column overlay so SYMBOL resolution uses
                    // this transaction's diff entries rather than the WalReader's
                    // cumulative (and potentially collision-overwritten) symbol map.
                    frameCursor.of(baseToken, walNameSink, segmentId, endRow, startRow, endRow, baseMetadata, dataInfo);
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
                        long ts = record.getTimestamp(baseTimestampIndex);
                        if (coldCutoff != Long.MIN_VALUE && ts < coldCutoff) {
                            skippedColdRows++;
                            continue;
                        }
                        if (anyUnboundedColdCutoff != Long.MIN_VALUE && ts < anyUnboundedColdCutoff) {
                            if (ts < batchMinColdTimestamp) {
                                batchMinColdTimestamp = ts;
                            }
                            continue;
                        }
                        mergeBuffer.addRow(record);
                    }
                }
            }
        } finally {
            Misc.free(frameCursor);
        }

        if (skippedColdRows > 0) {
            instance.addColdRowSkips(skippedColdRows);
            LOG.info().$("live view cold-path skipped rows [view=").$(instance.getDefinition().getViewName())
                    .$(", count=").$(skippedColdRows)
                    .$(", coldCutoffMicros=").$(coldCutoff).I$();
        }

        if (batchMinColdTimestamp != Long.MAX_VALUE) {
            // Any-unbounded view saw a row below merge-buffer retention. Route the
            // refresh through disk-read replay: bootstrap re-reads the base table
            // over (override, maxTs], rebuilds the accumulator so it includes the
            // cold row, and publishes a fresh snapshot. The diverted cold row is
            // already durable in the base table via WAL apply, so the disk scan
            // picks it up without the merge buffer having to carry it. Bootstrap
            // installs the effective lower bound on {@code stateBackfillTimestamp} only
            // after it succeeds, so a mid-way failure leaves the previous value intact.
            // <p>
            // {@code - 1}: bootstrap's bounded-backfill predicate is {@code ts >
            // lowerBound}, so passing {@code batchMinColdTimestamp - 1} makes the cold row
            // at exactly {@code batchMinColdTimestamp} pass the filter.
            long newBackfillTimestamp = Math.min(instance.getStateBackfillTimestamp(), batchMinColdTimestamp - 1);
            LOG.info().$("live view cold-path disk-read replay [view=")
                    .$(instance.getDefinition().getViewName())
                    .$(", minColdTimestampMicros=").$(batchMinColdTimestamp)
                    .$(", newBackfillTimestampMicros=").$(newBackfillTimestamp).I$();
            bootstrap(instance, forceDrain, newBackfillTimestamp);
            return;
        }

        drainAndCommit(instance, windowFactory, mergeBuffer, forceDrain);
    }

    /**
     * Reports whether the upcoming commit must route through
     * {@link #bootstrap}'s disk-read replay rather than a merge-buffer replay. Fires
     * only when the view has at least one unbounded window function, a warm path is
     * pending, and {@code stateBackfillTimestamp} sits older than the merge buffer's retention
     * coverage — at which point replaying the merge buffer alone would shrink the
     * accumulator by discarding rows the state still reflects.
     */
    private boolean needsDiskReadWarmPath(
            LiveViewInstance instance,
            WindowRecordCursorFactory windowFactory,
            MergeBuffer mergeBuffer
    ) {
        if (mergeBuffer.getPendingLateCount() == 0) {
            return false;
        }
        if (windowFactory.getMaxLookbackMicros() >= 0) {
            return false;
        }
        long backfillTimestamp = instance.getStateBackfillTimestamp();
        if (backfillTimestamp == Long.MAX_VALUE) {
            return false;
        }
        long maxTimestampSeen = mergeBuffer.getMaxTimestampSeen();
        if (maxTimestampSeen == Long.MIN_VALUE) {
            return false;
        }
        long retentionMicros = instance.getDefinition().getRetentionMicros();
        if (retentionMicros <= 0 || maxTimestampSeen < Long.MIN_VALUE + retentionMicros) {
            return false;
        }
        return backfillTimestamp < maxTimestampSeen - retentionMicros;
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
                    bootstrap(instance, forceDrain, Long.MAX_VALUE);
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
                // <p>
                // Compare against the current lastProcessedSeqTxn rather than the captured
                // {@code lastSeqTxn}: the disk-read replay path invokes {@link #bootstrap}
                // from within {@link #incrementalRefresh0}, which advances
                // lastProcessedSeqTxn to the reader's seqTxn (often past {@code seqTxn}).
                // A blind overwrite would downgrade it and force the next refresh to
                // re-read WAL segments already absorbed by the disk scan — for any-unbounded
                // views, the same cold row would trigger another disk-read replay.
                if (seqTxn > instance.getLastProcessedSeqTxn() && !(bootstrapAttempted && instance.isPendingRefresh())) {
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
