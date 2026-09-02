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

package io.questdb.cairo.wal;

import io.questdb.Metrics;
import io.questdb.Telemetry;
import io.questdb.TelemetryEvent;
import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DdlListener;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.cairo.wal.seq.MetadataServiceStub;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.SymbolMapWriterLite;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.AtomicIntList;
import io.questdb.std.BinarySequence;
import io.questdb.std.BoolList;
import io.questdb.std.Chars;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Utf8StringIntHashMap;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.SingleCharCharSequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryWalTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.cairo.wal.seq.TableSequencer.NO_TXN;

public class WalWriter extends WalWriterBase implements TableWriterAPI {
    private static final long COLUMN_DELETED_NULL_FLAG = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final int MEM_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;
    private static final Runnable NOOP = () -> {
    };
    // Number of empty seed transactions ALTER TABLE ... REBASE WAL commits on the new table. Coupled to
    // the replication uploader's rebase_new path, which skips seqTxn 1 and starts at seqTxn 2: keep this
    // >= 2 so an idle rebased table still has a seqTxn 2 to settle on instead of busy-spinning the
    // uploader. See commitRebaseSeed().
    private static final int REBASE_SEED_TXN_COUNT = 2;

    private final AlterOperation alterOp = new AlterOperation();
    private final ObjList<MemoryMA> columns;
    private final int columnsMadviseMode;
    private final DdlListener ddlListener;
    private final AtomicIntList initialSymbolCounts;
    private final IntList localSymbolIds;
    private final MetadataValidatorService metaValidatorSvc = new MetadataValidatorService();
    private final MetadataService metaWriterSvc = new MetadataWriterService();
    private final WalWriterMetadata metadata;
    // The per-table SeqTxnTracker, cached once (stable per table). Carries the table's EFFECTIVE commit
    // mode, read live on each commit via walCommitMode() so an ALTER ... SET PARAM commit_mode that
    // republishes the tracker is picked up without reopening this WAL writer. See Deferred 1.
    private final io.questdb.cairo.wal.seq.SeqTxnTracker seqTxnTracker;
    private final Metrics metrics;
    private final ObjList<Runnable> nullSetters;
    private final RecentWriteTracker recentWriteTracker;
    private final RowImpl row = new RowImpl();
    private final LongList rowValueIsNotNull = new LongList();
    private final BoolList symbolMapNullFlags = new BoolList();
    private final BoolList symbolMapNullFlagsChanged = new BoolList();
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final ObjList<DirectCharSequenceIntHashMap> symbolMaps = new ObjList<>();
    private final Telemetry<TelemetryWalTask> telemetryWal;
    private final TimestampDriver timestampDriver;
    private final int timestampIndex;
    private final ObjList<Utf8StringIntHashMap> utf8SymbolMaps = new ObjList<>();
    private final Uuid uuid = new Uuid();
    private final boolean walTelemetryEnabled;
    private long avgRecordSize;
    private SegmentColumnRollSink columnConversionSink;
    private int columnCount;
    private ColumnVersionReader columnVersionReader;
    private WalColumnarRowAppender columnarAppender;
    private ConversionSymbolMapWriter conversionSymbolMap;
    private ConversionSymbolTable conversionSymbolTable;
    private long currentTxnStartRowNum = -1;
    // --- adaptive group commit (Deferred 2) ---
    // The highest seqTxn whose WAL commit was SEQUENCED + msync'd (page-cache, ordered) but whose batched
    // device flush (fdatasync data→events→seq) has NOT yet been performed under W>0. -1 == nothing pending.
    // Guarded by `this` (the writer monitor) together with pendingSinceMicros: written by the committing
    // thread, read+cleared by both the committing thread (commit-driven flush) and the background flusher.
    private long pendingDurableSeqTxn = -1L;
    // Microsecond wall-clock of the OLDEST un-flushed pending commit (when pendingDurableSeqTxn was first
    // set after a flush). The flush trigger fires once now - pendingSinceMicros >= W. Guarded by `this`.
    private long pendingSinceMicros = -1L;
    // The OLDEST un-flushed seqTxn of the current group-commit batch (the first commit after a flush), also
    // registered on the shared SeqTxnTracker as this writer's contiguous-prefix pin (registerWriterPending).
    // -1 == nothing pending. Guarded by `this`. Concurrent writers of one table flush independently, so the
    // shared durable-ack frontier must only advance to min(oldest-un-flushed) across writers, not to this
    // writer's own flushed seqTxn (CRITICAL 2).
    private long pendingLoSeqTxn = -1L;
    private boolean isCommittingData;
    private byte lastDedupMode = WAL_DEDUP_MODE_DEFAULT;
    private long lastMatViewPeriodHi = WAL_DEFAULT_LAST_PERIOD_HI;
    private long lastMatViewRefreshBaseTxn = WAL_DEFAULT_BASE_TABLE_TXN;
    private long lastMatViewRefreshTimestamp = WAL_DEFAULT_LAST_REFRESH_TIMESTAMP;
    private long lastReplaceRangeHiTs = 0;
    private long lastReplaceRangeLowTs = 0;
    private long lastTxnMaxTimestamp = -1;
    private byte lastTxnType = WalTxnType.DATA;
    private long segmentRowCount = -1;
    private long totalSegmentsRowCount;
    private long totalSegmentsSize;
    private TxReader txReader;
    private long txnMaxTimestamp = -1;
    private long txnMinTimestamp = Long.MAX_VALUE;
    private boolean txnOutOfOrder = false;

    public WalWriter(
            CairoConfiguration configuration,
            TableToken tableToken,
            TableSequencerAPI tableSequencerAPI,
            DdlListener ddlListener,
            WalDirectoryPolicy walDirectoryPolicy,
            WalLocker walLocker,
            RecentWriteTracker recentWriteTracker,
            Telemetry<TelemetryWalTask> telemetryWal
    ) {
        super(configuration, tableToken, tableSequencerAPI, walDirectoryPolicy, walLocker);

        LOG.info().$("open [table=").$(tableToken).I$();
        this.columnsMadviseMode = configuration.getWalWriterMadviseMode();
        this.ddlListener = ddlListener;
        this.recentWriteTracker = recentWriteTracker;
        this.telemetryWal = telemetryWal;
        this.metrics = configuration.getMetrics();
        this.walTelemetryEnabled = !configuration.getTelemetryConfiguration().getDisableCompletely();

        try {
            lockWal();
            mkWalDir();

            metadata = new WalWriterMetadata(ff);
            sequencer.getTableMetadata(tableToken, metadata);
            timestampDriver = ColumnType.getTimestampDriver(metadata.getTimestampType());
            this.tableToken = metadata.getTableToken();
            // Cache the per-table tracker; the effective commit mode is read live from it on each commit
            // (walCommitMode), so an ALTER ... SET PARAM commit_mode is picked up without reopening.
            this.seqTxnTracker = sequencer.getTxnTracker(this.tableToken);

            columnCount = metadata.getColumnCount();
            timestampIndex = metadata.getTimestampIndex();
            columns = new ObjList<>(columnCount * 2);
            nullSetters = new ObjList<>(columnCount);
            initialSymbolCounts = new AtomicIntList(columnCount);
            localSymbolIds = new IntList(columnCount);

            events.of(symbolMaps, initialSymbolCounts, symbolMapNullFlags, symbolMapNullFlagsChanged);

            configureColumns();
            openNewSegment();
            configureSymbolTable();
        } catch (Throwable e) {
            if (CairoException.isDataSyncFailure(e)) {
                distressed = true;
                dropPendingDurable();
                sequencer.handleDataSyncFailure(e);
            }
            doClose(false);
            throw e;
        }
    }

    @Override
    public void addColumn(@NotNull CharSequence columnName, int columnType, SecurityContext securityContext) {
        addColumn(
                columnName,
                columnType,
                configuration.getDefaultSymbolCapacity(),
                configuration.getDefaultSymbolCacheFlag(),
                IndexType.NONE,
                configuration.getIndexValueBlockSize(),
                false,
                securityContext
        );
    }

    @Override
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            byte indexType,
            int indexValueBlockCapacity,
            boolean isDedupKey,
            SecurityContext securityContext
    ) {
        alterOp.clear();
        alterOp.ofAddColumn(
                getMetadata().getTableId(),
                tableToken,
                0,
                columnName,
                0,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                indexType,
                indexValueBlockCapacity,
                isDedupKey
        );
        alterOp.withSecurityContext(securityContext);
        apply(alterOp, true);
    }

    public long appendCustomEvent(byte txnType, WalEventPayloadWriter payload) {
        if (!WalTxnType.isDownstreamType(txnType)) {
            throw new IllegalArgumentException(
                    "custom event types must be in reserved range 64..127, got: " + txnType
            );
        }
        try {
            // A custom event is an ordering barrier. Publish any rows appended before it as a
            // DATA transaction first; otherwise the custom event would receive the earlier
            // seqTxn and a later commit would make those rows overtake their call-site order.
            commit();
            lastSegmentTxn = events.appendCustomEvent(txnType, payload);
            return getSequencerTxn();
        } catch (Throwable th) {
            distressed = true;
            throw th;
        }
    }

    @Override
    public long apply(AlterOperation alterOp, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
        alterOp.authorize();
        // Deferred 2 (group commit, W>0): a metadata change must not let its sequencer record become durable
        // ahead of prior pending DATA commits — a structural change's endMetadataChangeEntry() does an
        // MS_SYNC (fullSync) of the sequencer txn log, which would device-flush the seq while the prior data
        // commits' columns are only MS_ASYNC'd (page-cache), a torn data→events→seq order on crash. Flush the
        // pending backlog FIRST so every prior commit is on disk before this txn sequences.
        flushPendingDurable();

        // The sequencer tracker is the durability-mode authority for every WAL writer of this table.
        // TableWriter applies this ALTER asynchronously, so waiting for setMetaCommitMode() there leaves a
        // window in which subsequently acknowledged WAL commits still use the old grade. Publish transitions
        // TO adaptive before sequencing (stronger durability is safe if sequencing later fails), and publish
        // transitions AWAY only after the ALTER is sequenced (never weaken commits that can precede it).
        final boolean commitModeChange = alterOp.getCommand() == AlterOperation.SET_PARAM_COMMIT_MODE;
        final int newEffectiveCommitMode = commitModeChange
                ? CommitMode.effectiveCommitMode(alterOp.getSetCommitModeValue(), configuration.getCommitMode())
                : CommitMode.UNSET;
        if (commitModeChange && newEffectiveCommitMode == CommitMode.ADAPTIVE) {
            seqTxnTracker.strengthenCommitModeToAdaptive();
        }

        final long seqTxn;
        if (alterOp.isStructural()) {
            seqTxn = applyStructural(alterOp);
        } else {
            seqTxn = applyNonStructural(alterOp, false);
        }
        if (commitModeChange && newEffectiveCommitMode == CommitMode.ADAPTIVE) {
            seqTxnTracker.setCommitModeAtSeqTxn(newEffectiveCommitMode, seqTxn);
        }
        return seqTxn;
    }

    // Returns table transaction number
    @Override
    public long apply(UpdateOperation operation) {
        operation.authorize();
        if (inTransaction()) {
            throw CairoException.critical(0).put("cannot update table with uncommitted inserts [table=")
                    .put(tableToken.getTableName()).put(']');
        }
        // Deferred 2 (group commit, W>0): flush prior pending data commits before sequencing this update,
        // for the same data→events→seq ordering reason as in apply(AlterOperation).
        flushPendingDurable();

        // it is guaranteed that there is no join in UPDATE statement
        // because SqlCompiler rejects the UPDATE if it contains join
        return applyNonStructural(operation, true);

        // when join is allowed in UPDATE we have 2 options
        // 1. we could write the updated partitions into WAL.
        //   since we cannot really rely on row ids we should probably create
        //   a PARTITION_REWRITE event and use it here to replace the updated
        //   partitions entirely with new ones.
        // 2. we could still pass the SQL statement if we made sure that all
        //   tables involved in the join are guaranteed to be on the same
        //   version (exact same txn number) on each node when the update
        //   statement is run.
        //   so we would need to read current txn number for each table and
        //   put it into the SQL event as a requirement for running the SQL.
        //   when the WAL event is processed we would need to query the exact
        //   versions of each table involved in the join when running the SQL.
    }

    @Override
    public void close() {
        if (isOpen()) {
            try {
                cleanupBeforeClose();
            } finally {
                doClose(walDirectoryPolicy.truncateFilesOnClose());
            }
        }
    }

    @Override
    public void commit() {
        // plain old commit
        commit0(
                WalTxnType.DATA,
                WAL_DEFAULT_BASE_TABLE_TXN,
                WAL_DEFAULT_LAST_REFRESH_TIMESTAMP,
                WAL_DEFAULT_LAST_PERIOD_HI,
                0,
                0,
                WAL_DEDUP_MODE_DEFAULT
        );
    }

    /**
     * Commit the materialized view to update the last refresh timestamp.
     * Called as the last transaction of a materialized view refresh.
     *
     * @param lastRefreshBaseTxn    the base table seqTxn the mat view is refreshed at
     * @param lastRefreshTimestamp  the wall clock timestamp when the refresh is done
     * @param lastPeriodHi          the period high boundary timestamp the mat view is refreshed at
     * @param lastReplaceRangeLowTs the low timestamp of the range to be replaced, inclusive
     * @param lastReplaceRangeHiTs  the high timestamp of the range to be replaced, exclusive
     */
    public void commitMatView(long lastRefreshBaseTxn, long lastRefreshTimestamp, long lastPeriodHi, long lastReplaceRangeLowTs, long lastReplaceRangeHiTs) {
        assert lastReplaceRangeLowTs < lastReplaceRangeHiTs;
        assert txnMinTimestamp >= lastReplaceRangeLowTs;
        assert txnMaxTimestamp <= lastReplaceRangeHiTs;
        commit0(
                WalTxnType.MAT_VIEW_DATA,
                lastRefreshBaseTxn,
                lastRefreshTimestamp,
                lastPeriodHi,
                lastReplaceRangeLowTs,
                lastReplaceRangeHiTs,
                WAL_DEDUP_MODE_REPLACE_RANGE
        );
    }

    /**
     * Commits the live view's WAL block with the highest base sequencer txn whose
     * rows the block reflects. {@code ApplyWal2TableJob} uses this value to advance
     * {@code lvConsumedSeqTxn} only when the block has been applied to the live view's
     * own table, satisfying the "applied to the LV's own on-disk tier" rule for
     * base-WAL retention.
     */
    public void commitLiveView(long maxBaseSeqTxnInBlock) {
        commit0(
                WalTxnType.LIVE_VIEW_DATA,
                maxBaseSeqTxnInBlock,
                WAL_DEFAULT_LAST_REFRESH_TIMESTAMP,
                WAL_DEFAULT_LAST_PERIOD_HI,
                0,
                0,
                WAL_DEDUP_MODE_DEFAULT
        );
    }

    /**
     * Commits a live view's WAL block that replaces the live view's previously
     * applied output rows in the {@code [lowTs, hiTs)} timestamp range with the
     * rows just emitted into this transaction. Used by the refresh worker's
     * O3-replay path: after restoring window state (head-hit) or resetting it
     * (head-miss), the worker re-feeds base rows in ts order and emits replay
     * output as a single REPLACE_RANGE commit, so {@code TableWriter}'s apply
     * step rewrites the affected partitions transactionally.
     *
     * @param maxBaseSeqTxnInBlock highest base sequencer txn whose rows this
     *                             block reflects; advances {@code lvConsumedSeqTxn}
     *                             only after the block is applied (same rule as
     *                             {@link #commitLiveView(long)}).
     * @param lowTs                inclusive low boundary of the replaced range
     * @param hiTs                 exclusive high boundary of the replaced range;
     *                             must be strictly greater than {@code lowTs} and
     *                             cover every {@code (ts, ...)} row written in
     *                             the current transaction.
     */
    public void commitLiveViewWithReplaceRange(long maxBaseSeqTxnInBlock, long lowTs, long hiTs) {
        assert lowTs < hiTs;
        assert txnMinTimestamp >= lowTs;
        assert txnMaxTimestamp <= hiTs;
        commit0(
                WalTxnType.LIVE_VIEW_DATA,
                maxBaseSeqTxnInBlock,
                WAL_DEFAULT_LAST_REFRESH_TIMESTAMP,
                WAL_DEFAULT_LAST_PERIOD_HI,
                lowTs,
                hiTs,
                WAL_DEDUP_MODE_REPLACE_RANGE
        );
    }

    public void commitWithParams(long replaceRangeLowTs, long replaceRangeHiTs, byte dedupMode) {
        commit0(
                WalTxnType.DATA,
                WAL_DEFAULT_BASE_TABLE_TXN,
                WAL_DEFAULT_LAST_REFRESH_TIMESTAMP,
                WAL_DEFAULT_LAST_PERIOD_HI,
                replaceRangeLowTs,
                replaceRangeHiTs,
                dedupMode
        );
    }

    /**
     * Returns a columnar row appender for bulk column-oriented writes.
     * <p>
     * The columnar appender provides an alternative to the row-by-row API,
     * allowing entire columns to be written at once for better performance
     * when ingesting columnar data (like QWP v1).
     *
     * @return the columnar row appender for this writer
     */
    public ColumnarRowAppender getColumnarRowAppender() {
        if (columnarAppender == null) {
            columnarAppender = new WalColumnarRowAppender(this);
        }
        return columnarAppender;
    }

    /**
     * Returns the data memory for a column. Used by columnar appender.
     */
    MemoryMA getDataColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getDataColumnOffset(column));
    }

    @Override
    public TableRecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public long getMetadataVersion() {
        return metadata.getMetadataVersion();
    }

    /**
     * Returns the current row count in this segment.
     */
    public long getSegmentRowCount() {
        return segmentRowCount;
    }

    @Override
    public int getSymbolCountWatermark(int columnIndex) {
        // It could be the case that ILP I/O thread has newer metadata version than
        // the writer, so it may be requesting a watermark for a recently added column.
        if (columnIndex > initialSymbolCounts.size() - 1) {
            return 0;
        }
        return initialSymbolCounts.get(columnIndex);
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    @Override
    public long getUncommittedRowCount() {
        return segmentRowCount - currentTxnStartRowNum;
    }

    public void goActive() {
        goActive(Long.MAX_VALUE);
    }

    public void goActive(long maxStructureVersion) {
        try {
            applyMetadataChangeLog(maxStructureVersion);
        } catch (CairoException e) {
            distressed = true;
            if (e.isTableDropped()) {
                // Throw table dropped exception as is
                throw e;
            }
            LOG.critical().$("could not apply structure changes, WAL will be closed [table=").$(tableToken)
                    .$(", walId=").$(walId)
                    .$(", ex=").$((Throwable) e)
                    .$(", errno=").$(e.getErrno())
                    .I$();
            throw e;
        }
    }

    @Override
    public void ic() {
        commit();
    }

    @Override
    public void ic(long o3MaxLag) {
        commit();
    }

    @Override
    public TableWriter.Row newRow() {
        return newRow(0L);
    }

    @Override
    public TableWriter.Row newRow(long timestamp) {
        checkDistressed();
        if (isInColumnarWrite()) {
            throw CairoException.nonCritical().put("cannot use row-oriented newRow() during columnar write");
        }
        timestampDriver.validateBounds(timestamp);
        try {
            if (rollSegmentOnNextRow) {
                rollSegment();
                rollSegmentOnNextRow = false;
            }
            if (timestampIndex != -1) {
                row.setTimestamp(timestamp);
            }
            return row;
        } catch (Throwable e) {
            distressed = true;
            throw e;
        }
    }

    public long renameTable(@NotNull CharSequence oldName, String newTableName, SecurityContext securityContext) {
        if (!Chars.equalsIgnoreCaseNc(oldName, tableToken.getTableName())) {
            throw CairoException.tableDoesNotExist(oldName);
        }
        alterOp.clear();
        alterOp.ofRenameTable(tableToken, newTableName);
        alterOp.withSecurityContext(securityContext);
        long txn = apply(alterOp, true);
        assert Chars.equals(newTableName, tableToken.getTableName());
        return txn;
    }

    // Marks the materialized view as invalid or resets its invalidation status,
    // depending on the input values.
    public void resetMatViewState(
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn
    ) {
        try {
            lastSegmentTxn = events.appendMatViewInvalidate(
                    lastRefreshBaseTxn,
                    lastRefreshTimestamp,
                    invalid,
                    invalidationReason,
                    lastPeriodHi,
                    refreshIntervals,
                    refreshIntervalsBaseTxn
            );
            syncAdaptiveEventsBeforeSequencing();
            final long seqTxn = getSequencerTxn();
            // W>0: private events are already durable; record the deferred shared-sequencer barrier so the
            // next commit/background flusher bounds visibility to <= W even if commits stop.
            if (walCommitMode() == CommitMode.ADAPTIVE && deferDeviceFlush()) {
                recordPendingDurable(seqTxn);
            }
        } catch (Throwable th) {
            if (CairoException.isDataSyncFailure(th)) {
                distressed = true;
                dropPendingDurable();
                sequencer.handleDataSyncFailure(th);
            }
            rollback0();
            throw th;
        }
    }

    public void rollSegment() {
        try {
            openNewSegment();
        } catch (Throwable e) {
            distressed = true;
            if (CairoException.isDataSyncFailure(e)) {
                dropPendingDurable();
                sequencer.handleDataSyncFailure(e);
            }
            throw e;
        }
    }

    @Override
    public void rollback() {
        throwIfInColumnarWrite("rollback");
        rollback0();
    }

    protected final void cleanupBeforeClose() {
        // If distressed, no need to rollback, WalWriter will not be used any more.
        if (isDistressed()) {
            // A distressed writer is discarded; its un-flushed group-commit tail (if any) never advanced
            // localDurableSeqTxn, so no false durability is claimed. Clear the pending fields AND deregister
            // under the writer monitor BEFORE doClose closes any fd, so a background flusher that captured
            // this writer reference before the deregister (weakly-consistent iterator) cannot fdatasync a
            // closed fd (use-after-close). dropPendingDurable() restores the invariant on the distressed path.
            dropPendingDurable();
            return;
        }
        try {
            if (isInColumnarWrite()) {
                columnarAppender.cancelColumnarWrite();
            }
            // Deferred 2 (group commit): flush any pending device flush so a CLEAN handoff (pool return /
            // close) is durable — the next acquirer and any durable-ack consumer must see the frontier on
            // disk. Before rollback0(), which only rewinds UNCOMMITTED rows: the frontier being flushed
            // here is already committed, so the two are independent and this order keeps the durability
            // work ahead of the rewind.
            flushPendingDurable();
            rollback0();
        } catch (Throwable th) {
            // Latch so the expel path's second close attempt short-circuits on
            // the distressed check above instead of retrying the failed IO and
            // replacing the original exception. rollback0() already latches
            // its own failures; this extends the same guarantee to the
            // columnar cancel.
            distressed = true;
            // A flush failure here is a genuine durability fault, and distressing alone is not enough:
            // clear pending AND deregister under the writer monitor BEFORE doClose can close any fd, or a
            // background flusher holding this writer reference could fdatasync a closed fd.
            dropPendingDurable();
            throw th;
        }
    }

    private void rollback0() {
        try {
            if (!isDistressed() && (inTransaction() || hasDirtyColumns(currentTxnStartRowNum))) {
                setAppendPosition(currentTxnStartRowNum);
                segmentRowCount = currentTxnStartRowNum;
                txnMinTimestamp = Long.MAX_VALUE;
                txnMaxTimestamp = -1;
                txnOutOfOrder = false;
            }
        } catch (Throwable th) {
            // Set to dissatisfied state, otherwise the pool will keep trying to rollback until the stack overflow
            distressed = true;
            throw th;
        }
    }

    /**
     * Writes server-assigned timestamp for all rows (atNow case).
     * The designated timestamp uses 128-bit format: (timestamp, rowId) pairs.
     * All rows in the batch receive the same server timestamp so that the
     * caller's pre-captured min/max stays consistent with the written data.
     */
    public void putServerAssignedTimestampColumnar(int rowCount, long timestamp) {
        checkDistressed();
        assert isInColumnarWrite() : "putServerAssignedTimestampColumnar called outside columnar write";
        if (rowCount <= 0) {
            return;
        }
        MemoryMA dataMem = getDataColumn(timestampIndex);
        long startRowId = getSegmentRowCount();
        for (int row = 0; row < rowCount; row++) {
            dataMem.putLong128(timestamp, startRowId + row);
        }
        setRowValueNotNullColumnar(timestampIndex, startRowId + rowCount - 1);
    }

    @TestOnly
    public void setLegacyMatViewFormat(boolean legacyMatViewFormat) {
        events.setLegacyMatViewFormat(legacyMatViewFormat);
    }

    /**
     * Marks a column as having been written up to the specified row.
     * Used by columnar appender.
     */
    public void setRowValueNotNullColumnar(int columnIndex, long lastWrittenRow) {
        rowValueIsNotNull.setQuick(columnIndex, lastWrittenRow);
    }

    @TestOnly
    public void setSymbolMapReader(int columnIndex, SymbolMapReader symbolMapReader) {
        symbolMapReaders.setQuick(columnIndex, symbolMapReader);
    }

    /**
     * Validates that a designated timestamp value is within allowed bounds.
     * Used by columnar appender to match the validation in {@link #newRow(long)}.
     */
    void validateDesignatedTimestampBounds(long timestamp) {
        timestampDriver.validateBounds(timestamp);
    }

    @Override
    public boolean supportsMultipleWriters() {
        return true;
    }

    @Override
    public String toString() {
        return "WalWriter{" +
                "name=" + walName +
                ", table=" + tableToken.getTableName() +
                '}';
    }

    /**
     * Commits two empty (0-row) DATA transactions as the first transactions (seqTxn 1 and seqTxn 2) of a
     * table created by ALTER TABLE ... REBASE WAL, so real data starts at seqTxn 3. The replication
     * uploader skips seqTxn 1 and records seqTxn 2 as the table's first available txn (first_txn=2) in
     * the replication index, leaving the replica unable to apply onto the empty table until a physical
     * copy arrives.
     * <p>
     * Two seeds, not one: the uploader's rebase_new path starts at seqTxn 2, so the sequencer's max_txn
     * must be at least 2 the instant the rebase completes - otherwise a rebased table left idle (no data
     * written afterwards) would have max_txn=1 with nothing at seqTxn 2 to advance onto. The uploader
     * would then never record the table in the index and would busy-spin re-reading an empty txn range on
     * every poll (100% CPU, log/JNI flood) until data finally reaches seqTxn 2. The second empty seed
     * gives the uploader a no-op transaction to settle on (records first_txn=2, last_txn=2), so an idle
     * rebased table parks instead of spinning.
     */
    public void commitRebaseSeed() {
        try {
            // Each appendData + getSequencerTxn pair is one sequencer transaction (segment_txn 0 and 1 ->
            // seqTxn 1 and 2). Both seeds are identical 0-row commits, so no per-txn state needs resetting
            // in between (currentTxnStartRowNum and segmentRowCount both stay 0).
            for (int i = 0; i < REBASE_SEED_TXN_COUNT; i++) {
                lastSegmentTxn = events.appendData(
                        WalTxnType.DATA,
                        0,
                        0,
                        0,
                        0,
                        false,
                        WAL_DEFAULT_BASE_TABLE_TXN,
                        WAL_DEFAULT_LAST_REFRESH_TIMESTAMP,
                        WAL_DEFAULT_LAST_PERIOD_HI,
                        0,
                        0,
                        WAL_DEDUP_MODE_DEFAULT
                );
                syncAdaptiveEventsBeforeSequencing();
                final long seqTxn = getSequencerTxn();
                // W>0: carry the remaining shared-sequencer barrier in the batched flush, which
                // cleanupBeforeClose runs when this writer closes after the seeds.
                if (walCommitMode() == CommitMode.ADAPTIVE && deferDeviceFlush()) {
                    recordPendingDurable(seqTxn);
                }
            }
        } catch (Throwable th) {
            if (CairoException.isDataSyncFailure(th)) {
                distressed = true;
                dropPendingDurable();
                sequencer.handleDataSyncFailure(th);
            }
            rollback0();
            throw th;
        }
    }

    @Override
    public void truncate() {
        throw new UnsupportedOperationException("cannot truncate symbol tables on WAL table");
    }

    @Override
    public void truncateSoft() {
        try {
            lastSegmentTxn = events.truncate();
            syncAdaptiveEventsBeforeSequencing();
            final long seqTxn = getSequencerTxn();
            // W>0: private events are durable; record the remaining shared-sequencer barrier pending.
            if (walCommitMode() == CommitMode.ADAPTIVE && deferDeviceFlush()) {
                recordPendingDurable(seqTxn);
            }
        } catch (Throwable th) {
            if (CairoException.isDataSyncFailure(th)) {
                distressed = true;
                dropPendingDurable();
                sequencer.handleDataSyncFailure(th);
            }
            rollback0();
            throw th;
        }
    }

    private static void configureNullSetters(
            ObjList<Runnable> nullers,
            int type,
            MemoryMA dataMem,
            MemoryMA auxMem,
            int columnIndex,
            BoolList symbolMapNullFlagsChanged,
            BoolList symbolMapNullFlags
    ) {
        int columnTag = ColumnType.tagOf(type);
        if (ColumnType.isVarSize(columnTag)) {
            final ColumnTypeDriver typeDriver = ColumnType.getDriver(columnTag);
            nullers.add(() -> typeDriver.appendNull(auxMem, dataMem));
        } else {
            switch (columnTag) {
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    nullers.add(() -> dataMem.putByte((byte) 0));
                    break;
                case ColumnType.DOUBLE:
                    nullers.add(() -> dataMem.putDouble(Double.NaN));
                    break;
                case ColumnType.FLOAT:
                    nullers.add(() -> dataMem.putFloat(Float.NaN));
                    break;
                case ColumnType.INT:
                    nullers.add(() -> dataMem.putInt(Numbers.INT_NULL));
                    break;
                case ColumnType.IPv4:
                    nullers.add(() -> dataMem.putInt(Numbers.IPv4_NULL));
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                    nullers.add(() -> dataMem.putLong(Numbers.LONG_NULL));
                    break;
                case ColumnType.LONG256:
                    nullers.add(() -> dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL));
                    break;
                case ColumnType.SHORT:
                    nullers.add(() -> dataMem.putShort((short) 0));
                    break;
                case ColumnType.CHAR:
                    nullers.add(() -> dataMem.putChar((char) 0));
                    break;
                case ColumnType.SYMBOL:
                    nullers.add(() ->
                            {
                                dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                                if (!symbolMapNullFlags.get(columnIndex)) {
                                    symbolMapNullFlags.setQuick(columnIndex, true);
                                    symbolMapNullFlagsChanged.setQuick(columnIndex, true);
                                }
                            }
                    );
                    break;
                case ColumnType.GEOBYTE:
                    nullers.add(() -> dataMem.putByte(GeoHashes.BYTE_NULL));
                    break;
                case ColumnType.GEOSHORT:
                    nullers.add(() -> dataMem.putShort(GeoHashes.SHORT_NULL));
                    break;
                case ColumnType.GEOINT:
                    nullers.add(() -> dataMem.putInt(GeoHashes.INT_NULL));
                    break;
                case ColumnType.GEOLONG:
                    nullers.add(() -> dataMem.putLong(GeoHashes.NULL));
                    break;
                case ColumnType.LONG128:
                    // fall through
                case ColumnType.UUID:
                    nullers.add(() -> dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL));
                    break;
                case ColumnType.DECIMAL8:
                    nullers.add(() -> dataMem.putByte(Decimals.DECIMAL8_NULL));
                    break;
                case ColumnType.DECIMAL16:
                    nullers.add(() -> dataMem.putShort(Decimals.DECIMAL16_NULL));
                    break;
                case ColumnType.DECIMAL32:
                    nullers.add(() -> dataMem.putInt(Decimals.DECIMAL32_NULL));
                    break;
                case ColumnType.DECIMAL64:
                    nullers.add(() -> dataMem.putLong(Decimals.DECIMAL64_NULL));
                    break;
                case ColumnType.DECIMAL128:
                    nullers.add(() -> dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL));
                    break;
                case ColumnType.DECIMAL256:
                    nullers.add(() -> dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL));
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
            }
        }
    }

    private static void freeNullSetter(ObjList<Runnable> nullSetters, int columnIndex) {
        nullSetters.setQuick(columnIndex, NOOP);
    }

    private static int getAuxColumnOffset(int index) {
        return getDataColumnOffset(index) + 1;
    }

    private static int getDataColumnOffset(int columnIndex) {
        return columnIndex * 2;
    }

    private void applyMetadataChangeLog(long structureVersionHi) {
        try (TableMetadataChangeLog log = sequencer.getMetadataChangeLog(tableToken, getColumnStructureVersion())) {
            long structVer = getColumnStructureVersion();
            while (log.hasNext() && structVer < structureVersionHi) {
                TableMetadataChange chg = log.next();
                try {
                    chg.apply(metaWriterSvc, true);
                } catch (CairoException e) {
                    distressed = true;
                    throw e;
                }

                if (++structVer != getColumnStructureVersion()) {
                    distressed = true;
                    throw CairoException.critical(0)
                            .put("could not apply table definition changes to the current transaction, version unchanged");
                }
            }
        }
    }

    private void syncAdaptiveEventsBeforeSequencing() {
        final int commitMode = walCommitMode();
        if (commitMode == CommitMode.ADAPTIVE) {
            events.sync(commitMode);
            if (deferDeviceFlush()) {
                // Under W>0 sync() is MS_ASYNC. Private event/index/checksum dependencies still have to be
                // device-durable before any writer can flush the shared sequencer.
                events.barrierFsync();
            }
        }
    }

    private long applyNonStructural(AbstractOperation op, boolean verifyStructureVersion) {
        if (op.getSqlExecutionContext() == null) {
            throw CairoException.critical(0).put("failed to commit ALTER SQL to WAL, sql context is empty [table=").put(tableToken.getTableName()).put(']');
        }
        if (
                (verifyStructureVersion && op.getTableVersion() != getColumnStructureVersion())
                        || op.getTableId() != metadata.getTableId()) {
            throw TableReferenceOutOfDateException.of(tableToken, metadata.getTableId(), op.getTableId(), getColumnStructureVersion(), op.getTableVersion());
        }

        try {
            lastSegmentTxn = events.appendSql(op.getCmdType(), op.getSqlText(), op.getSqlExecutionContext());
            // Make THIS SQL txn's private event/index/checksum files durable BEFORE the shared sequencer
            // records it. This is required for both W=0 and W>0: another writer may fdatasync the shared
            // sequencer while this writer's batch is pending, so deferring private WAL durability would let
            // that peer publish a seqTxn whose _event.i entry can disappear on crash.
            final int commitMode = walCommitMode();
            if (commitMode == CommitMode.ADAPTIVE) {
                events.sync(commitMode);
                if (deferDeviceFlush()) {
                    // sync(ADAPTIVE) is MS_ASYNC when W>0; explicitly finish the private barrier now.
                    events.barrierFsync();
                }
            }
            final long seqTxn = getSequencerTxn();
            // Deferred 2 (group commit, W>0): mirror the commit0 durable-ack path. The callers
            // (apply(AlterOperation) and apply(UpdateOperation)) already flushed any prior pending DATA
            // commits before reaching here (preserving data→events→seq order for PRIOR data), so it is safe to
            // start a NEW pending batch for THIS SQL txn. Without this call, a SQL-only-then-idle table's
            // localDurableSeqTxn would never advance over the SQL txn under W>0 (durable-ack liveness gap); the
            // batched flush carries the SQL txn's shared sequencer barrier to durable within ≤W even when commits stop.
            if (walCommitMode() == CommitMode.ADAPTIVE && deferDeviceFlush()) {
                recordPendingDurable(seqTxn);
            }
            return seqTxn;
        } catch (Throwable th) {
            // perhaps half record was written to WAL-e, better to not use this WAL writer instance
            distressed = true;
            if (CairoException.isDataSyncFailure(th)) {
                dropPendingDurable();
                sequencer.handleDataSyncFailure(th);
            }
            throw th;
        }
    }

    private long applyStructural(AlterOperation alterOp) {
        long txn;
        do {
            boolean retry = true;
            try {
                metaValidatorSvc.startAlterValidation();
                alterOp.apply(metaValidatorSvc, true);
                if (metaValidatorSvc.structureVersion != getColumnStructureVersion() + 1) {
                    retry = false;
                    throw CairoException.nonCritical()
                            .put("statement is either no-op,")
                            .put(" or contains multiple transactions, such as 'alter table add column col1, col2',")
                            .put(" and currently not supported for WAL tables [table=").put(tableToken.getTableName())
                            .put(", oldStructureVersion=").put(getColumnStructureVersion())
                            .put(", newStructureVersion=").put(metaValidatorSvc.structureVersion).put(']');
                }
            } catch (CairoException e) {
                if (retry) {
                    // Table schema (metadata) changed and this Alter is not valid anymore.
                    // Try to update WAL metadata to latest and repeat one more time.
                    goActive();
                    alterOp.apply(metaValidatorSvc, true);
                } else {
                    throw e;
                }
            }

            try {
                txn = sequencer.nextStructureTxn(tableToken, getColumnStructureVersion(), alterOp);
                if (txn == NO_TXN) {
                    applyMetadataChangeLog(Long.MAX_VALUE);
                }
            } catch (CairoException e) {
                distressed = true;
                if (e.isDataSyncFailure()) {
                    dropPendingDurable();
                    sequencer.handleDataSyncFailure(e);
                }
                throw e;
            }
        } while (txn == NO_TXN);

        // Apply to itself.
        try {
            alterOp.apply(metaWriterSvc, true);
            LOG.info().$("committed structural metadata change [wal=").$(path).$(Files.SEPARATOR).$(segmentId)
                    .$(", segmentTxn=").$(lastSegmentTxn)
                    .$(", seqTxn=").$(txn)
                    .I$();
        } catch (Throwable th) {
            LOG.critical().$("Exception during alter [ex=").$(th).I$();
            distressed = true;
            if (CairoException.isDataSyncFailure(th)) {
                dropPendingDurable();
                sequencer.handleDataSyncFailure(th);
            }
            throw th;
        }
        return lastSeqTxn = txn;
    }

    private boolean breachedRolloverSizeThreshold() {
        final long threshold = configuration.getWalSegmentRolloverSize();
        if (threshold == 0) {
            return false;
        }

        if (avgRecordSize != 0) {
            return (segmentRowCount * avgRecordSize) > threshold;
        }

        long tally = 0;
        for (int colIndex = 0, colCount = columns.size(); colIndex < colCount; ++colIndex) {
            final MemoryMA column = columns.getQuick(colIndex);
            if ((column != null) && !(column instanceof NullMemory)) {
                final long columnSize = column.getAppendOffset();
                tally += columnSize;
            }
        }

        // The events file will also contain the symbols.
        tally += events.size();

        // If we have many columns it can be a bit expensive, we can optimise the check
        // by calculating the average record size.
        if ((totalSegmentsRowCount + segmentRowCount) > 1000) {
            avgRecordSize = (totalSegmentsSize + tally) / (totalSegmentsRowCount + segmentRowCount);
        }

        return tally > threshold;
    }

    void checkDistressed() {
        if (sequencer.isDurabilityFailed()) {
            distressed = true;
            throw new CairoError("engine is poisoned by a failed durability barrier");
        }
        if (!distressed) {
            return;
        }
        throw CairoException.critical(0)
                .put("WAL writer is distressed and cannot be used any more [table=").put(tableToken.getTableName())
                .put(", wal=").put(walId).put(']');
    }

    private void closeSegmentSwitchFiles(SegmentColumnRollSink newColumnFiles) {
        int commitMode = walCommitMode();
        for (int columnIndex = 0, n = newColumnFiles.count(); columnIndex < n; columnIndex++) {
            // A fixed-size column (or the designated timestamp) has no aux file, so its dest aux fd is -1;
            // a dropped / not-rolled column can likewise leave a -1 dest fd in the sink. fsyncAndClose(-1)
            // would throw "could not fsync [fd=-1]"; route the sentinel to plain close (a no-op on -1),
            // exactly as the NOSYNC branch already does.
            final long primaryFd = newColumnFiles.getDestPrimaryFd(columnIndex);
            if (commitMode != CommitMode.NOSYNC && primaryFd != -1) {
                ff.fsyncAndClose(primaryFd);
            } else {
                ff.close(primaryFd);
            }

            final long secondaryFd = newColumnFiles.getDestAuxFd(columnIndex);
            if (commitMode != CommitMode.NOSYNC && secondaryFd != -1) {
                ff.fsyncAndClose(secondaryFd);
            } else {
                ff.close(secondaryFd);
            }
        }
    }

    /**
     * TEST-ONLY seam for the CRIT-2 mid-flight window (Task 1b). Invoked inside {@link #commit0} for an
     * ADAPTIVE group-commit ({@code W>0}) deferral, AFTER the txn is sequenced (the shared tracker's seqTxn has
     * advanced to it) and BEFORE the durable-ack contiguous-prefix pin is registered. A test may interpose a
     * peer writer's flush here to deterministically reproduce the mid-flight over-claim; production leaves it
     * {@code null} (a single volatile read, then a no-op). Never installed outside tests.
     */
    @TestOnly
    public interface DeferredCommitInterceptor {
        void onSequencedBeforePin(int walId, long seqTxn);
    }

    @TestOnly
    public static volatile DeferredCommitInterceptor deferredCommitInterceptor;

    private void commit0(
            byte txnType,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            long lastPeriodHi,
            long replaceRangeLowTs,
            long replaceRangeHiTs,
            byte dedupMode
    ) {
        checkDistressed();
        throwIfInColumnarWrite("commit");
        try {
            if (inTransaction() || dedupMode == WAL_DEDUP_MODE_REPLACE_RANGE) {
                final long txnRowCount = getUncommittedRowCount();

                this.isCommittingData = true;
                this.lastTxnType = txnType;
                this.lastReplaceRangeLowTs = replaceRangeLowTs;
                this.lastReplaceRangeHiTs = replaceRangeHiTs;
                this.lastDedupMode = dedupMode;
                this.lastMatViewRefreshBaseTxn = lastRefreshBaseTxn;
                this.lastMatViewRefreshTimestamp = lastRefreshTimestamp;
                this.lastMatViewPeriodHi = lastPeriodHi;
                if (txnRowCount == 0) {
                    // Sometimes symbols are added but rows are cancelled.
                    // This can only theoretically happen for replace range commits
                    // but at the moment it can only happen in fuzz tests because
                    // in regular usage nothing uses row.cancel() when writing to WAL.
                    // In this exotic case we do not need to write symbol maps for empty txns.
                    resetSymbolMaps();
                }

                lastSegmentTxn = events.appendData(
                        txnType,
                        currentTxnStartRowNum,
                        segmentRowCount,
                        txnMinTimestamp,
                        txnMaxTimestamp,
                        txnOutOfOrder,
                        lastRefreshBaseTxn,
                        lastRefreshTimestamp,
                        lastPeriodHi,
                        replaceRangeLowTs,
                        replaceRangeHiTs,
                        dedupMode
                );
                // flush disk before getting next txn. Under ADAPTIVE+W=0 syncIfRequired/getSequencerTxn
                // fdatasync data→events→seq synchronously; under ADAPTIVE+W>0 they do the SYNC-grade msync
                // (page-cache, ordered) and DEFER the device flush to the batched flushPendingDurable below.
                syncIfRequired();
                final long seqTxn = getSequencerTxn();
                if (walCommitMode() == CommitMode.ADAPTIVE) {
                    if (deferDeviceFlush()) {
                        // TEST-ONLY seam (Task 1b): the mid-flight window — the txn is now sequenced (the shared
                        // tracker's seqTxn has advanced to it) but its durable-ack pin was registered ATOMICALLY
                        // with that assignment in the sequencer, so a peer's markWriterDurable here can no longer
                        // empty the pin map and over-claim this still-non-durable txn. Lets a test drive that race.
                        final DeferredCommitInterceptor interceptor = deferredCommitInterceptor;
                        if (interceptor != null) {
                            interceptor.onSequencedBeforePin(walId, seqTxn);
                        }
                        // Deferred 2 (group commit, W>0): the commit is SEQUENCED and msync'd to the page
                        // cache but NOT yet device-durable. Record it as the pending-durable frontier and
                        // DO NOT advance localDurableSeqTxn — the durable-ack must not fire until the batch
                        // fdatasync lands (flushPendingDurable). Then bound the backlog age to <= W on the
                        // commit path: if the oldest un-flushed commit is already W old, flush the whole
                        // backlog (including this commit) now. The background WalPurgeJob flusher covers the
                        // case where commits STOP before the window elapses.
                        recordPendingDurable(seqTxn);
                    } else {
                        // W=0 (today's behaviour): fdatasync completed (data→events→seq) before this point,
                        // so the commit is device-durable. Re-check the process fence before publishing.
                        checkDistressed();
                        seqTxnTracker.setLocalDurableSeqTxn(seqTxn);
                    }
                }
                if (walTelemetryEnabled) {
                    final long minTs = txnRowCount > 0 ? txnMinTimestamp : Numbers.LONG_NULL;
                    final long maxTs = txnRowCount > 0 ? txnMaxTimestamp : Numbers.LONG_NULL;
                    TelemetryWalTask.store(
                            telemetryWal,
                            TelemetryEvent.WAL_TXN_COMMITTED,
                            tableToken.getTableId(),
                            walId,
                            seqTxn,
                            txnRowCount,
                            txnRowCount,
                            0L,
                            minTs,
                            maxTs
                    );
                }
                final boolean hasReplaceRange = replaceRangeHiTs > replaceRangeLowTs;
                // Reduce logging when telemetry is enabled; all the information is saved in sys.telemetry_wal
                LogRecord logLine = hasReplaceRange || !walTelemetryEnabled ? LOG.info() : LOG.debug();
                try {
                    logLine.$("commit [wal=").$substr(pathRootSize, path).$(Files.SEPARATOR).$(segmentId)
                            .$(", segTxn=").$(lastSegmentTxn)
                            .$(", seqTxn=").$(seqTxn)
                            .$(", rowLo=").$(currentTxnStartRowNum).$(", rowHi=").$(segmentRowCount)
                            .$(", minTs=").$ts(timestampDriver, txnMinTimestamp).$(", maxTs=").$ts(timestampDriver, txnMaxTimestamp);
                    if (hasReplaceRange) {
                        logLine.$(", replaceRangeLo=").$ts(timestampDriver, replaceRangeLowTs).$(", replaceRangeHi=").$ts(timestampDriver, replaceRangeHiTs);
                    }
                } finally {
                    logLine.I$();
                }
                resetDataTxnProperties();
                mayRollSegmentOnNextRow();
                metrics.walMetrics().addRowsWritten(txnRowCount);
                // Track WAL commit for tables() function
                if (recentWriteTracker != null) {
                    recentWriteTracker.recordWalWrite(
                            tableToken,
                            seqTxn,
                            lastTxnMaxTimestamp == -1 ? Numbers.LONG_NULL : lastTxnMaxTimestamp,
                            txnRowCount
                    );
                }
            }
        } catch (CairoException | TableReferenceOutOfDateException ex) {
            distressed = true;
            if (CairoException.isDataSyncFailure(ex)) {
                dropPendingDurable();
                sequencer.handleDataSyncFailure(ex);
            }
            throw ex;
        } catch (Throwable th) {
            // If distressed, no need to rollback, WalWriter will not be used anymore
            if (!isDistressed()) {
                rollback0();
            }
            throw th;
        } finally {
            isCommittingData = false;
        }
    }

    private void configureColumn(int columnIndex, int columnType) {
        final int dataColumnOffset = getDataColumnOffset(columnIndex);
        if (columnType > 0) {
            final MemoryMA dataMem = Vm.getPMARInstance(configuration);
            final MemoryMA auxMem = createAuxColumnMem(columnType);
            columns.extendAndSet(dataColumnOffset, dataMem);
            columns.extendAndSet(dataColumnOffset + 1, auxMem);
            configureNullSetters(nullSetters, columnType, dataMem, auxMem, columnIndex, symbolMapNullFlagsChanged, symbolMapNullFlags);
            rowValueIsNotNull.add(-1);
        } else {
            columns.extendAndSet(dataColumnOffset, NullMemory.INSTANCE);
            columns.extendAndSet(dataColumnOffset + 1, NullMemory.INSTANCE);
            nullSetters.add(NOOP);
            rowValueIsNotNull.add(COLUMN_DELETED_NULL_FLAG);
        }
    }

    private void configureColumns() {
        for (int i = 0; i < columnCount; i++) {
            configureColumn(i, metadata.getColumnType(i));
        }
    }

    private void configureEmptySymbol(int columnWriterIndex) {
        symbolMapReaders.extendAndSet(columnWriterIndex, EmptySymbolMapReader.INSTANCE);
        initialSymbolCounts.extendAndSet(columnWriterIndex, 0);
        localSymbolIds.extendAndSet(columnWriterIndex, 0);
        symbolMapNullFlags.extendAndSet(columnWriterIndex, false);
        symbolMapNullFlagsChanged.extendAndSet(columnWriterIndex, false);
        symbolMaps.extendAndSet(columnWriterIndex, new DirectCharSequenceIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
        utf8SymbolMaps.extendAndSet(columnWriterIndex, new Utf8StringIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
    }

    private void configureSymbolMapWriter(
            int columnWriterIndex,
            CharSequence columnName,
            int symbolCount,
            long columnNameTxn
    ) {
        if (symbolCount == 0) {
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        // Copy or hard link symbol map files.
        FilesFacade ff = configuration.getFilesFacade();
        Path tempPath = Path.PATH.get();
        tempPath.of(configuration.getDbRoot()).concat(tableToken);
        int tempPathTripLen = tempPath.size();

        path.trimTo(pathSize);
        TableUtils.offsetFileName(tempPath, columnName, columnNameTxn);
        TableUtils.offsetFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link offset file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(pathSize);
        TableUtils.charFileName(tempPath, columnName, columnNameTxn);
        TableUtils.charFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link char file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, pathSize, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(pathSize);
        // Symbol map files always use SYMBOL format (.k/.v)
        IndexFactory.keyFileName(IndexType.BITMAP, tempPath, columnName, columnNameTxn);
        IndexFactory.keyFileName(IndexType.BITMAP, path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link key file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, pathSize, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(pathSize);
        // Symbol map files always use SYMBOL format (.k/.v); sealTxn is BITMAP-ignored.
        IndexFactory.valueFileName(IndexType.BITMAP, tempPath, columnName, columnNameTxn, -1L);
        IndexFactory.valueFileName(IndexType.BITMAP, path, columnName, COLUMN_NAME_TXN_NONE, -1L);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link value file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, pathSize, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        path.trimTo(pathSize);
        SymbolMapReader symbolMapReader = new SymbolMapReaderImpl(
                configuration,
                path,
                columnName,
                COLUMN_NAME_TXN_NONE,
                symbolCount
        );

        symbolMapReaders.extendAndSet(columnWriterIndex, symbolMapReader);
        symbolMaps.extendAndSet(columnWriterIndex, new DirectCharSequenceIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
        utf8SymbolMaps.extendAndSet(columnWriterIndex, new Utf8StringIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
        initialSymbolCounts.extendAndSet(columnWriterIndex, symbolCount);
        localSymbolIds.extendAndSet(columnWriterIndex, 0);
        symbolMapNullFlags.extendAndSet(columnWriterIndex, symbolMapReader.containsNullValue());
        symbolMapNullFlagsChanged.extendAndSet(columnWriterIndex, false);
    }

    private void configureSymbolTable() {
        boolean initialized = false;
        int denseSymbolIndex = 0;

        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            if (!ColumnType.isSymbol(columnType)) {
                // Maintain sparse list of symbol writers
                // Note: we don't need to set initialSymbolCounts and symbolMapNullFlags values
                // here since we already filled it with -1 and false initially
                symbolMapReaders.extendAndSet(i, null);
                symbolMaps.extendAndSet(i, null);
                utf8SymbolMaps.extendAndSet(i, null);
            } else {
                if (txReader == null) {
                    txReader = new TxReader(ff);
                    columnVersionReader = new ColumnVersionReader();
                }

                if (!initialized) {
                    MillisecondClock milliClock = configuration.getMillisecondClock();
                    long spinLockTimeout = configuration.getSpinLockTimeout();

                    // todo: use own path
                    Path path = Path.PATH2.get();
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME);

                    // Does not matter which PartitionBy, as long as it is partitioned
                    // WAL tables must be partitioned
                    txReader.ofRO(path.$(), metadata.getTimestampType(), PartitionBy.DAY);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME);
                    columnVersionReader.ofRO(ff, path.$());

                    initialized = true;
                    long structureVersion = getMetadataVersion();

                    do {
                        TableUtils.safeReadTxn(txReader, milliClock, spinLockTimeout);
                        if (txReader.getColumnStructureVersion() != structureVersion) {
                            initialized = false;
                            break;
                        }
                        columnVersionReader.readSafe(milliClock, spinLockTimeout);
                    } while (txReader.getColumnVersion() != columnVersionReader.getVersion());
                }

                if (initialized) {
                    int symbolValueCount = txReader.getSymbolValueCount(denseSymbolIndex);
                    long symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(i);
                    configureSymbolMapWriter(i, metadata.getColumnName(i), symbolValueCount, symbolTableNameTxn);
                } else {
                    // table on disk structure version does not match the structure version of the WalWriter
                    // it is not possible to re-use table symbol table because the column name may not match.
                    // The symbol counts stored as dense in _txn file and removal of symbols
                    // shifts the counts that's why it's not possible to find out the symbol count if metadata versions
                    // don't match.
                    configureSymbolMapWriter(i, metadata.getColumnName(i), 0, COLUMN_NAME_TXN_NONE);
                }
            }

            if (columnType == ColumnType.SYMBOL) {
                denseSymbolIndex++;
            }
        }
    }

    private MemoryMA createAuxColumnMem(int columnType) {
        return ColumnType.isVarSize(columnType) ? Vm.getPMARInstance(configuration) : null;
    }

    private SegmentColumnRollSink createSegmentColumnRollSink() {
        if (columnConversionSink == null) {
            columnConversionSink = new SegmentColumnRollSink();
        } else {
            columnConversionSink.clear();
        }
        return columnConversionSink;
    }

    private void doClose(boolean truncate) {
        if (open) {
            open = false;
            // Release every native resource even if one step throws. A power loss (simulated by the crash
            // harness, or a genuine disk fault in production) can fire on a close-time durability op; doClose has
            // already cleared `open`, so any resource skipped by an aborting throw would never be reclaimed (a
            // retry finds open==false and no-ops) and its memory would leak until the process exits. Run each
            // step in turn, keep the FIRST fault, and rethrow it after everything is released so callers/pools
            // still observe the failure.
            Throwable closeError = null;
            // Deferred 2 (group commit): ensure this writer is never left in the background flush queue past
            // its own lifetime. cleanupBeforeClose already flushed (clean) or dropped (distressed) it; this
            // is the belt-and-suspenders for any close path that bypasses cleanupBeforeClose (e.g. a
            // constructor failure before the writer ever committed). Use dropPendingDurable() (synchronized)
            // so pending is cleared under the monitor BEFORE the fd-close loop below — closing the
            // use-after-close race even on this fallback path (doClose is not itself synchronized). Wrapped so a
            // fault here (it can perform a device flush) still lets the memory frees below run.
            try {
                dropPendingDurable();
            } catch (Throwable th) {
                closeError = th;
            }
            if (metadata != null) {
                try {
                    metadata.close(truncate, Vm.TRUNCATE_TO_POINTER);
                } catch (Throwable th) {
                    closeError = closeError != null ? closeError : th;
                }
            }
            if (events != null) {
                try {
                    events.close(truncate, Vm.TRUNCATE_TO_POINTER);
                } catch (Throwable th) {
                    closeError = closeError != null ? closeError : th;
                }
            }
            if (columnarAppender != null) {
                try {
                    columnarAppender.close();
                } catch (Throwable th) {
                    closeError = closeError != null ? closeError : th;
                }
                columnarAppender = null;
            }
            try {
                freeSymbolMapReaders();
            } catch (Throwable th) {
                closeError = closeError != null ? closeError : th;
            }
            try {
                freeColumns(truncate);
            } catch (Throwable th) {
                closeError = closeError != null ? closeError : th;
            }

            if (minSegmentLocked > -1) {
                notifySegmentClosure(lastSegmentTxn, minSegmentLocked);
                minSegmentLocked = -1;
            }

            try {
                releaseWalLock();
            } finally {
                Misc.free(path);
                LOG.info().$("closed [table=").$(tableToken).I$();
            }
            // must happen after the WAL lock is released
            notifyWalClosure();
            columnVersionReader = Misc.free(columnVersionReader);
            txReader = Misc.free(txReader);

            // All native resources are now released; surface the first close-time fault (if any) to the caller.
            throwFirstCloseError(closeError);
        }
    }

    private void freeAndRemoveColumnPair(ObjList<MemoryMA> columns, int pi, int si) {
        final MemoryMA primaryColumn = columns.getAndSetQuick(pi, null);
        final MemoryMA secondaryColumn = columns.getAndSetQuick(si, null);
        // Both slots are detached from `columns` BEFORE either close, so a close that throws part-way makes
        // the other half unreachable -- freeColumns() can no longer see it, and nothing else ever will. Under
        // adaptive commit these closes carry a real durability barrier (MemoryPMARImpl.close -> msync), so a
        // simulated crash or a genuine EIO here would strand the secondary column's mapping and its fd.
        // Close BOTH, then surface the first fault, exactly as freeColumns does.
        Throwable closeError = null;
        try {
            primaryColumn.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
        } catch (Throwable th) {
            closeError = th;
        }
        if (secondaryColumn != null) {
            try {
                secondaryColumn.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
            } catch (Throwable th) {
                if (closeError == null) {
                    closeError = th;
                }
            }
        }
        throwFirstCloseError(closeError);
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            Throwable closeError = null;
            for (int i = 0, n = columns.size(); i < n; i++) {
                final MemoryMA m = columns.getQuick(i);
                if (m != null) {
                    try {
                        m.close(truncate, Vm.TRUNCATE_TO_POINTER);
                    } catch (Throwable th) {
                        // A close-time durability fault on one column (e.g. a power loss the crash harness
                        // simulates by throwing on its fd sync, or a genuine disk error) must not strand the
                        // REST mapped: MemoryCMARWImpl.close unmaps before that fd sync, so the faulting column
                        // is already released -- keep going so every other column is unmapped too (doClose has
                        // cleared `open`, so no retry would ever reach them). Rethrow the first fault after.
                        if (closeError == null) {
                            closeError = th;
                        }
                    }
                }
            }
            throwFirstCloseError(closeError);
        }
    }

    private static void throwFirstCloseError(Throwable closeError) {
        if (closeError != null) {
            if (closeError instanceof Error) {
                throw (Error) closeError;
            }
            if (closeError instanceof RuntimeException) {
                throw (RuntimeException) closeError;
            }
            throw new RuntimeException(closeError);
        }
    }

    private void freeSymbolMapReaders() {
        Misc.freeObjListIfCloseable(symbolMapReaders);
        Misc.freeObjListIfCloseable(symbolMaps);
    }

    private long getColumnStructureVersion() {
        // Sequencer metadata version is the same as column structure version of the table.
        return metadata.getMetadataVersion();
    }

    private SymbolTable getConversionSymbolMapReader(int columnIndex) {
        if (conversionSymbolTable == null) {
            conversionSymbolTable = new ConversionSymbolTable();
        }
        conversionSymbolTable.of(this, columnIndex);
        return conversionSymbolTable;
    }

    private SymbolMapWriterLite getConversionSymbolMapWriter(int columnIndex) {
        if (conversionSymbolMap == null) {
            conversionSymbolMap = new ConversionSymbolMapWriter();
        }
        conversionSymbolMap.of(this, columnIndex);
        return conversionSymbolMap;
    }

    private long getDataAppendPageSize() {
        return tableToken.isSystem() ? configuration.getSystemWalDataAppendPageSize() : configuration.getWalDataAppendPageSize();
    }

    private long getSequencerTxn() {
        try {
            long seqTxn;
            do {
                seqTxn = sequencer.nextTxn(tableToken, walId, getColumnStructureVersion(), segmentId, lastSegmentTxn, txnMinTimestamp, txnMaxTimestamp, segmentRowCount - currentTxnStartRowNum);
                if (seqTxn == NO_TXN) {
                    applyMetadataChangeLog(Long.MAX_VALUE);
                }
            } while (seqTxn == NO_TXN);
            return lastSeqTxn = seqTxn;
        } catch (Throwable failure) {
            if (CairoException.isDataSyncFailure(failure)) {
                distressed = true;
                dropPendingDurable();
                sequencer.handleDataSyncFailure(failure);
            }
            CairoException.rethrowCleanupFailure(failure);
            return NO_TXN;
        }
    }

    private boolean hasDirtyColumns(long currentTxnStartRowNum) {
        for (int i = 0; i < columnCount; i++) {
            long writtenCount = rowValueIsNotNull.getQuick(i);
            if (writtenCount >= currentTxnStartRowNum && writtenCount != COLUMN_DELETED_NULL_FLAG) {
                return true;
            }
        }
        return false;
    }

    private boolean inTransaction() {
        return segmentRowCount > currentTxnStartRowNum;
    }

    private boolean isInColumnarWrite() {
        return columnarAppender != null && columnarAppender.isInColumnarWrite();
    }

    private boolean isTruncateFilesOnClose() {
        return walDirectoryPolicy.truncateFilesOnClose();
    }

    private void markColumnRemoved(int columnIndex, int columnType) {
        if (ColumnType.isSymbol(columnType)) {
            removeSymbolMapReader(columnIndex);
        }
        final int pi = getDataColumnOffset(columnIndex);
        final int si = getAuxColumnOffset(columnIndex);
        freeNullSetter(nullSetters, columnIndex);
        freeAndRemoveColumnPair(columns, pi, si);
        rowValueIsNotNull.setQuick(columnIndex, COLUMN_DELETED_NULL_FLAG);
    }

    private void mayRollSegmentOnNextRow() {
        if (rollSegmentOnNextRow) {
            return;
        }
        rollSegmentOnNextRow = (segmentRowCount >= configuration.getWalSegmentRolloverRowCount())
                || breachedRolloverSizeThreshold()
                || (lastSegmentTxn > Integer.MAX_VALUE - 2);
    }

    private void openColumnFiles(CharSequence columnName, int columnType, int columnIndex, int pathTrimToLen) {
        try {
            final MemoryMA dataMem = getDataColumn(columnIndex);
            totalSegmentsSize += dataMem.getAppendOffset();
            dataMem.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
            dataMem.of(
                    ff,
                    dFile(path.trimTo(pathTrimToLen), columnName),
                    getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WAL_WRITER,
                    configuration.getWriterFileOpenOpts(),
                    columnsMadviseMode
            );
            // WAL column DATA vector is strictly append-only (row values appended via put*(value),
            // cursor moved only with jumpTo/truncate), so the SYNC msync can be narrowed to the
            // written range -- but ONLY under ADAPTIVE: legacy modes keep appendOnly=false so sync()
            // takes its full-extent else branch, byte-identical to master. Re-set after every of()
            // because WAL reuses the PMAR across segments.
            final boolean columnAppendOnly = walCommitMode() == CommitMode.ADAPTIVE;
            dataMem.setAppendOnly(columnAppendOnly);

            final MemoryMA auxMem = getAuxColumn(columnIndex);
            if (auxMem != null) {
                totalSegmentsSize += auxMem.getAppendOffset();
                auxMem.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
                ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                columnTypeDriver.configureAuxMemMA(
                        ff,
                        auxMem,
                        iFile(path.trimTo(pathTrimToLen), columnName),
                        getDataAppendPageSize(),
                        MemoryTag.MMAP_TABLE_WAL_WRITER,
                        configuration.getWriterFileOpenOpts(),
                        columnsMadviseMode
                );
                // WAL column AUX vector is likewise strictly append-only under ADAPTIVE only
                // (configureAuxMemMA re-opens it via of(), so set the flag afterwards).
                auxMem.setAppendOnly(columnAppendOnly);
            }
        } finally {
            path.trimTo(pathTrimToLen);
        }
    }

    private void openNewSegment() {
        // Deferred 2 (group commit): flush any pending device flush of the CURRENT segment BEFORE we close
        // and replace its column/events files. flushPendingDurable() atomically flushes, clears pending, and
        // DEREGISTERS this writer from the background flush queue under the writer monitor — so once it
        // returns, no background flusher will iterate the column list we are about to mutate (a flusher that
        // grabs the monitor afterwards finds pendingDurableSeqTxn == -1 and no-ops without touching any fd).
        // This closes the use-after-close race between the background flusher's fdatasync of the segment's
        // column/events fds and this segment roll's close/reopen of them. A no-op when nothing is pending
        // (W=0, or already flushed), so the constructor's first openNewSegment and the W=0 path are untouched.
        flushPendingDurable();
        boolean refreshed = refreshSymbolWatermarks();
        final int newSegmentId = segmentId + 1;
        final long oldLastSegmentTxn = lastSegmentTxn;
        // Declared out here (not inside the try) so the finally can release it: under non-NOSYNC the
        // segment-dir fd is opened below for a durability fsync, and a column/event file open between
        // that open and the success fsyncAndClose can fault - which would otherwise leak the fd.
        long dirFd = -1;
        try {
            totalSegmentsRowCount += Math.max(0, segmentRowCount);
            currentTxnStartRowNum = 0;
            rowValueIsNotNull.fill(0, columnCount, -1);
            final int segmentPathLen = createSegmentDir(newSegmentId);
            segmentId = newSegmentId;
            final int commitMode = walCommitMode();
            if (Os.isWindows() || commitMode == CommitMode.NOSYNC) {
                dirFd = -1;
            } else {
                dirFd = TableUtils.openRONoCache(ff, path.$(), LOG);
            }

            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                if (columnType > 0) {
                    final CharSequence columnName = metadata.getColumnName(i);
                    openColumnFiles(columnName, columnType, i, segmentPathLen);

                    if (columnType == ColumnType.SYMBOL && symbolMapReaders.size() > 0) {
                        final SymbolMapReader reader = symbolMapReaders.getQuick(i);
                        if (!refreshed) {
                            // fallback: use stale reader counts, possibly stale
                            initialSymbolCounts.set(i, reader.getSymbolCount());
                            symbolMapNullFlags.set(i, reader.containsNullValue());
                        }
                        localSymbolIds.set(i, 0);
                        symbolMapNullFlagsChanged.set(i, false);
                        symbolMaps.getQuick(i).clear();
                        utf8SymbolMaps.getQuick(i).clear();
                    }
                } else {
                    rowValueIsNotNull.setQuick(i, COLUMN_DELETED_NULL_FLAG);
                }
            }

            segmentRowCount = 0;
            metadata.switchTo(path, segmentPathLen, isTruncateFilesOnClose());
            totalSegmentsSize += events.size();
            events.openEventFile(path, segmentPathLen, isTruncateFilesOnClose(), tableToken.isSystem());
            if (commitMode != CommitMode.NOSYNC) {
                events.sync(commitMode);
            }

            if (dirFd != -1) {
                final long fd = dirFd;
                dirFd = -1; // clear before fsyncAndClose so the finally never double-closes (it closes even if the fsync fails)
                ff.fsyncAndClose(fd);
                fsyncWalNamespaceParents();
            }
            lastSegmentTxn = -1;
            LOG.info().$("opened WAL segment [path=").$substr(pathRootSize, path.parent()).I$();
        } finally {
            if (dirFd != -1) {
                // A column/event file open above faulted before the success fsyncAndClose; release the
                // segment-dir fd (opened under non-NOSYNC) so it does not leak. No fsync - the segment is
                // being abandoned - and no throw, so the in-flight exception is not masked.
                ff.close(dirFd);
            }
            int oldMinSegmentLocked = minSegmentLocked;
            if (moveMinSegmentLock(newSegmentId)) {
                notifySegmentClosure(oldLastSegmentTxn, oldMinSegmentLocked);
            }
            path.trimTo(pathSize);
        }
    }

    private void fsyncWalNamespaceParents() {
        try (Path dirPath = new Path().of(configuration.getDbRoot()).concat(tableToken).concat(walName)) {
            long fd = TableUtils.openRONoCache(ff, dirPath.$(), LOG);
            ff.fsyncAndClose(fd);

            dirPath.parent();
            fd = TableUtils.openRONoCache(ff, dirPath.$(), LOG);
            ff.fsyncAndClose(fd);
        }
    }

    /**
     * Refreshes symbol watermarks from _txn/_cv files on segment rollover.
     * Returns true if refresh succeeded, false if skipped (version mismatch or first open).
     */
    private boolean refreshSymbolWatermarks() {
        // Skip on first open - configureSymbolTable() hasn't run yet
        if (segmentId < 0) {
            return false;
        }

        // Count actual symbol columns
        int symbolColumnCount = 0;
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                symbolColumnCount++;
            }
        }
        if (symbolColumnCount == 0) {
            return true; // No symbols, nothing to refresh
        }

        // Lazy init readers if needed
        if (txReader == null) {
            txReader = new TxReader(ff);
        }
        if (columnVersionReader == null) {
            columnVersionReader = new ColumnVersionReader();
        }

        // Read _txn and _cv files
        MillisecondClock milliClock = configuration.getMillisecondClock();
        long spinLockTimeout = configuration.getSpinLockTimeout();

        Path txPath = Path.PATH2.get();
        txPath.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME);
        txReader.ofRO(txPath.$(), metadata.getTimestampType(), PartitionBy.DAY);

        txPath.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME);
        columnVersionReader.ofRO(ff, txPath.$());

        long structureVersion = getMetadataVersion();
        do {
            TableUtils.safeReadTxn(txReader, milliClock, spinLockTimeout);
            if (txReader.getColumnStructureVersion() != structureVersion) {
                return false; // Version mismatch - caller should use fallback
            }
            columnVersionReader.readSafe(milliClock, spinLockTimeout);
        } while (txReader.getColumnVersion() != columnVersionReader.getVersion());

        // Update each symbol column
        int denseSymbolIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            if (!ColumnType.isSymbol(columnType)) {
                continue;
            }

            int symbolValueCount = txReader.getSymbolValueCount(denseSymbolIndex);
            long symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(i);
            SymbolMapReader reader = symbolMapReaders.getQuick(i);

            if (reader == EmptySymbolMapReader.INSTANCE) {
                if (symbolValueCount > 0) {
                    // Upgrade empty reader to real reader (re-hardlinks files).
                    // Null out list entries after freeing so that doClose() does not
                    // double-close if configureSymbolMapWriter() throws below.
                    Misc.free(symbolMaps.getQuick(i));
                    symbolMaps.setQuick(i, null);
                    configureSymbolMapWriter(i, metadata.getColumnName(i), symbolValueCount, symbolTableNameTxn);
                } else {
                    // Still empty - ensure watermarks are reset (not stale from previous segments)
                    initialSymbolCounts.set(i, 0);
                    symbolMapNullFlags.set(i, false);
                }
            } else {
                SymbolMapReaderImpl readerImpl = (SymbolMapReaderImpl) reader;
                if (readerImpl.needsReopen(symbolTableNameTxn)) {
                    // Capacity rebuild - re-hardlink and reopen via configureSymbolMapWriter.
                    // Null out list entries after freeing so that doClose() does not
                    // double-close if configureSymbolMapWriter() throws below.
                    Misc.free(readerImpl);
                    symbolMapReaders.setQuick(i, null);
                    Misc.free(symbolMaps.getQuick(i));
                    symbolMaps.setQuick(i, null);
                    // Remove old symbol files before re-hardlinking (files exist from previous segment)
                    removeSymbolFiles(path, pathSize, metadata.getColumnName(i));
                    configureSymbolMapWriter(i, metadata.getColumnName(i), symbolValueCount, symbolTableNameTxn);
                } else {
                    // Just update count (extends memory mappings)
                    readerImpl.updateSymbolCount(symbolValueCount);
                    initialSymbolCounts.set(i, symbolValueCount);
                    symbolMapNullFlags.set(i, readerImpl.containsNullValue());
                }
            }

            denseSymbolIndex++;
        }
        return true;
    }

    private void removeSymbolFiles(Path path, int rootLen, CharSequence columnName) {
        // Symbol files in WAL directory are hard links to symbol files in the table.
        // Removing them does not affect the allocated disk space, and it is just
        // making directory tidy. On Windows OS, removing hard link can trigger
        // ACCESS_DENIED error, caused by the fact hard link destination file is open.
        // For those reasons we do not put maximum effort into removing the files here.

        // Symbol map files always use SYMBOL format (.k/.v); sealTxn is BITMAP-ignored.
        path.trimTo(rootLen);
        IndexFactory.valueFileName(IndexType.BITMAP, path, columnName, COLUMN_NAME_TXN_NONE, -1L);
        ff.removeQuiet(path.$());

        path.trimTo(rootLen);
        IndexFactory.keyFileName(IndexType.BITMAP, path, columnName, COLUMN_NAME_TXN_NONE);
        ff.removeQuiet(path.$());

        path.trimTo(rootLen);
        TableUtils.charFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        ff.removeQuiet(path.$());

        path.trimTo(rootLen);
        TableUtils.offsetFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        ff.removeQuiet(path.$());
    }

    private void removeSymbolMapReader(int index) {
        Misc.freeIfCloseable(symbolMapReaders.getAndSetQuick(index, null));
        Misc.free(symbolMaps.getAndSetQuick(index, null));
        utf8SymbolMaps.setQuick(index, null);
        initialSymbolCounts.set(index, -1);
        localSymbolIds.set(index, 0);
        symbolMapNullFlags.set(index, false);
        symbolMapNullFlagsChanged.set(index, false);
        removeSymbolFiles(path, pathSize, metadata.getColumnName(index));
    }

    private void renameColumnFiles(int columnType, CharSequence columnName, CharSequence newName) {
        path.trimTo(pathSize).slash().put(segmentId);
        final Path tempPath = Path.PATH.get().of(path);

        if (ColumnType.isVarSize(columnType)) {
            final int trimTo = path.size();
            iFile(path, columnName);
            iFile(tempPath, newName);
            if (ff.rename(path.$(), tempPath.$()) != Files.FILES_RENAME_OK) {
                throw CairoException.critical(ff.errno())
                        .put("could not rename WAL column file [from=").put(path)
                        .put(", to=").put(tempPath)
                        .put(']');
            }
            path.trimTo(trimTo);
            tempPath.trimTo(trimTo);
        }

        dFile(path, columnName);
        dFile(tempPath, newName);
        if (ff.rename(path.$(), tempPath.$()) != Files.FILES_RENAME_OK) {
            throw CairoException.critical(ff.errno()).put("could not rename WAL column file [from=").put(path).put(", to=").put(tempPath).put(']');
        }
    }

    private void resetDataTxnProperties() {
        currentTxnStartRowNum = segmentRowCount;
        txnMinTimestamp = Long.MAX_VALUE;
        // Store the max timestamp before resetting for tracking purposes
        lastTxnMaxTimestamp = txnMaxTimestamp;
        txnMaxTimestamp = -1;
        txnOutOfOrder = false;
        resetSymbolMaps();
    }

    private void throwIfInColumnarWrite(CharSequence operation) {
        if (isInColumnarWrite()) {
            throw CairoException.nonCritical().put("cannot ").put(operation).put(" during columnar write");
        }
    }

    private void resetSymbolMaps() {
        final int numOfColumns = symbolMaps.size();
        for (int i = 0; i < numOfColumns; i++) {
            final var symbolMap = symbolMaps.getQuick(i);
            if (symbolMap != null) {
                symbolMap.clear();
            }

            final Utf8StringIntHashMap dbcsSymbolMap = utf8SymbolMaps.getQuick(i);
            if (dbcsSymbolMap != null) {
                dbcsSymbolMap.clear();
            }

            final SymbolMapReader reader = symbolMapReaders.getQuick(i);
            if (reader != null) {
                initialSymbolCounts.set(i, reader.getSymbolCount());
                localSymbolIds.set(i, 0);
                symbolMapNullFlags.set(i, reader.containsNullValue());
                symbolMapNullFlagsChanged.set(i, false);
            }
        }
    }

    private void rollLastWalEventRecord(int newSegmentId, long uncommittedRows) {
        if (isCommittingData) {
            // Sometimes we only want to add a column without committing the data in the current wal segments in ILP.
            // When this happens the data stays in the WAL column files but is not committed
            // and the events file don't have a record about the column add transaction.
            // In this case we DO NOT roll back the last record in the events file.
            events.rollback();
        }
        path.trimTo(pathSize).slash().put(newSegmentId);
        events.openEventFile(path, path.size(), isTruncateFilesOnClose(), tableToken.isSystem());
        lastSegmentTxn = -1;
        if (isCommittingData) {
            // When current transaction is not a data transaction but a column add transaction
            // there is no need to add a record about it to the new segment event file.
            lastSegmentTxn = events.appendData(
                    lastTxnType,
                    0,
                    uncommittedRows,
                    txnMinTimestamp,
                    txnMaxTimestamp,
                    txnOutOfOrder,
                    lastMatViewRefreshBaseTxn,
                    lastMatViewRefreshTimestamp,
                    lastMatViewPeriodHi,
                    lastReplaceRangeLowTs,
                    lastReplaceRangeHiTs,
                    lastDedupMode
            );
        }
        events.sync(walCommitMode());
    }

    private void rollUncommittedToNewSegment(int convertColumnIndex, int convertToColumnType) {
        // Deferred 2 (group commit): like openNewSegment(), flush + deregister any pending device flush of
        // the current segment before its column files are closed/replaced, so the background flusher cannot
        // race the fd close. Defensive: the structural callers reach here via apply(), which already flushed;
        // this is a no-op then (and under W=0), and bulletproofs any future direct caller.
        flushPendingDurable();
        final long uncommittedRows = getUncommittedRowCount();
        final long oldLastSegmentTxn = lastSegmentTxn;
        long rowsRemainInCurrentSegment = currentTxnStartRowNum;

        if (uncommittedRows > 0) {
            final int oldSegmentId = segmentId;
            final int newSegmentId = segmentId + 1;
            if (newSegmentId > WalUtils.SEG_MAX_ID) {
                throw CairoException.critical(0)
                        .put("cannot roll over to new segment due to SEG_MAX_ID overflow [table=").put(tableToken)
                        .put(", walId=").put(walId)
                        .put(", segmentId=").put(newSegmentId).put(']');
            }
            try {
                createSegmentDir(newSegmentId);
                path.trimTo(pathSize);
                SegmentColumnRollSink columnRollSink = createSegmentColumnRollSink();
                rowValueIsNotNull.fill(0, columnCount, -1);

                int columnsToRoll = convertColumnIndex == -1 ? columnCount : columnCount - 1;
                try {
                    final int timestampIndex = metadata.getTimestampIndex();

                    if (convertColumnIndex < 0) {
                        LOG.info().$("rolling uncommitted rows to new segment [wal=")
                                .$(path).$(Files.SEPARATOR).$(oldSegmentId)
                                .$(", lastSegmentTxn=").$(lastSegmentTxn)
                                .$(", newSegmentId=").$(newSegmentId)
                                .$(", skipRows=").$(rowsRemainInCurrentSegment)
                                .$(", rowCount=").$(uncommittedRows)
                                .I$();
                    } else {
                        int existingType = metadata.getColumnType(convertColumnIndex);
                        LOG.info().$("rolling uncommitted rows to new segment with type conversion [wal=")
                                .$(path).$(Files.SEPARATOR).$(oldSegmentId)
                                .$(", lastSegmentTxn=").$(lastSegmentTxn)
                                .$(", newSegmentId=").$(newSegmentId)
                                .$(", skipRows=").$(rowsRemainInCurrentSegment)
                                .$(", rowCount=").$(uncommittedRows)
                                .$(", existingType=").$(ColumnType.nameOf(existingType))
                                .$(", newType=").$(ColumnType.nameOf(convertToColumnType))
                                .I$();
                    }

                    final int commitMode = walCommitMode();
                    for (int columnIndex = 0; columnIndex < columnsToRoll; columnIndex++) {
                        // Allocate space for new column in columnRollSink and move to next record
                        // Do it for deleted columns too, it will be skipped in exactly same way in switchColumnsToNewSegment
                        columnRollSink.nextColumn();
                        final int columnType = metadata.getColumnType(columnIndex);
                        if (columnType > 0) {
                            final MemoryMA primaryColumn = getDataColumn(columnIndex);
                            final MemoryMA secondaryColumn = getAuxColumn(columnIndex);
                            final String columnName = metadata.getColumnName(columnIndex);

                            SymbolMapWriterLite symbolMapWriter = null;
                            SymbolTable symbolTable = null;
                            if (columnIndex == convertColumnIndex) {
                                if (ColumnType.isSymbol(convertToColumnType)) {
                                    // New column destination is the column with last index
                                    symbolMapWriter = getConversionSymbolMapWriter(columnCount - 1);
                                }
                                if (ColumnType.isSymbol(columnType)) {
                                    symbolTable = getConversionSymbolMapReader(columnIndex);
                                }
                            }

                            int colType = columnIndex == timestampIndex ? -columnType : columnType;
                            int newColumnType = columnIndex == convertColumnIndex ? convertToColumnType : colType;
                            // Saves existing segment file offsets and new file sizes in columnRollSink.
                            CopyWalSegmentUtils.rollColumnToSegment(
                                    ff,
                                    configuration.getWriterFileOpenOpts(),
                                    primaryColumn,
                                    secondaryColumn,
                                    path,
                                    newSegmentId,
                                    columnName,
                                    colType,
                                    currentTxnStartRowNum,
                                    uncommittedRows,
                                    columnRollSink,
                                    commitMode,
                                    newColumnType,
                                    symbolTable,
                                    symbolMapWriter
                            );
                        } else {
                            // Deleted column
                            rowValueIsNotNull.setQuick(columnIndex, COLUMN_DELETED_NULL_FLAG);
                        }
                    }
                } catch (Throwable e) {
                    closeSegmentSwitchFiles(columnRollSink);
                    throw e;
                }
                switchColumnsToNewSegment(columnRollSink, columnsToRoll, convertColumnIndex);
                rollLastWalEventRecord(newSegmentId, uncommittedRows);
                segmentId = newSegmentId;
                segmentRowCount = uncommittedRows;
                currentTxnStartRowNum = 0;
            } finally {
                int oldMinSegmentLocked = minSegmentLocked;
                if (moveMinSegmentLock(newSegmentId)) {
                    notifySegmentClosure(oldLastSegmentTxn, oldMinSegmentLocked);
                }
            }
        } else if (segmentRowCount > 0 && uncommittedRows == 0) {
            rollSegmentOnNextRow = true;
        }
    }

    private void rowAppend(ObjList<Runnable> activeNullSetters, long rowTimestamp) {
        for (int i = 0; i < columnCount; i++) {
            if (rowValueIsNotNull.getQuick(i) < segmentRowCount) {
                activeNullSetters.getQuick(i).run();
            }
        }

        if (rowTimestamp > txnMaxTimestamp) {
            txnMaxTimestamp = rowTimestamp;
        } else {
            txnOutOfOrder |= (txnMaxTimestamp != rowTimestamp);
        }
        if (rowTimestamp < txnMinTimestamp) {
            txnMinTimestamp = rowTimestamp;
        }

        segmentRowCount++;
    }

    private void setAppendPosition(final long segmentRowCount) {
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            if (type > 0) {
                setAppendPosition0(i, segmentRowCount);
                rowValueIsNotNull.setQuick(i, segmentRowCount - 1);
            }
        }
    }

    private void setAppendPosition0(int columnIndex, long segmentRowCount) {
        MemoryMA dataMem = getDataColumn(columnIndex);
        MemoryMA auxMem = getAuxColumn(columnIndex);
        int columnType = metadata.getColumnType(columnIndex);
        if (columnType > 0) { // Not deleted
            final long rowCount = Math.max(0, segmentRowCount);
            final long dataMemOffset;
            if (ColumnType.isVarSize(columnType)) {
                assert auxMem != null;
                dataMemOffset = ColumnType.getDriver(columnType).setAppendAuxMemAppendPosition(auxMem, dataMem, columnType, rowCount);
            } else {
                dataMemOffset = rowCount << ColumnType.getWalDataColumnShl(columnType, columnIndex == metadata.getTimestampIndex());
            }
            dataMem.jumpTo(dataMemOffset);
        }
    }

    /**
     * MS_ASYNC vs MS_SYNC for the structural null-backfill, using the same rule as
     * {@link #syncIfRequired0()}: ASYNC mode is async by definition, and under ADAPTIVE the group-commit
     * window (W&gt;0) defers the device flush, so the msync only has to order the pages -- the explicit
     * fdatasync that follows is the barrier.
     */
    private boolean adaptiveMsyncAsync(int commitMode) {
        return commitMode == CommitMode.ASYNC
                || (commitMode == CommitMode.ADAPTIVE && deferDeviceFlush());
    }

    private void setColumnNull(int columnType, int columnIndex, long rowCount, int commitMode) {
        if (ColumnType.isVarSize(columnType)) {
            final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
            setVarColumnDataFileNull(columnTypeDriver, columnIndex, rowCount, commitMode);
            setVarColumnAuxFileNull(columnTypeDriver, columnIndex, rowCount, commitMode);
        } else {
            setFixColumnNulls(columnType, columnIndex, rowCount);
        }
    }

    private void setFixColumnNulls(int type, int columnIndex, long rowCount) {
        MemoryMA fixedSizeColumn = getDataColumn(columnIndex);
        long columnFileSize = rowCount * ColumnType.sizeOf(type);
        fixedSizeColumn.jumpTo(columnFileSize);
        if (columnFileSize > 0) {
            long address = TableUtils.mapRW(ff, fixedSizeColumn.getFd(), columnFileSize, MEM_TAG);
            try {
                TableUtils.setNull(type, address, rowCount);
            } finally {
                ff.munmap(address, columnFileSize, MEM_TAG);
            }
            ff.fsync(fixedSizeColumn.getFd());
        }
    }

    private void setRowValueNotNull(int columnIndex) {
        assert rowValueIsNotNull.getQuick(columnIndex) != segmentRowCount;
        rowValueIsNotNull.setQuick(columnIndex, segmentRowCount);
    }

    private void setVarColumnAuxFileNull(
            ColumnTypeDriver columnTypeDriver,
            int columnIndex,
            long rowCount,
            int commitMode
    ) {
        MemoryMA auxMem = getAuxColumn(columnIndex);
        final long auxMemSize = columnTypeDriver.getAuxVectorSize(rowCount);
        auxMem.jumpTo(auxMemSize);
        if (rowCount > 0) {
            final long auxMemAddr = TableUtils.mapRW(ff, auxMem.getFd(), auxMemSize, MEM_TAG);
            try {
                columnTypeDriver.setFullAuxVectorNull(auxMemAddr, rowCount);
                if (commitMode != CommitMode.NOSYNC) {
                    ff.msync(auxMemAddr, auxMemSize, adaptiveMsyncAsync(commitMode));
                }
            } finally {
                ff.munmap(auxMemAddr, auxMemSize, MEM_TAG);
            }
            // ADAPTIVE handshake, matching syncIfRequired0 and syncAdaptiveEventsBeforeSequencing: the
            // msync ORDERS the bytes (MS_ASYNC when the group-commit window defers the device flush,
            // MS_SYNC otherwise) and an EXPLICIT fdatasync provides the device barrier. Relying on
            // msync(MS_SYNC) alone would make this path's durability grade depend on the kernel treating
            // msync as a range-fsync, and under W>0 it would force a synchronous device flush on the
            // structural path that the group-commit design deliberately defers. This backfill is written
            // on the STRUCTURAL path, which sequences via events-only barriers and never runs
            // syncIfRequired0's per-column loop, so it must carry its own.
            if (commitMode == CommitMode.ADAPTIVE) {
                ff.barrierFsync(auxMem.getFd());
            }
        }
    }

    private void setVarColumnDataFileNull(ColumnTypeDriver columnTypeDriver, int columnIndex, long rowCount, int commitMode) {
        MemoryMA dataMem = getDataColumn(columnIndex);
        final long varColSize = rowCount * columnTypeDriver.getDataVectorMinEntrySize();
        dataMem.jumpTo(varColSize);
        if (rowCount > 0 && varColSize > 0) {
            final long dataMemAddr = TableUtils.mapRW(ff, dataMem.getFd(), varColSize, MEM_TAG);
            try {
                columnTypeDriver.setDataVectorEntriesToNull(dataMemAddr, rowCount);
                if (commitMode != CommitMode.NOSYNC) {
                    ff.msync(dataMemAddr, varColSize, adaptiveMsyncAsync(commitMode));
                }
            } finally {
                ff.munmap(dataMemAddr, varColSize, MEM_TAG);
            }
            // ADAPTIVE handshake, matching syncIfRequired0 and syncAdaptiveEventsBeforeSequencing: the
            // msync ORDERS the bytes (MS_ASYNC when the group-commit window defers the device flush,
            // MS_SYNC otherwise) and an EXPLICIT fdatasync provides the device barrier. Relying on
            // msync(MS_SYNC) alone would make this path's durability grade depend on the kernel treating
            // msync as a range-fsync, and under W>0 it would force a synchronous device flush on the
            // structural path that the group-commit design deliberately defers. This backfill is written
            // on the STRUCTURAL path, which sequences via events-only barriers and never runs
            // syncIfRequired0's per-column loop, so it must carry its own.
            if (commitMode == CommitMode.ADAPTIVE) {
                ff.barrierFsync(dataMem.getFd());
            }
        }
    }

    private void switchColumnsToNewSegment(SegmentColumnRollSink rollSink, int columnsToRoll, int convertColumnIndex) {
        for (int i = 0; i < columnsToRoll; i++) {
            final int columnType = metadata.getColumnType(i);
            if (columnType > 0) {
                if (i != convertColumnIndex) {
                    switchColumnsToNewSegmentRollColumn(rollSink, i, i);
                } else {
                    // Column is converted, the destination column objects are for the last added column
                    switchColumnsToNewSegmentRollColumn(rollSink, i, columnCount - 1);
                }
            }
        }
    }

    private void switchColumnsToNewSegmentRollColumn(SegmentColumnRollSink rollSink, int srcColumnIndex, int destColumnIndex) {
        long currentOffset = rollSink.getSrcPrimaryOffset(srcColumnIndex);
        MemoryMA primaryColumnFile = getDataColumn(srcColumnIndex);
        primaryColumnFile.jumpTo(currentOffset);
        primaryColumnFile.close(isTruncateFilesOnClose());

        MemoryMA auxColumn = getAuxColumn(srcColumnIndex);
        if (auxColumn != null) {
            long auxOffset = rollSink.getSrcAuxOffset(srcColumnIndex);
            auxColumn.jumpTo(auxOffset);
            auxColumn.close(isTruncateFilesOnClose());
        }

        long newSize = rollSink.getDestPrimarySize(srcColumnIndex);
        long newPrimaryFd = rollSink.getDestPrimaryFd(srcColumnIndex);
        MemoryMA destPrimeCol = getDataColumn(destColumnIndex);
        destPrimeCol.switchTo(ff, newPrimaryFd, getDataAppendPageSize(), newSize, isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);

        long newSecondaryFd = rollSink.getDestAuxFd(srcColumnIndex);
        if (newSecondaryFd > -1) {
            long secondarySize = rollSink.getDestAuxSize(srcColumnIndex);
            MemoryMA destAuxColumn = getAuxColumn(destColumnIndex);
            destAuxColumn.switchTo(ff, newSecondaryFd, getDataAppendPageSize(), secondarySize, isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
        }
    }

    /**
     * The EFFECTIVE commit mode for THIS table's WAL durability (Deferred 1): the per-table override
     * published on the tracker resolved against the global {@code cairo.commit.mode}. Read live so an
     * {@code ALTER ... SET PARAM commit_mode} (which republishes the tracker) takes effect on the next
     * commit without reopening this writer.
     */
    private int walCommitMode() {
        int mode = seqTxnTracker.getCommitMode();
        if (mode == CommitMode.UNSET) {
            // Tracker not yet published (e.g. a post-restart WAL commit that precedes the first apply for
            // this table). Resolve from _meta once; this publishes the effective mode onto the tracker so
            // subsequent commits take the cheap volatile-read path above.
            return sequencer.resolveEffectiveCommitMode(tableToken);
        }
        return CommitMode.effectiveCommitMode(mode, configuration.getCommitMode());
    }

    private void syncIfRequired() {
        try {
            syncIfRequired0();
        } catch (Throwable failure) {
            if (CairoException.isDataSyncFailure(failure)) {
                distressed = true;
                dropPendingDurable();
                sequencer.handleDataSyncFailure(failure);
            }
            CairoException.rethrowCleanupFailure(failure);
        }
    }

    private void syncIfRequired0() {
        int commitMode = walCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            // W>0 uses MS_ASYNC here, then fdatasyncs these writer-private files explicitly before
            // sequencing. Only the shared sequencer barrier is deferred/batched; this safe fallback prevents
            // one writer's shared flush from publishing another writer's volatile dependencies. Other modes
            // (and ADAPTIVE W=0) keep their exact existing mmap sync grade.
            final boolean deferDeviceFlush = commitMode == CommitMode.ADAPTIVE && deferDeviceFlush();
            final boolean async = commitMode == CommitMode.ASYNC || deferDeviceFlush;
            for (int i = 0, n = columns.size(); i < n; i++) {
                MemoryMA column = columns.getQuick(i);
                if (column != null) {
                    column.sync(async);
                }
            }
            // WRITEBACK DRAIN (advisory, no barrier moved). Each fdatasync below does writeback AND a
            // journal force, serially, so N files cost N serialised writebacks. One
            // sync_file_range(WRITE|WAIT_AFTER) pass first lets the device write every file back
            // concurrently, leaving each fdatasync with little more than its journal force.
            //
            // NOT a durability step: sync_file_range journals NO metadata, so it can never stand in for a
            // barrier -- every file still gets its own fdatasync below, which is what journals the extent
            // conversions this segment's ftruncate-preallocated mmap appends create. (Leaning on it as a
            // barrier, or on one foreign flush to journal N inodes, is the ext4 trap that cost this
            // codebase real data -- see BatchedFlushSharedJournalDependencyTest.) Being advisory is also
            // what makes it safe to skip on a filesystem where it does nothing: ZFS loses the speedup and
            // nothing else.
            //
            // Only under deferDeviceFlush (W>0): events.sync() is then MS_ASYNC, so hoisting it above the
            // column barriers moves no barrier and the data->events->seq fdatasync order below is intact.
            // Under W=0 events.sync() IS a barrier and must stay after the column barriers.
            final boolean drain = commitMode == CommitMode.ADAPTIVE && deferDeviceFlush && drainWriteback();
            if (drain) {
                events.sync(commitMode); // MS_ASYNC only: makes the event mappings' pages known-dirty
                for (int i = 0, n = columns.size(); i < n; i++) {
                    final MemoryMA column = columns.getQuick(i);
                    if (column != null) {
                        column.syncFlushDrain();
                    }
                }
                events.syncFlushDrain();
            }
            // Fail-safe group-commit protocol: every writer makes its private WAL dependencies durable
            // BEFORE sequencing. This prevents a peer's later fdatasync of the shared sequencer from
            // publishing a record whose data/_event is still volatile. W>0 still batches the shared
            // sequencer barrier, but no longer batches private data/event barriers.
            if (commitMode == CommitMode.ADAPTIVE) {
                for (int i = 0, n = columns.size(); i < n; i++) {
                    MemoryMA column = columns.getQuick(i);
                    if (column != null && !(column instanceof NullMemory) && column.getFd() != -1) {
                        // ORDERING only: these must precede the sequencer record on the medium, and the
                        // commit point's full device flush is what makes them durable. See
                        // Files.barrierFsync.
                        ff.barrierFsync(column.getFd());
                    }
                }
            }
            if (!drain) {
                events.sync(commitMode);
            }
            if (commitMode == CommitMode.ADAPTIVE && deferDeviceFlush) {
                events.barrierFsync();
            }
        }
    }

    /**
     * Whether to run the advisory writeback drain before the commit's barriers. Gated on the filesystem
     * ({@code sync_file_range} buys nothing on ZFS) and on the operator switch.
     */
    private boolean drainWriteback() {
        return configuration.isWalCommitWritebackDrainEnabled()
                && ff.isSyncFileRangeEffective(configuration.getDbRoot());
    }

    /**
     * Adaptive group commit: true when a window {@code W > 0} is configured. Private columns/events are
     * still fdatasync'd before sequencing; only the shared sequencer barrier is deferred to
     * {@link #flushPendingDurable()}. The caller invokes this only on adaptive paths.
     */
    private boolean deferDeviceFlush() {
        return configuration.getAdaptiveCommitGroupWindowUs() > 0;
    }

    /**
     * Record {@code seqTxn} as the pending (private dependencies durable, shared sequencer not yet
     * device-flushed) adaptive group-commit frontier, then apply the COMMIT-DRIVEN flush trigger: if the OLDEST un-flushed commit in
     * this backlog is already {@code >= W} old, flush the whole backlog now so the device-flush latency (and
     * thus RPO) is bounded to {@code <= W} on the commit path. Registers the writer with the background
     * flush queue so an idle tail (commits then STOP) is still flushed within {@code <= W}.
     *
     * <p>Synchronized on the writer monitor so the pending fields and the trigger decision are consistent
     * with {@link #forceDurableIfPending(long, long)} and {@link #flushPendingDurable()}.
     */
    private synchronized void recordPendingDurable(long seqTxn) {
        if (pendingDurableSeqTxn < 0) {
            // first commit of a new batch: stamp the batch's age clock (the OLDEST un-flushed commit). The
            // shared contiguous-prefix pin is NOT registered here — it is registered ATOMICALLY with the seqTxn
            // assignment inside the sequencer (TableSequencerImpl.nextTxn), so it is already in place before this
            // runs and NO mid-flight window exists in which a peer flush could over-claim this txn (Task 1b).
            // putIfAbsent there keeps the pin at this batch's OLDEST seqTxn until the writer's own flush drops it.
            pendingSinceMicros = configuration.getMicrosecondClock().getTicks();
            pendingLoSeqTxn = seqTxn;
        }
        pendingDurableSeqTxn = seqTxn;
        // Register BEFORE the trigger: if the trigger flushes, flushPendingDurable() deregisters; if it does
        // not, the writer is left registered so the background flusher can pick up the idle tail.
        sequencer.getWalGroupCommitFlushQueue().register(this);
        final long windowUs = configuration.getAdaptiveCommitGroupWindowUs();
        final long nowMicros = configuration.getMicrosecondClock().getTicks();
        final long elapsedMicros = nowMicros - pendingSinceMicros;
        // MicrosecondClock is wall time. If NTP/admin adjustment moves it backwards, flush immediately:
        // waiting for wall time to catch up would violate the configured RPO bound.
        if (elapsedMicros < 0 || elapsedMicros >= windowUs) {
            flushPendingDurable();
        }
    }

    /**
     * Perform the batched shared-sequencer flush for adaptive group commit, then advance
     * {@code localDurableSeqTxn} to the pending frontier and clear the pending state. Private WAL dependencies
     * were made durable before sequencing, so this barrier cannot publish volatile data/events.
     *
     * <p>{@code localDurableSeqTxn} (the durable-ack frontier) advances ONLY here, AFTER the device flush
     * completes — so a durable-ack'd txn is always physically on disk.
     *
     * <p>Synchronized on the writer monitor: the background flusher ({@link #forceDurableIfPending}) and the
     * committing thread (commit-driven trigger) both route through this method, so at most one device flush
     * of a given backlog runs and the pending fields are mutated under one lock. Idempotent: a no-op when
     * nothing is pending (a second caller that lost the race simply finds pendingDurableSeqTxn == -1).
     */
    private synchronized void flushPendingDurable() {
        final long flushTo = pendingDurableSeqTxn;
        if (flushTo < 0) {
            return; // nothing pending (already flushed by a peer, or never deferred)
        }
        checkDistressed();
        try {
            // Take the orphan-sweep mark BEFORE the barrier: only pins registered by now are provably
            // covered by the fdatasync below, so only those may be reaped. See
            // SeqTxnTracker.snapshotOrphanSweepMark.
            final long orphanSweepMark = seqTxnTracker.snapshotOrphanSweepMark();
            // Private WAL dependencies were fdatasync'd before sequencing. Only the shared sequencer
            // barrier remains deferred/batched, so it can never expose a peer's volatile dependency.
            sequencer.fdatasyncTxnLog(tableToken);
            // Only NOW is this writer's batch on disk. Drop our contiguous-prefix pin and let the shared frontier
            // advance to the durable prefix across ALL writers (min oldest-un-flushed - 1, or getSeqTxn() when
            // nothing is pending) — NOT to our own flushTo, which would over-claim a peer writer's still-unflushed
            // lower seqTxn (CRITICAL 2). markWriterDurable recomputes the prefix; flushTo above only gates the
            // nothing-pending early return. Re-check process poison after the barriers and before publication.
            checkDistressed();
            seqTxnTracker.markWriterDurable(walId, orphanSweepMark);
            pendingDurableSeqTxn = -1L;
            pendingSinceMicros = -1L;
            pendingLoSeqTxn = -1L;
            sequencer.getWalGroupCommitFlushQueue().unregister(this);
        } catch (CairoException e) {
            if (e.isDataSyncFailure()) {
                distressed = true;
                // The fdatasync FAILED, so this batch was not proven durable and its floor must stand. Orphan
                // (do not remove) the pin: this writer is now distressed and will never flush again, so only a
                // later SUCCESSFUL peer flush of the shared sequencer log may reap it.
                pendingDurableSeqTxn = -1L;
                pendingSinceMicros = -1L;
                pendingLoSeqTxn = -1L;
                seqTxnTracker.orphanWriterPending(walId);
                sequencer.getWalGroupCommitFlushQueue().unregister(this);
                sequencer.handleDataSyncFailure(e);
            }
            throw e;
        }
    }

    /**
     * Background-flusher entry point (Deferred 2): if this writer has a pending commit whose oldest
     * deferral is at least {@code windowUs} old relative to {@code nowMicros}, perform the batched device
     * flush so the commit becomes durable within {@code <= W} even though commits have STOPPED.
     *
     * <p>Thread-safe: synchronizes on the writer monitor (the same lock {@link #flushPendingDurable()} and a
     * committing thread's commit-driven flush take). If a commit is in flight it will either have already
     * advanced the frontier or will register fresh pending state; the monitor serialises us against it so we
     * never double-flush or read a torn pending snapshot. The age check is taken under the lock so a flush
     * that a concurrent commit just performed (clearing pending) makes this a clean no-op.
     *
     * @return {@code true} if a flush was performed
     */
    synchronized boolean forceDurableIfPending(long nowMicros, long windowUs) {
        // CLOCK-UNIT INVARIANT: both nowMicros (caller's clock) and pendingSinceMicros (set in
        // recordPendingDurable via configuration.getMicrosecondClock()) MUST be in MICROSECONDS.
        // WalPurgeJob constructs with getMicrosecondClock() by default (the single-arg constructor) and
        // passes t = clock.getTicks() here. If a future test or constructor ever injects a millisecond
        // clock, the age comparison will silently read 1000× too small and the W-window bound will break.
        // Keep both sources as MicrosecondClock (extends Clock); never inject a MillisecondClock here.
        if (pendingDurableSeqTxn < 0) {
            return false;
        }
        final long elapsedMicros = nowMicros - pendingSinceMicros;
        if (elapsedMicros >= 0 && elapsedMicros < windowUs) {
            return false; // still within the RPO budget — leave it for a later sweep / the next commit
        }
        // A backwards wall-clock step is not evidence that the batch became younger. Flush immediately
        // rather than extending the idle-tail RPO until the clock catches up.
        flushPendingDurable();
        return true;
    }

    /**
     * Clear the pending group-commit fields AND deregister from the background flush queue, ALL under the
     * writer monitor. This restores the invariant "pending is cleared before any fd is closed" on every
     * teardown path: a background flusher sweep that captured this writer reference before the deregister
     * (the {@link ConcurrentHashMap#newKeySet()} iterator is weakly consistent) will enter the synchronized
     * block, find {@code pendingDurableSeqTxn == -1}, and return immediately without touching any fd.
     *
     * <p>Must be called from ALL teardown paths ({@code cleanupBeforeClose} distressed branch, flush-failure
     * catch, and {@code doClose} belt-and-suspenders) BEFORE closing any column/events fds. {@code doClose}
     * is not itself synchronized, so it MUST route through this helper rather than calling {@code unregister}
     * directly.
     */
    private synchronized void dropPendingDurable() {
        pendingDurableSeqTxn = -1L;
        pendingSinceMicros = -1L;
        pendingLoSeqTxn = -1L;
        // Do NOT remove this writer's contiguous-prefix pin: this teardown did NOT device-flush the batch, so
        // right now its shared sequencer records are still page-cache-only and removing the pin would let the
        // durable-ack frontier advance over them (the CRITICAL-2 over-claim).
        //
        // Instead ORPHAN it. Nothing will ever call markWriterDurable(walId) for a torn-down writer, so a bare
        // "leave the pin" froze the frontier at min(pending)-1 for the rest of the process lifetime. An orphan
        // keeps the honest floor NOW but is reaped by the next peer flush, which fdatasyncs the whole shared
        // sequencer log and therefore makes our already-sequenced txns durable too (private WAL dependencies
        // were fdatasync'd before sequencing). See SeqTxnTracker.orphanWriterPending.
        //
        // NULL GUARD (load-bearing): this method runs on PARTIALLY-CONSTRUCTED writers. The constructor's
        // catch block calls dropPendingDurable() then doClose(false), and `seqTxnTracker` is assigned INSIDE
        // that same try — so any failure before that assignment (lockWal / mkWalDir / getTableMetadata, all
        // reachable when a test or a real fault makes file operations fail) reaches here with a null tracker.
        // An NPE here would REPLACE the constructor's real exception and skip doClose(false), leaking the
        // writer. Such a writer never committed, so it holds no pin and there is nothing to orphan.
        if (seqTxnTracker != null) {
            seqTxnTracker.orphanWriterPending(walId);
        }
        sequencer.getWalGroupCommitFlushQueue().unregister(this);
    }

    /**
     * TEST-ONLY crash-simulation seam (group-commit RPO oracle): model a POWER LOSS by distressing this
     * writer and dropping its pending group-commit state WITHOUT a device flush, so a subsequent
     * {@link #close()} takes the distressed path ({@code cleanupBeforeClose} early-returns, skipping
     * {@code flushPendingDurable}). This reproduces "the process died before the batch fdatasync ran": the
     * un-flushed tail is NOT made durable and {@code localDurableSeqTxn} is NOT advanced over it — exactly
     * what a real power loss leaves. Only flips the existing {@code distressed} state (a genuine production
     * failure mode) and clears the pending fields + flush-queue registration; it injects no new behaviour.
     */
    @TestOnly
    public synchronized void simulatePowerLossDropPending() {
        distressed = true;
        pendingDurableSeqTxn = -1L;
        pendingSinceMicros = -1L;
        pendingLoSeqTxn = -1L;
        // Orphan (do not remove) the shared tracker pin, exactly as dropPendingDurable does: a power loss did
        // NOT flush the batch, so the durable-ack frontier must stay honestly behind our un-flushed seqTxn
        // until some writer's real fdatasync of the shared sequencer log covers it.
        seqTxnTracker.orphanWriterPending(walId);
        sequencer.getWalGroupCommitFlushQueue().unregister(this);
    }

    /**
     * Cancels a columnar write operation. Package-private for columnar appender.
     */
    void cancelColumnarWrite(long startRowId) {
        setAppendPosition(startRowId);
    }

    /**
     * Finishes a columnar write operation. Package-private for columnar appender.
     */
    void finishColumnarWrite(int rowCount, long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        // Fill in nulls for any columns that weren't written
        long lastExpectedRow = segmentRowCount + rowCount - 1;
        for (int i = 0; i < columnCount; i++) {
            long lastWrittenRow = rowValueIsNotNull.getQuick(i);
            if (lastWrittenRow < lastExpectedRow) {
                if (rowCount > 0 && i == timestampIndex) {
                    throw CairoException.nonCritical()
                            .put("columnar write did not write designated timestamp column [table=")
                            .put(tableToken.getTableName())
                            .put(", column=").put(metadata.getColumnName(timestampIndex))
                            .put(']');
                }
                // Calculate how many nulls are needed
                long nullsNeeded = lastExpectedRow - Math.max(lastWrittenRow, segmentRowCount - 1);
                Runnable nullSetter = nullSetters.getQuick(i);
                for (long r = 0; r < nullsNeeded; r++) {
                    nullSetter.run();
                }
            }
        }

        if (rowCount > 0 && txnMaxTimestamp != -1 && minTimestamp < txnMaxTimestamp) {
            txnOutOfOrder = true;
        }

        // Update min/max timestamps
        if (minTimestamp < txnMinTimestamp) {
            txnMinTimestamp = minTimestamp;
        }
        if (maxTimestamp > txnMaxTimestamp) {
            txnMaxTimestamp = maxTimestamp;
        }
        txnOutOfOrder |= outOfOrder;

        // Update row count
        segmentRowCount += rowCount;
    }

    /**
     * Returns the aux memory for a column. Used by columnar appender.
     */
    MemoryMA getAuxColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getAuxColumnOffset(column));
    }

    /**
     * Marks a symbol column as containing a NULL value. Package-private for
     * columnar appender so the WAL event includes the null flag change.
     */
    void markSymbolMapNull(int columnIndex) {
        if (!symbolMapNullFlags.get(columnIndex)) {
            symbolMapNullFlags.set(columnIndex, true);
            symbolMapNullFlagsChanged.set(columnIndex, true);
        }
    }

    /**
     * Resolves a symbol value to its key. Package-private for columnar appender.
     *
     * @param columnIndex     the column index
     * @param symbolValue     the symbol value to resolve
     * @param symbolMapReader the symbol map reader
     * @return the symbol key
     */
    int resolveSymbol(int columnIndex, CharSequence symbolValue, SymbolMapReader symbolMapReader) {
        if (symbolValue == null) {
            markSymbolMapNull(columnIndex);
            return SymbolTable.VALUE_IS_NULL;
        }

        final var utf16Map = symbolMaps.getQuick(columnIndex);
        final int hashCode = Chars.hashCode(symbolValue);
        final int index = utf16Map.keyIndex(symbolValue, hashCode);
        if (index > -1) {
            int key = symbolMapReader.keyOf(symbolValue);
            if (key == SymbolTable.VALUE_NOT_FOUND) {
                // Add it to in-memory symbol map
                final int initialSymCount = initialSymbolCounts.get(columnIndex);
                key = initialSymCount + localSymbolIds.get(columnIndex);
                utf16Map.putAt(index, symbolValue, key, hashCode);
                localSymbolIds.increment(columnIndex);
            } else {
                utf16Map.putAt(index, symbolValue, key, hashCode);
            }
            return key;
        } else {
            return utf16Map.valueAt(index);
        }
    }

    protected void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private static class ConversionSymbolMapWriter implements SymbolMapWriterLite {
        private int columnIndex;
        private IntList localSymbolIds;
        private DirectCharSequenceIntHashMap symbolHashMap;
        private SymbolMapReader symbolMapReader;

        @Override
        public int resolveSymbol(CharSequence value) {
            return putSym0(columnIndex, value, symbolMapReader);
        }

        private int putSym0(int columnIndex, CharSequence utf16Value, SymbolMapReader symbolMapReader) {
            int key;
            if (utf16Value != null) {
                final var utf16Map = symbolHashMap;
                final int hashCode = Chars.hashCode(utf16Value);
                final int index = utf16Map.keyIndex(utf16Value, hashCode);
                if (index > -1) {
                    key = symbolMapReader.keyOf(utf16Value);
                    if (key == SymbolTable.VALUE_NOT_FOUND) {
                        // Add it to in-memory symbol map
                        // Locally added symbols must have a continuous range of keys
                        key = localSymbolIds.get(columnIndex);
                        utf16Map.putAt(index, utf16Value, key, hashCode);
                        localSymbolIds.increment(columnIndex);
                    } else {
                        utf16Map.putAt(index, utf16Value, key, hashCode);
                    }
                } else {
                    key = utf16Map.valueAt(index);
                }
            } else {
                key = SymbolTable.VALUE_IS_NULL;
            }
            return key;
        }

        void of(WalWriter writer, int columnIndex) {
            this.columnIndex = columnIndex;
            this.symbolMapReader = writer.getSymbolMapReader(columnIndex);
            this.symbolHashMap = writer.symbolMaps.getQuick(columnIndex);
            this.localSymbolIds = writer.localSymbolIds;
        }
    }


    private static class ConversionSymbolTable implements SymbolTable {
        private final IntList symbols = new IntList();
        private int symbolCountWatermark;
        private DirectCharSequenceIntHashMap symbolHashMap;
        private SymbolMapReader symbolMapReader;

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            if (key == SymbolTable.VALUE_IS_NULL) {
                return null;
            }
            if (key < symbolCountWatermark) {
                return symbolMapReader.valueOf(key);
            } else {
                int keyOffset = symbols.get(key - symbolCountWatermark);
                return symbolHashMap.getKey(keyOffset);
            }
        }

        void of(WalWriter writer, int columnIndex) {
            this.symbolMapReader = writer.getSymbolMapReader(columnIndex);
            this.symbolCountWatermark = writer.getSymbolCountWatermark(columnIndex);

            symbols.clear();
            symbolHashMap = writer.symbolMaps.getQuick(columnIndex);

            int remapSize = writer.localSymbolIds.get(columnIndex);
            if (remapSize > 0) {
                symbols.setPos(remapSize);
                for (int offset = symbolHashMap.nextOffset(); offset >= 0; offset = symbolHashMap.nextOffset(offset)) {
                    int index = symbolHashMap.get(offset);
                    if (index >= symbolCountWatermark) {
                        symbols.extendAndSet(index - symbolCountWatermark, offset);
                    }
                }
            }
        }
    }

    private class MetadataValidatorService implements MetadataServiceStub {
        public long structureVersion;

        @Override
        public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize, byte indexType) {
            if (metadata.getColumnIndexQuiet(columnName) < 0) {
                throw CairoException.nonCritical().put("column does not exist [name=").put(columnName).put(']');
            }
            structureVersion++;
        }

        @Override
        public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize, byte indexType, @Nullable ObjList<CharSequence> coveringColumnNames) {
            // Validation only checks the indexed column exists. INCLUDE
            // column validity (existence, no self-reference, no duplicates)
            // is enforced at SQL compile time in
            // SqlCompilerImpl.validateAndAddCoveringColumns.
            addIndex(columnName, indexValueBlockSize, indexType);
        }

        @Override
        public void addColumn(
                CharSequence columnName,
                int columnType,
                int symbolCapacity,
                boolean symbolCacheFlag,
                byte indexType,
                int indexValueBlockCapacity,
                boolean isSequential,
                boolean isDedupKey,
                SecurityContext securityContext
        ) {
            validateNewColumnName(columnName);
            validateNewColumnType(columnType);
            structureVersion++;
        }

        @Override
        public void changeColumnType(CharSequence columnName, int newType, int symbolCapacity, boolean symbolCacheFlag, byte indexType, int indexValueBlockCapacity, boolean isSequential, SecurityContext securityContext) {
            int columnIndex = validateExistingColumnName(columnName, "cannot change type");
            validateNewColumnType(newType);
            int existingType = metadata.getColumnType(columnIndex);
            if (existingType == newType) {
                throw CairoException.nonCritical().put("column '").put(columnName)
                        .put("' type is already '").put(ColumnType.nameOf(newType)).put('\'');
            }
            structureVersion++;
        }

        @Override
        public void disableDeduplication() {
            structureVersion++;
        }

        @Override
        public boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
            boolean isSubsetOfOldKeys = true;
            for (int i = 0, n = columnsIndexes.size(); i < n; i++) {
                int columnIndex = (int) columnsIndexes.get(i);
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType < 0) {
                    throw CairoException.nonCritical().put("cannot use dropped column for deduplication [column=").put(metadata.getColumnName(columnIndex)).put(']');
                }
                isSubsetOfOldKeys &= metadata.isDedupKey(columnIndex);
            }
            structureVersion++;
            return isSubsetOfOldKeys;
        }

        @Override
        public TableRecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public int getTimestampType() {
            return metadata.getTimestampType();
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnName, SecurityContext securityContext) {
            validateExistingColumnName(columnName, "cannot remove");
            structureVersion++;
        }

        @Override
        public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext) {
            validateExistingColumnName(columnName, "cannot rename");
            int columnIndexNew = metadata.getColumnIndexQuiet(newName);
            if (columnIndexNew > -1) {
                throw CairoException.nonCritical().put("cannot rename, column with the name already exists [table=").put(tableToken.getTableName())
                        .put(", newName=").put(newName).put(']');
            }
            if (!TableUtils.isValidColumnName(newName, newName.length())) {
                throw CairoException.nonCritical().put("invalid column name: ").put(newName);
            }
            structureVersion++;
        }

        @Override
        public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
            // this check deal with concurrency
            if (!Chars.equalsIgnoreCaseNc(fromNameTable, metadata.getTableToken().getTableName())) {
                throw CairoException.tableDoesNotExist(fromNameTable);
            }
            structureVersion++;
        }

        public void startAlterValidation() {
            structureVersion = getColumnStructureVersion();
        }

        private int validateExistingColumnName(CharSequence columnName, String errorPrefix) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex < 0) {
                throw CairoException.nonCritical().put(errorPrefix).put(", column does not exist [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName).put(']');
            }
            if (columnIndex == metadata.getTimestampIndex()) {
                throw CairoException.nonCritical().put(errorPrefix).put(" designated timestamp column [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName).put(']');
            }
            return columnIndex;
        }

        private void validateNewColumnName(CharSequence columnName) {
            if (!TableUtils.isValidColumnName(columnName, columnName.length())) {
                throw CairoException.nonCritical().put("invalid column name: ").put(columnName);
            }
            if (metadata.getColumnIndexQuiet(columnName) > -1) {
                throw CairoException.duplicateColumn(columnName);
            }
        }

        private void validateNewColumnType(int columnType) {
            if (columnType <= 0) {
                throw CairoException.nonCritical().put("invalid column type: ").put(columnType);
            }
        }
    }

    private class MetadataWriterService implements MetadataServiceStub {

        @Override
        public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize, byte indexType) {
            // WAL writer accepts add-index without local changes — the sequencer
            // metadata is updated when the WAL transaction is applied.
        }

        @Override
        public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize, byte indexType, @Nullable ObjList<CharSequence> coveringColumnNames) {
            // ADD INDEX (with or without INCLUDE) is non-structural for the
            // WAL writer's local metadata — see comment on the 3-arg
            // override. Sequencer metadata captures the INCLUDE list via
            // SequencerMetadataService.addIndex's 4-arg override.
            addIndex(columnName, indexValueBlockSize, indexType);
        }

        @Override
        public void addColumn(
                CharSequence columnName,
                int columnType,
                int symbolCapacity,
                boolean symbolCacheFlag,
                byte indexType,
                int indexValueBlockCapacity,
                boolean isSequential,
                boolean isDedupKey,
                SecurityContext securityContext
        ) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);

            if (columnIndex < 0 || metadata.getColumnType(columnIndex) < 0) {
                long uncommittedRows = getUncommittedRowCount();
                if (currentTxnStartRowNum > 0) {
                    // Roll last transaction to new segment
                    rollUncommittedToNewSegment(-1, -1);
                }

                if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                    long segmentRowCount = getUncommittedRowCount();
                    metadata.addColumn(
                            columnName,
                            columnType,
                            isDedupKey,
                            symbolCacheFlag,
                            symbolCapacity
                    );
                    columnCount = metadata.getColumnCount();
                    columnIndex = columnCount - 1;
                    // create column file
                    configureColumn(columnIndex, columnType);
                    if (ColumnType.isSymbol(columnType)) {
                        configureSymbolMapWriter(columnIndex, columnName, 0, -1);
                    }

                    if (!rollSegmentOnNextRow) {
                        // this means we have rolled uncommitted rows to a new segment already
                        // we should switch metadata to this new segment
                        path.trimTo(pathSize).slash().put(segmentId);
                        // this will close old _meta file and create the new one
                        // DATA BEFORE POINTER. The segment _meta published by switchTo DECLARES this column;
                        // apply then requires the column's file to exist for the committed row range. Doing
                        // the switch first left a window where the durable metadata named a file that had
                        // not been created yet -- a crash in it (the _meta.swp barrier) left the segment
                        // permanently unappliable, and recovery suspended the table with "WAL segment column
                        // too short for committed row range [... actual=-1]" (actual=-1 being a MISSING
                        // file, not a short one). So: create the files, make their names durable by fsyncing
                        // the segment directory (openNewSegment does the same for the files IT creates; a
                        // column added to an already-open segment needs it too), and only then publish the
                        // metadata that points at them.
                        final int segPathLen = path.size();
                        openColumnFiles(columnName, columnType, columnIndex, segPathLen);
                        if (walCommitMode() != CommitMode.NOSYNC) {
                            final long segDirFd = TableUtils.openRONoCache(ff, path.trimTo(segPathLen).$(), LOG);
                            ff.fsyncAndClose(segDirFd);
                        }
                        metadata.switchTo(path.trimTo(segPathLen), segPathLen, isTruncateFilesOnClose());
                        path.trimTo(pathSize);
                    }

                    // if we did not have to roll uncommitted rows to a new segment
                    // it will add the column file and switch metadata file on next row write
                    // as part of rolling to a new segment
                    if (uncommittedRows > 0) {
                        setColumnNull(columnType, columnIndex, segmentRowCount, walCommitMode());
                        if (ColumnType.isSymbol(columnType)) {
                            symbolMapNullFlagsChanged.set(columnIndex, true);
                            symbolMapNullFlags.set(columnIndex, true);
                            // Rewrite the WAL event if it was already written without the null flag.
                            if (lastSegmentTxn >= 0) {
                                lastSegmentTxn = events.rewriteLastDataRecord(
                                        lastTxnType,
                                        0,
                                        WalWriter.this.segmentRowCount,
                                        txnMinTimestamp,
                                        txnMaxTimestamp,
                                        txnOutOfOrder,
                                        lastMatViewRefreshBaseTxn,
                                        lastMatViewRefreshTimestamp,
                                        lastMatViewPeriodHi,
                                        lastReplaceRangeLowTs,
                                        lastReplaceRangeHiTs,
                                        lastDedupMode
                                );
                                events.sync(walCommitMode());
                            }
                        }
                    }

                    if (securityContext != null) {
                        ddlListener.onColumnAdded(securityContext, metadata.getTableToken(), columnName);
                    }
                    LOG.info().$("added column to WAL [path=").$substr(pathRootSize, path)
                            .$(", columnName=").$safe(columnName)
                            .$(", type=").$(ColumnType.nameOf(columnType))
                            .I$();
                } else {
                    throw CairoException.critical(0).put("column '").put(columnName)
                            .put("' was added, cannot apply commit because of concurrent table definition change");
                }
            } else {
                if (metadata.getColumnType(columnIndex) == columnType) {
                    LOG.info().$("column has already been added by another WAL [path=").$substr(pathRootSize, path)
                            .$(", columnName=").$safe(columnName)
                            .I$();
                } else {
                    throw CairoException.nonCritical().put("column '").put(columnName).put("' already exists");
                }
            }
        }

        @Override
        public void changeColumnType(
                CharSequence columnNameSeq,
                int newType,
                int symbolCapacity,
                boolean symbolCacheFlag,
                byte indexType,
                int indexValueBlockCapacity,
                boolean isSequential,
                SecurityContext securityContext
        ) {
            final int existingColumnIndex = metadata.getColumnIndexQuiet(columnNameSeq);
            if (existingColumnIndex > -1) {
                String columnName = metadata.getColumnName(existingColumnIndex);
                int existingColumnType = metadata.getColumnType(existingColumnIndex);
                if (existingColumnType > 0) {
                    if (existingColumnType != newType) {
                        // Configure new column, it will be used if the uncommitted data is rolled to a new segment
                        int newColumnIndex = columnCount;
                        configureColumn(newColumnIndex, newType);
                        if (ColumnType.isSymbol(newType)) {
                            configureSymbolMapWriter(newColumnIndex, columnName, 0, -1);
                        }
                        columnCount++;

                        long rowsRemainInCurrentSegment = currentTxnStartRowNum;
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment(existingColumnIndex, newType);

                        if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                            metadata.changeColumnType(
                                    columnName,
                                    newType,
                                    symbolCapacity,
                                    symbolCacheFlag,
                                    indexType,
                                    indexValueBlockCapacity
                            );
                            path.trimTo(pathSize).slash().put(segmentId);

                            markColumnRemoved(existingColumnIndex, existingColumnType);
                            if (!rollSegmentOnNextRow) {
                                // this means we have rolled uncommitted rows to a new segment already
                                // we should switch metadata to this new segment
                                path.trimTo(pathSize).slash().put(segmentId);
                                // CREATE BEFORE PUBLISH -- the same rule the addColumn and renameColumn
                                // in-segment paths above already follow, and the last place that still got it
                                // backwards. The segment _meta written by switchTo describes the column with
                                // its NEW type, and apply sizes the files from that type: converting to a
                                // var-size type makes apply demand an aux (.i) vector that only
                                // openColumnFiles creates. Publishing first left a window where the durable
                                // segment _meta named a VARCHAR column whose .i file did not exist yet, and a
                                // crash inside it (the _meta.swp barrier) made the segment permanently
                                // unappliable: "WAL segment column too short for committed row range
                                // [... actual=-1]", actual=-1 being a MISSING file, and the table suspended
                                // for good while the sequencer ran ahead.
                                //
                                // So: create the files, fsync the segment directory so their names are
                                // durable, and only then publish the metadata that points at them.
                                final int segPathLen = path.size();
                                if (segmentRowCount == 0) {
                                    openColumnFiles(columnName, newType, newColumnIndex, segPathLen);
                                }
                                if (walCommitMode() != CommitMode.NOSYNC) {
                                    final long segDirFd = TableUtils.openRONoCache(ff, path.trimTo(segPathLen).$(), LOG);
                                    ff.fsyncAndClose(segDirFd);
                                }
                                // this will close old _meta file and create the new one
                                metadata.switchTo(path.trimTo(segPathLen), segPathLen, isTruncateFilesOnClose());
                            }

                            if (rowsRemainInCurrentSegment == 0) {
                                // if we did not have to roll uncommitted rows to a new segment
                                // remove .i files when converting var type to fixed
                                if (ColumnType.isVarSize(existingColumnType) && !ColumnType.isVarSize(newType)) {
                                    path.trimTo(pathSize).slash().put(segmentId);
                                    LPSZ lpsz = iFile(path, columnName);
                                    if (ff.exists(lpsz)) {
                                        ff.remove(lpsz);
                                    }
                                }
                            }
                            path.trimTo(pathSize);
                        } else {
                            throw CairoException.critical(0).put("column '").put(columnName)
                                    .put("' was removed, cannot apply commit because of concurrent table definition change");
                        }
                    } else {
                        throw CairoException.nonCritical().put("column '").put(columnName)
                                .put("' type is already '").put(ColumnType.nameOf(newType)).put('\'');
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column '").put(columnNameSeq).put("' does not exist");
            }
        }

        @Override
        public void disableDeduplication() {
            metadata.disableDeduplicate();
        }

        @Override
        public boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
            return metadata.enableDeduplicationWithUpsertKeys();
        }

        @Override
        public TableRecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public int getTimestampType() {
            return metadata.getTimestampType();
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnNameSeq, SecurityContext securityContext) {
            final int columnIndex = metadata.getColumnIndexQuiet(columnNameSeq);
            if (columnIndex > -1) {
                String columnName = metadata.getColumnName(columnIndex);
                int type = metadata.getColumnType(columnIndex);
                if (type > 0) {
                    if (currentTxnStartRowNum > 0) {
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment(-1, -1);
                    }

                    if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                        int index = metadata.getColumnIndex(columnName);
                        metadata.removeColumn(columnName);
                        columnCount = metadata.getColumnCount();

                        if (!rollSegmentOnNextRow) {
                            // this means we have rolled uncommitted rows to a new segment already
                            // we should switch metadata to this new segment
                            path.trimTo(pathSize).slash().put(segmentId);
                            // this will close old _meta file and create the new one
                            metadata.switchTo(path, path.size(), isTruncateFilesOnClose());
                        }
                        // if we did not have to roll uncommitted rows to a new segment
                        // it will switch metadata file on next row write
                        // as part of rolling to a new segment

                        markColumnRemoved(index, type);

                        try {
                            ddlListener.onColumnDropped(metadata.getTableToken(), columnName);
                        } finally {
                            path.trimTo(pathSize);
                        }
                        LOG.info().$("removed column from WAL [path=").$substr(pathRootSize, path).$(Files.SEPARATOR).$(segmentId)
                                .$(", columnName=").$safe(columnName).I$();
                    } else {
                        throw CairoException.critical(0)
                                .put("column was removed, cannot apply commit because of concurrent table definition change")
                                .put(" [column=").put(columnName).put(']');
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column does not exist [column=").put(columnNameSeq).put(']');
            }
        }

        @Override
        public void renameColumn(
                @NotNull CharSequence columnNameSeq,
                @NotNull CharSequence newColumnName,
                SecurityContext securityContext
        ) {
            final int columnIndex = metadata.getColumnIndexQuiet(columnNameSeq);
            if (columnIndex > -1) {
                String columnName = metadata.getColumnName(columnIndex);
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType > 0) {
                    if (currentTxnStartRowNum > 0) {
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment(-1, -1);
                    }

                    if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                        metadata.renameColumn(columnName, newColumnName);
                        // We are not going to do any special for symbol readers which point
                        // to the files in the root of the table.
                        // We keep the symbol readers open against files with old name.
                        // Inconsistency between column name and symbol file names in the root
                        // does not matter, these files are for re-lookup only for the WAL writer
                        // and should not be serialised to the WAL segment.

                        if (!rollSegmentOnNextRow) {
                            // this means we have rolled uncommitted rows to a new segment already
                            // we should switch metadata to this new segment
                            path.trimTo(pathSize).slash().put(segmentId);
                            // RENAME BEFORE PUBLISH. The segment _meta written by switchTo names the column
                            // by its NEW name, and apply resolves that name to a file. Switching first left
                            // a window where durable metadata named new_col_N.d while the file on disk was
                            // still old_name.d: a crash in it (the _meta.swp barrier) made the segment
                            // permanently unappliable and suspended the table with "WAL segment column too
                            // short for committed row range [... actual=-1]" -- actual=-1 being a MISSING
                            // file. Move the names into place first, fsync the segment directory so those
                            // names are durable (a rename publishes a dentry in the PARENT, which needs its
                            // own barrier), and only then publish the metadata that points at them.
                            final int segPathLen = path.size();
                            renameColumnFiles(columnType, columnName, newColumnName);
                            if (walCommitMode() != CommitMode.NOSYNC) {
                                final long segDirFd = TableUtils.openRONoCache(ff, path.trimTo(segPathLen).$(), LOG);
                                ff.fsyncAndClose(segDirFd);
                            }
                            metadata.switchTo(path.trimTo(segPathLen), segPathLen, isTruncateFilesOnClose());
                        }
                        // if we did not have to roll uncommitted rows to a new segment
                        // it will switch metadata file on next row write
                        // as part of rolling to a new segment

                        try {
                            ddlListener.onColumnRenamed(metadata.getTableToken(), columnName, newColumnName);
                        } finally {
                            path.trimTo(pathSize);
                        }
                        LOG.info().$("renamed column in WAL [path=")
                                .$substr(pathRootSize, path).$(Files.SEPARATOR).$(segmentId)
                                .$(", columnName=").$safe(columnName)
                                .$(", newColumnName=").$safe(newColumnName)
                                .I$();
                    } else {
                        throw CairoException.critical(0)
                                .put("column was removed, cannot apply commit because of concurrent table definition change")
                                .put(" [column=").put(columnName).put(']');
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column does not exist [column=")
                        .put(columnNameSeq).put(']');
            }
        }

        @Override
        public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
            tableToken = metadata.getTableToken().renamed(Chars.toString(toTableName));
            metadata.renameTable(tableToken);
        }
    }

    private class RowImpl implements TableWriter.Row {
        private final Decimal256 decimal256Sink = new Decimal256();
        private final StringSink tempSink = new StringSink();
        private final Utf8StringSink tempUtf8Sink = new Utf8StringSink();
        private long timestamp;

        @Override
        public void append() {
            rowAppend(nullSetters, timestamp);
        }

        @Override
        public void cancel() {
            setAppendPosition(segmentRowCount);
        }

        @Override
        public void putArray(int columnIndex, @NotNull ArrayView arrayView) {
            ArrayTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex),
                    getPrimaryColumn(columnIndex),
                    arrayView
            );
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(address, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(sequence));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            getPrimaryColumn(columnIndex).putBool(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            getPrimaryColumn(columnIndex).putByte(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putChar(int columnIndex, char value) {
            getPrimaryColumn(columnIndex).putChar(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDate(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        @Override
        public void putDecimal(int columnIndex, Decimal256 value) {
            int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putDecimal(columnIndex, value, type, this);
        }

        @Override
        public void putDecimal128(int columnIndex, long high, long low) {
            getPrimaryColumn(columnIndex).putDecimal128(high, low);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
            getPrimaryColumn(columnIndex).putDecimal256(hh, hl, lh, ll);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDecimalChar(int columnIndex, char decimalValue) {
            int columnType = metadata.getColumnType(columnIndex);
            WriterRowUtils.putDecimalChar(columnIndex, decimal256Sink, decimalValue, columnType, this);
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
            int columnType = metadata.getColumnType(columnIndex);
            WriterRowUtils.putDecimalStr(columnIndex, decimal256Sink, decimalValue, columnType, this);
        }

        @Override
        public void putDecimalVarchar(int columnIndex, Utf8Sequence decimalValue) {
            int columnType = metadata.getColumnType(columnIndex);
            WriterRowUtils.putDecimalVarchar(columnIndex, decimal256Sink, decimalValue, columnType, this);
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            getPrimaryColumn(columnIndex).putDouble(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            getPrimaryColumn(columnIndex).putFloat(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
            int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoHash(columnIndex, value, type, this);
        }

        @Override
        public void putGeoHashDeg(int columnIndex, double lat, double lon) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoHash(columnIndex, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type, this);
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence hash) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoStr(columnIndex, hash, type, this);
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence hash) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoVarchar(columnIndex, hash, type, this);
        }

        @Override
        public void putIPv4(int columnIndex, int value) {
            putInt(columnIndex, value);
        }

        @Override
        public void putInt(int columnIndex, int value) {
            getPrimaryColumn(columnIndex).putInt(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            getPrimaryColumn(columnIndex).putLong(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            MemoryMA primaryColumn = getPrimaryColumn(columnIndex);
            primaryColumn.putLong(lo);
            primaryColumn.putLong(hi);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            getPrimaryColumn(columnIndex).putLong256(l0, l1, l2, l3);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            getPrimaryColumn(columnIndex).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            getPrimaryColumn(columnIndex).putLong256(hexString);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            getPrimaryColumn(columnIndex).putLong256(hexString, start, end);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
            getPrimaryColumn(columnIndex).putLong256Utf8(hexString);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256Utf8(int columnIndex, Utf8Sequence hexString) {
            if (hexString == null) {
                putLong256(columnIndex, (CharSequence) null);
                return;
            }
            if (hexString instanceof DirectUtf8Sequence) {
                putLong256Utf8(columnIndex, (DirectUtf8Sequence) hexString);
                return;
            }
            // Long256 hex strings are always ASCII
            putLong256(columnIndex, hexString.asAsciiCharSequence());
        }

        @Override
        public void putShort(int columnIndex, short value) {
            getPrimaryColumn(columnIndex).putShort(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, char value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value, pos, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStrUtf8(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStrUtf8(int columnIndex, Utf8Sequence value) {
            if (value instanceof DirectUtf8Sequence directValue) {
                putStrUtf8(columnIndex, directValue);
                return;
            }
            putStr(columnIndex, value != null ? Utf8s.utf8ToUtf16OrThrow(value, tempSink) : null);
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            final SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
            if (symbolMapReader != null) {
                putSym0(columnIndex, value, symbolMapReader);
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void putSym(int columnIndex, char value) {
            CharSequence str = SingleCharCharSequence.get(value);
            putSym(columnIndex, str);
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
            putInt(columnIndex, key);
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
            // this method will write column name to the buffer if it has to be UTF-8 decoded
            // otherwise it will write nothing.
            final SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
            if (symbolMapReader != null) {
                Utf8StringIntHashMap utf8Map = utf8SymbolMaps.getQuick(columnIndex);
                int index = utf8Map.keyIndex(value);
                if (index < 0) {
                    getPrimaryColumn(columnIndex).putInt(utf8Map.valueAt(index));
                    setRowValueNotNull(columnIndex);
                } else {
                    // slow path, symbol is not in utf8 cache
                    utf8Map.putAt(
                            index,
                            Utf8String.newInstance(value),
                            putSymUtf8Slow(columnIndex, value, symbolMapReader)
                    );
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            if (columnIndex == timestampIndex) {
                setTimestamp(value);
            } else {
                putLong(columnIndex, value);
            }
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuidStr) {
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            putLong128(columnIndex, uuid.getLo(), uuid.getHi());
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuidStr) {
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            putLong128(columnIndex, uuid.getLo(), uuid.getHi());
        }

        @Override
        public void putVarchar(int columnIndex, char value) {
            tempUtf8Sink.clear();
            tempUtf8Sink.put(value);
            VarcharTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                    tempUtf8Sink
            );
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            VarcharTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                    value
            );
            setRowValueNotNull(columnIndex);
        }

        private MemoryMA getPrimaryColumn(int columnIndex) {
            return columns.getQuick(getDataColumnOffset(columnIndex));
        }

        private MemoryMA getSecondaryColumn(int columnIndex) {
            return columns.getQuick(getAuxColumnOffset(columnIndex));
        }

        private int putSym0(int columnIndex, CharSequence utf16Value, SymbolMapReader symbolMapReader) {
            int key;
            if (utf16Value != null) {
                final var utf16Map = symbolMaps.getQuick(columnIndex);
                final int hashCode = Chars.hashCode(utf16Value);
                final int index = utf16Map.keyIndex(utf16Value, hashCode);
                if (index > -1) {
                    key = symbolMapReader.keyOf(utf16Value);
                    if (key == SymbolTable.VALUE_NOT_FOUND) {
                        // Add it to in-memory symbol map
                        // Locally added symbols must have a continuous range of keys
                        final int initialSymCount = initialSymbolCounts.get(columnIndex);
                        key = initialSymCount + localSymbolIds.get(columnIndex);
                        utf16Map.putAt(index, utf16Value, key, hashCode);
                        localSymbolIds.increment(columnIndex);
                    } else {
                        utf16Map.putAt(index, utf16Value, key, hashCode);
                    }
                } else {
                    key = utf16Map.valueAt(index);
                }
            } else {
                key = SymbolTable.VALUE_IS_NULL;
                if (!symbolMapNullFlags.get(columnIndex)) {
                    symbolMapNullFlags.set(columnIndex, true);
                    symbolMapNullFlagsChanged.set(columnIndex, true);
                }
            }

            getPrimaryColumn(columnIndex).putInt(key);
            setRowValueNotNull(columnIndex);
            return key;
        }

        private int putSymUtf8Slow(
                int columnIndex,
                DirectUtf8Sequence utf8Value,
                SymbolMapReader symbolMapReader
        ) {
            return putSym0(
                    columnIndex,
                    Utf8s.directUtf8ToUtf16(utf8Value, tempSink),
                    symbolMapReader
            );
        }

        private void setTimestamp(long value) {
            getPrimaryColumn(timestampIndex).putLong128(value, segmentRowCount);
            setRowValueNotNull(timestampIndex);
            this.timestamp = value;
        }
    }
}
