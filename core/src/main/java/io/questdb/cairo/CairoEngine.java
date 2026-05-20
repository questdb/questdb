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

package io.questdb.cairo;

import io.questdb.ConfigReloader;
import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.Metrics;
import io.questdb.Telemetry;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.frm.file.FrameFactory;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRecovery;
import io.questdb.cairo.lv.LiveViewRegistry;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.lv.LiveViewStateReader;
import io.questdb.cairo.lv.LiveViewStateStore;
import io.questdb.cairo.lv.LiveViewStateStoreImpl;
import io.questdb.cairo.lv.LiveViewTableStructure;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.mv.DependentViewGraph;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.mv.MatViewTimerTask;
import io.questdb.cairo.mv.NoOpMatViewStateStore;
import io.questdb.cairo.pool.AbstractMultiTenantPool;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.pool.SequencerMetadataPool;
import io.questdb.cairo.pool.SqlCompilerPool;
import io.questdb.cairo.pool.TableMetadataPool;
import io.questdb.cairo.pool.ViewWalWriterPool;
import io.questdb.cairo.pool.WalWriterPool;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.view.NoOpViewStateStore;
import io.questdb.cairo.view.ViewCompilerExecutionContext;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewGraph;
import io.questdb.cairo.view.ViewMetadata;
import io.questdb.cairo.view.ViewState;
import io.questdb.cairo.view.ViewStateStore;
import io.questdb.cairo.view.ViewStateStoreImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.DefaultDurableAckRegistry;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.DefaultWalListener;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cairo.wal.ViewWalWriter;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalListener;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cutlass.qwp.codec.QwpServerInfoProvider;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyImportContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryCacheBuilder;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerFactoryImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.ops.CreateLiveViewOperation;
import io.questdb.griffin.engine.ops.CreateMatViewOperation;
import io.questdb.griffin.engine.ops.CreateViewOperation;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.NoOpQueue;
import io.questdb.mp.Queue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.preferences.SettingsStore;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Transient;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.AbstractTelemetryTask;
import io.questdb.tasks.TelemetryMatViewTask;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.griffin.CompiledQuery.*;

public class CairoEngine implements Closeable, WriterSource {
    public static final String REASON_BUSY_READER = "busyReader";
    public static final String REASON_BUSY_SEQUENCER_METADATA_POOL = "busySequencerMetaPool";
    public static final String REASON_BUSY_TABLE_READER_METADATA_POOL = "busyTableReaderMetaPool";
    public static final String REASON_CHECKPOINT_IN_PROGRESS = "checkpointInProgress";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private static final int MAX_SLEEP_MILLIS = 250;
    private static final ThreadLocal<ObjList<LiveViewInstance>> tlInvalidateSink = new ThreadLocal<>(ObjList::new);
    private static final ThreadLocal<MatViewRefreshTask> tlMatViewRefreshTask = new ThreadLocal<>(MatViewRefreshTask::new);
    protected final CairoConfiguration configuration;
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final BackupSeqPartLock backupSeqPartLock = new BackupSeqPartLock();
    private final DatabaseCheckpointAgent checkpointAgent;
    private final CopyExportContext copyExportContext;
    private final CopyImportContext copyImportContext;
    private final ConcurrentHashMap<TableToken> createTableLock = new ConcurrentHashMap<>();
    private final DataID dataID;
    private final FunctionFactoryCache ffCache;
    private final LiveViewRegistry liveViewRegistry = new LiveViewRegistry();
    private final LiveViewStateStore liveViewStateStore = new LiveViewStateStoreImpl();
    private final DependentViewGraph dependentViewGraph;
    private final Queue<MatViewTimerTask> matViewTimerQueue;
    private final MessageBusImpl messageBus;
    private final MetadataCache metadataCache;
    private final Metrics metrics;
    private final PartitionOverwriteControl partitionOverwriteControl = new PartitionOverwriteControl();
    private final QueryRegistry queryRegistry;
    private final ReaderPool readerPool;
    private final RecentWriteTracker recentWriteTracker;
    private final SqlExecutionContext rootExecutionContext;
    private final TxnScoreboardPool scoreboardPool;
    private final SequencerMetadataPool sequencerMetadataPool;
    private final SettingsStore settingsStore;
    private final SqlCompilerPool sqlCompilerPool;
    private final TableFlagResolver tableFlagResolver;
    private final IDGenerator tableIdGenerator;
    private final TableMetadataPool tableMetadataPool;
    private final TableNameRegistry tableNameRegistry;
    private final TableSequencerAPI tableSequencerAPI;
    private final ObjList<Telemetry<? extends AbstractTelemetryTask>> telemetries;
    private final Telemetry<TelemetryTask> telemetry;
    private final Telemetry<TelemetryMatViewTask> telemetryMatView;
    private final Telemetry<TelemetryWalTask> telemetryWal;
    // initial value of unpublishedWalTxnCount is 1 because we want to scan for non-applied WAL transactions on startup
    private final AtomicLong unpublishedWalTxnCount = new AtomicLong(1);
    private final ViewGraph viewGraph;
    private final ViewWalWriterPool viewWalWriterPool;
    private final SimpleWaitingLock walPurgeJobLock = new SimpleWaitingLock();
    private final WalWriterPool walWriterPool;
    private final WriterPool writerPool;
    private volatile boolean closing;
    private @NotNull ConfigReloader configReloader = new ConfigReloader() {
        @Override
        public boolean reload() {
            return false;
        }

        @Override
        public void unwatch(long watchId) {

        }

        @Override
        public long watch(Listener listener) {
            return WatchRegistry.UNREGISTERED;
        }
    }; // no-op
    private @NotNull DdlListener ddlListener = DefaultDdlListener.INSTANCE;
    private volatile @NotNull DurableAckRegistry durableAckRegistry = DefaultDurableAckRegistry.INSTANCE;
    private FrameFactory frameFactory;
    private @NotNull MatViewStateStore matViewStateStore = NoOpMatViewStateStore.INSTANCE;
    private volatile Runnable recentWriteTrackerHydrationCallback;
    private @NotNull ViewStateStore viewStateStore = NoOpViewStateStore.INSTANCE;
    private @NotNull WalDirectoryPolicy walDirectoryPolicy = DefaultWalDirectoryPolicy.INSTANCE;
    private @NotNull WalListener walListener = DefaultWalListener.INSTANCE;
    private @NotNull WalLocker walLocker;

    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, new QdbrWalLocker());
    }

    public CairoEngine(CairoConfiguration configuration, @NotNull WalLocker walLocker) {
        try {
            this.walLocker = walLocker;
            this.ffCache = new FunctionFactoryCache(configuration, getFunctionFactories());
            this.tableFlagResolver = newTableFlagResolver(configuration);
            this.configuration = configuration;
            this.copyImportContext = new CopyImportContext(this, configuration);
            this.copyExportContext = new CopyExportContext(this);
            this.tableSequencerAPI = new TableSequencerAPI(this, configuration);
            this.messageBus = new MessageBusImpl(configuration);
            this.metrics = configuration.getMetrics();
            // Message bus and metrics must be initialized before the pools.
            this.recentWriteTracker = new RecentWriteTracker(configuration.getRecentWriteTrackerCapacity());
            this.writerPool = new WriterPool(configuration, this, recentWriteTracker);
            this.scoreboardPool = new TxnScoreboardPoolV2(configuration);
            this.readerPool = new ReaderPool(configuration, scoreboardPool, messageBus, partitionOverwriteControl);
            this.sequencerMetadataPool = new SequencerMetadataPool(configuration, this);
            this.tableMetadataPool = new TableMetadataPool(configuration);
            this.walWriterPool = new WalWriterPool(configuration, this);
            this.viewWalWriterPool = new ViewWalWriterPool(configuration, this);
            this.telemetry = createTelemetry(TelemetryTask.TELEMETRY, configuration);
            this.telemetryWal = createTelemetry(TelemetryWalTask.WAL_TELEMETRY, configuration);
            this.telemetryMatView = createTelemetry(TelemetryMatViewTask.MAT_VIEW_TELEMETRY, configuration);
            this.telemetries = new ObjList<>(telemetryWal, telemetryMatView);
            if (configuration.getTelemetryConfiguration().getEnabled()) {
                // This is the only one that can be switched off by the configuration
                telemetries.add(telemetry);
            }
            this.tableIdGenerator = IDGeneratorFactory.newIDGenerator(configuration, TableUtils.TAB_INDEX_FILE_NAME, 1);
            this.checkpointAgent = new DatabaseCheckpointAgent(this);
            this.queryRegistry = new QueryRegistry(configuration);
            this.rootExecutionContext = createRootExecutionContext();
            this.matViewTimerQueue = createMatViewTimerQueue();
            this.dependentViewGraph = createDependentViewGraph();
            this.viewGraph = createViewGraph();
            this.frameFactory = new FrameFactory(configuration);
            this.dataID = DataID.open(configuration);

            // IMPORTANT: Do not reorder statements!
            // The backup recovery process needs the `dataID` (since it will set it),
            // but it's important that's not initialized yet.
            // The `recoverBackup()` logic also needs to run before any table registry loading.
            restoreBackup();

            initDataID();

            settingsStore = new SettingsStore(configuration);

            tableIdGenerator.open();
            checkpointRecover();

            // Initialize settings store after checkpoint recovery so it reads the restored file
            settingsStore.init();

            // Migrate database files.
            EngineMigration.migrateEngineTo(this, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            tableNameRegistry = createTableNameRegistry(configuration, tableFlagResolver);
            tableNameRegistry.reload();

            this.sqlCompilerPool = new SqlCompilerPool(this);
            if (configuration.isPartitionO3OverwriteControlEnabled()) {
                enablePartitionOverwriteControl();
            }
            this.metadataCache = new MetadataCache(this);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static void execute(
            SqlCompiler compiler,
            CharSequence sqlText,
            SqlExecutionContext sqlExecutionContext,
            @Nullable SCSequence eventSubSeq
    ) throws SqlException {
        CompiledQuery cq = compiler.compile(sqlText, sqlExecutionContext);
        switch (cq.getType()) {
            case CREATE_TABLE:
            case CREATE_TABLE_AS_SELECT:
            case CREATE_MAT_VIEW:
            case CREATE_VIEW:
            case CREATE_LIVE_VIEW:
            case DROP:
                assert sqlExecutionContext.getCairoEngine() == compiler.getEngine();
                try (Operation op = cq.getOperation()) {
                    assert op != null;
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                }
                break;
            case INSERT:
            case INSERT_AS_SELECT:
                insert(cq, sqlExecutionContext);
                break;
            case SELECT:
                throw SqlException.$(0, "use select()");
            default:
                try (OperationFuture future = cq.execute(eventSubSeq)) {
                    future.await();
                }
                break;
        }
    }

    public static RecordCursorFactory select(SqlCompiler compiler, CharSequence selectSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return compiler.compile(selectSql, sqlExecutionContext).getRecordCursorFactory();
    }

    /**
     * Per-function half of {@link #validateLiveViewFactory}: rejects a window function
     * the incremental refresh path cannot drive. Extracted so the reject contract can be
     * unit-tested directly - both rejects are unreachable for GA functions today
     * (multi-pass and lead/percent_rank are caught upstream, and every migrated function
     * supports snapshots), so a stub function is the only way to pin the wording.
     * <p>
     * A non-{@link WindowFunction#ZERO_PASS} function needs caching or a lookahead pass
     * that incremental refresh has no way to supply. A function without snapshot support
     * would make the refresh worker silently skip checkpoint writes for the whole view,
     * routing every restart and every O3 through full head-miss replay from
     * viewLowerBoundTimestamp; surfacing the gap at CREATE keeps that surprise off the
     * steady-state hot path.
     */
    public static void validateLiveViewWindowFunction(WindowFunction f, int position) throws SqlException {
        if (f.getPassCount() != WindowFunction.ZERO_PASS) {
            throw SqlException.$(position, "live view select may only use window functions that support incremental refresh");
        }
        if (!f.supportsSnapshot()) {
            throw SqlException.$(position, "live view select cannot use window function ")
                    .put(f.getName())
                    .put("(); incremental snapshot is not supported for this function yet");
        }
    }

    /**
     * Advances the live view's {@code lvConsumedSeqTxn} to {@code maxBaseSeqTxn} after
     * the LV's own WAL block has been applied to its on-disk table. The advance is
     * monotonic and persisted to {@code _lv.s} so WAL purge sees the latest floor across
     * restarts. Called by {@link io.questdb.cairo.lv.LiveViewRefreshJob} after each
     * inline apply, and again on the no-row branch when the refresh cycle walks past
     * non-DATA / all-filtered seqTxns and emits no LV WAL block. Synchronizes on the
     * instance to coordinate with the refresh worker's own {@code _lv.s} rewrites for
     * {@code lastProcessedSeqTxn} bookkeeping.
     * <p>
     * Persists the new value to {@code _lv.s} before mutating in-memory state — disk is
     * the contract with {@code WalPurgeJob}. On persist failure, throws
     * {@link CairoException}; the in-memory floor stays at the prior durable value so the
     * next apply re-attempts the advance from the same point.
     * <p>
     * The {@code blockFileWriter} and {@code path} are caller-supplied so the per-FLUSH-
     * cycle hot path can reuse a single instance instead of allocating both per call.
     * The caller retains ownership and may use them again after this method returns; the
     * method does not call {@code close()} on either.
     */
    public void advanceLiveViewConsumedSeqTxn(
            TableToken liveViewToken,
            long maxBaseSeqTxn,
            BlockFileWriter blockFileWriter,
            Path path
    ) {
        if (maxBaseSeqTxn < 0) {
            return;
        }
        LiveViewInstance instance = liveViewRegistry.getViewInstance(liveViewToken.getTableName());
        if (instance == null) {
            return;
        }
        synchronized (instance) {
            LiveViewStateReader reader = instance.getStateReader();
            if (maxBaseSeqTxn <= reader.getLvConsumedSeqTxn()) {
                return;
            }
            // Durability rule: _lv.s is the contract with
            // WalPurgeJob; the in-memory floor must trail disk. Persist the new value first,
            // then publish in-memory. If persist fails, the in-memory value stays at the old
            // durable floor, so the next apply re-attempts the advance from the same point.
            try {
                path.of(configuration.getDbRoot()).concat(liveViewToken).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
                blockFileWriter.of(path.$());
                LiveViewState.append(
                        reader.isInvalid(),
                        reader.getInvalidationReason(),
                        reader.getInvalidationTimestampUs(),
                        reader.getSubscribeFromSeqTxn(),
                        reader.getLastProcessedSeqTxn(),
                        reader.getAppliedWatermark(),
                        maxBaseSeqTxn,
                        reader.getBackfillState(),
                        reader.getBackfillTargetSeqTxn(),
                        blockFileWriter
                );
            } catch (Throwable t) {
                LOG.error().$("could not persist live view consumed seqTxn [view=").$(liveViewToken)
                        .$(", maxBaseSeqTxn=").$(maxBaseSeqTxn)
                        .$(", error=").$(t).I$();
                throw CairoException.critical(0).put("could not persist live view consumed seqTxn [view=")
                        .put(liveViewToken.getTableName()).put(", maxBaseSeqTxn=").put(maxBaseSeqTxn).put(']');
            }
            instance.setLvConsumedSeqTxn(maxBaseSeqTxn);
        }
    }

    public void applyTableRename(TableToken token, TableToken updatedTableToken) {
        if (updatedTableToken.isMatView() && dependentViewGraph.getViewDefinition(updatedTableToken) == null) {
            throw CairoException.nonCritical().put("materialized view has not been registered yet [name=").put(updatedTableToken.getTableName()).put(']');
        }
        tableNameRegistry.rename(token.getTableName(), updatedTableToken.getTableName(), token);
        if (token.isWal()) {
            tableSequencerAPI.applyRename(updatedTableToken);
        }
        if (updatedTableToken.isMatView()) {
            dependentViewGraph.updateToken(updatedTableToken);
        }
        enqueueCompileView(token);
        enqueueCompileView(updatedTableToken);
    }

    public void attachReader(TableReader reader) {
        // Ignore the object close() call until attached back
        readerPool.attach(reader);
    }

    public void awaitTable(String tableName, long timeout, TimeUnit timeoutUnit) {
        awaitTxn(tableName, -1, timeout, timeoutUnit);
    }

    @TestOnly
    public void awaitTxn(String tableName, long txn, long timeout, TimeUnit timeoutUnit) {
        final long startTime = configuration.getMillisecondClock().getTicks();
        long maxWait = timeoutUnit.toMillis(timeout);
        int sleep = 10;

        TableToken tableToken = null;
        long seqTxn = txn;
        long writerTxn = -1;
        while (configuration.getMillisecondClock().getTicks() - startTime < maxWait) {
            if (tableToken == null) {
                try {
                    tableToken = verifyTableName(tableName);
                } catch (CairoException ex) {
                    Os.sleep(sleep);
                    sleep = Math.min(MAX_SLEEP_MILLIS, sleep * 2);
                    continue;
                }
            }

            if (tableToken != null) {
                seqTxn = seqTxn > -1 ? seqTxn : getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn();
                writerTxn = getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn();
                if (seqTxn <= writerTxn) {
                    return;
                }

                boolean isSuspended = getTableSequencerAPI().isSuspended(tableToken);
                if (isSuspended) {
                    throw CairoException.nonCritical().put("table is suspended [tableName=").put(tableName).put(']');
                }
                Os.sleep(sleep);
                sleep = Math.min(MAX_SLEEP_MILLIS, sleep * 2);
            }
        }
        throw CairoException.nonCritical()
                .put("txn timed out [table=").put(tableName)
                .put(", expectedTxn=").put(seqTxn)
                .put(", writerTxn=").put(writerTxn);
    }

    public void buildViewGraphs() {
        final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
        getTableTokens(tableTokenBucket, false);

        try (
                Path path = new Path();
                Path sweepPath = new Path();
                Path liveViewDirPath = new Path();
                BlockFileReader reader = new BlockFileReader(configuration);
                WalEventReader walEventReader = new WalEventReader(configuration);
                MemoryCMR txnMem = Vm.getCMRInstance(configuration.getBypassWalFdCache())
        ) {
            path.of(configuration.getDbRoot());
            final int pathLen = path.size();
            final MatViewStateReader matViewStateReader = new MatViewStateReader();
            // Reusable scratch for the per-LV checkpoint sweep (see the LV
            // branch below). Allocated once per buildViewGraphs() call.
            final StringSink sweepNameSink = new StringSink();
            for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
                final TableToken tableToken = tableTokenBucket.get(i);
                if (tableToken.isView() && TableUtils.isViewDefinitionFileExists(configuration, path, tableToken.getDirName())) {
                    try {
                        ViewDefinition viewDefinition = viewGraph.getViewDefinition(tableToken);
                        if (viewDefinition == null) {
                            viewDefinition = new ViewDefinition();
                            ViewDefinition.readFrom(
                                    viewDefinition,
                                    reader,
                                    path,
                                    pathLen,
                                    tableToken
                            );
                            if (viewGraph.addView(viewDefinition)) {
                                viewStateStore.createViewState(viewDefinition);
                            }
                        }
                    } catch (Throwable th) {
                        final LogRecord rec = LOG.error().$("could not load view [view=").$(tableToken);
                        if (th instanceof CairoException ce) {
                            rec.$(", msg=").$(ce.getFlyweightMessage())
                                    .$(", errno=").$(ce.getErrno());
                        } else {
                            rec.$(", msg=").$(th.getMessage());
                        }
                        rec.I$();
                    }
                }
                if (tableToken.isMatView() && TableUtils.isMatViewDefinitionFileExists(configuration, path, tableToken.getDirName())) {
                    try {
                        MatViewDefinition viewDefinition = dependentViewGraph.getViewDefinition(tableToken);
                        if (viewDefinition == null) {
                            viewDefinition = new MatViewDefinition();
                            MatViewDefinition.readFrom(
                                    this,
                                    viewDefinition,
                                    reader,
                                    path,
                                    pathLen,
                                    tableToken
                            );
                            if (dependentViewGraph.addView(viewDefinition)) {
                                matViewStateStore.createViewState(viewDefinition);
                            }
                        }

                        final MatViewState state = matViewStateStore.getViewState(tableToken);
                        // Can be null if the state store implementation is no-op.
                        // The no-op state store does nothing on view creation and other operations
                        // and is used when mat views are disabled.
                        if (state != null) {
                            final TableToken baseTableToken = tableNameRegistry.getTableToken(viewDefinition.getBaseTableName());
                            final boolean baseTableExists = baseTableToken != null && !tableNameRegistry.isTableDropped(baseTableToken);
                            if (!baseTableExists) {
                                // Print a warning, but let the mat view load in invalid state.
                                LOG.info().$("base table for materialized view does not exist [table=").$safe(viewDefinition.getBaseTableName())
                                        .$(", view=").$(tableToken)
                                        .I$();
                                matViewStateStore.enqueueInvalidate(tableToken, "base table does not exist");
                                continue;
                            }

                            if (!baseTableToken.isWal()) {
                                // Print a warning, but let the mat view load in invalid state.
                                LOG.info().$("base table for materialized view is not WAL table [table=").$safe(viewDefinition.getBaseTableName())
                                        .$(", view=").$(tableToken)
                                        .I$();
                                matViewStateStore.enqueueInvalidate(tableToken, "base table is not WAL table");
                                continue;
                            }

                            path.trimTo(pathLen).concat(tableToken);
                            if (!WalUtils.readMatViewState(path, tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader)) {
                                LOG.info().$("could not find materialized view state, default values will be used [table=")
                                        .$safe(viewDefinition.getBaseTableName())
                                        .$(", view=").$(tableToken)
                                        .I$();
                                continue;
                            }

                            state.initFromReader(matViewStateReader);
                            if (state.isInvalid()) {
                                continue;
                            }
                            long baseTableLastTxn = getTableSequencerAPI().lastTxn(baseTableToken);
                            if (state.getLastRefreshBaseTxn() > baseTableLastTxn) {
                                LOG.info().$("materialized view is ahead of base table and cannot be synchronized [table=")
                                        .$safe(viewDefinition.getBaseTableName())
                                        .$(", view=").$(tableToken)
                                        .$(", matViewBaseTxn=").$(state.getLastRefreshBaseTxn())
                                        .$(", baseTableTxn=").$(baseTableLastTxn)
                                        .I$();
                                matViewStateStore.enqueueInvalidate(tableToken, "materialized view is ahead of base table and cannot be synchronized");
                            } else if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
                                // Kickstart immediate refresh.
                                matViewStateStore.enqueueIncrementalRefresh(tableToken);
                            }
                        }
                    } catch (Throwable th) {
                        final LogRecord rec = LOG.error().$("could not load materialized view [view=").$(tableToken);
                        if (th instanceof CairoException ce) {
                            rec.$(", msg=").$safe(ce.getFlyweightMessage())
                                    .$(", errno=").$(ce.getErrno());
                        } else {
                            rec.$(", msg=").$safe(th.getMessage());
                        }
                        rec.I$();
                    }
                }
                if (tableToken.isLiveView()) {
                    if (TableUtils.isLiveViewDropSentinelFileExists(configuration, path, tableToken.getDirName())) {
                        // dropLiveView wrote the durable _lv.drop sentinel before
                        // any in-memory or on-disk teardown, then crashed.
                        // Finish the drop now so the directory does not survive
                        // and re-register as a healthy LV. Best-effort: a
                        // failure here only delays cleanup; the sentinel stays
                        // and the next start retries.
                        LOG.info().$("reaping live view with _lv.drop sentinel [view=").$(tableToken).I$();
                        try {
                            dropTableOrViewOrMatView(path, tableToken);
                        } catch (Throwable th) {
                            LOG.error().$("could not reap dropped live view [view=").$(tableToken)
                                    .$(", msg=").$safe(th.getMessage()).I$();
                        }
                        continue;
                    }
                    if (!TableUtils.isLiveViewDefinitionFileExists(configuration, path, tableToken.getDirName())) {
                        // Orphan LV directory: createLiveView writes _lv last as the
                        // atomic commit marker, so a missing _lv means CREATE crashed
                        // before finishing. Reap the directory so it does not pile up.
                        // Best-effort: a failure here only delays cleanup, never loses
                        // committed data (the LV never reached the visible state).
                        LOG.info().$("reaping half-created live view [view=").$(tableToken).I$();
                        try {
                            dropTableOrViewOrMatView(path, tableToken);
                        } catch (Throwable th) {
                            LOG.error().$("could not reap half-created live view [view=").$(tableToken)
                                    .$(", msg=").$safe(th.getMessage()).I$();
                        }
                        continue;
                    }
                    try {
                        if (liveViewRegistry.getViewInstance(tableToken.getTableName()) == null) {
                            final GenericRecordMetadata metadata;
                            try (TableReaderMetadata readerMetadata = new TableReaderMetadata(configuration, tableToken)) {
                                readerMetadata.loadMetadata();
                                metadata = GenericRecordMetadata.copyOfNew(readerMetadata);
                            }
                            final TableToken baseTableToken = tableNameRegistry.getTableToken(
                                    LiveViewDefinition.readBaseTableName(reader, path, pathLen, tableToken)
                            );
                            final boolean baseTableExists = baseTableToken != null && !tableNameRegistry.isTableDropped(baseTableToken);
                            LiveViewDefinition definition = LiveViewDefinition.readFrom(
                                    reader,
                                    path,
                                    pathLen,
                                    tableToken,
                                    baseTableToken,
                                    metadata
                            );
                            LiveViewInstance instance = new LiveViewInstance(definition, tableToken);
                            // _lv exists, so CREATE committed atomically (the engine writes
                            // _lv.s first and _lv last). If _lv.s is somehow missing here,
                            // the on-disk state is corrupt: refusing the load avoids re-
                            // replaying the entire base table from seqTxn 0.
                            if (!TableUtils.isLiveViewStateFileExists(configuration, path, tableToken.getDirName())) {
                                throw CairoException.critical(0)
                                        .put("live view state file missing alongside committed definition [view=")
                                        .put(tableToken.getTableName()).put(']');
                            }
                            path.of(configuration.getDbRoot()).concat(tableToken).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
                            reader.of(path.$());
                            LiveViewStateReader stateReader = new LiveViewStateReader();
                            stateReader.of(reader, tableToken);
                            instance.initFromState(stateReader);
                            // A persisted invalidation always wins — the original reason is more
                            // specific (base drop vs base rename vs schema change), so we only
                            // synthesize "base table does not exist" when nothing was persisted.
                            if (!instance.isInvalid()) {
                                long nowUs = configuration.getMicrosecondClock().getTicks();
                                if (!baseTableExists) {
                                    LOG.info().$("base table for live view does not exist [table=").$safe(definition.getBaseTableName())
                                            .$(", view=").$(tableToken)
                                            .I$();
                                    instance.markInvalid("base table does not exist", nowUs);
                                } else if (!baseTableToken.isWal()) {
                                    LOG.info().$("base table for live view is not WAL table [table=").$safe(definition.getBaseTableName())
                                            .$(", view=").$(tableToken)
                                            .I$();
                                    instance.markInvalid("base table is not WAL table", nowUs);
                                }
                            }
                            liveViewRegistry.registerView(instance);
                            // Register the LV with the shared dependents graph so
                            // orderByDependentViews honors LV-after-base ordering
                            // during DatabaseCheckpointAgent snapshots. Skipping this
                            // would deadlock the snapshot agent on multi-LV chains
                            // over the same base.
                            dependentViewGraph.addLiveView(tableToken, definition.getBaseTableName());
                            liveViewStateStore.registerBaseTable(definition.getBaseTableName());
                            // Startup sweep: clean .cp.tmp orphans
                            // and any .cp whose lvSeqTxn outran the applied
                            // watermark, then retain only the highest survivor.
                            // Stamp the survivor's lvSeqTxn on the instance so
                            // the first refresh cycle knows to attempt restore;
                            // maxTs / stateBytes stay LONG_NULL / 0 until that
                            // cycle reads the manifest.
                            liveViewDirPath.of(configuration.getDbRoot()).concat(tableToken);
                            final long headSeqTxn = LiveViewRecovery.sweepCheckpoints(
                                    configuration.getFilesFacade(),
                                    sweepPath,
                                    liveViewDirPath,
                                    stateReader.getAppliedWatermark(),
                                    sweepNameSink
                            );
                            if (headSeqTxn != Numbers.LONG_NULL) {
                                instance.setHeadCheckpoint(headSeqTxn, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL);
                            }
                        }
                    } catch (Throwable th) {
                        final LogRecord rec = LOG.error().$("could not load live view [view=").$(tableToken);
                        if (th instanceof CairoException ce) {
                            rec.$(", msg=").$safe(ce.getFlyweightMessage())
                                    .$(", errno=").$(ce.getErrno());
                        } else {
                            rec.$(", msg=").$safe(th.getMessage());
                        }
                        rec.I$();
                    }
                }
            }
        }
    }

    public void checkpointCreate(SqlExecutionCircuitBreaker circuitBreaker, boolean isIncrementalBackup) throws SqlException {
        checkpointAgent.checkpointCreate(circuitBreaker, false, isIncrementalBackup);
    }

    /**
     * Recovers database from checkpoint after restoring data from a snapshot.
     */
    public final void checkpointRecover() {
        checkpointAgent.recover();
    }

    public void checkpointRelease() throws SqlException {
        checkpointAgent.checkpointRelease();
    }

    @TestOnly
    public boolean clear() {
        checkpointAgent.clear();
        messageBus.clear();
        try (MetadataCacheWriter w = getMetadataCache().writeLock()) {
            w.clearCache();
        }
        viewGraph.clear();
        dependentViewGraph.clear();
        matViewStateStore.clear();
        matViewTimerQueue.clear();
        liveViewRegistry.clear();
        liveViewStateStore.clear();
        boolean b1 = readerPool.releaseAll();
        boolean b2 = writerPool.releaseAll();
        boolean b3 = tableSequencerAPI.releaseAll();
        boolean b4 = sequencerMetadataPool.releaseAll();
        boolean b5 = walWriterPool.releaseAll();
        boolean b6 = viewWalWriterPool.releaseAll();
        boolean b7 = tableMetadataPool.releaseAll();
        scoreboardPool.clear();
        partitionOverwriteControl.clear();
        frameFactory.clear();
        copyExportContext.clear();
        return b1 & b2 & b3 & b4 & b5 & b6 & b7;
    }

    @SuppressWarnings("unused")
    @TestOnly
    // this is used in replication test
    public void clearWalWriterPool() {
        walWriterPool.releaseAll();
    }

    @Override
    public void close() {
        Misc.free(sqlCompilerPool);
        Misc.free(writerPool);
        Misc.free(readerPool);
        Misc.free(sequencerMetadataPool);
        Misc.free(tableMetadataPool);
        Misc.free(walWriterPool);
        Misc.free(viewWalWriterPool);
        Misc.free(tableIdGenerator);
        Misc.free(messageBus);
        Misc.free(tableSequencerAPI);
        Misc.freeObjList(telemetries);
        Misc.free(tableNameRegistry);
        Misc.free(checkpointAgent);
        Misc.free(metadataCache);
        Misc.free(scoreboardPool);
        Misc.free(liveViewRegistry);
        Misc.free(liveViewStateStore);
        Misc.free(matViewStateStore);
        Misc.free(settingsStore);
        Misc.free(frameFactory);
        Misc.free(walLocker);
    }

    @TestOnly
    public void closeNameRegistry() {
        tableNameRegistry.close();
    }

    public void configureThreadLocalReaderPoolSupervisor(@NotNull ResourcePoolSupervisor<ReaderPool.R> supervisor) {
        readerPool.configureThreadLocalPoolSupervisor(supervisor);
    }

    public void createLiveView(
            CreateLiveViewOperation op,
            TableToken baseTableToken,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // reject DEDUP base tables: incremental refresh reads raw WAL segments, which
        // contain the pre-dedup row stream and cannot be reconciled with the applied
        // base table state.
        final int basePartitionBy;
        final int baseTimestampType;
        final long viewLowerBoundTimestamp;
        try (TableMetadata baseMetadata = getTableMetadata(baseTableToken)) {
            for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
                if (baseMetadata.isDedupKey(i)) {
                    throw SqlException.$(op.getBaseTableNamePosition(),
                            "live view cannot be created over a base table with DEDUP keys");
                }
            }
            basePartitionBy = baseMetadata.getPartitionBy();
            int tsIndex = baseMetadata.getTimestampIndex();
            if (tsIndex < 0) {
                throw SqlException.$(op.getBaseTableNamePosition(),
                        "live view base table must have a designated timestamp");
            }
            baseTimestampType = baseMetadata.getColumnType(tsIndex);
        }
        // viewLowerBoundTimestamp is the floor for O3 reachability and is compared
        // against late_row.ts in base-table units. The non-BACKFILL path takes the
        // wall-clock CREATE moment, scaled into base units via the base's driver
        // so MICRO and NANO bases both produce a comparable value. The catalogue
        // converts back to TIMESTAMP_MICRO at display time.
        //
        // BACKFILL views seed from the earliest visible base row, so the floor
        // sits at that row's timestamp (already in base units): every historical
        // row from there is admissible during the sweep, and any later O3 row
        // below it is rejected. An empty base falls back to the CREATE moment.
        final long createMomentLowerBound = ColumnType.getTimestampDriver(baseTimestampType)
                .fromMicros(configuration.getMicrosecondClock().getTicks());
        if (op.getBackfillRequested()) {
            try (TableReader baseReader = getReader(baseTableToken)) {
                viewLowerBoundTimestamp = baseReader.size() == 0
                        ? createMomentLowerBound
                        : baseReader.getMinTimestamp();
            }
        } else {
            viewLowerBoundTimestamp = createMomentLowerBound;
        }

        // compile the SELECT to validate and get metadata. The live-view-compile flag
        // suppresses indexed-symbol key extraction in WhereClauseParser so the planner
        // emits a plain FilteredRecordCursorFactory shape that the incremental refresh
        // path can handle.
        GenericRecordMetadata metadata;
        // Base-column names the SELECT's filter + window inputs + designated ts
        // depend on. ApplyWal2TableJob's schema-change hook narrows invalidation
        // using this set: only changes that touch one of these columns mark the
        // view INVALID; unrelated ALTERs leave it ACTIVE.
        final ObjList<String> dependencyColumnNames = new ObjList<>();
        try (SqlCompiler compiler = getSqlCompiler()) {
            executionContext.setLiveViewCompile(true);
            CompiledQuery cq;
            try {
                cq = compiler.compile(op.getSelectSql(), executionContext);
            } finally {
                executionContext.setLiveViewCompile(false);
            }
            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                final PageFrameRecordCursorFactory pfrcf = validateLiveViewFactory(factory, baseTableToken, op.getViewNamePosition());
                metadata = GenericRecordMetadata.copyOfNew(factory.getMetadata());

                // Capture each base-column name the SELECT projects. Resolving
                // against the base table here also catches the rare case of an LV
                // SELECT that names a column the base no longer has — better to fail
                // CREATE early than to land an LV that's invalid on first refresh.
                final RecordMetadata baseProjMeta = pfrcf.getMetadata();
                try (MetadataCacheReader metaRO = getMetadataCache().readLock()) {
                    final CairoTable baseTable = metaRO.getTable(baseTableToken);
                    if (baseTable == null) {
                        throw CairoException.tableDoesNotExist(baseTableToken.getTableName());
                    }
                    for (int i = 0, n = baseProjMeta.getColumnCount(); i < n; i++) {
                        CharSequence colName = baseProjMeta.getColumnName(i);
                        if (baseTable.getColumnQuiet(colName) == null) {
                            throw CairoException.critical(0)
                                    .put("live view base column not found [view=").put(op.getViewName())
                                    .put(", column=").put(colName).put(']');
                        }
                        dependencyColumnNames.add(Chars.toString(colName));
                    }
                }

                // Pass 2 of the ANCHOR EXPRESSION validator. Pass 1 (AST-level rejects of subqueries,
                // bind variables, rnd_*/now()/etc.) ran in the parser; this is the
                // function-property half that needs the compiled tree so it can see
                // post-constant-fold flags and runtime-state predicates per-fn.
                // Runs at CREATE only, never at restart.
                final LiveViewDefinition.LvAnchorSpec anchor = op.getAnchorSpec();
                if (anchor != null && anchor.anchorExpressionSql != null) {
                    final ExpressionNode anchorNode = compiler.parseExpression(anchor.anchorExpressionSql);
                    if (anchorNode != null) {
                        Function anchorFn = null;
                        try {
                            FunctionParser fp = new FunctionParser(configuration, getFunctionFactoryCache());
                            executionContext.setLiveViewCompile(true);
                            try {
                                anchorFn = fp.parseFunction(anchorNode, baseProjMeta, executionContext);
                            } finally {
                                executionContext.setLiveViewCompile(false);
                            }
                            // Anchor the reject position in the user's CREATE SQL (the
                            // ANCHOR keyword) rather than in the re-parsed desugared
                            // expression. anchorNode.position is an offset into the
                            // synthesized timestamp_floor* text for DAILY, and into a
                            // toSink-roundtripped expression for ANCHOR EXPRESSION; in
                            // both cases it diverges from what the user typed.
                            validateAnchorPurity(anchorFn, anchor.anchorPosition, true);
                        } finally {
                            Misc.free(anchorFn);
                        }
                    }
                }
            }
        }

        // Resolve PARTITION BY: PartitionBy.NONE is the "inherit" sentinel.
        final int partitionBy = LiveViewTableStructure.resolvePartitionBy(op.getPartitionBy(), basePartitionBy);

        // Capture base sequencer head. Non-BACKFILL views start empty and consume
        // commits with seqTxn >= subscribeFromSeqTxn. BACKFILL views capture the
        // same head as backfillTargetSeqTxn (the upper bound the sweep covers)
        // and start incremental consumption at head + 1 once the sweep completes.
        final long baseHeadSeqTxn = tableSequencerAPI.getTxnTracker(baseTableToken).getWriterTxn();
        final long subscribeFromSeqTxn = baseHeadSeqTxn + 1;
        final boolean backfillRequested = op.getBackfillRequested();
        final byte backfillState = backfillRequested
                ? LiveViewState.BACKFILL_STATE_BACKFILLING
                : LiveViewState.BACKFILL_STATE_ACTIVE;
        final long backfillTargetSeqTxn = backfillRequested
                ? baseHeadSeqTxn
                : Numbers.LONG_NULL;
        // Initial WAL purge floor this view publishes. BACKFILLING sits at
        // backfillTargetSeqTxn - 1: the snapshot reader MVCC-pins everything
        // <= the target, so base WAL up to the target is not load-bearing, while
        // one extra segment stays retained for the deferred ring drain after the
        // sweep. Non-BACKFILL views start at subscribeFromSeqTxn - 1 - everything
        // from subscribeFromSeqTxn forward is the view's responsibility.
        final long initialLvConsumedSeqTxn = backfillRequested
                ? backfillTargetSeqTxn - 1
                : subscribeFromSeqTxn - 1;

        LiveViewTableStructure struct = new LiveViewTableStructure(configuration, op.getViewName(), partitionBy, metadata);
        try (
                MemoryMARW mem = Vm.getCMARWInstance();
                BlockFileWriter blockFileWriter = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                Path path = new Path()
        ) {
            // Reserve the table name and create the WAL-backed FS skeleton, but
            // hold the name lock + create lock until _lv.s and _lv land durably
            // below. The registry commit is the LV's atomic CREATE point - a
            // concurrent reader that resolves the name will get
            // "table does not exist" until commitDeferredTableNameAndRelease
            // runs, so it cannot see a half-built LV. A null return signals
            // that no deferred handoff happened: IF NOT EXISTS hit the
            // pre-existing token path, or lockAll lost the race to a concurrent
            // CREATE. Either way the caller has nothing left to finalise.
            final TableToken liveViewToken = createTableOrViewOrMatViewUnsecure(
                    executionContext.getSecurityContext(),
                    mem,
                    blockFileWriter,
                    path,
                    op.isIgnoreIfExists(),
                    struct,
                    false,
                    false,
                    TableUtils.TABLE_KIND_REGULAR_TABLE,
                    true
            );
            if (liveViewToken == null) {
                return;
            }

            // From here on, any failure must roll back the table to avoid orphan
            // LV-typed directories the startup loader skips and never reclaims.
            try {
                // _lv is the atomic CREATE commit marker, so it must be written
                // last. Order: _lv.s (state) first, then _lv (definition). A crash
                // between the two leaves _lv missing and the partial CREATE looks
                // like an orphan LV directory to the loader. Rolling that back is
                // safe; loading a half-created LV is not (without _lv.s, lastProcessed
                // would default to -1 and the next refresh would re-replay the
                // entire base table).
                LiveViewDefinition definition = new LiveViewDefinition(
                        op.getViewName(),
                        op.getSelectSql(),
                        op.getBaseTableName(),
                        baseTableToken,
                        baseTimestampType,
                        op.getFlushEveryInterval(),
                        op.getFlushEveryIntervalUnit(),
                        op.getInMemoryInterval(),
                        op.getInMemoryIntervalUnit(),
                        partitionBy,
                        viewLowerBoundTimestamp,
                        backfillRequested,
                        op.getAnchorSpec(),
                        dependencyColumnNames,
                        metadata
                );

                // _checkpoints/ subdirectory holds the rolling head checkpoint.
                // Created before _lv.s so the rollback
                // path in dropTableOrViewOrMatView below covers it; _lv stays the
                // atomic CREATE commit marker. mkdirs() is used (not mkdir()) so
                // an IF NOT EXISTS CREATE against a pre-existing LV - whose
                // directory and _checkpoints/ are already present - re-enters this
                // path idempotently rather than failing with EEXIST.
                path.of(configuration.getDbRoot()).concat(liveViewToken)
                        .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
                if (configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode()) != 0) {
                    throw CairoException.critical(configuration.getFilesFacade().errno())
                            .put("could not create live view checkpoints directory [path=")
                            .put(path).put(']');
                }

                // write _lv.s state file with subscribeFromSeqTxn captured above.
                // BACKFILL views land BACKFILL_STATE_BACKFILLING plus the target
                // seqTxn the sweep must cover.
                path.of(configuration.getDbRoot()).concat(liveViewToken);
                blockFileWriter.of(path.concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME).$());
                LiveViewState.append(
                        false,
                        null,
                        Numbers.LONG_NULL,
                        subscribeFromSeqTxn,
                        subscribeFromSeqTxn - 1,
                        -1L,
                        initialLvConsumedSeqTxn,
                        backfillState,
                        backfillTargetSeqTxn,
                        blockFileWriter
                );

                // write _lv definition file (commit marker)
                path.of(configuration.getDbRoot()).concat(liveViewToken);
                blockFileWriter.of(path.concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME).$());
                LiveViewDefinition.append(definition, blockFileWriter);

                LiveViewInstance instance = new LiveViewInstance(definition, liveViewToken);
                instance.setSubscribeFromSeqTxn(subscribeFromSeqTxn);
                instance.setLastProcessedSeqTxn(subscribeFromSeqTxn - 1);
                instance.setAppliedWatermark(-1L);
                instance.setLvConsumedSeqTxn(initialLvConsumedSeqTxn);
                instance.setBackfillState(backfillState);
                instance.setBackfillTargetSeqTxn(backfillTargetSeqTxn);
                liveViewRegistry.registerView(instance);
                dependentViewGraph.addLiveView(liveViewToken, definition.getBaseTableName());
                liveViewStateStore.registerBaseTable(definition.getBaseTableName());

                // _lv.s and _lv are now durable; in-memory registries are populated.
                // Flip the registry name as the last step so concurrent readers
                // either see a fully-built LV or no LV at all.
                commitDeferredTableNameAndRelease(liveViewToken);
            } catch (Throwable t) {
                // Best-effort rollback. Failures here just leave the orphan in place;
                // the original cause is what the operator needs to see.
                try {
                    // If we got past liveViewRegistry.registerView before failing,
                    // remove the in-memory entry so the registry does not leak the
                    // freed instance after the table-drop below succeeds. The
                    // graph-side entry is removed in the same pass so a retried
                    // CREATE does not see a stale dependent.
                    LiveViewInstance partial = liveViewRegistry.removeView(op.getViewName());
                    dependentViewGraph.removeLiveView(liveViewToken, op.getBaseTableName());
                    Misc.free(partial);
                } catch (Throwable rollbackErr) {
                    LOG.error().$("could not unregister partially-created live view [view=").$(liveViewToken)
                            .$(", error=").$(rollbackErr).I$();
                }
                try {
                    rollbackDeferredLiveViewCreate(path, liveViewToken);
                } catch (Throwable rollbackErr) {
                    LOG.error().$("could not roll back partially-created live view [view=").$(liveViewToken)
                            .$(", error=").$(rollbackErr).I$();
                }
                throw t;
            }
        }
    }

    public @NotNull MatViewDefinition createMatView(
            SecurityContext securityContext,
            MemoryMARW mem,
            BlockFileWriter blockFileWriter,
            Path path,
            boolean ifNotExists,
            CreateMatViewOperation operation,
            boolean keepLock,
            boolean inVolume
    ) {
        securityContext.authorizeMatViewCreate();
        final TableToken matViewToken = createTableOrViewOrMatViewUnsecure(securityContext, mem, blockFileWriter, path, ifNotExists, operation, keepLock, inVolume, TableUtils.TABLE_KIND_REGULAR_TABLE, false);
        final MatViewDefinition matViewDefinition = operation.getMatViewDefinition();
        try {
            if (dependentViewGraph.addView(matViewDefinition)) {
                matViewStateStore.createViewState(matViewDefinition);
                if (!matViewDefinition.isDeferred()) {
                    matViewStateStore.enqueueIncrementalRefresh(matViewToken);
                }
            }
        } catch (CairoException e) {
            dropTableOrViewOrMatView(path, matViewToken);
            throw e;
        }
        return matViewDefinition;
    }

    public @NotNull TableToken createTable(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock
    ) {
        return createTable(securityContext, mem, path, ifNotExists, struct, keepLock, false, TableUtils.TABLE_KIND_REGULAR_TABLE);
    }

    public @NotNull TableToken createTable(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock,
            int tableKind
    ) {
        return createTable(securityContext, mem, path, ifNotExists, struct, keepLock, false, tableKind);
    }

    public @NotNull TableToken createTable(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock,
            boolean inVolume,
            int tableKind
    ) {
        if (tableKind != TableUtils.TABLE_KIND_TEMP_PARQUET_EXPORT && Chars.startsWith(struct.getTableName(), configuration.getParquetExportTableNamePrefix())) {
            throw CairoException.nonCritical().put("table name cannot start with reserved prefix [tableName=").put(struct.getTableName())
                    .put(", parquetExportPrefix=").put(configuration.getParquetExportTableNamePrefix())
                    .put(']');
        }
        securityContext.authorizeTableCreate(tableKind);
        return createTableOrViewOrMatViewUnsecure(securityContext, mem, null, path, ifNotExists, struct, keepLock, inVolume, tableKind, false);
    }

    public @NotNull ViewDefinition createView(
            SecurityContext securityContext,
            MemoryMARW mem,
            BlockFileWriter blockFileWriter,
            Path path,
            boolean ifNotExists,
            CreateViewOperation operation,
            @Nullable RecordMetadata metadata
    ) {
        securityContext.authorizeViewCreate();
        final TableToken viewToken = createTableOrViewOrMatViewUnsecure(securityContext, mem, blockFileWriter, path, ifNotExists, operation, false, false, TableUtils.TABLE_KIND_REGULAR_TABLE, false);
        final ViewDefinition viewDefinition = operation.getViewDefinition();
        try {
            if (viewGraph.addView(viewDefinition)) {
                viewStateStore.createViewState(viewDefinition, metadata);
            }
        } catch (CairoException e) {
            dropTableOrViewOrMatView(path, viewToken);
            throw e;
        }
        return viewDefinition;
    }

    public ViewCompilerExecutionContext createViewCompilerContext(int workerCount) {
        return new ViewCompilerExecutionContext(this, workerCount);
    }

    // The reader will ignore close() calls until attached back.
    public void detachReader(TableReader reader) {
        readerPool.detach(reader);
    }

    public void dropLiveView(CharSequence name) {
        // Stamp the durable _lv.drop sentinel before tearing anything down.
        // A crash between any of the steps below leaves a queryable-but-no-
        // longer-registered LV directory; the sentinel lets the startup loader
        // tell that case apart from a healthy LV and finish the drop on the
        // next start (see buildViewGraphs LV branch). The sentinel write runs
        // first so the signal is durable before any in-memory state or on-disk
        // file mutates.
        final TableToken token = tableNameRegistry.getTableToken(name);
        if (token != null && token.isLiveView()) {
            writeLiveViewDropSentinel(token);
        }
        LiveViewInstance instance = liveViewRegistry.removeView(name);
        if (instance != null) {
            // Drop the LV from the dependents graph too so a multi-LV chain's
            // snapshot ordering does not still see the dropped view.
            dependentViewGraph.removeLiveView(instance.getLiveViewToken(), instance.getDefinition().getBaseTableName());
            // Mark the instance dropped and attempt an immediate free. If a refresh or
            // reader is currently holding a lock, tryCloseIfDropped() bails and the
            // winning party (refresh finally hook or cursor close) performs the free.
            instance.markAsDropped();
            instance.tryCloseIfDropped();
        }
        if (token != null) {
            try (Path path = new Path()) {
                dropTableOrViewOrMatView(path, token);
            }
        }
    }

    public void dropTableOrViewOrMatView(@Transient Path path, TableToken tableToken) {
        verifyTableToken(tableToken);
        if (tableToken.isWal()) {
            if (notifyDropped(tableToken)) {
                durableAckRegistry.onTableDropped(tableToken);
                tableSequencerAPI.dropTable(tableToken, false);
                notifyViewStoresAboutDrop(tableToken);
                matViewStateStore.removeViewState(tableToken);
                dependentViewGraph.removeView(tableToken);
                invalidateLiveViewsForBaseTable(tableToken, "base table drop");
                recentWriteTracker.removeTable(tableToken);
            } else {
                LOG.info().$("table is already dropped [table=").$(tableToken).I$();
            }
        } else {
            CharSequence lockedReason = lockAll(tableToken, "removeTable", false);
            if (lockedReason == null) {
                try {
                    path.of(configuration.getDbRoot()).concat(tableToken).$();
                    if (!configuration.getFilesFacade().unlinkOrRemove(path, LOG)) {
                        throw CairoException.critical(configuration.getFilesFacade().errno())
                                .put("could not remove table [table=").put(tableToken).put(", thread=").put(Thread.currentThread().threadId()).put(']');
                    }

                    tableNameRegistry.dropTable(tableToken);
                    // Remove the scoreboard after dropping the table from the registry
                    // Otherwise someone (like Column Purge Job) can create pooled instances of the scoreboard
                    // it from the registry without knowing that the table is being dropped.
                    // Then it can push the scoreboard max txn value into incorrect state.
                    scoreboardPool.remove(tableToken);
                    notifyViewStoresAboutDrop(tableToken);
                    invalidateLiveViewsForBaseTable(tableToken, "base table drop");
                    recentWriteTracker.removeTable(tableToken);
                } finally {
                    unlockTableUnsafe(tableToken, null, false);
                }
                return;
            }
            throw CairoException.nonCritical().put("could not lock '").put(tableToken)
                    .put("' [reason='").put(lockedReason).put("']");
        }
    }

    public void enablePartitionOverwriteControl() {
        LOG.info().$("partition overwrite control is enabled").$();
        partitionOverwriteControl.enable();
    }

    public void enqueueCompileView(TableToken tableToken) {
        viewStateStore.enqueueCompile(tableToken);
    }

    public void execute(CharSequence sqlText) throws SqlException {
        execute(sqlText, rootExecutionContext);
    }

    public void execute(CharSequence sqlText, SqlExecutionContext sqlExecutionContext) throws SqlException {
        execute(sqlText, sqlExecutionContext, null);
    }

    public void execute(CharSequence sqlText, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        while (true) {
            try (SqlCompiler compiler = getSqlCompiler()) {
                execute(compiler, sqlText, sqlExecutionContext, eventSubSeq);
                return;
            } catch (TableReferenceOutOfDateException e) {
                // Retry on this exception, all interfaces like HTTP, Pg wire are supposed to retry too.
            }
        }
    }

    public BackupSeqPartLock getBackupSeqPartLock() {
        return backupSeqPartLock;
    }

    @TestOnly
    public int getBusyReaderCount() {
        return readerPool.getBusyCount();
    }

    @TestOnly
    public int getBusyWriterCount() {
        return writerPool.getBusyCount();
    }

    public @NotNull CheckpointListener getCheckpointListener() {
        return configuration.getCheckpointListener();
    }

    public DatabaseCheckpointStatus getCheckpointStatus() {
        return checkpointAgent;
    }

    public long getCommandCorrelationId() {
        return asyncCommandCorrelationId.incrementAndGet();
    }

    public @NotNull ConfigReloader getConfigReloader() {
        return configReloader;
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public CopyExportContext getCopyExportContext() {
        return copyExportContext;
    }

    public CopyImportContext getCopyImportContext() {
        return copyImportContext;
    }

    public @NotNull DataID getDataID() {
        return dataID;
    }

    public @NotNull DdlListener getDdlListener(TableToken tableToken) {
        return getDdlListener(tableToken.getTableName());
    }

    public @NotNull DdlListener getDdlListener(String tableName) {
        return tableFlagResolver.isSystem(tableName) ? DefaultDdlListener.INSTANCE : ddlListener;
    }

    public @NotNull DurableAckRegistry getDurableAckRegistry() {
        return durableAckRegistry;
    }

    public FrameFactory getFrameFactory() {
        return frameFactory;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return ffCache;
    }

    public TableRecordMetadata getLegacyMetadata(TableToken tableToken) {
        return getLegacyMetadata(tableToken, TableUtils.ANY_TABLE_VERSION);
    }

    /**
     * Retrieves up-to-date table metadata regardless of table type.
     *
     * @param tableToken     table token
     * @param desiredVersion version of table metadata used previously if consistent metadata reads are required
     * @return returns {@link SequencerMetadata} for WAL tables and {@link TableMetadata}
     * for non-WAL, which would be metadata of the {@link TableReader}
     */
    public TableRecordMetadata getLegacyMetadata(TableToken tableToken, long desiredVersion) {
        if (!tableToken.isWal()) {
            return getTableMetadata(tableToken, desiredVersion);
        }
        return getSequencerMetadata(tableToken, desiredVersion);
    }

    public LiveViewRegistry getLiveViewRegistry() {
        return liveViewRegistry;
    }

    public LiveViewStateStore getLiveViewStateStore() {
        return liveViewStateStore;
    }

    public @NotNull DependentViewGraph getDependentViewGraph() {
        return dependentViewGraph;
    }

    public @NotNull MatViewStateStore getMatViewStateStore() {
        return matViewStateStore;
    }

    public Queue<MatViewTimerTask> getMatViewTimerQueue() {
        return matViewTimerQueue;
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public MetadataCache getMetadataCache() {
        return metadataCache;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public int getNextTableId() {
        return (int) tableIdGenerator.getNextId();
    }

    public PartitionOverwriteControl getPartitionOverwriteControl() {
        return partitionOverwriteControl;
    }

    @TestOnly
    public PoolListener getPoolListener() {
        return this.writerPool.getPoolListener();
    }

    public QueryRegistry getQueryRegistry() {
        return queryRegistry;
    }

    public @NotNull QwpServerInfoProvider getQwpServerInfoProvider() {
        return configuration.getQwpServerInfoProvider();
    }

    public TableReader getReader(CharSequence tableName) {
        TableToken tableToken = verifyTableNameForRead(tableName);
        // Do not call getReader(TableToken tableToken), it will do unnecessary token verification
        return readerPool.get(tableToken);
    }

    public TableReader getReader(TableToken tableToken) {
        verifyTableToken(tableToken);
        return readerPool.get(tableToken);
    }

    public TableReader getReader(TableToken tableToken, long metadataVersion) {
        verifyTableToken(tableToken);
        final int tableId = tableToken.getTableId();
        TableReader reader = readerPool.get(tableToken);
        if ((metadataVersion > -1 && reader.getMetadataVersion() != metadataVersion)
                || (tableId > -1 && reader.getMetadata().getTableId() != tableId)) {
            TableReferenceOutOfDateException ex = TableReferenceOutOfDateException.of(
                    tableToken,
                    tableId,
                    reader.getMetadata().getTableId(),
                    metadataVersion,
                    reader.getMetadataVersion()
            );
            reader.close();
            throw ex;
        }
        return reader;
    }

    /**
     * Returns a pooled table reader that is pointed at the same transaction number
     * as the source reader.
     * <p>
     * If the source reader is detached and not in use, returns the source reader.
     * The source reader must be used only through calling this method.
     */
    public TableReader getReaderAtTxn(TableReader srcReader) {
        assert srcReader.isOpen() && srcReader.isActive();
        // Fast path: go with the base reader if it's not in-use.
        if (readerPool.isDetached(srcReader) && readerPool.getDetachedRefCount(srcReader) == 0) {
            readerPool.incDetachedRefCount(srcReader);
            return srcReader;
        }
        // Slow path: obtain a base reader copy from the pool.
        return readerPool.getCopyOf(srcReader);
    }

    public Map<CharSequence, AbstractMultiTenantPool.Entry<ReaderPool.R>> getReaderPoolEntries() {
        return readerPool.entries();
    }

    public TableReader getReaderWithRepair(TableToken tableToken) {
        // todo: untested verification
        verifyTableToken(tableToken);
        try {
            return getReader(tableToken);
        } catch (CairoException e) {
            // Cannot open reader on existing table is pretty bad.
            // In some messed states, for example after _meta file swap failure Reader cannot be opened
            // but writer can be. Opening writer fixes the table mess.
            tryRepairTable(tableToken, e);
        }
        try {
            return getReader(tableToken);
        } catch (CairoException e) {
            LOG.critical()
                    .$("could not open reader [table=").$(tableToken)
                    .$(", msg=").$safe(e.getFlyweightMessage())
                    .$(", errno=").$(e.getErrno())
                    .I$();
            throw e;
        }
    }

    public RecentWriteTracker getRecentWriteTracker() {
        return recentWriteTracker;
    }

    public TableRecordMetadata getSequencerMetadata(TableToken tableToken) {
        return getSequencerMetadata(tableToken, TableUtils.ANY_TABLE_VERSION);
    }

    /**
     * Table metadata as seen by the table sequencer. This is the most up-to-date table
     * metadata, and it can be used to positively confirm column metadata changes immediately after
     * making them.
     * <p>
     * However, this metadata cannot confirm all the changes, one of which is "dedup" flag on a table.
     * This is a shortcoming and to confirm the "dedup" flag {{@link #getTableMetadata(TableToken, long)}} should
     * be polled instead. We expect to fix issues like this one in the near future.
     *
     * @param tableToken     table token
     * @param desiredVersion version of table metadata used previously if consistent metadata reads are required
     * @return sequence metadata instance
     */
    public TableRecordMetadata getSequencerMetadata(TableToken tableToken, long desiredVersion) {
        assert tableToken.isWal();
        verifyTableToken(tableToken);
        if (tableToken.isView()) {
            return getViewMetadata(tableToken);
        }
        final TableRecordMetadata metadata = sequencerMetadataPool.get(tableToken);
        validateDesiredMetadataVersion(tableToken, metadata, desiredVersion);
        return metadata;
    }

    public @NotNull SettingsStore getSettingsStore() {
        return settingsStore;
    }

    public SqlCompiler getSqlCompiler() {
        return sqlCompilerPool.get();
    }

    public SqlCompilerFactory getSqlCompilerFactory() {
        return SqlCompilerFactoryImpl.INSTANCE;
    }

    public TableFlagResolver getTableFlagResolver() {
        return tableFlagResolver;
    }

    @TestOnly
    public IDGenerator getTableIdGenerator() {
        return tableIdGenerator;
    }

    /**
     * Same as {{@link #getTableMetadata(TableToken, long)}} but it will provide the most
     * up-to-date version of the metadata without correlating it with anything else.
     *
     * @param tableToken table token
     * @return pooled metadata instance
     */
    public TableMetadata getTableMetadata(TableToken tableToken) {
        return getTableMetadata(tableToken, TableUtils.ANY_TABLE_VERSION);
    }

    /**
     * This is explicitly "table" metadata. For legacy (non-WAL) tables, this metadata
     * is the same as writer metadata. For new WAL tables, table metadata could be "old",
     * as in not all WAL transactions has reached the table yet. In scenarios where
     * table modification is made and positively confirmed immediately after via metadata, {@link #getSequencerMetadata(TableToken, long)}
     * must be used instead.
     * <p>
     * Metadata provided by this method is good enough for the read-only queries.
     *
     * @param tableToken     table token
     * @param desiredVersion version of table metadata used previously if consistent metadata reads are required
     * @return pooled metadata instance
     */
    public TableMetadata getTableMetadata(TableToken tableToken, long desiredVersion) {
        verifyTableToken(tableToken);
        if (tableToken.isView()) {
            return getViewMetadata(tableToken);
        }
        try {
            final TableMetadata metadata = tableMetadataPool.get(tableToken);
            validateDesiredMetadataVersion(tableToken, metadata, desiredVersion);
            return metadata;
        } catch (CairoException e) {
            if (tableToken.isWal()) {
                throw e;
            } else {
                tryRepairTable(tableToken, e);
            }
        }
        TableMetadata metadata = tableMetadataPool.get(tableToken);
        validateDesiredMetadataVersion(tableToken, metadata, desiredVersion);
        return metadata;
    }

    public TableSequencerAPI getTableSequencerAPI() {
        return tableSequencerAPI;
    }

    public int getTableStatus(Path path, TableToken tableToken) {
        if (tableToken == TableNameRegistry.LOCKED_TOKEN) {
            return TableUtils.TABLE_RESERVED;
        }
        if (tableToken == TableNameRegistry.LOCKED_DROP_TOKEN) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        if (tableToken == null || !tableToken.equals(tableNameRegistry.getTableToken(tableToken.getTableName()))) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getDbRoot(), tableToken.getDirName());
    }

    public int getTableStatus(Path path, CharSequence tableName) {
        final TableToken tableToken = tableNameRegistry.getTableToken(tableName);
        if (tableToken == null) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        return getTableStatus(path, tableToken);
    }

    @TestOnly
    public int getTableStatus(CharSequence tableName) {
        return getTableStatus(Path.getThreadLocal(configuration.getDbRoot()), tableName);
    }

    public @Nullable TableToken getTableTokenByDirName(CharSequence dirName) {
        return tableNameRegistry.getTableTokenByDirName(dirName);
    }

    public int getTableTokenCount(boolean includeDropped) {
        return tableNameRegistry.getTableTokenCount(includeDropped);
    }

    public TableToken getTableTokenIfExists(CharSequence tableName) {
        final TableToken token = tableNameRegistry.getTableToken(tableName);
        if (TableNameRegistry.isLocked(token)) {
            return null;
        }
        return token;
    }

    public TableToken getTableTokenIfExists(CharSequence tableName, int lo, int hi) {
        final StringSink sink = Misc.getThreadLocalSink();
        sink.put(tableName, lo, hi);
        return getTableTokenIfExists(sink);
    }

    public void getTableTokens(ObjHashSet<TableToken> bucket, boolean includeDropped) {
        tableNameRegistry.getTableTokens(bucket, includeDropped);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(TableToken tableToken, @NotNull String lockReason) {
        verifyTableToken(tableToken);
        if (!tableToken.isWal()) {
            return writerPool.get(tableToken, lockReason);
        }
        return walWriterPool.get(tableToken);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(CharSequence tableName, @NotNull String lockReason) {
        TableToken tableToken = verifyTableNameForRead(tableName);
        // Do not call getTableWriterAPI(TableToken tableToken, String lockReason),
        // it will do unnecessary token verification
        if (!tableToken.isWal()) {
            return writerPool.get(tableToken, lockReason);
        }
        return walWriterPool.get(tableToken);
    }

    public ObjList<Telemetry<? extends AbstractTelemetryTask>> getTelemetries() {
        return telemetries;
    }

    public Telemetry<TelemetryTask> getTelemetry() {
        return telemetry;
    }

    public Telemetry<TelemetryMatViewTask> getTelemetryMatView() {
        return telemetryMatView;
    }

    public Telemetry<TelemetryWalTask> getTelemetryWal() {
        return telemetryWal;
    }

    public TxnScoreboard getTxnScoreboard(@NotNull TableToken tableToken) {
        return scoreboardPool.getTxnScoreboard(tableToken);
    }

    public TxnScoreboardPool getTxnScoreboardPool() {
        return scoreboardPool;
    }

    public long getUnpublishedWalTxnCount() {
        return unpublishedWalTxnCount.get();
    }

    public TableToken getUpdatedTableToken(TableToken tableToken) {
        return tableNameRegistry.getTokenByDirName(tableToken.getDirName());
    }

    public @NotNull ViewGraph getViewGraph() {
        return viewGraph;
    }

    public @NotNull ViewStateStore getViewStateStore() {
        return viewStateStore;
    }

    public @NotNull ViewWalWriter getViewWalWriter(TableToken viewToken) {
        verifyTableToken(viewToken);
        try {
            return viewWalWriterPool.get(viewToken);
        } catch (CairoException e) {
            if (isTableDropped(viewToken)) {
                throw CairoException.tableDropped(viewToken);
            }
            // Check if the view is concurrently dropped after token verification
            TableToken tt = tableNameRegistry.getTableToken(viewToken.getTableName());
            if (tt == null || TableNameRegistry.isLocked(tt)) {
                // Throw view does not exist exception to indicate that the view is gone.
                throw CairoException.viewDoesNotExist(viewToken.getTableName());
            }
            throw e;
        }
    }

    public @NotNull WalDirectoryPolicy getWalDirectoryPolicy() {
        return walDirectoryPolicy;
    }

    public @NotNull WalListener getWalListener() {
        return walListener;
    }

    public @NotNull WalLocker getWalLocker() {
        return walLocker;
    }

    // For testing only
    @TestOnly
    public WalReader getWalReader(
            @SuppressWarnings("unused") SecurityContext securityContext,
            TableToken tableToken,
            CharSequence walName,
            int segmentId,
            long walRowCount
    ) {
        if (tableToken.isWal()) {
            return new WalReader(configuration, tableToken, walName, segmentId, walRowCount);
        }
        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableToken.getTableName());
    }

    public @NotNull WalWriter getWalWriter(TableToken tableToken) {
        verifyTableToken(tableToken);
        try {
            return walWriterPool.get(tableToken);
        } catch (EntryLockedException e) {
            // WAL writer pool is locked only when the table is being dropped
            // Throw table does not exist exception to indicate that the table is gone already.
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
    }

    public TableWriter getWriter(TableToken tableToken, @NotNull String lockReason) {
        verifyTableToken(tableToken);
        return writerPool.get(tableToken, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(TableToken tableToken, @NotNull AsyncWriterCommand asyncWriterCommand) {
        verifyTableToken(tableToken);
        return writerPool.getWriterOrPublishCommand(tableToken, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    public Map<CharSequence, WriterPool.Entry> getWriterPoolEntries() {
        return writerPool.entries();
    }

    public TableWriter getWriterUnsafe(TableToken tableToken, @NotNull String lockReason) {
        return writerPool.get(tableToken, lockReason);
    }

    /**
     * Populates the RecentWriteTracker with data from existing tables.
     * This should be called at startup to ensure the tracker is not empty.
     * <p>
     * For each table, reads the _txn file to get the row count and uses
     * the file modification time as the last write timestamp.
     */
    public void hydrateRecentWriteTracker() {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence dbRoot = configuration.getDbRoot();
        final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();
        getTableTokens(tableTokens, false);

        try (TxReader txReader = new TxReader(ff); Path path = new Path().of(dbRoot)) {
            final int rootLen = path.size();

            int hydratedCount = 0;
            for (int i = 0, n = tableTokens.size(); i < n; i++) {
                TableToken tableToken = tableTokens.get(i);
                try {
                    path.trimTo(rootLen).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();

                    if (!ff.exists(path.$())) {
                        continue;
                    }

                    // Get metadata for timestampType and partitionBy
                    try (TableMetadata metadata = getTableMetadata(tableToken)) {
                        txReader.ofRO(path.$(), metadata.getTimestampType(), metadata.getPartitionBy());
                        TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

                        long minTimestamp = txReader.getMinTimestamp();
                        long maxTimestamp = txReader.getMaxTimestamp();
                        long rowCount = txReader.getRowCount();
                        long writerTxn = txReader.getSeqTxn();

                        // For WAL tables, get sequencerTxn from the sequencer
                        long sequencerTxn = Numbers.LONG_NULL;
                        long walTimestamp = Numbers.LONG_NULL;
                        if (tableToken.isWal()) {
                            try {
                                sequencerTxn = tableSequencerAPI.lastTxn(tableToken);
                                walTimestamp = maxTimestamp;
                            } catch (CairoException e) {
                                // Sequencer may not be available, use LONG_NULL
                                LOG.debug().$("could not get sequencer txn during hydration [table=").$(tableToken)
                                        .$(", error=").$(e.getMessage()).I$();
                            }
                        }

                        // Use CAS insert - writer data always wins over hydrated data
                        if (recentWriteTracker.recordWriteIfAbsent(
                                tableToken,
                                maxTimestamp,
                                rowCount,
                                writerTxn,
                                sequencerTxn,
                                walTimestamp,
                                minTimestamp == Long.MAX_VALUE ? Numbers.LONG_NULL : minTimestamp,
                                maxTimestamp
                        )) {
                            hydratedCount++;
                        }
                    }
                } catch (CairoException | TableReferenceOutOfDateException e) {
                    // Table may have been dropped or is in inconsistent state, skip it
                    LOG.info().$("skipping table during write tracker hydration [table=").$(tableToken)
                            .$(", error=").$(e.getMessage()).I$();
                } finally {
                    txReader.clear();
                }
            }

            LOG.info().$("recent write tracker hydrated [tables=").$(hydratedCount).I$();
        }

        Runnable callback = recentWriteTrackerHydrationCallback;
        if (callback != null) {
            callback.run();
        }
    }

    /**
     * Invalidates a single live view directly by instance reference, without going
     * through the per-base-table iteration. Used by paths that already hold the
     * instance — notably the refresh worker on flush retry budget exhaustion.
     * Mirrors the lock + persist pattern
     * of {@link #invalidateLiveViewsForBaseTable0}.
     */
    public void invalidateLiveView(LiveViewInstance instance, String reason) {
        final long invalidationTimestampUs = configuration.getMicrosecondClock().getTicks();
        synchronized (instance) {
            // Queue invalidation behind any in-progress checkpoint freeze so
            // the snapshot reflects the pre-invalidation state and the agent's
            // _lv.s copy is not raced by this rewrite.
            instance.waitForUnfrozen();
            instance.markInvalid(reason, invalidationTimestampUs);
            try (
                    BlockFileWriter blockFileWriter = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    Path path = new Path()
            ) {
                path.of(configuration.getDbRoot()).concat(instance.getLiveViewToken()).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
                blockFileWriter.of(path.$());
                LiveViewState.append(instance.getStateReader(), blockFileWriter);
            } catch (Throwable t) {
                LOG.error().$("could not persist live view invalidation [view=").$(instance.getLiveViewToken())
                        .$(", reason=").$safe(reason)
                        .$(", error=").$(t).I$();
            }
        }
    }

    public void invalidateLiveViewsForBaseTable(TableToken baseTableToken, String reason) {
        invalidateLiveViewsForBaseTable0(baseTableToken, reason, null);
    }

    /**
     * Schema-change-aware invalidation. Iterates live views whose base is
     * {@code baseTableToken} and only invalidates those that depend on a column
     * missing from {@code postChangeMetadata}. This narrows the broad invalidation
     * the {@link #invalidateLiveViewsForBaseTable} variant does on TRUNCATE / DROP
     * (where every dependent view is invalidated regardless of dependency set).
     */
    public void invalidateLiveViewsForBaseSchemaChange(
            TableToken baseTableToken,
            io.questdb.cairo.sql.RecordMetadata postChangeMetadata,
            String reason
    ) {
        invalidateLiveViewsForBaseTable0(baseTableToken, reason, postChangeMetadata);
    }

    private void invalidateLiveViewsForBaseTable0(
            TableToken baseTableToken,
            String reason,
            @Nullable io.questdb.cairo.sql.RecordMetadata postChangeMetadata
    ) {
        final long invalidationTimestampUs = configuration.getMicrosecondClock().getTicks();
        // Flip the in-memory bit then rewrite each affected view's _lv.s so the
        // invalidation survives restart. The registry-only helper exists for tests
        // that don't need durability.
        ObjList<LiveViewInstance> sink = tlInvalidateSink.get();
        sink.clear();
        liveViewRegistry.getViewsForBaseTable(baseTableToken.getTableName(), sink);
        try (
                BlockFileWriter blockFileWriter = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                Path path = new Path()
        ) {
            for (int i = 0, n = sink.size(); i < n; i++) {
                LiveViewInstance instance = sink.getQuick(i);
                if (postChangeMetadata != null && !instance.dependsOnMissingColumn(postChangeMetadata)) {
                    // Schema change touches columns the LV doesn't read — leave it valid.
                    continue;
                }
                synchronized (instance) {
                    // Queue invalidation behind any in-progress checkpoint
                    // freeze so the snapshot reflects the pre-invalidation
                    // state and the agent's _lv.s copy is not raced by this
                    // rewrite.
                    instance.waitForUnfrozen();
                    instance.markInvalid(reason, invalidationTimestampUs);
                    path.of(configuration.getDbRoot()).concat(instance.getLiveViewToken()).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
                    try {
                        blockFileWriter.of(path.$());
                        LiveViewState.append(instance.getStateReader(), blockFileWriter);
                    } catch (Throwable t) {
                        LOG.error().$("could not persist live view invalidation [view=").$(instance.getLiveViewToken())
                                .$(", reason=").$safe(reason)
                                .$(", error=").$(t).I$();
                    }
                }
            }
        }
    }

    public boolean isClosing() {
        return closing;
    }

    public boolean isTableDropped(TableToken tableToken) {
        return tableNameRegistry.isTableDropped(tableToken);
    }

    @TestOnly
    public boolean isWalPurgeJobLocked() {
        return walPurgeJobLock.isLocked();
    }

    public boolean isWalTable(TableToken tableToken) {
        return tableToken.isWal();
    }

    public boolean isWalTableDropped(CharSequence tableDir) {
        return tableNameRegistry.isWalTableDropped(tableDir);
    }

    public void load() {
        // Convert tables to WAL/non-WAL, if necessary.
        final ObjList<TableToken> convertedTables = TableConverter.convertTables(this, tableSequencerAPI, tableFlagResolver, tableNameRegistry);
        tableNameRegistry.reload(convertedTables);
        matViewStateStore = createMatViewStateStore();
        viewStateStore = new ViewStateStoreImpl(this);
    }

    public String lockAll(TableToken tableToken, String lockReason, boolean ignoreInProgressCheckpoint) {
        assert lockReason != null;
        if (!ignoreInProgressCheckpoint && checkpointAgent.isInProgress()) {
            // prevent reader locking before checkpoint is released
            return REASON_CHECKPOINT_IN_PROGRESS;
        }
        // busy metadata is same as busy reader from user perspective
        String lockedReason;
        if (tableMetadataPool.lock(tableToken)) {
            if (sequencerMetadataPool.lock(tableToken)) {
                lockedReason = writerPool.lock(tableToken, lockReason);
                if (lockedReason == null) {
                    // not locked
                    if (readerPool.lock(tableToken)) {
                        LOG.info().$("locked [table=").$(tableToken)
                                .$(", thread=").$(Thread.currentThread().threadId())
                                .I$();
                        return null;
                    }
                    writerPool.unlock(tableToken);
                    lockedReason = REASON_BUSY_READER;
                }
                sequencerMetadataPool.unlock(tableToken);
            } else {
                lockedReason = REASON_BUSY_SEQUENCER_METADATA_POOL;
            }
            tableMetadataPool.unlock(tableToken);
        } else {
            lockedReason = REASON_BUSY_TABLE_READER_METADATA_POOL;
        }
        return lockedReason;
    }

    public boolean lockReaders(TableToken tableToken) {
        verifyTableToken(tableToken);
        return lockReadersByTableToken(tableToken);
    }

    public boolean lockReadersAndMetadata(TableToken tableToken) {
        if (checkpointAgent.isInProgress()) {
            // prevent reader locking before checkpoint is released
            return false;
        }
        if (readerPool.lock(tableToken)) {
            if (tableMetadataPool.lock(tableToken)) {
                return true;
            } else {
                readerPool.unlock(tableToken);
            }
        }
        return false;
    }

    public boolean lockReadersByTableToken(TableToken tableToken) {
        if (checkpointAgent.isInProgress()) {
            // prevent reader locking before checkpoint is released
            return false;
        }
        return readerPool.lock(tableToken);
    }

    public boolean lockTableCreate(TableToken tableToken) {
        return createTableLock.putIfAbsent(tableToken.getTableName(), tableToken) == null;
    }

    public TableToken lockTableName(CharSequence tableName) {
        final int tableId = getNextTableId();
        return lockTableName(tableName, tableId, false, false, false);
    }

    @Nullable
    public TableToken lockTableName(CharSequence tableName, int tableId, boolean isView, boolean isMatView, boolean isWal) {
        return lockTableName(tableName, tableId, isView, isMatView, false, isWal);
    }

    @Nullable
    public TableToken lockTableName(CharSequence tableName, int tableId, boolean isView, boolean isMatView, boolean isLiveView, boolean isWal) {
        validNameOrThrow(tableName);
        final String tableNameStr = Chars.toString(tableName);
        final String dirName = TableUtils.getTableDir(configuration.mangleTableDirNames(), tableNameStr, tableId, isWal);
        return tableNameRegistry.lockTableName(tableNameStr, dirName, tableId, isView, isMatView, isLiveView, isWal);
    }

    @Nullable
    public TableToken lockTableName(CharSequence tableName, String dirName, int tableId, boolean isView, boolean isMatView, boolean isWal) {
        validNameOrThrow(tableName);
        final String tableNameStr = Chars.toString(tableName);
        return tableNameRegistry.lockTableName(tableNameStr, dirName, tableId, isView, isMatView, isWal);
    }

    public boolean lockWalWriters(TableToken tableToken) {
        return walWriterPool.lock(tableToken);
    }

    public boolean notifyDropped(TableToken tableToken) {
        if (tableNameRegistry.dropTable(tableToken)) {
            readerPool.notifyDropped(tableToken, false);
            walWriterPool.notifyDropped(tableToken, false);
            viewWalWriterPool.notifyDropped(tableToken, false);
            tableMetadataPool.notifyDropped(tableToken, false);
            sequencerMetadataPool.notifyDropped(tableToken, false);
            final MatViewRefreshTask matViewRefreshTask = tlMatViewRefreshTask.get();
            matViewRefreshTask.clear();
            matViewRefreshTask.baseTableToken = tableToken;
            matViewRefreshTask.operation = MatViewRefreshTask.INVALIDATE;
            matViewRefreshTask.invalidationReason = "table drop operation";
            notifyMatViewBaseTableCommit(matViewRefreshTask, tableSequencerAPI.lastTxn(tableToken));
            notifyViewStoresAboutDrop(tableToken);
            return true;
        }
        return false;
    }

    public void notifyLiveViewBaseTableCommit(TableToken baseTableToken, long seqTxn) {
        liveViewStateStore.notifyBaseTableCommit(baseTableToken, seqTxn);
    }

    public void notifyMatViewBaseTableCommit(MatViewRefreshTask task, long seqTxn) {
        matViewStateStore.notifyBaseTableCommit(task, seqTxn);
    }

    /**
     * Publishes notification of table transaction to the queue. The intent is to notify Apply2WalJob that
     * there are WAL files to be merged into the table. Notification can fail if the queue is full, in
     * which case it will have to be republished from a persisted storage. However, this method does not
     * care about that.
     *
     * @param tableToken table token of the table that has to be processed by the Apply2WalJob
     * @return true if the message was successfully put on the queue and false otherwise.
     */
    public boolean notifyWalTxnCommitted(@NotNull TableToken tableToken) {
        final Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                task.of(tableToken);
                pubSeq.done(cursor);
                return true;
            } else if (cursor == -1L) {
                LOG.info().$("cannot publish WAL notifications, queue is full [current=").$(pubSeq.current())
                        .$(", table=").$(tableToken)
                        .I$();
                // queue overflow, throw away notification and notify a job to rescan all tables
                notifyWalTxnRepublisher(tableToken);
                return false;
            }
        }
    }

    /**
     * This is a workaround for notification queue full and other off-piste events. It includes a hack to
     * prevent repeated notifications by uninitializing the writer's transaction tracker.
     *
     * @param tableToken the destination table for the notification.
     */
    public void notifyWalTxnRepublisher(TableToken tableToken) {
        tableSequencerAPI.updateWriterTxns(tableToken, SeqTxnTracker.UNINITIALIZED_TXN, SeqTxnTracker.UNINITIALIZED_TXN);
        unpublishedWalTxnCount.incrementAndGet();
    }

    public void print(CharSequence sql, MutableCharSink<?> sink) throws SqlException {
        print(sql, sink, rootExecutionContext);
    }

    public void print(CharSequence sql, MutableCharSink<?> sink, SqlExecutionContext executionContext) throws SqlException {
        sink.clear();
        try (
                RecordCursorFactory factory = select(sql, executionContext);
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
    }

    public void reconcileTableNameRegistryState() {
        tableNameRegistry.reconcile();
    }

    public void registerTableToken(TableToken tableToken) {
        tableNameRegistry.registerName(tableToken);
    }

    @TestOnly
    public boolean releaseAllReaders() {
        boolean b1 = sequencerMetadataPool.releaseAll();
        boolean b2 = tableMetadataPool.releaseAll();
        return readerPool.releaseAll() & b1 & b2;
    }

    @TestOnly
    public void releaseAllWalWriters() {
        walWriterPool.releaseAll();
    }

    @TestOnly
    public void releaseAllWriters() {
        writerPool.releaseAll();
    }

    public boolean releaseInactive() {
        boolean useful = writerPool.releaseInactive();
        useful |= readerPool.releaseInactive();
        useful |= tableSequencerAPI.releaseInactive();
        useful |= sequencerMetadataPool.releaseInactive();
        useful |= tableMetadataPool.releaseInactive();
        useful |= walWriterPool.releaseInactive();
        useful |= viewWalWriterPool.releaseInactive();
        useful |= scoreboardPool.releaseInactive();
        return useful;
    }

    @TestOnly
    public void releaseInactiveTableSequencers() {
        walWriterPool.releaseInactive();
        tableSequencerAPI.releaseInactive();
    }

    @TestOnly
    public boolean reloadTableNames() {
        return reloadTableNames(null);
    }

    @TestOnly
    public boolean reloadTableNames(@Nullable ObjList<TableToken> convertedTables) {
        return tableNameRegistry.reload(convertedTables);
    }

    public void removeTableToken(TableToken tableToken) {
        tableNameRegistry.purgeToken(tableToken);
        tableSequencerAPI.purgeTxnTracker(tableToken.getDirName());
        readerPool.notifyDropped(tableToken, true);
        walWriterPool.notifyDropped(tableToken, true);
        viewWalWriterPool.notifyDropped(tableToken, true);
        tableMetadataPool.notifyDropped(tableToken, true);
        sequencerMetadataPool.notifyDropped(tableToken, true);
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(
                    PoolListener.SRC_TABLE_REGISTRY,
                    Thread.currentThread().threadId(),
                    tableToken,
                    PoolListener.EV_REMOVE_TOKEN,
                    (short) 0,
                    (short) 0
            );
        }
        walLocker.clearTable(tableToken);
    }

    public void removeThreadLocalReaderPoolSupervisor() {
        readerPool.removeThreadLocalPoolSupervisor();
    }

    public TableToken rename(
            SecurityContext securityContext,
            Path fromPath,
            MemoryMARW memory,
            CharSequence fromTableName,
            Path toPath,
            CharSequence toTableName
    ) {
        validNameOrThrow(fromTableName);
        validNameOrThrow(toTableName);

        final TableToken fromTableToken = verifyTableName(fromTableName);
        if (Chars.equalsIgnoreCaseNc(fromTableName, toTableName)) {
            return fromTableToken;
        }

        securityContext.authorizeTableRename(fromTableToken);
        TableToken toTableToken;
        if (fromTableToken != null) {
            if (fromTableToken.isWal()) {
                String toTableNameStr = Chars.toString(toTableName);
                toTableToken = tableNameRegistry.addTableAlias(toTableNameStr, fromTableToken);
                if (toTableToken != null) {
                    boolean renamed = false;
                    try {
                        try (WalWriter walWriter = getWalWriter(fromTableToken)) {
                            long seqTxn = walWriter.renameTable(fromTableName, toTableNameStr, securityContext);
                            LOG.info().$("renaming table [from='").$safe(fromTableName)
                                    .$("', to='").$safe(toTableName)
                                    .$("', wal=").$(walWriter.getWalId())
                                    .$("', seqTxn=").$(seqTxn)
                                    .I$();
                            renamed = true;
                        }
                        TableUtils.overwriteTableNameFile(
                                fromPath.of(configuration.getDbRoot()).concat(toTableToken),
                                memory,
                                configuration.getFilesFacade(),
                                toTableToken.getTableName()
                        );
                    } finally {
                        if (renamed) {
                            tableNameRegistry.rename(fromTableToken, toTableToken);
                            if (fromTableToken.isWal()) {
                                matViewStateStore.enqueueInvalidateDependentViews(fromTableToken, "table rename operation");
                            }
                            invalidateLiveViewsForBaseTable(fromTableToken, "base table rename");
                        } else {
                            LOG.info()
                                    .$("failed to rename table [from=").$safe(fromTableName)
                                    .$(", to=").$safe(toTableName)
                                    .I$();
                            tableNameRegistry.removeAlias(toTableToken);
                        }
                    }
                } else {
                    throw CairoException.nonCritical()
                            .put("cannot rename table, new name is already in use [table=").put(fromTableName)
                            .put(", toTableName=").put(toTableName)
                            .put(']');
                }
            } else {
                String lockedReason = lockAll(fromTableToken, "renameTable", false);
                if (lockedReason == null) {
                    try {
                        // No readers exist for the table, no checkpoint
                        // it is ok to remove the scoreboard in case it's in memory implementation
                        scoreboardPool.remove(fromTableToken);
                        toTableToken = rename0(fromPath, fromTableToken, toPath, toTableName);
                        TableUtils.overwriteTableNameFile(
                                fromPath.of(configuration.getDbRoot()).concat(toTableToken),
                                memory,
                                configuration.getFilesFacade(),
                                toTableToken.getTableName()
                        );
                    } finally {
                        unlock(securityContext, fromTableToken, null, false);
                    }
                    tableNameRegistry.dropTable(fromTableToken);
                } else {
                    LOG.error()
                            .$("could not lock and rename [from=").$safe(fromTableName)
                            .$("', to=").$safe(toTableName)
                            .$("', reason=").$(lockedReason)
                            .I$();
                    throw EntryUnavailableException.instance(lockedReason);
                }
            }

            enqueueCompileView(fromTableToken);
            enqueueCompileView(toTableToken);
            getDdlListener(fromTableToken).onTableRenamed(fromTableToken, toTableToken);

            return toTableToken;
        } else {
            LOG.error().$("cannot rename, table does not exist [table=").$safe(fromTableName).I$();
            throw CairoException.nonCritical().put("cannot rename, table does not exist [table=").put(fromTableName).put(']');
        }
    }

    public void replaceViewDefinition(
            TableToken viewToken,
            @NotNull String viewSql,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies,
            BlockFileWriter blockFileWriter,
            Path path
    ) {
        final long seqTxn;
        try (ViewWalWriter walWriter = getViewWalWriter(viewToken)) {
            seqTxn = walWriter.replaceViewDefinition(viewSql, dependencies);
            updateViewDefinition(viewToken, viewSql, dependencies, seqTxn, blockFileWriter, path);
        }
    }

    @TestOnly
    public void resetFrameFactory() {
        frameFactory.close();
        frameFactory = new FrameFactory(configuration);
    }

    @TestOnly
    public void resetNameRegistryMemory() {
        tableNameRegistry.resetMemory();
    }

    public RecordCursorFactory select(CharSequence selectSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            return select(compiler, selectSql, sqlExecutionContext);
        }
    }

    public void setConfigReloader(@NotNull ConfigReloader configReloader) {
        this.configReloader = configReloader;
    }

    public void setDdlListener(@NotNull DdlListener ddlListener) {
        this.ddlListener = ddlListener;
    }

    public void setDurableAckRegistry(@NotNull DurableAckRegistry durableAckRegistry) {
        this.durableAckRegistry = durableAckRegistry;
    }

    @TestOnly
    public void setPoolListener(PoolListener poolListener) {
        this.tableMetadataPool.setPoolListener(poolListener);
        this.sequencerMetadataPool.setPoolListener(poolListener);
        this.writerPool.setPoolListener(poolListener);
        this.readerPool.setPoolListener(poolListener);
        this.walWriterPool.setPoolListener(poolListener);
        this.viewWalWriterPool.setPoolListener(poolListener);
    }

    @TestOnly
    public void setReaderListener(ReaderPool.ReaderListener readerListener) {
        readerPool.setTableReaderListener(readerListener);
    }

    /**
     * Sets a callback to be invoked after {@link #hydrateRecentWriteTracker()} completes.
     * This is primarily for testing to avoid sleep-based synchronization.
     *
     * @param callback the callback to invoke, or null to clear
     */
    @TestOnly
    public void setRecentWriteTrackerHydrationCallback(Runnable callback) {
        this.recentWriteTrackerHydrationCallback = callback;
    }

    @TestOnly
    public void setUp() {
    }

    public void setWalDirectoryPolicy(@NotNull WalDirectoryPolicy walDirectoryPolicy) {
        this.walDirectoryPolicy = walDirectoryPolicy;
    }

    public void setWalListener(@NotNull WalListener walListener) {
        this.walListener = walListener;
    }

    @TestOnly
    public void setWalLocker(@NotNull WalLocker walLocker) {
        this.walLocker = walLocker;
    }

    public void signalClose() {
        closing = true;
    }

    public void snapshotCreate(SqlExecutionCircuitBreaker circuitBreaker) throws SqlException {
        checkpointAgent.checkpointCreate(circuitBreaker, true, false);
    }

    public boolean tryLockWalPurgeJob(long timeout, TimeUnit timeUnit) {
        boolean isLocked = tryLockWalPurgeJob0(timeout, timeUnit);
        if (isLocked) {
            backupSeqPartLock.onLocked();
        }
        return isLocked;
    }

    public void unlock(
            @SuppressWarnings("unused") SecurityContext securityContext,
            TableToken tableToken,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        verifyTableToken(tableToken);
        unlockTableUnsafe(tableToken, writer, newTable);
        LOG.info().$("unlocked [table=").$(tableToken).$("]").$();
    }

    public void unlockReaders(TableToken tableToken) {
        verifyTableToken(tableToken);
        readerPool.unlock(tableToken);
    }

    public void unlockReadersAndMetadata(TableToken tableToken) {
        readerPool.unlock(tableToken);
        tableMetadataPool.unlock(tableToken);
    }

    public void unlockTableCreate(TableToken tableToken) {
        createTableLock.remove(tableToken.getTableName(), tableToken);
    }

    public void unlockTableName(TableToken tableToken) {
        tableNameRegistry.unlockTableName(tableToken);
    }

    public void unlockWalPurgeJob() {
        walPurgeJobLock.unlock();
    }

    public void unlockWalWriters(TableToken tableToken) {
        walWriterPool.unlock(tableToken, true);
    }

    public long update(CharSequence updateSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return update(updateSql, sqlExecutionContext, null);
    }

    public long update(CharSequence updateSql, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            while (true) {
                try {
                    CompiledQuery cc = compiler.compile(updateSql, sqlExecutionContext);
                    switch (cc.getType()) {
                        case UPDATE:
                            try (
                                    // update operation is stashed in the compiled query,
                                    // and it has to be released to avoid memory leak
                                    UpdateOperation ignore = cc.getUpdateOperation();
                                    OperationFuture future = cc.execute(eventSubSeq)
                            ) {
                                future.await();
                                return future.getAffectedRowsCount();
                            }
                        case INSERT:
                            throw SqlException.$(0, "use insert()");
                        case DROP:
                            throw SqlException.$(0, "use drop()");
                        case SELECT:
                            throw SqlException.$(0, "use select()");
                    }
                } catch (TableReferenceOutOfDateException ex) {
                    // retry, e.g. continue
                } catch (SqlException ex) {
                    if (Chars.contains(ex.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed")) {
                        continue;
                    }
                    throw ex;
                }
            }
        }
    }

    public void updateViewDefinition(
            TableToken viewToken,
            @NotNull String viewSql,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies,
            long seqTxn,
            BlockFileWriter blockFileWriter,
            Path path
    ) {
        path.of(configuration.getDbRoot());
        if (viewGraph.updateView(viewToken, viewSql, dependencies, seqTxn, blockFileWriter, path)) {
            viewStateStore.enqueueCompile(viewToken);
        }
    }

    public TableToken verifyTableName(final CharSequence tableName) {
        TableToken tableToken = tableNameRegistry.getTableToken(tableName);
        if (tableToken == null) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        if (tableToken == TableNameRegistry.LOCKED_TOKEN) {
            throw CairoException.nonCritical().put("table name is reserved [table=").put(tableName).put("]");
        }
        if (tableToken == TableNameRegistry.LOCKED_DROP_TOKEN) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return tableToken;
    }

    public TableToken verifyTableName(final CharSequence tableName, int lo, int hi) {
        StringSink sink = Misc.getThreadLocalSink();
        sink.put(tableName, lo, hi);
        return verifyTableName(sink);
    }

    public void verifyTableToken(TableToken tableToken) {
        TableToken tt = tableNameRegistry.getTableToken(tableToken.getTableName());
        if (tt == null || TableNameRegistry.isLocked(tt)) {
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
        if (!tt.equals(tableToken)) {
            throw TableReferenceOutOfDateException.of(tableToken, tableToken.getTableId(), tt.getTableId(), tt.getTableId(), -1);
        }
    }

    public void verifyViewToken(TableToken tableToken, long expectedTxn) {
        TableToken tt = tableNameRegistry.getTableToken(tableToken.getTableName());
        if (tt == null || TableNameRegistry.isLocked(tt)) {
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
        if (!tt.equals(tableToken)) {
            throw TableReferenceOutOfDateException.of(tableToken, tableToken.getTableId(), tt.getTableId(), tt.getTableId(), -1);
        }
        ViewDefinition vd = viewGraph.getViewDefinition(tableToken);
        if (vd == null) {
            // there is a race: tt.equals() passed but view definition got removed meanwhile
            throw TableReferenceOutOfDateException.of(tableToken, tableToken.getTableId(), tt.getTableId(), tt.getTableId(), -1);
        }
        if (vd.getSeqTxn() != expectedTxn) {
            throw TableReferenceOutOfDateException.ofOutdatedView(tableToken, expectedTxn, vd.getSeqTxn());
        }
    }

    private static void insert(
            CompiledQuery cq,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        switch (cq.getType()) {
            case INSERT:
            case INSERT_AS_SELECT:
                try (
                        InsertOperation insertOperation = cq.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.commit();
                }
                break;
            case SELECT:
                throw SqlException.$(0, "use select()");
            case DROP:
                throw SqlException.$(0, "use drop()");
            default:
                throw SqlException.$(0, "use ddl()");
        }
    }

    /**
     * Pass 2 of the ANCHOR EXPRESSION validator — the function-property half applied to
     * the post-constant-fold
     * Function tree. Rejects fold-to-constant top-level expressions, runtime-state
     * functions ({@code now()}, {@code current_timestamp}, ...), random functions,
     * non-deterministic functions, and aggregations. Pass 1 (AST-level rejects of
     * subqueries, bind variables, and well-known runtime/random function names by
     * token) lives in {@code SqlParser.walkAnchorExpressionForPurity}.
     * <p>
     * Walks the Function tree in pre-order: checks fire on the parent before
     * recursing into args. {@code BinaryFunction} / {@code UnaryFunction} et al.
     * propagate {@code isRandom} / {@code isNonDeterministic} from children, so the
     * recursion descends to surface the offending leaf's name in the error message.
     * <p>
     * The {@code isConstant()} check is gated by {@code isTopLevel} since constant
     * subexpressions are routine (e.g. the {@code '1d'} stride in
     * {@code timestamp_floor('1d', ts)}). The semantic guarantee is "the top-level
     * value depends on row data," which equals "the top-level expression is not
     * constant" post-fold.
     */
    private static void validateAnchorPurity(Function fn, int rootPosition, boolean isTopLevel) throws SqlException {
        if (fn == null) {
            return;
        }
        if (isTopLevel) {
            if (fn.isConstant()) {
                throw SqlException.$(rootPosition, "ANCHOR EXPRESSION must not be a constant");
            }
            validateAnchorReturnType(fn, rootPosition);
        }
        if (fn.isRandom() || fn.isRuntimeConstant() || fn.isNonDeterministic()) {
            throw SqlException.$(rootPosition, "ANCHOR EXPRESSION must be deterministic; ")
                    .put(fn.getName()).put("() is not allowed");
        }
        if (fn instanceof GroupByFunction) {
            throw SqlException.$(rootPosition, "ANCHOR EXPRESSION must not contain aggregation; ")
                    .put(fn.getName()).put("() is not allowed");
        }
        if (fn instanceof UnaryFunction u) {
            validateAnchorPurity(u.getArg(), rootPosition, false);
        } else if (fn instanceof BinaryFunction b) {
            validateAnchorPurity(b.getLeft(), rootPosition, false);
            validateAnchorPurity(b.getRight(), rootPosition, false);
        } else if (fn instanceof TernaryFunction t) {
            validateAnchorPurity(t.getLeft(), rootPosition, false);
            validateAnchorPurity(t.getCenter(), rootPosition, false);
            validateAnchorPurity(t.getRight(), rootPosition, false);
        } else if (fn instanceof MultiArgFunction m) {
            ObjList<Function> args = m.args();
            if (args != null) {
                for (int i = 0, n = args.size(); i < n; i++) {
                    validateAnchorPurity(args.getQuick(i), rootPosition, false);
                }
            }
        }
    }

    /**
     * Rejects anchor expressions whose top-level return type is not TIMESTAMP,
     * LONG, or INT. Other scalar primitives (BOOLEAN, DOUBLE, STRING, SYMBOL)
     * and composite types (ARRAY, GEOHASH, BINARY, INTERVAL, ...) cannot flow
     * through the anchor map's LONG slot. Catching the type mismatch at
     * CREATE keeps the failure visible to the operator instead of waiting for
     * the first refresh cycle to surface it.
     */
    private static void validateAnchorReturnType(Function fn, int position) throws SqlException {
        final int tag = ColumnType.tagOf(fn.getType());
        if (tag != ColumnType.TIMESTAMP && tag != ColumnType.LONG && tag != ColumnType.INT) {
            throw SqlException.$(position, "ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got ")
                    .put(ColumnType.nameOf(fn.getType()));
        }
    }

    /**
     * Validates the compiled SELECT shape against the live-view contract and returns
     * the leaf {@code PageFrameRecordCursorFactory} so the caller can resolve its
     * column dependencies against the base table.
     */
    private static PageFrameRecordCursorFactory validateLiveViewFactory(
            RecordCursorFactory factory,
            TableToken baseTableToken,
            int position
    ) throws SqlException {
        // SqlCompiler wraps every compiled query in a QueryProgress factory for registry tracking;
        // unwrap it (and any other transparent wrappers that expose getBaseFactory()) so we can
        // reason about the actual query shape.
        RecordCursorFactory root = factory;
        while (root instanceof QueryProgress) {
            root = root.getBaseFactory();
        }
        if (root instanceof CachedWindowRecordCursorFactory cwf) {
            // The planner picks the cached factory whenever any window function needs
            // multi-pass evaluation (e.g. lead, percentile, etc.). Surface the lead()
            // case with a specific message before the generic reject — lead()
            // is the most common cause and the user benefit of "use lag()" is high.
            rejectLeadIfPresent(cwf.getAllWindowFunctions(), position);
            throw SqlException.$(position, "live view select may only use window functions that support incremental refresh; " +
                    "this query requires caching or multi-pass evaluation");
        }
        if (!(root instanceof WindowRecordCursorFactory wf)) {
            throw SqlException.$(position, "live view select must contain at least one window function");
        }
        // incremental refresh only handles window functions that emit a value per input row
        // without looking ahead or buffering state across multiple passes
        ObjList<WindowFunction> fns = wf.getWindowFunctions();
        rejectLeadIfPresent(fns, position);
        for (int i = 0, n = fns.size(); i < n; i++) {
            validateLiveViewWindowFunction(fns.getQuick(i), position);
        }

        // Incremental refresh drives window functions manually over rows read directly
        // from WAL segments, so the factory tree must be exactly:
        //     WindowRecordCursorFactory -> [FilteredRecordCursorFactory?] -> PageFrameRecordCursorFactory
        // with no join, projection or grouping in between. See LiveViewRefreshJob.
        //
        // A single filter factory (FilteredRecordCursorFactory / AsyncFilteredRecordCursorFactory /
        // AsyncJitFilteredRecordCursorFactory) may sit between the window and the page frame factory;
        // the refresh job applies its Function filter row-by-row to WAL segment rows during
        // incremental refresh. Indexed-symbol key extraction is suppressed during live view
        // compilation (see SqlExecutionContext.isLiveViewCompile and WhereClauseParser), so the
        // planner never pushes the filter into the row cursor factory.
        RecordCursorFactory base = wf.getBaseFactory();
        if (base.getFilter() != null) {
            base = base.getBaseFactory();
            if (base == null) {
                throw SqlException.$(position, "live view select has a malformed filter factory");
            }
        }
        for (RecordCursorFactory f = base.getBaseFactory(); f != null; f = f.getBaseFactory()) {
            if (f.getFilter() != null) {
                throw SqlException.$(position, "live view select cannot use nested filter factories yet");
            }
        }
        if (!(base instanceof PageFrameRecordCursorFactory pfrcf) || base.getBaseFactory() != null) {
            throw SqlException.$(position, "live view select must be a simple scan of a single WAL base table; " +
                    "joins, subqueries, GROUP BY, ORDER BY and LIMIT are not supported yet");
        }
        if (pfrcf.hasFilter()) {
            // Defensive: WhereClauseParser is supposed to have suppressed indexed-symbol key
            // extraction for live view compiles, so the planner shouldn't produce an indexed
            // row cursor factory here. If it ever does, the filter Function is invisible to
            // the incremental refresh path and rows would slip through unfiltered.
            throw SqlException.$(position, "live view select produced an indexed row cursor factory unexpectedly");
        }
        TableToken scannedToken = base.getTableToken();
        if (scannedToken == null || !scannedToken.equals(baseTableToken)) {
            throw SqlException.$(position, "live view select must read from the declared base table");
        }
        return pfrcf;
    }

    /**
     * Throws an asserted-wording reject if any function in {@code fns} is {@code lead()}.
     * lead() needs rows the streaming append-only path has not yet seen; live views
     * reject it independently of the underlying factory shape.
     */
    private static void rejectLeadIfPresent(ObjList<WindowFunction> fns, int position) throws SqlException {
        for (int i = 0, n = fns.size(); i < n; i++) {
            if ("lead".equals(fns.getQuick(i).getName())) {
                throw SqlException.$(position, "lead() is not supported in live views; use lag() for lookback");
            }
        }
    }

    // Commits the registry name for a table created via the deferred-register
    // path (createTableOrViewOrMatViewUnsecure with deferRegisterName=true) and
    // releases the per-token name and create locks the deferred helper handed
    // off to the caller. The registry commit is the atomic visibility cut:
    // before this returns, concurrent name lookups fail with
    // "table does not exist"; after it returns the table is queryable.
    private void commitDeferredTableNameAndRelease(TableToken tableToken) {
        try {
            tableNameRegistry.registerName(tableToken);
        } finally {
            tableNameRegistry.unlockTableName(tableToken);
            unlockTableCreate(tableToken);
        }
        enqueueCompileView(tableToken);
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    private void createTableOrMatViewInVolumeUnsafe(MemoryMARW mem, @Nullable BlockFileWriter blockFileWriter, Path path, TableStructure struct, TableToken tableToken) {
        if (TableUtils.TABLE_DOES_NOT_EXIST != TableUtils.existsInVolume(configuration.getFilesFacade(), path, tableToken.getDirName())) {
            throw CairoException.nonCritical().put("name is reserved [table=").put(tableToken.getTableName()).put(']');
        }

        // only create the table after it has been registered
        TableUtils.createTableOrMatViewInVolume(
                configuration.getFilesFacade(),
                configuration.getDbRoot(),
                getTelemetry(),
                configuration.getMkDirMode(),
                mem,
                blockFileWriter,
                path,
                tableToken.getDirName(),
                struct,
                ColumnType.VERSION,
                tableToken.getTableId()
        );
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    private void createTableOrViewOrMatViewUnsafe(MemoryMARW mem, @Nullable BlockFileWriter blockFileWriter, Path path, TableStructure struct, TableToken tableToken) {
        if (TableUtils.exists(configuration.getFilesFacade(), path, configuration.getDbRoot(), tableToken.getDirName()) != TableUtils.TABLE_DOES_NOT_EXIST) {
            throw CairoException.nonCritical().put("name is reserved [table=").put(tableToken.getTableName()).put(']');
        }

        // only create the table after it has been registered
        TableUtils.createTableOrViewOrMatView(
                configuration.getFilesFacade(),
                configuration.getDbRoot(),
                configuration.getMkDirMode(),
                getTelemetry(),
                mem,
                blockFileWriter,
                path,
                tableToken.getDirName(),
                struct,
                ColumnType.VERSION,
                tableToken.getTableId()
        );
    }

    /**
     * Creates the on-disk filesystem skeleton for a table / view / mat view /
     * live view and reserves the registry name. When deferRegisterName is
     * false (the default for CREATE TABLE / VIEW / MAT VIEW), the registry
     * commit is the last step before return and the result is always non-null.
     * <p>
     * When deferRegisterName is true (CREATE LIVE VIEW), the registry commit
     * is held back: the caller now owns the name + create locks and must
     * invoke {@link #commitDeferredTableNameAndRelease} after fsyncing any
     * follow-up artifacts (e.g. {@code _lv.s} / {@code _lv}), or
     * {@link #rollbackDeferredLiveViewCreate} on failure. In deferred mode the
     * result is null when no deferred handoff took place - the IF NOT EXISTS
     * pre-existing path or the IF NOT EXISTS lock-race-lost path - so the
     * caller has no follow-up work in that case.
     */
    private TableToken createTableOrViewOrMatViewUnsecure(
            SecurityContext securityContext,
            MemoryMARW mem,
            @Nullable BlockFileWriter blockFileWriter,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock,
            boolean inVolume,
            int tableKind,
            boolean deferRegisterName
    ) {
        assert !struct.isWalEnabled() || PartitionBy.isPartitioned(struct.getPartitionBy()) : "WAL is only supported for partitioned tables";
        final CharSequence tableName = struct.getTableName();
        validNameOrThrow(tableName);

        final int tableId = (int) tableIdGenerator.getNextId();

        while (true) {
            TableToken tableToken = lockTableName(tableName, tableId, struct.isView(), struct.isMatView(), struct.isLiveView(), struct.isWalEnabled());
            if (tableToken == null) {
                if (ifNotExists) {
                    tableToken = getTableTokenIfExists(tableName);
                    if (tableToken != null) {
                        struct.init(tableToken);
                        // Deferred mode: the LV already exists - no handoff,
                        // no follow-up. Returning null tells the caller to
                        // skip the FS writes and the commit step.
                        return deferRegisterName ? null : tableToken;
                    }
                    Os.pause();
                    continue;
                }
                throw EntryUnavailableException.instance("table exists");
            }
            struct.init(tableToken);
            while (!lockTableCreate(tableToken)) {
                Os.pause();
            }
            boolean filesystemCreated = false;
            // Tracks whether the deferred caller now owns the name lock and the
            // per-token create lock. Stays false on every error path so the
            // outer finally unwinds the locks the same way as a non-deferred
            // create, and stays false on the lockedReason+ifNotExists fall-
            // through path so deferred callers see a null return instead of a
            // token whose name was never registered.
            boolean deferredHandoff = false;
            try {
                String lockedReason = lockAll(tableToken, "createTable", true);
                boolean locked = true;
                if (lockedReason == null) {
                    try {
                        if (inVolume) {
                            createTableOrMatViewInVolumeUnsafe(mem, blockFileWriter, path, struct, tableToken);
                        } else {
                            createTableOrViewOrMatViewUnsafe(mem, blockFileWriter, path, struct, tableToken);
                        }
                        filesystemCreated = true;

                        if (struct.isWalEnabled()) {
                            tableSequencerAPI.registerTable(tableToken.getTableId(), struct, tableToken);
                        }

                        if (!keepLock) {
                            // Unlock pools before registering the name
                            // to avoid `table busy` errors when trying to use the table immediately after registration
                            // in concurrent threads
                            unlockTableUnsafe(tableToken, null, true);
                            locked = false;
                            LOG.info().$("unlocked [table=").$(tableToken).$("]").$();
                        }
                        onTableOrViewOrMatViewCreated(securityContext, struct, tableToken, tableKind);
                        if (deferRegisterName) {
                            deferredHandoff = true;
                        } else {
                            tableNameRegistry.registerName(tableToken);
                        }
                    } catch (Throwable e) {
                        keepLock = false;
                        throw e;
                    } finally {
                        if (!keepLock && locked) {
                            unlockTableUnsafe(tableToken, null, false);
                            LOG.info().$("unlocked [table=").$(tableToken).$("]").$();
                        }
                    }
                } else {
                    if (!ifNotExists) {
                        throw EntryUnavailableException.instance(lockedReason);
                    }
                }
            } catch (Throwable th) {
                if (struct.isWalEnabled()) {
                    // tableToken.getLoggingName() === tableName, table cannot be renamed while creation hasn't finished
                    tableSequencerAPI.dropTable(tableToken, true);
                } else if (filesystemCreated) {
                    final FilesFacade ff = configuration.getFilesFacade();
                    try {
                        path.of(configuration.getDbRoot()).concat(tableToken).$();
                        Path volumeTarget = null;
                        if (ff.isSoftLink(path.$())) {
                            volumeTarget = Path.getThreadLocal2("");
                            if (!ff.readLink(path, volumeTarget)) {
                                volumeTarget = null;
                            }
                        }
                        if (!ff.unlinkOrRemove(path, LOG)) {
                            LOG.error().$("could not clean up partial table after failed create [table=").$(tableToken)
                                    .$(", errno=").$(ff.errno()).I$();
                        }
                        if (volumeTarget != null && !ff.unlinkOrRemove(volumeTarget, LOG)) {
                            LOG.error().$("could not clean up partial table in volume [table=").$(tableToken)
                                    .$(", errno=").$(ff.errno()).I$();
                        }
                    } catch (Throwable cleanupEx) {
                        LOG.error().$("cleanup after failed create threw [table=").$(tableToken)
                                .$(", err=").$(cleanupEx).I$();
                    }
                }
                throw th;
            } finally {
                if (!deferredHandoff) {
                    tableNameRegistry.unlockTableName(tableToken);
                    unlockTableCreate(tableToken);
                }
            }

            if (!deferredHandoff) {
                if (deferRegisterName) {
                    // IF NOT EXISTS lost the lockAll race - the helper did no
                    // work the deferred caller can finalise.
                    return null;
                }
                enqueueCompileView(tableToken);
            }
            return tableToken;
        }
    }

    private @NotNull ViewMetadata getViewMetadata(TableToken tableToken) {
        final ViewState state = viewStateStore.getViewState(tableToken);
        if (state == null) {
            throw CairoException.viewDoesNotExist(tableToken.getTableName());
        }
        return state.getViewMetadata();
    }

    private void notifyViewStoresAboutDrop(TableToken droppedToken) {
        viewGraph.removeView(droppedToken);
        viewStateStore.removeViewState(droppedToken);

        // this event will result in recompiling of dependent views, and updating their state
        enqueueCompileView(droppedToken);
    }

    private void onTableOrViewOrMatViewCreated(
            SecurityContext securityContext,
            TableStructure struct,
            TableToken tableToken,
            int tableKind
    ) {
        final DdlListener ddlListener = getDdlListener(tableToken);
        ddlListener.onTableOrViewOrMatViewCreated(securityContext, tableToken, tableKind);
        try {
            struct.onCreated(this, tableToken);
        } catch (Throwable th) {
            try {
                ddlListener.onTableOrViewOrMatViewDropped(tableToken);
            } catch (Throwable rollbackEx) {
                th.addSuppressed(rollbackEx);
            }
            throw th;
        }
    }

    private TableToken rename0(Path fromPath, TableToken fromTableToken, Path toPath, CharSequence toTableName) {

        // !!! we do not care what is inside the path1 & path2, we will reset them anyway
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getDbRoot();

        fromPath.of(root).concat(fromTableToken).$();

        final TableToken toTableToken = lockTableName(toTableName, fromTableToken.getTableId(), fromTableToken.isView(), fromTableToken.isMatView(), fromTableToken.isWal());
        if (toTableToken == null) {
            LOG.error()
                    .$("rename target exists [from='").$(fromTableToken)
                    .$("', to='").$safe(toTableName)
                    .I$();
            throw CairoException.nonCritical().put("Rename target exists");
        }
        while (!lockTableCreate(toTableToken)) {
            Os.pause();
        }

        if (ff.exists(toPath.of(root).concat(toTableToken).$())) {
            tableNameRegistry.unlockTableName(toTableToken);
        }

        try {
            if (ff.rename(fromPath.$(), toPath.$()) != Files.FILES_RENAME_OK) {
                final int error = ff.errno();
                LOG.error()
                        .$("could not rename [from='").$(fromPath)
                        .$("', to='").$(toPath)
                        .$("', error=").$(error)
                        .I$();
                throw CairoException.critical(error)
                        .put("could not rename [from='").put(fromPath)
                        .put("', to='").put(toPath)
                        .put(']');
            }
            tableNameRegistry.registerName(toTableToken);
            return toTableToken;
        } finally {
            tableNameRegistry.unlockTableName(toTableToken);
            unlockTableCreate(toTableToken);
        }
    }

    // Best-effort cleanup for a live view CREATE that failed between
    // createTableOrViewOrMatViewUnsecure(deferRegisterName=true) and
    // commitDeferredTableNameAndRelease. dropTableOrViewOrMatView cannot run
    // yet because the token is not committed to the registry, so this method
    // drops the sequencer entry, removes the on-disk directory, and releases
    // the per-token name and create locks. All steps swallow failures and log,
    // because the caller's outer throw carries the original cause.
    private void rollbackDeferredLiveViewCreate(Path path, TableToken tableToken) {
        try {
            try {
                tableSequencerAPI.dropTable(tableToken, true);
            } catch (Throwable th) {
                LOG.error().$("could not drop sequencer entry for partial live view [view=").$(tableToken)
                        .$(", err=").$(th).I$();
            }
            try {
                final FilesFacade ff = configuration.getFilesFacade();
                path.of(configuration.getDbRoot()).concat(tableToken).$();
                if (ff.exists(path.$()) && !ff.unlinkOrRemove(path, LOG)) {
                    LOG.error().$("could not clean up partial live view fs [view=").$(tableToken)
                            .$(", errno=").$(ff.errno()).I$();
                }
            } catch (Throwable th) {
                LOG.error().$("could not clean up partial live view fs [view=").$(tableToken)
                        .$(", err=").$(th).I$();
            }
        } finally {
            // Both unlocks are safe to call after commitDeferredTableNameAndRelease
            // already released them: unlockTableName is remove(name, LOCKED_TOKEN),
            // unlockTableCreate is remove(name, token) - both no-op when absent.
            try {
                tableNameRegistry.unlockTableName(tableToken);
            } catch (Throwable ignored) {
            }
            try {
                unlockTableCreate(tableToken);
            } catch (Throwable ignored) {
            }
        }
    }

    private boolean tryLockWalPurgeJob0(long timeout, TimeUnit timeUnit) {
        if (timeout == 0) {
            return walPurgeJobLock.tryLock();
        }
        return walPurgeJobLock.tryLock(timeout, timeUnit);
    }

    private void tryRepairTable(TableToken tableToken, CairoException rethrow) {
        LOG.info()
                .$("starting table repair [table=").$(tableToken)
                .$(", cause=").$safe(rethrow.getFlyweightMessage())
                .I$();
        try {
            writerPool.get(tableToken, "repair").close();
            LOG.info().$("table repair succeeded [table=").$(tableToken).I$();
        } catch (EntryUnavailableException e) {
            // This is fine, writer is busy. Throw back origin error.
            LOG.info().$("writer is busy, skipping repair [table=").$(tableToken).I$();
            throw rethrow;
        } catch (Throwable th) {
            LOG.critical()
                    .$("table repair failed [table=").$(tableToken)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            throw rethrow;
        }
    }

    private void unlockTableUnsafe(TableToken tableToken, TableWriter writer, boolean newTable) {
        readerPool.unlock(tableToken);
        writerPool.unlock(tableToken, writer, newTable);
        sequencerMetadataPool.unlock(tableToken);
        tableMetadataPool.unlock(tableToken);
    }

    private void validNameOrThrow(CharSequence tableName) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
    }

    private void validateDesiredMetadataVersion(TableToken tableToken, TableRecordMetadata metadata, long desiredVersion) {
        if ((desiredVersion != TableUtils.ANY_TABLE_VERSION && metadata.getMetadataVersion() != desiredVersion) || tableToken.getTableId() != metadata.getTableId()) {
            final TableReferenceOutOfDateException ex = TableReferenceOutOfDateException.of(
                    tableToken,
                    tableToken.getTableId(),
                    metadata.getTableId(),
                    desiredVersion,
                    metadata.getMetadataVersion()
            );
            metadata.close();
            throw ex;
        }
    }

    @NotNull
    private TableToken verifyTableNameForRead(CharSequence tableName) {
        TableToken token = getTableTokenIfExists(tableName);
        if (token == null || TableNameRegistry.isLocked(token)) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return token;
    }

    // Writes the durable _lv.drop sentinel into the live view's directory and
    // fsyncs it before returning. Idempotent: a repeated DROP that finds an
    // existing sentinel re-opens, fsyncs and closes - the file's existence is
    // the signal, not its contents. Failure throws CairoException with the
    // path; the caller's DROP aborts before any teardown so the LV remains
    // queryable.
    //
    // Unlike the .cp writer, this deliberately does NOT tmp+rename. Recovery
    // keys off existence alone, so a zero-byte or partially written _lv.drop
    // still correctly triggers the reap branch; a tmp+rename scheme would
    // instead lose a crash-before-rename drop intent. Existence-only is the
    // stronger durability contract here, not an oversight.
    private void writeLiveViewDropSentinel(TableToken token) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(LiveViewDefinition.LIVE_VIEW_DROP_SENTINEL_FILE_NAME).$();
            long fd = TableUtils.openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
            try {
                ff.fsync(fd);
            } finally {
                ff.close(fd);
            }
        }
    }

    protected void clearDdlListener() {
        ddlListener.clear();
    }

    protected @NotNull DependentViewGraph createDependentViewGraph() {
        return new DependentViewGraph();
    }

    // used in ent
    protected MatViewStateStore createMatViewStateStore() {
        return configuration.isMatViewEnabled() ? new MatViewStateStoreImpl(this) : NoOpMatViewStateStore.INSTANCE;
    }

    // used in ent
    protected Queue<MatViewTimerTask> createMatViewTimerQueue() {
        return configuration.isMatViewEnabled() ? ConcurrentQueue.createConcurrentQueue(MatViewTimerTask.ITEM_FACTORY) : new NoOpQueue<>();
    }

    protected SqlExecutionContext createRootExecutionContext() {
        return new SqlExecutionContextImpl(this, 0).with(AllowAllSecurityContext.INSTANCE);
    }

    protected @NotNull TableNameRegistry createTableNameRegistry(CairoConfiguration configuration, TableFlagResolver tableFlagResolver) {
        return configuration.isReadOnlyInstance()
                ? new TableNameRegistryRO(this, tableFlagResolver)
                : new TableNameRegistryRW(this, tableFlagResolver);
    }

    protected @NotNull <T extends AbstractTelemetryTask> Telemetry<T> createTelemetry(
            Telemetry.TelemetryTypeBuilder<T> builder,
            CairoConfiguration configuration
    ) {
        return new Telemetry<>(builder, configuration);
    }

    protected @NotNull ViewGraph createViewGraph() {
        return new ViewGraph();
    }

    protected Iterable<FunctionFactory> getFunctionFactories() {
        return new FunctionFactoryCacheBuilder().scan(LOG).build();
    }

    protected void initDataID() {
        if (!getConfiguration().isReadOnlyInstance() && !this.dataID.isInitialized()) {
            final Rnd rnd = configuration.getRandom();
            this.dataID.initialize(rnd.nextLong(), rnd.nextLong());
        }
    }

    protected TableFlagResolver newTableFlagResolver(CairoConfiguration configuration) {
        return new TableFlagResolverImpl(configuration.getSystemTableNamePrefix().toString());
    }

    protected void restoreBackup() {
        // Hook for backup functionality. See enterprise subclass.
    }
}
