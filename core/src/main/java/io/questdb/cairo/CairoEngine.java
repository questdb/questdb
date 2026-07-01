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
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewGraph;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.mv.MatViewTimerTask;
import io.questdb.cairo.mv.NoOpMatViewStateStore;
import io.questdb.cairo.mv.WalTxnRangeLoader;
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
import io.questdb.cairo.wal.UnsupportedWalTxnTypeHandler;
import io.questdb.cairo.wal.ViewWalWriter;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalListener;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalTxnTypeHandler;
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
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerFactoryImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.CreateMatViewOperation;
import io.questdb.griffin.engine.ops.CreateViewOperation;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.NoOpQueue;
import io.questdb.mp.Queue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.mp.continuation.TxnWaiter;
import io.questdb.preferences.SettingsStore;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.MemoryTrackerProvider;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.questdb.griffin.CompiledQuery.*;

public class CairoEngine implements Closeable, WriterSource {
    public static final String REASON_BUSY_READER = "busyReader";
    public static final String REASON_BUSY_SEQUENCER_METADATA_POOL = "busySequencerMetaPool";
    public static final String REASON_BUSY_TABLE_READER_METADATA_POOL = "busyTableReaderMetaPool";
    public static final String REASON_CHECKPOINT_IN_PROGRESS = "checkpointInProgress";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    // Hard cap on TableReferenceOutOfDateException recompile retries in execute(). A
    // healthy table converges in 1-2 retries; an unbounded loop here turns a permanent
    // metadata/token mismatch into a silent infinite hang (see PR #7031 CI timeout).
    private static final int MAX_EXECUTE_RETRIES = 1000;
    private static final int MAX_SLEEP_MILLIS = 250;
    private static final CarrierLocal<MatViewRefreshTask> tlMatViewRefreshTask = new CarrierLocal<>(MatViewRefreshTask::new);
    protected final CairoConfiguration configuration;
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final BackupSeqPartLock backupSeqPartLock = new BackupSeqPartLock();
    private final DatabaseCheckpointAgent checkpointAgent;
    private final CopyExportContext copyExportContext;
    private final CopyImportContext copyImportContext;
    private final ConcurrentHashMap<TableToken> createTableLock = new ConcurrentHashMap<>();
    private final DataID dataID;
    private final FunctionFactoryCache ffCache;
    private final MatViewGraph matViewGraph;
    private final Queue<MatViewTimerTask> matViewTimerQueue;
    private final MessageBusImpl messageBus;
    // volatile: assigned by completeInit() on the orchestrator thread, read by worker threads
    // and hydration threads; volatile ensures safe cross-thread publication after completeInit.
    private volatile MetadataCache metadataCache;
    private final Metrics metrics;
    private final PartitionOverwriteControl partitionOverwriteControl = new PartitionOverwriteControl();
    private final QueryRegistry queryRegistry;
    private final ReaderPool readerPool;
    private final RecentWriteTracker recentWriteTracker;
    // Fences client commits against the PRIMARY-to-REPLICA role flip. Commit/DDL paths
    // (TableUpdateDetails.commit/closeNoLock/releaseWriter, the /exec and pg-wire executor
    // commits, the ILP-UDP flush) hold the READ side while re-checking read-only mode and
    // calling writerAPI.commit(); the role-flip path in EntCairoEngine holds the WRITE side
    // around the REPLICA flag publish only. Read/read concurrency restores N-way commit
    // throughput across tables and protocols; the read/write exclusion is the invariant that
    // keeps a commit from slipping through the gate-read and landing on a demoting node. The
    // write side is never held across drainWriterPool. A plain OSS deployment that never flips
    // role only ever pays an uncontended read acquire (a single CAS in the common case), so no
    // separate "no flip path" gate is needed.
    // Test seam: when non-null, fireRoleSwitchMintObserver() runs this hook at an externalization that
    // mints replicated state, so a witness can pause the externalization there and interleave a
    // concurrent PRIMARY-to-REPLICA demote deterministically (without host load). It fires from two
    // kinds of sites, on BOTH the fenced and the unfenced tree, so a witness that arms this one seam
    // trips whether or not the role-switch fence wraps the externalization:
    //   1. Inside the role-switch read-lock hold of the parse-time DDL fences (TRUNCATE truncateSoft,
    //      RENAME TABLE, ALTER VIEW ... AS, ALTER TABLE ... SET STORAGE POLICY / SET TYPE) and the
    //      ENT replicated-write / mat-view fences. These mints externalize inside compile() and never
    //      reach OperationDispatcher.
    //   2. At the OperationDispatcher externalization site BEFORE its read-lock acquire (the WAL
    //      UPDATE / ALTER inline-apply and async-enqueue paths), so a reverted/absent dispatcher fence
    //      still fires the observer rather than degrading the witness to a timing-only sleep window.
    // A paused witness either blocks the demote behind the read hold or expires its tryLock budget,
    // exercising the demote race deterministically. Null in production (the default): every fire-site is
    // a single static volatile read with no side effect when no test installed a hook.
    @TestOnly
    private static volatile Runnable roleSwitchMintObserver;
    private final ReentrantReadWriteLock roleSwitchLock = new ReentrantReadWriteLock();
    private final SqlExecutionContext rootExecutionContext;
    private final TxnScoreboardPool scoreboardPool;
    private final SequencerMetadataPool sequencerMetadataPool;
    // volatile: see metadataCache comment above.
    private volatile SettingsStore settingsStore;
    // volatile: see metadataCache comment above.
    private volatile SqlCompilerPool sqlCompilerPool;
    private final TableFlagResolver tableFlagResolver;
    private final IDGenerator tableIdGenerator;
    private final TableMetadataPool tableMetadataPool;
    // volatile: see metadataCache comment above.
    private volatile TableNameRegistry tableNameRegistry;
    private final TableSequencerAPI tableSequencerAPI;
    private final ObjList<Telemetry<? extends AbstractTelemetryTask>> telemetries;
    private final Telemetry<TelemetryTask> telemetry;
    private final Telemetry<TelemetryMatViewTask> telemetryMatView;
    private final Telemetry<TelemetryWalTask> telemetryWal;
    private final TimerShards timerShards;
    // initial value of unpublishedWalTxnCount is 1 because we want to scan for non-applied WAL transactions on startup
    private final AtomicLong unpublishedWalTxnCount = new AtomicLong(1);
    private final ViewGraph viewGraph;
    private final ViewWalWriterPool viewWalWriterPool;
    private final SimpleWaitingLock walPurgeJobLock = new SimpleWaitingLock();
    private final WalWriterPool walWriterPool;
    private final WriterPool writerPool;
    private volatile boolean closing;
    private volatile boolean isCompleteInitDone;
    // volatile: configReloader is reassigned by EntCairoEngine.switchRole on the lifecycle
    // thread and read by SqlCompilerImpl, GenericDropOperation, WriterPool, WalWriterPool,
    // CopyImportTask on worker threads; no implicit fence between writer and readers.
    private volatile @NotNull ConfigReloader configReloader = new ConfigReloader() {
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
    // volatile: ddlListener is reassigned by EntCairoEngine.switchRole on the lifecycle thread
    // (DefaultDdlListener.INSTANCE on REPLICA, EntDdlListener instance on PRIMARY) and read by
    // SqlCompilerImpl on worker threads. Matches the sibling volatile durableAckRegistry.
    private volatile @NotNull DdlListener ddlListener = DefaultDdlListener.INSTANCE;
    private volatile @NotNull DurableAckRegistry durableAckRegistry = DefaultDurableAckRegistry.INSTANCE;
    private FrameFactory frameFactory;
    private @NotNull MatViewStateStore matViewStateStore = NoOpMatViewStateStore.INSTANCE;
    // Lazily initialized on first call to getMemoryTrackerProvider(), because the
    // FactoryProvider that produces it is not bound until config.init(engine, ...)
    // runs, which is *after* the engine constructor returns.
    private volatile MemoryTrackerProvider memoryTrackerProvider;
    private volatile Runnable recentWriteTrackerHydrationCallback;
    private @NotNull ViewStateStore viewStateStore = NoOpViewStateStore.INSTANCE;
    private @NotNull WalDirectoryPolicy walDirectoryPolicy = DefaultWalDirectoryPolicy.INSTANCE;
    // volatile: walListener is reassigned by PrimaryRoleState.openLoops on the lifecycle thread
    // and read by TableSequencerImpl and TableSequencerAPI on sequencer/apply threads; no
    // implicit fence between writer and readers.
    private volatile @NotNull WalListener walListener = DefaultWalListener.INSTANCE;
    private @NotNull WalLocker walLocker;

    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, new QdbrWalLocker());
    }

    public CairoEngine(CairoConfiguration configuration, boolean completeInit) {
        this(configuration, new QdbrWalLocker(), completeInit);
    }

    public CairoEngine(CairoConfiguration configuration, @NotNull WalLocker walLocker) {
        this(configuration, walLocker, true);
    }

    /**
     * Three-argument constructor that holds the substantive initialization body.
     * <p>
     * When {@code completeInit} is {@code false}, construction stops after
     * {@link DataID#open(CairoConfiguration)} -- the fields
     * {@code tableNameRegistry}, {@code metadataCache}, {@code sqlCompilerPool},
     * and {@code settingsStore} are {@code null}. The caller MUST invoke
     * {@link #completeInit()} before using those fields.
     * <p>
     * The back-compat one-arg and two-arg constructors delegate here with
     * {@code completeInit=true} so all existing call sites are unaffected.
     */
    public CairoEngine(CairoConfiguration configuration, @NotNull WalLocker walLocker, boolean completeInit) {
        try {
            this.walLocker = walLocker;
            this.ffCache = new FunctionFactoryCache(configuration, getFunctionFactories());
            this.tableFlagResolver = newTableFlagResolver(configuration);
            this.configuration = configuration;
            this.copyImportContext = new CopyImportContext(this, configuration);
            this.copyExportContext = new CopyExportContext(this);
            this.tableSequencerAPI = new TableSequencerAPI(this, configuration);
            // Per-deadline blocking timer threads. Each parked TxnWaiter (or other
            // DelayedFireable) sits in a shard and is woken at its precise deadline,
            // bounding resource retention when a wait_wal_table call parks and the
            // client disconnects or the table goes idle. Started eagerly here because
            // the threads are independent of any worker pool. signalClose() drains,
            // close() halts defensively.
            this.timerShards = new TimerShards(
                    configuration.getTimerShardCount(),
                    "questdb-timer",
                    LOG
            );
            this.timerShards.start();
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
            this.matViewGraph = createMatViewGraph();
            this.viewGraph = createViewGraph();
            this.frameFactory = new FrameFactory(configuration);
            this.dataID = DataID.open(configuration);

            if (completeInit) {
                completeInit();
            }
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
     * Adds a table to the runtime hard-suspend set, excluding it from WAL apply until removed.
     * Called by {@code ALTER TABLE ... SUSPEND WAL}.
     */
    public void addWalApplySuspended(TableToken tableToken) {
        tableSequencerAPI.setHardSuspended(tableToken, true);
    }

    public void applyTableRename(TableToken token, TableToken updatedTableToken) {
        if (updatedTableToken.isMatView() && matViewGraph.getViewDefinition(updatedTableToken) == null) {
            throw CairoException.nonCritical().put("materialized view has not been registered yet [name=").put(updatedTableToken.getTableName()).put(']');
        }
        tableNameRegistry.rename(token.getTableName(), updatedTableToken.getTableName(), token);
        if (token.isWal()) {
            tableSequencerAPI.applyRename(updatedTableToken);
        }
        if (updatedTableToken.isMatView()) {
            matViewGraph.updateToken(updatedTableToken);
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
                BlockFileReader reader = new BlockFileReader(configuration);
                WalEventReader walEventReader = new WalEventReader(configuration);
                MemoryCMR txnMem = Vm.getCMRInstance(configuration.getBypassWalFdCache())
        ) {
            path.of(configuration.getDbRoot());
            final int pathLen = path.size();
            final MatViewStateReader matViewStateReader = new MatViewStateReader();
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
                                // createViewState hydrates the metadata cache from the on-disk
                                // view definition only, which carries no designated timestamp.
                                // Enqueue a compile so the view compiler job recomputes the full
                                // view metadata (including the designated timestamp) once it starts,
                                // matching how the WAL apply path registers a freshly replicated view.
                                enqueueCompileView(tableToken);
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
                    // Boot path: read the on-disk definition when the graph has not seen this view yet,
                    // adding it to the graph and creating its state. createState=false here so a view
                    // already present in the graph keeps its existing state -- buildViewGraphs runs once
                    // at boot before the store has any state.
                    loadMatViewIntoStore(
                            tableToken,
                            path,
                            pathLen,
                            reader,
                            walEventReader,
                            txnMem,
                            matViewStateReader,
                            false
                    );
                }
            }
        }
    }

    /**
     * Repopulates the live mat-view state store from the view graph and the on-disk {@code _mv}
     * state, for every mat-view already present in {@code matViewGraph}. Unlike
     * {@link #buildViewGraphs()} (which only creates state for views not yet in the graph), this
     * forces {@code createViewState} for each graph view that has no state yet, so a freshly built
     * store on a role promote ends up populated rather than empty. Idempotent: a view that already
     * has state is re-initialized from disk, not duplicated.
     * <p>
     * Used by the enterprise role switch: a promote builds a real {@link MatViewStateStore} and then
     * calls this to hydrate it before writes open, so refresh resumes from the persisted baselines
     * instead of triggering a full-refresh storm.
     */
    public void hydrateMatViewStateStore() {
        final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
        getTableTokens(tableTokenBucket, false);
        try (
                Path path = new Path();
                BlockFileReader reader = new BlockFileReader(configuration);
                WalEventReader walEventReader = new WalEventReader(configuration);
                MemoryCMR txnMem = Vm.getCMRInstance(configuration.getBypassWalFdCache())
        ) {
            path.of(configuration.getDbRoot());
            final int pathLen = path.size();
            final MatViewStateReader matViewStateReader = new MatViewStateReader();
            for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
                final TableToken tableToken = tableTokenBucket.get(i);
                if (tableToken.isMatView() && TableUtils.isMatViewDefinitionFileExists(configuration, path, tableToken.getDirName())) {
                    loadMatViewIntoStore(
                            tableToken,
                            path,
                            pathLen,
                            reader,
                            walEventReader,
                            txnMem,
                            matViewStateReader,
                            true
                    );
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
        matViewGraph.clear();
        matViewStateStore.clear();
        matViewTimerQueue.clear();
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
        // Drain the per-workload memory-tracker pool so a tracker acquired in
        // a previous test does not survive as a retained native block and
        // trip the test infrastructure's leak checker.
        Misc.clear(memoryTrackerProvider);
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
        Misc.free(matViewStateStore);
        Misc.free(settingsStore);
        Misc.free(frameFactory);
        Misc.free(walLocker);
        Misc.free(memoryTrackerProvider);
        // Defensive: signalClose() already shutdown timerShards in the normal path.
        // halt() is idempotent and ensures the daemon threads are joined even if
        // close() runs without a prior signalClose(). Null guard covers the
        // partial-construction path where the constructor's catch block calls
        // close() before timerShards has been assigned.
        if (timerShards != null) {
            // close() must be reached through signalClose() whenever there are
            // parked continuations: signalClose() drains them while the worker
            // pools are still RUNNING, so the bodies remount and unwind. halt()
            // only joins the daemon threads, so a parked cont reaching here with
            // closing == false would be stranded (never resumed, native stack
            // abandoned). A non-empty shard set at this point is that bug.
            assert closing || timerShards.size() == 0
                    : "close() reached with parked continuations but without a prior signalClose(); they will be stranded";
            timerShards.halt();
        }
    }

    @TestOnly
    public void closeNameRegistry() {
        tableNameRegistry.close();
    }

    /**
     * Runs the post-restore engine initialization that historically lived inside the constructor.
     * MUST be called after
     * BackupRestoreEnvelope.start() reaches READY when the orchestrator boots the engine envelope.
     * Ordering is now enforced by the lifecycle DAG (engine.hardDeps includes "backup-restore"),
     * not by sequential statement ordering as it used to be inside the constructor.
     * <p>
     * Calling twice on the same instance has undefined behavior -- fields like tableNameRegistry
     * and metadataCache would be re-assigned, leaking the previous instances. Production code
     * (the orchestrator EngineEnvelope.start() and the back-compat constructors) invoke it
     * exactly once.
     */
    public void completeInit() {
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
        this.isCompleteInitDone = true;
    }

    public boolean isCompleteInitDone() {
        return isCompleteInitDone;
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
        final TableToken matViewToken = createTableOrViewOrMatViewUnsecure(securityContext, mem, blockFileWriter, path, ifNotExists, operation, keepLock, inVolume, TableUtils.TABLE_KIND_REGULAR_TABLE);
        final MatViewDefinition matViewDefinition = operation.getMatViewDefinition();
        try {
            if (matViewGraph.addView(matViewDefinition)) {
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
        return createTableOrViewOrMatViewUnsecure(securityContext, mem, null, path, ifNotExists, struct, keepLock, inVolume, tableKind);
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
        final TableToken viewToken = createTableOrViewOrMatViewUnsecure(securityContext, mem, blockFileWriter, path, ifNotExists, operation, false, false, TableUtils.TABLE_KIND_REGULAR_TABLE);
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

    public void dropTableOrViewOrMatView(@Transient Path path, TableToken tableToken) {
        verifyTableToken(tableToken);
        if (tableToken.isWal()) {
            if (notifyDropped(tableToken)) {
                durableAckRegistry.onTableDropped(tableToken);
                // Both-trees pre-externalization fire-point: fire the role-switch mint observer here,
                // immediately before tableSequencerAPI.dropTable mints the replicated drop. A WAL DROP
                // does not route through OperationDispatcher (it runs as a GenericDropOperation executed
                // directly), so this is the single externalization site for DROP TABLE/VIEW/MATERIALIZED
                // VIEW/ALL TABLES across pg-wire, HTTP /exec and the QWP egress channel. Firing here lets
                // a demote-race witness arm one seam that trips on both the fenced and the unfenced tree.
                // A strict no-op in production (the observer field is null).
                fireRoleSwitchMintObserver();
                tableSequencerAPI.dropTable(tableToken, false);
                notifyViewStoresAboutDrop(tableToken);
                matViewStateStore.removeViewState(tableToken);
                matViewGraph.removeView(tableToken);
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
        int retryCount = 0;
        while (true) {
            try (SqlCompiler compiler = getSqlCompiler()) {
                execute(compiler, sqlText, sqlExecutionContext, eventSubSeq);
                return;
            } catch (TableReferenceOutOfDateException e) {
                // Retry on this exception, all interfaces like HTTP, Pg wire are supposed to retry too.
                // The retry is bounded: a permanent mismatch (stale metadata/token vs writer) would
                // otherwise spin here forever with no output, which is exactly the CI hang we are
                // chasing. Surface the offending SQL and the exception detail and give up instead.
                if (++retryCount >= MAX_EXECUTE_RETRIES) {
                    LOG.error().$("giving up recompiling, table reference repeatedly out of date [retries=").$(retryCount)
                            .$(", sql=").$(sqlText)
                            .$(", ex=").$(e.getFlyweightMessage())
                            .$(']').$();
                    throw e;
                }
                if (retryCount == 1) {
                    LOG.info().$("retrying execute after table reference out of date [sql=").$(sqlText)
                            .$(", ex=").$(e.getFlyweightMessage())
                            .$(']').$();
                }
            }
        }
    }

    /**
     * Fires the role-switch mint hook when a test installed one. A strict no-op (single static volatile
     * read) in production where the field is null. Called both inside the parse-time DDL / replicated-write
     * fences (within their role-switch read-lock hold) and at the OperationDispatcher externalization site
     * before its read-lock acquire, so the one hook fires on both the fenced and the unfenced tree and a
     * witness can pause the externalization and interleave a demote deterministically regardless of whether
     * the fence wraps it.
     */
    public void fireRoleSwitchMintObserver() {
        final Runnable observer = roleSwitchMintObserver;
        if (observer != null) {
            observer.run();
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

    public @NotNull MatViewGraph getMatViewGraph() {
        return matViewGraph;
    }

    public @NotNull MatViewStateStore getMatViewStateStore() {
        return matViewStateStore;
    }

    public Queue<MatViewTimerTask> getMatViewTimerQueue() {
        return matViewTimerQueue;
    }

    public MemoryTrackerProvider getMemoryTrackerProvider() {
        MemoryTrackerProvider p = memoryTrackerProvider;
        if (p == null) {
            synchronized (this) {
                p = memoryTrackerProvider;
                if (p == null) {
                    p = configuration.getFactoryProvider().getMemoryTrackerProvider(configuration);
                    memoryTrackerProvider = p;
                }
            }
        }
        return p;
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

    public TableReader getReader(TableToken tableToken, @Nullable ResourcePoolSupervisor<TableReader> readerPoolSupervisor) {
        verifyTableToken(tableToken);
        return readerPool.get(tableToken, readerPoolSupervisor);
    }

    public TableReader getReader(TableToken tableToken, long metadataVersion, @Nullable ResourcePoolSupervisor<TableReader> readerPoolSupervisor) {
        verifyTableToken(tableToken);
        return checkReaderVersion(tableToken, metadataVersion, readerPool.get(tableToken, readerPoolSupervisor));
    }

    /**
     * Returns a pooled table reader that is pointed at the same transaction number
     * as the source reader.
     * <p>
     * If the source reader is detached and not in use, returns the source reader.
     * The source reader must be used only through calling this method.
     */
    public TableReader getReaderAtTxn(TableReader srcReader, SqlExecutionContext executionContext) {
        assert srcReader.isOpen() && srcReader.isActive();
        // Fast path: go with the base reader if it's not in-use. It was borrowed before the
        // current query, so it is intentionally not attributed to the query's supervisor.
        if (readerPool.isDetached(srcReader) && readerPool.getDetachedRefCount(srcReader) == 0) {
            readerPool.incDetachedRefCount(srcReader);
            return srcReader;
        }
        // Slow path: obtain a base reader copy from the pool, attributed to the query.
        return readerPool.getCopyOf(srcReader, executionContext.getReaderPoolSupervisor());
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

    public Lock getRoleSwitchReadLock() {
        return roleSwitchLock.readLock();
    }

    public int getRoleSwitchReadLockCount() {
        return roleSwitchLock.getReadLockCount();
    }

    public Lock getRoleSwitchWriteLock() {
        return roleSwitchLock.writeLock();
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

    public SqlCompilerPool getSqlCompilerPool() {
        return sqlCompilerPool;
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

    public TableNameRegistry getTableNameRegistry() {
        return tableNameRegistry;
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
        if (configuration.isWalApplySuspendedWriteDenied() && isWalApplySuspended(tableToken)) {
            throw CairoException.tableSuspended(tableToken);
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
        if (configuration.isWalApplySuspendedWriteDenied() && isWalApplySuspended(tableToken)) {
            throw CairoException.tableSuspended(tableToken);
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

    /**
     * Returns the periodic sweep that cancels {@link TxnWaiter}
     * instances whose deadline has elapsed. Without this job assigned, parked
     * suspending-function calls (e.g. {@code wait_wal_table}) cannot be cleaned up
     * after a client disconnect or on idle tables, so it MUST be assigned to a worker
     * pool for the SQL-suspend feature to be safe in production.
     */
    public TimerShards getTimerShards() {
        return timerShards;
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

    public WalTxnTypeHandler getWalTxnTypeHandler() {
        return UnsupportedWalTxnTypeHandler.INSTANCE;
    }

    public @NotNull WalWriter getWalWriter(TableToken tableToken) {
        return getWalWriterUnsafe(tableToken);
    }

    /**
     * Acquires a WAL writer WITHOUT the read-only client gate {@link #getWalWriter} carries
     * (the twin of {@link #getWriterUnsafe} for the WAL writer pool). The enterprise override of
     * {@link #getWalWriter} refuses on {@code isReadOnlyMode()} to block client writes on a read-only
     * replica / demoting node; this method is intentionally NOT overridden there, so engine-internal
     * paths that legitimately mint on a read-only node -- replica WAL apply and the read-only replica's
     * own {@code REBASE WAL INTO} reconstruction -- acquire through here. Callers are responsible for the
     * read-only/role gating themselves (e.g. rebaseWalTable0's assertRebaseRole + variant check).
     */
    public @NotNull WalWriter getWalWriterUnsafe(TableToken tableToken) {
        verifyTableToken(tableToken);
        if (configuration.isWalApplySuspendedWriteDenied() && isWalApplySuspended(tableToken)) {
            throw CairoException.tableSuspended(tableToken);
        }
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

    public boolean isClosing() {
        return closing;
    }

    /**
     * Reports whether the node still permits a force/WAL-bypass break-glass alter (e.g. FORCE DROP
     * PARTITION on a hard-suspended table) while it otherwise refuses ordinary writes via
     * {@link #isReadOnlyMode()}. Such an alter applies directly through the exclusive writer and
     * mints no replicated sequencer txn, so it carries none of the demote/replica acked-loss risk
     * the read-only fence guards against. Only consulted while {@link #isReadOnlyMode()} is true; on
     * a writable node the fence never engages.
     * <p>
     * The base engine never permits it: the sole read-only state OSS knows is the instance-level
     * {@code cairo.read.only} lockdown ({@link #isReadOnlyMode()} answers purely from
     * {@code isReadOnlyInstance()}), a hard operator refusal of every write. Enterprise overrides
     * this to permit it while the node is a read-only replica (the authorized maintenance path for a
     * frozen replica table), still refusing under the {@code cairo.read.only} lockdown -- the exact
     * mirror of how the enterprise engine widens {@link #isReadOnlyMode()} with the replica leg.
     */
    public boolean isForceAlterAllowed() {
        return false;
    }

    /**
     * Whether materialized-view refresh task execution is temporarily suspended (e.g. a role promote
     * has built and hydrated the real store but has not yet opened writes). Default: never suspended.
     * Enterprise overrides this for the promote window so a hydrate-enqueued task is not consumed and
     * dropped while the engine is still read-only.
     */
    public boolean isMatViewRefreshSuspended() {
        return false;
    }

    /**
     * Reports whether this engine currently refuses writes. Reads the LIVE state on every
     * call so callers can re-check it per write batch rather than trusting a value captured
     * earlier (for example, a SecurityContext cached at connection time). The base engine
     * answers from the static isReadOnlyInstance() flag; enterprise subclasses override this
     * to also report true while the node is acting as a read-only replica, a state an
     * in-place role switch can toggle dynamically.
     */
    public boolean isReadOnlyMode() {
        return configuration.isReadOnlyInstance();
    }

    public boolean isTableDropped(TableToken tableToken) {
        return tableNameRegistry.isTableDropped(tableToken);
    }

    /**
     * Whether the table is hard-suspended from WAL apply, either by the reloadable
     * {@code cairo.wal.apply.suspended.tables} config list or by a runtime
     * {@code ALTER TABLE ... SUSPEND WAL}. The ApplyWal2Table job skips such tables. Resuming
     * requires removing the table from the config list (and reloading) and running
     * {@code ALTER TABLE ... RESUME WAL}.
     */
    public boolean isWalApplySuspended(TableToken tableToken) {
        if (tableSequencerAPI.getTxnTracker(tableToken).isHardSuspended()) {
            return true;
        }
        final ObjHashSet<String> configured = configuration.getWalApplySuspendedTables();
        return configured != null && configured.contains(tableToken.getDirName());
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
        final String tableNameStr = Chars.toString(tableName);
        final String dirName = TableUtils.getTableDir(configuration.mangleTableDirNames(), tableNameStr, tableId, isWal);
        return lockTableName(tableNameStr, dirName, tableId, isView, isMatView, isWal);
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
            notifyPoolsTableDropped(tableToken, false);
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

    /**
     * Rebases a hard-suspended WAL table ({@code ALTER TABLE ... REBASE WAL}): clones the applied data
     * into a new dir with a new tableId and a brand-new sequencer (seqTxn reset to 0), discarding all
     * non-applied WAL (including pending structural changes), marks the new table rebased and seeds two
     * empty transactions (so real data starts at seqTxn 3), then repoints the logical name to the
     * new dir and drops the old one. On the replica, the rebased new table is recorded in the replication
     * index so it stalls (keeps no empty data) until a physical copy of the table arrives.
     * <p>
     * Preconditions: WAL table (not a view) and hard-suspended (ALTER TABLE ... SUSPEND WAL).
     * <p>
     * Note: crash-recovery startup reconcile of {@code _rebase.state} markers and mat-view dependent
     * migration are tracked as follow-ups; the happy path leaves no marker behind.
     *
     * @return the new table token
     */
    public TableToken rebaseWalTable(TableToken oldToken) {
        return rebaseWalTable0(oldToken, null, false);
    }

    /**
     * Replica-side variant of {@link #rebaseWalTable(TableToken)}
     * ({@code ALTER TABLE ... REBASE WAL INTO '<dirName>'}). Instead of minting a fresh tableId/dir it
     * reconstructs the table into {@code targetDirName} - the dir the primary already chose for its rebase
     * - so the replica follows the primary's rebase (e.g. past a poison-pill WAL transaction that stalls
     * apply at the same seqTxn on both nodes) without a full physical copy. It does NOT mark the new table
     * rebased, so the replica keeps following replication into this dir; the empty seeds it commits are
     * local-only (never uploaded) and only advance the new sequencer to seqTxn 2 so the downloader
     * resumes from the primary's seqTxn 3 instead of stalling. Valid only on a read-only replica
     * (enforced by {@link #assertRebaseRole(boolean)}).
     *
     * @return the new table token
     */
    public TableToken rebaseWalTableInto(TableToken oldToken, String targetDirName) {
        return rebaseWalTable0(oldToken, targetDirName, true);
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
        notifyPoolsTableDropped(tableToken, true);
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

    /**
     * Removes a table from the runtime hard-suspend set. Called by
     * {@code ALTER TABLE ... RESUME WAL}. A table configured via
     * {@code cairo.wal.apply.suspended.tables} stays suspended until also removed from the config.
     */
    public void removeWalApplySuspended(TableToken tableToken) {
        tableSequencerAPI.setHardSuspended(tableToken, false);
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

    @TestOnly
    public void setClosing(boolean closing) {
        this.closing = closing;
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

    /**
     * Test seam: installs a hook fired at the replicated-state externalization sites on both the fenced
     * and the unfenced tree -- inside the parse-time DDL / replicated-write fences (within their
     * role-switch read-lock hold) and at the OperationDispatcher externalization site before its
     * read-lock acquire. Pass null to uninstall. The hook is shared across engines, so an installer must
     * scope its own pause to the statement under test. Never set outside tests -- the field defaults to
     * null and the fire-site is a no-op then.
     */
    @TestOnly
    public static void setRoleSwitchMintObserver(Runnable observer) {
        roleSwitchMintObserver = observer;
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

        // Drain parked SQL waiters before worker pools halt: each waiter's continuation
        // gets its shutdown flag set and a scheduleResume, so workers (still RUNNING at
        // this point) remount them, the bodies observe isShuttingDown(), and unwind.
        // Without this, WorkerPool.halt() blocks on conts that nothing else will fire,
        // for at least getQueryContinuationWakeIntervalMillis() and potentially forever.
        // Order matters: timerShards.shutdown() MUST run before any worker pool halts.
        timerShards.shutdown();
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

    private static TableReader checkReaderVersion(TableToken tableToken, long metadataVersion, TableReader reader) {
        final int tableId = tableToken.getTableId();
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

    private @NotNull TableToken createTableOrViewOrMatViewUnsecure(
            SecurityContext securityContext,
            MemoryMARW mem,
            @Nullable BlockFileWriter blockFileWriter,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock,
            boolean inVolume,
            int tableKind
    ) {
        assert !struct.isWalEnabled() || PartitionBy.isPartitioned(struct.getPartitionBy()) : "WAL is only supported for partitioned tables";
        final CharSequence tableName = struct.getTableName();
        validNameOrThrow(tableName);

        final int tableId = (int) tableIdGenerator.getNextId();

        while (true) {
            TableToken tableToken = lockTableName(tableName, tableId, struct.isView(), struct.isMatView(), struct.isWalEnabled());
            if (tableToken == null) {
                if (ifNotExists) {
                    tableToken = getTableTokenIfExists(tableName);
                    if (tableToken != null) {
                        struct.init(tableToken);
                        return tableToken;
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
                        tableNameRegistry.registerName(tableToken);
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
                tableNameRegistry.unlockTableName(tableToken);
                unlockTableCreate(tableToken);
            }

            enqueueCompileView(tableToken);
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

    // Scans the base-table WAL gap (lastRefreshBaseTxn, baseTableLastTxn] for a TRUNCATE, using the
    // same range and loader an incremental refresh would use (so there is no off-by-one vs interval
    // planning). A truncate in the gap means a resumed incremental refresh would keep stale
    // pre-truncate rows, so the caller must invalidate the view instead. Runs only on the cold
    // load/hydrate path, so a transient loader is fine.
    private boolean hasBaseTableTruncateInWalGap(TableToken baseTableToken, long lastRefreshBaseTxn, long baseTableLastTxn) {
        if (lastRefreshBaseTxn >= baseTableLastTxn) {
            return false;
        }
        // This load-time probe scans the same (lastRefreshBaseTxn, baseTableLastTxn] gap that the
        // enqueued incremental refresh re-scans to build its intervals, so the no-truncate promote path
        // reads the gap twice (and allocates a fresh loader per view). It is bounded to lagging views on
        // the cold boot/promote path, so the redundant scan is a tracked optimization, not a steady-state
        // cost.
        try (
                Path path = new Path();
                WalTxnRangeLoader loader = new WalTxnRangeLoader(configuration)
        ) {
            final LongList intervals = new LongList();
            loader.load(this, path, baseTableToken, intervals, lastRefreshBaseTxn, baseTableLastTxn);
            return loader.hasTruncate();
        } catch (CairoException e) {
            // Missing or purged WAL files in the gap. Treat as "no truncate found" so the caller
            // still schedules the normal incremental refresh. The refresh path's own interval planning
            // hits the same read failure and falls back to a full refresh; note that fallback is a
            // REPLACE_RANGE over the current range, so if the purged gap held a truncate, pre-truncate
            // buckets outside the current range can survive (a stale-valid view). That purge-during-the-
            // pending-window residual is a known, tracked deferral. Letting the exception escape would
            // cause loadMatViewIntoStore's logging-only catch to swallow it, skipping
            // enqueueIncrementalRefresh and leaving the view silently unscheduled after a promote-hydrate.
            LOG.info().$("could not scan base WAL gap for truncate, scheduling refresh [baseTable=").$(baseTableToken)
                    .$(", errno=").$(e.getErrno())
                    .$(", msg=").$safe(e.getFlyweightMessage())
                    .I$();
            return false;
        }
    }

    /**
     * Loads one mat-view's definition and persisted state into the live mat-view state store.
     * Shared by {@link #buildViewGraphs()} (boot, {@code forceCreateState=false}) and
     * {@link #hydrateMatViewStateStore()} (role promote, {@code forceCreateState=true}). When
     * {@code forceCreateState} is true, a view already present in the graph still has its state
     * created so a freshly built store on promote is populated; when false, only a brand-new graph
     * entry gets its state created (the boot semantics).
     */
    private void loadMatViewIntoStore(
            TableToken tableToken,
            Path path,
            int pathLen,
            BlockFileReader reader,
            WalEventReader walEventReader,
            MemoryCMR txnMem,
            MatViewStateReader matViewStateReader,
            boolean forceCreateState
    ) {
        try {
            MatViewDefinition viewDefinition = matViewGraph.getViewDefinition(tableToken);
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
                if (matViewGraph.addView(viewDefinition)) {
                    matViewStateStore.createViewState(viewDefinition);
                }
            } else if (forceCreateState && matViewStateStore.getViewState(tableToken) == null) {
                // The graph already knows this view but the (freshly built) store has no state for
                // it yet -- the role-promote rehydration case. Create the state from the graph
                // definition so the store is populated rather than empty.
                matViewStateStore.createViewState(viewDefinition);
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
                    return;
                }

                if (!baseTableToken.isWal()) {
                    // Print a warning, but let the mat view load in invalid state.
                    LOG.info().$("base table for materialized view is not WAL table [table=").$safe(viewDefinition.getBaseTableName())
                            .$(", view=").$(tableToken)
                            .I$();
                    matViewStateStore.enqueueInvalidate(tableToken, "base table is not WAL table");
                    return;
                }

                path.trimTo(pathLen).concat(tableToken);
                if (!WalUtils.readMatViewState(path, tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader)) {
                    LOG.info().$("could not find materialized view state, default values will be used [table=")
                            .$safe(viewDefinition.getBaseTableName())
                            .$(", view=").$(tableToken)
                            .I$();
                    return;
                }

                state.initFromReader(matViewStateReader);
                if (state.isInvalid()) {
                    return;
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
                } else if (state.getLastRefreshBaseTxn() > -1 && hasBaseTableTruncateInWalGap(baseTableToken, state.getLastRefreshBaseTxn(), baseTableLastTxn)) {
                    // A truncate in the base WAL gap (lastRefreshBaseTxn, baseTableLastTxn] carries no
                    // data interval, so resuming an incremental refresh would silently advance past it
                    // and keep stale pre-truncate rows. Invalidate instead, the same way a primary
                    // already invalidates dependents on a truncate. Only for a view that has refreshed
                    // at least once (a never-refreshed view has nothing stale to retain).
                    LOG.info().$("materialized view base table was truncated, invalidating on load [table=")
                            .$safe(viewDefinition.getBaseTableName())
                            .$(", view=").$(tableToken)
                            .I$();
                    matViewStateStore.enqueueInvalidate(tableToken, "truncate operation");
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

    private void notifyPoolsTableDropped(TableToken tableToken, boolean fullDropped) {
        readerPool.notifyDropped(tableToken, fullDropped);
        walWriterPool.notifyDropped(tableToken, fullDropped);
        viewWalWriterPool.notifyDropped(tableToken, fullDropped);
        tableMetadataPool.notifyDropped(tableToken, fullDropped);
        sequencerMetadataPool.notifyDropped(tableToken, fullDropped);
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

    private TableToken rebaseWalTable0(TableToken oldToken, String suppliedDir, boolean replicaVariant) {
        assertRebaseRole(replicaVariant);
        if (!oldToken.isWal() || oldToken.isView()) {
            throw CairoException.nonCritical().put("REBASE WAL is supported only for WAL tables [table=").put(oldToken.getTableName()).put(']');
        }
        if (!tableSequencerAPI.getTxnTracker(oldToken).isHardSuspended()) {
            throw CairoException.nonCritical().put("REBASE WAL requires the table to be suspended first [table=").put(oldToken.getTableName()).put(']');
        }
        // Require suspension to actually stop writes, so the table is quiescent without extra locking:
        // with write-denial on, getWalWriter rejects ingestion for a hard-suspended table.
        if (!configuration.isWalApplySuspendedWriteDenied()) {
            throw CairoException.nonCritical().put("REBASE WAL requires cairo.wal.apply.suspended.write.denied=true so that suspension blocks writes [table=").put(oldToken.getTableName()).put(']');
        }
        // The rebase repoints the name registry (dropTable/registerName), which a read-only instance
        // (cairo.read.only=true) refuses. Fail early with a clear message instead of deep in the registry.
        if (configuration.isReadOnlyInstance()) {
            throw CairoException.nonCritical().put("REBASE WAL is not supported on a read-only instance [table=").put(oldToken.getTableName()).put(']');
        }

        final FilesFacade ff = configuration.getFilesFacade();
        final int mkDirMode = configuration.getMkDirMode();
        final CharSequence root = configuration.getDbRoot();
        final String tableName = oldToken.getTableName();
        final int newTableId;
        final String newDirName;
        if (suppliedDir != null) {
            // Replica variant: adopt the primary's dir (and the tableId encoded in it) verbatim so both
            // nodes converge on the same identity. The id generator is not consulted - replicated tables
            // always take their id from the primary, never from the local generator.
            try {
                newTableId = TableUtils.getTableIdFromTableDir(suppliedDir);
            } catch (NumericException e) {
                throw CairoException.nonCritical().put("invalid rebase target directory [dir=").put(suppliedDir).put(']');
            }
            newDirName = suppliedDir;
            // The supplied dir must be free. The swap below registers newDirName as the live table
            // (registerName unconditionally repoints the reverse-map entry), and ff.rename only refuses a
            // NON-empty destination - so an empty victim dir (a freshly-created, partition-less table) could
            // be hijacked and its registry entry clobbered. Reject if the dir is already present on disk.
            try (Path p = new Path()) {
                if (ff.exists(p.of(root).concat(newDirName).$())) {
                    throw CairoException.nonCritical().put("rebase target directory already exists [dir=").put(newDirName).put(']');
                }
            }
        } else {
            newTableId = (int) tableIdGenerator.getNextId();
            newDirName = TableUtils.getTableDir(configuration.mangleTableDirNames(), tableName, newTableId, true);
        }
        TableToken newToken = new TableToken(
                tableName, newDirName, configuration.getDbLogName(), newTableId,
                oldToken.isView(), oldToken.isMatView(), true,
                oldToken.isSystem(), oldToken.isProtected(), oldToken.isPublic()
        );

        // Serialize against concurrent create/drop/rename/rebase of this name. No write-quiescence lock is
        // needed: the gate above guarantees the table is hard-suspended with write-denial on, so getWalWriter
        // already rejects ingestion and the apply job skips it.
        while (!lockTableCreate(oldToken)) {
            Os.pause();
        }
        boolean staged = false;
        boolean renamed = false;
        boolean oldTableDropped = false;
        TableWriter oldWriter = null;
        try {
            // Fence any in-flight apply and snapshot a consistent applied state (the table is suspended,
            // so the writer is free). The replica REBASE WAL INTO variant is the read-only replica's own
            // follow-the-primary reconstruction (gated to a read-only replica by assertRebaseRole), so it
            // acquires through getWriterUnsafe -- the enterprise getWriter override would otherwise refuse
            // it on isReadOnlyMode(). The plain variant keeps the read-only-refusing getWriter acquire so a
            // client REBASE WAL on a demoting primary is refused (the demote write-fence).
            oldWriter = replicaVariant ? getWriterUnsafe(oldToken, "rebase") : getWriter(oldToken, "rebase");

            try (Path src = new Path(); Path dst = new Path()) {
                // Build the clone in a hidden ".rebase/" staging dir (mirrors ".download"/".checkpoint").
                // Startup table-dir scans only consider immediate db-root children that are complete tables
                // and never recurse into dot-prefixed dirs, so the in-progress clone is invisible; a crash
                // mid-build leaves only an ignored ".rebase/<dir>" orphan. The sequencer is created only
                // AFTER the rename, in the final dir, so the sequencer registry never binds the staging path.
                dst.of(root).concat(TableUtils.REBASE_TMP_DIR).concat(newToken);
                if (ff.mkdirs(dst.slash(), mkDirMode) != 0) {
                    throw CairoException.critical(ff.errno()).put("could not create rebase staging dir [path=").put(dst).put(']');
                }
                staged = true;

                // Build the rebased table in the staging dir: clone data (hard-link partitions, copy
                // table-root files, exclude txn_seq/wal*), reset _txn/_meta for a fresh table, create the
                // sequencer files, and (primary only) write the _rebase_new marker. The atomic rename below
                // then carries the complete table into place.
                WalUtils.cloneTableDirForRebase(
                        configuration,
                        getWalDirectoryPolicy(),
                        src.of(root).concat(oldToken),
                        dst.of(root).concat(TableUtils.REBASE_TMP_DIR).concat(newDirName),
                        newToken, newTableId,
                        !replicaVariant,
                        Misc.getThreadLocalSink()
                );

                // Atomically move the completed clone into its final location.
                if (ff.rename(src.of(root).concat(TableUtils.REBASE_TMP_DIR).concat(newToken).$(), dst.of(root).concat(newToken).$()) != Files.FILES_RENAME_OK) {
                    throw CairoException.critical(ff.errno()).put("could not move rebased table into place [from=").put(src).put(", to=").put(dst).put(']');
                }
                renamed = true;

                // Commit the swap in the registry: drop the old table (logs DROP to tables.d, marks the old
                // dir dropped, evicts its metadata-cache entry), then register the rebuilt dir as the live
                // table (logs ADD to tables.d, repoints the name, marks the new dir live, hydrates the cache).
                // lockTableName reserves the now-free name so registerName can commit it. The drop and the
                // register are NOT atomic: a crash between them leaves the new dir on disk but absent from
                // tables.d, so startup's reloadFromRootDirectory adopts it (without the empty seeds below).
                // Acceptable for this rare admin op - the table comes back, just unseeded.
                if (!tableNameRegistry.dropTable(oldToken)) {
                    throw CairoException.nonCritical()
                            .put("could not drop old table from registry [table=").put(oldToken.getTableName()).put(']');
                }
                oldTableDropped = true;
                final TableToken lockedToken = tableNameRegistry.lockTableName(
                        tableName,
                        newDirName,
                        newTableId,
                        oldToken.isView(),
                        oldToken.isMatView(),
                        true
                );
                if (lockedToken == null) {
                    throw CairoException.nonCritical()
                            .put("rebase target name was taken concurrently [table=").put(tableName).put(']');
                }
                newToken = lockedToken;
                try {
                    tableNameRegistry.registerName(newToken);
                } finally {
                    tableNameRegistry.unlockTableName(newToken);
                }

                // Seed two empty transactions so real data starts at seqTxn 3 (the uploader skips seqTxn 1
                // and records seqTxn 2 as first_txn=2). The second seed is what stops an idle rebased table
                // busy-spinning the uploader: it ensures max_txn >= 2 so the uploader has a seqTxn 2 to
                // settle on even when no data follows (see WalWriter.commitRebaseSeed). On a replica these
                // seeds are local-only (never uploaded): they advance the new sequencer to seqTxn 2 so the
                // downloader resumes from the primary's seqTxn 3 rather than stalling on the missing
                // baseline below first_txn.
                // The replica variant seeds on a read-only replica, so it acquires through
                // getWalWriterUnsafe (the enterprise getWalWriter override refuses on isReadOnlyMode());
                // the plain variant keeps the gated getWalWriter, consistent with the old-table acquire above.
                try (WalWriter walWriter = replicaVariant ? getWalWriterUnsafe(newToken) : getWalWriter(newToken)) {
                    walWriter.commitRebaseSeed();
                }

                getWalListener().tableCreated(newToken, configuration.getMicrosecondClock().getTicks());
            } catch (Throwable th) {
                try {
                    // Discard the half-built new dir ONLY while the old table is still intact. Once the swap
                    // has oldTableDropped (old dropped), the new dir IS the table - its hard links are the only
                    // surviving copy of the data - so leave it on disk for startup to adopt; deleting it here
                    // would destroy the table.
                    if (!oldTableDropped) {
                        try (Path p = new Path()) {
                            if (renamed) {
                                p.of(root).concat(newToken).$();
                            } else if (staged) {
                                p.of(root).concat(TableUtils.REBASE_TMP_DIR).concat(newToken).$();
                            }
                            if (renamed || staged) {
                                ff.rmdir(p, true);
                            }
                        }
                    }
                } catch (Throwable cleanupEx) {
                    th.addSuppressed(cleanupEx);
                }
                throw th;
            }

            // If the rebased object is a materialized view, register the new dir as a fresh mat view
            // (its _mv/_mv.s files were cloned). The refresh watermark is intentionally NOT carried over
            // (see plan): createViewState installs default state, so the view does one full refresh.
            // Best-effort: a registration failure must not undo the already-oldTableDropped rebase.
            if (newToken.isMatView()) {
                try (
                        Path p = new Path();
                        BlockFileReader blockReader = new BlockFileReader(configuration)
                ) {
                    final int mvRootLen = p.of(root).size();
                    final MatViewDefinition def = new MatViewDefinition();
                    MatViewDefinition.readFrom(this, def, blockReader, p, mvRootLen, newToken);
                    if (matViewGraph.addView(def)) {
                        matViewStateStore.createViewState(def);
                    }
                } catch (Throwable mvEx) {
                    LOG.error().$("could not register rebased materialized view, it may need manual recreation [view=")
                            .$(newToken).$(", e=").$(mvEx).I$();
                }
            }

            // Dependent mat views were refreshed against the old base sequencer; the rebase reset it to 0,
            // so their watermarks no longer map onto the new base. Force a full refresh of any dependents
            // (covers a rebased base table, and a rebased mat view that is itself a base of another).
            matViewStateStore.enqueueInvalidateDependentViews(newToken, "base table rebase");

            // Committed. Tear down the old table (data survives via new dir hard links). Mark the dir as
            // the rebase SOURCE first: the uploader stats this marker as the dir winds down and records
            // the table in the replication index with the rebase-source flag (the high bit of last_txn)
            // instead of a drop, so the object-store baseline is kept for the rebased table and replicas
            // keep the dir they already have. Remove _txn/_meta to tombstone the dir for WalPurgeJob, but
            // do NOT rmdir here and do NOT remove the upload.pending marker: that raced the uploader's
            // in-flight read of txn_seq and lost the record. The purge job reclaims the dir later, once
            // the uploader has recorded the flag and cleared the sequencer upload.pending marker itself.
            oldWriter.close();
            oldWriter = null;
            try (Path p = new Path()) {
                p.of(root).concat(oldToken);
                WalUtils.writeRebaseSourceMarker(ff, p);
                final int len = p.size();
                ff.removeQuiet(p.concat(TableUtils.TXN_FILE_NAME).$());
                ff.removeQuiet(p.trimTo(len).concat(TableUtils.META_FILE_NAME).$());
            }
            tableSequencerAPI.dropTable(oldToken, false);
            // Keep the old dir's reverse-map entry in the dropped state dropTable already set, and only
            // evict its pooled resources (exactly what a normal WAL drop does). Calling removeTableToken
            // here would purgeToken the entry, but WalPurgeJob enumerates tables solely via the reverse
            // map (forAllWalTables); with the entry gone it would never visit the dir and never reclaim
            // it, leaking the dir - txn_seq, _rebase_source marker, symbol maps - forever. Leaving the
            // dropped entry lets WalPurgeJob sweep the dir like any dropped table (and it is WalPurgeJob
            // that calls removeTableToken once the dir is actually deleted).
            notifyPoolsTableDropped(oldToken, false);
            if (oldToken.isMatView()) {
                // Drop the old mat view's graph/state entries (keyed by the old dir name).
                matViewStateStore.removeViewState(oldToken);
                matViewGraph.removeView(oldToken);
            }
        } finally {
            if (oldWriter != null) {
                oldWriter.close();
            }
            unlockTableCreate(oldToken);
        }
        enqueueCompileView(newToken);
        return newToken;
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

    /**
     * Role gate for REBASE WAL. OSS has no replica concept, so the {@code INTO} (replica) variant is
     * rejected here; the enterprise engine overrides this to require a read-only replica for the
     * {@code INTO} variant and a non-replica for the plain variant.
     */
    protected void assertRebaseRole(boolean replicaVariant) {
        if (replicaVariant) {
            throw CairoException.nonCritical().put("REBASE WAL INTO is only supported on a read-only replica");
        }
    }

    protected void clearDdlListener() {
        ddlListener.clear();
    }

    protected @NotNull MatViewGraph createMatViewGraph() {
        return new MatViewGraph();
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

}

