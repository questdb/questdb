/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.QuestDBNode;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.wal.*;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.catalogue.DumpThreadStacksFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class AbstractCairoTest {


    protected static final Log LOG = LogFactory.getLog(AbstractCairoTest.class);
    protected static final PlanSink planSink = new TextPlanSink();
    protected static final RecordCursorPrinter printer = new RecordCursorPrinter();
    protected static final StringSink sink = new StringSink();
    private static final long[] SNAPSHOT = new long[MemoryTag.SIZE];
    public static long dataAppendPageSize = -1;
    public static int recreateDistressedSequencerAttempts = 3;
    public static long spinLockTimeout = -1;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    public static int walTxnNotificationQueueCapacity = -1;
    public static long writerAsyncCommandBusyWaitTimeout = -1;
    public static long writerAsyncCommandMaxTimeout = -1;
    protected static String attachableDirSuffix = null;
    protected static CharSequence backupDir;
    protected static DateFormat backupDirTimestampFormat;
    protected static int binaryEncodingMaxLength = -1;
    protected static SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration;
    protected static long columnPurgeRetryDelay = -1;
    protected static int columnVersionPurgeQueueCapacity = -1;
    protected static CairoConfiguration configuration;
    protected static long currentMicros = -1;
    protected static final MicrosecondClock defaultMicrosecondClock = () -> currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
    protected static MicrosecondClock testMicrosClock = defaultMicrosecondClock;
    protected static CairoEngine engine;
    protected static FilesFacade ff;
    protected static String inputRoot = null;
    protected static String inputWorkRoot = null;
    protected static IOURingFacade ioURingFacade = IOURingFacadeImpl.INSTANCE;
    protected static MessageBus messageBus;
    protected static Metrics metrics;
    protected static QuestDBNode node1;
    protected static ObjList<QuestDBNode> nodes = new ObjList<>();
    protected static int pageFrameMaxRows = -1;
    protected static int pageFrameReduceQueueCapacity = -1;
    protected static int pageFrameReduceShardCount = -1;
    protected static int queryCacheEventQueueCapacity = -1;
    protected static CharSequence root;
    protected static DatabaseSnapshotAgent snapshotAgent;
    protected static String snapshotInstanceId = null;
    protected static Boolean snapshotRecoveryEnabled = null;
    protected static int sqlCopyBufferSize = 1024 * 1024;
    protected static int writerCommandQueueCapacity = 4;
    protected static long writerCommandQueueSlotSize = 2048L;
    static boolean[] FACTORY_TAGS = new boolean[MemoryTag.SIZE];
    private static long memoryUsage = -1;
    @Rule
    public final TestWatcher flushLogsOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            LogFactory.getInstance().flushJobs();
        }
    };
    @Rule
    public TestName testName = new TestName();
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    public static void configOverrideMangleTableDirNames(boolean mangle) {
        node1.getConfigurationOverrides().setMangleTableDirNames(mangle);
    }

    //ignores:
    // o3, mmap - because they're usually linked with table readers that are kept in pool
    // join map memory - because it's usually a small and can't really be released until factory is closed
    // native sample by long list - because it doesn't seem to grow beyond initial size (10kb)
    public static long getMemUsedByFactories() {
        long memUsed = 0;

        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (FACTORY_TAGS[i]) {
                memUsed += Unsafe.getMemUsedByTag(i);
            }
        }

        return memUsed;
    }

    public static void printFactoryMemoryUsageDiff() {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (!FACTORY_TAGS[i]) {
                continue;
            }

            long value = Unsafe.getMemUsedByTag(i) - SNAPSHOT[i];

            if (value != 0L) {
                System.out.println(MemoryTag.nameOf(i) + ":" + value);
            }
        }
    }

    @SuppressWarnings("unused")
    public static void printMemoryUsage() {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            System.err.print(MemoryTag.nameOf(i));
            System.err.print(":");
            System.err.println(Unsafe.getMemUsedByTag(i));
        }
    }

    @SuppressWarnings("unused")
    public static void printMemoryUsageDiff() {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            long value = Unsafe.getMemUsedByTag(i) - SNAPSHOT[i];

            if (value != 0L) {
                System.err.print(MemoryTag.nameOf(i));
                System.err.print(":");
                System.err.println(value);
            }
        }
    }

    public static void refreshTablesInBaseEngine() {
        engine.reloadTableNames();
    }

    @BeforeClass
    public static void setUpStatic() {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("begin").$();

        node1 = newNode(1, "dbRoot", new StaticOverrides());
        root = node1.getRoot();
        configuration = node1.getConfiguration();
        metrics = node1.getMetrics();
        engine = node1.getEngine();
        snapshotAgent = node1.getSnapshotAgent();
        messageBus = node1.getMessageBus();
    }

    public static void snapshotMemoryUsage() {
        memoryUsage = getMemUsedByFactories();

        for (int i = 0; i < MemoryTag.SIZE; i++) {
            SNAPSHOT[i] = Unsafe.getMemUsedByTag(i);
        }
    }

    @AfterClass
    public static void tearDownStatic() {
        forEachNode(QuestDBNode::closeCairo);
        nodes.clear();
        backupDir = null;
        backupDirTimestampFormat = null;
        DumpThreadStacksFunctionFactory.dumpThreadStacks();
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        LOG.info().$("Starting test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        forEachNode(QuestDBNode::setUpCairo);
        engine.resetNameRegistryMemory();
        refreshTablesInBaseEngine();
        SharedRandom.RANDOM.set(new Rnd());
        TestFilesFacadeImpl.resetTracking();
        memoryUsage = -1;
    }

    @After
    public void tearDown() {
        tearDown(true);
    }

    public void tearDown(boolean removeDir) {
        LOG.info().$("Tearing down test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        forEachNode(node -> node.tearDownCairo(removeDir));

        ioURingFacade = IOURingFacadeImpl.INSTANCE;
        sink.clear();
        memoryUsage = -1;
    }

    protected static void addColumn(TableWriterAPI writer, String columnName, int columnType) throws SqlException {
        AlterOperationBuilder addColumnC = new AlterOperationBuilder().ofAddColumn(0, writer.getTableToken(), 0);
        addColumnC.ofAddColumn(columnName, 1, columnType, 0, false, false, 0);
        writer.apply(addColumnC.build(), true);
    }

    protected static void assertFactoryMemoryUsage() {
        if (memoryUsage > -1) {
            long memAfterCursorClose = getMemUsedByFactories();
            long limit = memoryUsage + 64 * 1024;
            if (memAfterCursorClose > limit) {
                dumpMemoryUsage();
                printFactoryMemoryUsageDiff();
                Assert.fail("cursor memory usage should be less or equal " + limit + " but was " + memAfterCursorClose + ". Diff " + (memAfterCursorClose - memoryUsage));
            }
        }
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        assertMemoryLeak(AbstractCairoTest.ff, code);
    }

    protected static void assertMemoryLeak(@Nullable FilesFacade ff, TestUtils.LeakProneCode code) throws Exception {
        final FilesFacade ff2 = ff;
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        TestUtils.assertMemoryLeak(() -> {
            AbstractCairoTest.ff = ff2;
            try {
                code.run();
                forEachNode(node -> releaseInactive(node.getEngine()));
            } finally {
                forEachNode(node -> node.getEngine().clear());
                AbstractCairoTest.ff = ffBefore;
            }
        });
    }

    protected static void configOverrideColumnPreTouchEnabled(Boolean columnPreTouchEnabled) {
        node1.getConfigurationOverrides().setColumnPreTouchEnabled(columnPreTouchEnabled);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideColumnVersionTaskPoolCapacity(int columnVersionTaskPoolCapacity) {
        node1.getConfigurationOverrides().setColumnVersionTaskPoolCapacity(columnVersionTaskPoolCapacity);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideCopyPartitionOnAttach(Boolean copyPartitionOnAttach) {
        node1.getConfigurationOverrides().setCopyPartitionOnAttach(copyPartitionOnAttach);
    }

    protected static void configOverrideDefaultMapType(CharSequence defaultMapType) {
        node1.getConfigurationOverrides().setDefaultMapType(defaultMapType);
    }

    protected static void configOverrideDefaultTableWriteMode(int defaultTableWriteMode) {
        node1.getConfigurationOverrides().setDefaultTableWriteMode(defaultTableWriteMode);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideHideTelemetryTable(boolean hideTelemetryTable) {
        node1.getConfigurationOverrides().setHideTelemetryTable(hideTelemetryTable);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideIoURingEnabled(Boolean ioURingEnabled) {
        node1.getConfigurationOverrides().setIoURingEnabled(ioURingEnabled);
    }

    protected static void configOverrideJitMode(int jitMode) {
        node1.getConfigurationOverrides().setJitMode(jitMode);
    }

    protected static void configOverrideMaxUncommittedRows(int maxUncommittedRows) {
        node1.getConfigurationOverrides().setMaxUncommittedRows(maxUncommittedRows);
    }

    protected static void configOverrideO3MinLag(long minLag) {
        node1.getConfigurationOverrides().setO3MinLag(minLag);
    }


    protected static void configOverrideO3ColumnMemorySize(int size) {
        node1.getConfigurationOverrides().setO3ColumnMemorySize(size);
    }

    protected static void configOverrideO3MaxLag(int o3MaxLag) {
        node1.getConfigurationOverrides().setO3MaxLag(o3MaxLag);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideO3QuickSortEnabled(boolean o3QuickSortEnabled) {
        node1.getConfigurationOverrides().setO3QuickSortEnabled(o3QuickSortEnabled);
    }

    protected static void configOverrideParallelImportStatusLogKeepNDays(int parallelImportStatusLogKeepNDays) {
        node1.getConfigurationOverrides().setParallelImportStatusLogKeepNDays(parallelImportStatusLogKeepNDays);
    }

    protected static void configOverrideRndFunctionMemoryMaxPages(int rndFunctionMemoryMaxPages) {
        node1.getConfigurationOverrides().setRndFunctionMemoryMaxPages(rndFunctionMemoryMaxPages);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideRndFunctionMemoryPageSize(int rndFunctionMemoryPageSize) {
        node1.getConfigurationOverrides().setRndFunctionMemoryPageSize(rndFunctionMemoryPageSize);
    }

    protected static void configOverrideRostiAllocFacade(RostiAllocFacade rostiAllocFacade) {
        node1.getConfigurationOverrides().setRostiAllocFacade(rostiAllocFacade);
    }

    protected static void configOverrideSampleByIndexSearchPageSize(int sampleByIndexSearchPageSize) {
        node1.getConfigurationOverrides().setSampleByIndexSearchPageSize(sampleByIndexSearchPageSize);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideSqlJoinMetadataMaxResizes(int sqlJoinMetadataMaxResizes) {
        node1.getConfigurationOverrides().setSqlJoinMetadataMaxResizes(sqlJoinMetadataMaxResizes);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideSqlJoinMetadataPageSize(int sqlJoinMetadataPageSize) {
        node1.getConfigurationOverrides().setSqlJoinMetadataPageSize(sqlJoinMetadataPageSize);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideWalSegmentRolloverRowCount(long walSegmentRolloverRowCount) {
        node1.getConfigurationOverrides().setWalSegmentRolloverRowCount(walSegmentRolloverRowCount);
    }

    protected static void configureForBackups() throws IOException {
        backupDir = temp.newFolder().getAbsolutePath();
        backupDirTimestampFormat = new TimestampFormatCompiler().compile("ddMMMyyyy");
    }

    protected static ApplyWal2TableJob createWalApplyJob(QuestDBNode node) {
        return new ApplyWal2TableJob(node.getEngine(), 1, 1, null);
    }

    protected static ApplyWal2TableJob createWalApplyJob() {
        return new ApplyWal2TableJob(engine, 1, 1, null);
    }

    protected static void drainWalQueue(QuestDBNode node) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob(node)) {
            drainWalQueue(walApplyJob, node.getEngine());
        }
    }

    protected static void drainWalQueue(ApplyWal2TableJob walApplyJob) {
        drainWalQueue(walApplyJob, engine);
    }

    protected static void drainWalQueue(ApplyWal2TableJob walApplyJob, CairoEngine engine) {
        walApplyJob.drain(0);
        new CheckWalTransactionsJob(engine).run(0);
        // run once again as there might be notifications to handle now
        walApplyJob.drain(0);
    }

    protected static void drainWalQueue() {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            drainWalQueue(walApplyJob);
        }
    }

    protected static void dumpMemoryUsage() {
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            LOG.info().$(MemoryTag.nameOf(i)).$(": ").$(Unsafe.getMemUsedByTag(i)).$();
        }
    }

    protected static void forEachNode(QuestDBNodeTask task) {
        for (int i = 0; i < nodes.size(); i++) {
            task.run(nodes.get(i));
        }
    }

    protected static TableReader getReader(CairoEngine engine, CharSequence tableName) {
        return engine.getReader(AllowAllCairoSecurityContext.INSTANCE, engine.getTableToken(tableName));
    }

    protected static TableReader getReader(CharSequence tableName) {
        return engine.getReader(AllowAllCairoSecurityContext.INSTANCE, engine.getTableToken(tableName));
    }

    protected static TableWriterAPI getTableWriterAPI(CharSequence tableName) {
        return engine.getTableWriterAPI(AllowAllCairoSecurityContext.INSTANCE, engine.getTableToken(tableName), "test");
    }

    @NotNull
    protected static WalWriter getWalWriter(CharSequence tableName) {
        return engine.getWalWriter(AllowAllCairoSecurityContext.INSTANCE, engine.getTableToken(tableName));
    }

    protected static TableWriter getWriter(CharSequence tableName) {
        return engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, engine.getTableToken(tableName), "testing");
    }

    protected static TableWriter getWriter(CairoEngine engine, CharSequence tableName) {
        return engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, engine.getTableToken(tableName), "testing");
    }

    protected static TableWriter getWriter(TableToken tt) {
        return engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tt, "testing");
    }

    protected static QuestDBNode newNode(int nodeId) {
        return newNode(nodeId, "dbRoot" + nodeId, new Overrides());
    }

    protected static QuestDBNode newNode(int nodeId, String dbRoot, ConfigurationOverrides overrides) {
        final QuestDBNode node = new QuestDBNode(nodeId);
        node.initCairo(dbRoot, overrides);
        nodes.add(node);
        return node;
    }

    protected static TableReader newTableReader(CairoConfiguration configuration, CharSequence tableName) {
        return new TableReader(configuration, engine.getTableToken(tableName));
    }

    protected static TableWriter newTableWriter(CairoConfiguration configuration, CharSequence tableName, Metrics metrics) {
        return new TableWriter(configuration, engine.getTableToken(tableName), metrics);
    }

    protected static void releaseInactive(CairoEngine engine) {
        engine.releaseInactive();
        engine.releaseInactiveTableSequencers();
        engine.resetNameRegistryMemory();
        Assert.assertEquals("busy writer count", 0, engine.getBusyWriterCount());
        Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
    }

    protected static void replicate(String tableName, String wal, QuestDBNode srcNode, QuestDBNode dstNode) {
        TableToken srcTableToken = srcNode.getEngine().getTableToken(tableName);
        TableToken dstTableToken = dstNode.getEngine().getTableToken(tableName);

        dstNode.getEngine().getTableSequencerAPI().closeSequencer(dstTableToken);
        dstNode.getEngine().getTableSequencerAPI().releaseInactive();

        final FilesFacade ff = configuration.getFilesFacade();
        final int mkdirMode = configuration.getMkDirMode();

        final Path srcWal = Path.PATH.get().of(srcNode.getRoot()).concat(srcTableToken).concat(wal).$();
        final Path dstWal = Path.PATH2.get().of(dstNode.getRoot()).concat(dstTableToken).concat(wal).$();
        if (ff.exists(dstWal)) {
            Assert.assertEquals(0, ff.rmdir(dstWal));
        }
        Assert.assertEquals(0, ff.mkdir(dstWal, mkdirMode));
        Assert.assertEquals(0, ff.copyRecursive(srcWal, dstWal, mkdirMode));

        final Path srcTxnLog = Path.PATH.get().of(srcNode.getRoot()).concat(srcTableToken).concat(WalUtils.SEQ_DIR).$();
        final Path dstTxnLog = Path.PATH2.get().of(dstNode.getRoot()).concat(dstTableToken).concat(WalUtils.SEQ_DIR).$();
        Assert.assertEquals(0, ff.rmdir(dstTxnLog));
        Assert.assertEquals(0, ff.copyRecursive(srcTxnLog, dstTxnLog, mkdirMode));

        dstNode.getEngine().getTableSequencerAPI().openSequencer(srcTableToken);
    }

    protected static void replicateAndApplyToAllNodes(String tableName, String walName) {
        for (int i = 1, n = nodes.size(); i < n; i++) {
            final QuestDBNode node = nodes.get(i);
            replicate(tableName, walName, node1, node);
            drainWalQueue(node);
        }
    }

    protected static void runWalPurgeJob(FilesFacade ff) {
        try (WalPurgeJob job = new WalPurgeJob(engine, ff, engine.getConfiguration().getMicrosecondClock())) {
            snapshotAgent.setWalPurgeJobRunLock(job.getRunLock());
            job.drain(0);
        }
    }

    protected static void runWalPurgeJob() {
        runWalPurgeJob(engine.getConfiguration().getFilesFacade());
    }

    protected void assertCursor(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header) {
        TestUtils.assertCursor(expected, cursor, metadata, header, sink);
    }

    protected void assertCursorTwoPass(CharSequence expected, RecordCursor cursor, RecordMetadata metadata) {
        assertCursor(expected, cursor, metadata, true);
        cursor.toTop();
        assertCursor(expected, cursor, metadata, true);
    }

    protected boolean isWalTable(CharSequence tableName) {
        return engine.isWalTable(engine.getTableToken(tableName));
    }

    protected TableWriter newTableWriter(CairoConfiguration configuration, CharSequence tableName, MessageBus messageBus, Metrics metrics) {
        return new TableWriter(configuration, engine.getTableToken(tableName), messageBus, metrics);
    }

    protected TableToken registerTableName(CharSequence tableName) {
        TableToken token = engine.lockTableName(tableName, false);
        if (token != null) {
            engine.registerTableToken(token);
        }
        return token;
    }

    protected enum StringAsTagMode {
        WITH_STRINGS_AS_TAG, NO_STRINGS_AS_TAG
    }

    protected enum SymbolAsFieldMode {
        WITH_SYMBOLS_AS_FIELD, NO_SYMBOLS_AS_FIELD
    }

    protected enum WalMode {
        WITH_WAL, NO_WAL
    }

    @FunctionalInterface
    protected interface QuestDBNodeTask {
        void run(QuestDBNode node);
    }

    static {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            FACTORY_TAGS[i] = !Chars.startsWith(MemoryTag.nameOf(i), "MMAP");
        }

        FACTORY_TAGS[MemoryTag.NATIVE_O3] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_JOIN_MAP] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_OFFLOAD] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_READER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_WRITER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_IMPORT] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_PARALLEL_IMPORT] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_REPL] = false;
    }
}
