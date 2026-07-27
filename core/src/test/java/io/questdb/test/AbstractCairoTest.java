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

package io.questdb.test;

import io.questdb.FactoryProvider;
import io.questdb.MessageBus;
import io.questdb.ParanoiaState;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.catalogue.DumpThreadStacksFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IOURingFacade;
import io.questdb.std.IOURingFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.griffin.engine.join.AsOfJoinTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;

import java.io.File;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ClassEscapesDefinedScope")
public abstract class AbstractCairoTest extends AbstractTest {

    public static final int DEFAULT_SPIN_LOCK_TIMEOUT = 5000;
    protected static final Log LOG = LogFactory.getLog(AbstractCairoTest.class);
    protected static final String TIMESTAMP_NS_TYPE_NAME = "TIMESTAMP_NS";
    protected static final PlanSink planSink = new TextPlanSink();
    protected static final StringSink sink = new StringSink();
    private static final long[] SNAPSHOT = new long[MemoryTag.SIZE];
    public static String exportRoot = null;
    public static StaticOverrides staticOverrides = new StaticOverrides();
    protected static BindVariableService bindVariableService;
    protected static NetworkSqlExecutionCircuitBreaker circuitBreaker;
    protected static SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration;
    protected static CairoConfiguration configuration;
    protected static TestCairoConfigurationFactory configurationFactory;
    protected static long currentMicros = -1;
    protected static final MicrosecondClock defaultMicrosecondClock = () -> currentMicros != -1 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
    protected static MicrosecondClock testMicrosClock = defaultMicrosecondClock;
    protected static final NanosecondClock defaultNanosecondClock = () -> currentMicros != -1 ? currentMicros * 1000L : NanosecondClockImpl.INSTANCE.getTicks();
    protected static NanosecondClock testNanoClock = defaultNanosecondClock;
    protected static CairoEngine engine;
    protected static TestCairoEngineFactory engineFactory;
    protected static FactoryProvider factoryProvider;
    protected static FilesFacade ff;
    protected static String inputRoot = null;
    protected static String inputWorkRoot = null;
    protected static IOURingFacade ioURingFacade = IOURingFacadeImpl.INSTANCE;
    protected static MessageBus messageBus;
    protected static QuestDBTestNode node1;
    protected static ObjList<QuestDBTestNode> nodes = new ObjList<>();
    protected static SecurityContext securityContext;
    protected static long spinLockTimeout = DEFAULT_SPIN_LOCK_TIMEOUT;
    protected static SqlExecutionContext sqlExecutionContext;
    static boolean[] FACTORY_TAGS = new boolean[MemoryTag.SIZE];
    private static long fdReuseCount;
    private static long memoryUsage = -1;
    private static long mmapReuseCount;
    @Rule
    public final TestWatcher flushLogsOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            LogFactory.getInstance().flushJobs();
        }
    };
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    public static void assertReader(String expected, CharSequence tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            TestUtils.assertReader(expected, reader, sink);
        }
    }

    public static void assertVariableColumns(RecordCursorFactory factory, SqlExecutionContext executionContext) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                        case ColumnType.STRING: {
                            CharSequence a = record.getStrA(i);
                            CharSequence b = record.getStrB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                            } else {
                                if (a instanceof AbstractCharSequence) {
                                    // AbstractCharSequence are usually mutable. We cannot have a same mutable instance for A and B
                                    Assert.assertNotSame(a, b);
                                }
                                TestUtils.assertEquals(a, b);
                                Assert.assertEquals(a.length(), record.getStrLen(i));
                            }
                            break;
                        }
                        case ColumnType.VARCHAR: {
                            Utf8Sequence a = record.getVarcharA(i);
                            Utf8Sequence b = record.getVarcharB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getVarcharSize(i));
                            } else {
                                TestUtils.assertEquals(a, b);
                                Assert.assertEquals(a.size(), record.getVarcharSize(i));
                            }
                            break;
                        }
                        case ColumnType.BINARY: {
                            BinarySequence s = record.getBin(i);
                            if (s == null) {
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                            } else {
                                Assert.assertEquals(s.length(), record.getBinLen(i));
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            }
        }
        assertFactoryMemoryUsage();
    }

    public static void configOverrideMangleTableDirNames(boolean mangle) {
        node1.getConfigurationOverrides().setMangleTableDirNames(mangle);
    }

    public static TableToken create(TableModel model) {
        return TestUtils.createTable(engine, model);
    }

    public static void executeWithRewriteTimestamp(CharSequence sqlText, String timestampType) throws SqlException {
        sqlText = sqlText.toString().replaceAll("#TIMESTAMP", timestampType);
        engine.execute(sqlText, sqlExecutionContext);
    }

    //ignores:
    // o3, mmap - because they're usually linked with table readers that are kept in pool
    // join map memory - because it's usually a small and can't really be released until the factory is closed
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

    public static String getTimestampSuffix(String timestampType) {
        return AsOfJoinTest.TIMESTAMP_NS_TYPE_NAME.equals(timestampType) ? "000Z" : "Z";
    }

    public static long parseFloorPartialTimestamp(String toTs) {
        try {
            return MicrosTimestampDriver.floor(toTs);
        } catch (NumericException e) {
            throw new RuntimeException(e);
        }
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

    public static void println(RecordCursorFactory factory, RecordCursor cursor) {
        println(factory.getMetadata(), cursor);
    }

    public static void println(RecordMetadata metadata, RecordCursor cursor) {
        println(metadata, cursor, sink);
    }

    public static void println(RecordMetadata metadata, RecordCursor cursor, StringSink sink) {
        sink.clear();
        CursorPrinter.println(metadata, sink);

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TestUtils.println(record, metadata, sink);
        }
    }

    public static String readTxnToString(TableToken tt, boolean compareTxns, boolean compareTruncateVersion) {
        try (TxReader rdr = new TxReader(engine.getConfiguration().getFilesFacade())) {
            Path tempPath = Path.getThreadLocal(root);
            rdr.ofRO(tempPath.concat(tt).concat(TableUtils.TXN_FILE_NAME).$(), rdr.getTimestampType(), PartitionBy.DAY);
            rdr.unsafeLoadAll();

            return txnToString(rdr, compareTxns, compareTruncateVersion, false, true);
        }
    }

    public static String readTxnToString(
            TableToken tt,
            boolean compareTxns,
            boolean compareTruncateVersion,
            boolean comparePartitionTxns,
            boolean compareColumnVersions
    ) {
        try (TxReader rdr = new TxReader(engine.getConfiguration().getFilesFacade())) {
            Path tempPath = Path.getThreadLocal(root);
            rdr.ofRO(tempPath.concat(tt).concat(TableUtils.TXN_FILE_NAME).$(), rdr.getTimestampType(), PartitionBy.DAY);
            rdr.unsafeLoadAll();

            return txnToString(rdr, compareTxns, compareTruncateVersion, comparePartitionTxns, compareColumnVersions);
        }
    }

    public static void refreshTablesInBaseEngine() {
        engine.reloadTableNames();
    }

    public static String replaceTimestampSuffix(String expected, String timestampType) {
        return TIMESTAMP_NS_TYPE_NAME.equals(timestampType) ? expected.replace(".00", ".00000") : expected;
    }

    public static String replaceTimestampSuffix1(String expected, String timestampType) {
        return TIMESTAMP_NS_TYPE_NAME.equals(timestampType) ? expected.replace("00Z", "00000Z").replace("-00000", "-00000000") : expected;
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        fdReuseCount = Files.getFdReuseCount();
        mmapReuseCount = Files.getMmapReuseCount();

        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops,
        // which causes memory leak detector to fail should logger be
        // created mid-test
        AbstractTest.setUpStatic();
        nodes.clear();
        node1 = newNode(Chars.toString(root), false, 1, staticOverrides, getEngineFactory(), getConfigurationFactory());
        configuration = node1.getConfiguration();
        securityContext = configuration.getFactoryProvider().getSecurityContextFactory().getRootContext();
        engine = node1.getEngine();
        engine.load();
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.clearCache();
        }
        messageBus = node1.getMessageBus();

        node1.initGriffin(circuitBreaker);
        bindVariableService = node1.getBindVariableService();
        sqlExecutionContext = node1.getSqlExecutionContext();
        ((MicrosTimestampDriver) MicrosTimestampDriver.INSTANCE).setTicker(testMicrosClock);
        ((NanosTimestampDriver) NanosTimestampDriver.INSTANCE).setTicker(testNanoClock);
        ColumnType.makeUtf8DefaultString();
    }

    public static void snapshotMemoryUsage() {
        memoryUsage = getMemUsedByFactories();
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            SNAPSHOT[i] = Unsafe.getMemUsedByTag(i);
        }
    }

    @AfterClass
    public static void tearDownStatic() {
        staticOverrides.reset();
        forEachNode(QuestDBTestNode::closeCairo);
        circuitBreaker = Misc.free(circuitBreaker);
        nodes.clear();
        factoryProvider = null;
        engineFactory = null;
        configurationFactory = null;
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.clearCache();
        }
        AbstractTest.tearDownStatic();
        DumpThreadStacksFunctionFactory.dumpThreadStacks();
        LOG.info().$("fd reuse count=").$(Files.getFdReuseCount() - fdReuseCount)
                .$(", mmap resuse count=").$(Files.getMmapReuseCount() - mmapReuseCount).$();
    }

    public static Utf8String utf8(CharSequence value) {
        return value != null ? new Utf8String(value) : null;
    }

    @Before
    public void setUp() {
        super.setUp();
        SharedRandom.RANDOM.set(new Rnd());
        engine.getViewStateStore().clear();
        forEachNode(QuestDBTestNode::setUpCairo);
        engine.resetNameRegistryMemory();
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.clearCache();
        }
        refreshTablesInBaseEngine();
        SharedRandom.RANDOM.set(new Rnd());
        TestFilesFacadeImpl.resetTracking();
        memoryUsage = -1;
        forEachNode(QuestDBTestNode::setUpGriffin);
        sqlExecutionContext.reset();
        sqlExecutionContext.setParallelFilterEnabled(configuration.isSqlParallelFilterEnabled());
        sqlExecutionContext.setParallelGroupByEnabled(configuration.isSqlParallelGroupByEnabled());
        sqlExecutionContext.setParallelTopKEnabled(configuration.isSqlParallelTopKEnabled());
        sqlExecutionContext.setParallelWindowJoinEnabled(configuration.isSqlParallelWindowJoinEnabled());
        sqlExecutionContext.setParallelHorizonJoinEnabled(configuration.isSqlParallelHorizonJoinEnabled());
        sqlExecutionContext.setParallelReadParquetEnabled(configuration.isSqlParallelReadParquetEnabled());
        sqlExecutionContext.setParquetRowGroupPruningEnabled(configuration.isSqlParquetRowGroupPruningEnabled());
        // 30% chance to enable paranoia checking FD mode
        ParanoiaState.FD_PARANOIA_MODE = new Rnd(System.nanoTime(), System.currentTimeMillis()).nextInt(100) > 70;
        engine.getMetrics().clear();
        engine.getMatViewStateStore().clear();
    }

    public void tearDown(boolean removeDir) {
        LOG.info().$("Tearing down test ").$safe(getClass().getSimpleName()).$('#').$safe(testName.getMethodName()).$();
        forEachNode(node -> node.tearDownCairo(removeDir));
        ioURingFacade = IOURingFacadeImpl.INSTANCE;
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.clearCache();
        }
        sink.clear();
        memoryUsage = -1;
        if (inputWorkRoot != null) {
            try (Path path = new Path().of(inputWorkRoot)) {
                if (Files.exists(path.$())) {
                    Files.rmdir(path, true);
                }
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        tearDown(true);
        super.tearDown();
        spinLockTimeout = DEFAULT_SPIN_LOCK_TIMEOUT;
    }

    private static TestCairoConfigurationFactory getConfigurationFactory() {
        return configurationFactory != null ? configurationFactory : CairoTestConfiguration::new;
    }

    private static TestCairoEngineFactory getEngineFactory() {
        return engineFactory != null ? engineFactory : CairoEngine::new;
    }

    private static String txnToString(
            TxReader txReader,
            boolean compareTxns,
            boolean compareTruncateVersion,
            boolean comparePartitionTxns,
            boolean compareColumnVersions
    ) {
        // Used for debugging, don't use Misc.getThreadLocalSink() to not mess with other debugging values
        StringSink sink = Misc.getThreadLocalSink();
        sink.put("{");
        if (compareTxns) {
            sink.put("txn: ").put(txReader.getTxn());
        }
        sink.put(", attachedPartitions: [");
        for (int i = 0; i < txReader.getPartitionCount(); i++) {
            long timestamp = txReader.getPartitionTimestampByIndex(i);
            long rowCount = txReader.getPartitionRowCountByTimestamp(timestamp);

            if (i - 1 == txReader.getPartitionCount()) {
                rowCount = txReader.getTransientRowCount();
            }

            if (i > 0) {
                sink.put(",");
            }
            sink.put("\n{ts: '");
            MicrosFormatUtils.appendDateTime(sink, timestamp);
            sink.put("', rowCount: ").put(rowCount);
            // Do not print name txn, it can be different in expected and actual table
            if (comparePartitionTxns) {
                sink.put(", txn: ").put(txReader.getPartitionNameTxn(i));
            }

            if (txReader.isPartitionParquet(i)) {
                sink.put(", parquetSize: ").put(txReader.getPartitionParquetFileSize(i));
            }
            if (txReader.isPartitionReadOnly(i)) {
                sink.put(", readOnly=true");
            }
            sink.put("}");
        }
        sink.put("\n], transientRowCount: ").put(txReader.getTransientRowCount());
        sink.put(", fixedRowCount: ").put(txReader.getFixedRowCount());
        sink.put(", minTimestamp: '");
        MicrosFormatUtils.appendDateTime(sink, txReader.getMinTimestamp());
        sink.put("', maxTimestamp: '");
        MicrosFormatUtils.appendDateTime(sink, txReader.getMaxTimestamp());
        if (compareTruncateVersion) {
            sink.put("', dataVersion: ").put(txReader.getDataVersion());
        }
        sink.put(", structureVersion: ").put(txReader.getColumnStructureVersion());
        if (compareColumnVersions) {
            sink.put(", columnVersion: ").put(txReader.getColumnVersion());
        }
        if (compareTruncateVersion) {
            sink.put(", truncateVersion: ").put(txReader.getTruncateVersion());
        }

        if (compareTxns) {
            sink.put(", seqTxn: ").put(txReader.getSeqTxn());
        }
        sink.put(", symbolColumnCount: ").put(txReader.getSymbolColumnCount());
        sink.put(", lagRowCount: ").put(txReader.getLagRowCount());
        sink.put(", lagMinTimestamp: '");
        MicrosFormatUtils.appendDateTime(sink, txReader.getLagMinTimestamp());
        sink.put("', lagMaxTimestamp: '");
        MicrosFormatUtils.appendDateTime(sink, txReader.getLagMaxTimestamp());
        sink.put("', lagTxnCount: ").put(txReader.getLagRowCount());
        sink.put(", lagOrdered: ").put(txReader.isLagOrdered());
        sink.put("}");
        return sink.toString();
    }

    protected static void addColumn(TableWriterAPI writer, String columnName, int columnType) {
        AlterOperationBuilder addColumnBuilder = new AlterOperationBuilder().ofAddColumn(0, writer.getTableToken(), 0);
        addColumnBuilder.ofAddColumn(columnName, 1, columnType, 0, false, IndexType.NONE, 0);
        AlterOperation addColumnOp = addColumnBuilder.build();
        addColumnOp.withSecurityContext(AllowAllSecurityContext.INSTANCE);
        writer.apply(addColumnOp, true);
    }

    protected static void assertException(CharSequence sql, int errorPos, CharSequence contains) throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(sql, errorPos, contains, false));
    }

    protected static void assertException(CharSequence sql, int errorPos, CharSequence contains, SqlExecutionContext sqlExecutionContext) throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(sql, errorPos, contains, sqlExecutionContext));
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql) throws Exception {
        TestUtils.assertException(engine, sqlExecutionContext, false, sql, sink);
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, SqlExecutionContext executionContext) throws Exception {
        assertExceptionNoLeakCheck(sql, executionContext, false);
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos, CharSequence contains, SqlExecutionContext sqlExecutionContext) throws Exception {
        Assert.assertNotNull(contains);
        Assert.assertFalse(contains.isEmpty());
        try {
            assertExceptionNoLeakCheck(sql, sqlExecutionContext);
        } catch (Throwable e) {
            if (e instanceof FlyweightMessageContainer) {
                TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), contains);
                if (errorPos > -1) {
                    Assert.assertEquals(errorPos, ((FlyweightMessageContainer) e).getPosition());
                }
            } else {
                throw e;
            }
        }
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, SqlExecutionContext executionContext, boolean fullFatJoins) throws Exception {
        TestUtils.assertException(engine, executionContext, fullFatJoins, sql, sink);
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos, CharSequence contains, boolean fullFatJoins) throws Exception {
        assertExceptionNoLeakCheck(sql, errorPos, contains, fullFatJoins, sqlExecutionContext);
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos, CharSequence contains, boolean fullFatJoins, SqlExecutionContext sqlExecutionContext) throws Exception {
        Assert.assertNotNull(contains);
        try {
            assertExceptionNoLeakCheck(sql, sqlExecutionContext, fullFatJoins);
        } catch (Throwable e) {
            if (e instanceof FlyweightMessageContainer) {
                if (contains.isEmpty()) {
                    Assert.fail("position: " + ((FlyweightMessageContainer) e).getPosition() + ", message: " + e.getMessage());
                }
                TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), contains);
                if (errorPos > -1) {
                    Assert.assertEquals(errorPos, ((FlyweightMessageContainer) e).getPosition());
                }
            } else {
                throw e;
            }
        }
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos, CharSequence contains) throws Exception {
        assertExceptionNoLeakCheck(sql, errorPos, contains, false);
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos) throws Exception {
        assertExceptionNoLeakCheck(sql, errorPos, "inconvertible types: TIMESTAMP -> INT", false);
    }

    /**
     * Begin an assertion against a pre-built, caller-owned {@link RecordCursorFactory} instead of a SQL
     * string. The builder will NOT compile SQL, run DDL, free the factory, leak-check, or touch the shared
     * memory-snapshot accounting, and it asserts each cursor through caller-local scratch buffers, so it is
     * safe to call concurrently from multiple worker threads. Pass the per-thread context through
     * {@link QueryAssertion#withContext(SqlExecutionContext)} before the terminal. Supports
     * {@link QueryAssertion#withBaseFactoryClass}, {@link QueryAssertion#columnType},
     * {@link QueryAssertion#timestamp}, {@link QueryAssertion#noRandomAccess},
     * {@link QueryAssertion#expectSize()} and {@link QueryAssertion#sizeMayVary}; the
     * {@link QueryAssertion#returns(CharSequence)} terminal asserts the output.
     */
    protected QueryAssertion assertFactory(RecordCursorFactory factory) {
        return new QueryAssertion(engine, factory);
    }

    protected static void assertFactoryMemoryUsage() {
        if (memoryUsage > -1) {
            long memAfterCursorClose = getMemUsedByFactories();
            long limit = memoryUsage + 64 * 1024;
            if (memAfterCursorClose > limit) {
                dumpMemoryUsage();
                printFactoryMemoryUsageDiff();
                Assert.fail("cursor is allowed to keep up to 64 KiB of RSS after close. This cursor kept " + (memAfterCursorClose - memoryUsage) / 1024 + " KiB.");
            }
        }
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        engine.clear();
        assertMemoryLeak(AbstractCairoTest.ff, code);
    }

    protected static void assertMemoryLeak(long limitMiB, TestUtils.LeakProneCode code) throws Exception {
        engine.clear();
        long lim = Unsafe.getRssMemUsed() + limitMiB * 1024 * 1024;
        Unsafe.setRssMemLimit(lim);
        try {
            assertMemoryLeak(AbstractCairoTest.ff, code);
        } finally {
            Unsafe.setRssMemLimit(0);
        }
    }

    protected static void assertMemoryLeak(@Nullable FilesFacade ff, TestUtils.LeakProneCode code) throws Exception {
        final FilesFacade ff2 = ff;
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        TestUtils.assertMemoryLeak(() -> {
            AbstractCairoTest.ff = ff2;
            Throwable th1 = null;
            try {
                try {
                    code.run();
                } catch (Throwable th) {
                    th1 = th;
                    LOG.error().$("Exception in test: ").$(th).$();
                    throw th;
                }

                forEachNode(node -> releaseInactive(node.getEngine()));
            } finally {
                try {
                    CLOSEABLE.forEach(Misc::free);
                    // Catch held (unreleased) readers/writers at the SOURCE test. The clear()
                    // below "goodbye"s a leaked tenant but RETAINS it for reuse, so without this
                    // check a leak silently pollutes later tests -- surfacing far away as a stale
                    // schema/version mismatch or a hang. Capture before clear() resets the counts;
                    // the "table is left behind" log lines name the offending table.
                    final StringSink leakSink = new StringSink();
                    forEachNode(node -> {
                        final int busyReaders = node.getEngine().getBusyReaderCount();
                        final int busyWriters = node.getEngine().getBusyWriterCount();
                        if (busyReaders != 0 || busyWriters != 0) {
                            leakSink.put("[busyReaders=").put(busyReaders).put(", busyWriters=").put(busyWriters).put("] ");
                        }
                    });
                    forEachNode(node -> node.getEngine().clear());
                    AbstractCairoTest.ff = ffBefore;
                    if (!leakSink.isEmpty()) {
                        final AssertionError leakErr = new AssertionError(
                                "test leaked unreleased resources " + leakSink
                                        + "(see 'table is left behind' log lines for table names)");
                        if (th1 == null) {
                            th1 = leakErr;
                        } else {
                            th1.addSuppressed(leakErr);
                        }
                    }
                } catch (Throwable th) {
                    LOG.error().$("Exception in assertMemoryLeak finally ").$(th).$();
                    if (th1 == null) {
                        th1 = th;
                    }
                }
            }

            if (th1 != null) {
                throw new RuntimeException(th1);
            }
        });
    }

    protected static void assertSqlCursors(CharSequence expectedSql, CharSequence actualSql) throws SqlException {
        try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    sqlCompiler,
                    sqlExecutionContext,
                    expectedSql,
                    actualSql,
                    LOG
            );
        }
    }

    protected static void configOverrideRostiAllocFacade(RostiAllocFacade rostiAllocFacade) {
        node1.getConfigurationOverrides().setRostiAllocFacade(rostiAllocFacade);
    }

    protected static void configOverrideUseWithinLatestByOptimisation() {
        Overrides overrides = node1.getConfigurationOverrides();
        overrides.setProperty(PropertyKey.QUERY_WITHIN_LATEST_BY_OPTIMISATION_ENABLED, true);
    }

    protected static void configOverrideWalMaxLagTxnCount() {
        Overrides overrides = node1.getConfigurationOverrides();
        overrides.setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, 1);
    }

    protected static MatViewRefreshJob createMatViewRefreshJob() {
        return createMatViewRefreshJob(engine);
    }

    protected static TableToken createTable(TableModel model) {
        return TestUtils.createTable(engine, model);
    }

    protected static ApplyWal2TableJob createWalApplyJob(QuestDBTestNode node) {
        return createWalApplyJob(node.getEngine());
    }

    protected static ApplyWal2TableJob createWalApplyJob() {
        return createWalApplyJob(engine);
    }

    protected static void drainPurgeJob() {
        TestUtils.drainPurgeJob(engine);
    }

    protected static void drainViewQueue() {
        drainViewQueue(engine);
    }

    protected static void drainWalAndMatViewQueues() {
        drainWalAndMatViewQueues(engine);
    }

    protected static void drainWalAndViewQueues() {
        drainWalAndViewQueues(engine);
    }

    protected static void drainWalQueue(QuestDBTestNode node) {
        drainWalQueue(node.getEngine());
    }

    protected static void drainWalQueue(ApplyWal2TableJob walApplyJob) {
        drainWalQueue(walApplyJob, engine);
    }

    protected static void drainWalQueue() {
        drainWalQueue(engine);
    }

    protected static void dumpMemoryUsage() {
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            LOG.info().$(MemoryTag.nameOf(i)).$(": ").$(Unsafe.getMemUsedByTag(i)).$();
        }
    }

    protected static void execute(CharSequence sqlText) throws SqlException {
        engine.execute(sqlText, sqlExecutionContext);
    }

    protected static void execute(CharSequence sqlText, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.execute(sqlText, sqlExecutionContext);
    }

    protected static void execute(CharSequence sqlText, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        engine.execute(sqlText, sqlExecutionContext, eventSubSeq);
    }

    protected static void execute(SqlCompiler compiler, CharSequence ddl) throws SqlException {
        execute(compiler, ddl, sqlExecutionContext);
    }

    protected static void execute(SqlCompiler compiler, CharSequence sqlText, SqlExecutionContext sqlExecutionContext) throws SqlException {
        CairoEngine.execute(compiler, sqlText, sqlExecutionContext, null);
    }

    protected static void execute(CharSequence ddlSql, SqlExecutionContext sqlExecutionContext, boolean fullFatJoins) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            execute(compiler, ddlSql, sqlExecutionContext);
        }
    }

    protected static void forEachNode(QuestDBNodeTask task) {
        for (int i = 0; i < nodes.size(); i++) {
            task.run(nodes.get(i));
        }
    }

    protected static TableReader getReader(CharSequence tableName) {
        return engine.getReader(tableName);
    }

    protected static TableReader getReader(TableToken tt) {
        return engine.getReader(tt);
    }

    protected static TableWriterAPI getTableWriterAPI(CharSequence tableName) {
        return engine.getTableWriterAPI(tableName, "test");
    }

    @NotNull
    protected static WalWriter getWalWriter(CharSequence tableName) {
        return engine.getWalWriter(engine.verifyTableName(tableName));
    }

    protected static TableWriter getWriter(CharSequence tableName) {
        return TestUtils.getWriter(engine, tableName);
    }

    protected static TableWriter getWriter(CairoEngine engine, CharSequence tableName) {
        return TestUtils.getWriter(engine, tableName);
    }

    protected static TableWriter getWriter(TableToken tt) {
        return engine.getWriter(tt, "testing");
    }

    protected static QuestDBTestNode newNode(int nodeId) {
        String root = TestUtils.unchecked(() -> temp.newFolder("dbRoot" + nodeId).getAbsolutePath());
        return newNode(root, true, nodeId, new Overrides(), getEngineFactory(), getConfigurationFactory());
    }

    protected static QuestDBTestNode newNode(
            String root,
            boolean ownRoot,
            int nodeId,
            Overrides overrides,
            TestCairoEngineFactory engineFactory,
            TestCairoConfigurationFactory configurationFactory
    ) {
        final QuestDBTestNode node = new QuestDBTestNode(nodeId);
        node.initCairo(root, ownRoot, overrides, engineFactory, configurationFactory);
        nodes.add(node);
        return node;
    }

    protected static TableReader newOffPoolReader(CairoConfiguration configuration, CharSequence tableName) {
        return newOffPoolReader(configuration, tableName, engine);
    }

    protected static TableWriter newOffPoolWriter(CairoConfiguration configuration, CharSequence tableName) {
        return TestUtils.newOffPoolWriter(configuration, engine.verifyTableName(tableName), engine);
    }

    protected static TableWriter newOffPoolWriter(CharSequence tableName) {
        return TestUtils.newOffPoolWriter(configuration, engine.verifyTableName(tableName), engine);
    }

    protected static void printSql(CharSequence sql) throws SqlException {
        printSql(sql, sink);
    }

    protected static void printSql(CharSequence sql, MutableUtf16Sink sink) throws SqlException {
        engine.print(sql, sink, sqlExecutionContext);
    }

    protected static void printSql(CharSequence sql, boolean fullFatJoins) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            try {
                TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
            } finally {
                compiler.setFullFatJoins(false);
            }
        }
    }

    protected static void releaseInactive(CairoEngine engine) {
        engine.releaseInactive();
        engine.releaseInactiveTableSequencers();
        engine.resetNameRegistryMemory();
        engine.getTxnScoreboardPool().clear();
        // Drain the per-workload memory-tracker pool: a tracker acquired by a
        // registered query is returned to the pool (not freed) and would
        // otherwise trip the leak checker as a retained native block.
        engine.getMemoryTrackerProvider().clear();
        Assert.assertEquals("busy writer count", 0, engine.getBusyWriterCount());
        Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
    }

    protected static void replicate(String tableName, String wal, QuestDBTestNode srcNode, QuestDBTestNode dstNode) {
        TableToken srcTableToken = srcNode.getEngine().verifyTableName(tableName);
        TableToken dstTableToken = dstNode.getEngine().verifyTableName(tableName);

        dstNode.getEngine().getTableSequencerAPI().closeSequencer(dstTableToken);
        dstNode.getEngine().getTableSequencerAPI().releaseInactive();

        final FilesFacade ff = configuration.getFilesFacade();
        final int mkdirMode = configuration.getMkDirMode();

        final Path srcWal = Path.PATH.get().of(srcNode.getRoot()).concat(srcTableToken).concat(wal);
        final Path dstWal = Path.PATH2.get().of(dstNode.getRoot()).concat(dstTableToken).concat(wal);
        if (ff.exists(dstWal.$())) {
            Assert.assertTrue(ff.rmdir(dstWal));
        }
        Assert.assertEquals(0, ff.mkdir(dstWal.$(), mkdirMode));
        Assert.assertEquals(0, ff.copyRecursive(srcWal, dstWal, mkdirMode));

        final Path srcTxnLog = Path.PATH.get().of(srcNode.getRoot()).concat(srcTableToken).concat(WalUtils.SEQ_DIR);
        final Path dstTxnLog = Path.PATH2.get().of(dstNode.getRoot()).concat(dstTableToken).concat(WalUtils.SEQ_DIR);
        Assert.assertTrue(ff.rmdir(dstTxnLog));
        Assert.assertEquals(0, ff.copyRecursive(srcTxnLog, dstTxnLog, mkdirMode));

        dstNode.getEngine().getTableSequencerAPI().openSequencer(srcTableToken);
    }

    protected static void replicateAndApplyToAllNodes(String tableName, String walName) {
        for (int i = 1, n = nodes.size(); i < n; i++) {
            final QuestDBTestNode node = nodes.get(i);
            replicate(tableName, walName, node1, node);
            drainWalQueue(node);
        }
    }

    protected static RecordCursorFactory select(CharSequence selectSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return engine.select(selectSql, sqlExecutionContext);
    }

    protected static RecordCursorFactory select(SqlCompiler compiler, CharSequence selectSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return CairoEngine.select(compiler, selectSql, sqlExecutionContext);
    }

    protected static RecordCursorFactory select(CharSequence selectSql) throws SqlException {
        return select(selectSql, sqlExecutionContext);
    }

    protected static void setCurrentMicros(long currentMicros) {
        AbstractCairoTest.currentMicros = currentMicros;
        sqlExecutionContext.initNow();
    }

    protected static void setProperty(PropertyKey propertyKey, long maxValue) {
        staticOverrides.setProperty(propertyKey, maxValue);
    }

    protected static void setProperty(PropertyKey propertyKey, String value) {
        staticOverrides.setProperty(propertyKey, value);
    }

    protected static void tickWalQueue(int ticks) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            for (int i = 0; i < ticks; i++) {
                walApplyJob.run();
            }
        }
    }

    protected final void allowFunctionMemoization() {
        SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = true;
    }

    protected void assertCursor(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header) {
        TestUtils.assertCursor(expected, cursor, metadata, header, sink);
    }

    protected void assertCursorTwoPass(CharSequence expected, RecordCursor cursor, RecordMetadata metadata) {
        assertCursor(expected, cursor, metadata, true);
        cursor.toTop();
        assertCursor(expected, cursor, metadata, true);
    }

    /**
     * Entry point for the fluent query-assertion builder. The expected output is supplied to the
     * terminal step ({@link QueryAssertion#returns}, {@link QueryAssertion#returnsRecords},
     * {@link QueryAssertion#fails}), so every chain reads as query -&gt; options -&gt; expectation:
     * <pre>
     * assertQuery("select * from x")
     *         .timestamp("ts")
     *         .expectSize()
     *         .returns(expected);
     *
     * assertQuery("select foo from bar").fails(12, "Invalid column");
     * </pre>
     * Defaults mirror the most common historical behavior: leak checking on,
     * {@code supportsRandomAccess=true}, {@code expectSize=false}, {@code sizeCanBeVariable=false},
     * and no designated timestamp (the result is asserted to have no timestamp unless
     * {@link QueryAssertion#timestamp} is called).
     */
    protected QueryAssertion assertQuery(CharSequence query) {
        return new QueryAssertion(engine, sqlExecutionContext, this::prepareForQueryAssertion, query);
    }

    protected File assertSegmentExistence(boolean expectExists, String tableName, int walId, int segmentId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return assertSegmentExistence(expectExists, tableToken, walId, segmentId);
    }

    protected File assertSegmentExistence(boolean expectExists, @NotNull TableToken tableToken, int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId).slash().put(segmentId);
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
            return new File(Utf8s.toString(path));
        }
    }

    protected void assertSegmentLocked(TableToken tableToken, int walId, int segmentId) {
        final WalLocker locker = engine.getWalLocker();
        Assert.assertTrue(locker.isSegmentLocked(tableToken, walId, segmentId));
    }

    protected void assertSegmentLocked(String tableName, @SuppressWarnings("SameParameterValue") int walId, int segmentId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertSegmentLocked(tableToken, walId, segmentId);
    }

    protected void assertSqlRunWithJit(CharSequence selectSql) throws Exception {
        try (RecordCursorFactory factory = select(selectSql)) {
            Assert.assertTrue("JIT was not enabled for selectSql: " + selectSql, factory.usesCompiledFilter());
        }
    }

    protected void assertSqlWithTypes(CharSequence expected, CharSequence sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlWithTypes(compiler, sqlExecutionContext, sql, sink, expected);
        }
    }

    protected void assertTableExistence(boolean expectExists, @NotNull TableToken tableToken) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken);
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
        }
    }

    protected void assertWalExistence(boolean expectExists, String tableName, int walId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertWalExistence(expectExists, tableToken, walId);
    }

    protected void assertWalExistence(boolean expectExists, @NotNull TableToken tableToken, int walId) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId);
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
        }
    }

    protected void assertWalLocked(String tableName, @SuppressWarnings("SameParameterValue") int walId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertWalLocked(tableToken, walId);
    }

    protected void assertWalLocked(TableToken tableToken, int walId) {
        final WalLocker locker = engine.getWalLocker();
        Assert.assertTrue(locker.isWalLocked(tableToken, walId));
    }

    protected void assertWalNotLocked(String tableName, @SuppressWarnings("SameParameterValue") int walId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertWalNotLocked(tableToken, walId);
    }

    protected void assertWalNotLocked(TableToken tableToken, int walId) {
        final WalLocker locker = engine.getWalLocker();
        Assert.assertFalse(locker.isWalLocked(tableToken, walId));
    }

    protected void createPopulateTable(TableModel tableModel, int totalRows, String startDate, int partitionCount) throws NumericException, SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.createPopulateTable(compiler, sqlExecutionContext, tableModel, totalRows, startDate, partitionCount);
        }
    }

    protected TableToken createPopulateTable(int tableId, TableModel tableModel, int totalRows, String startDate, int partitionCount) throws NumericException, SqlException {
        return createPopulateTable(tableId, tableModel, 1, totalRows, startDate, partitionCount);
    }

    protected TableToken createPopulateTable(int tableId, TableModel tableModel, int insertIterations, int totalRowsPerIteration, String startDate, int partitionCount) throws NumericException, SqlException {
        try (
                MemoryMARW mem = Vm.getCMARWInstance();
                Path path = new Path()
        ) {
            TableToken token = TestUtils.createTable(engine, mem, path, tableModel, tableId, tableModel.getTableName());
            for (int i = 0; i < insertIterations; i++) {
                execute(TestUtils.insertFromSelectPopulateTableStmt(tableModel, totalRowsPerIteration, startDate, partitionCount));
            }
            return token;
        }
    }

    protected PoolListener createWriterReleaseListener(CharSequence tableName, SOCountDownLatch latch) {
        return (factoryType, _, tableToken, event, _, _) -> {
            if (
                    factoryType == PoolListener.SRC_WRITER
                            && event == PoolListener.EV_RETURN
                            && tableToken != null
                            && Chars.equalsIgnoreCase(tableToken.getTableName(), tableName)
            ) {
                latch.countDown();
            }
        };
    }

    protected PlanSink getPlanSink(CharSequence query) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            planSink.of(factory, sqlExecutionContext);
            return planSink;
        }
    }

    protected boolean isWalTable(CharSequence tableName) {
        return engine.isWalTable(engine.verifyTableName(tableName));
    }

    protected TableWriter newOffPoolWriter(CairoConfiguration configuration, CharSequence tableName, MessageBus messageBus) {
        return TestUtils.newOffPoolWriter(configuration, engine.verifyTableName(tableName), messageBus, engine);
    }

    /**
     * Hook invoked by the {@link QueryAssertion} builder immediately before it runs a query assertion.
     * The default implementation does nothing. Subclasses override it to bring shared state up to date
     * before the assertion reads it (for example, draining the WAL apply queue).
     */
    protected void prepareForQueryAssertion() {
    }

    protected long update(CharSequence updateSql) throws SqlException {
        return update(updateSql, sqlExecutionContext, null);
    }

    protected long update(
            CharSequence updateSql,
            SqlExecutionContext sqlExecutionContext,
            @Nullable SCSequence eventSubSeq
    ) throws SqlException {
        return engine.update(updateSql, sqlExecutionContext, eventSubSeq);
    }

    protected enum SymbolAsFieldMode {
        WITH_SYMBOLS_AS_FIELD, NO_SYMBOLS_AS_FIELD
    }

    public enum WalMode {
        WITH_WAL, NO_WAL
    }

    @FunctionalInterface
    protected interface QuestDBNodeTask {
        void run(QuestDBTestNode node);
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
        FACTORY_TAGS[MemoryTag.NATIVE_INDEX_READER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_WAL_WRITER] = false;
    }
}
