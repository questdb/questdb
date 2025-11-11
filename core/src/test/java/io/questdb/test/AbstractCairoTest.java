/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.ExplainPlanFactory;
import io.questdb.griffin.engine.functions.catalogue.DumpThreadStacksFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.jit.JitUtil;
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
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
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
import io.questdb.test.tools.BindVariableTestSetter;
import io.questdb.test.tools.BindVariableTestTuple;
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
import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@SuppressWarnings("ClassEscapesDefinedScope")
public abstract class AbstractCairoTest extends AbstractTest {

    public static final int DEFAULT_SPIN_LOCK_TIMEOUT = 5000;
    protected static final Log LOG = LogFactory.getLog(AbstractCairoTest.class);
    protected static final String TIMESTAMP_NS_TYPE_NAME = "TIMESTAMP_NS";
    protected static final PlanSink planSink = new TextPlanSink();
    protected static final StringSink sink = new StringSink();
    private static final double EPSILON = 0.000001;
    private static final long[] SNAPSHOT = new long[MemoryTag.SIZE];
    private static final LongList rows = new LongList();
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

    public static boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            boolean fragmentedSymbolTables
    ) {
        return assertCursor(
                expected,
                supportsRandomAccess,
                sizeExpected,
                sizeCanBeVariable,
                cursor,
                metadata,
                sink,
                rows,
                fragmentedSymbolTables
        );
    }

    // Thread-safe cursor assertion method.
    public static boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            StringSink sink,
            LongList rows,
            boolean fragmentedSymbolTables
    ) {
        long cursorSizeBeforeFetch = cursor.size();
        if (expected == null) {
            Assert.assertFalse(cursor.hasNext());
            cursor.toTop();
            Assert.assertFalse(cursor.hasNext());
            return true;
        }

        TestUtils.assertCursor(expected, cursor, metadata, true, sink);
        long preComputerStateSize = cursor.preComputedStateSize();

        testSymbolAPI(metadata, cursor, fragmentedSymbolTables);
        cursor.toTop();
        Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
        testStringsLong256AndBinary(metadata, cursor);

        // test API where the same record is being updated by cursor
        cursor.toTop();
        Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
        Record record = cursor.getRecord();
        Assert.assertNotNull(record);
        sink.clear();
        CursorPrinter.println(metadata, sink);
        long count = 0;
        long cursorSize = cursor.size();
        while (cursor.hasNext()) {
            TestUtils.println(record, metadata, sink);
            count++;
        }

        final Rnd rnd = TestUtils.generateRandom(LOG);
        int skip = count > 0 && rnd.nextBoolean() ? rnd.nextInt((int) count) : 0;
        int k = 0;
        while (k < skip && cursor.hasNext()) {
            k++;
        }
        sink.clear();
        cursor.toTop();
        TestUtils.assertCursor(expected, cursor, metadata, true, sink);

        if (!sizeCanBeVariable) {
            if (sizeExpected) {
                Assert.assertTrue("Concrete cursor size expected but was -1", cursorSize != -1);
            } else {
                Assert.assertTrue("Invalid/undetermined cursor size expected but was " + cursorSize, cursorSize <= 0);
            }
        }
        if (cursorSize != -1) {
            Assert.assertEquals("Actual cursor records vs cursor.size()", count, cursorSize);
            if (cursorSizeBeforeFetch != -1) {
                Assert.assertEquals("Cursor size before fetch and after", cursorSizeBeforeFetch, cursorSize);
            }
        } else {
            if (count > 0) {
                RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.toTop();
                skip = rnd.nextBoolean() ? rnd.nextInt((int) count) : 0;
                while (counter.get() < skip && cursor.hasNext()) {
                    counter.inc();
                }
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals("Actual cursor records vs cursor.calculateSize()", count, counter.get());
            } else {
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
            }
        }

        TestUtils.assertEquals(expected, sink);

        if (supportsRandomAccess) {
            cursor.toTop();
            Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
            sink.clear();
            rows.clear();
            while (cursor.hasNext()) {
                rows.add(record.getRowId());
            }

            final Record rec = cursor.getRecord();
            CursorPrinter.println(metadata, sink);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(rec, rows.getQuick(i));
                TestUtils.println(rec, metadata, sink);
            }

            TestUtils.assertEquals(expected, sink);

            sink.clear();

            final Record factRec = cursor.getRecordB();
            CursorPrinter.println(metadata, sink);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(factRec, rows.getQuick(i));
                TestUtils.println(factRec, metadata, sink);
            }

            TestUtils.assertEquals(expected, sink);

            // test that absolute positioning of record does not affect the state of record cursor
            if (rows.size() > 0) {
                sink.clear();

                cursor.toTop();
                Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
                int target = rows.size() / 2;
                CursorPrinter.println(metadata, sink);
                while (target-- > 0 && cursor.hasNext()) {
                    TestUtils.println(record, metadata, sink);
                }

                // no obliterated record with absolute positioning
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(factRec, rows.getQuick(i));
                }

                // now continue normal fetch
                while (cursor.hasNext()) {
                    TestUtils.println(record, metadata, sink);
                }

                TestUtils.assertEquals(expected, sink);

                // now test that cursor.hasNext() won't affect the state of recordB
                sink.clear();
                CursorPrinter.println(metadata, sink);
                cursor.toTop();
                target = rows.size() / 2;
                boolean cursorExhausted = false;
                for (int i = 0, n = target; i < n; i++) {
                    cursor.recordAt(factRec, rows.getQuick(i));
                    // intentionally calling hasNext() twice: we want to advance the cursor position,
                    // but we do *NOT* want to call it in step-lock with recordAt()
                    if (!cursorExhausted) {
                        cursorExhausted = !cursor.hasNext();
                    }
                    if (!cursorExhausted) {
                        cursorExhausted = !cursor.hasNext();
                    }
                    TestUtils.println(factRec, metadata, sink);
                }
                cursor.toTop();
                for (int i = target, n = rows.size(); i < n; i++) {
                    // in the 2nd part, we are intentionally not calling hasNext() at all
                    cursor.recordAt(factRec, rows.getQuick(i));
                    TestUtils.println(factRec, metadata, sink);
                }
                TestUtils.assertEquals(expected, sink);
            }
        } else {
            try {
                cursor.getRecordB();
                Assert.fail();
            } catch (UnsupportedOperationException ignore) {
            }

            try {
                cursor.recordAt(record, 0);
                Assert.fail();
            } catch (UnsupportedOperationException ignore) {
            }
        }
        return false;
    }

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

    public static boolean doubleEquals(double a, double b, double epsilon) {
        return a == b || Math.abs(a - b) < epsilon;
    }

    public static boolean doubleEquals(double a, double b) {
        return doubleEquals(a, b, EPSILON);
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
        return TIMESTAMP_NS_TYPE_NAME.equals(timestampType) ? expected.replaceAll("00Z", "00000Z").replaceAll("-00000", "-00000000") : expected;
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
        sqlExecutionContext.resetFlags();
        sqlExecutionContext.setParallelFilterEnabled(configuration.isSqlParallelFilterEnabled());
        sqlExecutionContext.setParallelGroupByEnabled(configuration.isSqlParallelGroupByEnabled());
        sqlExecutionContext.setParallelReadParquetEnabled(configuration.isSqlParallelReadParquetEnabled());
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

    private static void assertCalculateSize(RecordCursorFactory factory) throws SqlException {
        long size;
        SqlExecutionCircuitBreaker circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        RecordCursor.Counter counter = new RecordCursor.Counter();

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            cursor.calculateSize(circuitBreaker, counter);
            size = counter.get();
            long preComputeStateSize = cursor.preComputedStateSize();
            cursor.toTop();
            Assert.assertEquals(preComputeStateSize, cursor.preComputedStateSize());
            counter.clear();
            cursor.calculateSize(circuitBreaker, counter);
            long sizeAfterToTop = counter.get();

            Assert.assertEquals(size, sizeAfterToTop);
        }

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            counter.clear();
            cursor.calculateSize(circuitBreaker, counter);
            long sizeAfterReopen = counter.get();

            Assert.assertEquals(size, sizeAfterReopen);
        }
    }

    private static void assertException0(CharSequence sql, SqlExecutionContext sqlExecutionContext, boolean fullFatJoins) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            if (cq.getRecordCursorFactory() != null) {
                try (
                        RecordCursorFactory factory = cq.getRecordCursorFactory();
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    sink.clear();
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        // ignore the output, we're looking for an error
                        TestUtils.println(record, factory.getMetadata(), sink);
                        sink.clear();
                    }
                }
            } else if (cq.getOperation() != null) {
                try (
                        Operation op = cq.getOperation();
                        OperationFuture fut = op.execute(sqlExecutionContext, null)
                ) {
                    fut.await();
                }
            } else {
                // make sure to close update/insert operation
                try (
                        UpdateOperation ignore = cq.getUpdateOperation();
                        InsertOperation ignored = cq.popInsertOperation()
                ) {
                    execute(compiler, sql, sqlExecutionContext);
                }
            }
        }
        Assert.fail("SQL statement should have failed");
    }

    private static void assertSymbolColumnThreadSafety(int numberOfIterations, int symbolColumnCount, ObjList<SymbolTable> symbolTables, int[] symbolTableKeySnapshot, String[][] symbolTableValueSnapshot) {
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int i = 0; i < numberOfIterations; i++) {
            int symbolColIndex = rnd.nextInt(symbolColumnCount);
            SymbolTable symbolTable = symbolTables.getQuick(symbolColIndex);
            int max = symbolTableKeySnapshot[symbolColIndex] + 1;
            // max could be -1 meaning we have nulls; max can also be 0, meaning only one symbol value
            // basing boundary on 2 we convert -1 tp 1 and 0 to 2
            int key = rnd.nextInt(max + 1) - 1;
            String expected = symbolTableValueSnapshot[symbolColIndex][key + 1];
            TestUtils.assertEquals(expected, symbolTable.valueOf(key));
            // now test static symbol table
            if (expected != null && symbolTable instanceof StaticSymbolTable staticSymbolTable) {
                Assert.assertEquals(key, staticSymbolTable.keyOf(expected));
            }
        }
    }

    private static TestCairoConfigurationFactory getConfigurationFactory() {
        return configurationFactory != null ? configurationFactory : CairoTestConfiguration::new;
    }

    private static TestCairoEngineFactory getEngineFactory() {
        return engineFactory != null ? engineFactory : CairoEngine::new;
    }

    private static void testStringsLong256AndBinary(RecordMetadata metadata, RecordCursor cursor) {
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                    case ColumnType.STRING:
                        CharSequence s = record.getStrA(i);
                        if (s != null) {
                            Assert.assertEquals(s.length(), record.getStrLen(i));
                            CharSequence b = record.getStrB(i);
                            if (b instanceof AbstractCharSequence) {
                                // AbstractCharSequence are usually mutable. We cannot have a same mutable instance for A and B
                                Assert.assertNotSame("Expected string instances to be different for getStr and getStrB", s, b);
                            }
                        } else {
                            Assert.assertNull(record.getStrB(i));
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                        }
                        break;
                    case ColumnType.BINARY:
                        BinarySequence bs = record.getBin(i);
                        if (bs != null) {
                            Assert.assertEquals(bs.length(), record.getBinLen(i));
                        } else {
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                        }
                        break;
                    case ColumnType.LONG256:
                        Long256 l1 = record.getLong256A(i);
                        Long256 l2 = record.getLong256B(i);
                        if (l1 == Long256Impl.NULL_LONG256) {
                            Assert.assertSame(l1, l2);
                        } else {
                            Assert.assertNotSame(l1, l2);
                        }
                        Assert.assertEquals(l1.getLong0(), l2.getLong0());
                        Assert.assertEquals(l1.getLong1(), l2.getLong1());
                        Assert.assertEquals(l1.getLong2(), l2.getLong2());
                        Assert.assertEquals(l1.getLong3(), l2.getLong3());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private static void testSymbolAPI(RecordMetadata metadata, RecordCursor cursor, boolean fragmentedSymbolTables) {
        IntList symbolIndexes = null;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                if (symbolIndexes == null) {
                    symbolIndexes = new IntList();
                }
                symbolIndexes.add(i);
            }
        }

        if (symbolIndexes != null) {

            // create new symbol tables and make sure they are not the same
            // as the default ones

            ObjList<SymbolTable> clonedSymbolTables = new ObjList<>();
            ObjList<SymbolTable> originalSymbolTables = new ObjList<>();
            int[] symbolTableKeySnapshot = new int[symbolIndexes.size()];
            String[][] symbolTableValueSnapshot = new String[symbolIndexes.size()][];
            try {
                cursor.toTop();
                if (!fragmentedSymbolTables && cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        originalSymbolTables.add(cursor.getSymbolTable(columnIndex));
                    }

                    // take a snapshot of symbol tables
                    // multiple passes over the same cursor, if not very efficient, we
                    // can swap loops around
                    int sumOfMax = 0;
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        cursor.toTop();
                        final Record rec = cursor.getRecord();
                        final int column = symbolIndexes.getQuick(i);
                        int max = -1;
                        while (cursor.hasNext()) {
                            max = Math.max(max, rec.getInt(column));
                        }
                        String[] values = new String[max + 2];
                        final SymbolTable symbolTable = cursor.getSymbolTable(column);
                        for (int k = -1; k <= max; k++) {
                            values[k + 1] = Chars.toString(symbolTable.valueOf(k));
                        }
                        symbolTableKeySnapshot[i] = max;
                        symbolTableValueSnapshot[i] = values;
                        sumOfMax += max;
                    }

                    // We grab clones after iterating through the symbol values due to
                    // the cache warm up required by Cast*ToSymbolFunctionFactory functions.
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        SymbolTable tab = cursor.newSymbolTable(columnIndex);
                        Assert.assertNotNull(tab);
                        clonedSymbolTables.add(tab);
                    }

                    // Now start two threads, one will be using normal symbol table,
                    // another will be using a clone. Threads will randomly check that
                    // symbol table is able to convert keys to values without problems

                    int numberOfIterations = sumOfMax * 2;
                    int symbolColumnCount = symbolIndexes.size();
                    int workerCount = 2;
                    CyclicBarrier barrier = new CyclicBarrier(workerCount);
                    SOCountDownLatch doneLatch = new SOCountDownLatch(workerCount);
                    AtomicInteger errorCount = new AtomicInteger(0);

                    // thread that is hitting clones
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(numberOfIterations, symbolColumnCount, clonedSymbolTables, symbolTableKeySnapshot, symbolTableValueSnapshot);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            //noinspection CallToPrintStackTrace
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    // thread that is hitting the original symbol tables
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(numberOfIterations, symbolColumnCount, originalSymbolTables, symbolTableKeySnapshot, symbolTableValueSnapshot);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            //noinspection CallToPrintStackTrace
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    doneLatch.await();

                    Assert.assertEquals(0, errorCount.get());
                }

                cursor.toTop();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        int column = symbolIndexes.getQuick(i);
                        SymbolTable symbolTable = cursor.getSymbolTable(column);
                        if (symbolTable instanceof StaticSymbolTable) {
                            CharSequence sym = Chars.toString(record.getSymA(column));
                            int value = record.getInt(column);
                            if (((StaticSymbolTable) symbolTable).containsNullValue() && value == ((StaticSymbolTable) symbolTable).getSymbolCount()) {
                                Assert.assertEquals(Integer.MIN_VALUE, ((StaticSymbolTable) symbolTable).keyOf(sym));
                            } else {
                                Assert.assertEquals(value, ((StaticSymbolTable) symbolTable).keyOf(sym));
                            }
                            TestUtils.assertEquals(sym, symbolTable.valueOf(value));
                        } else {
                            final int value = record.getInt(column);
                            TestUtils.assertEquals(record.getSymA(column), symbolTable.valueOf(value));
                        }
                    }
                }
            } finally {
                Misc.freeObjListIfCloseable(clonedSymbolTables);
            }
        }
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

            long parquetSize = txReader.getPartitionParquetFileSize(i);

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
                sink.put(", parquetSize: ").put(parquetSize);
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
        AlterOperationBuilder addColumnC = new AlterOperationBuilder().ofAddColumn(0, writer.getTableToken(), 0);
        addColumnC.ofAddColumn(columnName, 1, columnType, 0, false, false, 0);
        writer.apply(addColumnC.build(), true);
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable, // this means size() can either be -1 in some cases or known in others
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        boolean cursorAsserted;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals("supports random access", supportsRandomAccess, factory.recordCursorSupportsRandomAccess());
            cursorAsserted = assertCursor(expected, supportsRandomAccess, sizeExpected, sizeCanBeVariable, cursor, factory.getMetadata(), factory.fragmentedSymbolTables());
        }

        assertFactoryMemoryUsage();

        if (cursorAsserted) {
            return;
        }

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            testSymbolAPI(factory.getMetadata(), cursor, factory.fragmentedSymbolTables());
        }

        assertFactoryMemoryUsage();
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean expectSize
    ) throws SqlException {
        assertCursor(expected, factory, supportsRandomAccess, expectSize, false);
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws SqlException {
        assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, sqlExecutionContext);
    }

    protected static void assertCursorRawRecords(Record[] expected, RecordCursorFactory factory, boolean expectSize) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            if (expected == null) {
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
                return;
            }

            final long rowsCount = cursor.size();
            if (expectSize) {
                Assert.assertEquals(rowsCount, expected.length);
            }

            RecordMetadata metadata = factory.getMetadata();

            testSymbolAPI(metadata, cursor, factory.fragmentedSymbolTables());
            cursor.toTop();
            testStringsLong256AndBinary(metadata, cursor);

            cursor.toTop();
            final Record record = cursor.getRecord();
            Assert.assertNotNull(record);
            int expectedRow = 0;
            while (cursor.hasNext()) {
                for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(col))) {
                        case ColumnType.BOOLEAN:
                            Assert.assertEquals(expected[expectedRow].getBool(col), record.getBool(col));
                            break;
                        case ColumnType.BYTE:
                            Assert.assertEquals(expected[expectedRow].getByte(col), record.getByte(col));
                            break;
                        case ColumnType.SHORT:
                            Assert.assertEquals(expected[expectedRow].getShort(col), record.getShort(col));
                            break;
                        case ColumnType.CHAR:
                            Assert.assertEquals(expected[expectedRow].getChar(col), record.getChar(col));
                            break;
                        case ColumnType.INT:
                            Assert.assertEquals(expected[expectedRow].getInt(col), record.getInt(col));
                            break;
                        case ColumnType.LONG:
                            Assert.assertEquals(expected[expectedRow].getLong(col), record.getLong(col));
                            break;
                        case ColumnType.DATE:
                            Assert.assertEquals(expected[expectedRow].getDate(col), record.getDate(col));
                            break;
                        case ColumnType.TIMESTAMP:
                            Assert.assertEquals(expected[expectedRow].getTimestamp(col), record.getTimestamp(col));
                            break;
                        case ColumnType.FLOAT:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getFloat(col), record.getFloat(col)));
                            break;
                        case ColumnType.DOUBLE:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getDouble(col), record.getDouble(col)));
                            break;
                        case ColumnType.STRING:
                            TestUtils.assertEquals(expected[expectedRow].getStrA(col), record.getStrA(col));
                            break;
                        case ColumnType.SYMBOL:
                            TestUtils.assertEquals(expected[expectedRow].getSymA(col), record.getSymA(col));
                            break;
                        case ColumnType.IPv4:
                            Assert.assertEquals(expected[expectedRow].getIPv4(col), record.getIPv4(col));
                            break;
                        case ColumnType.LONG256:
                            Long256 l1 = expected[expectedRow].getLong256A(col);
                            Long256 l2 = record.getLong256A(col);
                            Assert.assertEquals(l1.getLong0(), l2.getLong0());
                            Assert.assertEquals(l1.getLong1(), l2.getLong1());
                            Assert.assertEquals(l1.getLong2(), l2.getLong2());
                            Assert.assertEquals(l1.getLong3(), l2.getLong3());
                            break;
                        case ColumnType.BINARY:
                            TestUtils.assertEquals(expected[expectedRow].getBin(col), record.getBin(col), record.getBin(col).length());
                        default:
                            Assert.fail("Unknown column type");
                            break;
                    }
                }
                expectedRow++;
            }
            Assert.assertTrue((expectSize && rowsCount != -1) || (!expectSize && rowsCount == -1));
            Assert.assertTrue(rowsCount == -1 || expectedRow == rowsCount);
        }
        assertFactoryMemoryUsage();
    }

    protected static void assertException(CharSequence sql, int errorPos, CharSequence contains) throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(sql, errorPos, contains, false));
    }

    protected static void assertException(CharSequence sql, int errorPos, CharSequence contains, SqlExecutionContext sqlExecutionContext) throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(sql, errorPos, contains, sqlExecutionContext));
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql) throws Exception {
        assertException0(sql, sqlExecutionContext, false);
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
        assertException0(sql, executionContext, fullFatJoins);
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos, CharSequence contains, boolean fullFatJoins) throws Exception {
        assertExceptionNoLeakCheck(sql, errorPos, contains, fullFatJoins, sqlExecutionContext);
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos, CharSequence contains, boolean fullFatJoins, SqlExecutionContext sqlExecutionContext) throws Exception {
        Assert.assertNotNull(contains);
        Assert.assertFalse("provide matching text", contains.isEmpty());
        try {
            assertExceptionNoLeakCheck(sql, sqlExecutionContext, fullFatJoins);
        } catch (Throwable e) {
            if (e instanceof FlyweightMessageContainer) {
                TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), contains);
                Assert.assertEquals(errorPos, ((FlyweightMessageContainer) e).getPosition());
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
                    forEachNode(node -> node.getEngine().clear());
                    AbstractCairoTest.ff = ffBefore;
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

    protected static void assertQuery(Record[] expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean expectSize) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, expectSize);
    }

    protected static void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, true, false, false);
    }

    protected static void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean supportsRandomAccess) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, false, false);
    }

    /**
     * expectedTimestamp can either be an exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess
    ) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, false, false);
    }

    protected static void assertQuery(
            Record[] expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable Record[] expected2,
            boolean expectSize
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                execute(ddl);
            }

            snapshotMemoryUsage();

            RecordCursorFactory factory = select(query);
            try {
                assertTimestamp(expectedTimestamp, factory);
                assertCursorRawRecords(expected, factory, expectSize);
                // make sure we get the same outcome when we get factory to create new cursor
                assertCursorRawRecords(expected, factory, expectSize);
                // make sure strings, binary fields and symbols are compliant with expected record behaviour
                assertVariableColumns(factory, sqlExecutionContext);

                if (ddl2 != null) {
                    execute(ddl2, sqlExecutionContext);

                    int count = 3;
                    while (count > 0) {
                        try {
                            assertCursorRawRecords(expected2, factory, expectSize);
                            // and again
                            assertCursorRawRecords(expected2, factory, expectSize);
                            return;
                        } catch (TableReferenceOutOfDateException e) {
                            Misc.free(factory);
                            factory = select(query);
                            count--;
                        }
                    }
                }
            } finally {
                Misc.free(factory);
            }
        });
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, expectSize, sizeCanBeVariable));
    }

    /**
     * expectedTimestamp can either be an exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, expectSize, false);
    }

    protected static void assertQueryExpectSize(CharSequence expected, CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(expected, query, ddl, null, null, null, true, true, false);
    }

    /**
     * expectedTimestamp can either be an exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQueryNoLeakCheck(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess
    ) throws Exception {
        assertQueryNoLeakCheck(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, false, false);
    }

    protected static void assertQueryNoLeakCheck(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp) throws Exception {
        assertQueryNoLeakCheck(expected, query, ddl, expectedTimestamp, null, null, true, false, false);
    }

    protected static void assertQueryNoLeakCheck(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertQueryNoLeakCheck(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, expectSize, false);
    }

    protected static void assertQueryNoLeakCheck(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean supportsRandomAccess) throws Exception {
        assertQueryNoLeakCheck(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, false, false);
    }

    protected static void assertQueryNoLeakCheck(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws Exception {
        if (ddl != null) {
            execute(ddl);
            if (configuration.getWalEnabledDefault()) {
                drainWalQueue();
            }
        }
        printSqlResult(() -> expected, query, expectedTimestamp, ddl2, expected2, supportsRandomAccess, expectSize, sizeCanBeVariable, null);
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

    /**
     * expectedTimestamp can either be an exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory) throws SqlException {
        assertTimestamp(expectedTimestamp, factory, sqlExecutionContext);
    }

    /**
     * expectedTimestamp can either be an exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (expectedTimestamp == null || expectedTimestamp.isEmpty()) {
            int timestampIdx = factory.getMetadata().getTimestampIndex();
            if (timestampIdx != -1) {
                Assert.fail("Expected no timestamp but found " + factory.getMetadata().getColumnName(timestampIdx) + ", idx=" + timestampIdx);
            }
        } else {
            boolean expectAscendingOrder = true;
            String tsDesc = expectedTimestamp.toString();
            int position = tsDesc.indexOf("###");
            if (position > 0) {
                expectedTimestamp = tsDesc.substring(0, position);
                expectAscendingOrder = tsDesc.substring(position + 3).equalsIgnoreCase("asc");
            }

            if (expectAscendingOrder) {
                try {
                    Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, factory.getScanDirection());
                } catch (AssertionError e) {
                    throw new AssertionError("expected ASCENDING timestamp", e);
                }
            } else {
                try {
                    Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, factory.getScanDirection());
                } catch (AssertionError e) {
                    throw new AssertionError("expected DESCENDING timestamp", e);
                }
            }

            int index = factory.getMetadata().getColumnIndexQuiet(expectedTimestamp);
            Assert.assertTrue("Column '" + expectedTimestamp + "' can't be found in metadata", index > -1);
            Assert.assertNotEquals("Expected non-negative value as timestamp index", -1, index);
            Assert.assertEquals("Timestamp column index", index, factory.getMetadata().getTimestampIndex());
            assertTimestampColumnValues(factory, sqlExecutionContext, expectAscendingOrder);
        }
    }

    protected static void assertTimestampColumnValues(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext, boolean isAscending) throws SqlException {
        int index = factory.getMetadata().getTimestampIndex();
        Assert.assertEquals(ColumnType.TIMESTAMP, ColumnType.tagOf(factory.getMetadata().getColumnType(index)));
        long timestamp = isAscending ? Long.MIN_VALUE : Long.MAX_VALUE;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            long c = 0;
            while (cursor.hasNext()) {
                long ts = record.getTimestamp(index);
                if ((isAscending && timestamp > ts) || (!isAscending && timestamp < ts)) {
                    StringSink error = new StringSink();
                    error.put("record # ").put(c).put(" should have ").put(isAscending ? "bigger" : "smaller").put(" (or equal) timestamp than the row before. Values prior=");
                    MicrosFormatUtils.appendDateTimeUSec(error, timestamp);
                    error.put(" current=");
                    MicrosFormatUtils.appendDateTimeUSec(error, ts);

                    Assert.fail(error.toString());
                }
                timestamp = ts;
                c++;
            }
        }
        assertFactoryMemoryUsage();
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

    protected static boolean couldObtainLock(Path path) {
        final long lockFd = TableUtils.lock(TestFilesFacadeImpl.INSTANCE, path.$(), false);
        if (lockFd != -1L) {
            TestFilesFacadeImpl.INSTANCE.close(lockFd);
            return true;  // Could lock/unlock.
        }
        return false;  // Could not obtain lock.
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

    protected static void drainWalAndMatViewQueues() {
        drainWalAndMatViewQueues(engine);
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

    protected static void printSqlResult(CharSequence expected, CharSequence query, CharSequence expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        printSqlResult(() -> expected, query, expectedTimestamp, null, null, supportsRandomAccess, expectSize, false, null);
    }

    protected static void printSqlResult(
            Supplier<? extends CharSequence> expectedSupplier,
            CharSequence query,
            CharSequence expectedTimestamp,
            CharSequence ddl2,
            CharSequence expected2,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable,
            CharSequence expectedPlan
    ) throws SqlException {
        snapshotMemoryUsage();
        RecordCursorFactory factory = select(query);
        if (expectedPlan != null) {
            planSink.clear();
            factory.toPlan(planSink);
            assertCursor(expectedPlan, new ExplainPlanFactory(factory, ExplainModel.FORMAT_TEXT), false, expectSize, sizeCanBeVariable);
        }
        try {
            assertTimestamp(expectedTimestamp, factory);
            CharSequence expected = expectedSupplier.get();
            assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory, sqlExecutionContext);

            if (ddl2 != null) {
                execute(ddl2);
                if (configuration.getWalEnabledDefault()) {
                    drainWalQueue();
                }

                int count = 3;
                while (count > 0) {
                    try {
                        assertCursor(expected2, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
                        // and again
                        assertCursor(expected2, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
                        return;
                    } catch (TableReferenceOutOfDateException e) {
                        Misc.free(factory);
                        factory = select(query);
                        count--;
                    }
                }
            }

            // make sure calculateSize() produces consistent result
            assertCalculateSize(factory);
        } finally {
            Misc.free(factory);
        }
    }

    protected static void releaseInactive(CairoEngine engine) {
        engine.releaseInactive();
        engine.releaseInactiveTableSequencers();
        engine.resetNameRegistryMemory();
        engine.getTxnScoreboardPool().clear();
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
                walApplyJob.run(0);
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

    protected void assertException(CharSequence sql, @NotNull CharSequence ddl, int errorPos, @NotNull CharSequence contains) throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(sql, ddl, errorPos, contains));
    }

    protected void assertExceptionNoLeakCheck(CharSequence sql, @NotNull CharSequence ddl, int errorPos, @NotNull CharSequence contains) throws Exception {
        try {
            execute(ddl, sqlExecutionContext);
            assertException(sql, errorPos, contains);
            Assert.assertEquals(0, engine.getBusyReaderCount());
            Assert.assertEquals(0, engine.getBusyWriterCount());
        } finally {
            engine.clear();
        }
    }

    protected void assertFactoryCursor(
            String expected,
            String expectedTimestamp,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            SqlExecutionContext executionContext,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws SqlException {
        assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, executionContext);
        // v Please keep this check after ^ that one.
        // Factories that have a scan order dependent on the bind variable will not test correctly.
        // See generate_series
        assertTimestamp(expectedTimestamp, factory, executionContext);
        // make sure we get the same outcome when we get factory to create new cursor
        assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, executionContext);
        // make sure strings, binary fields and symbols are compliant with expected record behaviour
        assertVariableColumns(factory, executionContext);
    }

    // asserts plan without having to prefix a query with 'explain', specify the fixed output header, etc.
    protected void assertPlanNoLeakCheck(CharSequence query, CharSequence expectedPlan) throws SqlException {
        StringSink sink = new StringSink();
        sink.put("EXPLAIN ").put(query);
        try (
                ExplainPlanFactory planFactory = getPlanFactory(sink);
                RecordCursor cursor = planFactory.getCursor(sqlExecutionContext)
        ) {
            if (!JitUtil.isJitSupported()) {
                expectedPlan = Chars.toString(expectedPlan).replace("Async JIT", "Async");
            }
            TestUtils.assertCursor(expectedPlan, cursor, planFactory.getMetadata(), false, sink);
        }
    }

    protected void assertPlanNoLeakCheck(SqlCompiler compiler, CharSequence query, CharSequence expectedPlan, SqlExecutionContext sqlExecutionContext) throws SqlException {
        StringSink sink = new StringSink();
        sink.put("EXPLAIN ").put(query);
        try (
                ExplainPlanFactory planFactory = getPlanFactory(compiler, sink, sqlExecutionContext);
                RecordCursor cursor = planFactory.getCursor(sqlExecutionContext)
        ) {
            if (!JitUtil.isJitSupported()) {
                expectedPlan = Chars.toString(expectedPlan).replace("Async JIT", "Async");
            }
            TestUtils.assertCursor(expectedPlan, cursor, planFactory.getMetadata(), false, sink);
        }
    }

    protected void assertQuery(String expected, String query, boolean expectSize) throws Exception {
        assertQuery(expected, query, null, null, true, expectSize);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertMemoryLeak(() -> assertQueryFullFatNoLeakCheck(expected, query, expectedTimestamp, supportsRandomAccess, expectSize, false));
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess) throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(expected, query, expectedTimestamp, supportsRandomAccess));
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp) throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(expected, query, expectedTimestamp, false));
    }

    protected void assertQuery(String expected, String query, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        assertQueryFullFatNoLeakCheck(expected, query, null, supportsRandomAccess, expectSize, false);
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp, boolean expectSize) throws SqlException {
        assertQueryAndCache(expected, query, expectedTimestamp, false, expectSize);
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        assertQueryAndCacheFullFat(expected, query, expectedTimestamp, supportsRandomAccess, expectSize);
    }

    protected void assertQueryAndCacheFullFat(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = select(query)) {
            assertFactoryCursor(
                    expected,
                    expectedTimestamp,
                    factory,
                    supportsRandomAccess,
                    sqlExecutionContext,
                    expectSize,
                    false
            );
        }
    }

    protected void assertQueryAndPlan(String expected, String expectedPlan, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertMemoryLeak(() -> {
            assertPlanNoLeakCheck(query, expectedPlan);
            assertQueryFullFatNoLeakCheck(expected, query, expectedTimestamp, supportsRandomAccess, expectSize, false);
        });
    }

    protected void assertQueryAndPlan(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable,
            @Nullable CharSequence expectedPlan
    ) throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, expectSize, sizeCanBeVariable);
            assertPlanNoLeakCheck(query, expectedPlan);
        });
    }

    protected void assertQueryFullFatNoLeakCheck(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize, boolean fullFatJoin) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoin);
            assertQueryNoLeakCheck(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, expectSize);
        }
    }

    protected void assertQueryNoLeakCheck(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            SqlExecutionContext sqlExecutionContext,
            boolean supportsRandomAccess,
            boolean expectSize
    ) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = CairoEngine.select(compiler, query, sqlExecutionContext)) {
            assertFactoryCursor(
                    expected,
                    expectedTimestamp,
                    factory,
                    supportsRandomAccess,
                    sqlExecutionContext,
                    expectSize,
                    false
            );
        }
    }

    protected void assertQueryNoLeakCheck(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertQueryNoLeakCheck(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, false);
        }
    }

    protected void assertQueryNoLeakCheck(SqlCompiler compiler, String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext) throws SqlException {
        assertQueryNoLeakCheck(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, false);
    }

    protected void assertQueryNoLeakCheck(SqlCompiler compiler, String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext, boolean expectSize) throws SqlException {
        assertQueryNoLeakCheck(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, expectSize);
    }

    protected void assertQueryNoLeakCheck(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize, boolean sizeCanBeVariable) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = select(query)) {
            assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess, sqlExecutionContext, expectSize, sizeCanBeVariable);
        }
    }

    protected void assertQueryNoLeakCheck(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertQueryFullFatNoLeakCheck(expected, query, expectedTimestamp, supportsRandomAccess, expectSize, false);
    }

    protected void assertQueryNoLeakCheck(String expected, String query, String expectedTimestamp) throws SqlException {
        assertQueryNoLeakCheck(expected, query, expectedTimestamp, false);
    }

    protected void assertQueryNoLeakCheck(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertQueryNoLeakCheck(compiler, expected, query, expectedTimestamp, supportsRandomAccess, sqlExecutionContext);
        }
    }

    protected void assertQueryNoLeakCheck(String expected, String query) throws SqlException {
        snapshotMemoryUsage();
        try (RecordCursorFactory factory = select(query)) {
            assertFactoryCursor(
                    expected,
                    null,
                    factory,
                    true,
                    sqlExecutionContext,
                    true,
                    false
            );
        }
    }

    protected void assertQueryNoLeakCheckWithFatJoin(String sql, String expected, String ts, boolean fullFatJoins, boolean randomAccess, boolean expectedSize) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            assertQueryNoLeakCheck(expected, sql, ts, randomAccess, expectedSize);
        }
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

    protected void assertSegmentLockEngagement(boolean expectLocked, String tableName, int walId, int segmentId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertSegmentLockEngagement(expectLocked, tableToken, walId, segmentId);
    }

    protected void assertSegmentLockEngagement(boolean expectLocked, TableToken tableToken, int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId).slash().put(segmentId).put(".lock").$();
            final boolean could = couldObtainLock(path);
            Assert.assertEquals(Utf8s.toString(path), expectLocked, !could);
        }
    }

    protected void assertSegmentLockExistence(boolean expectExists, String tableName, @SuppressWarnings("SameParameterValue") int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(engine.verifyTableName(tableName)).concat("wal").put(walId).slash().put(segmentId).put(".lock");
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
        }
    }

    protected void assertSql(CharSequence expected, CharSequence sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSql(compiler, sqlExecutionContext, sql, sink, expected);
        }
    }

    protected void assertSql(CharSequence sql, ObjList<BindVariableTestTuple> tuples) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            int iterCount = tuples.size();
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < iterCount; i++) {

                    final BindVariableTestTuple tuple = tuples.getQuick(i);
                    final BindVariableTestSetter setter = tuple.getSetter();
                    final int errorPosition = tuple.getErrorPosition();
                    final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();

                    setter.assignBindVariables(sqlExecutionContext.getBindVariableService());

                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        if (errorPosition != BindVariableTestTuple.MUST_SUCCEED) {
                            Assert.fail(tuple.getDescription());
                        }
                        RecordMetadata metadata = factory.getMetadata();
                        sink.clear();
                        CursorPrinter.println(metadata, sink);

                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            TestUtils.println(record, metadata, sink);
                        }
                    } catch (SqlException e) {
                        if (errorPosition != BindVariableTestTuple.MUST_SUCCEED) {
                            Assert.assertEquals(tuple.getDescription(), errorPosition, e.getPosition());
                            TestUtils.assertContains(tuple.getDescription(), tuple.getExpected(), e.getFlyweightMessage());
                        } else {
                            System.out.println(tuple.getDescription());
                            throw e;
                        }
                    }
                    if (errorPosition == BindVariableTestTuple.MUST_SUCCEED) {
                        TestUtils.assertEquals(tuple.getDescription(), tuple.getExpected(), sink);
                    }
                }
            }
        }
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

    protected void assertWalLockEngagement(boolean expectLocked, String tableName, @SuppressWarnings("SameParameterValue") int walId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertWalLockEngagement(expectLocked, tableToken, walId);
    }

    protected void assertWalLockEngagement(boolean expectLocked, TableToken tableToken, int walId) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId).put(".lock").$();
            final boolean could = couldObtainLock(path);
            Assert.assertEquals(Utf8s.toString(path), expectLocked, !could);
        }
    }

    protected void assertWalLockExistence(boolean expectExists, String tableName, @SuppressWarnings("SameParameterValue") int walId) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(root).concat(tableToken).concat("wal").put(walId).put(".lock");
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
        }
    }

    protected void configureForBackups() throws IOException {
        String backupDir = temp.newFolder().getAbsolutePath();
        node1.setProperty(PropertyKey.CAIRO_SQL_BACKUP_ROOT, backupDir);
        node1.setProperty(PropertyKey.CAIRO_SQL_BACKUP_DIR_DATETIME_FORMAT, "ddMMMyyyy");
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
        return (factoryType, thread, tableToken, event, segment, position) -> {
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

    protected ExplainPlanFactory getPlanFactory(CharSequence query) throws SqlException {
        return (ExplainPlanFactory) select(query);
    }

    protected ExplainPlanFactory getPlanFactory(SqlCompiler compiler, CharSequence query, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return (ExplainPlanFactory) compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
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
