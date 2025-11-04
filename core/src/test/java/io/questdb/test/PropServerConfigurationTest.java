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

import io.questdb.BuildInformation;
import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfigurationException;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.SelectFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

@SuppressWarnings("ClassEscapesDefinedScope")
public class PropServerConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(PropServerConfigurationTest.class);
    protected static final Rnd rnd = new Rnd();
    protected static final StringSink sink = new StringSink();
    protected static String root;

    @AfterClass
    public static void afterClass() {
        TestUtils.removeTestPath(root);
    }

    @BeforeClass
    public static void setupMimeTypes() throws IOException {
        File root = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(root.getAbsolutePath());
        PropServerConfigurationTest.root = root.getAbsolutePath();
    }

    @Test
    public void testAllDefaults() throws Exception {
        Properties properties = new Properties();
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        FilesFacade ff = configuration.getCairoConfiguration().getFilesFacade();

        Assert.assertFalse(configuration.getCairoConfiguration().getLogLevelVerbose());
        Assert.assertEquals("Z", configuration.getCairoConfiguration().getLogTimestampTimezone());
        Assert.assertEquals(500, configuration.getHttpServerConfiguration().getAcceptLoopTimeout());
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());
        Assert.assertEquals(128, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
        Assert.assertEquals(512, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartHeaderBufferSize());
        Assert.assertEquals(10_000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
        Assert.assertEquals(64448, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRequestHeaderBufferSize());
        Assert.assertEquals(1_800_000_000L, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout());
        Assert.assertFalse(configuration.getHttpServerConfiguration().haltOnError());
        Assert.assertEquals(-1, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getJsonQueryConnectionLimit());
        Assert.assertEquals(-1, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getIlpConnectionLimit());
        Assert.assertEquals(SecurityContext.AUTH_TYPE_NONE, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getRequiredAuthType());
        Assert.assertTrue(configuration.getHttpServerConfiguration().isEnabled());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getDumpNetworkTraffic());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().allowDeflateBeforeSend());
        Assert.assertTrue(configuration.getHttpServerConfiguration().isQueryCacheEnabled());
        Assert.assertFalse(configuration.getHttpServerConfiguration().isSettingsReadOnly());
        Assert.assertEquals(32, configuration.getHttpServerConfiguration().getConcurrentCacheConfiguration().getBlocks());
        Assert.assertEquals(Math.max(configuration.getSharedWorkerPoolNetworkConfiguration().getWorkerCount(), 4), configuration.getHttpServerConfiguration().getConcurrentCacheConfiguration().getRows());

        Assert.assertEquals(10, configuration.getSharedWorkerPoolNetworkConfiguration().getYieldThreshold());
        Assert.assertEquals(10000, configuration.getSharedWorkerPoolNetworkConfiguration().getSleepThreshold());
        Assert.assertEquals(7000, configuration.getSharedWorkerPoolNetworkConfiguration().getNapThreshold());
        Assert.assertEquals(10, configuration.getSharedWorkerPoolNetworkConfiguration().getSleepTimeout());

        Assert.assertEquals(10, configuration.getHttpMinServerConfiguration().getYieldThreshold());
        Assert.assertEquals(100, configuration.getHttpMinServerConfiguration().getNapThreshold());
        Assert.assertEquals(100, configuration.getHttpMinServerConfiguration().getSleepThreshold());
        Assert.assertEquals(50, configuration.getHttpMinServerConfiguration().getSleepTimeout());
        Assert.assertEquals(1, configuration.getHttpMinServerConfiguration().getWorkerCount());
        Assert.assertEquals(500, configuration.getHttpMinServerConfiguration().getAcceptLoopTimeout());

        // this is going to need interesting validation logic
        // configuration path is expected to be relative, and we need to check if absolute path is good
        Assert.assertEquals(
                new File(root, "public").getAbsolutePath(),
                configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory()
        );

        Assert.assertEquals("Keep-Alive: timeout=5, max=10000" + Misc.EOL, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());

        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getLimit());
        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getEventCapacity());
        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getIOQueueCapacity());
        Assert.assertEquals(300000, configuration.getHttpServerConfiguration().getTimeout());
        Assert.assertEquals(5000, configuration.getHttpServerConfiguration().getQueueTimeout());
        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getInterestQueueCapacity());
        Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getInitialBias());
        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getListenBacklog());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getSendBufferSize());
        Assert.assertEquals(-1, configuration.getHttpServerConfiguration().getNetSendBufferSize());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getRecvBufferSize());
        Assert.assertEquals(-1, configuration.getHttpServerConfiguration().getNetRecvBufferSize());
        Assert.assertEquals(10, configuration.getHttpServerConfiguration().getSleepTimeout());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getTextConfiguration().getDateAdapterPoolCapacity());
        Assert.assertEquals(16384, configuration.getCairoConfiguration().getTextConfiguration().getJsonCacheLimit());
        Assert.assertEquals(8192, configuration.getCairoConfiguration().getTextConfiguration().getJsonCacheSize());
        Assert.assertEquals(0.1222d, configuration.getCairoConfiguration().getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000000001);
        Assert.assertEquals(0.8, configuration.getCairoConfiguration().getTextConfiguration().getMaxRequiredLineLengthStdDev(), 0.000000001);
        Assert.assertEquals(128, configuration.getCairoConfiguration().getTextConfiguration().getMetadataStringPoolCapacity());
        Assert.assertEquals(1024 * 4096, configuration.getCairoConfiguration().getTextConfiguration().getRollBufferLimit());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getTextConfiguration().getRollBufferSize());
        Assert.assertEquals(1000, configuration.getCairoConfiguration().getTextConfiguration().getTextAnalysisMaxLines());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getTextConfiguration().getTextLexerStringPoolCapacity());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getTextConfiguration().getTimestampAdapterPoolCapacity());
        Assert.assertEquals(4096, configuration.getCairoConfiguration().getTextConfiguration().getUtf8SinkSize());
        Assert.assertEquals(2, configuration.getCairoConfiguration().getScoreboardFormat());
        Assert.assertEquals(0, configuration.getHttpServerConfiguration().getBindIPv4Address());
        Assert.assertEquals(9000, configuration.getHttpServerConfiguration().getBindPort());

        Assert.assertEquals(1_000_000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getConnectionCheckFrequency());
        Assert.assertEquals("Keep-Alive: timeout=5, max=10000" + Misc.EOL, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getKeepAliveHeader());

        Assert.assertFalse(configuration.getHttpServerConfiguration().isPessimisticHealthCheckEnabled());
        Assert.assertEquals(SecurityContext.AUTH_TYPE_CREDENTIALS, configuration.getHttpServerConfiguration().getRequiredAuthType());
        Assert.assertFalse(configuration.getHttpMinServerConfiguration().isPessimisticHealthCheckEnabled());
        Assert.assertEquals(SecurityContext.AUTH_TYPE_CREDENTIALS, configuration.getHttpMinServerConfiguration().getRequiredAuthType());

        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
        Assert.assertEquals(Long.MAX_VALUE, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getMaxQueryResponseRowLimit());
        Assert.assertTrue(configuration.getCairoConfiguration().getCircuitBreakerConfiguration().isEnabled());
        Assert.assertEquals(2_000_000, configuration.getCairoConfiguration().getCircuitBreakerConfiguration().getCircuitBreakerThrottle());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getCircuitBreakerConfiguration().getBufferSize());

        Assert.assertTrue(configuration.getCairoConfiguration().getLogSqlQueryProgressExe());

        Assert.assertEquals(CommitMode.NOSYNC, configuration.getCairoConfiguration().getCommitMode());
        Assert.assertEquals(2097152, configuration.getCairoConfiguration().getSqlCopyBufferSize());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getCopyPoolCapacity());
        Assert.assertEquals(5, configuration.getCairoConfiguration().getCreateAsSelectRetryCount());
        Assert.assertTrue(configuration.getCairoConfiguration().isMatViewEnabled());
        Assert.assertEquals(10, configuration.getCairoConfiguration().getMatViewMaxRefreshRetries());
        Assert.assertEquals(200, configuration.getCairoConfiguration().getMatViewRefreshOomRetryTimeout());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getMatViewInsertAsSelectBatchSize());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getMatViewRowsPerQueryEstimate());
        Assert.assertTrue(configuration.getCairoConfiguration().isMatViewParallelSqlEnabled());
        Assert.assertEquals(100, configuration.getCairoConfiguration().getMatViewMaxRefreshIntervals());
        Assert.assertEquals(Micros.YEAR_MICROS_NONLEAP, configuration.getCairoConfiguration().getMatViewMaxRefreshStepUs());
        Assert.assertEquals(15_000, configuration.getCairoConfiguration().getMatViewRefreshIntervalsUpdatePeriod());
        Assert.assertTrue(configuration.getCairoConfiguration().getDefaultSymbolCacheFlag());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getDefaultSymbolCapacity());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getFileOperationRetryCount());
        Assert.assertEquals(300000, configuration.getCairoConfiguration().getIdleCheckInterval());
        Assert.assertEquals(10000, configuration.getCairoConfiguration().getInactiveReaderMaxOpenPartitions());
        Assert.assertEquals(120_000, configuration.getCairoConfiguration().getInactiveReaderTTL());
        Assert.assertEquals(600_000, configuration.getCairoConfiguration().getInactiveWriterTTL());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getIndexValueBlockSize());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getMaxSwapFileCount());
        Assert.assertEquals(509, configuration.getCairoConfiguration().getMkDirMode());
        Assert.assertEquals(509, configuration.getCairoConfiguration().getDetachedMkDirMode());
        Assert.assertEquals(8, configuration.getCairoConfiguration().getBindVariablePoolSize());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getQueryRegistryPoolSize());
        Assert.assertEquals(3, configuration.getCairoConfiguration().getCountDistinctCapacity());
        Assert.assertEquals(0.5, configuration.getCairoConfiguration().getCountDistinctLoadFactor(), 0.000001);

        Assert.assertEquals(100000, configuration.getCairoConfiguration().getParallelIndexThreshold());
        Assert.assertEquals(10, configuration.getCairoConfiguration().getReaderPoolMaxSegments());
        Assert.assertEquals(1_000, configuration.getCairoConfiguration().getSpinLockTimeout());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlCharacterStoreCapacity());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlCharacterStoreSequencePoolCapacity());
        Assert.assertEquals(4096, configuration.getCairoConfiguration().getSqlColumnPoolCapacity());
        Assert.assertEquals(8192, configuration.getCairoConfiguration().getSqlExpressionPoolCapacity());
        Assert.assertEquals(0.7, configuration.getCairoConfiguration().getSqlFastMapLoadFactor(), 0.0000001);
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlJoinContextPoolCapacity());
        Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlLexerPoolCapacity());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getSqlSmallMapKeyCapacity());
        Assert.assertEquals(32 * 1024, configuration.getCairoConfiguration().getSqlSmallMapPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlMapMaxPages());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlMapMaxResizes());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getSqlUnorderedMapMaxEntrySize());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlModelPoolCapacity());
        Assert.assertEquals(10_000, configuration.getCairoConfiguration().getSqlMaxNegativeLimit());
        Assert.assertEquals(128 * 1024, configuration.getCairoConfiguration().getSqlSortKeyPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlSortKeyMaxPages());
        Assert.assertEquals(128 * 1024, configuration.getCairoConfiguration().getSqlSortLightValuePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlSortLightValueMaxPages());
        Assert.assertEquals(16 * 1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinValuePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlHashJoinValueMaxPages());
        Assert.assertEquals(1000, configuration.getCairoConfiguration().getSqlLatestByRowCount());
        Assert.assertEquals(128 * 1024, configuration.getCairoConfiguration().getSqlHashJoinLightValuePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlHashJoinLightValueMaxPages());
        Assert.assertEquals(100, configuration.getCairoConfiguration().getSqlAsOfJoinLookAhead());
        Assert.assertEquals(10_000_000, configuration.getCairoConfiguration().getSqlAsOfJoinMapEvacuationThreshold());
        Assert.assertEquals(10_000_000, configuration.getCairoConfiguration().getSqlAsOfJoinShortCircuitCacheCapacity());
        Assert.assertEquals(16 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortValuePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlSortValueMaxPages());
        Assert.assertEquals(10000, configuration.getCairoConfiguration().getWorkStealTimeoutNanos());
        Assert.assertTrue(configuration.getCairoConfiguration().isParallelIndexingEnabled());
        Assert.assertEquals(16 * 1024, configuration.getCairoConfiguration().getSqlJoinMetadataPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlJoinMetadataMaxResizes());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getWindowColumnPoolCapacity());
        Assert.assertEquals(128, configuration.getCairoConfiguration().getSqlWindowMaxRecursion());
        Assert.assertEquals(512 * 1024, configuration.getCairoConfiguration().getSqlWindowTreeKeyPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlWindowTreeKeyMaxPages());
        Assert.assertEquals(1024 * 1024, configuration.getCairoConfiguration().getSqlWindowStorePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlWindowStoreMaxPages());
        Assert.assertEquals(512 * 1024, configuration.getCairoConfiguration().getSqlWindowRowIdPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlWindowRowIdMaxPages());
        Assert.assertEquals(128, configuration.getCairoConfiguration().getWithClauseModelPoolCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getRenameTableModelPoolCapacity());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getInsertModelPoolCapacity());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getInsertModelBatchSize());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getCreateTableColumnModelPoolCapacity());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getCreateTableModelBatchSize());
        Assert.assertEquals(1, configuration.getCairoConfiguration().getPartitionPurgeListCapacity());
        Assert.assertEquals(ff.allowMixedIO(root), configuration.getCairoConfiguration().isWriterMixedIOEnabled());
        Assert.assertEquals(CairoConfiguration.O_NONE, configuration.getCairoConfiguration().getWriterFileOpenOpts());
        Assert.assertTrue(configuration.getCairoConfiguration().isIOURingEnabled());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getPreferencesStringPoolCapacity());

        // cannot assert for exact number as it is platform dependant
        Assert.assertTrue(configuration.getCairoConfiguration().getSqlCompilerPoolCapacity() > 0);

        Assert.assertEquals(0, configuration.getLineUdpReceiverConfiguration().getBindIPv4Address());
        Assert.assertEquals(9009, configuration.getLineUdpReceiverConfiguration().getPort());
        Assert.assertEquals(-402587133, configuration.getLineUdpReceiverConfiguration().getGroupIPv4Address());
        Assert.assertEquals(1000000, configuration.getLineUdpReceiverConfiguration().getCommitRate());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(2048, configuration.getLineUdpReceiverConfiguration().getMsgBufferSize());
        Assert.assertEquals(10000, configuration.getLineUdpReceiverConfiguration().getMsgCount());
        Assert.assertEquals(8388608, configuration.getLineUdpReceiverConfiguration().getReceiveBufferSize());
        Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().isEnabled());
        Assert.assertEquals(-1, configuration.getLineUdpReceiverConfiguration().ownThreadAffinity());
        Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().ownThread());

        Assert.assertEquals(0.05, configuration.getCairoConfiguration().getSqlParallelFilterPreTouchThreshold(), 0.000001);
        Assert.assertTrue(configuration.getCairoConfiguration().isSqlParallelGroupByEnabled());
        Assert.assertTrue(configuration.getCairoConfiguration().isSqlParallelReadParquetEnabled());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getSqlParallelWorkStealingThreshold());
        Assert.assertEquals(3, configuration.getCairoConfiguration().getSqlParquetFrameCacheCapacity());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getSqlPageFrameMaxRows());
        Assert.assertEquals(100_000, configuration.getCairoConfiguration().getSqlPageFrameMinRows());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getPageFrameReduceRowIdListCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getPageFrameReduceColumnListCapacity());
        Assert.assertEquals(100_000, configuration.getCairoConfiguration().getGroupByShardingThreshold());
        Assert.assertTrue(configuration.getCairoConfiguration().isGroupByPresizeEnabled());
        Assert.assertEquals(100_000_000, configuration.getCairoConfiguration().getGroupByPresizeMaxCapacity());
        Assert.assertEquals(Numbers.SIZE_1GB, configuration.getCairoConfiguration().getGroupByPresizeMaxHeapSize());
        Assert.assertEquals(128 * 1024, configuration.getCairoConfiguration().getGroupByAllocatorDefaultChunkSize());
        Assert.assertTrue(configuration.getCairoConfiguration().isSqlOrderBySortEnabled());
        Assert.assertEquals(600, configuration.getCairoConfiguration().getSqlOrderByRadixSortThreshold());

        Assert.assertEquals(SqlJitMode.JIT_MODE_ENABLED, configuration.getCairoConfiguration().getSqlJitMode());
        Assert.assertEquals(8192, configuration.getCairoConfiguration().getSqlJitIRMemoryPageSize());
        Assert.assertEquals(8, configuration.getCairoConfiguration().getSqlJitIRMemoryMaxPages());
        Assert.assertEquals(4096, configuration.getCairoConfiguration().getSqlJitBindVarsMemoryPageSize());
        Assert.assertEquals(8, configuration.getCairoConfiguration().getSqlJitBindVarsMemoryMaxPages());
        Assert.assertEquals(1024 * 1024, configuration.getCairoConfiguration().getSqlJitPageAddressCacheThreshold());
        Assert.assertFalse(configuration.getCairoConfiguration().isSqlJitDebugEnabled());

        Assert.assertEquals(8192, configuration.getCairoConfiguration().getRndFunctionMemoryPageSize());
        Assert.assertEquals(128, configuration.getCairoConfiguration().getRndFunctionMemoryMaxPages());

        // statics
        Assert.assertSame(FilesFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getFilesFacade());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getClock());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMillisecondClock());
        Assert.assertSame(NanosecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getNanosecondClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getNetworkFacade());
        Assert.assertSame(EpollFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getEpollFacade());
        Assert.assertSame(SelectFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getSelectFacade());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getTestConnectionBufferSize());
        Assert.assertTrue(FilesFacadeImpl.class.isAssignableFrom(configuration.getCairoConfiguration().getFilesFacade().getClass()));
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMillisecondClock());
        Assert.assertSame(MicrosecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMicrosecondClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getLineUdpReceiverConfiguration().getNetworkFacade());
        Assert.assertEquals("http-server", configuration.getHttpServerConfiguration().getDispatcherLogName());

        TestUtils.assertEquals(new File(root, "db").getAbsolutePath(), configuration.getCairoConfiguration().getDbRoot());
        TestUtils.assertEquals(new File(root, "conf").getAbsolutePath(), configuration.getCairoConfiguration().getConfRoot());
        TestUtils.assertEquals(new File(root, TableUtils.LEGACY_CHECKPOINT_DIRECTORY).getAbsolutePath(), configuration.getCairoConfiguration().getLegacyCheckpointRoot());
        TestUtils.assertEquals(new File(root, TableUtils.CHECKPOINT_DIRECTORY).getAbsolutePath(), configuration.getCairoConfiguration().getCheckpointRoot());

        Assert.assertEquals("", configuration.getCairoConfiguration().getSnapshotInstanceId());
        Assert.assertTrue(configuration.getCairoConfiguration().isCheckpointRecoveryEnabled());

        // assert mime types
        TestUtils.assertEquals("application/json", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getMimeTypesCache().get(new Utf8String("json")));

        Assert.assertEquals(500_000, configuration.getCairoConfiguration().getMaxUncommittedRows());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getO3MinLag());
        Assert.assertEquals(600_000_000, configuration.getCairoConfiguration().getO3MaxLag());
        Assert.assertEquals(8388608, configuration.getCairoConfiguration().getO3ColumnMemorySize());
        Assert.assertEquals(262144, configuration.getCairoConfiguration().getSystemO3ColumnMemorySize());

        // influxdb line TCP protocol
        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().isEnabled());
        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().logMessageOnError());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getLimit());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getBindIPv4Address());
        Assert.assertEquals(9009, configuration.getLineTcpReceiverConfiguration().getBindPort());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getEventCapacity());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getIOQueueCapacity());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getTimeout());
        Assert.assertEquals(5000, configuration.getLineTcpReceiverConfiguration().getQueueTimeout());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getInterestQueueCapacity());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getListenBacklog());
        Assert.assertEquals(64, configuration.getLineTcpReceiverConfiguration().getTestConnectionBufferSize());
        Assert.assertEquals(8, configuration.getLineTcpReceiverConfiguration().getConnectionPoolInitialCapacity());
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_NANOS, configuration.getLineTcpReceiverConfiguration().getTimestampUnit());
        Assert.assertEquals(131072, configuration.getLineTcpReceiverConfiguration().getRecvBufferSize());
        Assert.assertEquals(-1, configuration.getLineTcpReceiverConfiguration().getNetRecvBufferSize());
        Assert.assertEquals(-1, configuration.getLineTcpReceiverConfiguration().getSendBufferSize());
        Assert.assertEquals(-1, configuration.getLineTcpReceiverConfiguration().getNetSendBufferSize());
        Assert.assertEquals(500, configuration.getLineTcpReceiverConfiguration().getAcceptLoopTimeout());
        Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(128, configuration.getLineTcpReceiverConfiguration().getWriterQueueCapacity());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerCount());
        Assert.assertEquals(10, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(7_000, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getNapThreshold());
        Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getSleepThreshold());
        Assert.assertArrayEquals(new int[]{}, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerAffinity());
        Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().haltOnError());
        Assert.assertEquals(10, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(7_000, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getNapThreshold());
        Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getSleepThreshold());
        Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().haltOnError());
        Assert.assertEquals(1000, configuration.getLineTcpReceiverConfiguration().getMaintenanceInterval());
        Assert.assertEquals(PropServerConfiguration.COMMIT_INTERVAL_DEFAULT, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(500, configuration.getLineTcpReceiverConfiguration().getWriterIdleTimeout());
        Assert.assertEquals(0, configuration.getCairoConfiguration().getSampleByIndexSearchPageSize());
        Assert.assertTrue(configuration.getCairoConfiguration().getSampleByDefaultAlignmentCalendar());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getWriterCommandQueueCapacity());
        Assert.assertEquals(2048, configuration.getCairoConfiguration().getWriterCommandQueueSlotSize());
        Assert.assertEquals(500, configuration.getCairoConfiguration().getWriterAsyncCommandBusyWaitTimeout());
        Assert.assertEquals(30_000, configuration.getCairoConfiguration().getWriterAsyncCommandMaxTimeout());
        Assert.assertEquals(1023, configuration.getCairoConfiguration().getWriterTickRowsCountMod());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());
        Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().isUseLegacyStringDefault());
        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getDisconnectOnError());

        Assert.assertTrue(configuration.getHttpServerConfiguration().getLineHttpProcessorConfiguration().logMessageOnError());

        Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getServerKeepAlive());
        Assert.assertEquals("HTTP/1.1 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());

        Assert.assertEquals("[DEVELOPMENT]", configuration.getCairoConfiguration().getBuildInformation().getSwVersion());
        Assert.assertEquals("unknown", configuration.getCairoConfiguration().getBuildInformation().getJdkVersion());
        Assert.assertEquals("unknown", configuration.getCairoConfiguration().getBuildInformation().getCommitHash());

        Assert.assertFalse(configuration.getMetricsConfiguration().isEnabled());
        Assert.assertFalse(configuration.getCairoConfiguration().isQueryTracingEnabled());

        Assert.assertEquals(4, configuration.getCairoConfiguration().getQueryCacheEventQueueCapacity());
        Assert.assertEquals(16777216, configuration.getCairoConfiguration().getDataAppendPageSize());
        Assert.assertEquals(262144, configuration.getCairoConfiguration().getSystemDataAppendPageSize());
        Assert.assertEquals(524288, configuration.getCairoConfiguration().getDataIndexKeyAppendPageSize());
        Assert.assertEquals(16777216, configuration.getCairoConfiguration().getDataIndexValueAppendPageSize());
        Assert.assertEquals(Files.PAGE_SIZE, configuration.getCairoConfiguration().getMiscAppendPageSize());
        Assert.assertEquals(Files.PAGE_SIZE, configuration.getCairoConfiguration().getSymbolTableMinAllocationPageSize());
        Assert.assertEquals(2.0, configuration.getHttpServerConfiguration().getWaitProcessorConfiguration().getExponentialWaitMultiplier(), 0.00001);

        Assert.assertEquals(128, configuration.getCairoConfiguration().getColumnPurgeQueueCapacity());
        Assert.assertEquals(10.0, configuration.getCairoConfiguration().getColumnPurgeRetryDelayMultiplier(), 0.00001);
        Assert.assertEquals(60000000, configuration.getCairoConfiguration().getColumnPurgeRetryDelayLimit());
        Assert.assertEquals(10000, configuration.getCairoConfiguration().getColumnPurgeRetryDelay());

        // PG wire
        Assert.assertEquals(64, configuration.getPGWireConfiguration().getLimit());
        Assert.assertEquals(500, configuration.getPGWireConfiguration().getAcceptLoopTimeout());
        Assert.assertEquals(64, configuration.getPGWireConfiguration().getTestConnectionBufferSize());
        Assert.assertEquals(2, configuration.getPGWireConfiguration().getBinParamCountCapacity());
        Assert.assertTrue(configuration.getPGWireConfiguration().isSelectCacheEnabled());
        Assert.assertEquals(32, configuration.getPGWireConfiguration().getConcurrentCacheConfiguration().getBlocks());
        Assert.assertEquals(Math.max(configuration.getSharedWorkerPoolNetworkConfiguration().getWorkerCount(), 4), configuration.getPGWireConfiguration().getConcurrentCacheConfiguration().getRows());
        Assert.assertTrue(configuration.getPGWireConfiguration().isInsertCacheEnabled());
        Assert.assertEquals(4, configuration.getPGWireConfiguration().getInsertCacheBlockCount());
        Assert.assertEquals(4, configuration.getPGWireConfiguration().getInsertCacheRowCount());
        Assert.assertTrue(configuration.getPGWireConfiguration().isUpdateCacheEnabled());
        Assert.assertEquals(4, configuration.getPGWireConfiguration().getUpdateCacheBlockCount());
        Assert.assertEquals(4, configuration.getPGWireConfiguration().getUpdateCacheRowCount());
        Assert.assertFalse(configuration.getPGWireConfiguration().readOnlySecurityContext());
        Assert.assertEquals("quest", configuration.getPGWireConfiguration().getDefaultPassword());
        Assert.assertEquals("admin", configuration.getPGWireConfiguration().getDefaultUsername());
        Assert.assertFalse(configuration.getPGWireConfiguration().isReadOnlyUserEnabled());
        Assert.assertEquals("quest", configuration.getPGWireConfiguration().getReadOnlyPassword());
        Assert.assertEquals("user", configuration.getPGWireConfiguration().getReadOnlyUsername());
        Assert.assertEquals(10_000, configuration.getPGWireConfiguration().getNamedStatementLimit());

        Assert.assertEquals(128, configuration.getCairoConfiguration().getColumnPurgeQueueCapacity());
        Assert.assertEquals(127, configuration.getCairoConfiguration().getMaxFileNameLength());
        Assert.assertEquals(127, configuration.getLineTcpReceiverConfiguration().getMaxFileNameLength());
        Assert.assertEquals(127, configuration.getLineUdpReceiverConfiguration().getMaxFileNameLength());

        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getAutoCreateNewColumns());
        Assert.assertTrue(configuration.getLineUdpReceiverConfiguration().getAutoCreateNewColumns());
        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getAutoCreateNewTables());
        Assert.assertTrue(configuration.getLineUdpReceiverConfiguration().getAutoCreateNewTables());

        Assert.assertEquals(TableUtils.ATTACHABLE_DIR_MARKER, configuration.getCairoConfiguration().getAttachPartitionSuffix());
        Assert.assertFalse(configuration.getCairoConfiguration().attachPartitionCopy());

        Assert.assertEquals(30_000, configuration.getCairoConfiguration().getWalPurgeInterval());
        Assert.assertEquals(3, configuration.getCairoConfiguration().getWalRecreateDistressedSequencerAttempts());
        Assert.assertEquals(120_000, configuration.getCairoConfiguration().getInactiveWalWriterTTL());
        Assert.assertEquals(4096, configuration.getCairoConfiguration().getWalTxnNotificationQueueCapacity());
        Assert.assertTrue(configuration.getCairoConfiguration().isWalSupported());
        Assert.assertTrue(configuration.getCairoConfiguration().getWalEnabledDefault());
        Assert.assertTrue(configuration.getCairoConfiguration().isWalApplyEnabled());
        Assert.assertTrue(configuration.getWalApplyPoolConfiguration().isEnabled());
        Assert.assertFalse(configuration.getWalApplyPoolConfiguration().haltOnError());
        Assert.assertEquals("wal-apply", configuration.getWalApplyPoolConfiguration().getPoolName());
        Assert.assertTrue(configuration.getWalApplyPoolConfiguration().getWorkerCount() >= 2);
        Assert.assertTrue(configuration.getWalApplyPoolConfiguration().getWorkerCount() <= 4);
        Assert.assertEquals(10, configuration.getWalApplyPoolConfiguration().getSleepTimeout());
        Assert.assertEquals(7_000, configuration.getWalApplyPoolConfiguration().getNapThreshold());
        Assert.assertEquals(10_000, configuration.getWalApplyPoolConfiguration().getSleepThreshold());
        Assert.assertEquals(1000, configuration.getWalApplyPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(200, configuration.getCairoConfiguration().getWalApplyLookAheadTransactionCount());
        Assert.assertEquals(4, configuration.getCairoConfiguration().getO3LagCalculationWindowsSize());
        Assert.assertEquals(200_000, configuration.getCairoConfiguration().getWalSegmentRolloverRowCount());
        Assert.assertEquals(20.0d, configuration.getCairoConfiguration().getWalLagRowsMultiplier(), 0.00001);
        Assert.assertEquals(-1, configuration.getCairoConfiguration().getWalMaxLagTxnCount());
        Assert.assertEquals(1048576, configuration.getCairoConfiguration().getWalDataAppendPageSize());
        Assert.assertEquals(262144, configuration.getCairoConfiguration().getSystemWalDataAppendPageSize());
        Assert.assertTrue(configuration.getCairoConfiguration().isTableTypeConversionEnabled());
        Assert.assertEquals(10, configuration.getCairoConfiguration().getWalWriterPoolMaxSegments());
        Assert.assertTrue(configuration.getCairoConfiguration().isWalApplyParallelSqlEnabled());

        Assert.assertEquals(20, configuration.getCairoConfiguration().getO3LastPartitionMaxSplits());
        Assert.assertEquals(50 * Numbers.SIZE_1MB, configuration.getCairoConfiguration().getPartitionO3SplitMinSize());
        Assert.assertFalse(configuration.getCairoConfiguration().getTextConfiguration().isUseLegacyStringDefault());

        Assert.assertTrue(configuration.getMatViewRefreshPoolConfiguration().isEnabled());
        Assert.assertTrue(configuration.getMatViewRefreshPoolConfiguration().getWorkerCount() > 0);
        Assert.assertFalse(configuration.getMatViewRefreshPoolConfiguration().haltOnError());
        Assert.assertEquals("mat-view-refresh", configuration.getMatViewRefreshPoolConfiguration().getPoolName());
        Assert.assertEquals(10, configuration.getMatViewRefreshPoolConfiguration().getSleepTimeout());
        Assert.assertEquals(7_000, configuration.getMatViewRefreshPoolConfiguration().getNapThreshold());
        Assert.assertEquals(10_000, configuration.getMatViewRefreshPoolConfiguration().getSleepThreshold());
        Assert.assertEquals(1000, configuration.getMatViewRefreshPoolConfiguration().getYieldThreshold());

        Assert.assertTrue(configuration.getExportPoolConfiguration().isEnabled());
        Assert.assertTrue(configuration.getExportPoolConfiguration().getWorkerCount() > 0);
        Assert.assertFalse(configuration.getExportPoolConfiguration().haltOnError());
        Assert.assertEquals("export", configuration.getExportPoolConfiguration().getPoolName());
        Assert.assertEquals(10, configuration.getExportPoolConfiguration().getSleepTimeout());
        Assert.assertEquals(7_000, configuration.getExportPoolConfiguration().getNapThreshold());
        Assert.assertEquals(10_000, configuration.getExportPoolConfiguration().getSleepThreshold());
        Assert.assertEquals(1000, configuration.getExportPoolConfiguration().getYieldThreshold());

        Assert.assertFalse(configuration.getCairoConfiguration().useWithinLatestByOptimisation());
    }

    @Test
    public void testCommitIntervalDefault() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("line.tcp.commit.interval.default", "0");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PropServerConfiguration.COMMIT_INTERVAL_DEFAULT, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());

        properties.setProperty("line.tcp.commit.interval.default", "-1");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PropServerConfiguration.COMMIT_INTERVAL_DEFAULT, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());

        properties.setProperty("line.tcp.commit.interval.default", "1000");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(1000, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());
    }

    @Test
    public void testContextPath() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getPropertyPath(), "/context");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals("/context", configuration.getHttpServerConfiguration().getContextPathWebConsole());

        properties.setProperty(PropertyKey.HTTP_CONTEXT_ILP.getPropertyPath(), "/ilp/write");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(1, configuration.getHttpServerConfiguration().getContextPathILP().size());
        Assert.assertEquals("/ilp/write", configuration.getHttpServerConfiguration().getContextPathILP().get(0));

        properties.setProperty(PropertyKey.HTTP_CONTEXT_ILP.getPropertyPath(), "/ilp/write,/write,/ilp");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(3, configuration.getHttpServerConfiguration().getContextPathILP().size());
        Assert.assertEquals("/ilp/write", configuration.getHttpServerConfiguration().getContextPathILP().get(0));
        Assert.assertEquals("/write", configuration.getHttpServerConfiguration().getContextPathILP().get(1));
        Assert.assertEquals("/ilp", configuration.getHttpServerConfiguration().getContextPathILP().get(2));
    }

    @Test
    public void testDefaultAddColumnTypeForFloat() throws Exception {
        Properties properties = new Properties();

        // default
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // empty
        properties.setProperty("line.float.default.column.type", "");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // double
        properties.setProperty("line.float.default.column.type", "DOUBLE");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // float
        properties.setProperty("line.float.default.column.type", "FLOAT");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.FLOAT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // lowercase
        properties.setProperty("line.float.default.column.type", "double");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // camel case
        properties.setProperty("line.float.default.column.type", "Float");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.FLOAT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // not allowed
        properties.setProperty("line.float.default.column.type", "STRING");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // not allowed
        properties.setProperty("line.float.default.column.type", "SHORT");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // nonexistent type
        properties.setProperty("line.float.default.column.type", "FLAT");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());
    }

    @Test
    public void testDefaultAddColumnTypeForInteger() throws Exception {
        Properties properties = new Properties();

        // default
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // empty
        properties.setProperty("line.integer.default.column.type", "");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // long
        properties.setProperty("line.integer.default.column.type", "LONG");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // int
        properties.setProperty("line.integer.default.column.type", "INT");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.INT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // short
        properties.setProperty("line.integer.default.column.type", "SHORT");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.SHORT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // byte
        properties.setProperty("line.integer.default.column.type", "BYTE");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.BYTE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // lowercase
        properties.setProperty("line.integer.default.column.type", "int");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.INT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // camel case
        properties.setProperty("line.integer.default.column.type", "Short");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.SHORT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // not allowed
        properties.setProperty("line.integer.default.column.type", "SYMBOL");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // not allowed
        properties.setProperty("line.integer.default.column.type", "FLOAT");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // nonexistent type
        properties.setProperty("line.integer.default.column.type", "BITE");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());
    }

    @Test
    public void testDefaultTimestampColumnType() throws Exception {
        Properties properties = new Properties();

        // default
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // empty
        properties.setProperty("line.timestamp.default.column.type", "");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // timestamp
        properties.setProperty("line.timestamp.default.column.type", "TIMESTAMP");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // timestamp_ns
        properties.setProperty("line.timestamp.default.column.type", "TIMESTAMP_NS");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_NANO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // lowercase
        properties.setProperty("line.timestamp.default.column.type", "timestamp");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // camel case
        properties.setProperty("line.timestamp.default.column.type", "Timestamp_Ns");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_NANO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // not allowed
        properties.setProperty("line.timestamp.default.column.type", "STRING");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // not allowed
        properties.setProperty("line.timestamp.default.column.type", "SHORT");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());

        // nonexistent type
        properties.setProperty("line.timestamp.default.column.type", "TIMESTAMP_MS");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, configuration.getLineTcpReceiverConfiguration().getDefaultCreateTimestampColumnType());
    }

    @Test
    public void testDeprecatedConfigKeys() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("config.validation.strict", "true");
        properties.setProperty("http.min.bind.to", "0.0.0.0:0");

        // Using deprecated settings will not throw an exception, despite validation enabled.
        newPropServerConfiguration(properties);
    }

    @Test
    public void testDeprecatedValidationResult() {
        Properties properties = new Properties();
        properties.setProperty("http.net.rcv.buf.size", "10000");
        PropServerConfiguration.ValidationResult result = validate(properties);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.isError);
        Assert.assertNotEquals(-1, result.message.indexOf("Deprecated settings"));
        Assert.assertNotEquals(-1, result.message.indexOf(
                "Replaced by `http.min.net.connection.rcvbuf` and `http.net.connection.rcvbuf`"));
    }

    @Test
    public void testEmptyTimestampTimezone() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("log.timestamp.timezone", "");
        try {
            newPropServerConfiguration(properties);
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertEquals("Invalid log timezone: ''", e.getMessage());
        }
    }

    @Test
    public void testEnvOverrides() throws Exception {
        final Properties properties = new Properties();
        final Map<String, String> env = new HashMap<>();

        // double
        properties.setProperty("http.text.max.required.delimiter.stddev", "1.2");
        env.put("QDB_HTTP_TEXT_MAX_REQUIRED_DELIMITER_STDDEV", "1.5");

        // int
        properties.setProperty("http.connection.string.pool.capacity", "1200");
        env.put("QDB_HTTP_CONNECTION_STRING_POOL_CAPACITY", "3000");

        // string
        properties.setProperty("http.version", "1.0");
        env.put("QDB_HTTP_VERSION", "2.0");

        // affinity
        properties.setProperty("shared.worker.count", "2");
        properties.setProperty("shared.worker.affinity", "2,3");
        env.put("QDB_SHARED_WORKER_COUNT", "3");
        env.put("QDB_SHARED_NETWORK_WORKER_AFFINITY", "5,6,7");

        // int size
        properties.setProperty("http.send.buffer.size", "4k");
        env.put("QDB_HTTP_SEND_BUFFER_SIZE", "12k");

        // long
        properties.setProperty("http.multipart.idle.spin.count", "400");
        env.put("QDB_HTTP_MULTIPART_IDLE_SPIN_COUNT", "900");

        // boolean
        properties.setProperty("http.security.readonly", "true");
        env.put("QDB_HTTP_SECURITY_READONLY", "false");

        // long size
        properties.setProperty("cairo.writer.data.append.page.size", "3G");
        env.put("QDB_CAIRO_WRITER_DATA_APPEND_PAGE_SIZE", "9G");

        properties.setProperty("cairo.o3.max.lag", "60");

        properties.setProperty("cairo.legacy.string.column.type.default", "false");
        env.put("QDB_CAIRO_LEGACY_STRING_COLUMN_TYPE_DEFAULT", "true");

        properties.setProperty("http.json.query.connection.limit", "6");
        env.put("QDB_HTTP_JSON_QUERY_CONNECTION_LIMIT", "12");
        properties.setProperty("http.ilp.connection.limit", "4");
        env.put("QDB_HTTP_ILP_CONNECTION_LIMIT", "8");

        properties.setProperty("http.session.timeout", "30m");
        env.put("QDB_HTTP_SESSION_TIMEOUT", "15m");

        properties.setProperty("telemetry.db.size.estimate.timeout", "2000");
        env.put("QDB_TELEMETRY_DB_SIZE_ESTIMATE_TIMEOUT", "3000");

        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, env, new BuildInformationHolder());
        Assert.assertEquals(1.5, configuration.getCairoConfiguration().getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000001);
        Assert.assertEquals(3000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
        Assert.assertEquals("2.0 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());
        Assert.assertEquals(3, configuration.getSharedWorkerPoolNetworkConfiguration().getWorkerCount());
        Assert.assertArrayEquals(new int[]{5, 6, 7}, configuration.getSharedWorkerPoolNetworkConfiguration().getWorkerAffinity());
        Assert.assertEquals(12288, configuration.getHttpServerConfiguration().getSendBufferSize());
        Assert.assertEquals(12, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getJsonQueryConnectionLimit());
        Assert.assertEquals(8, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getIlpConnectionLimit());
        Assert.assertEquals(900, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
        Assert.assertEquals(900_000_000L, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
        Assert.assertEquals(9663676416L, configuration.getCairoConfiguration().getDataAppendPageSize());
        Assert.assertEquals(60_000, configuration.getCairoConfiguration().getO3MaxLag());
        Assert.assertTrue(configuration.getCairoConfiguration().getTextConfiguration().isUseLegacyStringDefault());
        Assert.assertEquals(3000, configuration.getCairoConfiguration().getTelemetryConfiguration().getDbSizeEstimateTimeout());
    }

    @Test
    public void testExportTimeoutDependsOnQueryTimeout() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(PropertyKey.QUERY_TIMEOUT.getPropertyPath(), "600s");
        var propConf = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(600_000, propConf.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getExportTimeout());

        properties = new Properties();
        propConf = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(300_000, propConf.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getExportTimeout());
    }

    @Test
    public void testHttpConnectionLimits() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath(), "10");
        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "6");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "10");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "11");
        try {
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "Json query connection limit cannot be greater than the overall " +
                    "HTTP connection limit [http.json.query.connection.limit=11, http.net.connection.limit=10]");
        }

        properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath(), "10");
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "4");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "10");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "11");
        try {
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "HTTP over ILP connection limit cannot be greater than the overall " +
                    "HTTP connection limit [http.ilp.connection.limit=11, http.net.connection.limit=10]");
        }

        properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath(), "10");

        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "6");
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "1");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());

        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "7");
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "4");
        try {
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "The sum of the json query, export and HTTP over ILP connection limits " +
                    "cannot be greater than the overall HTTP connection limit " +
                    "[http.json.query.connection.limit=7, http.ilp.connection.limit=4, http.net.connection.limit=10]");
        }

        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "5");
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "6");
        try {
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "The sum of the json query, export and HTTP over ILP connection limits " +
                    "cannot be greater than the overall HTTP connection limit [http.json.query.connection.limit=5, " +
                    "http.ilp.connection.limit=6, http.net.connection.limit=10]");
        }

        properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath(), "10");
        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "7");
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "4");
        try {
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "The sum of the json query, export and HTTP over ILP connection limits " +
                    "cannot be greater than the overall HTTP connection limit [http.json.query.connection.limit=7, " +
                    "http.ilp.connection.limit=4, http.net.connection.limit=10]");
        }

        properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath(), "10");
        properties.setProperty(PropertyKey.HTTP_EXPORT_CONNECTION_LIMIT.getPropertyPath(), "20");
        try {
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "HTTP export connection limit cannot be greater than " +
                    "the overall HTTP connection limit [http.export.connection.limit=20, http.net.connection.limit=10]");
        }

        properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath(), "10");
        properties.setProperty(PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath(), "7");
        properties.setProperty(PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath(), "3");
        properties.setProperty(PropertyKey.HTTP_EXPORT_CONNECTION_LIMIT.getPropertyPath(), "1");
        try {
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "The sum of the json query, export and HTTP over ILP connection limits" +
                    " cannot be greater than the overall HTTP connection limit [http.json.query.connection.limit=7, " +
                    "http.ilp.connection.limit=3, http.export.connection.limit=1, http.net.connection.limit=10]");
        }
    }

    @Test
    public void testHttpDisabled() throws Exception {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server-http-disabled.conf")) {
            Properties properties = new Properties();
            properties.load(is);
            PropServerConfiguration configuration = newPropServerConfiguration(properties);
            Assert.assertFalse(configuration.getHttpServerConfiguration().isEnabled());
        }
    }

    @Test
    public void testHttpRedirects() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(PropertyKey.HTTP_REDIRECT_COUNT.getPropertyPath(), "2");
        properties.setProperty(PropertyKey.HTTP_REDIRECT_PREFIX.getPropertyPath() + "1", "/ -> /index.html");
        properties.setProperty(PropertyKey.HTTP_REDIRECT_PREFIX.getPropertyPath() + "2", "/x -> /x/index.html");

        final PropServerConfiguration configuration = newPropServerConfiguration(properties);
        final Utf8SequenceObjHashMap<Utf8Sequence> redirects = configuration.getHttpServerConfiguration()
                .getStaticContentProcessorConfiguration().getRedirectMap();

        Assert.assertEquals(3, redirects.size());
        Assert.assertEquals("/index.html", Utf8s.toString(redirects.get(new Utf8String(""))));
        Assert.assertEquals("/index.html", Utf8s.toString(redirects.get(new Utf8String("/"))));
        Assert.assertEquals("/x/index.html", Utf8s.toString(redirects.get(new Utf8String("/x"))));
    }

    @Test
    public void testILPMsgBufferSizeAdjustment() throws Exception {
        Properties properties = new Properties();

        properties.setProperty(PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.LINE_TCP_MSG_BUFFER_SIZE.getPropertyPath(), "8192");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(8192, configuration.getLineTcpReceiverConfiguration().getRecvBufferSize());

        properties.setProperty(PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.LINE_TCP_MSG_BUFFER_SIZE.getPropertyPath(), "1024");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getRecvBufferSize());

        // if the msg buffer size is smaller than the max measurement size,
        // then msg buffer size is adjusted to have enough space at least for a single measurement
        properties.setProperty(PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.LINE_TCP_MSG_BUFFER_SIZE.getPropertyPath(), "256");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getRecvBufferSize());
    }

    @Test
    public void testImportWorkRootCantBeTheSameAsOtherInstanceDirectories() throws Exception {
        Properties properties = new Properties();

        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertTrue(Chars.endsWith(configuration.getCairoConfiguration().getSqlCopyInputWorkRoot(), "tmp"));

        //direct cases
        assertInputWorkRootCantBeSetTo(properties, root);
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getDbRoot());
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getCheckpointRoot().toString());
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getConfRoot().toString());

        //relative cases
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(root));
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(configuration.getCairoConfiguration().getDbRoot()));
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(configuration.getCairoConfiguration().getCheckpointRoot().toString()));
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(configuration.getCairoConfiguration().getConfRoot().toString()));
    }

    @Test
    public void testImportWorkRootCantBeTheSameAsOtherInstanceDirectories2() throws Exception {
        Assume.assumeTrue(Os.isWindows());

        Properties properties = new Properties();

        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertTrue(Chars.endsWith(configuration.getCairoConfiguration().getSqlCopyInputWorkRoot(), "tmp"));

        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getDbRoot().toUpperCase());
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getDbRoot().toLowerCase());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToAddress() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6:8990");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToMissingColon() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToPort() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1:");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidConfigKeys() throws Exception {
        try (InputStream inputStream = PropServerConfigurationTest.class.getResourceAsStream("/server.conf")) {
            Properties properties = new Properties();
            properties.load(inputStream);
            properties.setProperty("this.will.throw", "Test");
            properties.setProperty("this.will.also", "throw");

            newPropServerConfiguration(properties);
        }
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidDouble() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.text.max.required.delimiter.stddev", "abc");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIPv4Address() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("line.udp.join", "12a.990.00");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidInt() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.connection.string.pool.capacity", "1234a");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIntSize() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.request.header.buffer.size", "22g");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidLineTcpBufferSize() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("line.tcp.max.measurement.size", "14");
        properties.setProperty("line.tcp.recv.buffer.size", "14");
        newPropServerConfiguration(properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidLong() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("cairo.idle.check.interval", "1234a");
        newPropServerConfiguration(properties);
    }

    @Test
    public void testInvalidTimestampLocale() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("log.timestamp.locale", "jp");
        try {
            newPropServerConfiguration(properties);
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertEquals("Invalid log locale: 'jp'", e.getMessage());
        }
    }

    @Test
    public void testInvalidTimestampTimezone() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("log.timestamp.timezone", "HKK");
        try {
            newPropServerConfiguration(properties);
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertEquals("Invalid log timezone: 'HKK'", e.getMessage());
        }
    }

    @Test
    public void testInvalidValidationResult() {
        Properties properties = new Properties();
        properties.setProperty("invalid.key", "value");
        PropServerConfiguration.ValidationResult result = validate(properties);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isError);
        Assert.assertNotEquals(-1, result.message.indexOf("Invalid settings"));
        Assert.assertNotEquals(-1, result.message.indexOf("* invalid.key"));
    }

    @Test
    public void testJpTimestampTimezoneCustomUtcOffset() throws Exception {
        assertTimestampTimezone(
                "月, 11 3月 2024 06:11:40 UTC+08",
                "UTC+08",
                "ja",
                "E, d MMM yyyy HH:mm:ss Z",
                "2024-03-10T22:11:40.000000Z"
        );

        assertTimestampTimezone(
                "lun, 11 mar 2024 00:11:40 CET",
                "CET",
                "it",
                "E, d MMM yyyy HH:mm:ss Z",
                "2024-03-10T23:11:40.000000Z"
        );

        assertTimestampTimezone(
                "周日, 10 3月 2024 13:11:40.222555 -10:00",
                "-10:00",
                "zh",
                "E, d MMM yyyy HH:mm:ss.SSSUUU Z",
                "2024-03-10T23:11:40.222555Z"
        );
    }

    @Test
    public void testLineUdpTimestamp() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.enabled", "false");
        properties.setProperty("line.udp.timestamp", "");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_NANOS, configuration.getLineUdpReceiverConfiguration().getTimestampUnit());

        properties.setProperty("line.udp.timestamp", "n");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_NANOS, configuration.getLineUdpReceiverConfiguration().getTimestampUnit());

        properties.setProperty("line.udp.timestamp", "u");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_MICROS, configuration.getLineUdpReceiverConfiguration().getTimestampUnit());

        properties.setProperty("line.udp.timestamp", "ms");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_MILLIS, configuration.getLineUdpReceiverConfiguration().getTimestampUnit());

        properties.setProperty("line.udp.timestamp", "s");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_SECONDS, configuration.getLineUdpReceiverConfiguration().getTimestampUnit());

        properties.setProperty("line.udp.timestamp", "m");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_MINUTES, configuration.getLineUdpReceiverConfiguration().getTimestampUnit());

        properties.setProperty("line.udp.timestamp", "h");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_HOURS, configuration.getLineUdpReceiverConfiguration().getTimestampUnit());
    }

    @Test
    public void testMinimum2SharedWorkers() throws Exception {
        final Properties properties = new Properties();
        final PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals("shared-network", configuration.getSharedWorkerPoolNetworkConfiguration().getPoolName());
        Assert.assertTrue("must be minimum of 2 shared workers", configuration.getSharedWorkerPoolNetworkConfiguration().getWorkerCount() >= 2);

        Assert.assertEquals("shared-query", configuration.getSharedWorkerPoolQueryConfiguration().getPoolName());
        Assert.assertTrue("must be minimum of 2 shared workers", configuration.getSharedWorkerPoolQueryConfiguration().getWorkerCount() >= 2);

        Assert.assertEquals("shared-write", configuration.getSharedWorkerPoolWriteConfiguration().getPoolName());
        Assert.assertTrue("must be minimum of 2 shared workers", configuration.getSharedWorkerPoolWriteConfiguration().getWorkerCount() >= 2);
    }

    @Test
    public void testNotValidAllowedVolumePaths0() throws Exception {
        File volumeA = temp.newFolder("volumeA");
        try {
            String aliasA = "volumeA";
            String aliasB = "volumeB";
            String volumeAPath = volumeA.getAbsolutePath();
            String volumeBPath = "banana";

            Properties properties = new Properties();
            sink.clear();
            loadVolumePath(aliasA, volumeAPath);
            sink.put(',');
            loadVolumePath(aliasB, volumeBPath);
            properties.setProperty(PropertyKey.CAIRO_VOLUMES.getPropertyPath(), sink.toString());
            try {
                newPropServerConfiguration(properties);
            } catch (ServerConfigurationException e) {
                TestUtils.assertContains(e.getMessage(), "inaccessible volume [path=banana]");
            }
        } finally {
            Assert.assertTrue(volumeA.delete());
        }
    }

    @Test
    public void testNotValidAllowedVolumePaths1() throws Exception {
        File volumeB = temp.newFolder("volumeB");
        try {
            String aliasA = "volumeA";
            String aliasB = "volumeB";
            String volumeAPath = "coconut";
            String volumeBPath = volumeB.getAbsolutePath();
            Properties properties = new Properties();
            sink.clear();
            loadVolumePath(aliasA, volumeAPath);
            sink.put(',');
            loadVolumePath(aliasB, volumeBPath);
            properties.setProperty(PropertyKey.CAIRO_VOLUMES.getPropertyPath(), sink.toString());
            try {
                newPropServerConfiguration(properties);
            } catch (ServerConfigurationException e) {
                TestUtils.assertContains(e.getMessage(), "inaccessible volume [path=coconut]");
            }
        } finally {
            Assert.assertTrue(volumeB.delete());
        }
    }

    @Test
    public void testNotValidAllowedVolumePaths2() throws Exception {
        String p = ",";
        Properties properties = new Properties();
        properties.setProperty(PropertyKey.CAIRO_VOLUMES.getPropertyPath(), p);
        try {
            newPropServerConfiguration(properties);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "invalid syntax, missing alias at offset 0");
        }
    }

    @Test
    public void testObsoleteValidationResult() {
        Properties properties = new Properties();
        properties.setProperty("line.tcp.commit.timeout", "10000");
        PropServerConfiguration.ValidationResult result = validate(properties);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isError);
        Assert.assertNotEquals(-1, result.message.indexOf("Obsolete settings"));
        Assert.assertNotEquals(-1, result.message.indexOf(
                "Replaced by `line.tcp.commit.interval.default` and `line.tcp.commit.interval.fraction`"));
    }

    @Test
    public void testPartitionBy() throws Exception {
        Properties properties = new Properties();
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.tcp.default.partition.by", "YEAR");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PartitionBy.YEAR, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "WEEK");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PartitionBy.WEEK, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.WEEK, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "MONTH");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PartitionBy.MONTH, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.MONTH, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "DAY");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "YEAR");
        properties.setProperty("line.tcp.default.partition.by", "MONTH");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(PartitionBy.YEAR, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.YEAR, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());
    }

    @Test
    public void testSetAllFromFile() throws Exception {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            PropServerConfiguration configuration = newPropServerConfiguration(properties);
            testSetAllFromFile(configuration.getCairoConfiguration());
            testSetAllFromFile(new CairoConfigurationWrapper(configuration.getCairoConfiguration()));

            Assert.assertEquals(1000, configuration.getHttpServerConfiguration().getAcceptLoopTimeout());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());
            Assert.assertEquals(512, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
            Assert.assertEquals(256, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartHeaderBufferSize());
            Assert.assertEquals(100_000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
            Assert.assertEquals(2048, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRequestHeaderBufferSize());
            Assert.assertEquals(6, configuration.getHttpServerConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2, 3, 4, 5, 6}, configuration.getHttpServerConfiguration().getWorkerAffinity());
            Assert.assertTrue(configuration.getHttpServerConfiguration().haltOnError());
            Assert.assertEquals(6, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getJsonQueryConnectionLimit());
            Assert.assertEquals(2, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getIlpConnectionLimit());
            Assert.assertEquals(1_200_000_000L, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout());
            Assert.assertEquals(SecurityContext.AUTH_TYPE_NONE, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getRequiredAuthType());
            Assert.assertFalse(configuration.getHttpServerConfiguration().isQueryCacheEnabled());
            Assert.assertTrue(configuration.getHttpServerConfiguration().isSettingsReadOnly());
            Assert.assertEquals(32, configuration.getHttpServerConfiguration().getConcurrentCacheConfiguration().getBlocks());
            Assert.assertEquals(16, configuration.getHttpServerConfiguration().getConcurrentCacheConfiguration().getRows());

            Assert.assertTrue(configuration.getHttpServerConfiguration().isPessimisticHealthCheckEnabled());
            Assert.assertEquals(SecurityContext.AUTH_TYPE_NONE, configuration.getHttpServerConfiguration().getRequiredAuthType());
            Assert.assertTrue(configuration.getHttpMinServerConfiguration().isPessimisticHealthCheckEnabled());
            Assert.assertEquals(SecurityContext.AUTH_TYPE_NONE, configuration.getHttpMinServerConfiguration().getRequiredAuthType());

            Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
            Assert.assertEquals(50000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getMaxQueryResponseRowLimit());

            Assert.assertEquals(100, configuration.getSharedWorkerPoolNetworkConfiguration().getYieldThreshold());
            Assert.assertEquals(90000, configuration.getSharedWorkerPoolNetworkConfiguration().getNapThreshold());
            Assert.assertEquals(100000, configuration.getSharedWorkerPoolNetworkConfiguration().getSleepThreshold());
            Assert.assertEquals(1000, configuration.getSharedWorkerPoolNetworkConfiguration().getSleepTimeout());

            Assert.assertEquals(101, configuration.getHttpServerConfiguration().getYieldThreshold());
            Assert.assertEquals(90001, configuration.getHttpServerConfiguration().getNapThreshold());
            Assert.assertEquals(100001, configuration.getHttpServerConfiguration().getSleepThreshold());
            Assert.assertEquals(1001, configuration.getHttpServerConfiguration().getSleepTimeout());

            Assert.assertEquals(102, configuration.getHttpMinServerConfiguration().getYieldThreshold());
            Assert.assertEquals(90002, configuration.getHttpMinServerConfiguration().getNapThreshold());
            Assert.assertEquals(100002, configuration.getHttpMinServerConfiguration().getSleepThreshold());
            Assert.assertEquals(1002, configuration.getHttpMinServerConfiguration().getSleepTimeout());
            Assert.assertEquals(16, configuration.getHttpMinServerConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(4, configuration.getHttpMinServerConfiguration().getWorkerCount());
            Assert.assertEquals(750, configuration.getHttpMinServerConfiguration().getAcceptLoopTimeout());

            Assert.assertEquals(
                    new File(root, "public_ok").getAbsolutePath(),
                    configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory()
            );

            Assert.assertEquals("Keep-Alive: timeout=10, max=50000" + Misc.EOL, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());
            Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().allowDeflateBeforeSend());

            Assert.assertEquals(63, configuration.getHttpServerConfiguration().getLimit());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getEventCapacity());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getIOQueueCapacity());
            Assert.assertEquals(7000000, configuration.getHttpServerConfiguration().getTimeout());
            Assert.assertEquals(1001, configuration.getHttpServerConfiguration().getQueueTimeout());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getInterestQueueCapacity());
            Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getInitialBias());
            Assert.assertEquals(63, configuration.getHttpServerConfiguration().getListenBacklog());
            Assert.assertEquals(4096, configuration.getHttpServerConfiguration().getSendBufferSize());
            Assert.assertEquals(4194304, configuration.getHttpServerConfiguration().getNetSendBufferSize());
            Assert.assertEquals(8192, configuration.getHttpServerConfiguration().getRecvBufferSize());
            Assert.assertEquals(8388608, configuration.getHttpServerConfiguration().getNetRecvBufferSize());
            Assert.assertEquals(16, configuration.getHttpServerConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(168101918, configuration.getHttpServerConfiguration().getBindIPv4Address());
            Assert.assertEquals(9900, configuration.getHttpServerConfiguration().getBindPort());
            Assert.assertEquals(2_000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getConnectionCheckFrequency());
            Assert.assertSame(FilesFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getFilesFacade());
            Assert.assertEquals("Keep-Alive: timeout=10, max=50000" + Misc.EOL, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getKeepAliveHeader());

            Assert.assertEquals(167903521, configuration.getLineUdpReceiverConfiguration().getBindIPv4Address());
            Assert.assertEquals(9915, configuration.getLineUdpReceiverConfiguration().getPort());
            Assert.assertEquals(-536805119, configuration.getLineUdpReceiverConfiguration().getGroupIPv4Address());
            Assert.assertEquals(100_000, configuration.getLineUdpReceiverConfiguration().getCommitRate());
            Assert.assertEquals(4 * 1024 * 1024, configuration.getLineUdpReceiverConfiguration().getMsgBufferSize());
            Assert.assertEquals(4000, configuration.getLineUdpReceiverConfiguration().getMsgCount());
            Assert.assertEquals(512, configuration.getLineUdpReceiverConfiguration().getReceiveBufferSize());
            Assert.assertEquals(PartitionBy.MONTH, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());
            Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().isEnabled());
            Assert.assertEquals(2, configuration.getLineUdpReceiverConfiguration().ownThreadAffinity());
            Assert.assertTrue(configuration.getLineUdpReceiverConfiguration().ownThread());

            // influxdb line TCP protocol
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().isEnabled());
            Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().logMessageOnError());
            Assert.assertEquals(1500, configuration.getLineTcpReceiverConfiguration().getAcceptLoopTimeout());
            Assert.assertEquals(11, configuration.getLineTcpReceiverConfiguration().getLimit());
            Assert.assertEquals(167903521, configuration.getLineTcpReceiverConfiguration().getBindIPv4Address());
            Assert.assertEquals(9916, configuration.getLineTcpReceiverConfiguration().getBindPort());
            Assert.assertEquals(64, configuration.getLineTcpReceiverConfiguration().getEventCapacity());
            Assert.assertEquals(64, configuration.getLineTcpReceiverConfiguration().getIOQueueCapacity());
            Assert.assertEquals(400_000, configuration.getLineTcpReceiverConfiguration().getTimeout());
            Assert.assertEquals(1_002, configuration.getLineTcpReceiverConfiguration().getQueueTimeout());
            Assert.assertEquals(64, configuration.getLineTcpReceiverConfiguration().getInterestQueueCapacity());
            Assert.assertEquals(11, configuration.getLineTcpReceiverConfiguration().getListenBacklog());
            Assert.assertEquals(16, configuration.getLineTcpReceiverConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(32, configuration.getLineTcpReceiverConfiguration().getConnectionPoolInitialCapacity());
            Assert.assertEquals(CommonUtils.TIMESTAMP_UNIT_MICROS, configuration.getLineTcpReceiverConfiguration().getTimestampUnit());
            Assert.assertEquals(2049, configuration.getLineTcpReceiverConfiguration().getRecvBufferSize());
            Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getNetRecvBufferSize());
            Assert.assertEquals(128, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
            Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getWriterQueueCapacity());
            Assert.assertEquals(2, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2}, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(20, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getYieldThreshold());
            Assert.assertEquals(9_002, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getNapThreshold());
            Assert.assertEquals(10_002, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getSleepThreshold());
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().haltOnError());
            Assert.assertEquals(3, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{3, 4, 5}, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(30, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getYieldThreshold());
            Assert.assertEquals(9_003, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getNapThreshold());
            Assert.assertEquals(10_003, configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().getSleepThreshold());
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getNetworkWorkerPoolConfiguration().haltOnError());
            Assert.assertEquals(1000, configuration.getLineTcpReceiverConfiguration().getMaintenanceInterval());
            Assert.assertEquals(PartitionBy.MONTH, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
            Assert.assertEquals(5_000, configuration.getLineTcpReceiverConfiguration().getWriterIdleTimeout());
            Assert.assertEquals(ColumnType.FLOAT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());
            Assert.assertEquals(ColumnType.INT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());
            Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getDisconnectOnError());

            Assert.assertFalse(configuration.getHttpServerConfiguration().getLineHttpProcessorConfiguration().logMessageOnError());

            Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getServerKeepAlive());
            Assert.assertEquals("HTTP/1.0 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());
            Assert.assertEquals(32, configuration.getCairoConfiguration().getQueryCacheEventQueueCapacity());
            Assert.assertEquals(1.5, configuration.getHttpServerConfiguration().getWaitProcessorConfiguration().getExponentialWaitMultiplier(), 0.00001);

            Assert.assertTrue(configuration.getMetricsConfiguration().isEnabled());

            Assert.assertFalse(configuration.getCairoConfiguration().isMatViewEnabled());
            Assert.assertEquals(100, configuration.getCairoConfiguration().getMatViewMaxRefreshRetries());
            Assert.assertEquals(10, configuration.getCairoConfiguration().getMatViewRefreshOomRetryTimeout());
            Assert.assertEquals(1000, configuration.getCairoConfiguration().getMatViewInsertAsSelectBatchSize());
            Assert.assertEquals(10000, configuration.getCairoConfiguration().getMatViewRowsPerQueryEstimate());
            Assert.assertFalse(configuration.getCairoConfiguration().isMatViewParallelSqlEnabled());
            Assert.assertEquals(10, configuration.getCairoConfiguration().getMatViewMaxRefreshIntervals());
            Assert.assertEquals(4200, configuration.getCairoConfiguration().getMatViewRefreshIntervalsUpdatePeriod());
            Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getMatViewMaxRefreshStepUs());

            // PG wire
            Assert.assertEquals(9, configuration.getPGWireConfiguration().getBinParamCountCapacity());
            Assert.assertFalse(configuration.getPGWireConfiguration().isSelectCacheEnabled());
            Assert.assertEquals(1, configuration.getPGWireConfiguration().getConcurrentCacheConfiguration().getBlocks());
            Assert.assertEquals(2, configuration.getPGWireConfiguration().getConcurrentCacheConfiguration().getRows());
            Assert.assertFalse(configuration.getPGWireConfiguration().isInsertCacheEnabled());
            Assert.assertEquals(128, configuration.getPGWireConfiguration().getInsertCacheBlockCount());
            Assert.assertEquals(256, configuration.getPGWireConfiguration().getInsertCacheRowCount());
            Assert.assertFalse(configuration.getPGWireConfiguration().isUpdateCacheEnabled());
            Assert.assertEquals(128, configuration.getPGWireConfiguration().getUpdateCacheBlockCount());
            Assert.assertEquals(256, configuration.getPGWireConfiguration().getUpdateCacheRowCount());
            Assert.assertTrue(configuration.getPGWireConfiguration().readOnlySecurityContext());
            Assert.assertEquals("my_quest", configuration.getPGWireConfiguration().getDefaultPassword());
            Assert.assertEquals("my_admin", configuration.getPGWireConfiguration().getDefaultUsername());
            Assert.assertTrue(configuration.getPGWireConfiguration().isReadOnlyUserEnabled());
            Assert.assertEquals("my_quest_ro", configuration.getPGWireConfiguration().getReadOnlyPassword());
            Assert.assertEquals("my_user", configuration.getPGWireConfiguration().getReadOnlyUsername());
            Assert.assertEquals(16, configuration.getPGWireConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(new DefaultPGConfiguration().getServerVersion(), configuration.getPGWireConfiguration().getServerVersion());
            Assert.assertEquals(10, configuration.getPGWireConfiguration().getNamedStatementLimit());
            Assert.assertEquals(250, configuration.getPGWireConfiguration().getAcceptLoopTimeout());

            Assert.assertEquals(255, configuration.getLineTcpReceiverConfiguration().getMaxFileNameLength());
            Assert.assertEquals(255, configuration.getLineUdpReceiverConfiguration().getMaxFileNameLength());

            Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getAutoCreateNewColumns());
            Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().getAutoCreateNewColumns());
            Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getAutoCreateNewTables());
            Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().getAutoCreateNewTables());

            Assert.assertTrue(configuration.getWalApplyPoolConfiguration().isEnabled());
            Assert.assertTrue(configuration.getWalApplyPoolConfiguration().haltOnError());
            Assert.assertEquals("wal-apply", configuration.getWalApplyPoolConfiguration().getPoolName());
            Assert.assertEquals(3, configuration.getWalApplyPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2, 3}, configuration.getWalApplyPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(55, configuration.getWalApplyPoolConfiguration().getSleepTimeout());
            Assert.assertEquals(23, configuration.getWalApplyPoolConfiguration().getNapThreshold());
            Assert.assertEquals(33, configuration.getWalApplyPoolConfiguration().getSleepThreshold());
            Assert.assertEquals(33033, configuration.getWalApplyPoolConfiguration().getYieldThreshold());
            Assert.assertFalse(configuration.getCairoConfiguration().isWalApplyParallelSqlEnabled());

            Assert.assertTrue(configuration.getMatViewRefreshPoolConfiguration().isEnabled());
            Assert.assertTrue(configuration.getMatViewRefreshPoolConfiguration().haltOnError());
            Assert.assertEquals("mat-view-refresh", configuration.getMatViewRefreshPoolConfiguration().getPoolName());
            Assert.assertEquals(3, configuration.getMatViewRefreshPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2, 3}, configuration.getMatViewRefreshPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(55, configuration.getMatViewRefreshPoolConfiguration().getSleepTimeout());
            Assert.assertEquals(23, configuration.getMatViewRefreshPoolConfiguration().getNapThreshold());
            Assert.assertEquals(33, configuration.getMatViewRefreshPoolConfiguration().getSleepThreshold());
            Assert.assertEquals(33033, configuration.getMatViewRefreshPoolConfiguration().getYieldThreshold());

            Assert.assertTrue(configuration.getExportPoolConfiguration().isEnabled());
            Assert.assertTrue(configuration.getExportPoolConfiguration().haltOnError());
            Assert.assertEquals("export", configuration.getExportPoolConfiguration().getPoolName());
            Assert.assertEquals(3, configuration.getExportPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2, 3}, configuration.getExportPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(55, configuration.getExportPoolConfiguration().getSleepTimeout());
            Assert.assertEquals(23, configuration.getExportPoolConfiguration().getNapThreshold());
            Assert.assertEquals(33, configuration.getExportPoolConfiguration().getSleepThreshold());
            Assert.assertEquals(33033, configuration.getExportPoolConfiguration().getYieldThreshold());

            Assert.assertEquals(32, configuration.getCairoConfiguration().getPreferencesStringPoolCapacity());
        }
    }

    @Test
    public void testSetAllInternalProperties() throws Exception {
        final BuildInformation buildInformation = new BuildInformationHolder("5.0.6", "0fff7d46fd13b4705770f1fb126dd9b889768643", "11.0.9.1", "QuestDB");
        final PropServerConfiguration configuration = newPropServerConfiguration(root, new Properties(), null, buildInformation);

        Assert.assertEquals("5.0.6", configuration.getCairoConfiguration().getBuildInformation().getSwVersion());
        Assert.assertEquals("11.0.9.1", configuration.getCairoConfiguration().getBuildInformation().getJdkVersion());
        Assert.assertEquals("0fff7d46fd13b4705770f1fb126dd9b889768643", configuration.getCairoConfiguration().getBuildInformation().getCommitHash());
    }

    @Test
    public void testSetAllNetFromFile() throws Exception {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server-net.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            PropServerConfiguration configuration = newPropServerConfiguration(properties);

            Assert.assertEquals(9020, configuration.getHttpServerConfiguration().getBindPort());
            Assert.assertEquals(63, configuration.getHttpServerConfiguration().getLimit());
            Assert.assertEquals(7000000, configuration.getHttpServerConfiguration().getTimeout());
            Assert.assertEquals(1001, configuration.getHttpServerConfiguration().getQueueTimeout());
            Assert.assertEquals(4096, configuration.getHttpServerConfiguration().getSendBufferSize());
            Assert.assertEquals(4194304, configuration.getHttpServerConfiguration().getNetSendBufferSize());
            Assert.assertEquals(8192, configuration.getHttpServerConfiguration().getRecvBufferSize());
            Assert.assertEquals(8388608, configuration.getHttpServerConfiguration().getNetRecvBufferSize());
            Assert.assertTrue(configuration.getHttpServerConfiguration().getHint());

            Assert.assertEquals(9120, configuration.getHttpMinServerConfiguration().getBindPort());
            Assert.assertEquals(8, configuration.getHttpMinServerConfiguration().getLimit());
            Assert.assertEquals(7000000, configuration.getHttpMinServerConfiguration().getTimeout());
            Assert.assertEquals(1001, configuration.getHttpMinServerConfiguration().getQueueTimeout());
            Assert.assertEquals(32768, configuration.getHttpMinServerConfiguration().getSendBufferSize());
            Assert.assertEquals(33554432, configuration.getHttpMinServerConfiguration().getNetSendBufferSize());
            Assert.assertEquals(16384, configuration.getHttpMinServerConfiguration().getRecvBufferSize());
            Assert.assertEquals(16777216, configuration.getHttpMinServerConfiguration().getNetRecvBufferSize());
            Assert.assertEquals(64, configuration.getHttpMinServerConfiguration().getTestConnectionBufferSize());
            Assert.assertTrue(configuration.getHttpMinServerConfiguration().getHint());

            // ILP/TCP
            Assert.assertEquals(11, configuration.getLineTcpReceiverConfiguration().getLimit());
            Assert.assertEquals(400_000, configuration.getLineTcpReceiverConfiguration().getTimeout());
            Assert.assertEquals(1_002, configuration.getLineTcpReceiverConfiguration().getQueueTimeout());
            Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getRecvBufferSize());
            Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getNetRecvBufferSize());
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getHint());

            // PGWire
            Assert.assertEquals(11, configuration.getPGWireConfiguration().getLimit());
            Assert.assertEquals(400000, configuration.getPGWireConfiguration().getTimeout());
            Assert.assertEquals(1002, configuration.getPGWireConfiguration().getQueueTimeout());
            Assert.assertEquals(1048576, configuration.getPGWireConfiguration().getRecvBufferSize());
            Assert.assertEquals(32768, configuration.getPGWireConfiguration().getNetRecvBufferSize());
            Assert.assertEquals(1048576, configuration.getPGWireConfiguration().getSendBufferSize());
            Assert.assertEquals(32800, configuration.getPGWireConfiguration().getNetSendBufferSize());
            Assert.assertTrue(configuration.getPGWireConfiguration().getHint());
        }
    }

    @Test
    public void testSetZeroKeepAlive() throws Exception {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server-keep-alive.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            PropServerConfiguration configuration = newPropServerConfiguration(properties);
            Assert.assertNull(configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());
        }
    }

    @Test
    public void testSqlJitMode() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("cairo.sql.jit.mode", "");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(SqlJitMode.JIT_MODE_ENABLED, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "on");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(SqlJitMode.JIT_MODE_ENABLED, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "scalar");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(SqlJitMode.JIT_MODE_FORCE_SCALAR, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "off");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(SqlJitMode.JIT_MODE_DISABLED, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "foobar");
        configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(SqlJitMode.JIT_MODE_ENABLED, configuration.getCairoConfiguration().getSqlJitMode());
    }

    @Test
    public void testValidAllowedVolumePaths0() throws Exception {
        File volumeA = temp.newFolder("volumeA");
        File volumeB = temp.newFolder("volumeB");
        File volumeC = temp.newFolder("volumeC");
        try {
            String aliasA = "volumeA";
            String aliasB = "volumeB";
            String aliasC = "volumeC";
            String volumeAPath = volumeA.getAbsolutePath();
            String volumeBPath = volumeB.getAbsolutePath();
            String volumeCPath = volumeC.getAbsolutePath();
            Properties properties = new Properties();
            properties.setProperty(PropertyKey.CAIRO_VOLUMES.getPropertyPath(), "");
            Assert.assertNull(validate(properties));
            for (int i = 0; i < 20; i++) {
                sink.clear();
                loadVolumePath(aliasA, volumeAPath);
                sink.put(',');
                loadVolumePath(aliasB, volumeBPath);
                sink.put(',');
                loadVolumePath(aliasC, volumeCPath);
                properties.setProperty(PropertyKey.CAIRO_VOLUMES.getPropertyPath(), sink.toString());
                CairoConfiguration cairoConfig = newPropServerConfiguration(properties).getCairoConfiguration();

                Assert.assertNotNull(cairoConfig.getVolumeDefinitions().resolveAlias(aliasA));
                Assert.assertNotNull(cairoConfig.getVolumeDefinitions().resolveAlias(aliasB));
                Assert.assertNotNull(cairoConfig.getVolumeDefinitions().resolveAlias(aliasC));
                Assert.assertNull(cairoConfig.getVolumeDefinitions().resolveAlias("banana"));
            }
        } finally {
            Assert.assertTrue(volumeA.delete());
            Assert.assertTrue(volumeB.delete());
            Assert.assertTrue(volumeC.delete());
        }
    }

    @Test
    public void testValidAllowedVolumePaths1() throws Exception {
        String p = "   ";
        Properties properties = new Properties();
        properties.setProperty(PropertyKey.CAIRO_VOLUMES.getPropertyPath(), p);
        CairoConfiguration cairoConfig = newPropServerConfiguration(properties).getCairoConfiguration();
        Assert.assertNull(cairoConfig.getVolumeDefinitions().resolveAlias("banana"));
    }

    @Test
    public void testValidConfiguration() {
        Properties properties = new Properties();
        properties.setProperty("http.net.connection.rcvbuf", "10000");
        PropServerConfiguration.ValidationResult result = validate(properties);
        Assert.assertNull(result);
    }

    @Test
    public void testValidationIsOffByDefault() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("this.will.not.throw", "Test");
        properties.setProperty("this.will.also.not", "throw");
        newPropServerConfiguration(properties);
    }

    @Test
    public void testWebConsolePathChangeUpdatesDefaultDependencies() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.context.web.console", "/new-path");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        // duplicates are ok
        Assert.assertEquals(
                "[/new-path/warnings,/new-path/warnings]",
                configuration.getHttpServerConfiguration().getContextPathWarnings().toString()
        );
        Assert.assertEquals(
                "[/new-path/exec,/new-path/exec]",
                configuration.getHttpServerConfiguration().getContextPathExec().toString()
        );
        Assert.assertEquals(
                "[/new-path/exp,/new-path/exp]",
                configuration.getHttpServerConfiguration().getContextPathExport().toString()
        );
        Assert.assertEquals(
                "[/new-path/imp,/new-path/imp]",
                configuration.getHttpServerConfiguration().getContextPathImport().toString()
        );

        Assert.assertEquals(
                "[/new-path/chk,/new-path/chk]",
                configuration.getHttpServerConfiguration().getContextPathTableStatus().toString()
        );

        Assert.assertEquals(
                "[/new-path/settings,/new-path/settings]",
                configuration.getHttpServerConfiguration().getContextPathSettings().toString()
        );

        // check the ILP did not move

        Assert.assertEquals(
                "[/write,/api/v2/write]",
                configuration.getHttpServerConfiguration().getContextPathILP().toString()
        );

        Assert.assertEquals(
                "[/ping]",
                configuration.getHttpServerConfiguration().getContextPathILPPing().toString()
        );
    }

    @Test
    public void testWebConsolePathChangeUpdatesDefaultDependenciesFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);

        final ObjList<FuzzItem> pathsThatCanBePinned = new ObjList<>();
        pathsThatCanBePinned.add(new FuzzItem("http.context.import", "/imp", HttpFullFatServerConfiguration::getContextPathImport));
        pathsThatCanBePinned.add(new FuzzItem("http.context.export", "/exp", HttpFullFatServerConfiguration::getContextPathExport));
        pathsThatCanBePinned.add(new FuzzItem("http.context.settings", "/settings", HttpFullFatServerConfiguration::getContextPathSettings));
        pathsThatCanBePinned.add(new FuzzItem("http.context.table.status", "/chk", HttpFullFatServerConfiguration::getContextPathTableStatus));
        pathsThatCanBePinned.add(new FuzzItem("http.context.warnings", "/warnings", HttpFullFatServerConfiguration::getContextPathWarnings));
        pathsThatCanBePinned.add(new FuzzItem("http.context.execute", "/exec", HttpFullFatServerConfiguration::getContextPathExec));

        String webConsolePath = rnd.nextString(64);
        Properties properties = new Properties();
        properties.setProperty("http.context.web.console", webConsolePath);
        int pinCount = rnd.nextInt(pathsThatCanBePinned.size() - 1) + 1; // at least one
        IntHashSet pinnedIndexes = new IntHashSet();
        for (int i = 0; i < pinCount; i++) {
            int index = rnd.nextPositiveInt() % pathsThatCanBePinned.size();
            FuzzItem item = pathsThatCanBePinned.getQuick(index);
            properties.setProperty(item.key, item.value);
            pinnedIndexes.add(index);
        }

        PropServerConfiguration configuration = newPropServerConfiguration(properties);

        for (int i = 0; i < pathsThatCanBePinned.size(); i++) {
            FuzzItem item = pathsThatCanBePinned.getQuick(i);
            String e1 = webConsolePath + item.value;
            String e2;
            if (pinnedIndexes.contains(i)) {
                e2 = item.value;
            } else {
                e2 = e1;
            }
            Assert.assertEquals(
                    "[" + e2 + "," + e1 + "]",
                    item.getter.apply(configuration.getHttpServerConfiguration()).toString()
            );
        }

        // check the ILP did not move
        Assert.assertEquals(
                "[/write,/api/v2/write]",
                configuration.getHttpServerConfiguration().getContextPathILP().toString()
        );

        Assert.assertEquals(
                "[/ping]",
                configuration.getHttpServerConfiguration().getContextPathILPPing().toString()
        );

        Utf8SequenceObjHashMap<Utf8Sequence> redirectMap = configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getRedirectMap();
        Assert.assertEquals(2, redirectMap.size());
        Assert.assertEquals(webConsolePath + "/index.html", redirectMap.get(new Utf8String(webConsolePath)).toString());
        Assert.assertEquals(webConsolePath + "/index.html", redirectMap.get(new Utf8String(webConsolePath + "/")).toString());
    }

    private void assertInputWorkRootCantBeSetTo(Properties properties, String value) throws Exception {
        try {
            properties.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT.getPropertyPath(), value);
            properties.setProperty(PropertyKey.CAIRO_SQL_COPY_WORK_ROOT.getPropertyPath(), value);
            newPropServerConfiguration(properties);
            Assert.fail("Should fail for " + value);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "cairo.sql.copy.work.root can't point to root, data, conf or snapshot dirs");
        }
    }

    private void assertTimestampTimezone(
            String expected,
            String timezone,
            String locale,
            String format,
            String timestamp
    ) throws Exception {
        sink.clear();
        Properties properties = new Properties();
        properties.setProperty("log.timestamp.timezone", timezone);
        properties.setProperty("log.timestamp.locale", locale);
        properties.setProperty("log.timestamp.format", format);
        long epoch = MicrosFormatUtils.parseTimestamp(timestamp);
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        DateFormat timestampFormat = configuration.getCairoConfiguration().getLogTimestampFormat();
        DateLocale timestampLocale = configuration.getCairoConfiguration().getLogTimestampTimezoneLocale();
        TimeZoneRules timestampTimezoneRules = configuration.getCairoConfiguration().getLogTimestampTimezoneRules();
        String timestampTimezone = configuration.getCairoConfiguration().getLogTimestampTimezone();
        timestampFormat.format(epoch + timestampTimezoneRules.getOffset(epoch), timestampLocale, timestampTimezone, sink);
        TestUtils.assertEquals(expected, sink);
    }

    private String getRelativePath(String path) {
        return path + File.separator + ".." + File.separator + new File(path).getName();
    }

    private void loadVolumePath(String alias, String volumePath) {
        randWhiteSpace();
        sink.put(alias);
        randWhiteSpace();
        sink.put("->");
        randWhiteSpace();
        sink.put(volumePath);
        randWhiteSpace();
    }

    private void randWhiteSpace() {
        for (int i = 0, n = Math.abs(rnd.nextInt()) % 4; i < n; i++) {
            sink.put(' ');
        }
    }

    private void testSetAllFromFile(CairoConfiguration configuration) {
        final FilesFacade ff = configuration.getFilesFacade();

        Assert.assertFalse(configuration.getCircuitBreakerConfiguration().isEnabled());
        Assert.assertEquals(500, configuration.getCircuitBreakerConfiguration().getCircuitBreakerThrottle());
        Assert.assertEquals(16, configuration.getCircuitBreakerConfiguration().getBufferSize());

        Assert.assertEquals(32, configuration.getTextConfiguration().getDateAdapterPoolCapacity());
        Assert.assertEquals(65536, configuration.getTextConfiguration().getJsonCacheLimit());
        Assert.assertEquals(8388608, configuration.getTextConfiguration().getJsonCacheSize());
        Assert.assertEquals(0.3d, configuration.getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000000001);
        Assert.assertEquals(0.9d, configuration.getTextConfiguration().getMaxRequiredLineLengthStdDev(), 0.000000001);
        Assert.assertEquals(512, configuration.getTextConfiguration().getMetadataStringPoolCapacity());
        Assert.assertEquals(6144, configuration.getTextConfiguration().getRollBufferLimit());
        Assert.assertEquals(3072, configuration.getTextConfiguration().getRollBufferSize());
        Assert.assertEquals(400, configuration.getTextConfiguration().getTextAnalysisMaxLines());
        Assert.assertEquals(128, configuration.getTextConfiguration().getTextLexerStringPoolCapacity());
        Assert.assertEquals(512, configuration.getTextConfiguration().getTimestampAdapterPoolCapacity());
        Assert.assertEquals(8192, configuration.getTextConfiguration().getUtf8SinkSize());
        Assert.assertEquals(1, configuration.getScoreboardFormat());
        Assert.assertEquals(4194304, configuration.getSqlCopyBufferSize());
        Assert.assertEquals(64, configuration.getCopyPoolCapacity());
        Assert.assertEquals("test-id-42", configuration.getSnapshotInstanceId());
        Assert.assertFalse(configuration.isCheckpointRecoveryEnabled());

        Assert.assertEquals(CommitMode.ASYNC, configuration.getCommitMode());
        Assert.assertEquals(12, configuration.getCreateAsSelectRetryCount());
        Assert.assertFalse(configuration.isMatViewEnabled());
        Assert.assertTrue(configuration.getDefaultSymbolCacheFlag());
        Assert.assertEquals(512, configuration.getDefaultSymbolCapacity());
        Assert.assertEquals(10, configuration.getFileOperationRetryCount());
        Assert.assertEquals(20_000, configuration.getIdleCheckInterval());
        Assert.assertEquals(42, configuration.getInactiveReaderMaxOpenPartitions());
        Assert.assertEquals(600_000, configuration.getInactiveReaderTTL());
        Assert.assertEquals(400_000, configuration.getInactiveWriterTTL());
        Assert.assertEquals(1024, configuration.getIndexValueBlockSize());
        Assert.assertEquals(23, configuration.getMaxSwapFileCount());
        Assert.assertEquals(509, configuration.getMkDirMode());
        Assert.assertEquals(509, configuration.getDetachedMkDirMode());
        Assert.assertEquals(1000000, configuration.getParallelIndexThreshold());
        Assert.assertEquals(42, configuration.getReaderPoolMaxSegments());
        Assert.assertEquals(5_000_000, configuration.getSpinLockTimeout());
        Assert.assertEquals(2048, configuration.getSqlCharacterStoreCapacity());
        Assert.assertEquals(128, configuration.getSqlCharacterStoreSequencePoolCapacity());
        Assert.assertEquals(2048, configuration.getSqlColumnPoolCapacity());
        Assert.assertEquals(1024, configuration.getSqlExpressionPoolCapacity());
        Assert.assertEquals(0.3, configuration.getSqlFastMapLoadFactor(), 0.0000001);
        Assert.assertEquals(32, configuration.getSqlJoinContextPoolCapacity());
        Assert.assertEquals(1024, configuration.getSqlLexerPoolCapacity());
        Assert.assertEquals(16, configuration.getSqlSmallMapKeyCapacity());
        Assert.assertEquals(42 * 1024, configuration.getSqlSmallMapPageSize());
        Assert.assertEquals(1026, configuration.getSqlMapMaxPages());
        Assert.assertEquals(128, configuration.getSqlMapMaxResizes());
        Assert.assertEquals(8, configuration.getSqlUnorderedMapMaxEntrySize());
        Assert.assertEquals(256, configuration.getSqlModelPoolCapacity());
        Assert.assertEquals(42, configuration.getSqlMaxNegativeLimit());
        Assert.assertEquals(10 * 1024 * 1024, configuration.getSqlSortKeyPageSize());
        Assert.assertEquals(256, configuration.getSqlSortKeyMaxPages());
        Assert.assertEquals(3 * 1024 * 1024, configuration.getSqlSortLightValuePageSize());
        Assert.assertEquals(1027, configuration.getSqlSortLightValueMaxPages());
        Assert.assertEquals(8 * 1024 * 1024, configuration.getSqlHashJoinValuePageSize());
        Assert.assertEquals(1024, configuration.getSqlHashJoinValueMaxPages());
        Assert.assertEquals(10000, configuration.getSqlLatestByRowCount());
        Assert.assertEquals(2 * 1024 * 1024, configuration.getSqlHashJoinLightValuePageSize());
        Assert.assertEquals(1025, configuration.getSqlHashJoinLightValueMaxPages());
        Assert.assertEquals(42, configuration.getSqlAsOfJoinLookAhead());
        Assert.assertEquals(1000, configuration.getSqlAsOfJoinShortCircuitCacheCapacity());
        Assert.assertEquals(1000, configuration.getSqlAsOfJoinMapEvacuationThreshold());
        Assert.assertEquals(4 * 1024 * 1024, configuration.getSqlSortValuePageSize());
        Assert.assertEquals(1028, configuration.getSqlSortValueMaxPages());
        Assert.assertEquals(1000000, configuration.getWorkStealTimeoutNanos());
        Assert.assertFalse(configuration.isParallelIndexingEnabled());
        Assert.assertEquals(8 * 1024, configuration.getSqlJoinMetadataPageSize());
        Assert.assertEquals(10_000, configuration.getSqlJoinMetadataMaxResizes());
        Assert.assertEquals(16, configuration.getBindVariablePoolSize());
        Assert.assertEquals(128, configuration.getQueryRegistryPoolSize());
        Assert.assertEquals(128, configuration.getCountDistinctCapacity());
        Assert.assertEquals(0.3, configuration.getCountDistinctLoadFactor(), 0.000001);

        Assert.assertEquals(256, configuration.getWindowColumnPoolCapacity());
        Assert.assertEquals(256, configuration.getSqlWindowMaxRecursion());
        Assert.assertEquals(512 * 1024, configuration.getSqlWindowTreeKeyPageSize());
        Assert.assertEquals(1031, configuration.getSqlWindowTreeKeyMaxPages());
        Assert.assertEquals(1024 * 1024, configuration.getSqlWindowStorePageSize());
        Assert.assertEquals(1029, configuration.getSqlWindowStoreMaxPages());
        Assert.assertEquals(524288, configuration.getSqlWindowRowIdPageSize());
        Assert.assertEquals(1030, configuration.getSqlWindowRowIdMaxPages());
        Assert.assertEquals(1024, configuration.getWithClauseModelPoolCapacity());
        Assert.assertEquals(512, configuration.getRenameTableModelPoolCapacity());
        Assert.assertEquals(128, configuration.getInsertModelPoolCapacity());
        Assert.assertEquals(256, configuration.getCreateTableColumnModelPoolCapacity());
        Assert.assertEquals(2001, configuration.getSampleByIndexSearchPageSize());
        Assert.assertFalse(configuration.getSampleByDefaultAlignmentCalendar());
        Assert.assertEquals(16, configuration.getWriterCommandQueueCapacity());
        Assert.assertEquals(4096, configuration.getWriterCommandQueueSlotSize());
        Assert.assertEquals(333000, configuration.getWriterAsyncCommandBusyWaitTimeout());
        Assert.assertEquals(7770001, configuration.getWriterAsyncCommandMaxTimeout());
        Assert.assertEquals(15, configuration.getWriterTickRowsCountMod());
        Assert.assertEquals(ff.allowMixedIO(root), configuration.isWriterMixedIOEnabled());
        Assert.assertEquals(CairoConfiguration.O_DIRECT | CairoConfiguration.O_SYNC, configuration.getWriterFileOpenOpts());
        Assert.assertFalse(configuration.isIOURingEnabled());

        Assert.assertEquals(100_000, configuration.getMaxUncommittedRows());
        Assert.assertEquals(42_000_000, configuration.getO3MinLag());
        Assert.assertEquals(420_000_000, configuration.getO3MaxLag());
        Assert.assertEquals(262144, configuration.getO3ColumnMemorySize());
        Assert.assertEquals(65536, configuration.getSystemO3ColumnMemorySize());

        Assert.assertEquals(256, configuration.getSqlDistinctTimestampKeyCapacity());
        Assert.assertEquals(0.4, configuration.getSqlDistinctTimestampLoadFactor(), 0.001);

        Assert.assertFalse(configuration.isSqlParallelFilterEnabled());
        Assert.assertEquals(0.1, configuration.getSqlParallelFilterPreTouchThreshold(), 0.000001);
        Assert.assertFalse(configuration.isSqlParallelTopKEnabled());
        Assert.assertFalse(configuration.isSqlParallelGroupByEnabled());
        Assert.assertFalse(configuration.isSqlParallelReadParquetEnabled());
        Assert.assertFalse(configuration.isSqlOrderBySortEnabled());
        Assert.assertEquals(100, configuration.getSqlOrderByRadixSortThreshold());
        Assert.assertEquals(32, configuration.getSqlParallelWorkStealingThreshold());
        Assert.assertEquals(42, configuration.getSqlParquetFrameCacheCapacity());
        Assert.assertEquals(1000, configuration.getSqlPageFrameMaxRows());
        Assert.assertEquals(100, configuration.getSqlPageFrameMinRows());
        Assert.assertEquals(128, configuration.getPageFrameReduceShardCount());
        Assert.assertEquals(1024, configuration.getPageFrameReduceQueueCapacity());
        Assert.assertEquals(8, configuration.getPageFrameReduceRowIdListCapacity());
        Assert.assertEquals(4, configuration.getPageFrameReduceColumnListCapacity());
        Assert.assertEquals(2048, configuration.getGroupByMergeShardQueueCapacity());
        Assert.assertEquals(100, configuration.getGroupByShardingThreshold());
        Assert.assertFalse(configuration.isGroupByPresizeEnabled());
        Assert.assertEquals(100_000, configuration.getGroupByPresizeMaxCapacity());
        Assert.assertEquals(1024, configuration.getGroupByPresizeMaxHeapSize());
        Assert.assertEquals(4096, configuration.getGroupByAllocatorDefaultChunkSize());

        Assert.assertEquals(SqlJitMode.JIT_MODE_FORCE_SCALAR, configuration.getSqlJitMode());
        Assert.assertEquals(2048, configuration.getSqlJitIRMemoryPageSize());
        Assert.assertEquals(2, configuration.getSqlJitIRMemoryMaxPages());
        Assert.assertEquals(1024, configuration.getSqlJitBindVarsMemoryPageSize());
        Assert.assertEquals(1, configuration.getSqlJitBindVarsMemoryMaxPages());
        Assert.assertEquals(1024, configuration.getSqlJitPageAddressCacheThreshold());
        Assert.assertTrue(configuration.isSqlJitDebugEnabled());

        Assert.assertEquals(16384, configuration.getRndFunctionMemoryPageSize());
        Assert.assertEquals(32, configuration.getRndFunctionMemoryMaxPages());

        Assert.assertEquals(16, configuration.getPartitionPurgeListCapacity());

        Assert.assertTrue(configuration.getTelemetryConfiguration().getEnabled());
        Assert.assertEquals(512, configuration.getTelemetryConfiguration().getQueueCapacity());
        Assert.assertEquals(1000, configuration.getTelemetryConfiguration().getDbSizeEstimateTimeout());

        Assert.assertEquals(1048576, configuration.getDataAppendPageSize());
        Assert.assertEquals(131072, configuration.getSystemDataAppendPageSize());
        Assert.assertEquals(Files.PAGE_SIZE, configuration.getDataIndexKeyAppendPageSize());
        Assert.assertEquals(262144, configuration.getDataIndexValueAppendPageSize());
        Assert.assertEquals(131072, configuration.getMiscAppendPageSize());
        Assert.assertEquals(65536, configuration.getSymbolTableMinAllocationPageSize());

        Assert.assertEquals(512, configuration.getColumnPurgeQueueCapacity());
        Assert.assertEquals(5.0, configuration.getColumnPurgeRetryDelayMultiplier(), 0.00001);
        Assert.assertEquals(30000000, configuration.getColumnPurgeRetryDelayLimit());
        Assert.assertEquals(30000, configuration.getColumnPurgeRetryDelay());

        Assert.assertEquals(255, configuration.getMaxFileNameLength());
        Assert.assertEquals(".detached", configuration.getAttachPartitionSuffix());
        Assert.assertTrue(configuration.attachPartitionCopy());

        Assert.assertEquals(333, configuration.getWalPurgeInterval());
        Assert.assertEquals(13, configuration.getWalRecreateDistressedSequencerAttempts());
        Assert.assertEquals(333303, configuration.getInactiveWalWriterTTL());
        Assert.assertEquals(128, configuration.getWalTxnNotificationQueueCapacity());
        Assert.assertTrue(configuration.isWalSupported());
        Assert.assertTrue(configuration.getWalEnabledDefault());
        Assert.assertFalse(configuration.isWalApplyEnabled());
        Assert.assertEquals(23, configuration.getWalApplyLookAheadTransactionCount());
        Assert.assertFalse(configuration.isTableTypeConversionEnabled());
        Assert.assertEquals(100, configuration.getWalWriterPoolMaxSegments());
        Assert.assertEquals(120, configuration.getO3LagCalculationWindowsSize());
        Assert.assertEquals(100, configuration.getWalSegmentRolloverRowCount());
        Assert.assertEquals(42.2d, configuration.getWalLagRowsMultiplier(), 0.00001);
        Assert.assertEquals(4242, configuration.getWalMaxLagTxnCount());
        Assert.assertEquals(262144, configuration.getWalDataAppendPageSize());
        Assert.assertEquals(524288, configuration.getSystemWalDataAppendPageSize());
        Assert.assertEquals(65536, configuration.getSymbolTableMinAllocationPageSize());

        Assert.assertEquals(1, configuration.getO3LastPartitionMaxSplits());
        final long TB = (long) Numbers.SIZE_1MB * Numbers.SIZE_1MB;
        Assert.assertEquals(TB, configuration.getPartitionO3SplitMinSize());

        Assert.assertEquals(10 * Numbers.SIZE_1MB, configuration.getWalMaxLagSize());
        Assert.assertEquals(50, configuration.getWalMaxSegmentFileDescriptorsCache());
    }

    private PropServerConfiguration.ValidationResult validate(Properties properties) {
        return new PropServerConfiguration.PropertyValidator().validate(properties);
    }

    @NotNull
    protected PropServerConfiguration newPropServerConfiguration(
            String root,
            Properties properties,
            Map<String, String> env,
            BuildInformation buildInformation
    ) throws Exception {
        return new PropServerConfiguration(root, properties, env, PropServerConfigurationTest.LOG, buildInformation);
    }

    protected PropServerConfiguration newPropServerConfiguration(Properties properties) throws Exception {
        return new PropServerConfiguration(root, properties, null, PropServerConfigurationTest.LOG, new BuildInformationHolder());
    }

    private record FuzzItem(String key, String value,
                            Function<HttpFullFatServerConfiguration, ObjList<String>> getter) {
    }
}
