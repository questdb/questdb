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

package io.questdb.test;

import io.questdb.*;
import io.questdb.cairo.*;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.*;
import io.questdb.cutlass.pgwire.DefaultPGWireConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.SelectFacadeImpl;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        FilesFacade ff = configuration.getCairoConfiguration().getFilesFacade();
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());
        Assert.assertEquals(128, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
        Assert.assertEquals(512, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartHeaderBufferSize());
        Assert.assertEquals(10_000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
        Assert.assertEquals(1048576, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRecvBufferSize());
        Assert.assertEquals(64448, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRequestHeaderBufferSize());
        Assert.assertFalse(configuration.getHttpServerConfiguration().haltOnError());
        Assert.assertFalse(configuration.getHttpServerConfiguration().haltOnError());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSendBufferSize());
        Assert.assertEquals("index.html", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getIndexFileName());
        Assert.assertTrue(configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().isAuthenticationRequired());
        Assert.assertTrue(configuration.getHttpServerConfiguration().isEnabled());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getDumpNetworkTraffic());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().allowDeflateBeforeSend());
        Assert.assertTrue(configuration.getHttpServerConfiguration().isQueryCacheEnabled());
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getQueryCacheBlockCount());
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getQueryCacheRowCount());

        Assert.assertEquals(10, configuration.getWorkerPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(10000, configuration.getWorkerPoolConfiguration().getSleepThreshold());
        Assert.assertEquals(10, configuration.getWorkerPoolConfiguration().getSleepTimeout());

        Assert.assertEquals(10, configuration.getHttpMinServerConfiguration().getYieldThreshold());
        Assert.assertEquals(100, configuration.getHttpMinServerConfiguration().getSleepThreshold());
        Assert.assertEquals(50, configuration.getHttpMinServerConfiguration().getSleepTimeout());
        Assert.assertEquals(1, configuration.getHttpMinServerConfiguration().getWorkerCount());

        // this is going to need interesting validation logic
        // configuration path is expected to be relative, and we need to check if absolute path is good
        Assert.assertEquals(
                new File(root, "public").getAbsolutePath(),
                configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory()
        );

        Assert.assertEquals("Keep-Alive: timeout=5, max=10000" + Misc.EOL, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());

        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getLimit());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEventCapacity());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
        Assert.assertEquals(300000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getTimeout());
        Assert.assertEquals(5000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getQueueTimeout());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
        Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInitialBias());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getListenBacklog());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSndBufSize());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
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
        Assert.assertEquals(0, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindIPv4Address());
        Assert.assertEquals(9000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindPort());

        Assert.assertEquals(1_000_000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getConnectionCheckFrequency());
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getFloatScale());
        Assert.assertEquals(12, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getDoubleScale());
        Assert.assertEquals("Keep-Alive: timeout=5, max=10000" + Misc.EOL, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getKeepAliveHeader());

        Assert.assertFalse(configuration.getHttpServerConfiguration().isPessimisticHealthCheckEnabled());
        Assert.assertTrue(configuration.getHttpServerConfiguration().isHealthCheckAuthenticationRequired());
        Assert.assertFalse(configuration.getHttpMinServerConfiguration().isPessimisticHealthCheckEnabled());
        Assert.assertTrue(configuration.getHttpMinServerConfiguration().isHealthCheckAuthenticationRequired());

        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
        Assert.assertEquals(Long.MAX_VALUE, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getMaxQueryResponseRowLimit());
        Assert.assertTrue(configuration.getCairoConfiguration().getCircuitBreakerConfiguration().isEnabled());
        Assert.assertEquals(2_000_000, configuration.getCairoConfiguration().getCircuitBreakerConfiguration().getCircuitBreakerThrottle());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getCircuitBreakerConfiguration().getBufferSize());

        Assert.assertEquals(CommitMode.NOSYNC, configuration.getCairoConfiguration().getCommitMode());
        Assert.assertEquals(2097152, configuration.getCairoConfiguration().getSqlCopyBufferSize());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getCopyPoolCapacity());
        Assert.assertEquals(5, configuration.getCairoConfiguration().getCreateAsSelectRetryCount());
        Assert.assertEquals("fast", configuration.getCairoConfiguration().getDefaultMapType());
        Assert.assertTrue(configuration.getCairoConfiguration().getDefaultSymbolCacheFlag());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getDefaultSymbolCapacity());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getFileOperationRetryCount());
        Assert.assertEquals(300000, configuration.getCairoConfiguration().getIdleCheckInterval());
        Assert.assertEquals(128, configuration.getCairoConfiguration().getInactiveReaderMaxOpenPartitions());
        Assert.assertEquals(120_000, configuration.getCairoConfiguration().getInactiveReaderTTL());
        Assert.assertEquals(600_000, configuration.getCairoConfiguration().getInactiveWriterTTL());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getIndexValueBlockSize());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getMaxSwapFileCount());
        Assert.assertEquals(509, configuration.getCairoConfiguration().getMkDirMode());
        Assert.assertEquals(509, configuration.getCairoConfiguration().getDetachedMkDirMode());
        Assert.assertEquals(8, configuration.getCairoConfiguration().getBindVariablePoolSize());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getCountDistinctCapacity());
        Assert.assertEquals(0.7, configuration.getCairoConfiguration().getCountDistinctLoadFactor(), 0.000001);

        Assert.assertEquals(100000, configuration.getCairoConfiguration().getParallelIndexThreshold());
        Assert.assertEquals(10, configuration.getCairoConfiguration().getReaderPoolMaxSegments());
        Assert.assertEquals(1_000, configuration.getCairoConfiguration().getSpinLockTimeout());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlCharacterStoreCapacity());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlCharacterStoreSequencePoolCapacity());
        Assert.assertEquals(4096, configuration.getCairoConfiguration().getSqlColumnPoolCapacity());
        Assert.assertEquals(0.7, configuration.getCairoConfiguration().getSqlCompactMapLoadFactor(), 0.000001);
        Assert.assertEquals(8192, configuration.getCairoConfiguration().getSqlExpressionPoolCapacity());
        Assert.assertEquals(0.7, configuration.getCairoConfiguration().getSqlFastMapLoadFactor(), 0.0000001);
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlJoinContextPoolCapacity());
        Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlLexerPoolCapacity());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlSmallMapKeyCapacity());
        Assert.assertEquals(32 * 1024, configuration.getCairoConfiguration().getSqlSmallMapPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlMapMaxPages());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlMapMaxResizes());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlModelPoolCapacity());
        Assert.assertEquals(10_000, configuration.getCairoConfiguration().getSqlMaxNegativeLimit());
        Assert.assertEquals(4 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortKeyPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlSortKeyMaxPages());
        Assert.assertEquals(8 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortLightValuePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlSortLightValueMaxPages());
        Assert.assertEquals(16 * 1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinValuePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlHashJoinValueMaxPages());
        Assert.assertEquals(1000, configuration.getCairoConfiguration().getSqlLatestByRowCount());
        Assert.assertEquals(1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinLightValuePageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlHashJoinLightValueMaxPages());
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
        Assert.assertEquals(64, configuration.getCairoConfiguration().getInsertPoolCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getColumnCastModelPoolCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getCreateTableModelPoolCapacity());
        Assert.assertEquals(1, configuration.getCairoConfiguration().getPartitionPurgeListCapacity());
        Assert.assertEquals(ff.allowMixedIO(root), configuration.getCairoConfiguration().isWriterMixedIOEnabled());
        Assert.assertEquals(CairoConfiguration.O_NONE, configuration.getCairoConfiguration().getWriterFileOpenOpts());
        Assert.assertTrue(configuration.getCairoConfiguration().isIOURingEnabled());

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

        Assert.assertTrue(configuration.getCairoConfiguration().isSqlParallelFilterPreTouchEnabled());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getSqlPageFrameMaxRows());
        Assert.assertEquals(1000, configuration.getCairoConfiguration().getSqlPageFrameMinRows());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getPageFrameReduceRowIdListCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getPageFrameReduceColumnListCapacity());
        Assert.assertEquals(10000, configuration.getCairoConfiguration().getGroupByShardingThreshold());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getGroupByShardCount());

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
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getClock());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getNetworkFacade());
        Assert.assertSame(EpollFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEpollFacade());
        Assert.assertSame(SelectFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSelectFacade());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
        Assert.assertTrue(FilesFacadeImpl.class.isAssignableFrom(configuration.getCairoConfiguration().getFilesFacade().getClass()));
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMillisecondClock());
        Assert.assertSame(MicrosecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMicrosecondClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getLineUdpReceiverConfiguration().getNetworkFacade());
        Assert.assertEquals("http-server", configuration.getHttpServerConfiguration().getDispatcherConfiguration().getDispatcherLogName());

        TestUtils.assertEquals(new File(root, "db").getAbsolutePath(), configuration.getCairoConfiguration().getRoot());
        TestUtils.assertEquals(new File(root, "conf").getAbsolutePath(), configuration.getCairoConfiguration().getConfRoot());
        TestUtils.assertEquals(new File(root, "snapshot").getAbsolutePath(), configuration.getCairoConfiguration().getSnapshotRoot());

        Assert.assertEquals("", configuration.getCairoConfiguration().getSnapshotInstanceId());
        Assert.assertTrue(configuration.getCairoConfiguration().isSnapshotRecoveryEnabled());

        // assert mime types
        TestUtils.assertEquals("application/json", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getMimeTypesCache().get(new Utf8String("json")));

        Assert.assertEquals(500_000, configuration.getCairoConfiguration().getMaxUncommittedRows());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getO3MinLag());
        Assert.assertEquals(600_000_000, configuration.getCairoConfiguration().getO3MaxLag());
        Assert.assertEquals(8388608, configuration.getCairoConfiguration().getO3ColumnMemorySize());
        Assert.assertEquals(262144, configuration.getCairoConfiguration().getSystemO3ColumnMemorySize());

        // influxdb line TCP protocol
        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().isEnabled());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getLimit());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getBindIPv4Address());
        Assert.assertEquals(9009, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getBindPort());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getEventCapacity());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getTimeout());
        Assert.assertEquals(5000, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getQueueTimeout());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getListenBacklog());
        Assert.assertEquals(-1, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getRcvBufSize());
        Assert.assertEquals(-1, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getSndBufSize());
        Assert.assertEquals(64, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
        Assert.assertEquals(8, configuration.getLineTcpReceiverConfiguration().getConnectionPoolInitialCapacity());
        Assert.assertEquals(LineNanoTimestampAdapter.INSTANCE, configuration.getLineTcpReceiverConfiguration().getTimestampAdapter().getDefaultAdapter());
        Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getNetMsgBufferSize());
        Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(128, configuration.getLineTcpReceiverConfiguration().getWriterQueueCapacity());
        Assert.assertEquals(1, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerCount());
        Assert.assertEquals(10, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getSleepThreshold());
        Assert.assertArrayEquals(new int[]{-1}, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerAffinity());
        Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().haltOnError());
        Assert.assertEquals(10, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getSleepThreshold());
        Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().haltOnError());
        Assert.assertEquals(1000, configuration.getLineTcpReceiverConfiguration().getMaintenanceInterval());
        Assert.assertEquals(PropServerConfiguration.COMMIT_INTERVAL_DEFAULT, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(500, configuration.getLineTcpReceiverConfiguration().getWriterIdleTimeout());
        Assert.assertEquals(0, configuration.getCairoConfiguration().getSampleByIndexSearchPageSize());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getWriterCommandQueueCapacity());
        Assert.assertEquals(2048, configuration.getCairoConfiguration().getWriterCommandQueueSlotSize());
        Assert.assertEquals(500, configuration.getCairoConfiguration().getWriterAsyncCommandBusyWaitTimeout());
        Assert.assertEquals(30_000, configuration.getCairoConfiguration().getWriterAsyncCommandMaxTimeout());
        Assert.assertEquals(1023, configuration.getCairoConfiguration().getWriterTickRowsCountMod());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());
        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getDisconnectOnError());

        Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getServerKeepAlive());
        Assert.assertEquals("HTTP/1.1 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());

        Assert.assertEquals("[DEVELOPMENT]", configuration.getCairoConfiguration().getBuildInformation().getSwVersion());
        Assert.assertEquals("unknown", configuration.getCairoConfiguration().getBuildInformation().getJdkVersion());
        Assert.assertEquals("unknown", configuration.getCairoConfiguration().getBuildInformation().getCommitHash());

        Assert.assertFalse(configuration.getMetricsConfiguration().isEnabled());

        Assert.assertEquals(16777216, configuration.getCairoConfiguration().getDataAppendPageSize());
        Assert.assertEquals(262144, configuration.getCairoConfiguration().getSystemDataAppendPageSize());
        Assert.assertEquals(524288, configuration.getCairoConfiguration().getDataIndexKeyAppendPageSize());
        Assert.assertEquals(16777216, configuration.getCairoConfiguration().getDataIndexValueAppendPageSize());
        Assert.assertEquals(Files.PAGE_SIZE, configuration.getCairoConfiguration().getMiscAppendPageSize());
        Assert.assertEquals(2.0, configuration.getHttpServerConfiguration().getWaitProcessorConfiguration().getExponentialWaitMultiplier(), 0.00001);

        Assert.assertEquals(128, configuration.getCairoConfiguration().getColumnPurgeQueueCapacity());
        Assert.assertEquals(10.0, configuration.getCairoConfiguration().getColumnPurgeRetryDelayMultiplier(), 0.00001);
        Assert.assertEquals(60000000, configuration.getCairoConfiguration().getColumnPurgeRetryDelayLimit());
        Assert.assertEquals(10000, configuration.getCairoConfiguration().getColumnPurgeRetryDelay());

        // PG wire
        Assert.assertEquals(64, configuration.getPGWireConfiguration().getDispatcherConfiguration().getLimit());
        Assert.assertEquals(64, configuration.getPGWireConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
        Assert.assertEquals(2, configuration.getPGWireConfiguration().getBinParamCountCapacity());
        Assert.assertTrue(configuration.getPGWireConfiguration().isSelectCacheEnabled());
        Assert.assertEquals(4, configuration.getPGWireConfiguration().getSelectCacheBlockCount());
        Assert.assertEquals(4, configuration.getPGWireConfiguration().getSelectCacheRowCount());
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
        Assert.assertFalse(configuration.getCairoConfiguration().getWalEnabledDefault());
        Assert.assertTrue(configuration.getCairoConfiguration().isWalApplyEnabled());
        Assert.assertTrue(configuration.getWalApplyPoolConfiguration().isEnabled());
        Assert.assertFalse(configuration.getWalApplyPoolConfiguration().haltOnError());
        Assert.assertEquals("wal-apply", configuration.getWalApplyPoolConfiguration().getPoolName());
        Assert.assertTrue(configuration.getWalApplyPoolConfiguration().getWorkerCount() > 0);
        Assert.assertEquals(10, configuration.getWalApplyPoolConfiguration().getSleepTimeout());
        Assert.assertEquals(10_000, configuration.getWalApplyPoolConfiguration().getSleepThreshold());
        Assert.assertEquals(10, configuration.getWalApplyPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(20, configuration.getCairoConfiguration().getWalApplyLookAheadTransactionCount());
        Assert.assertEquals(4, configuration.getCairoConfiguration().getO3LagCalculationWindowsSize());
        Assert.assertEquals(200_000, configuration.getCairoConfiguration().getWalSegmentRolloverRowCount());
        Assert.assertEquals(20.0d, configuration.getCairoConfiguration().getWalSquashUncommittedRowsMultiplier(), 0.00001);
        Assert.assertEquals(-1, configuration.getCairoConfiguration().getWalMaxLagTxnCount());
        Assert.assertEquals(1048576, configuration.getCairoConfiguration().getWalDataAppendPageSize());
        Assert.assertEquals(262144, configuration.getCairoConfiguration().getSystemWalDataAppendPageSize());
        Assert.assertTrue(configuration.getCairoConfiguration().isTableTypeConversionEnabled());
        Assert.assertEquals(10, configuration.getCairoConfiguration().getWalWriterPoolMaxSegments());

        Assert.assertEquals(20, configuration.getCairoConfiguration().getO3LastPartitionMaxSplits());
        Assert.assertEquals(50 * Numbers.SIZE_1MB, configuration.getCairoConfiguration().getPartitionO3SplitMinSize());
    }

    @Test
    public void testCommitIntervalDefault() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("line.tcp.commit.interval.default", "0");
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PropServerConfiguration.COMMIT_INTERVAL_DEFAULT, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());

        properties.setProperty("line.tcp.commit.interval.default", "-1");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PropServerConfiguration.COMMIT_INTERVAL_DEFAULT, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());

        properties.setProperty("line.tcp.commit.interval.default", "1000");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(1000, configuration.getLineTcpReceiverConfiguration().getCommitIntervalDefault());
    }

    @Test
    public void testDefaultAddColumnTypeForFloat() throws Exception {
        Properties properties = new Properties();

        // default
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // empty
        properties.setProperty("line.float.default.column.type", "");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // double
        properties.setProperty("line.float.default.column.type", "DOUBLE");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // float
        properties.setProperty("line.float.default.column.type", "FLOAT");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.FLOAT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // lowercase
        properties.setProperty("line.float.default.column.type", "double");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // camel case
        properties.setProperty("line.float.default.column.type", "Float");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.FLOAT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // not allowed
        properties.setProperty("line.float.default.column.type", "STRING");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // not allowed
        properties.setProperty("line.float.default.column.type", "SHORT");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());

        // nonexistent type
        properties.setProperty("line.float.default.column.type", "FLAT");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.DOUBLE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());
    }

    @Test
    public void testDefaultAddColumnTypeForInteger() throws Exception {
        Properties properties = new Properties();

        // default
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // empty
        properties.setProperty("line.integer.default.column.type", "");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // long
        properties.setProperty("line.integer.default.column.type", "LONG");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // int
        properties.setProperty("line.integer.default.column.type", "INT");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.INT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // short
        properties.setProperty("line.integer.default.column.type", "SHORT");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.SHORT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // byte
        properties.setProperty("line.integer.default.column.type", "BYTE");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.BYTE, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // lowercase
        properties.setProperty("line.integer.default.column.type", "int");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.INT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // camel case
        properties.setProperty("line.integer.default.column.type", "Short");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.SHORT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // not allowed
        properties.setProperty("line.integer.default.column.type", "SYMBOL");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // not allowed
        properties.setProperty("line.integer.default.column.type", "FLOAT");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());

        // nonexistent type
        properties.setProperty("line.integer.default.column.type", "BITE");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(ColumnType.LONG, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());
    }

    @Test
    public void testDeprecatedConfigKeys() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("config.validation.strict", "true");
        properties.setProperty("http.min.bind.to", "0.0.0.0:0");

        // Using deprecated settings will not throw an exception, despite validation enabled.
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
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
        env.put("QDB_SHARED_WORKER_AFFINITY", "5,6,7");

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

        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, env, new BuildInformationHolder());
        Assert.assertEquals(1.5, configuration.getCairoConfiguration().getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000001);
        Assert.assertEquals(3000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
        Assert.assertEquals("2.0 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());
        Assert.assertEquals(3, configuration.getWorkerPoolConfiguration().getWorkerCount());
        Assert.assertArrayEquals(new int[]{5, 6, 7}, configuration.getWorkerPoolConfiguration().getWorkerAffinity());
        Assert.assertEquals(12288, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSendBufferSize());
        Assert.assertEquals(900, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
        Assert.assertEquals(9663676416L, configuration.getCairoConfiguration().getDataAppendPageSize());
        Assert.assertEquals(60_000, configuration.getCairoConfiguration().getO3MaxLag());
    }

    @Test
    public void testHttpDisabled() throws Exception {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server-http-disabled.conf")) {
            Properties properties = new Properties();
            properties.load(is);
            PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.assertFalse(configuration.getHttpServerConfiguration().isEnabled());
        }
    }

    @Test
    public void testILPMsgBufferSizeAdjustment() throws Exception {
        Properties properties = new Properties();

        properties.setProperty(PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.LINE_TCP_MSG_BUFFER_SIZE.getPropertyPath(), "8192");
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(8192, configuration.getLineTcpReceiverConfiguration().getNetMsgBufferSize());

        properties.setProperty(PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.LINE_TCP_MSG_BUFFER_SIZE.getPropertyPath(), "1024");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getNetMsgBufferSize());

        // if the msg buffer size is smaller than the max measurement size,
        // then msg buffer size is adjusted to have enough space at least for a single measurement
        properties.setProperty(PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.LINE_TCP_MSG_BUFFER_SIZE.getPropertyPath(), "256");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getNetMsgBufferSize());
    }

    @Test
    public void testImportWorkRootCantBeTheSameAsOtherInstanceDirectories() throws Exception {
        Properties properties = new Properties();

        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertNull(configuration.getCairoConfiguration().getSqlCopyInputWorkRoot());

        //direct cases
        assertInputWorkRootCantBeSetTo(properties, root);
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getRoot());
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getSnapshotRoot().toString());
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getConfRoot().toString());

        //relative cases
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(root));
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(configuration.getCairoConfiguration().getRoot()));
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(configuration.getCairoConfiguration().getSnapshotRoot().toString()));
        assertInputWorkRootCantBeSetTo(properties, getRelativePath(configuration.getCairoConfiguration().getConfRoot().toString()));
    }

    @Test
    public void testImportWorkRootCantBeTheSameAsOtherInstanceDirectories2() throws Exception {
        Assume.assumeTrue(Os.isWindows());

        Properties properties = new Properties();

        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertNull(configuration.getCairoConfiguration().getSqlCopyInputWorkRoot());
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getRoot().toUpperCase());
        assertInputWorkRootCantBeSetTo(properties, configuration.getCairoConfiguration().getRoot().toLowerCase());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToAddress() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6:8990");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToMissingColon() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToPort() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1:");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidConfigKeys() throws Exception {
        try (InputStream inputStream = PropServerConfigurationTest.class.getResourceAsStream("/server.conf")) {
            Properties properties = new Properties();
            properties.load(inputStream);
            properties.setProperty("this.will.throw", "Test");
            properties.setProperty("this.will.also", "throw");

            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        }
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidDouble() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.text.max.required.delimiter.stddev", "abc");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIPv4Address() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("line.udp.join", "12a.990.00");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidInt() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.connection.string.pool.capacity", "1234a");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIntSize() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.request.header.buffer.size", "22g");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidLong() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("cairo.idle.check.interval", "1234a");
        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
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
    public void testLineUdpTimestamp() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("http.enabled", "false");
        properties.setProperty("line.udp.timestamp", "");
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertSame(LineNanoTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "n");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertSame(LineNanoTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "u");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertSame(LineMicroTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "ms");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertSame(LineMilliTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "s");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertSame(LineSecondTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "m");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertSame(LineMinuteTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "h");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertSame(LineHourTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());
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
                newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
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
                newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
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
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
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
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.tcp.default.partition.by", "YEAR");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PartitionBy.YEAR, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "WEEK");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PartitionBy.WEEK, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.WEEK, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "MONTH");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PartitionBy.MONTH, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.MONTH, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "DAY");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());

        properties.setProperty("line.default.partition.by", "YEAR");
        properties.setProperty("line.tcp.default.partition.by", "MONTH");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(PartitionBy.YEAR, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(PartitionBy.YEAR, configuration.getLineUdpReceiverConfiguration().getDefaultPartitionBy());
    }

    @Test
    public void testSetAllFromFile() throws Exception {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            FilesFacade ff = configuration.getCairoConfiguration().getFilesFacade();
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());
            Assert.assertEquals(512, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
            Assert.assertEquals(256, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartHeaderBufferSize());
            Assert.assertEquals(100_000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
            Assert.assertEquals(4096, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRecvBufferSize());
            Assert.assertEquals(2048, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRequestHeaderBufferSize());
            Assert.assertEquals(6, configuration.getHttpServerConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2, 3, 4, 5, 6}, configuration.getHttpServerConfiguration().getWorkerAffinity());
            Assert.assertTrue(configuration.getHttpServerConfiguration().haltOnError());
            Assert.assertEquals(128, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSendBufferSize());
            Assert.assertEquals("index2.html", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getIndexFileName());
            Assert.assertFalse(configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().isAuthenticationRequired());
            Assert.assertFalse(configuration.getHttpServerConfiguration().isQueryCacheEnabled());
            Assert.assertEquals(32, configuration.getHttpServerConfiguration().getQueryCacheBlockCount());
            Assert.assertEquals(16, configuration.getHttpServerConfiguration().getQueryCacheRowCount());

            Assert.assertTrue(configuration.getHttpServerConfiguration().isPessimisticHealthCheckEnabled());
            Assert.assertFalse(configuration.getHttpServerConfiguration().isHealthCheckAuthenticationRequired());
            Assert.assertTrue(configuration.getHttpMinServerConfiguration().isPessimisticHealthCheckEnabled());
            Assert.assertFalse(configuration.getHttpMinServerConfiguration().isHealthCheckAuthenticationRequired());

            Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
            Assert.assertEquals(50000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getMaxQueryResponseRowLimit());
            Assert.assertFalse(configuration.getCairoConfiguration().getCircuitBreakerConfiguration().isEnabled());
            Assert.assertEquals(500, configuration.getCairoConfiguration().getCircuitBreakerConfiguration().getCircuitBreakerThrottle());
            Assert.assertEquals(16, configuration.getCairoConfiguration().getCircuitBreakerConfiguration().getBufferSize());

            Assert.assertEquals(100, configuration.getWorkerPoolConfiguration().getYieldThreshold());
            Assert.assertEquals(100000, configuration.getWorkerPoolConfiguration().getSleepThreshold());
            Assert.assertEquals(1000, configuration.getWorkerPoolConfiguration().getSleepTimeout());

            Assert.assertEquals(101, configuration.getHttpServerConfiguration().getYieldThreshold());
            Assert.assertEquals(100001, configuration.getHttpServerConfiguration().getSleepThreshold());
            Assert.assertEquals(1001, configuration.getHttpServerConfiguration().getSleepTimeout());

            Assert.assertEquals(102, configuration.getHttpMinServerConfiguration().getYieldThreshold());
            Assert.assertEquals(100002, configuration.getHttpMinServerConfiguration().getSleepThreshold());
            Assert.assertEquals(1002, configuration.getHttpMinServerConfiguration().getSleepTimeout());
            Assert.assertEquals(16, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(4, configuration.getHttpMinServerConfiguration().getWorkerCount());

            Assert.assertEquals(
                    new File(root, "public_ok").getAbsolutePath(),
                    configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory()
            );

            Assert.assertEquals("Keep-Alive: timeout=10, max=50000" + Misc.EOL, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());
            Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().allowDeflateBeforeSend());

            Assert.assertEquals(63, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getLimit());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEventCapacity());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
            Assert.assertEquals(7000000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getTimeout());
            Assert.assertEquals(1001, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getQueueTimeout());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
            Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInitialBias());
            Assert.assertEquals(63, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getListenBacklog());
            Assert.assertEquals(4194304, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSndBufSize());
            Assert.assertEquals(8388608, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
            Assert.assertEquals(16, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(32, configuration.getCairoConfiguration().getTextConfiguration().getDateAdapterPoolCapacity());
            Assert.assertEquals(65536, configuration.getCairoConfiguration().getTextConfiguration().getJsonCacheLimit());
            Assert.assertEquals(8388608, configuration.getCairoConfiguration().getTextConfiguration().getJsonCacheSize());
            Assert.assertEquals(0.3d, configuration.getCairoConfiguration().getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000000001);
            Assert.assertEquals(0.9d, configuration.getCairoConfiguration().getTextConfiguration().getMaxRequiredLineLengthStdDev(), 0.000000001);
            Assert.assertEquals(512, configuration.getCairoConfiguration().getTextConfiguration().getMetadataStringPoolCapacity());
            Assert.assertEquals(6144, configuration.getCairoConfiguration().getTextConfiguration().getRollBufferLimit());
            Assert.assertEquals(3072, configuration.getCairoConfiguration().getTextConfiguration().getRollBufferSize());
            Assert.assertEquals(400, configuration.getCairoConfiguration().getTextConfiguration().getTextAnalysisMaxLines());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getTextConfiguration().getTextLexerStringPoolCapacity());
            Assert.assertEquals(512, configuration.getCairoConfiguration().getTextConfiguration().getTimestampAdapterPoolCapacity());
            Assert.assertEquals(8192, configuration.getCairoConfiguration().getTextConfiguration().getUtf8SinkSize());
            Assert.assertEquals(168101918, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindIPv4Address());
            Assert.assertEquals(9900, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindPort());
            Assert.assertEquals(2_000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getConnectionCheckFrequency());
            Assert.assertEquals(4, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getFloatScale());
            Assert.assertEquals(4194304, configuration.getCairoConfiguration().getSqlCopyBufferSize());
            Assert.assertEquals(64, configuration.getCairoConfiguration().getCopyPoolCapacity());
            Assert.assertSame(FilesFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getFilesFacade());
            Assert.assertEquals("Keep-Alive: timeout=10, max=50000" + Misc.EOL, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getKeepAliveHeader());
            Assert.assertEquals(8, configuration.getCairoConfiguration().getDoubleToStrCastScale());
            Assert.assertEquals(3, configuration.getCairoConfiguration().getFloatToStrCastScale());
            Assert.assertEquals("test-id-42", configuration.getCairoConfiguration().getSnapshotInstanceId());
            Assert.assertFalse(configuration.getCairoConfiguration().isSnapshotRecoveryEnabled());

            Assert.assertEquals(CommitMode.ASYNC, configuration.getCairoConfiguration().getCommitMode());
            Assert.assertEquals(12, configuration.getCairoConfiguration().getCreateAsSelectRetryCount());
            Assert.assertEquals("compact", configuration.getCairoConfiguration().getDefaultMapType());
            Assert.assertTrue(configuration.getCairoConfiguration().getDefaultSymbolCacheFlag());
            Assert.assertEquals(512, configuration.getCairoConfiguration().getDefaultSymbolCapacity());
            Assert.assertEquals(10, configuration.getCairoConfiguration().getFileOperationRetryCount());
            Assert.assertEquals(20_000, configuration.getCairoConfiguration().getIdleCheckInterval());
            Assert.assertEquals(42, configuration.getCairoConfiguration().getInactiveReaderMaxOpenPartitions());
            Assert.assertEquals(600_000, configuration.getCairoConfiguration().getInactiveReaderTTL());
            Assert.assertEquals(400_000, configuration.getCairoConfiguration().getInactiveWriterTTL());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getIndexValueBlockSize());
            Assert.assertEquals(23, configuration.getCairoConfiguration().getMaxSwapFileCount());
            Assert.assertEquals(509, configuration.getCairoConfiguration().getMkDirMode());
            Assert.assertEquals(509, configuration.getCairoConfiguration().getDetachedMkDirMode());
            Assert.assertEquals(1000000, configuration.getCairoConfiguration().getParallelIndexThreshold());
            Assert.assertEquals(42, configuration.getCairoConfiguration().getReaderPoolMaxSegments());
            Assert.assertEquals(5_000_000, configuration.getCairoConfiguration().getSpinLockTimeout());
            Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlCharacterStoreCapacity());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getSqlCharacterStoreSequencePoolCapacity());
            Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlColumnPoolCapacity());
            Assert.assertEquals(0.8, configuration.getCairoConfiguration().getSqlCompactMapLoadFactor(), 0.000001);
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlExpressionPoolCapacity());
            Assert.assertEquals(0.3, configuration.getCairoConfiguration().getSqlFastMapLoadFactor(), 0.0000001);
            Assert.assertEquals(32, configuration.getCairoConfiguration().getSqlJoinContextPoolCapacity());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlLexerPoolCapacity());
            Assert.assertEquals(32, configuration.getCairoConfiguration().getSqlSmallMapKeyCapacity());
            Assert.assertEquals(42 * 1024, configuration.getCairoConfiguration().getSqlSmallMapPageSize());
            Assert.assertEquals(1026, configuration.getCairoConfiguration().getSqlMapMaxPages());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getSqlMapMaxResizes());
            Assert.assertEquals(256, configuration.getCairoConfiguration().getSqlModelPoolCapacity());
            Assert.assertEquals(42, configuration.getCairoConfiguration().getSqlMaxNegativeLimit());
            Assert.assertEquals(10 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortKeyPageSize());
            Assert.assertEquals(256, configuration.getCairoConfiguration().getSqlSortKeyMaxPages());
            Assert.assertEquals(3 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortLightValuePageSize());
            Assert.assertEquals(1027, configuration.getCairoConfiguration().getSqlSortLightValueMaxPages());
            Assert.assertEquals(8 * 1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinValuePageSize());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlHashJoinValueMaxPages());
            Assert.assertEquals(10000, configuration.getCairoConfiguration().getSqlLatestByRowCount());
            Assert.assertEquals(2 * 1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinLightValuePageSize());
            Assert.assertEquals(1025, configuration.getCairoConfiguration().getSqlHashJoinLightValueMaxPages());
            Assert.assertEquals(4 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortValuePageSize());
            Assert.assertEquals(1028, configuration.getCairoConfiguration().getSqlSortValueMaxPages());
            Assert.assertEquals(1000000, configuration.getCairoConfiguration().getWorkStealTimeoutNanos());
            Assert.assertFalse(configuration.getCairoConfiguration().isParallelIndexingEnabled());
            Assert.assertEquals(8 * 1024, configuration.getCairoConfiguration().getSqlJoinMetadataPageSize());
            Assert.assertEquals(10_000, configuration.getCairoConfiguration().getSqlJoinMetadataMaxResizes());
            Assert.assertEquals(16, configuration.getCairoConfiguration().getBindVariablePoolSize());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getCountDistinctCapacity());
            Assert.assertEquals(0.3, configuration.getCairoConfiguration().getCountDistinctLoadFactor(), 0.000001);

            Assert.assertEquals(256, configuration.getCairoConfiguration().getWindowColumnPoolCapacity());
            Assert.assertEquals(256, configuration.getCairoConfiguration().getSqlWindowMaxRecursion());
            Assert.assertEquals(512 * 1024, configuration.getCairoConfiguration().getSqlWindowTreeKeyPageSize());
            Assert.assertEquals(1031, configuration.getCairoConfiguration().getSqlWindowTreeKeyMaxPages());
            Assert.assertEquals(1024 * 1024, configuration.getCairoConfiguration().getSqlWindowStorePageSize());
            Assert.assertEquals(1029, configuration.getCairoConfiguration().getSqlWindowStoreMaxPages());
            Assert.assertEquals(524288, configuration.getCairoConfiguration().getSqlWindowRowIdPageSize());
            Assert.assertEquals(1030, configuration.getCairoConfiguration().getSqlWindowRowIdMaxPages());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getWithClauseModelPoolCapacity());
            Assert.assertEquals(512, configuration.getCairoConfiguration().getRenameTableModelPoolCapacity());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getInsertPoolCapacity());
            Assert.assertEquals(256, configuration.getCairoConfiguration().getColumnCastModelPoolCapacity());
            Assert.assertEquals(64, configuration.getCairoConfiguration().getCreateTableModelPoolCapacity());
            Assert.assertEquals(2001, configuration.getCairoConfiguration().getSampleByIndexSearchPageSize());
            Assert.assertEquals(16, configuration.getCairoConfiguration().getWriterCommandQueueCapacity());
            Assert.assertEquals(4096, configuration.getCairoConfiguration().getWriterCommandQueueSlotSize());
            Assert.assertEquals(333000, configuration.getCairoConfiguration().getWriterAsyncCommandBusyWaitTimeout());
            Assert.assertEquals(7770001, configuration.getCairoConfiguration().getWriterAsyncCommandMaxTimeout());
            Assert.assertEquals(15, configuration.getCairoConfiguration().getWriterTickRowsCountMod());
            Assert.assertEquals(ff.allowMixedIO(root), configuration.getCairoConfiguration().isWriterMixedIOEnabled());
            Assert.assertEquals(CairoConfiguration.O_DIRECT | CairoConfiguration.O_SYNC, configuration.getCairoConfiguration().getWriterFileOpenOpts());
            Assert.assertFalse(configuration.getCairoConfiguration().isIOURingEnabled());

            Assert.assertEquals(100_000, configuration.getCairoConfiguration().getMaxUncommittedRows());
            Assert.assertEquals(42_000_000, configuration.getCairoConfiguration().getO3MinLag());
            Assert.assertEquals(420_000_000, configuration.getCairoConfiguration().getO3MaxLag());
            Assert.assertEquals(262144, configuration.getCairoConfiguration().getO3ColumnMemorySize());
            Assert.assertEquals(65536, configuration.getCairoConfiguration().getSystemO3ColumnMemorySize());

            Assert.assertEquals(256, configuration.getCairoConfiguration().getSqlDistinctTimestampKeyCapacity());
            Assert.assertEquals(0.4, configuration.getCairoConfiguration().getSqlDistinctTimestampLoadFactor(), 0.001);

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

            Assert.assertFalse(configuration.getCairoConfiguration().isSqlParallelFilterEnabled());
            Assert.assertFalse(configuration.getCairoConfiguration().isSqlParallelFilterPreTouchEnabled());
            Assert.assertFalse(configuration.getCairoConfiguration().isSqlParallelGroupByEnabled());
            Assert.assertEquals(1000, configuration.getCairoConfiguration().getSqlPageFrameMaxRows());
            Assert.assertEquals(100, configuration.getCairoConfiguration().getSqlPageFrameMinRows());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getPageFrameReduceShardCount());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getPageFrameReduceQueueCapacity());
            Assert.assertEquals(8, configuration.getCairoConfiguration().getPageFrameReduceRowIdListCapacity());
            Assert.assertEquals(4, configuration.getCairoConfiguration().getPageFrameReduceColumnListCapacity());
            Assert.assertEquals(2048, configuration.getCairoConfiguration().getGroupByMergeShardQueueCapacity());
            Assert.assertEquals(100, configuration.getCairoConfiguration().getGroupByShardingThreshold());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getGroupByShardCount());

            Assert.assertEquals(SqlJitMode.JIT_MODE_FORCE_SCALAR, configuration.getCairoConfiguration().getSqlJitMode());
            Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlJitIRMemoryPageSize());
            Assert.assertEquals(2, configuration.getCairoConfiguration().getSqlJitIRMemoryMaxPages());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlJitBindVarsMemoryPageSize());
            Assert.assertEquals(1, configuration.getCairoConfiguration().getSqlJitBindVarsMemoryMaxPages());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlJitPageAddressCacheThreshold());
            Assert.assertTrue(configuration.getCairoConfiguration().isSqlJitDebugEnabled());

            Assert.assertEquals(16384, configuration.getCairoConfiguration().getRndFunctionMemoryPageSize());
            Assert.assertEquals(32, configuration.getCairoConfiguration().getRndFunctionMemoryMaxPages());

            // influxdb line TCP protocol
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().isEnabled());
            Assert.assertEquals(11, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getLimit());
            Assert.assertEquals(167903521, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getBindIPv4Address());
            Assert.assertEquals(9916, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getBindPort());
            Assert.assertEquals(16, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getEventCapacity());
            Assert.assertEquals(16, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
            Assert.assertEquals(400_000, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getTimeout());
            Assert.assertEquals(1_002, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getQueueTimeout());
            Assert.assertEquals(16, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
            Assert.assertEquals(11, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getListenBacklog());
            Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getRcvBufSize());
            Assert.assertEquals(16, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(32, configuration.getLineTcpReceiverConfiguration().getConnectionPoolInitialCapacity());
            Assert.assertEquals(LineMicroTimestampAdapter.INSTANCE, configuration.getLineTcpReceiverConfiguration().getTimestampAdapter().getDefaultAdapter());
            Assert.assertEquals(2049, configuration.getLineTcpReceiverConfiguration().getNetMsgBufferSize());
            Assert.assertEquals(128, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
            Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getWriterQueueCapacity());
            Assert.assertEquals(2, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2}, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(20, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getYieldThreshold());
            Assert.assertEquals(10_002, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getSleepThreshold());
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().haltOnError());
            Assert.assertEquals(3, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{3, 4, 5}, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(30, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getYieldThreshold());
            Assert.assertEquals(10_003, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getSleepThreshold());
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().haltOnError());
            Assert.assertEquals(1000, configuration.getLineTcpReceiverConfiguration().getMaintenanceInterval());
            Assert.assertEquals(PartitionBy.MONTH, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
            Assert.assertEquals(5_000, configuration.getLineTcpReceiverConfiguration().getWriterIdleTimeout());
            Assert.assertEquals(16, configuration.getCairoConfiguration().getPartitionPurgeListCapacity());
            Assert.assertEquals(ColumnType.FLOAT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForFloat());
            Assert.assertEquals(ColumnType.INT, configuration.getLineTcpReceiverConfiguration().getDefaultColumnTypeForInteger());
            Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getDisconnectOnError());

            Assert.assertTrue(configuration.getCairoConfiguration().getTelemetryConfiguration().getEnabled());
            Assert.assertEquals(512, configuration.getCairoConfiguration().getTelemetryConfiguration().getQueueCapacity());

            Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getServerKeepAlive());
            Assert.assertEquals("HTTP/1.0 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());
            Assert.assertEquals(1048576, configuration.getCairoConfiguration().getDataAppendPageSize());
            Assert.assertEquals(131072, configuration.getCairoConfiguration().getSystemDataAppendPageSize());
            Assert.assertEquals(Files.PAGE_SIZE, configuration.getCairoConfiguration().getDataIndexKeyAppendPageSize());
            Assert.assertEquals(262144, configuration.getCairoConfiguration().getDataIndexValueAppendPageSize());
            Assert.assertEquals(131072, configuration.getCairoConfiguration().getMiscAppendPageSize());
            Assert.assertEquals(1.5, configuration.getHttpServerConfiguration().getWaitProcessorConfiguration().getExponentialWaitMultiplier(), 0.00001);

            Assert.assertTrue(configuration.getMetricsConfiguration().isEnabled());

            Assert.assertEquals(512, configuration.getCairoConfiguration().getColumnPurgeQueueCapacity());
            Assert.assertEquals(5.0, configuration.getCairoConfiguration().getColumnPurgeRetryDelayMultiplier(), 0.00001);
            Assert.assertEquals(30000000, configuration.getCairoConfiguration().getColumnPurgeRetryDelayLimit());
            Assert.assertEquals(30000, configuration.getCairoConfiguration().getColumnPurgeRetryDelay());

            // PG wire
            Assert.assertEquals(9, configuration.getPGWireConfiguration().getBinParamCountCapacity());
            Assert.assertFalse(configuration.getPGWireConfiguration().isSelectCacheEnabled());
            Assert.assertEquals(1, configuration.getPGWireConfiguration().getSelectCacheBlockCount());
            Assert.assertEquals(2, configuration.getPGWireConfiguration().getSelectCacheRowCount());
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
            Assert.assertEquals(16, configuration.getPGWireConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
            Assert.assertEquals(new DefaultPGWireConfiguration().getServerVersion(), configuration.getPGWireConfiguration().getServerVersion());

            Assert.assertEquals(255, configuration.getCairoConfiguration().getMaxFileNameLength());
            Assert.assertEquals(255, configuration.getLineTcpReceiverConfiguration().getMaxFileNameLength());
            Assert.assertEquals(255, configuration.getLineUdpReceiverConfiguration().getMaxFileNameLength());

            Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getAutoCreateNewColumns());
            Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().getAutoCreateNewColumns());
            Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getAutoCreateNewTables());
            Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().getAutoCreateNewTables());

            Assert.assertEquals(".detached", configuration.getCairoConfiguration().getAttachPartitionSuffix());
            Assert.assertTrue(configuration.getCairoConfiguration().attachPartitionCopy());

            Assert.assertEquals(333, configuration.getCairoConfiguration().getWalPurgeInterval());
            Assert.assertEquals(13, configuration.getCairoConfiguration().getWalRecreateDistressedSequencerAttempts());
            Assert.assertEquals(333303, configuration.getCairoConfiguration().getInactiveWalWriterTTL());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getWalTxnNotificationQueueCapacity());
            Assert.assertTrue(configuration.getCairoConfiguration().isWalSupported());
            Assert.assertTrue(configuration.getCairoConfiguration().getWalEnabledDefault());
            Assert.assertFalse(configuration.getCairoConfiguration().isWalApplyEnabled());
            Assert.assertTrue(configuration.getWalApplyPoolConfiguration().isEnabled());
            Assert.assertTrue(configuration.getWalApplyPoolConfiguration().haltOnError());
            Assert.assertEquals("wal-apply", configuration.getWalApplyPoolConfiguration().getPoolName());
            Assert.assertEquals(3, configuration.getWalApplyPoolConfiguration().getWorkerCount());
            Assert.assertArrayEquals(new int[]{1, 2, 3}, configuration.getWalApplyPoolConfiguration().getWorkerAffinity());
            Assert.assertEquals(55, configuration.getWalApplyPoolConfiguration().getSleepTimeout());
            Assert.assertEquals(33, configuration.getWalApplyPoolConfiguration().getSleepThreshold());
            Assert.assertEquals(33033, configuration.getWalApplyPoolConfiguration().getYieldThreshold());
            Assert.assertEquals(23, configuration.getCairoConfiguration().getWalApplyLookAheadTransactionCount());
            Assert.assertFalse(configuration.getCairoConfiguration().isTableTypeConversionEnabled());
            Assert.assertEquals(100, configuration.getCairoConfiguration().getWalWriterPoolMaxSegments());
            Assert.assertEquals(120, configuration.getCairoConfiguration().getO3LagCalculationWindowsSize());
            Assert.assertEquals(100, configuration.getCairoConfiguration().getWalSegmentRolloverRowCount());
            Assert.assertEquals(42.2d, configuration.getCairoConfiguration().getWalSquashUncommittedRowsMultiplier(), 0.00001);
            Assert.assertEquals(4242, configuration.getCairoConfiguration().getWalMaxLagTxnCount());
            Assert.assertEquals(262144, configuration.getCairoConfiguration().getWalDataAppendPageSize());
            Assert.assertEquals(524288, configuration.getCairoConfiguration().getSystemWalDataAppendPageSize());

            Assert.assertEquals(1, configuration.getCairoConfiguration().getO3LastPartitionMaxSplits());
            final long TB = (long) Numbers.SIZE_1MB * Numbers.SIZE_1MB;
            Assert.assertEquals(TB, configuration.getCairoConfiguration().getPartitionO3SplitMinSize());

            Assert.assertEquals(10 * Numbers.SIZE_1MB, configuration.getCairoConfiguration().getWalMaxLagSize());
            Assert.assertEquals(50, configuration.getCairoConfiguration().getWalMaxSegmentFileDescriptorsCache());
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

            PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());

            Assert.assertEquals(9020, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindPort());
            Assert.assertEquals(63, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getLimit());
            Assert.assertEquals(7000000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getTimeout());
            Assert.assertEquals(1001, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getQueueTimeout());
            Assert.assertEquals(4194304, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSndBufSize());
            Assert.assertEquals(8388608, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
            Assert.assertTrue(configuration.getHttpServerConfiguration().getDispatcherConfiguration().getHint());

            Assert.assertEquals(9120, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getBindPort());
            Assert.assertEquals(8, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getLimit());
            Assert.assertEquals(7000000, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getTimeout());
            Assert.assertEquals(1001, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getQueueTimeout());
            Assert.assertEquals(33554432, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getSndBufSize());
            Assert.assertEquals(16777216, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
            Assert.assertEquals(64, configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getTestConnectionBufferSize());
            Assert.assertTrue(configuration.getHttpMinServerConfiguration().getDispatcherConfiguration().getHint());

            // influxdb line TCP protocol
            Assert.assertEquals(11, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getLimit());
            Assert.assertEquals(400_000, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getTimeout());
            Assert.assertEquals(1_002, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getQueueTimeout());
            Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getRcvBufSize());
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration().getHint());

            // Pg wire
            Assert.assertEquals(11, configuration.getPGWireConfiguration().getDispatcherConfiguration().getLimit());
            Assert.assertEquals(400000, configuration.getPGWireConfiguration().getDispatcherConfiguration().getTimeout());
            Assert.assertEquals(1002, configuration.getPGWireConfiguration().getDispatcherConfiguration().getQueueTimeout());
            Assert.assertEquals(32768, configuration.getPGWireConfiguration().getDispatcherConfiguration().getRcvBufSize());
            Assert.assertEquals(32800, configuration.getPGWireConfiguration().getDispatcherConfiguration().getSndBufSize());
            Assert.assertTrue(configuration.getPGWireConfiguration().getDispatcherConfiguration().getHint());
        }
    }

    @Test
    public void testSetZeroKeepAlive() throws Exception {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server-keep-alive.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.assertNull(configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());
        }
    }

    @Test
    public void testSqlJitMode() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("cairo.sql.jit.mode", "");
        PropServerConfiguration configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(SqlJitMode.JIT_MODE_ENABLED, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "on");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(SqlJitMode.JIT_MODE_ENABLED, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "scalar");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(SqlJitMode.JIT_MODE_FORCE_SCALAR, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "off");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(SqlJitMode.JIT_MODE_DISABLED, configuration.getCairoConfiguration().getSqlJitMode());

        properties.setProperty("cairo.sql.jit.mode", "foobar");
        configuration = newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
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
                CairoConfiguration cairoConfig = newPropServerConfiguration(root, properties, null, new BuildInformationHolder()).getCairoConfiguration();

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
        CairoConfiguration cairoConfig = newPropServerConfiguration(root, properties, null, new BuildInformationHolder()).getCairoConfiguration();
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

        newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
    }

    private void assertInputWorkRootCantBeSetTo(Properties properties, String value) throws JsonException {
        try {
            properties.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT.getPropertyPath(), value);
            properties.setProperty(PropertyKey.CAIRO_SQL_COPY_WORK_ROOT.getPropertyPath(), value);
            newPropServerConfiguration(root, properties, null, new BuildInformationHolder());
            Assert.fail("Should fail for " + value);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "cairo.sql.copy.work.root can't point to root, data, conf or snapshot dirs");
        }
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

    private PropServerConfiguration.ValidationResult validate(Properties properties) {
        return new PropServerConfiguration.PropertyValidator().validate(properties);
    }

    @NotNull
    protected PropServerConfiguration newPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        return new PropServerConfiguration(root, properties, env, PropServerConfigurationTest.LOG, buildInformation);
    }
}
