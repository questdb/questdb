/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb;

import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.SelectFacadeImpl;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.test.tools.TestUtils;
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
    private final static Log LOG = LogFactory.getLog(PropServerConfigurationTest.class);
    private static String root;

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
    public void testAllDefaults() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        PropServerConfiguration configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertEquals(16, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());
        Assert.assertEquals(128, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
        Assert.assertEquals(512, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartHeaderBufferSize());
        Assert.assertEquals(10_000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
        Assert.assertEquals(1048576, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRecvBufferSize());
        Assert.assertEquals(64448, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getRequestHeaderBufferSize());
        Assert.assertFalse(configuration.getHttpServerConfiguration().haltOnError());
        Assert.assertFalse(configuration.getHttpServerConfiguration().haltOnError());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSendBufferSize());
        Assert.assertEquals("index.html", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getIndexFileName());
        Assert.assertTrue(configuration.getHttpServerConfiguration().isEnabled());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getDumpNetworkTraffic());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().allowDeflateBeforeSend());
        Assert.assertEquals(16, configuration.getHttpServerConfiguration().getQueryCacheRows());
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getQueryCacheBlocks());

        // this is going to need interesting validation logic
        // configuration path is expected to be relative and we need to check if absolute path is good
        Assert.assertEquals(new File(root, "public").getAbsolutePath(),
                configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory());

        Assert.assertEquals("Keep-Alive: timeout=5, max=10000" + Misc.EOL, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());

        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getActiveConnectionLimit());
        Assert.assertEquals(1024, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEventCapacity());
        Assert.assertEquals(1024, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
        Assert.assertEquals(300000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIdleConnectionTimeout());
        Assert.assertEquals(5000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getQueuedConnectionTimeout());
        Assert.assertEquals(1024, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
        Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInitialBias());
        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getListenBacklog());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSndBufSize());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
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

        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
        Assert.assertEquals(Long.MAX_VALUE, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getMaxQueryResponseRowLimit());
        Assert.assertTrue(configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getInterruptorConfiguration().isEnabled());
        Assert.assertEquals(2_000_000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getInterruptorConfiguration().getCountOfIterationsPerCheck());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getInterruptorConfiguration().getBufferSize());

        Assert.assertEquals(CommitMode.NOSYNC, configuration.getCairoConfiguration().getCommitMode());
        Assert.assertEquals(2097152, configuration.getCairoConfiguration().getSqlCopyBufferSize());
        Assert.assertEquals(32, configuration.getCairoConfiguration().getCopyPoolCapacity());
        Assert.assertEquals(5, configuration.getCairoConfiguration().getCreateAsSelectRetryCount());
        Assert.assertEquals("fast", configuration.getCairoConfiguration().getDefaultMapType());
        Assert.assertTrue(configuration.getCairoConfiguration().getDefaultSymbolCacheFlag());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getDefaultSymbolCapacity());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getFileOperationRetryCount());
        Assert.assertEquals(300000, configuration.getCairoConfiguration().getIdleCheckInterval());
        Assert.assertEquals(120_000, configuration.getCairoConfiguration().getInactiveReaderTTL());
        Assert.assertEquals(600_000, configuration.getCairoConfiguration().getInactiveWriterTTL());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getIndexValueBlockSize());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getMaxSwapFileCount());
        Assert.assertEquals(509, configuration.getCairoConfiguration().getMkDirMode());
        Assert.assertEquals(8, configuration.getCairoConfiguration().getBindVariablePoolSize());

        Assert.assertEquals(100000, configuration.getCairoConfiguration().getParallelIndexThreshold());
        Assert.assertEquals(5, configuration.getCairoConfiguration().getReaderPoolMaxSegments());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getSpinLockTimeoutUs());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlCharacterStoreCapacity());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlCharacterStoreSequencePoolCapacity());
        Assert.assertEquals(4096, configuration.getCairoConfiguration().getSqlColumnPoolCapacity());
        Assert.assertEquals(0.7, configuration.getCairoConfiguration().getSqlCompactMapLoadFactor(), 0.000001);
        Assert.assertEquals(8192, configuration.getCairoConfiguration().getSqlExpressionPoolCapacity());
        Assert.assertEquals(0.5, configuration.getCairoConfiguration().getSqlFastMapLoadFactor(), 0.0000001);
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlJoinContextPoolCapacity());
        Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlLexerPoolCapacity());
        Assert.assertEquals(2097152, configuration.getCairoConfiguration().getSqlMapKeyCapacity());
        Assert.assertEquals(4 * 1024 * 1024, configuration.getCairoConfiguration().getSqlMapPageSize());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlMapMaxPages());
        Assert.assertEquals(Integer.MAX_VALUE, configuration.getCairoConfiguration().getSqlMapMaxResizes());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlModelPoolCapacity());
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
        Assert.assertEquals(64, configuration.getCairoConfiguration().getAnalyticColumnPoolCapacity());
        Assert.assertEquals(128, configuration.getCairoConfiguration().getWithClauseModelPoolCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getRenameTableModelPoolCapacity());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getInsertPoolCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getColumnCastModelPoolCapacity());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getCreateTableModelPoolCapacity());

        Assert.assertEquals(0, configuration.getLineUdpReceiverConfiguration().getBindIPv4Address());
        Assert.assertEquals(9009, configuration.getLineUdpReceiverConfiguration().getPort());
        Assert.assertEquals(-402587133, configuration.getLineUdpReceiverConfiguration().getGroupIPv4Address());

        Assert.assertEquals(1000000, configuration.getLineUdpReceiverConfiguration().getCommitRate());

        Assert.assertEquals(2048, configuration.getLineUdpReceiverConfiguration().getMsgBufferSize());
        Assert.assertEquals(10000, configuration.getLineUdpReceiverConfiguration().getMsgCount());
        Assert.assertEquals(8388608, configuration.getLineUdpReceiverConfiguration().getReceiveBufferSize());
        Assert.assertSame(AllowAllCairoSecurityContext.INSTANCE, configuration.getLineUdpReceiverConfiguration().getCairoSecurityContext());
        Assert.assertTrue(configuration.getLineUdpReceiverConfiguration().isEnabled());
        Assert.assertEquals(-1, configuration.getLineUdpReceiverConfiguration().ownThreadAffinity());
        Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().ownThread());

        // statics
        Assert.assertSame(FilesFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getFilesFacade());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getClock());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getNetworkFacade());
        Assert.assertSame(EpollFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEpollFacade());
        Assert.assertSame(SelectFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSelectFacade());
        Assert.assertSame(FilesFacadeImpl.INSTANCE, configuration.getCairoConfiguration().getFilesFacade());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMillisecondClock());
        Assert.assertSame(MicrosecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMicrosecondClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getLineUdpReceiverConfiguration().getNetworkFacade());
        Assert.assertEquals("http-server", configuration.getHttpServerConfiguration().getDispatcherConfiguration().getDispatcherLogName());

        TestUtils.assertEquals(new File(root, "db").getAbsolutePath(), configuration.getCairoConfiguration().getRoot());

        // assert mime types
        TestUtils.assertEquals("application/json", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getMimeTypesCache().get("json"));

        Assert.assertEquals(500000, configuration.getCairoConfiguration().getMaxUncommittedRows());

        // influxdb line TCP protocol
        Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().isEnabled());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getActiveConnectionLimit());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getBindIPv4Address());
        Assert.assertEquals(9009, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getBindPort());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getEventCapacity());
        Assert.assertEquals(256, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getIOQueueCapacity());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getIdleConnectionTimeout());
        Assert.assertEquals(5000, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getQueuedConnectionTimeout());
        Assert.assertEquals(1024, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getInterestQueueCapacity());
        Assert.assertEquals(50_000, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getListenBacklog());
        Assert.assertEquals(-1, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getRcvBufSize());
        Assert.assertEquals(-1, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getSndBufSize());
        Assert.assertEquals(64, configuration.getLineTcpReceiverConfiguration().getConnectionPoolInitialCapacity());
        Assert.assertEquals(LineProtoNanoTimestampAdapter.INSTANCE, configuration.getLineTcpReceiverConfiguration().getTimestampAdapter());
        Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getNetMsgBufferSize());
        Assert.assertEquals(4096, configuration.getLineTcpReceiverConfiguration().getMaxMeasurementSize());
        Assert.assertEquals(128, configuration.getLineTcpReceiverConfiguration().getWriterQueueCapacity());
        Assert.assertEquals(1, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerCount());
        Assert.assertEquals(10, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getSleepThreshold());
        Assert.assertArrayEquals(new int[]{-1}, configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().getWorkerAffinity());
        Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getWriterWorkerPoolConfiguration().haltOnError());
        Assert.assertEquals(10, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getYieldThreshold());
        Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().getSleepThreshold());
        Assert.assertFalse(configuration.getLineTcpReceiverConfiguration().getIOWorkerPoolConfiguration().haltOnError());
        Assert.assertEquals(10_000_000, configuration.getLineTcpReceiverConfiguration().getNUpdatesPerLoadRebalance());
        Assert.assertEquals(1.9, configuration.getLineTcpReceiverConfiguration().getMaxLoadRatio(), 0.001);
        Assert.assertEquals(30_000, configuration.getLineTcpReceiverConfiguration().getMaintenanceInterval());
        Assert.assertEquals(PartitionBy.DAY, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
        Assert.assertEquals(0, configuration.getLineTcpReceiverConfiguration().getAggressiveReadRetryCount());
        Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getWriterIdleTimeout());

        Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getServerKeepAlive());
        Assert.assertEquals("HTTP/1.1 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());
        Assert.assertEquals(16777216, configuration.getCairoConfiguration().getAppendPageSize());

        Assert.assertEquals("Unknown Version", configuration.getCairoConfiguration().getBuildInformation().getQuestDbVersion());
        Assert.assertEquals("Unknown Version", configuration.getCairoConfiguration().getBuildInformation().getJdkVersion());
        Assert.assertEquals("Unknown Version", configuration.getCairoConfiguration().getBuildInformation().getCommitHash());

        Assert.assertFalse(configuration.getMetricsConfiguration().isEnabled());
    }

    @Test
    public void testEnvOverrides() throws ServerConfigurationException, JsonException {
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
        properties.setProperty("cairo.sql.append.page.size", "3G");
        env.put("QDB_CAIRO_SQL_APPEND_PAGE_SIZE", "9G");

        PropServerConfiguration configuration = new PropServerConfiguration(root, properties, env, LOG, new BuildInformationHolder());
        Assert.assertEquals(1.5, configuration.getCairoConfiguration().getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000001);
        Assert.assertEquals(3000, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionStringPoolCapacity());
        Assert.assertEquals("2.0 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());
        Assert.assertEquals(3, configuration.getWorkerPoolConfiguration().getWorkerCount());
        Assert.assertArrayEquals(new int[]{5, 6, 7}, configuration.getWorkerPoolConfiguration().getWorkerAffinity());
        Assert.assertEquals(12288, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getSendBufferSize());
        Assert.assertEquals(900, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getMultipartIdleSpinCount());
        Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
        Assert.assertEquals(9663676416L, configuration.getCairoConfiguration().getAppendPageSize());
    }

    @Test
    public void testHttpDisabled() throws IOException, ServerConfigurationException, JsonException {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server-http-disabled.conf")) {
            Properties properties = new Properties();
            properties.load(is);
            PropServerConfiguration configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
            Assert.assertFalse(configuration.getHttpServerConfiguration().isEnabled());
        }
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToAddress() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6:8990");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToMissingColon() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToPort() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1:");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidDouble() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("http.text.max.required.delimiter.stddev", "abc");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIPv4Address() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("line.udp.join", "12a.990.00");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidInt() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("http.connection.string.pool.capacity", "1234a");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIntSize() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("http.request.header.buffer.size", "22g");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidLong() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("cairo.idle.check.interval", "1234a");
        new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }

    @Test
    public void testLineUdpTimestamp() throws ServerConfigurationException, JsonException {
        Properties properties = new Properties();
        properties.setProperty("http.enabled", "false");
        properties.setProperty("line.udp.timestamp", "");
        PropServerConfiguration configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertSame(LineProtoNanoTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "n");
        configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertSame(LineProtoNanoTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "u");
        configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertSame(LineProtoMicroTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "ms");
        configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertSame(LineProtoMilliTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "s");
        configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertSame(LineProtoSecondTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "m");
        configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertSame(LineProtoMinuteTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());

        properties.setProperty("line.udp.timestamp", "h");
        configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
        Assert.assertSame(LineProtoHourTimestampAdapter.INSTANCE, configuration.getLineUdpReceiverConfiguration().getTimestampAdapter());
    }

    @Test
    public void testSetAllFromFile() throws IOException, ServerConfigurationException, JsonException {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            PropServerConfiguration configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
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
            Assert.assertEquals(32, configuration.getHttpServerConfiguration().getQueryCacheRows());
            Assert.assertEquals(16, configuration.getHttpServerConfiguration().getQueryCacheBlocks());

            Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext());
            Assert.assertEquals(50000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getMaxQueryResponseRowLimit());
            Assert.assertFalse(configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getInterruptorConfiguration().isEnabled());
            Assert.assertEquals(500, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getInterruptorConfiguration().getCountOfIterationsPerCheck());
            Assert.assertEquals(32, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getInterruptorConfiguration().getBufferSize());

            Assert.assertEquals(new File(root, "public_ok").getAbsolutePath(),
                    configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory());

            Assert.assertEquals("Keep-Alive: timeout=10, max=50000" + Misc.EOL, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());
            Assert.assertTrue(configuration.getHttpServerConfiguration().getHttpContextConfiguration().allowDeflateBeforeSend());

            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getActiveConnectionLimit());
            Assert.assertEquals(2048, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEventCapacity());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
            Assert.assertEquals(7000000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIdleConnectionTimeout());
            Assert.assertEquals(1001, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getQueuedConnectionTimeout());
            Assert.assertEquals(512, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
            Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInitialBias());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getListenBacklog());
            Assert.assertEquals(4194304, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSndBufSize());
            Assert.assertEquals(8388608, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
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

            Assert.assertEquals(CommitMode.ASYNC, configuration.getCairoConfiguration().getCommitMode());
            Assert.assertEquals(12, configuration.getCairoConfiguration().getCreateAsSelectRetryCount());
            Assert.assertEquals("compact", configuration.getCairoConfiguration().getDefaultMapType());
            Assert.assertTrue(configuration.getCairoConfiguration().getDefaultSymbolCacheFlag());
            Assert.assertEquals(512, configuration.getCairoConfiguration().getDefaultSymbolCapacity());
            Assert.assertEquals(10, configuration.getCairoConfiguration().getFileOperationRetryCount());
            Assert.assertEquals(20_000, configuration.getCairoConfiguration().getIdleCheckInterval());
            Assert.assertEquals(600_000, configuration.getCairoConfiguration().getInactiveReaderTTL());
            Assert.assertEquals(400_000, configuration.getCairoConfiguration().getInactiveWriterTTL());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getIndexValueBlockSize());
            Assert.assertEquals(23, configuration.getCairoConfiguration().getMaxSwapFileCount());
            Assert.assertEquals(509, configuration.getCairoConfiguration().getMkDirMode());
            Assert.assertEquals(1000000, configuration.getCairoConfiguration().getParallelIndexThreshold());
            Assert.assertEquals(10, configuration.getCairoConfiguration().getReaderPoolMaxSegments());
            Assert.assertEquals(5_000_000, configuration.getCairoConfiguration().getSpinLockTimeoutUs());
            Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlCharacterStoreCapacity());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getSqlCharacterStoreSequencePoolCapacity());
            Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlColumnPoolCapacity());
            Assert.assertEquals(0.8, configuration.getCairoConfiguration().getSqlCompactMapLoadFactor(), 0.000001);
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlExpressionPoolCapacity());
            Assert.assertEquals(0.3, configuration.getCairoConfiguration().getSqlFastMapLoadFactor(), 0.0000001);
            Assert.assertEquals(32, configuration.getCairoConfiguration().getSqlJoinContextPoolCapacity());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlLexerPoolCapacity());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlMapKeyCapacity());
            Assert.assertEquals(6 * 1024 * 1024, configuration.getCairoConfiguration().getSqlMapPageSize());
            Assert.assertEquals(1026, configuration.getCairoConfiguration().getSqlMapMaxPages());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getSqlMapMaxResizes());
            Assert.assertEquals(256, configuration.getCairoConfiguration().getSqlModelPoolCapacity());
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

            Assert.assertEquals(256, configuration.getCairoConfiguration().getAnalyticColumnPoolCapacity());
            Assert.assertEquals(1024, configuration.getCairoConfiguration().getWithClauseModelPoolCapacity());
            Assert.assertEquals(512, configuration.getCairoConfiguration().getRenameTableModelPoolCapacity());
            Assert.assertEquals(128, configuration.getCairoConfiguration().getInsertPoolCapacity());
            Assert.assertEquals(256, configuration.getCairoConfiguration().getColumnCastModelPoolCapacity());
            Assert.assertEquals(64, configuration.getCairoConfiguration().getCreateTableModelPoolCapacity());

            Assert.assertEquals(2_000_000, configuration.getCairoConfiguration().getCommitLag());
            Assert.assertEquals(100000, configuration.getCairoConfiguration().getMaxUncommittedRows());

            Assert.assertEquals(256, configuration.getCairoConfiguration().getSqlDistinctTimestampKeyCapacity());
            Assert.assertEquals(0.4, configuration.getCairoConfiguration().getSqlDistinctTimestampLoadFactor(), 0.001);

            Assert.assertEquals(167903521, configuration.getLineUdpReceiverConfiguration().getBindIPv4Address());
            Assert.assertEquals(9915, configuration.getLineUdpReceiverConfiguration().getPort());
            Assert.assertEquals(-536805119, configuration.getLineUdpReceiverConfiguration().getGroupIPv4Address());
            Assert.assertEquals(100_000, configuration.getLineUdpReceiverConfiguration().getCommitRate());
            Assert.assertEquals(4 * 1024 * 1024, configuration.getLineUdpReceiverConfiguration().getMsgBufferSize());
            Assert.assertEquals(4000, configuration.getLineUdpReceiverConfiguration().getMsgCount());
            Assert.assertEquals(512, configuration.getLineUdpReceiverConfiguration().getReceiveBufferSize());
            Assert.assertFalse(configuration.getLineUdpReceiverConfiguration().isEnabled());
            Assert.assertEquals(2, configuration.getLineUdpReceiverConfiguration().ownThreadAffinity());
            Assert.assertTrue(configuration.getLineUdpReceiverConfiguration().ownThread());

            // influxdb line TCP protocol
            Assert.assertTrue(configuration.getLineTcpReceiverConfiguration().isEnabled());
            Assert.assertEquals(11, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getActiveConnectionLimit());
            Assert.assertEquals(167903521, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getBindIPv4Address());
            Assert.assertEquals(9916, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getBindPort());
            Assert.assertEquals(1025, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getEventCapacity());
            Assert.assertEquals(1026, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getIOQueueCapacity());
            Assert.assertEquals(400_000, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getIdleConnectionTimeout());
            Assert.assertEquals(1_002, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getQueuedConnectionTimeout());
            Assert.assertEquals(1027, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getInterestQueueCapacity());
            Assert.assertEquals(55555, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getListenBacklog());
            Assert.assertEquals(32768, configuration.getLineTcpReceiverConfiguration().getNetDispatcherConfiguration().getRcvBufSize());
            Assert.assertEquals(32, configuration.getLineTcpReceiverConfiguration().getConnectionPoolInitialCapacity());
            Assert.assertEquals(LineProtoMicroTimestampAdapter.INSTANCE, configuration.getLineTcpReceiverConfiguration().getTimestampAdapter());
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
            Assert.assertEquals(100_000, configuration.getLineTcpReceiverConfiguration().getNUpdatesPerLoadRebalance());
            Assert.assertEquals(1.5, configuration.getLineTcpReceiverConfiguration().getMaxLoadRatio(), 0.001);
            Assert.assertEquals(1000, configuration.getLineTcpReceiverConfiguration().getMaintenanceInterval());
            Assert.assertEquals(PartitionBy.MONTH, configuration.getLineTcpReceiverConfiguration().getDefaultPartitionBy());
            Assert.assertEquals(10_000, configuration.getLineTcpReceiverConfiguration().getAggressiveReadRetryCount());
            Assert.assertEquals(5_000, configuration.getLineTcpReceiverConfiguration().getWriterIdleTimeout());

            Assert.assertTrue(configuration.getCairoConfiguration().getTelemetryConfiguration().getEnabled());
            Assert.assertEquals(512, configuration.getCairoConfiguration().getTelemetryConfiguration().getQueueCapacity());

            Assert.assertFalse(configuration.getHttpServerConfiguration().getHttpContextConfiguration().getServerKeepAlive());
            Assert.assertEquals("HTTP/1.0 ", configuration.getHttpServerConfiguration().getHttpContextConfiguration().getHttpVersion());
            Assert.assertEquals(33554432L, configuration.getCairoConfiguration().getAppendPageSize());

            Assert.assertTrue(configuration.getMetricsConfiguration().isEnabled());
        }
    }

    @Test
    public void testSetAllInternalProperties() throws ServerConfigurationException, JsonException {
        final BuildInformation buildInformation = new BuildInformationHolder("5.0.6", "11.0.9.1", "0fff7d46fd13b4705770f1fb126dd9b889768643");
        final PropServerConfiguration configuration = new PropServerConfiguration(root, new Properties(), null, LOG, buildInformation);

        Assert.assertEquals("5.0.6", configuration.getCairoConfiguration().getBuildInformation().getQuestDbVersion());
        Assert.assertEquals("11.0.9.1", configuration.getCairoConfiguration().getBuildInformation().getJdkVersion());
        Assert.assertEquals("0fff7d46fd13b4705770f1fb126dd9b889768643", configuration.getCairoConfiguration().getBuildInformation().getCommitHash());
    }

    @Test
    public void testSetZeroKeepAlive() throws IOException, ServerConfigurationException, JsonException {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server-keep-alive.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            PropServerConfiguration configuration = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
            Assert.assertNull(configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getKeepAliveHeader());
        }
    }
}
