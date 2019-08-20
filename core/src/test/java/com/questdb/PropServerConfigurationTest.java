/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb;

import com.questdb.network.EpollFacadeImpl;
import com.questdb.network.IOOperation;
import com.questdb.network.NetworkFacadeImpl;
import com.questdb.network.SelectFacadeImpl;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.microtime.MicrosecondClockImpl;
import com.questdb.std.time.MillisecondClockImpl;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropServerConfigurationTest {

    public final TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        temp.create();
    }

    private void copyMimeTypes(String targetDir) throws IOException {
        try (InputStream stream = PropServerConfigurationTest.class.getResourceAsStream("/site/conf/mime.types")) {
            Assert.assertNotNull(stream);
            final File target = new File(targetDir, "conf/mime.types");
            Assert.assertTrue(target.getParentFile().mkdirs());
            try (FileOutputStream fos = new FileOutputStream(target)) {
                byte[] buffer = new byte[1024 * 1204];
                int len;
                while ((len = stream.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
            }
        }
    }

    @Test
    public void testAllDefaults() throws ServerConfigurationException, IOException {
        Properties properties = new Properties();
        File root = new File(temp.getRoot(), "root");
        copyMimeTypes(root.getAbsolutePath());
        PropServerConfiguration configuration = new PropServerConfiguration(root.getAbsolutePath(), properties);
        Assert.assertEquals(16, configuration.getHttpServerConfiguration().getConnectionPoolInitialCapacity());
        Assert.assertEquals(128, configuration.getHttpServerConfiguration().getConnectionStringPoolCapacity());
        Assert.assertEquals(512, configuration.getHttpServerConfiguration().getMultipartHeaderBufferSize());
        Assert.assertEquals(10_000, configuration.getHttpServerConfiguration().getMultipartIdleSpinCount());
        Assert.assertEquals(1048576, configuration.getHttpServerConfiguration().getRecvBufferSize());
        Assert.assertEquals(64448, configuration.getHttpServerConfiguration().getRequestHeaderBufferSize());
        Assert.assertEquals(32768, configuration.getHttpServerConfiguration().getResponseHeaderBufferSize());
        Assert.assertEquals(0, configuration.getHttpServerConfiguration().getWorkerCount());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getSendBufferSize());
        Assert.assertEquals("index.html", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getIndexFileName());

        // this is going to need interesting validation logic
        // configuration path is expected to be relative and we need to check if absolute path is good
        Assert.assertEquals(new File(root, "public").getAbsolutePath(),
                configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory());

        Assert.assertTrue(configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().abortBrokenUploads());

        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getActiveConnectionLimit());
        Assert.assertEquals(1024, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEventCapacity());
        Assert.assertEquals(1024, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
        Assert.assertEquals(300000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIdleConnectionTimeout());
        Assert.assertEquals(1024, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
        Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInitialBias());
        Assert.assertEquals(256, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getListenBacklog());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSndBufSize());
        Assert.assertEquals(2097152, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
        Assert.assertEquals("/text_loader.json", configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getAdapterSetConfigurationFileName());
        Assert.assertEquals(16, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getDateAdapterPoolCapacity());
        Assert.assertEquals(16384, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getJsonCacheLimit());
        Assert.assertEquals(8192, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getJsonCacheSize());
        Assert.assertEquals(0.1222d, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000000001);
        Assert.assertEquals(128, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getMetadataStringPoolCapacity());
        Assert.assertEquals(1024 * 4096, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getRollBufferLimit());
        Assert.assertEquals(1024, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getRollBufferSize());
        Assert.assertEquals(1000, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getTextAnalysisMaxLines());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getTextLexerStringPoolCapacity());
        Assert.assertEquals(64, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getTimestampAdapterPoolCapacity());
        Assert.assertEquals(4096, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getUtf8SinkSize());
        Assert.assertEquals(0, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindIPv4Address());
        Assert.assertEquals(9000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindPort());

        Assert.assertEquals(1_000_000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getConnectionCheckFrequency());
        Assert.assertEquals(10, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getDoubleScale());
        Assert.assertEquals(10, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getFloatScale());

        Assert.assertEquals(5, configuration.getCairoConfiguration().getCreateAsSelectRetryCount());
        Assert.assertEquals("fast", configuration.getCairoConfiguration().getDefaultMapType());
        Assert.assertFalse(configuration.getCairoConfiguration().getDefaultSymbolCacheFlag());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getDefaultSymbolCapacity());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getFileOperationRetryCount());
        Assert.assertEquals(100, configuration.getCairoConfiguration().getIdleCheckInterval());
        Assert.assertEquals(-10_000, configuration.getCairoConfiguration().getInactiveReaderTTL());
        Assert.assertEquals(-10_000, configuration.getCairoConfiguration().getInactiveWriterTTL());
        Assert.assertEquals(256, configuration.getCairoConfiguration().getIndexValueBlockSize());
        Assert.assertEquals(30, configuration.getCairoConfiguration().getMaxSwapFileCount());
        Assert.assertEquals(509, configuration.getCairoConfiguration().getMkDirMode());

        Assert.assertEquals(100000, configuration.getCairoConfiguration().getParallelIndexThreshold());
        Assert.assertEquals(5, configuration.getCairoConfiguration().getReaderPoolMaxSegments());
        Assert.assertEquals(1_000_000, configuration.getCairoConfiguration().getSpinLockTimeoutUs());
        Assert.assertEquals(16, configuration.getCairoConfiguration().getSqlCacheRows());
        Assert.assertEquals(4, configuration.getCairoConfiguration().getSqlCacheBlocks());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlCharacterStoreCapacity());
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlCharacterStoreSequencePoolCapacity());
        Assert.assertEquals(4096, configuration.getCairoConfiguration().getSqlColumnPoolCapacity());
        Assert.assertEquals(0.7, configuration.getCairoConfiguration().getSqlCompactMapLoadFactor(), 0.000001);
        Assert.assertEquals(8192, configuration.getCairoConfiguration().getSqlExpressionPoolCapacity());
        Assert.assertEquals(0.5, configuration.getCairoConfiguration().getSqlFastMapLoadFactor(), 0.0000001);
        Assert.assertEquals(64, configuration.getCairoConfiguration().getSqlJoinContextPoolCapacity());
        Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlLexerPoolCapacity());
        Assert.assertEquals(2048, configuration.getCairoConfiguration().getSqlMapKeyCapacity());
        Assert.assertEquals(4 * 1024 * 1024, configuration.getCairoConfiguration().getSqlMapPageSize());
        Assert.assertEquals(1024, configuration.getCairoConfiguration().getSqlModelPoolCapacity());
        Assert.assertEquals(4 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortKeyPageSize());
        Assert.assertEquals(1024 * 1024, configuration.getCairoConfiguration().getSqlSortLightValuePageSize());
        Assert.assertEquals(16 * 1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinValuePageSize());
        Assert.assertEquals(4 * 1024 * 1024, configuration.getCairoConfiguration().getSqlTreePageSize());
        Assert.assertEquals(1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinLightValuePageSize());
        Assert.assertEquals(16 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortValuePageSize());
        Assert.assertEquals(10000, configuration.getCairoConfiguration().getWorkStealTimeoutNanos());
        Assert.assertTrue(configuration.getCairoConfiguration().isParallelIndexingEnabled());
        Assert.assertEquals(16 * 1024, configuration.getCairoConfiguration().getSqlJoinMetadataPageSize());

        Assert.assertEquals(0, configuration.getLineUdpReceiverConfiguration().getBindIPv4Address());
        Assert.assertEquals(9009, configuration.getLineUdpReceiverConfiguration().getPort());
        Assert.assertEquals(-402587133, configuration.getLineUdpReceiverConfiguration().getGroupIPv4Address());

        Assert.assertEquals(10000, configuration.getLineUdpReceiverConfiguration().getCommitRate());

        Assert.assertEquals(1024 * 1024, configuration.getLineUdpReceiverConfiguration().getMsgBufferSize());
        Assert.assertEquals(10000, configuration.getLineUdpReceiverConfiguration().getMsgCount());
        Assert.assertEquals(2048, configuration.getLineUdpReceiverConfiguration().getReceiveBufferSize());

        // statics
        Assert.assertSame(FilesFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getFilesFacade());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getClock());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getHttpServerConfiguration().getClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getNetworkFacade());
        Assert.assertSame(EpollFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEpollFacade());
        Assert.assertSame(SelectFacadeImpl.INSTANCE, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSelectFacade());
        Assert.assertSame(FilesFacadeImpl.INSTANCE, configuration.getCairoConfiguration().getFilesFacade());
        Assert.assertSame(MillisecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMillisecondClock());
        Assert.assertSame(MicrosecondClockImpl.INSTANCE, configuration.getCairoConfiguration().getMicrosecondClock());
        Assert.assertSame(NetworkFacadeImpl.INSTANCE, configuration.getLineUdpReceiverConfiguration().getNetworkFacade());
        TestUtils.assertEquals(new File(root, "db").getAbsolutePath(), configuration.getCairoConfiguration().getRoot());

        // assert mime types
        TestUtils.assertEquals("application/json", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getMimeTypesCache().get("json"));
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToAddress() throws ServerConfigurationException {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6:8990");
        new PropServerConfiguration("root", properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToMissingColon() throws ServerConfigurationException {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1");
        new PropServerConfiguration("root", properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidBindToPort() throws ServerConfigurationException {
        Properties properties = new Properties();
        properties.setProperty("http.bind.to", "10.5.6.1:");
        new PropServerConfiguration("root", properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidDouble() throws ServerConfigurationException {
        Properties properties = new Properties();
        properties.setProperty("http.text.max.required.delimiter.stddev", "abc");
        new PropServerConfiguration("root", properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIPv4Address() throws ServerConfigurationException, IOException {
        Properties properties = new Properties();
        properties.setProperty("line.udp.join", "12a.990.00");
        File root = new File(temp.getRoot(), "data");
        copyMimeTypes(root.getAbsolutePath());
        new PropServerConfiguration(root.getAbsolutePath(), properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidInt() throws ServerConfigurationException {
        Properties properties = new Properties();
        properties.setProperty("http.connection.string.pool.capacity", "1234a");
        new PropServerConfiguration("root", properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidIntSize() throws ServerConfigurationException {
        Properties properties = new Properties();
        properties.setProperty("http.request.header.buffer.size", "22g");
        new PropServerConfiguration("root", properties);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidLong() throws ServerConfigurationException, IOException {
        Properties properties = new Properties();
        properties.setProperty("cairo.idle.check.interval", "1234a");
        File root = new File(temp.getRoot(), "data");
        copyMimeTypes(root.getAbsolutePath());
        new PropServerConfiguration(root.getAbsolutePath(), properties);
    }

    @Test
    public void testSetAllFromFile() throws IOException, ServerConfigurationException {
        try (InputStream is = PropServerConfigurationTest.class.getResourceAsStream("/server.conf")) {
            Properties properties = new Properties();
            properties.load(is);

            File root = new File(temp.getRoot(), "data");
            copyMimeTypes(root.getAbsolutePath());

            PropServerConfiguration configuration = new PropServerConfiguration(root.getAbsolutePath(), properties);
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getConnectionPoolInitialCapacity());
            Assert.assertEquals(512, configuration.getHttpServerConfiguration().getConnectionStringPoolCapacity());
            Assert.assertEquals(256, configuration.getHttpServerConfiguration().getMultipartHeaderBufferSize());
            Assert.assertEquals(100_000, configuration.getHttpServerConfiguration().getMultipartIdleSpinCount());
            Assert.assertEquals(4096, configuration.getHttpServerConfiguration().getRecvBufferSize());
            Assert.assertEquals(2048, configuration.getHttpServerConfiguration().getRequestHeaderBufferSize());
            Assert.assertEquals(9012, configuration.getHttpServerConfiguration().getResponseHeaderBufferSize());
            Assert.assertEquals(6, configuration.getHttpServerConfiguration().getWorkerCount());
            Assert.assertEquals(128, configuration.getHttpServerConfiguration().getSendBufferSize());
            Assert.assertEquals("index2.html", configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getIndexFileName());

            Assert.assertEquals(new File(root, "public_ok").getAbsolutePath(),
                    configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration().getPublicDirectory());

            Assert.assertFalse(configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().abortBrokenUploads());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getActiveConnectionLimit());
            Assert.assertEquals(2048, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getEventCapacity());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIOQueueCapacity());
            Assert.assertEquals(7000000, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getIdleConnectionTimeout());
            Assert.assertEquals(512, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInterestQueueCapacity());
            Assert.assertEquals(IOOperation.READ, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getInitialBias());
            Assert.assertEquals(64, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getListenBacklog());
            Assert.assertEquals(4194304, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getSndBufSize());
            Assert.assertEquals(8388608, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getRcvBufSize());
            Assert.assertEquals("/loader.json", configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getAdapterSetConfigurationFileName());
            Assert.assertEquals(32, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getDateAdapterPoolCapacity());
            Assert.assertEquals(65536, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getJsonCacheLimit());
            Assert.assertEquals(8388608, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getJsonCacheSize());
            Assert.assertEquals(0.3d, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getMaxRequiredDelimiterStdDev(), 0.000000001);
            Assert.assertEquals(512, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getMetadataStringPoolCapacity());
            Assert.assertEquals(6144, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getRollBufferLimit());
            Assert.assertEquals(3072, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getRollBufferSize());
            Assert.assertEquals(400, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getTextAnalysisMaxLines());
            Assert.assertEquals(128, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getTextLexerStringPoolCapacity());
            Assert.assertEquals(512, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getTimestampAdapterPoolCapacity());
            Assert.assertEquals(8192, configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration().getTextConfiguration().getUtf8SinkSize());
            Assert.assertEquals(168101918, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindIPv4Address());
            Assert.assertEquals(9900, configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindPort());

            Assert.assertEquals(2_000, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getConnectionCheckFrequency());
            Assert.assertEquals(6, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getDoubleScale());
            Assert.assertEquals(4, configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getFloatScale());

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
            Assert.assertEquals(580, configuration.getCairoConfiguration().getMkDirMode());
            Assert.assertEquals(1000000, configuration.getCairoConfiguration().getParallelIndexThreshold());
            Assert.assertEquals(10, configuration.getCairoConfiguration().getReaderPoolMaxSegments());
            Assert.assertEquals(5_000_000, configuration.getCairoConfiguration().getSpinLockTimeoutUs());
            Assert.assertEquals(32, configuration.getCairoConfiguration().getSqlCacheRows());
            Assert.assertEquals(16, configuration.getCairoConfiguration().getSqlCacheBlocks());
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
            Assert.assertEquals(256, configuration.getCairoConfiguration().getSqlModelPoolCapacity());
            Assert.assertEquals(10 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortKeyPageSize());
            Assert.assertEquals(3 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortLightValuePageSize());
            Assert.assertEquals(8 * 1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinValuePageSize());
            Assert.assertEquals(16 * 1024 * 1024, configuration.getCairoConfiguration().getSqlTreePageSize());
            Assert.assertEquals(2 * 1024 * 1024, configuration.getCairoConfiguration().getSqlHashJoinLightValuePageSize());
            Assert.assertEquals(4 * 1024 * 1024, configuration.getCairoConfiguration().getSqlSortValuePageSize());
            Assert.assertEquals(1000000, configuration.getCairoConfiguration().getWorkStealTimeoutNanos());
            Assert.assertFalse(configuration.getCairoConfiguration().isParallelIndexingEnabled());
            Assert.assertEquals(8 * 1024, configuration.getCairoConfiguration().getSqlJoinMetadataPageSize());

            Assert.assertEquals(167903521, configuration.getLineUdpReceiverConfiguration().getBindIPv4Address());
            Assert.assertEquals(9915, configuration.getLineUdpReceiverConfiguration().getPort());
            Assert.assertEquals(-536805119, configuration.getLineUdpReceiverConfiguration().getGroupIPv4Address());
            Assert.assertEquals(100_000, configuration.getLineUdpReceiverConfiguration().getCommitRate());
            Assert.assertEquals(4 * 1024 * 1024, configuration.getLineUdpReceiverConfiguration().getMsgBufferSize());
            Assert.assertEquals(4000, configuration.getLineUdpReceiverConfiguration().getMsgCount());
            Assert.assertEquals(512, configuration.getLineUdpReceiverConfiguration().getReceiveBufferSize());
        }
    }
}