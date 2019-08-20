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

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cutlass.http.HttpServerConfiguration;
import com.questdb.cutlass.http.MimeTypesCache;
import com.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import com.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import com.questdb.cutlass.http.processors.TextImportProcessorConfiguration;
import com.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import com.questdb.cutlass.pgwire.DefaultPGWireConfiguration;
import com.questdb.cutlass.pgwire.PGWireConfiguration;
import com.questdb.cutlass.text.TextConfiguration;
import com.questdb.mp.WorkerPoolConfiguration;
import com.questdb.network.*;
import com.questdb.std.*;
import com.questdb.std.microtime.MicrosecondClock;
import com.questdb.std.microtime.MicrosecondClockImpl;
import com.questdb.std.str.Path;
import com.questdb.std.time.MillisecondClock;
import com.questdb.std.time.MillisecondClockImpl;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

public class PropServerConfiguration implements ServerConfigurationV2 {
    public static final String CONFIG_DIRECTORY = "conf";
    private final IODispatcherConfiguration httpIODispatcherConfiguration = new HttpIODispatcherConfiguration();
    private final TextImportProcessorConfiguration textImportProcessorConfiguration = new PropTextImportProcessorConfiguration();
    private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new PropStaticContentProcessorConfiguration();
    private final HttpServerConfiguration httpServerConfiguration = new PropHttpServerConfiguration();
    private final TextConfiguration textConfiguration = new PropTextConfiguration();
    private final CairoConfiguration cairoConfiguration = new PropCairoConfiguration();
    private final LineUdpReceiverConfiguration lineUdpReceiverConfiguration = new PropLineUdpReceiverConfiguration();
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new PropJsonQueryProcessorConfiguration();
    private final boolean httpServerEnabled;
    private final int createAsSelectRetryCount;
    private final CharSequence defaultMapType;
    private final boolean defaultSymbolCacheFlag;
    private final int defaultSymbolCapacity;
    private final int fileOperationRetryCount;
    private final long idleCheckInterval;
    private final long inactiveReaderTTL;
    private final long inactiveWriterTTL;
    private final int indexValueBlockSize;
    private final int maxSwapFileCount;
    private final int mkdirMode;
    private final int parallelIndexThreshold;
    private final int readerPoolMaxSegments;
    private final long spinLockTimeoutUs;
    private final int sqlCacheRows;
    private final int sqlCacheBlocks;
    private final int sqlCharacterStoreCapacity;
    private final int sqlCharacterStoreSequencePoolCapacity;
    private final int sqlColumnPoolCapacity;
    private final double sqlCompactMapLoadFactor;
    private final int sqlExpressionPoolCapacity;
    private final double sqlFastMapLoadFactor;
    private final int sqlJoinContextPoolCapacity;
    private final int sqlLexerPoolCapacity;
    private final int sqlMapKeyCapacity;
    private final int sqlMapPageSize;
    private final int sqlModelPoolCapacity;
    private final int sqlSortKeyPageSize;
    private final int sqlSortLightValuePageSize;
    private final int sqlHashJoinValuePageSize;
    private final int sqlTreePageSize;
    private final int sqlHashJoinLightValuePageSize;
    private final int sqlSortValuePageSize;
    private final long workStealTimeoutNanos;
    private final boolean parallelIndexingEnabled;
    private final int sqlJoinMetadataPageSize;
    private final int lineUdpCommitRate;
    private final int lineUdpGroupIPv4Address;
    private final int lineUdpMsgBufferSize;
    private final int lineUdpMsgCount;
    private final int lineUdpReceiveBufferSize;
    private final int[] sharedWorkerAffinity;
    private final int sharedWorkerCount;
    private final boolean shareWorkerHaltOnError;
    private final WorkerPoolConfiguration workerPoolConfiguration = new PropWorkerPoolConfiguration();
    private final PGWireConfiguration pgWireConfiguration = new DefaultPGWireConfiguration() {
        @Override
        public int getWorkerCount() {
            return 0;
        }
    };
    private int[] httpWorkerAffinity;
    private int connectionPoolInitialCapacity;
    private int connectionStringPoolCapacity;
    private int multipartHeaderBufferSize;
    private long multipartIdleSpinCount;
    private int recvBufferSize;
    private int requestHeaderBufferSize;
    private int responseHeaderBufferSize;
    private int httpWorkerCount;
    private boolean httpWorkerHaltOnError;
    private int sendBufferSize;
    private CharSequence indexFileName;
    private String publicDirectory;
    private boolean abortBrokenUploads;
    private int activeConnectionLimit;
    private int eventCapacity;
    private int ioQueueCapacity;
    private long idleConnectionTimeout;
    private int interestQueueCapacity;
    private int listenBacklog;
    private int sndBufSize;
    private int rcvBufSize;
    private String adapterSetConfigurationFileName;
    private int dateAdapterPoolCapacity;
    private int jsonCacheLimit;
    private int jsonCacheSize;
    private double maxRequiredDelimiterStdDev;
    private int metadataStringPoolCapacity;
    private int rollBufferLimit;
    private int rollBufferSize;
    private int textAnalysisMaxLines;
    private int textLexerStringPoolCapacity;
    private int timestampAdapterPoolCapacity;
    private int utf8SinkSize;
    private MimeTypesCache mimeTypesCache;
    private String databaseRoot;
    private String keepAliveHeader;
    private int bindIPv4Address;
    private int bindPort;
    private int lineUdpBindIPV4Address;
    private int lineUdpPort;
    private int jsonQueryFloatScale;
    private int jsonQueryDoubleScale;
    private int jsonQueryConnectionCheckFrequency;

    public PropServerConfiguration(String root, Properties properties) throws ServerConfigurationException {
        this.sharedWorkerCount = getInt(properties, "shared.worker.count", 2);
        this.sharedWorkerAffinity = getAffinity(properties, "shared.worker.affinity", sharedWorkerCount);
        this.shareWorkerHaltOnError = getBoolean(properties, "shared.worker.haltOnError", false);
        this.httpServerEnabled = getBoolean(properties, "http.enabled", true);
        if (httpServerEnabled) {
            this.connectionPoolInitialCapacity = getInt(properties, "http.connection.pool.initial.capacity", 16);
            this.connectionStringPoolCapacity = getInt(properties, "http.connection.string.pool.capacity", 128);
            this.multipartHeaderBufferSize = getIntSize(properties, "http.multipart.header.buffer.size", 512);
            this.multipartIdleSpinCount = getLong(properties, "http.multipart.idle.spin.count", 10_000);
            this.recvBufferSize = getIntSize(properties, "http.receive.buffer.size", 1024 * 1024);
            this.requestHeaderBufferSize = getIntSize(properties, "http.request.header.buffer.size", 32 * 2014);
            this.responseHeaderBufferSize = getIntSize(properties, "http.response.header.buffer.size", 32 * 1024);
            this.httpWorkerCount = getInt(properties, "http.worker.count", 0);
            this.httpWorkerAffinity = getAffinity(properties, "http.worker.affinity", httpWorkerCount);
            this.httpWorkerHaltOnError = getBoolean(properties, "http.worker.haltOnError", false);
            this.sendBufferSize = getIntSize(properties, "http.send.buffer.size", 2 * 1024 * 1024);
            this.indexFileName = getString(properties, "http.static.index.file.name", "index.html");

            int keepAliveTimeout = getInt(properties, "http.keep-alive.timeout", 5);
            int keepAliveMax = getInt(properties, "http.keep-alive.max", 10_000);

            if (keepAliveTimeout > 0 && keepAliveMax > 0) {
                this.keepAliveHeader = "Keep-Alive: timeout=5, max=10000" + Misc.EOL;
            } else {
                this.keepAliveHeader = null;
            }

            final String publicDirectory = getString(properties, "http.static.pubic.directory", "public");
            // translate public directory into absolute path
            // this will generate some garbage, but this is ok - we just doing this once on startup
            if (new File(publicDirectory).isAbsolute()) {
                this.publicDirectory = publicDirectory;
            } else {
                this.publicDirectory = new File(root, publicDirectory).getAbsolutePath();
            }

            final String databaseRoot = getString(properties, "cairo.root", "db");
            if (new File(databaseRoot).isAbsolute()) {
                this.databaseRoot = databaseRoot;
            } else {
                this.databaseRoot = new File(root, databaseRoot).getAbsolutePath();
            }

            this.abortBrokenUploads = getBoolean(properties, "http.text.abort.broken.uploads", true);
            this.activeConnectionLimit = getInt(properties, "http.net.active.connection.limit", 256);
            this.eventCapacity = getInt(properties, "http.net.event.capacity", 1024);
            this.ioQueueCapacity = getInt(properties, "http.net.io.queue.capacity", 1024);
            this.idleConnectionTimeout = getLong(properties, "http.net.idle.connection.timeout", 5 * 60 * 1000L);
            this.interestQueueCapacity = getInt(properties, "http.net.interest.queue.capacity", 1024);
            this.listenBacklog = getInt(properties, "http.net.listen.backlog", 256);
            this.sndBufSize = getIntSize(properties, "http.net.snd.buf.size", 2 * 1024 * 1024);
            this.rcvBufSize = getIntSize(properties, "http.net.rcv.buf.size", 2 * 1024 * 1024);
            this.adapterSetConfigurationFileName = getString(properties, "http.text.adapter.set.config", "/text_loader.json");
            this.dateAdapterPoolCapacity = getInt(properties, "http.text.date.adapter.pool.capacity", 16);
            this.jsonCacheLimit = getIntSize(properties, "http.text.json.cache.limit", 16384);
            this.jsonCacheSize = getIntSize(properties, "http.text.json.cache.size", 8192);
            this.maxRequiredDelimiterStdDev = getDouble(properties, "http.text.max.required.delimiter.stddev", 0.1222d);
            this.metadataStringPoolCapacity = getInt(properties, "http.text.metadata.string.pool.capacity", 128);

            this.rollBufferLimit = getIntSize(properties, "http.text.roll.buffer.limit", 1024 * 4096);
            this.rollBufferSize = getIntSize(properties, "http.text.roll.buffer.size", 1024);
            this.textAnalysisMaxLines = getInt(properties, "http.text.analysis.max.lines", 1000);
            this.textLexerStringPoolCapacity = getInt(properties, "http.text.lexer.string.pool.capacity", 64);
            this.timestampAdapterPoolCapacity = getInt(properties, "http.text.timestamp.adapter.pool.capacity", 64);
            this.utf8SinkSize = getIntSize(properties, "http.text.utf8.sink.size", 4096);

            this.jsonQueryConnectionCheckFrequency = getInt(properties, "http.json.query.connection.check.frequency", 1_000_000);
            this.jsonQueryDoubleScale = getInt(properties, "http.json.query.double.scale", 10);
            this.jsonQueryFloatScale = getInt(properties, "http.json.query.float.scale", 10);

            parseBindTo(properties, "http.bind.to", "0.0.0.0:9000", (a, p) -> {
                bindIPv4Address = a;
                bindPort = p;
            });

            // load mime types
            try (Path path = new Path().of(new File(new File(root, CONFIG_DIRECTORY), "mime.types").getAbsolutePath()).$()) {
                this.mimeTypesCache = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
            }
        }

        this.createAsSelectRetryCount = getInt(properties, "cairo.create.as.select.retry.count", 5);
        this.defaultMapType = getString(properties, "cairo.default.map.type", "fast");
        this.defaultSymbolCacheFlag = getBoolean(properties, "cairo.default.symbol.cache.flag", false);
        this.defaultSymbolCapacity = getInt(properties, "cairo.default.symbol.capacity", 256);
        this.fileOperationRetryCount = getInt(properties, "cairo.file.operation.retry.count", 30);
        this.idleCheckInterval = getLong(properties, "cairo.idle.check.interval", 100);
        this.inactiveReaderTTL = getLong(properties, "cairo.inactive.reader.ttl", -10000);
        this.inactiveWriterTTL = getLong(properties, "cairo.inactive.writer.ttl", -10000);
        this.indexValueBlockSize = Numbers.ceilPow2(getIntSize(properties, "cairo.index.value.block.size", 256));
        this.maxSwapFileCount = getInt(properties, "cairo.max.swap.file.count", 30);
        this.mkdirMode = getInt(properties, "cairo.mkdir.mode", 509);
        this.parallelIndexThreshold = getInt(properties, "cairo.parallel.index.threshold", 100000);
        this.readerPoolMaxSegments = getInt(properties, "cairo.reader.pool.max.segments", 5);
        this.spinLockTimeoutUs = getLong(properties, "cairo.spin.lock.timeout", 1_000_000);
        this.sqlCacheRows = getInt(properties, "cairo.cache.rows", 16);
        this.sqlCacheBlocks = getIntSize(properties, "cairo.cache.blocks", 4);
        this.sqlCharacterStoreCapacity = getInt(properties, "cairo.character.store.capacity", 1024);
        this.sqlCharacterStoreSequencePoolCapacity = getInt(properties, "cairo.character.store.sequence.pool.capacity", 64);
        this.sqlColumnPoolCapacity = getInt(properties, "cairo.column.pool.capacity", 4096);
        this.sqlCompactMapLoadFactor = getDouble(properties, "cairo.compact.map.load.factor", 0.7);
        this.sqlExpressionPoolCapacity = getInt(properties, "cairo.expression.pool.capacity", 8192);
        this.sqlFastMapLoadFactor = getDouble(properties, "cairo.fast.map.load.factor", 0.5);
        this.sqlJoinContextPoolCapacity = getInt(properties, "cairo.sql.join.context.pool.capacity", 64);
        this.sqlLexerPoolCapacity = getInt(properties, "cairo.lexer.pool.capacity", 2048);
        this.sqlMapKeyCapacity = getInt(properties, "cairo.sql.map.key.capacity", 2048);
        this.sqlMapPageSize = getIntSize(properties, "cairo.sql.map.page.size", 4 * 1024 * 1024);
        this.sqlModelPoolCapacity = getInt(properties, "cairo.model.pool.capacity", 1024);
        this.sqlSortKeyPageSize = getIntSize(properties, "cairo.sql.sort.key.page.size", 4 * 1024 * 1024);
        this.sqlSortLightValuePageSize = getIntSize(properties, "cairo.sql.sort.light.value.page.size", 1048576);
        this.sqlHashJoinValuePageSize = getIntSize(properties, "cairo.sql.hash.join.value.page.size", 16777216);
        this.sqlTreePageSize = getIntSize(properties, "cairo.sql.tree.page.size", 4 * 1024 * 1024);
        this.sqlHashJoinLightValuePageSize = getIntSize(properties, "cairo.sql.hash.join.light.value.page.size", 1048576);
        this.sqlSortValuePageSize = getIntSize(properties, "cairo.sql.sort.value.page.size", 16777216);
        this.workStealTimeoutNanos = getLong(properties, "cairo.work.steal.timeout.nanos", 10_000);
        this.parallelIndexingEnabled = getBoolean(properties, "cairo.parallel.indexing.enabled", true);
        this.sqlJoinMetadataPageSize = getIntSize(properties, "cairo.sql.join.metadata.page.size", 16384);

        parseBindTo(properties, "line.udp.bind.to", "0.0.0.0:9009", (a, p) -> {
            this.lineUdpBindIPV4Address = a;
            this.lineUdpPort = p;
        });

        this.lineUdpGroupIPv4Address = getIPv4Address(properties, "line.udp.join", "232.1.2.3");
        this.lineUdpCommitRate = getInt(properties, "line.udp.commit.rate", 10_000);
        this.lineUdpMsgBufferSize = getIntSize(properties, "line.udp.msg.buffer.size", 1024 * 1024);
        this.lineUdpMsgCount = getInt(properties, "line.udp.msg.count", 10_000);
        this.lineUdpReceiveBufferSize = getIntSize(properties, "line.udp.receive.buffer.size", 2048);
    }

    @Override
    public CairoConfiguration getCairoConfiguration() {
        return cairoConfiguration;
    }

    @Override
    public HttpServerConfiguration getHttpServerConfiguration() {
        return httpServerConfiguration;
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return pgWireConfiguration;
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return lineUdpReceiverConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return workerPoolConfiguration;
    }

    private int[] getAffinity(Properties properties, String key, int httpWorkerCount) throws ServerConfigurationException {
        final int[] result = new int[httpWorkerCount];
        String value = properties.getProperty(key);
        if (value == null) {
            Arrays.fill(result, -1);
        } else {
            String[] affinity = value.split(",");
            if (affinity.length != httpWorkerCount) {
                throw new ServerConfigurationException(key, "wrong number of affinity values");
            }
            for (int i = 0; i < httpWorkerCount; i++) {
                try {
                    result[i] = Numbers.parseInt(affinity[i]);
                } catch (NumericException e) {
                    throw new ServerConfigurationException(key, "Invalid affinity value: " + affinity[i]);
                }
            }
        }
        return result;
    }

    private boolean getBoolean(Properties properties, String key, boolean defaultValue) {
        final String value = properties.getProperty(key);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private double getDouble(Properties properties, String key, double defaultValue) throws ServerConfigurationException {
        final String value = properties.getProperty(key);
        try {
            return value != null ? Numbers.parseDouble(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private int getIPv4Address(Properties properties, String key, String defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, key, defaultValue);
        try {
            return Net.parseIPv4(value);
        } catch (NetworkError e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private int getInt(Properties properties, String key, int defaultValue) throws ServerConfigurationException {
        final String value = properties.getProperty(key);
        try {
            return value != null ? Numbers.parseInt(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private int getIntSize(Properties properties, String key, int defaultValue) throws ServerConfigurationException {
        final String value = properties.getProperty(key);
        try {
            return value != null ? Numbers.parseIntSize(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private long getLong(Properties properties, String key, long defaultValue) throws ServerConfigurationException {
        final String value = properties.getProperty(key);
        try {
            return value != null ? Numbers.parseLong(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private String getString(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    private void parseBindTo(
            Properties properties,
            String key,
            String defaultValue,
            BindToParser parser
    ) throws ServerConfigurationException {

        final String bindTo = getString(properties, key, defaultValue);
        final int colonIndex = bindTo.indexOf(':');
        if (colonIndex == -1) {
            throw new ServerConfigurationException(key, bindTo);
        }

        final String ipv4Str = bindTo.substring(0, colonIndex);
        final int ipv4;
        try {
            ipv4 = Net.parseIPv4(ipv4Str);
        } catch (NetworkError e) {
            throw new ServerConfigurationException(key, ipv4Str);
        }


        final String portStr = bindTo.substring(colonIndex + 1);
        final int port;
        try {
            port = Numbers.parseInt(portStr);
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, portStr);
        }

        parser.onReady(ipv4, port);
    }

    @FunctionalInterface
    private interface BindToParser {
        void onReady(int address, int port);
    }

    private class PropStaticContentProcessorConfiguration implements StaticContentProcessorConfiguration {
        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
        }

        @Override
        public CharSequence getIndexFileName() {
            return indexFileName;
        }

        @Override
        public MimeTypesCache getMimeTypesCache() {
            return mimeTypesCache;
        }

        /**
         * Absolute path to HTTP public directory.
         *
         * @return path to public directory
         */
        @Override
        public CharSequence getPublicDirectory() {
            return publicDirectory;
        }

        @Override
        public String getKeepAliveHeader() {
            return keepAliveHeader;
        }
    }

    private class PropTextImportProcessorConfiguration implements TextImportProcessorConfiguration {
        @Override
        public boolean abortBrokenUploads() {
            return abortBrokenUploads;
        }

        @Override
        public TextConfiguration getTextConfiguration() {
            return textConfiguration;
        }
    }

    private class HttpIODispatcherConfiguration implements IODispatcherConfiguration {
        @Override
        public int getActiveConnectionLimit() {
            return activeConnectionLimit;
        }

        @Override
        public int getBindIPv4Address() {
            return bindIPv4Address;
        }

        @Override
        public int getBindPort() {
            return bindPort;
        }

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public int getEventCapacity() {
            return eventCapacity;
        }

        @Override
        public int getIOQueueCapacity() {
            return ioQueueCapacity;
        }

        @Override
        public long getIdleConnectionTimeout() {
            return idleConnectionTimeout;
        }

        @Override
        public int getInterestQueueCapacity() {
            return interestQueueCapacity;
        }

        @Override
        public int getListenBacklog() {
            return listenBacklog;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getInitialBias() {
            return IOOperation.READ;
        }

        @Override
        public int getSndBufSize() {
            return sndBufSize;
        }

        @Override
        public int getRcvBufSize() {
            return rcvBufSize;
        }

        @Override
        public String getDispatcherLogName() {
            return "http-server";
        }
    }

    private class PropTextConfiguration implements TextConfiguration {
        @Override
        public String getAdapterSetConfigurationFileName() {
            return adapterSetConfigurationFileName;
        }

        @Override
        public int getDateAdapterPoolCapacity() {
            return dateAdapterPoolCapacity;
        }

        @Override
        public int getJsonCacheLimit() {
            return jsonCacheLimit;
        }

        @Override
        public int getJsonCacheSize() {
            return jsonCacheSize;
        }

        @Override
        public double getMaxRequiredDelimiterStdDev() {
            return maxRequiredDelimiterStdDev;
        }

        @Override
        public int getMetadataStringPoolCapacity() {
            return metadataStringPoolCapacity;
        }

        @Override
        public int getRollBufferLimit() {
            return rollBufferLimit;
        }

        @Override
        public int getRollBufferSize() {
            return rollBufferSize;
        }

        @Override
        public int getTextAnalysisMaxLines() {
            return textAnalysisMaxLines;
        }

        @Override
        public int getTextLexerStringPoolCapacity() {
            return textLexerStringPoolCapacity;
        }

        @Override
        public int getTimestampAdapterPoolCapacity() {
            return timestampAdapterPoolCapacity;
        }

        @Override
        public int getUtf8SinkSize() {
            return utf8SinkSize;
        }
    }

    private class PropHttpServerConfiguration implements HttpServerConfiguration {

        @Override
        public boolean workerHaltOnError() {
            return httpWorkerHaltOnError;
        }

        @Override
        public int getConnectionPoolInitialCapacity() {
            return connectionPoolInitialCapacity;
        }

        @Override
        public int getConnectionStringPoolCapacity() {
            return connectionStringPoolCapacity;
        }

        @Override
        public int getMultipartHeaderBufferSize() {
            return multipartHeaderBufferSize;
        }

        @Override
        public long getMultipartIdleSpinCount() {
            return multipartIdleSpinCount;
        }

        @Override
        public int getRecvBufferSize() {
            return recvBufferSize;
        }

        @Override
        public int getRequestHeaderBufferSize() {
            return requestHeaderBufferSize;
        }

        @Override
        public int getResponseHeaderBufferSize() {
            return responseHeaderBufferSize;
        }

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return httpIODispatcherConfiguration;
        }

        @Override
        public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
            return staticContentProcessorConfiguration;
        }

        @Override
        public TextImportProcessorConfiguration getTextImportProcessorConfiguration() {
            return textImportProcessorConfiguration;
        }

        @Override
        public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
            return jsonQueryProcessorConfiguration;
        }

        @Override
        public int getWorkerCount() {
            return httpWorkerCount;
        }

        @Override
        public int[] getWorkerAffinity() {
            return httpWorkerAffinity;
        }

        @Override
        public int getSendBufferSize() {
            return sendBufferSize;
        }

        @Override
        public boolean isEnabled() {
            return httpServerEnabled;
        }
    }

    private class PropCairoConfiguration implements CairoConfiguration {
        @Override
        public int getCreateAsSelectRetryCount() {
            return createAsSelectRetryCount;
        }

        @Override
        public CharSequence getDefaultMapType() {
            return defaultMapType;
        }

        @Override
        public boolean getDefaultSymbolCacheFlag() {
            return defaultSymbolCacheFlag;
        }

        @Override
        public int getDefaultSymbolCapacity() {
            return defaultSymbolCapacity;
        }

        @Override
        public int getFileOperationRetryCount() {
            return fileOperationRetryCount;
        }

        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
        }

        @Override
        public long getIdleCheckInterval() {
            return idleCheckInterval;
        }

        @Override
        public long getInactiveReaderTTL() {
            return inactiveReaderTTL;
        }

        @Override
        public long getInactiveWriterTTL() {
            return inactiveWriterTTL;
        }

        @Override
        public int getIndexValueBlockSize() {
            return indexValueBlockSize;
        }

        @Override
        public int getMaxSwapFileCount() {
            return maxSwapFileCount;
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return MicrosecondClockImpl.INSTANCE;
        }

        @Override
        public MillisecondClock getMillisecondClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public int getMkDirMode() {
            return mkdirMode;
        }

        @Override
        public int getParallelIndexThreshold() {
            return parallelIndexThreshold;
        }

        @Override
        public int getReaderPoolMaxSegments() {
            return readerPoolMaxSegments;
        }

        @Override
        public CharSequence getRoot() {
            return databaseRoot;
        }

        @Override
        public long getSpinLockTimeoutUs() {
            return spinLockTimeoutUs;
        }

        @Override
        public int getSqlCacheBlocks() {
            return sqlCacheBlocks;
        }

        @Override
        public int getSqlCacheRows() {
            return sqlCacheRows;
        }

        @Override
        public int getSqlCharacterStoreCapacity() {
            return sqlCharacterStoreCapacity;
        }

        @Override
        public int getSqlCharacterStoreSequencePoolCapacity() {
            return sqlCharacterStoreSequencePoolCapacity;
        }

        @Override
        public int getSqlColumnPoolCapacity() {
            return sqlColumnPoolCapacity;
        }

        @Override
        public double getSqlCompactMapLoadFactor() {
            return sqlCompactMapLoadFactor;
        }

        @Override
        public int getSqlExpressionPoolCapacity() {
            return sqlExpressionPoolCapacity;
        }

        @Override
        public double getSqlFastMapLoadFactor() {
            return sqlFastMapLoadFactor;
        }

        @Override
        public int getSqlJoinContextPoolCapacity() {
            return sqlJoinContextPoolCapacity;
        }

        @Override
        public int getSqlLexerPoolCapacity() {
            return sqlLexerPoolCapacity;
        }

        @Override
        public int getSqlMapKeyCapacity() {
            return sqlMapKeyCapacity;
        }

        @Override
        public int getSqlMapPageSize() {
            return sqlMapPageSize;
        }

        @Override
        public int getSqlModelPoolCapacity() {
            return sqlModelPoolCapacity;
        }

        @Override
        public int getSqlSortKeyPageSize() {
            return sqlSortKeyPageSize;
        }

        @Override
        public int getSqlSortLightValuePageSize() {
            return sqlSortLightValuePageSize;
        }

        @Override
        public int getSqlHashJoinValuePageSize() {
            return sqlHashJoinValuePageSize;
        }

        @Override
        public int getSqlTreePageSize() {
            return sqlTreePageSize;
        }

        @Override
        public int getSqlHashJoinLightValuePageSize() {
            return sqlHashJoinLightValuePageSize;
        }

        @Override
        public int getSqlSortValuePageSize() {
            return sqlSortValuePageSize;
        }

        @Override
        public long getWorkStealTimeoutNanos() {
            return workStealTimeoutNanos;
        }

        @Override
        public boolean isParallelIndexingEnabled() {
            return parallelIndexingEnabled;
        }

        @Override
        public int getSqlJoinMetadataPageSize() {
            return sqlJoinMetadataPageSize;
        }
    }

    private class PropLineUdpReceiverConfiguration implements LineUdpReceiverConfiguration {
        @Override
        public int getBindIPv4Address() {
            return lineUdpBindIPV4Address;
        }

        @Override
        public int getCommitRate() {
            return lineUdpCommitRate;
        }

        @Override
        public int getGroupIPv4Address() {
            return lineUdpGroupIPv4Address;
        }

        @Override
        public int getMsgBufferSize() {
            return lineUdpMsgBufferSize;
        }

        @Override
        public int getMsgCount() {
            return lineUdpMsgCount;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public int getPort() {
            return lineUdpPort;
        }

        @Override
        public int getReceiveBufferSize() {
            return lineUdpReceiveBufferSize;
        }
    }

    private class PropJsonQueryProcessorConfiguration implements JsonQueryProcessorConfiguration {
        @Override
        public CharSequence getKeepAliveHeader() {
            return keepAliveHeader;
        }

        @Override
        public int getFloatScale() {
            return jsonQueryFloatScale;
        }

        @Override
        public int getDoubleScale() {
            return jsonQueryDoubleScale;
        }

        @Override
        public int getConnectionCheckFrequency() {
            return jsonQueryConnectionCheckFrequency;
        }
    }

    private class PropWorkerPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public int[] getWorkerAffinity() {
            return sharedWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return sharedWorkerCount;
        }

        @Override
        public boolean haltOnError() {
            return shareWorkerHaltOnError;
        }
    }
}
