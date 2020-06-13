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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.MimeTypesCache;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.line.*;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.DefaultPGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.types.InputFormatConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.microtime.DateFormatCompiler;
import io.questdb.std.microtime.*;
import io.questdb.std.str.Path;
import io.questdb.std.time.*;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

public class PropServerConfiguration implements ServerConfiguration {
    public static final String CONFIG_DIRECTORY = "conf";
    private final IODispatcherConfiguration httpIODispatcherConfiguration = new HttpIODispatcherConfiguration();
    private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new PropStaticContentProcessorConfiguration();
    private final HttpServerConfiguration httpServerConfiguration = new PropHttpServerConfiguration();
    private final TextConfiguration textConfiguration = new PropTextConfiguration();
    private final CairoConfiguration cairoConfiguration = new PropCairoConfiguration();
    private final LineUdpReceiverConfiguration lineUdpReceiverConfiguration = new PropLineUdpReceiverConfiguration();
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new PropJsonQueryProcessorConfiguration();
    private final int commitMode;
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
    private final int sqlCopyModelPoolCapacity;
    private final double sqlCompactMapLoadFactor;
    private final int sqlExpressionPoolCapacity;
    private final double sqlFastMapLoadFactor;
    private final int sqlJoinContextPoolCapacity;
    private final int sqlLexerPoolCapacity;
    private final int sqlMapKeyCapacity;
    private final int sqlMapPageSize;
    private final int sqlModelPoolCapacity;
    private final long sqlSortKeyPageSize;
    private final long sqlSortLightValuePageSize;
    private final int sqlHashJoinValuePageSize;
    private final long sqlLatestByRowCount;
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
    private final int lineUdpCommitMode;
    private final int[] sharedWorkerAffinity;
    private final int sharedWorkerCount;
    private final boolean sharedWorkerHaltOnError;
    private final WorkerPoolConfiguration workerPoolConfiguration = new PropWorkerPoolConfiguration();
    private final PGWireConfiguration pgWireConfiguration = new DefaultPGWireConfiguration() {
        @Override
        public int getWorkerCount() {
            return 0;
        }
    };
    private final InputFormatConfiguration inputFormatConfiguration;
    private final LineProtoTimestampAdapter lineUdpTimestampAdapter;
    private final String inputRoot;
    private final boolean lineUdpEnabled;
    private final int lineUdpOwnThreadAffinity;
    private final boolean lineUdpUnicast;
    private final boolean lineUdpOwnThread;
    private final int sqlCopyBufferSize;
    private final int sqlAnalyticColumnPoolCapacity;
    private final int sqlCreateTableModelPoolCapacity;
    private final int sqlColumnCastModelPoolCapacity;
    private final int sqlRenameTableModelPoolCapacity;
    private final int sqlWithClauseModelPoolCapacity;
    private final int sqlInsertModelPoolCapacity;
    private final int sqlGroupByPoolCapacity;
    private final int sqlGroupByMapCapacity;
    private final DateLocale dateLocale;
    private final TimestampLocale timestampLocale;
    private final String backupRoot;
    private final TimestampFormat backupDirTimestampFormat;
    private final CharSequence backupTempDirName;
    private final int backupMkdirMode;
    private final int floatToStrCastScale;
    private final int doubleToStrCastScale;
    private boolean httpAllowDeflateBeforeSend;
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
    private int activeConnectionLimit;
    private int eventCapacity;
    private int ioQueueCapacity;
    private long idleConnectionTimeout;
    private int interestQueueCapacity;
    private int listenBacklog;
    private int sndBufSize;
    private int rcvBufSize;
    private int dateAdapterPoolCapacity;
    private int jsonCacheLimit;
    private int jsonCacheSize;
    private double maxRequiredDelimiterStdDev;
    private double maxRequiredLineLengthStdDev;
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
    private boolean httpFrozenClock;
    private boolean readOnlySecurityContext;
    private long maxInMemoryRows;
    private long maxHttpQueryResponseRowLimit;

    public PropServerConfiguration(String root, Properties properties) throws ServerConfigurationException, JsonException {
        this.sharedWorkerCount = getInt(properties, "shared.worker.count", 2);
        this.sharedWorkerAffinity = getAffinity(properties, "shared.worker.affinity", sharedWorkerCount);
        this.sharedWorkerHaltOnError = getBoolean(properties, "shared.worker.haltOnError", false);
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
            this.httpFrozenClock = getBoolean(properties, "http.frozen.clock", false);
            this.httpAllowDeflateBeforeSend = getBoolean(properties, "http.allow.deflate.before.send", false);

            int keepAliveTimeout = getInt(properties, "http.keep-alive.timeout", 5);
            int keepAliveMax = getInt(properties, "http.keep-alive.max", 10_000);

            if (keepAliveTimeout > 0 && keepAliveMax > 0) {
                this.keepAliveHeader = "Keep-Alive: timeout=" + keepAliveTimeout + ", max=" + keepAliveMax + Misc.EOL;
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

            this.activeConnectionLimit = getInt(properties, "http.net.active.connection.limit", 256);
            this.eventCapacity = getInt(properties, "http.net.event.capacity", 1024);
            this.ioQueueCapacity = getInt(properties, "http.net.io.queue.capacity", 1024);
            this.idleConnectionTimeout = getLong(properties, "http.net.idle.connection.timeout", 5 * 60 * 1000L);
            this.interestQueueCapacity = getInt(properties, "http.net.interest.queue.capacity", 1024);
            this.listenBacklog = getInt(properties, "http.net.listen.backlog", 256);
            this.sndBufSize = getIntSize(properties, "http.net.snd.buf.size", 2 * 1024 * 1024);
            this.rcvBufSize = getIntSize(properties, "http.net.rcv.buf.size", 2 * 1024 * 1024);
            this.dateAdapterPoolCapacity = getInt(properties, "http.text.date.adapter.pool.capacity", 16);
            this.jsonCacheLimit = getIntSize(properties, "http.text.json.cache.limit", 16384);
            this.jsonCacheSize = getIntSize(properties, "http.text.json.cache.size", 8192);
            this.maxRequiredDelimiterStdDev = getDouble(properties, "http.text.max.required.delimiter.stddev", 0.1222d);
            this.maxRequiredLineLengthStdDev = getDouble(properties, "http.text.max.required.line.length.stddev", 0.8);
            this.metadataStringPoolCapacity = getInt(properties, "http.text.metadata.string.pool.capacity", 128);

            this.rollBufferLimit = getIntSize(properties, "http.text.roll.buffer.limit", 1024 * 4096);
            this.rollBufferSize = getIntSize(properties, "http.text.roll.buffer.size", 1024);
            this.textAnalysisMaxLines = getInt(properties, "http.text.analysis.max.lines", 1000);
            this.textLexerStringPoolCapacity = getInt(properties, "http.text.lexer.string.pool.capacity", 64);
            this.timestampAdapterPoolCapacity = getInt(properties, "http.text.timestamp.adapter.pool.capacity", 64);
            this.utf8SinkSize = getIntSize(properties, "http.text.utf8.sink.size", 4096);

            this.jsonQueryConnectionCheckFrequency = getInt(properties, "http.json.query.connection.check.frequency", 1_000_000);
            this.jsonQueryFloatScale = getInt(properties, "http.json.query.float.scale", 4);
            this.jsonQueryDoubleScale = getInt(properties, "http.json.query.double.scale", 12);
            this.readOnlySecurityContext = getBoolean(properties, "http.security.readonly", false);
            this.maxInMemoryRows = getLong(properties, "http.security.max.in.memory.rows", Long.MAX_VALUE);
            this.maxHttpQueryResponseRowLimit = getLong(properties, "http.security.max.response.rows", Long.MAX_VALUE);

            parseBindTo(properties, "http.bind.to", "0.0.0.0:9000", (a, p) -> {
                bindIPv4Address = a;
                bindPort = p;
            });

            // load mime types
            try (Path path = new Path().of(new File(new File(root, CONFIG_DIRECTORY), "mime.types").getAbsolutePath()).$()) {
                this.mimeTypesCache = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
            }
        }

        this.commitMode = getCommitMode(properties, "cairo.commit.mode");
        this.createAsSelectRetryCount = getInt(properties, "cairo.create.as.select.retry.count", 5);
        this.defaultMapType = getString(properties, "cairo.default.map.type", "fast");
        this.defaultSymbolCacheFlag = getBoolean(properties, "cairo.default.symbol.cache.flag", true);
        this.defaultSymbolCapacity = getInt(properties, "cairo.default.symbol.capacity", 256);
        this.fileOperationRetryCount = getInt(properties, "cairo.file.operation.retry.count", 30);
        this.idleCheckInterval = getLong(properties, "cairo.idle.check.interval", 5 * 60 * 1000L);
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
        this.sqlMapKeyCapacity = getInt(properties, "cairo.sql.map.key.capacity", 2048 * 1024);
        this.sqlMapPageSize = getIntSize(properties, "cairo.sql.map.page.size", 4 * 1024 * 1024);
        this.sqlModelPoolCapacity = getInt(properties, "cairo.model.pool.capacity", 1024);
        this.sqlSortKeyPageSize = getLongSize(properties, "cairo.sql.sort.key.page.size", 4 * 1024 * 1024);
        this.sqlSortLightValuePageSize = getLongSize(properties, "cairo.sql.sort.light.value.page.size", 1048576);
        this.sqlHashJoinValuePageSize = getIntSize(properties, "cairo.sql.hash.join.value.page.size", 16777216);
        this.sqlLatestByRowCount = getInt(properties, "cairo.sql.latest.by.row.count", 1000);
        this.sqlHashJoinLightValuePageSize = getIntSize(properties, "cairo.sql.hash.join.light.value.page.size", 1048576);
        this.sqlSortValuePageSize = getIntSize(properties, "cairo.sql.sort.value.page.size", 16777216);
        this.workStealTimeoutNanos = getLong(properties, "cairo.work.steal.timeout.nanos", 10_000);
        this.parallelIndexingEnabled = getBoolean(properties, "cairo.parallel.indexing.enabled", true);
        this.sqlJoinMetadataPageSize = getIntSize(properties, "cairo.sql.join.metadata.page.size", 16384);
        this.sqlAnalyticColumnPoolCapacity = getInt(properties, "cairo.sql.analytic.column.pool.capacity", 64);
        this.sqlCreateTableModelPoolCapacity = getInt(properties, "cairo.sql.create.table.model.pool.capacity", 16);
        this.sqlColumnCastModelPoolCapacity = getInt(properties, "cairo.sql.column.cast.model.pool.capacity", 16);
        this.sqlRenameTableModelPoolCapacity = getInt(properties, "cairo.sql.rename.table.model.pool.capacity", 16);
        this.sqlWithClauseModelPoolCapacity = getInt(properties, "cairo.sql.with.clause.model.pool.capacity", 128);
        this.sqlInsertModelPoolCapacity = getInt(properties, "cairo.sql.insert.model.pool.capacity", 64);
        this.sqlCopyModelPoolCapacity = getInt(properties, "cairo.sql.copy.model.pool.capacity", 32);
        this.sqlCopyBufferSize = getIntSize(properties, "cairo.sql.copy.buffer.size", 2 * 1024 * 1024);
        this.doubleToStrCastScale = getInt(properties, "cairo.sql.double.cast.scale", 12);
        this.floatToStrCastScale = getInt(properties, "cairo.sql.float.cast.scale", 4);
        this.sqlGroupByMapCapacity = getInt(properties, "cairo.sql.groupby.map.capacity", 1024);
        this.sqlGroupByPoolCapacity = getInt(properties, "cairo.sql.groupby.pool.capacity", 1024);
        final String sqlCopyFormatsFile = getString(properties, "cairo.sql.copy.formats.file", "/text_loader.json");

        final String dateLocale = getString(properties, "cairo.date.locale", "en");
        this.dateLocale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
        if (this.dateLocale == null) {
            throw new ServerConfigurationException("cairo.date.locale", dateLocale);
        }

        final String timestampLocale = getString(properties, "cairo.timestamp.locale", "en");
        this.timestampLocale = TimestampLocaleFactory.INSTANCE.getLocale(timestampLocale);
        if (timestampLocale == null) {
            throw new ServerConfigurationException("cairo.timestamp.locale", timestampLocale);
        }

        this.inputFormatConfiguration = new InputFormatConfiguration(
                new DateFormatFactory(),
                DateLocaleFactory.INSTANCE,
                new TimestampFormatFactory(),
                TimestampLocaleFactory.INSTANCE,
                this.dateLocale,
                this.timestampLocale
        );

        try (JsonLexer lexer = new JsonLexer(1024, 1024)) {
            inputFormatConfiguration.parseConfiguration(lexer, sqlCopyFormatsFile);
        }

        this.inputRoot = getString(properties, "cairo.sql.copy.root", null);
        this.backupRoot = getString(properties, "cairo.sql.backup.root", null);
        this.backupDirTimestampFormat = getTimestampFormat(properties, "cairo.sql.backup.dir.datetime.format", "yyyy-MM-dd");
        this.backupTempDirName = getString(properties, "cairo.sql.backup.dir.tmp.name", "tmp");
        this.backupMkdirMode = getInt(properties, "cairo.sql.backup.mkdir.mode", 509);

        parseBindTo(properties, "line.udp.bind.to", "0.0.0.0:9009", (a, p) -> {
            this.lineUdpBindIPV4Address = a;
            this.lineUdpPort = p;
        });

        this.lineUdpGroupIPv4Address = getIPv4Address(properties, "line.udp.join", "232.1.2.3");
        this.lineUdpCommitRate = getInt(properties, "line.udp.commit.rate", 1_000_000);
        this.lineUdpMsgBufferSize = getIntSize(properties, "line.udp.msg.buffer.size", 2048);
        this.lineUdpMsgCount = getInt(properties, "line.udp.msg.count", 10_000);
        this.lineUdpReceiveBufferSize = getIntSize(properties, "line.udp.receive.buffer.size", 8 * 1024 * 1024);
        this.lineUdpEnabled = getBoolean(properties, "line.udp.enabled", true);
        this.lineUdpOwnThreadAffinity = getInt(properties, "line.udp.own.thread.affinity", -1);
        this.lineUdpOwnThread = getBoolean(properties, "line.udp.own.thread", false);
        this.lineUdpUnicast = getBoolean(properties, "line.udp.unicast", false);
        this.lineUdpCommitMode = getCommitMode(properties, "line.udp.commit.mode");

        final String lineUdpTimestampSwitch = getString(properties, "line.udp.timestamp", "n");
        switch (lineUdpTimestampSwitch) {
            case "u":
                lineUdpTimestampAdapter = LineProtoMicroTimestampAdapter.INSTANCE;
                break;
            case "ms":
                lineUdpTimestampAdapter = LineProtoMilliTimestampAdapter.INSTANCE;
                break;
            case "s":
                lineUdpTimestampAdapter = LineProtoSecondTimestampAdapter.INSTANCE;
                break;
            case "m":
                lineUdpTimestampAdapter = LineProtoMinuteTimestampAdapter.INSTANCE;
                break;
            case "h":
                lineUdpTimestampAdapter = LineProtoHourTimestampAdapter.INSTANCE;
                break;
            default:
                lineUdpTimestampAdapter = LineProtoNanoTimestampAdapter.INSTANCE;
                break;
        }
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
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return lineUdpReceiverConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return workerPoolConfiguration;
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return pgWireConfiguration;
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

    private int getCommitMode(Properties properties, String property) {
        final String commitMode = properties.getProperty(property);

        if (commitMode == null) {
            return CommitMode.NOSYNC;
        }

        if (Chars.equalsLowerCaseAscii(commitMode, "nosync")) {
            return CommitMode.NOSYNC;
        }

        if (Chars.equalsLowerCaseAscii(commitMode, "async")) {
            return CommitMode.ASYNC;
        }

        if (Chars.equalsLowerCaseAscii(commitMode, "sync")) {
            return CommitMode.SYNC;
        }

        return CommitMode.NOSYNC;
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

    private long getLongSize(Properties properties, String key, long defaultValue) throws ServerConfigurationException {
        final String value = properties.getProperty(key);
        try {
            return value != null ? Numbers.parseLongSize(value) : defaultValue;
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

    private TimestampFormat getTimestampFormat(Properties properties, String key, String defaultPattern) {
        String pattern = properties.getProperty(key);
        if (null == pattern) {
            pattern = defaultPattern;
        }
        DateFormatCompiler compiler = new DateFormatCompiler();
        if (null != pattern) {
            return compiler.compile(pattern);
        }
        return compiler.compile(defaultPattern);
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
        public String getDispatcherLogName() {
            return "http-server";
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
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
        public int getInitialBias() {
            return IOOperation.READ;
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
        public int getRcvBufSize() {
            return rcvBufSize;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSndBufSize() {
            return sndBufSize;
        }
    }

    private class PropTextConfiguration implements TextConfiguration {

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
        public double getMaxRequiredLineLengthStdDev() {
            return maxRequiredLineLengthStdDev;
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

        @Override
        public InputFormatConfiguration getInputFormatConfiguration() {
            return inputFormatConfiguration;
        }

        @Override
        public DateLocale getDefaultDateLocale() {
            return dateLocale;
        }

        @Override
        public TimestampLocale getDefaultTimestampLocale() {
            return timestampLocale;
        }
    }

    private class PropHttpServerConfiguration implements HttpServerConfiguration {

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
        public int getQueryCacheBlocks() {
            return sqlCacheBlocks;
        }

        @Override
        public int getQueryCacheRows() {
            return sqlCacheRows;
        }

        @Override
        public MillisecondClock getClock() {
            return httpFrozenClock ? StationaryMillisClock.INSTANCE : MillisecondClockImpl.INSTANCE;
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
        public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
            return jsonQueryProcessorConfiguration;
        }

        @Override
        public int getSendBufferSize() {
            return sendBufferSize;
        }

        @Override
        public boolean isEnabled() {
            return httpServerEnabled;
        }

        @Override
        public boolean getDumpNetworkTraffic() {
            return false;
        }

        @Override
        public boolean allowDeflateBeforeSend() {
            return httpAllowDeflateBeforeSend;
        }

        @Override
        public int[] getWorkerAffinity() {
            return httpWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return httpWorkerCount;
        }

        @Override
        public boolean haltOnError() {
            return httpWorkerHaltOnError;
        }

        @Override
        public boolean readOnlySecurityContext() {
            return readOnlySecurityContext;
        }

        @Override
        public long getMaxInMemoryRows() {
            return maxInMemoryRows;
        }
    }

    private class PropCairoConfiguration implements CairoConfiguration {

        @Override
        public int getSqlCopyBufferSize() {
            return sqlCopyBufferSize;
        }

        @Override
        public int getCopyPoolCapacity() {
            return sqlCopyModelPoolCapacity;
        }

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
        public boolean enableTestFactories() {
            return true;
        }

        @Override
        public int getDoubleToStrCastScale() {
            return doubleToStrCastScale;
        }

        @Override
        public int getFloatToStrCastScale() {
            return floatToStrCastScale;
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
        public NanosecondClock getNanosecondClock() {
            return NanosecondClockImpl.INSTANCE;
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
        public CharSequence getInputRoot() {
            return inputRoot;
        }

        @Override
        public CharSequence getBackupRoot() {
            return backupRoot;
        }

        @Override
        public TimestampFormat getBackupDirTimestampFormat() {
            return backupDirTimestampFormat;
        }

        @Override
        public CharSequence getBackupTempDirName() {
            return backupTempDirName;
        }

        @Override
        public int getBackupMkDirMode() {
            return backupMkdirMode;
        }

        @Override
        public long getSpinLockTimeoutUs() {
            return spinLockTimeoutUs;
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
        public long getSqlSortKeyPageSize() {
            return sqlSortKeyPageSize;
        }

        @Override
        public long getSqlSortLightValuePageSize() {
            return sqlSortLightValuePageSize;
        }

        @Override
        public int getSqlHashJoinValuePageSize() {
            return sqlHashJoinValuePageSize;
        }

        @Override
        public long getSqlLatestByRowCount() {
            return sqlLatestByRowCount;
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
        public TextConfiguration getTextConfiguration() {
            return textConfiguration;
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

        @Override
        public int getAnalyticColumnPoolCapacity() {
            return sqlAnalyticColumnPoolCapacity;
        }

        @Override
        public int getCreateTableModelPoolCapacity() {
            return sqlCreateTableModelPoolCapacity;
        }

        @Override
        public int getColumnCastModelPoolCapacity() {
            return sqlColumnCastModelPoolCapacity;
        }

        @Override
        public int getRenameTableModelPoolCapacity() {
            return sqlRenameTableModelPoolCapacity;
        }

        @Override
        public int getWithClauseModelPoolCapacity() {
            return sqlWithClauseModelPoolCapacity;
        }

        @Override
        public int getInsertPoolCapacity() {
            return sqlInsertModelPoolCapacity;
        }

        @Override
        public int getCommitMode() {
            return commitMode;
        }

        @Override
        public DateLocale getDefaultDateLocale() {
            return dateLocale;
        }

        @Override
        public TimestampLocale getDefaultTimestampLocale() {
            return timestampLocale;
        }

        @Override
        public int getGroupByPoolCapacity() {
            return sqlGroupByPoolCapacity;
        }

        @Override
        public int getGroupByMapCapacity() {
            return sqlGroupByMapCapacity;
        }
    }

    private class PropLineUdpReceiverConfiguration implements LineUdpReceiverConfiguration {
        @Override
        public int getCommitMode() {
            return lineUdpCommitMode;
        }

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

        @Override
        public CairoSecurityContext getCairoSecurityContext() {
            return AllowAllCairoSecurityContext.INSTANCE;
        }

        @Override
        public boolean isEnabled() {
            return lineUdpEnabled;
        }

        @Override
        public boolean isUnicast() {
            return lineUdpUnicast;
        }

        @Override
        public boolean ownThread() {
            return lineUdpOwnThread;
        }

        @Override
        public int ownThreadAffinity() {
            return lineUdpOwnThreadAffinity;
        }

        @Override
        public LineProtoTimestampAdapter getTimestampAdapter() {
            return lineUdpTimestampAdapter;
        }
    }

    private class PropJsonQueryProcessorConfiguration implements JsonQueryProcessorConfiguration {
        @Override
        public MillisecondClock getClock() {
            return httpFrozenClock ? StationaryMillisClock.INSTANCE : MillisecondClockImpl.INSTANCE;
        }

        @Override
        public int getConnectionCheckFrequency() {
            return jsonQueryConnectionCheckFrequency;
        }

        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
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
        public CharSequence getKeepAliveHeader() {
            return keepAliveHeader;
        }

        @Override
        public long getMaxQueryResponseRowLimit() {
            return maxHttpQueryResponseRowLimit;
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
            return sharedWorkerHaltOnError;
        }
    }
}
