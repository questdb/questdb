/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.line.*;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.types.InputFormatConfiguration;
import io.questdb.griffin.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.log.Log;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.*;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class PropServerConfiguration implements ServerConfiguration {
    public static final String CONFIG_DIRECTORY = "conf";
    public static final String DB_DIRECTORY = "db";
    private final IODispatcherConfiguration httpIODispatcherConfiguration = new PropHttpIODispatcherConfiguration();
    private final WaitProcessorConfiguration httpWaitProcessorConfiguration = new PropWaitProcessorConfiguration();
    private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new PropStaticContentProcessorConfiguration();
    private final HttpServerConfiguration httpServerConfiguration = new PropHttpServerConfiguration();
    private final TextConfiguration textConfiguration = new PropTextConfiguration();
    private final CairoConfiguration cairoConfiguration = new PropCairoConfiguration();
    private final LineUdpReceiverConfiguration lineUdpReceiverConfiguration = new PropLineUdpReceiverConfiguration();
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new PropJsonQueryProcessorConfiguration();
    private final TelemetryConfiguration telemetryConfiguration = new PropTelemetryConfiguration();
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
    private final boolean httpSqlCacheEnabled;
    private final int httpSqlCacheBlockCount;
    private final int httpSqlCacheRowCount;
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
    private final int sqlMapMaxPages;
    private final int sqlMapMaxResizes;
    private final int sqlModelPoolCapacity;
    private final long sqlSortKeyPageSize;
    private final int sqlSortKeyMaxPages;
    private final long sqlSortLightValuePageSize;
    private final int sqlSortLightValueMaxPages;
    private final int sqlHashJoinValuePageSize;
    private final int sqlHashJoinValueMaxPages;
    private final long sqlLatestByRowCount;
    private final int sqlHashJoinLightValuePageSize;
    private final int sqlHashJoinLightValueMaxPages;
    private final int sqlSortValuePageSize;
    private final int sqlSortValueMaxPages;
    private final long workStealTimeoutNanos;
    private final boolean parallelIndexingEnabled;
    private final int sqlJoinMetadataPageSize;
    private final int sqlJoinMetadataMaxResizes;
    private final int lineUdpCommitRate;
    private final int lineUdpGroupIPv4Address;
    private final int lineUdpMsgBufferSize;
    private final int lineUdpMsgCount;
    private final int lineUdpReceiveBufferSize;
    private final int lineUdpCommitMode;
    private final int[] sharedWorkerAffinity;
    private final int sharedWorkerCount;
    private final boolean sharedWorkerHaltOnError;
    private final long sharedWorkerYieldThreshold;
    private final long sharedWorkerSleepThreshold;
    private final long sharedWorkerSleepMs;
    private final WorkerPoolConfiguration workerPoolConfiguration = new PropWorkerPoolConfiguration();
    private final PGWireConfiguration pgWireConfiguration = new PropPGWireConfiguration();
    private final InputFormatConfiguration inputFormatConfiguration;
    private final LineProtoTimestampAdapter lineUdpTimestampAdapter;
    private int lineUdpDefaultPartitionBy;
    private final String inputRoot;
    private final boolean lineUdpEnabled;
    private final int lineUdpOwnThreadAffinity;
    private final boolean lineUdpUnicast;
    private final boolean lineUdpOwnThread;
    private final int sqlCopyBufferSize;
    private final long writerDataAppendPageSize;
    private final long writerMiscAppendPageSize;
    private final int sqlAnalyticColumnPoolCapacity;
    private final int sqlCreateTableModelPoolCapacity;
    private final int sqlColumnCastModelPoolCapacity;
    private final int sqlRenameTableModelPoolCapacity;
    private final int sqlWithClauseModelPoolCapacity;
    private final int sqlInsertModelPoolCapacity;
    private final int sqlGroupByPoolCapacity;
    private final int sqlGroupByMapCapacity;
    private final int sqlMaxSymbolNotEqualsCount;
    private final int sqlBindVariablePoolSize;
    private final int sqlPageFrameMaxSize;
    private final int sqlJitMode;
    private final int sqlJitIRMemoryPageSize;
    private final int sqlJitIRMemoryMaxPages;
    private final int sqlJitBindVarsMemoryPageSize;
    private final int sqlJitBindVarsMemoryMaxPages;
    private final int sqlJitRowsThreshold;
    private final int sqlJitPageAddressCacheThreshold;
    private final boolean sqlJitDebugEnabled;
    private final DateLocale locale;
    private final String backupRoot;
    private final DateFormat backupDirTimestampFormat;
    private final CharSequence backupTempDirName;
    private final int backupMkdirMode;
    private final int sqlFloatToStrCastScale;
    private final int sqlDoubleToStrCastScale;
    private final PropPGWireDispatcherConfiguration propPGWireDispatcherConfiguration = new PropPGWireDispatcherConfiguration();
    private final boolean pgEnabled;
    private final boolean telemetryEnabled;
    private final boolean telemetryDisableCompletely;
    private final int telemetryQueueCapacity;
    private final LineTcpReceiverConfiguration lineTcpReceiverConfiguration = new PropLineTcpReceiverConfiguration();
    private final IODispatcherConfiguration lineTcpReceiverDispatcherConfiguration = new PropLineTcpReceiverIODispatcherConfiguration();
    private final boolean lineTcpEnabled;
    private final WorkerPoolAwareConfiguration lineTcpWriterWorkerPoolConfiguration = new PropLineTcpWriterWorkerPoolConfiguration();
    private final WorkerPoolAwareConfiguration lineTcpIOWorkerPoolConfiguration = new PropLineTcpIOWorkerPoolConfiguration();
    private final Log log;
    private final PropHttpMinServerConfiguration httpMinServerConfiguration = new PropHttpMinServerConfiguration();
    private final PropHttpContextConfiguration httpContextConfiguration = new PropHttpContextConfiguration();
    private final boolean httpMinServerEnabled;
    private final PropHttpMinIODispatcherConfiguration httpMinIODispatcherConfiguration = new PropHttpMinIODispatcherConfiguration();
    private final PropSqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new PropSqlExecutionCircuitBreakerConfiguration();
    private final int sqlAnalyticStorePageSize;
    private final int sqlAnalyticStoreMaxPages;
    private final int sqlAnalyticRowIdPageSize;
    private final int sqlAnalyticRowIdMaxPages;
    private final int sqlAnalyticTreeKeyPageSize;
    private final int sqlAnalyticTreeKeyMaxPages;
    private final String root;
    private final String dbDirectory;
    private final String confRoot;
    private final long maxRerunWaitCapMs;
    private final double rerunExponentialWaitMultiplier;
    private final int rerunInitialWaitQueueSize;
    private final int rerunMaxProcessingQueueSize;
    private final BuildInformation buildInformation;
    private final int columnIndexerQueueCapacity;
    private final int vectorAggregateQueueCapacity;
    private final int o3CallbackQueueCapacity;
    private final int o3PartitionQueueCapacity;
    private final int o3OpenColumnQueueCapacity;
    private final int o3CopyQueueCapacity;
    private final int o3UpdPartitionSizeQueueCapacity;
    private final int o3PurgeDiscoveryQueueCapacity;
    private final int o3ColumnMemorySize;
    private final int maxUncommittedRows;
    private final long commitLag;
    private final long instanceHashLo;
    private final long instanceHashHi;
    private final int sqlTxnScoreboardEntryCount;
    private final boolean o3QuickSortEnabled;
    private final MetricsConfiguration metricsConfiguration = new PropMetricsConfiguration();
    private final boolean metricsEnabled;
    private final int sqlDistinctTimestampKeyCapacity;
    private final double sqlDistinctTimestampLoadFactor;
    private final int circuitBreakerThrottle;
    private final int circuitBreakerBufferSize;
    private final long circuitBreakerMaxTime;
    private final int latestByQueueCapacity;
    private final int sampleByIndexSearchPageSize;
    private final int binaryEncodingMaxLength;
    private final long writerDataIndexKeyAppendPageSize;
    private final long writerDataIndexValueAppendPageSize;
    private final long writerAsyncCommandBusyWaitTimeout;
    private final int writerAsyncCommandQueueCapacity;
    private final int writerTickRowsCountMod;
    private final long writerAsyncCommandMaxWaitTimeout;
    private final int o3PartitionPurgeListCapacity;
    private int httpMinNetConnectionLimit;
    private boolean httpMinNetConnectionHint;
    private boolean httpAllowDeflateBeforeSend;
    private int[] httpWorkerAffinity;
    private int[] httpMinWorkerAffinity;
    private int connectionPoolInitialCapacity;
    private int connectionStringPoolCapacity;
    private int multipartHeaderBufferSize;
    private long multipartIdleSpinCount;
    private int recvBufferSize;
    private int requestHeaderBufferSize;
    private int httpWorkerCount;
    private boolean httpWorkerHaltOnError;
    private long httpWorkerYieldThreshold;
    private long httpWorkerSleepThreshold;
    private long httpWorkerSleepMs;
    private boolean httpServerKeepAlive;
    private int sendBufferSize;
    private CharSequence indexFileName;
    private String publicDirectory;
    private int httpNetConnectionLimit;
    private boolean httpNetConnectionHint;
    private long httpNetConnectionTimeout;
    private long httpNetConnectionQueueTimeout;
    private int httpNetConnectionSndBuf;
    private int httpNetConnectionRcvBuf;
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
    private String keepAliveHeader;
    private int httpNetBindIPv4Address;
    private int httpNetBindPort;
    private int lineUdpBindIPV4Address;
    private int lineUdpPort;
    private int jsonQueryFloatScale;
    private int jsonQueryDoubleScale;
    private int jsonQueryConnectionCheckFrequency;
    private boolean httpFrozenClock;
    private boolean readOnlySecurityContext;
    private long maxHttpQueryResponseRowLimit;
    private boolean interruptOnClosedConnection;
    private int pgNetConnectionLimit;
    private boolean pgNetConnectionHint;
    private int pgNetBindIPv4Address;
    private int pgNetBindPort;
    private long pgNetIdleConnectionTimeout;
    private long pgNetConnectionQueueTimeout;
    private int pgNetConnectionRcvBuf;
    private int pgNetConnectionSndBuf;
    private int pgCharacterStoreCapacity;
    private int pgBinaryParamsCapacity;
    private int pgCharacterStorePoolCapacity;
    private int pgConnectionPoolInitialCapacity;
    private String pgPassword;
    private String pgUsername;
    private int pgMaxBlobSizeOnQuery;
    private int pgRecvBufferSize;
    private int pgSendBufferSize;
    private DateLocale pgDefaultLocale;
    private int[] pgWorkerAffinity;
    private int pgWorkerCount;
    private boolean pgHaltOnError;
    private long pgWorkerYieldThreshold;
    private long pgWorkerSleepThreshold;
    private boolean pgDaemonPool;
    private boolean pgSelectCacheEnabled;
    private int pgSelectCacheBlockCount;
    private int pgSelectCacheRowCount;
    private boolean pgInsertCacheEnabled;
    private int pgInsertCacheBlockCount;
    private int pgInsertCacheRowCount;
    private int pgInsertPoolCapacity;
    private int pgNamedStatementCacheCapacity;
    private int pgNamesStatementPoolCapacity;
    private int pgPendingWritersCacheCapacity;
    private int lineTcpNetConnectionLimit;
    private boolean lineTcpNetConnectionHint;
    private int lineTcpNetBindIPv4Address;
    private int lineTcpNetBindPort;
    private long lineTcpNetConnectionTimeout;
    private long lineTcpNetConnectionQueueTimeout;
    private int lineTcpNetConnectionRcvBuf;
    private int lineTcpConnectionPoolInitialCapacity;
    private LineProtoTimestampAdapter lineTcpTimestampAdapter;
    private int lineTcpMsgBufferSize;
    private int lineTcpMaxMeasurementSize;
    private int lineTcpWriterQueueCapacity;
    private int lineTcpWriterWorkerCount;
    private int[] lineTcpWriterWorkerAffinity;
    private boolean lineTcpWriterWorkerPoolHaltOnError;
    private long lineTcpWriterWorkerYieldThreshold;
    private long lineTcpWriterWorkerSleepThreshold;
    private int lineTcpIOWorkerCount;
    private int[] lineTcpIOWorkerAffinity;
    private boolean lineTcpIOWorkerPoolHaltOnError;
    private long lineTcpIOWorkerYieldThreshold;
    private long lineTcpIOWorkerSleepThreshold;
    private long lineTcpMaintenanceInterval;
    private long lineTcpCommitTimeout;
    private String lineTcpAuthDbPath;
    private int lineTcpDefaultPartitionBy;
    private long minIdleMsBeforeWriterRelease;
    private String httpVersion;
    private int httpMinWorkerCount;
    private boolean httpMinWorkerHaltOnError;
    private long httpMinWorkerYieldThreshold;
    private long httpMinWorkerSleepThreshold;
    private int httpMinBindIPv4Address;
    private int httpMinBindPort;
    private long httpMinNetConnectionTimeout;
    private long httpMinNetConnectionQueueTimeout;
    private int httpMinNetConnectionRcvBuf;
    private int httpMinNetConnectionSndBuf;
    private long symbolCacheWaitUsBeforeReload;

    public PropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        this.log = log;

        this.mkdirMode = getInt(properties, env, "cairo.mkdir.mode", 509);

        this.dbDirectory = getString(properties, env, "cairo.root", DB_DIRECTORY);
        if (new File(this.dbDirectory).isAbsolute()) {
            this.root = this.dbDirectory;
            this.confRoot = confRoot(this.root); // ../conf
        } else {
            this.root = new File(root, this.dbDirectory).getAbsolutePath();
            this.confRoot = new File(root, CONFIG_DIRECTORY).getAbsolutePath();
        }

        int cpuAvailable = Runtime.getRuntime().availableProcessors();
        int cpuUsed = 0;
        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        try (Path path = new Path()) {
            ff.mkdirs(path.of(this.root).slash$(), this.mkdirMode);
            path.of(this.root).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
            final long tableIndexFd = TableUtils.openFileRWOrFail(ff, path);
            final long fileSize = ff.length(tableIndexFd);
            if (fileSize < Long.BYTES) {
                if (!ff.allocate(tableIndexFd, Files.PAGE_SIZE)) {
                    ff.close(tableIndexFd);
                    throw CairoException.instance(ff.errno()).put("Could not allocate [file=").put(path).put(", actual=").put(fileSize).put(", desired=").put(Files.PAGE_SIZE).put(']');
                }
            }

            final long tableIndexMem = TableUtils.mapRWOrClose(ff, tableIndexFd, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
            Rnd rnd = new Rnd(getCairoConfiguration().getMicrosecondClock().getTicks(), getCairoConfiguration().getMillisecondClock().getTicks());
            if (Os.compareAndSwap(tableIndexMem + Long.BYTES, 0, rnd.nextLong()) == 0) {
                Unsafe.getUnsafe().putLong(tableIndexMem + Long.BYTES * 2, rnd.nextLong());
            }
            this.instanceHashLo = Unsafe.getUnsafe().getLong(tableIndexMem + Long.BYTES);
            this.instanceHashHi = Unsafe.getUnsafe().getLong(tableIndexMem + Long.BYTES * 2);
            ff.munmap(tableIndexMem, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
            ff.close(tableIndexFd);
            ///

            this.httpMinServerEnabled = getBoolean(properties, env, "http.min.enabled", true);
            if (httpMinServerEnabled) {
                this.httpMinWorkerHaltOnError = getBoolean(properties, env, "http.min.worker.haltOnError", false);
                this.httpMinWorkerCount = getInt(properties, env, "http.min.worker.count", cpuAvailable > 16 ? 1 : 0);
                cpuUsed += this.httpMinWorkerCount;
                this.httpMinWorkerAffinity = getAffinity(properties, env, "http.min.worker.affinity", httpMinWorkerCount);
                this.httpMinWorkerYieldThreshold = getLong(properties, env, "http.min.worker.yield.threshold", 10);
                this.httpMinWorkerSleepThreshold = getLong(properties, env, "http.min.worker.sleep.threshold", 10000);

                // obsolete
                String httpMinBindTo = getString(properties, env, "http.min.bind.to", "0.0.0.0:9003");

                parseBindTo(properties, env, "http.min.net.bind.to", httpMinBindTo, (a, p) -> {
                    httpMinBindIPv4Address = a;
                    httpMinBindPort = p;
                });

                this.httpMinNetConnectionLimit = getInt(properties, env, "http.min.net.connection.limit", 4);

                // obsolete
                this.httpMinNetConnectionTimeout = getLong(properties, env, "http.min.net.idle.connection.timeout", 5 * 60 * 1000L);
                this.httpMinNetConnectionTimeout = getLong(properties, env, "http.min.net.connection.timeout", this.httpMinNetConnectionTimeout);

                // obsolete
                this.httpMinNetConnectionQueueTimeout = getLong(properties, env, "http.min.net.queued.connection.timeout", 5 * 1000L);
                this.httpMinNetConnectionQueueTimeout = getLong(properties, env, "http.min.net.connection.queue.timeout", this.httpMinNetConnectionQueueTimeout);

                // obsolete
                this.httpMinNetConnectionSndBuf = getIntSize(properties, env, "http.min.net.snd.buf.size", 1024);
                this.httpMinNetConnectionSndBuf = getIntSize(properties, env, "http.min.net.connection.sndbuf", this.httpMinNetConnectionSndBuf);

                // obsolete
                this.httpMinNetConnectionRcvBuf = getIntSize(properties, env, "http.net.rcv.buf.size", 1024);
                this.httpMinNetConnectionRcvBuf = getIntSize(properties, env, "http.min.net.connection.rcvbuf", this.httpMinNetConnectionRcvBuf);
                this.httpMinNetConnectionHint = getBoolean(properties, env, "http.min.net.connection.hint", false);
            }

            this.httpServerEnabled = getBoolean(properties, env, "http.enabled", true);
            if (httpServerEnabled) {
                this.connectionPoolInitialCapacity = getInt(properties, env, "http.connection.pool.initial.capacity", 16);
                this.connectionStringPoolCapacity = getInt(properties, env, "http.connection.string.pool.capacity", 128);
                this.multipartHeaderBufferSize = getIntSize(properties, env, "http.multipart.header.buffer.size", 512);
                this.multipartIdleSpinCount = getLong(properties, env, "http.multipart.idle.spin.count", 10_000);
                this.recvBufferSize = getIntSize(properties, env, "http.receive.buffer.size", 1024 * 1024);
                this.requestHeaderBufferSize = getIntSize(properties, env, "http.request.header.buffer.size", 32 * 2014);
                this.httpWorkerCount = getInt(properties, env, "http.worker.count", 0);
                cpuUsed += this.httpWorkerCount;
                this.httpWorkerAffinity = getAffinity(properties, env, "http.worker.affinity", httpWorkerCount);
                this.httpWorkerHaltOnError = getBoolean(properties, env, "http.worker.haltOnError", false);
                this.httpWorkerYieldThreshold = getLong(properties, env, "http.worker.yield.threshold", 10);
                this.httpWorkerSleepThreshold = getLong(properties, env, "http.worker.sleep.threshold", 10000);
                this.httpWorkerSleepMs = getLong(properties, env, "http.worker.sleep.ms", 100);
                this.sendBufferSize = getIntSize(properties, env, "http.send.buffer.size", 2 * 1024 * 1024);
                this.indexFileName = getString(properties, env, "http.static.index.file.name", "index.html");
                this.httpFrozenClock = getBoolean(properties, env, "http.frozen.clock", false);
                this.httpAllowDeflateBeforeSend = getBoolean(properties, env, "http.allow.deflate.before.send", false);
                this.httpServerKeepAlive = getBoolean(properties, env, "http.server.keep.alive", true);
                this.httpVersion = getString(properties, env, "http.version", "HTTP/1.1");
                if (!httpVersion.endsWith(" ")) {
                    httpVersion += ' ';
                }

                int keepAliveTimeout = getInt(properties, env, "http.keep-alive.timeout", 5);
                int keepAliveMax = getInt(properties, env, "http.keep-alive.max", 10_000);

                if (keepAliveTimeout > 0 && keepAliveMax > 0) {
                    this.keepAliveHeader = "Keep-Alive: timeout=" + keepAliveTimeout + ", max=" + keepAliveMax + Misc.EOL;
                } else {
                    this.keepAliveHeader = null;
                }

                final String publicDirectory = getString(properties, env, "http.static.public.directory", "public");
                // translate public directory into absolute path
                // this will generate some garbage, but this is ok - we're just doing this once on startup
                if (new File(publicDirectory).isAbsolute()) {
                    this.publicDirectory = publicDirectory;
                } else {
                    this.publicDirectory = new File(root, publicDirectory).getAbsolutePath();
                }

                // maintain obsolete property name for the time being
                this.httpNetConnectionLimit = getInt(properties, env, "http.net.active.connection.limit", 256);
                this.httpNetConnectionLimit = getInt(properties, env, "http.net.connection.limit", this.httpNetConnectionLimit);
                this.httpNetConnectionHint = getBoolean(properties, env, "http.net.connection.hint", false);
                // obsolete
                this.httpNetConnectionTimeout = getLong(properties, env, "http.net.idle.connection.timeout", 5 * 60 * 1000L);
                this.httpNetConnectionTimeout = getLong(properties, env, "http.net.connection.timeout", this.httpNetConnectionTimeout);

                // obsolete
                this.httpNetConnectionQueueTimeout = getLong(properties, env, "http.net.queued.connection.timeout", 5 * 1000L);
                this.httpNetConnectionQueueTimeout = getLong(properties, env, "http.net.connection.queue.timeout", this.httpNetConnectionQueueTimeout);

                // obsolete
                this.httpNetConnectionSndBuf = getIntSize(properties, env, "http.net.snd.buf.size", 2 * 1024 * 1024);
                this.httpNetConnectionSndBuf = getIntSize(properties, env, "http.net.connection.sndbuf", this.httpNetConnectionSndBuf);

                // obsolete
                this.httpNetConnectionRcvBuf = getIntSize(properties, env, "http.net.rcv.buf.size", 2 * 1024 * 1024);
                this.httpNetConnectionRcvBuf = getIntSize(properties, env, "http.net.connection.rcvbuf", this.httpNetConnectionRcvBuf);

                this.dateAdapterPoolCapacity = getInt(properties, env, "http.text.date.adapter.pool.capacity", 16);
                this.jsonCacheLimit = getIntSize(properties, env, "http.text.json.cache.limit", 16384);
                this.jsonCacheSize = getIntSize(properties, env, "http.text.json.cache.size", 8192);
                this.maxRequiredDelimiterStdDev = getDouble(properties, env, "http.text.max.required.delimiter.stddev", 0.1222d);
                this.maxRequiredLineLengthStdDev = getDouble(properties, env, "http.text.max.required.line.length.stddev", 0.8);
                this.metadataStringPoolCapacity = getInt(properties, env, "http.text.metadata.string.pool.capacity", 128);

                this.rollBufferLimit = getIntSize(properties, env, "http.text.roll.buffer.limit", 1024 * 4096);
                this.rollBufferSize = getIntSize(properties, env, "http.text.roll.buffer.size", 1024);
                this.textAnalysisMaxLines = getInt(properties, env, "http.text.analysis.max.lines", 1000);
                this.textLexerStringPoolCapacity = getInt(properties, env, "http.text.lexer.string.pool.capacity", 64);
                this.timestampAdapterPoolCapacity = getInt(properties, env, "http.text.timestamp.adapter.pool.capacity", 64);
                this.utf8SinkSize = getIntSize(properties, env, "http.text.utf8.sink.size", 4096);

                this.jsonQueryConnectionCheckFrequency = getInt(properties, env, "http.json.query.connection.check.frequency", 1_000_000);
                this.jsonQueryFloatScale = getInt(properties, env, "http.json.query.float.scale", 4);
                this.jsonQueryDoubleScale = getInt(properties, env, "http.json.query.double.scale", 12);
                this.readOnlySecurityContext = getBoolean(properties, env, "http.security.readonly", false);
                this.maxHttpQueryResponseRowLimit = getLong(properties, env, "http.security.max.response.rows", Long.MAX_VALUE);
                this.interruptOnClosedConnection = getBoolean(properties, env, "http.security.interrupt.on.closed.connection", true);

                String httpBindTo = getString(properties, env, "http.bind.to", "0.0.0.0:9000");
                parseBindTo(properties, env, "http.net.bind.to", httpBindTo, (a, p) -> {
                    httpNetBindIPv4Address = a;
                    httpNetBindPort = p;
                });

                // load mime types
                path.of(new File(new File(root, CONFIG_DIRECTORY), "mime.types").getAbsolutePath()).$();
                this.mimeTypesCache = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
            }

            this.maxRerunWaitCapMs = getLong(properties, env, "http.busy.retry.maximum.wait.before.retry", 1000);
            this.rerunExponentialWaitMultiplier = getDouble(properties, env, "http.busy.retry.exponential.wait.multiplier", 2.0);
            this.rerunInitialWaitQueueSize = getIntSize(properties, env, "http.busy.retry.initialWaitQueueSize", 64);
            this.rerunMaxProcessingQueueSize = getIntSize(properties, env, "http.busy.retry.maxProcessingQueueSize", 4096);

            this.circuitBreakerThrottle = getInt(properties, env, "circuit.breaker.throttle", 2_000_000);
            this.circuitBreakerBufferSize = getInt(properties, env, "circuit.breaker.buffer.size", 64);
            this.circuitBreakerMaxTime = (long) (getDouble(properties, env, "query.timeout.sec", 60) * Timestamps.SECOND_MICROS);

            this.pgEnabled = getBoolean(properties, env, "pg.enabled", true);
            if (pgEnabled) {
                // obsolete
                pgNetConnectionLimit = getInt(properties, env, "pg.net.active.connection.limit", 10);
                pgNetConnectionLimit = getInt(properties, env, "pg.net.connection.limit", pgNetConnectionLimit);
                pgNetConnectionHint = getBoolean(properties, env, "pg.net.connection.hint", false);
                parseBindTo(properties, env, "pg.net.bind.to", "0.0.0.0:8812", (a, p) -> {
                    pgNetBindIPv4Address = a;
                    pgNetBindPort = p;
                });

                // obsolete
                this.pgNetIdleConnectionTimeout = getLong(properties, env, "pg.net.idle.timeout", 300_000);
                this.pgNetIdleConnectionTimeout = getLong(properties, env, "pg.net.connection.timeout", this.pgNetIdleConnectionTimeout);
                this.pgNetConnectionQueueTimeout = getLong(properties, env, "pg.net.connection.queue.timeout", 5_000);

                // obsolete
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, "pg.net.recv.buf.size", -1);
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, "pg.net.connection.rcvbuf", this.pgNetConnectionRcvBuf);

                // obsolete
                this.pgNetConnectionSndBuf = getIntSize(properties, env, "pg.net.send.buf.size", -1);
                this.pgNetConnectionSndBuf = getIntSize(properties, env, "pg.net.connection.sndbuf", this.pgNetConnectionSndBuf);

                this.pgCharacterStoreCapacity = getInt(properties, env, "pg.character.store.capacity", 4096);
                this.pgBinaryParamsCapacity = getInt(properties, env, "pg.binary.param.count.capacity", 2);
                this.pgCharacterStorePoolCapacity = getInt(properties, env, "pg.character.store.pool.capacity", 64);
                this.pgConnectionPoolInitialCapacity = getInt(properties, env, "pg.connection.pool.capacity", 64);
                this.pgPassword = getString(properties, env, "pg.password", "quest");
                this.pgUsername = getString(properties, env, "pg.user", "admin");
                this.pgMaxBlobSizeOnQuery = getIntSize(properties, env, "pg.max.blob.size.on.query", 512 * 1024);
                this.pgRecvBufferSize = getIntSize(properties, env, "pg.recv.buffer.size", 1024 * 1024);
                this.pgSendBufferSize = getIntSize(properties, env, "pg.send.buffer.size", 1024 * 1024);
                final String dateLocale = getString(properties, env, "pg.date.locale", "en");
                this.pgDefaultLocale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
                if (this.pgDefaultLocale == null) {
                    throw new ServerConfigurationException("pg.date.locale", dateLocale);
                }
                this.pgWorkerCount = getInt(properties, env, "pg.worker.count", 0);
                cpuUsed += this.pgWorkerCount;
                this.pgWorkerAffinity = getAffinity(properties, env, "pg.worker.affinity", pgWorkerCount);
                this.pgHaltOnError = getBoolean(properties, env, "pg.halt.on.error", false);
                this.pgWorkerYieldThreshold = getLong(properties, env, "pg.worker.yield.threshold", 10);
                this.pgWorkerSleepThreshold = getLong(properties, env, "pg.worker.sleep.threshold", 10000);
                this.pgDaemonPool = getBoolean(properties, env, "pg.daemon.pool", true);
                this.pgSelectCacheEnabled = getBoolean(properties, env, "pg.select.cache.enabled", true);
                this.pgSelectCacheBlockCount = getInt(properties, env, "pg.select.cache.block.count", 16);
                this.pgSelectCacheRowCount = getInt(properties, env, "pg.select.cache.row.count", 16);
                this.pgInsertCacheEnabled = getBoolean(properties, env, "pg.insert.cache.enabled", true);
                this.pgInsertCacheBlockCount = getInt(properties, env, "pg.insert.cache.block.count", 8);
                this.pgInsertCacheRowCount = getInt(properties, env, "pg.insert.cache.row.count", 8);
                this.pgInsertPoolCapacity = getInt(properties, env, "pg.insert.pool.capacity", 64);
                this.pgNamedStatementCacheCapacity = getInt(properties, env, "pg.named.statement.cache.capacity", 32);
                this.pgNamesStatementPoolCapacity = getInt(properties, env, "pg.named.statement.pool.capacity", 32);
                this.pgPendingWritersCacheCapacity = getInt(properties, env, "pg.pending.writers.cache.capacity", 16);
            }

            this.commitMode = getCommitMode(properties, env, "cairo.commit.mode");
            this.createAsSelectRetryCount = getInt(properties, env, "cairo.create.as.select.retry.count", 5);
            this.defaultMapType = getString(properties, env, "cairo.default.map.type", "fast");
            this.defaultSymbolCacheFlag = getBoolean(properties, env, "cairo.default.symbol.cache.flag", true);
            this.defaultSymbolCapacity = getInt(properties, env, "cairo.default.symbol.capacity", 256);
            this.fileOperationRetryCount = getInt(properties, env, "cairo.file.operation.retry.count", 30);
            this.idleCheckInterval = getLong(properties, env, "cairo.idle.check.interval", 5 * 60 * 1000L);
            this.inactiveReaderTTL = getLong(properties, env, "cairo.inactive.reader.ttl", 120_000);
            this.inactiveWriterTTL = getLong(properties, env, "cairo.inactive.writer.ttl", 600_000);
            this.indexValueBlockSize = Numbers.ceilPow2(getIntSize(properties, env, "cairo.index.value.block.size", 256));
            this.maxSwapFileCount = getInt(properties, env, "cairo.max.swap.file.count", 30);
            this.parallelIndexThreshold = getInt(properties, env, "cairo.parallel.index.threshold", 100000);
            this.readerPoolMaxSegments = getInt(properties, env, "cairo.reader.pool.max.segments", 5);
            this.spinLockTimeoutUs = getLong(properties, env, "cairo.spin.lock.timeout", 1_000_000);
            this.httpSqlCacheEnabled = getBoolean(properties, env, "http.query.cache.enabled", true);
            this.httpSqlCacheBlockCount = getInt(properties, env, "http.query.cache.block.count", 4);
            this.httpSqlCacheRowCount = getInt(properties, env, "http.query.cache.row.count", 16);
            this.sqlCharacterStoreCapacity = getInt(properties, env, "cairo.character.store.capacity", 1024);
            this.sqlCharacterStoreSequencePoolCapacity = getInt(properties, env, "cairo.character.store.sequence.pool.capacity", 64);
            this.sqlColumnPoolCapacity = getInt(properties, env, "cairo.column.pool.capacity", 4096);
            this.sqlCompactMapLoadFactor = getDouble(properties, env, "cairo.compact.map.load.factor", 0.7);
            this.sqlExpressionPoolCapacity = getInt(properties, env, "cairo.expression.pool.capacity", 8192);
            this.sqlFastMapLoadFactor = getDouble(properties, env, "cairo.fast.map.load.factor", 0.5);
            this.sqlJoinContextPoolCapacity = getInt(properties, env, "cairo.sql.join.context.pool.capacity", 64);
            this.sqlLexerPoolCapacity = getInt(properties, env, "cairo.lexer.pool.capacity", 2048);
            this.sqlMapKeyCapacity = getInt(properties, env, "cairo.sql.map.key.capacity", 2048 * 1024);
            this.sqlMapPageSize = getIntSize(properties, env, "cairo.sql.map.page.size", 4 * 1024 * 1024);
            this.sqlMapMaxPages = getIntSize(properties, env, "cairo.sql.map.max.pages", Integer.MAX_VALUE);
            this.sqlMapMaxResizes = getIntSize(properties, env, "cairo.sql.map.max.resizes", Integer.MAX_VALUE);
            this.sqlModelPoolCapacity = getInt(properties, env, "cairo.model.pool.capacity", 1024);
            this.sqlSortKeyPageSize = getLongSize(properties, env, "cairo.sql.sort.key.page.size", 4 * 1024 * 1024);
            this.sqlSortKeyMaxPages = getIntSize(properties, env, "cairo.sql.sort.key.max.pages", Integer.MAX_VALUE);
            this.sqlSortLightValuePageSize = getLongSize(properties, env, "cairo.sql.sort.light.value.page.size", 8 * 1048576);
            this.sqlSortLightValueMaxPages = getIntSize(properties, env, "cairo.sql.sort.light.value.max.pages", Integer.MAX_VALUE);
            this.sqlHashJoinValuePageSize = getIntSize(properties, env, "cairo.sql.hash.join.value.page.size", 16777216);
            this.sqlHashJoinValueMaxPages = getIntSize(properties, env, "cairo.sql.hash.join.value.max.pages", Integer.MAX_VALUE);
            this.sqlLatestByRowCount = getInt(properties, env, "cairo.sql.latest.by.row.count", 1000);
            this.sqlHashJoinLightValuePageSize = getIntSize(properties, env, "cairo.sql.hash.join.light.value.page.size", 1048576);
            this.sqlHashJoinLightValueMaxPages = getIntSize(properties, env, "cairo.sql.hash.join.light.value.max.pages", Integer.MAX_VALUE);
            this.sqlSortValuePageSize = getIntSize(properties, env, "cairo.sql.sort.value.page.size", 16777216);
            this.sqlSortValueMaxPages = getIntSize(properties, env, "cairo.sql.sort.value.max.pages", Integer.MAX_VALUE);
            this.workStealTimeoutNanos = getLong(properties, env, "cairo.work.steal.timeout.nanos", 10_000);
            this.parallelIndexingEnabled = getBoolean(properties, env, "cairo.parallel.indexing.enabled", true);
            this.sqlJoinMetadataPageSize = getIntSize(properties, env, "cairo.sql.join.metadata.page.size", 16384);
            this.sqlJoinMetadataMaxResizes = getIntSize(properties, env, "cairo.sql.join.metadata.max.resizes", Integer.MAX_VALUE);
            this.sqlAnalyticColumnPoolCapacity = getInt(properties, env, "cairo.sql.analytic.column.pool.capacity", 64);
            this.sqlCreateTableModelPoolCapacity = getInt(properties, env, "cairo.sql.create.table.model.pool.capacity", 16);
            this.sqlColumnCastModelPoolCapacity = getInt(properties, env, "cairo.sql.column.cast.model.pool.capacity", 16);
            this.sqlRenameTableModelPoolCapacity = getInt(properties, env, "cairo.sql.rename.table.model.pool.capacity", 16);
            this.sqlWithClauseModelPoolCapacity = getInt(properties, env, "cairo.sql.with.clause.model.pool.capacity", 128);
            this.sqlInsertModelPoolCapacity = getInt(properties, env, "cairo.sql.insert.model.pool.capacity", 64);
            this.sqlCopyModelPoolCapacity = getInt(properties, env, "cairo.sql.copy.model.pool.capacity", 32);
            this.sqlCopyBufferSize = getIntSize(properties, env, "cairo.sql.copy.buffer.size", 2 * 1024 * 1024);

            this.writerDataIndexKeyAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, "cairo.writer.data.index.key.append.page.size", 512 * 1024));
            this.writerDataIndexValueAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, "cairo.writer.data.index.value.append.page.size", 16 * 1024 * 1024));
            this.writerDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, "cairo.writer.data.append.page.size", 16 * 1024 * 1024));
            this.writerMiscAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, "cairo.writer.misc.append.page.size", Files.PAGE_SIZE));

            this.sampleByIndexSearchPageSize = getIntSize(properties, env, "cairo.sql.sampleby.page.size", 0);
            this.sqlDoubleToStrCastScale = getInt(properties, env, "cairo.sql.double.cast.scale", 12);
            this.sqlFloatToStrCastScale = getInt(properties, env, "cairo.sql.float.cast.scale", 4);
            this.sqlGroupByMapCapacity = getInt(properties, env, "cairo.sql.groupby.map.capacity", 1024);
            this.sqlGroupByPoolCapacity = getInt(properties, env, "cairo.sql.groupby.pool.capacity", 1024);
            this.sqlMaxSymbolNotEqualsCount = getInt(properties, env, "cairo.sql.max.symbol.not.equals.count", 100);
            this.sqlBindVariablePoolSize = getInt(properties, env, "cairo.sql.bind.variable.pool.size", 8);
            final String sqlCopyFormatsFile = getString(properties, env, "cairo.sql.copy.formats.file", "/text_loader.json");
            final String dateLocale = getString(properties, env, "cairo.date.locale", "en");
            this.locale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
            if (this.locale == null) {
                throw new ServerConfigurationException("cairo.date.locale", dateLocale);
            }
            this.sqlDistinctTimestampKeyCapacity = getInt(properties, env, "cairo.sql.distinct.timestamp.key.capacity", 512);
            this.sqlDistinctTimestampLoadFactor = getDouble(properties, env, "cairo.sql.distinct.timestamp.load.factor", 0.5);
            this.sqlPageFrameMaxSize = Numbers.ceilPow2(getIntSize(properties, env, "cairo.sql.page.frame.max.size", 8 * 1024 * 1024));

            this.sqlJitMode = getSqlJitMode(properties, env);
            this.sqlJitIRMemoryPageSize = getIntSize(properties, env, "cairo.sql.jit.ir.memory.page.size", 8 * 1024);
            this.sqlJitIRMemoryMaxPages = getInt(properties, env, "cairo.sql.jit.ir.memory.max.pages", 8);
            this.sqlJitBindVarsMemoryPageSize = getIntSize(properties, env, "cairo.sql.jit.bind.vars.memory.page.size", 4 * 1024);
            this.sqlJitBindVarsMemoryMaxPages = getInt(properties, env, "cairo.sql.jit.bind.vars.memory.max.pages", 8);
            this.sqlJitRowsThreshold = getIntSize(properties, env, "cairo.sql.jit.rows.threshold", 1024 * 1024);
            this.sqlJitPageAddressCacheThreshold = getIntSize(properties, env, "cairo.sql.jit.page.address.cache.threshold", 1024 * 1024);
            this.sqlJitDebugEnabled = getBoolean(properties, env, "cairo.sql.jit.debug.enabled", false);

            this.inputFormatConfiguration = new InputFormatConfiguration(
                    new DateFormatFactory(),
                    DateLocaleFactory.INSTANCE,
                    new TimestampFormatFactory(),
                    this.locale
            );

            try (JsonLexer lexer = new JsonLexer(1024, 1024)) {
                inputFormatConfiguration.parseConfiguration(lexer, sqlCopyFormatsFile);
            }

            this.inputRoot = getString(properties, env, "cairo.sql.copy.root", null);
            this.backupRoot = getString(properties, env, "cairo.sql.backup.root", null);
            this.backupDirTimestampFormat = getTimestampFormat(properties, env);
            this.backupTempDirName = getString(properties, env, "cairo.sql.backup.dir.tmp.name", "tmp");
            this.backupMkdirMode = getInt(properties, env, "cairo.sql.backup.mkdir.mode", 509);
            this.columnIndexerQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.column.indexer.queue.capacity", 64));
            this.vectorAggregateQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.vector.aggregate.queue.capacity", 128));
            this.o3CallbackQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.o3.callback.queue.capacity", 128));
            this.o3PartitionQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.o3.partition.queue.capacity", 128));
            this.o3OpenColumnQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.o3.open.column.queue.capacity", 128));
            this.o3CopyQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.o3.copy.queue.capacity", 128));
            this.o3UpdPartitionSizeQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.o3.upd.partition.size.queue.capacity", 128));
            this.o3PurgeDiscoveryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.o3.purge.discovery.queue.capacity", 128));
            this.o3ColumnMemorySize = (int) Files.ceilPageSize(getIntSize(properties, env, "cairo.o3.column.memory.size", 16 * Numbers.SIZE_1MB));
            this.maxUncommittedRows = getInt(properties, env, "cairo.max.uncommitted.rows", 500_000);
            this.commitLag = getLong(properties, env, "cairo.commit.lag", 300_000) * 1_000;
            this.o3QuickSortEnabled = getBoolean(properties, env, "cairo.o3.quicksort.enabled", false);
            this.sqlAnalyticStorePageSize = Numbers.ceilPow2(getIntSize(properties, env, "cairo.sql.analytic.store.page.size", 1024 * 1024));
            this.sqlAnalyticStoreMaxPages = Numbers.ceilPow2(getIntSize(properties, env, "cairo.sql.analytic.store.max.pages", Integer.MAX_VALUE));
            this.sqlAnalyticRowIdPageSize = Numbers.ceilPow2(getIntSize(properties, env, "cairo.sql.analytic.rowid.page.size", 512 * 1024));
            this.sqlAnalyticRowIdMaxPages = Numbers.ceilPow2(getInt(properties, env, "cairo.sql.analytic.rowid.max.pages", Integer.MAX_VALUE));
            this.sqlAnalyticTreeKeyPageSize = Numbers.ceilPow2(getIntSize(properties, env, "cairo.sql.analytic.tree.page.size", 512 * 1024));
            this.sqlAnalyticTreeKeyMaxPages = Numbers.ceilPow2(getInt(properties, env, "cairo.sql.analytic.tree.max.pages", Integer.MAX_VALUE));
            this.sqlTxnScoreboardEntryCount = Numbers.ceilPow2(getInt(properties, env, "cairo.o3.txn.scoreboard.entry.count", 16384));
            this.latestByQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.latestby.queue.capacity", 32));
            this.telemetryEnabled = getBoolean(properties, env, "telemetry.enabled", true);
            this.telemetryDisableCompletely = getBoolean(properties, env, "telemetry.disable.completely", false);
            this.telemetryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "telemetry.queue.capacity", 512));
            this.o3PartitionPurgeListCapacity = getInt(properties, env, "cairo.o3.partition.purge.list.initial.capacity", 1);

            parseBindTo(properties, env, "line.udp.bind.to", "0.0.0.0:9009", (a, p) -> {
                this.lineUdpBindIPV4Address = a;
                this.lineUdpPort = p;
            });

            this.lineUdpGroupIPv4Address = getIPv4Address(properties, env, "line.udp.join", "232.1.2.3");
            this.lineUdpCommitRate = getInt(properties, env, "line.udp.commit.rate", 1_000_000);
            this.lineUdpMsgBufferSize = getIntSize(properties, env, "line.udp.msg.buffer.size", 2048);
            this.lineUdpMsgCount = getInt(properties, env, "line.udp.msg.count", 10_000);
            this.lineUdpReceiveBufferSize = getIntSize(properties, env, "line.udp.receive.buffer.size", 8 * 1024 * 1024);
            this.lineUdpEnabled = getBoolean(properties, env, "line.udp.enabled", true);
            this.lineUdpOwnThreadAffinity = getInt(properties, env, "line.udp.own.thread.affinity", -1);
            this.lineUdpOwnThread = getBoolean(properties, env, "line.udp.own.thread", false);
            this.lineUdpUnicast = getBoolean(properties, env, "line.udp.unicast", false);
            this.lineUdpCommitMode = getCommitMode(properties, env, "line.udp.commit.mode");
            this.lineUdpTimestampAdapter = getLineTimestampAdaptor(properties, env, "line.udp.timestamp");
            String defaultUdpPartitionByProperty = getString(properties, env, "line.default.partition.by", "DAY");
            this.lineUdpDefaultPartitionBy = PartitionBy.fromString(defaultUdpPartitionByProperty);
            if (this.lineUdpDefaultPartitionBy == -1) {
                log.info().$("invalid partition by ").$(lineUdpDefaultPartitionBy).$("), will use DAY for UDP").$();
                this.lineUdpDefaultPartitionBy = PartitionBy.DAY;
            }

            this.lineTcpEnabled = getBoolean(properties, env, "line.tcp.enabled", true);
            if (lineTcpEnabled) {
                // obsolete
                lineTcpNetConnectionLimit = getInt(properties, env, "line.tcp.net.active.connection.limit", 256);
                lineTcpNetConnectionLimit = getInt(properties, env, "line.tcp.net.connection.limit", lineTcpNetConnectionLimit);
                lineTcpNetConnectionHint = getBoolean(properties, env, "line.tcp.net.connection.hint", false);
                parseBindTo(properties, env, "line.tcp.net.bind.to", "0.0.0.0:9009", (a, p) -> {
                    lineTcpNetBindIPv4Address = a;
                    lineTcpNetBindPort = p;
                });

                // obsolete
                this.lineTcpNetConnectionTimeout = getLong(properties, env, "line.tcp.net.idle.timeout", 0);
                this.lineTcpNetConnectionTimeout = getLong(properties, env, "line.tcp.net.connection.timeout", this.lineTcpNetConnectionTimeout);

                // obsolete
                this.lineTcpNetConnectionQueueTimeout = getLong(properties, env, "line.tcp.net.queued.timeout", 5_000);
                this.lineTcpNetConnectionQueueTimeout = getLong(properties, env, "line.tcp.net.connection.queue.timeout", this.lineTcpNetConnectionQueueTimeout);

                // obsolete
                this.lineTcpNetConnectionRcvBuf = getIntSize(properties, env, "line.tcp.net.recv.buf.size", -1);
                this.lineTcpNetConnectionRcvBuf = getIntSize(properties, env, "line.tcp.net.connection.rcvbuf", this.lineTcpNetConnectionRcvBuf);

                this.lineTcpConnectionPoolInitialCapacity = getInt(properties, env, "line.tcp.connection.pool.capacity", 64);
                this.lineTcpTimestampAdapter = getLineTimestampAdaptor(properties, env, "line.tcp.timestamp");
                this.lineTcpMsgBufferSize = getIntSize(properties, env, "line.tcp.msg.buffer.size", 32768);
                this.lineTcpMaxMeasurementSize = getIntSize(properties, env, "line.tcp.max.measurement.size", 32768);
                if (lineTcpMaxMeasurementSize > lineTcpMsgBufferSize) {
                    throw new IllegalArgumentException(
                            "line.tcp.max.measurement.size (" + this.lineTcpMaxMeasurementSize + ") cannot be more than line.tcp.msg.buffer.size (" + this.lineTcpMsgBufferSize + ")");
                }
                this.lineTcpWriterQueueCapacity = getQueueCapacity(properties, env, "line.tcp.writer.queue.capacity", 128);
                this.lineTcpWriterWorkerCount = getInt(properties, env, "line.tcp.writer.worker.count", 1);
                cpuUsed += this.lineTcpWriterWorkerCount;
                this.lineTcpWriterWorkerAffinity = getAffinity(properties, env, "line.tcp.writer.worker.affinity", lineTcpWriterWorkerCount);
                this.lineTcpWriterWorkerPoolHaltOnError = getBoolean(properties, env, "line.tcp.writer.halt.on.error", false);
                this.lineTcpWriterWorkerYieldThreshold = getLong(properties, env, "line.tcp.writer.worker.yield.threshold", 10);
                this.lineTcpWriterWorkerSleepThreshold = getLong(properties, env, "line.tcp.writer.worker.sleep.threshold", 10000);
                this.symbolCacheWaitUsBeforeReload = getLong(properties, env, "line.tcp.symbol.cache.wait.us.before.reload", 500_000);

                int ilpTcpWorkerCount;
                if (cpuAvailable < 9) {
                    ilpTcpWorkerCount = 0;
                } else if (cpuAvailable < 17) {
                    ilpTcpWorkerCount = 2;
                } else {
                    ilpTcpWorkerCount = 6;
                }
                this.lineTcpIOWorkerCount = getInt(properties, env, "line.tcp.io.worker.count", ilpTcpWorkerCount);
                cpuUsed += this.lineTcpIOWorkerCount;
                this.lineTcpIOWorkerAffinity = getAffinity(properties, env, "line.tcp.io.worker.affinity", lineTcpIOWorkerCount);
                this.lineTcpIOWorkerPoolHaltOnError = getBoolean(properties, env, "line.tcp.io.halt.on.error", false);
                this.lineTcpIOWorkerYieldThreshold = getLong(properties, env, "line.tcp.io.worker.yield.threshold", 10);
                this.lineTcpIOWorkerSleepThreshold = getLong(properties, env, "line.tcp.io.worker.sleep.threshold", 10000);
                this.lineTcpMaintenanceInterval = getInt(properties, env, "line.tcp.maintenance.job.interval", 30_000);
                this.lineTcpCommitTimeout = getInt(properties, env, "line.tcp.commit.timeout", 1000);
                this.lineTcpAuthDbPath = getString(properties, env, "line.tcp.auth.db.path", null);
                String defaultTcpPartitionByProperty = getString(properties, env, "line.default.partition.by", "line.tcp.default.partition.by", "DAY");
                this.lineTcpDefaultPartitionBy = PartitionBy.fromString(defaultTcpPartitionByProperty);
                if (this.lineTcpDefaultPartitionBy == -1) {
                    log.info().$("invalid partition by ").$(defaultTcpPartitionByProperty).$("), will use DAY for TCP").$();
                    this.lineTcpDefaultPartitionBy = PartitionBy.DAY;
                }
                if (null != lineTcpAuthDbPath) {
                    this.lineTcpAuthDbPath = new File(root, this.lineTcpAuthDbPath).getAbsolutePath();
                }
                this.minIdleMsBeforeWriterRelease = getLong(properties, env, "line.tcp.min.idle.ms.before.writer.release", 10_000);
            }

            this.sharedWorkerCount = getInt(properties, env, "shared.worker.count", Math.max(1, cpuAvailable / 2 - 1 - cpuUsed));
            this.sharedWorkerAffinity = getAffinity(properties, env, "shared.worker.affinity", sharedWorkerCount);
            this.sharedWorkerHaltOnError = getBoolean(properties, env, "shared.worker.haltOnError", false);
            this.sharedWorkerYieldThreshold = getLong(properties, env, "shared.worker.yield.threshold", 100);
            this.sharedWorkerSleepThreshold = getLong(properties, env, "shared.worker.sleep.threshold", 10_000);
            this.sharedWorkerSleepMs = getLong(properties, env, "shared.worker.sleep.ms", 100);

            this.metricsEnabled = getBoolean(properties, env, "metrics.enabled", false);
            this.writerAsyncCommandBusyWaitTimeout = getLong(properties, env, "cairo.writer.alter.busy.wait.timeout.micro", 500_000);
            this.writerAsyncCommandMaxWaitTimeout = getLong(properties, env, "cairo.writer.alter.max.wait.timeout.micro", 30_000_000L);
            this.writerTickRowsCountMod = Numbers.ceilPow2(getInt(properties, env, "cairo.writer.tick.rows.count", 1024)) - 1;
            this.writerAsyncCommandQueueCapacity = Numbers.ceilPow2(getInt(properties, env, "cairo.writer.command.queue.capacity", 32));

            this.buildInformation = buildInformation;
            this.binaryEncodingMaxLength = getInt(properties, env, "binarydata.encoding.maxlength", 32768);
        }
    }

    public static String confRoot(CharSequence dbRoot) {
        if (dbRoot != null) {
            int len = dbRoot.length();
            int end = len;
            boolean needsSlash = true;
            for (int i = len - 1; i > -1; --i) {
                if (dbRoot.charAt(i) == Files.SEPARATOR) {
                    if (i == len - 1) {
                        continue;
                    }
                    end = i + 1;
                    needsSlash = false;
                    break;
                }
            }
            StringSink sink = Misc.getThreadLocalBuilder();
            sink.put(dbRoot, 0, end);
            if (needsSlash) {
                sink.put(Files.SEPARATOR);
            }
            return sink.put(PropServerConfiguration.CONFIG_DIRECTORY).toString();
        }
        return null;
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
    public HttpMinServerConfiguration getHttpMinServerConfiguration() {
        return httpMinServerConfiguration;
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return lineUdpReceiverConfiguration;
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return lineTcpReceiverConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return workerPoolConfiguration;
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return pgWireConfiguration;
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return metricsConfiguration;
    }

    private int[] getAffinity(Properties properties, @Nullable Map<String, String> env, String key, int httpWorkerCount) throws ServerConfigurationException {
        final int[] result = new int[httpWorkerCount];
        String value = overrideWithEnv(properties, env, key);
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

    protected boolean getBoolean(Properties properties, @Nullable Map<String, String> env, String key, boolean defaultValue) {
        final String value = overrideWithEnv(properties, env, key);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private int getCommitMode(Properties properties, @Nullable Map<String, String> env, String key) {
        final String commitMode = overrideWithEnv(properties, env, key);

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

    private double getDouble(Properties properties, @Nullable Map<String, String> env, String key, double defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseDouble(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected int getIPv4Address(Properties properties, Map<String, String> env, String key, String defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, defaultValue);
        try {
            return Net.parseIPv4(value);
        } catch (NetworkError e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private int getInt(Properties properties, @Nullable Map<String, String> env, String key, int defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseInt(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    protected int getIntSize(Properties properties, @Nullable Map<String, String> env, String key, int defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseIntSize(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private LineProtoTimestampAdapter getLineTimestampAdaptor(Properties properties, Map<String, String> env, String propNm) {
        final String lineUdpTimestampSwitch = getString(properties, env, propNm, "n");
        switch (lineUdpTimestampSwitch) {
            case "u":
                return LineProtoMicroTimestampAdapter.INSTANCE;
            case "ms":
                return LineProtoMilliTimestampAdapter.INSTANCE;
            case "s":
                return LineProtoSecondTimestampAdapter.INSTANCE;
            case "m":
                return LineProtoMinuteTimestampAdapter.INSTANCE;
            case "h":
                return LineProtoHourTimestampAdapter.INSTANCE;
            default:
                return LineProtoNanoTimestampAdapter.INSTANCE;
        }
    }

    private long getLong(Properties properties, @Nullable Map<String, String> env, String key, long defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseLong(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private long getLongSize(Properties properties, @Nullable Map<String, String> env, String key, long defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseLongSize(value) : defaultValue;
        } catch (NumericException e) {
            throw new ServerConfigurationException(key, value);
        }
    }

    private int getQueueCapacity(Properties properties, @Nullable Map<String, String> env, String key, int defaultValue) throws ServerConfigurationException {
        final int value = getInt(properties, env, key, defaultValue);
        if (!Numbers.isPow2(value)) {
            throw new ServerConfigurationException(key, "Value must be power of 2, e.g. 1,2,4,8,16,32,64...");
        }
        return value;
    }

    private int getSqlJitMode(Properties properties, @Nullable Map<String, String> env) {
        final String key = "cairo.sql.jit.mode";
        final String jitMode = overrideWithEnv(properties, env, key);

        if (jitMode == null) {
            return SqlJitMode.JIT_MODE_DISABLED;
        }

        if (Chars.equalsLowerCaseAscii(jitMode, "on")) {
            return SqlJitMode.JIT_MODE_ENABLED;
        }

        if (Chars.equalsLowerCaseAscii(jitMode, "off")) {
            return SqlJitMode.JIT_MODE_DISABLED;
        }

        if (Chars.equalsLowerCaseAscii(jitMode, "scalar")) {
            return SqlJitMode.JIT_MODE_FORCE_SCALAR;
        }

        return SqlJitMode.JIT_MODE_DISABLED;
    }

    private String getString(Properties properties, @Nullable Map<String, String> env, String key, String fallbackKey, String defaultValue) {
        String value = overrideWithEnv(properties, env, key);
        if (value == null) {
            return getString(properties, env, fallbackKey, defaultValue);
        }
        return value;
    }

    private String getString(Properties properties, @Nullable Map<String, String> env, String key, String defaultValue) {
        String value = overrideWithEnv(properties, env, key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    private DateFormat getTimestampFormat(Properties properties, @Nullable Map<String, String> env) {
        final String pattern = overrideWithEnv(properties, env, "cairo.sql.backup.dir.datetime.format");
        TimestampFormatCompiler compiler = new TimestampFormatCompiler();
        //noinspection ReplaceNullCheck
        if (null != pattern) {
            return compiler.compile(pattern);
        }
        return compiler.compile("yyyy-MM-dd");
    }

    private String overrideWithEnv(Properties properties, @Nullable Map<String, String> env, String key) {
        String envCandidate = "QDB_" + key.replace('.', '_').toUpperCase();
        String envValue = env != null ? env.get(envCandidate) : null;
        if (envValue != null) {
            log.info().$("env config [key=").$(envCandidate).$(']').$();
            return envValue;
        }
        return properties.getProperty(key);
    }

    protected void parseBindTo(
            Properties properties,
            Map<String, String> env,
            String key,
            String defaultValue,
            BindToParser parser
    ) throws ServerConfigurationException {

        final String bindTo = getString(properties, env, key, defaultValue);
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
    protected interface BindToParser {
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

    private class PropHttpIODispatcherConfiguration implements IODispatcherConfiguration {
        @Override
        public int getLimit() {
            return httpNetConnectionLimit;
        }

        @Override
        public int getBindIPv4Address() {
            return httpNetBindIPv4Address;
        }

        @Override
        public int getBindPort() {
            return httpNetBindPort;
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
        public long getTimeout() {
            return httpNetConnectionTimeout;
        }

        @Override
        public int getInitialBias() {
            return IOOperation.READ;
        }

        @Override
        public boolean getHint() {
            return httpNetConnectionHint;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public int getRcvBufSize() {
            return httpNetConnectionRcvBuf;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSndBufSize() {
            return httpNetConnectionSndBuf;
        }

        @Override
        public long getQueueTimeout() {
            return httpNetConnectionQueueTimeout;
        }
    }

    private class PropHttpMinIODispatcherConfiguration implements IODispatcherConfiguration {
        @Override
        public int getLimit() {
            return httpMinNetConnectionLimit;
        }

        @Override
        public int getBindIPv4Address() {
            return httpMinBindIPv4Address;
        }

        @Override
        public int getBindPort() {
            return httpMinBindPort;
        }

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public String getDispatcherLogName() {
            return "http-min-server";
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
        }

        public long getTimeout() {
            return httpMinNetConnectionTimeout;
        }

        @Override
        public int getInitialBias() {
            return IOOperation.READ;
        }

        @Override
        public boolean getHint() {
            return httpMinNetConnectionHint;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public int getRcvBufSize() {
            return httpMinNetConnectionRcvBuf;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSndBufSize() {
            return httpMinNetConnectionSndBuf;
        }

        @Override
        public long getQueueTimeout() {
            return httpMinNetConnectionQueueTimeout;
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
            return locale;
        }
    }

    private class PropSqlExecutionCircuitBreakerConfiguration implements SqlExecutionCircuitBreakerConfiguration {
        @Override
        public int getBufferSize() {
            return circuitBreakerBufferSize;
        }

        @Override
        public int getCircuitBreakerThrottle() {
            return circuitBreakerThrottle;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public boolean isEnabled() {
            return interruptOnClosedConnection;
        }

        @Override
        public MicrosecondClock getClock() {
            return MicrosecondClockImpl.INSTANCE;
        }

        @Override
        public long getMaxTime() {
            return circuitBreakerMaxTime;
        }
    }

    private class PropHttpContextConfiguration implements HttpContextConfiguration {

        @Override
        public boolean allowDeflateBeforeSend() {
            return httpAllowDeflateBeforeSend;
        }

        @Override
        public MillisecondClock getClock() {
            return httpFrozenClock ? StationaryMillisClock.INSTANCE : MillisecondClockImpl.INSTANCE;
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
        public boolean getDumpNetworkTraffic() {
            return false;
        }

        @Override
        public String getHttpVersion() {
            return httpVersion;
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
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
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
        public int getSendBufferSize() {
            return sendBufferSize;
        }

        @Override
        public boolean getServerKeepAlive() {
            return httpServerKeepAlive;
        }

        @Override
        public boolean readOnlySecurityContext() {
            return readOnlySecurityContext;
        }
    }

    private class PropHttpServerConfiguration implements HttpServerConfiguration {

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return httpIODispatcherConfiguration;
        }

        @Override
        public HttpContextConfiguration getHttpContextConfiguration() {
            return httpContextConfiguration;
        }

        @Override
        public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
            return jsonQueryProcessorConfiguration;
        }

        @Override
        public boolean isQueryCacheEnabled() {
            return httpSqlCacheEnabled;
        }

        @Override
        public int getQueryCacheBlockCount() {
            return httpSqlCacheBlockCount;
        }

        @Override
        public int getQueryCacheRowCount() {
            return httpSqlCacheRowCount;
        }

        @Override
        public WaitProcessorConfiguration getWaitProcessorConfiguration() {
            return httpWaitProcessorConfiguration;
        }

        @Override
        public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
            return staticContentProcessorConfiguration;
        }

        @Override
        public boolean isEnabled() {
            return httpServerEnabled;
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
        public long getYieldThreshold() {
            return httpWorkerYieldThreshold;
        }

        @Override
        public long getSleepThreshold() {
            return httpWorkerSleepThreshold;
        }

        @Override
        public long getSleepMs() {
            return httpWorkerSleepMs;
        }
    }

    private class PropCairoConfiguration implements CairoConfiguration {

        @Override
        public boolean enableTestFactories() {
            return false;
        }

        @Override
        public int getAnalyticColumnPoolCapacity() {
            return sqlAnalyticColumnPoolCapacity;
        }

        @Override
        public long getDataAppendPageSize() {
            return writerDataAppendPageSize;
        }

        @Override
        public DateFormat getBackupDirTimestampFormat() {
            return backupDirTimestampFormat;
        }

        @Override
        public int getBackupMkDirMode() {
            return backupMkdirMode;
        }

        @Override
        public CharSequence getBackupRoot() {
            return backupRoot;
        }

        @Override
        public CharSequence getBackupTempDirName() {
            return backupTempDirName;
        }

        @Override
        public int getBinaryEncodingMaxLength() {
            return binaryEncodingMaxLength;
        }

        @Override
        public int getBindVariablePoolSize() {
            return sqlBindVariablePoolSize;
        }

        @Override
        public BuildInformation getBuildInformation() {
            return buildInformation;
        }

        @Override
        public int getColumnCastModelPoolCapacity() {
            return sqlColumnCastModelPoolCapacity;
        }

        @Override
        public int getColumnIndexerQueueCapacity() {
            return columnIndexerQueueCapacity;
        }

        @Override
        public long getCommitLag() {
            return commitLag;
        }

        @Override
        public int getCommitMode() {
            return commitMode;
        }

        @Override
        public CharSequence getConfRoot() {
            return confRoot;
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
        public int getCreateTableModelPoolCapacity() {
            return sqlCreateTableModelPoolCapacity;
        }

        @Override
        public long getDataIndexKeyAppendPageSize() {
            return writerDataIndexKeyAppendPageSize;
        }

        @Override
        public long getDataIndexValueAppendPageSize() {
            return writerDataIndexValueAppendPageSize;
        }

        @Override
        public long getDatabaseIdHi() {
            return instanceHashHi;
        }

        @Override
        public long getDatabaseIdLo() {
            return instanceHashLo;
        }

        @Override
        public CharSequence getDbDirectory() {
            return dbDirectory;
        }

        @Override
        public DateLocale getDefaultDateLocale() {
            return locale;
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
        public int getDoubleToStrCastScale() {
            return sqlDoubleToStrCastScale;
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
        public int getFloatToStrCastScale() {
            return sqlFloatToStrCastScale;
        }

        @Override
        public int getGroupByMapCapacity() {
            return sqlGroupByMapCapacity;
        }

        @Override
        public int getGroupByPoolCapacity() {
            return sqlGroupByPoolCapacity;
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
        public CharSequence getInputRoot() {
            return inputRoot;
        }

        @Override
        public int getInsertPoolCapacity() {
            return sqlInsertModelPoolCapacity;
        }

        @Override
        public int getLatestByQueueCapacity() {
            return latestByQueueCapacity;
        }

        @Override
        public int getMaxSwapFileCount() {
            return maxSwapFileCount;
        }

        @Override
        public int getMaxSymbolNotEqualsCount() {
            return sqlMaxSymbolNotEqualsCount;
        }

        @Override
        public int getMaxUncommittedRows() {
            return maxUncommittedRows;
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
        public int getO3CallbackQueueCapacity() {
            return o3CallbackQueueCapacity;
        }

        @Override
        public int getO3ColumnMemorySize() {
            return o3ColumnMemorySize;
        }

        @Override
        public int getO3CopyQueueCapacity() {
            return o3CopyQueueCapacity;
        }

        @Override
        public int getO3OpenColumnQueueCapacity() {
            return o3OpenColumnQueueCapacity;
        }

        @Override
        public int getO3PartitionQueueCapacity() {
            return o3PartitionQueueCapacity;
        }

        @Override
        public int getO3PartitionUpdateQueueCapacity() {
            return o3UpdPartitionSizeQueueCapacity;
        }

        @Override
        public int getO3PurgeDiscoveryQueueCapacity() {
            return o3PurgeDiscoveryQueueCapacity;
        }

        @Override
        public int getParallelIndexThreshold() {
            return parallelIndexThreshold;
        }

        @Override
        public int getPartitionPurgeListCapacity() {
            return o3PartitionPurgeListCapacity;
        }

        @Override
        public int getReaderPoolMaxSegments() {
            return readerPoolMaxSegments;
        }

        @Override
        public int getRenameTableModelPoolCapacity() {
            return sqlRenameTableModelPoolCapacity;
        }

        @Override
        public CharSequence getRoot() {
            return root;
        }

        @Override
        public int getSampleByIndexSearchPageSize() {
            return sampleByIndexSearchPageSize;
        }

        @Override
        public long getMiscAppendPageSize() {
            return writerMiscAppendPageSize;
        }

        @Override
        public long getSpinLockTimeoutUs() {
            return spinLockTimeoutUs;
        }

        @Override
        public int getSqlAnalyticRowIdMaxPages() {
            return sqlAnalyticRowIdMaxPages;
        }

        @Override
        public int getSqlAnalyticRowIdPageSize() {
            return sqlAnalyticRowIdPageSize;
        }

        @Override
        public int getSqlAnalyticStoreMaxPages() {
            return sqlAnalyticStoreMaxPages;
        }

        @Override
        public int getSqlAnalyticStorePageSize() {
            return sqlAnalyticStorePageSize;
        }

        @Override
        public int getSqlAnalyticTreeKeyMaxPages() {
            return sqlAnalyticTreeKeyMaxPages;
        }

        @Override
        public int getSqlAnalyticTreeKeyPageSize() {
            return sqlAnalyticTreeKeyPageSize;
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
        public int getSqlCopyBufferSize() {
            return sqlCopyBufferSize;
        }

        @Override
        public int getSqlDistinctTimestampKeyCapacity() {
            return sqlDistinctTimestampKeyCapacity;
        }

        @Override
        public double getSqlDistinctTimestampLoadFactor() {
            return sqlDistinctTimestampLoadFactor;
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
        public int getSqlHashJoinLightValueMaxPages() {
            return sqlHashJoinLightValueMaxPages;
        }

        @Override
        public int getSqlHashJoinLightValuePageSize() {
            return sqlHashJoinLightValuePageSize;
        }

        @Override
        public int getSqlHashJoinValueMaxPages() {
            return sqlHashJoinValueMaxPages;
        }

        @Override
        public int getSqlHashJoinValuePageSize() {
            return sqlHashJoinValuePageSize;
        }

        @Override
        public int getSqlJoinContextPoolCapacity() {
            return sqlJoinContextPoolCapacity;
        }

        @Override
        public int getSqlJoinMetadataMaxResizes() {
            return sqlJoinMetadataMaxResizes;
        }

        @Override
        public int getSqlJoinMetadataPageSize() {
            return sqlJoinMetadataPageSize;
        }

        @Override
        public long getSqlLatestByRowCount() {
            return sqlLatestByRowCount;
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
        public int getSqlMapMaxPages() {
            return sqlMapMaxPages;
        }

        @Override
        public int getSqlMapMaxResizes() {
            return sqlMapMaxResizes;
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
        public int getSqlSortKeyMaxPages() {
            return sqlSortKeyMaxPages;
        }

        @Override
        public long getSqlSortKeyPageSize() {
            return sqlSortKeyPageSize;
        }

        @Override
        public int getSqlSortLightValueMaxPages() {
            return sqlSortLightValueMaxPages;
        }

        @Override
        public long getSqlSortLightValuePageSize() {
            return sqlSortLightValuePageSize;
        }

        @Override
        public int getSqlSortValueMaxPages() {
            return sqlSortValueMaxPages;
        }

        @Override
        public int getSqlSortValuePageSize() {
            return sqlSortValuePageSize;
        }

        @Override
        public int getSqlPageFrameMaxSize() {
            return sqlPageFrameMaxSize;
        }

        @Override
        public int getSqlJitMode() {
            return sqlJitMode;
        }

        @Override
        public int getSqlJitIRMemoryPageSize() {
            return sqlJitIRMemoryPageSize;
        }

        @Override
        public int getSqlJitIRMemoryMaxPages() {
            return sqlJitIRMemoryMaxPages;
        }

        @Override
        public int getSqlJitBindVarsMemoryPageSize() {
            return sqlJitBindVarsMemoryPageSize;
        }

        @Override
        public int getSqlJitBindVarsMemoryMaxPages() {
            return sqlJitBindVarsMemoryMaxPages;
        }

        @Override
        public int getSqlJitRowsThreshold() {
            return sqlJitRowsThreshold;
        }

        @Override
        public int getSqlJitPageAddressCacheThreshold() {
            return sqlJitPageAddressCacheThreshold;
        }

        @Override
        public boolean isSqlJitDebugEnabled() {
            return sqlJitDebugEnabled;
        }

        public TelemetryConfiguration getTelemetryConfiguration() {
            return telemetryConfiguration;
        }

        @Override
        public TextConfiguration getTextConfiguration() {
            return textConfiguration;
        }

        @Override
        public int getTxnScoreboardEntryCount() {
            return sqlTxnScoreboardEntryCount;
        }

        @Override
        public int getVectorAggregateQueueCapacity() {
            return vectorAggregateQueueCapacity;
        }

        @Override
        public int getWithClauseModelPoolCapacity() {
            return sqlWithClauseModelPoolCapacity;
        }

        @Override
        public long getWorkStealTimeoutNanos() {
            return workStealTimeoutNanos;
        }

        @Override
        public long getWriterAsyncCommandBusyWaitTimeout() {
            return writerAsyncCommandBusyWaitTimeout;
        }

        @Override
        public long getWriterAsyncCommandMaxTimeout() {
            return writerAsyncCommandMaxWaitTimeout;
        }

        @Override
        public int getWriterCommandQueueCapacity() {
            return writerAsyncCommandQueueCapacity;
        }

        @Override
        public int getWriterTickRowsCountMod() {
            return writerTickRowsCountMod;
        }

        @Override
        public boolean isO3QuickSortEnabled() {
            return o3QuickSortEnabled;
        }

        @Override
        public boolean isParallelIndexingEnabled() {
            return parallelIndexingEnabled;
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

        @Override
        public int getDefaultPartitionBy() {
            return lineUdpDefaultPartitionBy;
        }
    }

    private class PropLineTcpReceiverIODispatcherConfiguration implements IODispatcherConfiguration {

        @Override
        public int getLimit() {
            return lineTcpNetConnectionLimit;
        }

        @Override
        public int getBindIPv4Address() {
            return lineTcpNetBindIPv4Address;
        }

        @Override
        public int getBindPort() {
            return lineTcpNetBindPort;
        }

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public String getDispatcherLogName() {
            return "tcp-line-server";
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
        }

        @Override
        public long getTimeout() {
            return lineTcpNetConnectionTimeout;
        }

        @Override
        public int getInitialBias() {
            return BIAS_READ;
        }

        @Override
        public boolean getHint() {
            return lineTcpNetConnectionHint;
        }

        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public int getRcvBufSize() {
            return lineTcpNetConnectionRcvBuf;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSndBufSize() {
            return -1;
        }

        @Override
        public long getQueueTimeout() {
            return lineTcpNetConnectionQueueTimeout;
        }
    }

    private class PropLineTcpWriterWorkerPoolConfiguration implements WorkerPoolAwareConfiguration {
        @Override
        public int[] getWorkerAffinity() {
            return lineTcpWriterWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return lineTcpWriterWorkerCount;
        }

        @Override
        public boolean haltOnError() {
            return lineTcpWriterWorkerPoolHaltOnError;
        }

        @Override
        public String getPoolName() {
            return "ilpwriter";
        }

        @Override
        public long getYieldThreshold() {
            return lineTcpWriterWorkerYieldThreshold;
        }

        @Override
        public long getSleepThreshold() {
            return lineTcpWriterWorkerSleepThreshold;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }
    }

    private class PropLineTcpIOWorkerPoolConfiguration implements WorkerPoolAwareConfiguration {
        @Override
        public int[] getWorkerAffinity() {
            return lineTcpIOWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return lineTcpIOWorkerCount;
        }

        @Override
        public boolean haltOnError() {
            return lineTcpIOWorkerPoolHaltOnError;
        }

        @Override
        public String getPoolName() {
            return "ilpio";
        }

        @Override
        public long getYieldThreshold() {
            return lineTcpIOWorkerYieldThreshold;
        }

        @Override
        public long getSleepThreshold() {
            return lineTcpIOWorkerSleepThreshold;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }
    }

    private class PropLineTcpReceiverConfiguration implements LineTcpReceiverConfiguration {

        @Override
        public String getAuthDbPath() {
            return lineTcpAuthDbPath;
        }

        @Override
        public CairoSecurityContext getCairoSecurityContext() {
            return AllowAllCairoSecurityContext.INSTANCE;
        }

        @Override
        public int getConnectionPoolInitialCapacity() {
            return lineTcpConnectionPoolInitialCapacity;
        }

        @Override
        public int getDefaultPartitionBy() {
            return lineTcpDefaultPartitionBy;
        }

        @Override
        public WorkerPoolAwareConfiguration getIOWorkerPoolConfiguration() {
            return lineTcpIOWorkerPoolConfiguration;
        }

        @Override
        public long getMaintenanceInterval() {
            return lineTcpMaintenanceInterval;
        }

        @Override
        public long getCommitTimeout() {
            return lineTcpCommitTimeout;
        }

        @Override
        public int getMaxMeasurementSize() {
            return lineTcpMaxMeasurementSize;
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
        public long getWriterIdleTimeout() {
            return minIdleMsBeforeWriterRelease;
        }

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return lineTcpReceiverDispatcherConfiguration;
        }

        @Override
        public int getNetMsgBufferSize() {
            return lineTcpMsgBufferSize;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public LineProtoTimestampAdapter getTimestampAdapter() {
            return lineTcpTimestampAdapter;
        }

        @Override
        public int getWriterQueueCapacity() {
            return lineTcpWriterQueueCapacity;
        }

        @Override
        public WorkerPoolAwareConfiguration getWriterWorkerPoolConfiguration() {
            return lineTcpWriterWorkerPoolConfiguration;
        }

        @Override
        public boolean isEnabled() {
            return lineTcpEnabled;
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return symbolCacheWaitUsBeforeReload;
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

        @Override
        public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
            return circuitBreakerConfiguration;
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

        @Override
        public long getYieldThreshold() {
            return sharedWorkerYieldThreshold;
        }

        @Override
        public long getSleepThreshold() {
            return sharedWorkerSleepThreshold;
        }

        @Override
        public long getSleepMs() {
            return sharedWorkerSleepMs;
        }
    }

    private class PropWaitProcessorConfiguration implements WaitProcessorConfiguration {

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public long getMaxWaitCapMs() {
            return maxRerunWaitCapMs;
        }

        @Override
        public double getExponentialWaitMultiplier() {
            return rerunExponentialWaitMultiplier;
        }

        @Override
        public int getInitialWaitQueueSize() {
            return rerunInitialWaitQueueSize;
        }

        @Override
        public int getMaxProcessingQueueSize() {
            return rerunMaxProcessingQueueSize;
        }
    }

    private class PropPGWireDispatcherConfiguration implements IODispatcherConfiguration {

        @Override
        public int getLimit() {
            return pgNetConnectionLimit;
        }

        @Override
        public int getBindIPv4Address() {
            return pgNetBindIPv4Address;
        }

        @Override
        public int getBindPort() {
            return pgNetBindPort;
        }

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public String getDispatcherLogName() {
            return "pg-server";
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
        }

        public long getTimeout() {
            return pgNetIdleConnectionTimeout;
        }

        @Override
        public int getInitialBias() {
            return BIAS_READ;
        }

        @Override
        public boolean getHint() {
            return pgNetConnectionHint;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public int getRcvBufSize() {
            return pgNetConnectionRcvBuf;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSndBufSize() {
            return pgNetConnectionSndBuf;
        }

        @Override
        public long getQueueTimeout() {
            return pgNetConnectionQueueTimeout;
        }
    }

    private class PropPGWireConfiguration implements PGWireConfiguration {
        @Override
        public int getBinParamCountCapacity() {
            return pgBinaryParamsCapacity;
        }

        @Override
        public int getCharacterStoreCapacity() {
            return pgCharacterStoreCapacity;
        }

        @Override
        public int getCharacterStorePoolCapacity() {
            return pgCharacterStorePoolCapacity;
        }

        @Override
        public int getConnectionPoolInitialCapacity() {
            return pgConnectionPoolInitialCapacity;
        }

        @Override
        public String getDefaultPassword() {
            return pgPassword;
        }

        @Override
        public String getDefaultUsername() {
            return pgUsername;
        }

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return propPGWireDispatcherConfiguration;
        }

        @Override
        public boolean isSelectCacheEnabled() {
            return pgSelectCacheEnabled;
        }

        @Override
        public int getSelectCacheBlockCount() {
            return pgSelectCacheBlockCount;
        }

        @Override
        public int getSelectCacheRowCount() {
            return pgSelectCacheRowCount;
        }

        @Override
        public boolean isInsertCacheEnabled() {
            return pgInsertCacheEnabled;
        }

        @Override
        public int getInsertCacheBlockCount() {
            return pgInsertCacheBlockCount;
        }

        @Override
        public int getInsertCacheRowCount() {
            return pgInsertCacheRowCount;
        }

        @Override
        public int getInsertPoolCapacity() {
            return pgInsertPoolCapacity;
        }

        @Override
        public int getMaxBlobSizeOnQuery() {
            return pgMaxBlobSizeOnQuery;
        }

        @Override
        public int getNamedStatementCacheCapacity() {
            return pgNamedStatementCacheCapacity;
        }

        @Override
        public int getNamesStatementPoolCapacity() {
            return pgNamesStatementPoolCapacity;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public int getPendingWritersCacheSize() {
            return pgPendingWritersCacheCapacity;
        }

        @Override
        public int getRecvBufferSize() {
            return pgRecvBufferSize;
        }

        @Override
        public int getSendBufferSize() {
            return pgSendBufferSize;
        }

        @Override
        public String getServerVersion() {
            return "11.3";
        }

        @Override
        public DateLocale getDefaultDateLocale() {
            return pgDefaultLocale;
        }

        @Override
        public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
            return circuitBreakerConfiguration;
        }

        @Override
        public int[] getWorkerAffinity() {
            return pgWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return pgWorkerCount;
        }

        @Override
        public boolean haltOnError() {
            return pgHaltOnError;
        }

        @Override
        public boolean isDaemonPool() {
            return pgDaemonPool;
        }

        @Override
        public long getYieldThreshold() {
            return pgWorkerYieldThreshold;
        }

        @Override
        public long getSleepThreshold() {
            return pgWorkerSleepThreshold;
        }

        @Override
        public boolean isEnabled() {
            return pgEnabled;
        }
    }

    private class PropTelemetryConfiguration implements TelemetryConfiguration {

        @Override
        public boolean getDisableCompletely() {
            return telemetryDisableCompletely;
        }

        @Override
        public boolean getEnabled() {
            return telemetryEnabled;
        }

        @Override
        public int getQueueCapacity() {
            return telemetryQueueCapacity;
        }
    }

    private class PropHttpMinServerConfiguration implements HttpMinServerConfiguration {

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return httpMinIODispatcherConfiguration;
        }

        @Override
        public HttpContextConfiguration getHttpContextConfiguration() {
            return httpContextConfiguration;
        }

        @Override
        public WaitProcessorConfiguration getWaitProcessorConfiguration() {
            return httpWaitProcessorConfiguration;
        }

        @Override
        public int[] getWorkerAffinity() {
            return httpMinWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return httpMinWorkerCount;
        }

        @Override
        public boolean haltOnError() {
            return httpMinWorkerHaltOnError;
        }

        @Override
        public long getYieldThreshold() {
            return httpMinWorkerYieldThreshold;
        }

        @Override
        public long getSleepThreshold() {
            return httpMinWorkerSleepThreshold;
        }

        @Override
        public boolean isEnabled() {
            return httpMinServerEnabled;
        }
    }

    private class PropMetricsConfiguration implements MetricsConfiguration {

        @Override
        public boolean isEnabled() {
            return metricsEnabled;
        }
    }
}
