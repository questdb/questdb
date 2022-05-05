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
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
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
import java.util.*;

public class PropServerConfiguration implements ServerConfiguration {
    public static final String CONFIG_DIRECTORY = "conf";
    public static final String DB_DIRECTORY = "db";
    public static final String SNAPSHOT_DIRECTORY = "snapshot";
    public static final long COMMIT_INTERVAL_DEFAULT = 2000;
    private static final LowerCaseCharSequenceIntHashMap WRITE_FO_OPTS = new LowerCaseCharSequenceIntHashMap();
    private static final Map<String, String> OBSOLETE_SETTINGS = new HashMap<>();
    private static final Map<PropertyKey, String> DEPRECATED_SETTINGS = new HashMap<>();
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
    private final int rndFunctionMemoryPageSize;
    private final int rndFunctionMemoryMaxPages;
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
    private final int sqlMaxNegativeLimit;
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
    private final int sqlPageFrameMinRows;
    private final int sqlPageFrameMaxRows;
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
    private final boolean telemetryHideTables;
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
    private final String snapshotRoot;
    private final String snapshotInstanceId;
    private final boolean snapshotRecoveryEnabled;
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
    private final long writerAsyncCommandQueueSlotSize;
    private final int writerTickRowsCountMod;
    private final long writerAsyncCommandMaxWaitTimeout;
    private final int o3PartitionPurgeListCapacity;
    private final int cairoPageFrameReduceQueueCapacity;
    private final int cairoPageFrameReduceRowIdListCapacity;
    private final int cairoPageFrameReduceColumnListCapacity;
    private final int cairoPageFrameReduceTaskPoolCapacity;
    private final long writerFileOpenOpts;
    private final boolean sqlParallelFilterEnabled;
    private final int cairoPageFrameReduceShardCount;
    private int lineUdpDefaultPartitionBy;
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
    private final int queryCacheEventQueueCapacity;
    private int jsonQueryFloatScale;
    private int jsonQueryDoubleScale;
    private int jsonQueryConnectionCheckFrequency;
    private boolean httpFrozenClock;
    private boolean httpReadOnlySecurityContext;
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
    private boolean pgReadOnlySecurityContext;
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
    private double lineTcpCommitIntervalFraction;
    private long lineTcpCommitIntervalDefault;
    private String lineTcpAuthDbPath;
    private int lineTcpDefaultPartitionBy;
    private long minIdleMsBeforeWriterRelease;
    private boolean lineTcpDisconnectOnError;
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
    private boolean stringToCharCastAllowed;
    private boolean symbolAsFieldSupported;
    private boolean isStringAsTagSupported;
    private short floatDefaultColumnType;
    private short integerDefaultColumnType;

    public PropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {

        this.log = log;

        boolean configValidationStrict = getBoolean(properties, env, PropertyKey.CONFIG_VALIDATION_STRICT, false);
        validateProperties(properties, configValidationStrict);

        this.mkdirMode = getInt(properties, env, PropertyKey.CAIRO_MKDIR_MODE, 509);

        this.dbDirectory = getString(properties, env, PropertyKey.CAIRO_ROOT, DB_DIRECTORY);
        if (new File(this.dbDirectory).isAbsolute()) {
            this.root = this.dbDirectory;
            this.confRoot = rootSubdir(this.root, PropServerConfiguration.CONFIG_DIRECTORY); // ../conf
            this.snapshotRoot = rootSubdir(this.root, PropServerConfiguration.SNAPSHOT_DIRECTORY); // ../snapshot
        } else {
            this.root = new File(root, this.dbDirectory).getAbsolutePath();
            this.confRoot = new File(root, CONFIG_DIRECTORY).getAbsolutePath();
            this.snapshotRoot = new File(root, SNAPSHOT_DIRECTORY).getAbsolutePath();
        }

        this.snapshotInstanceId = getString(properties, env, PropertyKey.CAIRO_SNAPSHOT_INSTANCE_ID, "");
        this.snapshotRecoveryEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SNAPSHOT_RECOVERY_ENABLED, true);

        int cpuAvailable = Runtime.getRuntime().availableProcessors();
        int cpuUsed = 0;
        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        try (Path path = new Path()) {
            ff.mkdirs(path.of(this.root).slash$(), this.mkdirMode);
            path.of(this.root).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
            final long tableIndexFd = TableUtils.openFileRWOrFail(ff, path, CairoConfiguration.O_NONE);
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

            this.httpMinServerEnabled = getBoolean(properties, env, PropertyKey.HTTP_MIN_ENABLED, true);
            if (httpMinServerEnabled) {
                this.httpMinWorkerHaltOnError = getBoolean(properties, env, PropertyKey.HTTP_MIN_WORKER_HALT_ON_ERROR, false);
                this.httpMinWorkerCount = getInt(properties, env, PropertyKey.HTTP_MIN_WORKER_COUNT, cpuAvailable > 16 ? 1 : 0);
                cpuUsed += this.httpMinWorkerCount;
                this.httpMinWorkerAffinity = getAffinity(properties, env, PropertyKey.HTTP_MIN_WORKER_AFFINITY, httpMinWorkerCount);
                this.httpMinWorkerYieldThreshold = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_YIELD_THRESHOLD, 10);
                this.httpMinWorkerSleepThreshold = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_SLEEP_THRESHOLD, 10000);

                // deprecated
                String httpMinBindTo = getString(properties, env, PropertyKey.HTTP_MIN_BIND_TO, "0.0.0.0:9003");

                parseBindTo(properties, env, PropertyKey.HTTP_MIN_NET_BIND_TO, httpMinBindTo, (a, p) -> {
                    httpMinBindIPv4Address = a;
                    httpMinBindPort = p;
                });

                this.httpMinNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_LIMIT, 4);

                // deprecated
                this.httpMinNetConnectionTimeout = getLong(properties, env, PropertyKey.HTTP_MIN_NET_IDLE_CONNECTION_TIMEOUT, 5 * 60 * 1000L);
                this.httpMinNetConnectionTimeout = getLong(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_TIMEOUT, this.httpMinNetConnectionTimeout);

                // deprecated
                this.httpMinNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.HTTP_MIN_NET_QUEUED_CONNECTION_TIMEOUT, 5 * 1000L);
                this.httpMinNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_QUEUE_TIMEOUT, this.httpMinNetConnectionQueueTimeout);

                // deprecated
                this.httpMinNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_MIN_NET_SND_BUF_SIZE, 1024);
                this.httpMinNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_SNDBUF, this.httpMinNetConnectionSndBuf);

                // deprecated
                this.httpMinNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_RCV_BUF_SIZE, 1024);
                this.httpMinNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_RCVBUF, this.httpMinNetConnectionRcvBuf);
                this.httpMinNetConnectionHint = getBoolean(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_HINT, false);
            }

            this.httpServerEnabled = getBoolean(properties, env, PropertyKey.HTTP_ENABLED, true);
            if (httpServerEnabled) {
                this.connectionPoolInitialCapacity = getInt(properties, env, PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY, 16);
                this.connectionStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_CONNECTION_STRING_POOL_CAPACITY, 128);
                this.multipartHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MULTIPART_HEADER_BUFFER_SIZE, 512);
                this.multipartIdleSpinCount = getLong(properties, env, PropertyKey.HTTP_MULTIPART_IDLE_SPIN_COUNT, 10_000);
                this.recvBufferSize = getIntSize(properties, env, PropertyKey.HTTP_RECEIVE_BUFFER_SIZE, 1024 * 1024);
                this.requestHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_REQUEST_HEADER_BUFFER_SIZE, 32 * 2014);
                this.httpWorkerCount = getInt(properties, env, PropertyKey.HTTP_WORKER_COUNT, 0);
                cpuUsed += this.httpWorkerCount;
                this.httpWorkerAffinity = getAffinity(properties, env, PropertyKey.HTTP_WORKER_AFFINITY, httpWorkerCount);
                this.httpWorkerHaltOnError = getBoolean(properties, env, PropertyKey.HTTP_WORKER_HALT_ON_ERROR, false);
                this.httpWorkerYieldThreshold = getLong(properties, env, PropertyKey.HTTP_WORKER_YIELD_THRESHOLD, 10);
                this.httpWorkerSleepThreshold = getLong(properties, env, PropertyKey.HTTP_WORKER_SLEEP_THRESHOLD, 10000);
                this.httpWorkerSleepMs = getLong(properties, env, PropertyKey.HTTP_WORKER_SLEEP_MS, 100);
                this.sendBufferSize = getIntSize(properties, env, PropertyKey.HTTP_SEND_BUFFER_SIZE, 2 * 1024 * 1024);
                this.indexFileName = getString(properties, env, PropertyKey.HTTP_STATIC_INDEX_FILE_NAME, "index.html");
                this.httpFrozenClock = getBoolean(properties, env, PropertyKey.HTTP_FROZEN_CLOCK, false);
                this.httpAllowDeflateBeforeSend = getBoolean(properties, env, PropertyKey.HTTP_ALLOW_DEFLATE_BEFORE_SEND, false);
                this.httpServerKeepAlive = getBoolean(properties, env, PropertyKey.HTTP_SERVER_KEEP_ALIVE, true);
                this.httpVersion = getString(properties, env, PropertyKey.HTTP_VERSION, "HTTP/1.1");
                if (!httpVersion.endsWith(" ")) {
                    httpVersion += ' ';
                }

                int keepAliveTimeout = getInt(properties, env, PropertyKey.HTTP_KEEP_ALIVE_TIMEOUT, 5);
                int keepAliveMax = getInt(properties, env, PropertyKey.HTTP_KEEP_ALIVE_MAX, 10_000);

                if (keepAliveTimeout > 0 && keepAliveMax > 0) {
                    this.keepAliveHeader = "Keep-Alive: timeout=" + keepAliveTimeout + ", max=" + keepAliveMax + Misc.EOL;
                } else {
                    this.keepAliveHeader = null;
                }

                final String publicDirectory = getString(properties, env, PropertyKey.HTTP_STATIC_PUBLIC_DIRECTORY, "public");
                // translate public directory into absolute path
                // this will generate some garbage, but this is ok - we're just doing this once on startup
                if (new File(publicDirectory).isAbsolute()) {
                    this.publicDirectory = publicDirectory;
                } else {
                    this.publicDirectory = new File(root, publicDirectory).getAbsolutePath();
                }

                // maintain deprecated property name for the time being
                this.httpNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_NET_ACTIVE_CONNECTION_LIMIT, 256);
                this.httpNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_NET_CONNECTION_LIMIT, this.httpNetConnectionLimit);
                this.httpNetConnectionHint = getBoolean(properties, env, PropertyKey.HTTP_NET_CONNECTION_HINT, false);
                // deprecated
                this.httpNetConnectionTimeout = getLong(properties, env, PropertyKey.HTTP_NET_IDLE_CONNECTION_TIMEOUT, 5 * 60 * 1000L);
                this.httpNetConnectionTimeout = getLong(properties, env, PropertyKey.HTTP_NET_CONNECTION_TIMEOUT, this.httpNetConnectionTimeout);

                // deprecated
                this.httpNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.HTTP_NET_QUEUED_CONNECTION_TIMEOUT, 5 * 1000L);
                this.httpNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.HTTP_NET_CONNECTION_QUEUE_TIMEOUT, this.httpNetConnectionQueueTimeout);

                // deprecated
                this.httpNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_SND_BUF_SIZE, 2 * 1024 * 1024);
                this.httpNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_CONNECTION_SNDBUF, this.httpNetConnectionSndBuf);

                // deprecated
                this.httpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_RCV_BUF_SIZE, 2 * 1024 * 1024);
                this.httpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_CONNECTION_RCVBUF, this.httpNetConnectionRcvBuf);

                this.dateAdapterPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_DATE_ADAPTER_POOL_CAPACITY, 16);
                this.jsonCacheLimit = getIntSize(properties, env, PropertyKey.HTTP_TEXT_JSON_CACHE_LIMIT, 16384);
                this.jsonCacheSize = getIntSize(properties, env, PropertyKey.HTTP_TEXT_JSON_CACHE_SIZE, 8192);
                this.maxRequiredDelimiterStdDev = getDouble(properties, env, PropertyKey.HTTP_TEXT_MAX_REQUIRED_DELIMITER_STDDEV, 0.1222d);
                this.maxRequiredLineLengthStdDev = getDouble(properties, env, PropertyKey.HTTP_TEXT_MAX_REQUIRED_LINE_LENGTH_STDDEV, 0.8);
                this.metadataStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_METADATA_STRING_POOL_CAPACITY, 128);

                this.rollBufferLimit = getIntSize(properties, env, PropertyKey.HTTP_TEXT_ROLL_BUFFER_LIMIT, 1024 * 4096);
                this.rollBufferSize = getIntSize(properties, env, PropertyKey.HTTP_TEXT_ROLL_BUFFER_SIZE, 1024);
                this.textAnalysisMaxLines = getInt(properties, env, PropertyKey.HTTP_TEXT_ANALYSIS_MAX_LINES, 1000);
                this.textLexerStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_LEXER_STRING_POOL_CAPACITY, 64);
                this.timestampAdapterPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_TIMESTAMP_ADAPTER_POOL_CAPACITY, 64);
                this.utf8SinkSize = getIntSize(properties, env, PropertyKey.HTTP_TEXT_UTF8_SINK_SIZE, 4096);

                this.jsonQueryConnectionCheckFrequency = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_CONNECTION_CHECK_FREQUENCY, 1_000_000);
                this.jsonQueryFloatScale = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_FLOAT_SCALE, 4);
                this.jsonQueryDoubleScale = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_DOUBLE_SCALE, 12);
                this.httpReadOnlySecurityContext = getBoolean(properties, env, PropertyKey.HTTP_SECURITY_READONLY, false);
                this.maxHttpQueryResponseRowLimit = getLong(properties, env, PropertyKey.HTTP_SECURITY_MAX_RESPONSE_ROWS, Long.MAX_VALUE);
                this.interruptOnClosedConnection = getBoolean(properties, env, PropertyKey.HTTP_SECURITY_INTERRUPT_ON_CLOSED_CONNECTION, true);

                String httpBindTo = getString(properties, env, PropertyKey.HTTP_BIND_TO, "0.0.0.0:9000");
                parseBindTo(properties, env, PropertyKey.HTTP_NET_BIND_TO, httpBindTo, (a, p) -> {
                    httpNetBindIPv4Address = a;
                    httpNetBindPort = p;
                });

                // load mime types
                path.of(new File(new File(root, CONFIG_DIRECTORY), "mime.types").getAbsolutePath()).$();
                this.mimeTypesCache = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
            }

            this.maxRerunWaitCapMs = getLong(properties, env, PropertyKey.HTTP_BUSY_RETRY_MAXIMUM_WAIT_BEFORE_RETRY, 1000);
            this.rerunExponentialWaitMultiplier = getDouble(properties, env, PropertyKey.HTTP_BUSY_RETRY_EXPONENTIAL_WAIT_MULTIPLIER, 2.0);
            this.rerunInitialWaitQueueSize = getIntSize(properties, env, PropertyKey.HTTP_BUSY_RETRY_INITIAL_WAIT_QUEUE_SIZE, 64);
            this.rerunMaxProcessingQueueSize = getIntSize(properties, env, PropertyKey.HTTP_BUSY_RETRY_MAX_PROCESSING_QUEUE_SIZE, 4096);

            this.circuitBreakerThrottle = getInt(properties, env, PropertyKey.CIRCUIT_BREAKER_THROTTLE, 2_000_000);
            this.circuitBreakerBufferSize = getInt(properties, env, PropertyKey.CIRCUIT_BREAKER_BUFFER_SIZE, 64);
            this.circuitBreakerMaxTime = (long) (getDouble(properties, env, PropertyKey.QUERY_TIMEOUT_SEC, 60) * Timestamps.SECOND_MICROS);

            this.pgEnabled = getBoolean(properties, env, PropertyKey.PG_ENABLED, true);
            if (pgEnabled) {
                // deprecated
                pgNetConnectionLimit = getInt(properties, env, PropertyKey.PG_NET_ACTIVE_CONNECTION_LIMIT, 10);
                pgNetConnectionLimit = getInt(properties, env, PropertyKey.PG_NET_CONNECTION_LIMIT, pgNetConnectionLimit);
                pgNetConnectionHint = getBoolean(properties, env, PropertyKey.PG_NET_CONNECTION_HINT, false);
                parseBindTo(properties, env, PropertyKey.PG_NET_BIND_TO, "0.0.0.0:8812", (a, p) -> {
                    pgNetBindIPv4Address = a;
                    pgNetBindPort = p;
                });

                // deprecated
                this.pgNetIdleConnectionTimeout = getLong(properties, env, PropertyKey.PG_NET_IDLE_TIMEOUT, 300_000);
                this.pgNetIdleConnectionTimeout = getLong(properties, env, PropertyKey.PG_NET_CONNECTION_TIMEOUT, this.pgNetIdleConnectionTimeout);
                this.pgNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.PG_NET_CONNECTION_QUEUE_TIMEOUT, 5_000);

                // deprecated
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.PG_NET_RECV_BUF_SIZE, -1);
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.PG_NET_CONNECTION_RCVBUF, this.pgNetConnectionRcvBuf);

                // deprecated
                this.pgNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.PG_NET_SEND_BUF_SIZE, -1);
                this.pgNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.PG_NET_CONNECTION_SNDBUF, this.pgNetConnectionSndBuf);

                this.pgCharacterStoreCapacity = getInt(properties, env, PropertyKey.PG_CHARACTER_STORE_CAPACITY, 4096);
                this.pgBinaryParamsCapacity = getInt(properties, env, PropertyKey.PG_BINARY_PARAM_COUNT_CAPACITY, 2);
                this.pgCharacterStorePoolCapacity = getInt(properties, env, PropertyKey.PG_CHARACTER_STORE_POOL_CAPACITY, 64);
                this.pgConnectionPoolInitialCapacity = getInt(properties, env, PropertyKey.PG_CONNECTION_POOL_CAPACITY, 64);
                this.pgPassword = getString(properties, env, PropertyKey.PG_PASSWORD, "quest");
                this.pgUsername = getString(properties, env, PropertyKey.PG_USER, "admin");
                this.pgReadOnlySecurityContext = getBoolean(properties, env, PropertyKey.PG_SECURITY_READONLY, false);
                this.pgMaxBlobSizeOnQuery = getIntSize(properties, env, PropertyKey.PG_MAX_BLOB_SIZE_ON_QUERY, 512 * 1024);
                this.pgRecvBufferSize = getIntSize(properties, env, PropertyKey.PG_RECV_BUFFER_SIZE, 1024 * 1024);
                this.pgSendBufferSize = getIntSize(properties, env, PropertyKey.PG_SEND_BUFFER_SIZE, 1024 * 1024);
                final String dateLocale = getString(properties, env, PropertyKey.PG_DATE_LOCALE, "en");
                this.pgDefaultLocale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
                if (this.pgDefaultLocale == null) {
                    throw ServerConfigurationException.forInvalidKey(PropertyKey.PG_DATE_LOCALE.getPropertyPath(), dateLocale);
                }
                this.pgWorkerCount = getInt(properties, env, PropertyKey.PG_WORKER_COUNT, 0);
                cpuUsed += this.pgWorkerCount;
                this.pgWorkerAffinity = getAffinity(properties, env, PropertyKey.PG_WORKER_AFFINITY, pgWorkerCount);
                this.pgHaltOnError = getBoolean(properties, env, PropertyKey.PG_HALT_ON_ERROR, false);
                this.pgWorkerYieldThreshold = getLong(properties, env, PropertyKey.PG_WORKER_YIELD_THRESHOLD, 10);
                this.pgWorkerSleepThreshold = getLong(properties, env, PropertyKey.PG_WORKER_SLEEP_THRESHOLD, 10000);
                this.pgDaemonPool = getBoolean(properties, env, PropertyKey.PG_DAEMON_POOL, true);
                this.pgSelectCacheEnabled = getBoolean(properties, env, PropertyKey.PG_SELECT_CACHE_ENABLED, true);
                this.pgSelectCacheBlockCount = getInt(properties, env, PropertyKey.PG_SELECT_CACHE_BLOCK_COUNT, 16);
                this.pgSelectCacheRowCount = getInt(properties, env, PropertyKey.PG_SELECT_CACHE_ROW_COUNT, 16);
                this.pgInsertCacheEnabled = getBoolean(properties, env, PropertyKey.PG_INSERT_CACHE_ENABLED, true);
                this.pgInsertCacheBlockCount = getInt(properties, env, PropertyKey.PG_INSERT_CACHE_BLOCK_COUNT, 8);
                this.pgInsertCacheRowCount = getInt(properties, env, PropertyKey.PG_INSERT_CACHE_ROW_COUNT, 8);
                this.pgInsertPoolCapacity = getInt(properties, env, PropertyKey.PG_INSERT_POOL_CAPACITY, 64);
                this.pgNamedStatementCacheCapacity = getInt(properties, env, PropertyKey.PG_NAMED_STATEMENT_CACHE_CAPACITY, 32);
                this.pgNamesStatementPoolCapacity = getInt(properties, env, PropertyKey.PG_NAMED_STATEMENT_POOL_CAPACITY, 32);
                this.pgPendingWritersCacheCapacity = getInt(properties, env, PropertyKey.PG_PENDING_WRITERS_CACHE_CAPACITY, 16);
            }

            this.commitMode = getCommitMode(properties, env, PropertyKey.CAIRO_COMMIT_MODE);
            this.createAsSelectRetryCount = getInt(properties, env, PropertyKey.CAIRO_CREAT_AS_SELECT_RETRY_COUNT, 5);
            this.defaultMapType = getString(properties, env, PropertyKey.CAIRO_DEFAULT_MAP_TYPE, "fast");
            this.defaultSymbolCacheFlag = getBoolean(properties, env, PropertyKey.CAIRO_DEFAULT_SYMBOL_CACHE_FLAG, true);
            this.defaultSymbolCapacity = getInt(properties, env, PropertyKey.CAIRO_DEFAULT_SYMBOL_CAPACITY, 256);
            this.fileOperationRetryCount = getInt(properties, env, PropertyKey.CAIRO_FILE_OPERATION_RETRY_COUNT, 30);
            this.idleCheckInterval = getLong(properties, env, PropertyKey.CAIRO_IDLE_CHECK_INTERVAL, 5 * 60 * 1000L);
            this.inactiveReaderTTL = getLong(properties, env, PropertyKey.CAIRO_INACTIVE_READER_TTL, 120_000);
            this.inactiveWriterTTL = getLong(properties, env, PropertyKey.CAIRO_INACTIVE_WRITER_TTL, 600_000);
            this.indexValueBlockSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_INDEX_VALUE_BLOCK_SIZE, 256));
            this.maxSwapFileCount = getInt(properties, env, PropertyKey.CAIRO_MAX_SWAP_FILE_COUNT, 30);
            this.parallelIndexThreshold = getInt(properties, env, PropertyKey.CAIRO_PARALLEL_INDEX_THRESHOLD, 100000);
            this.readerPoolMaxSegments = getInt(properties, env, PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS, 5);
            this.spinLockTimeoutUs = getLong(properties, env, PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1_000_000);
            this.httpSqlCacheEnabled = getBoolean(properties, env, PropertyKey.HTTP_QUERY_CACHE_ENABLED, true);
            this.httpSqlCacheBlockCount = getInt(properties, env, PropertyKey.HTTP_QUERY_CACHE_BLOCK_COUNT, 4);
            this.httpSqlCacheRowCount = getInt(properties, env, PropertyKey.HTTP_QUERY_CACHE_ROW_COUNT, 16);
            this.sqlCharacterStoreCapacity = getInt(properties, env, PropertyKey.CAIRO_CHARACTER_STORE_CAPACITY, 1024);
            this.sqlCharacterStoreSequencePoolCapacity = getInt(properties, env, PropertyKey.CAIRO_CHARACTER_STORE_SEQUENCE_POOL_CAPACITY, 64);
            this.sqlColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_COLUMN_POOL_CAPACITY, 4096);
            this.sqlCompactMapLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_COMPACT_MAP_LOAD_FACTOR, 0.7);
            this.sqlExpressionPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_EXPRESSION_POOL_CAPACITY, 8192);
            this.sqlFastMapLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_FAST_MAP_LOAD_FACTOR, 0.5);
            this.sqlJoinContextPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_JOIN_CONTEXT_POOL_CAPACITY, 64);
            this.sqlLexerPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_LEXER_POOL_CAPACITY, 2048);
            this.sqlMapKeyCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_MAP_KEY_CAPACITY, 2048 * 1024);
            this.sqlMapPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_MAP_PAGE_SIZE, 4 * 1024 * 1024);
            this.sqlMapMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_MAP_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlMapMaxResizes = getIntSize(properties, env, PropertyKey.CAIRO_SQL_MAP_MAX_RESIZES, Integer.MAX_VALUE);
            this.sqlModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_MODEL_POOL_CAPACITY, 1024);
            this.sqlMaxNegativeLimit = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 10_000);
            this.sqlSortKeyPageSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 4 * 1024 * 1024);
            this.sqlSortKeyMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlSortLightValuePageSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_PAGE_SIZE, 8 * 1048576);
            this.sqlSortLightValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlHashJoinValuePageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 16777216);
            this.sqlHashJoinValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlLatestByRowCount = getInt(properties, env, PropertyKey.CAIRO_SQL_LATEST_BY_ROW_COUNT, 1000);
            this.sqlHashJoinLightValuePageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 1048576);
            this.sqlHashJoinLightValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlSortValuePageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 16777216);
            this.sqlSortValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.workStealTimeoutNanos = getLong(properties, env, PropertyKey.CAIRO_WORK_STEAL_TIMEOUT_NANOS, 10_000);
            this.parallelIndexingEnabled = getBoolean(properties, env, PropertyKey.CAIRO_PARALLEL_INDEXING_ENABLED, true);
            this.sqlJoinMetadataPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JOIN_METADATA_PAGE_SIZE, 16384);
            this.sqlJoinMetadataMaxResizes = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JOIN_METADATA_MAX_RESIZES, Integer.MAX_VALUE);
            this.sqlAnalyticColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_COLUMN_POOL_CAPACITY, 64);
            this.sqlCreateTableModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_CREATE_TABEL_MODEL_POOL_CAPACITY, 16);
            this.sqlColumnCastModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COLUMN_CAST_MODEL_POOL_CAPACITY, 16);
            this.sqlRenameTableModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_RENAME_TABLE_MODEL_POOL_CAPACITY, 16);
            this.sqlWithClauseModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_WITH_CLAUSE_MODEL_POOL_CAPACITY, 128);
            this.sqlInsertModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_INSERT_MODEL_POOL_CAPACITY, 64);
            this.sqlCopyModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COPY_MODEL_POOL_CAPACITY, 32);
            this.sqlCopyBufferSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE, 2 * 1024 * 1024);

            this.cairoPageFrameReduceQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 64));
            this.cairoPageFrameReduceRowIdListCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 256));
            this.cairoPageFrameReduceColumnListCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_COLUMN_LIST_CAPACITY, 16));
            this.sqlParallelFilterEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, true);
            this.cairoPageFrameReduceShardCount = getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 4);
            this.cairoPageFrameReduceTaskPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_TASK_POOL_CAPACITY, 4);

            this.writerDataIndexKeyAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_INDEX_KEY_APPEND_PAGE_SIZE, 512 * 1024));
            this.writerDataIndexValueAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE, 16 * 1024 * 1024));
            this.writerDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 16 * 1024 * 1024));
            this.writerMiscAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_MISC_APPEND_PAGE_SIZE, Files.PAGE_SIZE));

            this.sampleByIndexSearchPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SAMPLEBY_PAGE_SIZE, 0);
            this.sqlDoubleToStrCastScale = getInt(properties, env, PropertyKey.CAIRO_SQL_DOUBLE_CAST_SCALE, 12);
            this.sqlFloatToStrCastScale = getInt(properties, env, PropertyKey.CAIRO_SQL_FLOAT_CAST_SCALE, 4);
            this.sqlGroupByMapCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_MAP_CAPACITY, 1024);
            this.sqlGroupByPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_POOL_CAPACITY, 1024);
            this.sqlMaxSymbolNotEqualsCount = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_SYMBOL_NOT_EQUALS_COUNT, 100);
            this.sqlBindVariablePoolSize = getInt(properties, env, PropertyKey.CAIRO_SQL_BIND_VARIABLE_POOL_SIZE, 8);
            final String sqlCopyFormatsFile = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_FORMATS_FILE, "/text_loader.json");
            final String dateLocale = getString(properties, env, PropertyKey.CAIRO_DATE_LOCALE, "en");
            this.locale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
            if (this.locale == null) {
                throw ServerConfigurationException.forInvalidKey(PropertyKey.CAIRO_DATE_LOCALE.getPropertyPath(), dateLocale);
            }
            this.sqlDistinctTimestampKeyCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_DISTINCT_TIMESTAMP_KEY_CAPACITY, 512);
            this.sqlDistinctTimestampLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_SQL_DISTINCT_TIMESTAMP_LOAD_FACTOR, 0.5);
            this.sqlPageFrameMinRows = getInt(properties, env, PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
            this.sqlPageFrameMaxRows = getInt(properties, env, PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);

            this.sqlJitMode = getSqlJitMode(properties, env);
            this.sqlJitIRMemoryPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_IR_MEMORY_PAGE_SIZE, 8 * 1024);
            this.sqlJitIRMemoryMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_JIT_IR_MEMORY_MAX_PAGES, 8);
            this.sqlJitBindVarsMemoryPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE, 4 * 1024);
            this.sqlJitBindVarsMemoryMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_MAX_PAGES, 8);
            this.sqlJitRowsThreshold = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_ROWS_THRESHOLD, 1024 * 1024);
            this.sqlJitPageAddressCacheThreshold = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_PAGE_ADDRESS_CACHE_THRESHOLD, 1024 * 1024);
            this.sqlJitDebugEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_JIT_DEBUG_ENABLED, false);

            String value = getString(properties, env, PropertyKey.CAIRO_WRITER_FO_OPTS, "o_none");
            long lopts = CairoConfiguration.O_NONE;
            String[] opts = value.split("\\|");
            for (String opt : opts) {
                int index = WRITE_FO_OPTS.keyIndex(opt.trim());
                if (index < 0) {
                    lopts |= WRITE_FO_OPTS.valueAt(index);
                }
            }
            writerFileOpenOpts = lopts;

            this.inputFormatConfiguration = new InputFormatConfiguration(
                    new DateFormatFactory(),
                    DateLocaleFactory.INSTANCE,
                    new TimestampFormatFactory(),
                    this.locale
            );

            try (JsonLexer lexer = new JsonLexer(1024, 1024)) {
                inputFormatConfiguration.parseConfiguration(lexer, confRoot, sqlCopyFormatsFile);
            }

            this.inputRoot = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_ROOT, null);
            this.backupRoot = getString(properties, env, PropertyKey.CAIRO_SQL_BACKUP_ROOT, null);
            this.backupDirTimestampFormat = getTimestampFormat(properties, env);
            this.backupTempDirName = getString(properties, env, PropertyKey.CAIRO_SQL_BACKUP_DIR_TMP_NAME, "tmp");
            this.backupMkdirMode = getInt(properties, env, PropertyKey.CAIRO_SQL_BACKUP_MKDIR_MODE, 509);
            this.columnIndexerQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_COLUMN_INDEXER_QUEUE_CAPACITY, 64));
            this.vectorAggregateQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_VECTOR_AGGREGATE_QUEUE_CAPACITY, 128));
            this.o3CallbackQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_CALLBACK_QUEUE_CAPACITY, 128));
            this.o3PartitionQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_PARTITION_QUEUE_CAPACITY, 128));
            this.o3OpenColumnQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_OPEN_COLUMN_QUEUE_CAPACITY, 128));
            this.o3CopyQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_COPY_QUEUE_CAPACITY, 128));
            this.o3UpdPartitionSizeQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_UPD_PARTITION_SIZE_QUEUE_CAPACITY, 128));
            this.o3PurgeDiscoveryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_PURGE_DISCOVERY_QUEUE_CAPACITY, 128));
            this.o3ColumnMemorySize = (int) Files.ceilPageSize(getIntSize(properties, env, PropertyKey.CAIRO_O3_COLUMN_MEMORY_SIZE, 16 * Numbers.SIZE_1MB));
            this.maxUncommittedRows = getInt(properties, env, PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
            this.commitLag = getLong(properties, env, PropertyKey.CAIRO_COMMIT_LAG, 300_000) * 1_000;
            this.o3QuickSortEnabled = getBoolean(properties, env, PropertyKey.CAIRO_O3_QUICKSORT_ENABLED, false);
            this.rndFunctionMemoryPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_RND_MEMORY_PAGE_SIZE, 8192));
            this.rndFunctionMemoryMaxPages = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_RND_MEMORY_MAX_PAGES, 128));
            this.sqlAnalyticStorePageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_STORE_PAGE_SIZE, 1024 * 1024));
            this.sqlAnalyticStoreMaxPages = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_STORE_MAX_PAGES, Integer.MAX_VALUE));
            this.sqlAnalyticRowIdPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_ROWID_PAGE_SIZE, 512 * 1024));
            this.sqlAnalyticRowIdMaxPages = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_ROWID_MAX_PAGES, Integer.MAX_VALUE));
            this.sqlAnalyticTreeKeyPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_TREE_PAGE_SIZE, 512 * 1024));
            this.sqlAnalyticTreeKeyMaxPages = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_TREE_MAX_PAGES, Integer.MAX_VALUE));
            this.sqlTxnScoreboardEntryCount = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, 16384));
            this.latestByQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_LATESTBY_QUEUE_CAPACITY, 32));
            this.telemetryEnabled = getBoolean(properties, env, PropertyKey.TELEMETRY_ENABLED, true);
            this.telemetryDisableCompletely = getBoolean(properties, env, PropertyKey.TELEMETRY_DISABLE_COMPLETELY, false);
            this.telemetryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.TELEMETRY_QUEUE_CAPACITY, 512));
            this.telemetryHideTables = getBoolean(properties, env, PropertyKey.TELEMETRY_HIDE_TABLES, true);
            this.o3PartitionPurgeListCapacity = getInt(properties, env, PropertyKey.CAIRO_O3_PARTITION_PURGE_LIST_INITIAL_CAPACITY, 1);

            parseBindTo(properties, env, PropertyKey.LINE_UDP_BIND_TO, "0.0.0.0:9009", (a, p) -> {
                this.lineUdpBindIPV4Address = a;
                this.lineUdpPort = p;
            });

            this.lineUdpGroupIPv4Address = getIPv4Address(properties, env, PropertyKey.LINE_UDP_JOIN, "232.1.2.3");
            this.lineUdpCommitRate = getInt(properties, env, PropertyKey.LINE_UDP_COMMIT_RATE, 1_000_000);
            this.lineUdpMsgBufferSize = getIntSize(properties, env, PropertyKey.LINE_UDP_MSG_BUFFER_SIZE, 2048);
            this.lineUdpMsgCount = getInt(properties, env, PropertyKey.LINE_UDP_MSG_COUNT, 10_000);
            this.lineUdpReceiveBufferSize = getIntSize(properties, env, PropertyKey.LINE_UDP_RECEIVE_BUFFER_SIZE, 8 * 1024 * 1024);
            this.lineUdpEnabled = getBoolean(properties, env, PropertyKey.LINE_UDP_ENABLED, true);
            this.lineUdpOwnThreadAffinity = getInt(properties, env, PropertyKey.LINE_UDP_OWN_THREAD_AFFINITY, -1);
            this.lineUdpOwnThread = getBoolean(properties, env, PropertyKey.LINE_UDP_OWN_THREAD, false);
            this.lineUdpUnicast = getBoolean(properties, env, PropertyKey.LINE_UDP_UNICAST, false);
            this.lineUdpCommitMode = getCommitMode(properties, env, PropertyKey.LINE_UDP_COMMIT_MODE);
            this.lineUdpTimestampAdapter = getLineTimestampAdaptor(properties, env, PropertyKey.LINE_UDP_TIMESTAMP);
            String defaultUdpPartitionByProperty = getString(properties, env, PropertyKey.LINE_DEFAULT_PARTITION_BY, "DAY");
            this.lineUdpDefaultPartitionBy = PartitionBy.fromString(defaultUdpPartitionByProperty);
            if (this.lineUdpDefaultPartitionBy == -1) {
                log.info().$("invalid partition by ").$(lineUdpDefaultPartitionBy).$("), will use DAY for UDP").$();
                this.lineUdpDefaultPartitionBy = PartitionBy.DAY;
            }

            this.lineTcpEnabled = getBoolean(properties, env, PropertyKey.LINE_TCP_ENABLED, true);
            if (lineTcpEnabled) {
                // obsolete
                lineTcpNetConnectionLimit = getInt(properties, env, PropertyKey.LINE_TCP_NET_ACTIVE_CONNECTION_LIMIT, 256);
                lineTcpNetConnectionLimit = getInt(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_LIMIT, lineTcpNetConnectionLimit);
                lineTcpNetConnectionHint = getBoolean(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_HINT, false);
                parseBindTo(properties, env, PropertyKey.LINE_TCP_NET_BIND_TO, "0.0.0.0:9009", (a, p) -> {
                    lineTcpNetBindIPv4Address = a;
                    lineTcpNetBindPort = p;
                });

                // deprecated
                this.lineTcpNetConnectionTimeout = getLong(properties, env, PropertyKey.LINE_TCP_NET_IDLE_TIMEOUT, 0);
                this.lineTcpNetConnectionTimeout = getLong(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_TIMEOUT, this.lineTcpNetConnectionTimeout);

                // deprecated
                this.lineTcpNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.LINE_TCP_NET_QUEUED_TIMEOUT, 5_000);
                this.lineTcpNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_QUEUE_TIMEOUT, this.lineTcpNetConnectionQueueTimeout);

                // deprecated
                this.lineTcpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.LINE_TCP_NET_RECV_BUF_SIZE, -1);
                this.lineTcpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_RCVBUF, this.lineTcpNetConnectionRcvBuf);

                this.lineTcpConnectionPoolInitialCapacity = getInt(properties, env, PropertyKey.LINE_TCP_CONNECTION_POOL_CAPACITY, 8);
                this.lineTcpTimestampAdapter = getLineTimestampAdaptor(properties, env, PropertyKey.LINE_TCP_TIMESTAMP);
                this.lineTcpMsgBufferSize = getIntSize(properties, env, PropertyKey.LINE_TCP_MSG_BUFFER_SIZE, 32768);
                this.lineTcpMaxMeasurementSize = getIntSize(properties, env, PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE, 32768);
                if (lineTcpMaxMeasurementSize > lineTcpMsgBufferSize) {
                    throw new IllegalArgumentException(
                            PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getPropertyPath() + " (" + this.lineTcpMaxMeasurementSize + ") cannot be more than line.tcp.msg.buffer.size (" + this.lineTcpMsgBufferSize + ")");
                }
                this.lineTcpWriterQueueCapacity = getQueueCapacity(properties, env, PropertyKey.LINE_TCP_WRITER_QUEUE_CAPACITY, 128);
                this.lineTcpWriterWorkerCount = getInt(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_COUNT, 1);
                cpuUsed += this.lineTcpWriterWorkerCount;
                this.lineTcpWriterWorkerAffinity = getAffinity(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_AFFINITY, lineTcpWriterWorkerCount);
                this.lineTcpWriterWorkerPoolHaltOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_WRITER_HALT_ON_ERROR, false);
                this.lineTcpWriterWorkerYieldThreshold = getLong(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_YIELD_THRESHOLD, 10);
                this.lineTcpWriterWorkerSleepThreshold = getLong(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_SLEEP_THRESHOLD, 10000);
                this.symbolCacheWaitUsBeforeReload = getLong(properties, env, PropertyKey.LINE_TCP_SYMBOL_CACHE_WAIT_US_BEFORE_RELOAD, 500_000);

                int ilpTcpWorkerCount;
                if (cpuAvailable < 9) {
                    ilpTcpWorkerCount = 0;
                } else if (cpuAvailable < 17) {
                    ilpTcpWorkerCount = 2;
                } else {
                    ilpTcpWorkerCount = 6;
                }
                this.lineTcpIOWorkerCount = getInt(properties, env, PropertyKey.LINE_TCP_IO_WORKER_COUNT, ilpTcpWorkerCount);
                cpuUsed += this.lineTcpIOWorkerCount;
                this.lineTcpIOWorkerAffinity = getAffinity(properties, env, PropertyKey.LINE_TCP_IO_WORKER_AFFINITY, lineTcpIOWorkerCount);
                this.lineTcpIOWorkerPoolHaltOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_IO_HALT_ON_ERROR, false);
                this.lineTcpIOWorkerYieldThreshold = getLong(properties, env, PropertyKey.LINE_TCP_IO_WORKER_YIELD_THRESHOLD, 10);
                this.lineTcpIOWorkerSleepThreshold = getLong(properties, env, PropertyKey.LINE_TCP_IO_WORKER_SLEEP_THRESHOLD, 10000);
                this.lineTcpMaintenanceInterval = getLong(properties, env, PropertyKey.LINE_TCP_MAINTENANCE_JOB_INTERVAL, 30_000);
                this.lineTcpCommitIntervalFraction = getDouble(properties, env, PropertyKey.LINE_TCP_COMMIT_INTERVAL_FRACTION, 0.5);
                this.lineTcpCommitIntervalDefault = getLong(properties, env, PropertyKey.LINE_TCP_COMMIT_INTERVAL_DEFAULT, COMMIT_INTERVAL_DEFAULT);
                if (this.lineTcpCommitIntervalDefault < 1L) {
                    log.info().$("invalid default commit interval ").$(lineTcpCommitIntervalDefault).$("), will use ").$(COMMIT_INTERVAL_DEFAULT).$();
                    this.lineTcpCommitIntervalDefault = COMMIT_INTERVAL_DEFAULT;
                }
                this.lineTcpAuthDbPath = getString(properties, env, PropertyKey.LINE_TCP_AUTH_DB_PATH, null);
                // deprecated
                String defaultTcpPartitionByProperty = getString(properties, env, PropertyKey.LINE_TCP_DEFAULT_PARTITION_BY, "DAY");
                defaultTcpPartitionByProperty = getString(properties, env, PropertyKey.LINE_DEFAULT_PARTITION_BY, defaultTcpPartitionByProperty);
                this.lineTcpDefaultPartitionBy = PartitionBy.fromString(defaultTcpPartitionByProperty);
                if (this.lineTcpDefaultPartitionBy == -1) {
                    log.info().$("invalid partition by ").$(defaultTcpPartitionByProperty).$("), will use DAY for TCP").$();
                    this.lineTcpDefaultPartitionBy = PartitionBy.DAY;
                }
                if (null != lineTcpAuthDbPath) {
                    this.lineTcpAuthDbPath = new File(root, this.lineTcpAuthDbPath).getAbsolutePath();
                }
                this.minIdleMsBeforeWriterRelease = getLong(properties, env, PropertyKey.LINE_TCP_MIN_IDLE_MS_BEFORE_WRITER_RELEASE, 10_000);
                this.lineTcpDisconnectOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_DISCONNECT_ON_ERROR, true);
                this.stringToCharCastAllowed = getBoolean(properties, env, PropertyKey.LINE_TCP_UNDOCUMENTED_STRING_TO_CHAR_CAST_ALLOWED, false);
                this.symbolAsFieldSupported = getBoolean(properties, env, PropertyKey.LINE_TCP_UNDOCUMENTED_SYMBOL_AS_FIELD_SUPPORTED, false);
                this.isStringAsTagSupported = getBoolean(properties, env, PropertyKey.LINE_TCP_UNDOCUMENTED_STRING_AS_TAG_SUPPORTED, false);
                String floatDefaultColumnTypeName = getString(properties, env, PropertyKey.LINE_FLOAT_DEFAULT_COLUMN_TYPE, ColumnType.nameOf(ColumnType.DOUBLE));
                this.floatDefaultColumnType = ColumnType.tagOf(floatDefaultColumnTypeName);
                if (floatDefaultColumnType != ColumnType.DOUBLE && floatDefaultColumnType != ColumnType.FLOAT) {
                    log.info().$("invalid default column type for float ").$(floatDefaultColumnTypeName).$("), will use DOUBLE").$();
                    this.floatDefaultColumnType = ColumnType.DOUBLE;
                }
                String integerDefaultColumnTypeName = getString(properties, env, PropertyKey.LINE_INTEGER_DEFAULT_COLUMN_TYPE, ColumnType.nameOf(ColumnType.LONG));
                this.integerDefaultColumnType = ColumnType.tagOf(integerDefaultColumnTypeName);
                if (integerDefaultColumnType != ColumnType.LONG && integerDefaultColumnType != ColumnType.INT && integerDefaultColumnType != ColumnType.SHORT && integerDefaultColumnType != ColumnType.BYTE) {
                    log.info().$("invalid default column type for integer ").$(integerDefaultColumnTypeName).$("), will use LONG").$();
                    this.integerDefaultColumnType = ColumnType.LONG;
                }
            }

            this.sharedWorkerCount = getInt(properties, env, PropertyKey.SHARED_WORKER_COUNT, Math.max(1, cpuAvailable / 2 - 1 - cpuUsed));
            this.sharedWorkerAffinity = getAffinity(properties, env, PropertyKey.SHARED_WORKER_AFFINITY, sharedWorkerCount);
            this.sharedWorkerHaltOnError = getBoolean(properties, env, PropertyKey.SHARED_WORKER_HALT_ON_ERROR, false);
            this.sharedWorkerYieldThreshold = getLong(properties, env, PropertyKey.SHARED_WORKER_YIELD_THRESHOLD, 100);
            this.sharedWorkerSleepThreshold = getLong(properties, env, PropertyKey.SHARED_WORKER_SLEEP_THRESHOLD, 10_000);
            this.sharedWorkerSleepMs = getLong(properties, env, PropertyKey.SHARED_WORKER_SLEEP_MS, 100);

            this.metricsEnabled = getBoolean(properties, env, PropertyKey.METRICS_ENABLED, false);
            this.writerAsyncCommandBusyWaitTimeout = getLong(properties, env, PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT_MICRO, 500_000);
            this.writerAsyncCommandMaxWaitTimeout = getLong(properties, env, PropertyKey.CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT_MICRO, 30_000_000L);
            this.writerTickRowsCountMod = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_WRITER_TICK_ROWS_COUNT, 1024)) - 1;
            this.writerAsyncCommandQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 32));
            this.writerAsyncCommandQueueSlotSize = Numbers.ceilPow2(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_SLOT_SIZE, 2048));

            this.queryCacheEventQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_QUERY_CACHE_EVENT_QUEUE_CAPACITY, 4));

            this.buildInformation = buildInformation;
            this.binaryEncodingMaxLength = getInt(properties, env, PropertyKey.BINARYDATA_ENCODING_MAXLENGTH, 32768);
        }
    }

    public static String rootSubdir(CharSequence dbRoot, CharSequence subdir) {
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
            return sink.put(subdir).toString();
        }
        return null;
    }

    public static ValidationResult validate(Properties properties) {
        // Settings that used to be valid but no longer are.
        Map<String, String> obsolete = new HashMap<>();

        // Settings that are still valid but are now superseded by newer ones.
        Map<String, String> deprecated = new HashMap<>();

        // Settings that are not recognized.
        Set<String> incorrect = new HashSet<>();

        for (String propName : properties.stringPropertyNames()) {
            Optional<PropertyKey> prop = PropertyKey.getByString(propName);
            if (prop.isPresent()) {
                String deprecationMsg = DEPRECATED_SETTINGS.get(prop.get());
                if (deprecationMsg != null) {
                    deprecated.put(propName, deprecationMsg);
                }
            } else {
                String obsoleteMsg = OBSOLETE_SETTINGS.get(propName);
                if (obsoleteMsg != null) {
                    obsolete.put(propName, obsoleteMsg);
                } else {
                    incorrect.add(propName);
                }
            }
        }

        if (obsolete.isEmpty() && deprecated.isEmpty() && incorrect.isEmpty()) {
            return null;
        }

        boolean isError = false;

        StringBuilder sb = new StringBuilder("Configuration issues:\n");

        if (!incorrect.isEmpty()) {
            isError = true;
            sb.append("    Invalid settings (not recognized, probable typos):\n");
            for (String key : incorrect) {
                sb.append("        * ");
                sb.append(key);
                sb.append('\n');
            }
        }

        if (!obsolete.isEmpty()) {
            isError = true;
            sb.append("    Obsolete settings (no longer recognized):\n");
            for (Map.Entry<String, String> entry : obsolete.entrySet()) {
                sb.append("        * ");
                sb.append(entry.getKey());
                sb.append(": ");
                sb.append(entry.getValue());
                sb.append('\n');
            }
        }

        if (!deprecated.isEmpty()) {
            sb.append("    Deprecated settings (recognized but superseded by newer settings):\n");
            for (Map.Entry<String, String> entry : deprecated.entrySet()) {
                sb.append("        * ");
                sb.append(entry.getKey());
                sb.append(": ");
                sb.append(entry.getValue());
                sb.append('\n');
            }
        }

        return new ValidationResult(isError, sb.toString());
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

    private static <KeyT> void registerReplacements(
            Map<KeyT, String> map,
            KeyT old,
            PropertyKey... replacements) {
        StringBuilder sb = new StringBuilder("Replaced by ");
        for (int index = 0; index < replacements.length; ++index) {
            if (index > 0) {
                sb.append(index < (replacements.length - 1)
                        ? ", "
                        : " and ");
            }
            String replacement = replacements[index].getPropertyPath();
            sb.append('`');
            sb.append(replacement);
            sb.append('`');
        }
        map.put(old, sb.toString());
    }

    private static void registerObsolete(String old, PropertyKey... replacements) {
        registerReplacements(OBSOLETE_SETTINGS, old, replacements);
    }

    private static void registerDeprecated(PropertyKey old, PropertyKey... replacements) {
        registerReplacements(DEPRECATED_SETTINGS, old, replacements);
    }

    private int[] getAffinity(Properties properties, @Nullable Map<String, String> env, PropertyKey key, int httpWorkerCount) throws ServerConfigurationException {
        final int[] result = new int[httpWorkerCount];
        String value = overrideWithEnv(properties, env, key);
        if (value == null) {
            Arrays.fill(result, -1);
        } else {
            String[] affinity = value.split(",");
            if (affinity.length != httpWorkerCount) {
                throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "wrong number of affinity values");
            }
            for (int i = 0; i < httpWorkerCount; i++) {
                try {
                    result[i] = Numbers.parseInt(affinity[i]);
                } catch (NumericException e) {
                    throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "Invalid affinity value: " + affinity[i]);
                }
            }
        }
        return result;
    }

    protected boolean getBoolean(Properties properties, @Nullable Map<String, String> env, PropertyKey key, boolean defaultValue) {
        final String value = overrideWithEnv(properties, env, key);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private int getCommitMode(Properties properties, @Nullable Map<String, String> env, PropertyKey key) {
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

    private double getDouble(Properties properties, @Nullable Map<String, String> env, PropertyKey key, double defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseDouble(value) : defaultValue;
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected int getIPv4Address(Properties properties, Map<String, String> env, PropertyKey key, String defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, defaultValue);
        try {
            return Net.parseIPv4(value);
        } catch (NetworkError e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    private int getInt(Properties properties, @Nullable Map<String, String> env, PropertyKey key, int defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseInt(value) : defaultValue;
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected int getIntSize(Properties properties, @Nullable Map<String, String> env, PropertyKey key, int defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseIntSize(value) : defaultValue;
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    private LineProtoTimestampAdapter getLineTimestampAdaptor(Properties properties, Map<String, String> env, PropertyKey propNm) {
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

    private long getLong(Properties properties, @Nullable Map<String, String> env, PropertyKey key, long defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseLong(value) : defaultValue;
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    private long getLongSize(Properties properties, @Nullable Map<String, String> env, PropertyKey key, long defaultValue) throws ServerConfigurationException {
        final String value = overrideWithEnv(properties, env, key);
        try {
            return value != null ? Numbers.parseLongSize(value) : defaultValue;
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    private int getQueueCapacity(Properties properties, @Nullable Map<String, String> env, PropertyKey key, int defaultValue) throws ServerConfigurationException {
        final int value = getInt(properties, env, key, defaultValue);
        if (!Numbers.isPow2(value)) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "Value must be power of 2, e.g. 1,2,4,8,16,32,64...");
        }
        return value;
    }

    private int getSqlJitMode(Properties properties, @Nullable Map<String, String> env) {
        final String jitMode = overrideWithEnv(properties, env, PropertyKey.CAIRO_SQL_JIT_MODE);

        if (jitMode == null) {
            return SqlJitMode.JIT_MODE_ENABLED;
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

        return SqlJitMode.JIT_MODE_ENABLED;
    }

    private String getString(Properties properties, @Nullable Map<String, String> env, PropertyKey key, String defaultValue) {
        String value = overrideWithEnv(properties, env, key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    private DateFormat getTimestampFormat(Properties properties, @Nullable Map<String, String> env) {
        final String pattern = overrideWithEnv(properties, env, PropertyKey.CAIRO_SQL_BACKUP_DIR_DATETIME_FORMAT);
        TimestampFormatCompiler compiler = new TimestampFormatCompiler();
        //noinspection ReplaceNullCheck
        if (null != pattern) {
            return compiler.compile(pattern);
        }
        return compiler.compile("yyyy-MM-dd");
    }

    private String overrideWithEnv(Properties properties, @Nullable Map<String, String> env, PropertyKey key) {
        String envCandidate = "QDB_" + key.getPropertyPath().replace('.', '_').toUpperCase();
        String envValue = env != null ? env.get(envCandidate) : null;
        if (envValue != null) {
            log.info().$("env config [key=").$(envCandidate).$(']').$();
            return envValue;
        }
        return properties.getProperty(key.getPropertyPath());
    }

    protected void parseBindTo(
            Properties properties,
            Map<String, String> env,
            PropertyKey key,
            String defaultValue,
            BindToParser parser
    ) throws ServerConfigurationException {

        final String bindTo = getString(properties, env, key, defaultValue);
        final int colonIndex = bindTo.indexOf(':');
        if (colonIndex == -1) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), bindTo);
        }

        final String ipv4Str = bindTo.substring(0, colonIndex);
        final int ipv4;
        try {
            ipv4 = Net.parseIPv4(ipv4Str);
        } catch (NetworkError e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), ipv4Str);
        }

        final String portStr = bindTo.substring(colonIndex + 1);
        final int port;
        try {
            port = Numbers.parseInt(portStr);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), portStr);
        }

        parser.onReady(ipv4, port);
    }

    private void validateProperties(Properties properties, boolean configValidationStrict) throws ServerConfigurationException {
        ValidationResult validation = validate(properties);
        if (validation != null) {
            if (validation.isError && configValidationStrict) {
                throw new ServerConfigurationException(validation.message);
            } else {
                log.advisory().$(validation.message).$();
            }
        }
    }

    @FunctionalInterface
    protected interface BindToParser {
        void onReady(int address, int port);
    }

    static class ValidationResult {
        final boolean isError;
        final String message;

        private ValidationResult(boolean isError, String message) {
            this.isError = isError;
            this.message = message;
        }
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
            return httpReadOnlySecurityContext;
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
        public boolean enableDevelopmentUpdates() {
            return false;
        }

        @Override
        public boolean enableTestFactories() {
            return false;
        }

        @Override
        public int getAnalyticColumnPoolCapacity() {
            return sqlAnalyticColumnPoolCapacity;
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
        public CharSequence getSnapshotRoot() {
            return snapshotRoot;
        }

        @Override
        public CharSequence getSnapshotInstanceId() {
            return snapshotInstanceId;
        }

        @Override
        public boolean isSnapshotRecoveryEnabled() {
            return snapshotRecoveryEnabled;
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
        public long getDataAppendPageSize() {
            return writerDataAppendPageSize;
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
        public long getMiscAppendPageSize() {
            return writerMiscAppendPageSize;
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
        public int getPageFrameReduceQueueCapacity() {
            return cairoPageFrameReduceQueueCapacity;
        }

        @Override
        public boolean isSqlParallelFilterEnabled() {
            return sqlParallelFilterEnabled;
        }

        @Override
        public int getPageFrameReduceShardCount() {
            return cairoPageFrameReduceShardCount;
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
        public int getRndFunctionMemoryPageSize() {
            return rndFunctionMemoryPageSize;
        }

        @Override
        public int getRndFunctionMemoryMaxPages() {
            return rndFunctionMemoryMaxPages;
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
        public int getSqlJitBindVarsMemoryMaxPages() {
            return sqlJitBindVarsMemoryMaxPages;
        }

        @Override
        public int getSqlJitBindVarsMemoryPageSize() {
            return sqlJitBindVarsMemoryPageSize;
        }

        @Override
        public int getSqlJitIRMemoryMaxPages() {
            return sqlJitIRMemoryMaxPages;
        }

        @Override
        public int getSqlJitIRMemoryPageSize() {
            return sqlJitIRMemoryPageSize;
        }

        @Override
        public int getSqlJitMode() {
            return sqlJitMode;
        }

        @Override
        public int getSqlJitPageAddressCacheThreshold() {
            return sqlJitPageAddressCacheThreshold;
        }

        @Override
        public int getSqlJitRowsThreshold() {
            return sqlJitRowsThreshold;
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
        public int getSqlMaxNegativeLimit() {
            return sqlMaxNegativeLimit;
        }

        @Override
        public int getSqlPageFrameMinRows() {
            return sqlPageFrameMinRows;
        }

        @Override
        public int getSqlPageFrameMaxRows() {
            return sqlPageFrameMaxRows;
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
        public long getWriterCommandQueueSlotSize() {
            return writerAsyncCommandQueueSlotSize;
        }

        @Override
        public long getWriterFileOpenOpts() {
            return writerFileOpenOpts;
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

        @Override
        public boolean isSqlJitDebugEnabled() {
            return sqlJitDebugEnabled;
        }

        @Override
        public int getPageFrameReduceRowIdListCapacity() {
            return cairoPageFrameReduceRowIdListCapacity;
        }

        @Override
        public int getPageFrameReduceColumnListCapacity() {
            return cairoPageFrameReduceColumnListCapacity;
        }

        @Override
        public int getPageFrameReduceTaskPoolCapacity() {
            return cairoPageFrameReduceTaskPoolCapacity;
        }

        @Override
        public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
            return circuitBreakerConfiguration;
        }

        @Override
        public int getQueryCacheEventQueueCapacity() {
            return queryCacheEventQueueCapacity;
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

        @Override
        public short getDefaultColumnTypeForFloat() {
            return floatDefaultColumnType;
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return integerDefaultColumnType;
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
        public double getCommitIntervalFraction() {
            return lineTcpCommitIntervalFraction;
        }

        @Override
        public long getCommitIntervalDefault() {
            return lineTcpCommitIntervalDefault;
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
        public boolean getDisconnectOnError() {
            return lineTcpDisconnectOnError;
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return symbolCacheWaitUsBeforeReload;
        }

        @Override
        public boolean isStringToCharCastAllowed() {
            return stringToCharCastAllowed;
        }

        @Override
        public boolean isSymbolAsFieldSupported() {
            return symbolAsFieldSupported;
        }

        @Override
        public boolean isStringAsTagSupported() {
            return isStringAsTagSupported;
        }

        @Override
        public short getDefaultColumnTypeForFloat() {
            return floatDefaultColumnType;
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return integerDefaultColumnType;
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
        public boolean readOnlySecurityContext() {
            return pgReadOnlySecurityContext;
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

        @Override
        public boolean hideTables() {
            return telemetryHideTables;
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

    static {
        WRITE_FO_OPTS.put("o_direct", (int) CairoConfiguration.O_DIRECT);
        WRITE_FO_OPTS.put("o_sync", (int) CairoConfiguration.O_SYNC);
        WRITE_FO_OPTS.put("o_async", (int) CairoConfiguration.O_ASYNC);
        WRITE_FO_OPTS.put("o_none", (int) CairoConfiguration.O_NONE);

        registerObsolete(
                "line.tcp.commit.timeout",
                PropertyKey.LINE_TCP_COMMIT_INTERVAL_DEFAULT,
                PropertyKey.LINE_TCP_COMMIT_INTERVAL_FRACTION);
        registerObsolete(
                "cairo.timestamp.locale",
                PropertyKey.CAIRO_DATE_LOCALE);
        registerObsolete(
                "pg.timestamp.locale",
                PropertyKey.PG_DATE_LOCALE);

        registerDeprecated(
                PropertyKey.HTTP_MIN_BIND_TO,
                PropertyKey.HTTP_MIN_NET_BIND_TO);
        registerDeprecated(
                PropertyKey.HTTP_MIN_NET_IDLE_CONNECTION_TIMEOUT,
                PropertyKey.HTTP_MIN_NET_CONNECTION_TIMEOUT);
        registerDeprecated(
                PropertyKey.HTTP_MIN_NET_QUEUED_CONNECTION_TIMEOUT,
                PropertyKey.HTTP_MIN_NET_CONNECTION_QUEUE_TIMEOUT);
        registerDeprecated(
                PropertyKey.HTTP_MIN_NET_SND_BUF_SIZE,
                PropertyKey.HTTP_MIN_NET_CONNECTION_SNDBUF);
        registerDeprecated(
                PropertyKey.HTTP_NET_RCV_BUF_SIZE,
                PropertyKey.HTTP_MIN_NET_CONNECTION_RCVBUF,
                PropertyKey.HTTP_NET_CONNECTION_RCVBUF);
        registerDeprecated(
                PropertyKey.HTTP_NET_ACTIVE_CONNECTION_LIMIT,
                PropertyKey.HTTP_NET_CONNECTION_LIMIT);
        registerDeprecated(
                PropertyKey.HTTP_NET_IDLE_CONNECTION_TIMEOUT,
                PropertyKey.HTTP_NET_CONNECTION_TIMEOUT);
        registerDeprecated(
                PropertyKey.HTTP_NET_QUEUED_CONNECTION_TIMEOUT,
                PropertyKey.HTTP_NET_CONNECTION_QUEUE_TIMEOUT);
        registerDeprecated(
                PropertyKey.HTTP_NET_SND_BUF_SIZE,
                PropertyKey.HTTP_NET_CONNECTION_SNDBUF);
        registerDeprecated(
                PropertyKey.PG_NET_ACTIVE_CONNECTION_LIMIT,
                PropertyKey.PG_NET_CONNECTION_LIMIT);
        registerDeprecated(
                PropertyKey.PG_NET_IDLE_TIMEOUT,
                PropertyKey.PG_NET_CONNECTION_TIMEOUT);
        registerDeprecated(
                PropertyKey.PG_NET_RECV_BUF_SIZE,
                PropertyKey.PG_NET_CONNECTION_RCVBUF);
        registerDeprecated(
                PropertyKey.LINE_TCP_NET_ACTIVE_CONNECTION_LIMIT,
                PropertyKey.LINE_TCP_NET_CONNECTION_LIMIT);
        registerDeprecated(
                PropertyKey.LINE_TCP_NET_IDLE_TIMEOUT,
                PropertyKey.LINE_TCP_NET_CONNECTION_TIMEOUT);
        registerDeprecated(
                PropertyKey.LINE_TCP_NET_QUEUED_TIMEOUT,
                PropertyKey.LINE_TCP_NET_CONNECTION_QUEUE_TIMEOUT);
        registerDeprecated(
                PropertyKey.LINE_TCP_NET_RECV_BUF_SIZE,
                PropertyKey.LINE_TCP_NET_CONNECTION_RCVBUF);
        registerDeprecated(
                PropertyKey.LINE_TCP_DEFAULT_PARTITION_BY,
                PropertyKey.LINE_DEFAULT_PARTITION_BY);
    }
}
