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

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.line.*;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfigurationHelper;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.text.CsvFileIndexer;
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
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class PropServerConfiguration implements ServerConfiguration {

    public static final long COMMIT_INTERVAL_DEFAULT = 2000;
    public static final String CONFIG_DIRECTORY = "conf";
    public static final String DB_DIRECTORY = "db";
    public static final String SNAPSHOT_DIRECTORY = "snapshot";
    public static final String TMP_DIRECTORY = "tmp";
    private static final LowerCaseCharSequenceIntHashMap WRITE_FO_OPTS = new LowerCaseCharSequenceIntHashMap();
    private final ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> allPairs = new ObjObjHashMap<>();
    private final DateFormat backupDirTimestampFormat;
    private final int backupMkdirMode;
    private final String backupRoot;
    private final CharSequence backupTempDirName;
    private final int binaryEncodingMaxLength;
    private final BuildInformation buildInformation;
    private final boolean cairoAttachPartitionCopy;
    private final String cairoAttachPartitionSuffix;
    private final CairoConfiguration cairoConfiguration = new PropCairoConfiguration();
    private final int cairoGroupByMergeShardQueueCapacity;
    private final int cairoGroupByShardCount;
    private final int cairoGroupByShardingThreshold;
    private final int cairoMaxCrashFiles;
    private final int cairoPageFrameReduceColumnListCapacity;
    private final int cairoPageFrameReduceQueueCapacity;
    private final int cairoPageFrameReduceRowIdListCapacity;
    private final int cairoPageFrameReduceShardCount;
    private final int cairoSQLCopyIdSupplier;
    private final int cairoSqlCopyLogRetentionDays;
    private final int cairoSqlCopyQueueCapacity;
    private final String cairoSqlCopyRoot;
    private final String cairoSqlCopyWorkRoot;
    private final long cairoTableRegistryAutoReloadFrequency;
    private final int cairoTableRegistryCompactionThreshold;
    private final PropSqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new PropSqlExecutionCircuitBreakerConfiguration();
    private final int circuitBreakerThrottle;
    private final int columnIndexerQueueCapacity;
    private final int columnPurgeQueueCapacity;
    private final long columnPurgeRetryDelay;
    private final long columnPurgeRetryDelayLimit;
    private final double columnPurgeRetryDelayMultiplier;
    private final int columnPurgeTaskPoolCapacity;
    private final int commitMode;
    private final TimestampFormatCompiler compiler = new TimestampFormatCompiler();
    private final String confRoot;
    private final int connectionPoolInitialCapacity;
    private final int connectionStringPoolCapacity;
    private final int createAsSelectRetryCount;
    private final int dateAdapterPoolCapacity;
    private final String dbDirectory;
    private final CharSequence defaultMapType;
    private final boolean defaultSymbolCacheFlag;
    private final int defaultSymbolCapacity;
    private final int detachedMkdirMode;
    private final int fileOperationRetryCount;
    private final FilesFacade filesFacade;
    private final FactoryProviderFactory fpf;
    private final boolean httpAllowDeflateBeforeSend;
    private final PropHttpContextConfiguration httpContextConfiguration = new PropHttpContextConfiguration();
    private final boolean httpFrozenClock;
    private final boolean httpHealthCheckAuthRequired;
    private final IODispatcherConfiguration httpIODispatcherConfiguration = new PropHttpIODispatcherConfiguration();
    private final PropHttpMinIODispatcherConfiguration httpMinIODispatcherConfiguration = new PropHttpMinIODispatcherConfiguration();
    private final PropHttpMinServerConfiguration httpMinServerConfiguration = new PropHttpMinServerConfiguration();
    private final boolean httpMinServerEnabled;
    private final boolean httpNetConnectionHint;
    private final boolean httpPessimisticHealthCheckEnabled;
    private final boolean httpReadOnlySecurityContext;
    private final int httpRecvBufferSize;
    private final int httpSendBufferSize;
    private final HttpServerConfiguration httpServerConfiguration = new PropHttpServerConfiguration();
    private final boolean httpServerCookiesEnabled;
    private final boolean httpServerEnabled;
    private final boolean httpServerKeepAlive;
    private final int httpSqlCacheBlockCount;
    private final boolean httpSqlCacheEnabled;
    private final int httpSqlCacheRowCount;
    private final boolean httpStaticAuthRequired;
    private final WaitProcessorConfiguration httpWaitProcessorConfiguration = new PropWaitProcessorConfiguration();
    private final int[] httpWorkerAffinity;
    private final int httpWorkerCount;
    private final boolean httpWorkerHaltOnError;
    private final long httpWorkerSleepThreshold;
    private final long httpWorkerSleepTimeout;
    private final long httpWorkerYieldThreshold;
    private final long idleCheckInterval;
    private final boolean ilpAutoCreateNewColumns;
    private final boolean ilpAutoCreateNewTables;
    private final int inactiveReaderMaxOpenPartitions;
    private final long inactiveReaderTTL;
    private final long inactiveWalWriterTTL;
    private final long inactiveWriterTTL;
    private final CharSequence indexFileName;
    private final int indexValueBlockSize;
    private final InputFormatConfiguration inputFormatConfiguration;
    private final long instanceHashHi;
    private final long instanceHashLo;
    private final boolean interruptOnClosedConnection;
    private final boolean ioURingEnabled;
    private final boolean isReadOnlyInstance;
    private final int jsonCacheLimit;
    private final int jsonCacheSize;
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new PropJsonQueryProcessorConfiguration();
    private final String keepAliveHeader;
    private final int latestByQueueCapacity;
    private final boolean lineHttpEnabled;
    private final CharSequence lineHttpPingVersion;
    private final LineHttpProcessorConfiguration lineHttpProcessorConfiguration = new PropLineHttpProcessorConfiguration();
    private final String lineTcpAuthDB;
    private final boolean lineTcpEnabled;
    private final WorkerPoolConfiguration lineTcpIOWorkerPoolConfiguration = new PropLineTcpIOWorkerPoolConfiguration();
    private final LineTcpReceiverConfiguration lineTcpReceiverConfiguration = new PropLineTcpReceiverConfiguration();
    private final IODispatcherConfiguration lineTcpReceiverDispatcherConfiguration = new PropLineTcpReceiverIODispatcherConfiguration();
    private final WorkerPoolConfiguration lineTcpWriterWorkerPoolConfiguration = new PropLineTcpWriterWorkerPoolConfiguration();
    private final int lineUdpCommitMode;
    private final int lineUdpCommitRate;
    private final boolean lineUdpEnabled;
    private final int lineUdpGroupIPv4Address;
    private final int lineUdpMsgBufferSize;
    private final int lineUdpMsgCount;
    private final boolean lineUdpOwnThread;
    private final int lineUdpOwnThreadAffinity;
    private final int lineUdpReceiveBufferSize;
    private final LineUdpReceiverConfiguration lineUdpReceiverConfiguration = new PropLineUdpReceiverConfiguration();
    private final LineTimestampAdapter lineUdpTimestampAdapter;
    private final boolean lineUdpUnicast;
    private final DateLocale locale;
    private final Log log;
    private final int maxFileNameLength;
    private final long maxHttpQueryResponseRowLimit;
    private final double maxRequiredDelimiterStdDev;
    private final double maxRequiredLineLengthStdDev;
    private final long maxRerunWaitCapMs;
    private final int maxSwapFileCount;
    private final int maxUncommittedRows;
    private final int metadataStringPoolCapacity;
    private final MetricsConfiguration metricsConfiguration = new PropMetricsConfiguration();
    private final boolean metricsEnabled;
    private final MicrosecondClock microsecondClock;
    private final int mkdirMode;
    private final int multipartHeaderBufferSize;
    private final long multipartIdleSpinCount;
    private final int o3CallbackQueueCapacity;
    private final int o3ColumnMemorySize;
    private final int o3CopyQueueCapacity;
    private final int o3LagCalculationWindowsSize;
    private final int o3LastPartitionMaxSplits;
    private final long o3MaxLag;
    private final long o3MinLagUs;
    private final int o3OpenColumnQueueCapacity;
    private final int o3PartitionPurgeListCapacity;
    private final int o3PartitionQueueCapacity;
    private final long o3PartitionSplitMinSize;
    private final int o3PurgeDiscoveryQueueCapacity;
    private final boolean o3QuickSortEnabled;
    private final int parallelIndexThreshold;
    private final boolean parallelIndexingEnabled;
    private final boolean pgEnabled;
    private final PGWireConfiguration pgWireConfiguration = new PropPGWireConfiguration();
    private final PropPGWireDispatcherConfiguration propPGWireDispatcherConfiguration = new PropPGWireDispatcherConfiguration();
    private final String publicDirectory;
    private final long queryTimeout;
    private final int readerPoolMaxSegments;
    private final int repeatMigrationFromVersion;
    private final int requestHeaderBufferSize;
    private final double rerunExponentialWaitMultiplier;
    private final int rerunInitialWaitQueueSize;
    private final int rerunMaxProcessingQueueSize;
    private final int rndFunctionMemoryMaxPages;
    private final int rndFunctionMemoryPageSize;
    private final int rollBufferLimit;
    private final int rollBufferSize;
    private final String root;
    private final int sampleByIndexSearchPageSize;
    private final int[] sharedWorkerAffinity;
    private final int sharedWorkerCount;
    private final boolean sharedWorkerHaltOnError;
    private final WorkerPoolConfiguration sharedWorkerPoolConfiguration = new PropWorkerPoolConfiguration();
    private final long sharedWorkerSleepThreshold;
    private final long sharedWorkerSleepTimeout;
    private final long sharedWorkerYieldThreshold;
    private final boolean simulateCrashEnabled;
    private final String snapshotInstanceId;
    private final boolean snapshotRecoveryEnabled;
    private final String snapshotRoot;
    private final long spinLockTimeout;
    private final int sqlBindVariablePoolSize;
    private final int sqlCharacterStoreCapacity;
    private final int sqlCharacterStoreSequencePoolCapacity;
    private final int sqlColumnCastModelPoolCapacity;
    private final int sqlColumnPoolCapacity;
    private final double sqlCompactMapLoadFactor;
    private final int sqlCompilerPoolCapacity;
    private final int sqlCopyBufferSize;
    private final int sqlCopyModelPoolCapacity;
    private final int sqlCountDistinctCapacity;
    private final double sqlCountDistinctLoadFactor;
    private final int sqlCreateTableModelPoolCapacity;
    private final int sqlDistinctTimestampKeyCapacity;
    private final double sqlDistinctTimestampLoadFactor;
    private final int sqlDoubleToStrCastScale;
    private final int sqlExplainModelPoolCapacity;
    private final int sqlExpressionPoolCapacity;
    private final double sqlFastMapLoadFactor;
    private final int sqlFloatToStrCastScale;
    private final int sqlGroupByMapCapacity;
    private final int sqlGroupByPoolCapacity;
    private final int sqlHashJoinLightValueMaxPages;
    private final int sqlHashJoinLightValuePageSize;
    private final int sqlHashJoinValueMaxPages;
    private final int sqlHashJoinValuePageSize;
    private final int sqlInsertModelPoolCapacity;
    private final int sqlJitBindVarsMemoryMaxPages;
    private final int sqlJitBindVarsMemoryPageSize;
    private final boolean sqlJitDebugEnabled;
    private final int sqlJitIRMemoryMaxPages;
    private final int sqlJitIRMemoryPageSize;
    private final int sqlJitMode;
    private final int sqlJitPageAddressCacheThreshold;
    private final int sqlJoinContextPoolCapacity;
    private final int sqlJoinMetadataMaxResizes;
    private final int sqlJoinMetadataPageSize;
    private final long sqlLatestByRowCount;
    private final int sqlLexerPoolCapacity;
    private final int sqlMapMaxPages;
    private final int sqlMapMaxResizes;
    private final int sqlMaxNegativeLimit;
    private final int sqlMaxSymbolNotEqualsCount;
    private final int sqlModelPoolCapacity;
    private final int sqlPageFrameMaxRows;
    private final int sqlPageFrameMinRows;
    private final boolean sqlParallelFilterEnabled;
    private final boolean sqlParallelFilterPreTouchEnabled;
    private final boolean sqlParallelGroupByEnabled;
    private final int sqlRenameTableModelPoolCapacity;
    private final int sqlSmallMapKeyCapacity;
    private final int sqlSmallMapPageSize;
    private final int sqlSortKeyMaxPages;
    private final long sqlSortKeyPageSize;
    private final int sqlSortLightValueMaxPages;
    private final long sqlSortLightValuePageSize;
    private final int sqlSortValueMaxPages;
    private final int sqlSortValuePageSize;
    private final int sqlStrFunctionBufferMaxSize;
    private final int sqlTxnScoreboardEntryCount;
    private final int sqlWindowColumnPoolCapacity;
    private final int sqlWindowInitialRangeBufferSize;
    private final int sqlWindowMaxRecursion;
    private final int sqlWindowRowIdMaxPages;
    private final int sqlWindowRowIdPageSize;
    private final int sqlWindowStoreMaxPages;
    private final int sqlWindowStorePageSize;
    private final int sqlWindowTreeKeyMaxPages;
    private final int sqlWindowTreeKeyPageSize;
    private final int sqlWithClauseModelPoolCapacity;
    private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new PropStaticContentProcessorConfiguration();
    private final int systemO3ColumnMemorySize;
    private final String systemTableNamePrefix;
    private final long systemWalWriterDataAppendPageSize;
    private final long systemWriterDataAppendPageSize;
    private final boolean tableTypeConversionEnabled;
    private final TelemetryConfiguration telemetryConfiguration = new PropTelemetryConfiguration();
    private final boolean telemetryDisableCompletely;
    private final boolean telemetryEnabled;
    private final boolean telemetryHideTables;
    private final int telemetryQueueCapacity;
    private final CharSequence tempRenamePendingTablePrefix;
    private final int textAnalysisMaxLines;
    private final TextConfiguration textConfiguration = new PropTextConfiguration();
    private final int textLexerStringPoolCapacity;
    private final int timestampAdapterPoolCapacity;
    private final int utf8SinkSize;
    private final PropertyValidator validator;
    private final int vectorAggregateQueueCapacity;
    private final VolumeDefinitions volumeDefinitions = new VolumeDefinitions();
    private final boolean walApplyEnabled;
    private final int walApplyLookAheadTransactionCount;
    private final WorkerPoolConfiguration walApplyPoolConfiguration = new PropWalApplyPoolConfiguration();
    private final long walApplySleepTimeout;
    private final long walApplyTableTimeQuota;
    private final int[] walApplyWorkerAffinity;
    private final int walApplyWorkerCount;
    private final boolean walApplyWorkerHaltOnError;
    private final long walApplyWorkerSleepThreshold;
    private final long walApplyWorkerYieldThreshold;
    private final boolean walEnabledDefault;
    private final long walMaxLagSize;
    private final int walMaxLagTxnCount;
    private final int walMaxSegmentFileDescriptorsCache;
    private final long walPurgeInterval;
    private final int walPurgeWaitBeforeDelete;
    private final int walRecreateDistressedSequencerAttempts;
    private final long walSegmentRolloverRowCount;
    private final long walSegmentRolloverSize;
    private final double walSquashUncommittedRowsMultiplier;
    private final boolean walSupported;
    private final int walTxnNotificationQueueCapacity;
    private final long walWriterDataAppendPageSize;
    private final int walWriterPoolMaxSegments;
    private final long workStealTimeoutNanos;
    private final long writerAsyncCommandBusyWaitTimeout;
    private final long writerAsyncCommandMaxWaitTimeout;
    private final int writerAsyncCommandQueueCapacity;
    private final long writerAsyncCommandQueueSlotSize;
    private final long writerDataAppendPageSize;
    private final long writerDataIndexKeyAppendPageSize;
    private final long writerDataIndexValueAppendPageSize;
    private final long writerFileOpenOpts;
    private final long writerMemoryLimit;
    private final long writerMiscAppendPageSize;
    private final boolean writerMixedIOEnabled;
    private final int writerTickRowsCountMod;
    private long cairoSqlCopyMaxIndexChunkSize;
    private FactoryProvider factoryProvider;
    private short floatDefaultColumnType;
    private int forceRecvFragmentationChunkSize;
    private int forceSendFragmentationChunkSize;
    private int httpMinBindIPv4Address;
    private int httpMinBindPort;
    private boolean httpMinNetConnectionHint;
    private int httpMinNetConnectionLimit;
    private long httpMinNetConnectionQueueTimeout;
    private int httpMinNetConnectionRcvBuf;
    private int httpMinNetConnectionSndBuf;
    private long httpMinNetConnectionTimeout;
    private int[] httpMinWorkerAffinity;
    private int httpMinWorkerCount;
    private boolean httpMinWorkerHaltOnError;
    private long httpMinWorkerSleepThreshold;
    private long httpMinWorkerSleepTimeout;
    private long httpMinWorkerYieldThreshold;
    private int httpNetBindIPv4Address;
    private int httpNetBindPort;
    private int httpNetConnectionLimit;
    private long httpNetConnectionQueueTimeout;
    private int httpNetConnectionRcvBuf;
    private int httpNetConnectionSndBuf;
    private long httpNetConnectionTimeout;
    private String httpVersion;
    private short integerDefaultColumnType;
    private int jsonQueryConnectionCheckFrequency;
    private int jsonQueryDoubleScale;
    private int jsonQueryFloatScale;
    private long lineTcpCommitIntervalDefault;
    private double lineTcpCommitIntervalFraction;
    private int lineTcpConnectionPoolInitialCapacity;
    private int lineTcpDefaultPartitionBy;
    private boolean lineTcpDisconnectOnError;
    private int[] lineTcpIOWorkerAffinity;
    private int lineTcpIOWorkerCount;
    private boolean lineTcpIOWorkerPoolHaltOnError;
    private long lineTcpIOWorkerSleepThreshold;
    private long lineTcpIOWorkerYieldThreshold;
    private long lineTcpMaintenanceInterval;
    private int lineTcpMaxMeasurementSize;
    private int lineTcpMsgBufferSize;
    private int lineTcpNetBindIPv4Address;
    private int lineTcpNetBindPort;
    private long lineTcpNetConnectionHeartbeatInterval;
    private boolean lineTcpNetConnectionHint;
    private int lineTcpNetConnectionLimit;
    private long lineTcpNetConnectionQueueTimeout;
    private int lineTcpNetConnectionRcvBuf;
    private long lineTcpNetConnectionTimeout;
    private LineTcpTimestampAdapter lineTcpTimestampAdapter;
    private int lineTcpWriterQueueCapacity;
    private int[] lineTcpWriterWorkerAffinity;
    private int lineTcpWriterWorkerCount;
    private boolean lineTcpWriterWorkerPoolHaltOnError;
    private long lineTcpWriterWorkerSleepThreshold;
    private long lineTcpWriterWorkerYieldThreshold;
    private int lineUdpBindIPV4Address;
    private int lineUdpDefaultPartitionBy;
    private int lineUdpPort;
    private MimeTypesCache mimeTypesCache;
    private long minIdleMsBeforeWriterRelease;
    private int netTestConnectionBufferSize;
    private int pgBinaryParamsCapacity;
    private int pgCharacterStoreCapacity;
    private int pgCharacterStorePoolCapacity;
    private int pgConnectionPoolInitialCapacity;
    private boolean pgDaemonPool;
    private DateLocale pgDefaultLocale;
    private boolean pgHaltOnError;
    private int pgInsertCacheBlockCount;
    private boolean pgInsertCacheEnabled;
    private int pgInsertCacheRowCount;
    private int pgMaxBlobSizeOnQuery;
    private int pgNamedStatementCacheCapacity;
    private int pgNamesStatementPoolCapacity;
    private int pgNetBindIPv4Address;
    private int pgNetBindPort;
    private boolean pgNetConnectionHint;
    private int pgNetConnectionLimit;
    private long pgNetConnectionQueueTimeout;
    private int pgNetConnectionRcvBuf;
    private int pgNetConnectionSndBuf;
    private long pgNetIdleConnectionTimeout;
    private String pgPassword;
    private int pgPendingWritersCacheCapacity;
    private String pgReadOnlyPassword;
    private boolean pgReadOnlySecurityContext;
    private boolean pgReadOnlyUserEnabled;
    private String pgReadOnlyUsername;
    private int pgRecvBufferSize;
    private int pgSelectCacheBlockCount;
    private boolean pgSelectCacheEnabled;
    private int pgSelectCacheRowCount;
    private int pgSendBufferSize;
    private int pgUpdateCacheBlockCount;
    private boolean pgUpdateCacheEnabled;
    private int pgUpdateCacheRowCount;
    private String pgUsername;
    private int[] pgWorkerAffinity;
    private int pgWorkerCount;
    private long pgWorkerSleepThreshold;
    private long pgWorkerYieldThreshold;
    private boolean stringAsTagSupported;
    private boolean stringToCharCastAllowed;
    private boolean symbolAsFieldSupported;
    private long symbolCacheWaitUsBeforeReload;

    public PropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        this(
                root,
                properties,
                env,
                log,
                buildInformation,
                FilesFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                (configuration, engine, freeOnExitList) -> DefaultFactoryProvider.INSTANCE
        );
    }

    public PropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf
    ) throws ServerConfigurationException, JsonException {
        this.log = log;
        this.filesFacade = filesFacade;
        this.fpf = fpf;
        this.microsecondClock = microsecondClock;
        this.validator = newValidator();
        boolean configValidationStrict = getBoolean(properties, env, PropertyKey.CONFIG_VALIDATION_STRICT, false);
        validateProperties(properties, configValidationStrict);

        this.writerMemoryLimit = getLongSize(properties, env, PropertyKey.WRITER_MEMORY_LIMIT, 0);
        this.isReadOnlyInstance = getBoolean(properties, env, PropertyKey.READ_ONLY_INSTANCE, false);
        this.cairoTableRegistryAutoReloadFrequency = getLong(properties, env, PropertyKey.CAIRO_TABLE_REGISTRY_AUTO_RELOAD_FREQUENCY, 500);
        this.cairoTableRegistryCompactionThreshold = getInt(properties, env, PropertyKey.CAIRO_TABLE_REGISTRY_COMPACTION_THRESHOLD, 30);
        this.repeatMigrationFromVersion = getInt(properties, env, PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, 426);
        this.mkdirMode = getInt(properties, env, PropertyKey.CAIRO_MKDIR_MODE, 509);
        this.maxFileNameLength = getInt(properties, env, PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH, 127);
        // changing the default value of walEnabledDefault to true would mean that QuestDB instances upgraded from
        // a pre-WAL version suddenly would start to create WAL tables by default, this could come as a surprise to users
        // instead cairo.wal.enabled.default=true is added to the config, so only new QuestDB installations have WAL enabled by default
        this.walEnabledDefault = getBoolean(properties, env, PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, false);
        this.walPurgeInterval = getLong(properties, env, PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 30_000);
        this.walPurgeWaitBeforeDelete = getInt(properties, env, PropertyKey.DEBUG_WAL_PURGE_WAIT_BEFORE_DELETE, 0);
        this.walTxnNotificationQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY, 4096);
        this.walRecreateDistressedSequencerAttempts = getInt(properties, env, PropertyKey.CAIRO_WAL_RECREATE_DISTRESSED_SEQUENCER_ATTEMPTS, 3);
        this.walSupported = getBoolean(properties, env, PropertyKey.CAIRO_WAL_SUPPORTED, true);
        walApplyEnabled = getBoolean(properties, env, PropertyKey.CAIRO_WAL_APPLY_ENABLED, true);
        this.walSegmentRolloverRowCount = getLong(properties, env, PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 200_000);
        this.walSegmentRolloverSize = getLong(properties, env, PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_SIZE, 0);  // disabled by default.
        if ((this.walSegmentRolloverSize != 0) && (this.walSegmentRolloverSize < 1024)) {  // 1KiB segments minimum
            throw CairoException.critical(0).put("cairo.wal.segment.rollover.size must be 0 (disabled) or >= 1024 (1KiB)");
        }
        this.walWriterDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, Numbers.SIZE_1MB));
        this.systemWalWriterDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_SYSTEM_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 256 * 1024));
        this.walSquashUncommittedRowsMultiplier = getDouble(properties, env, PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, "20.0");
        this.walMaxLagTxnCount = getInt(properties, env, PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        this.walMaxLagSize = getLongSize(properties, env, PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 75 * Numbers.SIZE_1MB);
        this.walMaxSegmentFileDescriptorsCache = getInt(properties, env, PropertyKey.CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE, 30);
        this.walApplyTableTimeQuota = getLong(properties, env, PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 1000);
        this.walApplyLookAheadTransactionCount = getInt(properties, env, PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 20);
        this.tableTypeConversionEnabled = getBoolean(properties, env, PropertyKey.TABLE_TYPE_CONVERSION_ENABLED, true);
        this.tempRenamePendingTablePrefix = getString(properties, env, PropertyKey.CAIRO_WAL_TEMP_PENDING_RENAME_TABLE_PREFIX, "temp_5822f658-31f6-11ee-be56-0242ac120002");
        if (tempRenamePendingTablePrefix.length() > maxFileNameLength - 4) {
            throw CairoException.critical(0).put("Temp pending table prefix is too long [")
                    .put(PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH.toString()).put("=")
                    .put(maxFileNameLength).put(", ")
                    .put(PropertyKey.CAIRO_WAL_TEMP_PENDING_RENAME_TABLE_PREFIX.toString()).put("=")
                    .put(tempRenamePendingTablePrefix).put(']');
        }
        if (!TableUtils.isValidTableName(tempRenamePendingTablePrefix, maxFileNameLength)) {
            throw CairoException.critical(0).put("Invalid temp pending table prefix [")
                    .put(PropertyKey.CAIRO_WAL_TEMP_PENDING_RENAME_TABLE_PREFIX.toString()).put("=")
                    .put(tempRenamePendingTablePrefix).put(']');
        }

        this.dbDirectory = getString(properties, env, PropertyKey.CAIRO_ROOT, DB_DIRECTORY);
        String tmpRoot;
        if (new File(this.dbDirectory).isAbsolute()) {
            this.root = this.dbDirectory;
            this.confRoot = rootSubdir(this.root, CONFIG_DIRECTORY); // ../conf
            this.snapshotRoot = rootSubdir(this.root, SNAPSHOT_DIRECTORY); // ../snapshot
            tmpRoot = rootSubdir(this.root, TMP_DIRECTORY); // ../tmp
        } else {
            this.root = new File(root, this.dbDirectory).getAbsolutePath();
            this.confRoot = new File(root, CONFIG_DIRECTORY).getAbsolutePath();
            this.snapshotRoot = new File(root, SNAPSHOT_DIRECTORY).getAbsolutePath();
            tmpRoot = new File(root, TMP_DIRECTORY).getAbsolutePath();
        }

        this.cairoAttachPartitionSuffix = getString(properties, env, PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX, TableUtils.ATTACHABLE_DIR_MARKER);
        this.cairoAttachPartitionCopy = getBoolean(properties, env, PropertyKey.CAIRO_ATTACH_PARTITION_COPY, false);

        this.snapshotInstanceId = getString(properties, env, PropertyKey.CAIRO_SNAPSHOT_INSTANCE_ID, "");
        this.snapshotRecoveryEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SNAPSHOT_RECOVERY_ENABLED, true);
        this.simulateCrashEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SIMULATE_CRASH_ENABLED, false);

        int cpuAvailable = Runtime.getRuntime().availableProcessors();
        int cpuUsed = 0;
        int cpuSpare = 0;
        if (cpuAvailable > 16) {
            cpuSpare = 2;
        } else if (cpuAvailable > 8) {
            cpuSpare = 1;
        }

        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        try (Path path = new Path()) {
            volumeDefinitions.of(getString(properties, env, PropertyKey.CAIRO_VOLUMES, null), path, root);
            ff.mkdirs(path.of(this.root).slash$(), this.mkdirMode);
            path.of(this.root).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
            final int tableIndexFd = TableUtils.openFileRWOrFail(ff, path, CairoConfiguration.O_NONE);
            final long fileSize = ff.length(tableIndexFd);
            if (fileSize < Long.BYTES) {
                if (!ff.allocate(tableIndexFd, Files.PAGE_SIZE)) {
                    ff.close(tableIndexFd);
                    throw CairoException.critical(ff.errno()).put("Could not allocate [file=").put(path).put(", actual=").put(fileSize).put(", desired=").put(Files.PAGE_SIZE).put(']');
                }
            }

            final long tableIndexMem = TableUtils.mapRWOrClose(ff, tableIndexFd, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
            Rnd rnd = new Rnd(cairoConfiguration.getMicrosecondClock().getTicks(), cairoConfiguration.getMillisecondClock().getTicks());
            if (Os.compareAndSwap(tableIndexMem + Long.BYTES, 0, rnd.nextLong()) == 0) {
                Unsafe.getUnsafe().putLong(tableIndexMem + Long.BYTES * 2, rnd.nextLong());
            }
            this.instanceHashLo = Unsafe.getUnsafe().getLong(tableIndexMem + Long.BYTES);
            this.instanceHashHi = Unsafe.getUnsafe().getLong(tableIndexMem + Long.BYTES * 2);
            ff.munmap(tableIndexMem, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
            ff.close(tableIndexFd);

            this.httpMinServerEnabled = getBoolean(properties, env, PropertyKey.HTTP_MIN_ENABLED, true);
            if (httpMinServerEnabled) {
                this.httpMinWorkerHaltOnError = getBoolean(properties, env, PropertyKey.HTTP_MIN_WORKER_HALT_ON_ERROR, false);
                this.httpMinWorkerCount = getInt(properties, env, PropertyKey.HTTP_MIN_WORKER_COUNT, 1);
                this.httpMinWorkerAffinity = getAffinity(properties, env, PropertyKey.HTTP_MIN_WORKER_AFFINITY, httpMinWorkerCount);
                this.httpMinWorkerYieldThreshold = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_YIELD_THRESHOLD, 10);
                this.httpMinWorkerSleepThreshold = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_SLEEP_THRESHOLD, 100);
                this.httpMinWorkerSleepTimeout = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_SLEEP_TIMEOUT, 50);

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

            this.httpRecvBufferSize = getIntSize(properties, env, PropertyKey.HTTP_RECEIVE_BUFFER_SIZE, Numbers.SIZE_1MB);
            this.requestHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_REQUEST_HEADER_BUFFER_SIZE, 32 * 2014);
            this.httpSendBufferSize = getIntSize(properties, env, PropertyKey.HTTP_SEND_BUFFER_SIZE, 2 * Numbers.SIZE_1MB);
            if (httpSendBufferSize < HttpServerConfiguration.MIN_SEND_BUFFER_SIZE) {
                throw new ServerConfigurationException("invalid configuration value [key=" + PropertyKey.HTTP_SEND_BUFFER_SIZE.getPropertyPath() +
                        ", description=http response send buffer should be at least " + HttpServerConfiguration.MIN_SEND_BUFFER_SIZE + " bytes]");
            }
            this.httpServerEnabled = getBoolean(properties, env, PropertyKey.HTTP_ENABLED, true);
            this.connectionStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_CONNECTION_STRING_POOL_CAPACITY, 128);
            this.connectionPoolInitialCapacity = getInt(properties, env, PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY, 4);
            this.multipartHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MULTIPART_HEADER_BUFFER_SIZE, 512);
            this.multipartIdleSpinCount = getLong(properties, env, PropertyKey.HTTP_MULTIPART_IDLE_SPIN_COUNT, 10_000);
            this.httpWorkerCount = getInt(properties, env, PropertyKey.HTTP_WORKER_COUNT, 0);
            cpuUsed += this.httpWorkerCount;
            this.httpWorkerAffinity = getAffinity(properties, env, PropertyKey.HTTP_WORKER_AFFINITY, httpWorkerCount);
            this.httpWorkerHaltOnError = getBoolean(properties, env, PropertyKey.HTTP_WORKER_HALT_ON_ERROR, false);
            this.httpWorkerYieldThreshold = getLong(properties, env, PropertyKey.HTTP_WORKER_YIELD_THRESHOLD, 10);
            this.httpWorkerSleepThreshold = getLong(properties, env, PropertyKey.HTTP_WORKER_SLEEP_THRESHOLD, 10_000);
            this.httpWorkerSleepTimeout = getLong(properties, env, PropertyKey.HTTP_WORKER_SLEEP_TIMEOUT, 10);
            this.indexFileName = getString(properties, env, PropertyKey.HTTP_STATIC_INDEX_FILE_NAME, "index.html");
            this.httpStaticAuthRequired = getBoolean(properties, env, PropertyKey.HTTP_STATIC_AUTHENTICATION_REQUIRED, true);
            this.httpFrozenClock = getBoolean(properties, env, PropertyKey.HTTP_FROZEN_CLOCK, false);
            this.httpAllowDeflateBeforeSend = getBoolean(properties, env, PropertyKey.HTTP_ALLOW_DEFLATE_BEFORE_SEND, false);
            this.httpServerKeepAlive = getBoolean(properties, env, PropertyKey.HTTP_SERVER_KEEP_ALIVE, true);
            this.httpServerCookiesEnabled = getBoolean(properties, env, PropertyKey.HTTP_SERVER_KEEP_ALIVE, true);
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
            this.httpNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_NET_ACTIVE_CONNECTION_LIMIT, 64);
            this.httpNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_NET_CONNECTION_LIMIT, this.httpNetConnectionLimit);
            this.httpNetConnectionHint = getBoolean(properties, env, PropertyKey.HTTP_NET_CONNECTION_HINT, false);
            // deprecated
            this.httpNetConnectionTimeout = getLong(properties, env, PropertyKey.HTTP_NET_IDLE_CONNECTION_TIMEOUT, 5 * 60 * 1000L);
            this.httpNetConnectionTimeout = getLong(properties, env, PropertyKey.HTTP_NET_CONNECTION_TIMEOUT, this.httpNetConnectionTimeout);

            // deprecated
            this.httpNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.HTTP_NET_QUEUED_CONNECTION_TIMEOUT, 5 * 1000L);
            this.httpNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.HTTP_NET_CONNECTION_QUEUE_TIMEOUT, this.httpNetConnectionQueueTimeout);

            // deprecated
            this.httpNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_SND_BUF_SIZE, 2 * Numbers.SIZE_1MB);
            this.httpNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_CONNECTION_SNDBUF, this.httpNetConnectionSndBuf);

            // deprecated
            this.httpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_RCV_BUF_SIZE, 2 * Numbers.SIZE_1MB);
            this.httpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_CONNECTION_RCVBUF, this.httpNetConnectionRcvBuf);

            this.dateAdapterPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_DATE_ADAPTER_POOL_CAPACITY, 16);
            this.jsonCacheLimit = getIntSize(properties, env, PropertyKey.HTTP_TEXT_JSON_CACHE_LIMIT, 16384);
            this.jsonCacheSize = getIntSize(properties, env, PropertyKey.HTTP_TEXT_JSON_CACHE_SIZE, 8192);
            this.maxRequiredDelimiterStdDev = getDouble(properties, env, PropertyKey.HTTP_TEXT_MAX_REQUIRED_DELIMITER_STDDEV, "0.1222");
            this.maxRequiredLineLengthStdDev = getDouble(properties, env, PropertyKey.HTTP_TEXT_MAX_REQUIRED_LINE_LENGTH_STDDEV, "0.8");
            this.metadataStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_METADATA_STRING_POOL_CAPACITY, 128);

            this.rollBufferLimit = getIntSize(properties, env, PropertyKey.HTTP_TEXT_ROLL_BUFFER_LIMIT, 1024 * 4096);
            this.rollBufferSize = getIntSize(properties, env, PropertyKey.HTTP_TEXT_ROLL_BUFFER_SIZE, 1024);
            this.textAnalysisMaxLines = getInt(properties, env, PropertyKey.HTTP_TEXT_ANALYSIS_MAX_LINES, 1000);
            this.textLexerStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_LEXER_STRING_POOL_CAPACITY, 64);
            this.timestampAdapterPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_TIMESTAMP_ADAPTER_POOL_CAPACITY, 64);
            this.utf8SinkSize = getIntSize(properties, env, PropertyKey.HTTP_TEXT_UTF8_SINK_SIZE, 4096);

            this.httpPessimisticHealthCheckEnabled = getBoolean(properties, env, PropertyKey.HTTP_PESSIMISTIC_HEALTH_CHECK, false);
            this.httpHealthCheckAuthRequired = getBoolean(properties, env, PropertyKey.HTTP_HEALTH_CHECK_AUTHENTICATION_REQUIRED, true);
            this.httpReadOnlySecurityContext = getBoolean(properties, env, PropertyKey.HTTP_SECURITY_READONLY, false);
            this.maxHttpQueryResponseRowLimit = getLong(properties, env, PropertyKey.HTTP_SECURITY_MAX_RESPONSE_ROWS, Long.MAX_VALUE);
            this.interruptOnClosedConnection = getBoolean(properties, env, PropertyKey.HTTP_SECURITY_INTERRUPT_ON_CLOSED_CONNECTION, true);

            if (httpServerEnabled) {
                this.jsonQueryConnectionCheckFrequency = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_CONNECTION_CHECK_FREQUENCY, 1_000_000);
                this.jsonQueryFloatScale = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_FLOAT_SCALE, 4);
                this.jsonQueryDoubleScale = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_DOUBLE_SCALE, 12);
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
            this.rerunExponentialWaitMultiplier = getDouble(properties, env, PropertyKey.HTTP_BUSY_RETRY_EXPONENTIAL_WAIT_MULTIPLIER, "2.0");
            this.rerunInitialWaitQueueSize = getIntSize(properties, env, PropertyKey.HTTP_BUSY_RETRY_INITIAL_WAIT_QUEUE_SIZE, 64);
            this.rerunMaxProcessingQueueSize = getIntSize(properties, env, PropertyKey.HTTP_BUSY_RETRY_MAX_PROCESSING_QUEUE_SIZE, 4096);

            this.circuitBreakerThrottle = getInt(properties, env, PropertyKey.CIRCUIT_BREAKER_THROTTLE, 2_000_000);
            this.queryTimeout = (long) (getDouble(properties, env, PropertyKey.QUERY_TIMEOUT_SEC, "60") * Timestamps.SECOND_MILLIS);
            this.netTestConnectionBufferSize = getInt(properties, env, PropertyKey.CIRCUIT_BREAKER_BUFFER_SIZE, 64);
            this.netTestConnectionBufferSize = getInt(properties, env, PropertyKey.NET_TEST_CONNECTION_BUFFER_SIZE, netTestConnectionBufferSize);

            this.pgEnabled = getBoolean(properties, env, PropertyKey.PG_ENABLED, true);
            if (pgEnabled) {
                // deprecated
                pgNetConnectionLimit = getInt(properties, env, PropertyKey.PG_NET_ACTIVE_CONNECTION_LIMIT, 64);
                pgNetConnectionLimit = getInt(properties, env, PropertyKey.PG_NET_CONNECTION_LIMIT, pgNetConnectionLimit);
                pgNetConnectionHint = getBoolean(properties, env, PropertyKey.PG_NET_CONNECTION_HINT, false);
                parseBindTo(properties, env, PropertyKey.PG_NET_BIND_TO, "0.0.0.0:8812", (a, p) -> {
                    pgNetBindIPv4Address = a;
                    pgNetBindPort = p;
                });

                // deprecated
                this.pgNetIdleConnectionTimeout = getLong(properties, env, PropertyKey.PG_NET_IDLE_TIMEOUT, 300_000);
                this.pgNetIdleConnectionTimeout = getLong(properties, env, PropertyKey.PG_NET_CONNECTION_TIMEOUT, this.pgNetIdleConnectionTimeout);
                this.pgNetConnectionQueueTimeout = getLong(properties, env, PropertyKey.PG_NET_CONNECTION_QUEUE_TIMEOUT, 300_000);

                // deprecated
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.PG_NET_RECV_BUF_SIZE, -1);
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.PG_NET_CONNECTION_RCVBUF, this.pgNetConnectionRcvBuf);

                // deprecated
                this.pgNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.PG_NET_SEND_BUF_SIZE, -1);
                this.pgNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.PG_NET_CONNECTION_SNDBUF, this.pgNetConnectionSndBuf);

                this.pgCharacterStoreCapacity = getInt(properties, env, PropertyKey.PG_CHARACTER_STORE_CAPACITY, 4096);
                this.pgBinaryParamsCapacity = getInt(properties, env, PropertyKey.PG_BINARY_PARAM_COUNT_CAPACITY, 2);
                this.pgCharacterStorePoolCapacity = getInt(properties, env, PropertyKey.PG_CHARACTER_STORE_POOL_CAPACITY, 64);
                this.pgConnectionPoolInitialCapacity = getInt(properties, env, PropertyKey.PG_CONNECTION_POOL_CAPACITY, 4);
                this.pgPassword = getString(properties, env, PropertyKey.PG_PASSWORD, "quest");
                this.pgUsername = getString(properties, env, PropertyKey.PG_USER, "admin");
                this.pgReadOnlyPassword = getString(properties, env, PropertyKey.PG_RO_PASSWORD, "quest");
                this.pgReadOnlyUsername = getString(properties, env, PropertyKey.PG_RO_USER, "user");
                this.pgReadOnlyUserEnabled = getBoolean(properties, env, PropertyKey.PG_RO_USER_ENABLED, false);
                this.pgReadOnlySecurityContext = getBoolean(properties, env, PropertyKey.PG_SECURITY_READONLY, false);
                this.pgMaxBlobSizeOnQuery = getIntSize(properties, env, PropertyKey.PG_MAX_BLOB_SIZE_ON_QUERY, 512 * 1024);
                this.pgRecvBufferSize = getIntSize(properties, env, PropertyKey.PG_RECV_BUFFER_SIZE, Numbers.SIZE_1MB);
                this.pgSendBufferSize = getIntSize(properties, env, PropertyKey.PG_SEND_BUFFER_SIZE, Numbers.SIZE_1MB);
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
                this.pgWorkerSleepThreshold = getLong(properties, env, PropertyKey.PG_WORKER_SLEEP_THRESHOLD, 10_000);
                this.pgDaemonPool = getBoolean(properties, env, PropertyKey.PG_DAEMON_POOL, true);
                this.pgSelectCacheEnabled = getBoolean(properties, env, PropertyKey.PG_SELECT_CACHE_ENABLED, true);
                this.pgSelectCacheBlockCount = getInt(properties, env, PropertyKey.PG_SELECT_CACHE_BLOCK_COUNT, 4);
                this.pgSelectCacheRowCount = getInt(properties, env, PropertyKey.PG_SELECT_CACHE_ROW_COUNT, 4);
                this.pgInsertCacheEnabled = getBoolean(properties, env, PropertyKey.PG_INSERT_CACHE_ENABLED, true);
                this.pgInsertCacheBlockCount = getInt(properties, env, PropertyKey.PG_INSERT_CACHE_BLOCK_COUNT, 4);
                this.pgInsertCacheRowCount = getInt(properties, env, PropertyKey.PG_INSERT_CACHE_ROW_COUNT, 4);
                this.pgUpdateCacheEnabled = getBoolean(properties, env, PropertyKey.PG_UPDATE_CACHE_ENABLED, true);
                this.pgUpdateCacheBlockCount = getInt(properties, env, PropertyKey.PG_UPDATE_CACHE_BLOCK_COUNT, 4);
                this.pgUpdateCacheRowCount = getInt(properties, env, PropertyKey.PG_UPDATE_CACHE_ROW_COUNT, 4);
                this.pgNamedStatementCacheCapacity = getInt(properties, env, PropertyKey.PG_NAMED_STATEMENT_CACHE_CAPACITY, 32);
                this.pgNamesStatementPoolCapacity = getInt(properties, env, PropertyKey.PG_NAMED_STATEMENT_POOL_CAPACITY, 32);
                this.pgPendingWritersCacheCapacity = getInt(properties, env, PropertyKey.PG_PENDING_WRITERS_CACHE_CAPACITY, 16);

                this.forceSendFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE, Integer.MAX_VALUE);
                this.forceRecvFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE, Integer.MAX_VALUE);
            }

            int walApplyWorkers = 2;
            if (cpuAvailable > 16) {
                walApplyWorkers = 4;
            } else if (cpuAvailable > 8) {
                walApplyWorkers = 3;
            }
            this.walApplyWorkerCount = getInt(properties, env, PropertyKey.WAL_APPLY_WORKER_COUNT, walApplyWorkers);
            this.walApplyWorkerAffinity = getAffinity(properties, env, PropertyKey.WAL_APPLY_WORKER_AFFINITY, walApplyWorkerCount);
            this.walApplyWorkerHaltOnError = getBoolean(properties, env, PropertyKey.WAL_APPLY_WORKER_HALT_ON_ERROR, false);
            this.walApplyWorkerSleepThreshold = getLong(properties, env, PropertyKey.WAL_APPLY_WORKER_SLEEP_THRESHOLD, 10_000);
            this.walApplySleepTimeout = getLong(properties, env, PropertyKey.WAL_APPLY_WORKER_SLEEP_TIMEOUT, 10);
            this.walApplyWorkerYieldThreshold = getLong(properties, env, PropertyKey.WAL_APPLY_WORKER_YIELD_THRESHOLD, 10);

            this.commitMode = getCommitMode(properties, env, PropertyKey.CAIRO_COMMIT_MODE);
            this.createAsSelectRetryCount = getInt(properties, env, PropertyKey.CAIRO_CREATE_AS_SELECT_RETRY_COUNT, 5);
            this.defaultMapType = getString(properties, env, PropertyKey.CAIRO_DEFAULT_MAP_TYPE, "fast");
            this.defaultSymbolCacheFlag = getBoolean(properties, env, PropertyKey.CAIRO_DEFAULT_SYMBOL_CACHE_FLAG, true);
            this.defaultSymbolCapacity = getInt(properties, env, PropertyKey.CAIRO_DEFAULT_SYMBOL_CAPACITY, 256);
            this.fileOperationRetryCount = getInt(properties, env, PropertyKey.CAIRO_FILE_OPERATION_RETRY_COUNT, 30);
            this.idleCheckInterval = getLong(properties, env, PropertyKey.CAIRO_IDLE_CHECK_INTERVAL, 5 * 60 * 1000L);
            this.inactiveReaderMaxOpenPartitions = getInt(properties, env, PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, 128);
            this.inactiveReaderTTL = getLong(properties, env, PropertyKey.CAIRO_INACTIVE_READER_TTL, 120_000);
            this.inactiveWriterTTL = getLong(properties, env, PropertyKey.CAIRO_INACTIVE_WRITER_TTL, 600_000);
            this.inactiveWalWriterTTL = getLong(properties, env, PropertyKey.CAIRO_WAL_INACTIVE_WRITER_TTL, 120_000);
            this.indexValueBlockSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_INDEX_VALUE_BLOCK_SIZE, 256));
            this.maxSwapFileCount = getInt(properties, env, PropertyKey.CAIRO_MAX_SWAP_FILE_COUNT, 30);
            this.parallelIndexThreshold = getInt(properties, env, PropertyKey.CAIRO_PARALLEL_INDEX_THRESHOLD, 100000);
            this.readerPoolMaxSegments = getInt(properties, env, PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS, 10);
            this.walWriterPoolMaxSegments = getInt(properties, env, PropertyKey.CAIRO_WAL_WRITER_POOL_MAX_SEGMENTS, 10);
            this.spinLockTimeout = getLong(properties, env, PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1_000);
            this.httpSqlCacheEnabled = getBoolean(properties, env, PropertyKey.HTTP_QUERY_CACHE_ENABLED, true);
            this.httpSqlCacheBlockCount = getInt(properties, env, PropertyKey.HTTP_QUERY_CACHE_BLOCK_COUNT, 4);
            this.httpSqlCacheRowCount = getInt(properties, env, PropertyKey.HTTP_QUERY_CACHE_ROW_COUNT, 4);
            this.sqlCharacterStoreCapacity = getInt(properties, env, PropertyKey.CAIRO_CHARACTER_STORE_CAPACITY, 1024);
            this.sqlCharacterStoreSequencePoolCapacity = getInt(properties, env, PropertyKey.CAIRO_CHARACTER_STORE_SEQUENCE_POOL_CAPACITY, 64);
            this.sqlColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_COLUMN_POOL_CAPACITY, 4096);
            this.sqlCompactMapLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_COMPACT_MAP_LOAD_FACTOR, "0.7");
            this.sqlExpressionPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_EXPRESSION_POOL_CAPACITY, 8192);
            this.sqlFastMapLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_FAST_MAP_LOAD_FACTOR, "0.7");
            this.sqlJoinContextPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_JOIN_CONTEXT_POOL_CAPACITY, 64);
            this.sqlLexerPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_LEXER_POOL_CAPACITY, 2048);
            this.sqlSmallMapKeyCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 1024);
            this.sqlSmallMapPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 32 * 1024);
            this.sqlMapMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_MAP_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlMapMaxResizes = getIntSize(properties, env, PropertyKey.CAIRO_SQL_MAP_MAX_RESIZES, Integer.MAX_VALUE);
            this.sqlExplainModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_EXPLAIN_MODEL_POOL_CAPACITY, 32);
            this.sqlModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_MODEL_POOL_CAPACITY, 1024);
            this.sqlMaxNegativeLimit = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 10_000);
            this.sqlSortKeyPageSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 4 * Numbers.SIZE_1MB);
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
            int sqlWindowColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_COLUMN_POOL_CAPACITY, 64);
            this.sqlWindowColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_WINDOW_COLUMN_POOL_CAPACITY, sqlWindowColumnPoolCapacity);
            this.sqlCreateTableModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_CREATE_TABEL_MODEL_POOL_CAPACITY, 16);
            this.sqlColumnCastModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COLUMN_CAST_MODEL_POOL_CAPACITY, 16);
            this.sqlRenameTableModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_RENAME_TABLE_MODEL_POOL_CAPACITY, 16);
            this.sqlWithClauseModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_WITH_CLAUSE_MODEL_POOL_CAPACITY, 128);
            this.sqlInsertModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_INSERT_MODEL_POOL_CAPACITY, 64);
            this.sqlCopyModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COPY_MODEL_POOL_CAPACITY, 32);
            this.sqlCopyBufferSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE, 2 * Numbers.SIZE_1MB);
            this.columnPurgeQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY, 128);
            this.columnPurgeTaskPoolCapacity = getIntSize(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_TASK_POOL_CAPACITY, 256);
            this.columnPurgeRetryDelayLimit = getLong(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 60_000_000L);
            this.columnPurgeRetryDelay = getLong(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 10_000);
            this.columnPurgeRetryDelayMultiplier = getDouble(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_MULTIPLIER, "10.0");
            this.systemTableNamePrefix = getString(properties, env, PropertyKey.CAIRO_SQL_SYSTEM_TABLE_PREFIX, "sys.");

            this.writerDataIndexKeyAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_INDEX_KEY_APPEND_PAGE_SIZE, 512 * 1024));
            this.writerDataIndexValueAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE, 16 * Numbers.SIZE_1MB));
            this.writerDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 16 * Numbers.SIZE_1MB));
            this.systemWriterDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_SYSTEM_WRITER_DATA_APPEND_PAGE_SIZE, 256 * 1024));
            this.writerMiscAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_MISC_APPEND_PAGE_SIZE, Files.PAGE_SIZE));

            this.sampleByIndexSearchPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SAMPLEBY_PAGE_SIZE, 0);
            this.sqlDoubleToStrCastScale = getInt(properties, env, PropertyKey.CAIRO_SQL_DOUBLE_CAST_SCALE, 12);
            this.sqlFloatToStrCastScale = getInt(properties, env, PropertyKey.CAIRO_SQL_FLOAT_CAST_SCALE, 4);
            this.sqlGroupByMapCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_MAP_CAPACITY, 1024);
            this.sqlGroupByPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_POOL_CAPACITY, 1024);
            this.sqlMaxSymbolNotEqualsCount = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_SYMBOL_NOT_EQUALS_COUNT, 100);
            this.sqlBindVariablePoolSize = getInt(properties, env, PropertyKey.CAIRO_SQL_BIND_VARIABLE_POOL_SIZE, 8);
            this.sqlCountDistinctCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COUNT_DISTINCT_CAPACITY, 16);
            this.sqlCountDistinctLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_SQL_COUNT_DISTINCT_LOAD_FACTOR, "0.7");
            final String sqlCopyFormatsFile = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_FORMATS_FILE, "/text_loader.json");
            final String dateLocale = getString(properties, env, PropertyKey.CAIRO_DATE_LOCALE, "en");
            this.locale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
            if (this.locale == null) {
                throw ServerConfigurationException.forInvalidKey(PropertyKey.CAIRO_DATE_LOCALE.getPropertyPath(), dateLocale);
            }
            this.sqlDistinctTimestampKeyCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_DISTINCT_TIMESTAMP_KEY_CAPACITY, 512);
            this.sqlDistinctTimestampLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_SQL_DISTINCT_TIMESTAMP_LOAD_FACTOR, "0.5");
            this.sqlPageFrameMinRows = getInt(properties, env, PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
            this.sqlPageFrameMaxRows = getInt(properties, env, PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);

            this.sqlJitMode = getSqlJitMode(properties, env);
            this.sqlJitIRMemoryPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_IR_MEMORY_PAGE_SIZE, 8 * 1024);
            this.sqlJitIRMemoryMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_JIT_IR_MEMORY_MAX_PAGES, 8);
            this.sqlJitBindVarsMemoryPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE, 4 * 1024);
            this.sqlJitBindVarsMemoryMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_MAX_PAGES, 8);
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
            this.writerFileOpenOpts = lopts;

            this.writerMixedIOEnabled = ff.allowMixedIO(this.root);

            this.inputFormatConfiguration = new InputFormatConfiguration(
                    new DateFormatFactory(),
                    DateLocaleFactory.INSTANCE,
                    new TimestampFormatFactory(),
                    this.locale
            );

            try (JsonLexer lexer = new JsonLexer(1024, 1024)) {
                inputFormatConfiguration.parseConfiguration(PropServerConfiguration.class, lexer, confRoot, sqlCopyFormatsFile);
            }

            this.cairoSqlCopyRoot = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_ROOT, null);
            String cairoSqlCopyWorkRoot = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_WORK_ROOT, tmpRoot);
            if (cairoSqlCopyRoot != null) {
                this.cairoSqlCopyWorkRoot = getCanonicalPath(cairoSqlCopyWorkRoot);
            } else {
                this.cairoSqlCopyWorkRoot = null;
            }

            if (pathEquals(root, this.cairoSqlCopyWorkRoot)
                    || pathEquals(this.root, this.cairoSqlCopyWorkRoot)
                    || pathEquals(this.confRoot, this.cairoSqlCopyWorkRoot)
                    || pathEquals(this.snapshotRoot, this.cairoSqlCopyWorkRoot)) {
                throw new ServerConfigurationException("Configuration value for " + PropertyKey.CAIRO_SQL_COPY_WORK_ROOT.getPropertyPath() + " can't point to root, data, conf or snapshot dirs. ");
            }

            String cairoSQLCopyIdSupplier = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_ID_SUPPLIER, "random");
            this.cairoSQLCopyIdSupplier = Chars.equalsLowerCaseAscii(cairoSQLCopyIdSupplier, "sequential") ? 1 : 0;

            this.cairoSqlCopyMaxIndexChunkSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_COPY_MAX_INDEX_CHUNK_SIZE, 100 * Numbers.SIZE_1MB);
            this.cairoSqlCopyMaxIndexChunkSize -= (cairoSqlCopyMaxIndexChunkSize % CsvFileIndexer.INDEX_ENTRY_SIZE);
            if (this.cairoSqlCopyMaxIndexChunkSize < 16) {
                throw new ServerConfigurationException("invalid configuration value [key=" + PropertyKey.CAIRO_SQL_COPY_MAX_INDEX_CHUNK_SIZE.getPropertyPath() +
                        ", description=max import chunk size can't be smaller than 16]");
            }
            this.cairoSqlCopyQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_COPY_QUEUE_CAPACITY, 32));
            this.cairoSqlCopyLogRetentionDays = getInt(properties, env, PropertyKey.CAIRO_SQL_COPY_LOG_RETENTION_DAYS, 3);
            this.o3MinLagUs = getLong(properties, env, PropertyKey.CAIRO_O3_MIN_LAG, 1_000) * 1_000L;

            this.backupRoot = getString(properties, env, PropertyKey.CAIRO_SQL_BACKUP_ROOT, null);
            this.backupDirTimestampFormat = getTimestampFormat(properties, env);
            this.backupTempDirName = getString(properties, env, PropertyKey.CAIRO_SQL_BACKUP_DIR_TMP_NAME, "tmp");
            this.backupMkdirMode = getInt(properties, env, PropertyKey.CAIRO_SQL_BACKUP_MKDIR_MODE, 509);
            this.detachedMkdirMode = getInt(properties, env, PropertyKey.CAIRO_DETACHED_MKDIR_MODE, 509);
            this.columnIndexerQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_COLUMN_INDEXER_QUEUE_CAPACITY, 64);
            this.vectorAggregateQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_VECTOR_AGGREGATE_QUEUE_CAPACITY, 128);
            this.o3CallbackQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_CALLBACK_QUEUE_CAPACITY, 128);
            this.o3PartitionQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_PARTITION_QUEUE_CAPACITY, 128);
            this.o3OpenColumnQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_OPEN_COLUMN_QUEUE_CAPACITY, 128);
            this.o3CopyQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_COPY_QUEUE_CAPACITY, 128);
            this.o3LagCalculationWindowsSize = getIntSize(properties, env, PropertyKey.CAIRO_O3_LAG_CALCULATION_WINDOW_SIZE, 4);
            this.o3PurgeDiscoveryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_PURGE_DISCOVERY_QUEUE_CAPACITY, 128));
            this.o3ColumnMemorySize = (int) Files.ceilPageSize(getIntSize(properties, env, PropertyKey.CAIRO_O3_COLUMN_MEMORY_SIZE, 8 * Numbers.SIZE_1MB));
            this.systemO3ColumnMemorySize = (int) Files.ceilPageSize(getIntSize(properties, env, PropertyKey.CAIRO_SYSTEM_O3_COLUMN_MEMORY_SIZE, 256 * 1024));
            this.maxUncommittedRows = getInt(properties, env, PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            long o3MaxLag = getLong(properties, env, PropertyKey.CAIRO_COMMIT_LAG, 10 * Dates.MINUTE_MILLIS);
            this.o3MaxLag = getLong(properties, env, PropertyKey.CAIRO_O3_MAX_LAG, o3MaxLag) * 1_000;

            this.o3QuickSortEnabled = getBoolean(properties, env, PropertyKey.CAIRO_O3_QUICKSORT_ENABLED, false);
            this.rndFunctionMemoryPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_RND_MEMORY_PAGE_SIZE, 8192));
            this.rndFunctionMemoryMaxPages = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_RND_MEMORY_MAX_PAGES, 128));
            this.sqlStrFunctionBufferMaxSize = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, Numbers.SIZE_1MB));
            this.sqlWindowMaxRecursion = getInt(properties, env, PropertyKey.CAIRO_SQL_WINDOW_MAX_RECURSION, 128);
            int sqlWindowStorePageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_STORE_PAGE_SIZE, Numbers.SIZE_1MB));
            this.sqlWindowStorePageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, sqlWindowStorePageSize));
            int sqlWindowStoreMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_STORE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlWindowStoreMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, sqlWindowStoreMaxPages);
            int sqlWindowRowIdPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_ROWID_PAGE_SIZE, 512 * 1024));
            this.sqlWindowRowIdPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_WINDOW_ROWID_PAGE_SIZE, sqlWindowRowIdPageSize));
            int sqlWindowRowIdMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_ROWID_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlWindowRowIdMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_PAGES, sqlWindowRowIdMaxPages);
            int sqlWindowTreeKeyPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_TREE_PAGE_SIZE, 512 * 1024));
            this.sqlWindowTreeKeyPageSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_SQL_WINDOW_TREE_PAGE_SIZE, sqlWindowTreeKeyPageSize));
            int sqlWindowTreeKeyMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_TREE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlWindowTreeKeyMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_PAGES, sqlWindowTreeKeyMaxPages);
            this.sqlWindowInitialRangeBufferSize = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_INITIAL_RANGE_BUFFER_SIZE, 32);
            this.sqlTxnScoreboardEntryCount = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, 16384));
            this.latestByQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_LATESTBY_QUEUE_CAPACITY, 32));
            this.telemetryEnabled = getBoolean(properties, env, PropertyKey.TELEMETRY_ENABLED, true);
            this.telemetryDisableCompletely = getBoolean(properties, env, PropertyKey.TELEMETRY_DISABLE_COMPLETELY, false);
            this.telemetryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.TELEMETRY_QUEUE_CAPACITY, 512));
            this.telemetryHideTables = getBoolean(properties, env, PropertyKey.TELEMETRY_HIDE_TABLES, true);
            this.o3PartitionPurgeListCapacity = getInt(properties, env, PropertyKey.CAIRO_O3_PARTITION_PURGE_LIST_INITIAL_CAPACITY, 1);
            this.ioURingEnabled = getBoolean(properties, env, PropertyKey.CAIRO_IO_URING_ENABLED, true);
            this.cairoMaxCrashFiles = getInt(properties, env, PropertyKey.CAIRO_MAX_CRASH_FILES, 100);
            this.o3LastPartitionMaxSplits = Math.max(1, getInt(properties, env, PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20));
            this.o3PartitionSplitMinSize = getLongSize(properties, env, PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 50 * Numbers.SIZE_1MB);

            parseBindTo(properties, env, PropertyKey.LINE_UDP_BIND_TO, "0.0.0.0:9009", (a, p) -> {
                this.lineUdpBindIPV4Address = a;
                this.lineUdpPort = p;
            });

            this.lineUdpGroupIPv4Address = getIPv4Address(properties, env, PropertyKey.LINE_UDP_JOIN, "232.1.2.3");
            this.lineUdpCommitRate = getInt(properties, env, PropertyKey.LINE_UDP_COMMIT_RATE, 1_000_000);
            this.lineUdpMsgBufferSize = getIntSize(properties, env, PropertyKey.LINE_UDP_MSG_BUFFER_SIZE, 2048);
            this.lineUdpMsgCount = getInt(properties, env, PropertyKey.LINE_UDP_MSG_COUNT, 10_000);
            this.lineUdpReceiveBufferSize = getIntSize(properties, env, PropertyKey.LINE_UDP_RECEIVE_BUFFER_SIZE, 8 * Numbers.SIZE_1MB);
            this.lineUdpEnabled = getBoolean(properties, env, PropertyKey.LINE_UDP_ENABLED, false);
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
            this.lineHttpEnabled = getBoolean(properties, env, PropertyKey.LINE_HTTP_ENABLED, true);
            this.lineHttpPingVersion = getString(properties, env, PropertyKey.LINE_HTTP_PING_VERSION, "v2.7.4");
            if (lineTcpEnabled || lineHttpEnabled) {
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

                this.lineTcpMsgBufferSize = getIntSize(properties, env, PropertyKey.LINE_TCP_MSG_BUFFER_SIZE, 32768);
                this.lineTcpMaxMeasurementSize = getIntSize(properties, env, PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE, 32768);
                if (lineTcpMaxMeasurementSize > lineTcpMsgBufferSize) {
                    lineTcpMsgBufferSize = lineTcpMaxMeasurementSize;
                }
                this.lineTcpWriterQueueCapacity = getQueueCapacity(properties, env, PropertyKey.LINE_TCP_WRITER_QUEUE_CAPACITY, 128);
                this.lineTcpWriterWorkerCount = getInt(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_COUNT, 1);
                cpuUsed += this.lineTcpWriterWorkerCount;
                this.lineTcpWriterWorkerAffinity = getAffinity(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_AFFINITY, lineTcpWriterWorkerCount);
                this.lineTcpWriterWorkerPoolHaltOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_WRITER_HALT_ON_ERROR, false);
                this.lineTcpWriterWorkerYieldThreshold = getLong(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_YIELD_THRESHOLD, 10);
                this.lineTcpWriterWorkerSleepThreshold = getLong(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_SLEEP_THRESHOLD, 10_000);
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
                this.lineTcpIOWorkerSleepThreshold = getLong(properties, env, PropertyKey.LINE_TCP_IO_WORKER_SLEEP_THRESHOLD, 10_000);
                this.lineTcpMaintenanceInterval = getLong(properties, env, PropertyKey.LINE_TCP_MAINTENANCE_JOB_INTERVAL, 1000);
                this.lineTcpCommitIntervalFraction = getDouble(properties, env, PropertyKey.LINE_TCP_COMMIT_INTERVAL_FRACTION, "0.5");
                this.lineTcpCommitIntervalDefault = getLong(properties, env, PropertyKey.LINE_TCP_COMMIT_INTERVAL_DEFAULT, COMMIT_INTERVAL_DEFAULT);
                if (this.lineTcpCommitIntervalDefault < 1L) {
                    log.info().$("invalid default commit interval ").$(lineTcpCommitIntervalDefault).$("), will use ").$(COMMIT_INTERVAL_DEFAULT).$();
                    this.lineTcpCommitIntervalDefault = COMMIT_INTERVAL_DEFAULT;
                }
                this.lineTcpAuthDB = getString(properties, env, PropertyKey.LINE_TCP_AUTH_DB_PATH, null);
                // deprecated
                String defaultTcpPartitionByProperty = getString(properties, env, PropertyKey.LINE_TCP_DEFAULT_PARTITION_BY, "DAY");
                defaultTcpPartitionByProperty = getString(properties, env, PropertyKey.LINE_DEFAULT_PARTITION_BY, defaultTcpPartitionByProperty);
                this.lineTcpDefaultPartitionBy = PartitionBy.fromString(defaultTcpPartitionByProperty);
                if (this.lineTcpDefaultPartitionBy == -1) {
                    log.info().$("invalid partition by ").$(defaultTcpPartitionByProperty).$("), will use DAY for TCP").$();
                    this.lineTcpDefaultPartitionBy = PartitionBy.DAY;
                }
                this.minIdleMsBeforeWriterRelease = getLong(properties, env, PropertyKey.LINE_TCP_MIN_IDLE_MS_BEFORE_WRITER_RELEASE, 500);
                this.lineTcpDisconnectOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_DISCONNECT_ON_ERROR, true);
                final long heartbeatInterval = LineTcpReceiverConfigurationHelper.calcCommitInterval(
                        this.o3MinLagUs,
                        this.lineTcpCommitIntervalFraction,
                        this.lineTcpCommitIntervalDefault
                );
                this.lineTcpNetConnectionHeartbeatInterval = getLong(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_HEARTBEAT_INTERVAL, heartbeatInterval);
            } else {
                this.lineTcpAuthDB = null;
            }

            if (lineTcpEnabled || (lineHttpEnabled && httpServerEnabled)) {
                LineTimestampAdapter timestampAdapter = getLineTimestampAdaptor(properties, env, PropertyKey.LINE_TCP_TIMESTAMP);
                this.lineTcpTimestampAdapter = new LineTcpTimestampAdapter(timestampAdapter);
                this.stringToCharCastAllowed = getBoolean(properties, env, PropertyKey.LINE_TCP_UNDOCUMENTED_STRING_TO_CHAR_CAST_ALLOWED, false);
                this.symbolAsFieldSupported = getBoolean(properties, env, PropertyKey.LINE_TCP_UNDOCUMENTED_SYMBOL_AS_FIELD_SUPPORTED, false);
                this.stringAsTagSupported = getBoolean(properties, env, PropertyKey.LINE_TCP_UNDOCUMENTED_STRING_AS_TAG_SUPPORTED, false);
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

            this.ilpAutoCreateNewColumns = getBoolean(properties, env, PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS, true);
            this.ilpAutoCreateNewTables = getBoolean(properties, env, PropertyKey.LINE_AUTO_CREATE_NEW_TABLES, true);

            this.sharedWorkerCount = getInt(properties, env, PropertyKey.SHARED_WORKER_COUNT, Math.max(2, cpuAvailable - cpuSpare - cpuUsed));
            this.sharedWorkerAffinity = getAffinity(properties, env, PropertyKey.SHARED_WORKER_AFFINITY, sharedWorkerCount);
            this.sharedWorkerHaltOnError = getBoolean(properties, env, PropertyKey.SHARED_WORKER_HALT_ON_ERROR, false);
            this.sharedWorkerYieldThreshold = getLong(properties, env, PropertyKey.SHARED_WORKER_YIELD_THRESHOLD, 10);
            this.sharedWorkerSleepThreshold = getLong(properties, env, PropertyKey.SHARED_WORKER_SLEEP_THRESHOLD, 10_000);
            this.sharedWorkerSleepTimeout = getLong(properties, env, PropertyKey.SHARED_WORKER_SLEEP_TIMEOUT, 10);

            this.sqlCompilerPoolCapacity = 2 * (httpWorkerCount + pgWorkerCount + sharedWorkerCount + walApplyWorkerCount);

            final int defaultReduceQueueCapacity = Math.min(2 * sharedWorkerCount, 64);
            this.cairoPageFrameReduceQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, defaultReduceQueueCapacity));
            this.cairoGroupByMergeShardQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUP_BY_MERGE_QUEUE_CAPACITY, defaultReduceQueueCapacity));
            this.cairoGroupByShardingThreshold = getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUP_BY_SHARDING_THRESHOLD, 10000);
            this.cairoGroupByShardCount = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUP_BY_SHARD_COUNT, 64));
            this.cairoPageFrameReduceRowIdListCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 256));
            this.cairoPageFrameReduceColumnListCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_COLUMN_LIST_CAPACITY, 16));
            final int defaultReduceShardCount = Math.min(sharedWorkerCount, 4);
            this.cairoPageFrameReduceShardCount = getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, defaultReduceShardCount);
            this.sqlParallelFilterPreTouchEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_FILTER_PRETOUCH_ENABLED, true);

            boolean defaultParallelSqlEnabled = sharedWorkerCount >= 4;
            this.sqlParallelFilterEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, defaultParallelSqlEnabled);
            this.sqlParallelGroupByEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUP_BY_ENABLED, defaultParallelSqlEnabled);

            this.metricsEnabled = getBoolean(properties, env, PropertyKey.METRICS_ENABLED, false);
            this.writerAsyncCommandBusyWaitTimeout = getLong(properties, env, PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
            this.writerAsyncCommandMaxWaitTimeout = getLong(properties, env, PropertyKey.CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT, 30_000);
            this.writerTickRowsCountMod = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_WRITER_TICK_ROWS_COUNT, 1024)) - 1;
            this.writerAsyncCommandQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 32));
            this.writerAsyncCommandQueueSlotSize = Numbers.ceilPow2(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_SLOT_SIZE, 2048));

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
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(dbRoot, 0, end);
            if (needsSlash) {
                sink.put(Files.SEPARATOR);
            }
            return sink.put(subdir).toString();
        }
        return null;
    }

    @Override
    public CairoConfiguration getCairoConfiguration() {
        return cairoConfiguration;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        if (factoryProvider == null) {
            throw new IllegalStateException("configuration.init() has not been invoked");
        }
        return factoryProvider;
    }

    @Override
    public HttpMinServerConfiguration getHttpMinServerConfiguration() {
        return httpMinServerConfiguration;
    }

    @Override
    public HttpServerConfiguration getHttpServerConfiguration() {
        return httpServerConfiguration;
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return lineTcpReceiverConfiguration;
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return lineUdpReceiverConfiguration;
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return metricsConfiguration;
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return pgWireConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return walApplyPoolConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return sharedWorkerPoolConfiguration;
    }

    @Override
    public void init(CairoEngine engine, FreeOnExit freeOnExit) {
        this.factoryProvider = fpf.getInstance(this, engine, freeOnExit);
    }

    private int[] getAffinity(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, int workerCount) throws ServerConfigurationException {
        final int[] result = new int[workerCount];
        String value = getString(properties, env, key, null);
        if (value == null) {
            Arrays.fill(result, -1);
        } else {
            String[] affinity = value.split(",");
            if (affinity.length != workerCount) {
                throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "wrong number of affinity values");
            }
            for (int i = 0; i < workerCount; i++) {
                try {
                    result[i] = Numbers.parseInt(affinity[i]);
                } catch (NumericException e) {
                    throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "Invalid affinity value: " + affinity[i]);
                }
            }
        }
        return result;
    }

    private int getCommitMode(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key) {
        final String commitMode = getString(properties, env, key, "nosync");

        // must not be null because we provided non-null default value
        assert commitMode != null;

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

    private LineTimestampAdapter getLineTimestampAdaptor(Properties properties, Map<String, String> env, ConfigPropertyKey propNm) {
        final String lineUdpTimestampSwitch = getString(properties, env, propNm, "n");
        switch (lineUdpTimestampSwitch) {
            case "u":
                return LineMicroTimestampAdapter.INSTANCE;
            case "ms":
                return LineMilliTimestampAdapter.INSTANCE;
            case "s":
                return LineSecondTimestampAdapter.INSTANCE;
            case "m":
                return LineMinuteTimestampAdapter.INSTANCE;
            case "h":
                return LineHourTimestampAdapter.INSTANCE;
            default:
                return LineNanoTimestampAdapter.INSTANCE;
        }
    }

    private int getSqlJitMode(Properties properties, @Nullable Map<String, String> env) {
        final String jitMode = getString(properties, env, PropertyKey.CAIRO_SQL_JIT_MODE, "on");

        assert jitMode != null;

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

    private DateFormat getTimestampFormat(Properties properties, @Nullable Map<String, String> env) {
        return compiler.compile(getString(properties, env, PropertyKey.CAIRO_SQL_BACKUP_DIR_DATETIME_FORMAT, "yyyy-MM-dd"));
    }

    private boolean pathEquals(String p1, String p2) {
        try {
            if (p1 == null || p2 == null) {
                return false;
            }
            //unfortunately java.io.Files.isSameFile() doesn't work on files that don't exist
            return new File(p1).getCanonicalPath().replace(File.separatorChar, '/')
                    .equals(new File(p2).getCanonicalPath().replace(File.separatorChar, '/'));
        } catch (IOException e) {
            log.info().$("Can't validate configuration property [key=").$(PropertyKey.CAIRO_SQL_COPY_WORK_ROOT.getPropertyPath())
                    .$(", value=").$(p2).$("]");
            return false;
        }
    }

    private void validateProperties(Properties properties, boolean configValidationStrict) throws ServerConfigurationException {
        ValidationResult validation = validator.validate(properties);
        if (validation != null) {
            if (validation.isError && configValidationStrict) {
                throw new ServerConfigurationException(validation.message);
            } else {
                log.advisory().$(validation.message).$();
            }
        }
    }

    protected boolean getBoolean(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, boolean defaultValue) {
        return Boolean.parseBoolean(getString(properties, env, key, Boolean.toString(defaultValue)));
    }

    String getCanonicalPath(String path) throws ServerConfigurationException {
        try {
            return new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw new ServerConfigurationException("Cannot calculate canonical path for configuration property [key=" + PropertyKey.CAIRO_SQL_COPY_WORK_ROOT.getPropertyPath() + ",value=" + path + "]");
        }
    }

    protected double getDouble(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, String defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, defaultValue);
        try {
            return Numbers.parseDouble(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected int getIPv4Address(Properties properties, Map<String, String> env, ConfigPropertyKey key, String defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, defaultValue);
        try {
            return Net.parseIPv4(value);
        } catch (NetworkError e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected int getInt(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, int defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, Integer.toString(defaultValue));
        try {
            return Numbers.parseInt(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected int getIntSize(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, int defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, Integer.toString(defaultValue));
        try {
            return Numbers.parseIntSize(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected long getLong(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, long defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, Long.toString(defaultValue));
        try {
            return Numbers.parseLong(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected long getLongSize(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, long defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, Long.toString(defaultValue));
        try {
            return Numbers.parseLongSize(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected int getQueueCapacity(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, int defaultValue) throws ServerConfigurationException {
        final int value = getInt(properties, env, key, defaultValue);
        if (!Numbers.isPow2(value)) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "Value must be power of 2, e.g. 1,2,4,8,16,32,64...");
        }
        return value;
    }

    protected String getString(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, String defaultValue) {
        String envCandidate = key.getEnvVarName();
        String result = env != null ? env.get(envCandidate) : null;
        final int valueSource;
        if (result != null) {
            log.info().$("env config [key=").$(envCandidate).I$();
            valueSource = ConfigPropertyValue.VALUE_SOURCE_ENV;
        } else {
            result = properties.getProperty(key.getPropertyPath());
            if (result == null) {
                result = defaultValue;
                valueSource = ConfigPropertyValue.VALUE_SOURCE_DEFAULT;
            } else {
                valueSource = ConfigPropertyValue.VALUE_SOURCE_CONF;
            }
        }

        allPairs.put(key, new ConfigPropertyValueImpl(result, valueSource, false));
        return result;
    }

    protected PropertyValidator newValidator() {
        return new PropertyValidator();
    }

    protected void parseBindTo(
            Properties properties,
            Map<String, String> env,
            ConfigPropertyKey key,
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

    @FunctionalInterface
    protected interface BindToParser {
        void onReady(int address, int port);
    }

    public static class PropertyValidator {
        protected final Map<ConfigPropertyKey, String> deprecatedSettings = new HashMap<>();
        protected final Map<String, String> obsoleteSettings = new HashMap<>();

        public PropertyValidator() {
            registerObsolete(
                    "line.tcp.commit.timeout",
                    PropertyKey.LINE_TCP_COMMIT_INTERVAL_DEFAULT,
                    PropertyKey.LINE_TCP_COMMIT_INTERVAL_FRACTION
            );
            registerObsolete(
                    "cairo.timestamp.locale",
                    PropertyKey.CAIRO_DATE_LOCALE
            );
            registerObsolete(
                    "pg.timestamp.locale",
                    PropertyKey.PG_DATE_LOCALE
            );
            registerObsolete(
                    "cairo.sql.append.page.size",
                    PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE
            );

            registerDeprecated(
                    PropertyKey.HTTP_MIN_BIND_TO,
                    PropertyKey.HTTP_MIN_NET_BIND_TO
            );
            registerDeprecated(
                    PropertyKey.HTTP_MIN_NET_IDLE_CONNECTION_TIMEOUT,
                    PropertyKey.HTTP_MIN_NET_CONNECTION_TIMEOUT
            );
            registerDeprecated(
                    PropertyKey.HTTP_MIN_NET_QUEUED_CONNECTION_TIMEOUT,
                    PropertyKey.HTTP_MIN_NET_CONNECTION_QUEUE_TIMEOUT
            );
            registerDeprecated(
                    PropertyKey.HTTP_MIN_NET_SND_BUF_SIZE,
                    PropertyKey.HTTP_MIN_NET_CONNECTION_SNDBUF
            );
            registerDeprecated(
                    PropertyKey.HTTP_NET_RCV_BUF_SIZE,
                    PropertyKey.HTTP_MIN_NET_CONNECTION_RCVBUF,
                    PropertyKey.HTTP_NET_CONNECTION_RCVBUF
            );
            registerDeprecated(
                    PropertyKey.HTTP_NET_ACTIVE_CONNECTION_LIMIT,
                    PropertyKey.HTTP_NET_CONNECTION_LIMIT
            );
            registerDeprecated(
                    PropertyKey.HTTP_NET_IDLE_CONNECTION_TIMEOUT,
                    PropertyKey.HTTP_NET_CONNECTION_TIMEOUT
            );
            registerDeprecated(
                    PropertyKey.HTTP_NET_QUEUED_CONNECTION_TIMEOUT,
                    PropertyKey.HTTP_NET_CONNECTION_QUEUE_TIMEOUT
            );
            registerDeprecated(
                    PropertyKey.HTTP_NET_SND_BUF_SIZE,
                    PropertyKey.HTTP_NET_CONNECTION_SNDBUF
            );
            registerDeprecated(
                    PropertyKey.PG_NET_ACTIVE_CONNECTION_LIMIT,
                    PropertyKey.PG_NET_CONNECTION_LIMIT
            );
            registerDeprecated(
                    PropertyKey.PG_NET_IDLE_TIMEOUT,
                    PropertyKey.PG_NET_CONNECTION_TIMEOUT
            );
            registerDeprecated(
                    PropertyKey.PG_NET_RECV_BUF_SIZE,
                    PropertyKey.PG_NET_CONNECTION_RCVBUF
            );
            registerDeprecated(
                    PropertyKey.LINE_TCP_NET_ACTIVE_CONNECTION_LIMIT,
                    PropertyKey.LINE_TCP_NET_CONNECTION_LIMIT
            );
            registerDeprecated(
                    PropertyKey.LINE_TCP_NET_IDLE_TIMEOUT,
                    PropertyKey.LINE_TCP_NET_CONNECTION_TIMEOUT
            );
            registerDeprecated(
                    PropertyKey.LINE_TCP_NET_QUEUED_TIMEOUT,
                    PropertyKey.LINE_TCP_NET_CONNECTION_QUEUE_TIMEOUT
            );
            registerDeprecated(
                    PropertyKey.LINE_TCP_NET_RECV_BUF_SIZE,
                    PropertyKey.LINE_TCP_NET_CONNECTION_RCVBUF
            );
            registerDeprecated(
                    PropertyKey.LINE_TCP_DEFAULT_PARTITION_BY,
                    PropertyKey.LINE_DEFAULT_PARTITION_BY
            );
            registerDeprecated(
                    PropertyKey.CAIRO_REPLACE_BUFFER_MAX_SIZE,
                    PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE
            );
            registerDeprecated(
                    PropertyKey.CIRCUIT_BREAKER_BUFFER_SIZE,
                    PropertyKey.NET_TEST_CONNECTION_BUFFER_SIZE
            );
            registerDeprecated(
                    PropertyKey.CAIRO_PAGE_FRAME_TASK_POOL_CAPACITY
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_MAP_PAGE_SIZE,
                    PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_MAP_KEY_CAPACITY,
                    PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY
            );
            registerDeprecated(PropertyKey.PG_INSERT_POOL_CAPACITY);
            registerDeprecated(PropertyKey.LINE_UDP_TIMESTAMP);
            registerDeprecated(PropertyKey.LINE_TCP_TIMESTAMP);
            registerDeprecated(PropertyKey.CAIRO_QUERY_CACHE_EVENT_QUEUE_CAPACITY);
            registerDeprecated(PropertyKey.CAIRO_SQL_JIT_ROWS_THRESHOLD);
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_ANALYTIC_COLUMN_POOL_CAPACITY,
                    PropertyKey.CAIRO_SQL_WINDOW_COLUMN_POOL_CAPACITY
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_ANALYTIC_STORE_PAGE_SIZE,
                    PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_ANALYTIC_STORE_MAX_PAGES,
                    PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_ANALYTIC_ROWID_PAGE_SIZE,
                    PropertyKey.CAIRO_SQL_WINDOW_ROWID_PAGE_SIZE
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_ANALYTIC_ROWID_MAX_PAGES,
                    PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_PAGES
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_ANALYTIC_TREE_PAGE_SIZE,
                    PropertyKey.CAIRO_SQL_WINDOW_TREE_PAGE_SIZE
            );
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_ANALYTIC_TREE_MAX_PAGES,
                    PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_PAGES
            );
        }

        public ValidationResult validate(Properties properties) {
            // Settings that used to be valid but no longer are.
            Map<String, String> obsolete = new HashMap<>();

            // Settings that are still valid but are now superseded by newer ones.
            Map<String, String> deprecated = new HashMap<>();

            // Settings that are not recognized.
            Set<String> incorrect = new HashSet<>();

            for (String propName : properties.stringPropertyNames()) {
                Optional<ConfigPropertyKey> prop = lookupConfigProperty(propName);
                if (prop.isPresent()) {
                    String deprecationMsg = deprecatedSettings.get(prop.get());
                    if (deprecationMsg != null) {
                        deprecated.put(propName, deprecationMsg);
                    }
                } else {
                    String obsoleteMsg = obsoleteSettings.get(propName);
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

        private static <KeyT> void registerReplacements(
                Map<KeyT, String> map,
                KeyT old,
                ConfigPropertyKey... replacements
        ) {
            if (replacements.length > 0) {
                final StringBuilder sb = new StringBuilder("Replaced by ");
                for (int index = 0; index < replacements.length; index++) {
                    if (index > 0) {
                        sb.append(index < (replacements.length - 1) ? ", " : " and ");
                    }
                    String replacement = replacements[index].getPropertyPath();
                    sb.append('`');
                    sb.append(replacement);
                    sb.append('`');
                }
                map.put(old, sb.toString());
            } else {
                map.put(old, "No longer used");
            }
        }

        protected Optional<ConfigPropertyKey> lookupConfigProperty(String propName) {
            return PropertyKey.getByString(propName).map(prop -> prop);
        }

        protected void registerDeprecated(ConfigPropertyKey old, ConfigPropertyKey... replacements) {
            registerReplacements(deprecatedSettings, old, replacements);
        }

        protected void registerObsolete(String old, ConfigPropertyKey... replacements) {
            registerReplacements(obsoleteSettings, old, replacements);
        }
    }

    public static class ValidationResult {
        public final boolean isError;
        public final String message;

        private ValidationResult(boolean isError, String message) {
            this.isError = isError;
            this.message = message;
        }
    }

    class PropCairoConfiguration implements CairoConfiguration {
        private final LongSupplier randomIDSupplier = () -> getRandom().nextPositiveLong();
        private final LongSupplier sequentialIDSupplier = new LongSupplier() {
            final AtomicLong value = new AtomicLong();

            @Override
            public long getAsLong() {
                return value.incrementAndGet();
            }
        };

        @Override
        public boolean attachPartitionCopy() {
            return cairoAttachPartitionCopy;
        }

        @Override
        public boolean enableTestFactories() {
            return false;
        }

        @Override
        public @Nullable ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> getAllPairs() {
            return allPairs;
        }

        @Override
        public boolean getAllowTableRegistrySharedWrite() {
            return false;
        }

        @Override
        public @NotNull String getAttachPartitionSuffix() {
            return cairoAttachPartitionSuffix;
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
        public @NotNull CharSequence getBackupTempDirName() {
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
        public @NotNull BuildInformation getBuildInformation() {
            return buildInformation;
        }

        @Override
        public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
            return circuitBreakerConfiguration;
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
        public int getColumnPurgeQueueCapacity() {
            return columnPurgeQueueCapacity;
        }

        @Override
        public long getColumnPurgeRetryDelay() {
            return columnPurgeRetryDelay;
        }

        @Override
        public long getColumnPurgeRetryDelayLimit() {
            return columnPurgeRetryDelayLimit;
        }

        @Override
        public double getColumnPurgeRetryDelayMultiplier() {
            return columnPurgeRetryDelayMultiplier;
        }

        @Override
        public int getColumnPurgeTaskPoolCapacity() {
            return columnPurgeTaskPoolCapacity;
        }

        @Override
        public int getCommitMode() {
            return commitMode;
        }

        @Override
        public @NotNull CharSequence getConfRoot() {
            return confRoot;
        }

        @Override
        public @NotNull LongSupplier getCopyIDSupplier() {
            if (cairoSQLCopyIdSupplier == 0) {
                return randomIDSupplier;
            }
            return sequentialIDSupplier;
        }

        @Override
        public int getCopyPoolCapacity() {
            return sqlCopyModelPoolCapacity;
        }

        @Override
        public int getCountDistinctCapacity() {
            return sqlCountDistinctCapacity;
        }

        @Override
        public double getCountDistinctLoadFactor() {
            return sqlCountDistinctLoadFactor;
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
        public @NotNull CharSequence getDbDirectory() {
            return dbDirectory;
        }

        @Override
        public @NotNull DateLocale getDefaultDateLocale() {
            return locale;
        }

        @Override
        public @NotNull CharSequence getDefaultMapType() {
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
        public int getDetachedMkDirMode() {
            return detachedMkdirMode;
        }

        @Override
        public int getDoubleToStrCastScale() {
            return sqlDoubleToStrCastScale;
        }

        @Override
        public int getExplainPoolCapacity() {
            return sqlExplainModelPoolCapacity;
        }

        @Override
        public @NotNull FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public int getFileOperationRetryCount() {
            return fileOperationRetryCount;
        }

        @Override
        public @NotNull FilesFacade getFilesFacade() {
            return filesFacade;
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
        public int getGroupByMergeShardQueueCapacity() {
            return cairoGroupByMergeShardQueueCapacity;
        }

        @Override
        public int getGroupByPoolCapacity() {
            return sqlGroupByPoolCapacity;
        }

        @Override
        public int getGroupByShardCount() {
            return cairoGroupByShardCount;
        }

        @Override
        public int getGroupByShardingThreshold() {
            return cairoGroupByShardingThreshold;
        }

        @Override
        public long getIdleCheckInterval() {
            return idleCheckInterval;
        }

        @Override
        public int getInactiveReaderMaxOpenPartitions() {
            return inactiveReaderMaxOpenPartitions;
        }

        @Override
        public long getInactiveReaderTTL() {
            return inactiveReaderTTL;
        }

        @Override
        public long getInactiveWalWriterTTL() {
            return inactiveWalWriterTTL;
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
        public int getInsertPoolCapacity() {
            return sqlInsertModelPoolCapacity;
        }

        @Override
        public int getLatestByQueueCapacity() {
            return latestByQueueCapacity;
        }

        @Override
        public int getMaxCrashFiles() {
            return cairoMaxCrashFiles;
        }

        @Override
        public int getMaxFileNameLength() {
            return maxFileNameLength;
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
        public int getMetadataPoolCapacity() {
            return sqlModelPoolCapacity;
        }

        @Override
        public @NotNull MicrosecondClock getMicrosecondClock() {
            return microsecondClock;
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
        public int getO3LagCalculationWindowsSize() {
            return o3LagCalculationWindowsSize;
        }

        @Override
        public int getO3LastPartitionMaxSplits() {
            return o3LastPartitionMaxSplits;
        }

        @Override
        public long getO3MaxLag() {
            return o3MaxLag;
        }

        @Override
        public int getO3MemMaxPages() {
            return Integer.MAX_VALUE;
        }

        @Override
        public long getO3MinLag() {
            return o3MinLagUs;
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
        public int getO3PurgeDiscoveryQueueCapacity() {
            return o3PurgeDiscoveryQueueCapacity;
        }

        @Override
        public int getPageFrameReduceColumnListCapacity() {
            return cairoPageFrameReduceColumnListCapacity;
        }

        @Override
        public int getPageFrameReduceQueueCapacity() {
            return cairoPageFrameReduceQueueCapacity;
        }

        @Override
        public int getPageFrameReduceRowIdListCapacity() {
            return cairoPageFrameReduceRowIdListCapacity;
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
        public long getPartitionO3SplitMinSize() {
            return o3PartitionSplitMinSize;
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
        public int getRepeatMigrationsFromVersion() {
            return repeatMigrationFromVersion;
        }

        @Override
        public int getRndFunctionMemoryMaxPages() {
            return rndFunctionMemoryMaxPages;
        }

        @Override
        public int getRndFunctionMemoryPageSize() {
            return rndFunctionMemoryPageSize;
        }

        @Override
        public @NotNull String getRoot() {
            return root;
        }

        @Override
        public int getSampleByIndexSearchPageSize() {
            return sampleByIndexSearchPageSize;
        }

        @Override
        public boolean getSimulateCrashEnabled() {
            return simulateCrashEnabled;
        }

        @Override
        public @NotNull CharSequence getSnapshotInstanceId() {
            return snapshotInstanceId;
        }

        @Override
        public @NotNull CharSequence getSnapshotRoot() {
            return snapshotRoot;
        }

        @Override
        public long getSpinLockTimeout() {
            return spinLockTimeout;
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
        public int getSqlCompilerPoolCapacity() {
            return sqlCompilerPoolCapacity;
        }

        @Override
        public int getSqlCopyBufferSize() {
            return sqlCopyBufferSize;
        }

        @Override
        public CharSequence getSqlCopyInputRoot() {
            return cairoSqlCopyRoot;
        }

        @Override
        public CharSequence getSqlCopyInputWorkRoot() {
            return cairoSqlCopyWorkRoot;
        }

        @Override
        public int getSqlCopyLogRetentionDays() {
            return cairoSqlCopyLogRetentionDays;
        }

        @Override
        public long getSqlCopyMaxIndexChunkSize() {
            return cairoSqlCopyMaxIndexChunkSize;
        }

        @Override
        public int getSqlCopyQueueCapacity() {
            return cairoSqlCopyQueueCapacity;
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
        public int getSqlMapMaxPages() {
            return sqlMapMaxPages;
        }

        @Override
        public int getSqlMapMaxResizes() {
            return sqlMapMaxResizes;
        }

        @Override
        public int getSqlMaxNegativeLimit() {
            return sqlMaxNegativeLimit;
        }

        @Override
        public int getSqlModelPoolCapacity() {
            return sqlModelPoolCapacity;
        }

        @Override
        public int getSqlPageFrameMaxRows() {
            return sqlPageFrameMaxRows;
        }

        @Override
        public int getSqlPageFrameMinRows() {
            return sqlPageFrameMinRows;
        }

        @Override
        public int getSqlSmallMapKeyCapacity() {
            return sqlSmallMapKeyCapacity;
        }

        @Override
        public int getSqlSmallMapPageSize() {
            return sqlSmallMapPageSize;
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
        public int getSqlWindowInitialRangeBufferSize() {
            return sqlWindowInitialRangeBufferSize;
        }

        @Override
        public int getSqlWindowMaxRecursion() {
            return sqlWindowMaxRecursion;
        }

        @Override
        public int getSqlWindowRowIdMaxPages() {
            return sqlWindowRowIdMaxPages;
        }

        @Override
        public int getSqlWindowRowIdPageSize() {
            return sqlWindowRowIdPageSize;
        }

        @Override
        public int getSqlWindowStoreMaxPages() {
            return sqlWindowStoreMaxPages;
        }

        @Override
        public int getSqlWindowStorePageSize() {
            return sqlWindowStorePageSize;
        }

        @Override
        public int getSqlWindowTreeKeyMaxPages() {
            return sqlWindowTreeKeyMaxPages;
        }

        @Override
        public int getSqlWindowTreeKeyPageSize() {
            return sqlWindowTreeKeyPageSize;
        }

        @Override
        public int getStrFunctionMaxBufferLength() {
            return sqlStrFunctionBufferMaxSize;
        }

        @Override
        public long getSystemDataAppendPageSize() {
            return systemWriterDataAppendPageSize;
        }

        @Override
        public int getSystemO3ColumnMemorySize() {
            return systemO3ColumnMemorySize;
        }

        @Override
        public @NotNull CharSequence getSystemTableNamePrefix() {
            return systemTableNamePrefix;
        }

        @Override
        public long getSystemWalDataAppendPageSize() {
            return systemWalWriterDataAppendPageSize;
        }

        @Override
        public long getTableRegistryAutoReloadFrequency() {
            return cairoTableRegistryAutoReloadFrequency;
        }

        @Override
        public int getTableRegistryCompactionThreshold() {
            return cairoTableRegistryCompactionThreshold;
        }

        public @NotNull TelemetryConfiguration getTelemetryConfiguration() {
            return telemetryConfiguration;
        }

        @Override
        public CharSequence getTempRenamePendingTablePrefix() {
            return tempRenamePendingTablePrefix;
        }

        @Override
        public @NotNull TextConfiguration getTextConfiguration() {
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
        public @NotNull VolumeDefinitions getVolumeDefinitions() {
            return volumeDefinitions;
        }

        @Override
        public int getWalApplyLookAheadTransactionCount() {
            return walApplyLookAheadTransactionCount;
        }

        @Override
        public long getWalApplyTableTimeQuota() {
            return walApplyTableTimeQuota;
        }

        @Override
        public long getWalDataAppendPageSize() {
            return walWriterDataAppendPageSize;
        }

        @Override
        public boolean getWalEnabledDefault() {
            return walEnabledDefault;
        }

        @Override
        public long getWalMaxLagSize() {
            return walMaxLagSize;
        }

        @Override
        public int getWalMaxLagTxnCount() {
            return walMaxLagTxnCount;
        }

        @Override
        public int getWalMaxSegmentFileDescriptorsCache() {
            return walMaxSegmentFileDescriptorsCache;
        }

        @Override
        public long getWalPurgeInterval() {
            return walPurgeInterval;
        }

        @Override
        public int getWalPurgeWaitBeforeDelete() {
            return walPurgeWaitBeforeDelete;
        }

        @Override
        public int getWalRecreateDistressedSequencerAttempts() {
            return walRecreateDistressedSequencerAttempts;
        }

        @Override
        public long getWalSegmentRolloverRowCount() {
            return walSegmentRolloverRowCount;
        }

        @Override
        public long getWalSegmentRolloverSize() {
            return walSegmentRolloverSize;
        }

        @Override
        public double getWalSquashUncommittedRowsMultiplier() {
            return walSquashUncommittedRowsMultiplier;
        }

        @Override
        public int getWalTxnNotificationQueueCapacity() {
            return walTxnNotificationQueueCapacity;
        }

        @Override
        public int getWalWriterPoolMaxSegments() {
            return walWriterPoolMaxSegments;
        }

        @Override
        public int getWindowColumnPoolCapacity() {
            return sqlWindowColumnPoolCapacity;
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
        public long getWriterMemoryLimit() {
            return writerMemoryLimit;
        }

        @Override
        public int getWriterTickRowsCountMod() {
            return writerTickRowsCountMod;
        }

        @Override
        public boolean isIOURingEnabled() {
            return ioURingEnabled;
        }

        @Override
        public boolean isMultiKeyDedupEnabled() {
            return false;
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
        public boolean isReadOnlyInstance() {
            return isReadOnlyInstance;
        }

        @Override
        public boolean isSnapshotRecoveryEnabled() {
            return snapshotRecoveryEnabled;
        }

        @Override
        public boolean isSqlJitDebugEnabled() {
            return sqlJitDebugEnabled;
        }

        @Override
        public boolean isSqlParallelFilterEnabled() {
            return sqlParallelFilterEnabled;
        }

        @Override
        public boolean isSqlParallelFilterPreTouchEnabled() {
            return sqlParallelFilterPreTouchEnabled;
        }

        @Override
        public boolean isSqlParallelGroupByEnabled() {
            return sqlParallelGroupByEnabled;
        }

        @Override
        public boolean isTableTypeConversionEnabled() {
            return tableTypeConversionEnabled;
        }

        @Override
        public boolean isWalApplyEnabled() {
            return walApplyEnabled;
        }

        public boolean isWalSupported() {
            return walSupported;
        }

        @Override
        public boolean isWriterMixedIOEnabled() {
            return writerMixedIOEnabled;
        }

        @Override
        public boolean mangleTableDirNames() {
            return false;
        }
    }

    private class PropHttpContextConfiguration implements HttpContextConfiguration {

        @Override
        public boolean allowDeflateBeforeSend() {
            return httpAllowDeflateBeforeSend;
        }

        @Override
        public boolean areCookiesEnabled() {
            return httpServerCookiesEnabled;
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
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public int getForceRecvFragmentationChunkSize() {
            return forceRecvFragmentationChunkSize;
        }

        @Override
        public int getForceSendFragmentationChunkSize() {
            return forceSendFragmentationChunkSize;
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
            return httpRecvBufferSize;
        }

        @Override
        public int getRequestHeaderBufferSize() {
            return requestHeaderBufferSize;
        }

        @Override
        public int getSendBufferSize() {
            return httpSendBufferSize;
        }

        @Override
        public boolean getServerKeepAlive() {
            return httpServerKeepAlive;
        }

        @Override
        public boolean readOnlySecurityContext() {
            return httpReadOnlySecurityContext || isReadOnlyInstance;
        }
    }

    private class PropHttpIODispatcherConfiguration implements IODispatcherConfiguration {
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
        public long getHeartbeatInterval() {
            return -1L;
        }

        @Override
        public boolean getHint() {
            return httpNetConnectionHint;
        }

        @Override
        public KqueueFacade getKqueueFacade() {
            return KqueueFacadeImpl.INSTANCE;
        }

        @Override
        public int getLimit() {
            return httpNetConnectionLimit;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public long getQueueTimeout() {
            return httpNetConnectionQueueTimeout;
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
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return httpNetConnectionTimeout;
        }
    }

    private class PropHttpMinIODispatcherConfiguration implements IODispatcherConfiguration {
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

        @Override
        public long getHeartbeatInterval() {
            return -1L;
        }

        @Override
        public boolean getHint() {
            return httpMinNetConnectionHint;
        }

        @Override
        public KqueueFacade getKqueueFacade() {
            return KqueueFacadeImpl.INSTANCE;
        }

        @Override
        public int getLimit() {
            return httpMinNetConnectionLimit;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public long getQueueTimeout() {
            return httpMinNetConnectionQueueTimeout;
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
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return httpMinNetConnectionTimeout;
        }
    }

    private class PropHttpMinServerConfiguration implements HttpMinServerConfiguration {

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return httpMinIODispatcherConfiguration;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public HttpContextConfiguration getHttpContextConfiguration() {
            return httpContextConfiguration;
        }

        @Override
        public String getPoolName() {
            return "minhttp";
        }

        @Override
        public long getSleepThreshold() {
            return httpMinWorkerSleepThreshold;
        }

        @Override
        public long getSleepTimeout() {
            return httpMinWorkerSleepTimeout;
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
        public long getYieldThreshold() {
            return httpMinWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return httpMinWorkerHaltOnError;
        }

        @Override
        public boolean isEnabled() {
            return httpMinServerEnabled;
        }

        @Override
        public boolean isHealthCheckAuthenticationRequired() {
            return httpHealthCheckAuthRequired;
        }

        @Override
        public boolean isPessimisticHealthCheckEnabled() {
            return httpPessimisticHealthCheckEnabled;
        }
    }

    private class PropHttpServerConfiguration implements HttpServerConfiguration {

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return httpIODispatcherConfiguration;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
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
        public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
            return lineHttpProcessorConfiguration;
        }

        @Override
        public String getPoolName() {
            return "http";
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
        public long getSleepThreshold() {
            return httpWorkerSleepThreshold;
        }

        @Override
        public long getSleepTimeout() {
            return httpWorkerSleepTimeout;
        }

        @Override
        public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
            return staticContentProcessorConfiguration;
        }

        @Override
        public WaitProcessorConfiguration getWaitProcessorConfiguration() {
            return httpWaitProcessorConfiguration;
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
        public long getYieldThreshold() {
            return httpWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return httpWorkerHaltOnError;
        }

        @Override
        public boolean isEnabled() {
            return httpServerEnabled;
        }

        @Override
        public boolean isHealthCheckAuthenticationRequired() {
            return httpHealthCheckAuthRequired;
        }

        @Override
        public boolean isPessimisticHealthCheckEnabled() {
            return httpPessimisticHealthCheckEnabled;
        }

        @Override
        public boolean isQueryCacheEnabled() {
            return httpSqlCacheEnabled;
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
        public int getDoubleScale() {
            return jsonQueryDoubleScale;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
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
        public CharSequence getKeepAliveHeader() {
            return keepAliveHeader;
        }

        @Override
        public long getMaxQueryResponseRowLimit() {
            return maxHttpQueryResponseRowLimit;
        }
    }

    private class PropLineHttpProcessorConfiguration implements LineHttpProcessorConfiguration {
        @Override
        public boolean autoCreateNewColumns() {
            return ilpAutoCreateNewColumns;
        }

        @Override
        public boolean autoCreateNewTables() {
            return ilpAutoCreateNewTables;
        }

        @Override
        public short getDefaultColumnTypeForFloat() {
            return floatDefaultColumnType;
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return integerDefaultColumnType;
        }

        @Override
        public int getDefaultPartitionBy() {
            return lineTcpDefaultPartitionBy;
        }

        @Override
        public CharSequence getInfluxPingVersion() {
            return lineHttpPingVersion;
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return microsecondClock;
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return symbolCacheWaitUsBeforeReload;
        }

        @Override
        public LineTcpTimestampAdapter getTimestampAdapter() {
            return lineTcpTimestampAdapter;
        }

        @Override
        public boolean isEnabled() {
            return lineHttpEnabled;
        }

        @Override
        public boolean isStringAsTagSupported() {
            return stringAsTagSupported;
        }

        @Override
        public boolean isStringToCharCastAllowed() {
            return stringToCharCastAllowed;
        }

        @Override
        public boolean isSymbolAsFieldSupported() {
            return symbolAsFieldSupported;
        }
    }

    private class PropLineTcpIOWorkerPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public String getPoolName() {
            return "ilpio";
        }

        @Override
        public long getSleepThreshold() {
            return lineTcpIOWorkerSleepThreshold;
        }

        @Override
        public int[] getWorkerAffinity() {
            return lineTcpIOWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return lineTcpIOWorkerCount;
        }

        @Override
        public long getYieldThreshold() {
            return lineTcpIOWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return lineTcpIOWorkerPoolHaltOnError;
        }
    }

    private class PropLineTcpReceiverConfiguration implements LineTcpReceiverConfiguration {
        @Override
        public String getAuthDB() {
            return lineTcpAuthDB;
        }

        @Override
        public boolean getAutoCreateNewColumns() {
            return ilpAutoCreateNewColumns;
        }

        @Override
        public boolean getAutoCreateNewTables() {
            return ilpAutoCreateNewTables;
        }

        @Override
        public long getCommitInterval() {
            return LineTcpReceiverConfigurationHelper.calcCommitInterval(
                    cairoConfiguration.getO3MinLag(),
                    getCommitIntervalFraction(),
                    getCommitIntervalDefault()
            );
        }

        public long getCommitIntervalDefault() {
            return lineTcpCommitIntervalDefault;
        }

        @Override
        public double getCommitIntervalFraction() {
            return lineTcpCommitIntervalFraction;
        }

        @Override
        public int getConnectionPoolInitialCapacity() {
            return lineTcpConnectionPoolInitialCapacity;
        }

        @Override
        public short getDefaultColumnTypeForFloat() {
            return floatDefaultColumnType;
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return integerDefaultColumnType;
        }

        @Override
        public int getDefaultPartitionBy() {
            return lineTcpDefaultPartitionBy;
        }

        @Override
        public boolean getDisconnectOnError() {
            return lineTcpDisconnectOnError;
        }

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return lineTcpReceiverDispatcherConfiguration;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
        }

        @Override
        public WorkerPoolConfiguration getIOWorkerPoolConfiguration() {
            return lineTcpIOWorkerPoolConfiguration;
        }

        @Override
        public long getMaintenanceInterval() {
            return lineTcpMaintenanceInterval;
        }

        @Override
        public int getMaxFileNameLength() {
            return maxFileNameLength;
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
        public int getNetMsgBufferSize() {
            return lineTcpMsgBufferSize;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return symbolCacheWaitUsBeforeReload;
        }

        @Override
        public LineTcpTimestampAdapter getTimestampAdapter() {
            return lineTcpTimestampAdapter;
        }

        @Override
        public long getWriterIdleTimeout() {
            return minIdleMsBeforeWriterRelease;
        }

        @Override
        public int getWriterQueueCapacity() {
            return lineTcpWriterQueueCapacity;
        }

        @Override
        public WorkerPoolConfiguration getWriterWorkerPoolConfiguration() {
            return lineTcpWriterWorkerPoolConfiguration;
        }

        @Override
        public boolean isEnabled() {
            return lineTcpEnabled;
        }

        @Override
        public boolean isStringAsTagSupported() {
            return stringAsTagSupported;
        }

        @Override
        public boolean isStringToCharCastAllowed() {
            return stringToCharCastAllowed;
        }

        @Override
        public boolean isSymbolAsFieldSupported() {
            return symbolAsFieldSupported;
        }
    }

    private class PropLineTcpReceiverIODispatcherConfiguration implements IODispatcherConfiguration {

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
        public long getHeartbeatInterval() {
            return lineTcpNetConnectionHeartbeatInterval;
        }

        @Override
        public boolean getHint() {
            return lineTcpNetConnectionHint;
        }

        @Override
        public KqueueFacade getKqueueFacade() {
            return KqueueFacadeImpl.INSTANCE;
        }

        @Override
        public int getLimit() {
            return lineTcpNetConnectionLimit;
        }

        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public long getQueueTimeout() {
            return lineTcpNetConnectionQueueTimeout;
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
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return lineTcpNetConnectionTimeout;
        }
    }

    private class PropLineTcpWriterWorkerPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public String getPoolName() {
            return "ilpwriter";
        }


        @Override
        public long getSleepThreshold() {
            return lineTcpWriterWorkerSleepThreshold;
        }

        @Override
        public int[] getWorkerAffinity() {
            return lineTcpWriterWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return lineTcpWriterWorkerCount;
        }

        @Override
        public long getYieldThreshold() {
            return lineTcpWriterWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return lineTcpWriterWorkerPoolHaltOnError;
        }
    }

    private class PropLineUdpReceiverConfiguration implements LineUdpReceiverConfiguration {
        @Override
        public boolean getAutoCreateNewColumns() {
            return ilpAutoCreateNewColumns;
        }

        @Override
        public boolean getAutoCreateNewTables() {
            return ilpAutoCreateNewTables;
        }

        @Override
        public int getBindIPv4Address() {
            return lineUdpBindIPV4Address;
        }

        @Override
        public int getCommitMode() {
            return lineUdpCommitMode;
        }

        @Override
        public int getCommitRate() {
            return lineUdpCommitRate;
        }

        @Override
        public short getDefaultColumnTypeForFloat() {
            return floatDefaultColumnType;
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return integerDefaultColumnType;
        }

        @Override
        public int getDefaultPartitionBy() {
            return lineUdpDefaultPartitionBy;
        }

        @Override
        public int getGroupIPv4Address() {
            return lineUdpGroupIPv4Address;
        }

        @Override
        public int getMaxFileNameLength() {
            return maxFileNameLength;
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
        public LineTimestampAdapter getTimestampAdapter() {
            return lineUdpTimestampAdapter;
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
    }

    private class PropMetricsConfiguration implements MetricsConfiguration {

        @Override
        public boolean isEnabled() {
            return metricsEnabled;
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
        public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
            return circuitBreakerConfiguration;
        }

        @Override
        public int getConnectionPoolInitialCapacity() {
            return pgConnectionPoolInitialCapacity;
        }

        @Override
        public DateLocale getDefaultDateLocale() {
            return pgDefaultLocale;
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
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public int getForceRecvFragmentationChunkSize() {
            return forceRecvFragmentationChunkSize;
        }

        @Override
        public int getForceSendFragmentationChunkSize() {
            return forceSendFragmentationChunkSize;
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
        public String getPoolName() {
            return "pgwire";
        }

        @Override
        public String getReadOnlyPassword() {
            return pgReadOnlyPassword;
        }

        @Override
        public String getReadOnlyUsername() {
            return pgReadOnlyUsername;
        }

        @Override
        public int getRecvBufferSize() {
            return pgRecvBufferSize;
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
        public int getSendBufferSize() {
            return pgSendBufferSize;
        }

        @Override
        public String getServerVersion() {
            return "11.3";
        }

        @Override
        public long getSleepThreshold() {
            return pgWorkerSleepThreshold;
        }

        @Override
        public int getUpdateCacheBlockCount() {
            return pgUpdateCacheBlockCount;
        }

        @Override
        public int getUpdateCacheRowCount() {
            return pgUpdateCacheRowCount;
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
        public long getYieldThreshold() {
            return pgWorkerYieldThreshold;
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
        public boolean isEnabled() {
            return pgEnabled;
        }

        @Override
        public boolean isInsertCacheEnabled() {
            return pgInsertCacheEnabled;
        }

        @Override
        public boolean isReadOnlyUserEnabled() {
            return pgReadOnlyUserEnabled;
        }

        @Override
        public boolean isSelectCacheEnabled() {
            return pgSelectCacheEnabled;
        }

        @Override
        public boolean isUpdateCacheEnabled() {
            return pgUpdateCacheEnabled;
        }

        @Override
        public boolean readOnlySecurityContext() {
            return pgReadOnlySecurityContext || isReadOnlyInstance;
        }
    }

    private class PropPGWireDispatcherConfiguration implements IODispatcherConfiguration {

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

        @Override
        public long getHeartbeatInterval() {
            return -1L;
        }

        @Override
        public boolean getHint() {
            return pgNetConnectionHint;
        }

        @Override
        public KqueueFacade getKqueueFacade() {
            return KqueueFacadeImpl.INSTANCE;
        }

        @Override
        public int getLimit() {
            return pgNetConnectionLimit;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public long getQueueTimeout() {
            return pgNetConnectionQueueTimeout;
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
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return pgNetIdleConnectionTimeout;
        }
    }

    private class PropSqlExecutionCircuitBreakerConfiguration implements SqlExecutionCircuitBreakerConfiguration {

        @Override
        public boolean checkConnection() {
            return true;
        }

        @Override
        public int getBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public int getCircuitBreakerThrottle() {
            return circuitBreakerThrottle;
        }

        @Override
        @NotNull
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        @NotNull
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public long getQueryTimeout() {
            return queryTimeout;
        }

        @Override
        public boolean isEnabled() {
            return interruptOnClosedConnection;
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
        public String getKeepAliveHeader() {
            return keepAliveHeader;
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
        public boolean isAuthenticationRequired() {
            return httpStaticAuthRequired;
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

    private class PropTextConfiguration implements TextConfiguration {

        @Override
        public int getDateAdapterPoolCapacity() {
            return dateAdapterPoolCapacity;
        }

        @Override
        public DateLocale getDefaultDateLocale() {
            return locale;
        }

        @Override
        public InputFormatConfiguration getInputFormatConfiguration() {
            return inputFormatConfiguration;
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
    }

    private class PropWaitProcessorConfiguration implements WaitProcessorConfiguration {

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
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

        @Override
        public long getMaxWaitCapMs() {
            return maxRerunWaitCapMs;
        }
    }

    private class PropWalApplyPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public String getPoolName() {
            return "wal-apply";
        }

        @Override
        public long getSleepThreshold() {
            return walApplyWorkerSleepThreshold;
        }

        @Override
        public long getSleepTimeout() {
            return walApplySleepTimeout;
        }

        @Override
        public int[] getWorkerAffinity() {
            return walApplyWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return walApplyWorkerCount;
        }

        @Override
        public long getYieldThreshold() {
            return walApplyWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return walApplyWorkerHaltOnError;
        }

        @Override
        public boolean isEnabled() {
            return walApplyWorkerCount > 0;
        }
    }

    private class PropWorkerPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public String getPoolName() {
            return "shared";
        }

        @Override
        public long getSleepThreshold() {
            return sharedWorkerSleepThreshold;
        }

        @Override
        public long getSleepTimeout() {
            return sharedWorkerSleepTimeout;
        }

        @Override
        public int[] getWorkerAffinity() {
            return sharedWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return sharedWorkerCount;
        }

        @Override
        public long getYieldThreshold() {
            return sharedWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return sharedWorkerHaltOnError;
        }
    }

    static {
        WRITE_FO_OPTS.put("o_direct", (int) CairoConfiguration.O_DIRECT);
        WRITE_FO_OPTS.put("o_sync", (int) CairoConfiguration.O_SYNC);
        WRITE_FO_OPTS.put("o_async", (int) CairoConfiguration.O_ASYNC);
        WRITE_FO_OPTS.put("o_none", (int) CairoConfiguration.O_NONE);
    }
}
