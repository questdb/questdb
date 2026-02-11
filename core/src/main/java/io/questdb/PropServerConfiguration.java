/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.MimeTypesCache;
import io.questdb.cutlass.http.WaitProcessorConfiguration;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfigurationHelper;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.text.CsvFileIndexer;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.types.InputFormatConfiguration;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.log.Log;
import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.EpollFacade;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.KqueueFacade;
import io.questdb.network.KqueueFacadeImpl;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.SelectFacade;
import io.questdb.network.SelectFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.*;

public class PropServerConfiguration implements ServerConfiguration {
    public static final String ACCEPTING_WRITES = "accepting.writes";
    public static final String ACL_ENABLED = "acl.enabled";
    public static final int COLUMN_ALIAS_GENERATED_MAX_SIZE_DEFAULT = 64;
    public static final int COLUMN_ALIAS_GENERATED_MAX_SIZE_MINIMUM = 4;
    public static final long COMMIT_INTERVAL_DEFAULT = 2000;
    public static final String CONFIG_DIRECTORY = "conf";
    public static final String DB_DIRECTORY = "db";
    public static final int MIN_TCP_ILP_BUF_SIZE = AuthUtils.CHALLENGE_LEN + 1;
    public static final String TMP_DIRECTORY = "tmp";
    static final String SECRET_FILE_ENV_VAR_SUFFIX = "_FILE";
    static final String SECRET_FILE_PROPERTY_SUFFIX = ".file";
    private static final String ILP_PROTO_SUPPORT_VERSIONS = "[1,2,3]";
    private static final String ILP_PROTO_SUPPORT_VERSIONS_NAME = "line.proto.support.versions";
    private static final String ILP_PROTO_TRANSPORTS = "ilp.proto.transports";
    private static final String RELEASE_TYPE = "release.type";
    private static final String RELEASE_VERSION = "release.version";
    private static final int SECRET_FILE_MAX_SIZE = 65536; // 64KB max for secret files
    private static final LowerCaseCharSequenceIntHashMap WRITE_FO_OPTS = new LowerCaseCharSequenceIntHashMap();
    protected final byte httpHealthCheckAuthType;
    private final String acceptingWrites;
    private final ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> allPairs = new ObjObjHashMap<>();
    private final boolean allowTableRegistrySharedWrite;
    private final boolean asyncMunmapEnabled;
    private final int binaryEncodingMaxLength;
    private final BuildInformation buildInformation;
    private final boolean cairoAttachPartitionCopy;
    private final String cairoAttachPartitionSuffix;
    private final boolean cairoAutoScaleSymbolCapacity;
    private final double cairoAutoScaleSymbolCapacityThreshold;
    private final long cairoCommitLatency;
    private final CairoConfiguration cairoConfiguration = new PropCairoConfiguration();
    private final int cairoGroupByMergeShardQueueCapacity;
    private final boolean cairoGroupByPresizeEnabled;
    private final long cairoGroupByPresizeMaxCapacity;
    private final long cairoGroupByPresizeMaxHeapSize;
    private final int cairoGroupByShardingThreshold;
    private final int cairoGroupByTopKQueueCapacity;
    private final long cairoGroupByTopKThreshold;
    private final int cairoMaxCrashFiles;
    private final int cairoPageFrameReduceColumnListCapacity;
    private final int cairoPageFrameReduceQueueCapacity;
    private final int cairoPageFrameReduceRowIdListCapacity;
    private final int cairoPageFrameReduceShardCount;
    private final boolean cairoResourcePoolTracingEnabled;
    private final int cairoSQLCopyIdSupplier;
    private final boolean cairoSqlColumnAliasExpressionEnabled;
    private final int cairoSqlCopyExportQueueCapacity;
    private final String cairoSqlCopyExportRoot;
    private final int cairoSqlCopyLogRetentionDays;
    private final int cairoSqlCopyQueueCapacity;
    private final String cairoSqlCopyRoot;
    private final String cairoSqlCopyWorkRoot;
    private final boolean cairoSqlLegacyOperatorPrecedence;
    private final long cairoTableRegistryAutoReloadFrequency;
    private final int cairoTableRegistryCompactionThreshold;
    private final long cairoWriteBackOffTimeoutOnMemPressureMs;
    private final boolean checkpointRecoveryEnabled;
    private final boolean checkpointRecoveryRebuildColumnIndexes;
    private final int checkpointRecoveryThreadpoolMax;
    private final int checkpointRecoveryThreadpoolMin;
    private final String checkpointRoot;
    private final PropSqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new PropSqlExecutionCircuitBreakerConfiguration();
    private final int circuitBreakerThrottle;
    private final int columnIndexerQueueCapacity;
    private final int columnPurgeQueueCapacity;
    private final long columnPurgeRetryDelay;
    private final long columnPurgeRetryDelayLimit;
    private final double columnPurgeRetryDelayMultiplier;
    private final int columnPurgeTaskPoolCapacity;
    private final int commitMode;
    private final String confRoot;
    private final boolean configReloadEnabled;
    private final boolean copierChunkedEnabled;
    private final int copierType;
    private final int createAsSelectRetryCount;
    private final int dateAdapterPoolCapacity;
    private final String dbDirectory;
    private final String dbLogName;
    private final String dbRoot;
    private final boolean debugWalApplyBlockFailureNoRetry;
    private final int decimalAdapterPoolCapacity;
    private final int defaultSeqPartTxnCount;
    private final boolean defaultSymbolCacheFlag;
    private final int defaultSymbolCapacity;
    private final int detachedMkdirMode;
    private final boolean devModeEnabled;
    private final Set<? extends ConfigPropertyKey> dynamicProperties;
    private final boolean enableTestFactories;
    private final WorkerPoolConfiguration exportPoolConfiguration = new PropExportPoolConfiguration();
    private final int[] exportWorkerAffinity;
    private final int exportWorkerCount;
    private final boolean exportWorkerHaltOnError;
    private final long exportWorkerNapThreshold;
    private final long exportWorkerSleepThreshold;
    private final long exportWorkerSleepTimeout;
    private final long exportWorkerYieldThreshold;
    private final boolean fileDescriptorCacheEnabled;
    private final int fileOperationRetryCount;
    private final FilesFacade filesFacade;
    private final FactoryProviderFactory fpf;
    private final PropHttpContextConfiguration httpContextConfiguration;
    private final ObjHashSet<String> httpContextPathExport = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathILP = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathILPPing = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathImport = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathSettings = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathSqlExecute = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathSqlValidate = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathTableStatus = new ObjHashSet<>();
    private final ObjHashSet<String> httpContextPathWarnings = new ObjHashSet<>();
    private final String httpContextWebConsole;
    private final long httpExportTimeout;
    private final boolean httpFrozenClock;
    private final PropHttpConcurrentCacheConfiguration httpMinConcurrentCacheConfiguration = new PropHttpConcurrentCacheConfiguration();
    private final PropHttpContextConfiguration httpMinContextConfiguration;
    private final boolean httpMinServerEnabled;
    private final long httpNetAcceptLoopTimeout;
    private final boolean httpNetConnectionHint;
    private final String httpPassword;
    private final boolean httpPessimisticHealthCheckEnabled;
    private final long httpRecvMaxBufferSize;
    private final int httpSendBufferSize;
    private final boolean httpServerEnabled;
    private final boolean httpSettingsReadOnly;
    private final int httpSqlCacheBlockCount;
    private final boolean httpSqlCacheEnabled;
    private final int httpSqlCacheRowCount;
    private final String httpUsername;
    private final WaitProcessorConfiguration httpWaitProcessorConfiguration = new PropWaitProcessorConfiguration();
    private final int[] httpWorkerAffinity;
    private final int httpWorkerCount;
    private final boolean httpWorkerHaltOnError;
    private final long httpWorkerNapThreshold;
    private final long httpWorkerSleepThreshold;
    private final long httpWorkerSleepTimeout;
    private final long httpWorkerYieldThreshold;
    private final int idGenerateBatchStep;
    private final long idleCheckInterval;
    private final boolean ilpAutoCreateNewColumns;
    private final boolean ilpAutoCreateNewTables;
    private final String ilpProtoTransports;
    private final int inactiveReaderMaxOpenPartitions;
    private final long inactiveReaderTTL;
    private final long inactiveViewWalWriterTTL;
    private final long inactiveWalWriterTTL;
    private final long inactiveWriterTTL;
    private final int indexValueBlockSize;
    private final InputFormatConfiguration inputFormatConfiguration;
    private final String installRoot;
    private final long instanceHashHi;
    private final long instanceHashLo;
    private final boolean interruptOnClosedConnection;
    private final boolean ioURingEnabled;
    private final boolean isQueryTracingEnabled;
    private final boolean isReadOnlyInstance;
    private final int jsonCacheLimit;
    private final int jsonCacheSize;
    private final String keepAliveHeader;
    private final int latestByQueueCapacity;
    private final String legacyCheckpointRoot;
    private final boolean lineHttpEnabled;
    private final CharSequence lineHttpPingVersion;
    private final LineHttpProcessorConfiguration lineHttpProcessorConfiguration = new PropLineHttpProcessorConfiguration();
    private final String lineTcpAuthDB;
    private final boolean lineTcpEnabled;
    private final WorkerPoolConfiguration lineTcpIOWorkerPoolConfiguration = new PropLineTcpIOWorkerPoolConfiguration();
    private final LineTcpReceiverConfiguration lineTcpReceiverConfiguration = new PropLineTcpReceiverConfiguration();
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
    private final byte lineUdpTimestampUnit;
    private final boolean lineUdpUnicast;
    private final DateLocale locale;
    private final Log log;
    private final boolean logLevelVerbose;
    private final boolean logSqlQueryProgressExe;
    private final DateFormat logTimestampFormat;
    private final DateLocale logTimestampLocale;
    private final String logTimestampTimezone;
    private final TimeZoneRules logTimestampTimezoneRules;
    private final boolean matViewEnabled;
    private final long matViewInsertAsSelectBatchSize;
    private final int matViewMaxRefreshIntervals;
    private final int matViewMaxRefreshRetries;
    private final long matViewMaxRefreshStepUs;
    private final boolean matViewParallelExecutionEnabled;
    private final long matViewRefreshIntervalsUpdatePeriod;
    private final boolean matViewRefreshMissingWalFilesFatal;
    private final long matViewRefreshOomRetryTimeout;
    private final WorkerPoolConfiguration matViewRefreshPoolConfiguration = new PropMatViewsRefreshPoolConfiguration();
    private final long matViewRefreshSleepTimeout;
    private final int[] matViewRefreshWorkerAffinity;
    private final int matViewRefreshWorkerCount;
    private final boolean matViewRefreshWorkerHaltOnError;
    private final long matViewRefreshWorkerNapThreshold;
    private final long matViewRefreshWorkerSleepThreshold;
    private final long matViewRefreshWorkerYieldThreshold;
    private final long matViewRowsPerQueryEstimate;
    private final int maxFileNameLength;
    private final long maxHttpQueryResponseRowLimit;
    private final double maxRequiredDelimiterStdDev;
    private final double maxRequiredLineLengthStdDev;
    private final long maxRerunWaitCapMs;
    private final int maxSqlRecompileAttempts;
    private final int maxSwapFileCount;
    private final int maxUncommittedRows;
    private final MemoryConfiguration memoryConfiguration;
    private final int metadataStringPoolCapacity;
    private final Metrics metrics;
    private final MetricsConfiguration metricsConfiguration = new PropMetricsConfiguration();
    private final boolean metricsEnabled;
    private final MicrosecondClock microsecondClock;
    private final int mkdirMode;
    private final int o3CallbackQueueCapacity;
    private final int o3ColumnMemorySize;
    private final int o3CopyQueueCapacity;
    private final int o3LagCalculationWindowsSize;
    private final int o3LastPartitionMaxSplits;
    private final long o3MaxLagUs;
    private final long o3MinLagUs;
    private final int o3OpenColumnQueueCapacity;
    private final boolean o3PartitionOverwriteControlEnabled;
    private final int o3PartitionPurgeListCapacity;
    private final int o3PartitionQueueCapacity;
    private final long o3PartitionSplitMinSize;
    private final int o3PurgeDiscoveryQueueCapacity;
    private final boolean o3QuickSortEnabled;
    private final int parallelIndexThreshold;
    private final boolean parallelIndexingEnabled;
    private final long parquetExportBatchSize;
    private final int parquetExportCompressionCodec;
    private final int parquetExportCompressionLevel;
    private final int parquetExportCopyReportFrequencyLines;
    private final int parquetExportDataPageSize;
    private final boolean parquetExportRawArrayEncoding;
    private final int parquetExportRowGroupSize;
    private final boolean parquetExportStatisticsEnabled;
    private final CharSequence parquetExportTableNamePrefix;
    private final int parquetExportVersion;
    private final int partitionEncoderParquetCompressionCodec;
    private final int partitionEncoderParquetCompressionLevel;
    private final int partitionEncoderParquetDataPageSize;
    private final boolean partitionEncoderParquetRawArrayEncoding;
    private final int partitionEncoderParquetRowGroupSize;
    private final boolean partitionEncoderParquetStatisticsEnabled;
    private final int partitionEncoderParquetVersion;
    private final PGConfiguration pgConfiguration = new PropPGConfiguration();
    private final boolean pgEnabled;
    private final PropPGWireConcurrentCacheConfiguration pgWireConcurrentCacheConfiguration = new PropPGWireConcurrentCacheConfiguration();
    private final int poolSegmentSize;
    private final String posthogApiKey;
    private final boolean posthogEnabled;
    private final int preferencesStringPoolCapacity;
    private final String publicDirectory;
    private final PublicPassthroughConfiguration publicPassthroughConfiguration = new PropPublicPassthroughConfiguration();
    private final int queryCacheEventQueueCapacity;
    private final boolean queryWithinLatestByOptimisationEnabled;
    private final int readerPoolMaxSegments;
    private final int recentWriteTrackerCapacity;
    private final Utf8SequenceObjHashMap<Utf8Sequence> redirectMap;
    private final int repeatMigrationFromVersion;
    private final double rerunExponentialWaitMultiplier;
    private final int rerunInitialWaitQueueSize;
    private final int rerunMaxProcessingQueueSize;
    private final int rmdirMaxDepth;
    private final int rndFunctionMemoryMaxPages;
    private final int rndFunctionMemoryPageSize;
    private final int rollBufferLimit;
    private final int rollBufferSize;
    private final long sequencerCheckInterval;
    private final PropWorkerPoolConfiguration sharedWorkerPoolNetworkConfiguration = new PropWorkerPoolConfiguration("shared-network");
    private final PropWorkerPoolConfiguration sharedWorkerPoolQueryConfiguration = new PropWorkerPoolConfiguration("shared-query");
    private final PropWorkerPoolConfiguration sharedWorkerPoolWriteConfiguration = new PropWorkerPoolConfiguration("shared-write");
    private final String snapshotInstanceId;
    private final long spinLockTimeout;
    private final int sqlAsOfJoinEvacuationThreshold;
    private final int sqlAsOfJoinLookahead;
    private final int sqlAsOfJoinShortCircuitCacheCapacity;
    private final int sqlBindVariablePoolSize;
    private final int sqlCharacterStoreCapacity;
    private final int sqlCharacterStoreSequencePoolCapacity;
    private final int sqlColumnPoolCapacity;
    private final int sqlCompileViewModelPoolCapacity;
    private final int sqlCompilerPoolCapacity;
    private final int sqlCopyBufferSize;
    private final int sqlCopyModelPoolCapacity;
    private final int sqlCountDistinctCapacity;
    private final double sqlCountDistinctLoadFactor;
    private final int sqlCreateTableColumnModelPoolCapacity;
    private final long sqlCreateTableModelBatchSize;
    private final int sqlDistinctTimestampKeyCapacity;
    private final double sqlDistinctTimestampLoadFactor;
    private final int sqlExplainModelPoolCapacity;
    private final int sqlExpressionPoolCapacity;
    private final double sqlFastMapLoadFactor;
    private final long sqlGroupByAllocatorChunkSize;
    private final long sqlGroupByAllocatorMaxChunkSize;
    private final int sqlGroupByMapCapacity;
    private final int sqlGroupByPoolCapacity;
    private final int sqlHashJoinLightValueMaxPages;
    private final int sqlHashJoinLightValuePageSize;
    private final int sqlHashJoinValueMaxPages;
    private final int sqlHashJoinValuePageSize;
    private final long sqlInsertModelBatchSize;
    private final int sqlInsertModelPoolCapacity;
    private final int sqlIntervalIncrementalMergeThreshold;
    private final int sqlIntervalMaxBracketDepth;
    private final int sqlIntervalMaxIntervalsAfterMerge;
    private final int sqlJitBindVarsMemoryMaxPages;
    private final int sqlJitBindVarsMemoryPageSize;
    private final boolean sqlJitDebugEnabled;
    private final int sqlJitIRMemoryMaxPages;
    private final int sqlJitIRMemoryPageSize;
    private final int sqlJitMaxInListSizeThreshold;
    private final int sqlJitMode;
    private final int sqlJoinContextPoolCapacity;
    private final int sqlJoinMetadataMaxResizes;
    private final int sqlJoinMetadataPageSize;
    private final long sqlLatestByRowCount;
    private final int sqlLexerPoolCapacity;
    private final int sqlMapMaxPages;
    private final int sqlMapMaxResizes;
    private final int sqlMaxArrayElementCount;
    private final int sqlMaxNegativeLimit;
    private final int sqlMaxSymbolNotEqualsCount;
    private final int sqlModelPoolCapacity;
    private final int sqlOrderByRadixSortThreshold;
    private final boolean sqlOrderBySortEnabled;
    private final int sqlPageFrameMaxRows;
    private final int sqlPageFrameMinRows;
    private final int sqlParallelFilterDispatchLimit;
    private final boolean sqlParallelFilterEnabled;
    private final double sqlParallelFilterPreTouchThreshold;
    private final boolean sqlParallelGroupByEnabled;
    private final boolean sqlParallelHorizonJoinEnabled;
    private final boolean sqlParallelReadParquetEnabled;
    private final boolean sqlParallelTopKEnabled;
    private final boolean sqlParallelWindowJoinEnabled;
    private final long sqlParallelWorkStealingSpinTimeout;
    private final int sqlParallelWorkStealingThreshold;
    private final int sqlParquetFrameCacheCapacity;
    private final int sqlPivotForColumnPoolCapacity;
    private final int sqlPivotMaxProducedColumns;
    private final int sqlQueryRegistryPoolSize;
    private final int sqlRenameTableModelPoolCapacity;
    private final boolean sqlSampleByDefaultAlignment;
    private final int sqlSampleByIndexSearchPageSize;
    private final boolean sqlSampleByValidateFillType;
    private final int sqlSmallMapKeyCapacity;
    private final long sqlSmallMapPageSize;
    private final int sqlSmallPageFrameMaxRows;
    private final int sqlSmallPageFrameMinRows;
    private final int sqlSortKeyMaxPages;
    private final long sqlSortKeyPageSize;
    private final int sqlSortLightValueMaxPages;
    private final long sqlSortLightValuePageSize;
    private final int sqlSortValueMaxPages;
    private final int sqlSortValuePageSize;
    private final int sqlStrFunctionBufferMaxSize;
    private final int sqlTxnScoreboardEntryCount;
    private final int sqlUnorderedMapMaxEntrySize;
    private final int sqlViewLexerPoolCapacity;
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
    private final long symbolTableMaxAllocationPageSize;
    private final long symbolTableMinAllocationPageSize;
    private final int systemO3ColumnMemorySize;
    private final String systemTableNamePrefix;
    private final long systemWalWriterDataAppendPageSize;
    private final long systemWalWriterEventAppendPageSize;
    private final long systemWriterDataAppendPageSize;
    private final boolean tableTypeConversionEnabled;
    private final TelemetryConfiguration telemetryConfiguration;
    private final long telemetryDbSizeEstimateTimeout;
    private final boolean telemetryDisableCompletely;
    private final CharSequence tempRenamePendingTablePrefix;
    private final int textAnalysisMaxLines;
    private final TextConfiguration textConfiguration = new PropTextConfiguration();
    private final int textLexerStringPoolCapacity;
    private final int timestampAdapterPoolCapacity;
    private final boolean ttlUseWallClock;
    private final boolean useLegacyStringDefault;
    private final int utf8SinkSize;
    private final PropertyValidator validator;
    private final int vectorAggregateQueueCapacity;
    private final WorkerPoolConfiguration viewCompilerPoolConfiguration = new PropViewCompilerPoolConfiguration();
    private final long viewCompilerSleepTimeout;
    private final int[] viewCompilerWorkerAffinity;
    private final int viewCompilerWorkerCount;
    private final boolean viewCompilerWorkerHaltOnError;
    private final long viewCompilerWorkerNapThreshold;
    private final long viewCompilerWorkerSleepThreshold;
    private final long viewCompilerWorkerYieldThreshold;
    private final int viewWalWriterPoolMaxSegments;
    private final VolumeDefinitions volumeDefinitions = new VolumeDefinitions();
    private final boolean walApplyEnabled;
    private final int walApplyLookAheadTransactionCount;
    private final WorkerPoolConfiguration walApplyPoolConfiguration = new PropWalApplyPoolConfiguration();
    private final long walApplySleepTimeout;
    private final long walApplyTableTimeQuota;
    private final int[] walApplyWorkerAffinity;
    private final int walApplyWorkerCount;
    private final boolean walApplyWorkerHaltOnError;
    private final long walApplyWorkerNapThreshold;
    private final long walApplyWorkerSleepThreshold;
    private final long walApplyWorkerYieldThreshold;
    private final boolean walEnabledDefault;
    private final long walMaxLagSize;
    private final int walMaxLagTxnCount;
    private final int walMaxSegmentFileDescriptorsCache;
    private final boolean walParallelExecutionEnabled;
    private final long walPurgeInterval;
    private final int walPurgeWaitBeforeDelete;
    private final int walRecreateDistressedSequencerAttempts;
    private final long walSegmentRolloverRowCount;
    private final double walSquashUncommittedRowsMultiplier;
    private final boolean walSupported;
    private final int walTxnNotificationQueueCapacity;
    private final long walWriterDataAppendPageSize;
    private final long walWriterEventAppendPageSize;
    private final int walWriterPoolMaxSegments;
    private final long workStealTimeoutNanos;
    private final long writerAsyncCommandBusyWaitTimeout;
    private final long writerAsyncCommandMaxWaitTimeout;
    private final int writerAsyncCommandQueueCapacity;
    private final long writerAsyncCommandQueueSlotSize;
    private final long writerDataAppendPageSize;
    private final long writerDataIndexKeyAppendPageSize;
    private final long writerDataIndexValueAppendPageSize;
    private final int writerFileOpenOpts;
    private final long writerMiscAppendPageSize;
    private final boolean writerMixedIOEnabled;
    private final int writerTickRowsCountMod;
    protected HttpServerConfiguration httpMinServerConfiguration = new PropHttpMinServerConfiguration();
    protected HttpFullFatServerConfiguration httpServerConfiguration = new PropHttpServerConfiguration();
    protected JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new PropJsonQueryProcessorConfiguration();
    protected StaticContentProcessorConfiguration staticContentProcessorConfiguration;
    protected long walSegmentRolloverSize;
    private int cairoSqlColumnAliasGeneratedMaxSize;
    private long cairoSqlCopyMaxIndexChunkSize;
    private FactoryProvider factoryProvider;
    private short floatDefaultColumnType;
    private int httpMinBindIPv4Address;
    private int httpMinBindPort;
    private long httpMinNetAcceptLoopTimeout;
    private boolean httpMinNetConnectionHint;
    private int httpMinNetConnectionLimit;
    private long httpMinNetConnectionQueueTimeout;
    private int httpMinNetConnectionRcvBuf;
    private int httpMinNetConnectionSndBuf;
    private long httpMinNetConnectionTimeout;
    private int httpMinRecvBufferSize;
    private int httpMinSendBufferSize;
    private int[] httpMinWorkerAffinity;
    private int httpMinWorkerCount;
    private boolean httpMinWorkerHaltOnError;
    private long httpMinWorkerNapThreshold;
    private int httpMinWorkerPoolPriority;
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
    private int httpRecvBufferSize;
    private short integerDefaultColumnType;
    private int jsonQueryConnectionCheckFrequency;
    private int lineDefaultTimestampColumnType;
    private boolean lineLogMessageOnError;
    private long lineTcpCommitIntervalDefault;
    private double lineTcpCommitIntervalFraction;
    private int lineTcpConnectionPoolInitialCapacity;
    private int lineTcpDefaultPartitionBy;
    private boolean lineTcpDisconnectOnError;
    private int[] lineTcpIOWorkerAffinity;
    private int lineTcpIOWorkerCount;
    private long lineTcpIOWorkerNapThreshold;
    private boolean lineTcpIOWorkerPoolHaltOnError;
    private long lineTcpIOWorkerSleepThreshold;
    private long lineTcpIOWorkerYieldThreshold;
    private long lineTcpMaintenanceInterval;
    private int lineTcpMaxMeasurementSize;
    private long lineTcpMaxRecvBufferSize;
    private long lineTcpNetAcceptLoopTimeout;
    private int lineTcpNetBindIPv4Address;
    private int lineTcpNetBindPort;
    private long lineTcpNetConnectionHeartbeatInterval;
    private boolean lineTcpNetConnectionHint;
    private int lineTcpNetConnectionLimit;
    private long lineTcpNetConnectionQueueTimeout;
    private int lineTcpNetConnectionRcvBuf;
    private long lineTcpNetConnectionTimeout;
    private int lineTcpRecvBufferSize;
    private byte lineTcpTimestampUnit;
    private int lineTcpWriterQueueCapacity;
    private int[] lineTcpWriterWorkerAffinity;
    private int lineTcpWriterWorkerCount;
    private long lineTcpWriterWorkerNapThreshold;
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
    private int pgForceRecvFragmentationChunkSize;
    private int pgForceSendFragmentationChunkSize;
    private boolean pgHaltOnError;
    private int pgInsertCacheBlockCount;
    private boolean pgInsertCacheEnabled;
    private int pgInsertCacheRowCount;
    private int pgMaxBlobSizeOnQuery;
    private int pgNamedStatementCacheCapacity;
    private int pgNamedStatementLimit;
    private int pgNamesStatementPoolCapacity;
    private long pgNetAcceptLoopTimeout;
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
    private int pgPipelineCapacity;
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
    private long pgWorkerNapThreshold;
    private long pgWorkerSleepThreshold;
    private long pgWorkerYieldThreshold;
    private long queryTimeout;
    private boolean stringToCharCastAllowed;
    private long symbolCacheWaitBeforeReload;

    public PropServerConfiguration(
            String installRoot,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        this(
                installRoot,
                properties,
                null,
                env,
                log,
                buildInformation,
                FilesFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                (configuration, engine, freeOnExitList) -> DefaultFactoryProvider.INSTANCE,
                true
        );
    }

    public PropServerConfiguration(
            String installRoot,
            Properties properties,
            @Nullable Set<? extends ConfigPropertyKey> dynamicProperties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf
    ) throws ServerConfigurationException, JsonException {
        this(
                installRoot,
                properties,
                dynamicProperties,
                env,
                log,
                buildInformation,
                filesFacade,
                microsecondClock,
                fpf,
                true
        );
    }

    public PropServerConfiguration(
            String installRoot,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf
    ) throws ServerConfigurationException, JsonException {
        this(
                installRoot,
                properties,
                null,
                env,
                log,
                buildInformation,
                filesFacade,
                microsecondClock,
                fpf,
                true
        );
    }

    public PropServerConfiguration(
            String installRoot,
            Properties properties,
            @Nullable Set<? extends ConfigPropertyKey> dynamicProperties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf,
            boolean loadAdditionalConfigurations
    ) throws ServerConfigurationException, JsonException {
        this.log = log;
        this.metricsEnabled = getBoolean(properties, env, PropertyKey.METRICS_ENABLED, false);
        this.metrics = metricsEnabled ? new Metrics(true, new MetricsRegistryImpl()) : Metrics.DISABLED;
        this.logSqlQueryProgressExe = getBoolean(properties, env, PropertyKey.LOG_SQL_QUERY_PROGRESS_EXE, true);
        this.logLevelVerbose = getBoolean(properties, env, PropertyKey.LOG_LEVEL_VERBOSE, false);
        this.logTimestampTimezone = getString(properties, env, PropertyKey.LOG_TIMESTAMP_TIMEZONE, "Z");
        final String logTimestampFormatStr = getString(properties, env, PropertyKey.LOG_TIMESTAMP_FORMAT, "yyyy-MM-ddTHH:mm:ss.SSSUUUz");
        final String logTimestampLocaleStr = getString(properties, env, PropertyKey.LOG_TIMESTAMP_LOCALE, "en");
        this.logTimestampLocale = DateLocaleFactory.INSTANCE.getLocale(logTimestampLocaleStr);
        if (logTimestampLocale == null) {
            throw new ServerConfigurationException("Invalid log locale: '" + logTimestampLocaleStr + "'");
        }
        this.logTimestampFormat = MicrosTimestampDriver.INSTANCE.getTimestampDateFormatFactory().get(logTimestampFormatStr);
        try {
            this.logTimestampTimezoneRules = Micros.getTimezoneRules(logTimestampLocale, logTimestampTimezone);
        } catch (NumericException e) {
            throw new ServerConfigurationException("Invalid log timezone: '" + logTimestampTimezone + "'");
        }
        this.filesFacade = filesFacade;
        this.fpf = fpf;
        this.microsecondClock = microsecondClock;
        this.validator = newValidator();
        this.staticContentProcessorConfiguration = new PropStaticContentProcessorConfiguration();
        this.dynamicProperties = dynamicProperties;
        boolean configValidationStrict = getBoolean(properties, env, PropertyKey.CONFIG_VALIDATION_STRICT, false);
        validateProperties(properties, configValidationStrict);

        this.memoryConfiguration = new MemoryConfigurationImpl(
                getLongSize(properties, env, PropertyKey.RAM_USAGE_LIMIT_BYTES, 0),
                getIntPercentage(properties, env, PropertyKey.RAM_USAGE_LIMIT_PERCENT, 90)
        );
        this.isReadOnlyInstance = getBoolean(properties, env, PropertyKey.READ_ONLY_INSTANCE, false);
        this.isQueryTracingEnabled = getBoolean(properties, env, PropertyKey.QUERY_TRACING_ENABLED, false);
        this.cairoTableRegistryAutoReloadFrequency = getMillis(properties, env, PropertyKey.CAIRO_TABLE_REGISTRY_AUTO_RELOAD_FREQUENCY, 500);
        this.cairoTableRegistryCompactionThreshold = getInt(properties, env, PropertyKey.CAIRO_TABLE_REGISTRY_COMPACTION_THRESHOLD, 30);
        this.cairoWriteBackOffTimeoutOnMemPressureMs = getMillis(properties, env, PropertyKey.CAIRO_WRITE_BACK_OFF_TIMEOUT_ON_MEM_PRESSURE, 4000);
        this.repeatMigrationFromVersion = getInt(properties, env, PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, 426);
        this.mkdirMode = getInt(properties, env, PropertyKey.CAIRO_MKDIR_MODE, 509);
        this.maxFileNameLength = getInt(properties, env, PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH, 127);
        // changing the default value of walEnabledDefault to true would mean that QuestDB instances upgraded from
        // a pre-WAL version suddenly would start to create WAL tables by default, this could come as a surprise to users
        // instead cairo.wal.enabled.default=true is added to the config, so only new QuestDB installations have WAL enabled by default
        this.walEnabledDefault = getBoolean(properties, env, PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
        this.walPurgeInterval = getMillis(properties, env, PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 30_000);
        this.matViewRefreshIntervalsUpdatePeriod = getMillis(properties, env, PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, walPurgeInterval / 2);
        this.matViewMaxRefreshStepUs = getMicros(properties, env, PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_STEP, Micros.YEAR_MICROS_NONLEAP);
        this.matViewRefreshMissingWalFilesFatal = getBoolean(properties, env, PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, false);
        this.walPurgeWaitBeforeDelete = getInt(properties, env, PropertyKey.DEBUG_WAL_PURGE_WAIT_BEFORE_DELETE, 0);
        this.walTxnNotificationQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY, 4096);
        this.walRecreateDistressedSequencerAttempts = getInt(properties, env, PropertyKey.CAIRO_WAL_RECREATE_DISTRESSED_SEQUENCER_ATTEMPTS, 3);
        this.walSupported = getBoolean(properties, env, PropertyKey.CAIRO_WAL_SUPPORTED, true);
        walApplyEnabled = getBoolean(properties, env, PropertyKey.CAIRO_WAL_APPLY_ENABLED, true);
        this.walSegmentRolloverRowCount = getLong(properties, env, PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 200_000);
        this.walSegmentRolloverSize = getLongSize(properties, env, PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_SIZE, 50 * Numbers.SIZE_1MB);
        if ((this.walSegmentRolloverSize != 0) && (this.walSegmentRolloverSize < 1024)) {  // 1KiB segments minimum
            throw CairoException.critical(0).put("cairo.wal.segment.rollover.size must be 0 (disabled) or >= 1024 (1KiB)");
        }
        this.walWriterDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, Numbers.SIZE_1MB));
        this.walWriterEventAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WAL_WRITER_EVENT_APPEND_PAGE_SIZE, 128 * 1024));
        this.systemWalWriterDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_SYSTEM_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 256 * 1024));
        this.systemWalWriterEventAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_SYSTEM_WAL_WRITER_EVENT_APPEND_PAGE_SIZE, 16 * 1024));
        this.walSquashUncommittedRowsMultiplier = getDouble(properties, env, PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, "20.0");
        this.walMaxLagTxnCount = getInt(properties, env, PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        this.debugWalApplyBlockFailureNoRetry = getBoolean(properties, env, PropertyKey.DEBUG_WAL_APPLY_BLOCK_FAILURE_NO_RETRY, false);
        this.walMaxLagSize = getLongSize(properties, env, PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 75 * Numbers.SIZE_1MB);
        this.walMaxSegmentFileDescriptorsCache = getInt(properties, env, PropertyKey.CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE, 30);
        this.walApplyTableTimeQuota = getMillis(properties, env, PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 1000);
        this.walApplyLookAheadTransactionCount = getInt(properties, env, PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 200);
        this.tableTypeConversionEnabled = getBoolean(properties, env, PropertyKey.TABLE_TYPE_CONVERSION_ENABLED, true);
        this.tempRenamePendingTablePrefix = getString(properties, env, PropertyKey.CAIRO_WAL_TEMP_PENDING_RENAME_TABLE_PREFIX, "temp_5822f658-31f6-11ee-be56-0242ac120002");
        this.sequencerCheckInterval = getMillis(properties, env, PropertyKey.CAIRO_WAL_SEQUENCER_CHECK_INTERVAL, 10_000);
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
        this.cairoResourcePoolTracingEnabled = getBoolean(properties, env, PropertyKey.CAIRO_RESOURCE_POOL_TRACING_ENABLED, false);

        this.installRoot = installRoot;
        this.dbDirectory = getString(properties, env, PropertyKey.CAIRO_ROOT, DB_DIRECTORY);
        this.dbLogName = getString(properties, env, PropertyKey.DEBUG_DB_LOG_NAME, null);
        String tmpRoot;
        boolean absDbDir = new File(this.dbDirectory).isAbsolute();
        if (absDbDir) {
            this.dbRoot = this.dbDirectory;
            this.confRoot = rootSubdir(this.dbRoot, CONFIG_DIRECTORY); // ../conf
            this.checkpointRoot = rootSubdir(this.dbRoot, TableUtils.CHECKPOINT_DIRECTORY); // ../.checkpoint
            this.legacyCheckpointRoot = rootSubdir(this.dbRoot, TableUtils.LEGACY_CHECKPOINT_DIRECTORY);
            tmpRoot = rootSubdir(this.dbRoot, TMP_DIRECTORY); // ../tmp
        } else {
            this.dbRoot = new File(installRoot, this.dbDirectory).getAbsolutePath();
            this.confRoot = new File(installRoot, CONFIG_DIRECTORY).getAbsolutePath();
            this.checkpointRoot = new File(installRoot, TableUtils.CHECKPOINT_DIRECTORY).getAbsolutePath();
            this.legacyCheckpointRoot = new File(installRoot, TableUtils.LEGACY_CHECKPOINT_DIRECTORY).getAbsolutePath();
            tmpRoot = new File(installRoot, TMP_DIRECTORY).getAbsolutePath();
        }


        String configuredCairoSqlCopyRoot = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_ROOT, "import");
        if (!Chars.empty(configuredCairoSqlCopyRoot)) {
            if (new File(configuredCairoSqlCopyRoot).isAbsolute()) {
                this.cairoSqlCopyRoot = configuredCairoSqlCopyRoot;
            } else {
                if (absDbDir) {
                    this.cairoSqlCopyRoot = rootSubdir(this.dbRoot, configuredCairoSqlCopyRoot); // ../import
                } else {
                    this.cairoSqlCopyRoot = new File(installRoot, configuredCairoSqlCopyRoot).getAbsolutePath();
                }
            }
            String cairoSqlCopyWorkRoot = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_WORK_ROOT, tmpRoot);
            this.cairoSqlCopyWorkRoot = getCanonicalPath(cairoSqlCopyWorkRoot);
            if (pathEquals(installRoot, this.cairoSqlCopyWorkRoot)
                    || pathEquals(this.dbRoot, this.cairoSqlCopyWorkRoot)
                    || pathEquals(this.confRoot, this.cairoSqlCopyWorkRoot)
                    || pathEquals(this.checkpointRoot, this.cairoSqlCopyWorkRoot)) {
                throw new ServerConfigurationException(
                        "Configuration value for "
                                + PropertyKey.CAIRO_SQL_COPY_WORK_ROOT.getPropertyPath()
                                + " can't point to root, data, conf or snapshot dirs."
                );
            }
        } else {
            this.cairoSqlCopyRoot = null;
            this.cairoSqlCopyWorkRoot = null;
        }

        String configuredCairoSqlCopyExportRoot = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, "export");
        if (!Chars.empty(configuredCairoSqlCopyExportRoot)) {
            if (new File(configuredCairoSqlCopyExportRoot).isAbsolute()) {
                this.cairoSqlCopyExportRoot = configuredCairoSqlCopyExportRoot;
            } else {
                if (absDbDir) {
                    this.cairoSqlCopyExportRoot = rootSubdir(this.dbRoot, configuredCairoSqlCopyExportRoot); // ../export
                } else {
                    this.cairoSqlCopyExportRoot = new File(installRoot, configuredCairoSqlCopyExportRoot).getAbsolutePath();
                }
            }
        } else {
            this.cairoSqlCopyExportRoot = null;
        }

        this.cairoAttachPartitionSuffix = getString(properties, env, PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX, TableUtils.ATTACHABLE_DIR_MARKER);
        this.cairoAttachPartitionCopy = getBoolean(properties, env, PropertyKey.CAIRO_ATTACH_PARTITION_COPY, false);
        this.cairoCommitLatency = getMicros(properties, env, PropertyKey.CAIRO_COMMIT_LATENCY, 30_000_000);
        // opt-in only in this version
        // when symbol capacity is changed on-the-fly, symbol table files are copy-on-written (if that's a thing) and
        // this is when their version moves. This new version is stored in a NEW slot, in column version file.
        // This slot WILL NOT be read by older versions, hence rolling back QuestDB version will be problematic, but not
        // impossible. Old version will not find new files, the new files will have to be renamed manually. To avoid
        // possible rollback havoc, we have auto-scaling as opt-in. It will be opt-out in the release after 9.1.1
        this.cairoAutoScaleSymbolCapacity = getBoolean(properties, env, PropertyKey.CAIRO_AUTO_SCALE_SYMBOL_CAPACITY, true);
        this.cairoAutoScaleSymbolCapacityThreshold = getDouble(properties, env, PropertyKey.CAIRO_AUTO_SCALE_SYMBOL_CAPACITY_THRESHOLD, "0.8");
        if (cairoAutoScaleSymbolCapacityThreshold <= 0 || !Double.isFinite(cairoAutoScaleSymbolCapacityThreshold)) {
            throw new ServerConfigurationException("Configuration value for " + PropertyKey.CAIRO_AUTO_SCALE_SYMBOL_CAPACITY_THRESHOLD.getPropertyPath() + " has to be a positive non-zero real number.");
        }

        this.snapshotInstanceId = getString(properties, env, PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "");
        this.checkpointRecoveryEnabled = getBoolean(
                properties,
                env,
                PropertyKey.CAIRO_LEGACY_SNAPSHOT_RECOVERY_ENABLED,
                getBoolean(
                        properties,
                        env,
                        PropertyKey.CAIRO_CHECKPOINT_RECOVERY_ENABLED,
                        true
                )
        );
        this.checkpointRecoveryRebuildColumnIndexes = getBoolean(properties, env, PropertyKey.CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES, false);
        int checkpointRecoveryThreadpoolMinRaw = getInt(properties, env, PropertyKey.CAIRO_CHECKPOINT_RECOVERY_THREADPOOL_MIN, 4);
        int checkpointRecoveryThreadpoolMaxRaw = getInt(properties, env, PropertyKey.CAIRO_CHECKPOINT_RECOVERY_THREADPOOL_MAX, 12);
        if (checkpointRecoveryThreadpoolMinRaw < 2 || checkpointRecoveryThreadpoolMinRaw > 32) {
            throw new ServerConfigurationException(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_THREADPOOL_MIN.getPropertyPath() + " must be between 2 and 32");
        }
        if (checkpointRecoveryThreadpoolMaxRaw < 2 || checkpointRecoveryThreadpoolMaxRaw > 32) {
            throw new ServerConfigurationException(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_THREADPOOL_MAX.getPropertyPath() + " must be between 2 and 32");
        }
        if (checkpointRecoveryThreadpoolMinRaw > checkpointRecoveryThreadpoolMaxRaw) {
            throw new ServerConfigurationException(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_THREADPOOL_MIN.getPropertyPath() + " must be less than or equal to " + PropertyKey.CAIRO_CHECKPOINT_RECOVERY_THREADPOOL_MAX.getPropertyPath());
        }
        this.checkpointRecoveryThreadpoolMin = checkpointRecoveryThreadpoolMinRaw;
        this.checkpointRecoveryThreadpoolMax = checkpointRecoveryThreadpoolMaxRaw;
        this.devModeEnabled = getBoolean(properties, env, PropertyKey.DEV_MODE_ENABLED, false);

        int cpuAvailable = Runtime.getRuntime().availableProcessors();
        int cpuWalApplyWorkers = 2;
        int cpuExportWorkers = 1;
        int cpuSpare = 0;

        if (cpuAvailable > 32) {
            cpuWalApplyWorkers = 4;
            cpuExportWorkers = 4;
            cpuSpare = 2;
        } else if (cpuAvailable > 16) {
            cpuWalApplyWorkers = 3;
            cpuExportWorkers = 2;
            cpuSpare = 1;
        } else if (cpuAvailable > 8) {
            cpuWalApplyWorkers = 3;
        }

        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        try (Path path = new Path()) {
            volumeDefinitions.of(getString(properties, env, PropertyKey.CAIRO_VOLUMES, null), path, installRoot);
            ff.mkdirs(path.of(this.dbRoot).slash(), this.mkdirMode);
            path.of(this.dbRoot).concat(TableUtils.TAB_INDEX_FILE_NAME);
            final long tableIndexFd = TableUtils.openFileRWOrFail(ff, path.$(), CairoConfiguration.O_NONE);
            try {
                final long fileSize = ff.length(tableIndexFd);
                if (fileSize < Long.BYTES) {
                    if (!ff.allocate(tableIndexFd, Files.PAGE_SIZE)) {
                        throw CairoException.critical(ff.errno())
                                .put("Could not allocate [file=").put(path)
                                .put(", actual=").put(fileSize)
                                .put(", desired=").put(Files.PAGE_SIZE).put(']');
                    }
                }
                final long tableIndexMem = TableUtils.mapRW(ff, tableIndexFd, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
                try {
                    Rnd rnd = new Rnd(cairoConfiguration.getMicrosecondClock().getTicks(), cairoConfiguration.getMillisecondClock().getTicks());
                    if (Os.compareAndSwap(tableIndexMem + Long.BYTES, 0, rnd.nextLong()) == 0) {
                        Unsafe.getUnsafe().putLong(tableIndexMem + Long.BYTES * 2, rnd.nextLong());
                    }
                    this.instanceHashLo = Unsafe.getUnsafe().getLong(tableIndexMem + Long.BYTES);
                    this.instanceHashHi = Unsafe.getUnsafe().getLong(tableIndexMem + Long.BYTES * 2);
                } finally {
                    ff.munmap(tableIndexMem, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
                }
            } finally {
                ff.close(tableIndexFd);
            }

            this.httpMinServerEnabled = getBoolean(properties, env, PropertyKey.HTTP_MIN_ENABLED, true);
            if (httpMinServerEnabled) {
                this.httpMinWorkerHaltOnError = getBoolean(properties, env, PropertyKey.HTTP_MIN_WORKER_HALT_ON_ERROR, false);
                this.httpMinWorkerCount = getInt(properties, env, PropertyKey.HTTP_MIN_WORKER_COUNT, 1);

                final int httpMinWorkerPoolPriority = getInt(properties, env, PropertyKey.HTTP_MIN_WORKER_POOL_PRIORITY, Thread.MAX_PRIORITY - 2);
                this.httpMinWorkerPoolPriority = Math.min(Thread.MAX_PRIORITY, Math.max(Thread.MIN_PRIORITY, httpMinWorkerPoolPriority));

                this.httpMinWorkerAffinity = getAffinity(properties, env, PropertyKey.HTTP_MIN_WORKER_AFFINITY, httpMinWorkerCount);
                this.httpMinWorkerYieldThreshold = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_YIELD_THRESHOLD, 10);
                this.httpMinWorkerNapThreshold = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_NAP_THRESHOLD, 100);
                this.httpMinWorkerSleepThreshold = getLong(properties, env, PropertyKey.HTTP_MIN_WORKER_SLEEP_THRESHOLD, 100);
                this.httpMinWorkerSleepTimeout = getMillis(properties, env, PropertyKey.HTTP_MIN_WORKER_SLEEP_TIMEOUT, 50);

                // deprecated
                String httpMinBindTo = getString(properties, env, PropertyKey.HTTP_MIN_BIND_TO, "0.0.0.0:9003");

                parseBindTo(properties, env, PropertyKey.HTTP_MIN_NET_BIND_TO, httpMinBindTo, (a, p) -> {
                    httpMinBindIPv4Address = a;
                    httpMinBindPort = p;
                });

                this.httpMinNetAcceptLoopTimeout = getMillis(properties, env, PropertyKey.HTTP_MIN_NET_ACCEPT_LOOP_TIMEOUT, 500);
                this.httpMinNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_LIMIT, 64);

                // deprecated
                this.httpMinNetConnectionTimeout = getMillis(properties, env, PropertyKey.HTTP_MIN_NET_IDLE_CONNECTION_TIMEOUT, 5 * 60 * 1000L);
                this.httpMinNetConnectionTimeout = getMillis(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_TIMEOUT, this.httpMinNetConnectionTimeout);

                // deprecated
                this.httpMinNetConnectionQueueTimeout = getMillis(properties, env, PropertyKey.HTTP_MIN_NET_QUEUED_CONNECTION_TIMEOUT, 5 * 1000L);
                this.httpMinNetConnectionQueueTimeout = getMillis(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_QUEUE_TIMEOUT, this.httpMinNetConnectionQueueTimeout);

                // deprecated
                this.httpMinNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_MIN_NET_SND_BUF_SIZE, -1);
                this.httpMinNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_SNDBUF, httpMinNetConnectionSndBuf);
                this.httpMinSendBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MIN_SEND_BUFFER_SIZE, 1024);

                // deprecated
                this.httpMinNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_RCV_BUF_SIZE, -1);
                this.httpMinNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_RCVBUF, httpMinNetConnectionRcvBuf);
                // deprecated
                this.httpMinRecvBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MIN_RECEIVE_BUFFER_SIZE, 1024);
                this.httpMinRecvBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MIN_RECV_BUFFER_SIZE, httpMinRecvBufferSize);
                this.httpMinNetConnectionHint = getBoolean(properties, env, PropertyKey.HTTP_MIN_NET_CONNECTION_HINT, false);
            }

            int requestHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_REQUEST_HEADER_BUFFER_SIZE, 32 * 2014);
            this.httpServerEnabled = getBoolean(properties, env, PropertyKey.HTTP_ENABLED, true);
            final int forceSendFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE, Integer.MAX_VALUE);
            final int forceRecvFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE, Integer.MAX_VALUE);
            this.httpWorkerCount = getInt(properties, env, PropertyKey.HTTP_WORKER_COUNT, 0);
            this.httpWorkerAffinity = getAffinity(properties, env, PropertyKey.HTTP_WORKER_AFFINITY, httpWorkerCount);
            this.httpWorkerHaltOnError = getBoolean(properties, env, PropertyKey.HTTP_WORKER_HALT_ON_ERROR, false);
            this.httpWorkerYieldThreshold = getLong(properties, env, PropertyKey.HTTP_WORKER_YIELD_THRESHOLD, 10);
            this.httpWorkerNapThreshold = getLong(properties, env, PropertyKey.HTTP_WORKER_NAP_THRESHOLD, 7_000);
            this.httpWorkerSleepThreshold = getLong(properties, env, PropertyKey.HTTP_WORKER_SLEEP_THRESHOLD, 10_000);
            this.httpWorkerSleepTimeout = getMillis(properties, env, PropertyKey.HTTP_WORKER_SLEEP_TIMEOUT, 10);

            this.httpSettingsReadOnly = getBoolean(properties, env, PropertyKey.HTTP_SETTINGS_READONLY, false);

            // context paths
            this.httpContextWebConsole = stripTrailingSlash(getString(properties, env, PropertyKey.HTTP_CONTEXT_WEB_CONSOLE, "/"));
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_ILP, this.httpContextPathILP, "/write", "/api/v2/write");
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_ILP_PING, this.httpContextPathILPPing, "/ping");
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_IMPORT, this.httpContextPathImport, httpContextWebConsole + "/imp");
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_EXPORT, this.httpContextPathExport, httpContextWebConsole + "/exp");
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_SETTINGS, this.httpContextPathSettings, httpContextWebConsole + "/settings");
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_TABLE_STATUS, this.httpContextPathTableStatus, httpContextWebConsole + "/chk");
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_EXECUTE, this.httpContextPathSqlExecute, httpContextWebConsole + "/exec", httpContextWebConsole + "/api/v1/sql/execute");
            getUrls(properties, env, PropertyKey.HTTP_CONTEXT_WARNINGS, this.httpContextPathWarnings, httpContextWebConsole + "/warnings");

            // If any REST services that the Web Console depends on are overridden,
            // ensure the required context paths remain available,
            // so that customization does not break the Web Console.

            // The following paths need to be added for the Web Console to work properly. This
            // deals with the cases where the context path was overridden by the user. Adding duplicate
            // paths is ok because duplicates are squashed by the HTTP server.
            // 1. import, to support CSV import UI
            // 2. export, to support CSV export UI
            // 3. settings, a union of selected config properties and preferences,
            //     the Web Console loads it on startup, the preferences part can be updated via POST/PUT
            // 4. table status, to support CSV import UI
            // 5. JSON query execution, e.g. exec
            // 6. warnings, that displays warnings in the table view

            // we use defaults, because this is what the Web Console expects
            httpContextPathImport.add(httpContextWebConsole + "/imp");
            httpContextPathExport.add(httpContextWebConsole + "/exp");
            httpContextPathSettings.add(httpContextWebConsole + "/settings");
            httpContextPathTableStatus.add(httpContextWebConsole + "/chk");
            httpContextPathSqlExecute.add(httpContextWebConsole + "/exec");
            httpContextPathSqlExecute.add(httpContextWebConsole + "/api/v1/sql/execute");
            httpContextPathSqlValidate.add(httpContextWebConsole + "/api/v1/sql/validate");
            httpContextPathWarnings.add(httpContextWebConsole + "/warnings");

            // read the redirect map
            this.redirectMap = new Utf8SequenceObjHashMap<>();
            int redirectCount = getInt(properties, env, PropertyKey.HTTP_REDIRECT_COUNT, 0);
            if (redirectCount > 0) {
                // read the redirect map
                for (int i = 0; i < redirectCount; i++) {
                    // all redirect values must be read to reconcile with the count
                    final RedirectPropertyKey key = new RedirectPropertyKey(i + 1);
                    String redirectConfig = getString(properties, env, key, null);
                    if (redirectConfig != null) {
                        String[] parts = redirectConfig.split("->");
                        if (parts.length == 2) {
                            String from = parts[0].trim();
                            String to = parts[1].trim();
                            if (!from.isEmpty() && !to.isEmpty()) {
                                redirectMap.put(new Utf8String(from), new Utf8String(to));
                            }
                        } else {
                            throw new ServerConfigurationException("could not parse redirect value [key=" + key.getPropertyPath() + ", value=" + redirectConfig + ']');
                        }
                    } else {
                        throw new ServerConfigurationException("undefined redirect value [" + key.getPropertyPath() + "]");
                    }
                }
            }
            // also add web console redirect for custom context
            Utf8String redirectTarget = new Utf8String(httpContextWebConsole + "/index.html");
            redirectMap.put(new Utf8String(httpContextWebConsole), redirectTarget);
            redirectMap.put(new Utf8String(httpContextWebConsole + "/"), redirectTarget);

            String httpVersion = getString(properties, env, PropertyKey.HTTP_VERSION, "HTTP/1.1");
            if (!httpVersion.endsWith(" ")) {
                httpVersion += ' ';
            }
            this.httpFrozenClock = getBoolean(properties, env, PropertyKey.HTTP_FROZEN_CLOCK, false);
            int httpForceSendFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE, forceSendFragmentationChunkSize);
            int httpForceRecvFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE, forceRecvFragmentationChunkSize);

            int connectionStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_CONNECTION_STRING_POOL_CAPACITY, 128);
            int connectionPoolInitialCapacity = getInt(properties, env, PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY, 4);
            int multipartHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MULTIPART_HEADER_BUFFER_SIZE, 512);
            long multipartIdleSpinCount = getLong(properties, env, PropertyKey.HTTP_MULTIPART_IDLE_SPIN_COUNT, 10_000);
            boolean httpAllowDeflateBeforeSend = getBoolean(properties, env, PropertyKey.HTTP_ALLOW_DEFLATE_BEFORE_SEND, false);
            boolean httpServerKeepAlive = getBoolean(properties, env, PropertyKey.HTTP_SERVER_KEEP_ALIVE, true);
            boolean httpServerCookiesEnabled = getBoolean(properties, env, PropertyKey.HTTP_SERVER_COOKIES_ENABLED, true);
            long httpSessionTimeout = getMicros(properties, env, PropertyKey.HTTP_SESSION_TIMEOUT, 1_800_000_000L);
            boolean httpReadOnlySecurityContext = getBoolean(properties, env, PropertyKey.HTTP_SECURITY_READONLY, false);

            this.httpNetAcceptLoopTimeout = getMillis(properties, env, PropertyKey.HTTP_NET_ACCEPT_LOOP_TIMEOUT, 500);

            // maintain deprecated property name for the time being
            this.httpNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_NET_ACTIVE_CONNECTION_LIMIT, 256);
            this.httpNetConnectionLimit = getInt(properties, env, PropertyKey.HTTP_NET_CONNECTION_LIMIT, httpNetConnectionLimit);

            int httpJsonQueryConnectionLimit = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT, -1);
            int httpIlpConnectionLimit = getInt(properties, env, PropertyKey.HTTP_ILP_CONNECTION_LIMIT, -1);


            // Set the default as -1000 and if it's not set by the user, we override it later
            int httpExportConnectionLimit = getInt(properties, env, PropertyKey.HTTP_EXPORT_CONNECTION_LIMIT, -1000);
            // Check the limits are in valid range, without setting final value of httpExportConnectionLimit
            // So if someone has the json, ilp limits set the new limit does not throw a new validation exception here when export limit is introduced
            validateHttpConnectionLimits(httpJsonQueryConnectionLimit, httpIlpConnectionLimit, httpExportConnectionLimit, httpNetConnectionLimit);

            if (httpExportConnectionLimit == -1000) {
                // We set the limit so that parquet exports don't kill web console queries if the limit is not set by the user
                int httpExportConnectionLimitDefault = Math.min(cpuAvailable, Math.max(httpNetConnectionLimit / 4, 1));
                // We have to use `getInt()` method so that the SHOW PROPERTIES shows the correct default value
                httpExportConnectionLimit = getInt(properties, env, PropertyKey.HTTP_EXPORT_CONNECTION_LIMIT, httpExportConnectionLimitDefault);
            }

            httpContextConfiguration = new PropHttpContextConfiguration(
                    connectionPoolInitialCapacity,
                    connectionStringPoolCapacity,
                    this,
                    httpAllowDeflateBeforeSend,
                    httpForceRecvFragmentationChunkSize,
                    httpForceSendFragmentationChunkSize,
                    httpFrozenClock,
                    httpReadOnlySecurityContext,
                    httpServerCookiesEnabled,
                    httpSessionTimeout,
                    httpServerKeepAlive,
                    httpVersion,
                    isReadOnlyInstance,
                    multipartHeaderBufferSize,
                    multipartIdleSpinCount,
                    requestHeaderBufferSize,
                    httpJsonQueryConnectionLimit,
                    httpIlpConnectionLimit,
                    httpExportConnectionLimit
            );

            // Use a separate configuration for min server. It does not make sense for the min server to grow the buffer sizes together with the main http server
            int minHttpConnectionStringPoolCapacity = getInt(properties, env, PropertyKey.HTTP_MIN_CONNECTION_STRING_POOL_CAPACITY, 2);
            int minHttpConnectionPoolInitialCapacity = getInt(properties, env, PropertyKey.HTTP_MIN_CONNECTION_POOL_INITIAL_CAPACITY, 2);
            int minHttpMultipartHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MIN_MULTIPART_HEADER_BUFFER_SIZE, 512);
            long minHttpMultipartIdleSpinCount = getLong(properties, env, PropertyKey.HTTP_MIN_MULTIPART_IDLE_SPIN_COUNT, 0);
            boolean minHttpAllowDeflateBeforeSend = getBoolean(properties, env, PropertyKey.HTTP_MIN_ALLOW_DEFLATE_BEFORE_SEND, false);
            boolean minHttpMinServerKeepAlive = getBoolean(properties, env, PropertyKey.HTTP_MIN_SERVER_KEEP_ALIVE, true);
            int httpMinRequestHeaderBufferSize = getIntSize(properties, env, PropertyKey.HTTP_MIN_REQUEST_HEADER_BUFFER_SIZE, 4096);

            httpMinContextConfiguration = new PropHttpContextConfiguration(
                    minHttpConnectionStringPoolCapacity,
                    minHttpConnectionPoolInitialCapacity,
                    this,
                    minHttpAllowDeflateBeforeSend,
                    httpForceRecvFragmentationChunkSize,
                    httpForceSendFragmentationChunkSize,
                    httpFrozenClock,
                    true,
                    httpServerCookiesEnabled,
                    httpSessionTimeout,
                    minHttpMinServerKeepAlive,
                    httpVersion,
                    isReadOnlyInstance,
                    minHttpMultipartHeaderBufferSize,
                    minHttpMultipartIdleSpinCount,
                    httpMinRequestHeaderBufferSize
            );

            int keepAliveTimeout = getInt(properties, env, PropertyKey.HTTP_KEEP_ALIVE_TIMEOUT, 5);
            int keepAliveMax = getInt(properties, env, PropertyKey.HTTP_KEEP_ALIVE_MAX, 10_000);

            if (keepAliveTimeout > 0 && keepAliveMax > 0) {
                this.keepAliveHeader = "Keep-Alive: timeout=" + keepAliveTimeout + ", max=" + keepAliveMax + Misc.EOL;
            } else {
                this.keepAliveHeader = null;
            }

            final String publicDirectory = getString(properties, env, PropertyKey.HTTP_STATIC_PUBLIC_DIRECTORY, "public");
            // translate public directory into an absolute path
            // this will generate some garbage, but this is ok - we're just doing this once on startup
            if (new File(publicDirectory).isAbsolute()) {
                this.publicDirectory = publicDirectory;
            } else {
                this.publicDirectory = new File(installRoot, publicDirectory).getAbsolutePath();
            }

            this.defaultSeqPartTxnCount = getInt(properties, env, PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
            this.httpNetConnectionHint = getBoolean(properties, env, PropertyKey.HTTP_NET_CONNECTION_HINT, false);
            // deprecated
            this.httpNetConnectionTimeout = getMillis(properties, env, PropertyKey.HTTP_NET_IDLE_CONNECTION_TIMEOUT, 5 * 60 * 1000L);
            this.httpNetConnectionTimeout = getMillis(properties, env, PropertyKey.HTTP_NET_CONNECTION_TIMEOUT, this.httpNetConnectionTimeout);

            // deprecated
            this.httpNetConnectionQueueTimeout = getMillis(properties, env, PropertyKey.HTTP_NET_QUEUED_CONNECTION_TIMEOUT, 5 * 1000L);
            this.httpNetConnectionQueueTimeout = getMillis(properties, env, PropertyKey.HTTP_NET_CONNECTION_QUEUE_TIMEOUT, this.httpNetConnectionQueueTimeout);

            // deprecated
            this.httpNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_SND_BUF_SIZE, -1);
            this.httpNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_CONNECTION_SNDBUF, httpNetConnectionSndBuf);
            this.httpSendBufferSize = getIntSize(properties, env, PropertyKey.HTTP_SEND_BUFFER_SIZE, 2 * Numbers.SIZE_1MB);

            // deprecated
            this.httpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_RCV_BUF_SIZE, -1);
            this.httpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.HTTP_NET_CONNECTION_RCVBUF, httpNetConnectionRcvBuf);
            this.httpRecvBufferSize = getIntSize(properties, env, PropertyKey.HTTP_RECEIVE_BUFFER_SIZE, 2 * Numbers.SIZE_1MB);
            this.httpRecvBufferSize = getIntSize(properties, env, PropertyKey.HTTP_RECV_BUFFER_SIZE, httpRecvBufferSize);
            this.httpRecvMaxBufferSize = getLongSize(properties, env, PropertyKey.LINE_HTTP_MAX_RECV_BUFFER_SIZE, Numbers.SIZE_1GB);

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
            this.decimalAdapterPoolCapacity = getInt(properties, env, PropertyKey.HTTP_TEXT_DECIMAL_ADAPTER_POOL_CAPACITY, 64);
            this.utf8SinkSize = getIntSize(properties, env, PropertyKey.HTTP_TEXT_UTF8_SINK_SIZE, 4096);

            this.httpPessimisticHealthCheckEnabled = getBoolean(properties, env, PropertyKey.HTTP_PESSIMISTIC_HEALTH_CHECK, false);
            final boolean httpHealthCheckAuthRequired = getBoolean(properties, env, PropertyKey.HTTP_HEALTH_CHECK_AUTHENTICATION_REQUIRED, true);
            this.httpHealthCheckAuthType = httpHealthCheckAuthRequired ? SecurityContext.AUTH_TYPE_CREDENTIALS : SecurityContext.AUTH_TYPE_NONE;
            this.maxHttpQueryResponseRowLimit = getLong(properties, env, PropertyKey.HTTP_SECURITY_MAX_RESPONSE_ROWS, Long.MAX_VALUE);
            this.interruptOnClosedConnection = getBoolean(properties, env, PropertyKey.HTTP_SECURITY_INTERRUPT_ON_CLOSED_CONNECTION, true);
            this.httpUsername = getString(properties, env, PropertyKey.HTTP_USER, "");
            this.httpPassword = getString(properties, env, PropertyKey.HTTP_PASSWORD, "");
            if (!Chars.empty(httpUsername) && Chars.empty(httpPassword)) {
                throw new ServerConfigurationException("HTTP username is set but password is missing. " +
                        "Use the '" + PropertyKey.HTTP_PASSWORD.getPropertyPath() + "' configuration property to set a password. [user=" + httpUsername + "]");
            } else if (Chars.empty(httpUsername) && !Chars.empty(httpPassword)) {
                throw new ServerConfigurationException("HTTP password is set but username is missing. " +
                        "Use the '" + PropertyKey.HTTP_USER.getPropertyPath() + "' configuration property to set a username.");
            }

            if (loadAdditionalConfigurations && httpServerEnabled) {
                this.jsonQueryConnectionCheckFrequency = getInt(properties, env, PropertyKey.HTTP_JSON_QUERY_CONNECTION_CHECK_FREQUENCY, 1_000_000);
                String httpBindTo = getString(properties, env, PropertyKey.HTTP_BIND_TO, "0.0.0.0:9000");
                parseBindTo(properties, env, PropertyKey.HTTP_NET_BIND_TO, httpBindTo, (a, p) -> {
                    httpNetBindIPv4Address = a;
                    httpNetBindPort = p;
                });
                // load mime types
                path.of(new File(new File(installRoot, CONFIG_DIRECTORY), "mime.types").getAbsolutePath());
                this.mimeTypesCache = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path.$());
            }

            this.maxRerunWaitCapMs = getMillis(properties, env, PropertyKey.HTTP_BUSY_RETRY_MAXIMUM_WAIT_BEFORE_RETRY, 1000);
            this.rerunExponentialWaitMultiplier = getDouble(properties, env, PropertyKey.HTTP_BUSY_RETRY_EXPONENTIAL_WAIT_MULTIPLIER, "2.0");
            this.rerunInitialWaitQueueSize = getIntSize(properties, env, PropertyKey.HTTP_BUSY_RETRY_INITIAL_WAIT_QUEUE_SIZE, 64);
            this.rerunMaxProcessingQueueSize = getIntSize(properties, env, PropertyKey.HTTP_BUSY_RETRY_MAX_PROCESSING_QUEUE_SIZE, 4096);

            this.circuitBreakerThrottle = getInt(properties, env, PropertyKey.CIRCUIT_BREAKER_THROTTLE, 2_000_000);
            // obsolete
            this.queryTimeout = (long) (getDouble(properties, env, PropertyKey.QUERY_TIMEOUT_SEC, "60") * Micros.SECOND_MILLIS);
            this.queryTimeout = getMillis(properties, env, PropertyKey.QUERY_TIMEOUT, this.queryTimeout);

            // Make export timeout at least as long as query timeout by default
            this.httpExportTimeout = getMillis(properties, env, PropertyKey.HTTP_EXPORT_TIMEOUT, Math.max(queryTimeout, 300_000));

            this.queryWithinLatestByOptimisationEnabled = getBoolean(properties, env, PropertyKey.QUERY_WITHIN_LATEST_BY_OPTIMISATION_ENABLED, false);
            this.netTestConnectionBufferSize = getInt(properties, env, PropertyKey.CIRCUIT_BREAKER_BUFFER_SIZE, 64);
            this.netTestConnectionBufferSize = getInt(properties, env, PropertyKey.NET_TEST_CONNECTION_BUFFER_SIZE, netTestConnectionBufferSize);

            this.pgEnabled = getBoolean(properties, env, PropertyKey.PG_ENABLED, true);
            if (pgEnabled) {
                this.pgForceSendFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_PG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE, forceSendFragmentationChunkSize);
                this.pgForceRecvFragmentationChunkSize = getInt(properties, env, PropertyKey.DEBUG_PG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE, forceRecvFragmentationChunkSize);

                // deprecated
                pgNetConnectionLimit = getInt(properties, env, PropertyKey.PG_NET_ACTIVE_CONNECTION_LIMIT, 64);
                pgNetConnectionLimit = getInt(properties, env, PropertyKey.PG_NET_CONNECTION_LIMIT, pgNetConnectionLimit);
                pgNetConnectionHint = getBoolean(properties, env, PropertyKey.PG_NET_CONNECTION_HINT, false);
                parseBindTo(properties, env, PropertyKey.PG_NET_BIND_TO, "0.0.0.0:8812", (a, p) -> {
                    pgNetBindIPv4Address = a;
                    pgNetBindPort = p;
                });

                this.pgNetAcceptLoopTimeout = getMillis(properties, env, PropertyKey.PG_NET_ACCEPT_LOOP_TIMEOUT, 500);

                // deprecated
                this.pgNetIdleConnectionTimeout = getMillis(properties, env, PropertyKey.PG_NET_IDLE_TIMEOUT, 300_000);
                this.pgNetIdleConnectionTimeout = getMillis(properties, env, PropertyKey.PG_NET_CONNECTION_TIMEOUT, this.pgNetIdleConnectionTimeout);
                this.pgNetConnectionQueueTimeout = getMillis(properties, env, PropertyKey.PG_NET_CONNECTION_QUEUE_TIMEOUT, 300_000);

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

                // deprecated
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.PG_NET_RECV_BUF_SIZE, -1);
                this.pgNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.PG_NET_CONNECTION_RCVBUF, pgNetConnectionRcvBuf);
                this.pgRecvBufferSize = getIntSize(properties, env, PropertyKey.PG_RECV_BUFFER_SIZE, Numbers.SIZE_1MB);

                // deprecated
                this.pgNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.PG_NET_SEND_BUF_SIZE, -1);
                this.pgNetConnectionSndBuf = getIntSize(properties, env, PropertyKey.PG_NET_CONNECTION_SNDBUF, pgNetConnectionSndBuf);
                this.pgSendBufferSize = getIntSize(properties, env, PropertyKey.PG_SEND_BUFFER_SIZE, Numbers.SIZE_1MB);

                final String dateLocale = getString(properties, env, PropertyKey.PG_DATE_LOCALE, "en");
                this.pgDefaultLocale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
                if (this.pgDefaultLocale == null) {
                    throw ServerConfigurationException.forInvalidKey(PropertyKey.PG_DATE_LOCALE.getPropertyPath(), dateLocale);
                }
                this.pgWorkerCount = getInt(properties, env, PropertyKey.PG_WORKER_COUNT, 0);
                this.pgWorkerAffinity = getAffinity(properties, env, PropertyKey.PG_WORKER_AFFINITY, pgWorkerCount);
                this.pgHaltOnError = getBoolean(properties, env, PropertyKey.PG_HALT_ON_ERROR, false);
                this.pgWorkerYieldThreshold = getLong(properties, env, PropertyKey.PG_WORKER_YIELD_THRESHOLD, 10);
                this.pgWorkerNapThreshold = getLong(properties, env, PropertyKey.PG_WORKER_NAP_THRESHOLD, 7_000);
                this.pgWorkerSleepThreshold = getLong(properties, env, PropertyKey.PG_WORKER_SLEEP_THRESHOLD, 10_000);
                this.pgDaemonPool = getBoolean(properties, env, PropertyKey.PG_DAEMON_POOL, true);
                this.pgInsertCacheEnabled = getBoolean(properties, env, PropertyKey.PG_INSERT_CACHE_ENABLED, true);
                this.pgInsertCacheBlockCount = getInt(properties, env, PropertyKey.PG_INSERT_CACHE_BLOCK_COUNT, 4);
                this.pgInsertCacheRowCount = getInt(properties, env, PropertyKey.PG_INSERT_CACHE_ROW_COUNT, 4);
                this.pgUpdateCacheEnabled = getBoolean(properties, env, PropertyKey.PG_UPDATE_CACHE_ENABLED, true);
                this.pgUpdateCacheBlockCount = getInt(properties, env, PropertyKey.PG_UPDATE_CACHE_BLOCK_COUNT, 4);
                this.pgUpdateCacheRowCount = getInt(properties, env, PropertyKey.PG_UPDATE_CACHE_ROW_COUNT, 4);
                this.pgNamedStatementCacheCapacity = getInt(properties, env, PropertyKey.PG_NAMED_STATEMENT_CACHE_CAPACITY, 32);
                this.pgNamesStatementPoolCapacity = getInt(properties, env, PropertyKey.PG_NAMED_STATEMENT_POOL_CAPACITY, 32);
                this.pgPendingWritersCacheCapacity = getInt(properties, env, PropertyKey.PG_PENDING_WRITERS_CACHE_CAPACITY, 16);
                this.pgNamedStatementLimit = getInt(properties, env, PropertyKey.PG_NAMED_STATEMENT_LIMIT, 10_000);
                this.pgPipelineCapacity = getInt(properties, env, PropertyKey.PG_PIPELINE_CAPACITY, 64);
            }

            // Do not use shared write pool by default for wal-apply
            this.walApplyWorkerCount = getInt(properties, env, PropertyKey.WAL_APPLY_WORKER_COUNT, cpuWalApplyWorkers);
            this.walApplyWorkerAffinity = getAffinity(properties, env, PropertyKey.WAL_APPLY_WORKER_AFFINITY, walApplyWorkerCount);
            this.walApplyWorkerHaltOnError = getBoolean(properties, env, PropertyKey.WAL_APPLY_WORKER_HALT_ON_ERROR, false);
            this.walApplyWorkerNapThreshold = getLong(properties, env, PropertyKey.WAL_APPLY_WORKER_NAP_THRESHOLD, 7_000);
            this.walApplyWorkerSleepThreshold = getLong(properties, env, PropertyKey.WAL_APPLY_WORKER_SLEEP_THRESHOLD, 10_000);
            this.walApplySleepTimeout = getMillis(properties, env, PropertyKey.WAL_APPLY_WORKER_SLEEP_TIMEOUT, 10);
            this.walApplyWorkerYieldThreshold = getLong(properties, env, PropertyKey.WAL_APPLY_WORKER_YIELD_THRESHOLD, 1000);

            // reuse wal-apply defaults for mat view workers
            this.matViewEnabled = getBoolean(properties, env, PropertyKey.CAIRO_MAT_VIEW_ENABLED, true);
            this.matViewMaxRefreshRetries = getInt(properties, env, PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_RETRIES, 10);
            this.matViewRefreshOomRetryTimeout = getMillis(properties, env, PropertyKey.CAIRO_MAT_VIEW_REFRESH_OOM_RETRY_TIMEOUT, 200);
            // Do not use shared write pool by default for mat-view-refresh, use same worker count as wal-apply
            this.matViewRefreshWorkerCount = getInt(properties, env, PropertyKey.MAT_VIEW_REFRESH_WORKER_COUNT, cpuWalApplyWorkers);
            this.matViewRefreshWorkerAffinity = getAffinity(properties, env, PropertyKey.MAT_VIEW_REFRESH_WORKER_AFFINITY, matViewRefreshWorkerCount);
            this.matViewRefreshWorkerHaltOnError = getBoolean(properties, env, PropertyKey.MAT_VIEW_REFRESH_WORKER_HALT_ON_ERROR, false);
            this.matViewRefreshWorkerNapThreshold = getLong(properties, env, PropertyKey.MAT_VIEW_REFRESH_WORKER_NAP_THRESHOLD, 7_000);
            this.matViewRefreshWorkerSleepThreshold = getLong(properties, env, PropertyKey.MAT_VIEW_REFRESH_WORKER_SLEEP_THRESHOLD, 10_000);
            this.matViewRefreshSleepTimeout = getMillis(properties, env, PropertyKey.MAT_VIEW_REFRESH_WORKER_SLEEP_TIMEOUT, 10);
            this.matViewRefreshWorkerYieldThreshold = getLong(properties, env, PropertyKey.MAT_VIEW_REFRESH_WORKER_YIELD_THRESHOLD, 1000);

            this.viewCompilerWorkerCount = getInt(properties, env, PropertyKey.VIEW_COMPILER_WORKER_COUNT, 2);
            this.viewCompilerWorkerNapThreshold = getLong(properties, env, PropertyKey.VIEW_COMPILER_WORKER_NAP_THRESHOLD, 7_000);
            this.viewCompilerWorkerSleepThreshold = getLong(properties, env, PropertyKey.VIEW_COMPILER_WORKER_SLEEP_THRESHOLD, 10_000);
            this.viewCompilerSleepTimeout = getMillis(properties, env, PropertyKey.VIEW_COMPILER_WORKER_SLEEP_TIMEOUT, 10);
            this.viewCompilerWorkerAffinity = getAffinity(properties, env, PropertyKey.VIEW_COMPILER_WORKER_AFFINITY, viewCompilerWorkerCount);
            this.viewCompilerWorkerYieldThreshold = getLong(properties, env, PropertyKey.VIEW_COMPILER_WORKER_YIELD_THRESHOLD, 1000);
            this.viewCompilerWorkerHaltOnError = getBoolean(properties, env, PropertyKey.VIEW_COMPILER_WORKER_HALT_ON_ERROR, false);

            // Export pool configuration
            this.exportWorkerCount = getInt(properties, env, PropertyKey.EXPORT_WORKER_COUNT, cpuExportWorkers);
            this.exportWorkerAffinity = getAffinity(properties, env, PropertyKey.EXPORT_WORKER_AFFINITY, exportWorkerCount);
            this.exportWorkerHaltOnError = getBoolean(properties, env, PropertyKey.EXPORT_WORKER_HALT_ON_ERROR, false);
            this.exportWorkerNapThreshold = getLong(properties, env, PropertyKey.EXPORT_WORKER_NAP_THRESHOLD, 7_000);
            this.exportWorkerSleepThreshold = getLong(properties, env, PropertyKey.EXPORT_WORKER_SLEEP_THRESHOLD, 10_000);
            this.exportWorkerSleepTimeout = getMillis(properties, env, PropertyKey.EXPORT_WORKER_SLEEP_TIMEOUT, 10);
            this.exportWorkerYieldThreshold = getLong(properties, env, PropertyKey.EXPORT_WORKER_YIELD_THRESHOLD, 1000);

            this.commitMode = getCommitMode(properties, env, PropertyKey.CAIRO_COMMIT_MODE);
            this.createAsSelectRetryCount = getInt(properties, env, PropertyKey.CAIRO_CREATE_AS_SELECT_RETRY_COUNT, 5);
            this.defaultSymbolCacheFlag = getBoolean(properties, env, PropertyKey.CAIRO_DEFAULT_SYMBOL_CACHE_FLAG, true);
            this.defaultSymbolCapacity = getInt(properties, env, PropertyKey.CAIRO_DEFAULT_SYMBOL_CAPACITY, 256);
            this.fileOperationRetryCount = getInt(properties, env, PropertyKey.CAIRO_FILE_OPERATION_RETRY_COUNT, 30);
            this.idleCheckInterval = getMillis(properties, env, PropertyKey.CAIRO_IDLE_CHECK_INTERVAL, 5 * 60 * 1000L);
            this.idGenerateBatchStep = getInt(properties, env, PropertyKey.CAIRO_ID_GENERATE_STEP, 512);
            this.inactiveReaderMaxOpenPartitions = getInt(properties, env, PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, 10000);
            this.inactiveReaderTTL = getMillis(properties, env, PropertyKey.CAIRO_INACTIVE_READER_TTL, 120_000);
            this.inactiveWriterTTL = getMillis(properties, env, PropertyKey.CAIRO_INACTIVE_WRITER_TTL, 600_000);
            this.recentWriteTrackerCapacity = getInt(properties, env, PropertyKey.CAIRO_RECENT_WRITE_TRACKER_CAPACITY, 1000);
            this.inactiveWalWriterTTL = getMillis(properties, env, PropertyKey.CAIRO_WAL_INACTIVE_WRITER_TTL, 120_000);
            this.inactiveViewWalWriterTTL = getMillis(properties, env, PropertyKey.CAIRO_VIEW_WAL_INACTIVE_WRITER_TTL, 60_000);
            this.ttlUseWallClock = getBoolean(properties, env, PropertyKey.CAIRO_TTL_USE_WALL_CLOCK, true);
            this.indexValueBlockSize = Numbers.ceilPow2(getIntSize(properties, env, PropertyKey.CAIRO_INDEX_VALUE_BLOCK_SIZE, 256));
            this.maxSwapFileCount = getInt(properties, env, PropertyKey.CAIRO_MAX_SWAP_FILE_COUNT, 30);
            this.parallelIndexThreshold = getInt(properties, env, PropertyKey.CAIRO_PARALLEL_INDEX_THRESHOLD, 100000);
            this.readerPoolMaxSegments = getInt(properties, env, PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS, 10);
            this.poolSegmentSize = getIntSize(properties, env, PropertyKey.DEBUG_CAIRO_POOL_SEGMENT_SIZE, 32);
            this.walWriterPoolMaxSegments = getInt(properties, env, PropertyKey.CAIRO_WAL_WRITER_POOL_MAX_SEGMENTS, 10);
            this.viewWalWriterPoolMaxSegments = getInt(properties, env, PropertyKey.CAIRO_VIEW_WAL_WRITER_POOL_MAX_SEGMENTS, 4);
            this.spinLockTimeout = getMillis(properties, env, PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1_000);
            this.sqlCharacterStoreCapacity = getInt(properties, env, PropertyKey.CAIRO_CHARACTER_STORE_CAPACITY, 1024);
            this.sqlCharacterStoreSequencePoolCapacity = getInt(properties, env, PropertyKey.CAIRO_CHARACTER_STORE_SEQUENCE_POOL_CAPACITY, 64);
            this.sqlColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_COLUMN_POOL_CAPACITY, 4096);
            this.sqlExpressionPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_EXPRESSION_POOL_CAPACITY, 8192);
            this.sqlFastMapLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_FAST_MAP_LOAD_FACTOR, "0.7");
            this.sqlJoinContextPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_JOIN_CONTEXT_POOL_CAPACITY, 64);
            this.sqlLexerPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_LEXER_POOL_CAPACITY, 2048);
            this.sqlSmallMapKeyCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 32);
            this.sqlSmallMapPageSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 32 * 1024);
            this.sqlUnorderedMapMaxEntrySize = getInt(properties, env, PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 32);
            this.sqlMapMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_MAP_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlMapMaxResizes = getIntSize(properties, env, PropertyKey.CAIRO_SQL_MAP_MAX_RESIZES, Integer.MAX_VALUE);
            this.sqlViewLexerPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_VIEW_LEXER_POOL_CAPACITY, 8);
            this.sqlExplainModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_EXPLAIN_MODEL_POOL_CAPACITY, 32);
            this.sqlModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_MODEL_POOL_CAPACITY, 1024);
            this.sqlMaxNegativeLimit = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 10_000);
            this.sqlSortKeyPageSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 128 * 1024);
            this.sqlSortKeyMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlSortLightValuePageSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_PAGE_SIZE, 128 * 1024);
            this.sqlSortLightValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlHashJoinValuePageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 16777216);
            this.sqlHashJoinValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlLatestByRowCount = getInt(properties, env, PropertyKey.CAIRO_SQL_LATEST_BY_ROW_COUNT, 1000);
            this.sqlHashJoinLightValuePageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 128 * 1024);
            this.sqlHashJoinLightValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.sqlAsOfJoinLookahead = getInt(properties, env, PropertyKey.CAIRO_SQL_ASOF_JOIN_LOOKAHEAD, 100);
            this.sqlAsOfJoinShortCircuitCacheCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_ASOF_JOIN_SHORT_CIRCUIT_CACHE_CAPACITY, 10_000_000);
            this.sqlAsOfJoinEvacuationThreshold = getInt(properties, env, PropertyKey.CAIRO_SQL_ASOF_JOIN_EVACUATION_THRESHOLD, 10_000_000);
            this.sqlSortValuePageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 16777216);
            this.sqlSortValueMaxPages = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_PAGES, Integer.MAX_VALUE);
            this.workStealTimeoutNanos = getNanos(properties, env, PropertyKey.CAIRO_WORK_STEAL_TIMEOUT_NANOS, 10_000);
            this.parallelIndexingEnabled = getBoolean(properties, env, PropertyKey.CAIRO_PARALLEL_INDEXING_ENABLED, true);
            this.sqlJoinMetadataPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JOIN_METADATA_PAGE_SIZE, 16384);
            this.sqlJoinMetadataMaxResizes = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JOIN_METADATA_MAX_RESIZES, Integer.MAX_VALUE);
            int sqlWindowColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_COLUMN_POOL_CAPACITY, 64);
            this.sqlWindowColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_WINDOW_COLUMN_POOL_CAPACITY, sqlWindowColumnPoolCapacity);
            this.sqlPivotForColumnPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_PIVOT_COLUMN_POOL_CAPACITY, 8);
            this.sqlPivotMaxProducedColumns = getInt(properties, env, PropertyKey.CAIRO_SQL_PIVOT_MAX_PRODUCED_COLUMNS, 5_000);
            this.sqlCreateTableModelBatchSize = getLong(properties, env, PropertyKey.CAIRO_SQL_CREATE_TABLE_MODEL_BATCH_SIZE, 1_000_000);
            this.sqlCreateTableColumnModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_CREATE_TABLE_COLUMN_MODEL_POOL_CAPACITY, 16);
            this.sqlRenameTableModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_RENAME_TABLE_MODEL_POOL_CAPACITY, 16);
            this.sqlWithClauseModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_WITH_CLAUSE_MODEL_POOL_CAPACITY, 128);
            this.sqlInsertModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_INSERT_MODEL_POOL_CAPACITY, 64);
            this.sqlInsertModelBatchSize = getLong(properties, env, PropertyKey.CAIRO_SQL_INSERT_MODEL_BATCH_SIZE, 1_000_000);
            this.matViewInsertAsSelectBatchSize = getLong(properties, env, PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, sqlInsertModelBatchSize);
            this.matViewRowsPerQueryEstimate = getLong(properties, env, PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1_000_000L);
            this.matViewMaxRefreshIntervals = getInt(properties, env, PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_INTERVALS, 100);
            this.sqlCompileViewModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COMPILE_VIEW_MODEL_POOL_CAPACITY, 8);
            this.sqlCopyBufferSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE, 2 * Numbers.SIZE_1MB);
            this.columnPurgeQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY, 128);
            this.columnPurgeTaskPoolCapacity = getIntSize(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_TASK_POOL_CAPACITY, 256);
            this.columnPurgeRetryDelayLimit = getMicros(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 60_000_000L);
            this.columnPurgeRetryDelay = getMicros(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 10_000);
            this.columnPurgeRetryDelayMultiplier = getDouble(properties, env, PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_MULTIPLIER, "10.0");
            this.systemTableNamePrefix = getString(properties, env, PropertyKey.CAIRO_SQL_SYSTEM_TABLE_PREFIX, "sys.");
            this.parquetExportTableNamePrefix = getString(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_TABLE_PREFIX, "zzz.copy.");
            this.parquetExportCopyReportFrequencyLines = getInt(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_COPY_REPORT_FREQUENCY_LINES, 500_000);
            this.parquetExportVersion = getInt(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_VERSION, ParquetVersion.PARQUET_VERSION_V1);
            this.parquetExportStatisticsEnabled = getBoolean(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_STATISTICS_ENABLED, true);
            this.parquetExportCompressionCodec = ParquetCompression.getCompressionCodec(getString(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_COMPRESSION_CODEC, "LZ4_RAW"));

            this.parquetExportRawArrayEncoding = getBoolean(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_RAW_ARRAY_ENCODING_ENABLED, false);
            int defaultCompressionLevel = parquetExportCompressionCodec == ParquetCompression.COMPRESSION_ZSTD ? 9 : 0;
            this.parquetExportCompressionLevel = getInt(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_COMPRESSION_LEVEL, defaultCompressionLevel);
            this.parquetExportRowGroupSize = getInt(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_ROW_GROUP_SIZE, 100_000);
            this.parquetExportDataPageSize = getInt(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_DATA_PAGE_SIZE, 1_048_576);
            this.parquetExportBatchSize = getLong(properties, env, PropertyKey.CAIRO_PARQUET_EXPORT_BATCH_SIZE, 100_000);
            this.sqlMaxArrayElementCount = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_ARRAY_ELEMENT_COUNT, 10_000_000);
            this.preferencesStringPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_PREFERENCES_STRING_POOL_CAPACITY, 64);

            this.writerDataIndexKeyAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_INDEX_KEY_APPEND_PAGE_SIZE, 512 * 1024));
            this.writerDataIndexValueAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE, 16 * Numbers.SIZE_1MB));
            this.writerDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 16 * Numbers.SIZE_1MB));
            this.systemWriterDataAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_SYSTEM_WRITER_DATA_APPEND_PAGE_SIZE, 256 * 1024));
            this.writerMiscAppendPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_MISC_APPEND_PAGE_SIZE, Files.PAGE_SIZE));
            this.symbolTableMinAllocationPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_SYMBOL_TABLE_MIN_ALLOCATION_PAGE_SIZE, Files.PAGE_SIZE));
            this.symbolTableMaxAllocationPageSize = Files.ceilPageSize(getLongSize(properties, env, PropertyKey.CAIRO_SYMBOL_TABLE_MAX_ALLOCATION_PAGE_SIZE, 8 * 1024 * 1024));

            this.sqlSampleByIndexSearchPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_SAMPLEBY_PAGE_SIZE, 0);
            this.sqlSampleByDefaultAlignment = getBoolean(properties, env, PropertyKey.CAIRO_SQL_SAMPLEBY_DEFAULT_ALIGNMENT_CALENDAR, true);
            this.sqlGroupByMapCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_MAP_CAPACITY, 1024);
            this.sqlGroupByAllocatorChunkSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 128 * 1024);
            this.sqlGroupByAllocatorMaxChunkSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_MAX_CHUNK_SIZE, 4 * Numbers.SIZE_1GB);
            this.sqlGroupByPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_GROUPBY_POOL_CAPACITY, 1024);
            this.sqlMaxSymbolNotEqualsCount = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_SYMBOL_NOT_EQUALS_COUNT, 100);
            this.sqlBindVariablePoolSize = getInt(properties, env, PropertyKey.CAIRO_SQL_BIND_VARIABLE_POOL_SIZE, 8);
            this.sqlQueryRegistryPoolSize = getInt(properties, env, PropertyKey.CAIRO_SQL_QUERY_REGISTRY_POOL_SIZE, 32);
            this.sqlCountDistinctCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COUNT_DISTINCT_CAPACITY, 3);
            this.sqlCountDistinctLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_SQL_COUNT_DISTINCT_LOAD_FACTOR, "0.5");
            final String sqlCopyFormatsFile = getString(properties, env, PropertyKey.CAIRO_SQL_COPY_FORMATS_FILE, "/text_loader.json");
            final String dateLocale = getString(properties, env, PropertyKey.CAIRO_DATE_LOCALE, "en");
            this.locale = DateLocaleFactory.INSTANCE.getLocale(dateLocale);
            if (this.locale == null) {
                throw ServerConfigurationException.forInvalidKey(PropertyKey.CAIRO_DATE_LOCALE.getPropertyPath(), dateLocale);
            }
            this.sqlDistinctTimestampKeyCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_DISTINCT_TIMESTAMP_KEY_CAPACITY, 512);
            this.sqlDistinctTimestampLoadFactor = getDouble(properties, env, PropertyKey.CAIRO_SQL_DISTINCT_TIMESTAMP_LOAD_FACTOR, "0.5");
            this.sqlPageFrameMinRows = getInt(properties, env, PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100_000);
            this.sqlPageFrameMaxRows = getInt(properties, env, PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
            this.sqlSmallPageFrameMinRows = getInt(properties, env, PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10_000);
            this.sqlSmallPageFrameMaxRows = getInt(properties, env, PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 100_000);

            this.sqlJitMode = getSqlJitMode(properties, env);
            this.sqlJitIRMemoryPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_IR_MEMORY_PAGE_SIZE, 8 * 1024);
            this.sqlJitIRMemoryMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_JIT_IR_MEMORY_MAX_PAGES, 8);
            this.sqlJitBindVarsMemoryPageSize = getIntSize(properties, env, PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE, 4 * 1024);
            this.sqlJitBindVarsMemoryMaxPages = getInt(properties, env, PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_MAX_PAGES, 8);
            this.sqlJitDebugEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_JIT_DEBUG_ENABLED, false);
            this.sqlJitMaxInListSizeThreshold = getInt(properties, env, PropertyKey.CAIRO_SQL_JIT_MAX_IN_LIST_SIZE_THRESHOLD, 10);

            this.maxSqlRecompileAttempts = getInt(properties, env, PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, 10);

            String value = getString(properties, env, PropertyKey.CAIRO_WRITER_FO_OPTS, "o_none");
            int lopts = CairoConfiguration.O_NONE;
            String[] opts = value.split("\\|");
            for (String opt : opts) {
                int index = WRITE_FO_OPTS.keyIndex(opt.trim());
                if (index < 0) {
                    lopts |= WRITE_FO_OPTS.valueAt(index);
                }
            }
            this.writerFileOpenOpts = lopts;

            this.writerMixedIOEnabled = getBoolean(properties, env, PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, ff.allowMixedIO(this.dbRoot));
            this.fileDescriptorCacheEnabled = getBoolean(properties, env, PropertyKey.CAIRO_FILE_DESCRIPTOR_CACHE_ENABLED, true);
            this.asyncMunmapEnabled = getBoolean(properties, env, PropertyKey.CAIRO_FILE_ASYNC_MUNMAP_ENABLED, false);
            if (asyncMunmapEnabled && Os.isWindows()) {
                throw new ServerConfigurationException("Async munmap is not supported on Windows");
            }
            this.rmdirMaxDepth = getInt(properties, env, PropertyKey.CAIRO_RMDIR_MAX_DEPTH, 5);

            this.inputFormatConfiguration = new InputFormatConfiguration(
                    DateFormatFactory.INSTANCE,
                    DateLocaleFactory.INSTANCE,
                    this.locale
            );

            try (JsonLexer lexer = new JsonLexer(1024, 1024)) {
                inputFormatConfiguration.parseConfiguration(PropServerConfiguration.class, lexer, confRoot, sqlCopyFormatsFile);
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
            this.cairoSqlCopyExportQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_COPY_EXPORT_QUEUE_CAPACITY, 32));
            this.cairoSqlCopyLogRetentionDays = getInt(properties, env, PropertyKey.CAIRO_SQL_COPY_LOG_RETENTION_DAYS, 3);
            this.o3MinLagUs = getMicros(properties, env, PropertyKey.CAIRO_O3_MIN_LAG, 1_000) * 1_000L;

            this.detachedMkdirMode = getInt(properties, env, PropertyKey.CAIRO_DETACHED_MKDIR_MODE, 509);
            this.columnIndexerQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_COLUMN_INDEXER_QUEUE_CAPACITY, 64);
            this.o3CallbackQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_CALLBACK_QUEUE_CAPACITY, 128);
            this.o3PartitionQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_PARTITION_QUEUE_CAPACITY, 128);
            this.o3OpenColumnQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_OPEN_COLUMN_QUEUE_CAPACITY, 128);
            this.o3CopyQueueCapacity = getQueueCapacity(properties, env, PropertyKey.CAIRO_O3_COPY_QUEUE_CAPACITY, 128);
            this.o3LagCalculationWindowsSize = getIntSize(properties, env, PropertyKey.CAIRO_O3_LAG_CALCULATION_WINDOW_SIZE, 4);
            this.o3PurgeDiscoveryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_PURGE_DISCOVERY_QUEUE_CAPACITY, 128));
            int debugO3MemSize = getInt(properties, env, PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, 0);
            if (debugO3MemSize != 0) {
                this.o3ColumnMemorySize = debugO3MemSize;
            } else {
                this.o3ColumnMemorySize = (int) Files.ceilPageSize(getIntSize(properties, env, PropertyKey.CAIRO_O3_COLUMN_MEMORY_SIZE, 8 * Numbers.SIZE_1MB));
            }
            this.systemO3ColumnMemorySize = (int) Files.ceilPageSize(getIntSize(properties, env, PropertyKey.CAIRO_SYSTEM_O3_COLUMN_MEMORY_SIZE, 256 * 1024));
            this.maxUncommittedRows = getInt(properties, env, PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            long o3MaxLagMs = getMillis(properties, env, PropertyKey.CAIRO_COMMIT_LAG, 10 * Dates.MINUTE_MILLIS);
            this.o3MaxLagUs = getMillis(properties, env, PropertyKey.CAIRO_O3_MAX_LAG, o3MaxLagMs) * 1_000;

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
            this.sqlIntervalMaxBracketDepth = getInt(properties, env, PropertyKey.CAIRO_SQL_INTERVAL_MAX_BRACKET_DEPTH, 8);
            this.sqlIntervalMaxIntervalsAfterMerge = getInt(properties, env, PropertyKey.CAIRO_SQL_INTERVAL_MAX_INTERVALS_AFTER_MERGE, 1024);
            this.sqlIntervalIncrementalMergeThreshold = getInt(properties, env, PropertyKey.CAIRO_SQL_INTERVAL_INCREMENTAL_MERGE_THRESHOLD, 256);
            if (this.sqlIntervalMaxIntervalsAfterMerge <= this.sqlIntervalIncrementalMergeThreshold) {
                throw new ServerConfigurationException(
                        PropertyKey.CAIRO_SQL_INTERVAL_MAX_INTERVALS_AFTER_MERGE.getPropertyPath()
                                + " (" + this.sqlIntervalMaxIntervalsAfterMerge + ") must be greater than "
                                + PropertyKey.CAIRO_SQL_INTERVAL_INCREMENTAL_MERGE_THRESHOLD.getPropertyPath()
                                + " (" + this.sqlIntervalIncrementalMergeThreshold + ")"
                );
            }
            this.cairoSqlLegacyOperatorPrecedence = getBoolean(properties, env, PropertyKey.CAIRO_SQL_LEGACY_OPERATOR_PRECEDENCE, false);
            this.sqlWindowInitialRangeBufferSize = getInt(properties, env, PropertyKey.CAIRO_SQL_ANALYTIC_INITIAL_RANGE_BUFFER_SIZE, 32);
            this.sqlTxnScoreboardEntryCount = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, 16384));
            this.latestByQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_LATEST_ON_QUEUE_CAPACITY, 32));

            // telemetry config
            boolean telemetryEnabled = getBoolean(properties, env, PropertyKey.TELEMETRY_ENABLED, true);
            this.telemetryDisableCompletely = getBoolean(properties, env, PropertyKey.TELEMETRY_DISABLE_COMPLETELY, false);
            int telemetryQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.TELEMETRY_QUEUE_CAPACITY, 512));
            boolean telemetryHideTables = getBoolean(properties, env, PropertyKey.TELEMETRY_HIDE_TABLES, true);
            this.telemetryDbSizeEstimateTimeout = getMillis(properties, env, PropertyKey.TELEMETRY_DB_SIZE_ESTIMATE_TIMEOUT, Micros.SECOND_MILLIS);
            int telemetryTableTTLWeeks = getInt(properties, env, PropertyKey.TELEMETRY_TABLE_TTL_WEEKS, 4);
            long telemetryThrottleInterval = getMicros(properties, env, PropertyKey.TELEMETRY_EVENT_THROTTLE_INTERVAL, 60000000L);
            this.telemetryConfiguration = new PropTelemetryConfiguration(telemetryEnabled, telemetryQueueCapacity, telemetryHideTables, telemetryTableTTLWeeks, telemetryThrottleInterval);

            this.o3PartitionPurgeListCapacity = getInt(properties, env, PropertyKey.CAIRO_O3_PARTITION_PURGE_LIST_INITIAL_CAPACITY, 1);
            this.ioURingEnabled = getBoolean(properties, env, PropertyKey.CAIRO_IO_URING_ENABLED, true);
            this.cairoMaxCrashFiles = getInt(properties, env, PropertyKey.CAIRO_MAX_CRASH_FILES, 100);
            this.o3LastPartitionMaxSplits = Math.max(1, getInt(properties, env, PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20));
            this.o3PartitionSplitMinSize = getLongSize(properties, env, PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 50 * Numbers.SIZE_1MB);
            this.o3PartitionOverwriteControlEnabled = getBoolean(properties, env, PropertyKey.CAIRO_O3_PARTITION_OVERWRITE_CONTROL_ENABLED, false);

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
            this.lineUdpTimestampUnit = getLineTimestampUnit(properties, env, PropertyKey.LINE_UDP_TIMESTAMP);
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

                this.lineTcpNetAcceptLoopTimeout = getMillis(properties, env, PropertyKey.LINE_TCP_NET_ACCEPT_LOOP_TIMEOUT, 500);

                // deprecated
                this.lineTcpNetConnectionTimeout = getMillis(properties, env, PropertyKey.LINE_TCP_NET_IDLE_TIMEOUT, 0);
                this.lineTcpNetConnectionTimeout = getMillis(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_TIMEOUT, this.lineTcpNetConnectionTimeout);

                // deprecated
                this.lineTcpNetConnectionQueueTimeout = getMillis(properties, env, PropertyKey.LINE_TCP_NET_QUEUED_TIMEOUT, 5_000);
                this.lineTcpNetConnectionQueueTimeout = getMillis(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_QUEUE_TIMEOUT, this.lineTcpNetConnectionQueueTimeout);

                this.lineTcpConnectionPoolInitialCapacity = getInt(properties, env, PropertyKey.LINE_TCP_CONNECTION_POOL_CAPACITY, 8);

                // deprecated
                this.lineTcpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.LINE_TCP_NET_RECV_BUF_SIZE, -1);
                this.lineTcpNetConnectionRcvBuf = getIntSize(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_RCVBUF, lineTcpNetConnectionRcvBuf);
                // deprecated
                this.lineTcpRecvBufferSize = getIntSize(properties, env, PropertyKey.LINE_TCP_MSG_BUFFER_SIZE, 131072);
                this.lineTcpRecvBufferSize = getIntSize(properties, env, PropertyKey.LINE_TCP_RECV_BUFFER_SIZE, lineTcpRecvBufferSize);
                this.lineTcpMaxMeasurementSize = getIntSize(properties, env, PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE, 32768);
                if (lineTcpMaxMeasurementSize > lineTcpRecvBufferSize) {
                    lineTcpRecvBufferSize = lineTcpMaxMeasurementSize;
                }
                this.lineTcpMaxRecvBufferSize = getLongSize(properties, env, PropertyKey.LINE_TCP_MAX_RECV_BUFFER_SIZE, Numbers.SIZE_1GB);
                if (lineTcpRecvBufferSize > lineTcpMaxRecvBufferSize) {
                    lineTcpMaxRecvBufferSize = lineTcpRecvBufferSize;
                }
                if (lineTcpRecvBufferSize < MIN_TCP_ILP_BUF_SIZE) {
                    throw new ServerConfigurationException(
                            "TCP ILP buffer size is too small, should be at least " + MIN_TCP_ILP_BUF_SIZE + ", ["
                                    + PropertyKey.LINE_TCP_RECV_BUFFER_SIZE.getPropertyPath() + "=" + lineTcpRecvBufferSize + ']');
                }

                this.lineTcpWriterQueueCapacity = getQueueCapacity(properties, env, PropertyKey.LINE_TCP_WRITER_QUEUE_CAPACITY, 128);
                this.lineTcpWriterWorkerCount = getInt(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_COUNT, 0); // Use shared write pool by default
                this.lineTcpWriterWorkerAffinity = getAffinity(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_AFFINITY, lineTcpWriterWorkerCount);
                this.lineTcpWriterWorkerPoolHaltOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_WRITER_HALT_ON_ERROR, false);
                this.lineTcpWriterWorkerYieldThreshold = getLong(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_YIELD_THRESHOLD, 10);
                this.lineTcpWriterWorkerNapThreshold = getLong(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_NAP_THRESHOLD, 7_000);
                this.lineTcpWriterWorkerSleepThreshold = getLong(properties, env, PropertyKey.LINE_TCP_WRITER_WORKER_SLEEP_THRESHOLD, 10_000);
                this.symbolCacheWaitBeforeReload = getMicros(properties, env, PropertyKey.LINE_TCP_SYMBOL_CACHE_WAIT_BEFORE_RELOAD, 500_000);
                this.lineTcpIOWorkerCount = getInt(properties, env, PropertyKey.LINE_TCP_IO_WORKER_COUNT, 0); // Use shared IO pool by default
                this.lineTcpIOWorkerAffinity = getAffinity(properties, env, PropertyKey.LINE_TCP_IO_WORKER_AFFINITY, lineTcpIOWorkerCount);
                this.lineTcpIOWorkerPoolHaltOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_IO_HALT_ON_ERROR, false);
                this.lineTcpIOWorkerYieldThreshold = getLong(properties, env, PropertyKey.LINE_TCP_IO_WORKER_YIELD_THRESHOLD, 10);
                this.lineTcpIOWorkerNapThreshold = getLong(properties, env, PropertyKey.LINE_TCP_IO_WORKER_NAP_THRESHOLD, 7_000);
                this.lineTcpIOWorkerSleepThreshold = getLong(properties, env, PropertyKey.LINE_TCP_IO_WORKER_SLEEP_THRESHOLD, 10_000);
                this.lineTcpMaintenanceInterval = getMillis(properties, env, PropertyKey.LINE_TCP_MAINTENANCE_JOB_INTERVAL, 1000);
                this.lineTcpCommitIntervalFraction = getDouble(properties, env, PropertyKey.LINE_TCP_COMMIT_INTERVAL_FRACTION, "0.5");
                this.lineTcpCommitIntervalDefault = getMillis(properties, env, PropertyKey.LINE_TCP_COMMIT_INTERVAL_DEFAULT, COMMIT_INTERVAL_DEFAULT);
                if (this.lineTcpCommitIntervalDefault < 1L) {
                    log.info().$("invalid default commit interval ").$(lineTcpCommitIntervalDefault).$("), will use ").$(COMMIT_INTERVAL_DEFAULT).$();
                    this.lineTcpCommitIntervalDefault = COMMIT_INTERVAL_DEFAULT;
                }
                this.lineTcpAuthDB = getString(properties, env, PropertyKey.LINE_TCP_AUTH_DB_PATH, null);
                this.lineLogMessageOnError = getBoolean(properties, env, PropertyKey.LINE_LOG_MESSAGE_ON_ERROR, true);
                // deprecated
                String defaultTcpPartitionByProperty = getString(properties, env, PropertyKey.LINE_TCP_DEFAULT_PARTITION_BY, "DAY");
                defaultTcpPartitionByProperty = getString(properties, env, PropertyKey.LINE_DEFAULT_PARTITION_BY, defaultTcpPartitionByProperty);
                this.lineTcpDefaultPartitionBy = PartitionBy.fromString(defaultTcpPartitionByProperty);
                if (this.lineTcpDefaultPartitionBy == -1) {
                    log.info().$("invalid partition by ").$safe(defaultTcpPartitionByProperty).$("), will use DAY for TCP").$();
                    this.lineTcpDefaultPartitionBy = PartitionBy.DAY;
                }
                this.minIdleMsBeforeWriterRelease = getMillis(properties, env, PropertyKey.LINE_TCP_MIN_IDLE_MS_BEFORE_WRITER_RELEASE, 500);
                this.lineTcpDisconnectOnError = getBoolean(properties, env, PropertyKey.LINE_TCP_DISCONNECT_ON_ERROR, true);
                final long heartbeatInterval = LineTcpReceiverConfigurationHelper.calcCommitInterval(
                        this.o3MinLagUs,
                        this.lineTcpCommitIntervalFraction,
                        this.lineTcpCommitIntervalDefault
                );
                this.lineTcpNetConnectionHeartbeatInterval = getMillis(properties, env, PropertyKey.LINE_TCP_NET_CONNECTION_HEARTBEAT_INTERVAL, heartbeatInterval);
            } else {
                this.lineTcpAuthDB = null;
            }

            this.useLegacyStringDefault = getBoolean(properties, env, PropertyKey.CAIRO_LEGACY_STRING_COLUMN_TYPE_DEFAULT, false);
            if (lineTcpEnabled || (lineHttpEnabled && httpServerEnabled)) {
                this.lineTcpTimestampUnit = getLineTimestampUnit(properties, env, PropertyKey.LINE_TCP_TIMESTAMP);
                this.stringToCharCastAllowed = getBoolean(properties, env, PropertyKey.LINE_TCP_UNDOCUMENTED_STRING_TO_CHAR_CAST_ALLOWED, false);

                String floatDefaultColumnTypeName = getString(properties, env, PropertyKey.LINE_FLOAT_DEFAULT_COLUMN_TYPE, ColumnType.nameOf(ColumnType.DOUBLE));
                this.floatDefaultColumnType = ColumnType.tagOf(floatDefaultColumnTypeName);
                if (floatDefaultColumnType != ColumnType.DOUBLE && floatDefaultColumnType != ColumnType.FLOAT) {
                    log.info().$("invalid default column type for float ").$safe(floatDefaultColumnTypeName).$(", will use DOUBLE").$();
                    this.floatDefaultColumnType = ColumnType.DOUBLE;
                }

                String integerDefaultColumnTypeName = getString(properties, env, PropertyKey.LINE_INTEGER_DEFAULT_COLUMN_TYPE, ColumnType.nameOf(ColumnType.LONG));
                this.integerDefaultColumnType = ColumnType.tagOf(integerDefaultColumnTypeName);
                if (integerDefaultColumnType != ColumnType.LONG && integerDefaultColumnType != ColumnType.INT && integerDefaultColumnType != ColumnType.SHORT && integerDefaultColumnType != ColumnType.BYTE) {
                    log.info().$("invalid default column type for integer ").$safe(integerDefaultColumnTypeName).$(", will use LONG").$();
                    this.integerDefaultColumnType = ColumnType.LONG;
                }

                String timestampDefaultColumnTypeName = getString(properties, env, PropertyKey.LINE_TIMESTAMP_DEFAULT_COLUMN_TYPE, ColumnType.nameOf(ColumnType.TIMESTAMP_MICRO));
                this.lineDefaultTimestampColumnType = ColumnType.typeOf(timestampDefaultColumnTypeName);
                if (!ColumnType.isTimestamp(lineDefaultTimestampColumnType)) {
                    log.info().$("invalid default column type for timestamp ").$(timestampDefaultColumnTypeName).$(", will use TIMESTAMP_MICRO").$();
                    this.lineDefaultTimestampColumnType = ColumnType.TIMESTAMP_MICRO;
                }
            }

            this.ilpAutoCreateNewColumns = getBoolean(properties, env, PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS, true);
            this.ilpAutoCreateNewTables = getBoolean(properties, env, PropertyKey.LINE_AUTO_CREATE_NEW_TABLES, true);

            // Legacy shared pool, it used to be a single shared pool for all the tasks.
            // Now it's split into 3: IO, Query and Write
            // But the old props are the defaults for the new shared pools, read them.
            final int sharedWorkerCount = getInt(properties, env, PropertyKey.SHARED_WORKER_COUNT, Math.max(2, cpuAvailable - cpuSpare));
            final boolean sharedWorkerHaltOnError = getBoolean(properties, env, PropertyKey.SHARED_WORKER_HALT_ON_ERROR, false);
            final long sharedWorkerYieldThreshold = getLong(properties, env, PropertyKey.SHARED_WORKER_YIELD_THRESHOLD, 10);
            final long sharedWorkerNapThreshold = getLong(properties, env, PropertyKey.SHARED_WORKER_NAP_THRESHOLD, 7_000);
            final long sharedWorkerSleepThreshold = getLong(properties, env, PropertyKey.SHARED_WORKER_SLEEP_THRESHOLD, 10_000);
            final long sharedWorkerSleepTimeout = getMillis(properties, env, PropertyKey.SHARED_WORKER_SLEEP_TIMEOUT, 10);

            // IO will be slightly higher priority than query and write pools to make the server more responsive
            final int networkPoolWorkerCount = configureSharedThreadPool(
                    properties,
                    env,
                    sharedWorkerPoolNetworkConfiguration,
                    PropertyKey.SHARED_NETWORK_WORKER_COUNT,
                    PropertyKey.SHARED_NETWORK_WORKER_AFFINITY,
                    sharedWorkerCount,
                    Thread.NORM_PRIORITY + 1,
                    sharedWorkerHaltOnError,
                    sharedWorkerYieldThreshold,
                    sharedWorkerNapThreshold,
                    sharedWorkerSleepThreshold,
                    sharedWorkerSleepTimeout
            );

            final int queryWorkers = configureSharedThreadPool(
                    properties,
                    env,
                    sharedWorkerPoolQueryConfiguration,
                    PropertyKey.SHARED_QUERY_WORKER_COUNT,
                    PropertyKey.SHARED_QUERY_WORKER_AFFINITY,
                    sharedWorkerCount,
                    Thread.NORM_PRIORITY,
                    sharedWorkerHaltOnError,
                    sharedWorkerYieldThreshold,
                    sharedWorkerNapThreshold,
                    sharedWorkerSleepThreshold,
                    sharedWorkerSleepTimeout
            );

            final int writeWorkers = configureSharedThreadPool(
                    properties,
                    env,
                    sharedWorkerPoolWriteConfiguration,
                    PropertyKey.SHARED_WRITE_WORKER_COUNT,
                    PropertyKey.SHARED_WRITE_WORKER_AFFINITY,
                    sharedWorkerCount,
                    Thread.NORM_PRIORITY - 1,
                    sharedWorkerHaltOnError,
                    sharedWorkerYieldThreshold,
                    sharedWorkerNapThreshold,
                    sharedWorkerSleepThreshold,
                    sharedWorkerSleepTimeout
            );

            // Now all worker counts are known, so we can set select cache capacity props.
            if (pgEnabled) {
                this.pgSelectCacheEnabled = getBoolean(properties, env, PropertyKey.PG_SELECT_CACHE_ENABLED, true);
                final int effectivePGWorkerCount = pgWorkerCount > 0 ? pgWorkerCount : networkPoolWorkerCount;
                this.pgSelectCacheBlockCount = getInt(properties, env, PropertyKey.PG_SELECT_CACHE_BLOCK_COUNT, 32);
                this.pgSelectCacheRowCount = getInt(properties, env, PropertyKey.PG_SELECT_CACHE_ROW_COUNT, Math.max(effectivePGWorkerCount, 4));
            }
            final int effectiveHttpWorkerCount = httpWorkerCount > 0 ? httpWorkerCount : networkPoolWorkerCount;
            this.httpSqlCacheEnabled = getBoolean(properties, env, PropertyKey.HTTP_QUERY_CACHE_ENABLED, true);
            this.httpSqlCacheBlockCount = getInt(properties, env, PropertyKey.HTTP_QUERY_CACHE_BLOCK_COUNT, 32);
            this.httpSqlCacheRowCount = getInt(properties, env, PropertyKey.HTTP_QUERY_CACHE_ROW_COUNT, Math.max(effectiveHttpWorkerCount, 4));
            this.queryCacheEventQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_QUERY_CACHE_EVENT_QUEUE_CAPACITY, 4));

            this.sqlCompilerPoolCapacity = 2 * (httpWorkerCount + pgWorkerCount + writeWorkers + networkPoolWorkerCount);

            final int defaultReduceQueueCapacity = Math.min(4 * queryWorkers, 256);
            this.cairoPageFrameReduceQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, defaultReduceQueueCapacity));
            this.cairoGroupByMergeShardQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_MERGE_QUEUE_CAPACITY, defaultReduceQueueCapacity));
            this.vectorAggregateQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_VECTOR_AGGREGATE_QUEUE_CAPACITY, defaultReduceQueueCapacity));
            this.cairoGroupByTopKQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_TOP_K_QUEUE_CAPACITY, defaultReduceQueueCapacity));
            this.cairoGroupByShardingThreshold = getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 10_000);
            this.cairoGroupByPresizeEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_ENABLED, true);
            this.cairoGroupByPresizeMaxCapacity = getLong(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_MAX_CAPACITY, 100_000_000);
            this.cairoGroupByPresizeMaxHeapSize = getLongSize(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_MAX_HEAP_SIZE, Numbers.SIZE_1GB);
            this.cairoGroupByTopKThreshold = getLong(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_TOP_K_THRESHOLD, 5_000_000);
            this.cairoPageFrameReduceRowIdListCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 256));
            this.cairoPageFrameReduceColumnListCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_COLUMN_LIST_CAPACITY, 16));
            final int defaultReduceShardCount = queryWorkers > 0 ? Math.min(queryWorkers, 4) : 0;
            this.cairoPageFrameReduceShardCount = getInt(properties, env, PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, defaultReduceShardCount);
            this.sqlParallelFilterPreTouchThreshold = getDouble(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_FILTER_PRETOUCH_THRESHOLD, "0.05");
            this.sqlParallelFilterDispatchLimit = getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, Math.min(queryWorkers, 32));
            this.sqlCopyModelPoolCapacity = getInt(properties, env, PropertyKey.CAIRO_SQL_COPY_MODEL_POOL_CAPACITY, 32);

            final boolean defaultParallelSqlEnabled = queryWorkers > 0;
            this.sqlParallelFilterEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, defaultParallelSqlEnabled);
            this.sqlParallelTopKEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, defaultParallelSqlEnabled);
            this.sqlParallelHorizonJoinEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, defaultParallelSqlEnabled);
            this.sqlParallelWindowJoinEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, defaultParallelSqlEnabled);
            this.sqlParallelGroupByEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, defaultParallelSqlEnabled);
            this.sqlParallelReadParquetEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, defaultParallelSqlEnabled);
            if (!sqlParallelFilterEnabled && !sqlParallelGroupByEnabled && !sqlParallelHorizonJoinEnabled
                    && !sqlParallelReadParquetEnabled && !sqlParallelTopKEnabled && !sqlParallelWindowJoinEnabled) {
                // All type of parallel queries are disabled. Don't start the query thread pool
                sharedWorkerPoolQueryConfiguration.sharedWorkerCount = 0;
            }

            this.walParallelExecutionEnabled = getBoolean(properties, env, PropertyKey.CAIRO_WAL_APPLY_PARALLEL_SQL_ENABLED, true);
            this.matViewParallelExecutionEnabled = getBoolean(properties, env, PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, true);
            this.sqlParallelWorkStealingThreshold = getInt(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 16);
            this.sqlParallelWorkStealingSpinTimeout = getNanos(properties, env, PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_SPIN_TIMEOUT, 50_000);
            this.sqlParquetFrameCacheCapacity = Math.max(getInt(properties, env, PropertyKey.CAIRO_SQL_PARQUET_FRAME_CACHE_CAPACITY, 8), 8);
            this.sqlOrderBySortEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
            this.sqlOrderByRadixSortThreshold = getInt(properties, env, PropertyKey.CAIRO_SQL_ORDER_BY_RADIX_SORT_THRESHOLD, 600);
            this.copierChunkedEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_COPIER_CHUNKED, true);
            this.copierType = getInt(properties, env, PropertyKey.DEBUG_CAIRO_COPIER_TYPE, 0);
            this.writerAsyncCommandBusyWaitTimeout = getMillis(properties, env, PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
            this.writerAsyncCommandMaxWaitTimeout = getMillis(properties, env, PropertyKey.CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT, 30_000);
            this.writerTickRowsCountMod = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_WRITER_TICK_ROWS_COUNT, 1024)) - 1;
            this.writerAsyncCommandQueueCapacity = Numbers.ceilPow2(getInt(properties, env, PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 32));
            this.writerAsyncCommandQueueSlotSize = Numbers.ceilPow2(getLongSize(properties, env, PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_SLOT_SIZE, 2048));

            this.buildInformation = buildInformation;
            this.binaryEncodingMaxLength = getInt(properties, env, PropertyKey.BINARYDATA_ENCODING_MAXLENGTH, 32768);
        }
        this.ilpProtoTransports = initIlpTransport();
        this.acceptingWrites = initAcceptingWrites();
        this.allowTableRegistrySharedWrite = getBoolean(properties, env, PropertyKey.DEBUG_ALLOW_TABLE_REGISTRY_SHARED_WRITE, false);
        this.enableTestFactories = getBoolean(properties, env, PropertyKey.DEBUG_ENABLE_TEST_FACTORIES, false);

        this.posthogEnabled = getBoolean(properties, env, PropertyKey.POSTHOG_ENABLED, false);
        this.posthogApiKey = getString(properties, env, PropertyKey.POSTHOG_API_KEY, null);
        this.configReloadEnabled = getBoolean(properties, env, PropertyKey.CONFIG_RELOAD_ENABLED, true);

        this.partitionEncoderParquetVersion = getInt(properties, env, PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, ParquetVersion.PARQUET_VERSION_V1);
        this.partitionEncoderParquetStatisticsEnabled = getBoolean(properties, env, PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, true);
        this.partitionEncoderParquetCompressionCodec = ParquetCompression.getCompressionCodec(getString(properties, env, PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, "LZ4_RAW"));
        // Use raw array encoding in partition-to-parquet conversion for better performance.
        // Raw encoding writes arrays in QuestDB's native binary layout, avoiding the overhead
        // of Parquet's nested LIST encoding. Compatibility with external tools is not a concern
        // since these parquet files are internal to QuestDB.
        this.partitionEncoderParquetRawArrayEncoding = getBoolean(properties, env, PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, true);
        int defaultCompressionLevel = partitionEncoderParquetCompressionCodec == ParquetCompression.COMPRESSION_ZSTD ? 9 : 0;
        this.partitionEncoderParquetCompressionLevel = getInt(properties, env, PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL, defaultCompressionLevel);
        this.partitionEncoderParquetRowGroupSize = getInt(properties, env, PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        this.partitionEncoderParquetDataPageSize = getInt(properties, env, PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE, Numbers.SIZE_1MB);

        // compatibility switch, to be removed in future
        this.sqlSampleByValidateFillType = getBoolean(properties, env, PropertyKey.CAIRO_SQL_SAMPLEBY_VALIDATE_FILL_TYPE, true);

        this.cairoSqlColumnAliasExpressionEnabled = getBoolean(properties, env, PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, true);
        this.cairoSqlColumnAliasGeneratedMaxSize = getInt(properties, env, PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, COLUMN_ALIAS_GENERATED_MAX_SIZE_DEFAULT);
        if (this.cairoSqlColumnAliasGeneratedMaxSize < COLUMN_ALIAS_GENERATED_MAX_SIZE_MINIMUM) {
            log.info()
                    .$("expected a column alias truncate length superior or equal to ")
                    .$(COLUMN_ALIAS_GENERATED_MAX_SIZE_MINIMUM)
                    .$(" but got ")
                    .$(cairoSqlColumnAliasGeneratedMaxSize)
                    .$(". Using ")
                    .$(COLUMN_ALIAS_GENERATED_MAX_SIZE_DEFAULT)
                    .$(" instead").$();
            this.cairoSqlColumnAliasGeneratedMaxSize = COLUMN_ALIAS_GENERATED_MAX_SIZE_DEFAULT;
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
    public WorkerPoolConfiguration getExportPoolConfiguration() {
        return exportPoolConfiguration;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        if (factoryProvider == null) {
            throw new IllegalStateException("configuration.init() has not been invoked");
        }
        return factoryProvider;
    }

    @Override
    public HttpServerConfiguration getHttpMinServerConfiguration() {
        return httpMinServerConfiguration;
    }

    @Override
    public HttpFullFatServerConfiguration getHttpServerConfiguration() {
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
    public WorkerPoolConfiguration getMatViewRefreshPoolConfiguration() {
        return matViewRefreshPoolConfiguration;
    }

    @Override
    public MemoryConfiguration getMemoryConfiguration() {
        return memoryConfiguration;
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return metricsConfiguration;
    }

    @Override
    public PGConfiguration getPGWireConfiguration() {
        return pgConfiguration;
    }

    @Override
    public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
        return publicPassthroughConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getSharedWorkerPoolNetworkConfiguration() {
        return sharedWorkerPoolNetworkConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getSharedWorkerPoolQueryConfiguration() {
        return sharedWorkerPoolQueryConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getSharedWorkerPoolWriteConfiguration() {
        return sharedWorkerPoolWriteConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getViewCompilerPoolConfiguration() {
        return viewCompilerPoolConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return walApplyPoolConfiguration;
    }

    @Override
    public void init(CairoEngine engine, FreeOnExit freeOnExit) {
        this.factoryProvider = fpf.getInstance(this, engine, freeOnExit);
    }

    public void init(ServerConfiguration config, CairoEngine engine, FreeOnExit freeOnExit) {
        this.factoryProvider = fpf.getInstance(config, engine, freeOnExit);
    }

    public boolean isConfigReloadEnabled() {
        return configReloadEnabled;
    }

    // Used by dynamic configuration to reuse the already created factory provider.
    public void reinit(FactoryProvider factoryProvider) {
        this.factoryProvider = factoryProvider;
    }

    private static @NotNull String stripTrailingSlash(@NotNull String httpContextWebConsole) {
        int n = 0;
        for (int j = httpContextWebConsole.length() - 1; j > -1; j--) {
            if (httpContextWebConsole.charAt(j) == '/') {
                n++;
            } else {
                break;
            }
        }
        if (n > 0) {
            httpContextWebConsole = httpContextWebConsole.substring(0, httpContextWebConsole.length() - n);
        }
        return httpContextWebConsole;
    }

    private int configureSharedThreadPool(
            Properties properties,
            Map<String, String> env,
            PropWorkerPoolConfiguration poolConfiguration,
            PropertyKey workerCountProp,
            PropertyKey affinityProp,
            int sharedWorkerCount,
            int priority,
            boolean sharedWorkerHaltOnError,
            long sharedWorkerYieldThreshold,
            long sharedWorkerNapThreshold,
            long sharedWorkerSleepThreshold,
            long sharedWorkerSleepTimeout
    ) throws ServerConfigurationException {
        poolConfiguration.sharedWorkerCount = getInt(properties, env, workerCountProp, sharedWorkerCount);
        poolConfiguration.sharedWorkerAffinity =
                getAffinity(properties, env, affinityProp, poolConfiguration.sharedWorkerCount);
        poolConfiguration.sharedWorkerHaltOnError = sharedWorkerHaltOnError;
        poolConfiguration.sharedWorkerYieldThreshold = sharedWorkerYieldThreshold;
        poolConfiguration.sharedWorkerNapThreshold = sharedWorkerNapThreshold;
        poolConfiguration.sharedWorkerSleepThreshold = sharedWorkerSleepThreshold;
        poolConfiguration.sharedWorkerSleepTimeout = sharedWorkerSleepTimeout;
        poolConfiguration.metrics = this.metrics;
        poolConfiguration.workerPoolPriority = priority;
        return poolConfiguration.sharedWorkerCount;
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

    private byte getLineTimestampUnit(Properties properties, Map<String, String> env, ConfigPropertyKey propNm) {
        final String lineUdpTimestampSwitch = getString(properties, env, propNm, "n");
        return switch (lineUdpTimestampSwitch) {
            case "u" -> CommonUtils.TIMESTAMP_UNIT_MICROS;
            case "ms" -> CommonUtils.TIMESTAMP_UNIT_MILLIS;
            case "s" -> CommonUtils.TIMESTAMP_UNIT_SECONDS;
            case "m" -> CommonUtils.TIMESTAMP_UNIT_MINUTES;
            case "h" -> CommonUtils.TIMESTAMP_UNIT_HOURS;
            default -> CommonUtils.TIMESTAMP_UNIT_NANOS;
        };
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

    // The enterprise version needs to add tcps and https
    private String initIlpTransport() {
        StringSink sink = Misc.getThreadLocalSink();
        sink.put('[');
        boolean addComma = false;
        if (lineTcpEnabled) {
            addComma = true;
            sink.put("\"tcp\"");
        }
        if (lineHttpEnabled && httpServerEnabled) {
            if (addComma) {
                sink.put(", ");
            }
            sink.put("\"http\"");
            addComma = true;
        }
        if (lineUdpEnabled) {
            if (addComma) {
                sink.put(", ");
            }
            sink.put("\"udp\"");
        }
        sink.put(']');
        return sink.toString();
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
                    .$(", value=").$(p2).I$();
            return false;
        }
    }

    private void validateHttpConnectionLimits(
            int httpJsonQueryConnectionLimit, int httpIlpConnectionLimit, int httpExportConnectionLimit, int httpNetConnectionLimit
    ) throws ServerConfigurationException {
        if (httpJsonQueryConnectionLimit > httpNetConnectionLimit) {
            throw new ServerConfigurationException(
                    "Json query connection limit cannot be greater than the overall HTTP connection limit ["
                            + PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath() + "=" + httpJsonQueryConnectionLimit + ", "
                            + PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath() + "=" + httpNetConnectionLimit + ']');
        }

        if (httpIlpConnectionLimit > httpNetConnectionLimit) {
            throw new ServerConfigurationException(
                    "HTTP over ILP connection limit cannot be greater than the overall HTTP connection limit ["
                            + PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath() + "=" + httpIlpConnectionLimit + ", "
                            + PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath() + "=" + httpNetConnectionLimit + ']');
        }

        if (httpExportConnectionLimit > httpNetConnectionLimit) {
            throw new ServerConfigurationException(
                    "HTTP export connection limit cannot be greater than the overall HTTP connection limit ["
                            + PropertyKey.HTTP_EXPORT_CONNECTION_LIMIT.getPropertyPath() + "=" + httpExportConnectionLimit + ", "
                            + PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath() + "=" + httpNetConnectionLimit + ']');
        }

        httpJsonQueryConnectionLimit = Math.max(httpJsonQueryConnectionLimit, 0);
        httpIlpConnectionLimit = Math.max(httpIlpConnectionLimit, 0);
        httpExportConnectionLimit = Math.max(httpExportConnectionLimit, 0);

        if ((httpJsonQueryConnectionLimit + httpIlpConnectionLimit + httpExportConnectionLimit) > httpNetConnectionLimit) {
            throw new ServerConfigurationException(
                    "The sum of the json query, export and HTTP over ILP connection limits cannot be greater than the overall HTTP connection limit ["
                            + ((httpJsonQueryConnectionLimit > 0) ? PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getPropertyPath() + "=" + httpJsonQueryConnectionLimit + ", " : "")
                            + ((httpIlpConnectionLimit > 0) ? PropertyKey.HTTP_ILP_CONNECTION_LIMIT.getPropertyPath() + "=" + httpIlpConnectionLimit + ", " : "")
                            + ((httpExportConnectionLimit > 0) ? PropertyKey.HTTP_EXPORT_CONNECTION_LIMIT.getPropertyPath() + "=" + httpExportConnectionLimit + ", " : "")
                            + PropertyKey.HTTP_NET_CONNECTION_LIMIT.getPropertyPath() + "=" + httpNetConnectionLimit + ']');
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

    protected String getAcceptingWrites() {
        return acceptingWrites;
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

    @SuppressWarnings("SameParameterValue")
    protected int getIntPercentage(
            Properties properties,
            @Nullable Map<String, String> env,
            ConfigPropertyKey key,
            int defaultValue
    ) throws ServerConfigurationException {
        int percentage = getInt(properties, env, key, defaultValue);
        if (percentage < 0 || percentage > 100) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), Integer.toString(percentage));
        }
        return percentage;
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

    protected long getMicros(
            Properties properties,
            @Nullable Map<String, String> env,
            ConfigPropertyKey key,
            long defaultValue
    ) throws ServerConfigurationException {
        final String value = getString(properties, env, key, Long.toString(defaultValue));
        try {
            return Numbers.parseMicros(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected long getMillis(
            Properties properties,
            @Nullable Map<String, String> env,
            ConfigPropertyKey key,
            long defaultValue
    ) throws ServerConfigurationException {
        final String value = getString(properties, env, key, Long.toString(defaultValue));
        try {
            return Numbers.parseMillis(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected long getNanos(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, long defaultValue) throws ServerConfigurationException {
        final String value = getString(properties, env, key, Long.toString(defaultValue));
        try {
            return Numbers.parseNanos(value);
        } catch (NumericException e) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), value);
        }
    }

    protected int getQueueCapacity(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, int defaultValue) throws ServerConfigurationException {
        final int value = getInt(properties, env, key, defaultValue);
        if (!Numbers.isPow2(value) && value != 0) {
            throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "Value must be 0 or a power of 2, e.g. 0,1,2,4,8,16,32,64...");
        }
        return value;
    }

    /**
     * Gets the file path for a secret property by checking the _FILE variant.
     * Checks environment variable first (KEY_FILE), then property file (key.file).
     *
     * @return the file path if specified, null otherwise
     */
    protected String getSecretFilePath(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key) {
        // Check env var: QDB_KEY_FILE
        String envFileKey = key.getEnvVarName() + SECRET_FILE_ENV_VAR_SUFFIX;
        String filePath = env != null ? env.get(envFileKey) : null;
        if (filePath != null) {
            return filePath.trim();
        }
        // Check property: key.file
        String propFileKey = key.getPropertyPath() + SECRET_FILE_PROPERTY_SUFFIX;
        filePath = properties.getProperty(propFileKey);
        return filePath != null ? filePath.trim() : null;
    }

    protected String getString(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, String defaultValue) {
        // For sensitive properties, check if a _FILE variant is specified
        // This supports Kubernetes secret file mounts
        if (key.isSensitive() && filesFacade != null) {
            String secretFilePath = getSecretFilePath(properties, env, key);
            if (secretFilePath != null && !secretFilePath.isEmpty()) {
                log.info().$("reading secret from file [key=").$(key.getPropertyPath()).I$();
                String result = readSecretFromFile(secretFilePath);
                if (result.isEmpty()) {
                    log.advisory().$("secret file is empty or contains only whitespace, this may weaken authentication [key=")
                            .$(key.getPropertyPath()).I$();
                }
                if (!key.isDebug()) {
                    boolean dynamic = dynamicProperties != null && dynamicProperties.contains(key);
                    allPairs.put(key, new ConfigPropertyValueImpl(result, ConfigPropertyValue.VALUE_SOURCE_FILE, dynamic));
                }
                return result;
            }
        }

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

        // Sometimes there can be spaces coming from environment variables, cut them off
        result = (result != null) ? result.trim() : null;
        if (!key.isDebug()) {
            boolean dynamic = dynamicProperties != null && dynamicProperties.contains(key);
            allPairs.put(key, new ConfigPropertyValueImpl(result, valueSource, dynamic));
        }
        return result;
    }

    protected void getUrls(
            Properties properties,
            @Nullable Map<String, String> env,
            ConfigPropertyKey key,
            ObjHashSet<String> target,
            String... defaultValue
    ) throws ServerConfigurationException {
        String envCandidate = key.getEnvVarName();
        String unparsedResult = env != null ? env.get(envCandidate) : null;
        final int valueSource;
        if (unparsedResult != null) {
            log.info().$("env config [key=").$(envCandidate).I$();
            valueSource = ConfigPropertyValue.VALUE_SOURCE_ENV;
        } else {
            unparsedResult = properties.getProperty(key.getPropertyPath());
            if (unparsedResult == null) {
                valueSource = ConfigPropertyValue.VALUE_SOURCE_DEFAULT;
            } else {
                valueSource = ConfigPropertyValue.VALUE_SOURCE_CONF;
            }
        }

        String[] parts;
        if (valueSource == ConfigPropertyValue.VALUE_SOURCE_DEFAULT) {
            parts = defaultValue;
        } else {
            parts = unparsedResult.split(",");
        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = parts.length; i < n; i++) {
            String url = parts[i].trim();
            if (url.isEmpty()) {
                throw ServerConfigurationException.forInvalidKey(key.getPropertyPath(), "empty URL in the list");
            }
            target.add(stripTrailingSlash(url));
        }

        // Sometimes there can be spaces coming from environment variables, cut them off
        unparsedResult = (unparsedResult != null) ? unparsedResult.trim() : null;
        if (!key.isDebug()) {
            boolean dynamic = dynamicProperties != null && dynamicProperties.contains(key);
            allPairs.put(key, new ConfigPropertyValueImpl(unparsedResult, valueSource, dynamic));
        }
    }

    protected String initAcceptingWrites() {
        StringSink sink = Misc.getThreadLocalSink();
        sink.put('[');
        if (instanceAcceptingWrites()) {
            boolean addComma = false;
            if (!httpContextConfiguration.readOnlySecurityContext()) {
                sink.put("\"http\"");
                addComma = true;
            }
            if (lineTcpReceiverConfiguration.isEnabled()) {
                // tcp does not have read-only mode
                if (addComma) {
                    sink.put(", ");
                }
                sink.put("\"tcp\"");
                addComma = true;
            }
            if (pgConfiguration.isEnabled() && !pgConfiguration.readOnlySecurityContext()) {
                if (addComma) {
                    sink.put(", ");
                }
                sink.put("\"pgwire\"");
            }
        }
        sink.put(']');
        return sink.toString();
    }

    protected boolean instanceAcceptingWrites() {
        // overwritten in Enterprise
        return !isReadOnlyInstance;
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

    /**
     * Reads a secret value from a file. The file content is read as UTF-8 and trimmed.
     * This is useful for Kubernetes deployments where secrets are mounted as files.
     *
     * @param filePath the path to the secret file
     * @return the trimmed file content
     * @throws CairoException if the file is too large or cannot be read
     */
    protected String readSecretFromFile(String filePath) {
        // Minimal path hardening to prevent accidental misconfigurations
        // /dev/ - prevents reading devices, FIFOs (can cause hangs)
        // /proc/, /sys/ - blocks common info-disclosure paths
        // .. - rejects obvious path traversal
        if (filePath.contains("..") ||
                filePath.startsWith("/dev/") ||
                filePath.startsWith("/proc/") ||
                filePath.startsWith("/sys/")) {
            throw CairoException.critical(0)
                    .put("secret file path not allowed [path=").put(filePath).put(']');
        }

        long fd = -1;
        long address = 0;
        long size = 0;
        try (Path path = new Path()) {
            path.of(filePath);
            // Reject directories (symlinks to directories are also rejected)
            if (filesFacade.isDirOrSoftLinkDir(path.$())) {
                throw CairoException.critical(0)
                        .put("secret file path is a directory [path=").put(filePath).put(']');
            }
            fd = filesFacade.openRO(path.$());
            if (fd < 0) {
                throw CairoException.critical(filesFacade.errno())
                        .put("cannot open secret file [path=").put(filePath).put(']');
            }

            // Retry loop to handle race condition where file is modified between
            // getting size and reading content
            final int maxRetries = 3;
            for (int attempt = 0; attempt < maxRetries; attempt++) {
                // Free previous allocation if retrying
                if (address != 0) {
                    Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
                    address = 0;
                }

                size = filesFacade.length(fd);
                if (size < 0) {
                    throw CairoException.critical(filesFacade.errno())
                            .put("cannot get size of secret file [path=").put(filePath).put(']');
                }
                if (size > SECRET_FILE_MAX_SIZE) {
                    throw CairoException.critical(0)
                            .put("secret file is too large [path=").put(filePath)
                            .put(", size=").put(size)
                            .put(", maxSize=").put(SECRET_FILE_MAX_SIZE).put(']');
                }
                if (size == 0) {
                    return "";
                }

                address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                long bytesRead = filesFacade.read(fd, address, size, 0);

                if (bytesRead == size) {
                    // Successful read - verify size hasn't changed
                    long newSize = filesFacade.length(fd);
                    if (newSize == size) {
                        return Utf8s.stringFromUtf8Bytes(address, address + size).trim();
                    }
                    // Size changed during read, retry
                    log.info().$("secret file size changed during read, retrying [attempt=").$(attempt + 1).I$();
                } else {
                    // Partial read, retry
                    log.info().$("secret file partial read, retrying [expected=").$(size)
                            .$(", read=").$(bytesRead).$(", attempt=").$(attempt + 1).I$();
                }
            }

            throw CairoException.critical(0)
                    .put("cannot read secret file after retries [path=").put(filePath)
                    .put(", retries=").put(maxRetries).put(']');
        } finally {
            if (address != 0) {
                Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
            }
            if (fd >= 0) {
                filesFacade.close(fd);
            }
        }
    }

    @FunctionalInterface
    protected interface BindToParser {
        void onReady(int address, int port);
    }

    public static class JsonPropertyValueFormatter {
        public static void arrayStr(CharSequence key, String value, CharSink<?> sink) {
            sink.putQuoted(key).putAscii(':').put(value).putAscii(',');
        }

        public static void bool(CharSequence key, boolean value, CharSink<?> sink) {
            sink.putQuoted(key).putAscii(':').put(value).putAscii(',');
        }

        public static void integer(CharSequence key, long value, CharSink<?> sink) {
            sink.putQuoted(key).putAscii(':').put(value).putAscii(',');
        }

        public static void str(CharSequence key, CharSequence value, CharSink<?> sink) {
            sink.putQuoted(key).putAscii(':');
            if (value != null) {
                sink.putQuoted(value);
            } else {
                sink.put("null");
            }
            sink.putAscii(',');
        }
    }

    private static class PropWorkerPoolConfiguration implements WorkerPoolConfiguration {
        private final String name;
        public Metrics metrics;
        public int[] sharedWorkerAffinity;
        public int sharedWorkerCount;
        public boolean sharedWorkerHaltOnError;
        public long sharedWorkerNapThreshold;
        public long sharedWorkerSleepThreshold;
        public long sharedWorkerSleepTimeout;
        public long sharedWorkerYieldThreshold;
        public int workerPoolPriority = Thread.NORM_PRIORITY;

        private PropWorkerPoolConfiguration(String name) {
            this.name = name;
        }

        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return sharedWorkerNapThreshold;
        }

        @Override
        public String getPoolName() {
            return name;
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

        @Override
        public int workerPoolPriority() {
            return workerPoolPriority;
        }
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
            registerObsolete("cairo.sql.asof.join.fast");
            registerObsolete("shared.worker.affinity", PropertyKey.SHARED_NETWORK_WORKER_AFFINITY, PropertyKey.SHARED_QUERY_WORKER_AFFINITY, PropertyKey.SHARED_WRITE_WORKER_AFFINITY);

            registerDeprecated(PropertyKey.CAIRO_TXN_SCOREBOARD_FORMAT);
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
            registerDeprecated(PropertyKey.HTTP_MIN_RECEIVE_BUFFER_SIZE);
            registerDeprecated(PropertyKey.HTTP_RECEIVE_BUFFER_SIZE);
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
                    PropertyKey.PG_NET_SEND_BUF_SIZE,
                    PropertyKey.PG_NET_CONNECTION_SNDBUF
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
                    PropertyKey.LINE_TCP_MSG_BUFFER_SIZE,
                    PropertyKey.LINE_TCP_RECV_BUFFER_SIZE
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
                    PropertyKey.QUERY_TIMEOUT_SEC,
                    PropertyKey.QUERY_TIMEOUT
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
            registerDeprecated(
                    PropertyKey.CAIRO_SQL_COLUMN_CAST_MODEL_POOL_CAPACITY,
                    PropertyKey.CAIRO_SQL_CREATE_TABLE_COLUMN_MODEL_POOL_CAPACITY
            );
            registerDeprecated(PropertyKey.PG_INSERT_POOL_CAPACITY);
            registerDeprecated(PropertyKey.LINE_UDP_TIMESTAMP);
            registerDeprecated(PropertyKey.LINE_TCP_TIMESTAMP);
            registerDeprecated(PropertyKey.CAIRO_SQL_JIT_ROWS_THRESHOLD);
            registerDeprecated(PropertyKey.CAIRO_COMPACT_MAP_LOAD_FACTOR);
            registerDeprecated(PropertyKey.CAIRO_DEFAULT_MAP_TYPE);
            registerDeprecated(PropertyKey.HTTP_JSON_QUERY_DOUBLE_SCALE);
            registerDeprecated(PropertyKey.HTTP_JSON_QUERY_FLOAT_SCALE);
            registerDeprecated(PropertyKey.CAIRO_SQL_DOUBLE_CAST_SCALE);
            registerDeprecated(PropertyKey.CAIRO_SQL_FLOAT_CAST_SCALE);
            registerDeprecated(PropertyKey.CAIRO_MAT_VIEW_MIN_REFRESH_INTERVAL);
            registerDeprecated(PropertyKey.CAIRO_SYMBOL_TABLE_APPEND_PAGE_SIZE);
            registerDeprecated(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_PRETOUCH_ENABLED);
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
                sb.append("    Deprecated settings (recognized but optionally superseded by newer settings):\n");
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

    private static class RedirectPropertyKey implements ConfigPropertyKey {
        final String envVarName;
        final String propertyPath;

        public RedirectPropertyKey(int index) {
            this.propertyPath = PropertyKey.HTTP_REDIRECT_PREFIX.getPropertyPath() + index;
            this.envVarName = ServerMain.propertyPathToEnvVarName(propertyPath);
        }

        @Override
        public String getEnvVarName() {
            return envVarName;
        }

        @Override
        public String getPropertyPath() {
            return propertyPath;
        }

        @Override
        public boolean isDebug() {
            return false;
        }

        @Override
        public boolean isSensitive() {
            return false;
        }
    }

    public record ValidationResult(boolean isError, String message) {
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
        public boolean autoScaleSymbolCapacity() {
            return cairoAutoScaleSymbolCapacity;
        }

        @Override
        public double autoScaleSymbolCapacityThreshold() {
            return cairoAutoScaleSymbolCapacityThreshold;
        }

        @Override
        public boolean cairoResourcePoolTracingEnabled() {
            return cairoResourcePoolTracingEnabled;
        }

        @Override
        public boolean enableTestFactories() {
            return enableTestFactories;
        }

        @Override
        public boolean exportConfiguration(CharSink<?> sink) {
            final String releaseType = getReleaseType();
            str(RELEASE_TYPE, releaseType, sink);
            str(RELEASE_VERSION, getBuildInformation().getSwVersion(), sink);
            if (Chars.equalsNc(releaseType, OSS)) {
                bool(PropertyKey.HTTP_SETTINGS_READONLY.getPropertyPath(), httpSettingsReadOnly, sink);
            }
            if (!Chars.empty(httpUsername)) {
                bool(ACL_ENABLED, true, sink);
            }
            arrayStr(ACCEPTING_WRITES, getAcceptingWrites(), sink);
            arrayStr(ILP_PROTO_SUPPORT_VERSIONS_NAME, ILP_PROTO_SUPPORT_VERSIONS, sink);
            arrayStr(ILP_PROTO_TRANSPORTS, ilpProtoTransports, sink);
            return true;
        }

        @Override
        public @Nullable ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> getAllPairs() {
            return allPairs;
        }

        @Override
        public boolean getAllowTableRegistrySharedWrite() {
            return allowTableRegistrySharedWrite;
        }

        @Override
        public boolean getAsyncMunmapEnabled() {
            return asyncMunmapEnabled;
        }

        @Override
        public @NotNull String getAttachPartitionSuffix() {
            return cairoAttachPartitionSuffix;
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
        public boolean getCairoSqlLegacyOperatorPrecedence() {
            return cairoSqlLegacyOperatorPrecedence;
        }

        @Override
        public boolean getCheckpointRecoveryRebuildColumnIndexes() {
            return checkpointRecoveryRebuildColumnIndexes;
        }

        @Override
        public int getCheckpointRecoveryThreadpoolMax() {
            return checkpointRecoveryThreadpoolMax;
        }

        @Override
        public int getCheckpointRecoveryThreadpoolMin() {
            return checkpointRecoveryThreadpoolMin;
        }

        @Override
        public @NotNull CharSequence getCheckpointRoot() {
            return checkpointRoot;
        }

        @Override
        public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
            return circuitBreakerConfiguration;
        }

        @Override
        public int getColumnAliasGeneratedMaxSize() {
            return cairoSqlColumnAliasGeneratedMaxSize;
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
        public long getCommitLatency() {
            return cairoCommitLatency;
        }

        @Override
        public int getCommitMode() {
            return commitMode;
        }

        @Override
        public int getCompileViewModelPoolCapacity() {
            return sqlCompileViewModelPoolCapacity;
        }

        @Override
        public @NotNull CharSequence getConfRoot() {
            return confRoot;
        }

        @Override
        public int getCopierType() {
            return copierType;
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
        public int getCreateTableColumnModelPoolCapacity() {
            return sqlCreateTableColumnModelPoolCapacity;
        }

        @Override
        public long getCreateTableModelBatchSize() {
            return sqlCreateTableModelBatchSize;
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
        public @Nullable String getDbLogName() {
            return dbLogName;
        }

        @Override
        public @NotNull String getDbRoot() {
            return dbRoot;
        }

        @Override
        public boolean getDebugWalApplyBlockFailureNoRetry() {
            return debugWalApplyBlockFailureNoRetry;
        }

        @Override
        public @NotNull DateLocale getDefaultDateLocale() {
            return locale;
        }

        @Override
        public int getDefaultSeqPartTxnCount() {
            return defaultSeqPartTxnCount;
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
        public int getExplainPoolCapacity() {
            return sqlExplainModelPoolCapacity;
        }

        @Override
        public @NotNull FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public boolean getFileDescriptorCacheEnabled() {
            return fileDescriptorCacheEnabled;
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
        public long getGroupByAllocatorDefaultChunkSize() {
            return sqlGroupByAllocatorChunkSize;
        }

        @Override
        public long getGroupByAllocatorMaxChunkSize() {
            return sqlGroupByAllocatorMaxChunkSize;
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
        public long getGroupByParallelTopKThreshold() {
            return cairoGroupByTopKThreshold;
        }

        @Override
        public int getGroupByPoolCapacity() {
            return sqlGroupByPoolCapacity;
        }

        @Override
        public long getGroupByPresizeMaxCapacity() {
            return cairoGroupByPresizeMaxCapacity;
        }

        @Override
        public long getGroupByPresizeMaxHeapSize() {
            return cairoGroupByPresizeMaxHeapSize;
        }

        @Override
        public int getGroupByShardingThreshold() {
            return cairoGroupByShardingThreshold;
        }

        @Override
        public int getGroupByTopKQueueCapacity() {
            return cairoGroupByTopKQueueCapacity;
        }

        @Override
        public int getIdGenerateBatchStep() {
            return idGenerateBatchStep;
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
        public long getInactiveViewWalWriterTTL() {
            return inactiveViewWalWriterTTL;
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
        public long getInsertModelBatchSize() {
            return sqlInsertModelBatchSize;
        }

        @Override
        public int getInsertModelPoolCapacity() {
            return sqlInsertModelPoolCapacity;
        }

        @Override
        public @NotNull String getInstallRoot() {
            return installRoot;
        }

        @Override
        public int getLatestByQueueCapacity() {
            return latestByQueueCapacity;
        }

        @Override
        public @NotNull CharSequence getLegacyCheckpointRoot() {
            return legacyCheckpointRoot;
        }

        @Override
        public boolean getLogLevelVerbose() {
            return logLevelVerbose;
        }

        @Override
        public boolean getLogSqlQueryProgressExe() {
            return logSqlQueryProgressExe;
        }

        @Override
        public DateFormat getLogTimestampFormat() {
            return logTimestampFormat;
        }

        @Override
        public @Nullable String getLogTimestampTimezone() {
            return logTimestampTimezone;
        }

        @Override
        public DateLocale getLogTimestampTimezoneLocale() {
            return logTimestampLocale;
        }

        @Override
        public TimeZoneRules getLogTimestampTimezoneRules() {
            return logTimestampTimezoneRules;
        }

        @Override
        public long getMatViewInsertAsSelectBatchSize() {
            return matViewInsertAsSelectBatchSize;
        }

        @Override
        public int getMatViewMaxRefreshIntervals() {
            return matViewMaxRefreshIntervals;
        }

        @Override
        public int getMatViewMaxRefreshRetries() {
            return matViewMaxRefreshRetries;
        }

        @Override
        public long getMatViewMaxRefreshStepUs() {
            return matViewMaxRefreshStepUs;
        }

        @Override
        public long getMatViewRefreshIntervalsUpdatePeriod() {
            return matViewRefreshIntervalsUpdatePeriod;
        }

        @Override
        public long getMatViewRefreshOomRetryTimeout() {
            return matViewRefreshOomRetryTimeout;
        }

        @Override
        public long getMatViewRowsPerQueryEstimate() {
            return matViewRowsPerQueryEstimate;
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
        public int getMaxSqlRecompileAttempts() {
            return maxSqlRecompileAttempts;
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
        public Metrics getMetrics() {
            return metrics;
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
            return o3MaxLagUs;
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
        public int getParquetExportCompressionCodec() {
            return parquetExportCompressionCodec;
        }

        @Override
        public int getParquetExportCompressionLevel() {
            return parquetExportCompressionLevel;
        }

        @Override
        public int getParquetExportCopyReportFrequencyLines() {
            return parquetExportCopyReportFrequencyLines;
        }

        @Override
        public long getParquetExportBatchSize() {
            return parquetExportBatchSize;
        }

        @Override
        public int getParquetExportDataPageSize() {
            return parquetExportDataPageSize;
        }

        @Override
        public int getParquetExportRowGroupSize() {
            return parquetExportRowGroupSize;
        }

        @Override
        public CharSequence getParquetExportTableNamePrefix() {
            return parquetExportTableNamePrefix;
        }

        @Override
        public int getParquetExportVersion() {
            return parquetExportVersion;
        }

        @Override
        public int getPartitionEncoderParquetCompressionCodec() {
            return partitionEncoderParquetCompressionCodec;
        }

        @Override
        public int getPartitionEncoderParquetCompressionLevel() {
            return partitionEncoderParquetCompressionLevel;
        }

        @Override
        public int getPartitionEncoderParquetDataPageSize() {
            return partitionEncoderParquetDataPageSize;
        }

        @Override
        public int getPartitionEncoderParquetRowGroupSize() {
            return partitionEncoderParquetRowGroupSize;
        }

        @Override
        public int getPartitionEncoderParquetVersion() {
            return partitionEncoderParquetVersion;
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
        public int getPivotColumnPoolCapacity() {
            return sqlPivotForColumnPoolCapacity;
        }

        @Override
        public int getPoolSegmentSize() {
            return poolSegmentSize;
        }

        @Override
        public int getPreferencesStringPoolCapacity() {
            return preferencesStringPoolCapacity;
        }

        @Override
        public int getQueryCacheEventQueueCapacity() {
            return queryCacheEventQueueCapacity;
        }

        @Override
        public int getQueryRegistryPoolSize() {
            return sqlQueryRegistryPoolSize;
        }

        @Override
        public int getReaderPoolMaxSegments() {
            return readerPoolMaxSegments;
        }

        @Override
        public int getRecentWriteTrackerCapacity() {
            return recentWriteTrackerCapacity;
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
        public int getRmdirMaxDepth() {
            return rmdirMaxDepth;
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
        public boolean getSampleByDefaultAlignmentCalendar() {
            return sqlSampleByDefaultAlignment;
        }

        @Override
        public int getSampleByIndexSearchPageSize() {
            return sqlSampleByIndexSearchPageSize;
        }

        @Override
        public long getSequencerCheckInterval() {
            return sequencerCheckInterval;
        }

        @Override
        public @NotNull CharSequence getSnapshotInstanceId() {
            return snapshotInstanceId;
        }

        @Override
        public long getSpinLockTimeout() {
            return spinLockTimeout;
        }

        @Override
        public int getSqlAsOfJoinLookAhead() {
            return sqlAsOfJoinLookahead;
        }

        @Override
        public int getSqlAsOfJoinMapEvacuationThreshold() {
            return sqlAsOfJoinEvacuationThreshold;
        }

        @Override
        public int getSqlAsOfJoinShortCircuitCacheCapacity() {
            return sqlAsOfJoinShortCircuitCacheCapacity;
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
        public int getSqlCompilerPoolCapacity() {
            return sqlCompilerPoolCapacity;
        }

        @Override
        public int getSqlCopyBufferSize() {
            return sqlCopyBufferSize;
        }

        @Override
        public int getSqlCopyExportQueueCapacity() {
            return cairoSqlCopyExportQueueCapacity;
        }

        @Override
        public CharSequence getSqlCopyExportRoot() {
            return cairoSqlCopyExportRoot;
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
        public int getSqlIntervalIncrementalMergeThreshold() {
            return sqlIntervalIncrementalMergeThreshold;
        }

        @Override
        public int getSqlIntervalMaxBracketDepth() {
            return sqlIntervalMaxBracketDepth;
        }

        @Override
        public int getSqlIntervalMaxIntervalsAfterMerge() {
            return sqlIntervalMaxIntervalsAfterMerge;
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
        public int getSqlJitMaxInListSizeThreshold() {
            return sqlJitMaxInListSizeThreshold;
        }

        @Override
        public int getSqlJitMode() {
            return sqlJitMode;
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
        public int getSqlOrderByRadixSortThreshold() {
            return sqlOrderByRadixSortThreshold;
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
        public int getSqlParallelFilterDispatchLimit() {
            return sqlParallelFilterDispatchLimit;
        }

        @Override
        public double getSqlParallelFilterPreTouchThreshold() {
            return sqlParallelFilterPreTouchThreshold;
        }

        @Override
        public long getSqlParallelWorkStealingSpinTimeout() {
            return sqlParallelWorkStealingSpinTimeout;
        }

        @Override
        public int getSqlParallelWorkStealingThreshold() {
            return sqlParallelWorkStealingThreshold;
        }

        @Override
        public int getSqlParquetFrameCacheCapacity() {
            return sqlParquetFrameCacheCapacity;
        }

        @Override
        public int getSqlPivotMaxProducedColumns() {
            return sqlPivotMaxProducedColumns;
        }

        @Override
        public int getSqlSmallMapKeyCapacity() {
            return sqlSmallMapKeyCapacity;
        }

        @Override
        public long getSqlSmallMapPageSize() {
            return sqlSmallMapPageSize;
        }

        @Override
        public int getSqlSmallPageFrameMaxRows() {
            return sqlSmallPageFrameMaxRows;
        }

        @Override
        public int getSqlSmallPageFrameMinRows() {
            return sqlSmallPageFrameMinRows;
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
        public int getSqlUnorderedMapMaxEntrySize() {
            return sqlUnorderedMapMaxEntrySize;
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
        public long getSymbolTableMaxAllocationPageSize() {
            return symbolTableMaxAllocationPageSize;
        }

        @Override
        public long getSymbolTableMinAllocationPageSize() {
            return symbolTableMinAllocationPageSize;
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
        public long getSystemWalEventAppendPageSize() {
            return systemWalWriterEventAppendPageSize;
        }

        @Override
        public long getTableRegistryAutoReloadFrequency() {
            return cairoTableRegistryAutoReloadFrequency;
        }

        @Override
        public int getTableRegistryCompactionThreshold() {
            return cairoTableRegistryCompactionThreshold;
        }

        @Override
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
        public int getViewLexerPoolCapacity() {
            return sqlViewLexerPoolCapacity;
        }

        @Override
        public int getViewWalWriterPoolMaxSegments() {
            return viewWalWriterPoolMaxSegments;
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
        public long getWalEventAppendPageSize() {
            return walWriterEventAppendPageSize;
        }

        @Override
        public double getWalLagRowsMultiplier() {
            return walSquashUncommittedRowsMultiplier;
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
        public long getWriteBackOffTimeoutOnMemPressureMs() {
            return cairoWriteBackOffTimeoutOnMemPressureMs;
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
        public int getWriterFileOpenOpts() {
            return writerFileOpenOpts;
        }

        @Override
        public int getWriterTickRowsCountMod() {
            return writerTickRowsCountMod;
        }

        @Override
        public boolean isCheckpointRecoveryEnabled() {
            return checkpointRecoveryEnabled;
        }

        @Override
        public boolean isColumnAliasExpressionEnabled() {
            return cairoSqlColumnAliasExpressionEnabled;
        }

        @Override
        public boolean isCopierChunkedEnabled() {
            return copierChunkedEnabled;
        }

        @Override
        public boolean isDevModeEnabled() {
            return devModeEnabled;
        }

        @Override
        public boolean isGroupByPresizeEnabled() {
            return cairoGroupByPresizeEnabled;
        }

        @Override
        public boolean isIOURingEnabled() {
            return ioURingEnabled;
        }

        @Override
        public boolean isMatViewEnabled() {
            return matViewEnabled;
        }

        @Override
        public boolean isMatViewParallelSqlEnabled() {
            return matViewParallelExecutionEnabled;
        }

        @Override
        public boolean isMatViewRefreshMissingWalFilesFatal() {
            return matViewRefreshMissingWalFilesFatal;
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
        public boolean isParquetExportRawArrayEncoding() {
            return parquetExportRawArrayEncoding;
        }

        @Override
        public boolean isParquetExportStatisticsEnabled() {
            return parquetExportStatisticsEnabled;
        }

        @Override
        public boolean isPartitionEncoderParquetRawArrayEncoding() {
            return partitionEncoderParquetRawArrayEncoding;
        }

        @Override
        public boolean isPartitionEncoderParquetStatisticsEnabled() {
            return partitionEncoderParquetStatisticsEnabled;
        }

        @Override
        public boolean isPartitionO3OverwriteControlEnabled() {
            return o3PartitionOverwriteControlEnabled;
        }

        @Override
        public boolean isQueryTracingEnabled() {
            return isQueryTracingEnabled;
        }

        @Override
        public boolean isReadOnlyInstance() {
            return isReadOnlyInstance;
        }

        @Override
        public boolean isSqlJitDebugEnabled() {
            return sqlJitDebugEnabled;
        }

        @Override
        public boolean isSqlOrderBySortEnabled() {
            return sqlOrderBySortEnabled;
        }

        @Override
        public boolean isSqlParallelFilterEnabled() {
            return sqlParallelFilterEnabled;
        }

        @Override
        public boolean isSqlParallelGroupByEnabled() {
            return sqlParallelGroupByEnabled;
        }

        @Override
        public boolean isSqlParallelHorizonJoinEnabled() {
            return sqlParallelHorizonJoinEnabled;
        }

        @Override
        public boolean isSqlParallelReadParquetEnabled() {
            return sqlParallelReadParquetEnabled;
        }

        @Override
        public boolean isSqlParallelTopKEnabled() {
            return sqlParallelTopKEnabled;
        }

        @Override
        public boolean isSqlParallelWindowJoinEnabled() {
            return sqlParallelWindowJoinEnabled;
        }

        @Override
        public boolean isTableTypeConversionEnabled() {
            return tableTypeConversionEnabled;
        }

        @Override
        public boolean isTtlWallClockEnabled() {
            return ttlUseWallClock;
        }

        @Override
        public boolean isValidateSampleByFillType() {
            return sqlSampleByValidateFillType;
        }

        @Override
        public boolean isWalApplyEnabled() {
            return walApplyEnabled;
        }

        @Override
        public boolean isWalApplyParallelSqlEnabled() {
            return walParallelExecutionEnabled;
        }

        @Override
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

        @Override
        public int maxArrayElementCount() {
            return sqlMaxArrayElementCount;
        }

        @Override
        public boolean useWithinLatestByOptimisation() {
            return queryWithinLatestByOptimisationEnabled;
        }
    }

    private class PropExportPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return exportWorkerNapThreshold;
        }

        @Override
        public String getPoolName() {
            return "export";
        }

        @Override
        public long getSleepThreshold() {
            return exportWorkerSleepThreshold;
        }

        @Override
        public long getSleepTimeout() {
            return exportWorkerSleepTimeout;
        }

        @Override
        public int[] getWorkerAffinity() {
            return exportWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return exportWorkerCount;
        }

        @Override
        public long getYieldThreshold() {
            return exportWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return exportWorkerHaltOnError;
        }

        @Override
        public boolean isEnabled() {
            return exportWorkerCount > 0;
        }
    }

    public class PropHttpConcurrentCacheConfiguration implements ConcurrentCacheConfiguration {
        @Override
        public int getBlocks() {
            return httpSqlCacheBlockCount;
        }

        @Override
        public LongGauge getCachedGauge() {
            return metrics.jsonQueryMetrics().cachedQueriesGauge();
        }

        @Override
        public Counter getHiCounter() {
            return metrics.jsonQueryMetrics().cacheHitCounter();
        }

        @Override
        public Counter getMissCounter() {
            return metrics.jsonQueryMetrics().cacheMissCounter();
        }

        @Override
        public int getRows() {
            return httpSqlCacheRowCount;
        }
    }

    public class PropHttpMinServerConfiguration implements HttpServerConfiguration {

        @Override
        public long getAcceptLoopTimeout() {
            return httpMinNetAcceptLoopTimeout;
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
        public LongGauge getConnectionCountGauge() {
            return metrics.httpMetrics().connectionCountGauge();
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
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
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
        public HttpContextConfiguration getHttpContextConfiguration() {
            return httpMinContextConfiguration;
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
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return httpMinWorkerNapThreshold;
        }

        @Override
        public int getNetRecvBufferSize() {
            return httpMinNetConnectionRcvBuf;
        }

        @Override
        public int getNetSendBufferSize() {
            return httpMinNetConnectionSndBuf;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public String getPoolName() {
            return "minhttp";
        }

        @Override
        public long getQueueTimeout() {
            return httpMinNetConnectionQueueTimeout;
        }

        @Override
        public int getRecvBufferSize() {
            return httpMinRecvBufferSize;
        }

        @Override
        public byte getRequiredAuthType() {
            return httpHealthCheckAuthType;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSendBufferSize() {
            return httpMinSendBufferSize;
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
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return httpMinNetConnectionTimeout;
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
        public boolean isPessimisticHealthCheckEnabled() {
            return httpPessimisticHealthCheckEnabled;
        }

        @Override
        public Counter listenerStateChangeCounter() {
            return metrics.httpMetrics().listenerStateChangeCounter();
        }

        @Override
        public boolean preAllocateBuffers() {
            return true;
        }

        @Override
        public int workerPoolPriority() {
            return httpMinWorkerPoolPriority;
        }
    }

    public class PropHttpServerConfiguration implements HttpFullFatServerConfiguration {

        @Override
        public long getAcceptLoopTimeout() {
            return httpNetAcceptLoopTimeout;
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
        public ConcurrentCacheConfiguration getConcurrentCacheConfiguration() {
            return httpMinConcurrentCacheConfiguration;
        }

        @Override
        public LongGauge getConnectionCountGauge() {
            return metrics.httpMetrics().connectionCountGauge();
        }

        @Override
        public ObjHashSet<String> getContextPathExec() {
            return httpContextPathSqlExecute;
        }

        @Override
        public ObjHashSet<String> getContextPathExport() {
            return httpContextPathExport;
        }

        @Override
        public ObjHashSet<String> getContextPathILP() {
            return httpContextPathILP;
        }

        @Override
        public ObjHashSet<String> getContextPathILPPing() {
            return httpContextPathILPPing;
        }

        @Override
        public ObjHashSet<String> getContextPathImport() {
            return httpContextPathImport;
        }

        @Override
        public ObjHashSet<String> getContextPathSettings() {
            return httpContextPathSettings;
        }

        @Override
        public ObjHashSet<String> getContextPathSqlValidation() {
            return httpContextPathSqlValidate;
        }

        @Override
        public ObjHashSet<String> getContextPathTableStatus() {
            return httpContextPathTableStatus;
        }

        @Override
        public ObjHashSet<String> getContextPathWarnings() {
            return httpContextPathWarnings;
        }

        @Override
        public String getContextPathWebConsole() {
            return httpContextWebConsole;
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
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
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
        public HttpContextConfiguration getHttpContextConfiguration() {
            return httpContextConfiguration;
        }

        @Override
        public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
            return jsonQueryProcessorConfiguration;
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
        public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
            return lineHttpProcessorConfiguration;
        }

        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return httpWorkerNapThreshold;
        }

        @Override
        public int getNetRecvBufferSize() {
            return httpNetConnectionRcvBuf;
        }

        @Override
        public int getNetSendBufferSize() {
            return httpNetConnectionSndBuf;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public String getPassword() {
            return httpPassword;
        }

        @Override
        public String getPoolName() {
            return "http";
        }

        @Override
        public long getQueueTimeout() {
            return httpNetConnectionQueueTimeout;
        }

        @Override
        public int getRecvBufferSize() {
            return httpRecvBufferSize;
        }

        @Override
        public byte getRequiredAuthType() {
            return httpHealthCheckAuthType;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSendBufferSize() {
            return httpSendBufferSize;
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
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return httpNetConnectionTimeout;
        }

        @Override
        public String getUsername() {
            return httpUsername;
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
        public boolean isAcceptingWrites() {
            return !isReadOnlyInstance
                    && httpServerEnabled
                    && lineHttpEnabled
                    && !httpContextConfiguration.readOnlySecurityContext();
        }

        @Override
        public boolean isEnabled() {
            return httpServerEnabled;
        }

        @Override
        public boolean isPessimisticHealthCheckEnabled() {
            return httpPessimisticHealthCheckEnabled;
        }

        @Override
        public boolean isQueryCacheEnabled() {
            return httpSqlCacheEnabled;
        }

        @Override
        public boolean isSettingsReadOnly() {
            return httpSettingsReadOnly;
        }

        @Override
        public Counter listenerStateChangeCounter() {
            return metrics.httpMetrics().listenerStateChangeCounter();
        }

        @Override
        public boolean preAllocateBuffers() {
            return false;
        }
    }

    public class PropJsonQueryProcessorConfiguration implements JsonQueryProcessorConfiguration {

        @Override
        public int getConnectionCheckFrequency() {
            return jsonQueryConnectionCheckFrequency;
        }

        @Override
        public long getExportTimeout() {
            return httpExportTimeout;
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
        public CharSequence getKeepAliveHeader() {
            return keepAliveHeader;
        }

        @Override
        public long getMaxQueryResponseRowLimit() {
            return maxHttpQueryResponseRowLimit;
        }

        @Override
        public MillisecondClock getMillisecondClock() {
            return httpFrozenClock ? StationaryMillisClock.INSTANCE : MillisecondClockImpl.INSTANCE;
        }

        @Override
        public NanosecondClock getNanosecondClock() {
            return httpFrozenClock ? StationaryNanosClock.INSTANCE : NanosecondClockImpl.INSTANCE;
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
        public CairoConfiguration getCairoConfiguration() {
            return cairoConfiguration;
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
        public int getDefaultTimestampColumnType() {
            return lineDefaultTimestampColumnType;
        }

        @Override
        public CharSequence getInfluxPingVersion() {
            return lineHttpPingVersion;
        }

        @Override
        public long getMaxRecvBufferSize() {
            return httpRecvMaxBufferSize;
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return microsecondClock;
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return symbolCacheWaitBeforeReload;
        }

        @Override
        public byte getTimestampUnit() {
            return lineTcpTimestampUnit;
        }

        @Override
        public boolean isEnabled() {
            return lineHttpEnabled;
        }

        @Override
        public boolean isStringToCharCastAllowed() {
            return stringToCharCastAllowed;
        }

        @Override
        public boolean isUseLegacyStringDefault() {
            return useLegacyStringDefault;
        }

        @Override
        public boolean logMessageOnError() {
            return lineLogMessageOnError;
        }
    }

    private class PropLineTcpIOWorkerPoolConfiguration implements WorkerPoolConfiguration {

        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return lineTcpIOWorkerNapThreshold;
        }

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
        public long getAcceptLoopTimeout() {
            return lineTcpNetAcceptLoopTimeout;
        }

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
        public int getBindIPv4Address() {
            return lineTcpNetBindIPv4Address;
        }

        @Override
        public int getBindPort() {
            return lineTcpNetBindPort;
        }

        @Override
        public CairoConfiguration getCairoConfiguration() {
            return cairoConfiguration;
        }

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public long getCommitInterval() {
            return LineTcpReceiverConfigurationHelper.calcCommitInterval(
                    cairoConfiguration.getO3MinLag(),
                    getCommitIntervalFraction(),
                    getCommitIntervalDefault()
            );
        }

        @Override
        public long getCommitIntervalDefault() {
            return lineTcpCommitIntervalDefault;
        }

        @Override
        public double getCommitIntervalFraction() {
            return lineTcpCommitIntervalFraction;
        }

        @Override
        public LongGauge getConnectionCountGauge() {
            return metrics.lineMetrics().tcpConnectionCountGauge();
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
        public int getDefaultCreateTimestampColumnType() {
            return lineDefaultTimestampColumnType;
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
        public String getDispatcherLogName() {
            return "tcp-line-server";
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
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
        public long getMaxRecvBufferSize() {
            return lineTcpMaxRecvBufferSize;
        }

        @Override
        public Metrics getMetrics() {
            return metrics;
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
        public int getNetRecvBufferSize() {
            return lineTcpNetConnectionRcvBuf;
        }

        @Override
        public int getNetSendBufferSize() {
            return -1;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public WorkerPoolConfiguration getNetworkWorkerPoolConfiguration() {
            return lineTcpIOWorkerPoolConfiguration;
        }

        @Override
        public long getQueueTimeout() {
            return lineTcpNetConnectionQueueTimeout;
        }

        @Override
        public int getRecvBufferSize() {
            return lineTcpRecvBufferSize;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getSendBufferSize() {
            return -1;
        }

        @Override
        public long getSymbolCacheWaitBeforeReload() {
            return symbolCacheWaitBeforeReload;
        }

        @Override
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return lineTcpNetConnectionTimeout;
        }

        @Override
        public byte getTimestampUnit() {
            return lineTcpTimestampUnit;
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
        public boolean isStringToCharCastAllowed() {
            return stringToCharCastAllowed;
        }

        @Override
        public boolean isUseLegacyStringDefault() {
            return useLegacyStringDefault;
        }

        @Override
        public Counter listenerStateChangeCounter() {
            return metrics.lineMetrics().aboveMaxConnectionCountCounter();
        }

        @Override
        public boolean logMessageOnError() {
            return lineLogMessageOnError;
        }
    }

    private class PropLineTcpWriterWorkerPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return lineTcpWriterWorkerNapThreshold;
        }

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
        public byte getTimestampUnit() {
            return lineUdpTimestampUnit;
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
        public boolean isUseLegacyStringDefault() {
            return useLegacyStringDefault;
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

    private class PropMatViewsRefreshPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return matViewRefreshWorkerNapThreshold;
        }

        @Override
        public String getPoolName() {
            return "mat-view-refresh";
        }

        @Override
        public long getSleepThreshold() {
            return matViewRefreshWorkerSleepThreshold;
        }

        @Override
        public long getSleepTimeout() {
            return matViewRefreshSleepTimeout;
        }

        @Override
        public int[] getWorkerAffinity() {
            return matViewRefreshWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return matViewRefreshWorkerCount;
        }

        @Override
        public long getYieldThreshold() {
            return matViewRefreshWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return matViewRefreshWorkerHaltOnError;
        }

        @Override
        public boolean isEnabled() {
            return matViewRefreshWorkerCount > 0;
        }
    }

    private class PropMetricsConfiguration implements MetricsConfiguration {

        @Override
        public boolean isEnabled() {
            return metricsEnabled;
        }
    }

    private class PropPGConfiguration implements PGConfiguration {

        @Override
        public long getAcceptLoopTimeout() {
            return pgNetAcceptLoopTimeout;
        }

        @Override
        public int getBinParamCountCapacity() {
            return pgBinaryParamsCapacity;
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
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public ConcurrentCacheConfiguration getConcurrentCacheConfiguration() {
            return pgWireConcurrentCacheConfiguration;
        }

        @Override
        public LongGauge getConnectionCountGauge() {
            return metrics.pgWireMetrics().connectionCountGauge();
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
        public String getDispatcherLogName() {
            return "pg-server";
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public int getForceRecvFragmentationChunkSize() {
            return pgForceRecvFragmentationChunkSize;
        }

        @Override
        public int getForceSendFragmentationChunkSize() {
            return pgForceSendFragmentationChunkSize;
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
        public int getInsertCacheBlockCount() {
            return pgInsertCacheBlockCount;
        }

        @Override
        public int getInsertCacheRowCount() {
            return pgInsertCacheRowCount;
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
        public int getMaxBlobSizeOnQuery() {
            return pgMaxBlobSizeOnQuery;
        }

        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public int getNamedStatementCacheCapacity() {
            return pgNamedStatementCacheCapacity;
        }

        @Override
        public int getNamedStatementLimit() {
            return pgNamedStatementLimit;
        }

        @Override
        public int getNamesStatementPoolCapacity() {
            return pgNamesStatementPoolCapacity;
        }

        @Override
        public long getNapThreshold() {
            return pgWorkerNapThreshold;
        }

        @Override
        public int getNetRecvBufferSize() {
            return pgNetConnectionRcvBuf;
        }

        @Override
        public int getNetSendBufferSize() {
            return pgNetConnectionSndBuf;
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
        public int getPipelineCapacity() {
            return pgPipelineCapacity;
        }

        @Override
        public String getPoolName() {
            return "pgwire";
        }

        @Override
        public long getQueueTimeout() {
            return pgNetConnectionQueueTimeout;
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
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
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
        public int getTestConnectionBufferSize() {
            return netTestConnectionBufferSize;
        }

        @Override
        public long getTimeout() {
            return pgNetIdleConnectionTimeout;
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
        public Counter listenerStateChangeCounter() {
            return metrics.pgWireMetrics().listenerStateChangeCounter();
        }

        @Override
        public boolean readOnlySecurityContext() {
            return pgReadOnlySecurityContext || isReadOnlyInstance;
        }
    }

    private class PropPGWireConcurrentCacheConfiguration implements ConcurrentCacheConfiguration {
        @Override
        public int getBlocks() {
            return pgSelectCacheBlockCount;
        }

        @Override
        public LongGauge getCachedGauge() {
            return metrics.pgWireMetrics().cachedSelectsGauge();
        }

        @Override
        public Counter getHiCounter() {
            return metrics.pgWireMetrics().selectCacheHitCounter();
        }

        @Override
        public Counter getMissCounter() {
            return metrics.pgWireMetrics().selectCacheMissCounter();
        }

        @Override
        public int getRows() {
            return pgSelectCacheRowCount;
        }
    }

    class PropPublicPassthroughConfiguration implements PublicPassthroughConfiguration {
        @Override
        public boolean exportConfiguration(CharSink<?> sink) {
            bool(PropertyKey.POSTHOG_ENABLED.getPropertyPath(), isPosthogEnabled(), sink);
            str(PropertyKey.POSTHOG_API_KEY.getPropertyPath(), getPosthogApiKey(), sink);
            integer(PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH.toString(), maxFileNameLength, sink);
            return true;
        }

        @Override
        public String getPosthogApiKey() {
            return posthogApiKey;
        }

        @Override
        public boolean isPosthogEnabled() {
            return posthogEnabled;
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

    public class PropStaticContentProcessorConfiguration implements StaticContentProcessorConfiguration {

        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
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
        public Utf8SequenceObjHashMap<Utf8Sequence> getRedirectMap() {
            return redirectMap;
        }

        @Override
        public byte getRequiredAuthType() {
            return SecurityContext.AUTH_TYPE_NONE;
        }
    }

    private class PropTelemetryConfiguration implements TelemetryConfiguration {
        private final long ThrottleInterval;
        private final Boolean enabled;
        private final boolean hideTable;
        private final int queueCapacity;
        private final int ttlWeeks;

        PropTelemetryConfiguration(boolean enabled, int queueCapacity, boolean hideTable, int ttlWeeks, long ThrottleInterval) {
            this.enabled = enabled;
            this.queueCapacity = queueCapacity;
            this.hideTable = hideTable;
            this.ttlWeeks = ttlWeeks;
            this.ThrottleInterval = ThrottleInterval;
        }

        @Override
        public long getDbSizeEstimateTimeout() {
            return telemetryDbSizeEstimateTimeout;
        }

        @Override
        public boolean getDisableCompletely() {
            return telemetryDisableCompletely;
        }

        @Override
        public boolean getEnabled() {
            return enabled;
        }

        @Override
        public int getQueueCapacity() {
            return queueCapacity;
        }

        @Override
        public long getThrottleIntervalMicros() {
            return ThrottleInterval;
        }

        @Override
        public int getTtlWeeks() {
            return ttlWeeks;
        }

        @Override
        public boolean hideTables() {
            return hideTable;
        }
    }

    private class PropTextConfiguration implements TextConfiguration {

        @Override
        public int getDateAdapterPoolCapacity() {
            return dateAdapterPoolCapacity;
        }

        @Override
        public int getDecimalAdapterPoolCapacity() {
            return decimalAdapterPoolCapacity;
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

        @Override
        public boolean isUseLegacyStringDefault() {
            return useLegacyStringDefault;
        }
    }

    private class PropViewCompilerPoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return viewCompilerWorkerNapThreshold;
        }

        @Override
        public String getPoolName() {
            return "view-compiler";
        }

        @Override
        public long getSleepThreshold() {
            return viewCompilerWorkerSleepThreshold;
        }

        @Override
        public long getSleepTimeout() {
            return viewCompilerSleepTimeout;
        }

        @Override
        public int[] getWorkerAffinity() {
            return viewCompilerWorkerAffinity;
        }

        @Override
        public int getWorkerCount() {
            return viewCompilerWorkerCount;
        }

        @Override
        public long getYieldThreshold() {
            return viewCompilerWorkerYieldThreshold;
        }

        @Override
        public boolean haltOnError() {
            return viewCompilerWorkerHaltOnError;
        }

        @Override
        public boolean isEnabled() {
            return viewCompilerWorkerCount > 0;
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
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getNapThreshold() {
            return walApplyWorkerNapThreshold;
        }

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

    static {
        WRITE_FO_OPTS.put("o_direct", CairoConfiguration.O_DIRECT);
        WRITE_FO_OPTS.put("o_sync", CairoConfiguration.O_SYNC);
        WRITE_FO_OPTS.put("o_async", CairoConfiguration.O_ASYNC);
        WRITE_FO_OPTS.put("o_none", CairoConfiguration.O_NONE);
    }
}
