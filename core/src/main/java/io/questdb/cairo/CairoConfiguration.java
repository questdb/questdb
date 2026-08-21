/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.BuildInformation;
import io.questdb.ConfigPropertyKey;
import io.questdb.ConfigPropertyValue;
import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.TelemetryConfiguration;
import io.questdb.VolumeDefinitions;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.qwp.codec.DefaultQwpServerInfoProvider;
import io.questdb.cutlass.qwp.codec.QwpServerInfoProvider;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.mp.continuation.DelayedFireable;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.FilesFacade;
import io.questdb.std.IOURingFacade;
import io.questdb.std.IOURingFacadeImpl;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.Rnd;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.RostiAllocFacadeImpl;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.LongSupplier;

public interface CairoConfiguration {

    int O_ASYNC = 0x40;
    int O_DIRECT = 0x4000;
    int O_NONE = 0;
    int O_SYNC = 0x80;
    ThreadLocal<Rnd> RANDOM = new ThreadLocal<>();

    boolean attachPartitionCopy();

    /**
     * Flag to enable or disable symbol capacity auto-scaling. Auto-scaling means resizing
     * symbol table data structures as the number of symbols in the table grows. Optimal sizing of
     * these data structures ensures optimal ingres performance.
     * <p>
     * By default, the auto-scaling is enabled. This is optimal. You may want to disable auto-scaling in case
     * something goes wrong.
     *
     * @return true - auto-scaling is enabled and false - otherwise.
     */
    boolean autoScaleSymbolCapacity();

    /**
     * No-zero positive value. It is used as percentage of symbol counts in
     * the symbol table relative to the table capacity, after which table is resized. For example 0.8 would indicate
     * that as soon as symbol count goes over 80% of the capacity, the symbol table is resized.
     *
     * @return resize threshold
     */
    double autoScaleSymbolCapacityThreshold();

    boolean cairoResourcePoolTracingEnabled();

    default boolean disableColumnPurgeJob() {
        return false;
    }

    boolean enableTestFactories();

    /**
     * Exports subset of configuration parameters into a sink. Configuration
     * parameters are exported in JSON format.
     *
     * @return true if anything was exported
     */
    default boolean exportConfiguration(CharSink<?> sink) {
        return false;
    }

    default boolean freeLeakedReaders() {
        return true;
    }

    /**
     * All effective configuration values are seen by the server instance.
     *
     * @return key value pairs of the configuration
     */
    default @Nullable ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> getAllPairs() {
        return null;
    }

    boolean getAllowTableRegistrySharedWrite();

    // the '+' is used to prevent overlap with table names
    @NotNull
    default String getArchivedCrashFilePrefix() {
        return "crash+";
    }

    boolean getAsyncMunmapEnabled();

    @NotNull
    String getAttachPartitionSuffix();

    int getBinaryEncodingMaxLength();

    int getBindVariablePoolSize();

    @NotNull
    BuildInformation getBuildInformation();

    default boolean getBypassWalFdCache() {
        // If wal fd re-usage is not allowed it means fd cache should not be used for wal and sequencer files.
        // This typically means that those files be renamed/replaced outside QuestDB java code.
        return getWalMaxSegmentFileDescriptorsCache() < 1;
    }

    boolean getCairoSqlLegacyOperatorPrecedence();

    @NotNull
    default CheckpointListener getCheckpointListener() {
        return DefaultCheckpointListener.INSTANCE;
    }

    /**
     * Enable/disable full rebuild of bitmap indexes for symbol columns in partitions
     */
    boolean getCheckpointRecoveryRebuildColumnIndexes();

    /**
     * Maximum thread pool size for checkpoint recovery operations.
     * The actual size is determined by clamping the available processor count between min and max.
     */
    int getCheckpointRecoveryThreadpoolMax();

    /**
     * Minimum thread pool size for checkpoint recovery operations.
     * The actual size is determined by clamping the available processor count between min and max.
     */
    int getCheckpointRecoveryThreadpoolMin();

    @NotNull
    CharSequence getCheckpointRoot(); // same as root/../.checkpoint

    @NotNull
    SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration();

    /**
     * Maximum size for a generated alias, the column will be truncated if it's longer than that. Note
     * that this flag only works if isColumnAliasExpressionEnabled is enabled.
     *
     * @return the maximum size of a generated alias.
     */
    int getColumnAliasGeneratedMaxSize();

    int getColumnIndexerQueueCapacity();

    int getColumnPurgeQueueCapacity();

    long getColumnPurgeRetryDelay();

    long getColumnPurgeRetryDelayLimit();

    double getColumnPurgeRetryDelayMultiplier();

    int getColumnPurgeTaskPoolCapacity();

    long getCommitLatency();

    int getCommitMode();

    int getCompileViewModelPoolCapacity();

    @NotNull
    CharSequence getConfRoot(); // same as root/../conf

    /**
     * Returns the forced copier type for testing, or COPIER_TYPE_DEFAULT for auto-selection.
     * See RecordToRowCopierUtils.COPIER_TYPE_* constants.
     */
    default int getCopierType() {
        return 0; // COPIER_TYPE_DEFAULT
    }

    @NotNull
    LongSupplier getCopyIDSupplier();

    int getCopyPoolCapacity();

    int getCountDistinctCapacity();

    double getCountDistinctLoadFactor();

    int getCreateAsSelectRetryCount();

    int getCreateTableColumnModelPoolCapacity();

    long getCreateTableModelBatchSize();

    long getDataAppendPageSize();

    long getDataIndexKeyAppendPageSize();

    long getDataIndexValueAppendPageSize();

    long getDatabaseIdHi();

    long getDatabaseIdLo();

    @NotNull
    CharSequence getDbDirectory(); // env['cairo.root'], defaults to db

    @Nullable
    String getDbLogName();

    @NotNull
    String getDbRoot(); // some folder with suffix env['cairo.root'] e.g. /.../db

    boolean getDebugWalApplyBlockFailureNoRetry();

    @NotNull
    DateLocale getDefaultDateLocale();

    int getDefaultSeqPartTxnCount();

    boolean getDefaultSymbolCacheFlag();

    int getDefaultSymbolCapacity();

    byte getDefaultSymbolIndexType();

    int getDetachedMkDirMode();

    default Map<String, String> getEnv() {
        return System.getenv();
    }

    int getExplainPoolCapacity();

    @NotNull
    FactoryProvider getFactoryProvider();

    boolean getFileDescriptorCacheEnabled();

    int getFileOperationRetryCount();

    @NotNull
    FilesFacade getFilesFacade();

    long getGroupByAllocatorDefaultChunkSize();

    long getGroupByAllocatorMaxChunkSize();

    int getGroupByBatchSize();

    int getGroupByMapCapacity();

    int getGroupByMergeShardQueueCapacity();

    long getGroupByParallelTopKThreshold();

    int getGroupByPoolCapacity();

    long getGroupByPresizeMaxCapacity();

    long getGroupByPresizeMaxHeapSize();

    int getGroupByShardingThreshold();

    int getGroupByTopKQueueCapacity();

    @NotNull
    default IOURingFacade getIOURingFacade() {
        return IOURingFacadeImpl.INSTANCE;
    }

    int getIdGenerateBatchStep();

    long getIdleCheckInterval();

    int getInactiveReaderMaxOpenPartitions();

    long getInactiveReaderTTL();

    long getInactiveViewWalWriterTTL();

    long getInactiveWalWriterTTL();

    long getInactiveWriterTTL();

    int getIndexValueBlockSize();

    long getInsertModelBatchSize();

    int getInsertModelPoolCapacity();

    /**
     * Installation root, i.e., the directory that usually contains the "conf", "db", etc. directories.
     */
    @NotNull
    String getInstallRoot();

    int getJsonUnnestMaxValueSize();

    int getLatestByQueueCapacity();

    @NotNull
    CharSequence getLegacyCheckpointRoot(); // same as root/../snapshot

    /**
     * Cadence, in live-view checkpoint seals, at which the refresh worker attempts
     * one physical compaction pass over the live view's checkpoint timeline.
     * Compaction repacks the still-live state pages of sparse data segments into a
     * fresh segment and redirects the roots onto it, so the drained segments retire
     * and the purge job reclaims their dead bytes. Zero (the default) disables it,
     * leaving superseded pages to be reclaimed only when their whole segment
     * becomes unreferenced.
     */
    long getLiveViewCheckpointCompactionInterval();

    /**
     * Wall-clock ceiling between consecutive head-checkpoint writes for a
     * live view. The refresh worker writes a fresh head once this duration
     * has elapsed since the prior write, even when
     * {@link #getLiveViewCheckpointRows()} has not been reached. Caps the
     * worst-case O3 / restart replay window for low-rate views.
     */
    long getLiveViewCheckpointMaxDurationMicros();

    /**
     * Cadence, in live-view checkpoint seals, at which the refresh worker sweeps
     * the view's checkpoint directory for segments no generation references any
     * more, unlinking them and staging their catalogue entries for the next seal
     * to remove. Without it a sweep runs only when a worker reconciles the
     * directory - once per process - so every segment a seal, a repair or a
     * compaction supersedes waits for a restart before its bytes come back. Zero
     * disables the cadence, leaving that reconciliation the only sweep.
     */
    long getLiveViewCheckpointPurgeInterval();

    /**
     * Per-turn budget on the base rows one localized out-of-order repair may
     * replay. The repair's convergence boundary makes its work finite, but a
     * dense interval can still hold more rows than one refresh turn should
     * carry, so a replay that crosses this budget stops at the end of the
     * current timestamp group and continues in a later turn. It publishes
     * nothing while suspended: the replacement stays uncommitted in the
     * live-view writer the repair holds, and no generation names its staged
     * roots. The turn also ends on
     * {@link #getLiveViewRefreshTurnMaxDurationMicros()}, whichever comes
     * first. A value {@code <= 0} disables the row budget, leaving the
     * wall-clock one alone to bound the turn.
     */
    long getLiveViewCheckpointRepairReplayMaxRows();

    /**
     * Budget on the partition keys one localized out-of-order repair may plan
     * to re-emit. A timestamp-global replacement re-emits every key with a
     * qualifying row in the replacement interval, and the repair holds a
     * counter per such key while it discovers its bounds, so the key domain
     * sizes both the planning memory and, for an indexed predecessor search,
     * the number of index seeks. A discovery that crosses this budget reports
     * an explicit budget status and hands the repair back to the unlocalized
     * rebuild instead of planning a replacement of that width. A value
     * {@code <= 0} disables the budget.
     */
    long getLiveViewCheckpointRepairScanMaxKeys();

    /**
     * Budget on the base rows one localized out-of-order repair may read while
     * discovering its bounds. Counts rows pulled from the base table across
     * every scan of one discovery - forward convergence search, backward
     * predecessor walk, and per-key indexed seeks alike - including rows the
     * view's {@code WHERE} filter then discards, since those cost the same read.
     * A {@code ROWS N PRECEDING} dependency is discovered rather than derived,
     * and a sparse partition key can spread its {@code N} rows over an
     * arbitrary span, so this is the bound that keeps discovery from costing
     * the view's whole history. Crossing it reports an explicit budget status
     * and leaves the conservative bound in place rather than continuing the
     * scan. A value {@code <= 0} disables the budget.
     */
    long getLiveViewCheckpointRepairScanMaxRows();

    /**
     * Row-count cadence trigger for head-checkpoint writes. The refresh
     * worker writes a fresh head once this many live-view rows have been
     * applied since the prior head. The natural sizing knob for high-rate
     * views: raising it spaces checkpoints further apart at the cost of a
     * larger O3 / restart replay window.
     */
    long getLiveViewCheckpointRows();

    int getLiveViewFlushRetryMax();

    long getLiveViewFlushRetryMaxDurationMicros();

    /**
     * Fast-path growth budget. When the published in-memory slot's footprint
     * already meets or exceeds this size, the refresh worker falls
     * back to a slow-path swap (which evicts rows older than {@code IN MEMORY}
     * and may shrink the slot) instead of appending in place. Acts as a
     * safety backstop against unbounded slot growth between slow-path edges.
     * Operators with an {@code IN MEMORY} window large enough to exceed the
     * default should raise this proportionally to keep the fast-path engaged.
     */
    long getLiveViewInMemoryBufferGrowthBytes();

    long getLiveViewInMemoryBufferInitialBytes();

    long getLiveViewInMemoryMaxMicros();

    /**
     * Reclaimable-partition threshold for the frontier sweep in {@code LiveViewWindow}. The
     * sweep drops the anchor-map partitions that have fallen behind the previous anchor
     * bucket, and it runs only once the anchor has advanced past a bucket boundary since the
     * last sweep and all three of these hold together: the anchor map holds more partitions
     * than this threshold, at least this many of them are reclaimable, and the reclaimable
     * ones are at least half the map. A higher value leaves the anchor map larger between
     * sweeps; a lower one sweeps more often. Neither matters for a view whose anchor is not
     * provably monotone, or is NULL - such a view never compacts, at any value of this key.
     */
    int getLiveViewPartitionCompactThreshold();

    /**
     * @return the byte limit applied to one live view's refresh, measured as the PEAK of a
     * refresh cycle. {@code 0} means unlimited; only the global RSS limit applies.
     * <p>
     * The tracker is per-view and its lifetime matches the view's cached state, not one
     * refresh attempt, because the persistent part of that state - the anchor map plus each
     * anchored window function's partition map and ring buffers - outlives the cycle that
     * built it. Unlike a query or a materialized-view refresh, this state is what the limit
     * exists to bound: it is the only backstop for a view whose ANCHOR cannot drive frontier
     * compaction (a LONG/INT anchor, or any anchor not provably monotone with the base scan
     * order), since such a view retains every partition key it has ever seen.
     * <p>
     * The tracker also charges the transient per-cycle buffers of the view's compiled SELECT:
     * LiveViewRefreshSqlExecutionContext hands it to AbstractPageFrameRecordCursor, which binds
     * it into the frame memory pool and so into RowGroupBuffers. Parquet decode buffers are
     * therefore charged alongside the persistent state, and freed at cursor close, so the
     * accounting stays symmetric across cycles but the limit bounds the cycle's peak rather
     * than its residue. Size the limit to include those transients: a peak that crosses it
     * invalidates the view (LiveViewRefreshJob.handleRefreshFailure invalidates immediately on
     * a limit breach rather than spending the retry budget), and invalidation is durable and
     * sticky - recovery is an operator DROP + CREATE.
     * <p>
     * Two floors dominate that sizing. A view reading parquet partitions decodes a whole row
     * group at a time. And a view whose SELECT binds a ring buffer - any RANGE frame, or a
     * {@code PARTITION BY} ROWS / lag() / lead() ring - charges a whole
     * {@link #getSqlWindowStorePageSize()} (1 MiB by default) on its first allocation (its
     * first partition, where the frame is partitioned), however small the frame: the ring is
     * created at page granularity. A NON-partitioned ROWS or
     * lag()/lead() ring is fixed by the query text and stays on global accounting, so it
     * imposes no floor here (see {@code WindowFunction.setMemoryTracker}).
     */
    long getLiveViewRefreshMemoryLimitBytes();

    int getLiveViewRefreshTurnMaxCommits();

    long getLiveViewRefreshTurnMaxDurationMicros();

    /**
     * Worker count of the dedicated live-view refresh pool
     * ({@code live.view.refresh.worker.count}). A positive value is what makes
     * {@code ServerMain} start {@code LiveViewRefreshJob}s at all, so it is also the
     * signal {@link #isLiveViewRefreshEnabled()} reads. Lives on the cairo
     * configuration rather than only on the pool configuration because the engine and
     * {@code WalPurgeJob} have to answer "will anything ever refresh a live view?"
     * without reaching into the server configuration.
     */
    int getLiveViewRefreshWorkerCount();

    boolean getLogLevelVerbose();

    boolean getLogSqlQueryProgressExe();

    DateFormat getLogTimestampFormat();

    String getLogTimestampTimezone();

    DateLocale getLogTimestampTimezoneLocale();

    TimeZoneRules getLogTimestampTimezoneRules();

    long getMatViewInsertAsSelectBatchSize();

    int getMatViewMaxRefreshIntervals();

    int getMatViewMaxRefreshRetries();

    long getMatViewMaxRefreshStepUs();

    int getMatViewRefreshBusyRetryLimit();

    long getMatViewRefreshBusyRetryTimeout();

    long getMatViewRefreshIntervalsUpdatePeriod();

    int getMatViewRefreshMaxClusters();

    /**
     * @return the per-event byte limit applied to one materialized view refresh
     * attempt. {@code 0} means unlimited; only the global RSS limit applies.
     */
    long getMatViewRefreshMemoryLimitBytes();

    long getMatViewRowsPerQueryEstimate();

    int getMaxCrashFiles();

    int getMaxFileNameLength();

    int getMaxSqlRecompileAttempts();

    int getMaxSwapFileCount();

    int getMaxSymbolNotEqualsCount();

    int getMaxUncommittedRows();

    int getMetadataPoolCapacity();

    Metrics getMetrics();

    @NotNull
    default MicrosecondClock getMicrosecondClock() {
        return MicrosecondClockImpl.INSTANCE;
    }

    @NotNull
    default MillisecondClock getMillisecondClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    long getMiscAppendPageSize();

    int getMkDirMode();

    default NanosecondClock getNanosecondClock() {
        return NanosecondClockImpl.INSTANCE;
    }

    int getO3CallbackQueueCapacity();

    int getO3ColumnMemorySize();

    int getO3CopyQueueCapacity();

    int getO3LagCalculationWindowsSize();

    default double getO3LagDecreaseFactor() {
        return 0.5;
    }

    default double getO3LagIncreaseFactor() {
        return 1.5;
    }

    int getO3LastPartitionMaxSplits();

    /**
     * Default commit lag in microseconds for new tables. This value
     * can be overridden with 'create table' statement.
     *
     * @return upper bound of "commit lag" in micros
     */
    long getO3MaxLag();

    int getO3MemMaxPages();

    int getO3MidPartitionMaxSplits();

    long getO3MinLag();

    int getO3OpenColumnQueueCapacity();

    int getO3PartitionQueueCapacity();

    int getO3PurgeDiscoveryQueueCapacity();

    // the '+' is used to prevent overlap with table names
    @NotNull
    default String getOGCrashFilePrefix() {
        return "hs_err_pid+";
    }

    int getPageFrameReduceColumnListCapacity();

    int getPageFrameReduceQueueCapacity();

    int getPageFrameReduceRowIdListCapacity();

    int getPageFrameReduceShardCount();

    int getParallelIndexThreshold();

    long getParquetExportBatchSize();

    double getParquetExportBloomFilterFpp();

    int getParquetExportCompressionCodec();

    int getParquetExportCompressionLevel();

    int getParquetExportCopyReportFrequencyLines();

    int getParquetExportDataPageSize();

    int getParquetExportRowGroupSize();

    CharSequence getParquetExportTableNamePrefix();

    int getParquetExportVersion();

    double getPartitionEncoderParquetBloomFilterFpp();

    int getPartitionEncoderParquetCompressionCodec();

    int getPartitionEncoderParquetCompressionLevel();

    int getPartitionEncoderParquetDataPageSize();

    double getPartitionEncoderParquetMinCompressionRatio();

    long getPartitionEncoderParquetO3RewriteUnusedMaxBytes();

    double getPartitionEncoderParquetO3RewriteUnusedRatio();

    int getPartitionEncoderParquetRowGroupSize();

    int getPartitionEncoderParquetVersion();

    long getPartitionO3SplitMinSize();

    int getPartitionPurgeListCapacity();

    int getPivotColumnPoolCapacity();

    int getPoolSegmentSize();

    /**
     * Threshold at which the adaptive posting-index row-id encoder forces
     * DELTA instead of running the size-only EF-vs-DELTA race. When a key has
     * {@code >= getPostingIndexAdaptiveDeltaAtOrAbove()} row IDs the writer
     * skips EF and emits DELTA directly. Default 2000.
     * <p>
     * The size-only adaptive pick is essentially a coin-flip at large counts
     * because both encodings produce similar byte sizes, but DELTA reads
     * markedly faster (per-block unpack, cache-line-friendly) than EF for
     * dense keys that span a long high-bits bitmap. For Zipfian-skewed
     * workloads with hot keys at 100k+ row IDs the threshold lifts point /
     * scan / range queries by 15-60% with no measurable regression on
     * uniform-distribution scenarios where keys stay below the threshold.
     * <p>
     * Set to {@link Integer#MAX_VALUE} to restore pure size comparison.
     */
    default int getPostingIndexAdaptiveDeltaAtOrAbove() {
        return 2000;
    }

    default double getPostingIndexAlignedBitWidthThreshold() {
        return 0.0;
    }

    default byte getPostingIndexRowIdEncoding() {
        return PostingIndexUtils.ENCODING_ADAPTIVE;
    }

    /**
     * Maximum bytes the posting index writer's per-key spill buffers may hold
     * before it triggers a mid-stream {@code flushAllPending} + free cycle to
     * bound peak RSS during long indexing runs (ALTER ADD INDEX TYPE POSTING,
     * IndexBuilder, the per-O3-seal rebuild loop). Returning {@code 0} or a
     * negative value disables the back-pressure entirely (legacy behaviour:
     * accumulate until {@code seal()}). Default is 256 MiB.
     */
    default long getPostingIndexerSpillBytesMax() {
        return 256L << 20;
    }

    int getPostingSealGenThreshold();

    /**
     * Hard cap on the per-writer in-memory outbox of superseded posting-seal
     * generations awaiting publish to the global purge queue. When the cap
     * is reached the writer evicts the oldest entry and emits a critical
     * log message -- the file the entry pointed at is then left on disk (a
     * bounded leak); no writer-open scan reclaims it.
     * <p>
     * Sized for steady-state operation where the purge queue is healthy. If
     * the queue is saturated for an extended period (e.g. background job
     * disabled) the outbox saturates and oldest entries are dropped, leaking
     * their files until the partition is rewritten -- keep the purge job
     * running to avoid this.
     */
    default int getPostingSealPurgeOutboxMax() {
        return 8192;
    }

    int getPreferencesStringPoolCapacity();

    int getQueryCacheEventQueueCapacity();

    long getQueryContinuationWakeIntervalMillis();

    /**
     * @return the per-query byte limit applied to user SQL execution. {@code 0}
     * means unlimited; only the global RSS limit applies.
     */
    long getQueryMemoryLimitBytes();

    int getQueryRegistryPoolSize();

    /**
     * Operator override for the zstd compression level used on QWP egress
     * {@code RESULT_BATCH} frames. Default {@code 0} means "honor the
     * client-negotiated level" -- the server uses whatever the client
     * advertised via {@code X-QWP-Accept-Encoding}, clamped to the wire
     * range {@code [COMPRESSION_ZSTD_MIN_LEVEL, COMPRESSION_ZSTD_MAX_LEVEL]}.
     * <p>
     * A value in {@code [1, 9]} forces every ZSTD-negotiated connection on
     * this server to use that level regardless of what the client asked
     * for; out-of-range values are clamped at the override site. A misbehaving
     * client cannot raise the server's CPU spend above the operator's chosen
     * ceiling. The {@code X-QWP-Content-Encoding} response header echoes
     * the effective (post-override) level so the client can observe what was
     * actually used.
     * <p>
     * Read from the configuration object on every handshake (not cached at
     * processor construction), so a live config reload takes effect on the
     * next new connection. Connections already established keep their
     * already-built ZSTD contexts -- runtime mutation of an in-flight cctx
     * level is not safe.
     */
    default int getQwpEgressForcedZstdLevel() {
        return 0;
    }

    /**
     * Source of the role / cluster / node identity emitted in the QWP egress
     * {@code SERVER_INFO} frame. Default is the standalone OSS provider; the
     * Enterprise configuration overrides this with a provider backed by the
     * live replication role so clients can route reads to primary vs replica.
     */
    @NotNull
    default QwpServerInfoProvider getQwpServerInfoProvider() {
        return DefaultQwpServerInfoProvider.INSTANCE;
    }

    @NotNull
    default Rnd getRandom() {
        Rnd rnd = RANDOM.get();
        if (rnd == null) {
            RANDOM.set(rnd = new Rnd(
                            getNanosecondClock().getTicks(),
                            getMicrosecondClock().getTicks()
                    )
            );
        }
        return rnd;
    }

    int getReaderPoolMaxSegments();

    int getRecentWriteTrackerCapacity();

    int getRenameTableModelPoolCapacity();

    int getRepeatMigrationsFromVersion();

    int getRmdirMaxDepth();

    int getRndFunctionMemoryMaxPages();

    int getRndFunctionMemoryPageSize();

    @NotNull
    default RostiAllocFacade getRostiAllocFacade() {
        return RostiAllocFacadeImpl.INSTANCE;
    }

    boolean getSampleByDefaultAlignmentCalendar();

    /**
     * Selects the sort backend the SAMPLE BY FILL fast path stacks above
     * the GROUP BY output. Returns one of {@link SampleBySortStrategy}'s int
     * constants. The default is {@link SampleBySortStrategy#LIGHT_ENCODED}.
     */
    int getSampleByFillSortStrategy();

    int getSampleByIndexSearchPageSize();

    long getSequencerCheckInterval();

    /**
     * Returns database instance id. The instance id is used by the snapshot recovery mechanism:
     * on database start the id is compared with the ID stored in the checkpoint, if any. If the ids
     * are different, snapshot recovery is being triggered.
     *
     * @return instance id.
     */
    @NotNull
    CharSequence getSnapshotInstanceId();

    long getSpinLockTimeout();

    int getSqlAsOfJoinLookAhead();

    int getSqlAsOfJoinMapEvacuationThreshold();

    int getSqlAsOfJoinShortCircuitCacheCapacity();

    int getSqlCharacterStoreCapacity();

    int getSqlCharacterStoreSequencePoolCapacity();

    int getSqlColumnPoolCapacity();

    int getSqlCompilerPoolCapacity();

    int getSqlCopyBufferSize();

    int getSqlCopyExportQueueCapacity();

    @Nullable CharSequence getSqlCopyExportRoot();

    @Nullable CharSequence getSqlCopyInputRoot();

    @Nullable CharSequence getSqlCopyInputWorkRoot();

    int getSqlCopyLogRetentionDays();

    long getSqlCopyMaxIndexChunkSize();

    int getSqlCopyQueueCapacity();

    int getSqlDistinctTimestampKeyCapacity();

    double getSqlDistinctTimestampLoadFactor();

    int getSqlExpressionPoolCapacity();

    double getSqlFastMapLoadFactor();

    int getSqlHashJoinLightValueMaxPages();

    int getSqlHashJoinLightValuePageSize();

    int getSqlHashJoinValueMaxPages();

    int getSqlHashJoinValuePageSize();

    long getSqlHorizonJoinBwdScanAbsoluteThreshold();

    long getSqlHorizonJoinBwdScanMinGap();

    long getSqlHorizonJoinBwdScanSwitchFactor();

    int getSqlHorizonJoinMaxOffsets();

    /**
     * When the number of intervals exceeds this threshold during bracket expansion,
     * intervals are merged to prevent unbounded memory growth.
     */
    int getSqlIntervalIncrementalMergeThreshold();

    /**
     * Maximum recursion depth for bracket expansion in interval parsing (one level per bracket group).
     */
    int getSqlIntervalMaxBracketDepth();

    /**
     * Maximum number of intervals allowed after bracket expansion and merging.
     * This limit prevents memory exhaustion from large non-adjacent interval sets.
     */
    int getSqlIntervalMaxIntervalsAfterMerge();

    int getSqlJitBindVarsMemoryMaxPages();

    int getSqlJitBindVarsMemoryPageSize();

    int getSqlJitIRMemoryMaxPages();

    int getSqlJitIRMemoryPageSize();

    int getSqlJitMaxInListSizeThreshold();

    int getSqlJitMode();

    int getSqlJoinContextPoolCapacity();

    int getSqlJoinMetadataMaxResizes();

    /**
     * These holds table metadata, which is usually quite small. 16K page should be adequate.
     *
     * @return memory page size
     */
    int getSqlJoinMetadataPageSize();

    long getSqlLatestByRowCount();

    int getSqlLexerPoolCapacity();

    int getSqlMapMaxPages();

    int getSqlMapMaxResizes();

    int getSqlMaxNegativeLimit();

    int getSqlModelPoolCapacity();

    int getSqlPageFrameMaxRows();

    int getSqlPageFrameMinRows();

    int getSqlParallelFilterDispatchLimit();

    double getSqlParallelFilterPreTouchThreshold();

    long getSqlParallelWorkStealingSpinTimeout();

    int getSqlParallelWorkStealingThreshold();

    long getSqlParquetCacheMemorySize();

    int getSqlPivotMaxProducedColumns();

    int getSqlSmallMapKeyCapacity();

    long getSqlSmallMapPageSize();

    int getSqlSmallPageFrameMaxRows();

    int getSqlSmallPageFrameMinRows();

    long getSqlSortEncodedParallelThreshold();

    int getSqlSortKeyMaterializationThreshold();

    long getSqlSortKeyMaxBytes();

    long getSqlSortKeyPageSize();

    long getSqlSortLightValueMaxBytes();

    long getSqlSortLightValuePageSize();

    long getSqlSortValueMaxBytes();

    int getSqlSortValuePageSize();

    int getSqlUnorderedMapMaxEntrySize();

    long getSqlWindowCacheMaxBytes();

    /**
     * Resolves which config key the CachedWindow record-store cap was sourced from. Returned as a
     * property path string (e.g. "cairo.sql.window.cache.max.bytes") so error messages can name the
     * actual binding constraint when growth fails. The new bytes key wins when explicitly set; the
     * legacy pages key wins when only it is explicit; the new bytes default wins otherwise.
     */
    String getSqlWindowCacheMaxPagesConfigKey();

    /**
     * Effective cap (in pages of {@link #getSqlWindowStorePageSize()}) on the CachedWindow record
     * store, after reconciling cairo.sql.window.cache.max.bytes and the legacy
     * cairo.sql.window.store.max.pages. Paired with {@link #getSqlWindowCacheMaxPagesConfigKey()}.
     */
    int getSqlWindowCacheMaxPagesResolved();

    int getSqlWindowInitialRangeBufferSize();

    int getSqlWindowMaxRecursion();

    long getSqlWindowRowIdMaxBytes();

    int getSqlWindowRowIdPageSize();

    int getSqlWindowStoreMaxPages();

    int getSqlWindowStorePageSize();

    long getSqlWindowTreeKeyMaxBytes();

    int getSqlWindowTreeKeyPageSize();

    int getStrFunctionMaxBufferLength();

    long getSymbolTableMaxAllocationPageSize();

    long getSymbolTableMinAllocationPageSize();

    long getSystemDataAppendPageSize();

    int getSystemO3ColumnMemorySize();

    @NotNull
    CharSequence getSystemTableNamePrefix();

    long getSystemWalDataAppendPageSize();

    long getSystemWalEventAppendPageSize();

    long getTableRegistryAutoReloadFrequency();

    int getTableRegistryCompactionThreshold();

    @NotNull
    TelemetryConfiguration getTelemetryConfiguration();

    CharSequence getTempRenamePendingTablePrefix();

    @NotNull
    TextConfiguration getTextConfiguration();

    /**
     * Number of {@link TimerShards} shards (one daemon thread each) used
     * to fire timer-based wakeups for parked SQL continuations and other
     * {@link DelayedFireable} entries. Higher values reduce
     * {@code DelayQueue} lock contention but cost one always-on thread per shard.
     */
    int getTimerShardCount();

    int getTxnScoreboardEntryCount();

    int getUnorderedPageFrameReduceQueueCapacity();

    int getVectorAggregateQueueCapacity();

    int getViewLexerPoolCapacity();

    int getViewWalWriterPoolMaxSegments();

    @NotNull
    VolumeDefinitions getVolumeDefinitions();

    int getWalApplyLookAheadTransactionCount();

    /**
     * @return the per-event byte limit applied to one WAL apply batch.
     * {@code 0} means unlimited; only the global RSS limit applies.
     */
    long getWalApplyMemoryLimitBytes();

    /**
     * Set of table directory names (e.g. {@code my_table~3}) whose WAL transactions must not be
     * applied by the ApplyWal2Table job ("hard suspended" tables). Directory names are matched, not
     * logical names, so the suspension binds to the physical table across a rename and a fresh table
     * reusing the name is unaffected. Configured via {@code cairo.wal.apply.suspended.tables}
     * (comma-separated) and reloadable. The runtime set extended through
     * {@code ALTER TABLE ... SUSPEND WAL} is held separately on the engine.
     *
     * @return the configured set, or null when none are configured (treated as empty).
     */
    @Nullable
    default ObjHashSet<String> getWalApplySuspendedTables() {
        return null;
    }

    long getWalApplyTableTimeQuota();

    /**
     * Whether WAL-apply-suspended tables (see {@link #getWalApplySuspendedTables()} and
     * {@code ALTER TABLE ... SUSPEND WAL}) also deny WAL writes, rejecting commits like a dropped
     * table but with a distinct exception. When false, suspension only excludes the table from WAL
     * apply while writes keep buffering for later. Configured via
     * {@code cairo.wal.apply.suspended.write.denied} and reloadable.
     */
    default boolean isWalApplySuspendedWriteDenied() {
        return false;
    }

    long getWalDataAppendPageSize();

    boolean getWalEnabledDefault();

    long getWalEventAppendPageSize();

    double getWalLagRowsMultiplier();

    long getWalMaxLagSize();

    int getWalMaxLagTxnCount();

    int getWalMaxSegmentFileDescriptorsCache();

    long getWalPurgeInterval();

    default int getWalPurgeWaitBeforeDelete() {
        return 0;
    }

    int getWalRecreateDistressedSequencerAttempts();

    /**
     * If after a commit a WAL segment has more than this number of rows, roll the next transaction onto a new segment.
     * <p>
     *
     * @see #getWalSegmentRolloverSize()
     */
    long getWalSegmentRolloverRowCount();

    /**
     * If after a commit a WAL segment is larger than this size, roll the next transaction onto a new segment.
     * <p>
     *
     * @see #getWalSegmentRolloverRowCount()
     */
    long getWalSegmentRolloverSize();

    int getWalTxnNotificationQueueCapacity();

    int getWalWriterMadviseMode();

    int getWalWriterPoolMaxSegments();

    int getWindowColumnPoolCapacity();

    int getWithClauseModelPoolCapacity();

    long getWorkStealTimeoutNanos();

    long getWriteBackOffTimeoutOnMemPressureMs();

    long getWriterAsyncCommandBusyWaitTimeout();

    long getWriterAsyncCommandMaxTimeout();

    int getWriterCommandQueueCapacity();

    long getWriterCommandQueueSlotSize();

    int getWriterFileOpenOpts();

    int getWriterTickRowsCountMod();

    boolean isCairoMetadataCacheSnapshotOrdered();

    /**
     * Rollback flag for the by-name column emit to UNION siblings in the SQL optimizer's top-down
     * column propagation. The optimizer matches UNION columns by position; the legacy by-name emit
     * could prune one branch inconsistently and crash code generation when branch aliases differed.
     * When {@code true}, restores the legacy by-name behavior. Defaults to {@code false}.
     *
     * @return whether to restore the legacy by-name UNION column propagation
     */
    default boolean isCairoSqlLegacyUnionColumnPropagation() {
        return false;
    }

    /**
     * A flag to enable/disable checkpoint recovery mechanism. Defaults to {@code true}.
     *
     * @return enable/disable flag for recovering from the checkpoint
     */
    boolean isCheckpointRecoveryEnabled();

    /**
     * This is a flag to enable/disable the generation of column alias based on the expression passed as a query.
     *
     * @return true if SqlParser should return the expression normalized instead of the default behavior.
     */
    boolean isColumnAliasExpressionEnabled();

    boolean isCopierChunkedEnabled();

    boolean isDevModeEnabled();

    boolean isGroupByPresizeEnabled();

    boolean isIOURingEnabled();

    boolean isLiveViewEnabled();

    /**
     * True when a {@code LiveViewRefreshJob} will actually run for this configuration:
     * the feature flag is on AND the dedicated refresh pool has at least one worker.
     * <p>
     * Every decision that only makes sense while something advances a live view's
     * watermarks must read THIS, not {@link #isLiveViewEnabled()} alone. Registering an
     * unattended instance pins the base WAL at its genesis watermark forever, because
     * {@code WalPurgeJob} clamps the purge floor to every registered view's
     * {@code lvConsumedSeqTxn} and nothing would ever advance it. The predicate is
     * config-derived and evaluated on the boot thread before the pools start, so the
     * registration guard and the purge gate cannot disagree.
     * <p>
     * Deliberately excludes {@code isReadOnlyInstance()}: a read-only replica runs no
     * refresh job either, but it still needs registered instances for the read path and
     * for a later promote, and it creates no {@code WalPurgeJob} at all, so there is no
     * floor to release there.
     */
    default boolean isLiveViewRefreshEnabled() {
        return isLiveViewEnabled() && getLiveViewRefreshWorkerCount() > 0;
    }

    boolean isMatViewCoveringIndexEnabled();

    boolean isMatViewEnabled();

    boolean isMatViewParallelSqlEnabled();

    /**
     * Returns true if the materialized view with the given name is in the configured refresh block
     * list ({@code cairo.mat.view.refresh.block.list}). Blocked views are skipped by every refresh
     * path; they may still be invalidated by a base-table/parent cascade or an explicit INVALIDATE.
     * Invalidation is safe for a blocked view: it runs no view SQL (so it can't trigger the crash
     * the block list guards against) and it releases the base table's WAL retention. This is an
     * operator escape hatch for a view whose refresh keeps crashing the database: blocking it lets
     * the database start and stay up. Because a blocked view that is never invalidated never advances
     * its last refreshed base txn, it can pin the base table's WAL retention until it is dropped or
     * removed from the block list. This applies equally to all
     * refresh types: the block skip in the refresh job short-circuits before the refresh-intervals
     * caching bump, so blocked timer and manual views stop caching intervals just like immediate
     * views, and the base WAL is pinned just as hard.
     */
    default boolean isMatViewRefreshBlocked(CharSequence viewName) {
        return false;
    }

    boolean isMatViewRefreshMissingWalFilesFatal();

    boolean isMultiKeyDedupEnabled();

    boolean isO3QuickSortEnabled();

    boolean isParallelIndexingEnabled();

    boolean isParquetExportRawArrayEncoding();

    boolean isParquetExportStatisticsEnabled();

    boolean isPartitionEncoderParquetRawArrayEncoding();

    boolean isPartitionEncoderParquetStatisticsEnabled();

    boolean isPartitionO3OverwriteControlEnabled();

    boolean isPostingIndexAutoIncludeTimestamp();

    boolean isQueryTracingEnabled();

    boolean isReadOnlyInstance();

    // Test-only seam, with no backing production property: always true in a running server, so
    // the optimiser always rewrites SELECT DISTINCT to GROUP BY. Tests override it to false in a
    // CairoConfiguration subclass to keep DISTINCT as a Distinct factory and reach
    // DistinctTimeSeriesRecordCursorFactory.
    boolean isSqlDistinctGroupByRewriteEnabled();

    boolean isSqlJitDebugEnabled();

    boolean isSqlOrderBySortEnabled();

    boolean isSqlParallelFilterEnabled();

    boolean isSqlParallelGroupByEnabled();

    boolean isSqlParallelHorizonJoinEnabled();

    boolean isSqlParallelReadParquetEnabled();

    boolean isSqlParallelTopKEnabled();

    boolean isSqlParallelWindowJoinEnabled();

    boolean isSqlParquetRowGroupPruningEnabled();

    boolean isSqlWindowCachedLightEnabled();

    /**
     * When true (the default), several window functions over one identical window may share a
     * single partition Map: the group keeps one key domain and makes one lookup per row where
     * each function would otherwise keep and probe a Map of its own.
     * <p>
     * The switch changes no answer - a group co-locates state that stays each member's own - so
     * it is an operational escape hatch for a shape whose Map implementation or key distribution
     * regresses in the field, and the control the differential tests compare against. It gates
     * the runtime binding only; the group is compiled either way, and nothing user-visible,
     * {@code EXPLAIN} included, differs between the two settings.
     */
    boolean isSqlWindowMapFusionEnabled();

    boolean isTableTypeConversionEnabled();

    /**
     * When true (the default), TTL enforcement uses the minimum of the max timestamp in the table
     * and the current wall clock time. This prevents accidental data loss when future timestamps
     * are inserted into a table with TTL enabled.
     * <p>
     * When false, TTL enforcement uses only the max timestamp in the table, which can cause
     * unexpected partition eviction if future timestamps are inserted.
     *
     * @return true if wall clock should be used for TTL enforcement (default), false otherwise
     */
    default boolean isTtlWallClockEnabled() {
        return true;
    }

    /**
     * A compatibility switch that controls validation of sample-by fill type.
     * <p>
     * This temporary switch maintains backward compatibility following changes introduced in
     * <a href="https://github.com/questdb/questdb/pull/5324">this PR</a>.
     * The pull request implemented stricter validation of sample validity, where:
     * <p>
     * 1. LINEAR interpolation is disabled by default
     * 2. Group-by functions must explicitly declare support for interpolation
     * <p>
     * Currently, LINEAR interpolation is enabled only for functions with verified test coverage.
     * However, there may be other functions that support interpolation but lack proper testing.
     * The introduction of strict validation could break these untested functions.
     * <p>
     * This switch allows users to disable the validation check and maintain the previous behavior.
     * Note: This configuration option is temporary and will be removed in a future release, at
     * which point sample-by-fill type validation will become mandatory.
     *
     * @return true if sample-by-fill type validation is enabled (default), false otherwise
     */
    default boolean isValidateSampleByFillType() {
        return true;
    }

    /**
     * Experimental: route the {@code ~} regex operator over VARCHAR through the native
     * (Rust {@code regex} crate) backend bound via the JDK FFM API, instead of
     * {@code java.util.regex}. Controlled by {@code cairo.sql.varchar.regex.native.enabled}
     * (default off). When on but a pattern is unsupported by the native engine
     * (e.g. backreferences/lookaround) the operator falls back to {@code java.util.regex}.
     *
     * @return true if the native VARCHAR regex backend should be used when available
     */
    default boolean isVarcharRegexNativeEnabled() {
        return false;
    }

    boolean isWalApplyEnabled();

    boolean isWalApplyParallelSqlEnabled();

    boolean isWalSupported();

    boolean isWriterMixedIOEnabled();

    /**
     * This is a flag to enable/disable making table directory names different to table names for non-WAL tables.
     * When it is enabled directory name of table TRADE becomes TRADE~, so that ~ sign is added at the end.
     * The flag is enabled in tests and disabled in released code for backward compatibility. Tests verify that
     * we do not rely on the fact that table directory name is the same as table name.
     *
     * @return true if mangling of directory names for non-WAL tables is enabled, false otherwise.
     */
    boolean mangleTableDirNames();

    int maxArrayElementCount();

    default ParquetPartitionDecoder newParquetPartitionDecoder() {
        return new ParquetPartitionDecoder();
    }

    boolean useWithinLatestByOptimisation();
}
