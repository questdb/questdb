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

package io.questdb.cairo;

import io.questdb.*;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongSupplier;

public class DefaultCairoConfiguration implements CairoConfiguration {
    private final BuildInformation buildInformation = new BuildInformationHolder();
    private final SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration();
    private final CharSequence confRoot;
    private final long databaseIdHi;
    private final long databaseIdLo;
    private final LongSupplier importIDSupplier = () -> getRandom().nextPositiveLong();
    private final String root;
    private final CharSequence snapshotRoot;
    private final DefaultTelemetryConfiguration telemetryConfiguration = new DefaultTelemetryConfiguration();
    private final TextConfiguration textConfiguration;
    private final VolumeDefinitions volumeDefinitions = new VolumeDefinitions();
    private final boolean writerMixedIOEnabled;

    public DefaultCairoConfiguration(CharSequence root) {
        this.root = Chars.toString(root);
        this.confRoot = PropServerConfiguration.rootSubdir(root, PropServerConfiguration.CONFIG_DIRECTORY);
        this.textConfiguration = new DefaultTextConfiguration(Chars.toString(confRoot));
        this.snapshotRoot = PropServerConfiguration.rootSubdir(root, PropServerConfiguration.SNAPSHOT_DIRECTORY);
        Rnd rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
        this.databaseIdLo = rnd.nextLong();
        this.databaseIdHi = rnd.nextLong();
        this.writerMixedIOEnabled = getFilesFacade().allowMixedIO(root);
    }

    @Override
    public boolean attachPartitionCopy() {
        return false;
    }

    @Override
    public boolean enableTestFactories() {
        return true;
    }

    @Override
    public boolean getAllowTableRegistrySharedWrite() {
        return false;
    }

    @Override
    public @NotNull String getAttachPartitionSuffix() {
        return TableUtils.ATTACHABLE_DIR_MARKER;
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return null;
    }

    @Override
    public int getBackupMkDirMode() {
        return 509;
    }

    @Override
    public CharSequence getBackupRoot() {
        return null;
    }

    @Override
    public @NotNull CharSequence getBackupTempDirName() {
        return "tmp";
    }

    @Override
    public int getBinaryEncodingMaxLength() {
        return 32768;
    }

    @Override
    public int getBindVariablePoolSize() {
        return 8;
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
        return 32;
    }

    @Override
    public int getColumnIndexerQueueCapacity() {
        return 1024;
    }

    @Override
    public int getColumnPurgeQueueCapacity() {
        return 64;
    }

    @Override
    public long getColumnPurgeRetryDelay() {
        return 10_000;
    }

    @Override
    public long getColumnPurgeRetryDelayLimit() {
        return 60_000_000;
    }

    @Override
    public double getColumnPurgeRetryDelayMultiplier() {
        return 2.0;
    }

    @Override
    public int getColumnPurgeTaskPoolCapacity() {
        return getColumnPurgeQueueCapacity();
    }

    @Override
    public int getCommitMode() {
        return CommitMode.NOSYNC;
    }

    @Override
    public @NotNull CharSequence getConfRoot() {
        return confRoot;
    }

    @Override
    public @NotNull LongSupplier getCopyIDSupplier() {
        return importIDSupplier;
    }

    @Override
    public int getCopyPoolCapacity() {
        return 16;
    }

    @Override
    public int getCountDistinctCapacity() {
        return 16;
    }

    @Override
    public double getCountDistinctLoadFactor() {
        return 0.7;
    }

    @Override
    public int getCreateAsSelectRetryCount() {
        return 5;
    }

    @Override
    public int getCreateTableModelPoolCapacity() {
        return 32;
    }

    @Override
    public long getDataAppendPageSize() {
        return 2 * 1024 * 1024;
    }

    @Override
    public long getDataIndexKeyAppendPageSize() {
        return Files.PAGE_SIZE;
    }

    @Override
    public long getDataIndexValueAppendPageSize() {
        return Files.ceilPageSize(1024 * 1024);
    }

    @Override
    public long getDatabaseIdHi() {
        return databaseIdHi;
    }

    @Override
    public long getDatabaseIdLo() {
        return databaseIdLo;
    }

    @Override
    public @NotNull CharSequence getDbDirectory() {
        return PropServerConfiguration.DB_DIRECTORY;
    }

    @Override
    public @NotNull DateLocale getDefaultDateLocale() {
        return DateFormatUtils.EN_LOCALE;
    }

    @Override
    public @NotNull CharSequence getDefaultMapType() {
        return "fast";
    }

    @Override
    public boolean getDefaultSymbolCacheFlag() {
        return true;
    }

    @Override
    public int getDefaultSymbolCapacity() {
        return 128;
    }

    @Override
    public int getDetachedMkDirMode() {
        return 509;
    }

    @Override
    public int getDoubleToStrCastScale() {
        return Numbers.MAX_SCALE;
    }

    @Override
    public int getExplainPoolCapacity() {
        return 32;
    }

    @Override
    public @NotNull FactoryProvider getFactoryProvider() {
        return DefaultFactoryProvider.INSTANCE;
    }

    @Override
    public int getFileOperationRetryCount() {
        return 30;
    }

    @Override
    public @NotNull FilesFacade getFilesFacade() {
        return FilesFacadeImpl.INSTANCE;
    }

    @Override
    public int getFloatToStrCastScale() {
        return 4;
    }

    @Override
    public int getGroupByMapCapacity() {
        return 1024;
    }

    @Override
    public int getGroupByMergeShardQueueCapacity() {
        return 32;
    }

    @Override
    public int getGroupByPoolCapacity() {
        return 1024;
    }

    @Override
    public int getGroupByShardCount() {
        return 32;
    }

    @Override
    public int getGroupByShardingThreshold() {
        return 1000;
    }

    @Override
    public long getIdleCheckInterval() {
        return 100;
    }

    @Override
    public int getInactiveReaderMaxOpenPartitions() {
        return 128;
    }

    @Override
    public long getInactiveReaderTTL() {
        return -10000;
    }

    @Override
    public long getInactiveWalWriterTTL() {
        return 60_000;
    }

    @Override
    public long getInactiveWriterTTL() {
        return -10000;
    }

    @Override
    public int getIndexValueBlockSize() {
        return 256;
    }

    @Override
    public int getInsertPoolCapacity() {
        return 8;
    }

    @Override
    public int getLatestByQueueCapacity() {
        return 32;
    }

    @Override
    public int getMaxCrashFiles() {
        return 1;
    }

    @Override
    public int getMaxFileNameLength() {
        return 127;
    }

    @Override
    public int getMaxSwapFileCount() {
        return 30;
    }

    @Override
    public int getMaxSymbolNotEqualsCount() {
        return 100;
    }

    @Override
    public int getMaxUncommittedRows() {
        return 1000;
    }

    @Override
    public int getMetadataPoolCapacity() {
        return getSqlModelPoolCapacity();
    }

    @Override
    public long getMiscAppendPageSize() {
        return getFilesFacade().getPageSize();
    }

    @Override
    public int getMkDirMode() {
        return 509;
    }

    @Override
    public int getO3CallbackQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3ColumnMemorySize() {
        return 8 * Numbers.SIZE_1MB;
    }

    @Override
    public int getO3CopyQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3LagCalculationWindowsSize() {
        return 4;
    }

    @Override
    public int getO3LastPartitionMaxSplits() {
        return 15;
    }

    @Override
    public long getO3MaxLag() {
        // 5 min
        return 300_000_000L;
    }

    @Override
    public int getO3MemMaxPages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long getO3MinLag() {
        return 1_000_000;
    }

    @Override
    public int getO3OpenColumnQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3PartitionQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3PurgeDiscoveryQueueCapacity() {
        return 1024;
    }

    @Override
    public int getPageFrameReduceColumnListCapacity() {
        return 16;
    }

    @Override
    public int getPageFrameReduceQueueCapacity() {
        return 32;
    }

    @Override
    public int getPageFrameReduceRowIdListCapacity() {
        return 32;
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return 4;
    }

    @Override
    public int getParallelIndexThreshold() {
        return 100000;
    }

    @Override
    public long getPartitionO3SplitMinSize() {
        return 50 * Numbers.SIZE_1MB;
    }

    @Override
    public int getPartitionPurgeListCapacity() {
        return 64;
    }

    @Override
    public int getReaderPoolMaxSegments() {
        return 5;
    }

    @Override
    public int getRenameTableModelPoolCapacity() {
        return 8;
    }

    @Override
    public int getRepeatMigrationsFromVersion() {
        return -1;
    }

    @Override
    public int getRndFunctionMemoryMaxPages() {
        return 128;
    }

    @Override
    public int getRndFunctionMemoryPageSize() {
        return 8192;
    }

    @Override
    public @NotNull String getRoot() {
        return root;
    }

    @Override
    public int getSampleByIndexSearchPageSize() {
        return 0;
    }

    @Override
    public boolean getSimulateCrashEnabled() {
        return false;
    }

    @Override
    public @NotNull CharSequence getSnapshotInstanceId() {
        return "";
    }

    @Override
    public @NotNull CharSequence getSnapshotRoot() {
        return snapshotRoot;
    }

    @Override
    public long getSpinLockTimeout() {
        return 5000;
    }

    @Override
    public int getSqlCharacterStoreCapacity() {
        // 1024 seems like a good fit, but tests need
        // smaller capacity so that resize is tested correctly
        return 64;
    }

    @Override
    public int getSqlCharacterStoreSequencePoolCapacity() {
        return 64;
    }

    @Override
    public int getSqlColumnPoolCapacity() {
        return 4096;
    }

    @Override
    public double getSqlCompactMapLoadFactor() {
        return 0.8;
    }

    @Override
    public int getSqlCompilerPoolCapacity() {
        return 10;
    }

    @Override
    public int getSqlCopyBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public CharSequence getSqlCopyInputRoot() {
        return null;
    }

    @Override
    public CharSequence getSqlCopyInputWorkRoot() {
        return null;
    }

    @Override
    public int getSqlCopyLogRetentionDays() {
        return 3;
    }

    @Override
    public long getSqlCopyMaxIndexChunkSize() {
        return 1024 * 1024L;
    }

    @Override
    public int getSqlCopyQueueCapacity() {
        return 32;
    }

    @Override
    public int getSqlDistinctTimestampKeyCapacity() {
        return 256;
    }

    @Override
    public double getSqlDistinctTimestampLoadFactor() {
        return 0.5;
    }

    @Override
    public int getSqlExpressionPoolCapacity() {
        return 8192;
    }

    @Override
    public double getSqlFastMapLoadFactor() {
        return 0.7;
    }

    @Override
    public int getSqlHashJoinLightValueMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlHashJoinLightValuePageSize() {
        return Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlHashJoinValueMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlHashJoinValuePageSize() {
        return Numbers.SIZE_1MB * 16;
    }

    @Override
    public int getSqlJitBindVarsMemoryMaxPages() {
        return 8;
    }

    @Override
    public int getSqlJitBindVarsMemoryPageSize() {
        return 4096;
    }

    @Override
    public int getSqlJitIRMemoryMaxPages() {
        return 8;
    }

    @Override
    public int getSqlJitIRMemoryPageSize() {
        return 8192;
    }

    @Override
    public int getSqlJitMode() {
        return SqlJitMode.JIT_MODE_ENABLED;
    }

    @Override
    public int getSqlJitPageAddressCacheThreshold() {
        return Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlJoinContextPoolCapacity() {
        return 64;
    }

    @Override
    public int getSqlJoinMetadataMaxResizes() {
        return 10;
    }

    @Override
    public int getSqlJoinMetadataPageSize() {
        return 16 * 1024;
    }

    @Override
    public long getSqlLatestByRowCount() {
        return 1000;
    }

    @Override
    public int getSqlLexerPoolCapacity() {
        return 2048;
    }

    @Override
    public int getSqlMapMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlMapMaxResizes() {
        return 64;
    }

    @Override
    public int getSqlMaxNegativeLimit() {
        return 10_000;
    }

    @Override
    public int getSqlModelPoolCapacity() {
        return 1024;
    }

    @Override
    public int getSqlPageFrameMaxRows() {
        return 1_000_000;
    }

    @Override
    public int getSqlPageFrameMinRows() {
        return 1_000;
    }

    @Override
    public int getSqlSmallMapKeyCapacity() {
        return 64;
    }

    @Override
    public int getSqlSmallMapPageSize() {
        return 4 * 1024;
    }

    @Override
    public int getSqlSortKeyMaxPages() {
        return 128;
    }

    @Override
    public long getSqlSortKeyPageSize() {
        return 4 * Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlSortLightValueMaxPages() {
        return 1024;
    }

    @Override
    public long getSqlSortLightValuePageSize() {
        return 8 * Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlSortValueMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlSortValuePageSize() {
        return Numbers.SIZE_1MB * 16;
    }

    @Override
    public int getSqlWindowInitialRangeBufferSize() {
        return 32;
    }

    @Override
    public int getSqlWindowMaxRecursion() {
        return 128;
    }

    @Override
    public int getSqlWindowRowIdMaxPages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSqlWindowRowIdPageSize() {
        return 1024;
    }

    @Override
    public int getSqlWindowStoreMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlWindowStorePageSize() {
        return 1024 * 1024;
    }

    @Override
    public int getSqlWindowTreeKeyMaxPages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSqlWindowTreeKeyPageSize() {
        return 4 * 1024;
    }

    @Override
    public int getStrFunctionMaxBufferLength() {
        return 1024 * 1024;
    }

    @Override
    public long getSystemDataAppendPageSize() {
        return 256 * 1024;
    }

    @Override
    public int getSystemO3ColumnMemorySize() {
        return 256 * 1024;
    }

    @Override
    public @NotNull CharSequence getSystemTableNamePrefix() {
        return "__sys";
    }

    @Override
    public long getSystemWalDataAppendPageSize() {
        return 256 * 1024;
    }

    @Override
    public long getTableRegistryAutoReloadFrequency() {
        return 500;
    }

    @Override
    public int getTableRegistryCompactionThreshold() {
        return 100;
    }

    @Override
    public @NotNull TelemetryConfiguration getTelemetryConfiguration() {
        return telemetryConfiguration;
    }

    @Override
    public CharSequence getTempRenamePendingTablePrefix() {
        return "temp_5822f658-31f6-11ee-be56-0242ac120002";
    }

    @Override
    public @NotNull TextConfiguration getTextConfiguration() {
        return textConfiguration;
    }

    @Override
    public int getTxnScoreboardEntryCount() {
        return 8192;
    }

    @Override
    public int getVectorAggregateQueueCapacity() {
        return 1024;
    }

    @Override
    public @NotNull VolumeDefinitions getVolumeDefinitions() {
        return volumeDefinitions;
    }

    @Override
    public int getWalApplyLookAheadTransactionCount() {
        return 20;
    }

    @Override
    public long getWalApplyTableTimeQuota() {
        return 1000L;
    }

    @Override
    public long getWalDataAppendPageSize() {
        return 1024 * 1024;
    }

    @Override
    public boolean getWalEnabledDefault() {
        return false;
    }

    @Override
    public long getWalMaxLagSize() {
        return 75 * Numbers.SIZE_1MB;
    }

    @Override
    public int getWalMaxLagTxnCount() {
        return 20;
    }

    @Override
    public int getWalMaxSegmentFileDescriptorsCache() {
        return 1000;
    }

    @Override
    public long getWalPurgeInterval() {
        return 30_000;
    }

    @Override
    public int getWalRecreateDistressedSequencerAttempts() {
        return 3;
    }

    @Override
    public long getWalSegmentRolloverRowCount() {
        return 200000;
    }

    @Override
    public long getWalSegmentRolloverSize() {
        return 0;  // watermark level disabled.
    }

    @Override
    public double getWalSquashUncommittedRowsMultiplier() {
        return 20;
    }

    @Override
    public int getWalTxnNotificationQueueCapacity() {
        return 4096;
    }

    @Override
    public int getWalWriterPoolMaxSegments() {
        return 5;
    }

    @Override
    public int getWindowColumnPoolCapacity() {
        return 64;
    }

    @Override
    public int getWithClauseModelPoolCapacity() {
        return 128;
    }

    @Override
    public long getWorkStealTimeoutNanos() {
        return 10000;
    }

    @Override
    public long getWriterAsyncCommandBusyWaitTimeout() {
        return 1000L;
    }

    @Override
    public long getWriterAsyncCommandMaxTimeout() {
        return 30_000L;
    }

    @Override
    public int getWriterCommandQueueCapacity() {
        return 4;
    }

    @Override
    public long getWriterCommandQueueSlotSize() {
        return 1024;
    }

    @Override
    public long getWriterFileOpenOpts() {
        // In some places we rely on the fact that data written via conventional IO
        // is immediately visible to mapped memory for the same area of file. While this is the
        // case on Linux it is absolutely not the case on Windows. We must not enable anything other
        // than MMAP on Windows.
        return Os.type != Os.WINDOWS ? O_ASYNC : O_NONE;
    }

    @Override
    public long getWriterMemoryLimit() {
        return 0;
    }

    @Override
    public int getWriterTickRowsCountMod() {
        return 1024 - 1;
    }

    @Override
    public boolean isIOURingEnabled() {
        return true;
    }

    @Override
    public boolean isMultiKeyDedupEnabled() {
        return false;
    }

    @Override
    public boolean isO3QuickSortEnabled() {
        return false;
    }

    @Override
    public boolean isParallelIndexingEnabled() {
        return true;
    }

    @Override
    public boolean isReadOnlyInstance() {
        return false;
    }

    @Override
    public boolean isSnapshotRecoveryEnabled() {
        return true;
    }

    @Override
    public boolean isSqlJitDebugEnabled() {
        return false;
    }

    @Override
    public boolean isSqlParallelFilterEnabled() {
        return true;
    }

    @Override
    public boolean isSqlParallelFilterPreTouchEnabled() {
        return true;
    }

    @Override
    public boolean isSqlParallelGroupByEnabled() {
        return true;
    }

    @Override
    public boolean isTableTypeConversionEnabled() {
        return true;
    }

    @Override
    public boolean isWalApplyEnabled() {
        return true;
    }

    @Override
    public boolean isWalSupported() {
        return true;
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
