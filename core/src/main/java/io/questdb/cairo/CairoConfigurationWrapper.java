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

import io.questdb.BuildInformation;
import io.questdb.FactoryProvider;
import io.questdb.TelemetryConfiguration;
import io.questdb.VolumeDefinitions;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongSupplier;

public class CairoConfigurationWrapper implements CairoConfiguration {
    private final CairoConfiguration delegate;

    public CairoConfigurationWrapper(@NotNull CairoConfiguration delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean attachPartitionCopy() {
        return delegate.attachPartitionCopy();
    }

    @Override
    public boolean enableTestFactories() {
        return delegate.enableTestFactories();
    }

    @Override
    public boolean getAllowTableRegistrySharedWrite() {
        return delegate.getAllowTableRegistrySharedWrite();
    }

    @Override
    public int getAnalyticColumnPoolCapacity() {
        return delegate.getAnalyticColumnPoolCapacity();
    }

    @Override
    public String getAttachPartitionSuffix() {
        return delegate.getAttachPartitionSuffix();
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return delegate.getBackupDirTimestampFormat();
    }

    @Override
    public int getBackupMkDirMode() {
        return delegate.getBackupMkDirMode();
    }

    @Override
    public CharSequence getBackupRoot() {
        return delegate.getBackupRoot();
    }

    @Override
    public CharSequence getBackupTempDirName() {
        return delegate.getBackupTempDirName();
    }

    @Override
    public int getBinaryEncodingMaxLength() {
        return delegate.getBinaryEncodingMaxLength();
    }

    @Override
    public int getBindVariablePoolSize() {
        return delegate.getBindVariablePoolSize();
    }

    @Override
    public BuildInformation getBuildInformation() {
        return delegate.getBuildInformation();
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return delegate.getCircuitBreakerConfiguration();
    }

    @Override
    public int getColumnCastModelPoolCapacity() {
        return delegate.getColumnCastModelPoolCapacity();
    }

    @Override
    public int getColumnIndexerQueueCapacity() {
        return delegate.getColumnIndexerQueueCapacity();
    }

    @Override
    public int getColumnPurgeQueueCapacity() {
        return delegate.getColumnPurgeQueueCapacity();
    }

    @Override
    public long getColumnPurgeRetryDelay() {
        return delegate.getColumnPurgeRetryDelay();
    }

    @Override
    public long getColumnPurgeRetryDelayLimit() {
        return delegate.getColumnPurgeRetryDelayLimit();
    }

    @Override
    public double getColumnPurgeRetryDelayMultiplier() {
        return delegate.getColumnPurgeRetryDelayMultiplier();
    }

    @Override
    public int getColumnPurgeTaskPoolCapacity() {
        return delegate.getColumnPurgeTaskPoolCapacity();
    }

    @Override
    public int getCommitMode() {
        return delegate.getCommitMode();
    }

    @Override
    public CharSequence getConfRoot() {
        return delegate.getConfRoot();
    }

    @Override
    public LongSupplier getCopyIDSupplier() {
        return delegate.getCopyIDSupplier();
    }

    @Override
    public int getCopyPoolCapacity() {
        return delegate.getCopyPoolCapacity();
    }

    @Override
    public int getCreateAsSelectRetryCount() {
        return delegate.getCreateAsSelectRetryCount();
    }

    @Override
    public int getCreateTableModelPoolCapacity() {
        return delegate.getCreateTableModelPoolCapacity();
    }

    @Override
    public long getDataAppendPageSize() {
        return delegate.getDataAppendPageSize();
    }

    @Override
    public long getDataIndexKeyAppendPageSize() {
        return delegate.getDataIndexKeyAppendPageSize();
    }

    @Override
    public long getDataIndexValueAppendPageSize() {
        return delegate.getDataIndexValueAppendPageSize();
    }

    @Override
    public long getDatabaseIdHi() {
        return delegate.getDatabaseIdHi();
    }

    @Override
    public long getDatabaseIdLo() {
        return delegate.getDatabaseIdLo();
    }

    @Override
    public CharSequence getDbDirectory() {
        return delegate.getDbDirectory();
    }

    @Override
    public DateLocale getDefaultDateLocale() {
        return delegate.getDefaultDateLocale();
    }

    @Override
    public CharSequence getDefaultMapType() {
        return delegate.getDefaultMapType();
    }

    @Override
    public boolean getDefaultSymbolCacheFlag() {
        return delegate.getDefaultSymbolCacheFlag();
    }

    @Override
    public int getDefaultSymbolCapacity() {
        return delegate.getDefaultSymbolCapacity();
    }

    @Override
    public int getDoubleToStrCastScale() {
        return delegate.getDoubleToStrCastScale();
    }

    @Override
    public int getExplainPoolCapacity() {
        return delegate.getExplainPoolCapacity();
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return delegate.getFactoryProvider();
    }

    @Override
    public int getFileOperationRetryCount() {
        return delegate.getFileOperationRetryCount();
    }

    @Override
    public FilesFacade getFilesFacade() {
        return delegate.getFilesFacade();
    }

    @Override
    public int getFloatToStrCastScale() {
        return delegate.getFloatToStrCastScale();
    }

    @Override
    public int getGroupByMapCapacity() {
        return delegate.getGroupByMapCapacity();
    }

    @Override
    public int getGroupByPoolCapacity() {
        return delegate.getGroupByPoolCapacity();
    }

    @Override
    public long getIdleCheckInterval() {
        return delegate.getIdleCheckInterval();
    }

    @Override
    public int getInactiveReaderMaxOpenPartitions() {
        return delegate.getInactiveReaderMaxOpenPartitions();
    }

    @Override
    public long getInactiveReaderTTL() {
        return delegate.getInactiveReaderTTL();
    }

    @Override
    public long getInactiveWalWriterTTL() {
        return delegate.getInactiveWalWriterTTL();
    }

    @Override
    public long getInactiveWriterTTL() {
        return delegate.getInactiveWriterTTL();
    }

    @Override
    public int getIndexValueBlockSize() {
        return delegate.getIndexValueBlockSize();
    }

    @Override
    public int getInsertPoolCapacity() {
        return delegate.getInsertPoolCapacity();
    }

    @Override
    public int getLatestByQueueCapacity() {
        return delegate.getLatestByQueueCapacity();
    }

    @Override
    public int getMaxCrashFiles() {
        return delegate.getMaxCrashFiles();
    }

    @Override
    public int getMaxFileNameLength() {
        return delegate.getMaxFileNameLength();
    }

    @Override
    public int getMaxSwapFileCount() {
        return delegate.getMaxSwapFileCount();
    }

    @Override
    public int getMaxSymbolNotEqualsCount() {
        return delegate.getMaxSymbolNotEqualsCount();
    }

    @Override
    public int getMaxUncommittedRows() {
        return delegate.getMaxUncommittedRows();
    }

    @Override
    public int getMetadataPoolCapacity() {
        return delegate.getMetadataPoolCapacity();
    }

    @Override
    public long getMiscAppendPageSize() {
        return delegate.getMiscAppendPageSize();
    }

    @Override
    public int getMkDirMode() {
        return delegate.getMkDirMode();
    }

    @Override
    public int getO3CallbackQueueCapacity() {
        return delegate.getO3CallbackQueueCapacity();
    }

    @Override
    public int getO3ColumnMemorySize() {
        return delegate.getO3ColumnMemorySize();
    }

    @Override
    public int getO3CopyQueueCapacity() {
        return delegate.getO3CopyQueueCapacity();
    }

    @Override
    public int getO3LagCalculationWindowsSize() {
        return delegate.getO3LagCalculationWindowsSize();
    }

    @Override
    public int getO3LastPartitionMaxSplits() {
        return delegate.getO3LastPartitionMaxSplits();
    }

    @Override
    public long getO3MaxLag() {
        return delegate.getO3MaxLag();
    }

    @Override
    public int getO3MemMaxPages() {
        return delegate.getO3MemMaxPages();
    }

    @Override
    public long getO3MinLag() {
        return delegate.getO3MinLag();
    }

    @Override
    public int getO3OpenColumnQueueCapacity() {
        return delegate.getO3OpenColumnQueueCapacity();
    }

    @Override
    public int getO3PartitionQueueCapacity() {
        return delegate.getO3PartitionQueueCapacity();
    }

    @Override
    public int getO3PurgeDiscoveryQueueCapacity() {
        return delegate.getO3PurgeDiscoveryQueueCapacity();
    }

    @Override
    public int getPageFrameReduceColumnListCapacity() {
        return delegate.getPageFrameReduceColumnListCapacity();
    }

    @Override
    public int getPageFrameReduceQueueCapacity() {
        return delegate.getPageFrameReduceQueueCapacity();
    }

    @Override
    public int getPageFrameReduceRowIdListCapacity() {
        return delegate.getPageFrameReduceRowIdListCapacity();
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return delegate.getPageFrameReduceShardCount();
    }

    @Override
    public int getPageFrameReduceTaskPoolCapacity() {
        return delegate.getPageFrameReduceTaskPoolCapacity();
    }

    @Override
    public int getParallelIndexThreshold() {
        return delegate.getParallelIndexThreshold();
    }

    @Override
    public long getPartitionO3SplitMinSize() {
        return delegate.getPartitionO3SplitMinSize();
    }

    @Override
    public int getPartitionPurgeListCapacity() {
        return delegate.getPartitionPurgeListCapacity();
    }

    @Override
    public int getQueryCacheEventQueueCapacity() {
        return delegate.getQueryCacheEventQueueCapacity();
    }

    @Override
    public int getReaderPoolMaxSegments() {
        return delegate.getReaderPoolMaxSegments();
    }

    @Override
    public int getRenameTableModelPoolCapacity() {
        return delegate.getRenameTableModelPoolCapacity();
    }

    @Override
    public int getRepeatMigrationsFromVersion() {
        return delegate.getRepeatMigrationsFromVersion();
    }

    @Override
    public int getRndFunctionMemoryMaxPages() {
        return delegate.getRndFunctionMemoryMaxPages();
    }

    @Override
    public int getRndFunctionMemoryPageSize() {
        return delegate.getRndFunctionMemoryPageSize();
    }

    @Override
    public String getRoot() {
        return delegate.getRoot();
    }

    @Override
    public int getSampleByIndexSearchPageSize() {
        return delegate.getSampleByIndexSearchPageSize();
    }

    @Override
    public boolean getSimulateCrashEnabled() {
        return delegate.getSimulateCrashEnabled();
    }

    @Override
    public CharSequence getSnapshotInstanceId() {
        return delegate.getSnapshotInstanceId();
    }

    @Override
    public CharSequence getSnapshotRoot() {
        return delegate.getSnapshotRoot();
    }

    @Override
    public long getSpinLockTimeout() {
        return delegate.getSpinLockTimeout();
    }

    @Override
    public int getSqlAnalyticRowIdMaxPages() {
        return delegate.getSqlAnalyticRowIdMaxPages();
    }

    @Override
    public int getSqlAnalyticRowIdPageSize() {
        return delegate.getSqlAnalyticRowIdPageSize();
    }

    @Override
    public int getSqlAnalyticStoreMaxPages() {
        return delegate.getSqlAnalyticStoreMaxPages();
    }

    @Override
    public int getSqlAnalyticStorePageSize() {
        return delegate.getSqlAnalyticStorePageSize();
    }

    @Override
    public int getSqlAnalyticTreeKeyMaxPages() {
        return delegate.getSqlAnalyticTreeKeyMaxPages();
    }

    @Override
    public int getSqlAnalyticTreeKeyPageSize() {
        return delegate.getSqlAnalyticTreeKeyPageSize();
    }

    @Override
    public int getSqlCharacterStoreCapacity() {
        return delegate.getSqlCharacterStoreCapacity();
    }

    @Override
    public int getSqlCharacterStoreSequencePoolCapacity() {
        return delegate.getSqlCharacterStoreSequencePoolCapacity();
    }

    @Override
    public int getSqlColumnPoolCapacity() {
        return delegate.getSqlColumnPoolCapacity();
    }

    @Override
    public double getSqlCompactMapLoadFactor() {
        return delegate.getSqlCompactMapLoadFactor();
    }

    @Override
    public int getSqlCopyBufferSize() {
        return delegate.getSqlCopyBufferSize();
    }

    @Override
    public CharSequence getSqlCopyInputRoot() {
        return delegate.getSqlCopyInputRoot();
    }

    @Override
    public CharSequence getSqlCopyInputWorkRoot() {
        return delegate.getSqlCopyInputWorkRoot();
    }

    @Override
    public int getSqlCopyLogRetentionDays() {
        return delegate.getSqlCopyLogRetentionDays();
    }

    @Override
    public long getSqlCopyMaxIndexChunkSize() {
        return delegate.getSqlCopyMaxIndexChunkSize();
    }

    @Override
    public int getSqlCopyQueueCapacity() {
        return delegate.getSqlCopyQueueCapacity();
    }

    @Override
    public int getSqlDistinctTimestampKeyCapacity() {
        return delegate.getSqlDistinctTimestampKeyCapacity();
    }

    @Override
    public double getSqlDistinctTimestampLoadFactor() {
        return delegate.getSqlDistinctTimestampLoadFactor();
    }

    @Override
    public int getSqlExpressionPoolCapacity() {
        return delegate.getSqlExpressionPoolCapacity();
    }

    @Override
    public double getSqlFastMapLoadFactor() {
        return delegate.getSqlFastMapLoadFactor();
    }

    @Override
    public int getSqlHashJoinLightValueMaxPages() {
        return delegate.getSqlHashJoinLightValueMaxPages();
    }

    @Override
    public int getSqlHashJoinLightValuePageSize() {
        return delegate.getSqlHashJoinLightValuePageSize();
    }

    @Override
    public int getSqlHashJoinValueMaxPages() {
        return delegate.getSqlHashJoinValueMaxPages();
    }

    @Override
    public int getSqlHashJoinValuePageSize() {
        return delegate.getSqlHashJoinValuePageSize();
    }

    @Override
    public int getSqlJitBindVarsMemoryMaxPages() {
        return delegate.getSqlJitBindVarsMemoryMaxPages();
    }

    @Override
    public int getSqlJitBindVarsMemoryPageSize() {
        return delegate.getSqlJitBindVarsMemoryPageSize();
    }

    @Override
    public int getSqlJitIRMemoryMaxPages() {
        return delegate.getSqlJitIRMemoryMaxPages();
    }

    @Override
    public int getSqlJitIRMemoryPageSize() {
        return delegate.getSqlJitIRMemoryPageSize();
    }

    @Override
    public int getSqlJitMode() {
        return delegate.getSqlJitMode();
    }

    @Override
    public int getSqlJitPageAddressCacheThreshold() {
        return delegate.getSqlJitPageAddressCacheThreshold();
    }

    @Override
    public int getSqlJitRowsThreshold() {
        return delegate.getSqlJitRowsThreshold();
    }

    @Override
    public int getSqlJoinContextPoolCapacity() {
        return delegate.getSqlJoinContextPoolCapacity();
    }

    @Override
    public int getSqlJoinMetadataMaxResizes() {
        return delegate.getSqlJoinMetadataMaxResizes();
    }

    @Override
    public int getSqlJoinMetadataPageSize() {
        return delegate.getSqlJoinMetadataPageSize();
    }

    @Override
    public long getSqlLatestByRowCount() {
        return delegate.getSqlLatestByRowCount();
    }

    @Override
    public int getSqlLexerPoolCapacity() {
        return delegate.getSqlLexerPoolCapacity();
    }

    @Override
    public int getSqlMapKeyCapacity() {
        return delegate.getSqlMapKeyCapacity();
    }

    @Override
    public int getSqlMapMaxPages() {
        return delegate.getSqlMapMaxPages();
    }

    @Override
    public int getSqlMapMaxResizes() {
        return delegate.getSqlMapMaxResizes();
    }

    @Override
    public int getSqlMapPageSize() {
        return delegate.getSqlMapPageSize();
    }

    @Override
    public int getSqlMaxNegativeLimit() {
        return delegate.getSqlMaxNegativeLimit();
    }

    @Override
    public int getSqlModelPoolCapacity() {
        return delegate.getSqlModelPoolCapacity();
    }

    @Override
    public int getSqlPageFrameMaxRows() {
        return delegate.getSqlPageFrameMaxRows();
    }

    @Override
    public int getSqlPageFrameMinRows() {
        return delegate.getSqlPageFrameMinRows();
    }

    @Override
    public int getSqlSmallMapKeyCapacity() {
        return delegate.getSqlSmallMapKeyCapacity();
    }

    @Override
    public int getSqlSmallMapPageSize() {
        return delegate.getSqlSmallMapPageSize();
    }

    @Override
    public int getSqlSortKeyMaxPages() {
        return delegate.getSqlSortKeyMaxPages();
    }

    @Override
    public long getSqlSortKeyPageSize() {
        return delegate.getSqlSortKeyPageSize();
    }

    @Override
    public int getSqlSortLightValueMaxPages() {
        return delegate.getSqlSortLightValueMaxPages();
    }

    @Override
    public long getSqlSortLightValuePageSize() {
        return delegate.getSqlSortLightValuePageSize();
    }

    @Override
    public int getSqlSortValueMaxPages() {
        return delegate.getSqlSortValueMaxPages();
    }

    @Override
    public int getSqlSortValuePageSize() {
        return delegate.getSqlSortValuePageSize();
    }

    @Override
    public int getStrFunctionMaxBufferLength() {
        return delegate.getStrFunctionMaxBufferLength();
    }

    @Override
    public CharSequence getSystemTableNamePrefix() {
        return delegate.getSystemTableNamePrefix();
    }

    @Override
    public long getTableRegistryAutoReloadFrequency() {
        return delegate.getTableRegistryAutoReloadFrequency();
    }

    @Override
    public int getTableRegistryCompactionThreshold() {
        return delegate.getTableRegistryCompactionThreshold();
    }

    public TelemetryConfiguration getTelemetryConfiguration() {
        return delegate.getTelemetryConfiguration();
    }

    @Override
    public TextConfiguration getTextConfiguration() {
        return delegate.getTextConfiguration();
    }

    @Override
    public int getTxnScoreboardEntryCount() {
        return delegate.getTxnScoreboardEntryCount();
    }

    @Override
    public int getVectorAggregateQueueCapacity() {
        return delegate.getVectorAggregateQueueCapacity();
    }

    @Override
    public VolumeDefinitions getVolumeDefinitions() {
        return delegate.getVolumeDefinitions();
    }

    @Override
    public int getWalApplyLookAheadTransactionCount() {
        return delegate.getWalApplyLookAheadTransactionCount();
    }

    @Override
    public long getWalApplyTableTimeQuota() {
        return delegate.getWalApplyTableTimeQuota();
    }

    @Override
    public long getWalDataAppendPageSize() {
        return delegate.getWalDataAppendPageSize();
    }

    @Override
    public boolean getWalEnabledDefault() {
        return delegate.getWalEnabledDefault();
    }

    @Override
    public int getWalMaxLagTxnCount() {
        return delegate.getWalMaxLagTxnCount();
    }

    @Override
    public long getWalPurgeInterval() {
        return delegate.getWalPurgeInterval();
    }

    @Override
    public int getWalRecreateDistressedSequencerAttempts() {
        return delegate.getWalRecreateDistressedSequencerAttempts();
    }

    @Override
    public long getWalSegmentRolloverRowCount() {
        return delegate.getWalSegmentRolloverRowCount();
    }

    @Override
    public double getWalSquashUncommittedRowsMultiplier() {
        return delegate.getWalSquashUncommittedRowsMultiplier();
    }

    @Override
    public int getWalTxnNotificationQueueCapacity() {
        return delegate.getWalTxnNotificationQueueCapacity();
    }

    @Override
    public int getWithClauseModelPoolCapacity() {
        return delegate.getWithClauseModelPoolCapacity();
    }

    @Override
    public long getWorkStealTimeoutNanos() {
        return delegate.getWorkStealTimeoutNanos();
    }

    @Override
    public long getWriterAsyncCommandBusyWaitTimeout() {
        return delegate.getWriterAsyncCommandBusyWaitTimeout();
    }

    @Override
    public long getWriterAsyncCommandMaxTimeout() {
        return delegate.getWriterAsyncCommandMaxTimeout();
    }

    @Override
    public int getWriterCommandQueueCapacity() {
        return delegate.getWriterCommandQueueCapacity();
    }

    @Override
    public long getWriterCommandQueueSlotSize() {
        return delegate.getWriterCommandQueueSlotSize();
    }

    @Override
    public long getWriterFileOpenOpts() {
        return delegate.getWriterFileOpenOpts();
    }

    @Override
    public int getWriterTickRowsCountMod() {
        return delegate.getWriterTickRowsCountMod();
    }

    @Override
    public boolean isIOURingEnabled() {
        return delegate.isIOURingEnabled();
    }

    @Override
    public boolean isMultiKeyDedupEnabled() {
        return delegate.isMultiKeyDedupEnabled();
    }

    @Override
    public boolean isO3QuickSortEnabled() {
        return delegate.isO3QuickSortEnabled();
    }

    @Override
    public boolean isParallelIndexingEnabled() {
        return delegate.isParallelIndexingEnabled();
    }

    @Override
    public boolean isReadOnlyInstance() {
        return delegate.isReadOnlyInstance();
    }

    @Override
    public boolean isSnapshotRecoveryEnabled() {
        return delegate.isSnapshotRecoveryEnabled();
    }

    @Override
    public boolean isSqlJitDebugEnabled() {
        return delegate.isSqlJitDebugEnabled();
    }

    @Override
    public boolean isSqlParallelFilterEnabled() {
        return delegate.isSqlParallelFilterEnabled();
    }

    @Override
    public boolean isSqlParallelFilterPreTouchEnabled() {
        return delegate.isSqlParallelFilterPreTouchEnabled();
    }

    @Override
    public boolean isTableTypeConversionEnabled() {
        return delegate.isTableTypeConversionEnabled();
    }

    @Override
    public boolean isWalApplyEnabled() {
        return delegate.isWalApplyEnabled();
    }

    public boolean isWalSupported() {
        return delegate.isWalSupported();
    }

    @Override
    public boolean isWriterMixedIOEnabled() {
        return delegate.isWriterMixedIOEnabled();
    }

    @Override
    public boolean mangleTableDirNames() {
        return delegate.mangleTableDirNames();
    }
}
