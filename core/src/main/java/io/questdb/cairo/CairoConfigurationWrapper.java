/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.LongSupplier;

public class CairoConfigurationWrapper implements CairoConfiguration {
    private final CairoConfiguration delegate;

    protected CairoConfigurationWrapper() {
        delegate = null;
    }

    public CairoConfigurationWrapper(@NotNull CairoConfiguration delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean attachPartitionCopy() {
        return getDelegate().attachPartitionCopy();
    }

    @Override
    public boolean enableTestFactories() {
        return getDelegate().enableTestFactories();
    }

    @Override
    public @Nullable ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> getAllPairs() {
        return getDelegate().getAllPairs();
    }

    @Override
    public boolean getAllowTableRegistrySharedWrite() {
        return getDelegate().getAllowTableRegistrySharedWrite();
    }

    @Override
    public @NotNull String getAttachPartitionSuffix() {
        return getDelegate().getAttachPartitionSuffix();
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return getDelegate().getBackupDirTimestampFormat();
    }

    @Override
    public int getBackupMkDirMode() {
        return getDelegate().getBackupMkDirMode();
    }

    @Override
    public CharSequence getBackupRoot() {
        return getDelegate().getBackupRoot();
    }

    @Override
    public @NotNull CharSequence getBackupTempDirName() {
        return getDelegate().getBackupTempDirName();
    }

    @Override
    public int getBinaryEncodingMaxLength() {
        return getDelegate().getBinaryEncodingMaxLength();
    }

    @Override
    public int getBindVariablePoolSize() {
        return getDelegate().getBindVariablePoolSize();
    }

    @Override
    public @NotNull BuildInformation getBuildInformation() {
        return getDelegate().getBuildInformation();
    }

    @Override
    public boolean getCairoSqlLegacyOperatorPrecedence() {
        return getDelegate().getCairoSqlLegacyOperatorPrecedence();
    }

    @Override
    public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return getDelegate().getCircuitBreakerConfiguration();
    }

    @Override
    public int getColumnCastModelPoolCapacity() {
        return getDelegate().getColumnCastModelPoolCapacity();
    }

    @Override
    public int getColumnIndexerQueueCapacity() {
        return getDelegate().getColumnIndexerQueueCapacity();
    }

    @Override
    public int getColumnPurgeQueueCapacity() {
        return getDelegate().getColumnPurgeQueueCapacity();
    }

    @Override
    public long getColumnPurgeRetryDelay() {
        return getDelegate().getColumnPurgeRetryDelay();
    }

    @Override
    public long getColumnPurgeRetryDelayLimit() {
        return getDelegate().getColumnPurgeRetryDelayLimit();
    }

    @Override
    public double getColumnPurgeRetryDelayMultiplier() {
        return getDelegate().getColumnPurgeRetryDelayMultiplier();
    }

    @Override
    public int getColumnPurgeTaskPoolCapacity() {
        return getDelegate().getColumnPurgeTaskPoolCapacity();
    }

    @Override
    public int getCommitMode() {
        return getDelegate().getCommitMode();
    }

    @Override
    public @NotNull CharSequence getConfRoot() {
        return getDelegate().getConfRoot();
    }

    @Override
    public @NotNull LongSupplier getCopyIDSupplier() {
        return getDelegate().getCopyIDSupplier();
    }

    @Override
    public int getCopyPoolCapacity() {
        return getDelegate().getCopyPoolCapacity();
    }

    @Override
    public int getCountDistinctCapacity() {
        return getDelegate().getCountDistinctCapacity();
    }

    @Override
    public double getCountDistinctLoadFactor() {
        return getDelegate().getCountDistinctLoadFactor();
    }

    @Override
    public int getCreateAsSelectRetryCount() {
        return getDelegate().getCreateAsSelectRetryCount();
    }

    @Override
    public long getCreateTableModelBatchSize() {
        return getDelegate().getCreateTableModelBatchSize();
    }

    @Override
    public int getCreateTableModelPoolCapacity() {
        return getDelegate().getCreateTableModelPoolCapacity();
    }

    @Override
    public long getDataAppendPageSize() {
        return getDelegate().getDataAppendPageSize();
    }

    @Override
    public long getDataIndexKeyAppendPageSize() {
        return getDelegate().getDataIndexKeyAppendPageSize();
    }

    @Override
    public long getDataIndexValueAppendPageSize() {
        return getDelegate().getDataIndexValueAppendPageSize();
    }

    @Override
    public long getDatabaseIdHi() {
        return getDelegate().getDatabaseIdHi();
    }

    @Override
    public long getDatabaseIdLo() {
        return getDelegate().getDatabaseIdLo();
    }

    @Override
    public @NotNull CharSequence getDbDirectory() {
        return getDelegate().getDbDirectory();
    }

    @Override
    public @NotNull DateLocale getDefaultDateLocale() {
        return getDelegate().getDefaultDateLocale();
    }

    @Override
    public int getDefaultSeqPartTxnCount() {
        return getDelegate().getDefaultSeqPartTxnCount();
    }

    @Override
    public boolean getDefaultSymbolCacheFlag() {
        return getDelegate().getDefaultSymbolCacheFlag();
    }

    @Override
    public int getDefaultSymbolCapacity() {
        return getDelegate().getDefaultSymbolCapacity();
    }

    @Override
    public int getDetachedMkDirMode() {
        return getDelegate().getDetachedMkDirMode();
    }

    @Override
    public int getDoubleToStrCastScale() {
        return getDelegate().getDoubleToStrCastScale();
    }

    @Override
    public int getExplainPoolCapacity() {
        return getDelegate().getExplainPoolCapacity();
    }

    @Override
    public @NotNull FactoryProvider getFactoryProvider() {
        return getDelegate().getFactoryProvider();
    }

    @Override
    public int getFileOperationRetryCount() {
        return getDelegate().getFileOperationRetryCount();
    }

    @Override
    public @NotNull FilesFacade getFilesFacade() {
        return getDelegate().getFilesFacade();
    }

    @Override
    public int getFloatToStrCastScale() {
        return getDelegate().getFloatToStrCastScale();
    }

    @Override
    public long getGroupByAllocatorDefaultChunkSize() {
        return getDelegate().getGroupByAllocatorDefaultChunkSize();
    }

    @Override
    public long getGroupByAllocatorMaxChunkSize() {
        return getDelegate().getGroupByAllocatorMaxChunkSize();
    }

    @Override
    public int getGroupByMapCapacity() {
        return getDelegate().getGroupByMapCapacity();
    }

    @Override
    public int getGroupByMergeShardQueueCapacity() {
        return getDelegate().getGroupByMergeShardQueueCapacity();
    }

    @Override
    public int getGroupByPoolCapacity() {
        return getDelegate().getGroupByPoolCapacity();
    }

    @Override
    public long getGroupByPresizeMaxHeapSize() {
        return getDelegate().getGroupByPresizeMaxHeapSize();
    }

    @Override
    public long getGroupByPresizeMaxSize() {
        return getDelegate().getGroupByPresizeMaxSize();
    }

    @Override
    public int getGroupByShardingThreshold() {
        return getDelegate().getGroupByShardingThreshold();
    }

    @Override
    public long getIdleCheckInterval() {
        return getDelegate().getIdleCheckInterval();
    }

    @Override
    public int getInactiveReaderMaxOpenPartitions() {
        return getDelegate().getInactiveReaderMaxOpenPartitions();
    }

    @Override
    public long getInactiveReaderTTL() {
        return getDelegate().getInactiveReaderTTL();
    }

    @Override
    public long getInactiveWalWriterTTL() {
        return getDelegate().getInactiveWalWriterTTL();
    }

    @Override
    public long getInactiveWriterTTL() {
        return getDelegate().getInactiveWriterTTL();
    }

    @Override
    public int getIndexValueBlockSize() {
        return getDelegate().getIndexValueBlockSize();
    }

    @Override
    public long getInsertModelBatchSize() {
        return getDelegate().getInsertModelBatchSize();
    }

    @Override
    public int getInsertModelPoolCapacity() {
        return getDelegate().getInsertModelPoolCapacity();
    }

    @Override
    public int getLatestByQueueCapacity() {
        return getDelegate().getLatestByQueueCapacity();
    }

    @Override
    public int getMaxCrashFiles() {
        return getDelegate().getMaxCrashFiles();
    }

    @Override
    public int getMaxFileNameLength() {
        return getDelegate().getMaxFileNameLength();
    }

    @Override
    public int getMaxSqlRecompileAttempts() {
        return getDelegate().getMaxSqlRecompileAttempts();
    }

    @Override
    public int getMaxSwapFileCount() {
        return getDelegate().getMaxSwapFileCount();
    }

    @Override
    public int getMaxSymbolNotEqualsCount() {
        return getDelegate().getMaxSymbolNotEqualsCount();
    }

    @Override
    public int getMaxUncommittedRows() {
        return getDelegate().getMaxUncommittedRows();
    }

    @Override
    public int getMetadataPoolCapacity() {
        return getDelegate().getMetadataPoolCapacity();
    }

    @Override
    public @NotNull MicrosecondClock getMicrosecondClock() {
        return getDelegate().getMicrosecondClock();
    }

    @Override
    public @NotNull MillisecondClock getMillisecondClock() {
        return getDelegate().getMillisecondClock();
    }

    @Override
    public long getMiscAppendPageSize() {
        return getDelegate().getMiscAppendPageSize();
    }

    @Override
    public int getMkDirMode() {
        return getDelegate().getMkDirMode();
    }

    @Override
    public int getO3CallbackQueueCapacity() {
        return getDelegate().getO3CallbackQueueCapacity();
    }

    @Override
    public int getO3ColumnMemorySize() {
        return getDelegate().getO3ColumnMemorySize();
    }

    @Override
    public int getO3CopyQueueCapacity() {
        return getDelegate().getO3CopyQueueCapacity();
    }

    @Override
    public int getO3LagCalculationWindowsSize() {
        return getDelegate().getO3LagCalculationWindowsSize();
    }

    @Override
    public int getO3LastPartitionMaxSplits() {
        return getDelegate().getO3LastPartitionMaxSplits();
    }

    @Override
    public long getO3MaxLag() {
        return getDelegate().getO3MaxLag();
    }

    @Override
    public int getO3MemMaxPages() {
        return getDelegate().getO3MemMaxPages();
    }

    @Override
    public long getO3MinLag() {
        return getDelegate().getO3MinLag();
    }

    @Override
    public int getO3OpenColumnQueueCapacity() {
        return getDelegate().getO3OpenColumnQueueCapacity();
    }

    @Override
    public int getO3PartitionQueueCapacity() {
        return getDelegate().getO3PartitionQueueCapacity();
    }

    @Override
    public int getO3PurgeDiscoveryQueueCapacity() {
        return getDelegate().getO3PurgeDiscoveryQueueCapacity();
    }

    @Override
    public int getPageFrameReduceColumnListCapacity() {
        return getDelegate().getPageFrameReduceColumnListCapacity();
    }

    @Override
    public int getPageFrameReduceQueueCapacity() {
        return getDelegate().getPageFrameReduceQueueCapacity();
    }

    @Override
    public int getPageFrameReduceRowIdListCapacity() {
        return getDelegate().getPageFrameReduceRowIdListCapacity();
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return getDelegate().getPageFrameReduceShardCount();
    }

    @Override
    public int getParallelIndexThreshold() {
        return getDelegate().getParallelIndexThreshold();
    }

    @Override
    public long getPartitionO3SplitMinSize() {
        return getDelegate().getPartitionO3SplitMinSize();
    }

    @Override
    public int getPartitionPurgeListCapacity() {
        return getDelegate().getPartitionPurgeListCapacity();
    }

    @Override
    public int getQueryRegistryPoolSize() {
        return getDelegate().getQueryRegistryPoolSize();
    }

    @Override
    public int getReaderPoolMaxSegments() {
        return getDelegate().getReaderPoolMaxSegments();
    }

    @Override
    public int getRenameTableModelPoolCapacity() {
        return getDelegate().getRenameTableModelPoolCapacity();
    }

    @Override
    public int getRepeatMigrationsFromVersion() {
        return getDelegate().getRepeatMigrationsFromVersion();
    }

    @Override
    public int getRndFunctionMemoryMaxPages() {
        return getDelegate().getRndFunctionMemoryMaxPages();
    }

    @Override
    public int getRndFunctionMemoryPageSize() {
        return getDelegate().getRndFunctionMemoryPageSize();
    }

    @Override
    public @NotNull String getRoot() {
        return getDelegate().getRoot();
    }

    @Override
    public boolean getSampleByDefaultAlignmentCalendar() {
        return getDelegate().getSampleByDefaultAlignmentCalendar();
    }

    @Override
    public int getSampleByIndexSearchPageSize() {
        return getDelegate().getSampleByIndexSearchPageSize();
    }

    @Override
    public long getSequencerCheckInterval() {
        return getDelegate().getSequencerCheckInterval();
    }

    @Override
    public boolean getSimulateCrashEnabled() {
        return getDelegate().getSimulateCrashEnabled();
    }

    @Override
    public @NotNull CharSequence getSnapshotInstanceId() {
        return getDelegate().getSnapshotInstanceId();
    }

    @Override
    public @NotNull CharSequence getSnapshotRoot() {
        return getDelegate().getSnapshotRoot();
    }

    @Override
    public long getSpinLockTimeout() {
        return getDelegate().getSpinLockTimeout();
    }

    @Override
    public int getSqlAsOfJoinLookAhead() {
        return getDelegate().getSqlAsOfJoinLookAhead();
    }

    @Override
    public int getSqlCharacterStoreCapacity() {
        return getDelegate().getSqlCharacterStoreCapacity();
    }

    @Override
    public int getSqlCharacterStoreSequencePoolCapacity() {
        return getDelegate().getSqlCharacterStoreSequencePoolCapacity();
    }

    @Override
    public int getSqlColumnPoolCapacity() {
        return getDelegate().getSqlColumnPoolCapacity();
    }

    @Override
    public int getSqlCompilerPoolCapacity() {
        return getDelegate().getSqlCompilerPoolCapacity();
    }

    @Override
    public int getSqlCopyBufferSize() {
        return getDelegate().getSqlCopyBufferSize();
    }

    @Override
    public CharSequence getSqlCopyInputRoot() {
        return getDelegate().getSqlCopyInputRoot();
    }

    @Override
    public CharSequence getSqlCopyInputWorkRoot() {
        return getDelegate().getSqlCopyInputWorkRoot();
    }

    @Override
    public int getSqlCopyLogRetentionDays() {
        return getDelegate().getSqlCopyLogRetentionDays();
    }

    @Override
    public long getSqlCopyMaxIndexChunkSize() {
        return getDelegate().getSqlCopyMaxIndexChunkSize();
    }

    @Override
    public int getSqlCopyQueueCapacity() {
        return getDelegate().getSqlCopyQueueCapacity();
    }

    @Override
    public int getSqlDistinctTimestampKeyCapacity() {
        return getDelegate().getSqlDistinctTimestampKeyCapacity();
    }

    @Override
    public double getSqlDistinctTimestampLoadFactor() {
        return getDelegate().getSqlDistinctTimestampLoadFactor();
    }

    @Override
    public int getSqlExpressionPoolCapacity() {
        return getDelegate().getSqlExpressionPoolCapacity();
    }

    @Override
    public double getSqlFastMapLoadFactor() {
        return getDelegate().getSqlFastMapLoadFactor();
    }

    @Override
    public int getSqlHashJoinLightValueMaxPages() {
        return getDelegate().getSqlHashJoinLightValueMaxPages();
    }

    @Override
    public int getSqlHashJoinLightValuePageSize() {
        return getDelegate().getSqlHashJoinLightValuePageSize();
    }

    @Override
    public int getSqlHashJoinValueMaxPages() {
        return getDelegate().getSqlHashJoinValueMaxPages();
    }

    @Override
    public int getSqlHashJoinValuePageSize() {
        return getDelegate().getSqlHashJoinValuePageSize();
    }

    @Override
    public int getSqlJitBindVarsMemoryMaxPages() {
        return getDelegate().getSqlJitBindVarsMemoryMaxPages();
    }

    @Override
    public int getSqlJitBindVarsMemoryPageSize() {
        return getDelegate().getSqlJitBindVarsMemoryPageSize();
    }

    @Override
    public int getSqlJitIRMemoryMaxPages() {
        return getDelegate().getSqlJitIRMemoryMaxPages();
    }

    @Override
    public int getSqlJitIRMemoryPageSize() {
        return getDelegate().getSqlJitIRMemoryPageSize();
    }

    @Override
    public int getSqlJitMode() {
        return getDelegate().getSqlJitMode();
    }

    @Override
    public int getSqlJitPageAddressCacheThreshold() {
        return getDelegate().getSqlJitPageAddressCacheThreshold();
    }

    @Override
    public int getSqlJoinContextPoolCapacity() {
        return getDelegate().getSqlJoinContextPoolCapacity();
    }

    @Override
    public int getSqlJoinMetadataMaxResizes() {
        return getDelegate().getSqlJoinMetadataMaxResizes();
    }

    @Override
    public int getSqlJoinMetadataPageSize() {
        return getDelegate().getSqlJoinMetadataPageSize();
    }

    @Override
    public long getSqlLatestByRowCount() {
        return getDelegate().getSqlLatestByRowCount();
    }

    @Override
    public int getSqlLexerPoolCapacity() {
        return getDelegate().getSqlLexerPoolCapacity();
    }

    @Override
    public int getSqlMapMaxPages() {
        return getDelegate().getSqlMapMaxPages();
    }

    @Override
    public int getSqlMapMaxResizes() {
        return getDelegate().getSqlMapMaxResizes();
    }

    @Override
    public int getSqlMaxNegativeLimit() {
        return getDelegate().getSqlMaxNegativeLimit();
    }

    @Override
    public int getSqlModelPoolCapacity() {
        return getDelegate().getSqlModelPoolCapacity();
    }

    @Override
    public int getSqlPageFrameMaxRows() {
        return getDelegate().getSqlPageFrameMaxRows();
    }

    @Override
    public int getSqlPageFrameMinRows() {
        return getDelegate().getSqlPageFrameMinRows();
    }

    @Override
    public int getSqlParallelWorkStealingThreshold() {
        return getDelegate().getSqlParallelWorkStealingThreshold();
    }

    @Override
    public int getSqlSmallMapKeyCapacity() {
        return getDelegate().getSqlSmallMapKeyCapacity();
    }

    @Override
    public long getSqlSmallMapPageSize() {
        return getDelegate().getSqlSmallMapPageSize();
    }

    @Override
    public int getSqlSortKeyMaxPages() {
        return getDelegate().getSqlSortKeyMaxPages();
    }

    @Override
    public long getSqlSortKeyPageSize() {
        return getDelegate().getSqlSortKeyPageSize();
    }

    @Override
    public int getSqlSortLightValueMaxPages() {
        return getDelegate().getSqlSortLightValueMaxPages();
    }

    @Override
    public long getSqlSortLightValuePageSize() {
        return getDelegate().getSqlSortLightValuePageSize();
    }

    @Override
    public int getSqlSortValueMaxPages() {
        return getDelegate().getSqlSortValueMaxPages();
    }

    @Override
    public int getSqlSortValuePageSize() {
        return getDelegate().getSqlSortValuePageSize();
    }

    @Override
    public int getSqlUnorderedMapMaxEntrySize() {
        return getDelegate().getSqlUnorderedMapMaxEntrySize();
    }

    @Override
    public int getSqlWindowInitialRangeBufferSize() {
        return getDelegate().getSqlWindowInitialRangeBufferSize();
    }

    @Override
    public int getSqlWindowMaxRecursion() {
        return getDelegate().getSqlWindowMaxRecursion();
    }

    @Override
    public int getSqlWindowRowIdMaxPages() {
        return getDelegate().getSqlWindowRowIdMaxPages();
    }

    @Override
    public int getSqlWindowRowIdPageSize() {
        return getDelegate().getSqlWindowRowIdPageSize();
    }

    @Override
    public int getSqlWindowStoreMaxPages() {
        return getDelegate().getSqlWindowStoreMaxPages();
    }

    @Override
    public int getSqlWindowStorePageSize() {
        return getDelegate().getSqlWindowStorePageSize();
    }

    @Override
    public int getSqlWindowTreeKeyMaxPages() {
        return getDelegate().getSqlWindowTreeKeyMaxPages();
    }

    @Override
    public int getSqlWindowTreeKeyPageSize() {
        return getDelegate().getSqlWindowTreeKeyPageSize();
    }

    @Override
    public int getStrFunctionMaxBufferLength() {
        return getDelegate().getStrFunctionMaxBufferLength();
    }

    @Override
    public long getSystemDataAppendPageSize() {
        return getDelegate().getSystemDataAppendPageSize();
    }

    @Override
    public int getSystemO3ColumnMemorySize() {
        return getDelegate().getSystemO3ColumnMemorySize();
    }

    @Override
    public @NotNull CharSequence getSystemTableNamePrefix() {
        return getDelegate().getSystemTableNamePrefix();
    }

    @Override
    public long getSystemWalDataAppendPageSize() {
        return getDelegate().getSystemWalDataAppendPageSize();
    }

    @Override
    public long getSystemWalEventAppendPageSize() {
        return getDelegate().getSystemWalEventAppendPageSize();
    }

    @Override
    public long getTableRegistryAutoReloadFrequency() {
        return getDelegate().getTableRegistryAutoReloadFrequency();
    }

    @Override
    public int getTableRegistryCompactionThreshold() {
        return getDelegate().getTableRegistryCompactionThreshold();
    }

    @Override
    public @NotNull TelemetryConfiguration getTelemetryConfiguration() {
        return getDelegate().getTelemetryConfiguration();
    }

    @Override
    public CharSequence getTempRenamePendingTablePrefix() {
        return getDelegate().getTempRenamePendingTablePrefix();
    }

    @Override
    public @NotNull TextConfiguration getTextConfiguration() {
        return getDelegate().getTextConfiguration();
    }

    @Override
    public int getTxnScoreboardEntryCount() {
        return getDelegate().getTxnScoreboardEntryCount();
    }

    @Override
    public int getVectorAggregateQueueCapacity() {
        return getDelegate().getVectorAggregateQueueCapacity();
    }

    @Override
    public @NotNull VolumeDefinitions getVolumeDefinitions() {
        return getDelegate().getVolumeDefinitions();
    }

    @Override
    public int getWalApplyLookAheadTransactionCount() {
        return getDelegate().getWalApplyLookAheadTransactionCount();
    }

    @Override
    public long getWalApplyTableTimeQuota() {
        return getDelegate().getWalApplyTableTimeQuota();
    }

    @Override
    public long getWalDataAppendPageSize() {
        return getDelegate().getWalDataAppendPageSize();
    }

    @Override
    public boolean getWalEnabledDefault() {
        return getDelegate().getWalEnabledDefault();
    }

    @Override
    public long getWalEventAppendPageSize() {
        return getDelegate().getWalEventAppendPageSize();
    }

    @Override
    public long getWalMaxLagSize() {
        return getDelegate().getWalMaxLagSize();
    }

    @Override
    public int getWalMaxLagTxnCount() {
        return getDelegate().getWalMaxLagTxnCount();
    }

    @Override
    public int getWalMaxSegmentFileDescriptorsCache() {
        return getDelegate().getWalMaxSegmentFileDescriptorsCache();
    }

    @Override
    public long getWalPurgeInterval() {
        return getDelegate().getWalPurgeInterval();
    }

    @Override
    public int getWalPurgeWaitBeforeDelete() {
        return getDelegate().getWalPurgeWaitBeforeDelete();
    }

    @Override
    public int getWalRecreateDistressedSequencerAttempts() {
        return getDelegate().getWalRecreateDistressedSequencerAttempts();
    }

    @Override
    public long getWalSegmentRolloverRowCount() {
        return getDelegate().getWalSegmentRolloverRowCount();
    }

    @Override
    public long getWalSegmentRolloverSize() {
        return getDelegate().getWalSegmentRolloverSize();
    }

    @Override
    public double getWalSquashUncommittedRowsMultiplier() {
        return getDelegate().getWalSquashUncommittedRowsMultiplier();
    }

    @Override
    public int getWalTxnNotificationQueueCapacity() {
        return getDelegate().getWalTxnNotificationQueueCapacity();
    }

    @Override
    public int getWalWriterPoolMaxSegments() {
        return getDelegate().getWalWriterPoolMaxSegments();
    }

    @Override
    public int getWindowColumnPoolCapacity() {
        return getDelegate().getWindowColumnPoolCapacity();
    }

    @Override
    public int getWithClauseModelPoolCapacity() {
        return getDelegate().getWithClauseModelPoolCapacity();
    }

    @Override
    public long getWorkStealTimeoutNanos() {
        return getDelegate().getWorkStealTimeoutNanos();
    }

    @Override
    public long getWriterAsyncCommandBusyWaitTimeout() {
        return getDelegate().getWriterAsyncCommandBusyWaitTimeout();
    }

    @Override
    public long getWriterAsyncCommandMaxTimeout() {
        return getDelegate().getWriterAsyncCommandMaxTimeout();
    }

    @Override
    public int getWriterCommandQueueCapacity() {
        return getDelegate().getWriterCommandQueueCapacity();
    }

    @Override
    public long getWriterCommandQueueSlotSize() {
        return getDelegate().getWriterCommandQueueSlotSize();
    }

    @Override
    public long getWriterFileOpenOpts() {
        return getDelegate().getWriterFileOpenOpts();
    }

    @Override
    public int getWriterTickRowsCountMod() {
        return getDelegate().getWriterTickRowsCountMod();
    }

    @Override
    public boolean isGroupByPresizeEnabled() {
        return getDelegate().isGroupByPresizeEnabled();
    }

    @Override
    public boolean isIOURingEnabled() {
        return getDelegate().isIOURingEnabled();
    }

    @Override
    public boolean isMultiKeyDedupEnabled() {
        return getDelegate().isMultiKeyDedupEnabled();
    }

    @Override
    public boolean isO3QuickSortEnabled() {
        return getDelegate().isO3QuickSortEnabled();
    }

    @Override
    public boolean isParallelIndexingEnabled() {
        return getDelegate().isParallelIndexingEnabled();
    }

    @Override
    public boolean isReadOnlyInstance() {
        return getDelegate().isReadOnlyInstance();
    }

    @Override
    public boolean isSnapshotRecoveryEnabled() {
        return getDelegate().isSnapshotRecoveryEnabled();
    }

    @Override
    public boolean isSqlJitDebugEnabled() {
        return getDelegate().isSqlJitDebugEnabled();
    }

    @Override
    public boolean isSqlParallelFilterEnabled() {
        return getDelegate().isSqlParallelFilterEnabled();
    }

    @Override
    public boolean isSqlParallelFilterPreTouchEnabled() {
        return getDelegate().isSqlParallelFilterPreTouchEnabled();
    }

    @Override
    public boolean isSqlParallelGroupByEnabled() {
        return getDelegate().isSqlParallelGroupByEnabled();
    }

    @Override
    public boolean isTableTypeConversionEnabled() {
        return getDelegate().isTableTypeConversionEnabled();
    }

    @Override
    public boolean isWalApplyEnabled() {
        return getDelegate().isWalApplyEnabled();
    }

    public boolean isWalSupported() {
        return getDelegate().isWalSupported();
    }

    @Override
    public boolean isWriterMixedIOEnabled() {
        return getDelegate().isWriterMixedIOEnabled();
    }

    @Override
    public boolean mangleTableDirNames() {
        return getDelegate().mangleTableDirNames();
    }

    @Override
    public void populateSettings(CharSequenceObjHashMap<CharSequence> settings) {
        getDelegate().populateSettings(settings);
    }

    protected CairoConfiguration getDelegate() {
        return delegate;
    }
}
