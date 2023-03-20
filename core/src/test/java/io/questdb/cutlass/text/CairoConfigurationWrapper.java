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

package io.questdb.cutlass.text;

import io.questdb.BuildInformation;
import io.questdb.TelemetryConfiguration;
import io.questdb.VolumeDefinitions;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public class CairoConfigurationWrapper implements CairoConfiguration {

    private final CairoConfiguration conf;

    public CairoConfigurationWrapper(CairoConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public boolean attachPartitionCopy() {
        return false;
    }

    @Override
    public boolean enableTestFactories() {
        return conf.enableTestFactories();
    }

    @Override
    public boolean getAllowTableRegistrySharedWrite() {
        return conf.getAllowTableRegistrySharedWrite();
    }

    @Override
    public int getAnalyticColumnPoolCapacity() {
        return conf.getAnalyticColumnPoolCapacity();
    }

    @Override
    public String getAttachPartitionSuffix() {
        return conf.getAttachPartitionSuffix();
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return conf.getBackupDirTimestampFormat();
    }

    @Override
    public int getBackupMkDirMode() {
        return conf.getBackupMkDirMode();
    }

    @Override
    public CharSequence getBackupRoot() {
        return conf.getBackupRoot();
    }

    @Override
    public CharSequence getBackupTempDirName() {
        return conf.getBackupTempDirName();
    }

    @Override
    public int getBinaryEncodingMaxLength() {
        return conf.getBinaryEncodingMaxLength();
    }

    @Override
    public int getBindVariablePoolSize() {
        return conf.getBindVariablePoolSize();
    }

    @Override
    public BuildInformation getBuildInformation() {
        return conf.getBuildInformation();
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return conf.getCircuitBreakerConfiguration();
    }

    @Override
    public int getColumnCastModelPoolCapacity() {
        return conf.getColumnCastModelPoolCapacity();
    }

    @Override
    public int getColumnIndexerQueueCapacity() {
        return conf.getColumnIndexerQueueCapacity();
    }

    @Override
    public int getColumnPurgeQueueCapacity() {
        return conf.getColumnPurgeQueueCapacity();
    }

    @Override
    public long getColumnPurgeRetryDelay() {
        return conf.getColumnPurgeRetryDelay();
    }

    @Override
    public long getColumnPurgeRetryDelayLimit() {
        return conf.getColumnPurgeRetryDelayLimit();
    }

    @Override
    public double getColumnPurgeRetryDelayMultiplier() {
        return conf.getColumnPurgeRetryDelayMultiplier();
    }

    @Override
    public int getColumnPurgeTaskPoolCapacity() {
        return conf.getColumnPurgeTaskPoolCapacity();
    }

    @Override
    public int getCommitMode() {
        return conf.getCommitMode();
    }

    @Override
    public CharSequence getConfRoot() {
        return conf.getConfRoot();
    }

    @Override
    public int getCopyPoolCapacity() {
        return conf.getCopyPoolCapacity();
    }

    @Override
    public int getCreateAsSelectRetryCount() {
        return conf.getCreateAsSelectRetryCount();
    }

    @Override
    public int getCreateTableModelPoolCapacity() {
        return conf.getCreateTableModelPoolCapacity();
    }

    @Override
    public long getDataAppendPageSize() {
        return conf.getDataAppendPageSize();
    }

    @Override
    public long getDataIndexKeyAppendPageSize() {
        return conf.getDataIndexKeyAppendPageSize();
    }

    @Override
    public long getDataIndexValueAppendPageSize() {
        return conf.getDataIndexValueAppendPageSize();
    }

    @Override
    public long getDatabaseIdHi() {
        return conf.getDatabaseIdHi();
    }

    @Override
    public long getDatabaseIdLo() {
        return conf.getDatabaseIdLo();
    }

    @Override
    public CharSequence getDbDirectory() {
        return conf.getDbDirectory();
    }

    @Override
    public DateLocale getDefaultDateLocale() {
        return conf.getDefaultDateLocale();
    }

    @Override
    public CharSequence getDefaultMapType() {
        return conf.getDefaultMapType();
    }

    @Override
    public boolean getDefaultSymbolCacheFlag() {
        return conf.getDefaultSymbolCacheFlag();
    }

    @Override
    public int getDefaultSymbolCapacity() {
        return conf.getDefaultSymbolCapacity();
    }

    @Override
    public int getDoubleToStrCastScale() {
        return conf.getDoubleToStrCastScale();
    }

    @Override
    public int getExplainPoolCapacity() {
        return conf.getExplainPoolCapacity();
    }

    @Override
    public int getFileOperationRetryCount() {
        return conf.getFileOperationRetryCount();
    }

    @Override
    public FilesFacade getFilesFacade() {
        return conf.getFilesFacade();
    }

    @Override
    public int getFloatToStrCastScale() {
        return conf.getFloatToStrCastScale();
    }

    @Override
    public int getGroupByMapCapacity() {
        return conf.getGroupByMapCapacity();
    }

    @Override
    public int getGroupByPoolCapacity() {
        return conf.getGroupByPoolCapacity();
    }

    @Override
    public long getIdleCheckInterval() {
        return conf.getIdleCheckInterval();
    }

    @Override
    public long getInactiveReaderTTL() {
        return conf.getInactiveReaderTTL();
    }

    @Override
    public long getInactiveWalWriterTTL() {
        return conf.getInactiveWalWriterTTL();
    }

    @Override
    public long getInactiveWriterTTL() {
        return conf.getInactiveWriterTTL();
    }

    @Override
    public int getIndexValueBlockSize() {
        return conf.getIndexValueBlockSize();
    }

    @Override
    public int getInsertPoolCapacity() {
        return conf.getInsertPoolCapacity();
    }

    @Override
    public int getLatestByQueueCapacity() {
        return conf.getLatestByQueueCapacity();
    }

    @Override
    public int getMaxCrashFiles() {
        return conf.getMaxCrashFiles();
    }

    @Override
    public int getMaxFileNameLength() {
        return conf.getMaxFileNameLength();
    }

    @Override
    public int getMaxSwapFileCount() {
        return conf.getMaxSwapFileCount();
    }

    @Override
    public int getMaxSymbolNotEqualsCount() {
        return conf.getMaxSymbolNotEqualsCount();
    }

    @Override
    public int getMaxUncommittedRows() {
        return conf.getMaxUncommittedRows();
    }

    @Override
    public int getMetadataPoolCapacity() {
        return conf.getMetadataPoolCapacity();
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return conf.getMicrosecondClock();
    }

    @Override
    public MillisecondClock getMillisecondClock() {
        return conf.getMillisecondClock();
    }

    @Override
    public long getMiscAppendPageSize() {
        return conf.getMiscAppendPageSize();
    }

    @Override
    public int getMkDirMode() {
        return conf.getMkDirMode();
    }

    @Override
    public int getO3CallbackQueueCapacity() {
        return conf.getO3CallbackQueueCapacity();
    }

    @Override
    public int getO3ColumnMemorySize() {
        return conf.getO3ColumnMemorySize();
    }

    @Override
    public int getO3CopyQueueCapacity() {
        return conf.getO3CopyQueueCapacity();
    }

    @Override
    public int getO3LagCalculationWindowsSize() {
        return conf.getO3LagCalculationWindowsSize();
    }

    @Override
    public long getO3MaxLag() {
        return conf.getO3MaxLag();
    }

    @Override
    public int getO3MemMaxPages() {
        return conf.getO3MemMaxPages();
    }

    @Override
    public long getO3MinLag() {
        return conf.getO3MinLag();
    }

    @Override
    public int getO3OpenColumnQueueCapacity() {
        return conf.getO3OpenColumnQueueCapacity();
    }

    @Override
    public int getO3PartitionQueueCapacity() {
        return conf.getO3PartitionQueueCapacity();
    }

    @Override
    public int getO3PurgeDiscoveryQueueCapacity() {
        return conf.getO3PurgeDiscoveryQueueCapacity();
    }

    @Override
    public int getPageFrameReduceColumnListCapacity() {
        return conf.getPageFrameReduceColumnListCapacity();
    }

    @Override
    public int getPageFrameReduceQueueCapacity() {
        return conf.getPageFrameReduceQueueCapacity();
    }

    @Override
    public int getPageFrameReduceRowIdListCapacity() {
        return conf.getPageFrameReduceRowIdListCapacity();
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return conf.getPageFrameReduceShardCount();
    }

    @Override
    public int getPageFrameReduceTaskPoolCapacity() {
        return conf.getPageFrameReduceTaskPoolCapacity();
    }

    @Override
    public int getParallelIndexThreshold() {
        return conf.getParallelIndexThreshold();
    }

    @Override
    public int getPartitionPurgeListCapacity() {
        return conf.getPartitionPurgeListCapacity();
    }

    @Override
    public int getQueryCacheEventQueueCapacity() {
        return conf.getQueryCacheEventQueueCapacity();
    }

    @Override
    public int getReaderPoolMaxSegments() {
        return conf.getReaderPoolMaxSegments();
    }

    @Override
    public int getRenameTableModelPoolCapacity() {
        return conf.getRenameTableModelPoolCapacity();
    }

    @Override
    public int getRndFunctionMemoryMaxPages() {
        return conf.getRndFunctionMemoryMaxPages();
    }

    @Override
    public int getRndFunctionMemoryPageSize() {
        return conf.getRndFunctionMemoryPageSize();
    }

    @Override
    public CharSequence getRoot() {
        return conf.getRoot();
    }

    @Override
    public int getSampleByIndexSearchPageSize() {
        return conf.getSampleByIndexSearchPageSize();
    }

    @Override
    public boolean getSimulateCrashEnabled() {
        return conf.getSimulateCrashEnabled();
    }

    @Override
    public CharSequence getSnapshotInstanceId() {
        return conf.getSnapshotInstanceId();
    }

    @Override
    public CharSequence getSnapshotRoot() {
        return conf.getSnapshotRoot();
    }

    @Override
    public long getSpinLockTimeout() {
        return conf.getSpinLockTimeout();
    }

    @Override
    public int getSqlAnalyticRowIdMaxPages() {
        return conf.getSqlAnalyticRowIdMaxPages();
    }

    @Override
    public int getSqlAnalyticRowIdPageSize() {
        return conf.getSqlAnalyticRowIdPageSize();
    }

    @Override
    public int getSqlAnalyticStoreMaxPages() {
        return conf.getSqlAnalyticStoreMaxPages();
    }

    @Override
    public int getSqlAnalyticStorePageSize() {
        return conf.getSqlAnalyticStorePageSize();
    }

    @Override
    public int getSqlAnalyticTreeKeyMaxPages() {
        return conf.getSqlAnalyticTreeKeyMaxPages();
    }

    @Override
    public int getSqlAnalyticTreeKeyPageSize() {
        return conf.getSqlAnalyticTreeKeyPageSize();
    }

    @Override
    public int getSqlCharacterStoreCapacity() {
        return conf.getSqlCharacterStoreCapacity();
    }

    @Override
    public int getSqlCharacterStoreSequencePoolCapacity() {
        return conf.getSqlCharacterStoreSequencePoolCapacity();
    }

    @Override
    public int getSqlColumnPoolCapacity() {
        return conf.getSqlColumnPoolCapacity();
    }

    @Override
    public double getSqlCompactMapLoadFactor() {
        return conf.getSqlCompactMapLoadFactor();
    }

    @Override
    public int getSqlCopyBufferSize() {
        return conf.getSqlCopyBufferSize();
    }

    @Override
    public CharSequence getSqlCopyInputRoot() {
        return conf.getSqlCopyInputRoot();
    }

    @Override
    public CharSequence getSqlCopyInputWorkRoot() {
        return conf.getSqlCopyInputWorkRoot();
    }

    @Override
    public int getSqlCopyLogRetentionDays() {
        return conf.getSqlCopyLogRetentionDays();
    }

    @Override
    public long getSqlCopyMaxIndexChunkSize() {
        return conf.getSqlCopyMaxIndexChunkSize();
    }

    @Override
    public int getSqlCopyQueueCapacity() {
        return conf.getSqlCopyQueueCapacity();
    }

    @Override
    public int getSqlDistinctTimestampKeyCapacity() {
        return conf.getSqlDistinctTimestampKeyCapacity();
    }

    @Override
    public double getSqlDistinctTimestampLoadFactor() {
        return conf.getSqlDistinctTimestampLoadFactor();
    }

    @Override
    public int getSqlExpressionPoolCapacity() {
        return conf.getSqlExpressionPoolCapacity();
    }

    @Override
    public double getSqlFastMapLoadFactor() {
        return conf.getSqlFastMapLoadFactor();
    }

    @Override
    public int getSqlHashJoinLightValueMaxPages() {
        return conf.getSqlHashJoinLightValueMaxPages();
    }

    @Override
    public int getSqlHashJoinLightValuePageSize() {
        return conf.getSqlHashJoinLightValuePageSize();
    }

    @Override
    public int getSqlHashJoinValueMaxPages() {
        return conf.getSqlHashJoinValueMaxPages();
    }

    @Override
    public int getSqlHashJoinValuePageSize() {
        return conf.getSqlHashJoinValuePageSize();
    }

    @Override
    public int getSqlJitBindVarsMemoryMaxPages() {
        return conf.getSqlJitBindVarsMemoryMaxPages();
    }

    @Override
    public int getSqlJitBindVarsMemoryPageSize() {
        return conf.getSqlJitBindVarsMemoryPageSize();
    }

    @Override
    public int getSqlJitIRMemoryMaxPages() {
        return conf.getSqlJitIRMemoryMaxPages();
    }

    @Override
    public int getSqlJitIRMemoryPageSize() {
        return conf.getSqlJitIRMemoryPageSize();
    }

    @Override
    public int getSqlJitMode() {
        return conf.getSqlJitMode();
    }

    @Override
    public int getSqlJitPageAddressCacheThreshold() {
        return conf.getSqlJitPageAddressCacheThreshold();
    }

    @Override
    public int getSqlJitRowsThreshold() {
        return conf.getSqlJitRowsThreshold();
    }

    @Override
    public int getSqlJoinContextPoolCapacity() {
        return conf.getSqlJoinContextPoolCapacity();
    }

    @Override
    public int getSqlJoinMetadataMaxResizes() {
        return conf.getSqlJoinMetadataMaxResizes();
    }

    @Override
    public int getSqlJoinMetadataPageSize() {
        return conf.getSqlJoinMetadataPageSize();
    }

    @Override
    public long getSqlLatestByRowCount() {
        return conf.getSqlLatestByRowCount();
    }

    @Override
    public int getSqlLexerPoolCapacity() {
        return conf.getSqlLexerPoolCapacity();
    }

    @Override
    public int getSqlMapKeyCapacity() {
        return conf.getSqlMapKeyCapacity();
    }

    @Override
    public int getSqlMapMaxPages() {
        return conf.getSqlMapMaxPages();
    }

    @Override
    public int getSqlMapMaxResizes() {
        return conf.getSqlMapMaxResizes();
    }

    @Override
    public int getSqlMapPageSize() {
        return conf.getSqlMapPageSize();
    }

    @Override
    public int getSqlMaxNegativeLimit() {
        return conf.getSqlMaxNegativeLimit();
    }

    @Override
    public int getSqlModelPoolCapacity() {
        return conf.getSqlModelPoolCapacity();
    }

    @Override
    public int getSqlPageFrameMaxRows() {
        return conf.getSqlPageFrameMaxRows();
    }

    @Override
    public int getSqlPageFrameMinRows() {
        return conf.getSqlPageFrameMinRows();
    }

    @Override
    public int getSqlSmallMapKeyCapacity() {
        return conf.getSqlSmallMapKeyCapacity();
    }

    @Override
    public int getSqlSmallMapPageSize() {
        return conf.getSqlSmallMapPageSize();
    }

    @Override
    public int getSqlSortKeyMaxPages() {
        return conf.getSqlSortKeyMaxPages();
    }

    @Override
    public long getSqlSortKeyPageSize() {
        return conf.getSqlSortKeyPageSize();
    }

    @Override
    public int getSqlSortLightValueMaxPages() {
        return conf.getSqlSortLightValueMaxPages();
    }

    @Override
    public long getSqlSortLightValuePageSize() {
        return conf.getSqlSortLightValuePageSize();
    }

    @Override
    public int getSqlSortValueMaxPages() {
        return conf.getSqlSortValueMaxPages();
    }

    @Override
    public int getSqlSortValuePageSize() {
        return conf.getSqlSortValuePageSize();
    }

    @Override
    public int getStrFunctionMaxBufferLength() {
        return conf.getStrFunctionMaxBufferLength();
    }

    @Override
    public CharSequence getSystemTableNamePrefix() {
        return conf.getSystemTableNamePrefix();
    }

    @Override
    public long getTableRegistryAutoReloadFrequency() {
        return conf.getTableRegistryAutoReloadFrequency();
    }

    @Override
    public int getTableRegistryCompactionThreshold() {
        return conf.getTableRegistryCompactionThreshold();
    }

    @Override
    public TelemetryConfiguration getTelemetryConfiguration() {
        return conf.getTelemetryConfiguration();
    }

    @Override
    public TextConfiguration getTextConfiguration() {
        return conf.getTextConfiguration();
    }

    @Override
    public int getTxnScoreboardEntryCount() {
        return conf.getTxnScoreboardEntryCount();
    }

    @Override
    public int getVectorAggregateQueueCapacity() {
        return conf.getVectorAggregateQueueCapacity();
    }

    @Override
    public VolumeDefinitions getVolumeDefinitions() {
        return conf.getVolumeDefinitions();
    }

    @Override
    public int getWalApplyLookAheadTransactionCount() {
        return conf.getWalApplyLookAheadTransactionCount();
    }

    @Override
    public int getWalCommitSquashRowLimit() {
        return conf.getWalCommitSquashRowLimit();
    }

    @Override
    public boolean getWalEnabledDefault() {
        return conf.getWalEnabledDefault();
    }

    @Override
    public long getWalPurgeInterval() {
        return conf.getWalPurgeInterval();
    }

    @Override
    public int getWalRecreateDistressedSequencerAttempts() {
        return conf.getWalRecreateDistressedSequencerAttempts();
    }

    @Override
    public long getWalSegmentRolloverRowCount() {
        return conf.getWalSegmentRolloverRowCount();
    }

    @Override
    public int getWalTxnNotificationQueueCapacity() {
        return conf.getWalTxnNotificationQueueCapacity();
    }

    @Override
    public int getWithClauseModelPoolCapacity() {
        return conf.getWithClauseModelPoolCapacity();
    }

    @Override
    public long getWorkStealTimeoutNanos() {
        return conf.getWorkStealTimeoutNanos();
    }

    @Override
    public long getWriterAsyncCommandBusyWaitTimeout() {
        return conf.getWriterAsyncCommandBusyWaitTimeout();
    }

    @Override
    public long getWriterAsyncCommandMaxTimeout() {
        return conf.getWriterAsyncCommandMaxTimeout();
    }

    @Override
    public int getWriterCommandQueueCapacity() {
        return conf.getWriterCommandQueueCapacity();
    }

    @Override
    public long getWriterCommandQueueSlotSize() {
        return conf.getWriterCommandQueueSlotSize();
    }

    @Override
    public long getWriterFileOpenOpts() {
        return conf.getWriterFileOpenOpts();
    }

    @Override
    public int getWriterTickRowsCountMod() {
        return conf.getWriterTickRowsCountMod();
    }

    @Override
    public boolean isIOURingEnabled() {
        return conf.isIOURingEnabled();
    }

    @Override
    public boolean isO3QuickSortEnabled() {
        return conf.isO3QuickSortEnabled();
    }

    @Override
    public boolean isParallelIndexingEnabled() {
        return conf.isParallelIndexingEnabled();
    }

    @Override
    public boolean isReadOnlyInstance() {
        return conf.isReadOnlyInstance();
    }

    @Override
    public boolean isSnapshotRecoveryEnabled() {
        return conf.isSnapshotRecoveryEnabled();
    }

    @Override
    public boolean isSqlJitDebugEnabled() {
        return conf.isSqlJitDebugEnabled();
    }

    @Override
    public boolean isSqlParallelFilterEnabled() {
        return conf.isSqlParallelFilterEnabled();
    }

    @Override
    public boolean isSqlParallelFilterPreTouchEnabled() {
        return conf.isSqlParallelFilterPreTouchEnabled();
    }

    @Override
    public boolean isTableTypeConversionEnabled() {
        return conf.isTableTypeConversionEnabled();
    }

    @Override
    public boolean isWalSupported() {
        return conf.isWalSupported();
    }

    @Override
    public boolean mangleTableDirNames() {
        return conf.mangleTableDirNames();
    }
}
