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

import io.questdb.TelemetryConfiguration;
import io.questdb.VolumeDefinitions;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.NanosecondClock;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public class CairoTestConfiguration extends DefaultTestCairoConfiguration {
    private final ConfigurationOverrides overrides;
    private final TelemetryConfiguration telemetryConfiguration;
    private final VolumeDefinitions volumeDefinitions = new VolumeDefinitions();

    public CairoTestConfiguration(CharSequence root, TelemetryConfiguration telemetryConfiguration, ConfigurationOverrides overrides) {
        super(root);
        this.telemetryConfiguration = telemetryConfiguration;
        this.overrides = overrides;
    }

    @Override
    public boolean attachPartitionCopy() {
        return overrides.getCopyPartitionOnAttach() == null ? super.attachPartitionCopy() : overrides.getCopyPartitionOnAttach();
    }

    @Override
    public String getAttachPartitionSuffix() {
        return overrides.getAttachableDirSuffix() == null ? super.getAttachPartitionSuffix() : overrides.getAttachableDirSuffix();
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return overrides.getBackupDirTimestampFormat() == null ? super.getBackupDirTimestampFormat() : overrides.getBackupDirTimestampFormat();
    }

    @Override
    public CharSequence getBackupRoot() {
        return overrides.getBackupDir() == null ? super.getBackupRoot() : overrides.getBackupDir();
    }

    @Override
    public int getBinaryEncodingMaxLength() {
        return overrides.getBinaryEncodingMaxLength() > 0 ? overrides.getBinaryEncodingMaxLength() : super.getBinaryEncodingMaxLength();
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return overrides.getCircuitBreakerConfiguration() != null ? overrides.getCircuitBreakerConfiguration() : super.getCircuitBreakerConfiguration();
    }

    @Override
    public int getColumnPurgeQueueCapacity() {
        return overrides.getColumnVersionPurgeQueueCapacity() < 0 ? super.getColumnPurgeQueueCapacity() : overrides.getColumnVersionPurgeQueueCapacity();
    }

    @Override
    public long getColumnPurgeRetryDelay() {
        return overrides.getColumnPurgeRetryDelay() > 0 ? overrides.getColumnPurgeRetryDelay() : 10;
    }

    @Override
    public int getColumnPurgeTaskPoolCapacity() {
        return overrides.getColumnVersionTaskPoolCapacity() >= 0 ? overrides.getColumnVersionTaskPoolCapacity() : super.getColumnPurgeTaskPoolCapacity();
    }

    @Override
    public int getCopyPoolCapacity() {
        return overrides.getCapacity() == -1 ? super.getCopyPoolCapacity() : overrides.getCapacity();
    }

    @Override
    public long getDataAppendPageSize() {
        return overrides.getDataAppendPageSize() > 0 ? overrides.getDataAppendPageSize() : super.getDataAppendPageSize();
    }

    @Override
    public CharSequence getDefaultMapType() {
        return overrides.getDefaultMapType() == null ? super.getDefaultMapType() : overrides.getDefaultMapType();
    }

    @Override
    public FilesFacade getFilesFacade() {
        return overrides.getFilesFacade() != null ? overrides.getFilesFacade() : super.getFilesFacade();
    }

    @Override
    public long getInactiveWalWriterTTL() {
        return -10000;
    }

    @Override
    public int getMaxFileNameLength() {
        return overrides.getMaxFileNameLength() > 0 ? overrides.getMaxFileNameLength() : super.getMaxFileNameLength();
    }

    @Override
    public int getMaxUncommittedRows() {
        return overrides.getMaxUncommittedRows() >= 0 ? overrides.getMaxUncommittedRows() : super.getMaxUncommittedRows();
    }

    @Override
    public int getMetadataPoolCapacity() {
        return 1;
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return overrides.getTestMicrosClock();
    }

    @Override
    public MillisecondClock getMillisecondClock() {
        return () -> overrides.getTestMicrosClock().getTicks() / 1000L;
    }

    @Override
    public NanosecondClock getNanosecondClock() {
        return () -> overrides.getTestMicrosClock().getTicks() * 1000L;
    }

    @Override
    public int getO3ColumnMemorySize() {
        return overrides.getO3ColumnMemorySize() < 0 ? super.getO3ColumnMemorySize() : overrides.getO3ColumnMemorySize();
    }

    @Override
    public long getO3MaxLag() {
        return overrides.getO3MaxLag() >= 0 ? overrides.getO3MaxLag() : super.getO3MaxLag();
    }

    @Override
    public long getO3MinLag() {
        return overrides.getO3MinLag() >= 0 ? overrides.getO3MinLag() : super.getO3MinLag();
    }

    @Override
    public int getPageFrameReduceQueueCapacity() {
        return overrides.getPageFrameReduceQueueCapacity() < 0 ? super.getPageFrameReduceQueueCapacity() : overrides.getPageFrameReduceQueueCapacity();
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return overrides.getPageFrameReduceShardCount() < 0 ? super.getPageFrameReduceShardCount() : overrides.getPageFrameReduceShardCount();
    }

    @Override
    public int getPartitionPurgeListCapacity() {
        // Bump it to high number so that test doesn't fail with memory leak if LongList re-allocates
        return 512;
    }

    @Override
    public int getQueryCacheEventQueueCapacity() {
        return overrides.getQueryCacheEventQueueCapacity() < 0 ? super.getQueryCacheEventQueueCapacity() : overrides.getQueryCacheEventQueueCapacity();
    }

    @Override
    public int getRndFunctionMemoryMaxPages() {
        return overrides.getRndFunctionMemoryMaxPages() < 0 ? super.getRndFunctionMemoryMaxPages() : overrides.getRndFunctionMemoryMaxPages();
    }

    @Override
    public int getRndFunctionMemoryPageSize() {
        return overrides.getRndFunctionMemoryPageSize() < 0 ? super.getRndFunctionMemoryPageSize() : overrides.getRndFunctionMemoryPageSize();
    }

    @Override
    public RostiAllocFacade getRostiAllocFacade() {
        return overrides.getRostiAllocFacade() != null ? overrides.getRostiAllocFacade() : super.getRostiAllocFacade();
    }

    @Override
    public int getSampleByIndexSearchPageSize() {
        return overrides.getSampleByIndexSearchPageSize() > 0 ? overrides.getSampleByIndexSearchPageSize() : super.getSampleByIndexSearchPageSize();
    }

    @Override
    public CharSequence getSnapshotInstanceId() {
        return overrides.getSnapshotInstanceId() != null ? overrides.getSnapshotInstanceId() : super.getSnapshotInstanceId();
    }

    @Override
    public long getSpinLockTimeout() {
        return overrides.getSpinLockTimeout() > -1 ? overrides.getSpinLockTimeout() : 5000L;
    }

    @Override
    public int getSqlCopyBufferSize() {
        return overrides.getSqlCopyBufferSize();
    }

    @Override
    public CharSequence getSqlCopyInputRoot() {
        return overrides.getInputRoot();
    }

    @Override
    public CharSequence getSqlCopyInputWorkRoot() {
        return overrides.getInputWorkRoot();
    }

    @Override
    public int getSqlCopyLogRetentionDays() {
        return overrides.getParallelImportStatusLogKeepNDays() >= 0 ? overrides.getParallelImportStatusLogKeepNDays() : super.getSqlCopyLogRetentionDays();
    }

    @Override
    public int getSqlJitMode() {
        return overrides.getJitMode();
    }

    @Override
    public int getSqlJoinMetadataMaxResizes() {
        return overrides.getSqlJoinMetadataMaxResizes() > -1 ? overrides.getSqlJoinMetadataMaxResizes() : super.getSqlJoinMetadataMaxResizes();
    }

    @Override
    public int getSqlJoinMetadataPageSize() {
        return overrides.getSqlJoinMetadataPageSize() > -1 ? overrides.getSqlJoinMetadataPageSize() : super.getSqlJoinMetadataPageSize();
    }

    @Override
    public int getSqlPageFrameMaxRows() {
        return overrides.getPageFrameMaxRows() < 0 ? super.getSqlPageFrameMaxRows() : overrides.getPageFrameMaxRows();
    }

    @Override
    public int getTableRegistryCompactionThreshold() {
        return overrides.getTableRegistryCompactionThreshold() > 0 ? overrides.getTableRegistryCompactionThreshold() : super.getTableRegistryCompactionThreshold();
    }

    @Override
    public TelemetryConfiguration getTelemetryConfiguration() {
        return telemetryConfiguration;
    }

    @Override
    public VolumeDefinitions getVolumeDefinitions() {
        return volumeDefinitions;
    }

    @Override
    public boolean getWalEnabledDefault() {
        return overrides.getDefaultTableWriteMode() < 0 ? super.getWalEnabledDefault() : overrides.getDefaultTableWriteMode() == 1;
    }

    @Override
    public long getWalPurgeInterval() {
        return overrides.getWalPurgeInterval() < 0 ? super.getWalPurgeInterval() : overrides.getWalPurgeInterval();
    }

    @Override
    public int getWalRecreateDistressedSequencerAttempts() {
        return overrides.getRecreateDistressedSequencerAttempts();
    }

    @Override
    public long getWalSegmentRolloverRowCount() {
        return overrides.getWalSegmentRolloverRowCount() < 0 ? super.getWalSegmentRolloverRowCount() : overrides.getWalSegmentRolloverRowCount();
    }

    @Override
    public int getWalTxnNotificationQueueCapacity() {
        return overrides.getWalTxnNotificationQueueCapacity() > 0 ? overrides.getWalTxnNotificationQueueCapacity() : 256;
    }

    @Override
    public long getWriterAsyncCommandBusyWaitTimeout() {
        return overrides.getWriterAsyncCommandBusyWaitTimeout() < 0 ? super.getWriterAsyncCommandBusyWaitTimeout() : overrides.getWriterAsyncCommandBusyWaitTimeout();
    }

    @Override
    public long getWriterAsyncCommandMaxTimeout() {
        return overrides.getWriterAsyncCommandMaxTimeout() < 0 ? super.getWriterAsyncCommandMaxTimeout() : overrides.getWriterAsyncCommandMaxTimeout();
    }

    @Override
    public int getWriterCommandQueueCapacity() {
        return overrides.getWriterCommandQueueCapacity();
    }

    @Override
    public long getWriterCommandQueueSlotSize() {
        return overrides.getWriterCommandQueueSlotSize();
    }

    @Override
    public boolean isIOURingEnabled() {
        return overrides.isIoURingEnabled() != null ? overrides.isIoURingEnabled() : super.isIOURingEnabled();
    }

    @Override
    public boolean isO3QuickSortEnabled() {
        return overrides.isO3QuickSortEnabled();
    }

    @Override
    public boolean isSnapshotRecoveryEnabled() {
        return overrides.getSnapshotRecoveryEnabled() == null ? super.isSnapshotRecoveryEnabled() : overrides.getSnapshotRecoveryEnabled();
    }

    @Override
    public boolean isSqlParallelFilterEnabled() {
        return overrides.isParallelFilterEnabled() != null ? overrides.isParallelFilterEnabled() : super.isSqlParallelFilterEnabled();
    }

    @Override
    public boolean isSqlParallelFilterPreTouchEnabled() {
        return overrides.isColumnPreTouchEnabled() != null ? overrides.isColumnPreTouchEnabled() : super.isSqlParallelFilterPreTouchEnabled();
    }

    @Override
    public boolean isWalSupported() {
        return true;
    }

    @Override
    public boolean mangleTableDirNames() {
        return overrides.mangleTableDirNames();
    }
}
