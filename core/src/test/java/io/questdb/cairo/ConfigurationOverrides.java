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

import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;

@SuppressWarnings("unused")
public interface ConfigurationOverrides {
    String getAttachableDirSuffix();

    CharSequence getBackupDir();

    DateFormat getBackupDirTimestampFormat();

    int getBinaryEncodingMaxLength();

    int getCapacity();

    SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration();

    long getColumnPurgeRetryDelay();

    int getColumnVersionPurgeQueueCapacity();

    int getColumnVersionTaskPoolCapacity();

    Boolean getCopyPartitionOnAttach();

    long getCurrentMicros();

    long getDataAppendPageSize();

    CharSequence getDefaultMapType();

    int getDefaultTableWriteMode();

    FilesFacade getFilesFacade();

    String getInputRoot();

    String getInputWorkRoot();

    int getJitMode();

    int getMaxFileNameLength();

    int getMaxUncommittedRows();

    int getO3ColumnMemorySize();

    long getO3MaxLag();

    long getO3MinLag();

    int getPageFrameMaxRows();

    int getPageFrameReduceQueueCapacity();

    int getPageFrameReduceShardCount();

    int getParallelImportStatusLogKeepNDays();

    int getQueryCacheEventQueueCapacity();

    int getRecreateDistressedSequencerAttempts();

    int getRndFunctionMemoryMaxPages();

    int getRndFunctionMemoryPageSize();

    RostiAllocFacade getRostiAllocFacade();

    int getSampleByIndexSearchPageSize();

    String getSnapshotInstanceId();

    Boolean getSnapshotRecoveryEnabled();

    long getSpinLockTimeout();

    int getSqlCopyBufferSize();

    int getSqlJoinMetadataMaxResizes();

    int getSqlJoinMetadataPageSize();

    int getTableRegistryCompactionThreshold();

    MicrosecondClock getTestMicrosClock();

    long getWalPurgeInterval();

    long getWalSegmentRolloverRowCount();

    int getWalTxnNotificationQueueCapacity();

    long getWriterAsyncCommandBusyWaitTimeout();

    long getWriterAsyncCommandMaxTimeout();

    int getWriterCommandQueueCapacity();

    long getWriterCommandQueueSlotSize();

    Boolean isColumnPreTouchEnabled();

    boolean isHidingTelemetryTable();

    Boolean isIoURingEnabled();

    boolean isO3QuickSortEnabled();

    Boolean isParallelFilterEnabled();

    boolean mangleTableDirNames();

    void reset();

    void setAttachableDirSuffix(String attachableDirSuffix);

    void setBackupDir(CharSequence backupDir);

    void setBackupDirTimestampFormat(DateFormat backupDirTimestampFormat);

    void setBinaryEncodingMaxLength(int binaryEncodingMaxLength);

    void setCapacity(int capacity);

    void setCircuitBreakerConfiguration(SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration);

    void setColumnPreTouchEnabled(Boolean columnPreTouchEnabled);

    void setColumnPurgeRetryDelay(long columnPurgeRetryDelay);

    void setColumnVersionPurgeQueueCapacity(int columnVersionPurgeQueueCapacity);

    void setColumnVersionTaskPoolCapacity(int columnVersionTaskPoolCapacity);

    void setCopyPartitionOnAttach(Boolean copyPartitionOnAttach);

    void setCurrentMicros(long currentMicros);

    void setDataAppendPageSize(long dataAppendPageSize);

    void setDefaultMapType(CharSequence defaultMapType);

    void setDefaultTableWriteMode(int defaultTableWriteMode);

    void setFilesFacade(FilesFacade ff);

    void setHideTelemetryTable(boolean hideTelemetryTable);

    void setInputRoot(String inputRoot);

    void setInputWorkRoot(String inputWorkRoot);

    void setIoURingEnabled(Boolean ioURingEnabled);

    void setJitMode(int jitMode);

    void setMangleTableDirNames(boolean mangle);

    void setMaxFileNameLength(int maxFileNameLength);

    void setMaxUncommittedRows(int configOverrideMaxUncommittedRows);

    void setO3ColumnMemorySize(int size);

    void setO3MaxLag(long configOverrideO3MaxLag);

    void setO3MinLag(long minLag);

    void setO3QuickSortEnabled(boolean o3QuickSortEnabled);

    void setPageFrameMaxRows(int pageFrameMaxRows);

    void setPageFrameReduceQueueCapacity(int pageFrameReduceQueueCapacity);

    void setPageFrameReduceShardCount(int pageFrameReduceShardCount);

    void setParallelFilterEnabled(Boolean parallelFilterEnabled);

    void setParallelImportStatusLogKeepNDays(int parallelImportStatusLogKeepNDays);

    void setQueryCacheEventQueueCapacity(int queryCacheEventQueueCapacity);

    void setRecreateDistressedSequencerAttempts(int recreateDistressedSequencerAttempts);

    void setRegistryCompactionThreshold(int value);

    void setRndFunctionMemoryMaxPages(int rndFunctionMemoryMaxPages);

    void setRndFunctionMemoryPageSize(int rndFunctionMemoryPageSize);

    void setRostiAllocFacade(RostiAllocFacade rostiAllocFacade);

    void setSampleByIndexSearchPageSize(int sampleByIndexSearchPageSize);

    void setSnapshotInstanceId(String snapshotInstanceId);

    void setSnapshotRecoveryEnabled(Boolean snapshotRecoveryEnabled);

    void setSpinLockTimeout(long spinLockTimeout);

    void setSqlCopyBufferSize(int sqlCopyBufferSize);

    void setSqlJoinMetadataMaxResizes(int sqlJoinMetadataMaxResizes);

    void setSqlJoinMetadataPageSize(int sqlJoinMetadataPageSize);

    void setTestMicrosClock(MicrosecondClock testMicrosClock);

    void setWalPurgeInterval(long walPurgeInterval);

    void setWalSegmentRolloverRowCount(long walSegmentRolloverRowCount);

    void setWalTxnNotificationQueueCapacity(int walTxnNotificationQueueCapacity);

    void setWriterAsyncCommandBusyWaitTimeout(long writerAsyncCommandBusyWaitTimeout);

    void setWriterAsyncCommandMaxTimeout(long writerAsyncCommandMaxTimeout);

    void setWriterCommandQueueCapacity(int writerCommandQueueCapacity);

    void setWriterCommandQueueSlotSize(long writerCommandQueueSlotSize);
}
