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

package io.questdb.cairo;

import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;

class StaticOverrides extends Overrides {

    @Override
    public String getAttachableDirSuffix() {
        return AbstractCairoTest.attachableDirSuffix;
    }

    @Override
    public CharSequence getBackupDir() {
        return AbstractCairoTest.backupDir;
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return AbstractCairoTest.backupDirTimestampFormat;
    }

    @Override
    public int getBinaryEncodingMaxLength() {
        return AbstractCairoTest.binaryEncodingMaxLength;
    }

    @Override
    public int getCapacity() {
        return AbstractCairoTest.capacity;
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return AbstractCairoTest.circuitBreakerConfiguration;
    }

    @Override
    public long getColumnPurgeRetryDelay() {
        return AbstractCairoTest.columnPurgeRetryDelay;
    }

    @Override
    public int getColumnVersionPurgeQueueCapacity() {
        return AbstractCairoTest.columnVersionPurgeQueueCapacity;
    }

    @Override
    public int getColumnVersionTaskPoolCapacity() {
        return AbstractCairoTest.columnVersionTaskPoolCapacity;
    }

    @Override
    public Boolean getCopyPartitionOnAttach() {
        return AbstractCairoTest.copyPartitionOnAttach;
    }

    @Override
    public long getCurrentMicros() {
        return AbstractCairoTest.currentMicros;
    }

    @Override
    public long getDataAppendPageSize() {
        return AbstractCairoTest.dataAppendPageSize;
    }

    @Override
    public CharSequence getDefaultMapType() {
        return AbstractCairoTest.defaultMapType;
    }

    @Override
    public int getDefaultTableWriteMode() {
        return AbstractCairoTest.defaultTableWriteMode;
    }

    @Override
    public Boolean getEnableColumnPreTouch() {
        return AbstractCairoTest.enableColumnPreTouch;
    }

    @Override
    public Boolean getEnableParallelFilter() {
        return AbstractCairoTest.enableParallelFilter;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return AbstractCairoTest.ff;
    }

    @Override
    public String getInputRoot() {
        return AbstractCairoTest.inputRoot;
    }

    @Override
    public String getInputWorkRoot() {
        return AbstractCairoTest.inputWorkRoot;
    }

    @Override
    public int getIsO3QuickSortEnabled() {
        return AbstractCairoTest.isO3QuickSortEnabled;
    }

    @Override
    public int getJitMode() {
        return AbstractCairoTest.jitMode;
    }

    @Override
    public int getPageFrameMaxRows() {
        return AbstractCairoTest.pageFrameMaxRows;
    }

    @Override
    public int getPageFrameReduceQueueCapacity() {
        return AbstractCairoTest.pageFrameReduceQueueCapacity;
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return AbstractCairoTest.pageFrameReduceShardCount;
    }

    @Override
    public int getParallelImportStatusLogKeepNDays() {
        return AbstractCairoTest.parallelImportStatusLogKeepNDays;
    }

    @Override
    public int getQueryCacheEventQueueCapacity() {
        return AbstractCairoTest.queryCacheEventQueueCapacity;
    }

    @Override
    public int getRecreateDistressedSequencerAttempts() {
        return AbstractCairoTest.recreateDistressedSequencerAttempts;
    }

    @Override
    public int getRndFunctionMemoryMaxPages() {
        return AbstractCairoTest.rndFunctionMemoryMaxPages;
    }

    @Override
    public int getRndFunctionMemoryPageSize() {
        return AbstractCairoTest.rndFunctionMemoryPageSize;
    }

    @Override
    public String getSnapshotInstanceId() {
        return AbstractCairoTest.snapshotInstanceId;
    }

    @Override
    public Boolean getSnapshotRecoveryEnabled() {
        return AbstractCairoTest.snapshotRecoveryEnabled;
    }

    @Override
    public long getSpinLockTimeout() {
        return AbstractCairoTest.spinLockTimeout;
    }

    @Override
    public int getSqlCopyBufferSize() {
        return AbstractCairoTest.sqlCopyBufferSize;
    }

    @Override
    public MicrosecondClock getTestMicrosClock() {
        return AbstractCairoTest.testMicrosClock;
    }

    @Override
    public long getWalSegmentRolloverRowCount() {
        return AbstractCairoTest.walSegmentRolloverRowCount;
    }

    @Override
    public int getWalTxnNotificationQueueCapacity() {
        return AbstractCairoTest.walTxnNotificationQueueCapacity;
    }

    @Override
    public long getWriterAsyncCommandBusyWaitTimeout() {
        return AbstractCairoTest.writerAsyncCommandBusyWaitTimeout;
    }

    @Override
    public long getWriterAsyncCommandMaxTimeout() {
        return AbstractCairoTest.writerAsyncCommandMaxTimeout;
    }

    @Override
    public int getWriterCommandQueueCapacity() {
        return AbstractCairoTest.writerCommandQueueCapacity;
    }

    @Override
    public long getWriterCommandQueueSlotSize() {
        return AbstractCairoTest.writerCommandQueueSlotSize;
    }

    @Override
    public Boolean isIoURingEnabled() {
        return AbstractCairoTest.ioURingEnabled;
    }

    @Override
    public void reset() {
        super.reset();

        AbstractCairoTest.currentMicros = -1;
        AbstractCairoTest.testMicrosClock = AbstractCairoTest.defaultMicrosecondClock;
        AbstractCairoTest.defaultMapType = null;
        AbstractCairoTest.writerAsyncCommandBusyWaitTimeout = -1;
        AbstractCairoTest.writerAsyncCommandMaxTimeout = -1;
        AbstractCairoTest.pageFrameMaxRows = -1;
        AbstractCairoTest.jitMode = SqlJitMode.JIT_MODE_ENABLED;
        AbstractCairoTest.rndFunctionMemoryPageSize = -1;
        AbstractCairoTest.rndFunctionMemoryMaxPages = -1;
        AbstractCairoTest.spinLockTimeout = -1;
        AbstractCairoTest.walTxnNotificationQueueCapacity = -1;
        AbstractCairoTest.snapshotInstanceId = null;
        AbstractCairoTest.snapshotRecoveryEnabled = null;
        AbstractCairoTest.enableParallelFilter = null;
        AbstractCairoTest.enableColumnPreTouch = null;
        AbstractCairoTest.writerCommandQueueCapacity = 4;
        AbstractCairoTest.queryCacheEventQueueCapacity = -1;
        AbstractCairoTest.pageFrameReduceShardCount = -1;
        AbstractCairoTest.pageFrameReduceQueueCapacity = -1;
        AbstractCairoTest.columnVersionPurgeQueueCapacity = -1;
        AbstractCairoTest.columnVersionTaskPoolCapacity = -1;
        AbstractCairoTest.sqlCopyBufferSize = 1024 * 1024;
        AbstractCairoTest.ioURingEnabled = null;
        AbstractCairoTest.parallelImportStatusLogKeepNDays = -1;
        AbstractCairoTest.defaultTableWriteMode = -1;
        AbstractCairoTest.copyPartitionOnAttach = null;
        AbstractCairoTest.attachableDirSuffix = null;
        AbstractCairoTest.ff = null;
        AbstractCairoTest.dataAppendPageSize = -1;
        AbstractCairoTest.isO3QuickSortEnabled = 0;
        AbstractCairoTest.walSegmentRolloverRowCount = -1;
    }

    @Override
    public void setAttachableDirSuffix(String attachableDirSuffix) {
        AbstractCairoTest.attachableDirSuffix = attachableDirSuffix;
    }

    @Override
    public void setBackupDir(CharSequence backupDir) {
        AbstractCairoTest.backupDir = backupDir;
    }

    @Override
    public void setBackupDirTimestampFormat(DateFormat backupDirTimestampFormat) {
        AbstractCairoTest.backupDirTimestampFormat = backupDirTimestampFormat;
    }

    @Override
    public void setBinaryEncodingMaxLength(int binaryEncodingMaxLength) {
        AbstractCairoTest.binaryEncodingMaxLength = binaryEncodingMaxLength;
    }

    @Override
    public void setCapacity(int capacity) {
        AbstractCairoTest.capacity = capacity;
    }

    @Override
    public void setCircuitBreakerConfiguration(SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration) {
        AbstractCairoTest.circuitBreakerConfiguration = circuitBreakerConfiguration;
    }

    @Override
    public void setColumnPurgeRetryDelay(long columnPurgeRetryDelay) {
        AbstractCairoTest.columnPurgeRetryDelay = columnPurgeRetryDelay;
    }

    @Override
    public void setColumnVersionPurgeQueueCapacity(int columnVersionPurgeQueueCapacity) {
        AbstractCairoTest.columnVersionPurgeQueueCapacity = columnVersionPurgeQueueCapacity;
    }

    @Override
    public void setColumnVersionTaskPoolCapacity(int columnVersionTaskPoolCapacity) {
        AbstractCairoTest.columnVersionTaskPoolCapacity = columnVersionTaskPoolCapacity;
    }

    @Override
    public void setCopyPartitionOnAttach(Boolean copyPartitionOnAttach) {
        AbstractCairoTest.copyPartitionOnAttach = copyPartitionOnAttach;
    }

    @Override
    public void setCurrentMicros(long currentMicros) {
        AbstractCairoTest.currentMicros = currentMicros;
    }

    @Override
    public void setDataAppendPageSize(long dataAppendPageSize) {
        AbstractCairoTest.dataAppendPageSize = dataAppendPageSize;
    }

    @Override
    public void setDefaultMapType(CharSequence defaultMapType) {
        AbstractCairoTest.defaultMapType = defaultMapType;
    }

    @Override
    public void setDefaultTableWriteMode(int defaultTableWriteMode) {
        AbstractCairoTest.defaultTableWriteMode = defaultTableWriteMode;
    }

    @Override
    public void setEnableColumnPreTouch(Boolean enableColumnPreTouch) {
        AbstractCairoTest.enableColumnPreTouch = enableColumnPreTouch;
    }

    @Override
    public void setEnableParallelFilter(Boolean enableParallelFilter) {
        AbstractCairoTest.enableParallelFilter = enableParallelFilter;
    }

    @Override
    public void setFilesFacade(FilesFacade ff) {
        AbstractCairoTest.ff = ff;
    }

    @Override
    public void setInputRoot(String inputRoot) {
        AbstractCairoTest.inputRoot = inputRoot;
    }

    @Override
    public void setInputWorkRoot(String inputWorkRoot) {
        AbstractCairoTest.inputWorkRoot = inputWorkRoot;
    }

    @Override
    public void setIoURingEnabled(Boolean ioURingEnabled) {
        AbstractCairoTest.ioURingEnabled = ioURingEnabled;
    }

    @Override
    public void setIsO3QuickSortEnabled(int isO3QuickSortEnabled) {
        AbstractCairoTest.isO3QuickSortEnabled = isO3QuickSortEnabled;
    }

    @Override
    public void setJitMode(int jitMode) {
        AbstractCairoTest.jitMode = jitMode;
    }

    @Override
    public void setPageFrameMaxRows(int pageFrameMaxRows) {
        AbstractCairoTest.pageFrameMaxRows = pageFrameMaxRows;
    }

    @Override
    public void setPageFrameReduceQueueCapacity(int pageFrameReduceQueueCapacity) {
        AbstractCairoTest.pageFrameReduceQueueCapacity = pageFrameReduceQueueCapacity;
    }

    @Override
    public void setPageFrameReduceShardCount(int pageFrameReduceShardCount) {
        AbstractCairoTest.pageFrameReduceShardCount = pageFrameReduceShardCount;
    }

    @Override
    public void setParallelImportStatusLogKeepNDays(int parallelImportStatusLogKeepNDays) {
        AbstractCairoTest.parallelImportStatusLogKeepNDays = parallelImportStatusLogKeepNDays;
    }

    @Override
    public void setQueryCacheEventQueueCapacity(int queryCacheEventQueueCapacity) {
        AbstractCairoTest.queryCacheEventQueueCapacity = queryCacheEventQueueCapacity;
    }

    @Override
    public void setRecreateDistressedSequencerAttempts(int recreateDistressedSequencerAttempts) {
        AbstractCairoTest.recreateDistressedSequencerAttempts = recreateDistressedSequencerAttempts;
    }

    @Override
    public void setRndFunctionMemoryMaxPages(int rndFunctionMemoryMaxPages) {
        AbstractCairoTest.rndFunctionMemoryMaxPages = rndFunctionMemoryMaxPages;
    }

    @Override
    public void setRndFunctionMemoryPageSize(int rndFunctionMemoryPageSize) {
        AbstractCairoTest.rndFunctionMemoryPageSize = rndFunctionMemoryPageSize;
    }

    @Override
    public void setSnapshotInstanceId(String snapshotInstanceId) {
        AbstractCairoTest.snapshotInstanceId = snapshotInstanceId;
    }

    @Override
    public void setSnapshotRecoveryEnabled(Boolean snapshotRecoveryEnabled) {
        AbstractCairoTest.snapshotRecoveryEnabled = snapshotRecoveryEnabled;
    }

    @Override
    public void setSpinLockTimeout(long spinLockTimeout) {
        AbstractCairoTest.spinLockTimeout = spinLockTimeout;
    }

    @Override
    public void setSqlCopyBufferSize(int sqlCopyBufferSize) {
        AbstractCairoTest.sqlCopyBufferSize = sqlCopyBufferSize;
    }

    @Override
    public void setTestMicrosClock(MicrosecondClock testMicrosClock) {
        AbstractCairoTest.testMicrosClock = testMicrosClock;
    }

    @Override
    public void setWalSegmentRolloverRowCount(long walSegmentRolloverRowCount) {
        AbstractCairoTest.walSegmentRolloverRowCount = walSegmentRolloverRowCount;
    }

    @Override
    public void setWalTxnNotificationQueueCapacity(int walTxnNotificationQueueCapacity) {
        AbstractCairoTest.walTxnNotificationQueueCapacity = walTxnNotificationQueueCapacity;
    }

    @Override
    public void setWriterAsyncCommandBusyWaitTimeout(long writerAsyncCommandBusyWaitTimeout) {
        AbstractCairoTest.writerAsyncCommandBusyWaitTimeout = writerAsyncCommandBusyWaitTimeout;
    }

    @Override
    public void setWriterAsyncCommandMaxTimeout(long writerAsyncCommandMaxTimeout) {
        AbstractCairoTest.writerAsyncCommandMaxTimeout = writerAsyncCommandMaxTimeout;
    }

    @Override
    public void setWriterCommandQueueCapacity(int writerCommandQueueCapacity) {
        AbstractCairoTest.writerCommandQueueCapacity = writerCommandQueueCapacity;
    }

    @Override
    public void setWriterCommandQueueSlotSize(long writerCommandQueueSlotSize) {
        AbstractCairoTest.writerCommandQueueSlotSize = writerCommandQueueSlotSize;
    }
}
