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

package io.questdb.test.cairo;

import io.questdb.FactoryProvider;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.SqlWalMode;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

import java.util.Map;

public class Overrides implements ConfigurationOverrides {
    private String attachableDirSuffix = null;
    private CharSequence backupDir;
    private DateFormat backupDirTimestampFormat;
    private int binaryEncodingMaxLength = -1;
    private int capacity = -1;
    private SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration;
    private Boolean columnPreTouchEnabled = null;
    private long columnPurgeRetryDelay = -1;
    private int columnVersionPurgeQueueCapacity = -1;
    private int columnVersionTaskPoolCapacity = -1;
    private Boolean copyPartitionOnAttach = null;
    private long currentMicros = -1;
    private final MicrosecondClock defaultMicrosecondClock = () -> currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
    private MicrosecondClock testMicrosClock = defaultMicrosecondClock;
    private long dataAppendPageSize = -1;
    private CharSequence defaultMapType;
    private int defaultTableWriteMode = SqlWalMode.WAL_NOT_SET;
    private Map<String, String> env = null;
    private FactoryProvider factoryProvider = null;
    private FilesFacade ff;
    private int groupByShardingThreshold = -1;
    private boolean hideTelemetryTable = false;
    private String inputRoot = null;
    private String inputWorkRoot = null;
    private Boolean ioURingEnabled = null;
    private int jitMode = SqlJitMode.JIT_MODE_ENABLED;
    private boolean mangleTableDirNames = true;
    private int maxFileNameLength = -1;
    private int maxOpenPartitions = -1;
    private int maxUncommittedRows = -1;
    private int o3ColumnMemorySize = -1;
    private long o3MaxLag = -1;
    private long o3MinLag = -1;
    private int o3PartitionSplitMaxCount = -1;
    private boolean o3QuickSortEnabled = false;
    private int pageFrameMaxRows = -1;
    private int pageFrameReduceQueueCapacity = -1;
    private int pageFrameReduceShardCount = -1;
    private Boolean parallelFilterEnabled = null;
    private Boolean parallelGroupByEnabled = null;
    private int parallelImportStatusLogKeepNDays = -1;
    private long partitionO3SplitThreshold;
    private int recreateDistressedSequencerAttempts = 3;
    private int repeatMigrationsFromVersion = -1;
    private int rndFunctionMemoryMaxPages = -1;
    private int rndFunctionMemoryPageSize = -1;
    private RostiAllocFacade rostiAllocFacade = null;
    private int sampleByIndexSearchPageSize;
    private boolean simulateCrashEnabled;
    private String snapshotInstanceId = null;
    private Boolean snapshotRecoveryEnabled = null;
    private long spinLockTimeout = -1;
    private int sqlCopyBufferSize = 1024 * 1024;
    private int sqlJoinMetadataMaxResizes = -1;
    private int sqlJoinMetadataPageSize = -1;
    private int sqlWindowMaxRecursion;
    private int sqlWindowStoreMaxPages;
    private int sqlWindowStorePageSize;
    private int tableRegistryCompactionThreshold;
    private long walApplyTableTimeQuota = -1;
    private int walLookAheadTransactionCount = -1;
    private long walMaxLagSize = -1;
    private int walMaxLagTxnCount = -1;
    private int walMaxSegmentFileDescriptorsCache = -1;
    private long walPurgeInterval = -1;
    private long walSegmentRolloverRowCount = -1;
    private long walSegmentRolloverSize = -1;
    private int walTxnNotificationQueueCapacity = -1;
    private long writerAsyncCommandBusyWaitTimeout = -1;
    private long writerAsyncCommandMaxTimeout = -1;
    private int writerCommandQueueCapacity = 4;
    private long writerCommandQueueSlotSize = 2048L;
    private Boolean writerMixedIOEnabled = null;

    @Override
    public String getAttachableDirSuffix() {
        return attachableDirSuffix;
    }

    @Override
    public CharSequence getBackupDir() {
        return backupDir;
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return backupDirTimestampFormat;
    }

    @Override
    public int getBinaryEncodingMaxLength() {
        return binaryEncodingMaxLength;
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return circuitBreakerConfiguration;
    }

    @Override
    public long getColumnPurgeRetryDelay() {
        return columnPurgeRetryDelay;
    }

    @Override
    public int getColumnVersionPurgeQueueCapacity() {
        return columnVersionPurgeQueueCapacity;
    }

    @Override
    public int getColumnVersionTaskPoolCapacity() {
        return columnVersionTaskPoolCapacity;
    }

    @Override
    public Boolean getCopyPartitionOnAttach() {
        return copyPartitionOnAttach;
    }

    @Override
    public long getCurrentMicros() {
        return currentMicros;
    }

    @Override
    public long getDataAppendPageSize() {
        return dataAppendPageSize;
    }

    @Override
    public CharSequence getDefaultMapType() {
        return defaultMapType;
    }

    @Override
    public int getDefaultTableWriteMode() {
        return defaultTableWriteMode;
    }

    @Override
    public Map<String, String> getEnv() {
        return env != null ? env : System.getenv();
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return factoryProvider;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public int getGroupByShardingThreshold() {
        return groupByShardingThreshold;
    }

    @Override
    public int getInactiveReaderMaxOpenPartitions() {
        return maxOpenPartitions;
    }

    @Override
    public String getInputRoot() {
        return inputRoot;
    }

    @Override
    public String getInputWorkRoot() {
        return inputWorkRoot;
    }

    @Override
    public int getJitMode() {
        return jitMode;
    }

    @Override
    public int getMaxFileNameLength() {
        return maxFileNameLength;
    }

    @Override
    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    @Override
    public int getO3ColumnMemorySize() {
        return o3ColumnMemorySize;
    }

    @Override
    public long getO3MaxLag() {
        return o3MaxLag;
    }

    @Override
    public long getO3MinLag() {
        return o3MinLag;
    }

    @Override
    public int getO3PartitionSplitMaxCount() {
        return o3PartitionSplitMaxCount;
    }

    @Override
    public int getPageFrameMaxRows() {
        return pageFrameMaxRows;
    }

    @Override
    public int getPageFrameReduceQueueCapacity() {
        return pageFrameReduceQueueCapacity;
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return pageFrameReduceShardCount;
    }

    @Override
    public int getParallelImportStatusLogKeepNDays() {
        return parallelImportStatusLogKeepNDays;
    }

    @Override
    public long getPartitionO3SplitThreshold() {
        return partitionO3SplitThreshold;
    }

    @Override
    public int getRecreateDistressedSequencerAttempts() {
        return recreateDistressedSequencerAttempts;
    }

    @Override
    public int getRepeatMigrationsFromVersion() {
        return repeatMigrationsFromVersion;
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
    public RostiAllocFacade getRostiAllocFacade() {
        return rostiAllocFacade;
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
    public String getSnapshotInstanceId() {
        return snapshotInstanceId;
    }

    @Override
    public Boolean getSnapshotRecoveryEnabled() {
        return snapshotRecoveryEnabled;
    }

    @Override
    public long getSpinLockTimeout() {
        return spinLockTimeout;
    }

    @Override
    public int getSqlCopyBufferSize() {
        return sqlCopyBufferSize;
    }

    @Override
    public int getSqlJoinMetadataMaxResizes() {
        return sqlJoinMetadataMaxResizes;
    }

    @Override
    public int getSqlJoinMetadataPageSize() {
        return sqlJoinMetadataPageSize;
    }

    public int getSqlWindowMaxRecursion() {
        return sqlWindowMaxRecursion;
    }

    public int getSqlWindowStoreMaxPages() {
        return sqlWindowStoreMaxPages;
    }

    public int getSqlWindowStorePageSize() {
        return sqlWindowStorePageSize;
    }

    @Override
    public int getTableRegistryCompactionThreshold() {
        return tableRegistryCompactionThreshold;
    }

    @Override
    public MicrosecondClock getTestMicrosClock() {
        return testMicrosClock;
    }

    @Override
    public int getWalApplyLookAheadTransactionCount() {
        return walLookAheadTransactionCount;
    }

    @Override
    public long getWalApplyTableTimeQuota() {
        return walApplyTableTimeQuota;
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
    public long getWriterAsyncCommandBusyWaitTimeout() {
        return writerAsyncCommandBusyWaitTimeout;
    }

    @Override
    public long getWriterAsyncCommandMaxTimeout() {
        return writerAsyncCommandMaxTimeout;
    }

    @Override
    public int getWriterCommandQueueCapacity() {
        return writerCommandQueueCapacity;
    }

    @Override
    public long getWriterCommandQueueSlotSize() {
        return writerCommandQueueSlotSize;
    }

    @Override
    public Boolean isColumnPreTouchEnabled() {
        return columnPreTouchEnabled;
    }

    @Override
    public boolean isHidingTelemetryTable() {
        return hideTelemetryTable;
    }

    @Override
    public Boolean isIoURingEnabled() {
        return ioURingEnabled;
    }

    @Override
    public boolean isO3QuickSortEnabled() {
        return o3QuickSortEnabled;
    }

    @Override
    public Boolean isParallelFilterEnabled() {
        return parallelFilterEnabled;
    }

    @Override
    public Boolean isParallelGroupByEnabled() {
        return parallelGroupByEnabled;
    }

    @Override
    public Boolean isWriterMixedIOEnabled() {
        return writerMixedIOEnabled;
    }

    @Override
    public boolean mangleTableDirNames() {
        return mangleTableDirNames;
    }

    @Override
    public void reset() {
        hideTelemetryTable = false;
        maxUncommittedRows = -1;
        o3MaxLag = -1;
        o3MinLag = -1;
        currentMicros = -1;
        testMicrosClock = defaultMicrosecondClock;
        sampleByIndexSearchPageSize = -1;
        defaultMapType = null;
        writerAsyncCommandBusyWaitTimeout = -1;
        writerAsyncCommandMaxTimeout = -1;
        pageFrameMaxRows = -1;
        groupByShardingThreshold = -1;
        jitMode = SqlJitMode.JIT_MODE_ENABLED;
        rndFunctionMemoryPageSize = -1;
        rndFunctionMemoryMaxPages = -1;
        spinLockTimeout = -1;
        snapshotInstanceId = null;
        snapshotRecoveryEnabled = null;
        parallelFilterEnabled = null;
        parallelGroupByEnabled = null;
        writerMixedIOEnabled = null;
        columnPreTouchEnabled = null;
        writerCommandQueueCapacity = 4;
        pageFrameReduceShardCount = -1;
        pageFrameReduceQueueCapacity = -1;
        columnVersionPurgeQueueCapacity = -1;
        columnVersionTaskPoolCapacity = -1;
        rostiAllocFacade = null;
        sqlCopyBufferSize = 1024 * 1024;
        sqlJoinMetadataPageSize = -1;
        sqlJoinMetadataMaxResizes = -1;
        ioURingEnabled = null;
        parallelImportStatusLogKeepNDays = -1;
        defaultTableWriteMode = SqlWalMode.WAL_NOT_SET;
        copyPartitionOnAttach = null;
        attachableDirSuffix = null;
        ff = null;
        dataAppendPageSize = -1;
        o3QuickSortEnabled = false;
        walSegmentRolloverRowCount = -1;
        mangleTableDirNames = true;
        walPurgeInterval = -1;
        tableRegistryCompactionThreshold = -1;
        maxOpenPartitions = -1;
        walApplyTableTimeQuota = -1;
        walLookAheadTransactionCount = -1;
        walMaxLagTxnCount = -1;
        repeatMigrationsFromVersion = -1;
        factoryProvider = null;
        simulateCrashEnabled = false;
        env = null;
        walMaxLagSize = -1;
    }

    @Override
    public void setAttachableDirSuffix(String attachableDirSuffix) {
        this.attachableDirSuffix = attachableDirSuffix;
    }

    @Override
    public void setBackupDir(CharSequence backupDir) {
        this.backupDir = backupDir;
    }

    @Override
    public void setBackupDirTimestampFormat(DateFormat backupDirTimestampFormat) {
        this.backupDirTimestampFormat = backupDirTimestampFormat;
    }

    @Override
    public void setBinaryEncodingMaxLength(int binaryEncodingMaxLength) {
        this.binaryEncodingMaxLength = binaryEncodingMaxLength;
    }

    @Override
    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public void setCircuitBreakerConfiguration(SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.circuitBreakerConfiguration = circuitBreakerConfiguration;
    }

    @Override
    public void setColumnPreTouchEnabled(Boolean columnPreTouchEnabled) {
        this.columnPreTouchEnabled = columnPreTouchEnabled;
    }

    @Override
    public void setColumnPurgeRetryDelay(long columnPurgeRetryDelay) {
        this.columnPurgeRetryDelay = columnPurgeRetryDelay;
    }

    @Override
    public void setColumnVersionPurgeQueueCapacity(int columnVersionPurgeQueueCapacity) {
        this.columnVersionPurgeQueueCapacity = columnVersionPurgeQueueCapacity;
    }

    @Override
    public void setColumnVersionTaskPoolCapacity(int columnVersionTaskPoolCapacity) {
        this.columnVersionTaskPoolCapacity = columnVersionTaskPoolCapacity;
    }

    @Override
    public void setCopyPartitionOnAttach(Boolean copyPartitionOnAttach) {
        this.copyPartitionOnAttach = copyPartitionOnAttach;
    }

    @Override
    public void setCurrentMicros(long currentMicros) {
        this.currentMicros = currentMicros;
    }

    @Override
    public void setDataAppendPageSize(long dataAppendPageSize) {
        this.dataAppendPageSize = dataAppendPageSize;
    }

    @Override
    public void setDefaultMapType(CharSequence defaultMapType) {
        this.defaultMapType = defaultMapType;
    }

    @Override
    public void setDefaultTableWriteMode(int defaultTableWriteMode) {
        this.defaultTableWriteMode = defaultTableWriteMode;
    }

    public void setEnv(Map<String, String> env) {
        this.env = env;
    }

    @Override
    public void setFactoryProvider(FactoryProvider factoryProvider) {
        this.factoryProvider = factoryProvider;
    }

    @Override
    public void setFilesFacade(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void setGroupByShardingThreshold(int groupByShardingThreshold) {
        this.groupByShardingThreshold = groupByShardingThreshold;
    }

    @Override
    public void setHideTelemetryTable(boolean hideTelemetryTable) {
        this.hideTelemetryTable = hideTelemetryTable;
    }

    @Override
    public void setInactiveReaderMaxOpenPartitions(int maxOpenPartitions) {
        this.maxOpenPartitions = maxOpenPartitions;
    }

    @Override
    public void setInputRoot(String inputRoot) {
        this.inputRoot = inputRoot;
    }

    @Override
    public void setInputWorkRoot(String inputWorkRoot) {
        this.inputWorkRoot = inputWorkRoot;
    }

    @Override
    public void setIoURingEnabled(Boolean ioURingEnabled) {
        this.ioURingEnabled = ioURingEnabled;
    }

    @Override
    public void setJitMode(int jitMode) {
        this.jitMode = jitMode;
    }

    @Override
    public void setMangleTableDirNames(boolean mangle) {
        this.mangleTableDirNames = mangle;
    }

    @Override
    public void setMaxFileNameLength(int maxFileNameLength) {
        this.maxFileNameLength = maxFileNameLength;
    }

    @Override
    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    @Override
    public void setO3ColumnMemorySize(int size) {
        o3ColumnMemorySize = size;
    }

    @Override
    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    @Override
    public void setO3MinLag(long minLag) {
        o3MinLag = minLag;
    }

    @Override
    public void setO3PartitionSplitMaxCount(int o3PartitionSplitMaxCount) {
        this.o3PartitionSplitMaxCount = o3PartitionSplitMaxCount;
    }

    @Override
    public void setO3QuickSortEnabled(boolean o3QuickSortEnabled) {
        this.o3QuickSortEnabled = o3QuickSortEnabled;
    }

    @Override
    public void setPageFrameMaxRows(int pageFrameMaxRows) {
        this.pageFrameMaxRows = pageFrameMaxRows;
    }

    @Override
    public void setPageFrameReduceQueueCapacity(int pageFrameReduceQueueCapacity) {
        this.pageFrameReduceQueueCapacity = pageFrameReduceQueueCapacity;
    }

    @Override
    public void setPageFrameReduceShardCount(int pageFrameReduceShardCount) {
        this.pageFrameReduceShardCount = pageFrameReduceShardCount;
    }

    @Override
    public void setParallelFilterEnabled(Boolean parallelFilterEnabled) {
        this.parallelFilterEnabled = parallelFilterEnabled;
    }

    @Override
    public void setParallelGroupByEnabled(Boolean parallelGroupByEnabled) {
        this.parallelGroupByEnabled = parallelGroupByEnabled;
    }

    @Override
    public void setParallelImportStatusLogKeepNDays(int parallelImportStatusLogKeepNDays) {
        this.parallelImportStatusLogKeepNDays = parallelImportStatusLogKeepNDays;
    }

    @Override
    public void setPartitionO3SplitThreshold(long value) {
        this.partitionO3SplitThreshold = value;
    }

    @Override
    public void setRecreateDistressedSequencerAttempts(int recreateDistressedSequencerAttempts) {
        this.recreateDistressedSequencerAttempts = recreateDistressedSequencerAttempts;
    }

    @Override
    public void setRegistryCompactionThreshold(int value) {
        tableRegistryCompactionThreshold = value;
    }

    @Override
    public void setRepeatMigrationsFromVersion(int value) {
        repeatMigrationsFromVersion = value;
    }

    @Override
    public void setRndFunctionMemoryMaxPages(int rndFunctionMemoryMaxPages) {
        this.rndFunctionMemoryMaxPages = rndFunctionMemoryMaxPages;
    }

    @Override
    public void setRndFunctionMemoryPageSize(int rndFunctionMemoryPageSize) {
        this.rndFunctionMemoryPageSize = rndFunctionMemoryPageSize;
    }

    @Override
    public void setRostiAllocFacade(RostiAllocFacade rostiAllocFacade) {
        this.rostiAllocFacade = rostiAllocFacade;
    }

    @Override
    public void setSampleByIndexSearchPageSize(int sampleByIndexSearchPageSize) {
        this.sampleByIndexSearchPageSize = sampleByIndexSearchPageSize;
    }

    @Override
    public void setSimulateCrashEnabled(boolean enabled) {
        this.simulateCrashEnabled = enabled;
    }

    @Override
    public void setSnapshotInstanceId(String snapshotInstanceId) {
        this.snapshotInstanceId = snapshotInstanceId;
    }

    @Override
    public void setSnapshotRecoveryEnabled(Boolean snapshotRecoveryEnabled) {
        this.snapshotRecoveryEnabled = snapshotRecoveryEnabled;
    }

    @Override
    public void setSpinLockTimeout(long spinLockTimeout) {
        this.spinLockTimeout = spinLockTimeout;
    }

    @Override
    public void setSqlCopyBufferSize(int sqlCopyBufferSize) {
        this.sqlCopyBufferSize = sqlCopyBufferSize;
    }

    @Override
    public void setSqlJoinMetadataMaxResizes(int sqlJoinMetadataMaxResizes) {
        this.sqlJoinMetadataMaxResizes = sqlJoinMetadataMaxResizes;
    }

    @Override
    public void setSqlJoinMetadataPageSize(int sqlJoinMetadataPageSize) {
        this.sqlJoinMetadataPageSize = sqlJoinMetadataPageSize;
    }

    @Override
    public void setSqlWindowMaxRecursion(int maxRecursion) {
        this.sqlWindowMaxRecursion = maxRecursion;
    }

    @Override
    public void setSqlWindowStoreMaxPages(int windowStoreMaxPages) {
        this.sqlWindowStoreMaxPages = windowStoreMaxPages;
    }

    @Override
    public void setSqlWindowStorePageSize(int windowStorePageSize) {
        this.sqlWindowStorePageSize = windowStorePageSize;
    }

    @Override
    public void setTestMicrosClock(MicrosecondClock testMicrosClock) {
        this.testMicrosClock = testMicrosClock;
    }

    @Override
    public void setWalApplyTableTimeQuota(long walApplyTableTimeQuota) {
        this.walApplyTableTimeQuota = walApplyTableTimeQuota;
    }

    @Override
    public void setWalLookAheadTransactionCount(int walLookAheadTransactionCount) {
        this.walLookAheadTransactionCount = walLookAheadTransactionCount;
    }

    @Override
    public void setWalMaxLagSize(long value) {
        walMaxLagSize = value;
    }

    public void setWalMaxLagTxnCount(int walMaxLagTxnCount) {
        this.walMaxLagTxnCount = walMaxLagTxnCount;
    }

    @Override
    public void setWalMaxSegmentFileDescriptorsCache(int value) {
        walMaxSegmentFileDescriptorsCache = value;
    }

    @Override
    public void setWalPurgeInterval(long walPurgeInterval) {
        this.walPurgeInterval = walPurgeInterval;
    }

    @Override
    public void setWalSegmentRolloverRowCount(long walSegmentRolloverRowCount) {
        this.walSegmentRolloverRowCount = walSegmentRolloverRowCount;
    }

    @Override
    public void setWalSegmentRolloverSize(long walSegmentRolloverSize) {
        this.walSegmentRolloverSize = walSegmentRolloverSize;
    }

    @Override
    public void setWalTxnNotificationQueueCapacity(int walTxnNotificationQueueCapacity) {
        this.walTxnNotificationQueueCapacity = walTxnNotificationQueueCapacity;
    }

    @Override
    public void setWriterAsyncCommandBusyWaitTimeout(long writerAsyncCommandBusyWaitTimeout) {
        this.writerAsyncCommandBusyWaitTimeout = writerAsyncCommandBusyWaitTimeout;
    }

    @Override
    public void setWriterAsyncCommandMaxTimeout(long writerAsyncCommandMaxTimeout) {
        this.writerAsyncCommandMaxTimeout = writerAsyncCommandMaxTimeout;
    }

    @Override
    public void setWriterCommandQueueCapacity(int writerCommandQueueCapacity) {
        this.writerCommandQueueCapacity = writerCommandQueueCapacity;
    }

    @Override
    public void setWriterCommandQueueSlotSize(long writerCommandQueueSlotSize) {
        this.writerCommandQueueSlotSize = writerCommandQueueSlotSize;
    }

    @Override
    public void setWriterMixedIOEnabled(Boolean writerMixedIOEnabled) {
        this.writerMixedIOEnabled = writerMixedIOEnabled;
    }
}
