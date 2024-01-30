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
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.SqlWalMode;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.tools.TestUtils;

import java.util.Map;
import java.util.Properties;

public class Overrides {
    private final Properties properties = new Properties();
    private CharSequence backupDir;
    private DateFormat backupDirTimestampFormat;
    private int binaryEncodingMaxLength = -1;
    private int capacity = -1;
    private boolean changed = true;
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
    private int defaultTableWriteMode = SqlWalMode.WAL_NOT_SET;
    private Map<String, String> env = null;
    private FactoryProvider factoryProvider = null;
    private FilesFacade ff;
    private long groupByAllocatorDefaultChunkSize = -1;
    private int groupByShardingThreshold = -1;
    private boolean hideTelemetryTable = false;
    private String inputRoot = null;
    private String inputWorkRoot = null;
    private Boolean ioURingEnabled = null;
    private int jitMode = SqlJitMode.JIT_MODE_ENABLED;
    private boolean mangleTableDirNames = true;
    private int maxFileNameLength = -1;
    private int maxOpenPartitions = -1;
    private int maxSqlRecompileAttempts = -1;
    private int maxUncommittedRows = -1;
    private int o3ColumnMemorySize = -1;
    private long o3MaxLag = -1;
    private long o3MinLag = -1;
    private int o3PartitionSplitMaxCount = -1;
    private boolean o3QuickSortEnabled = false;
    private Boolean parallelFilterEnabled = null;
    private Boolean parallelGroupByEnabled = null;
    private int parallelImportStatusLogKeepNDays = -1;
    private long partitionO3SplitThreshold;
    private CairoConfiguration propsConfig;
    private int repeatMigrationsFromVersion = -1;
    private int rndFunctionMemoryMaxPages = -1;
    private int rndFunctionMemoryPageSize = -1;
    private RostiAllocFacade rostiAllocFacade = null;
    private int sampleByIndexSearchPageSize;
    private boolean simulateCrashEnabled;
    private long spinLockTimeout = -1;
    private int sqlCopyBufferSize = 1024 * 1024;
    private int sqlJoinMetadataMaxResizes = -1;
    private int sqlJoinMetadataPageSize = -1;
    private int sqlWindowMaxRecursion;
    private int sqlWindowStoreMaxPages;
    private int sqlWindowStorePageSize;
    private int tableRegistryCompactionThreshold;
    private int walLookAheadTransactionCount = -1;
    private long walMaxLagSize = -1;
    private int walMaxLagTxnCount = -1;
    private int walMaxSegmentFileDescriptorsCache = -1;
    private long walPurgeInterval = -1;
    private long walSegmentRolloverRowCount = -1;
    private long walSegmentRolloverSize = -1;
    private int walTxnNotificationQueueCapacity = -1;
    private long writerAsyncCommandMaxTimeout = -1;
    private int writerCommandQueueCapacity = 4;
    private long writerCommandQueueSlotSize = 2048L;
    private Boolean writerMixedIOEnabled = null;

    public Overrides() {
        TestUtils.resetToDefaultTestProperties(properties);
    }



    public CharSequence getBackupDir() {
        return backupDir;
    }


    public DateFormat getBackupDirTimestampFormat() {
        return backupDirTimestampFormat;
    }


    public int getBinaryEncodingMaxLength() {
        return binaryEncodingMaxLength;
    }


    public int getCapacity() {
        return capacity;
    }


    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return circuitBreakerConfiguration;
    }


    public long getColumnPurgeRetryDelay() {
        return columnPurgeRetryDelay;
    }


    public int getColumnVersionPurgeQueueCapacity() {
        return columnVersionPurgeQueueCapacity;
    }


    public int getColumnVersionTaskPoolCapacity() {
        return columnVersionTaskPoolCapacity;
    }

    public CairoConfiguration getConfiguration(String root) {
        if (changed) {
            this.propsConfig = io.questdb.test.tools.TestUtils.getTestConfiguration(root, properties);
            changed = false;
        }
        return this.propsConfig;
    }


    public Boolean getCopyPartitionOnAttach() {
        return copyPartitionOnAttach;
    }


    public long getCurrentMicros() {
        return currentMicros;
    }


    public long getDataAppendPageSize() {
        return dataAppendPageSize;
    }


    public int getDefaultTableWriteMode() {
        return defaultTableWriteMode;
    }


    public Map<String, String> getEnv() {
        return env != null ? env : System.getenv();
    }


    public FactoryProvider getFactoryProvider() {
        return factoryProvider;
    }


    public FilesFacade getFilesFacade() {
        return ff;
    }


    public long getGroupByAllocatorDefaultChunkSize() {
        return groupByAllocatorDefaultChunkSize;
    }


    public int getGroupByShardingThreshold() {
        return groupByShardingThreshold;
    }


    public int getInactiveReaderMaxOpenPartitions() {
        return maxOpenPartitions;
    }


    public String getInputRoot() {
        return inputRoot;
    }


    public String getInputWorkRoot() {
        return inputWorkRoot;
    }


    public int getJitMode() {
        return jitMode;
    }


    public int getMaxFileNameLength() {
        return maxFileNameLength;
    }


    public int getMaxSqlRecompileAttempts() {
        return maxSqlRecompileAttempts;
    }


    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public int getO3ColumnMemorySize() {
        return o3ColumnMemorySize;
    }

    public long getO3MaxLag() {
        return o3MaxLag;
    }


    public long getO3MinLag() {
        return o3MinLag;
    }


    public int getO3PartitionSplitMaxCount() {
        return o3PartitionSplitMaxCount;
    }


    public int getParallelImportStatusLogKeepNDays() {
        return parallelImportStatusLogKeepNDays;
    }


    public long getPartitionO3SplitThreshold() {
        return partitionO3SplitThreshold;
    }


    public int getRepeatMigrationsFromVersion() {
        return repeatMigrationsFromVersion;
    }


    public int getRndFunctionMemoryMaxPages() {
        return rndFunctionMemoryMaxPages;
    }


    public int getRndFunctionMemoryPageSize() {
        return rndFunctionMemoryPageSize;
    }


    public RostiAllocFacade getRostiAllocFacade() {
        return rostiAllocFacade;
    }


    public int getSampleByIndexSearchPageSize() {
        return sampleByIndexSearchPageSize;
    }


    public boolean getSimulateCrashEnabled() {
        return simulateCrashEnabled;
    }


    public long getSpinLockTimeout() {
        return spinLockTimeout;
    }


    public int getSqlCopyBufferSize() {
        return sqlCopyBufferSize;
    }


    public int getSqlJoinMetadataMaxResizes() {
        return sqlJoinMetadataMaxResizes;
    }


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


    public int getTableRegistryCompactionThreshold() {
        return tableRegistryCompactionThreshold;
    }


    public MicrosecondClock getTestMicrosClock() {
        return testMicrosClock;
    }


    public int getWalApplyLookAheadTransactionCount() {
        return walLookAheadTransactionCount;
    }


    public long getWalMaxLagSize() {
        return walMaxLagSize;
    }


    public int getWalMaxLagTxnCount() {
        return walMaxLagTxnCount;
    }


    public int getWalMaxSegmentFileDescriptorsCache() {
        return walMaxSegmentFileDescriptorsCache;
    }


    public long getWalPurgeInterval() {
        return walPurgeInterval;
    }


    public long getWalSegmentRolloverRowCount() {
        return walSegmentRolloverRowCount;
    }


    public long getWalSegmentRolloverSize() {
        return walSegmentRolloverSize;
    }


    public int getWalTxnNotificationQueueCapacity() {
        return walTxnNotificationQueueCapacity;
    }


    public long getWriterAsyncCommandMaxTimeout() {
        return writerAsyncCommandMaxTimeout;
    }


    public int getWriterCommandQueueCapacity() {
        return writerCommandQueueCapacity;
    }


    public long getWriterCommandQueueSlotSize() {
        return writerCommandQueueSlotSize;
    }


    public Boolean isColumnPreTouchEnabled() {
        return columnPreTouchEnabled;
    }


    public boolean isHidingTelemetryTable() {
        return hideTelemetryTable;
    }


    public Boolean isIoURingEnabled() {
        return ioURingEnabled;
    }


    public boolean isO3QuickSortEnabled() {
        return o3QuickSortEnabled;
    }


    public Boolean isParallelFilterEnabled() {
        return parallelFilterEnabled;
    }


    public Boolean isParallelGroupByEnabled() {
        return parallelGroupByEnabled;
    }


    public Boolean isWriterMixedIOEnabled() {
        return writerMixedIOEnabled;
    }


    public boolean mangleTableDirNames() {
        return mangleTableDirNames;
    }


    public void reset() {
        o3ColumnMemorySize = -1;
        hideTelemetryTable = false;
        maxUncommittedRows = -1;
        o3MaxLag = -1;
        o3MinLag = -1;
        currentMicros = -1;
        testMicrosClock = defaultMicrosecondClock;
        sampleByIndexSearchPageSize = -1;
        writerAsyncCommandMaxTimeout = -1;
        groupByShardingThreshold = -1;
        jitMode = SqlJitMode.JIT_MODE_ENABLED;
        rndFunctionMemoryPageSize = -1;
        rndFunctionMemoryMaxPages = -1;
        spinLockTimeout = -1;
        parallelFilterEnabled = null;
        parallelGroupByEnabled = null;
        writerMixedIOEnabled = null;
        columnPreTouchEnabled = null;
        writerCommandQueueCapacity = 4;
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
        ff = null;
        dataAppendPageSize = -1;
        o3QuickSortEnabled = false;
        walSegmentRolloverRowCount = -1;
        mangleTableDirNames = true;
        walPurgeInterval = -1;
        tableRegistryCompactionThreshold = -1;
        maxOpenPartitions = -1;
        walLookAheadTransactionCount = -1;
        walMaxLagTxnCount = -1;
        repeatMigrationsFromVersion = -1;
        factoryProvider = null;
        simulateCrashEnabled = false;
        env = null;
        walMaxLagSize = -1;
        groupByAllocatorDefaultChunkSize = -1;
        maxSqlRecompileAttempts = -1;

        TestUtils.resetToDefaultTestProperties(properties);
        changed = true;
    }


    public void setBackupDir(CharSequence backupDir) {
        this.backupDir = backupDir;
    }


    public void setBackupDirTimestampFormat(DateFormat backupDirTimestampFormat) {
        this.backupDirTimestampFormat = backupDirTimestampFormat;
    }


    public void setBinaryEncodingMaxLength(int binaryEncodingMaxLength) {
        this.binaryEncodingMaxLength = binaryEncodingMaxLength;
    }


    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }


    public void setCircuitBreakerConfiguration(SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.circuitBreakerConfiguration = circuitBreakerConfiguration;
    }


    public void setColumnPreTouchEnabled(Boolean columnPreTouchEnabled) {
        this.columnPreTouchEnabled = columnPreTouchEnabled;
    }


    public void setColumnPurgeRetryDelay(long columnPurgeRetryDelay) {
        this.columnPurgeRetryDelay = columnPurgeRetryDelay;
    }


    public void setColumnVersionPurgeQueueCapacity(int columnVersionPurgeQueueCapacity) {
        this.columnVersionPurgeQueueCapacity = columnVersionPurgeQueueCapacity;
    }


    public void setColumnVersionTaskPoolCapacity(int columnVersionTaskPoolCapacity) {
        this.columnVersionTaskPoolCapacity = columnVersionTaskPoolCapacity;
    }


    public void setCopyPartitionOnAttach(Boolean copyPartitionOnAttach) {
        this.copyPartitionOnAttach = copyPartitionOnAttach;
    }


    public void setCurrentMicros(long currentMicros) {
        this.currentMicros = currentMicros;
    }


    public void setDataAppendPageSize(long dataAppendPageSize) {
        this.dataAppendPageSize = dataAppendPageSize;
    }


    public void setDefaultTableWriteMode(int defaultTableWriteMode) {
        this.defaultTableWriteMode = defaultTableWriteMode;
    }

    public void setEnv(Map<String, String> env) {
        this.env = env;
    }


    public void setFactoryProvider(FactoryProvider factoryProvider) {
        this.factoryProvider = factoryProvider;
    }


    public void setFilesFacade(FilesFacade ff) {
        this.ff = ff;
    }


    public void setGroupByAllocatorDefaultChunkSize(long groupByAllocatorDefaultChunkSize) {
        this.groupByAllocatorDefaultChunkSize = groupByAllocatorDefaultChunkSize;
    }


    public void setGroupByShardingThreshold(int groupByShardingThreshold) {
        this.groupByShardingThreshold = groupByShardingThreshold;
    }


    public void setHideTelemetryTable(boolean hideTelemetryTable) {
        this.hideTelemetryTable = hideTelemetryTable;
    }


    public void setInactiveReaderMaxOpenPartitions(int maxOpenPartitions) {
        this.maxOpenPartitions = maxOpenPartitions;
    }


    public void setInputRoot(String inputRoot) {
        this.inputRoot = inputRoot;
    }


    public void setInputWorkRoot(String inputWorkRoot) {
        this.inputWorkRoot = inputWorkRoot;
    }


    public void setIoURingEnabled(Boolean ioURingEnabled) {
        this.ioURingEnabled = ioURingEnabled;
    }


    public void setJitMode(int jitMode) {
        this.jitMode = jitMode;
    }


    public void setMangleTableDirNames(boolean mangle) {
        this.mangleTableDirNames = mangle;
    }


    public void setMaxFileNameLength(int maxFileNameLength) {
        this.maxFileNameLength = maxFileNameLength;
    }


    public void setMaxSqlRecompileAttempts(int maxSqlRecompileAttempts) {
        this.maxSqlRecompileAttempts = maxSqlRecompileAttempts;
    }


    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }


    public void setO3ColumnMemorySize(int size) {
        o3ColumnMemorySize = size;
    }


    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }


    public void setO3MinLag(long minLag) {
        o3MinLag = minLag;
    }


    public void setO3PartitionSplitMaxCount(int o3PartitionSplitMaxCount) {
        this.o3PartitionSplitMaxCount = o3PartitionSplitMaxCount;
    }


    public void setO3QuickSortEnabled(boolean o3QuickSortEnabled) {
        this.o3QuickSortEnabled = o3QuickSortEnabled;
    }


    public void setParallelFilterEnabled(Boolean parallelFilterEnabled) {
        this.parallelFilterEnabled = parallelFilterEnabled;
    }


    public void setParallelGroupByEnabled(Boolean parallelGroupByEnabled) {
        this.parallelGroupByEnabled = parallelGroupByEnabled;
    }


    public void setParallelImportStatusLogKeepNDays(int parallelImportStatusLogKeepNDays) {
        this.parallelImportStatusLogKeepNDays = parallelImportStatusLogKeepNDays;
    }


    public void setPartitionO3SplitThreshold(long value) {
        this.partitionO3SplitThreshold = value;
    }

    public void setProperty(PropertyKey propertyKey, long value) {
        properties.setProperty(propertyKey.getPropertyPath(), String.valueOf(value));
        changed = true;
    }

    public void setProperty(PropertyKey propertyKey, String value) {
        if (value != null) {
            properties.setProperty(propertyKey.getPropertyPath(), value);
        } else {
            properties.remove(propertyKey.getPropertyPath());
        }
        changed = true;
    }

    public void setRegistryCompactionThreshold(int value) {
        tableRegistryCompactionThreshold = value;
    }

    public void setRepeatMigrationsFromVersion(int value) {
        repeatMigrationsFromVersion = value;
    }

    public void setRndFunctionMemoryMaxPages(int rndFunctionMemoryMaxPages) {
        this.rndFunctionMemoryMaxPages = rndFunctionMemoryMaxPages;
    }

    public void setRndFunctionMemoryPageSize(int rndFunctionMemoryPageSize) {
        this.rndFunctionMemoryPageSize = rndFunctionMemoryPageSize;
    }

    public void setRostiAllocFacade(RostiAllocFacade rostiAllocFacade) {
        this.rostiAllocFacade = rostiAllocFacade;
    }

    public void setSampleByIndexSearchPageSize(int sampleByIndexSearchPageSize) {
        this.sampleByIndexSearchPageSize = sampleByIndexSearchPageSize;
    }

    public void setSimulateCrashEnabled(boolean enabled) {
        this.simulateCrashEnabled = enabled;
    }

    public void setSpinLockTimeout(long spinLockTimeout) {
        this.spinLockTimeout = spinLockTimeout;
    }

    public void setSqlCopyBufferSize(int sqlCopyBufferSize) {
        this.sqlCopyBufferSize = sqlCopyBufferSize;
    }

    public void setSqlJoinMetadataMaxResizes(int sqlJoinMetadataMaxResizes) {
        this.sqlJoinMetadataMaxResizes = sqlJoinMetadataMaxResizes;
    }

    public void setSqlJoinMetadataPageSize(int sqlJoinMetadataPageSize) {
        this.sqlJoinMetadataPageSize = sqlJoinMetadataPageSize;
    }

    public void setSqlWindowMaxRecursion(int maxRecursion) {
        this.sqlWindowMaxRecursion = maxRecursion;
    }

    public void setSqlWindowStoreMaxPages(int windowStoreMaxPages) {
        this.sqlWindowStoreMaxPages = windowStoreMaxPages;
    }

    public void setSqlWindowStorePageSize(int windowStorePageSize) {
        this.sqlWindowStorePageSize = windowStorePageSize;
    }

    public void setTestMicrosClock(MicrosecondClock testMicrosClock) {
        this.testMicrosClock = testMicrosClock;
    }

    public void setWalApplyTableTimeQuota(long walApplyTableTimeQuota) {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, walApplyTableTimeQuota);
    }

    public void setWalLookAheadTransactionCount(int walLookAheadTransactionCount) {
        this.walLookAheadTransactionCount = walLookAheadTransactionCount;
    }


    public void setWalMaxLagSize(long value) {
        walMaxLagSize = value;
    }

    public void setWalMaxLagTxnCount(int walMaxLagTxnCount) {
        this.walMaxLagTxnCount = walMaxLagTxnCount;
    }


    public void setWalMaxSegmentFileDescriptorsCache(int value) {
        walMaxSegmentFileDescriptorsCache = value;
    }


    public void setWalPurgeInterval(long walPurgeInterval) {
        this.walPurgeInterval = walPurgeInterval;
    }


    public void setWalSegmentRolloverRowCount(long walSegmentRolloverRowCount) {
        this.walSegmentRolloverRowCount = walSegmentRolloverRowCount;
    }


    public void setWalSegmentRolloverSize(long walSegmentRolloverSize) {
        this.walSegmentRolloverSize = walSegmentRolloverSize;
    }


    public void setWalTxnNotificationQueueCapacity(int walTxnNotificationQueueCapacity) {
        this.walTxnNotificationQueueCapacity = walTxnNotificationQueueCapacity;
    }


    public void setWriterAsyncCommandMaxTimeout(long writerAsyncCommandMaxTimeout) {
        this.writerAsyncCommandMaxTimeout = writerAsyncCommandMaxTimeout;
    }


    public void setWriterCommandQueueCapacity(int writerCommandQueueCapacity) {
        this.writerCommandQueueCapacity = writerCommandQueueCapacity;
    }


    public void setWriterCommandQueueSlotSize(long writerCommandQueueSlotSize) {
        this.writerCommandQueueSlotSize = writerCommandQueueSlotSize;
    }


    public void setWriterMixedIOEnabled(Boolean writerMixedIOEnabled) {
        this.writerMixedIOEnabled = writerMixedIOEnabled;
    }
}
