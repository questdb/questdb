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

import io.questdb.BuildInformation;
import io.questdb.TelemetryConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.lang.ThreadLocal;

public interface CairoConfiguration {

    long O_NONE = 0;
    long O_ASYNC = 0x40;
    long O_SYNC = 0x80;
    long O_DIRECT = 0x4000;

    ThreadLocal<Rnd> RANDOM = new ThreadLocal<>();

    boolean attachPartitionCopy();

    boolean enableTestFactories();

    int getAnalyticColumnPoolCapacity();

    // the '+' is used to prevent overlap with table names
    default String getArchivedCrashFilePrefix() {
        return "crash+";
    }

    String getAttachPartitionSuffix();

    DateFormat getBackupDirTimestampFormat();

    int getBackupMkDirMode();

    // null disables backups
    CharSequence getBackupRoot();

    CharSequence getBackupTempDirName();

    int getBinaryEncodingMaxLength();

    int getBindVariablePoolSize();

    BuildInformation getBuildInformation();

    SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration();

    int getColumnCastModelPoolCapacity();

    int getColumnIndexerQueueCapacity();

    int getColumnPurgeQueueCapacity();

    long getColumnPurgeRetryDelay();

    long getColumnPurgeRetryDelayLimit();

    double getColumnPurgeRetryDelayMultiplier();

    int getColumnPurgeTaskPoolCapacity();

    /**
     * Default commit lag in microseconds for new tables. This value
     * can be overridden with 'create table' statement.
     *
     * @return commit lag in microseconds
     */
    long getCommitLag();

    int getCommitMode();

    CharSequence getConfRoot(); // same as root/../conf

    int getCopyPoolCapacity();

    int getCreateAsSelectRetryCount();

    int getCreateTableModelPoolCapacity();

    long getDataAppendPageSize();

    long getDataIndexKeyAppendPageSize();

    long getDataIndexValueAppendPageSize();

    long getDatabaseIdHi();

    long getDatabaseIdLo();

    CharSequence getDbDirectory(); // env['cairo.root'], defaults to db

    DateLocale getDefaultDateLocale();

    CharSequence getDefaultMapType();

    boolean getDefaultSymbolCacheFlag();

    int getDefaultSymbolCapacity();

    int getDoubleToStrCastScale();

    int getFileOperationRetryCount();

    FilesFacade getFilesFacade();

    int getFloatToStrCastScale();

    int getGroupByMapCapacity();

    int getGroupByPoolCapacity();

    default IOURingFacade getIOURingFacade() {
        return IOURingFacadeImpl.INSTANCE;
    }

    long getIdleCheckInterval();

    long getInactiveReaderTTL();

    long getInactiveWriterTTL();

    int getIndexValueBlockSize();

    int getInsertPoolCapacity();

    int getLatestByQueueCapacity();

    int getMaxCrashFiles();

    int getMaxFileNameLength();

    int getMaxSwapFileCount();

    int getMaxSymbolNotEqualsCount();

    int getMaxUncommittedRows();

    MicrosecondClock getMicrosecondClock();

    MillisecondClock getMillisecondClock();

    long getMiscAppendPageSize();

    int getMkDirMode();

    default NanosecondClock getNanosecondClock() {
        return NanosecondClockImpl.INSTANCE;
    }

    int getO3CallbackQueueCapacity();

    int getO3ColumnMemorySize();

    int getO3CopyQueueCapacity();

    int getO3OpenColumnQueueCapacity();

    int getO3PartitionQueueCapacity();

    int getO3PurgeDiscoveryQueueCapacity();

    // the '+' is used to prevent overlap with table names
    default String getOGCrashFilePrefix() {
        return "hs_err_pid+";
    }

    int getPageFrameReduceColumnListCapacity();

    int getPageFrameReduceQueueCapacity();

    int getPageFrameReduceRowIdListCapacity();

    int getPageFrameReduceShardCount();

    int getPageFrameReduceTaskPoolCapacity();

    int getParallelIndexThreshold();

    int getPartitionPurgeListCapacity();

    int getQueryCacheEventQueueCapacity();

    default Rnd getRandom() {
        Rnd rnd = RANDOM.get();
        if (rnd == null) {
            RANDOM.set(rnd = new Rnd(
                    getNanosecondClock().getTicks(),
                    getMicrosecondClock().getTicks())
            );
        }
        return rnd;
    }

    int getReaderPoolMaxSegments();

    int getRenameTableModelPoolCapacity();

    int getStrFunctionMaxBufferLength();

    int getRndFunctionMemoryMaxPages();

    int getRndFunctionMemoryPageSize();

    CharSequence getRoot(); // some folder with suffix env['cairo.root'] e.g. /.../db

    default RostiAllocFacade getRostiAllocFacade() {
        return RostiAllocFacadeImpl.INSTANCE;
    }

    int getSampleByIndexSearchPageSize();

    boolean getSimulateCrashEnabled();

    /**
     * Returns database instance id. The instance id is used by the snapshot recovery mechanism:
     * on database start the id is compared with the id stored in a snapshot, if any. If the ids
     * are different, snapshot recovery is being triggered.
     *
     * @return instance id.
     */
    CharSequence getSnapshotInstanceId();

    CharSequence getSnapshotRoot(); // same as root/../snapshot

    long getSpinLockTimeout();

    int getSqlAnalyticRowIdMaxPages();

    int getSqlAnalyticRowIdPageSize();

    int getSqlAnalyticStoreMaxPages();

    int getSqlAnalyticStorePageSize();

    int getSqlAnalyticTreeKeyMaxPages();

    int getSqlAnalyticTreeKeyPageSize();

    int getSqlCharacterStoreCapacity();

    int getSqlCharacterStoreSequencePoolCapacity();

    int getSqlColumnPoolCapacity();

    double getSqlCompactMapLoadFactor();

    int getSqlCopyBufferSize();

    // null input root disables "copy" sql
    CharSequence getSqlCopyInputRoot();

    CharSequence getSqlCopyInputWorkRoot();

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

    int getSqlJitBindVarsMemoryMaxPages();

    int getSqlJitBindVarsMemoryPageSize();

    int getSqlJitIRMemoryMaxPages();

    int getSqlJitIRMemoryPageSize();

    int getSqlJitMode();

    int getSqlJitPageAddressCacheThreshold();

    int getSqlJitRowsThreshold();

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

    int getSqlMapKeyCapacity();

    int getSqlMapMaxPages();

    int getSqlMapMaxResizes();

    int getSqlMapPageSize();

    int getSqlMaxNegativeLimit();

    int getSqlModelPoolCapacity();

    int getSqlPageFrameMaxRows();

    int getSqlPageFrameMinRows();

    int getSqlSmallMapKeyCapacity();

    int getSqlSortKeyMaxPages();

    long getSqlSortKeyPageSize();

    int getSqlSortLightValueMaxPages();

    long getSqlSortLightValuePageSize();

    int getSqlSortValueMaxPages();

    int getSqlSortValuePageSize();

    CharSequence getSystemTableNamePrefix();

    TelemetryConfiguration getTelemetryConfiguration();

    TextConfiguration getTextConfiguration();

    int getTxnScoreboardEntryCount();

    int getVectorAggregateQueueCapacity();

    boolean getWallEnabledDefault();

    int getWithClauseModelPoolCapacity();

    long getWorkStealTimeoutNanos();

    long getWriterAsyncCommandBusyWaitTimeout();

    long getWriterAsyncCommandMaxTimeout();

    int getWriterCommandQueueCapacity();

    long getWriterCommandQueueSlotSize();

    long getWriterFileOpenOpts();

    int getWriterTickRowsCountMod();

    boolean isIOURingEnabled();

    boolean isO3QuickSortEnabled();

    boolean isParallelIndexingEnabled();

    /**
     * A flag to enable/disable snapshot recovery mechanism. Defaults to {@code true}.
     *
     * @return enable/disable snapshot recovery flag
     */
    boolean isSnapshotRecoveryEnabled();

    boolean isSqlJitDebugEnabled();

    boolean isSqlParallelFilterEnabled();

    boolean isSqlParallelFilterPreTouchEnabled();
}
