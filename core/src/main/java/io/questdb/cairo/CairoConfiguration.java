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
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.NanosecondClock;
import io.questdb.std.NanosecondClockImpl;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public interface CairoConfiguration {

    ThreadLocal<Rnd> RANDOM = new ThreadLocal<>();

    boolean enableTestFactories();

    int getAnalyticColumnPoolCapacity();

    long getDataAppendPageSize();

    DateFormat getBackupDirTimestampFormat();

    int getBackupMkDirMode();

    // null disables backups
    CharSequence getBackupRoot();

    CharSequence getBackupTempDirName();

    int getBinaryEncodingMaxLength();

    int getBindVariablePoolSize();

    BuildInformation getBuildInformation();

    int getColumnCastModelPoolCapacity();

    int getColumnIndexerQueueCapacity();

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

    long getIdleCheckInterval();

    long getInactiveReaderTTL();

    long getInactiveWriterTTL();

    int getIndexValueBlockSize();

    // null input root disables "copy" sql
    CharSequence getInputRoot();

    int getInsertPoolCapacity();

    int getLatestByQueueCapacity();

    int getMaxSwapFileCount();

    int getMaxSymbolNotEqualsCount();

    int getMaxUncommittedRows();

    MicrosecondClock getMicrosecondClock();

    MillisecondClock getMillisecondClock();

    int getMkDirMode();

    default NanosecondClock getNanosecondClock() {
        return NanosecondClockImpl.INSTANCE;
    }

    int getO3CallbackQueueCapacity();

    int getO3ColumnMemorySize();

    int getO3CopyQueueCapacity();

    int getO3OpenColumnQueueCapacity();

    int getO3PartitionQueueCapacity();

    int getO3PartitionUpdateQueueCapacity();

    int getO3PurgeDiscoveryQueueCapacity();

    int getO3PurgeQueueCapacity();

    int getParallelIndexThreshold();

    default Rnd getRandom() {
        Rnd rnd = RANDOM.get();
        if (rnd == null) {
            RANDOM.set(rnd = new Rnd(
                    getMillisecondClock().getTicks(),
                    getMicrosecondClock().getTicks())
            );
        }
        return rnd;
    }

    int getReaderPoolMaxSegments();

    int getRenameTableModelPoolCapacity();

    CharSequence getRoot(); // some folder with suffix env['cairo.root'] e.g. /.../db

    int getSampleByIndexSearchPageSize();

    long getMiscAppendPageSize();

    long getSpinLockTimeoutUs();

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

    int getSqlDistinctTimestampKeyCapacity();

    double getSqlDistinctTimestampLoadFactor();

    int getSqlExpressionPoolCapacity();

    double getSqlFastMapLoadFactor();

    int getSqlHashJoinLightValueMaxPages();

    int getSqlHashJoinLightValuePageSize();

    int getSqlHashJoinValueMaxPages();

    int getSqlHashJoinValuePageSize();

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

    int getSqlModelPoolCapacity();

    int getSqlSortKeyMaxPages();

    long getSqlSortKeyPageSize();

    int getSqlSortLightValueMaxPages();

    long getSqlSortLightValuePageSize();

    int getSqlSortValueMaxPages();

    int getSqlSortValuePageSize();

    int getSqlPageFrameMaxSize();

    int getSqlJitMode();

    int getSqlJitIRMemoryPageSize();

    int getSqlJitIRMemoryMaxPages();

    int getSqlJitBindVarsMemoryPageSize();

    int getSqlJitBindVarsMemoryMaxPages();

    int getSqlJitRowsThreshold();

    int getSqlJitPageAddressCacheThreshold();

    boolean isSqlJitDebugEnabled();

    int getTableBlockWriterQueueCapacity();

    TelemetryConfiguration getTelemetryConfiguration();

    TextConfiguration getTextConfiguration();

    int getTxnScoreboardEntryCount();

    int getVectorAggregateQueueCapacity();

    int getWithClauseModelPoolCapacity();

    long getWorkStealTimeoutNanos();

    long getWriterAsyncCommandBusyWaitTimeout();

    long getWriterAsyncCommandMaxTimeout();

    int getWriterCommandQueueCapacity();

    int getWriterTickRowsCountMod();

    boolean isO3QuickSortEnabled();

    boolean isParallelIndexingEnabled();
}
