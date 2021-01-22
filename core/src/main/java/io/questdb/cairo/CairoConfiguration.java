/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public interface CairoConfiguration {

    int getBindVariablePoolSize();

    int getSqlCopyBufferSize();

    int getCopyPoolCapacity();

    int getCreateAsSelectRetryCount();

    CharSequence getDefaultMapType();

    boolean getDefaultSymbolCacheFlag();

    int getDefaultSymbolCapacity();

    int getFileOperationRetryCount();

    FilesFacade getFilesFacade();

    long getIdleCheckInterval();

    long getInactiveReaderTTL();

    long getInactiveWriterTTL();

    int getIndexValueBlockSize();

    int getDoubleToStrCastScale();

    int getFloatToStrCastScale();

    int getMaxSwapFileCount();

    MicrosecondClock getMicrosecondClock();

    MillisecondClock getMillisecondClock();

    NanosecondClock getNanosecondClock();

    int getMkDirMode();

    int getParallelIndexThreshold();

    int getReaderPoolMaxSegments();

    CharSequence getRoot();

    // null input root disables "copy" sql
    CharSequence getInputRoot();

    // null disables backups
    CharSequence getBackupRoot();

    DateFormat getBackupDirTimestampFormat();

    CharSequence getBackupTempDirName();

    int getBackupMkDirMode();

    long getSpinLockTimeoutUs();

    int getSqlCharacterStoreCapacity();

    int getSqlCharacterStoreSequencePoolCapacity();

    int getSqlColumnPoolCapacity();

    double getSqlCompactMapLoadFactor();

    int getSqlExpressionPoolCapacity();

    double getSqlFastMapLoadFactor();

    int getSqlJoinContextPoolCapacity();

    int getSqlLexerPoolCapacity();

    int getSqlMapKeyCapacity();

    int getSqlMapPageSize();

    int getSqlMapMaxPages();

    int getSqlMapMaxResizes();

    int getSqlModelPoolCapacity();

    long getSqlSortKeyPageSize();

    int getSqlSortKeyMaxPages();

    long getSqlSortLightValuePageSize();

    int getSqlSortLightValueMaxPages();

    int getSqlHashJoinValuePageSize();

    int getSqlHashJoinValueMaxPages();

    int getSqlAnalyticStorePageSize();

    int getSqlAnalyticStoreMaxPages();

    int getSqlAnalyticRowIdPageSize();

    int getSqlAnalyticRowIdMaxPages();

    int getSqlAnalyticTreeKeyPageSize();

    int getSqlAnalyticTreeKeyMaxPages();

    long getSqlLatestByRowCount();

    int getSqlHashJoinLightValuePageSize();

    int getSqlHashJoinLightValueMaxPages();

    int getSqlSortValuePageSize();

    int getSqlSortValueMaxPages();

    TextConfiguration getTextConfiguration();

    long getWorkStealTimeoutNanos();

    boolean isParallelIndexingEnabled();

    /**
     * This holds table metadata, which is usually quite small. 16K page should be adequate.
     *
     * @return memory page size
     */
    int getSqlJoinMetadataPageSize();

    int getSqlJoinMetadataMaxResizes();

    int getAnalyticColumnPoolCapacity();

    int getCreateTableModelPoolCapacity();

    int getColumnCastModelPoolCapacity();

    int getRenameTableModelPoolCapacity();

    int getWithClauseModelPoolCapacity();

    int getInsertPoolCapacity();

    int getCommitMode();

    DateLocale getDefaultDateLocale();

    int getGroupByPoolCapacity();

    int getMaxSymbolNotEqualsCount();

    int getGroupByMapCapacity();

    boolean enableTestFactories();

    TelemetryConfiguration getTelemetryConfiguration();

    long getAppendPageSize();

    int getTableBlockWriterQueueCapacity();

    BuildInformation getBuildInformation();

    default boolean isOutOfOrderEnabled() {
        return false;
    }
}
