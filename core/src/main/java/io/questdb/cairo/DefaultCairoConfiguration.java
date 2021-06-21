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
import io.questdb.BuildInformationHolder;
import io.questdb.DefaultTelemetryConfiguration;
import io.questdb.TelemetryConfiguration;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;

public class DefaultCairoConfiguration implements CairoConfiguration {

    private final CharSequence root;

    private final TextConfiguration textConfiguration = new DefaultTextConfiguration();

    private final DefaultTelemetryConfiguration telemetryConfiguration = new DefaultTelemetryConfiguration();

    private final BuildInformation buildInformation = new BuildInformationHolder();

    private final long databaseIdLo;
    private final long databaseIdHi;

    public DefaultCairoConfiguration(CharSequence root) {
        this.root = Chars.toString(root);
        Rnd rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
        this.databaseIdLo = rnd.nextLong();
        this.databaseIdHi = rnd.nextLong();
    }

    @Override
    public int getBindVariablePoolSize() {
        return 8;
    }

    @Override
    public int getO3PurgeDiscoveryQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3PurgeQueueCapacity() {
        return 1024;
    }

    @Override
    public int getSqlCopyBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public int getCopyPoolCapacity() {
        return 16;
    }

    @Override
    public int getCreateAsSelectRetryCount() {
        return 5;
    }

    @Override
    public CharSequence getDefaultMapType() {
        return "fast";
    }

    @Override
    public boolean getDefaultSymbolCacheFlag() {
        return true;
    }

    @Override
    public int getDefaultSymbolCapacity() {
        return 128;
    }

    @Override
    public int getFileOperationRetryCount() {
        return 30;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return FilesFacadeImpl.INSTANCE;
    }

    @Override
    public long getIdleCheckInterval() {
        return 100;
    }

    @Override
    public long getInactiveReaderTTL() {
        return -10000;
    }

    @Override
    public long getInactiveWriterTTL() {
        return -10000;
    }

    @Override
    public int getIndexValueBlockSize() {
        return 256;
    }

    @Override
    public int getDoubleToStrCastScale() {
        return Numbers.MAX_SCALE;
    }

    @Override
    public int getFloatToStrCastScale() {
        return 4;
    }

    @Override
    public int getMaxSwapFileCount() {
        return 30;
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return MicrosecondClockImpl.INSTANCE;
    }

    @Override
    public MillisecondClock getMillisecondClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    @Override
    public NanosecondClock getNanosecondClock() {
        return NanosecondClockImpl.INSTANCE;
    }

    @Override
    public int getMkDirMode() {
        return 509;
    }

    @Override
    public int getParallelIndexThreshold() {
        return 100000;
    }

    @Override
    public int getReaderPoolMaxSegments() {
        return 5;
    }

    @Override
    public CharSequence getRoot() {
        return root;
    }

    @Override
    public CharSequence getInputRoot() {
        return null;
    }

    @Override
    public CharSequence getBackupRoot() {
        return null;
    }

    @Override
    public DateFormat getBackupDirTimestampFormat() {
        return null;
    }

    @Override
    public CharSequence getBackupTempDirName() {
        return "tmp";
    }

    @Override
    public int getBackupMkDirMode() {
        return 509;
    }

    @Override
    public long getSpinLockTimeoutUs() {
        return 5000000;
    }

    @Override
    public int getSqlCharacterStoreCapacity() {
        // 1024 seems like a good fit, but tests need
        // smaller capacity so that resize is tested correctly
        return 64;
    }

    @Override
    public int getSqlCharacterStoreSequencePoolCapacity() {
        return 64;
    }

    @Override
    public int getSqlColumnPoolCapacity() {
        return 4096;
    }

    @Override
    public double getSqlCompactMapLoadFactor() {
        return 0.8;
    }

    @Override
    public int getSqlExpressionPoolCapacity() {
        return 8192;
    }

    @Override
    public double getSqlFastMapLoadFactor() {
        return 0.5;
    }

    @Override
    public int getSqlJoinContextPoolCapacity() {
        return 64;
    }

    @Override
    public int getSqlLexerPoolCapacity() {
        return 2048;
    }

    @Override
    public int getSqlMapKeyCapacity() {
        return 128;
    }

    @Override
    public int getSqlMapPageSize() {
        return 16 * Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlDistinctTimestampKeyCapacity() {
        return 256;
    }

    @Override
    public double getSqlDistinctTimestampLoadFactor() {
        return 0.5;
    }

    @Override
    public int getSqlMapMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlMapMaxResizes() {
        return 64;
    }

    @Override
    public int getSqlModelPoolCapacity() {
        return 1024;
    }

    @Override
    public long getSqlSortKeyPageSize() {
        return 4 * Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlSortKeyMaxPages() {
        return 128;
    }

    @Override
    public long getSqlSortLightValuePageSize() {
        return 8 * Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlSortLightValueMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlHashJoinValuePageSize() {
        return Numbers.SIZE_1MB * 16;
    }

    @Override
    public int getSqlHashJoinValueMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlAnalyticStorePageSize() {
        return 4 * 1024;
    }

    @Override
    public int getSqlAnalyticStoreMaxPages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSqlAnalyticRowIdPageSize() {
        return 1024;
    }

    @Override
    public int getSqlAnalyticRowIdMaxPages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSqlAnalyticTreeKeyPageSize() {
        return 4 * 1024;
    }

    @Override
    public int getSqlAnalyticTreeKeyMaxPages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long getSqlLatestByRowCount() {
        return 1000;
    }

    @Override
    public int getSqlHashJoinLightValuePageSize() {
        return Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlHashJoinLightValueMaxPages() {
        return 1024;
    }

    @Override
    public int getSqlSortValuePageSize() {
        return Numbers.SIZE_1MB * 16;
    }

    @Override
    public int getSqlSortValueMaxPages() {
        return 1024;
    }

    @Override
    public TextConfiguration getTextConfiguration() {
        return textConfiguration;
    }

    @Override
    public long getWorkStealTimeoutNanos() {
        return 10000;
    }

    @Override
    public boolean isParallelIndexingEnabled() {
        return true;
    }

    @Override
    public int getSqlJoinMetadataPageSize() {
        return 16 * 1024;
    }

    @Override
    public int getSqlJoinMetadataMaxResizes() {
        return 10;
    }

    @Override
    public int getAnalyticColumnPoolCapacity() {
        return 64;
    }

    @Override
    public int getCreateTableModelPoolCapacity() {
        return 32;
    }

    @Override
    public int getColumnCastModelPoolCapacity() {
        return 32;
    }

    @Override
    public int getRenameTableModelPoolCapacity() {
        return 8;
    }

    @Override
    public int getWithClauseModelPoolCapacity() {
        return 128;
    }

    @Override
    public int getInsertPoolCapacity() {
        return 8;
    }

    @Override
    public int getCommitMode() {
        return CommitMode.NOSYNC;
    }

    @Override
    public DateLocale getDefaultDateLocale() {
        return DateFormatUtils.enLocale;
    }

    @Override
    public int getGroupByPoolCapacity() {
        return 1024;
    }

    @Override
    public int getMaxSymbolNotEqualsCount() {
        return 100;
    }

    @Override
    public int getGroupByMapCapacity() {
        return 1024;
    }

    @Override
    public boolean enableTestFactories() {
        return true;
    }

    @Override
    public TelemetryConfiguration getTelemetryConfiguration() {
        return telemetryConfiguration;
    }

    @Override
    public long getAppendPageSize() {
        return getFilesFacade().getMapPageSize();
    }

    @Override
    public int getTableBlockWriterQueueCapacity() {
        return 4;
    }

    @Override
    public int getColumnIndexerQueueCapacity() {
        return 1024;
    }

    @Override
    public int getVectorAggregateQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3CallbackQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3PartitionQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3OpenColumnQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3CopyQueueCapacity() {
        return 1024;
    }

    @Override
    public int getO3PartitionUpdateQueueCapacity() {
        return 1024;
    }

    @Override
    public BuildInformation getBuildInformation() {
        return buildInformation;
    }

    @Override
    public long getDatabaseIdHi() {
        return databaseIdHi;
    }

    @Override
    public long getDatabaseIdLo() {
        return databaseIdLo;
    }

    @Override
    public int getTxnScoreboardEntryCount() {
        return 8192;
    }

    @Override
    public int getMaxUncommittedRows() {
        return 1000;
    }

    @Override
    public long getCommitLag() {
        return 0;
    }

    @Override
    public boolean isO3QuickSortEnabled() {
        return false;
    }

    @Override
    public int getLatestByQueueCapacity() {
        return 32;
    }
}
