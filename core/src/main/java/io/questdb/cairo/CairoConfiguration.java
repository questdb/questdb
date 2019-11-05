/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.time.MillisecondClock;

public interface CairoConfiguration {

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

    int getMaxSwapFileCount();

    MicrosecondClock getMicrosecondClock();

    MillisecondClock getMillisecondClock();

    int getMkDirMode();

    int getParallelIndexThreshold();

    int getReaderPoolMaxSegments();

    CharSequence getRoot();

    long getSpinLockTimeoutUs();

    int getSqlCacheBlocks();

    int getSqlCacheRows();

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

    int getSqlModelPoolCapacity();

    int getSqlSortKeyPageSize();

    int getSqlSortLightValuePageSize();

    int getSqlHashJoinValuePageSize();

    long getSqlLatestByRowCount();

    int getSqlHashJoinLightValuePageSize();

    int getSqlSortValuePageSize();

    TextConfiguration getTextConfiguration();

    long getWorkStealTimeoutNanos();

    boolean isParallelIndexingEnabled();

    /**
     * This holds table metadata, which is usually quite small. 16K page should be adequate.
     *
     * @return memory page size
     */
    int getSqlJoinMetadataPageSize();

    int getAnalyticColumnPoolCapacity();

    int getCreateTableModelPoolCapacity();

    int getColumnCastModelPoolCapacity();

    int getRenameTableModelPoolCapacity();

    int getWithClauseModelPoolCapacity();

    int getInsertPoolCapacity();
}
