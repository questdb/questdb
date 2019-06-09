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

package com.questdb.cairo;

import com.questdb.std.Chars;
import com.questdb.std.FilesFacade;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.Numbers;
import com.questdb.std.microtime.MicrosecondClock;
import com.questdb.std.microtime.MicrosecondClockImpl;
import com.questdb.std.time.MillisecondClock;
import com.questdb.std.time.MillisecondClockImpl;

public class DefaultCairoConfiguration implements CairoConfiguration {

    private final CharSequence root;

    public DefaultCairoConfiguration(CharSequence root) {
        this.root = Chars.stringOf(root);
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
    public long getSpinLockTimeoutUs() {
        return 1000000;
    }

    @Override
    public int getSqlCacheBlocks() {
        return 4;
    }

    @Override
    public int getSqlCacheRows() {
        return 16;
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
        return 4 * Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlModelPoolCapacity() {
        return 1024;
    }

    @Override
    public int getSqlSortKeyPageSize() {
        return 4 * Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlSortLightValuePageSize() {
        return Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlHashJoinValuePageSize() {
        return Numbers.SIZE_1MB * 16;
    }

    @Override
    public int getSqlTreePageSize() {
        return Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlHashJoinLightValuePageSize() {
        return Numbers.SIZE_1MB;
    }

    @Override
    public int getSqlSortValuePageSize() {
        return Numbers.SIZE_1MB * 16;
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
}
