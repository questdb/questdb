/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;

public class DefaultTestCairoConfiguration extends DefaultCairoConfiguration {
    private final Metrics metrics = Metrics.ENABLED;

    public DefaultTestCairoConfiguration(CharSequence dbRoot, CharSequence installRoot) {
        super(dbRoot, installRoot);
    }

    public DefaultTestCairoConfiguration(CharSequence dbRoot) {
        super(dbRoot);
    }

    @Override
    public boolean disableColumnPurgeJob() {
        return true;
    }

    @Override
    public boolean getAllowTableRegistrySharedWrite() {
        return true;
    }

    @Override
    public @NotNull FilesFacade getFilesFacade() {
        return TestFilesFacadeImpl.INSTANCE;
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }

    @Override
    public int getO3ColumnMemorySize() {
        // Reduce test memory consumption, set o3 column memory to 1MB
        return 1 << 20;
    }

    @Override
    public int getPartitionEncoderParquetDataPageSize() {
        return 1024; // 1KB
    }

    @Override
    public int getPartitionEncoderParquetRowGroupSize() {
        return 1000;
    }

    @Override
    public long getPartitionO3SplitMinSize() {
        return 512 * (1L << 10); // 512KiB
    }

    @Override
    public @NotNull CharSequence getSystemTableNamePrefix() {
        return "sys.";
    }

    @Override
    public int getTableRegistryCompactionThreshold() {
        return 0;
    }

    @Override
    public long getWalApplyTableTimeQuota() {
        // Unrestricted.
        return -1L;
    }

    @Override
    public boolean isColumnAliasExpressionEnabled() {
        return false;
    }

    @Override
    public boolean isDevModeEnabled() {
        return true;
    }

    @Override
    public boolean mangleTableDirNames() {
        return true;
    }
}
