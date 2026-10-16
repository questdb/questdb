/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.HashJoinBuildMode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Overrides rebuilds the configuration on the first read after a batch of setProperty() calls
 * that changed anything. A call that changes nothing must not hide an earlier change in the
 * same batch.
 */
public class OverridesTest extends AbstractCairoTest {

    @Test
    public void testBuildModeReappliedAfterAnotherMode() throws Exception {
        // PARALLEL sets the minimum row count and the rows per partition. Once SERIAL replaces the
        // minimum, applying PARALLEL again changes the minimum and sets the rows per partition to
        // the value it already holds.
        assertMemoryLeak(() -> {
            final Overrides overrides = node1.getConfigurationOverrides();
            HashJoinBuildMode.PARALLEL.applyProperties(overrides);
            Assert.assertEquals(0, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            HashJoinBuildMode.SERIAL.applyProperties(overrides);
            Assert.assertEquals(Long.MAX_VALUE, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            HashJoinBuildMode.PARALLEL.applyProperties(overrides);
            Assert.assertEquals(0, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            Assert.assertEquals(1, configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition());
        });
    }

    @Test
    public void testRemovingAbsentPropertyKeepsEarlierChange() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 10);
            Assert.assertEquals(10, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 20);
            // No override of this key exists, so removing it changes nothing.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, null);
            Assert.assertEquals(20, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
        });
    }

    @Test
    public void testSettingCurrentValueKeepsEarlierChange() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 10);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 5);
            Assert.assertEquals(10, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            Assert.assertEquals(5, configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition());
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 20);
            // The override already holds this value, so this call changes nothing.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 5);
            Assert.assertEquals(20, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            Assert.assertEquals(5, configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition());
        });
    }
}
