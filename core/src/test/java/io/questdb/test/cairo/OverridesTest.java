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
 * Unit tests for the property override store every {@code AbstractCairoTest} subclass writes to.
 * Overrides rebuilds the configuration on the first read after a batch of setProperty() calls
 * that changed anything. A call that changes nothing must not hide an earlier change in the
 * same batch. Tests that use a standalone {@code Overrides} allocate no native memory beyond
 * what building a {@code CairoConfiguration} does, so they do not use {@code assertMemoryLeak()}.
 */
public class OverridesTest extends AbstractCairoTest {
    // cairo.max.uncommitted.rows as Overrides' own default test properties set it.
    private static final int DEFAULT_TEST_MAX_UNCOMMITTED_ROWS = 1000;
    // cairo.sql.page.frame.min.rows as Overrides' own default test properties set it.
    private static final int DEFAULT_TEST_PAGE_FRAME_MIN_ROWS = 1000;

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
    public void testResetDropsOverrides() {
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        overrides.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 4_321);
        Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());

        overrides.reset();
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
        Assert.assertEquals(DEFAULT_TEST_PAGE_FRAME_MIN_ROWS, overrides.getConfiguration(root).getSqlPageFrameMinRows());
    }

    @Test
    public void testSetPropertyNullRemovesOverrideAfterConfigurationRead() {
        // A configuration read over a non-empty override map clears the rebuild flag, so the
        // removal is what has to set it again for the next read to see the default.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());

        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, null);
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
    }

    @Test
    public void testSetPropertyNullRemovesOverrideBeforeConfigurationRead() {
        // A fresh Overrides starts with the rebuild flag already set, which is the state that used
        // to make the removal short-circuit away and leave the cancelled override in the map.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, null);
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
    }

    @Test
    public void testSetPropertyNullRemovesOverrideWithAnotherOverrideStanding() {
        // Removal with another override still standing: the map stays non-empty, so the read goes
        // through the rebuilt props configuration rather than falling back to the default one.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 4_321);
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getSqlPageFrameMinRows());

        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, null);
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getSqlPageFrameMinRows());
    }

    @Test
    public void testSetPropertyReplacesOverrideAfterConfigurationRead() {
        // The sibling branch of the same flag: a plain overwrite still reaches the next read.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());

        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 4_321);
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getMaxUncommittedRows());
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
