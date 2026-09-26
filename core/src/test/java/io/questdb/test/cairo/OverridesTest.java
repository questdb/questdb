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
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the property override store every {@code AbstractCairoTest} subclass writes to.
 * Nothing here allocates native memory beyond what building a {@code CairoConfiguration} does, so
 * these tests do not use {@code assertMemoryLeak()}.
 */
public class OverridesTest extends AbstractTest {
    // cairo.max.uncommitted.rows as Overrides' own default test properties set it.
    private static final int DEFAULT_TEST_MAX_UNCOMMITTED_ROWS = 1000;
    // cairo.sql.page.frame.min.rows as Overrides' own default test properties set it.
    private static final int DEFAULT_TEST_PAGE_FRAME_MIN_ROWS = 1000;

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
}
