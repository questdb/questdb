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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link NamedWindowFuzzTest} with {@code cairo.sql.window.map.fusion.enabled} off.
 * <p>
 * It is the one existing suite whose queries fuse: its cases put two to four functions on one
 * shared window, which is exactly the shape a group is for, and a default run of it binds
 * groups by the dozen. Its assertions are equivalences rather than pinned values - a named
 * window against the inline spelling of the same specification - and both sides of each of
 * them fuse together, so what this run adds is the other half: the same equivalences with no
 * group in the picture at all, over a fresh random seed.
 *
 * @see WindowMapFusionFuzzTest for the harness that compares the two settings row for row
 */
public class NamedWindowFuzzFusionDisabledTest extends NamedWindowFuzzTest {

    /**
     * Per test rather than once per class: a node's teardown resets the configuration
     * overrides, and it runs after every test method, so a {@code @BeforeClass} property would
     * hold for the first inherited case and for neither of the others.
     */
    @Override
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        super.setUp();
    }

    /**
     * The inherited cases say nothing about the switch, so without this one a reset of the
     * overrides, a renamed key or a default that stopped being read would leave the class
     * silently running its parent a second time.
     */
    @Test
    public void testFusionIsDisabledForThisRun() {
        Assert.assertFalse(configuration.isSqlWindowMapFusionEnabled());
    }
}
