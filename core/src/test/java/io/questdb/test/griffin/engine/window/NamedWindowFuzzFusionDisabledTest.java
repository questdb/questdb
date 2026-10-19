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
 * It is the one existing suite whose queries fuse: {@code testNamedWindowSharedByMultipleFunctions}
 * puts two to four functions on one shared window, which is exactly the shape a group is for,
 * and a default run of it binds a group per iteration of that case.
 * <p>
 * What this run is <b>not</b> is a fused-versus-unfused differential, and the distinction
 * matters when reading it as coverage. The inherited assertions are equivalences rather than
 * pinned values - a named window against the inline spelling of the same specification - and
 * both sides of each of them fuse together, so every one of them holds whether the switch is on
 * or off. Neither setting is the other's reference. What the run adds is the same equivalences
 * with no group in the picture at all, over a fresh random seed, which is worth having and is
 * not a comparison. {@link WindowMapFusionFuzzTest} is the harness that does compare the two
 * settings row for row, and {@code WindowFunctionFusionDisabledTest} the copy whose inherited
 * assertions are literals and so does read as one.
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

    /**
     * That the run this class copies fuses the shape the class says it does, and that this run
     * does not. The inherited equivalences cannot say so - they hold either way - so without
     * this the class's premise rests on its own javadoc: a decline rule widened far enough would
     * leave the default run unfused too, and every case here would still pass.
     * <p>
     * The window is one {@code randomWindowSpec} generates and the functions are three of
     * {@code AGGREGATE_FUNCTIONS}, so what binds here is a shape the inherited cases really
     * reach. A lower bound rather than a count, because how many of the 200 iterations fuse is
     * the generator's answer and a fresh seed's.
     */
    @Test
    public void testTheDefaultRunFusesWhatThisRunDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table fusion_guard (x int, category symbol, ts timestamp) timestamp(ts)");
            final String sql = "SELECT x, sum(x) OVER w, avg(x) OVER w, count(x) OVER w "
                    + "FROM fusion_guard "
                    + "WINDOW w AS (PARTITION BY category ORDER BY ts "
                    + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
            WindowMapStateTest.assertIsBound(sql, false);
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "true");
            WindowMapStateTest.assertIsBound(sql, true);
        });
    }
}
