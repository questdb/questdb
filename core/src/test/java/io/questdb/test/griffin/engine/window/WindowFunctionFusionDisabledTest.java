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
 * The whole of {@link WindowFunctionTest} with {@code cairo.sql.window.map.fusion.enabled}
 * off, against the same expected values.
 * <p>
 * That is the differential the kill switch was landed for: the suite pins its answers as
 * literals, so running it at both settings compares each against a fixed reference rather
 * than against the other run. The default run is the switch's other half - the class this one
 * extends, unchanged.
 * <p>
 * What it covers is narrower than that sounds, and it is worth stating rather than discovering.
 * The suite asks for one window function at a time almost everywhere, and a window carrying one
 * fusible function forms no group; the queries that do form one are the 25 this suite compiles,
 * all of which bind now that neither the co-location-only rule nor the Map-implementation rule
 * stands between a compiled plan and a runtime. So the two runs differ on 25 queries of
 * 600-odd, and the rest of the class is a second run of one path.
 *
 * @see WindowMapFusionFuzzTest for the differential that fuses on purpose
 */
public class WindowFunctionFusionDisabledTest extends WindowFunctionTest {

    /**
     * Per test rather than once per class: a node's teardown resets the configuration
     * overrides, and it runs after every test method, so a {@code @BeforeClass} property would
     * hold for the first inherited case and for none of the other six hundred.
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
     * The other half of the same guard, and the one the setting alone cannot give: that the run
     * this class is a copy of fuses anything at all. A decline rule widened far enough - a
     * family withdrawn, a spec discrimination added - would leave both runs unfused and every
     * inherited case comparing one path against itself, with the switch still off here and the
     * assertion above still green.
     * <p>
     * A lower bound rather than the number in this class's own javadoc: how many of the suite's
     * queries fuse is a function of the suite, and pinning it here would make an added case a
     * failure of the switch.
     */
    @Test
    public void testTheDefaultRunFusesWhatThisRunDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fusion_guard (ts TIMESTAMP, k SYMBOL, x DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY");
            // Three outputs on one window over one argument, which is the shape the suite's own
            // fusing queries are: a window carrying one fusible function forms no group.
            final String sql = "SELECT ts, sum(x) OVER w, avg(x) OVER w, count(x) OVER w "
                    + "FROM fusion_guard "
                    + "WINDOW w AS (PARTITION BY k ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
            WindowMapStateTest.assertIsBound(sql, false);
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "true");
            WindowMapStateTest.assertIsBound(sql, true);
        });
    }
}
