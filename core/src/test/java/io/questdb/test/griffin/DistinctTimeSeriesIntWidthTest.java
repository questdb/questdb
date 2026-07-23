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

package io.questdb.test.griffin;

import io.questdb.griffin.engine.groupby.DistinctTimeSeriesRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Pins the INT-width store contract for {@link DistinctTimeSeriesRecordCursorFactory}. That factory
 * is a live pass-through - its cursor hands the base record straight through and the dataMap only
 * detects adjacent duplicates - so an overflowing INT projection under DISTINCT must widen on store,
 * exactly like the filter / limit / latest-by / join-master wrappers. Without the
 * {@code isColumnIntWidthStable} delegation the factory kept the conservative default true and
 * truncated the stored value.
 * <p>
 * The factory is only reachable with the distinct-to-GROUP BY rewrite disabled, which has no
 * production property and is overridden to false on the {@link CairoTestConfiguration} in
 * {@link #setUpStatic()}; a running server always rewrites plain SELECT DISTINCT to (Async) GROUP BY,
 * whose map-backed cursor materialises the value into a 4-byte slot and correctly keeps the default.
 * This is therefore a factory-consistency guarantee rather than a user-visible production path.
 */
public class DistinctTimeSeriesIntWidthTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean isSqlDistinctGroupByRewriteEnabled() {
                        return false;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testStoreWidensOverflowingIntExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (2_000_000_000, 2_000_000_000, '2024-01-01T00:00:00.000000Z')");

            // pin DistinctTimeSeries and confirm the INT projection itself wraps (getInt width)
            assertQuery("SELECT DISTINCT (a + b) AS v, ts FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("DistinctTimeSeries")
                    .returns("v\tts\n-294967296\t2024-01-01T00:00:00.000000Z\n");

            // store through DISTINCT must widen to the full value
            execute("CREATE TABLE dd (l LONG, ts TIMESTAMP)");
            execute("INSERT INTO dd SELECT DISTINCT (a + b), ts FROM t");
            assertQuery("SELECT l FROM dd").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // the same store without DISTINCT produces the identical value
            execute("CREATE TABLE sd (l LONG, ts TIMESTAMP)");
            execute("INSERT INTO sd SELECT (a + b), ts FROM t");
            assertQuery("SELECT l FROM sd").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // and the explicit ::long cast under DISTINCT agrees too
            execute("CREATE TABLE cd (l LONG, ts TIMESTAMP)");
            execute("INSERT INTO cd SELECT DISTINCT (a + b)::long, ts FROM t");
            assertQuery("SELECT l FROM cd").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // a plain INT column under DISTINCT has no wider value: it must keep its INT-width read
            // and NOT over-read the 4-byte slot
            execute("INSERT INTO t VALUES (-2_147_483_648, 7, '2024-01-02T00:00:00.000000Z')");
            execute("CREATE TABLE pd (l LONG, ts TIMESTAMP)");
            execute("INSERT INTO pd SELECT DISTINCT a, ts FROM t");
            assertQuery("SELECT l FROM pd ORDER BY l").noLeakCheck().expectSize().returns("l\nnull\n2000000000\n");
        });
    }
}
