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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A materialized view over a COMPOSITE-partitioned base table is SUPPORTED as of 2026-08-26.
 * <p>
 * This class was the gate's regression lock, and its own doc set the exit condition: "Sub-project 7
 * replaces this gate with either proven support or a permanent refusal." The proof is
 * {@link io.questdb.test.cairo.CompositeMatViewTest} -- a view over a composite base against a view
 * over its plain twin, matching both at creation and after incremental refresh, including an
 * out-of-order insert into an already-populated cell. The suspicion recorded here turned out to be
 * right: refresh reads the base through ordinary SQL, which is twin-correct for composite.
 * <p>
 * The tests are kept rather than deleted, inverted to assert the combination WORKS -- including over
 * a dormant composite base, which was the edge the gate was most careful about.
 * <p>
 * {@link #testMatViewOverPlainBaseStillWorks()} is the POSITIVE CONTROL. Without it, the gate could
 * be rejecting every base table and this suite would still pass.
 */
public class CompositeMatViewGateTest extends AbstractCairoTest {

    @Test
    public void testMatViewOverCompositeBaseWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'BTC', 1.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS ("
                    + "SELECT ts, avg(px) AS ap FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalQueue();
            drainMatViewQueue(engine);
            drainWalQueue();
            assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'mv'")
                    .noLeakCheck().noRandomAccess()
                    .returns("view_status\nvalid\n");
        });
    }

    /**
     * A DORMANT composite base -- declared composite, no rows routed yet. This was the edge the gate
     * was most careful about, so it is kept, inverted: creating a view over it must work rather than
     * be refused for a routing state the base has not reached yet.
     */
    @Test
    public void testMatViewOverEmptyCompositeBaseWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_empty (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("CREATE MATERIALIZED VIEW mv_empty AS ("
                    + "SELECT ts, avg(px) AS ap FROM base_empty SAMPLE BY 1h) PARTITION BY DAY");
            drainWalQueue();
            drainMatViewQueue(engine);
            assertQuery("SELECT count() FROM mv_empty")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n0\n");
        });
    }

    /** POSITIVE CONTROL: a plain base must still work, or the gate is over-reaching. */
    @Test
    public void testMatViewOverPlainBaseStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base2 (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base2 VALUES ('2024-01-01T00:00:00.000000Z', 'BTC', 1.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv2 AS ("
                    + "SELECT ts, avg(px) AS ap FROM base2 SAMPLE BY 1h) PARTITION BY DAY");
        });
    }
}
