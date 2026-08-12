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
 * A materialized view over a COMPOSITE-partitioned base table is refused, loudly.
 * <p>
 * This gate is deliberately conservative rather than a statement that the combination is broken.
 * {@code cairo/mv/} contains no composite awareness and no composite mat-view test existed, so the
 * combination was simply unexercised: neither supported nor refused. Refresh reads the base through
 * ordinary SQL, which IS twin-correct for composite, so it may well work -- but this feature's rule
 * is that composite behaves exactly like its plain twin or fails loudly, and "probably fine" is
 * neither. Sub-project 7 replaces this gate with either proven support or a permanent refusal.
 * <p>
 * {@link #testMatViewOverPlainBaseStillWorks()} is the POSITIVE CONTROL. Without it, the gate could
 * be rejecting every base table and this suite would still pass.
 */
public class CompositeMatViewGateTest extends AbstractCairoTest {

    @Test
    public void testMatViewOverCompositeBaseIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'BTC', 1.0)");
            drainWalQueue();
            try {
                execute("CREATE MATERIALIZED VIEW mv AS ("
                        + "SELECT ts, avg(px) AS ap FROM base SAMPLE BY 1h) PARTITION BY DAY");
                Assert.fail("expected a composite base to be refused");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite");
            }
        });
    }

    /**
     * The gate must not fire for a DORMANT composite table either way round: a table declared
     * composite is composite whether or not it has routed a second cell yet, so this asserts the
     * refusal is driven by the declared partition spec, not by runtime routing state.
     */
    @Test
    public void testMatViewOverEmptyCompositeBaseIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_empty (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            try {
                execute("CREATE MATERIALIZED VIEW mv_empty AS ("
                        + "SELECT ts, avg(px) AS ap FROM base_empty SAMPLE BY 1h) PARTITION BY DAY");
                Assert.fail("expected a composite base to be refused even when empty");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite");
            }
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
