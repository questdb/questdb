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

package io.questdb.test.cairo.lv;

import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Semantic-validation rejects for CREATE LIVE VIEW that the grammar/shape validators
 * do not cover.
 * <p>
 * A live view is only well-defined if its result is a pure function of the base
 * table: the forward-append refresh emits survivors row by row and never retracts,
 * and an O3 or checkpoint replay recomputes ranges, so a non-deterministic function
 * anywhere in the body would let the view diverge from any recompute (permanently,
 * when it sits in a WHERE that admits or drops rows). CREATE therefore rejects
 * non-deterministic functions in the projection, the WHERE filter and window-function
 * arguments - the same guard materialized views arm around their SELECT. (The ANCHOR
 * EXPRESSION is validated separately by validateAnchorPurity and is not covered here.)
 */
public class LiveViewValidationTest extends AbstractCairoTest {

    @Test
    public void testRejectNonDeterministicFunctionInWhere() throws Exception {
        // WHERE is the worst case: a row admitted on one random draw cannot be
        // un-emitted, so the row set diverges permanently from any recompute.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE v > rnd_double()");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE ts > now()");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE ts > systimestamp()");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE ts > sysdate()");
        });
    }

    @Test
    public void testRejectNonDeterministicFunctionInWindowArg() throws Exception {
        // Non-determinism nested in a window expression stays on the window fast path
        // and its argument compiles under the LV context, yielding timing-dependent
        // output; the guard must reach into the window-function argument too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String frame = " OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base";
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + now()::long)" + frame);
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + systimestamp()::long)" + frame);
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + sysdate()::long)" + frame);
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + rnd_double(0))" + frame);
        });
    }

    private void assertLiveViewCreateRejected(String selectSql) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " + selectSql);
            // Should not reach here; drop defensively so a spurious success does not
            // leave a view that trips the next assertion on the same name.
            execute("DROP LIVE VIEW lv");
            Assert.fail("expected non-deterministic function reject for: " + selectSql);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for: " + selectSql,
                    Chars.contains(e.getFlyweightMessage(), "non-deterministic function cannot be used in materialized view")
            );
        }
    }
}
