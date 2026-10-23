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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Join, HORIZON JOIN and set operation errors that the statement text and catalog decide are raised while
 * binding, so they surface even when the optimiser prunes the sub-query that holds them or folds the
 * predicate that reads it, and when a view's query holds them.
 */
public class StaticJoinErrorTest extends AbstractCairoTest {

    @Test
    public void testAsofDesignatedTimestampKey() throws Exception {
        assertStaticError("SELECT p.price FROM trades t ASOF JOIN prices p ON (sym, ts)",
                29, "ASOF/LT JOIN cannot use designated timestamp as a join key");
    }

    @Test
    public void testAsofToleranceUnit() throws Exception {
        assertStaticError("SELECT p.price FROM trades t ASOF JOIN prices p ON (sym) TOLERANCE 1y",
                67, "unsupported TOLERANCE unit [unit=y]");
    }

    @Test
    public void testHorizonJoinKeyTypeMismatch() throws Exception {
        assertStaticError("SELECT avg(p.price) FROM trades t HORIZON JOIN prices p ON (t.price = p.sym) RANGE FROM 0s TO 2s STEP 1s AS h",
                70, "join column type mismatch");
    }

    @Test
    public void testHorizonJoinListOrder() throws Exception {
        assertStaticError("SELECT avg(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (2s, 1s) AS h",
                85, "LIST offsets must be monotonically increasing");
    }

    @Test
    public void testHorizonJoinRangeBounds() throws Exception {
        assertStaticError("SELECT avg(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) RANGE FROM 5s TO 2s STEP 1s AS h",
                86, "FROM must be less than or equal to TO");
    }

    @Test
    public void testHorizonJoinRightTimestamp() throws Exception {
        assertStaticError("SELECT avg(p.price) FROM trades t HORIZON JOIN plain p ON (t.sym = p.sym) RANGE FROM 0s TO 2s STEP 1s AS h",
                34, "right side of time series join has no timestamp");
    }

    @Test
    public void testJoinKeyTypeMismatch() throws Exception {
        assertStaticError("SELECT p.price FROM trades t JOIN prices p ON t.price = p.sym",
                56, "join column type mismatch");
    }

    @Test
    public void testSetOperationCast() throws Exception {
        assertStaticError("SELECT c FROM (SELECT 'a'::CHAR c FROM trades UNION ALL SELECT price FROM prices)",
                15, "unsupported cast [column=c, from=CHAR, to=DOUBLE]");
    }

    @Test
    public void testSpliceJoinExpression() throws Exception {
        assertStaticError("SELECT p.price FROM trades t SPLICE JOIN prices p ON t.price < p.price",
                61, "unsupported SPLICE join expression [expr='t.price < p.price']");
    }

    @Test
    public void testSpliceJoinLeftTimestamp() throws Exception {
        assertStaticError("SELECT p.price FROM plain t SPLICE JOIN prices p ON (sym)",
                28, "left side of time series join has no timestamp");
    }

    @Test
    public void testWindowJoinBounds() throws Exception {
        assertStaticError("SELECT sum(p.price) FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym) RANGE BETWEEN 2 minute PRECEDING AND 4 minute PRECEDING",
                111, "WINDOW join hi value cannot be less than lo value");
    }

    @Test
    public void testWindowJoinRightTimestamp() throws Exception {
        assertStaticError("SELECT sum(p.price) FROM trades t WINDOW JOIN plain p ON (t.sym = p.sym) RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING",
                34, "right side of time series join has no timestamp");
    }

    private void assertStaticError(String subquery, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE plain (sym SYMBOL, price DOUBLE, ts TIMESTAMP)");
            assertQuery(subquery).noLeakCheck().fails(position, message);
            final String folded = "SELECT price FROM trades WHERE true OR price = (";
            assertQuery(folded + subquery + ")").noLeakCheck().fails(folded.length() + position, message);
            final String pruned = "SELECT price FROM (SELECT price, coalesce((";
            assertQuery(pruned + subquery + "), 0) v FROM trades)").noLeakCheck().fails(pruned.length() + position, message);
            final String view = "CREATE VIEW v AS (";
            assertException(view + subquery + ")", view.length() + position, message);
        });
    }
}
