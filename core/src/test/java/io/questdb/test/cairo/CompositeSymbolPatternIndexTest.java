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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The adaptive symbol-pattern index -- LIKE/ILIKE/regex on an indexed SYMBOL column -- on a composite
 * table, against its plain twin.
 * <p>
 * That path arrived from upstream master and had never seen a composite table. It is index-driven, so
 * its page frames arrive in (day ASC, cellKey ASC) -- CELL-MAJOR -- order, while the metadata it
 * advertises says designated-timestamp order. Every other index-driven path on this branch is wrapped
 * in an ascending timestamp sort for exactly that reason; this one was not, because it did not exist
 * when those wraps were written.
 * <p>
 * This is the "new code assuming one partition per day" class that this branch's merge checklist
 * exists to catch, and it is why that checklist is worth running rather than trusting a clean
 * auto-merge: nothing conflicted here. The compiler was happy and the rows came back in the wrong
 * order.
 * <p>
 * ORDER matters to the assertion, not just membership: a wrong result here is a correctly-sized set of
 * correct rows in cell-major sequence, which every count- or set-based check would pass.
 */
public class CompositeSymbolPatternIndexTest extends AbstractCairoTest {

    private static final String COLUMNS = "(ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE)";

    @Test
    public void testIlikeOnIndexedSymbolMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            assertTwins("SELECT * FROM %s WHERE sym ILIKE 'aa%%' ORDER BY ts");
        });
    }

    @Test
    public void testLikeOnIndexedSymbolMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            // No ORDER BY: the scan's own emission order is what is being compared, which is the
            // whole point -- an ORDER BY would sort the defect away.
            assertTwins("SELECT * FROM %s WHERE sym LIKE 'aa%%'");
        });
    }

    @Test
    public void testRegexOnIndexedSymbolMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            assertTwins("SELECT * FROM %s WHERE sym ~ '^aa'");
        });
    }

    private void assertTwins(String queryTemplate) throws Exception {
        assertSqlCursors(
                String.format(queryTemplate, "p"),
                String.format(queryTemplate, "c")
        );
    }

    private void createAndSeed() throws Exception {
        execute("CREATE TABLE src AS (SELECT"
                + " timestamp_sequence(1672531200000000L, 3600000000L) ts,"
                + " rnd_symbol('E0','E1','E2') exch,"
                + " rnd_symbol('aa1','aa2','bb1','bb2') sym,"
                + " rnd_double() px"
                + " FROM long_sequence(240)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE c " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO c SELECT * FROM src");
        execute("INSERT INTO p SELECT * FROM src");
        drainWalQueue();
    }
}
