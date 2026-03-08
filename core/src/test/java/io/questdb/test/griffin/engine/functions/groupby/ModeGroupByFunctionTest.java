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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Temporary tables are used in this class, generated via union and long_sequence.
 * Please check the comment on `testModeWithGroupBy` for clarity.
 */
public class ModeGroupByFunctionTest extends AbstractCairoTest {

    @Test
    public void testModeAllNull() throws Exception {
        assertQuery(
                """
                        mode_long\tmode_string\tmode_varchar
                        null\t\t
                        """,
                "select mode(l) as mode_long, mode(s) as mode_string, mode(v) as mode_varchar from tab",
                "create table tab as (" +
                        "select null::long as l, null::string as s, null::varchar as v from long_sequence(5)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeDouble() throws Exception {
        assertQuery(
                """
                        mode
                        1.5
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 1.5 as f from long_sequence(3) " +
                        "union all " +
                        "select 2.5 as f from long_sequence(1) " +
                        "union all " +
                        "select 3.5 as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeEmpty() throws Exception {
        assertQuery(
                """
                        mode_long\tmode_string\tmode_varchar
                        null\t\t
                        """,
                "select mode(l) as mode_long, mode(s) as mode_string, mode(v) as mode_varchar from tab",
                "create table tab (l long, s string, v varchar)",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeString() throws Exception {
        assertQuery(
                """
                        mode
                        apple
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'apple' as f from long_sequence(3) " +
                        "union all " +
                        "select 'banana' as f from long_sequence(1) " +
                        "union all " +
                        "select 'cherry' as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeSymbol() throws Exception {
        assertQuery(
                """
                        mode
                        AAPL
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'AAPL'::symbol as f from long_sequence(4) " +
                        "union all " +
                        "select 'MSFT'::symbol as f from long_sequence(2) " +
                        "union all " +
                        "select 'GOOGL'::symbol as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeVarchar() throws Exception {
        assertQuery(
                """
                        mode
                        hello
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'hello'::varchar as f from long_sequence(3) " +
                        "union all " +
                        "select 'world'::varchar as f from long_sequence(1) " +
                        "union all " +
                        "select 'test'::varchar as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }


    /**
     * The temporary table unrolls to this:
     * <p>
     * | trader | symbol |<br>
     * | ----- | ------- |<br>
     * | Alice | AAPL    |<br>
     * | Alice | AAPL    |<br>
     * | Alice | AAPL    |<br>
     * | Alice | GOOGL   |<br>
     * | Bob   | MSFT    |<br>
     * | Bob   | MSFT    |<br>
     * | Bob   | MSFT    |<br>
     * | Bob   | MSFT    |<br>
     * | Bob   | ISLA    |<br>
     * | Bob   | ISLA    |<br>
     * </p>
     */
    @Test
    public void testModeWithGroupBy() throws Exception {
        assertQuery(
                """
                        trader\tmost_frequent_symbol
                        Alice\tAAPL
                        Bob\tMSFT
                        """,
                "select trader, mode(symbol) as most_frequent_symbol from trades group by trader order by trader",
                "create table trades as (" +
                        "select 'Alice' as trader, 'AAPL'::symbol as symbol from long_sequence(3) " +
                        "union all " +
                        "select 'Alice' as trader, 'GOOGL'::symbol as symbol from long_sequence(1) " +
                        "union all " +
                        "select 'Bob' as trader, 'MSFT'::symbol as symbol from long_sequence(4) " +
                        "union all " +
                        "select 'Bob' as trader, 'TSLA'::symbol as symbol from long_sequence(2)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithHighCardinality() throws Exception {
        assertQuery(
                """
                        mode
                        frequent_value
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'frequent_value' as f from long_sequence(50) " +
                        "union all " +
                        "select 'value_' || x as f from long_sequence(1000)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table prices as (" +
                            "select 'AAPL'::symbol as symbol, 150.0 as price from long_sequence(1) " +
                            "union all " +
                            "select 'MSFT'::symbol as symbol, 200.0 as price from long_sequence(1)" +
                            "); ");
            execute(
                    "create table orders as (select 'AAPL'::symbol as symbol, 'BUY' as side from long_sequence(3) " +
                            "union all " +
                            "select 'AAPL'::symbol as symbol, 'SELL' as side from long_sequence(1) " +
                            "union all " +
                            "select 'MSFT'::symbol as symbol, 'SELL' as side from long_sequence(4) " +
                            "union all " +
                            "select 'MSFT'::symbol as symbol, 'BUY' as side from long_sequence(2))"
            );
            assertQueryNoLeakCheck(
                    """
                            symbol\tmode_side
                            AAPL\tBUY
                            MSFT\tSELL
                            """,
                    "select p.symbol, mode(o.side) as mode_side " +
                            "from prices p " +
                            "join orders o on p.symbol = o.symbol " +
                            "group by p.symbol " +
                            "order by p.symbol",
                    null,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testModeWithLargeDataset() throws Exception {
        assertQuery(
                """
                        mode
                        target
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'target' as f from long_sequence(100) " +
                        "union all " +
                        "select rnd_str(10, 10, 0) as f from long_sequence(5000)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithMixedDataTypes() throws Exception {
        assertQuery(
                """
                        mode_long\tmode_double\tmode_string\tmode_symbol\tmode_boolean
                        100\t1.5\tapple\tAAPL\ttrue
                        """,
                "select mode(l) mode_long, mode(d) mode_double, mode(s) mode_string, mode(sym) mode_symbol, mode(b) mode_boolean from tab",
                "create table tab as (" +
                        "select 100L as l, 1.5 as d, 'apple' as s, 'AAPL'::symbol as sym, true as b from long_sequence(3) " +
                        "union all " +
                        "select 200L as l, 2.5 as d, 'banana' as s, 'MSFT'::symbol as sym, false as b from long_sequence(1) " +
                        "union all " +
                        "select 300L as l, 3.5 as d, 'cherry' as s, 'GOOGL'::symbol as sym, true as b from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithNaNAndInfinity() throws Exception {
        assertQuery(
                """
                        mode
                        1.0
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 1.0 as f from long_sequence(3) " +
                        "union all " +
                        "select 'NaN'::double as f from long_sequence(1) " +
                        "union all " +
                        "select cast('+Infinity' as double) as f from long_sequence(1) " +
                        "union all " +
                        "select cast('-Infinity' as double) as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithNulls() throws Exception {
        assertQuery(
                """
                        mode
                        5
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select null::long as f from long_sequence(3) " +
                        "union all " +
                        "select 5L as f from long_sequence(4) " +
                        "union all " +
                        "select 10L as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithOrderBy() throws Exception {
        assertQuery(
                """
                        g\tmode
                        C\t3
                        B\t2
                        A\t1
                        """,
                "select g, mode(f) from tab group by g order by mode(f) desc",
                "create table tab as (" +
                        "select 'A' as g, 1L as f from long_sequence(5) " +
                        "union all " +
                        "select 'B' as g, 2L as f from long_sequence(5) " +
                        "union all " +
                        "select 'C' as g, 3L as f from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithSampleBy() throws Exception {
        assertQuery(
                """
                        k\tmode
                        1970-01-01T00:00:00.000000Z\tA
                        1970-01-01T01:00:00.000000Z\tB
                        1970-01-01T02:00:00.000000Z\tC
                        """,
                "select k, mode(f) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "case when x % 3 = 1 then 'A' " +
                        "when x % 3 = 2 then 'B' " +
                        "else 'C' end as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testModeWithSubquery() throws Exception {
        assertQuery(
                """
                        department\tmost_common_rating
                        Engineering\t85
                        Sales\t75
                        """,
                "select department, mode(rating) as most_common_rating " +
                        "from (" +
                        "  select department, rating " +
                        "  from employees " +
                        "  where rating >= 70" +
                        ") " +
                        "group by department " +
                        "order by department",
                "create table employees as (" +
                        "select 'Engineering' as department, 85L as rating from long_sequence(4) " +
                        "union all " +
                        "select 'Engineering' as department, 90L as rating from long_sequence(2) " +
                        "union all " +
                        "select 'Engineering' as department, 60L as rating from long_sequence(1) " +
                        "union all " +
                        "select 'Sales' as department, 75L as rating from long_sequence(3) " +
                        "union all " +
                        "select 'Sales' as department, 80L as rating from long_sequence(1) " +
                        "union all " +
                        "select 'Sales' as department, 65L as rating from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }
}