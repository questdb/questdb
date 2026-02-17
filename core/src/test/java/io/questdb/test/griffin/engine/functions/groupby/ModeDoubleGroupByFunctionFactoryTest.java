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
public class ModeDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testModeAllNull() throws Exception {
        assertQuery(
                """
                        mode
                        null
                        """,
                "select mode(f) from tab",
                "create table tab as (select null::double as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeBasic() throws Exception {
        assertQuery(
                """
                        mode
                        1.0
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select x::double as f from long_sequence(5) " +
                        "union all " +
                        "select 1.0 as f from long_sequence(3)" +
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
                        mode
                        null
                        """,
                "select mode(f) from tab",
                "create table tab (f double)",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeSingleValue() throws Exception {
        assertQuery(
                """
                        mode
                        42.5
                        """,
                "select mode(f) from tab",
                "create table tab as (select 42.5 as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeSomeNull() throws Exception {
        assertQuery(
                """
                        mode
                        5.5
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select null::double as f from long_sequence(2) " +
                        "union all " +
                        "select 5.5 as f from long_sequence(3) " +
                        "union all " +
                        "select 7.7 as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    /**
     * The temporary table unrolls to this:
     * <p>
     * | g | f    |<br>
     * | - | ---- |<br>
     * | A | 10.5 |<br>
     * | A | 10.5 |<br>
     * | A | 10.5 |<br>
     * | A | 20.5 |<br>
     * | B | 20.5 |<br>
     * | B | 20.5 |<br>
     * | B | 30.5 |<br>
     * </p>
     */
    @Test
    public void testModeWithGroupBy() throws Exception {
        assertQuery(
                """
                        g\tmode
                        A\t10.5
                        B\t20.5
                        """,
                "select g, mode(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, 10.5 as f from long_sequence(3) " +
                        "union all " +
                        "select 'A' as g, 20.5 as f from long_sequence(1) " +
                        "union all " +
                        "select 'B' as g, 20.5 as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, 30.5 as f from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithLargeValues() throws Exception {
        assertQuery(
                """
                        mode
                        1.7976931348623157E308
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 1.7976931348623157E308 as f from long_sequence(3) " +
                        "union all " +
                        "select -1.7976931348623157E308 as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithManyGroups() throws Exception {
        assertQuery(
                """
                        g\tmode
                        0\t0.0
                        1\t1000.0
                        2\t2000.0
                        3\t3000.0
                        4\t4000.0
                        """,
                "select g, mode(f) from tab order by g limit 5",
                "create table tab as (" +
                        "select " +
                        "x % 100 as g, " +
                        "case when (x % 10) < 5 then (x % 100) * 1000.0 else (x % 100) * 1000.0 + x::double end as f " +
                        "from long_sequence(2000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithNegativeValues() throws Exception {
        assertQuery(
                """
                        mode
                        -1.1
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select -1.1 as f from long_sequence(3) " +
                        "union all " +
                        "select -2.2 as f from long_sequence(2) " +
                        "union all " +
                        "select -3.3 as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithOrderByAndLimit() throws Exception {
        assertQuery(
                """
                        g\tmode
                        E\t40.5
                        D\t30.5
                        C\t20.5
                        """,
                "select g, mode(f) from tab order by g desc limit 3",
                "create table tab as (" +
                        "select " +
                        "case when x % 5 = 1 then 'A' " +
                        "when x % 5 = 2 then 'B' " +
                        "when x % 5 = 3 then 'C' " +
                        "when x % 5 = 4 then 'D' " +
                        "else 'E' end as g, " +
                        "case when x % 5 = 1 then 10.5 " +
                        "when x % 5 = 2 then 20.5 " +
                        "when x % 5 = 3 then 20.5 " +
                        "when x % 5 = 4 then 30.5 " +
                        "else 40.5 end as f " +
                        "from long_sequence(100)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithRandomData() throws Exception {
        assertQuery(
                """
                        mode
                        123456.789
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 123456.789 as f from long_sequence(10) " +
                        "union all " +
                        "select rnd_double(0) * 999999.0 + 1.0 as f from long_sequence(50)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithSampleBy() throws Exception {
        assertQuery(
                """
                        k\tmode
                        1970-01-01T00:00:00.000000Z\t1.1
                        1970-01-01T01:00:00.000000Z\t2.2
                        1970-01-01T02:00:00.000000Z\t3.3
                        """,
                "select k, mode(f) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "case when x % 3 = 1 then 1.1 " +
                        "when x % 3 = 2 then 2.2 " +
                        "else 3.3 end as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testModeWithWhereClause() throws Exception {
        assertQuery(
                """
                        mode
                        100.5
                        """,
                "select mode(f) from tab where filter < 5",
                "create table tab as (" +
                        "select " +
                        "x % 10 as filter, " +
                        "case when x % 10 < 5 then 100.5 else x::double end as f " +
                        "from long_sequence(100)" +
                        ")",
                null,
                false,
                true
        );
    }
}