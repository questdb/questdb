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
public class ModeVarcharGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testModeAllNull() throws Exception {
        assertQuery(
                """
                        mode
                        
                        """,
                "select mode(f) from tab",
                "create table tab as (select null::varchar as f from long_sequence(5))",
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
                        varchar_hello
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('varchar_' || x as varchar) as f from long_sequence(5) " +
                        "union all " +
                        "select cast('varchar_hello' as varchar) as f from long_sequence(3)" +
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
                        
                        """,
                "select mode(f) from tab",
                "create table tab (f varchar)",
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
                        single_varchar
                        """,
                "select mode(f) from tab",
                "create table tab as (select cast('single_varchar' as varchar) as f from long_sequence(1))",
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
                        common_varchar
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select null::varchar as f from long_sequence(2) " +
                        "union all " +
                        "select cast('common_varchar' as varchar) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('rare_varchar' as varchar) as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithEmptyVarchar() throws Exception {
        assertQuery(
                """
                        mode
                        
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('' as varchar) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('nonempty_varchar' as varchar) as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    /**
     * The temporary table unrolls to this:
     * <p>
     * | g | f             |<br>
     * | - | ------------- |<br>
     * | A | alpha_varchar |<br>
     * | A | alpha_varchar |<br>
     * | A | alpha_varchar |<br>
     * | A | other_varchar |<br>
     * | B | beta_varchar  |<br>
     * | B | beta_varchar  |<br>
     * | B | gamma_varchar |<br>
     * </p>
     */
    @Test
    public void testModeWithGroupBy() throws Exception {
        assertQuery(
                """
                        g\tmode
                        A\talpha_varchar
                        B\tbeta_varchar
                        """,
                "select g, mode(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, cast('alpha_varchar' as varchar) as f from long_sequence(3) " +
                        "union all " +
                        "select 'A' as g, cast('other_varchar' as varchar) as f from long_sequence(1) " +
                        "union all " +
                        "select 'B' as g, cast('beta_varchar' as varchar) as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, cast('gamma_varchar' as varchar) as f from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithLongVarchar() throws Exception {
        assertQuery(
                """
                        mode
                        very_long_varchar_value_that_exceeds_typical_string_length_limits_and_tests_memory_handling
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('very_long_varchar_value_that_exceeds_typical_string_length_limits_and_tests_memory_handling' as varchar) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('short_varchar' as varchar) as f from long_sequence(2)" +
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
                        0\tvarchar_group_0
                        1\tvarchar_group_1
                        2\tvarchar_group_2
                        3\tvarchar_group_3
                        4\tvarchar_group_4
                        """,
                "select g, mode(f) from tab order by g limit 5",
                "create table tab as (" +
                        "select " +
                        "x % 100 as g, " +
                        "cast(case when (x % 10) < 5 then 'varchar_group_' || (x % 100) else 'alt_varchar_' || (x % 100) || '_' || x end as varchar) as f " +
                        "from long_sequence(2000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithNumericVarchar() throws Exception {
        assertQuery(
                """
                        mode
                        12345.6789
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('12345.6789' as varchar) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('98765.4321' as varchar) as f from long_sequence(2) " +
                        "union all " +
                        "select cast('11111.1111' as varchar) as f from long_sequence(1)" +
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
                        E\techo_varchar
                        D\tdelta_varchar
                        C\tcharlie_varchar
                        """,
                "select g, mode(f) from tab order by g desc limit 3",
                "create table tab as (" +
                        "select " +
                        "case when x % 5 = 1 then 'A' " +
                        "when x % 5 = 2 then 'B' " +
                        "when x % 5 = 3 then 'C' " +
                        "when x % 5 = 4 then 'D' " +
                        "else 'E' end as g, " +
                        "cast(case when x % 5 = 1 then 'alpha_varchar' " +
                        "when x % 5 = 2 then 'bravo_varchar' " +
                        "when x % 5 = 3 then 'charlie_varchar' " +
                        "when x % 5 = 4 then 'delta_varchar' " +
                        "else 'echo_varchar' end as varchar) as f " +
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
                        constant_varchar
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('constant_varchar' as varchar) as f from long_sequence(10) " +
                        "union all " +
                        "select cast('random_varchar_' || cast(rnd_long(1, 999999, 0) as string) as varchar) as f from long_sequence(50)" +
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
                        1970-01-01T00:00:00.000000Z\tone_varchar
                        1970-01-01T01:00:00.000000Z\ttwo_varchar
                        1970-01-01T02:00:00.000000Z\tthree_varchar
                        """,
                "select k, mode(f) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "cast(case when x % 3 = 1 then 'one_varchar' " +
                        "when x % 3 = 2 then 'two_varchar' " +
                        "else 'three_varchar' end as varchar) as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testModeWithSpecialCharacters() throws Exception {
        assertQuery(
                """
                        mode
                        special_chars_!@#$%^&*()
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('special_chars_!@#$%^&*()' as varchar) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('normal_varchar' as varchar) as f from long_sequence(2) " +
                        "union all " +
                        "select cast('another_varchar' as varchar) as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithUnicodeVarchar() throws Exception {
        assertQuery(
                """
                        mode
                        unicode_varchar_αβγ_中文
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('unicode_varchar_αβγ_中文' as varchar) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('other_unicode_varchar_🚀🌟' as varchar) as f from long_sequence(2) " +
                        "union all " +
                        "select cast('simple_varchar' as varchar) as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithWhereClause() throws Exception {
        assertQuery(
                """
                        mode
                        filtered_varchar
                        """,
                "select mode(f) from tab where filter < 5",
                "create table tab as (" +
                        "select " +
                        "x % 10 as filter, " +
                        "cast(case when x % 10 < 5 then 'filtered_varchar' else 'excluded_varchar_' || x end as varchar) as f " +
                        "from long_sequence(100)" +
                        ")",
                null,
                false,
                true
        );
    }
}