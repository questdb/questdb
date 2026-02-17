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
public class ModeStringGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testModeAllNull() throws Exception {
        assertQuery(
                """
                        mode
                        
                        """,
                "select mode(f) from tab",
                "create table tab as (select null::string as f from long_sequence(5))",
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
                        hello
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast(x as string) as f from long_sequence(5) " +
                        "union all " +
                        "select 'hello' as f from long_sequence(3)" +
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
                "create table tab (f string)",
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
                        world
                        """,
                "select mode(f) from tab",
                "create table tab as (select 'world' as f from long_sequence(1))",
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
                        test
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select null::string as f from long_sequence(2) " +
                        "union all " +
                        "select 'test' as f from long_sequence(3) " +
                        "union all " +
                        "select 'other' as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithEmptyStrings() throws Exception {
        assertQuery(
                """
                        mode
                        
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select '' as f from long_sequence(3) " +
                        "union all " +
                        "select 'nonempty' as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }


    /**
     * The temporary table unrolls to this:
     * <p>
     * | g | f          |<br>
     * | - | ---------- |<br>
     * | A | alphabetic |<br>
     * | A | alphabetic |<br>
     * | A | alphabetic |<br>
     * | A | other      |<br>
     * | B | beta       |<br>
     * | B | beta       |<br>
     * | B | gamma      |<br>
     * </p>
     */
    @Test
    public void testModeWithGroupBy() throws Exception {
        assertQuery(
                """
                        g\tmode
                        A\talphabetic
                        B\tbeta
                        """,
                "select g, mode(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, 'alphabetic' as f from long_sequence(3) " +
                        "union all " +
                        "select 'A' as g, 'other' as f from long_sequence(1) " +
                        "union all " +
                        "select 'B' as g, 'beta' as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, 'gamma' as f from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithLongStrings() throws Exception {
        assertQuery(
                """
                        mode
                        this_is_a_very_long_string_that_should_be_handled_correctly_by_the_mode_function
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'this_is_a_very_long_string_that_should_be_handled_correctly_by_the_mode_function' as f from long_sequence(3) " +
                        "union all " +
                        "select 'short' as f from long_sequence(2)" +
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
                        0\tgroup0
                        1\tgroup1
                        2\tgroup2
                        3\tgroup3
                        4\tgroup4
                        """,
                "select g, mode(f) from tab order by g limit 5",
                "create table tab as (" +
                        "select " +
                        "x % 100 as g, " +
                        "case when (x % 10) < 5 then 'group' || (x % 100) else 'other' || (x % 100) || '_' || x end as f " +
                        "from long_sequence(2000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithOrderByAndLimit() throws Exception {
        assertQuery(
                """
                        g\tmode
                        E\techo
                        D\tdelta
                        C\tcharlie
                        """,
                "select g, mode(f) from tab order by g desc limit 3",
                "create table tab as (" +
                        "select " +
                        "case when x % 5 = 1 then 'A' " +
                        "when x % 5 = 2 then 'B' " +
                        "when x % 5 = 3 then 'C' " +
                        "when x % 5 = 4 then 'D' " +
                        "else 'E' end as g, " +
                        "case when x % 5 = 1 then 'alpha' " +
                        "when x % 5 = 2 then 'bravo' " +
                        "when x % 5 = 3 then 'charlie' " +
                        "when x % 5 = 4 then 'delta' " +
                        "else 'echo' end as f " +
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
                        constant_value
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'constant_value' as f from long_sequence(10) " +
                        "union all " +
                        "select 'random_' || cast(rnd_long(1, 999999, 0) as string) as f from long_sequence(50)" +
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
                        1970-01-01T00:00:00.000000Z\tone
                        1970-01-01T01:00:00.000000Z\ttwo
                        1970-01-01T02:00:00.000000Z\tthree
                        """,
                "select k, mode(f) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "case when x % 3 = 1 then 'one' " +
                        "when x % 3 = 2 then 'two' " +
                        "else 'three' end as f, " +
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
                        special!@#$%^&*()
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'special!@#$%^&*()' as f from long_sequence(3) " +
                        "union all " +
                        "select 'normal_text' as f from long_sequence(2) " +
                        "union all " +
                        "select 'another_string' as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithUnicodeStrings() throws Exception {
        assertQuery(
                """
                        mode
                        unicode_αβγ_δε
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'unicode_αβγ_δε' as f from long_sequence(3) " +
                        "union all " +
                        "select 'other_unicode_中文' as f from long_sequence(2) " +
                        "union all " +
                        "select 'emoji_🚀🌟' as f from long_sequence(1)" +
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
                        filtered_value
                        """,
                "select mode(f) from tab where filter < 5",
                "create table tab as (" +
                        "select " +
                        "x % 10 as filter, " +
                        "case when x % 10 < 5 then 'filtered_value' else 'excluded_' || x end as f " +
                        "from long_sequence(100)" +
                        ")",
                null,
                false,
                true
        );
    }
}