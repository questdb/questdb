/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
public class ModeSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testModeAllNull() throws Exception {
        assertQuery(
                "mode\n" +
                        "\n",
                "select mode(f) from tab",
                "create table tab as (select null::symbol as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeBasic() throws Exception {
        assertQuery(
                "mode\n" +
                        "SYM_A\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('SYM_' || x as symbol) as f from long_sequence(5) " +
                        "union all " +
                        "select cast('SYM_A' as symbol) as f from long_sequence(3)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeEmpty() throws Exception {
        assertQuery(
                "mode\n" +
                        "\n",
                "select mode(f) from tab",
                "create table tab (f symbol)",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeSingleValue() throws Exception {
        assertQuery(
                "mode\n" +
                        "SINGLE\n",
                "select mode(f) from tab",
                "create table tab as (select cast('SINGLE' as symbol) as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeSomeNull() throws Exception {
        assertQuery(
                "mode\n" +
                        "COMMON\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select null::symbol as f from long_sequence(2) " +
                        "union all " +
                        "select cast('COMMON' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('RARE' as symbol) as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithDuplicateSymbolsInDifferentCases() throws Exception {
        assertQuery(
                "mode\n" +
                        "lowercase\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('lowercase' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('UPPERCASE' as symbol) as f from long_sequence(2) " +
                        "union all " +
                        "select cast('MixedCase' as symbol) as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithEmptySymbols() throws Exception {
        assertQuery(
                "mode\n" +
                        "\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('NONEMPTY' as symbol) as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    /**
     * The temporary table unrolls to this:
     * <p>
     * | g | f         |<br>
     * | - | --------- |<br>
     * | A | SYM_ALPHA |<br>
     * | A | SYM_ALPHA |<br>
     * | A | SYM_ALPHA |<br>
     * | A | SYM_OTHER |<br>
     * | B | SYM_BETA  |<br>
     * | B | SYM_BETA  |<br>
     * | B | SYM_GAMMA |<br>
     * </p>
     */
    @Test
    public void testModeWithGroupBy() throws Exception {
        assertQuery(
                "g\tmode\n" +
                        "A\tSYM_ALPHA\n" +
                        "B\tSYM_BETA\n",
                "select g, mode(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, cast('SYM_ALPHA' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select 'A' as g, cast('SYM_OTHER' as symbol) as f from long_sequence(1) " +
                        "union all " +
                        "select 'B' as g, cast('SYM_BETA' as symbol) as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, cast('SYM_GAMMA' as symbol) as f from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithManyGroups() throws Exception {
        assertQuery(
                "g\tmode\n" +
                        "0\tSYM_0\n" +
                        "1\tSYM_1\n" +
                        "2\tSYM_2\n" +
                        "3\tSYM_3\n" +
                        "4\tSYM_4\n",
                "select g, mode(f) from tab order by g limit 5",
                "create table tab as (" +
                        "select " +
                        "x % 100 as g, " +
                        "cast(case when (x % 10) < 5 then 'SYM_' || (x % 100) else 'ALT_' || (x % 100) || '_' || x end as symbol) as f " +
                        "from long_sequence(2000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithNumericSymbols() throws Exception {
        assertQuery(
                "mode\n" +
                        "12345\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('12345' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('67890' as symbol) as f from long_sequence(2) " +
                        "union all " +
                        "select cast('54321' as symbol) as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithOrderByAndLimit() throws Exception {
        assertQuery(
                "g\tmode\n" +
                        "E\tECHO\n" +
                        "D\tDELTA\n" +
                        "C\tCHARLIE\n",
                "select g, mode(f) from tab order by g desc limit 3",
                "create table tab as (" +
                        "select " +
                        "case when x % 5 = 1 then 'A' " +
                        "when x % 5 = 2 then 'B' " +
                        "when x % 5 = 3 then 'C' " +
                        "when x % 5 = 4 then 'D' " +
                        "else 'E' end as g, " +
                        "cast(case when x % 5 = 1 then 'ALPHA' " +
                        "when x % 5 = 2 then 'BRAVO' " +
                        "when x % 5 = 3 then 'CHARLIE' " +
                        "when x % 5 = 4 then 'DELTA' " +
                        "else 'ECHO' end as symbol) as f " +
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
                "mode\n" +
                        "FIXED_SYMBOL\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select cast('FIXED_SYMBOL' as symbol) as f from long_sequence(10) " +
                        "union all " +
                        "select cast('RND_' || cast(rnd_long(1, 999999, 0) as string) as symbol) as f from long_sequence(50)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithSampleBy() throws Exception {
        assertQuery(
                "k\tmode\n" +
                        "1970-01-01T00:00:00.000000Z\tONE\n" +
                        "1970-01-01T01:00:00.000000Z\tTWO\n" +
                        "1970-01-01T02:00:00.000000Z\tTHREE\n",
                "select k, mode(f) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "cast(case when x % 3 = 1 then 'ONE' " +
                        "when x % 3 = 2 then 'TWO' " +
                        "else 'THREE' end as symbol) as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testModeWithSymbolCapacity() throws Exception {
        assertQuery(
                "mode\n" +
                        "CAPACITY_TEST\n",
                "select mode(f) from tab",
                "create table tab as (\n" +
                        "select f::symbol f FROM (\n" +
                        "select cast('CAPACITY_TEST' as symbol) as f from long_sequence(5)\n" +
                        "union all\n" +
                        "select cast('OTHER_' || x as symbol) as f from long_sequence(3)\n" +
                        "))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithSymbolIndex() throws Exception {
        assertQuery(
                "mode\n" +
                        "IDX_SYMBOL\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select f::symbol f FROM (" +
                        "select 'IDX_SYMBOL'::symbol as f from long_sequence(3) " +
                        "union all " +
                        "select 'OTHER_SYMBOL'::symbol as f from long_sequence(2)" +
                        ")), index(f)",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithWhereClause() throws Exception {
        assertQuery(
                "mode\n" +
                        "FILTERED\n",
                "select mode(f) from tab where filter < 5",
                "create table tab as (" +
                        "select " +
                        "x % 10 as filter, " +
                        "cast(case when x % 10 < 5 then 'FILTERED' else 'EXCLUDED_' || x end as symbol) as f " +
                        "from long_sequence(100)" +
                        ")",
                null,
                false,
                true
        );
    }
}