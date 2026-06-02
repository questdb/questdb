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
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (select null::symbol as f from long_sequence(5))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        
                        """);
    }

    @Test
    public void testModeBasic() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (" +
                        "select cast('SYM_' || x as symbol) as f from long_sequence(5) " +
                        "union all " +
                        "select cast('SYM_A' as symbol) as f from long_sequence(3)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        SYM_A
                        """);
    }

    @Test
    public void testModeConstantOverEmpty() throws Exception {
        // mode(constant) over a WHERE-folded empty table must return NULL: getInt
        // yields VALUE_IS_NULL when no rows were aggregated, and SymbolConstant.valueOf
        // must honour that key. Before the fix the constant was returned verbatim.
        assertQuery("select mode(('0.83055')::symbol) a0 from tab where 1 = 0")
                .ddl("create table tab as (select rnd_int() a from long_sequence(10))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        a0
                        
                        """);
    }

    @Test
    public void testModeEmpty() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab (f symbol)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        
                        """);
    }

    @Test
    public void testModeSingleValue() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (select cast('SINGLE' as symbol) as f from long_sequence(1))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        SINGLE
                        """);
    }

    @Test
    public void testModeSomeNull() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (" +
                        "select null::symbol as f from long_sequence(2) " +
                        "union all " +
                        "select cast('COMMON' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('RARE' as symbol) as f from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        COMMON
                        """);
    }

    @Test
    public void testModeWithDuplicateSymbolsInDifferentCases() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (" +
                        "select cast('lowercase' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('UPPERCASE' as symbol) as f from long_sequence(2) " +
                        "union all " +
                        "select cast('MixedCase' as symbol) as f from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        lowercase
                        """);
    }

    @Test
    public void testModeWithEmptySymbols() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (" +
                        "select cast('' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('NONEMPTY' as symbol) as f from long_sequence(2)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        
                        """);
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
        assertQuery("select g, mode(f) from tab order by g")
                .ddl("create table tab as (" +
                        "select 'A' as g, cast('SYM_ALPHA' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select 'A' as g, cast('SYM_OTHER' as symbol) as f from long_sequence(1) " +
                        "union all " +
                        "select 'B' as g, cast('SYM_BETA' as symbol) as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, cast('SYM_GAMMA' as symbol) as f from long_sequence(1)" +
                        ")")
                .expectSize()
                .returns("""
                        g\tmode
                        A\tSYM_ALPHA
                        B\tSYM_BETA
                        """);
    }

    @Test
    public void testModeWithManyGroups() throws Exception {
        assertQuery("select g, mode(f) from tab order by g limit 5")
                .ddl("create table tab as (" +
                        "select " +
                        "x % 100 as g, " +
                        "cast(case when (x % 10) < 5 then 'SYM_' || (x % 100) else 'ALT_' || (x % 100) || '_' || x end as symbol) as f " +
                        "from long_sequence(2000)" +
                        ")")
                .expectSize()
                .returns("""
                        g\tmode
                        0\tSYM_0
                        1\tSYM_1
                        2\tSYM_2
                        3\tSYM_3
                        4\tSYM_4
                        """);
    }

    @Test
    public void testModeWithNumericSymbols() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (" +
                        "select cast('12345' as symbol) as f from long_sequence(3) " +
                        "union all " +
                        "select cast('67890' as symbol) as f from long_sequence(2) " +
                        "union all " +
                        "select cast('54321' as symbol) as f from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        12345
                        """);
    }

    @Test
    public void testModeWithOrderByAndLimit() throws Exception {
        assertQuery("select g, mode(f) from tab order by g desc limit 3")
                .ddl("create table tab as (" +
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
                        ")")
                .expectSize()
                .returns("""
                        g\tmode
                        E\tECHO
                        D\tDELTA
                        C\tCHARLIE
                        """);
    }

    @Test
    public void testModeWithRandomData() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (" +
                        "select cast('FIXED_SYMBOL' as symbol) as f from long_sequence(10) " +
                        "union all " +
                        "select cast('RND_' || cast(rnd_long(1, 999999, 0) as string) as symbol) as f from long_sequence(50)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        FIXED_SYMBOL
                        """);
    }

    @Test
    public void testModeWithSampleBy() throws Exception {
        assertQuery("select k, mode(f) from tab sample by 1h")
                .ddl("create table tab as (" +
                        "select " +
                        "cast(case when x % 3 = 1 then 'ONE' " +
                        "when x % 3 = 2 then 'TWO' " +
                        "else 'THREE' end as symbol) as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR")
                .timestamp("k")
                .expectSize()
                .returns("""
                        k\tmode
                        1970-01-01T00:00:00.000000Z\tONE
                        1970-01-01T01:00:00.000000Z\tTWO
                        1970-01-01T02:00:00.000000Z\tTHREE
                        """);
    }

    @Test
    public void testModeWithSymbolCapacity() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("""
                        create table tab as (
                        select f::symbol f FROM (
                        select cast('CAPACITY_TEST' as symbol) as f from long_sequence(5)
                        union all
                        select cast('OTHER_' || x as symbol) as f from long_sequence(3)
                        ))""")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        CAPACITY_TEST
                        """);
    }

    @Test
    public void testModeWithSymbolIndex() throws Exception {
        assertQuery("select mode(f) from tab")
                .ddl("create table tab as (" +
                        "select f::symbol f FROM (" +
                        "select 'IDX_SYMBOL'::symbol as f from long_sequence(3) " +
                        "union all " +
                        "select 'OTHER_SYMBOL'::symbol as f from long_sequence(2)" +
                        ")), index(f)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        IDX_SYMBOL
                        """);
    }

    @Test
    public void testModeWithWhereClause() throws Exception {
        assertQuery("select mode(f) from tab where filter < 5")
                .ddl("create table tab as (" +
                        "select " +
                        "x % 10 as filter, " +
                        "cast(case when x % 10 < 5 then 'FILTERED' else 'EXCLUDED_' || x end as symbol) as f " +
                        "from long_sequence(100)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        mode
                        FILTERED
                        """);
    }
}
