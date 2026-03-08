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
public class ModeBooleanGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testModeAllFalse() throws Exception {
        assertQuery(
                """
                        mode
                        false
                        """,
                "select mode(f) from tab",
                "create table tab as (select false as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeAllNull() throws Exception {
        assertQuery(
                """
                        mode
                        false
                        """,
                "select mode(f) from tab",
                "create table tab as (select null::boolean as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeAllTrue() throws Exception {
        assertQuery(
                """
                        mode
                        true
                        """,
                "select mode(f) from tab",
                "create table tab as (select true as f from long_sequence(5))",
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
                        true
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select true as f from long_sequence(3) " +
                        "union all " +
                        "select false as f from long_sequence(2)" +
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
                        false
                        """,
                "select mode(f) from tab",
                "create table tab (f boolean)",
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
                        true
                        """,
                "select mode(f) from tab",
                "create table tab as (select true as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    // 3 of each, tie breaker makes it true
    @Test
    public void testModeSomeNull() throws Exception {
        assertQuery(
                """
                        mode
                        true
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select null::boolean as f from long_sequence(2) " +
                        "union all " +
                        "select true as f from long_sequence(3) " +
                        "union all " +
                        "select false as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    /**
     * The temporary table unrolls to this:
     * <p>
     * | g | f     |<br>
     * | - | ----- |<br>
     * | A | true  |<br>
     * | A | true  |<br>
     * | A | false |<br>
     * | B | false |<br>
     * | B | false |<br>
     * | B | false |<br>
     * | B | true  |<br>
     * | C | false |<br>
     * | C | false |<br>
     * | C | true  |<br>
     * | C | true  |<br>
     * </p>
     */
    @Test
    public void testModeWithGroupBy() throws Exception {
        assertQuery(
                """
                        g\tmode
                        A\ttrue
                        B\tfalse
                        C\ttrue
                        """,
                "select g, mode(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, true as f from long_sequence(2) " +
                        "union all " +
                        "select 'A' as g, false as f from long_sequence(1) " +
                        "union all " +
                        "select 'B' as g, false as f from long_sequence(3) " +
                        "union all " +
                        "select 'B' as g, true as f from long_sequence(1)" +
                        "union all " +
                        "select 'C' as g, false as f from long_sequence(2) " +
                        "union all " +
                        "select 'C' as g, true as f from long_sequence(2)" +
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
                        true
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select true as f from long_sequence(60) " +
                        "union all " +
                        "select rnd_boolean() as f from long_sequence(40)" +
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
                        1970-01-01T00:00:00.000000Z\ttrue
                        1970-01-01T01:00:00.000000Z\ttrue
                        1970-01-01T02:00:00.000000Z\ttrue
                        """,
                "select k, mode(f) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "case when x % 3 = 0 then true " +
                        "when x % 3 = 1 then false " +
                        "else true end as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }
}