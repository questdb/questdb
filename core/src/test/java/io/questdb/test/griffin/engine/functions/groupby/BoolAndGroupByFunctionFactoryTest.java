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

public class BoolAndGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBoolAndAllTrue() throws Exception {
        assertQuery(
                """
                        bool_and
                        true
                        """,
                "select bool_and(f) from tab",
                "create table tab as (select true as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolAndAllFalse() throws Exception {
        assertQuery(
                """
                        bool_and
                        false
                        """,
                "select bool_and(f) from tab",
                "create table tab as (select false as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolAndMixed() throws Exception {
        // One false makes the entire result false
        assertQuery(
                """
                        bool_and
                        false
                        """,
                "select bool_and(f) from tab",
                "create table tab as (" +
                        "select true as f from long_sequence(3) " +
                        "union all " +
                        "select false as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolAndEmpty() throws Exception {
        // Empty table returns false (identity for AND in terms of aggregation)
        assertQuery(
                """
                        bool_and
                        false
                        """,
                "select bool_and(f) from tab",
                "create table tab (f boolean)",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolAndSingleTrue() throws Exception {
        assertQuery(
                """
                        bool_and
                        true
                        """,
                "select bool_and(f) from tab",
                "create table tab as (select true as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolAndSingleFalse() throws Exception {
        assertQuery(
                """
                        bool_and
                        false
                        """,
                "select bool_and(f) from tab",
                "create table tab as (select false as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolAndWithGroupBy() throws Exception {
        assertQuery(
                """
                        g\tbool_and
                        A\ttrue
                        B\tfalse
                        C\tfalse
                        """,
                "select g, bool_and(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, true as f from long_sequence(3) " +
                        "union all " +
                        "select 'B' as g, false as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, true as f from long_sequence(1) " +
                        "union all " +
                        "select 'C' as g, true as f from long_sequence(2) " +
                        "union all " +
                        "select 'C' as g, false as f from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBoolAndWithExpression() throws Exception {
        // Using a boolean expression
        assertQuery(
                """
                        bool_and
                        false
                        """,
                "select bool_and(x > 0) from tab",
                "create table tab as (select x - 5 as x from long_sequence(10))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolAndWithSampleBy() throws Exception {
        assertQuery(
                """
                        k\tbool_and
                        1970-01-01T00:00:00.000000Z\ttrue
                        1970-01-01T01:00:00.000000Z\ttrue
                        1970-01-01T02:00:00.000000Z\ttrue
                        """,
                "select k, bool_and(f) from tab sample by 1h",
                "create table tab as (" +
                        "select true as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testBoolAndWithSampleByMixed() throws Exception {
        assertQuery(
                """
                        k\tbool_and
                        1970-01-01T00:00:00.000000Z\tfalse
                        1970-01-01T01:00:00.000000Z\tfalse
                        1970-01-01T02:00:00.000000Z\tfalse
                        """,
                "select k, bool_and(f) from tab sample by 1h",
                "create table tab as (" +
                        "select (x % 2 = 0) as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testBoolAndConstantTrue() throws Exception {
        // Constant folding: bool_and(true) = true
        assertQuery(
                """
                        bool_and
                        true
                        """,
                "select bool_and(true) from tab",
                "create table tab as (select x from long_sequence(5))",
                null,
                true,
                true
        );
    }

    @Test
    public void testBoolAndConstantFalse() throws Exception {
        // Constant folding: bool_and(false) = false
        assertQuery(
                """
                        bool_and
                        false
                        """,
                "select bool_and(false) from tab",
                "create table tab as (select x from long_sequence(5))",
                null,
                true,
                true
        );
    }
}
