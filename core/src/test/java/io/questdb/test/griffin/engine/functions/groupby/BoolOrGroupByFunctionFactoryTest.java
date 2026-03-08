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

public class BoolOrGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBoolOrAllTrue() throws Exception {
        assertQuery(
                """
                        bool_or
                        true
                        """,
                "select bool_or(f) from tab",
                "create table tab as (select true as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolOrAllFalse() throws Exception {
        assertQuery(
                """
                        bool_or
                        false
                        """,
                "select bool_or(f) from tab",
                "create table tab as (select false as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolOrMixed() throws Exception {
        // One true makes the entire result true
        assertQuery(
                """
                        bool_or
                        true
                        """,
                "select bool_or(f) from tab",
                "create table tab as (" +
                        "select false as f from long_sequence(3) " +
                        "union all " +
                        "select true as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolOrEmpty() throws Exception {
        // Empty table returns false (identity for OR in terms of aggregation)
        assertQuery(
                """
                        bool_or
                        false
                        """,
                "select bool_or(f) from tab",
                "create table tab (f boolean)",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolOrSingleTrue() throws Exception {
        assertQuery(
                """
                        bool_or
                        true
                        """,
                "select bool_or(f) from tab",
                "create table tab as (select true as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolOrSingleFalse() throws Exception {
        assertQuery(
                """
                        bool_or
                        false
                        """,
                "select bool_or(f) from tab",
                "create table tab as (select false as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolOrWithGroupBy() throws Exception {
        assertQuery(
                """
                        g\tbool_or
                        A\tfalse
                        B\ttrue
                        C\ttrue
                        """,
                "select g, bool_or(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, false as f from long_sequence(3) " +
                        "union all " +
                        "select 'B' as g, false as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, true as f from long_sequence(1) " +
                        "union all " +
                        "select 'C' as g, false as f from long_sequence(2) " +
                        "union all " +
                        "select 'C' as g, true as f from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBoolOrWithExpression() throws Exception {
        // Using a boolean expression
        assertQuery(
                """
                        bool_or
                        true
                        """,
                "select bool_or(x > 5) from tab",
                "create table tab as (select x from long_sequence(10))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBoolOrWithSampleBy() throws Exception {
        assertQuery(
                """
                        k\tbool_or
                        1970-01-01T00:00:00.000000Z\tfalse
                        1970-01-01T01:00:00.000000Z\tfalse
                        1970-01-01T02:00:00.000000Z\tfalse
                        """,
                "select k, bool_or(f) from tab sample by 1h",
                "create table tab as (" +
                        "select false as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testBoolOrWithSampleByMixed() throws Exception {
        assertQuery(
                """
                        k\tbool_or
                        1970-01-01T00:00:00.000000Z\ttrue
                        1970-01-01T01:00:00.000000Z\ttrue
                        1970-01-01T02:00:00.000000Z\ttrue
                        """,
                "select k, bool_or(f) from tab sample by 1h",
                "create table tab as (" +
                        "select (x % 3 = 0) as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) k " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testBoolOrConstantTrue() throws Exception {
        // Constant folding: bool_or(true) = true
        assertQuery(
                """
                        bool_or
                        true
                        """,
                "select bool_or(true) from tab",
                "create table tab as (select x from long_sequence(5))",
                null,
                true,
                true
        );
    }

    @Test
    public void testBoolOrConstantFalse() throws Exception {
        // Constant folding: bool_or(false) = false
        assertQuery(
                """
                        bool_or
                        false
                        """,
                "select bool_or(false) from tab",
                "create table tab as (select x from long_sequence(5))",
                null,
                true,
                true
        );
    }
}
