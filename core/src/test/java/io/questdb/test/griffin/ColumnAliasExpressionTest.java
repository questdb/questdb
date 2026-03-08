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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ColumnAliasExpressionTest extends AbstractCairoTest {
    @Test
    public void testOperators() throws Exception {
        assertGeneratedColumnEqual(
                "a * 2 + b / (d - c)\n",
                "select a*2+b/(d-c) from tab",
                "create table tab (a int, b int, c int, d int)",
                0
        );
    }

    @Test
    public void testDots() throws Exception {
        assertGeneratedColumnEqual(
                "\"floor(1.2)\"\t\"'Hello there.'\"\t\"i.o\"\n1.0\tHello there.\t1\n",
                "select floor(1.2), 'Hello there.', 1 \"i.o\"",
                0
        );
    }

    @Test
    public void testFunctionCalls() throws Exception {
        assertGeneratedColumnEqual(
                "trim(a)\t\"floor(b * 1.5)\"\n",
                "select trim(a), floor(b * 1.5) from tab",
                "create table tab (a string, b double)",
                0
        );
    }

    @Test
    public void testNestedParentheses() throws Exception {
        assertGeneratedColumnEqual(
                "(a + b) * c / (d - (a + 1))\n",
                "select (a + b) * c / (d - (a + 1)) from tab",
                "create table tab (a int, b int, c int, d int)",
                0
        );
    }

    @Test
    public void testRemoveUnnecessaryParentheses() throws Exception {
        assertGeneratedColumnEqual(
                "a + b * c / d\n",
                "select a + ((b * c) / d) from tab",
                "create table tab (a int, b int, c int, d int)",
                0
        );
    }

    @Test
    public void testStringConcatenation() throws Exception {
        assertGeneratedColumnEqual(
                "concat(a, '_', b)\n",
                "select a || '_' || b from tab",
                "create table tab (a string, b string)",
                0
        );
    }

    @Test
    public void testBooleanExpressions() throws Exception {
        assertGeneratedColumnEqual(
                "a > b and c < d\n",
                "select a > b AND c < d from tab",
                "create table tab (a int, b int, c int, d int)",
                0
        );
    }

    @Test
    public void testCaseExpressions() throws Exception {
        assertGeneratedColumnEqual(
                "case when a > b then a + b else a - b end\n",
                "select CASE WHEN a > b THEN a + b ELSE a - b END from tab",
                "create table tab (a int, b int)",
                0
        );
    }

    @Test
    public void testAliasOverride() throws Exception {
        assertGeneratedColumnEqual(
                "sum\n",
                "select a + b AS sum from tab",
                "create table tab (a int, b int)",
                0
        );
    }

    @Test
    public void testMaxSizeLimit() throws Exception {
        assertGeneratedColumnEqual(
                "a * b\n",
                "select a*b*c from tab",
                "create table tab (a int, b int, c int)",
                5
        );
    }

    @Test
    public void testTrimming() throws Exception {
        assertGeneratedColumnEqual(
                "a * b\n",
                "select a*b*c from tab",
                "create table tab (a int, b int, c int)",
                6
        );
    }

    @Test
    public void testDuplicates() throws Exception {
        assertGeneratedColumnEqual(
                "a * b\ta * b_2\ta * b_3\ta * b_4\n",
                "select a*b, a*b, a*b, a*b from tab",
                "create table tab (a int, b int)",
                0
        );
    }

    @Test
    public void testMultiParams() throws Exception {
        assertGeneratedColumnEqual(
                "replace(a, 'a', 'b')\n",
                "select replace(a, 'a', 'b') from tab",
                "create table tab (a string)",
                0
        );
    }

    @Test
    public void testArrayDereference() throws Exception {
        assertGeneratedColumnEqual(
                "arr[10][2]\n",
                "select arr[10][2] from tab",
                "create table tab (arr double[][])",
                0
        );
    }

    @Test
    public void testSelectArray() throws Exception {
        assertGeneratedColumnEqual(
                "ARRAY[1, 2, 3]\n[1.0,2.0,3.0]\n",
                "select array[1, 2, 3]",
                0
        );
        assertGeneratedColumnEqual(
                "ARRAY[1, 2]\n[1.0,2.0]\n",
                "select array[1, 2]",
                0
        );
        assertGeneratedColumnEqual(
                "ARRAY[1]\n[1.0]\n",
                "select array[1]",
                0
        );
        assertGeneratedColumnEqual(
                "ARRAY[ARRAY[1]]\n[[1.0]]\n",
                "select array[array[1]]",
                0
        );
    }

    @Test
    public void testCase() throws Exception {
        assertGeneratedColumnEqual(
                "case when a >= 0 then 'positive' else 'negative' end\n",
                "select case when a >= 0 then 'positive' else 'negative' end from tab",
                "create table tab (a int)",
                0
        );
        assertGeneratedColumnEqual(
                "case when a > 0 then 'pos' when a < 0 then 'neg' else 'zero' end\n",
                "select case when a > 0 then 'pos' when a < 0 then 'neg' else 'zero' end from tab",
                0
        );
        assertGeneratedColumnEqual(
                "case when a > 0 then 'pos' end\n",
                "select case when a > 0 then 'pos' end from tab",
                0
        );
        assertGeneratedColumnEqual(
                "case when a > 0 then 'pos' when a < 0 then 'neg' end\n",
                "select case when a > 0 then 'pos' when a < 0 then 'neg' end from tab",
                0
        );
    }

    @Test
    public void testCast() throws Exception {
        assertGeneratedColumnEqual(
                "a::long\tb::long\t(a + 1)::string\n",
                "select cast(a as long), b::long, cast(a + 1 as string) from tab",
                "create table tab (a int, b int)",
                0
        );
    }

    private void assertGeneratedColumnEqual(String expected, String query, String ddl, int maxSize) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        if (maxSize > 0) {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, maxSize);
        }

        assertQuery(expected, query, ddl, "", true, true);
    }

    private void assertGeneratedColumnEqual(String expected, String query, int maxSize) throws Exception {
        assertGeneratedColumnEqual(expected, query, null, maxSize);
    }
}
