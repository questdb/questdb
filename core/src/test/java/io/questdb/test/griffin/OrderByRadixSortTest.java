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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

// Tests quick and radix sort-based factories
@RunWith(Parameterized.class)
public class OrderByRadixSortTest extends AbstractCairoTest {
    private final SortMode sortMode;
    private final SortType sortType;

    public OrderByRadixSortTest(SortMode sortMode, SortType sortType) {
        this.sortMode = sortMode;
        this.sortType = sortType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {SortMode.SORT_ENABLED, SortType.MIXED_SORT},
                {SortMode.SORT_ENABLED, SortType.RADIX_SORT_ONLY},
                {SortMode.DISABLED, SortType.MIXED_SORT},
        });
    }

    @Override
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, sortMode == SortMode.SORT_ENABLED);
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_RADIX_SORT_THRESHOLD, sortType == SortType.RADIX_SORT_ONLY ? 0 : 600);
        super.setUp();
    }

    @Test
    public void testOrderByDateColumnAscMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1969-12-31T23:59:59.990Z\n" +
                        "1969-12-31T23:59:59.991Z\n" +
                        "1969-12-31T23:59:59.992Z\n" +
                        "1969-12-31T23:59:59.993Z\n" +
                        "1969-12-31T23:59:59.994Z\n" +
                        "1969-12-31T23:59:59.995Z\n" +
                        "1969-12-31T23:59:59.996Z\n" +
                        "1969-12-31T23:59:59.997Z\n" +
                        "1969-12-31T23:59:59.998Z\n" +
                        "1969-12-31T23:59:59.999Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.002Z\n" +
                        "1970-01-01T00:00:00.003Z\n" +
                        "1970-01-01T00:00:00.004Z\n" +
                        "1970-01-01T00:00:00.005Z\n" +
                        "1970-01-01T00:00:00.006Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.009Z\n",
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as date) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByDateColumnDescMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "1970-01-01T00:00:00.009Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.006Z\n" +
                        "1970-01-01T00:00:00.005Z\n" +
                        "1970-01-01T00:00:00.004Z\n" +
                        "1970-01-01T00:00:00.003Z\n" +
                        "1970-01-01T00:00:00.002Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1969-12-31T23:59:59.999Z\n" +
                        "1969-12-31T23:59:59.998Z\n" +
                        "1969-12-31T23:59:59.997Z\n" +
                        "1969-12-31T23:59:59.996Z\n" +
                        "1969-12-31T23:59:59.995Z\n" +
                        "1969-12-31T23:59:59.994Z\n" +
                        "1969-12-31T23:59:59.993Z\n" +
                        "1969-12-31T23:59:59.992Z\n" +
                        "1969-12-31T23:59:59.991Z\n" +
                        "1969-12-31T23:59:59.990Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as date) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByIPv4ColumnAscMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.3\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "0.0.0.6\n" +
                        "0.0.0.7\n" +
                        "0.0.0.8\n" +
                        "0.0.0.9\n" +
                        "255.255.255.246\n" +
                        "255.255.255.247\n" +
                        "255.255.255.248\n" +
                        "255.255.255.249\n" +
                        "255.255.255.250\n" +
                        "255.255.255.251\n" +
                        "255.255.255.252\n" +
                        "255.255.255.253\n" +
                        "255.255.255.254\n" +
                        "255.255.255.255\n",
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as IPv4) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByIPv4ColumnDescMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "255.255.255.255\n" +
                        "255.255.255.254\n" +
                        "255.255.255.253\n" +
                        "255.255.255.252\n" +
                        "255.255.255.251\n" +
                        "255.255.255.250\n" +
                        "255.255.255.249\n" +
                        "255.255.255.248\n" +
                        "255.255.255.247\n" +
                        "255.255.255.246\n" +
                        "0.0.0.9\n" +
                        "0.0.0.8\n" +
                        "0.0.0.7\n" +
                        "0.0.0.6\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "0.0.0.3\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as IPv4) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByIntColumnDescMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "9\n" +
                        "8\n" +
                        "7\n" +
                        "6\n" +
                        "5\n" +
                        "4\n" +
                        "3\n" +
                        "2\n" +
                        "1\n" +
                        "0\n" +
                        "-1\n" +
                        "-2\n" +
                        "-3\n" +
                        "-4\n" +
                        "-5\n" +
                        "-6\n" +
                        "-7\n" +
                        "-8\n" +
                        "-9\n" +
                        "-10\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n",
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByLongColumnAscMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "-10\n" +
                        "-9\n" +
                        "-8\n" +
                        "-7\n" +
                        "-6\n" +
                        "-5\n" +
                        "-4\n" +
                        "-3\n" +
                        "-2\n" +
                        "-1\n" +
                        "0\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n",
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByLongColumnDescMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "9\n" +
                        "8\n" +
                        "7\n" +
                        "6\n" +
                        "5\n" +
                        "4\n" +
                        "3\n" +
                        "2\n" +
                        "1\n" +
                        "0\n" +
                        "-1\n" +
                        "-2\n" +
                        "-3\n" +
                        "-4\n" +
                        "-5\n" +
                        "-6\n" +
                        "-7\n" +
                        "-8\n" +
                        "-9\n" +
                        "-10\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n",
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByTimestampColumnAscMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1969-12-31T23:59:59.999990Z\n" +
                        "1969-12-31T23:59:59.999991Z\n" +
                        "1969-12-31T23:59:59.999992Z\n" +
                        "1969-12-31T23:59:59.999993Z\n" +
                        "1969-12-31T23:59:59.999994Z\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999996Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000006Z\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000009Z\n",
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as timestamp) as a" +
                        " from long_sequence(25)" +
                        ")",
                "a",
                true,
                true
        );
    }

    @Test
    public void testOrderByTimestampColumnDescMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "1970-01-01T00:00:00.000009Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000006Z\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999996Z\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999994Z\n" +
                        "1969-12-31T23:59:59.999993Z\n" +
                        "1969-12-31T23:59:59.999992Z\n" +
                        "1969-12-31T23:59:59.999991Z\n" +
                        "1969-12-31T23:59:59.999990Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as timestamp) as a" +
                        " from long_sequence(25)" +
                        ")",
                "a###desc",
                true,
                true
        );
    }

    @Test
    public void testOrderIntColumnAscMixedValues() throws Exception {
        assertQuery(
                "a\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "-10\n" +
                        "-9\n" +
                        "-8\n" +
                        "-7\n" +
                        "-6\n" +
                        "-5\n" +
                        "-4\n" +
                        "-3\n" +
                        "-2\n" +
                        "-1\n" +
                        "0\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n",
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    public enum SortMode {
        SORT_ENABLED, DISABLED
    }

    public enum SortType {
        MIXED_SORT, RADIX_SORT_ONLY
    }
}
