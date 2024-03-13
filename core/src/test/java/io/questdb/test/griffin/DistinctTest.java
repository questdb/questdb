/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * These tests cover distinct variations.
 */
public class DistinctTest extends AbstractCairoTest {

    @Test
    public void testDuplicateColumn() throws Exception {
        assertQuery(
                "e1\te2\n" +
                        "24814\t24814\n" +
                        "-13027\t-13027\n" +
                        "-22955\t-22955\n",
                "SELECT DISTINCT event e1, event e2 FROM x;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                true,
                false
        );
    }

    @Test
    public void testDuplicateColumnInWhereClauseSubQuery() throws Exception {
        assertQuery(
                "origin\tevent\tcreated\n" +
                        "-27056\ta\t1970-01-01T00:00:00.000000Z\n" +
                        "-11455\tc\t1970-01-01T00:00:00.000000Z\n" +
                        "-21227\tc\t1970-01-01T00:00:00.000000Z\n",
                "SELECT * FROM x WHERE event IN (SELECT * FROM (SELECT DISTINCT event, event FROM x));",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_symbol('a','b','c') event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                "created",
                true,
                false
        );
    }

    @Test
    public void testDuplicateColumnWithSubQuery() throws Exception {
        assertQuery(
                "e1\te2\n" +
                        "-24814\t-24814\n" +
                        "13027\t13027\n" +
                        "22955\t22955\n",
                "SELECT DISTINCT event e1, event e2 FROM (SELECT origin, (-event) event FROM x);",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                true,
                false
        );
    }

    @Test
    public void testDuplicateColumnWithUnion() throws Exception {
        assertQuery(
                "e1\te2\n" +
                        "42\t42\n" +
                        "24814\t24814\n" +
                        "-13027\t-13027\n" +
                        "-22955\t-22955\n",
                "(SELECT 42 e1, 42 e2) UNION (SELECT DISTINCT event e1, event e2 FROM x);",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                false,
                false
        );
    }

    @Test
    public void testDuplicateCount() throws Exception {
        assertQuery(
                "count\tcount1\n" +
                        "10\t10\n",
                "SELECT DISTINCT count(*), count(*) FROM x;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                false
        );
    }

    @Test
    public void testDuplicateCountNested() throws Exception {
        assertQuery(
                "count\tcount1\n" +
                        "10\t10\n",
                "SELECT * FROM (SELECT DISTINCT count(*), count(*) FROM x);",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                false
        );
    }

    @Test
    public void testOrderByIsAppliedAfterDistinct() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "create table tab (" +
                            "x0 boolean, " +
                            "x1 byte, " +
                            "x2 geohash(12b), " +
                            "x3 char, " +
                            "x4 long, " +
                            "x5 timestamp, " +
                            "x6 float, " +
                            "x7 double, " +
                            "x8 string, " +
                            "x9 date)"
            );

            for (int i = 0; i < 10; i++) {
                assertPlan(
                        "select DISTINCT x" + i + " from tab order by x" + i + " DESC",
                        "Sort light\n" +
                                "  keys: [x" + i + " desc]\n" +
                                "    Distinct\n" +
                                "      keys: x" + i + "\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: tab\n"
                );

                assertPlan(
                        "select DISTINCT x" + i + " from tab order by x" + i + " ASC",
                        "Sort light\n" +
                                "  keys: [x" + i + "]\n" +
                                "    Distinct\n" +
                                "      keys: x" + i + "\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: tab\n"
                );
            }
        });
    }
}
