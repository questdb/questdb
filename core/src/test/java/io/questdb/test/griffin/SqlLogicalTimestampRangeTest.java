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

package io.questdb.test.griffin;

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SqlLogicalTimestampRangeTest extends AbstractCairoTest {
    @Test
    public void testBetweenConstantsNormalizeBoundsAndKeepNullSemantics() throws Exception {
        {
            final boolean nanos = false;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String keyword = " BETWEEN ";
                    {
                        final String endpoints = "'2020-01-01' AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND '2020-01-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """);
                    }
                    {
                        final String endpoints = "'2020-01' AND '2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                """);
                    }
                    {
                        final String endpoints = "null AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                """);
                    }
                    {
                        final String endpoints = "CAST('2020-01-01' AS TIMESTAMP_NS) AND CAST('2020-01-02' AS TIMESTAMP)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """);
                    }
                }
                {
                    final String keyword = " NOT BETWEEN ";
                    {
                        final String endpoints = "'2020-01-01' AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND '2020-01-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "'2020-01' AND '2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "null AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "CAST('2020-01-01' AS TIMESTAMP_NS) AND CAST('2020-01-02' AS TIMESTAMP)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                }
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE ts BETWEEN '2020-01-01' AND '2020-01-02' AND id>1",
                        """
                                ts	id
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE other BETWEEN '2020-01-01' AND '2020-01-02'",
                        """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                """
                );
                assertExpected("SELECT id FROM lp_range WHERE ts BETWEEN '2020-01-02' AND '2020-01-01'", "id\n1\n2\n3\n");
                assertExpected("SELECT id FROM lp_range WHERE ts NOT BETWEEN '2020-01-02' AND '2020-01-01'", "id\n4\n5\n");
                execute("DROP TABLE lp_range");
            });
        }
        {
            final boolean nanos = true;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String keyword = " BETWEEN ";
                    {
                        final String endpoints = "'2020-01-01' AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND '2020-01-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """);
                    }
                    {
                        final String endpoints = "'2020-01' AND '2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                """);
                    }
                    {
                        final String endpoints = "null AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                """);
                    }
                    {
                        final String endpoints = "CAST('2020-01-01' AS TIMESTAMP_NS) AND CAST('2020-01-02' AS TIMESTAMP)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """);
                    }
                }
                {
                    final String keyword = " NOT BETWEEN ";
                    {
                        final String endpoints = "'2020-01-01' AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND '2020-01-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "'2020-01' AND '2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "null AND '2020-01-02'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "'2020-01-02' AND null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String endpoints = "CAST('2020-01-01' AS TIMESTAMP_NS) AND CAST('2020-01-02' AS TIMESTAMP)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + endpoints, """
                                ts	id
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                }
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE ts BETWEEN '2020-01-01' AND '2020-01-02' AND id>1",
                        """
                                ts	id
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE other BETWEEN '2020-01-01' AND '2020-01-02'",
                        """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                """
                );
                assertExpected("SELECT id FROM lp_range WHERE ts BETWEEN '2020-01-02' AND '2020-01-01'", "id\n1\n2\n3\n");
                assertExpected("SELECT id FROM lp_range WHERE ts NOT BETWEEN '2020-01-02' AND '2020-01-01'", "id\n4\n5\n");
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testBetweenTextAndTypedNegativeEpochKeepDifferentRounding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_range(id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_range VALUES (1,'1970-01-01'),(2,'1970-01-01T00:00:00.000001Z')");
            final String value = "'1969-12-31T23:59:59.999999999Z'";
            final String text = "SELECT id FROM lp_range WHERE ts-1L BETWEEN " + value + " AND " + value;
            final String typed = "SELECT id FROM lp_range WHERE ts-1L BETWEEN CAST(" + value + " AS TIMESTAMP_NS) AND CAST(" + value + " AS TIMESTAMP_NS)";
            assertQueryRows(text, """
                    id
                    1
                    """);
            assertQueryRows(typed, """
                    id
                    2
                    """);
            assertExpected(text, "id\n1\n");
            assertExpected(typed, "id\n2\n");
        });
    }

    @Test
    public void testInTextIntervalsAndDiscretePointsRemainDistinct() throws Exception {
        {
            final boolean nanos = false;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String keyword = " IN ";
                    {
                        final String values = "'2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """);
                    }
                    {
                        final String values = "'2020-01-01;1d;1M;3'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-02-01')";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-02-01T00:00:00.000000Z	4
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-01-01',null)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                """);
                    }
                    {
                        final String values = "null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                """);
                    }
                    {
                        final String values = "CAST('2020-01' AS VARCHAR)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """);
                    }
                    {
                        final String values = "(CAST('2020-01-01' AS TIMESTAMP),CAST('2020-02-01' AS TIMESTAMP_NS))";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-02-01T00:00:00.000000Z	4
                                """);
                    }
                }
                {
                    final String keyword = " NOT IN ";
                    {
                        final String values = "'2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String values = "'2020-01-01;1d;1M;3'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-02T00:00:00.000000Z	3
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-02-01')";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-01-01',null)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String values = "null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String values = "CAST('2020-01' AS VARCHAR)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-02-01T00:00:00.000000Z	4
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                    {
                        final String values = "(CAST('2020-01-01' AS TIMESTAMP),CAST('2020-02-01' AS TIMESTAMP_NS))";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                2020-03-01T00:00:00.000000Z	5
                                """);
                    }
                }
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE CAST(ts AS " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ") IN '2020-01'",
                        """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE other IN ('2020-01-01','2020-01-02')",
                        """
                                ts	id
                                2020-01-01T00:00:00.000000Z	1
                                2020-01-01T12:00:00.000000Z	2
                                """
                );
                assertExpected("SELECT id FROM lp_range WHERE ts IN '2020-01'", "id\n1\n2\n3\n");
                assertExpected("SELECT id FROM lp_range WHERE ts IN ('2020-01-01','2020-02-01')", "id\n1\n4\n");
                execute("DROP TABLE lp_range");
            });
        }
        {
            final boolean nanos = true;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String keyword = " IN ";
                    {
                        final String values = "'2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """);
                    }
                    {
                        final String values = "'2020-01-01;1d;1M;3'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-02-01')";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-02-01T00:00:00.000000000Z	4
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-01-01',null)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                """);
                    }
                    {
                        final String values = "null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                """);
                    }
                    {
                        final String values = "CAST('2020-01' AS VARCHAR)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """);
                    }
                    {
                        final String values = "(CAST('2020-01-01' AS TIMESTAMP),CAST('2020-02-01' AS TIMESTAMP_NS))";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-02-01T00:00:00.000000000Z	4
                                """);
                    }
                }
                {
                    final String keyword = " NOT IN ";
                    {
                        final String values = "'2020-01'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String values = "'2020-01-01;1d;1M;3'";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-02T00:00:00.000000000Z	3
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-02-01')";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String values = "('2020-01-01','2020-01-01',null)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String values = "null";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String values = "CAST('2020-01' AS VARCHAR)";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-02-01T00:00:00.000000000Z	4
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                    {
                        final String values = "(CAST('2020-01-01' AS TIMESTAMP),CAST('2020-02-01' AS TIMESTAMP_NS))";
                        assertQueryRows("SELECT ts,id FROM lp_range WHERE ts" + keyword + values, """
                                ts	id
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                2020-03-01T00:00:00.000000000Z	5
                                """);
                    }
                }
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE CAST(ts AS " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ") IN '2020-01'",
                        """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE other IN ('2020-01-01','2020-01-02')",
                        """
                                ts	id
                                2020-01-01T00:00:00.000000000Z	1
                                2020-01-01T12:00:00.000000000Z	2
                                """
                );
                assertExpected("SELECT id FROM lp_range WHERE ts IN '2020-01'", "id\n1\n2\n3\n");
                assertExpected("SELECT id FROM lp_range WHERE ts IN ('2020-01-01','2020-02-01')", "id\n1\n4\n");
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testInAndOrExtractionKeepsConjunctOrderAndResiduals() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            {
                final String predicate = "ts IN '2020-01' OR ts IN '2020-03'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "ts IN ('2020-01-01','2020-02-01') OR ts='2020-03-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-02-01T00:00:00.000000Z	4
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "ts IN '2020-01-01;1d;1M;3' OR ts IN '2020-01-02'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-02-01T00:00:00.000000Z	4
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "ts>'2020-01-01' AND (ts IN '2020-01' OR ts='2020-03-01')";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "(ts IN '2020-01' OR ts='2020-03-01') AND ts>'2020-01-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "ts IN ('2020-01-01','2020-02-01') AND ts>'2020-01-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-02-01T00:00:00.000000Z	4
                        """);
            }
            {
                final String predicate = "ts>'2020-01-01' AND ts IN ('2020-01-01','2020-02-01')";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-02-01T00:00:00.000000Z	4
                        """);
            }
            {
                final String predicate = "ts NOT IN ('2020-01-01','2020-02-01') AND ts>'2020-01-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "ts IN '2020-01' OR id=5";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "ts IN '2020-01' OR ts BETWEEN '2020-02-01' AND '2020-03-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-02-01T00:00:00.000000Z	4
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "ts BETWEEN '2020-01-01' AND '2020-01-02' AND ts BETWEEN '2020-03-01' AND '2020-04-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        """);
            }
        });
    }

    @Test
    public void testRuntimeBetweenRebindsAfterCompilerClose() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                assertRebound("SELECT ts,id FROM lp_range WHERE ts BETWEEN $1 AND $2", nanos,
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n",
                        "ts\tid\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts BETWEEN $1 AND '2020-02-01'", nanos,
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts BETWEEN '2020-02-01' AND $2", nanos,
                        "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts NOT BETWEEN $1 AND $2", nanos,
                        "ts\tid\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts NOT BETWEEN $1 AND '2020-02-01'", nanos,
                        "ts\tid\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-03-01T00:00:00.000000Z\t5\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts NOT BETWEEN '2020-02-01' AND $2", nanos,
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n");
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testRuntimeBetweenTextKeepsNativePrecisionAfterRebinding() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE lp_range(id INT,ts " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP")
                        + ") TIMESTAMP(ts) PARTITION BY DAY");
                execute("INSERT INTO lp_range VALUES (1,'1970-01-01'),(2,'1970-01-02')");
                final int oldMode = sqlExecutionContext.getJitMode();
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                try {
                    for (String keyword : new String[]{" BETWEEN ", " NOT BETWEEN "}) {
                        bindVariableService.setStr(0, "1970-01-01");
                        bindVariableService.setStr(1, "1970-01-02");
                        final String sql = "SELECT id FROM lp_range WHERE ts" + keyword + "$1 AND $2";
                        final boolean isNot = keyword.contains("NOT");
                        final String inside = isNot ? "id\n" : "id\n1\n2\n";
                        final String outside = isNot ? "id\n1\n2\n" : "id\n";
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertRows(factory, inside);
                            bindVariableService.setStr(0, "1970-01-02");
                            bindVariableService.setStr(1, "1970-01-01");
                            assertRows(factory, inside);
                            bindVariableService.setStr(0, null);
                            assertRows(factory, outside);
                            bindVariableService.setStr(0, "1969-12-31T23:59:59.999999999Z");
                            bindVariableService.setStr(1, "1969-12-31T23:59:59.999999999Z");
                            assertRows(factory, outside);
                            bindVariableService.setStr(0, "1970-01-01");
                            bindVariableService.setStr(1, null);
                            assertRows(factory, outside);
                        }
                        bindVariableService.clear();
                    }
                } finally {
                    sqlExecutionContext.setJitMode(oldMode);
                }
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testRuntimeInTextAndPointsRebindAndRecoverFromNull() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final int oldMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                for (String keyword : new String[]{" IN ", " NOT IN "}) {
                    bindVariableService.setStr(0, "2020-01");
                    final String sql = "SELECT ts,id FROM lp_range WHERE ts" + keyword + "$1";
                    final boolean isNot = keyword.contains("NOT");
                    try (RecordCursorFactory factory = compile(sql)) {
                        assertRows(factory, isNot
                                ? "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n"
                                : "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n");
                        bindVariableService.setStr(0, "2020-02");
                        assertRows(factory, isNot
                                ? "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-03-01T00:00:00.000000Z\t5\n"
                                : "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n");
                        bindVariableService.setStr(0, null);
                        assertRows(factory, isNot
                                ? "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n"
                                : "ts\tid\n");
                        bindVariableService.setStr(0, "2020-01-01;1d;1M;3");
                        assertRows(factory, isNot
                                ? "ts\tid\n2020-01-02T00:00:00.000000Z\t3\n"
                                : "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n");
                    }
                    bindVariableService.clear();
                }
                assertRebound("SELECT ts,id FROM lp_range WHERE ts IN $1", false,
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n",
                        "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts NOT IN $1", false,
                        "ts\tid\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts IN ($1,$2)", false,
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts IN $1 OR ts IN '2020-02'", false,
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-02-01T00:00:00.000000Z\t4\n");
                assertRebound("SELECT ts,id FROM lp_range WHERE ts IN ($1,$2) OR ts='2020-03-01'", false,
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-03-01T00:00:00.000000Z\t5\n",
                        "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-03-01T00:00:00.000000Z\t5\n");
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
            }
        });
    }

    @Test
    public void testTypedNotInExcludesThePointAndRebindsNull() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                final String timestampType = nanos ? "TIMESTAMP_NS" : "TIMESTAMP";
                assertExpected("SELECT id FROM lp_range WHERE ts NOT IN CAST('2020-01-01' AS " + timestampType + ')',
                        "id\n2\n3\n4\n5\n");
                assertExpected("SELECT id FROM lp_range WHERE ts NOT IN CAST(null AS " + timestampType + ')',
                        "id\n1\n2\n3\n4\n5\n");
                setTimestamp(0, 1_577_836_800_000_000L, nanos);
                try (RecordCursorFactory factory = compile("SELECT id FROM lp_range WHERE ts NOT IN $1")) {
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                            .sizeMayVary().returns("id\n2\n3\n4\n5\n");
                    setTimestamp(0, Numbers.LONG_NULL, nanos);
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                            .sizeMayVary().returns("id\n1\n2\n3\n4\n5\n");
                    setTimestamp(0, 1_580_515_200_000_000L, nanos);
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                            .sizeMayVary().returns("id\n1\n2\n3\n5\n");
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testMonotonicBetweenUsesSharedInverterAndKeepsResidual() throws Exception {
        {
            final boolean nanos = false;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String expression = "timestamp_floor('d',ts)";
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-01' AND '2020-01-02'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000Z	1
                                    2020-01-01T12:00:00.000000Z	2
                                    2020-01-02T00:00:00.000000Z	3
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-02' AND '2020-01-01'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000Z	1
                                    2020-01-01T12:00:00.000000Z	2
                                    2020-01-02T00:00:00.000000Z	3
                                    """
                    );
                    assertRebound("SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN $1 AND $2", nanos,
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n",
                            "ts\tid\n");
                }
                {
                    final String expression = "dateadd('h',1,ts)";
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-01' AND '2020-01-02'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000Z	1
                                    2020-01-01T12:00:00.000000Z	2
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-02' AND '2020-01-01'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000Z	1
                                    2020-01-01T12:00:00.000000Z	2
                                    """
                    );
                    assertRebound("SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN $1 AND $2", nanos,
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n",
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n",
                            "ts\tid\n",
                            "ts\tid\n");
                }
                {
                    final String expression = "CAST(ts AS " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ")";
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-01' AND '2020-01-02'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000Z	1
                                    2020-01-01T12:00:00.000000Z	2
                                    2020-01-02T00:00:00.000000Z	3
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-02' AND '2020-01-01'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000Z	1
                                    2020-01-01T12:00:00.000000Z	2
                                    2020-01-02T00:00:00.000000Z	3
                                    """
                    );
                    assertRebound("SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN $1 AND $2", nanos,
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n",
                            "ts\tid\n");
                }
                execute("DROP TABLE lp_range");
            });
        }
        {
            final boolean nanos = true;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String expression = "timestamp_floor('d',ts)";
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-01' AND '2020-01-02'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000000Z	1
                                    2020-01-01T12:00:00.000000000Z	2
                                    2020-01-02T00:00:00.000000000Z	3
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-02' AND '2020-01-01'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000000Z	1
                                    2020-01-01T12:00:00.000000000Z	2
                                    2020-01-02T00:00:00.000000000Z	3
                                    """
                    );
                    assertRebound("SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN $1 AND $2", nanos,
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n",
                            "ts\tid\n");
                }
                {
                    final String expression = "dateadd('h',1,ts)";
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-01' AND '2020-01-02'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000000Z	1
                                    2020-01-01T12:00:00.000000000Z	2
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-02' AND '2020-01-01'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000000Z	1
                                    2020-01-01T12:00:00.000000000Z	2
                                    """
                    );
                    assertRebound("SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN $1 AND $2", nanos,
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n",
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n",
                            "ts\tid\n",
                            "ts\tid\n");
                }
                {
                    final String expression = "CAST(ts AS " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ")";
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-01' AND '2020-01-02'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000000Z	1
                                    2020-01-01T12:00:00.000000000Z	2
                                    2020-01-02T00:00:00.000000000Z	3
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN '2020-01-02' AND '2020-01-01'",
                            """
                                    ts	id
                                    2020-01-01T00:00:00.000000000Z	1
                                    2020-01-01T12:00:00.000000000Z	2
                                    2020-01-02T00:00:00.000000000Z	3
                                    """
                    );
                    assertRebound("SELECT ts,id FROM lp_range WHERE " + expression + " BETWEEN $1 AND $2", nanos,
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n2020-02-01T00:00:00.000000Z\t4\n",
                            "ts\tid\n",
                            "ts\tid\n");
                }
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testMonotonicInSingleTextIntervals() throws Exception {
        {
            final boolean nanos = false;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String expression = "date_trunc('day',ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                }
                {
                    final String expression = "timestamp_floor('d',ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                }
                {
                    final String expression = "timestamp_ceil('d',ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        """
                        );
                    }
                }
                {
                    final String expression = "dateadd('d',1,ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        """
                        );
                    }
                }
                {
                    final String expression = "ts+1L";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "ts-1L";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        2020-02-01T00:00:00.000000Z	4
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "dateadd('h',-1,timestamp_floor('d',ts))";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        2020-02-01T00:00:00.000000Z	4
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "CAST(ts AS TIMESTAMP)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                }
                {
                    final String expression = "CAST(ts AS TIMESTAMP_NS)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000Z	1
                                        2020-01-01T12:00:00.000000Z	2
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE timestamp_floor('d',ts) IN '2020-01-02T12:00:00Z'",
                        """
                                ts	id
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE timestamp_floor('d',ts) IN '2020-01' AND ts>'2020-01-01'",
                        """
                                ts	id
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE ts>'2020-01-01' AND timestamp_floor('d',ts) IN '2020-01'",
                        """
                                ts	id
                                2020-01-01T12:00:00.000000Z	2
                                2020-01-02T00:00:00.000000Z	3
                                """
                );
                assertExpected("SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN '2020-01-01'", "id\n1\n2\n");
                execute("DROP TABLE lp_range");
            });
        }
        {
            final boolean nanos = true;
            assertMemoryLeak(() -> {
                createRows(nanos);
                {
                    final String expression = "date_trunc('day',ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "timestamp_floor('d',ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "timestamp_ceil('d',ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "dateadd('d',1,ts)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "ts+1L";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                }
                {
                    final String expression = "ts-1L";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        2020-02-01T00:00:00.000000000Z	4
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "dateadd('h',-1,timestamp_floor('d',ts))";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        2020-02-01T00:00:00.000000000Z	4
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                {
                    final String expression = "CAST(ts AS TIMESTAMP)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                }
                {
                    final String expression = "CAST(ts AS TIMESTAMP_NS)";
                    {
                        final String interval = "'2020-01'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-01T00:00:00.000000000Z	1
                                        2020-01-01T12:00:00.000000000Z	2
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02;1d'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        2020-01-02T00:00:00.000000000Z	3
                                        """
                        );
                    }
                    {
                        final String interval = "'2020-01-02T00:00:00.000000001Z'";
                        assertQueryRows(
                                "SELECT ts,id FROM lp_range WHERE " + expression + " IN " + interval,
                                """
                                        ts	id
                                        """
                        );
                    }
                }
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE timestamp_floor('d',ts) IN '2020-01-02T12:00:00Z'",
                        """
                                ts	id
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE timestamp_floor('d',ts) IN '2020-01' AND ts>'2020-01-01'",
                        """
                                ts	id
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """
                );
                assertQueryRows(
                        "SELECT ts,id FROM lp_range WHERE ts>'2020-01-01' AND timestamp_floor('d',ts) IN '2020-01'",
                        """
                                ts	id
                                2020-01-01T12:00:00.000000000Z	2
                                2020-01-02T00:00:00.000000000Z	3
                                """
                );
                assertExpected("SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN '2020-01-01'", "id\n1\n2\n");
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testMonotonicInSupersetsKeepTheirResiduals() throws Exception {
        {
            final boolean nanos = false;
            assertMemoryLeak(() -> {
                createRows(nanos);
                execute("INSERT INTO lp_range VALUES (6,'2020-03-29T00:30:00Z',null),(7,'2020-03-29T01:30:00Z',null)");
                {
                    final String predicate = "dateadd('M',1,ts) IN '2020-02'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-01-01T00:00:00.000000Z	1
                            2020-01-01T12:00:00.000000Z	2
                            2020-01-02T00:00:00.000000Z	3
                            """);
                }
                {
                    final String predicate = "dateadd('y',1,ts) IN '2021-01-01'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-01-01T00:00:00.000000Z	1
                            2020-01-01T12:00:00.000000Z	2
                            """);
                }
                {
                    final String predicate = "to_timezone(ts,'Europe/London') IN '2020-03-29T02'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-03-29T01:30:00.000000Z	7
                            """);
                }
                {
                    final String predicate = "to_utc(ts,'Europe/London') IN '2020-03-29T00'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-03-29T00:30:00.000000Z	6
                            """);
                }
                {
                    final String predicate = "to_timezone(ts,'+02:00') IN '2020-01-01'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-01-01T00:00:00.000000Z	1
                            2020-01-01T12:00:00.000000Z	2
                            """);
                }
                assertExpected("SELECT id FROM lp_range WHERE to_timezone(ts,'Europe/London') IN '2020-03-29T02'", "id\n7\n");
                assertExpected("SELECT id FROM lp_range WHERE dateadd('M',1,ts) IN '2020-02'", "id\n1\n2\n3\n");
                execute("DROP TABLE lp_range");
            });
        }
        {
            final boolean nanos = true;
            assertMemoryLeak(() -> {
                createRows(nanos);
                execute("INSERT INTO lp_range VALUES (6,'2020-03-29T00:30:00Z',null),(7,'2020-03-29T01:30:00Z',null)");
                {
                    final String predicate = "dateadd('M',1,ts) IN '2020-02'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-01-01T00:00:00.000000000Z	1
                            2020-01-01T12:00:00.000000000Z	2
                            2020-01-02T00:00:00.000000000Z	3
                            """);
                }
                {
                    final String predicate = "dateadd('y',1,ts) IN '2021-01-01'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-01-01T00:00:00.000000000Z	1
                            2020-01-01T12:00:00.000000000Z	2
                            """);
                }
                {
                    final String predicate = "to_timezone(ts,'Europe/London') IN '2020-03-29T02'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-03-29T01:30:00.000000000Z	7
                            """);
                }
                {
                    final String predicate = "to_utc(ts,'Europe/London') IN '2020-03-29T00'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-03-29T00:30:00.000000000Z	6
                            """);
                }
                {
                    final String predicate = "to_timezone(ts,'+02:00') IN '2020-01-01'";
                    assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                            ts	id
                            2020-01-01T00:00:00.000000000Z	1
                            2020-01-01T12:00:00.000000000Z	2
                            """);
                }
                assertExpected("SELECT id FROM lp_range WHERE to_timezone(ts,'Europe/London') IN '2020-03-29T02'", "id\n7\n");
                assertExpected("SELECT id FROM lp_range WHERE dateadd('M',1,ts) IN '2020-02'", "id\n1\n2\n3\n");
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testMonotonicInDeclinedShapesAndNullKeepTheirFilters() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            {
                final String predicate = "timestamp_floor('d',ts) IN '2020-01-01;1d;1M;3'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        2020-02-01T00:00:00.000000Z	4
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "timestamp_floor('d',ts) IN ('2020-01-01','2020-02-01')";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        2020-02-01T00:00:00.000000Z	4
                        """);
            }
            {
                final String predicate = "timestamp_floor('d',ts) NOT IN '2020-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-02-01T00:00:00.000000Z	4
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            {
                final String predicate = "timestamp_floor('d',other) IN '2020-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        """);
            }
            {
                final String predicate = "timestamp_floor('d',ts) IN null";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        """);
            }
            {
                final String predicate = "timestamp_floor('d',ts) IN CAST(null AS STRING)";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        """);
            }
            {
                final String predicate = "timestamp_floor('d',ts) IN CAST(null AS TIMESTAMP)";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        """);
            }
            {
                final String predicate = "ts+9000000000000000000L IN '2020-01'";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        """);
            }
            {
                final String predicate = "timestamp_floor('d',ts) IN '2020-01' OR id=5";
                assertQueryRows("SELECT ts,id FROM lp_range WHERE " + predicate, """
                        ts	id
                        2020-01-01T00:00:00.000000Z	1
                        2020-01-01T12:00:00.000000Z	2
                        2020-01-02T00:00:00.000000Z	3
                        2020-03-01T00:00:00.000000Z	5
                        """);
            }
            assertQueryRows(
                    "SELECT ts,id FROM (SELECT ts,id FROM lp_range LIMIT 2) WHERE timestamp_floor('d',ts) IN '2020-02'",
                    """
                            ts	id
                            """
            );
            bindVariableService.setStr(0, "2020-01");
            final String sql = "SELECT ts,id FROM lp_range WHERE timestamp_floor('d',ts) IN $1";
            try (RecordCursorFactory factory = compile(sql)) {
                assertRows(factory, "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-01-02T00:00:00.000000Z\t3\n");
                bindVariableService.setStr(0, "2020-02");
                assertRows(factory, "ts\tid\n2020-02-01T00:00:00.000000Z\t4\n");
                bindVariableService.setStr(0, null);
                assertRows(factory, "ts\tid\n");
                bindVariableService.setStr(0, "2020-01-01;1d;1M;3");
                assertRows(factory, "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n2020-01-01T12:00:00.000000Z\t2\n2020-02-01T00:00:00.000000Z\t4\n2020-03-01T00:00:00.000000Z\t5\n");
            }
            bindVariableService.clear();
        });
    }

    @Test
    public void testMonotonicInNegativeEpochTextAndTypedPoints() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_range(id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_range VALUES (1,'1970-01-01'),(2,'1970-01-01T00:00:00.000001Z')");
            final String value = "'1969-12-31T23:59:59.999999999Z'";
            final String text = "SELECT id FROM lp_range WHERE ts-1L IN " + value;
            final String typed = "SELECT id FROM lp_range WHERE ts-1L IN CAST(" + value + " AS TIMESTAMP_NS)";
            assertQueryRows(text, """
                    id
                    1
                    """);
            assertQueryRows(typed, """
                    id
                    2
                    """);
            assertExpected(text, "id\n1\n");
            assertExpected(typed, "id\n2\n");
        });
    }

    @Test
    public void testMonotonicInFoldedTextConstant() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String sql = "SELECT ts,id FROM lp_range WHERE timestamp_floor('d',ts) IN CAST('2020-01' AS STRING)";
            assertExpected("SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN CAST('2020-01' AS STRING)", "id\n1\n2\n3\n");
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: lp_range
                                  intervals: [("2020-01-01T00:00:00.000000Z","2020-01-31T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts	id
                            2020-01-01T00:00:00.000000Z	1
                            2020-01-01T12:00:00.000000Z	2
                            2020-01-02T00:00:00.000000Z	3
                            """);
        });
    }

    @Test
    public void testMonotonicInDateVariablesReevaluateOnEveryCursorOpen() throws Exception {
        {
            final boolean nanos = false;
            assertMemoryLeak(() -> {
                createRows(nanos);
                try {
                    {
                        final String interval = "'$today'";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                """);
                    }
                    {
                        final String interval = "CAST('$today' AS STRING)";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                """);
                    }
                    {
                        final String interval = "'[$today,$tomorrow]'";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                3
                                """);
                    }
                    {
                        final String interval = "CAST('[$today,$tomorrow]' AS STRING)";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                3
                                """);
                    }
                } finally {
                    setCurrentMicros(-1);
                }
                execute("DROP TABLE lp_range");
            });
        }
        {
            final boolean nanos = true;
            assertMemoryLeak(() -> {
                createRows(nanos);
                try {
                    {
                        final String interval = "'$today'";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                """);
                    }
                    {
                        final String interval = "CAST('$today' AS STRING)";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                """);
                    }
                    {
                        final String interval = "'[$today,$tomorrow]'";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                3
                                """);
                    }
                    {
                        final String interval = "CAST('[$today,$tomorrow]' AS STRING)";
                        final boolean includesTomorrow = interval.contains("$tomorrow");
                        final String sql = "SELECT id FROM lp_range WHERE timestamp_floor('d',ts) IN " + interval;
                        setCurrentMicros(1_577_836_800_000_000L);
                        try (RecordCursorFactory factory = compile(sql)) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                            setCurrentMicros(1_577_923_200_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n3\n");
                            setCurrentMicros(1_583_020_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns("id\n5\n");
                            setCurrentMicros(1_577_836_800_000_000L);
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(includesTomorrow ? "id\n1\n2\n3\n" : "id\n1\n2\n");
                        }
                        assertQueryRows(sql, """
                                id
                                1
                                2
                                3
                                """);
                    }
                } finally {
                    setCurrentMicros(-1);
                }
                execute("DROP TABLE lp_range");
            });
        }
    }

    @Test
    public void testLimitAndCastProjectionKeepPredicateScope() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertQueryRows(
                    "SELECT ts,id FROM (SELECT ts,id FROM lp_range LIMIT 2) WHERE ts IN '2020-02'",
                    """
                            ts	id
                            """
            );
            assertQueryRows(
                    "SELECT ts,id FROM (SELECT ts,id FROM lp_range LIMIT 2) WHERE ts BETWEEN '2020-02-01' AND '2020-03-01'",
                    """
                            ts	id
                            """
            );
            assertQueryRows(
                    "SELECT ts,id FROM (SELECT ts::timestamp ts,id FROM lp_range) WHERE ts"
                    + " BETWEEN '2020-01-01T00:00:00.000000001Z' AND '2020-01-01T00:00:00.000000001Z'",
                    """
                            ts	id
                            2020-01-01T00:00:00.000000Z	1
                            """
            );
        });
    }

    private static String nativeRows(String microsRows, boolean nanos) {
        return nanos ? microsRows.replaceAll("(\\.\\d{6})Z", "$1000Z") : microsRows;
    }

    private void assertExpected(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
        }
    }

    private void assertRebound(String sql, boolean nanos, String initial, String swapped, String firstNull, String secondNull) throws Exception {
        final int oldMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        setTimestamp(0, 1_577_836_800_000_000L, nanos);
        setTimestamp(1, 1_580_515_200_000_000L, nanos);
        try (RecordCursorFactory factory = compile(sql)) {
            assertRows(factory, nativeRows(initial, nanos));
            setTimestamp(0, 1_580_515_200_000_000L, nanos);
            setTimestamp(1, 1_577_836_800_000_000L, nanos);
            assertRows(factory, nativeRows(swapped, nanos));
            setTimestamp(0, Numbers.LONG_NULL, nanos);
            assertRows(factory, nativeRows(firstNull, nanos));
            setTimestamp(0, 1_577_836_800_000_000L, nanos);
            setTimestamp(1, Numbers.LONG_NULL, nanos);
            assertRows(factory, nativeRows(secondNull, nanos));
        } finally {
            sqlExecutionContext.setJitMode(oldMode);
            bindVariableService.clear();
        }
    }

    private void assertRows(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private RecordCursorFactory compile(String sql) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            return compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        }
    }

    private void createRows(boolean nanos) throws SqlException {
        execute("CREATE TABLE lp_range(id INT,ts " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP")
                + ",other TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO lp_range VALUES (1,'2020-01-01','2020-01-01'),(2,'2020-01-01T12:00:00','2020-01-02'),"
                + "(3,'2020-01-02',null),(4,'2020-02-01','2020-02-01'),(5,'2020-03-01','2020-03-01')");
    }

    private void setTimestamp(int index, long micros, boolean nanos) throws SqlException {
        if (nanos) {
            bindVariableService.setTimestampNano(index, micros == Numbers.LONG_NULL ? micros : micros * 1_000);
        } else {
            bindVariableService.setTimestamp(index, micros);
        }
    }
    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
