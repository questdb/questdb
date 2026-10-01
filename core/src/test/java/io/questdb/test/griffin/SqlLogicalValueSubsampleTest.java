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

import io.questdb.PropertyKey;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalValueSubsampleTest extends AbstractCairoTest {
    @Test
    public void testAliasesNumericTypesAndHiddenOrderColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String[] types = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE"};
            {
                final String method = bucketMethods()[0];
                {
                    final String type = types[0];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:03.000000Z	0
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[1];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:03.000000Z	0
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[2];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[3];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[4];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String type = types[5];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                assertQueryRows(
                        "SELECT ts clock,v+id AS value FROM lp_value_subsample SUBSAMPLE " + method
                        + "(value,4) ORDER BY lp_value_subsample.x",
                        """
                                clock	value
                                2024-01-01T00:10:08.000000Z	18.0
                                2024-01-01T00:10:07.000000Z	67.0
                                2024-01-01T00:00:01.000000Z	11.0
                                """
                );
                assertQueryRows(
                        "SELECT ts,v AS __keep_subsample FROM lp_value_subsample SUBSAMPLE " + method
                        + "(__keep_subsample,4) ORDER BY x%2,v DESC",
                        """
                                ts	__keep_subsample
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "SELECT *,v AS value FROM lp_value_subsample SUBSAMPLE " + method + "(value,4)",
                        """
                                id	v	x	ts	value
                                1	10.0	80	2024-01-01T00:00:01.000000Z	10.0
                                7	60.0	70	2024-01-01T00:10:07.000000Z	60.0
                                8	10.0	60	2024-01-01T00:10:08.000000Z	10.0
                                """
                );
            }
            {
                final String method = bucketMethods()[1];
                {
                    final String type = types[0];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:00:03.000000Z	0
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[1];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:00:03.000000Z	0
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[2];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[3];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[4];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String type = types[5];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                assertQueryRows(
                        "SELECT ts clock,v+id AS value FROM lp_value_subsample SUBSAMPLE " + method
                        + "(value,4) ORDER BY lp_value_subsample.x",
                        """
                                clock	value
                                2024-01-01T00:00:02.000000Z	52.0
                                2024-01-01T00:10:08.000000Z	18.0
                                2024-01-01T00:10:07.000000Z	67.0
                                2024-01-01T00:00:01.000000Z	11.0
                                """
                );
                assertQueryRows(
                        "SELECT ts,v AS __keep_subsample FROM lp_value_subsample SUBSAMPLE " + method
                        + "(__keep_subsample,4) ORDER BY x%2,v DESC",
                        """
                                ts	__keep_subsample
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "SELECT *,v AS value FROM lp_value_subsample SUBSAMPLE " + method + "(value,4)",
                        """
                                id	v	x	ts	value
                                1	10.0	80	2024-01-01T00:00:01.000000Z	10.0
                                2	50.0	10	2024-01-01T00:00:02.000000Z	50.0
                                7	60.0	70	2024-01-01T00:10:07.000000Z	60.0
                                8	10.0	60	2024-01-01T00:10:08.000000Z	10.0
                                """
                );
            }
            {
                final String method = bucketMethods()[2];
                {
                    final String type = types[0];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[1];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[2];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[3];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10
                                    2024-01-01T00:00:02.000000Z	50
                                    2024-01-01T00:10:07.000000Z	60
                                    2024-01-01T00:10:08.000000Z	10
                                    """
                    );
                }
                {
                    final String type = types[4];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String type = types[5];
                    assertQueryRows(
                            "SELECT ts clock,v::" + type + " value FROM lp_value_subsample SUBSAMPLE "
                            + method + "(value,4)",
                            """
                                    clock	value
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                assertQueryRows(
                        "SELECT ts clock,v+id AS value FROM lp_value_subsample SUBSAMPLE " + method
                        + "(value,4) ORDER BY lp_value_subsample.x",
                        """
                                clock	value
                                2024-01-01T00:00:02.000000Z	52.0
                                2024-01-01T00:10:08.000000Z	18.0
                                2024-01-01T00:10:07.000000Z	67.0
                                2024-01-01T00:00:01.000000Z	11.0
                                """
                );
                assertQueryRows(
                        "SELECT ts,v AS __keep_subsample FROM lp_value_subsample SUBSAMPLE " + method
                        + "(__keep_subsample,4) ORDER BY x%2,v DESC",
                        """
                                ts	__keep_subsample
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "SELECT *,v AS value FROM lp_value_subsample SUBSAMPLE " + method + "(value,4)",
                        """
                                id	v	x	ts	value
                                1	10.0	80	2024-01-01T00:00:01.000000Z	10.0
                                2	50.0	10	2024-01-01T00:00:02.000000Z	50.0
                                7	60.0	70	2024-01-01T00:10:07.000000Z	60.0
                                8	10.0	60	2024-01-01T00:10:08.000000Z	10.0
                                """
                );
            }
            assertQueryRows(
                    "SELECT ts clock,v+id value FROM lp_value_subsample SUBSAMPLE sdt(value,0.5) ORDER BY x",
                    """
                            clock	value
                            2024-01-01T00:00:02.000000Z	52.0
                            2024-01-01T00:00:04.000000Z	44.0
                            2024-01-01T00:10:05.000000Z	35.0
                            2024-01-01T00:00:03.000000Z	null
                            2024-01-01T00:10:06.000000Z	26.0
                            2024-01-01T00:10:08.000000Z	18.0
                            2024-01-01T00:10:07.000000Z	67.0
                            2024-01-01T00:00:01.000000Z	11.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,v AS \"value.dot\" FROM lp_value_subsample SUBSAMPLE sdt(\"value.dot\",abs(-0.5))",
                    """
                            ts	value.dot
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:02.000000Z	50.0
                            2024-01-01T00:00:03.000000Z	null
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:10:05.000000Z	30.0
                            2024-01-01T00:10:06.000000Z	20.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """
            );
            assertQueryRows(
                    "SELECT ts AS \"clock.dot\",v AS \"value.dot\" FROM lp_value_subsample SUBSAMPLE m4(\"value.dot\",4)",
                    """
                            clock.dot	value.dot
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """
            );
        });
    }

    @Test
    public void testBucketCompletedAggregationsAndJoins() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String method = bucketMethods()[0];
                assertQueryRows(
                        "SELECT ts,sum(v) value FROM lp_value_subsample GROUP BY ts SUBSAMPLE " + method + "(value,4)",
                        """
                                ts	value
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:00:01.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "SELECT DISTINCT ts,v FROM lp_value_subsample SUBSAMPLE " + method + "(v,4) ORDER BY v",
                        """
                                ts	v
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:10:07.000000Z	60.0
                                """
                );
                assertQueryRows(
                        "SELECT a.ts,b.v value FROM lp_value_subsample a ASOF JOIN lp_value_subsample b SUBSAMPLE "
                        + method + "(value,4)",
                        """
                                ts	value
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "WITH q AS(SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + method
                        + "(v,4)) SELECT * FROM q UNION ALL SELECT * FROM q ORDER BY ts,v",
                        """
                                ts	v
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
            }
            {
                final String method = bucketMethods()[1];
                assertQueryRows(
                        "SELECT ts,sum(v) value FROM lp_value_subsample GROUP BY ts SUBSAMPLE " + method + "(value,4)",
                        """
                                ts	value
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:00:01.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "SELECT DISTINCT ts,v FROM lp_value_subsample SUBSAMPLE " + method + "(v,4) ORDER BY v",
                        """
                                ts	v
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:10:07.000000Z	60.0
                                """
                );
                assertQueryRows(
                        "SELECT a.ts,b.v value FROM lp_value_subsample a ASOF JOIN lp_value_subsample b SUBSAMPLE "
                        + method + "(value,4)",
                        """
                                ts	value
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "WITH q AS(SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + method
                        + "(v,4)) SELECT * FROM q UNION ALL SELECT * FROM q ORDER BY ts,v",
                        """
                                ts	v
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
            }
            {
                final String method = bucketMethods()[2];
                assertQueryRows(
                        "SELECT ts,sum(v) value FROM lp_value_subsample GROUP BY ts SUBSAMPLE " + method + "(value,4)",
                        """
                                ts	value
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:00:01.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "SELECT DISTINCT ts,v FROM lp_value_subsample SUBSAMPLE " + method + "(v,4) ORDER BY v",
                        """
                                ts	v
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:10:07.000000Z	60.0
                                """
                );
                assertQueryRows(
                        "SELECT a.ts,b.v value FROM lp_value_subsample a ASOF JOIN lp_value_subsample b SUBSAMPLE "
                        + method + "(value,4)",
                        """
                                ts	value
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
                assertQueryRows(
                        "WITH q AS(SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + method
                        + "(v,4)) SELECT * FROM q UNION ALL SELECT * FROM q ORDER BY ts,v",
                        """
                                ts	v
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:00:01.000000Z	10.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:00:02.000000Z	50.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:07.000000Z	60.0
                                2024-01-01T00:10:08.000000Z	10.0
                                2024-01-01T00:10:08.000000Z	10.0
                                """
                );
            }
        });
    }

    @Test
    public void testBucketTargetBindingsAndRetainedFactory() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (String method : bucketMethods()) {
                final String sql = "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + method + "(v,$1)";
                bindVariableService.setLong(0, 4);
                final String expected = "m4".equals(method) ? """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:10:07.000000Z	60.0
                        2024-01-01T00:10:08.000000Z	10.0
                        """ : """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:02.000000Z	50.0
                        2024-01-01T00:10:07.000000Z	60.0
                        2024-01-01T00:10:08.000000Z	10.0
                        """;
                RecordCursorFactory retained = null;
                try {
                    final String expectedPlan;
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                        retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                        assertResult(retained, expected);
                        expectedPlan = plan(retained);
                        compiler.clear();
                        try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_value_subsample", sqlExecutionContext).getRecordCursorFactory()) {
                            Assert.assertNotNull(ignored);
                        }
                    }
                    assertResult(retained, expected);
                    TestUtils.assertEquals(expectedPlan, plan(retained));
                    bindVariableService.setLong(0, 30);
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                         RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(retained, print(factory));
                    }
                    bindVariableService.setLong(0, 1);
                    try (RecordCursor ignored = retained.getCursor(sqlExecutionContext)) {
                        Assert.fail("target must be rejected on reopen");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "at least 2");
                    }
                    bindVariableService.setLong(0, 4);
                    assertResult(retained, expected);
                } finally {
                    Misc.free(retained);
                }
            }
        });
    }

    @Test
    public void testCachedSelectionEmptyNullsAndInputOrder() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String[] calls = {"m4(v,4)", "minmax(v,4)", "lttb(v,4)", "sdt(v,0.5)"};
            {
                final int light = 0;
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                {
                    final String call = calls[0];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    """
                    );
                }
                {
                    final String call = calls[1];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:02.000000Z	50.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String call = calls[2];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:02.000000Z	50.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String call = calls[3];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:02.000000Z	50.0
                            2024-01-01T00:00:03.000000Z	null
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:10:05.000000Z	30.0
                            2024-01-01T00:10:06.000000Z	20.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:00:03.000000Z	null
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:06.000000Z	20.0
                                    2024-01-01T00:10:05.000000Z	30.0
                                    2024-01-01T00:00:04.000000Z	40.0
                                    2024-01-01T00:00:03.000000Z	null
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:04.000000Z	40.0
                                    """
                    );
                }
            }
            {
                final int light = 1;
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                {
                    final String call = calls[0];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    """
                    );
                }
                {
                    final String call = calls[1];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:02.000000Z	50.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String call = calls[2];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:02.000000Z	50.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String call = calls[3];
                    assertQueryRows("SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call, """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:02.000000Z	50.0
                            2024-01-01T00:00:03.000000Z	null
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:10:05.000000Z	30.0
                            2024-01-01T00:10:06.000000Z	20.0
                            2024-01-01T00:10:07.000000Z	60.0
                            2024-01-01T00:10:08.000000Z	10.0
                            """);
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE id<0 SUBSAMPLE " + call,
                            """
                                    ts	v
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample WHERE v IS NULL SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:00:03.000000Z	null
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM (SELECT * FROM lp_value_subsample ORDER BY ts DESC) SUBSAMPLE " + call,
                            """
                                    ts	v
                                    2024-01-01T00:10:08.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:06.000000Z	20.0
                                    2024-01-01T00:10:05.000000Z	30.0
                                    2024-01-01T00:00:04.000000Z	40.0
                                    2024-01-01T00:00:03.000000Z	null
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:01.000000Z	10.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,v FROM lp_value_subsample SUBSAMPLE " + call + " ORDER BY x LIMIT 2",
                            """
                                    ts	v
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:00:04.000000Z	40.0
                                    """
                    );
                }
            }
        });
    }

    @Test
    public void testMicroNanoAndLttbGap() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_value_subsample_ns AS (SELECT id,v,x,ts::TIMESTAMP_NS ts FROM lp_value_subsample) TIMESTAMP(ts)");
            {
                final int precision = 0;
                final String table = precision == 0 ? "lp_value_subsample" : "lp_value_subsample_ns";
                {
                    final String method = bucketMethods()[0];
                    assertQueryRows(
                            "SELECT ts,v FROM " + table + " SUBSAMPLE " + method + "(v,4)",
                            """
                                    ts	v
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String method = bucketMethods()[1];
                    assertQueryRows(
                            "SELECT ts,v FROM " + table + " SUBSAMPLE " + method + "(v,4)",
                            """
                                    ts	v
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                {
                    final String method = bucketMethods()[2];
                    assertQueryRows(
                            "SELECT ts,v FROM " + table + " SUBSAMPLE " + method + "(v,4)",
                            """
                                    ts	v
                                    2024-01-01T00:00:01.000000Z	10.0
                                    2024-01-01T00:00:02.000000Z	50.0
                                    2024-01-01T00:10:07.000000Z	60.0
                                    2024-01-01T00:10:08.000000Z	10.0
                                    """
                    );
                }
                assertQueryRows("SELECT ts,v FROM " + table + " SUBSAMPLE lttb(v,4,'1m')", """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:04.000000Z	40.0
                        2024-01-01T00:10:05.000000Z	30.0
                        2024-01-01T00:10:08.000000Z	10.0
                        """);
                assertQueryRows("SELECT ts,v FROM " + table + " SUBSAMPLE sdt(v,0.5)", """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:02.000000Z	50.0
                        2024-01-01T00:00:03.000000Z	null
                        2024-01-01T00:00:04.000000Z	40.0
                        2024-01-01T00:10:05.000000Z	30.0
                        2024-01-01T00:10:06.000000Z	20.0
                        2024-01-01T00:10:07.000000Z	60.0
                        2024-01-01T00:10:08.000000Z	10.0
                        """);
            }
            {
                final int precision = 1;
                final String table = precision == 0 ? "lp_value_subsample" : "lp_value_subsample_ns";
                {
                    final String method = bucketMethods()[0];
                    assertQueryRows(
                            "SELECT ts,v FROM " + table + " SUBSAMPLE " + method + "(v,4)",
                            """
                                    ts	v
                                    2024-01-01T00:00:01.000000000Z	10.0
                                    2024-01-01T00:10:07.000000000Z	60.0
                                    2024-01-01T00:10:08.000000000Z	10.0
                                    """
                    );
                }
                {
                    final String method = bucketMethods()[1];
                    assertQueryRows(
                            "SELECT ts,v FROM " + table + " SUBSAMPLE " + method + "(v,4)",
                            """
                                    ts	v
                                    2024-01-01T00:00:01.000000000Z	10.0
                                    2024-01-01T00:00:02.000000000Z	50.0
                                    2024-01-01T00:10:07.000000000Z	60.0
                                    2024-01-01T00:10:08.000000000Z	10.0
                                    """
                    );
                }
                {
                    final String method = bucketMethods()[2];
                    assertQueryRows(
                            "SELECT ts,v FROM " + table + " SUBSAMPLE " + method + "(v,4)",
                            """
                                    ts	v
                                    2024-01-01T00:00:01.000000000Z	10.0
                                    2024-01-01T00:00:02.000000000Z	50.0
                                    2024-01-01T00:10:07.000000000Z	60.0
                                    2024-01-01T00:10:08.000000000Z	10.0
                                    """
                    );
                }
                assertQueryRows("SELECT ts,v FROM " + table + " SUBSAMPLE lttb(v,4,'1m')", """
                        ts	v
                        2024-01-01T00:00:01.000000000Z	10.0
                        2024-01-01T00:00:04.000000000Z	40.0
                        2024-01-01T00:10:05.000000000Z	30.0
                        2024-01-01T00:10:08.000000000Z	10.0
                        """);
                assertQueryRows("SELECT ts,v FROM " + table + " SUBSAMPLE sdt(v,0.5)", """
                        ts	v
                        2024-01-01T00:00:01.000000000Z	10.0
                        2024-01-01T00:00:02.000000000Z	50.0
                        2024-01-01T00:00:03.000000000Z	null
                        2024-01-01T00:00:04.000000000Z	40.0
                        2024-01-01T00:10:05.000000000Z	30.0
                        2024-01-01T00:10:06.000000000Z	20.0
                        2024-01-01T00:10:07.000000000Z	60.0
                        2024-01-01T00:10:08.000000000Z	10.0
                        """);
            }
        });
    }

    @Test
    public void testSdtEndpointsHaveIndependentOracle() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sdt_line AS (SELECT x::DOUBLE v,x::TIMESTAMP ts FROM long_sequence(5)) TIMESTAMP(ts)");
            try (RecordCursorFactory factory = select("SELECT ts,v FROM lp_sdt_line SUBSAMPLE sdt(v,0.5)")) {
                assertResult(factory, "ts\tv\n1970-01-01T00:00:00.000001Z\t1.0\n1970-01-01T00:00:00.000005Z\t5.0\n");
            }
        });
    }

    @Test
    public void testValidationPrecedenceAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String source = " FROM lp_value_subsample SUBSAMPLE ";
            assertQuery("SELECT ts,v" + source + "m4(v)").noLeakCheck().fails(46, "m4() requires at least 2 arguments: column and target points");
            assertQuery("SELECT ts,v" + source + "minmax(v,4,5)").noLeakCheck().fails(57, "minmax() accepts exactly 2 arguments: column and target points");
            assertQuery("SELECT ts,v" + source + "lttb(v,4,'1m',5)").noLeakCheck().fails(60, "lttb() accepts at most 3 arguments: column, target points, and optional gap threshold");
            assertQuery("SELECT ts,v" + source + "m4(missing,0)").noLeakCheck().fails(49, "column not found in SELECT list: missing");
            assertQuery("SELECT ts,v" + source + "m4(v+1,4)").noLeakCheck().fails(50, "SUBSAMPLE value argument must be a column name; alias the expression in the SELECT list and reference the alias");
            assertQuery("SELECT ts,v" + source + "m4(1,4)").noLeakCheck().fails(49, "SUBSAMPLE value argument must be a column name, not a constant");
            assertQuery("SELECT ts,v" + source + "m4($1,4)").noLeakCheck().fails(49, "SUBSAMPLE value argument must be a column name, not a bind variable");
            assertQuery("SELECT ts,v" + source + "m4(lp_value_subsample.v,4)").noLeakCheck().fails(49, "qualified column names are not supported in SUBSAMPLE arguments; use the unqualified SELECT list name");
            assertQuery("SELECT ts,v value" + source + "m4(v,4)").noLeakCheck().fails(55, "column not found in SELECT list: v");
            assertQuery("SELECT v" + source + "minmax(v,4)").noLeakCheck().fails(33, "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertQuery("SELECT v" + source + "minmax(missing,4)").noLeakCheck().fails(50, "column not found in SELECT list: missing");
            assertQuery("SELECT ts,v::STRING v" + source + "minmax(v,0)").noLeakCheck().fails(63, "numeric column expected, got: STRING");
            assertQuery("SELECT ts,v" + source + "lttb(v,4,'1'||'m')").noLeakCheck().fails(58, "gap threshold must be a string constant such as '1h'");
            assertQuery("SELECT ts,v" + source + "lttb(v,0,'bad')").noLeakCheck().fails(53, "target points must be at least 2");
            assertQuery("SELECT ts,v" + source + "lttb(v,4,'1w')").noLeakCheck().fails(57, "unsupported interval unit: w. Supported: s, m, h, d");
            assertQuery("SELECT ts,v" + source + "sdt(v)").noLeakCheck().fails(46, "sdt() requires exactly 2 arguments: column and compdev");
            assertQuery("SELECT v" + source + "sdt(missing,-1)").noLeakCheck().fails(43, "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertQuery("SELECT ts,v" + source + "sdt(v,-1)").noLeakCheck().fails(52, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertQuery("SELECT ts,v" + source + "sdt(v,NULL)").noLeakCheck().fails(52, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertQuery("SELECT ts,v" + source + "sdt(v,'0.5')").noLeakCheck().fails(52, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertQuery("SELECT ts,v" + source + "sdt(v,1.0/0.0)").noLeakCheck().fails(55, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertQuery("SELECT ts,v" + source + "sdt(v,missing)").noLeakCheck().fails(52, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertQuery("SELECT ts,v" + source + "sdt(v,:missing)").noLeakCheck().fails(52, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertQuery("SELECT ts,v" + source + "sdt(v,$0)").noLeakCheck().fails(52, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            bindVariableService.setDouble(0, 0.5);
            assertQuery("SELECT ts,v" + source + "sdt(v,$1)").noLeakCheck().fails(52, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertQuery("SELECT ts,sum(v) v FROM lp_value_subsample GROUP BY ts SUBSAMPLE sdt(v,0.5)").noLeakCheck().fails(65, "SUBSAMPLE sdt is not supported in an aggregation context");
            assertQuery("SELECT DISTINCT ts,v" + source + "sdt(v,0.5)").noLeakCheck().fails(55, "SUBSAMPLE sdt is not supported in an aggregation context");
            assertQuery("SELECT a.ts,a.v FROM lp_value_subsample a ASOF JOIN lp_value_subsample b SUBSAMPLE sdt(v,0.5)").noLeakCheck().fails(83, "SUBSAMPLE sdt is not supported inside a join");
            assertQuery("SELECT a.ts,b.v FROM lp_value_subsample a ASOF JOIN "
                    + "(SELECT ts,v FROM lp_value_subsample SUBSAMPLE sdt(v,0.5)) b").noLeakCheck().fails(99, "SUBSAMPLE sdt is not supported inside a join");
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private static String[] bucketMethods() {
        return new String[]{"m4", "minmax", "lttb"};
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_value_subsample(id INT,v DOUBLE,x INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_value_subsample VALUES(1,10.0,80,'2024-01-01T00:00:01'),"
                + "(2,50.0,10,'2024-01-01T00:00:02'),(3,NULL,40,'2024-01-01T00:00:03'),"
                + "(4,40.0,20,'2024-01-01T00:00:04'),(5,30.0,30,'2024-01-01T00:10:05'),"
                + "(6,20.0,50,'2024-01-01T00:10:06'),(7,60.0,70,'2024-01-01T00:10:07'),"
                + "(8,10.0,60,'2024-01-01T00:10:08')");
    }

    private boolean hasSubsample(LogicalPlan plan) {
        if (plan.getType() == LogicalPlan.Type.WINDOW) {
            final WindowPlan window = (WindowPlan) plan;
            for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
                if (window.getSpecs().getQuick(i).isSubsampleKeepFlag()) {
                    return true;
                }
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasSubsample(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        return sink.getSink().toString();
    }

    private String print(RecordCursorFactory factory) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }
    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
