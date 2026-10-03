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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ArrayElementTest extends AbstractCairoTest {
    @Test
    public void testSingleArgumentOverloadsRemainAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String name = "sum";
                assertQueryRows("SELECT array_elem_" + name + "(a) FROM lp_array_element", """
                        array_elem_sum
                        [7.0,10.0]
                        """);
                assertQueryRows(
                        "SELECT id%2 k,array_elem_" + name + "(a) FROM lp_array_element GROUP BY k ORDER BY k",
                        """
                                k	array_elem_sum
                                0	[1.0,2.0]
                                1	[6.0,8.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a) FROM lp_array_element WHERE id<0",
                        """
                                array_elem_sum
                                null
                                """
                );
            }
            {
                final String name = "avg";
                assertQueryRows("SELECT array_elem_" + name + "(a) FROM lp_array_element", """
                        array_elem_avg
                        [2.3333333333333335,3.3333333333333335]
                        """);
                assertQueryRows(
                        "SELECT id%2 k,array_elem_" + name + "(a) FROM lp_array_element GROUP BY k ORDER BY k",
                        """
                                k	array_elem_avg
                                0	[1.0,2.0]
                                1	[3.0,4.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a) FROM lp_array_element WHERE id<0",
                        """
                                array_elem_avg
                                null
                                """
                );
            }
            {
                final String name = "min";
                assertQueryRows("SELECT array_elem_" + name + "(a) FROM lp_array_element", """
                        array_elem_min
                        [1.0,2.0]
                        """);
                assertQueryRows(
                        "SELECT id%2 k,array_elem_" + name + "(a) FROM lp_array_element GROUP BY k ORDER BY k",
                        """
                                k	array_elem_min
                                0	[1.0,2.0]
                                1	[1.0,2.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a) FROM lp_array_element WHERE id<0",
                        """
                                array_elem_min
                                null
                                """
                );
            }
            {
                final String name = "max";
                assertQueryRows("SELECT array_elem_" + name + "(a) FROM lp_array_element", """
                        array_elem_max
                        [5.0,6.0]
                        """);
                assertQueryRows(
                        "SELECT id%2 k,array_elem_" + name + "(a) FROM lp_array_element GROUP BY k ORDER BY k",
                        """
                                k	array_elem_max
                                0	[1.0,2.0]
                                1	[5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a) FROM lp_array_element WHERE id<0",
                        """
                                array_elem_max
                                null
                                """
                );
            }
            assertQueryRows("SELECT array_elem_sum(a) s FROM lp_array_element", "s\n[7.0,10.0]\n");
        });
    }

    @Test
    public void testScalarOverloadsBecomeImplicitKeysOnlyOutsideAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String name = "sum";
                final String call = "array_elem_" + name + "(a,b)";
                assertQueryRows("SELECT " + call + " FROM lp_array_element", """
                        array_elem_sum
                        [4.0,6.0]
                        [12.0,14.0]
                        """);
                assertQueryRows("SELECT " + call + ",count() FROM lp_array_element", """
                        array_elem_sum	count
                        [4.0,6.0]	2
                        [12.0,14.0]	1
                        """);
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,b,a) FROM lp_array_element",
                        """
                                array_elem_sum
                                [5.0,8.0]
                                [17.0,20.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,NULL::DOUBLE[]) FROM lp_array_element",
                        """
                                array_elem_sum
                                [1.0,2.0]
                                [5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a[1:2],b[1:2]) FROM lp_array_element",
                        """
                                array_elem_sum
                                [4.0]
                                [12.0]
                                """
                );
                assertQueryRows("SELECT array_sum(" + call + ") FROM lp_array_element", """
                        array_sum
                        10.0
                        26.0
                        """);
                assertQueryRows("SELECT sum(array_sum(" + call + ")) FROM lp_array_element", """
                        sum
                        46.0
                        """);
                assertQueryRows(
                        "SELECT id FROM lp_array_element WHERE array_sum(" + call + ")>0 ORDER BY id",
                        """
                                id
                                1
                                2
                                3
                                """
                );
            }
            {
                final String name = "avg";
                final String call = "array_elem_" + name + "(a,b)";
                assertQueryRows("SELECT " + call + " FROM lp_array_element", """
                        array_elem_avg
                        [2.0,3.0]
                        [6.0,7.0]
                        """);
                assertQueryRows("SELECT " + call + ",count() FROM lp_array_element", """
                        array_elem_avg	count
                        [2.0,3.0]	2
                        [6.0,7.0]	1
                        """);
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,b,a) FROM lp_array_element",
                        """
                                array_elem_avg
                                [1.6666666666666667,2.6666666666666665]
                                [5.666666666666667,6.666666666666667]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,NULL::DOUBLE[]) FROM lp_array_element",
                        """
                                array_elem_avg
                                [1.0,2.0]
                                [5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a[1:2],b[1:2]) FROM lp_array_element",
                        """
                                array_elem_avg
                                [2.0]
                                [6.0]
                                """
                );
                assertQueryRows("SELECT array_sum(" + call + ") FROM lp_array_element", """
                        array_sum
                        5.0
                        13.0
                        """);
                assertQueryRows("SELECT sum(array_sum(" + call + ")) FROM lp_array_element", """
                        sum
                        23.0
                        """);
                assertQueryRows(
                        "SELECT id FROM lp_array_element WHERE array_sum(" + call + ")>0 ORDER BY id",
                        """
                                id
                                1
                                2
                                3
                                """
                );
            }
            {
                final String name = "min";
                final String call = "array_elem_" + name + "(a,b)";
                assertQueryRows("SELECT " + call + " FROM lp_array_element", """
                        array_elem_min
                        [1.0,2.0]
                        [5.0,6.0]
                        """);
                assertQueryRows("SELECT " + call + ",count() FROM lp_array_element", """
                        array_elem_min	count
                        [1.0,2.0]	2
                        [5.0,6.0]	1
                        """);
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,b,a) FROM lp_array_element",
                        """
                                array_elem_min
                                [1.0,2.0]
                                [5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,NULL::DOUBLE[]) FROM lp_array_element",
                        """
                                array_elem_min
                                [1.0,2.0]
                                [5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a[1:2],b[1:2]) FROM lp_array_element",
                        """
                                array_elem_min
                                [1.0]
                                [5.0]
                                """
                );
                assertQueryRows("SELECT array_sum(" + call + ") FROM lp_array_element", """
                        array_sum
                        3.0
                        11.0
                        """);
                assertQueryRows("SELECT sum(array_sum(" + call + ")) FROM lp_array_element", """
                        sum
                        17.0
                        """);
                assertQueryRows(
                        "SELECT id FROM lp_array_element WHERE array_sum(" + call + ")>0 ORDER BY id",
                        """
                                id
                                1
                                2
                                3
                                """
                );
            }
            {
                final String name = "max";
                final String call = "array_elem_" + name + "(a,b)";
                assertQueryRows("SELECT " + call + " FROM lp_array_element", """
                        array_elem_max
                        [3.0,4.0]
                        [7.0,8.0]
                        """);
                assertQueryRows("SELECT " + call + ",count() FROM lp_array_element", """
                        array_elem_max	count
                        [3.0,4.0]	2
                        [7.0,8.0]	1
                        """);
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,b,a) FROM lp_array_element",
                        """
                                array_elem_max
                                [3.0,4.0]
                                [7.0,8.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a,NULL::DOUBLE[]) FROM lp_array_element",
                        """
                                array_elem_max
                                [1.0,2.0]
                                [5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT array_elem_" + name + "(a[1:2],b[1:2]) FROM lp_array_element",
                        """
                                array_elem_max
                                [3.0]
                                [7.0]
                                """
                );
                assertQueryRows("SELECT array_sum(" + call + ") FROM lp_array_element", """
                        array_sum
                        7.0
                        15.0
                        """);
                assertQueryRows("SELECT sum(array_sum(" + call + ")) FROM lp_array_element", """
                        sum
                        29.0
                        """);
                assertQueryRows(
                        "SELECT id FROM lp_array_element WHERE array_sum(" + call + ")>0 ORDER BY id",
                        """
                                id
                                1
                                2
                                3
                                """
                );
            }
            assertQueryRows("SELECT array_elem_sum(a,b) s,count() n FROM lp_array_element ORDER BY n",
                    "s\tn\n[12.0,14.0]\t1\n[4.0,6.0]\t2\n");
            assertQueryRows("SELECT array_sum(array_elem_sum(a,b)) s FROM lp_array_element", "s\n10.0\n26.0\n");
            assertQueryRows("SELECT sum(array_sum(array_elem_sum(a,b))) s FROM lp_array_element", "s\n46.0\n");
            assertQueryRows("SELECT count(),array_elem_sum(a,b) FROM lp_array_element", """
                    count	array_elem_sum
                    2	[4.0,6.0]
                    1	[12.0,14.0]
                    """);
            assertQueryRows(
                    "SELECT array_elem_sum(a,b) k,array_elem_sum(a) s FROM lp_array_element",
                    """
                            k	s
                            [4.0,6.0]	[2.0,4.0]
                            [12.0,14.0]	[5.0,6.0]
                            """
            );
        });
    }

    @Test
    public void testConstantScalarKeysKeepEmptyInputSemantics() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String name = "sum";
                {
                    final String filter = "";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_sum
                                    [3.0]
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    3.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_sum
                                    [3.0]
                                    """
                    );
                }
                {
                    final String filter = " WHERE id<0";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_sum
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_sum
                                    null
                                    """
                    );
                }
            }
            {
                final String name = "avg";
                {
                    final String filter = "";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_avg
                                    [1.5]
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    1.5
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_avg
                                    [1.0]
                                    """
                    );
                }
                {
                    final String filter = " WHERE id<0";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_avg
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_avg
                                    null
                                    """
                    );
                }
            }
            {
                final String name = "min";
                {
                    final String filter = "";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_min
                                    [1.0]
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    1.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_min
                                    [1.0]
                                    """
                    );
                }
                {
                    final String filter = " WHERE id<0";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_min
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_min
                                    null
                                    """
                    );
                }
            }
            {
                final String name = "max";
                {
                    final String filter = "";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_max
                                    [2.0]
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    2.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_max
                                    [1.0]
                                    """
                    );
                }
                {
                    final String filter = " WHERE id<0";
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_max
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_sum(array_elem_" + name + "(ARRAY[1.0],ARRAY[2.0])) FROM lp_array_element" + filter,
                            """
                                    array_sum
                                    """
                    );
                    assertQueryRows(
                            "SELECT array_elem_" + name + "(ARRAY[1.0]) FROM lp_array_element" + filter,
                            """
                                    array_elem_max
                                    null
                                    """
                    );
                }
            }
            assertQueryRows("SELECT array_elem_sum(ARRAY[1.0],ARRAY[2.0]) s FROM lp_array_element", "s\n[3.0]\n");
            assertQueryRows("SELECT array_elem_sum(ARRAY[1.0],ARRAY[2.0]) s FROM lp_array_element WHERE id<0", "s\n");
            assertQueryRows("SELECT array_elem_sum(ARRAY[1.0]) s FROM lp_array_element WHERE id<0", "s\nnull\n");
        });
    }

    @Test
    public void testAggregateOutputPermutationPreservesVectorAndSymbolLayouts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_array_projection(g INT,s SYMBOL,v INT,a DOUBLE[])");
            execute("INSERT INTO lp_array_projection VALUES(1,'a',10,ARRAY[1.0]),"
                    + "(1,'a',20,ARRAY[2.0]),(2,'b',30,ARRAY[3.0])");
            assertQueryRows("SELECT sum(v) total,g,max(v) maximum FROM lp_array_projection ORDER BY g",
                    "total\tg\tmaximum\n30\t1\t20\n30\t2\t30\n");
            assertQueryRows("SELECT maximum,g,total FROM (SELECT g,sum(v) total,max(v) maximum FROM lp_array_projection) ORDER BY g",
                    "maximum\tg\ttotal\n20\t1\t30\n30\t2\t30\n");
            assertQueryRows("SELECT sum(v) total,s,max(v) maximum FROM lp_array_projection ORDER BY s",
                    "total\ts\tmaximum\n30\ta\t20\n30\tb\t30\n");
            assertQueryRows("SELECT array_elem_sum(a) total,s,count() n FROM lp_array_projection ORDER BY s",
                    "total\ts\tn\n[3.0]\ta\t2\n[3.0]\tb\t1\n");
            assertQueryRows(
                    "SELECT g%2 parity,array_elem_sum(a) FROM lp_array_projection GROUP BY parity ORDER BY parity",
                    """
                            parity	array_elem_sum
                            0	[3.0]
                            1	[3.0]
                            """
            );
        });
    }

    @Test
    public void testKeylessAggregatePermutationKeepsValuesWithTheirColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_array_keyless(v INT,a DOUBLE[])");
            execute("INSERT INTO lp_array_keyless VALUES(10,ARRAY[1.0]),(20,ARRAY[2.0])");
            assertQueryRows("SELECT maximum,total FROM (SELECT sum(v) total,max(v) maximum FROM lp_array_keyless)", "maximum\ttotal\n20\t30\n");
            assertQueryRows("SELECT n,total FROM (SELECT array_elem_sum(a) total,count() n FROM lp_array_keyless)", "n\ttotal\n2\t[3.0]\n");
        });
    }

    @Test
    public void testRepeatedCallsAndHiddenOrderKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT array_elem_sum(a,b),array_elem_sum(a,b) FROM lp_array_element",
                    """
                            array_elem_sum	array_elem_sum1
                            [4.0,6.0]	[4.0,6.0]
                            [12.0,14.0]	[12.0,14.0]
                            """
            );
            assertQueryRows(
                    "SELECT array_elem_sum(a,b) s FROM lp_array_element ORDER BY array_sum(array_elem_sum(a,b)) DESC",
                    """
                            s
                            [12.0,14.0]
                            [4.0,6.0]
                            """
            );
            assertQueryRows(
                    "SELECT id,array_elem_sum(a,b) s FROM lp_array_element ORDER BY id DESC",
                    """
                            id	s
                            3	[12.0,14.0]
                            2	[4.0,6.0]
                            1	[4.0,6.0]
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_array_element ORDER BY array_sum(array_elem_sum(a,b)) DESC,id",
                    """
                            id
                            3
                            1
                            2
                            """
            );
        });
    }

    @Test
    public void testCalendarSamplingRetainsScalarKeysAndAggregateValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String name = "sum";
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_sum
                                2020-01-01T00:00:00.000000Z	[2.0,4.0]
                                2020-01-01T02:00:00.000000Z	[5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a,b) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_sum
                                2020-01-01T00:00:00.000000Z	[4.0,6.0]
                                2020-01-01T02:00:00.000000Z	[12.0,14.0]
                                """
                );
            }
            {
                final String name = "avg";
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_avg
                                2020-01-01T00:00:00.000000Z	[1.0,2.0]
                                2020-01-01T02:00:00.000000Z	[5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a,b) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_avg
                                2020-01-01T00:00:00.000000Z	[2.0,3.0]
                                2020-01-01T02:00:00.000000Z	[6.0,7.0]
                                """
                );
            }
            {
                final String name = "min";
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_min
                                2020-01-01T00:00:00.000000Z	[1.0,2.0]
                                2020-01-01T02:00:00.000000Z	[5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a,b) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_min
                                2020-01-01T00:00:00.000000Z	[1.0,2.0]
                                2020-01-01T02:00:00.000000Z	[5.0,6.0]
                                """
                );
            }
            {
                final String name = "max";
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_max
                                2020-01-01T00:00:00.000000Z	[1.0,2.0]
                                2020-01-01T02:00:00.000000Z	[5.0,6.0]
                                """
                );
                assertQueryRows(
                        "SELECT ts,array_elem_" + name + "(a,b) FROM lp_array_element SAMPLE BY 1h",
                        """
                                ts	array_elem_max
                                2020-01-01T00:00:00.000000Z	[3.0,4.0]
                                2020-01-01T02:00:00.000000Z	[7.0,8.0]
                                """
                );
            }
            assertQueryRows(
                    "SELECT ts,array_sum(array_elem_sum(a,b)) FROM lp_array_element SAMPLE BY 1h",
                    """
                            ts	array_sum
                            2020-01-01T00:00:00.000000Z	10.0
                            2020-01-01T02:00:00.000000Z	26.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,array_elem_sum(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element SAMPLE BY 1h",
                    """
                            ts	array_elem_sum
                            2020-01-01T00:00:00.000000Z	[3.0]
                            2020-01-01T02:00:00.000000Z	[3.0]
                            """
            );
            assertQueryRows(
                    "SELECT ts,array_elem_sum(a,b) FROM lp_array_element WHERE id<0 SAMPLE BY 1h",
                    """
                            ts	array_elem_sum
                            """
            );
        });
    }

    @Test
    public void testScalarKeysSurvivePruningAndFillWithNameClassification() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String[] names = {"sum", "avg", "min", "max"};
            final String[] firstKeys = {"[4.0,6.0]", "[2.0,3.0]", "[1.0,2.0]", "[3.0,4.0]"};
            final String[] lastKeys = {"[12.0,14.0]", "[6.0,7.0]", "[5.0,6.0]", "[7.0,8.0]"};
            for (int i = 0; i < names.length; i++) {
                final String name = names[i];
                final String hiddenKey = "SELECT n FROM (SELECT array_elem_" + name + "(a,b) k,count() n FROM lp_array_element) ORDER BY n";
                assertQueryRows("SELECT n FROM (SELECT " + name + "_key k,count() n FROM lp_array_element) ORDER BY n", "n\n1\n2\n");
                assertQueryRows(hiddenKey, "n\n1\n2\n");

                final String first = firstKeys[i];
                final String last = lastKeys[i];
                final String expected = "ts\tk\tn\n"
                        + "2020-01-01T00:00:00.000000Z\t" + first + "\t2\n"
                        + "2020-01-01T00:00:00.000000Z\t" + last + "\tnull\n"
                        + "2020-01-01T01:00:00.000000Z\t" + first + "\tnull\n"
                        + "2020-01-01T01:00:00.000000Z\t" + last + "\tnull\n"
                        + "2020-01-01T02:00:00.000000Z\t" + last + "\t1\n"
                        + "2020-01-01T02:00:00.000000Z\t" + first + "\tnull\n";
                assertQueryRows("SELECT ts," + name + "_key k,count() n FROM lp_array_element SAMPLE BY 1h FILL(NULL)", expected);
                final String fill = "SELECT ts,array_elem_" + name + "(a,b) k,count() n FROM lp_array_element SAMPLE BY 1h FILL(NULL)";
                assertQueryRows(fill, expected);
                final String expectedKeys = "ts\tk\n"
                        + "2020-01-01T00:00:00.000000Z\t" + first + "\n"
                        + "2020-01-01T00:00:00.000000Z\t" + last + "\n"
                        + "2020-01-01T01:00:00.000000Z\t" + first + "\n"
                        + "2020-01-01T01:00:00.000000Z\t" + last + "\n"
                        + "2020-01-01T02:00:00.000000Z\t" + last + "\n"
                        + "2020-01-01T02:00:00.000000Z\t" + first + "\n";
                assertQueryRows("SELECT ts,k FROM (SELECT ts," + name + "_key k,count() n FROM lp_array_element SAMPLE BY 1h FILL(NULL))", expectedKeys);
                final String keyFill = "SELECT ts,array_elem_" + name + "(a,b) k FROM lp_array_element SAMPLE BY 1h FILL(NULL)";
                assertQueryRows(keyFill, expectedKeys);
            }
        });
    }

    @Test
    public void testGroupingAndOrderDiagnosticsAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String sql = "SELECT array_elem_sum(a,b) FROM lp_array_element GROUP BY id";
                assertQuery(sql).noLeakCheck().fails(0, "not enough columns in group by");
            }
            {
                final String sql = "SELECT id,array_elem_sum(a,b),count() FROM lp_array_element GROUP BY id";
                assertQuery(sql).noLeakCheck().fails(0, "not enough columns in group by");
            }
            {
                final String sql = "SELECT array_elem_sum(a,b),count() FROM lp_array_element GROUP BY array_elem_sum(a,b)";
                assertQuery(sql).noLeakCheck().fails(66, "aggregate functions are not allowed in GROUP BY");
            }
            {
                final String sql = "SELECT array_elem_sum(a,b),count() FROM lp_array_element GROUP BY 1";
                assertQuery(sql).noLeakCheck().fails(66, "aggregate functions are not allowed in GROUP BY");
            }
            {
                final String sql = "SELECT array_elem_sum(a,b) FROM lp_array_element GROUP BY a,b";
                assertQuery(sql).noLeakCheck().fails(0, "not enough columns in group by");
            }
            {
                final String sql = "SELECT a,b,array_elem_sum(a,b) FROM lp_array_element GROUP BY a,b";
                assertQuery(sql).noLeakCheck().fails(0, "not enough columns in group by");
            }
            {
                final String sql = "SELECT array_elem_sum(ARRAY[1.0],ARRAY[2.0]) FROM lp_array_element GROUP BY id";
                assertQuery(sql).noLeakCheck().fails(0, "not enough columns in group by");
            }
            {
                final String sql = "SELECT array_elem_sum(a,b) s FROM lp_array_element ORDER BY s";
                assertQuery(sql).noLeakCheck().fails(60, "DOUBLE[] is not a supported type in ORDER BY clause");
            }
            {
                final String sql = "SELECT array_elem_sum(a,b) s FROM lp_array_element ORDER BY array_sum(s)";
                assertQuery(sql).noLeakCheck().fails(70, "Invalid column: s");
            }
            {
                final String sql = "SELECT array_elem_sum(array_agg(id::DOUBLE),ARRAY[1.0]) FROM lp_array_element";
                assertQuery(sql).noLeakCheck().fails(22, "Aggregate function cannot be passed as an argument");
            }
        });
    }

    @Test
    public void testRetainedScalarKeyAndAggregateReopenAfterCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 1);
            final String sql = "SELECT array_elem_sum(a,shift(b,$1)) s,array_elem_sum(a) total,count() n "
                    + "FROM lp_array_element ORDER BY n";
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    compiler.clear();
                    try (RecordCursorFactory recovery = compiler.compile("SELECT count() FROM lp_array_element", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(recovery, "count\n3\n");
                    }
                }
                final ObjList<String> expected = new ObjList<>(
                        "s\ttotal\tn\n[13.0,6.0]\t[5.0,6.0]\t1\n[5.0,2.0]\t[2.0,4.0]\t2\n",
                        "s\ttotal\tn\n[12.0,14.0]\t[5.0,6.0]\t1\n[4.0,6.0]\t[2.0,4.0]\t2\n",
                        "s\ttotal\tn\n[5.0,13.0]\t[5.0,6.0]\t1\n[1.0,5.0]\t[2.0,4.0]\t2\n"
                );
                for (int pass = 0; pass < 3; pass++) {
                    bindVariableService.setInt(0, pass - 1);
                    assertResult(retained, expected.getQuick(pass));
                }
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void assertResult(RecordCursorFactory factory, String rows) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(rows);
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE lp_array_element(ts TIMESTAMP,id INT,a DOUBLE[],b DOUBLE[],"
                + "sum_key DOUBLE[],avg_key DOUBLE[],min_key DOUBLE[],max_key DOUBLE[]) TIMESTAMP(ts)");
        execute("INSERT INTO lp_array_element VALUES('2020-01-01T00:00:00Z',1,ARRAY[1.0,2.0],ARRAY[3.0,4.0],"
                + "ARRAY[4.0,6.0],ARRAY[2.0,3.0],ARRAY[1.0,2.0],ARRAY[3.0,4.0]),"
                + "('2020-01-01T00:30:00Z',2,ARRAY[1.0,2.0],ARRAY[3.0,4.0],"
                + "ARRAY[4.0,6.0],ARRAY[2.0,3.0],ARRAY[1.0,2.0],ARRAY[3.0,4.0]),"
                + "('2020-01-01T02:00:00Z',3,ARRAY[5.0,6.0],ARRAY[7.0,8.0],"
                + "ARRAY[12.0,14.0],ARRAY[6.0,7.0],ARRAY[5.0,6.0],ARRAY[7.0,8.0])");
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
