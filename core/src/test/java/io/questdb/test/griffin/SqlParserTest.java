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

import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8s;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SqlParserTest extends AbstractSqlParserTest {

    private final static List<String> frameTypes = Arrays.asList("rows  ", "groups", "range ");

    @Test
    public void test2Between() throws Exception {
        assertQuery(
                "select-choose t from (select [t, tt] from x where t between ('2020-01-01','2021-01-02') and tt between ('2021-01-02','2021-01-31'))",
                "select t from x where t between '2020-01-01' and '2021-01-02' and tt between '2021-01-02' and '2021-01-31'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testACBetweenOrClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between 12 preceding or 23 following) from xyz",
                78,
                "'and' expected"
        );
    }

    @Test
    public void testACCurrentCurrentClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between current row and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between current row and current row) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACCurrentExprFollowingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between current row and 4 + 3 #UNIT following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between current row and 4+3 following) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACCurrentExprPrecedingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between current row and 4+3 preceding) from xyz",
                85,
                "start row is CURRENT, end row not must be PRECEDING"
        );
    }

    @Test
    public void testACCurrentRowsUnboundedFollowingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between current rows and unbounded following) from xyz",
                73,
                "'row' expected"
        );
    }

    @Test
    public void testACCurrentUnboundedFollowingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between current row and unbounded following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between current row and unbounded following) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACCurrentUnboundedPrecedingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between current row and unbounded preceding) from xyz",
                91,
                "'following' expected"
        );
    }

    @Test
    public void testACExprFollowingExprFollowingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 12 #UNIT following and 23 #UNIT following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 12 following and 23 following) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprFollowingExprFollowingClauseInvalid() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between 22 following and 3 following) from xyz",
                65,
                "start of window must be lower than end of window",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprInvalidExprFollowingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between 12 something and 23 following) from xyz",
                68,
                "'preceding' or 'following' expected"
        );
    }

    @Test
    public void testACExprPrecedingCurrentClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 1 #UNIT preceding and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 1 preceding and current row) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingCurrentInvalidClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between 12 preceding and current rows) from xyz",
                90,
                "'row' expected"
        );
    }

    @Test
    public void testACExprPrecedingExprFollowingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 10 #UNIT preceding and 10 #UNIT following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 10 preceding and 10 following) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingExprFollowingClausePos() throws Exception {
        createModelsAndRun(
                () -> {
                    sink.clear();
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        for (String frameType : frameTypes) {
                            ExecutionModel model = compiler.testCompileModel(
                                    "select a,b, f(c) over (partition by b order by ts #FRAME between 10 preceding and 10 following) from xyz".replace("#FRAME", frameType),
                                    sqlExecutionContext
                            );
                            ObjList<QueryColumn> columns = model.getQueryModel().getBottomUpColumns();
                            Assert.assertEquals(3, columns.size());

                            QueryColumn ac = columns.getQuick(2);
                            Assert.assertTrue(ac.isWindowColumn());
                            WindowColumn ac2 = (WindowColumn) ac;

                            // start of window expr position
                            Assert.assertEquals(65, ac2.getRowsLoExprPos());
                            Assert.assertEquals(68, ac2.getRowsLoKindPos());

                            // end of window expr position
                            Assert.assertEquals(82, ac2.getRowsHiExprPos());
                            Assert.assertEquals(85, ac2.getRowsHiKindPos());
                        }
                    }
                },
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );

    }

    @Test
    public void testACExprPrecedingExprInvalidClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between 12 preceding and 23 something1) from xyz",
                85,
                "'preceding' or 'following' expected"
        );
    }

    @Test
    public void testACExprPrecedingExprPrecedingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 28 #UNIT preceding and 12 #UNIT preceding exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 28 preceding and 12 preceding) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingExprPrecedingClauseInvalid() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between 10 preceding and 20 preceding) from xyz",
                65,
                "start of window must be lower than end of window",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingExprPrecedingClauseInvalidUnion() throws Exception {
        assertWindowSyntaxError(
                "select a, b, c from xyz" +
                        " union all " +
                        "select a,b, f(c) over (partition by b order by ts #FRAME between 10 preceding and 20 preceding) from xyz",
                99,
                "start of window must be lower than end of window",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingExprPrecedingClauseValid() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 20 #UNIT preceding and 10 #UNIT preceding exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 20 preceding and 10 preceding) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 1 / 1 #UNIT preceding and unbounded following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 1/1 preceding and unbounded following) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeCurrentRowClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 0 + 1 #UNIT preceding and unbounded following exclude current row) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 0+1 preceding and unbounded following exclude current row) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeCurrentRowInvalid() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between -1 preceding and unbounded following exclude current table) from xyz",
                118,
                "'row' expected"
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeGroupClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 1 + 0 #UNIT preceding and unbounded following exclude group) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 1+0 preceding and unbounded following exclude group) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeInvalid() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between -1 preceding and unbounded following exclude other) from xyz",
                110,
                "'current', 'group', 'ties' or 'no other' expected"
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeMissing() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between -1 preceding and unbounded following exclude) from xyz",
                109,
                "'current', 'group', 'ties' or 'no other' expected"
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeNoOthersClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 1 + 1 #UNIT preceding and unbounded following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 1+1 preceding and unbounded following exclude no others) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeNoOthersInvalid() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between -1 preceding and unbounded following exclude no prisoners) from xyz",
                113,
                "'others' expected"
        );
    }

    @Test
    public void testACExprPrecedingUnboundedFollowingExcludeTiesClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between 1 / 1 #UNIT preceding and unbounded following exclude ties) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between 1/1 preceding and unbounded following exclude ties) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACExprPrecedingUnboundedPrecedingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between -1 preceding and unbounded preceding) from xyz",
                92,
                "'following' expected"
        );
    }

    @Test
    public void testACGroupFrameRejectsTimeUnits() throws Exception {
        assertSyntaxError(
                "select a,b, f(c) over (partition by b order by a groups 10 day preceding) from xyz",
                59,
                "'preceding' expected"
        );

        assertSyntaxError(
                "select a,b, f(c) over (partition by b order by a groups between unbounded preceding and 10 day following) from xyz",
                91,
                "'preceding' or 'following' expected"
        );
    }

    @Test
    public void testACGroupsWithMissingOrderBy() throws Exception {
        assertSyntaxError(
                "select a,b, f(c) over (partition by b groups) from xyz",
                38,
                "GROUPS mode requires an ORDER BY clause"
        );
    }

    @Test
    public void testACRangeFrameAcceptsTimeUnits() throws Exception {
        String[] unitsAndValues = new String[]{
                "microsecond",
                "microseconds",
                "millisecond",
                "milliseconds",
                "second",
                "seconds",
                "minute",
                "minutes",
                "hour",
                "hours",
                "day",
                "days"
        };
        for (int i = 0; i < unitsAndValues.length; i++) {
            String expectedUnit = unitsAndValues[i].replaceAll("s$", "");
            assertQuery(
                    ("select-window a, b, f(c) f over (partition by b order by ts range between 10 #unit preceding and current row exclude no others) " +
                            "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))")
                            .replace("#unit", expectedUnit),
                    "select a,b, f(c) over (partition by b order by ts range 10 #unit preceding) from xyz"
                            .replace("#unit", unitsAndValues[i]),
                    modelOf("xyz")
                            .col("a", ColumnType.INT)
                            .col("b", ColumnType.INT)
                            .col("c", ColumnType.INT)
                            .timestamp("ts")
            );

            assertQuery(
                    ("select-window a, b, f(c) f over (partition by b order by ts range between 10 #unit preceding and 1 #unit following exclude no others) " +
                            "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))")
                            .replace("#unit", expectedUnit),
                    "select a,b, f(c) over (partition by b order by ts range between 10 #unit preceding and 1 #unit following) from xyz"
                            .replace("#unit", unitsAndValues[i]),
                    modelOf("xyz")
                            .col("a", ColumnType.INT)
                            .col("b", ColumnType.INT)
                            .col("c", ColumnType.INT)
                            .timestamp("ts")
            );
        }
    }

    @Test
    public void testACRangeRequiredOrderByOnNonDefaultFrameBoundaries() throws Exception {
        assertSyntaxError(
                "select a,b, f(c) over (partition by b range 1 preceding ) from xyz",
                46,
                "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
        );

        assertSyntaxError(
                "select a,b, f(c) over (partition by b range between 1 preceding and 1 following ) from xyz",
                54,
                "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
        );

        assertSyntaxError(
                "select a,b, f(c) over (partition by b range between unbounded preceding and 1 following ) from xyz",
                78,
                "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
        );
    }

    @Test
    public void testACRejectsWrongExclusionMode() throws Exception {
        assertWindowSyntaxError(
                "select a,b, avg(c) over (partition by b order by ts #FRAME UNBOUNDED PRECEDING EXCLUDE WHAT) from xyz",
                87,
                "'current', 'group', 'ties' or 'no other' expected",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACRequiredNonNegativePrecedingAndFollowingValues() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME -1 preceding) from xyz",
                57,
                "non-negative integer expression expected",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );

        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between -1 preceding and 1 following) from xyz",
                65,
                "non-negative integer expression expected",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );

        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between 1 preceding and -1 following) from xyz",
                81,
                "non-negative integer expression expected",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACRowsFrameRejectsTimeUnits() throws Exception {
        assertSyntaxError(
                "select a,b, f(c) over (partition by b rows 10 day preceding) from xyz",
                46,
                "'preceding' expected"
        );

        assertSyntaxError(
                "select a,b, f(c) over (partition by b rows between unbounded preceding and 10 day following) from xyz",
                78,
                "'preceding' or 'following' expected"
        );
    }

    @Test
    public void testACShorthandCurrentRow() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between current row and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME current row) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACShorthandExprFollowing() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME 12 following) from xyz",
                60,
                "'preceding' expected",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACShorthandExprPreceding() throws Exception {
        assertWindowQuery("select-window a, b, f(c) f over (partition by b order by ts #FRAME between 12 #UNIT preceding and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME 12 preceding) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACShorthandUnboundedFollowing() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME unbounded following) from xyz",
                67,
                "'preceding' expected"
        );
    }

    @Test
    public void testACShorthandUnboundedPreceding() throws Exception {
        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts groups between unbounded preceding and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts GROUPS unbounded preceding) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );

        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts rows between unbounded preceding and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts ROWS unbounded preceding) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );

        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts RANGE unbounded preceding) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACUnboundedFollowingUnboundedPrecedingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between unbounded following and unbounded preceding) from xyz",
                75,
                "'preceding' expected"
        );
    }

    @Test
    public void testACUnboundedPrecedingCurrentClause() throws Exception {
        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts groups between unbounded preceding and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts GROUPS between unbounded preceding and current row) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );

        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts rows between unbounded preceding and current row exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts ROWS between unbounded preceding and current row) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );

        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts RANGE between unbounded preceding and current row) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACUnboundedPrecedingExprFollowingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between unbounded preceding and 10 #UNIT following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between unbounded preceding and 10 following) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACUnboundedPrecedingExprPrecedingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between unbounded preceding and unbounded preceding) from xyz",
                99,
                "'following' expected"
        );
    }

    @Test
    public void testACUnboundedPrecedingUnboundedFollowingClause() throws Exception {
        assertWindowQuery(
                "select-window a, b, f(c) f over (partition by b order by ts #FRAME between unbounded preceding and unbounded following exclude no others) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts #FRAME between unbounded preceding and unbounded following) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testACUnboundedPrecedingUnboundedPrecedingClause() throws Exception {
        assertWindowSyntaxError(
                "select a,b, f(c) over (partition by b order by ts #FRAME between unbounded preceding and unbounded preceding) from xyz",
                99,
                "'following' expected"
        );
    }

    @Test
    public void testACWrongFrameTypeUsed() throws Exception {
        assertSyntaxError(
                "select a,b, f(c) over (partition by b order by ts rangez ) from xyz",
                50,
                "'rows', 'groups', 'range' or ')' expected"
        );
    }

    @Test
    public void testAggregateFunctionExpr() throws SqlException {
        assertQuery(
                "select-group-by sum(max(x) + 2) sum, f(x) f from (select [x] from long_sequence(10))",
                "select sum(max(x) + 2), f(x) from long_sequence(10)"
        );
    }

    @Test
    public void testAggregateOperationExpr() throws SqlException {
        assertQuery(
                "select-group-by sum(max(x) + 2) sum, x + 1 column from (select [x] from long_sequence(10))",
                "select sum(max(x) + 2), x + 1 from long_sequence(10)"
        );
    }

    @Test
    public void testAliasInExplicitGroupByFormula() throws SqlException {
        // table alias in group-by formula should be replaced to align with "choose" model
        assertQuery(
                "select-virtual count from (select-group-by [count(event1) count, column] column, count(event1) count from (select-virtual [event1, event1 + event column] event1 + event column, event1 from (select-choose [a.event event1, b.event event] b.event event, a.event event1 from (select [event, created] from telemetry a timestamp (created) join (select [event, created] from telemetry b timestamp (created) where event < 1) b on b.created = a.created where event > 0) a) a) a) a",
                "select\n" +
                        "    count(a.event)\n" +
                        "    from\n" +
                        "    telemetry as a\n" +
                        "    inner join telemetry as b on a.created = b.created\n" +
                        "            where\n" +
                        "    a.event > 0\n" +
                        "    and b.event < 1\n" +
                        "    group by\n" +
                        "    a.event + b.event",
                modelOf("telemetry").timestamp("created").col("event", ColumnType.SHORT)
        );
    }

    @Test
    public void testAliasInImplicitGroupByFormula() throws SqlException {
        // table alias in group-by formula should be replaced to align with "choose" model
        assertQuery(
                "select-group-by column, count(event1) count from (select-virtual [event1 + event column, event1] event1 + event column, event1 from (select-choose [b.event event, a.event event1] b.event event, a.event event1 from (select [event, created] from telemetry a timestamp (created) join (select [event, created] from telemetry b timestamp (created) where event < 1) b on b.created = a.created where event > 0) a) a) a",
                "select \n" +
                        "  a.event + b.event,\n" +
                        "  count(a.event)\n" +
                        "from\n" +
                        "  telemetry as a\n" +
                        "  inner join telemetry as b on a.created = b.created\n" +
                        "where\n" +
                        "  a.event > 0\n" +
                        "  and b.event < 1\n",
                modelOf("telemetry").timestamp("created").col("event", ColumnType.SHORT)
        );
    }

    @Test
    public void testAliasSecondJoinTable() throws SqlException {
        assertQuery(
                "select-choose tx.a a, tx.b b from (select [a, b, xid] from x tx left join select [yid, a, b] from y ty on yid = xid post-join-where ty.a = 1 or ty.b = 2) tx",
                "select tx.a, tx.b from x as tx left join y as ty on xid = yid where ty.a = 1 or ty.b=2",
                modelOf("x").col("xid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT),
                modelOf("y").col("yid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAliasTopJoinTable() throws SqlException {
        assertQuery(
                "select-choose tx.a a, tx.b b from (select [a, b, xid] from x tx left join select [yid] from y ty on yid = xid where a = 1 or b = 2) tx",
                "select tx.a, tx.b from x as tx left join y as ty on xid = yid where tx.a = 1 or tx.b=2",
                modelOf("x").col("xid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT),
                modelOf("y").col("yid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAliasWithKeyword() throws Exception {
        assertQuery(
                "select-choose x from (select [x] from x as where x > 1) as",
                "x \"as\" where x > 1",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testAliasWithSpaceDoubleQuote() throws Exception {
        assertQuery(
                "select-choose x from (select [x] from x 'b a' where x > 1) 'b a'",
                "x \"b a\" where x > 1",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testAliasWithSpaceX() throws Exception {
        assertSyntaxError("x 'a b' where x > 1", 0, "impossible type cast, invalid type");
    }

    @Test
    public void testAliasWithWildcard1() throws SqlException {
        assertQuery(
                "select-choose a1, a1 a, b from (select-choose [a a1, b] a a1, b from (select [a, b] from x))",
                "select a as a1, * from x",
                modelOf("x").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAliasWithWildcard2() throws SqlException {
        assertQuery(
                "select-virtual cast(a,long) a1, a, b from (select [a, b] from x)",
                "select a::long as a1, * from x",
                modelOf("x").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAliasWithWildcard3() throws SqlException {
        assertQuery(
                "select-virtual cast(a,long) a1, a, b from (select [a, b] from x)",
                "select cast(a as long) a1, * from x",
                modelOf("x").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAliasWithWildcard4() throws SqlException {
        assertQuery(
                "select-virtual a, b, cast(a,long) a1 from (select [a, b] from x)",
                "select *, cast(a as long) a1 from x",
                modelOf("x").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAmbiguousColumn() throws Exception {
        assertSyntaxError("orders join customers on customerId = customerId", 25, "Ambiguous",
                modelOf("orders").col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testAsOfJoin() throws SqlException {
        assertQuery(
                "select-choose t.timestamp timestamp, t.tag tag, q.timestamp timestamp1 from (select [timestamp, tag] from trades t timestamp (timestamp) asof join select [timestamp] from quotes q timestamp (timestamp) where tag = null) t",
                "trades t ASOF JOIN quotes q WHERE tag = null",
                modelOf("trades").timestamp().col("tag", ColumnType.SYMBOL),
                modelOf("quotes").timestamp()
        );
    }

    @Test
    public void testAsOfJoinColumnAliasNull() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c asof join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = null) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " asof join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testAsOfJoinOrder() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, e.employeeId employeeId, o.customerId customerId1 from (select [customerId] from customers c asof join select [employeeId] from employees e on e.employeeId = c.customerId join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " asof join employees e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees").col("employeeId", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testAsOfJoinOuterWhereClause() throws Exception {
        assertQuery(
                "select-choose trade_time, quote_time, trade_price, trade_size, quote_price, quote_size from (select-choose [trade.ts trade_time, book.ts quote_time, trade.price trade_price, trade.size trade_size, book.price quote_price, book.size quote_size] trade.ts trade_time, book.ts quote_time, trade.price trade_price, trade.size trade_size, book.price quote_price, book.size quote_size from (select [ts, price, size, sym] from trade asof join select [ts, price, size, sym] from book post-join-where trade.sym = book.sym and book.price != NaN))",
                "select * from \n" +
                        "(\n" +
                        "select \n" +
                        "trade.ts as trade_time,\n" +
                        "book.ts as quote_time,\n" +
                        "trade.price as trade_price,\n" +
                        "trade.size as trade_size,\n" +
                        "book.price as quote_price,\n" +
                        "book.size as quote_size\n" +
                        "from trade\n" +
                        "asof join book\n" +
                        "where trade.sym = book.sym\n" +
                        ")\n" +
                        "where quote_price != NaN;",
                modelOf("trade")
                        .col("ts", ColumnType.TIMESTAMP)
                        .col("sym", ColumnType.SYMBOL)
                        .col("price", ColumnType.DOUBLE)
                        .col("size", ColumnType.LONG),
                modelOf("book")
                        .col("ts", ColumnType.TIMESTAMP)
                        .col("sym", ColumnType.SYMBOL)
                        .col("price", ColumnType.DOUBLE)
                        .col("size", ColumnType.LONG)
        );
    }

    @Test
    public void testAsOfJoinSubQuery() throws Exception {
        // execution order must be (src: SQL Server)
        //        1. FROM
        //        2. ON
        //        3. JOIN
        //        4. WHERE
        //        5. GROUP BY
        //        6. WITH CUBE or WITH ROLLUP
        //        7. HAVING
        //        8. SELECT
        //        9. DISTINCT
        //        10. ORDER BY
        //        11. TOP
        //
        // which means "where" clause for "e" table has to be explicitly as post-join-where
        assertQuery(
                "select-choose c.customerId customerId, e.blah blah, e.lastName lastName, e.employeeId employeeId, e.timestamp timestamp, o.customerId customerId1 from (select [customerId] from customers c asof join select [blah, lastName, employeeId, timestamp] from (select-virtual ['1' blah, lastName, employeeId, timestamp] '1' blah, lastName, employeeId, timestamp from (select [lastName, employeeId, timestamp] from employees) order by lastName) e on e.employeeId = c.customerId post-join-where e.lastName = 'x' and e.blah = 'y' join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId, timestamp from employees order by lastName) e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId where e.lastName = 'x' and e.blah = 'y'",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP),
                modelOf("orders")
                        .col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testAsOfJoinSubQueryInnerPredicates() throws Exception {
        // which means "where" clause for "e" table has to be explicitly as post-join-where
        assertQuery(
                "select-choose c.customerId customerId, e.blah blah, e.lastName lastName, e.employeeId employeeId, e.timestamp timestamp, o.customerId customerId1 from " +
                        "(select [customerId] from customers c " +
                        "asof join select [blah, lastName, employeeId, timestamp] from " +
                        "(select-virtual ['1' blah, lastName, employeeId, timestamp] '1' blah, lastName, employeeId, timestamp from " +
                        "(select [lastName, employeeId, timestamp] from employees) " +
                        "order by lastName) e on e.employeeId = c.customerId post-join-where e.lastName = 'x' and e.blah = 'y' join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId, timestamp from employees order by lastName) e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId and e.lastName = 'x' and e.blah = 'y'",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP),
                modelOf("orders")
                        .col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testAsOfJoinSubQuerySimpleAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, a.blah blah, a.lastName lastName, a.customerId customerId1, a.timestamp timestamp from (select [customerId] from customers c asof join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) a on a.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) a on (customerId)",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testAsOfJoinSubQuerySimpleNoAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, _xQdbA0.blah blah, _xQdbA0.lastName lastName, _xQdbA0.customerId customerId1, _xQdbA0.timestamp timestamp from (select [customerId] from customers c asof join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) _xQdbA0 on _xQdbA0.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) on (customerId)",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testAtAsColumnAlias() throws Exception {
        assertQuery(
                "select-choose l at from (select [l] from testat timestamp (ts))",
                "select l at from testat",
                modelOf("testat").col("l", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsColumnName() throws Exception {
        assertQuery(
                "select-choose at from (select [at] from testat timestamp (ts))",
                "select at from testat",
                modelOf("testat").col("at", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsTableAlias() throws Exception {
        assertQuery(
                "select-choose l, ts from (select [l, ts] from testat at timestamp (ts)) at",
                "select at.l, at.ts from testat at",
                modelOf("testat").col("l", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsTableAliasInJoin() throws Exception {
        assertQuery(
                "select-choose at.l l, at.ts ts, at2.l l1, at2.ts ts1 from (select [l, ts] from testat at timestamp (ts) join select [l, ts] from testat at2 timestamp (ts) on at2.l = at.l) at",
                "select * from testat at join testat at2 on at.l = at2.l",
                modelOf("testat").col("l", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsTableAliasSingleColumn() throws Exception {
        assertQuery(
                "select-choose l from (select [l] from testat at timestamp (ts)) at",
                "select at.l from testat as at",
                modelOf("testat").col("l", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsTableAliasStar() throws Exception {
        assertQuery(
                "select-choose l, ts from (select [l, ts] from testat at timestamp (ts)) at",
                "select at.* from testat at",
                modelOf("testat").col("l", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsTableName() throws Exception {
        assertQuery(
                "select-choose at from (select [at] from at timestamp (ts))",
                "select at.at from at",
                modelOf("at").col("at", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsTableNameAndExpr() throws Exception {
        assertQuery(
                "select-virtual at1 + at column from (select-choose [at, at at1] at, at at1 from (select [at] from at timestamp (ts)))",
                "select (at.at + at) from at",
                modelOf("at").col("at", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testAtAsWithAlias() throws Exception {
        assertQuery(
                "select-choose l, ts from (select-choose [l, ts] l, ts from (select [l, ts] from testat timestamp (ts))) at",
                "with at as (select * from testat )  selecT * from at",
                modelOf("testat").col("l", ColumnType.INT).timestamp("ts")
        );
    }

    @Test
    public void testBadAlias() throws Exception {
        assertIdentifierError("select 'a' ! ");
        assertIdentifierError("select 'a' } ");
        assertIdentifierError("select 'a' { ");
        assertIdentifierError("select 'a' ] ");
        assertIdentifierError("select 'a' : ");
        assertIdentifierError("select 'a' ? ");
        assertIdentifierError("select 'a' @ ");
        assertSyntaxError("select 'a' ) ", 11, "unexpected token [)]");
        assertIdentifierError("select 'a' $ ");
        assertIdentifierError("select 'a' 0 ");
        assertIdentifierError("select 'a' 12 ");
        assertIdentifierError("select 'a' \"\" ");
        assertIdentifierError("select 'a' ''' ");
        assertIdentifierError("select 'a' '' ");
        assertIdentifierError("select 'a' ' ");
        assertIdentifierError("select 'a' ``` ");
        assertIdentifierError("select 'a' `` ");
        assertIdentifierError("select 'a' ` ");
    }

    @Test
    public void testBadTableExpression() throws Exception {
        assertSyntaxError(")", 0, "table name expected");
    }

    @Test
    public void testBetween() throws Exception {
        assertQuery(
                "select-choose t from (select [t] from x where t between ('2020-01-01','2021-01-02'))",
                "x where t between '2020-01-01' and '2021-01-02'",
                modelOf("x").col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBetweenInsideCast() throws Exception {
        assertQuery(
                "select-virtual cast(t between (cast('2020-01-01',TIMESTAMP),'2021-01-02'),INT) + 1 column from (select [t] from x)",
                "select CAST(t between CAST('2020-01-01' AS TIMESTAMP) and '2021-01-02' AS INT) + 1 from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBetweenUnfinished() throws Exception {
        assertSyntaxError(
                "select tt from x where t between '2020-01-01'",
                25,
                "too few arguments for 'between' [found=2,expected=3]",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBetweenWithCase() throws Exception {
        assertQuery(
                "select-virtual case(t between (cast('2020-01-01',TIMESTAMP),'2021-01-02'),'a','b') case from (select [t] from x)",
                "select case when t between CAST('2020-01-01' AS TIMESTAMP) and '2021-01-02' then 'a' else 'b' end from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBetweenWithCast() throws Exception {
        assertQuery(
                "select-choose t from (select [t] from x where t between (cast('2020-01-01',TIMESTAMP),'2021-01-02'))",
                "select t from x where t between CAST('2020-01-01' AS TIMESTAMP) and '2021-01-02'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBetweenWithCastAndSum() throws Exception {
        assertQuery(
                "select-choose tt from (select [tt, t] from x where t between ('2020-01-01',now() + cast(NULL,LONG)))",
                "select tt from x where t between '2020-01-01' and (now() + CAST(NULL AS LONG))",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBetweenWithCastAndSum2() throws Exception {
        assertQuery(
                "select-choose tt from (select [tt, t] from x where t between (now() + cast(NULL,LONG),'2020-01-01'))",
                "select tt from x where t between (now() + CAST(NULL AS LONG)) and '2020-01-01'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBlockCommentAtMiddle() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where /*this is a random comment */a > 1) 'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT)
        );
    }

    @Test
    public void testBlockCommentNested() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) /* comment /* ok */  whatever */'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT)
        );
    }

    @Test
    public void testBlockCommentUnclosed() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) 'b a' where x > 1 /* this block comment",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT)
        );
    }

    @Test
    public void testBracedArithmeticsInsideCast() throws SqlException {
        assertQuery(
                "select-virtual cast(500000000000L + case(x > 400,x - 1,x),timestamp) ts from (select [x] from long_sequence(1000))",
                "select cast((500000000000L + case when x > 400 then x - 1 else x end) as timestamp) ts from long_sequence(1000)",
                modelOf("ts").col("ts", ColumnType.TIMESTAMP)
        );
        assertQuery(
                "select-virtual cast(500000000000L + case(x > 400,x - 1 / 4 * 1000L + 4 - x - 1 % 4 * 89,x),timestamp) ts from (select [x] from long_sequence(1000))",
                "select cast(" +
                        "(500000000000L + " +
                        "  case when x > 400 " +
                        "    then ((x - 1) / 4) * 1000L + (4 - (x - 1) % 4) * 89 " +
                        "  else x end) " +
                        "as timestamp) ts from long_sequence(1000)",
                modelOf("ts").col("ts", ColumnType.TIMESTAMP)
        );
        assertQuery(
                "select-virtual x - 1 - x t1 from (select [x] from long_sequence(1000))",
                "select (x - (1 - x)) as t1 from long_sequence(1000)",
                modelOf("t1").col("t1", ColumnType.LONG)
        );
    }

    @Test
    public void testCaseImpossibleRewrite1() throws SqlException {
        // referenced columns in 'when' clauses are different
        assertQuery(
                "select-virtual case(a = 1,'A',2 = b,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 = b then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseImpossibleRewrite2() throws SqlException {
        // 'when' is non-constant
        assertQuery(
                "select-virtual case(a = 1,'A',2 + b = a,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 + b = a then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseNoElseClause() throws SqlException {
        // referenced columns in 'when' clauses are different
        assertQuery(
                "select-virtual case(a = 1,'A',2 = b,'B') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 = b then 'B' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseToSwitchExpression() throws SqlException {
        assertQuery(
                "select-virtual switch(a,1,'A',2,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when a = 2 then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseToSwitchExpression2() throws SqlException {
        // this test has inverted '=' arguments but should still be rewritten to 'switch'
        assertQuery(
                "select-virtual switch(a,1,'A',2,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 = a then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testColumnAliasDoubleQuoted() throws Exception {
        assertQuery(
                "select-choose x aaaasssss from (select [x] from x where x > 1)",
                "select x \"aaaasssss\" from x where x > 1",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testColumnTopToBottom() throws SqlException {
        assertQuery(
                "select-choose x.i i, x.sym sym, x.amt amt, price, x.timestamp timestamp, y.timestamp timestamp1 from (select [i, sym, amt, timestamp] from x timestamp (timestamp) splice join select [price, timestamp, sym2, trader] from y timestamp (timestamp) on y.sym2 = x.sym post-join-where trader = 'ABC')",
                "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym where trader = 'ABC'",
                modelOf("x")
                        .col("i", ColumnType.INT)
                        .col("sym", ColumnType.SYMBOL)
                        .col("amt", ColumnType.DOUBLE)
                        .col("comment", ColumnType.STRING)
                        .col("venue", ColumnType.SYMBOL)
                        .timestamp(),
                modelOf("y")
                        .col("price", ColumnType.DOUBLE)
                        .col("sym2", ColumnType.SYMBOL)
                        .col("trader", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testColumnsOfSimpleSelectWithSemicolon() throws SqlException {
        assertColumnNames("select 1;", "1");
        assertColumnNames("select 1, 1, 1;", "1", "column", "column1");
    }

    @Test
    public void testConcat3Args() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x,'3') concat from (select [x] from tab)",
                "select 1, x, concat('2', x, '3') from tab",
                modelOf("tab").col("x", ColumnType.STRING)
        );
    }

    @Test
    public void testConcatSimple() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x) concat from (select [x] from tab)",
                "select 1, x, '2' || x from tab",
                modelOf("tab").col("x", ColumnType.STRING)
        );
    }

    @Test
    public void testConsistentColumnOrder() throws SqlException {
        assertQuery(
                "select-choose rnd_int, rnd_int1, rnd_boolean, rnd_str, rnd_double, rnd_float, rnd_short, rnd_short1, rnd_date, rnd_timestamp, rnd_symbol, rnd_long, rnd_long1, ts, rnd_byte, rnd_bin from (select-virtual [rnd_int() rnd_int, rnd_int(0,30,2) rnd_int1, rnd_boolean() rnd_boolean, rnd_str(3,3,2) rnd_str, rnd_double(2) rnd_double, rnd_float(2) rnd_float, rnd_short(10,1024) rnd_short, rnd_short() rnd_short1, rnd_date(to_date('2015','yyyy'),to_date('2016','yyyy'),2) rnd_date, rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) rnd_timestamp, rnd_symbol(4,4,4,2) rnd_symbol, rnd_long(100,200,2) rnd_long, rnd_long() rnd_long1, timestamp_sequence(0,1000000000) ts, rnd_byte(2,50) rnd_byte, rnd_bin(10,20,2) rnd_bin] rnd_int() rnd_int, rnd_int(0,30,2) rnd_int1, rnd_boolean() rnd_boolean, rnd_str(3,3,2) rnd_str, rnd_double(2) rnd_double, rnd_float(2) rnd_float, rnd_short(10,1024) rnd_short, rnd_short() rnd_short1, rnd_date(to_date('2015','yyyy'),to_date('2016','yyyy'),2) rnd_date, rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) rnd_timestamp, rnd_symbol(4,4,4,2) rnd_symbol, rnd_long(100,200,2) rnd_long, rnd_long() rnd_long1, timestamp_sequence(0,1000000000) ts, rnd_byte(2,50) rnd_byte, rnd_bin(10,20,2) rnd_bin from (long_sequence(20)))",
                "select * from (select" +
                        " rnd_int()," +
                        " rnd_int(0, 30, 2)," +
                        " rnd_boolean()," +
                        " rnd_str(3,3,2)," +
                        " rnd_double(2)," +
                        " rnd_float(2)," +
                        " rnd_short(10,1024)," +
                        " rnd_short()," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " rnd_symbol(4,4,4,2)," +
                        " rnd_long(100,200,2)," +
                        " rnd_long()," +
                        " timestamp_sequence(0, 1000000000) ts," +
                        " rnd_byte(2,50)," +
                        " rnd_bin(10, 20, 2)" +
                        " from long_sequence(20))"
        );
    }

    @Test
    public void testConstantFunctionAsArg() throws Exception {
        assertQuery(
                "select-choose customerId from (select [customerId] from customers where f(1.2) > 1)",
                "select * from customers where f(1.2) > 1",
                modelOf("customers").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testConstantIsNotNull() throws Exception {
        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x const-where null != null)",
                "tab1 join tab2 on tab1.x = tab2.y where null is not null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );

        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x const-where 'null' != null)",
                "tab1 join tab2 on tab1.x = tab2.y where 'null' is not null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testConstantIsNull() throws Exception {
        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x const-where null = null)",
                "tab1 join tab2 on tab1.x = tab2.y where null is null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );

        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x const-where 'null' = null)",
                "tab1 join tab2 on tab1.x = tab2.y where 'null' is null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );
    }

    @Test
    @Ignore
    // not yet implemented, taking the non-correlated sub-query out as a join
    // will duplicate column X that optimiser needs to deal with
    public void testCorrelatedSubQueryCross() throws Exception {
        assertQuery(
                "select-virtual (select-choose x from (select [x] from a)) y, x from (select [x] from a)",
                "select (select x from a) y, x from a",
                modelOf("a").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testCount() throws Exception {
        assertQuery(
                "select-group-by customerId, count() count from (select-choose [c.customerId customerId] c.customerId customerId from (select [customerId] from customers c left join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = NaN) c) c",
                "select c.customerId, count() from customers c" +
                        " left join orders o on c.customerId = o.customerId " +
                        " where o.customerId = NaN",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING)
        );
    }

    @Test
    public void testCountStarRewrite() throws SqlException {
        assertQuery(
                "select-group-by count() count, x from (select [x] from long_sequence(10))",
                "select count(*), x from long_sequence(10)"
        );
    }

    @Test
    public void testCountStarRewriteLeaveArgs() throws SqlException {
        assertQuery(
                "select-group-by x, count(2 * x) count from (select [x] from long_sequence(10))",
                "select x, count(2*x) from long_sequence(10)"
        );
    }

    @Test
    public void testCreateAsSelectDuplicateColumn0() throws Exception {
        assertSyntaxError(
                "create table tab as (select rnd_byte() b, rnd_boolean() b from long_sequence(1))",
                56,
                "Duplicate column [name=b]",
                modelOf("tab")
                        .col("b", ColumnType.BYTE)
                        .col("b", ColumnType.BOOLEAN)
        );
    }

    @Test
    public void testCreateAsSelectDuplicateColumn1() throws Exception {
        assertSyntaxError(
                "create table tab as (select rnd_byte() b, rnd_boolean() \"B\" from long_sequence(1))",
                56,
                "Duplicate column [name=B]",
                modelOf("tab")
                        .col("b", ColumnType.BYTE)
                        .col("b", ColumnType.BOOLEAN)
        );
    }

    @Test
    public void testCreateAsSelectDuplicateColumnNonAscii() throws Exception {
        assertSyntaxError(
                "create table tab as (select rnd_byte() \"गाजर का हलवा\", rnd_boolean() as \"गाजर का हलवा\" from long_sequence(1))",
                69,
                "Duplicate column [name=गाजर का हलवा]",
                modelOf("tab")
                        .col("b", ColumnType.BYTE)
                        .col("b", ColumnType.BOOLEAN)
        );
    }

    @Test
    public void testCreateAsSelectInvalidIndex() throws Exception {
        assertSyntaxError(
                "create table X as ( select a, b, c from tab ), index(x)",
                53,
                "Invalid column",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateAsSelectMissingTimestamp() throws Exception {
        assertSyntaxError(
                "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 100000000000l) t from long_sequence(100000))) partition by DAY",
                153,
                "partitioning is possible only on tables with designated timestamps"
        );
    }

    @Test
    public void testCreateAsSelectTimestampNotRequired() throws SqlException {
        assertCreateTable(
                "create table tst as (select-choose a, b, t from (select-virtual [rnd_int() a, rnd_double() b, timestamp_sequence(0,100000000000l) t] rnd_int() a, rnd_double() b, timestamp_sequence(0,100000000000l) t from (long_sequence(100000))))",
                "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 100000000000l) t from long_sequence(100000)))"
        );
    }

    @Test
    public void testCreateLikeTableInvalidSyntax() throws Exception {
        assertSyntaxError(
                "create table x (like y), index(s1)",
                23,
                "unexpected token [,]"
        );
    }

    @Test
    public void testCreateLikeTableNameAsDot() throws Exception {
        assertSyntaxError(
                "create table newtab ( like . )",
                27,
                "'.' is an invalid table name"
        );
    }

    @Test
    public void testCreateLikeTableNameFullOfHacks() throws Exception {
        assertSyntaxError(
                "create table x (like '../../../')",
                21,
                "'.' is not allowed"
        );
    }

    @Test
    public void testCreateLikeTableNameWithDot() throws Exception {
        assertSyntaxError(
                "create table x (like Y.z)",
                22,
                "unexpected token [.]"
        );
    }

    @Test
    public void testCreateNameDot() throws Exception {
        assertSyntaxError(
                "create table . as ( select a, b, c from tab )",
                13,
                "'.' is an invalid table name",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateNameFullOfHacks() throws Exception {
        assertSyntaxError(
                "create table '../../../' as ( select a, b, c from tab )",
                13,
                "'.' is not allowed",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateNameWithDot() throws Exception {
        assertSyntaxError(
                "create table X.y as ( select a, b, c from tab )",
                14,
                "unexpected token [.]",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTable() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 cache," +
                        " z STRING," +
                        " y BOOLEAN) timestamp(t) partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testCreateTableAsSelect() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab))",
                "create table X as ( select a, b, c from tab )",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableAsSelectIndex() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab)), index(b capacity 256)",
                "create table X as ( select a, b, c from tab ), index(b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableAsSelectIndexCapacity() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab)), index(b capacity 64)",
                "create table X as ( select a, b, c from tab ), index(b capacity 64)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableAsSelectTimestamp() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab)) timestamp(b)",
                "create table X as ( select a, b, c from tab ) timestamp(b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableBadColumnDef() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP blah, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR index",
                61,
                "',' or ')' expected"
        );
    }

    @Test
    public void testCreateTableCacheCapacity() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 64 cache," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL CAPACITY 64 CACHE, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR"
        );
    }

    @Test
    public void testCreateTableCastCapacityDef() throws SqlException {
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 16 cache)",
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 16)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastDef() throws SqlException {
        // these numbers in expected string are position of type keyword
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 128 cache)",
                "create table x as (tab), cast(a as double), cast(c as symbol)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastDefSymbolCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 1100000000)",
                70,
                "max symbol capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastDefSymbolCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity -10)",
                70,
                "min symbol capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache), index(c capacity 100000000)",
                100,
                "max index block capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache), index(c capacity 1)",
                100,
                "min index block capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexDef() throws SqlException {
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), index(c capacity 512), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 32 nocache)",
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache), index(c capacity 300)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexInvalidCapacity() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache), index(c capacity -)",
                101,
                "bad integer",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexNegativeCapacity() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache), index(c capacity -3)",
                100,
                "min index block capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastMultiSpaceMultiNewlineAndComment() throws SqlException {
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:38), cast(c as SYMBOL:83 capacity 16 cache)",
                "create table x as (tab), cast   (a as double  ), cast\n--- this is a comment\n\n(c as symbol capacity 16\n)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastRoundedSymbolCapacityDef() throws SqlException {
        // 20 is rounded to next power of 2, which is 32
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 32 cache)",
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastUnsupportedType() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(b as integer)",
                35,
                "unsupported column type",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableDuplicateCast() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(b as double), cast(b as long)",
                49,
                "duplicate cast",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableDuplicateColumn() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "T BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                124,
                "Duplicate column [name=T]"
        );
    }

    @Test
    public void testCreateTableDuplicateColumnNonAscii() throws Exception {
        assertSyntaxError(
                "create table x (侘寂 INT, 侘寂 BOOLEAN)",
                24,
                "Duplicate column [name=侘寂]"
        );
    }

    @Test
    public void testCreateTableForKafka() throws SqlException {
        assertCreateTable(
                "create table quickstart-events4 (" +
                        "flag BOOLEAN, " +
                        "id8 SHORT, " +
                        "id16 SHORT, " +
                        "id32 INT, " +
                        "id64 LONG, " +
                        "idFloat FLOAT, " +
                        "idDouble DOUBLE, " +
                        "idBytes STRING, " +
                        "msg STRING)",
                "CREATE TABLE \"quickstart-events4\" (\n" +
                        "\"flag\" BOOLEAN NOT NULL,\n" +
                        "\"id8\" SMALLINT NOT NULL,\n" +
                        "\"id16\" SMALLINT NOT NULL,\n" +
                        "\"id32\" INT NOT NULL,\n" +
                        "\"id64\" BIGINT NOT NULL,\n" +
                        "\"idFloat\" REAL NOT NULL,\n" +
                        "\"idDouble\" DOUBLE PRECISION NOT NULL,\n" +
                        "\"idBytes\" BYTEA NOT NULL,\n" +
                        "\"msg\" TEXT NULL)"
        );
    }

    @Test
    public void testCreateTableIf() throws Exception {
        assertSyntaxError("create table if", 15, "'not' expected");
    }

    @Test
    public void testCreateTableIfNot() throws Exception {
        assertSyntaxError("create table if not", 19, "'exists' expected");
    }

    @Test
    public void testCreateTableIfNotExistsTableNameIsQuotedKeyword() throws SqlException {
        assertModel("create table from (a INT)", "create table if not exists \"from\" (a int)", ExecutionModel.CREATE_TABLE);
    }

    @Test
    public void testCreateTableIfNotExistsTableNameIsUnquotedKeyword() throws Exception {
        assertException(
                "create table if not exists from (a int)",
                27,
                "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\""
        );
    }

    @Test
    public void testCreateTableIfNotTable() throws Exception {
        assertSyntaxError("create table if not x", 20, "'if not exists' expected");
    }

    @Test
    public void testCreateTableIfTable() throws Exception {
        assertSyntaxError("create table if x", 16, "'if not exists' expected");
    }

    @Test
    public void testCreateTableInPlaceIndex() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 cache index capacity 256," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " + // <-- index here
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexAndBlockSize() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 128," +
                        " z STRING," +
                        " y BOOLEAN) timestamp(t) partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 128, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 10000000, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                122,
                "max index block capacity is"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityInvalid() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity -, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                123,
                "bad integer"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 2, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                122,
                "min index block capacity is"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityLow2() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity -9, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                122,
                "min index block capacity is"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityRounding() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 128," +
                        " z STRING," +
                        " y BOOLEAN) timestamp(t) partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 120, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testCreateTableInVolume() throws SqlException {
        Assume.assumeFalse(Os.isWindows()); // soft links are not supported in Windows
        assertCreateTable(
                "create table tst0 (i INT) in volume 'volume'",
                "create table tst0 (i int) in volume 'volume'"
        );

        assertCreateTable(
                "create table tst1 (i INT) in volume 'volume'",
                "create table tst1 (i int) in volume volume"
        );

        assertCreateTable(
                "create table tst2 (i INT, ts TIMESTAMP) timestamp(ts) in volume 'volume'",
                "create table tst2 (i int, ts timestamp) timestamp(ts) in volume 'volume'"
        );

        assertCreateTable(
                "create table tst3 (i INT, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst3 (i int, ts timestamp) timestamp(ts) partition by day in volume 'volume'"
        );

        assertCreateTable(
                "create table tst4 (i INT, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst4 (i int, ts timestamp) timestamp(ts) partition by day with maxUncommittedRows=7, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst5 (i INT, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst5 (i int, ts timestamp) timestamp(ts) partition by day with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst6 (i SYMBOL capacity 128 cache index capacity 32, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst6 (i symbol, ts timestamp), index(i capacity 32) timestamp(ts) partition by day with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst7 (i SYMBOL capacity 128 cache index capacity 32, ts TIMESTAMP) in volume 'volume'",
                "create table tst7 (i symbol, ts timestamp), index(i capacity 32) in volume 'volume'"
        );

        assertCreateTable(
                "create table tst8 (i SYMBOL capacity 128 cache index capacity 32, ts TIMESTAMP) in volume 'volume'",
                "create table tst8 (i symbol, ts timestamp), index(i capacity 32) with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst8 (i SYMBOL capacity 128 cache index capacity 32, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst8 (i symbol, ts timestamp), index(i capacity 32) timestamp(ts) partition by day with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );
    }

    @Test
    public void testCreateTableInVolumeBypassWal() throws SqlException {
        Assume.assumeFalse(Os.isWindows()); // soft links are not supported in Windows

        assertCreateTable(
                "create table tst3 (i INT, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst3 (i int, ts timestamp) timestamp(ts) partition by day bypass wal in volume 'volume'"
        );

        assertCreateTable(
                "create table tst4 (i INT, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst4 (i int, ts timestamp) timestamp(ts) partition by day bypass wal with maxUncommittedRows=7, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst5 (i INT, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst5 (i int, ts timestamp) timestamp(ts) partition by day bypass wal with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst6 (i SYMBOL capacity 128 cache index capacity 32, ts TIMESTAMP) timestamp(ts) partition by day in volume 'volume'",
                "create table tst6 (i symbol, ts timestamp), index(i capacity 32) timestamp(ts) partition by day bypass wal with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );
    }

    @Test
    public void testCreateTableInVolumeFail() throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddl("create table tst0 (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c CHAR, " +
                        "t TIMESTAMP) " +
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR IN VOLUME 12", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                if (Os.isWindows()) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "'in volume' is not supported on Windows");
                } else {
                    TestUtils.assertContains(e.getFlyweightMessage(), "volume alias is not allowed [alias=12]");
                }
            }
        });
    }

    @Test
    public void testCreateTableInVolumeSyntaxError() throws Exception {
        assertSyntaxError(
                "create table tst0 (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c CHAR, " +
                        "t TIMESTAMP) " +
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR VOLUME peterson",
                86,
                "unexpected token [VOLUME]"
        );

        assertSyntaxError(
                "create table tst0 (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c CHAR, " +
                        "t TIMESTAMP) " +
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR IN peterson",
                97,
                "expected 'volume'"
        );

        assertSyntaxError(
                "create table tst0 (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c CHAR, " +
                        "t TIMESTAMP) " +
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR IN VOLUME",
                95,
                "path for volume expected"
        );
    }

    @Test
    public void testCreateTableInVolumeWal() throws SqlException {
        Assume.assumeFalse(Os.isWindows()); // soft links are not supported in Windows

        assertCreateTable(
                "create table tst3 (i INT, ts TIMESTAMP) timestamp(ts) partition by day wal in volume 'volume'",
                "create table tst3 (i int, ts timestamp) timestamp(ts) partition by day wal in volume 'volume'"
        );

        assertCreateTable(
                "create table tst4 (i INT, ts TIMESTAMP) timestamp(ts) partition by day wal in volume 'volume'",
                "create table tst4 (i int, ts timestamp) timestamp(ts) partition by day wal with maxUncommittedRows=7, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst5 (i INT, ts TIMESTAMP) timestamp(ts) partition by day wal in volume 'volume'",
                "create table tst5 (i int, ts timestamp) timestamp(ts) partition by day wal with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );

        assertCreateTable(
                "create table tst6 (i SYMBOL capacity 128 cache index capacity 32, ts TIMESTAMP) timestamp(ts) partition by day wal in volume 'volume'",
                "create table tst6 (i symbol, ts timestamp), index(i capacity 32) timestamp(ts) partition by day wal with maxUncommittedRows=7, o3MaxLag=12d, in volume 'volume'"
        );
    }

    @Test
    public void testCreateTableIndexUnsupportedColumnType() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE INDEX, " + // INDEX is not supported for non-SYMBOL columns
                        "c CHAR, " +
                        "t TIMESTAMP) " +
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR",
                30,
                "',' or ')' expected"
        );
    }

    @Test
    public void testCreateTableIndexUnsupportedColumnType2() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c CHAR, " +
                        "t TIMESTAMP), " +
                        "INDEX (b) " + // INDEX is not supported for non-SYMBOL columns
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR",
                60,
                "indexes are supported only for SYMBOL columns: b"
        );
    }

    @Test
    public void testCreateTableInvalidCapacity() throws Exception {
        assertSyntaxError(
                "create table x (a symbol capacity z)",
                34,
                "bad integer"
        );
    }

    @Test
    public void testCreateTableInvalidColumnInIndex() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN), " +
                        "index(k) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                109,
                "Invalid column"
        );
    }

    @Test
    public void testCreateTableInvalidColumnType() throws Exception {
        assertSyntaxError(
                "create table tab (a int, b integer)",
                27,
                "unsupported column type"
        );
    }

    @Test
    public void testCreateTableInvalidPartitionBy() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by EPOCH",
                128,
                "'NONE', 'HOUR', 'DAY', 'MONTH' or 'YEAR' expected"
        );
    }

    @Test
    public void testCreateTableInvalidTimestampColumn() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(zyz) " +
                        "partition by YEAR",
                112,
                "Invalid column"
        );
    }

    @Test
    public void testCreateTableLike() throws Exception {
        assertCreateTable(
                "create table x (like y)",
                "create table x (like y)"
        );
    }

    @Test
    public void testCreateTableMisplacedCastCapacity() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double capacity 16)",
                42,
                "')' expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableMisplacedCastDef() throws Exception {
        assertSyntaxError(
                "create table tab (a int, b long), cast (a as double)",
                34,
                "cast is only supported"
        );
    }

    @Test
    public void testCreateTableMissing() throws Exception {
        assertSyntaxError(
                "create",
                6,
                "'table' expected"
        );
    }

    @Test
    public void testCreateTableMissingColumnDef() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN, ) " +
                        "timestamp(t) " +
                        "partition by YEAR index",
                102,
                "missing column definition"
        );
    }

    @Test
    public void testCreateTableMissingDef() throws Exception {
        assertSyntaxError("create table xyx", 16, "'(' or 'as' expected");
    }

    @Test
    public void testCreateTableMissingName() throws Exception {
        assertSyntaxError("create table ", 13, "table name or 'if' expected");
    }

    @Test
    public void testCreateTableNoCache() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 nocache," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL NOCACHE, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "TIMESTAMP(t) " +
                        "PARTITION by YEAR"
        );
    }

    @Test
    public void testCreateTableNoCacheIndex() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 nocache index capacity 256," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL nocache index, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR"
        );
    }

    @Test
    public void testCreateTableOutOfPlaceIndex() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a SYMBOL capacity 128 cache index capacity 256," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 256," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by MONTH",
                "create table x (" +
                        "a SYMBOL, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a) " +
                        ", index (x) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testCreateTableOutOfPlaceIndexAndCapacity() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a SYMBOL capacity 128 cache index capacity 16," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 32," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by MONTH",
                "create table x (" +
                        "a SYMBOL, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity 24) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a SYMBOL, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity 10000000) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                176,
                "max index block capacity is"
        );
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityInvalid() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a SYMBOL, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity -) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                177,
                "bad integer"
        );
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a SYMBOL, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity 1) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                176,
                "min index block capacity is"
        );
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityLow2() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a SYMBOL, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity -10) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                176,
                "min index block capacity is"
        );
    }

    @Test
    public void testCreateTableRoundedSymbolCapacity() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 512 cache," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL capacity 500, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testCreateTableSymbolCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 1100000000, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                80,
                "max symbol capacity is"
        );
    }

    @Test
    public void testCreateTableSymbolCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity -10, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                80,
                "min symbol capacity is"
        );
    }

    @Test
    public void testCreateTableTableNameIsQuotedKeyword() throws SqlException {
        assertModel("create table from (a INT)", "create table \"from\" (a int)", ExecutionModel.CREATE_TABLE);
    }

    @Test
    public void testCreateTableTableNameIsUnquotedKeyword() throws Exception {
        assertException(
                "create table from (a int)",
                13,
                "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\""
        );
    }

    @Test
    public void testCreateTableUnexpectedToken() throws Exception {
        assertSyntaxError(
                "create table x blah",
                15,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableUnexpectedToken2() throws Exception {
        assertSyntaxError(
                "create table x (a int, b double), xyz",
                34,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableUnexpectedTrailingToken() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR index",
                133,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableUnexpectedTrailingToken2() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        " index",
                116,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableWitInvalidMaxUncommittedRows() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=asif,",
                95,
                "could not parse maxUncommittedRows value \"asif\""
        );
    }

    @Test
    public void testCreateTableWitInvalidO3MaxLag() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH o3MaxLag=asif,",
                89,
                "invalid interval qualifier asif"
        );
    }

    @Test
    public void testCreateTableWithGeoHash1() throws Exception {
        assertCreateTable(
                "create table x (gh GEOHASH(8c), t TIMESTAMP) timestamp(t) partition by DAY",
                "create table x (gh GEOHASH(8c), t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, o3MaxLag=250ms;"
        );
    }

    @Test
    public void testCreateTableWithGeoHash2() throws Exception {
        assertCreateTable(
                "create table x (gh GEOHASH(51b), t TIMESTAMP) timestamp(t) partition by DAY",
                "create table x (gh GEOHASH(51b), t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, o3MaxLag=250ms;"
        );
    }

    @Test
    public void testCreateTableWithGeoHashNoSizeUnit() throws Exception {
        assertSyntaxError(
                "create table x (gh GEOHASH(12), t TIMESTAMP) timestamp(t) partition by DAY",
                26, "invalid GEOHASH size units, must be 'c', 'C' for chars, or 'b', 'B' for bits"
        );
    }

    @Test
    public void testCreateTableWithGeoHashVariablePrecisionIsNotSupportedYet() throws Exception {
        assertSyntaxError(
                "create table x (gh GEOHASH(), t TIMESTAMP) timestamp(t) partition by DAY",
                27, "literal expected"
        );
    }

    @Test
    public void testCreateTableWithGeoHashWrongSize1() throws Exception {
        assertSyntaxError(
                "create table x (gh GEOHASH(0b), t TIMESTAMP) timestamp(t) partition by DAY",
                26, "invalid GEOHASH type precision range, must be [1, 60] bits, provided=0"
        );
    }

    @Test
    public void testCreateTableWithGeoHashWrongSize2() throws Exception {
        assertSyntaxError(
                "create table x (gh GEOHASH(61b), t TIMESTAMP) timestamp(t) partition by DAY",
                26, "invalid GEOHASH type precision range, must be [1, 60] bits, provided=61"
        );
    }

    @Test
    public void testCreateTableWithGeoHashWrongSizeUnit() throws Exception {
        assertSyntaxError(
                "create table x (gh GEOHASH(12s), t TIMESTAMP) timestamp(t) partition by DAY",
                26, "invalid GEOHASH size units, must be 'c', 'C' for chars, or 'b', 'B' for bits"
        );
    }

    @Test
    public void testCreateTableWithInvalidParameter1() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, o3invalid=250ms",
                112,
                "unrecognized o3invalid after WITH"
        );
    }

    @Test
    public void testCreateTableWithInvalidParameter2() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000 x o3MaxLag=250ms",
                96,
                "unexpected token [x]"
        );
    }

    @Test
    public void testCreateTableWithO3() throws Exception {
        assertCreateTable(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY",
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, o3MaxLag=250ms;"
        );
    }

    @Test
    public void testCreateTableWithPartialParameter1() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, o3MaxLag=",
                105,
                "too few arguments for '=' [found=1,expected=2]"
        );
    }

    @Test
    public void testCreateTableWithPartialParameter2() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, o3MaxLag",
                105,
                "expected parameter after WITH"
        );
    }

    @Test
    public void testCreateTableWithPartialParameter3() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000,",
                95,
                "unexpected token [,]"
        );
    }

    @Test
    public void testCreateTableWithoutDesignatedTimestamp() throws Exception {
        assertSyntaxError(
                "create table x (a timestamp) " +
                        "partition by DAY",
                42,
                "partitioning is possible only on tables with designated timestamps"
        );
    }

    @Test
    public void testCreateUnsupported() throws Exception {
        assertSyntaxError("create object x", 7, "table");
    }

    @Test
    public void testCrossJoin() throws Exception {
        assertSyntaxError("select x from a a cross join b on b.x = a.x", 31, "cannot");
    }

    @Test
    public void testCrossJoin2() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a cross join b z) a",
                "select a.x from a a cross join b z",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testCrossJoin3() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a join select [x] from c on c.x = a.x cross join b z) a",
                "select a.x from a a " +
                        "cross join b z " +
                        "join c on a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testCrossJoinNoAlias() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a join select [x] from c on c.x = a.x cross join b) a",
                "select a.x from a a " +
                        "cross join b " +
                        "join c on a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testCrossJoinToInnerJoin() throws SqlException {
        assertQuery(
                "select-choose tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x)",
                "select * from tab1 cross join tab2  where tab1.x = tab2.x",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testCrossJoinWithClause() throws SqlException {
        assertQuery(
                "select-choose c.customerId customerId, c.name name, c.age age, c1.customerId customerId1, c1.name name1, c1.age age1 from (select [customerId, name, age] from (select-choose [customerId, name, age] customerId, name, age from (select [customerId, name, age] from customers where name ~ 'X')) c cross join select [customerId, name, age] from (select-choose [customerId, name, age] customerId, name, age from (select [customerId, name, age] from customers where name ~ 'X' and age = 30)) c1) c limit 10",
                "with" +
                        " cust as (customers where name ~ 'X')" +
                        " select * from cust c cross join cust c1 where c1.age = 30 " +
                        " limit 10",
                modelOf("customers")
                        .col("customerId", ColumnType.INT)
                        .col("name", ColumnType.STRING)
                        .col("age", ColumnType.BYTE)
        );
    }

    @Test
    public void testCursorFromFuncAliasConfusing() throws SqlException {
        assertQuery(
                "select-choose x1 from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() x1] pg_catalog.pg_class() x1 from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorFromFuncAliasUnique() throws SqlException {
        assertQuery(
                "select-choose z from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() z] pg_catalog.pg_class() z from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() z from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelect() throws SqlException {
        assertQuery(
                "select-virtual x1, x1 . n column from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() x1] pg_catalog.pg_class() x1 from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() x, (pg_catalog.pg_class()).n from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectConfusingAliases() throws Exception {
        assertException(
                "select (pg_catalog.pg_class()).n, (pg_catalog.pg_description()).z, pg_catalog.pg_class() x, pg_catalog.pg_description() x from long_sequence(2)",
                120,
                "Duplicate column [name=x]"
        );
    }

    @Test
    public void testCursorInSelectExprFirst() throws SqlException {
        assertQuery(
                "select-virtual pg_class . n column, pg_class x from (select-choose [pg_class] pg_class, pg_class x from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1))",
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectNotAliased() throws SqlException {
        assertQuery(
                "select-virtual pg_class, pg_class . n column from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class(), (pg_catalog.pg_class()).n from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectOneColumn() throws SqlException {
        assertQuery(
                "select-choose x1 from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() x1] pg_catalog.pg_class() x1 from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectOneColumnSansAlias() throws SqlException {
        assertQuery
                (
                        "select-choose pg_class from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)",
                        "select pg_catalog.pg_class() from long_sequence(2)"
                );
    }

    @Test
    public void testCursorInSelectReverseOrder() throws SqlException {
        assertQuery(
                "select-virtual pg_class . n column, pg_class x from (select-choose [pg_class] pg_class, pg_class x from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1))",
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectReverseOrderRepeatAlias() throws SqlException {
        assertQuery(
                "select-virtual pg_class . n column, pg_class from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)",
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() pg_class from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectSameTwice() throws Exception {
        assertSyntaxError(
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() x, pg_catalog.pg_class() X from long_sequence(2)",
                81,
                "Duplicate column [name=X]"
        );
    }

    @Test
    public void testCursorInSelectSameTwiceNonAscii() throws Exception {
        assertSyntaxError(
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() 侘寂, pg_catalog.pg_class() 侘寂 from long_sequence(2)",
                82,
                "Duplicate column [name=侘寂]"
        );
    }

    @Test
    public void testCursorInSelectWithAggregation() throws SqlException {
        assertQuery(
                "select-virtual sum - 20 column, column1 from (select-group-by [sum(pg_catalog.pg_class() . n + 1) sum, column1] sum(pg_catalog.pg_class() . n + 1) sum, column1 from (select-virtual [pg_class . y column1] pg_class . y column1 from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)))",
                "select sum((pg_catalog.pg_class()).n + 1) - 20, pg_catalog.pg_class().y from long_sequence(2)"
        );
    }

    @Test
    public void testCursorMultiple() throws SqlException {
        assertQuery(
                "select-choose pg_class, pg_description from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1 cross join select-cursor [pg_catalog.pg_description() pg_description] pg_catalog.pg_description() pg_description from (pg_catalog.pg_description()) _xQdbA2)",
                "select pg_catalog.pg_class(), pg_catalog.pg_description() from long_sequence(2)"
        );
    }

    @Test
    public void testCursorMultipleDuplicateAliases() throws Exception {
        assertSyntaxError(
                "select pg_catalog.pg_class() cc, pg_catalog.pg_description() cc from long_sequence(2)",
                61,
                "Duplicate column [name=cc]"
        );
    }

    @Test
    public void testDisallowDotInColumnAlias() throws Exception {
        assertSyntaxError("select x x.y, y from tab order by x", 10, "',', 'from' or 'over' expected");
    }

    @Test
    public void testDisallowDotInColumnAlias2() throws Exception {
        assertSyntaxError("select x ., y from tab order by x", 9, "not allowed");
    }

    @Test
    public void testDisallowedColumnAliases() throws SqlException {
        assertQuery(
                "select-virtual x + z column, x - z column1, x * z column2, x / z column3, x % z column4, x ^ z column5 from (select [z, x] from tab1)",
                "select x+z, x-z, x*z, x/z, x%z, x^z from tab1",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testDodgyCaseExpression() throws Exception {
        assertSyntaxError(
                "select case end + 1, b from tab",
                12,
                "'when' expected",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testDottedConstAlias() throws Exception {
        assertSql("column1\tdjnfkvbjke\n" +
                ".f.e.j.hve\tdjnfkvbjke\n", "select '.f.e.j.hve', 'djnfkvbjke'");
    }

    @Test
    public void testDottedConstAlias2() throws Exception {
        assertSql("column1\tdjnfkvbjke\tcolumn2\tcolumn3\tcolumn4\n" +
                ".f.e.j.hve\tdjnfkvbjke\t2.2\ta.a\t6.4\n", "select '.f.e.j.hve' column1, 'djnfkvbjke', 2.2, 'a.a', 6.4");
    }

    @Test
    public void testDottedConstAlias3() throws Exception {
        assertSql("column2\tdjnfkvbjke\tcolumn1\tcolumn3\tcolumn4\n" +
                ".f.e.j.hve\tdjnfkvbjke\t2.2\taghtrtr.ahnyyn\t6.4\n", "select '.f.e.j.hve', 'djnfkvbjke', 2.2 column1, 'aghtrtr.ahnyyn', 6.4");
    }

    @Test
    public void testDottedConstAlias4() throws Exception {
        assertSql("x\tx1\n" +
                "1\t1\n" +
                "1\t2\n" +
                "1\t3\n" +
                "1\t4\n" +
                "1\t5\n" +
                "1\t6\n" +
                "1\t7\n" +
                "1\t8\n" +
                "1\t9\n" +
                "1\t10\n" +
                "2\t1\n" +
                "2\t2\n" +
                "2\t3\n" +
                "2\t4\n" +
                "2\t5\n" +
                "2\t6\n" +
                "2\t7\n" +
                "2\t8\n" +
                "2\t9\n" +
                "2\t10\n" +
                "3\t1\n" +
                "3\t2\n" +
                "3\t3\n" +
                "3\t4\n" +
                "3\t5\n" +
                "3\t6\n" +
                "3\t7\n" +
                "3\t8\n" +
                "3\t9\n" +
                "3\t10\n" +
                "4\t1\n" +
                "4\t2\n" +
                "4\t3\n" +
                "4\t4\n" +
                "4\t5\n" +
                "4\t6\n" +
                "4\t7\n" +
                "4\t8\n" +
                "4\t9\n" +
                "4\t10\n" +
                "5\t1\n" +
                "5\t2\n" +
                "5\t3\n" +
                "5\t4\n" +
                "5\t5\n" +
                "5\t6\n" +
                "5\t7\n" +
                "5\t8\n" +
                "5\t9\n" +
                "5\t10\n" +
                "6\t1\n" +
                "6\t2\n" +
                "6\t3\n" +
                "6\t4\n" +
                "6\t5\n" +
                "6\t6\n" +
                "6\t7\n" +
                "6\t8\n" +
                "6\t9\n" +
                "6\t10\n" +
                "7\t1\n" +
                "7\t2\n" +
                "7\t3\n" +
                "7\t4\n" +
                "7\t5\n" +
                "7\t6\n" +
                "7\t7\n" +
                "7\t8\n" +
                "7\t9\n" +
                "7\t10\n" +
                "8\t1\n" +
                "8\t2\n" +
                "8\t3\n" +
                "8\t4\n" +
                "8\t5\n" +
                "8\t6\n" +
                "8\t7\n" +
                "8\t8\n" +
                "8\t9\n" +
                "8\t10\n" +
                "9\t1\n" +
                "9\t2\n" +
                "9\t3\n" +
                "9\t4\n" +
                "9\t5\n" +
                "9\t6\n" +
                "9\t7\n" +
                "9\t8\n" +
                "9\t9\n" +
                "9\t10\n" +
                "10\t1\n" +
                "10\t2\n" +
                "10\t3\n" +
                "10\t4\n" +
                "10\t5\n" +
                "10\t6\n" +
                "10\t7\n" +
                "10\t8\n" +
                "10\t9\n" +
                "10\t10\n", "select a.x, b.x from long_sequence(10) a cross join long_sequence(10) b");
    }

    @Test
    public void testDropTablesMissingComma() throws Exception {
        assertSyntaxError(
                "drop tables tab1 tab2",
                5,
                "'table' or 'all tables' expected",
                modelOf("tab1").col("a", ColumnType.INT).col("b", ColumnType.INT),
                modelOf("tab2").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testDropTablesMissingName() throws Exception {
        assertSyntaxError(
                "drop tables tab1, ",
                5,
                "'table' or 'all tables' expected",
                modelOf("tab1").col("a", ColumnType.INT).col("b", ColumnType.INT),
                modelOf("tab2").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testDropTablesTableIsSingular() throws Exception {
        assertSyntaxError(
                "drop table tab1, tab2",
                15,
                "unexpected token [,]",
                modelOf("tab1").col("a", ColumnType.INT).col("b", ColumnType.INT),
                modelOf("tab2").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testDuplicateAlias() throws Exception {
        assertSyntaxError("customers a" +
                        " cross join orders a", 30, "Duplicate table or alias: a",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING)
        );
    }

    @Test
    public void testDuplicateColumnErrorPos() throws Exception {
        assertException(
                "create table test(col1 int, col2 long, col3 double, col4 string, ts timestamp, col4 symbol) timestamp(ts) partition by DAY;",
                79,
                "Duplicate column [name=col4]"
        );
    }

    @Test
    public void testDuplicateColumnGroupBy() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h",
                "select b, sum(a), k k1, k from x y sample by 3h",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateColumnsBasicSelect() throws SqlException {
        assertQuery(
                "select-choose b, a, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1 from (select [b, a, k] from x timestamp (timestamp)))",
                "select b, a, k k1, k from x",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateColumnsVirtualAndGroupBySelect() throws SqlException {
        assertQuery(
                "select-group-by sum(b + a) sum, column, k1, k1 k from (select-virtual [a, b, a + b column, k1] a, b, a + b column, k1, k1 k, timestamp from (select-choose [a, b, k k1] a, b, k k1, timestamp from (select [a, b, k] from x timestamp (timestamp)))) sample by 1m",
                "select sum(b+a), a+b, k k1, k from x sample by 1m",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateColumnsVirtualSelect() throws SqlException {
        assertQuery(
                "select-virtual b + a column, k1, k1 k from (select-choose [a, b, k k1] a, b, k k1 from (select [a, b, k] from x timestamp (timestamp)))",
                "select b+a, k k1, k from x",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateTables() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, customers.customerName customerName, cust.customerId customerId1, cust.customerName customerName1 from (select [customerId, customerName] from customers cross join select [customerId, customerName] from customers cust)",
                "customers cross join customers cust",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING)
        );
    }

    @Test
    public void testEmptyColumnAliasDisallowed() throws Exception {
        assertSyntaxError("select x as '' from long_sequence(1)", 12, "non-empty identifier");
        assertSyntaxError("select 'x' '' from long_sequence(1)", 11, "non-empty identifier");
        assertSyntaxError("select x as \"\" from long_sequence(1)", 12, "non-empty identifier");
    }

    @Test
    public void testEmptyCommonTableExpressionNameDisallowed() throws Exception {
        assertSyntaxError("with \"\" as (select 'a' ) select * from \"\"", 5, "empty");
    }

    @Test
    public void testEmptyOrderBy() throws Exception {
        assertSyntaxError("select x, y from tab order by", 29, "literal expected");
    }

    @Test
    public void testEmptySampleBy() throws Exception {
        assertSyntaxError("select x, y from tab sample by", 30, "time interval unit expected");
    }

    @Test
    public void testEmptyTableAliasDisallowed() throws Exception {
        assertSyntaxError("select x from long_sequence(1) ''", 31, "table alias");
        assertSyntaxError("select x from long_sequence(1) as ''", 34, "table alias");
        assertSyntaxError("select * from long_sequence(1) \"\"", 31, "table alias");
        assertSyntaxError("select * from long_sequence(1) as \"\"", 34, "table alias");
        assertSyntaxError("select ''.* from long_sequence(1) as ''", 37, "table alias");
        assertSyntaxError("select \"\".* from long_sequence(1) as \"\"", 37, "table alias");
        assertSyntaxError("select ''.\"*\" from long_sequence(1) as ''", 39, "table alias");
    }

    @Test
    public void testEmptyWhere() throws Exception {
        assertException(
                "(select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)) where",
                "create table tab (\n" +
                        "    tag string,\n" +
                        "    seq long\n" +
                        ")",
                71,
                "empty where clause"
        );
    }

    @Test
    public void testEqualsConstantTransitivityLhs() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, o.customerId customerId1 from (select [customerId] from customers c left join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.customerId where 100 = customerId) c",
                "customers c" +
                        " left join orders o on c.customerId = o.customerId" +
                        " where 100 = c.customerId",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testEqualsConstantTransitivityRhs() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, o.customerId customerId1 from (select [customerId] from customers c left join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.customerId where customerId = 100) c",
                "customers c" +
                        " left join orders o on c.customerId = o.customerId" +
                        " where c.customerId = 100",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testEraseColumnPrefix() throws SqlException {
        assertQuery(
                "select-choose name from (select [name] from cust where name ~ 'x')",
                "cust where cust.name ~ 'x'",
                modelOf("cust").col("name", ColumnType.STRING)
        );
    }

    @Test
    public void testEraseColumnPrefixInJoin() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, o.customerId customerId1, o.x x from (select [customerId] from customers c left join select [customerId, x] from (select-choose [customerId, x] customerId, x from (select [customerId, x] from orders o where x = 10 and customerId = 100) o) o on customerId = c.customerId where customerId = 100) c",
                "customers c" +
                        " left join (orders o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.customerId = 100",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testEraseColumnPrefixInJoinWithNestedUnion() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, o.customerId customerId1, o.x x from (select [customerId] from customers c left join select [customerId, x] from (select-choose [customerId, x] customerId, x from (select [customerId, x] from (select-choose [customerId, x] customerId, x from (select [customerId, x] from orders) union select-choose [customerId, x] customerId, x from (select [customerId, x] from orders)) o where x = 10 and customerId = 100) o) o on customerId = c.customerId where customerId = 100) c",
                "customers c" +
                        " left join ((orders union orders) o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.customerId = 100",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testEraseColumnPrefixInJoinWithOuterUnion() throws Exception {
        assertQuery(
                "select-choose customerId from (select-choose [c.customerId customerId] c.customerId customerId from (select [customerId] from customers c left join select [customerId] from (select-choose [customerId] customerId, x from (select [customerId, x] from orders o where x = 10 and customerId = 100) o) o on customerId = c.customerId where customerId = 100) c)" +
                        " union all" +
                        " select-choose customerId from (select-choose [c.customerId customerId] c.customerId customerId from (select [customerId] from customers c left join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.customerId where customerId = 100) c)",
                "(select c.customerId" +
                        " from customers c" +
                        " left join (orders o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.customerId = 100)" +
                        " union all" +
                        " (select c.customerId " +
                        "  from customers c" +
                        "  left join orders o on c.customerId = o.customerId" +
                        "  where c.customerId = 100)",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExcelODBCQ2() throws Exception {
        assertQuery(
                "select-choose" +
                        " ta.attname attname," +
                        " ia.attnum attnum," +
                        " ic.relname relname," +
                        " n.nspname nspname," +
                        " tc.relname relname1" +
                        " from (" +
                        "select [attname, attnum, attrelid, attisdropped] from pg_catalog.pg_attribute() ta" +
                        " join (select" +
                        " [relname, oid, relnamespace]" +
                        " from pg_catalog.pg_class() tc" +
                        " where relname = 'telemetry_config') tc" +
                        " on tc.oid = ta.attrelid" +
                        " join (select" +
                        " [indexrelid, indkey, indrelid, indisprimary]" +
                        " from pg_catalog.pg_index() i" +
                        " where indisprimary = 't') i" +
                        " on i.indrelid = ta.attrelid" +
                        " join (" +
                        "select [attnum, attrelid, attisdropped]" +
                        " from pg_catalog.pg_attribute() ia" +
                        " where not(attisdropped)) ia" +
                        " on ia.attrelid = i.indexrelid" +
                        " post-join-where ta.attnum = [](i.indkey,ia.attnum - 1)" +
                        " join (select [nspname, oid]" +
                        " from pg_catalog.pg_namespace() n" +
                        " where nspname = 'public') n" +
                        " on n.oid = tc.relnamespace" +
                        " join select [relname, oid]" +
                        " from pg_catalog.pg_class() ic" +
                        " on ic.oid = i.indexrelid" +
                        " where not(attisdropped)) ta" +
                        " order by attnum",
                "select\n" +
                        "  ta.attname,\n" +
                        "  ia.attnum,\n" +
                        "  ic.relname,\n" +
                        "  n.nspname,\n" +
                        "  tc.relname\n" +
                        "from\n" +
                        "  pg_catalog.pg_attribute ta,\n" +
                        "  pg_catalog.pg_attribute ia,\n" +
                        "  pg_catalog.pg_class tc,\n" +
                        "  pg_catalog.pg_index i,\n" +
                        "  pg_catalog.pg_namespace n,\n" +
                        "  pg_catalog.pg_class ic\n" +
                        "where\n" +
                        "  tc.relname = 'telemetry_config'\n" +
                        "  AND n.nspname = 'public'\n" +
                        "  AND tc.oid = i.indrelid\n" +
                        "  AND n.oid = tc.relnamespace\n" +
                        "  AND i.indisprimary = 't'\n" +
                        "  AND ia.attrelid = i.indexrelid\n" +
                        "  AND ta.attrelid = i.indrelid\n" +
                        "  AND ta.attnum = i.indkey [ ia.attnum -1 ]\n" +
                        "  AND (NOT ta.attisdropped)\n" +
                        "  AND (NOT ia.attisdropped)\n" +
                        "  AND ic.oid = i.indexrelid\n" +
                        "order by\n" +
                        "  ia.attnum\n" +
                        ";\n"
        );
    }

    @Test
    public void testExcelODBCQ3() throws Exception {
        assertQuery(
                "select-virtual" +
                        " attname, attnum, relname, nspname, NULL NULL" +
                        " from (select-choose" +
                        " [ta.attname attname, ia.attnum attnum, ic.relname relname, n.nspname nspname]" +
                        " ta.attname attname, ia.attnum attnum, ic.relname relname, n.nspname nspname" +
                        " from (select" +
                        " [attname, attnum, attrelid, attisdropped]" +
                        " from pg_catalog.pg_attribute() ta" +
                        " join select" +
                        " [indexrelid, indkey, indrelid]" +
                        " from pg_catalog.pg_index() i" +
                        " on i.indrelid = ta.attrelid" +
                        " join (select" +
                        " [attnum, attrelid, attisdropped]" +
                        " from pg_catalog.pg_attribute() ia" +
                        " where not(attisdropped)) ia" +
                        " on ia.attrelid = i.indexrelid" +
                        " post-join-where ta.attnum = [](i.indkey,ia.attnum - 1)" +
                        " join (select [relname, oid, relnamespace]" +
                        " from pg_catalog.pg_class() ic" +
                        " where relname = 'telemetry_config_pkey') ic" +
                        " on ic.oid = ia.attrelid" +
                        " join (select [nspname, oid]" +
                        " from pg_catalog.pg_namespace() n" +
                        " where nspname = 'public') n" +
                        " on n.oid = ic.relnamespace" +
                        " where not(attisdropped)) ta) ta" +
                        " order by attnum",
                "select\n" +
                        "  ta.attname,\n" +
                        "  ia.attnum,\n" +
                        "  ic.relname,\n" +
                        "  n.nspname,\n" +
                        "  NULL\n" +
                        "from\n" +
                        "  pg_catalog.pg_attribute ta,\n" +
                        "  pg_catalog.pg_attribute ia,\n" +
                        "  pg_catalog.pg_class ic,\n" +
                        "  pg_catalog.pg_index i,\n" +
                        "  pg_catalog.pg_namespace n\n" +
                        "where\n" +
                        "  ic.relname = 'telemetry_config_pkey'\n" +
                        "  AND n.nspname = 'public'\n" +
                        "  AND ic.oid = i.indexrelid\n" +
                        "  AND n.oid = ic.relnamespace\n" +
                        "  AND ia.attrelid = i.indexrelid\n" +
                        "  AND ta.attrelid = i.indrelid\n" +
                        "  AND ta.attnum = i.indkey [ ia.attnum -1 ]\n" +
                        "  AND (NOT ta.attisdropped)\n" +
                        "  AND (NOT ia.attisdropped)\n" +
                        "order by\n" +
                        "  ia.attnum\n" +
                        ";\n"
        );
    }

    @Test
    public void testExceptSansSelect() throws Exception {
        assertQuery(
                "select-choose ts, x from (select-choose [ts, x] ts, x from (select [ts, x] from t1) except select-choose [ts, x] ts, x from (select [ts, x] from t2))",
                "(t1 except t2)",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                modelOf("t2").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithBadFormat() throws Exception {
        assertSyntaxError("explain (format xyz) select * from x", 16, "unexpected explain format found",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithBadOption() throws Exception {
        assertSyntaxError("explain (xyz) select * from x", 14, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithBadSql1() throws Exception {
        assertSyntaxError("explain select 1)", 16, "unexpected token [)]",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithBadSql2() throws Exception {
        assertSyntaxError("explain select 1))", 16, "unexpected token [)]",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithDefaultFormat() throws Exception {
        assertModel("EXPLAIN (FORMAT TEXT) ",
                "explain select * from x", ExecutionModel.EXPLAIN,
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithJsonFormat() throws Exception {
        assertModel("EXPLAIN (FORMAT JSON) ",
                "explain (format json) select * from x", ExecutionModel.EXPLAIN,
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithMissingFormat() throws Exception {
        assertSyntaxError("explain (format) select * from x", 15, "unexpected explain format found",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithQueryInParentheses1() throws Exception {
        assertModel("EXPLAIN (FORMAT TEXT) ",
                "explain (select x from x) ", ExecutionModel.EXPLAIN,
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithQueryInParentheses2() throws Exception {
        assertModel("EXPLAIN (FORMAT TEXT) ",
                "explain (x) ", ExecutionModel.EXPLAIN,
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExplainWithTextFormat() throws Exception {
        assertModel("EXPLAIN (FORMAT TEXT) ",
                "explain (format text ) select * from x", ExecutionModel.EXPLAIN,
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExpressionIsNotNull() throws Exception {
        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x where coalesce(x,42) != null)",
                "tab1 join tab2 on tab1.x = tab2.y where coalesce(tab1.x, 42) is not null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testExpressionIsNull() throws Exception {
        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x where coalesce(x,42) = null)",
                "tab1 join tab2 on tab1.x = tab2.y where coalesce(tab1.x, 42) is null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testExpressionSyntaxError() throws Exception {
        assertSyntaxError("select x from a where a + b(c,) > 10", 30, "missing argument");

        // when AST cache is not cleared below query will pick up "garbage" and will misrepresent error
        assertSyntaxError("orders join customers on orders.customerId = c.customerId", 45, "alias",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testExtraComma2OrderByInWindowFunction() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ts,) from xyz", 50, "Expression expected");
    }

    @Test
    public void testExtraCommaOrderByInWindowFunction() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ,ts) from xyz", 47, "Expression expected");
    }

    @Test
    public void testExtraCommaPartitionByInWindowFunction() throws Exception {
        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts) from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b, order by ts) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")

        );
    }

    @Test
    public void testFailureOrderByGroupByColPrefixed() throws Exception {
        assertException(
                "select a, sum(b) b from tab order by tab.b, a",
                "create table tab (\n" +
                        "    a int,\n" +
                        "    b int\n" +
                        ")",
                37,
                "Invalid column: tab.b"
        );
    }

    @Test
    public void testFailureOrderByGroupByColPrefixed2() throws Exception {
        assertException(
                "select a, sum(b) b from tab order by a, tab.b",
                "create table tab (\n" +
                        "    a int,\n" +
                        "    b int\n" +
                        ")",
                40,
                "Invalid column: tab.b"
        );
    }

    @Test
    public void testFailureOrderByGroupByColPrefixed3() throws Exception {
        assertException(
                "select a, sum(b) b from tab order by tab.a, tab.b",
                "create table tab (\n" +
                        "    a int,\n" +
                        "    b int\n" +
                        ")",
                44,
                "Invalid column: tab.b"
        );
    }

    @Test
    public void testFailureOrderByOnOuterResultWhenOrderByColumnIsNotSelected() throws Exception {
        assertException(
                "select x, sum(2*y+x) + sum(3/x) z from tab order by z asc, tab.y desc",
                "create table tab (\n" +
                        "    x double,\n" +
                        "    y int\n" +
                        ")",
                59,
                "Invalid column: tab.y"
        );
    }

    @Test
    public void testFilter1() throws SqlException {
        assertQuery(
                "select-virtual x, cast(x + 10,timestamp) cast from (select-virtual [x, rnd_double() rnd] x, rnd_double() rnd from (select [x] from long_sequence(100000)) where rnd < 0.9999)",
                "select x, cast(x+10 as timestamp) from (select x, rnd_double() rnd from long_sequence(100000)) where rnd<0.9999"
        );
    }

    @Test
    public void testFilter2() throws Exception {
        assertQuery(
                "select-virtual customerId + 1 column, name, count from (select-group-by [customerId, name, count() count] customerId, name, count() count from (select-choose [customerId, customerName name] customerId, customerName name from (select [customerId, customerName] from customers where customerName = 'X')))",
                "select customerId+1, name, count from (select customerId, customerName name, count() count from customers) where name = 'X'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testFilterOnGroupBy() throws SqlException {
        assertQuery(
                "select-choose hash, count from (select-group-by [hash, count() count] hash, count() count from (select [hash] from transactions.csv) where count > 1)",
                "select * from (select hash, count() from 'transactions.csv') where count > 1;",
                modelOf("transactions.csv").col("hash", ColumnType.LONG256)
        );
    }

    @Test
    public void testFilterOnSubQuery() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, c.customerName customerName, c.count count, o.orderId orderId, o.customerId customerId1 from (select [customerId, customerName, count] from (select-group-by [customerId, customerName, count() count] customerId, customerName, count() count from (select [customerId, customerName] from customers where customerId > 400 and customerId < 1200) where count > 1) c left join select [orderId, customerId] from orders o on o.customerId = c.customerId post-join-where o.orderId = NaN) c order by customerId",
                "(select customerId, customerName, count() count from customers) c" +
                        " left join orders o on c.customerId = o.customerId " +
                        " where o.orderId = NaN and c.customerId > 400 and c.customerId < 1200 and count > 1 order by c.customerId",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testFilterPostJoin() throws SqlException {
        assertQuery(
                "select-choose a.tag tag, a.seq hi, b.seq lo from (select [tag, seq] from tab a asof join select [seq, tag] from tab b on b.tag = a.tag post-join-where b.seq < a.seq) a",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag) where b.seq < a.seq",
                modelOf("tab").col("tag", ColumnType.STRING).col("seq", ColumnType.LONG)
        );
    }

    @Test
    public void testFilterPostJoinSubQuery() throws SqlException {
        assertQuery(
                "select-choose tag, hi, lo from (select-choose [a.tag tag, a.seq hi, b.seq lo] a.tag tag, a.seq hi, b.seq lo from (select [tag, seq] from tab a asof join select [seq, tag] from tab b on b.tag = a.tag post-join-where a.seq > b.seq + 1) a)",
                "(select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)) where hi > lo + 1",
                modelOf("tab").col("tag", ColumnType.STRING).col("seq", ColumnType.LONG)
        );
    }

    @Test
    public void testForOrderByOnAliasedColumnNotOnSelect() throws Exception {
        assertQuery(
                "select-choose y from (select-choose [y, x] y, x from (select [y, x] from tab) order by x)",
                "select y from tab order by tab.x",
                modelOf("tab").col("x", ColumnType.DOUBLE).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testForOrderByOnNonSelectedColumn() throws Exception {
        assertQuery(
                "select-choose column from (select-virtual [2 * y + x column, x] 2 * y + x column, x from (select-choose [x, y] x, y from (select [x, y] from tab)) order by x)",
                "select 2*y+x from tab order by x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testForOrderByOnSelectedColumnThatHasNoAlias() throws Exception {
        assertQuery(
                "select-choose column, column1 from (select-virtual [2 * y + x column, 3 / x column1, x] 2 * y + x column, 3 / x column1, x from (select-choose [x, y] x, y from (select [x, y] from tab)) order by x)",
                "select 2*y+x, 3/x from tab order by x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testFullJoinAfterSubqueryClauseGivesError() throws Exception {
        assertSyntaxError(
                "select * from (select * from t1 ) full join t1 on x",
                34,
                "unsupported join type",
                modelOf("t1").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testFullJoinAsFirstJoinGivesError() throws Exception {
        assertSyntaxError(
                "select * from t1 full join t1 on x",
                17,
                "unsupported join type",
                modelOf("t1").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testFullJoinDeepInFromClauseGivesError() throws Exception {
        assertSyntaxError(
                "select * from t1 join t1 on x full join t1 on x",
                30,
                "unsupported join type",
                modelOf("t1").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testFunctionWithoutAlias() throws SqlException {
        assertQuery(
                "select-virtual f(x) f, x from (select [x] from x where x > 1)",
                "select f(x), x from x where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testGenericPreFilterPlacement() throws Exception {
        assertQuery(
                "select-choose customerName, orderId, productId from (select [customerName, customerId] from customers join (select [orderId, productId, customerId, product] from orders where product = 'X') orders on orders.customerId = customers.customerId where customerName ~ 'WTBHZVPVZZ')",
                "select customerName, orderId, productId " +
                        "from customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ' and product = 'X'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING).col("orderId", ColumnType.INT).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testGoodNonQuotedAlias() throws SqlException {
        assertColumnNames("select 'a' a", "a");
        assertColumnNames("select 'a' _", "_");
        assertColumnNames("select 'a' a1$_", "a1$_");
        assertColumnNames("select 'a' _a1$", "_a1$");
    }

    @Test
    public void testGroupByConstant1() throws SqlException {
        assertQuery(
                "select-virtual 'nts' nts, now() now, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select 'nts', now(), min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByConstant2() throws SqlException {
        assertQuery(
                "select-virtual min, 'a' a from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select min(nts), 'a' from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByConstant3() throws SqlException {
        assertQuery(
                "select-virtual 1 + 1 column, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select 1+1, min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByConstant4() throws SqlException {
        assertQuery(
                "select-virtual min, 1 + 2 * 3 column from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select min(nts), 1 + 2 * 3 from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByConstant5() throws SqlException {
        assertQuery(
                "select-virtual min, 1 + now() * 3 column from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select min(nts), 1 + now() * 3 from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByConstant6() throws SqlException {
        assertQuery(
                "select-virtual now() + now() column, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select now() + now(), min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByConstantFunctionMatchingColumnName() throws SqlException {
        assertQuery(
                "select-virtual now() now, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select now(), min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByConstantMatchingColumnName() throws SqlException {
        assertQuery(
                "select-virtual 'nts' nts, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select 'nts', min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testGroupByNotConstant1() throws SqlException {
        assertQuery(
                "select-group-by min(nts) min, 1 + day(nts) * 3 column from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z')",
                "select min(nts), 1 + day(nts) * 3 from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    @Ignore
    // todo: this is not parsed correctly. Parser/Optimiser removes group by clause.
    public void testGroupBySansSelect() throws Exception {
        assertQuery(
                "select-choose ts, x from (select-choose [ts, x] ts, x from (select [ts, x] from t1 latest by x))",
                "(t1 group by x)",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testGroupByTimeInSubQuery() throws SqlException {
        assertQuery(
                "select-group-by min(s) min, max(s) max, avg(s) avg from (select-group-by [sum(value) s, ts] ts, sum(value) s from (select [value, ts] from erdem_x timestamp (ts)))",
                "select min(s), max(s), avg(s) from (select ts, sum(value) s from erdem_x)",
                modelOf("erdem_x").timestamp("ts").col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testGroupByWithLimit() throws SqlException {
        assertQuery(
                "select-group-by fromAddress, toAddress, count() count from (select [fromAddress, toAddress] from transactions.csv) limit 10000",
                "select fromAddress, toAddress, count() from 'transactions.csv' limit 10000",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.STRING)
                        .col("toAddress", ColumnType.STRING)
        );
    }

    @Test
    public void testGroupByWithSubQueryLimit() throws SqlException {
        assertQuery(
                "select-group-by fromAddress, toAddress, count() count from (select-choose [fromAddress, toAddress] fromAddress, toAddress from (select [fromAddress, toAddress] from transactions.csv) limit 10000)",
                "select fromAddress, toAddress, count() from ('transactions.csv' limit 10000)",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.STRING)
                        .col("toAddress", ColumnType.STRING)
        );
    }

    @Test
    public void testInnerJoin() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a join select [x] from b on b.x = a.x) a",
                "select a.x from a a inner join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoin2() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, customers.customerName customerName, orders.customerId customerId1 from (select [customerId, customerName] from customers join select [customerId] from orders on orders.customerId = customers.customerId where customerName ~ 'WTBHZVPVZZ')",
                "customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoinColumnAliasNull() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c join (select [customerId] from orders o where customerId = null) o on o.customerId = c.customerId) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() " +
                        "from customers c" +
                        " join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoinEqualsConstant() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, orders.customerId customerId1, orders.productName productName from (select [customerId] from customers join (select [customerId, productName] from orders where productName = 'WTBHZVPVZZ') orders on orders.customerId = customers.customerId)",
                "customers join orders on customers.customerId = orders.customerId where productName = 'WTBHZVPVZZ'",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING)
        );
    }

    @Test
    public void testInnerJoinEqualsConstantLhs() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, orders.customerId customerId1, orders.productName productName from (select [customerId] from customers join (select [customerId, productName] from orders where 'WTBHZVPVZZ' = productName) orders on orders.customerId = customers.customerId)",
                "customers join orders on customers.customerId = orders.customerId where 'WTBHZVPVZZ' = productName",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING)
        );
    }

    @Test
    public void testInnerJoinPostFilter() throws SqlException {
        assertQuery(
                "select-virtual c, a, b, d, d - b column from (select-choose [z.c c, x.a a, b, d] z.c c, x.a a, b, d from (select [a, c] from x join (select [b, m] from y where b < 20) y on y.m = x.c join select [c, d] from z on z.c = x.c))",
                "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20",
                modelOf("x")
                        .col("c", ColumnType.INT)
                        .col("a", ColumnType.INT),
                modelOf("y")
                        .col("m", ColumnType.INT)
                        .col("b", ColumnType.INT),
                modelOf("z")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoinSubQuery() throws Exception {
        assertQuery(
                "select-choose customerName, productName, orderId from (select [customerName, productName, orderId, productId] from (select-choose [customerName, productName, orderId, productId] customerName, orderId, productId, productName from (select [customerName, customerId] from customers join (select [productName, orderId, productId, customerId] from orders where productName ~ 'WTBHZVPVZZ') orders on orders.customerId = customers.customerId)) x join select [productId] from products p on p.productId = x.productId) x",
                "select customerName, productName, orderId from (" +
                        "select \"customerName\", orderId, productId, productName " +
                        "from \"customers\" join orders on \"customers\".\"customerId\" = orders.customerId where productName ~ 'WTBHZVPVZZ'" +
                        ") x" +
                        " join products p on p.productId = x.productId",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT)
        );

        assertQuery(
                "select-choose customerName, productName, orderId from (select [customerName, customerId] from customers join (select [productName, orderId, customerId, productId] from orders o where productName ~ 'WTBHZVPVZZ') o on o.customerId = customers.customerId join select [productId] from products p on p.productId = o.productId)",
                "select customerName, productName, orderId " +
                        " from customers join orders o on customers.customerId = o.customerId " +
                        " join products p on p.productId = o.productId" +
                        " where productName ~ 'WTBHZVPVZZ'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testInsertAsSelect() throws SqlException {
        assertModel(
                "insert into x select-choose c, d from (select [c, d] from y)",
                "insert into x select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectBadBatchSize() throws Exception {
        assertSyntaxError(
                "insert batch 2a lag 100000 into x select * from y",
                13, "bad long integer",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectBatchSize() throws SqlException {
        assertModel(
                "insert batch 15000 into x select-choose c, d from (select [c, d] from y)",
                "insert batch 15000 into x select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectBatchSizeAndLag() throws SqlException {
        assertModel(
                "insert batch 10000 lag 100000 into x select-choose c, d from (select [c, d] from y)",
                "insert batch 10000 o3MaxLag 100ms into x select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectColumnCountMismatch() throws Exception {
        assertSyntaxError("insert into x (b) select * from y",
                12, "column count mismatch",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectColumnList() throws SqlException {
        assertModel(
                "insert into x (a, b) select-choose c, d from (select [c, d] from y)",
                "insert into x (a,b) select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectDuplicateColumns() throws Exception {
        assertSyntaxError("insert into x (b,b) select * from y",
                17, "Duplicate column [name=b]",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectNegativeBatchSize() throws Exception {
        assertSyntaxError(
                "insert batch -25 lag 100000 into x select * from y",
                14, "must be positive",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectNegativeLag() throws Exception {
        assertSyntaxError(
                "insert batch 2 o3MaxLag -4s into x select * from y",
                25, "invalid interval qualifier -",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertColumnValueMismatch() throws Exception {
        assertSyntaxError(
                "insert into x (a,b) values (?)",
                29,
                "value count does not match column count",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertColumnsAndValues() throws SqlException {
        assertModel(
                "insert into x (a, b) values (3, ?)",
                "insert into x (a,b) values (3, ?)",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertColumnsAndValuesQuoted() throws SqlException {
        assertModel(
                "insert into x (a, b) values (3, ?)",
                "insert into \"x\" (\"a\",\"b\") values (3, ?)",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertDanglingValue() throws Exception {
        assertSyntaxError(
                "insert into x(a,b) values(1,)",
                28,
                "Expression expected"
        );
    }

    @Test
    public void testInsertIntoMissing() throws Exception {
        assertSyntaxError(
                "insert onto",
                7,
                "'into' expected"
        );
    }

    @Test
    public void testInsertMisSpeltValues() throws Exception {
        assertSyntaxError(
                "insert into x(a,b) valuos(1,9)",
                19,
                "'select' or 'values' expected"
        );
    }

    @Test
    public void testInsertMissingClosingBracket() throws Exception {
        assertSyntaxError(
                "insert into x values (?,?",
                25,
                "',' expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertMissingClosingQuote() throws Exception {
        assertSyntaxError(
                "insert into x values ('abc)",
                22,
                "unclosed quoted string?",
                modelOf("x")
                        .col("a", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertMissingColumn() throws Exception {
        assertSyntaxError(
                "insert into x(a,)",
                16,
                "missing column name"
        );
    }

    @Test
    public void testInsertMissingValue() throws Exception {
        assertSyntaxError(
                "insert into x values ()",
                22,
                "Expression expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertMissingValueAfterComma() throws Exception {
        assertSyntaxError(
                "insert into x values (?,",
                24,
                "Expression expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertMissingValues() throws Exception {
        assertSyntaxError(
                "insert into x values",
                20,
                "'(' expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertValues() throws SqlException {
        assertModel(
                "insert into x values (3, 'abc', ?)",
                "insert into x values (3, 'abc', ?)",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testInvalidAlias() throws Exception {
        assertSyntaxError("orders join customers on orders.customerId = c.customerId", 45, "alias",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidColumn() throws Exception {
        assertSyntaxError("orders join customers on customerIdx = customerId", 25, "Invalid column",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidColumnInExpression() throws Exception {
        assertSyntaxError(
                "select a + b x from tab",
                11,
                "Invalid column",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidGroupBy1() throws Exception {
        assertSyntaxError("select x, y from tab sample by x,", 32, "one letter sample by period unit expected");
    }

    @Test
    public void testInvalidGroupBy2() throws Exception {
        assertSyntaxError("select x, y from (tab sample by x,)", 33, "one letter sample by period unit expected");
    }

    @Test
    public void testInvalidGroupBy3() throws Exception {
        assertSyntaxError("select x, y from tab sample by x, order by y", 32, "one letter sample by period unit expected");
    }

    @Test
    public void testInvalidInnerJoin1() throws Exception {
        assertSyntaxError("select x from a a inner join b z", 31, "'on'");
    }

    @Test
    public void testInvalidInnerJoin2() throws Exception {
        assertSyntaxError("select x from a a inner join b z on", 33, "Expression");
    }

    @Test
    public void testInvalidIsNotNull() throws Exception {
        assertSyntaxError(
                "a where x is not 12",
                10,
                "IS NOT must be followed by NULL",
                modelOf("a").col("x", ColumnType.INT)
        );
        assertSyntaxError(
                "a where a.x is not 12",
                12,
                "IS NOT must be followed by NULL",
                modelOf("a").col("x", ColumnType.INT)
        );
        assertSyntaxError(
                "a where x is not",
                10,
                "IS NOT must be followed by NULL",
                modelOf("a").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidIsNull() throws Exception {
        assertSyntaxError(
                "a where x is 12",
                10,
                "IS must be followed by NULL",
                modelOf("a").col("x", ColumnType.INT)
        );
        assertSyntaxError(
                "a where a.x is 12",
                12,
                "IS must be followed by NULL",
                modelOf("a").col("x", ColumnType.INT)
        );
        assertSyntaxError(
                "a where x is",
                10,
                "IS must be followed by [NOT] NULL",
                modelOf("a").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidOrderBy1() throws Exception {
        assertSyntaxError("select x, y from tab order by x,", 32, "literal expected");
    }

    @Test
    public void testInvalidOrderBy2() throws Exception {
        assertSyntaxError("select x, y from (tab order by x,)", 33, "literal or expression expected");
    }

    @Test
    public void testInvalidOuterJoin1() throws Exception {
        assertSyntaxError("select x from a a left join b z", 30, "'on'");
    }

    @Test
    public void testInvalidOuterJoin2() throws Exception {
        assertSyntaxError("select x from a a left join b z on", 32, "Expression");
    }

    @Test
    public void testInvalidSelectColumn() throws Exception {
        assertSyntaxError("select c.customerId, orderIdx, o.productId from " +
                        "customers c " +
                        "join (" +
                        "orders where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`) latest on ts partition by customerId" +
                        ") o on c.customerId = o.customerId", 21, "Invalid column",
                modelOf("customers").col("customerName", ColumnType.STRING).col("customerId", ColumnType.INT),
                modelOf("orders").timestamp("ts").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );

        assertSyntaxError("select c.customerId, orderId, o.productId2 from " +
                        "customers c " +
                        "join (" +
                        "orders where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`) latest on ts partition by customerId" +
                        ") o on c.customerId = o.customerId", 30, "Invalid column",
                modelOf("customers").col("customerName", ColumnType.STRING).col("customerId", ColumnType.INT),
                modelOf("orders").timestamp("ts").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );

        assertSyntaxError("select c.customerId, orderId, o2.productId from " +
                        "customers c " +
                        "join (" +
                        "orders where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`) latest on ts partition by customerId" +
                        ") o on c.customerId = o.customerId", 30, "Invalid table name",
                modelOf("customers").col("customerName", ColumnType.STRING).col("customerId", ColumnType.INT),
                modelOf("orders").timestamp("ts").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidSubQuery() throws Exception {
        assertSyntaxError("select x,y from (tab where x = 100 latest by x)", 42, "'on' expected");
    }

    @Test
    public void testInvalidTableName() throws Exception {
        assertSyntaxError("orders join customer on customerId = customerId", 12, "does not exist",
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidTypeCast() throws Exception {
        assertSyntaxError(
                "select cast('2005-04-02 12:00:00-07' as timestamp with time z) col from x",
                50,
                "dangling literal",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testInvalidTypeCast2() throws Exception {
        assertSyntaxError(
                "select cast('2005-04-02 12:00:00-07' as timestamp with tz) col from x",
                50,
                "dangling literal",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testInvalidTypeLiteralCast() throws Exception {
        assertSyntaxError(
                "select * from x where t > timestamp_with_time_zone '2005-04-02 12:00:00-07'",
                26,
                "invalid type",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testInvalidTypeUsedAsDesignatedTimestamp() throws Exception {
        assertSyntaxError(
                "CREATE TABLE ts_test ( close_date date ) timestamp(close_date);",
                51,
                "TIMESTAMP column expected [actual=DATE]"
        );
    }

    @Test
    public void testJoin1() throws Exception {
        assertQuery(
                "select-choose t1.x x, y from (select [x] from (select-choose [x] x from (select [x] from tab t2 where x > 100 latest on ts partition by x) t2) t1 join select [x] from tab2 xx2 on xx2.x = t1.x join select [y, x] from (select-choose [y, x] x, y from (select [y, x, z, b, a] from tab4 where a > b latest on ts partition by z) where y > 0) x4 on x4.x = t1.x cross join select [b] from tab3 post-join-where xx2.x > tab3.b) t1",
                "select t1.x, y from (select x from tab t2 where x > 100 latest on ts partition by x) t1 " +
                        "join tab2 xx2 on xx2.x = t1.x " +
                        "join tab3 on xx2.x > tab3.b " +
                        "join (select x,y from tab4 where a > b latest on ts partition by z) x4 on x4.x = t1.x " +
                        "where y > 0",
                modelOf("tab").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT),
                modelOf("tab3").col("b", ColumnType.INT),
                modelOf("tab4").timestamp("ts").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testJoin3() throws Exception {
        assertQuery(
                "select-choose x from (select-choose [tab2.x x] tab2.x x from (select [x] from tab join select [x] from tab2 on tab2.x = tab.x cross join select [x] from tab3 post-join-where f(tab3.x,tab2.x) = tab.x))",
                "select x from (select tab2.x from tab join tab2 on tab.x=tab2.x join tab3 on f(tab3.x,tab2.x) = tab.x)",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT),
                modelOf("tab3").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinClauseAlignmentBug() throws SqlException {
        assertQuery(
                "select-virtual NULL TABLE_CAT, TABLE_SCHEM, TABLE_NAME, switch(TABLE_SCHEM ~ '^pg_' or TABLE_SCHEM = 'information_schema',true,case(TABLE_SCHEM = 'pg_catalog' or TABLE_SCHEM = 'information_schema',switch(relkind,'r','SYSTEM TABLE','v','SYSTEM VIEW','i','SYSTEM INDEX',NULL),TABLE_SCHEM = 'pg_toast',switch(relkind,'r','SYSTEM TOAST TABLE','i','SYSTEM TOAST INDEX',NULL),switch(relkind,'r','TEMPORARY TABLE','p','TEMPORARY TABLE','i','TEMPORARY INDEX','S','TEMPORARY SEQUENCE','v','TEMPORARY VIEW',NULL)),false,switch(relkind,'r','TABLE','p','PARTITIONED TABLE','i','INDEX','S','SEQUENCE','v','VIEW','c','TYPE','f','FOREIGN TABLE','m','MATERIALIZED VIEW',NULL),NULL) TABLE_TYPE, REMARKS, '' TYPE_CAT, '' TYPE_SCHEM, '' TYPE_NAME, '' SELF_REFERENCING_COL_NAME, '' REF_GENERATION from (select-choose [n.nspname TABLE_SCHEM, c.relname TABLE_NAME, c.relkind relkind, d.description REMARKS] n.nspname TABLE_SCHEM, c.relname TABLE_NAME, c.relkind relkind, d.description REMARKS from (select [nspname, oid] from pg_catalog.pg_namespace() n join (select [relname, relkind, relnamespace, oid] from pg_catalog.pg_class() c where relname like 'quickstart-events2') c on c.relnamespace = n.oid post-join-where false or c.relkind = 'r' and n.nspname !~ '^pg_' and n.nspname != 'information_schema' left join select [description, objoid, objsubid, classoid] from pg_catalog.pg_description() d on d.objoid = c.oid outer-join-expression d.objsubid = 0 left join select [oid, relname, relnamespace] from pg_catalog.pg_class() dc on dc.oid = d.classoid outer-join-expression dc.relname = 'pg_class' left join select [oid, nspname] from pg_catalog.pg_namespace() dn on dn.oid = dc.relnamespace outer-join-expression dn.nspname = 'pg_catalog') n) n order by TABLE_TYPE, TABLE_SCHEM, TABLE_NAME",
                "SELECT \n" +
                        "     NULL AS TABLE_CAT, \n" +
                        "     n.nspname AS TABLE_SCHEM, \n" +
                        "     \n" +
                        "     c.relname AS TABLE_NAME,  \n" +
                        "     CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  \n" +
                        "        WHEN true THEN \n" +
                        "           CASE  \n" +
                        "                WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN \n" +
                        "                    CASE c.relkind   \n" +
                        "                        WHEN 'r' THEN 'SYSTEM TABLE' \n" +
                        "                        WHEN 'v' THEN 'SYSTEM VIEW'\n" +
                        "                        WHEN 'i' THEN 'SYSTEM INDEX'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END\n" +
                        "                WHEN n.nspname = 'pg_toast' THEN \n" +
                        "                    CASE c.relkind   \n" +
                        "                        WHEN 'r' THEN 'SYSTEM TOAST TABLE'\n" +
                        "                        WHEN 'i' THEN 'SYSTEM TOAST INDEX'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END\n" +
                        "                ELSE \n" +
                        "                    CASE c.relkind\n" +
                        "                        WHEN 'r' THEN 'TEMPORARY TABLE'\n" +
                        "                        WHEN 'p' THEN 'TEMPORARY TABLE'\n" +
                        "                        WHEN 'i' THEN 'TEMPORARY INDEX'\n" +
                        "                        WHEN 'S' THEN 'TEMPORARY SEQUENCE'\n" +
                        "                        WHEN 'v' THEN 'TEMPORARY VIEW'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END  \n" +
                        "            END  \n" +
                        "        WHEN false THEN \n" +
                        "            CASE c.relkind  \n" +
                        "                WHEN 'r' THEN 'TABLE'  \n" +
                        "                WHEN 'p' THEN 'PARTITIONED TABLE'  \n" +
                        "                WHEN 'i' THEN 'INDEX'  \n" +
                        "                WHEN 'S' THEN 'SEQUENCE'  \n" +
                        "                WHEN 'v' THEN 'VIEW'  \n" +
                        "                WHEN 'c' THEN 'TYPE'  \n" +
                        "                WHEN 'f' THEN 'FOREIGN TABLE'  \n" +
                        "                WHEN 'm' THEN 'MATERIALIZED VIEW'  \n" +
                        "                ELSE NULL  \n" +
                        "            END  \n" +
                        "        ELSE NULL  \n" +
                        "    END AS TABLE_TYPE, \n" +
                        "    d.description AS REMARKS,\n" +
                        "    '' as TYPE_CAT,\n" +
                        "    '' as TYPE_SCHEM,\n" +
                        "    '' as TYPE_NAME,\n" +
                        "    '' AS SELF_REFERENCING_COL_NAME,\n" +
                        "    '' AS REF_GENERATION\n" +
                        "FROM \n" +
                        "    pg_catalog.pg_namespace n, \n" +
                        "    pg_catalog.pg_class c  \n" +
                        "    LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) \n" +
                        "    LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')\n" +
                        "    LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')\n" +
                        "WHERE \n" +
                        "    c.relnamespace = n.oid  \n" +
                        "    AND c.relname LIKE 'quickstart-events2' \n" +
                        "    AND (\n" +
                        "        false  \n" +
                        "        OR  ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) \n" +
                        "        ) \n" +
                        "ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME"
        );
    }

    @Test
    public void testJoinColumnPropagation() throws SqlException {
        assertQuery(
                "select-group-by city, max(temp) max from (select-choose [city, readings.temp temp] city, readings.temp temp from (select [temp, sensorId] from readings timestamp (ts) join select [city, sensId] from (select-choose [city, ID sensId] ID sensId, city from (select [city, ID] from sensors)) _xQdbA1 on sensId = readings.sensorId))",
                "SELECT city, max(temp)\n" +
                        "FROM readings\n" +
                        "JOIN(\n" +
                        "    SELECT ID sensId, city\n" +
                        "    FROM sensors)\n" +
                        "ON readings.sensorId = sensId",
                modelOf("sensors")
                        .col("ID", ColumnType.LONG)
                        .col("make", ColumnType.STRING)
                        .col("city", ColumnType.STRING),
                modelOf("readings")
                        .col("ID", ColumnType.LONG)
                        .timestamp("ts")
                        .col("temp", ColumnType.DOUBLE)
                        .col("sensorId", ColumnType.LONG)
        );
    }

    @Test
    public void testJoinColumnResolutionOnSubQuery() throws SqlException {
        assertQuery(
                "select-group-by sum(timestamp) sum from (select-choose [_xQdbA1.timestamp timestamp] _xQdbA1.timestamp timestamp from (select [timestamp] from (select-choose [timestamp] ccy, timestamp from (select [timestamp] from y)) _xQdbA1 cross join (select-choose ccy from (select [ccy] from x)) _xQdbA2))",
                "select sum(timestamp) from (select * from y) cross join (x)",
                modelOf("x").col("ccy", ColumnType.SYMBOL),
                modelOf("y").col("ccy", ColumnType.SYMBOL).col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testJoinColumnResolutionOnSubQuery2() throws SqlException {
        assertQuery(
                "select-group-by sum(timestamp) sum from (select-choose [_xQdbA1.timestamp timestamp] _xQdbA1.timestamp timestamp from (select [timestamp, ccy, sym] from (select-choose [timestamp, ccy, sym] ccy, timestamp, sym from (select [timestamp, ccy, sym] from y)) _xQdbA1 join select [ccy, sym] from (select-choose [ccy, sym] ccy, sym from (select [ccy, sym] from x)) _xQdbA2 on _xQdbA2.ccy = _xQdbA1.ccy and _xQdbA2.sym = _xQdbA1.sym))",
                "select sum(timestamp) from (select * from y) join (select * from x) on (ccy, sym)",
                modelOf("x").col("ccy", ColumnType.SYMBOL).col("sym", ColumnType.INT),
                modelOf("y").col("ccy", ColumnType.SYMBOL).col("timestamp", ColumnType.TIMESTAMP).col("sym", ColumnType.INT)
        );
    }

    @Test
    public void testJoinColumnResolutionOnSubQuery3() throws SqlException {
        assertQuery(
                "select-group-by sum(timestamp) sum from (select-choose [_xQdbA1.timestamp timestamp] _xQdbA1.timestamp timestamp from (select [timestamp] from (select-choose [timestamp] ccy, timestamp from (select [timestamp] from y)) _xQdbA1 cross join x))",
                "select sum(timestamp) from (select * from y) cross join x",
                modelOf("x").col("ccy", ColumnType.SYMBOL),
                modelOf("y").col("ccy", ColumnType.SYMBOL).col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testJoinCycle() throws Exception {
        assertQuery(
                "select-choose" +
                        " orders.customerId customerId," +
                        " orders.orderId orderId," +
                        " customers.customerId customerId1," +
                        " d.orderId orderId1," +
                        " d.productId productId," +
                        " suppliers.supplier supplier," +
                        " products.productId productId1," +
                        " products.supplier supplier1 " +
                        "from (" +
                        "select [customerId, orderId] from orders" +
                        " join select [customerId] from customers on customers.customerId = orders.customerId" +
                        " join (select [orderId, productId] from orderDetails d where orderId = productId) d on d.productId = orders.orderId" +
                        " join select [supplier] from suppliers on suppliers.supplier = orders.orderId" +
                        " join select [productId, supplier] from products on products.productId = orders.orderId and products.supplier = suppliers.supplier" +
                        ")",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and orders.orderId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " where orders.orderId = suppliers.supplier",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testJoinCycle2() throws Exception {
        assertQuery(
                "select-choose orders.customerId customerId, orders.orderId orderId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, suppliers.x x, products.productId productId1, products.supplier supplier1 from (select [customerId, orderId] from orders join select [customerId] from customers on customers.customerId = orders.customerId join select [orderId, productId] from orderDetails d on d.productId = orders.orderId join select [supplier, x] from suppliers on suppliers.x = d.orderId and suppliers.supplier = orders.orderId join select [productId, supplier] from products on products.productId = orders.orderId and products.supplier = suppliers.supplier)",
                "orders" +
                        " join customers on orders.orderId = products.productId" +
                        " join orderDetails d on products.supplier = suppliers.supplier" +
                        " join suppliers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " where orders.orderId = suppliers.supplier and d.orderId = suppliers.x",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinDuplicateTables() throws Exception {
        assertSyntaxError(
                "select * from tab cross join tab",
                29,
                "Duplicate table or alias: tab",
                modelOf("tab").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testJoinFunction() throws SqlException {
        assertQuery(
                "select-choose tab.x x, t.y y, t1.z z from (select [x] from tab cross join select [y] from t post-join-where f(x) = f(y) cross join select [z] from t1 post-join-where z = f(x) const-where 1 = 1)",
                "select * from tab join t on f(x)=f(y) join t1 on 1=1 where z=f(x)",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("t").col("y", ColumnType.INT),
                modelOf("t1").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testJoinGroupBy() throws Exception {
        assertQuery(
                "select-group-by country, sum(quantity) sum from (select-choose [country, d.quantity quantity] country, d.quantity quantity from (select [customerId, orderId] from orders o join (select [country, customerId] from customers c where country ~ '^Z') c on c.customerId = o.customerId join select [quantity, orderId] from orderDetails d on d.orderId = o.orderId) o) o",
                "select country, sum(quantity) from orders o " +
                        "join customers c on c.customerId = o.customerId " +
                        "join orderDetails d on o.orderId = d.orderId" +
                        " where country ~ '^Z'",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT).col("country", ColumnType.SYMBOL),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("quantity", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinGroupByFilter() throws Exception {
        assertQuery(
                "select-choose country, sum from (select-group-by [country, sum(quantity) sum] country, sum(quantity) sum from (select-choose [country, o.quantity quantity] country, o.quantity quantity from (select [quantity, customerId, orderId] from orders o join (select [country, customerId] from customers c where country ~ '^Z') c on c.customerId = o.customerId join select [orderId] from orderDetails d on d.orderId = o.orderId) o) o where sum > 2)",
                "(select country, sum(quantity) sum " +
                        "from orders o " +
                        "join customers c on c.customerId = o.customerId " +
                        "join orderDetails d on o.orderId = d.orderId " +
                        "where country ~ '^Z') where sum > 2",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT).col("quantity", ColumnType.DOUBLE),
                modelOf("customers").col("customerId", ColumnType.INT).col("country", ColumnType.SYMBOL),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("comment", ColumnType.STRING)
        );
    }

    @Test
    public void testJoinImpliedCrosses() throws Exception {
        assertQuery(
                "select-choose orders.customerId customerId, orders.orderId orderId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [customerId, orderId] from orders cross join select [productId, supplier] from products join select [supplier] from suppliers on suppliers.supplier = products.supplier cross join select [customerId] from customers cross join select [orderId, productId] from orderDetails d const-where 1 = 1 and 2 = 2 and 3 = 3)",
                "orders" +
                        " join customers on 1=1" +
                        " join orderDetails d on 2=2" +
                        " join products on 3=3" +
                        " join suppliers on products.supplier = suppliers.supplier",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testJoinMultipleFields() throws Exception {
        assertQuery(
                "select-choose orders.customerId customerId, orders.orderId orderId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [customerId, orderId] from orders join select [customerId] from customers on customers.customerId = orders.customerId join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = customers.customerId and d.orderId = orders.orderId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier)",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testJoinOfJoin() throws SqlException {
        assertQuery(
                "select-choose tt.x x, tt.y y, tt.x1 x1, tt.z z, tab2.z z1, tab2.k k from (select [x, y, x1, z] from (select-choose [tab.x x, tab.y y, tab1.x x1, tab1.z z] tab.x x, tab.y y, tab1.x x1, tab1.z z from (select [x, y] from tab join select [x, z] from tab1 on tab1.x = tab.x)) tt join select [z, k] from tab2 on tab2.z = tt.z) tt",
                "select * from (select * from tab join tab1 on (x)) tt join tab2 on(z)",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT),
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("z", ColumnType.INT)
                        .col("k", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnCase() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a cross join b where switch(x,1,10,15)) a",
                "select a.x from a a join b on (CASE WHEN a.x = 1 THEN 10 ELSE 15 END)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnCaseDanglingThen() throws Exception {
        assertSyntaxError(
                "select a.x from a a join b on (CASE WHEN a.x THEN 10 10+4 ELSE 15 END)",
                53,
                "dangling expression",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnColumns() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y from (select [x, z] from tab1 a join select [y, z] from tab2 b on b.z = a.z) a",
                "select a.x, b.y from tab1 a join tab2 b on (z)",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnEqCondition() throws SqlException {
        assertQuery(
                "select-choose a.id id, b.id id1, b.c c, b.m m from (select [id] from a left join select [id, c, m] from b on b.id = a.id outer-join-expression c = 2 post-join-where m > 20)",
                "select * from a left join b on ( a.id=b.id and c = 2) where m > 20",
                modelOf("a").col("id", ColumnType.INT),
                modelOf("b").col("id", ColumnType.INT).col("c", ColumnType.INT).col("m", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnExpression() throws Exception {
        assertSyntaxError(
                "a join b on (x,x+1)",
                18,
                "Column name expected",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnExpression2() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.x x1 from (select [x] from a cross join (select [x] from b where x) b where x + 1)",
                "a join b on a.x+1 and b.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnOrCondition() throws SqlException {
        assertQuery(
                "select-choose a.id id, b.id id1, b.c c, b.m m from (select [id] from a left join select [id, c, m] from b on b.id = a.id outer-join-expression c = 2 or c = 10 post-join-where m > 20)",
                "select * from a left join b on (a.id=b.id and (c = 2 or c = 10)) where m > 20",
                modelOf("a").col("id", ColumnType.INT),
                modelOf("b").col("id", ColumnType.INT).col("c", ColumnType.INT).col("m", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnOtherCondition() throws SqlException {
        assertQuery(
                "select-choose a.id id, b.id id1, b.c c, b.m m from (select [id] from a left join select [id, c, m] from b on b.id = a.id outer-join-expression c > 0 post-join-where m > 20)",
                "select * from a left join b on ( a.id=b.id and c > 0) where m > 20",
                modelOf("a").col("id", ColumnType.INT),
                modelOf("b").col("id", ColumnType.INT).col("c", ColumnType.INT).col("m", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOneFieldToTwoAcross2() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, orders.customerId customerId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [orderId, customerId] from orders join select [customerId] from customers on customers.customerId = orders.orderId join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.orderId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier where customerId = orderId)",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = customers.customerId and orders.orderId = d.orderId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOneFieldToTwoReorder() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, orders.customerId customerId, d.orderId orderId1, d.productId productId, customers.customerId customerId1, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [orderId, customerId] from orders join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.customerId join select [customerId] from customers on customers.customerId = orders.customerId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier where orderId = customerId)",
                "orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOrder4() throws SqlException {
        assertQuery(
                "select-choose a.o o, b.id id, c.x x, d.y y, e.id id1 from (select [o] from a cross join select [id] from b asof join select [y] from d join select [id] from e on e.id = b.id cross join select [x] from c)",
                "a" +
                        " cross join b cross join c" +
                        " asof join d inner join e on b.id = e.id",
                modelOf("a").col("o", ColumnType.DOUBLE),
                modelOf("b").col("id", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT),
                modelOf("d").col("y", ColumnType.LONG),
                modelOf("e").col("id", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorder() throws Exception {
        assertQuery(
                "select-choose" +
                        " orders.orderId orderId," +
                        " customers.customerId customerId," +
                        " d.orderId orderId1," +
                        " d.productId productId," +
                        " products.productId productId1," +
                        " products.supplier supplier," +
                        " suppliers.supplier supplier1 " +
                        "from (" +
                        "select [orderId] from orders" +
                        " join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.orderId" +
                        " join select [customerId] from customers on customers.customerId = d.productId" +
                        " join select [productId, supplier] from products on products.productId = d.productId" +
                        " join select [supplier] from suppliers on suppliers.supplier = products.supplier" +
                        " const-where 1 = 1)",
                "orders" +
                        " join customers on 1=1" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorder3() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, customers.customerId customerId, shippers.shipper shipper, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1 " +
                        "from (select [orderId] from orders " +
                        "join select [shipper] from shippers on shippers.shipper = orders.orderId " +
                        "join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = shippers.shipper and d.orderId = orders.orderId " +
                        "join select [productId, supplier] from products on products.productId = d.productId " +
                        "join select [supplier] from suppliers on suppliers.supplier = products.supplier " +
                        "join select [customerId] from customers outer-join-expression 1 = 1)",
                "orders" +
                        " left join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorderRoot() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, orders.orderId orderId, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [customerId] from customers join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = customers.customerId join select [orderId] from orders on orders.orderId = d.orderId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier)",
                "customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",

                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorderRoot2() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, customers.customerId customerId, shippers.shipper shipper, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 " +
                        "from (select [orderId] from orders " +
                        "join select [shipper] from shippers on shippers.shipper = orders.orderId join " +
                        "(select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = shippers.shipper and d.orderId = orders.orderId " +
                        "join select [productId, supplier] from products on products.productId = d.productId " +
                        "join select [supplier] from suppliers on suppliers.supplier = products.supplier " +
                        "join select [customerId] from customers outer-join-expression 1 = 1)",
                "orders" +
                        " left join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testJoinSansSelect() throws Exception {
        assertQuery(
                "select-choose t1.ts ts, t1.x x, t2.ts ts1, t2.x x1 from (select [ts, x] from t1 join select [ts, x] from t2 on t2.x = t1.x)",
                "(t1 join t2 on x)",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                modelOf("t2").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinSubQuery() throws Exception {
        assertQuery(
                "select-choose" +
                        " orders.orderId orderId," +
                        " _xQdbA1.customerId customerId," +
                        " _xQdbA1.customerName customerName " +
                        "from (" +
                        "select [orderId] from orders" +
                        " join select [customerId, customerName] from (select-choose [customerId, customerName] customerId, customerName from (select [customerId, customerName] from customers where customerName ~ 'X')) _xQdbA1 on customerName = orderId)",
                "orders" +
                        " cross join (select customerId, customerName from customers where customerName ~ 'X')" +
                        " where orderId = customerName",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING)
        );
    }

    @Test
    public void testJoinSubQueryConstantWhere() throws Exception {
        assertQuery(
                "select-choose o.customerId customerId from (select [cid] from (select-choose [customerId cid] customerId cid from (select [customerId] from customers where 100 = customerId)) c left join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.cid const-where 10 = 9) c",
                "select o.customerId from (select customerId cid from customers) c" +
                        " left join orders o on c.cid = o.customerId" +
                        " where 100 = c.cid and 10=9",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testJoinSubQueryWherePosition() throws Exception {
        assertQuery(
                "select-choose o.customerId customerId from (select [cid] from (select-choose [customerId cid] customerId cid from (select [customerId] from customers where 100 = customerId)) c left join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.cid) c",
                "select o.customerId from (select customerId cid from customers) c" +
                        " left join orders o on c.cid = o.customerId" +
                        " where 100 = c.cid",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testJoinSyntaxError() throws Exception {
        assertSyntaxError(
                "select a.x from a a join b on (a + case when a.x = 1 then 10 else end)",
                66,
                "missing argument",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinTableMissing() throws Exception {
        assertSyntaxError(
                "select a from tab join",
                22,
                "table name or sub-query expected"
        );
    }

    @Test
    public void testJoinTimestampPropagation() throws SqlException {
        assertQuery(
                "select-choose ts_stop, id, ts_start, id1 from (select-choose [a.created ts_stop, a.id id, b.created ts_start, b.id id1] a.created ts_stop, a.id id, b.created ts_start, b.id id1 from (select [created, id] from (select-choose [created, id] id, created, event, timestamp from (select-choose [created, id] id, created, event, timestamp from (select [created, id, event] from telemetry_users timestamp (timestamp) where event = 101 and id != '0x05ab1e873d165b00000005743f2c17') order by created) timestamp (created)) a lt join select [created, id] from (select-choose [created, id] id, created, event, timestamp from (select-choose [created, id] id, created, event, timestamp from (select [created, id, event] from telemetry_users timestamp (timestamp) where event = 100) order by created) timestamp (created)) b on b.id = a.id post-join-where a.created - b.created > 10000000000) a)",
                "with \n" +
                        "    starts as ((telemetry_users where event = 100 order by created) timestamp(created)),\n" +
                        "    stops as ((telemetry_users where event = 101 order by created) timestamp(created))\n" +
                        "\n" +
                        "select * from (select a.created ts_stop, a.id, b.created ts_start, b.id from stops a lt join starts b on (id)) where id <> '0x05ab1e873d165b00000005743f2c17' and ts_stop - ts_start > 10000000000\n",
                modelOf("telemetry_users")
                        .col("id", ColumnType.LONG256)
                        .col("created", ColumnType.TIMESTAMP)
                        .col("event", ColumnType.SHORT)
                        .timestamp()
        );
    }

    @Test
    public void testJoinTimestampPropagationWhenTimestampNotSelected() throws SqlException {
        assertQuery(
                "select-distinct [id] id from (select-choose [id] id from (select-choose [a.id id] a.created ts_stop, a.id id, b.created ts_start, b.id id1 from " +
                        "(select [id, created] from (select-choose [id, created] id, created, event, timestamp from " +
                        "(select-choose [created, id] id, created, event, timestamp from " +
                        "(select [created, id, event] from telemetry_users timestamp (timestamp) " +
                        "where event = 101 " +
                        "and id != '0x05ab1e873d165b00000005743f2c17') " +
                        "order by created) timestamp (created)) a " +
                        "lt join select [id, created] from (select-choose [id, created] id, created, event, timestamp from (select-choose [created, id] id, created, event, timestamp " +
                        "from (select [created, id, event] from telemetry_users timestamp (timestamp) " +
                        "where event = 100) " +
                        "order by created) timestamp (created)) b on b.id = a.id " +
                        "post-join-where a.created - b.created > 10000000000) a))",
                "with \n" +
                        "    starts as ((telemetry_users where event = 100 order by created) timestamp(created)),\n" +
                        "    stops as ((telemetry_users where event = 101 order by created) timestamp(created))\n" +
                        "\n" +
                        "select distinct id from (select a.created ts_stop, a.id, b.created ts_start, b.id " +
                        "from stops a " +
                        "lt join starts b on (id)) " +
                        "where id <> '0x05ab1e873d165b00000005743f2c17' " +
                        "and ts_stop - ts_start > 10000000000\n",
                modelOf("telemetry_users")
                        .col("id", ColumnType.LONG256)
                        .col("created", ColumnType.TIMESTAMP)
                        .col("event", ColumnType.SHORT)
                        .timestamp()
        );
    }

    @Test
    public void testJoinTriangle() throws Exception {
        assertQuery(
                "select-choose o.a a, o.b b, o.c c, c.c c1, c.d d, c.e e, d.b b1, d.d d1, d.quantity quantity from (select [a, b, c] from orders o join select [c, d, e] from customers c on c.c = o.c join select [b, d, quantity] from orderDetails d on d.d = c.d and d.b = o.b) o",
                "orders o" +
                        " join customers c on(c)" +
                        " join orderDetails d on o.b = d.b and c.d = d.d",

                modelOf("orders")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.LONG),
                modelOf("customers")
                        .col("c", ColumnType.LONG)
                        .col("d", ColumnType.INT)
                        .col("e", ColumnType.INT),
                modelOf("orderDetails")
                        .col("b", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("quantity", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinWith() throws SqlException {
        assertQuery(
                "select-choose x.y y, x1.y y1, x2.y y2 from (select [y] from (select-choose [y] y from (select [y] from tab)) x cross join select [y] from (select-choose [y] y from (select [y] from tab)) x1 cross join select [y] from (select-choose [y] y from (select [y] from tab)) x2) x",
                "with x as (select * from tab) select * from x cross join x x1 cross join x x2",
                modelOf("tab").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testJoinWithClausesDefaultAlias() throws SqlException {
        assertQuery(
                "select-choose cust.customerId customerId, cust.name name, ord.customerId customerId1 from (select [customerId, name] from (select-choose [customerId, name] customerId, name from (select [customerId, name] from customers where name ~ 'X')) cust left join select [customerId] from (select-choose [customerId] customerId from (select [customerId, amount] from orders where amount > 100)) ord on ord.customerId = cust.customerId post-join-where ord.customerId != null) cust limit 10",
                "with" +
                        " cust as (customers where name ~ 'X')," +
                        " ord as (select customerId from orders where amount > 100)" +
                        " select * from cust left join ord on (customerId) " +
                        " where ord.customerId != null" +
                        " limit 10",
                modelOf("customers").col("customerId", ColumnType.INT).col("name", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("amount", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinWithClausesExplicitAlias() throws SqlException {
        assertQuery(
                "select-choose c.customerId customerId, c.name name, o.customerId customerId1 from (select [customerId, name] from (select-choose [customerId, name] customerId, name from (select [customerId, name] from customers where name ~ 'X')) c left join select [customerId] from (select-choose [customerId] customerId from (select [customerId, amount] from orders where amount > 100)) o on o.customerId = c.customerId post-join-where o.customerId != null) c limit 10",
                "with" +
                        " cust as (customers where name ~ 'X')," +
                        " ord as (select customerId from orders where amount > 100)" +
                        " select * from cust c left join ord o on (customerId) " +
                        " where o.customerId != null" +
                        " limit 10",
                modelOf("customers").col("customerId", ColumnType.INT).col("name", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("amount", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinWithFilter() throws Exception {
        assertQuery(
                "select-choose" +
                        " customers.customerId customerId," +
                        " orders.orderId orderId," +
                        " d.orderId orderId1," +
                        " d.productId productId," +
                        " d.quantity quantity," +
                        " products.productId productId1," +
                        " products.supplier supplier," +
                        " products.price price," +
                        " suppliers.supplier supplier1" +
                        " from (" +
                        "select [customerId] from customers" +
                        " join (select [orderId, productId, quantity] from orderDetails d where productId = orderId) d on d.productId = customers.customerId" +
                        " join select [orderId] from orders on orders.orderId = d.orderId post-join-where d.quantity < orders.orderId" +
                        " join select [productId, supplier, price] from products on products.productId = d.productId post-join-where products.price > d.quantity or d.orderId = orders.orderId" +
                        " join select [supplier] from suppliers on suppliers.supplier = products.supplier" +
                        ")",
                "customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId" +
                        " and (products.price > d.quantity or d.orderId = orders.orderId) and d.quantity < orders.orderId",

                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails")
                        .col("orderId", ColumnType.INT)
                        .col("productId", ColumnType.INT)
                        .col("quantity", ColumnType.DOUBLE),
                modelOf("products").col("productId", ColumnType.INT)
                        .col("supplier", ColumnType.INT)
                        .col("price", ColumnType.DOUBLE),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinWithFunction() throws SqlException {
        assertQuery(
                "select-choose x1.a a, x1.s s, x2.a a1, x2.s s1 from (select [a, s] from (select-virtual [rnd_int() a, rnd_symbol(4,4,4,2) s] rnd_int() a, rnd_symbol(4,4,4,2) s from (long_sequence(10))) x1 join select [a, s] from (select-virtual [rnd_int() a, rnd_symbol(4,4,4,2) s] rnd_int() a, rnd_symbol(4,4,4,2) s from (long_sequence(10))) x2 on x2.s = x1.s) x1",
                "with x as (select rnd_int() a, rnd_symbol(4,4,4,2) s from long_sequence(10)) " +
                        "select * from x x1 join x x2 on (s)"
        );
    }

    @Test
    public void testLatestByDeprecatedKeepWhereOutside() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from x latest by b where b = 'PEHN' and a < 22 and test_match())",
                "select * from x latest by b where b = 'PEHN' and a < 22 and test_match()",
                modelOf("x").col("a", ColumnType.INT).col("b", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByDeprecatedMultipleColumns() throws SqlException {
        assertQuery(
                "select-group-by ts, market_type, avg(bid_price) avg from (select [ts, market_type, bid_price] from market_updates timestamp (ts) latest by ts,market_type) sample by 1s",
                "select ts, market_type, avg(bid_price) FROM market_updates LATEST BY ts, market_type SAMPLE BY 1s",
                modelOf("market_updates").timestamp("ts").col("market_type", ColumnType.SYMBOL).col("bid_price", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testLatestByDeprecatedNonSelectedColumn() throws Exception {
        assertQuery(
                "select-choose x, y from (select-choose [x, y] x, y from (select [y, x, z] from tab t2 latest by z where x > 100) t2 where y > 0) t1",
                "select x, y from (select x, y from tab t2 latest by z where x > 100) t1 where y > 0",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByDeprecatedSyntax() throws Exception {
        assertSyntaxError(
                "select * from tab latest by x, ",
                30,
                "literal expected"
        );
    }

    @Test
    public void testLatestByDeprecatedSyntax2() throws Exception {
        assertSyntaxError(
                "select * from tab latest by",
                27,
                "literal expected"
        );
    }

    @Test
    public void testLatestByDeprecatedSyntax3() throws Exception {
        assertSyntaxError(
                "select * from tab latest by x+1",
                29,
                "unexpected token [+]"
        );
    }

    @Test
    public void testLatestByDeprecatedWithOuterFilter() throws SqlException {
        assertQuery(
                "select-choose time, uuid from (select-choose [time, uuid] time, uuid from (select [uuid, time] from positions timestamp (time) latest by uuid where time < '2021-05-11T14:00') where uuid = '006cb7c6-e0d5-3fea-87f2-83cf4a75bc28')",
                "(positions latest by uuid where time < '2021-05-11T14:00') where uuid = '006cb7c6-e0d5-3fea-87f2-83cf4a75bc28'",
                modelOf("positions").timestamp("time").col("uuid", ColumnType.SYMBOL)

        );
    }

    @Test
    public void testLatestByMultipleColumns() throws SqlException {
        assertQuery(
                "select-group-by ts, market_type, avg(bid_price) avg from (select [ts, market_type, bid_price, country] from market_updates latest on ts partition by market_type,country) sample by 1s",
                "select ts, market_type, avg(bid_price) FROM market_updates latest on ts partition by market_type, country sample by 1s",
                modelOf("market_updates").timestamp("ts").col("market_type", ColumnType.SYMBOL).col("country", ColumnType.SYMBOL).col("bid_price", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testLatestByNoWhere() throws SqlException {
        assertQuery(
                "select-choose ts, a from (select [ts, a] from x latest on ts partition by a)",
                "select * from x latest on ts partition by a",
                modelOf("x").timestamp("ts").col("a", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByNonSelectedColumn() throws Exception {
        assertQuery(
                "select-choose x, y from (select-choose [x, y] x, y from (select [y, x, z] from tab t2 where x > 100 latest on ts partition by z) t2 where y > 0) t1",
                "select x, y from (select x, y from tab t2 where x > 100 latest on ts partition by z) t1 where y > 0",
                modelOf("tab").timestamp("ts").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByOnDesignatedTimestampWithSubQuery() throws SqlException {
        assertQuery(
                "select-choose ts, x, y, z from (select [ts, x, y, z] from (select-choose [ts, x, y, z] ts, x, y, z from (select [ts, x, y, z] from tab timestamp (ts)) order by ts) _xQdbA1 latest on ts partition by z)",
                "select * from (tab order by ts) latest on ts partition by z",
                modelOf("tab").timestamp("ts").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByOnNonDesignatedTimestampWithSubQuery() throws SqlException {
        assertQuery(
                "select-choose ts, another_ts, x, y, z from (select [ts, another_ts, x, y, z] from (select-choose [another_ts, ts, x, y, z] ts, another_ts, x, y, z from (select [another_ts, ts, x, y, z] from tab timestamp (ts)) order by another_ts) _xQdbA1 latest on another_ts partition by z)",
                "(tab order by another_ts) latest on another_ts partition by z",
                modelOf("tab").timestamp("ts").col("another_ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestBySansSelect() throws Exception {
        assertQuery(
                "select-choose ts, x from (select-choose [ts, x] ts, x from (select [ts, x] from t1 latest by x))",
                "(t1 latest by x)",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testLatestBySyntax() throws Exception {
        assertSyntaxError(
                "select * from tab latest",
                18,
                "'by' expected"
        );
    }

    @Test
    public void testLatestBySyntax2() throws Exception {
        assertSyntaxError(
                "select * from tab latest on ts, ",
                30,
                "'partition' expected"
        );
    }

    @Test
    public void testLatestBySyntax3() throws Exception {
        assertSyntaxError(
                "select * from tab latest on",
                27,
                "literal expected"
        );
    }

    @Test
    public void testLatestBySyntax4() throws Exception {
        assertSyntaxError(
                "select * from tab latest on ts+1",
                30,
                "'partition' expected"
        );
    }

    @Test
    public void testLatestBySyntax5() throws Exception {
        assertSyntaxError(
                "select * from tab latest on ts partition",
                40,
                "'by' expected"
        );
    }

    @Test
    public void testLatestBySyntax6() throws Exception {
        assertSyntaxError(
                "select * from tab latest on ts partition by",
                43,
                "literal expected"
        );
    }

    @Test
    public void testLatestBySyntax7() throws Exception {
        assertSyntaxError(
                "select * from tab latest on ts partition by x,",
                46,
                "literal expected"
        );
    }

    @Test
    public void testLatestBySyntax8() throws Exception {
        assertSyntaxError(
                "select * from tab latest on ts partition by x+1",
                45,
                "unexpected token [+]"
        );
    }

    @Test
    public void testLatestBySyntax9() throws Exception {
        assertSyntaxError(
                "select * from tab latest on ts partition by x where y > 0",
                46,
                "unexpected where clause after 'latest on'"
        );
    }

    @Test
    public void testLatestByWhereInside() throws SqlException {
        assertQuery(
                "select-choose ts, a, b from (select [ts, a, b] from x where b = 'PEHN' and a < 22 and test_match() latest on ts partition by b)",
                "select * from x where b = 'PEHN' and a < 22 and test_match() latest on ts partition by b",
                modelOf("x").timestamp("ts").col("a", ColumnType.INT).col("b", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByWithFilterAndSubQuery() throws SqlException {
        assertQuery(
                "select-choose x, ts from (select [x, ts, z] from (select-choose [ts, x, z] ts, x, y, z from (select [ts, x, z] from tab timestamp (ts) where x > 0) order by ts) _xQdbA1 latest on ts partition by z)",
                "select x, ts from (tab order by ts) where x > 0 latest on ts partition by z",
                modelOf("tab").timestamp("ts").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByWithOuterFilter() throws SqlException {
        assertQuery(
                "select-choose time, uuid from (select-choose [time, uuid] time, uuid from (select [uuid, time] from positions where time < '2021-05-11T14:00' latest on time partition by uuid) where uuid = '006cb7c6-e0d5-3fea-87f2-83cf4a75bc28')",
                "(positions where time < '2021-05-11T14:00' latest on time partition by uuid) where uuid = '006cb7c6-e0d5-3fea-87f2-83cf4a75bc28'",
                modelOf("positions").timestamp("time").col("uuid", ColumnType.SYMBOL)

        );
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a left join select [x] from b on b.x = a.x) a",
                "select a.x from a a left join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testLexerReset() {
        for (int i = 0; i < 10; i++) {
            try {
                select(
                        "select \n" +
                                "-- ltod(Date)\n" +
                                "count() \n" +
                                "-- from acc\n" +
                                "from acc(Date) sample by 1d\n" +
                                "-- where x = 10\n"
                );
                Assert.fail();
            } catch (SqlException e) {
                // we now allow column reference from SQL although column access will fail
                TestUtils.assertEquals("unknown function name: acc(LONG)", e.getFlyweightMessage());
            }
        }
    }

    @Test
    public void testLimitSansSelect() throws SqlException {
        assertQuery(
                "select-choose ts, x from (select-choose [ts, x] ts, x from (select [ts, x] from t1) limit 5)",
                "(t1 limit 5)",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testLineCommentAtEnd() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) 'b a' where x > 1\n--this is comment",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT)
        );
    }

    @Test
    public void testLineCommentAtMiddle() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) \n" +
                        " -- this is a comment \n" +
                        "'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT)
        );
    }

    @Test
    public void testLineCommentAtStart() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "-- hello, this is a comment\n (x where a > 1) 'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT)
        );
    }

    @Test
    public void testLiteralIsNotNull() throws Exception {
        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x where x != null)",
                "tab1 join tab2 on tab1.x = tab2.y where tab1.x is not null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testLiteralIsNull() throws Exception {
        assertQuery(
                "select-choose tab1.ts ts, tab1.x x, tab2.y y from (select [ts, x] from tab1 timestamp (ts) join select [y] from tab2 on tab2.y = tab1.x where x = null)",
                "tab1 join tab2 on tab1.x = tab2.y where tab1.x is null",
                modelOf("tab1").timestamp("ts").col("x", ColumnType.INT),
                modelOf("tab2").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testMissingArgument() throws Exception {
        assertSyntaxError(
                "select x from tab where not (x != 1 and)",
                36,
                "too few arguments for 'and' [found=1,expected=2]",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testMissingTable() throws Exception {
        assertSyntaxError(
                "select a from",
                13,
                "table name or sub-query expected"
        );
    }

    @Test
    public void testMissingTableInSubQuery() throws Exception {
        assertSyntaxError(
                "with x as (select a from) x",
                25,
                "table name or sub-query expected",
                modelOf("tab").col("b", ColumnType.INT)
        );
    }

    @Test
    public void testMissingWhere() {
        try {
            select("select id, x + 10, x from tab id ~ 'HBRO'");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(33, e.getPosition());
        }
    }

    @Test
    public void testMixedFieldsSubQuery() throws Exception {
        assertQuery(
                "select-choose x, y from (select-virtual [x, z + x y] x, z + x y from (select [x, z] from tab t2 where x > 100 latest on ts partition by x) t2 where y > 0) t1",
                "select x, y from (select x,z + x y from tab t2 where x > 100 latest on ts partition by x) t1 where y > 0",
                modelOf("tab").timestamp("ts").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testMostRecentWhereClause() throws Exception {
        assertQuery(
                "select-virtual x, sum + 25 ohoh from (select-group-by [x, sum(z) sum] x, sum(z) sum from (select-virtual [a + b * c x, z] a + b * c x, z from (select [a, c, b, z, x, y] from zyzy where a in (x,y) and b = 10 latest on ts partition by x)))",
                "select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 10 latest on ts partition by x",
                modelOf("zyzy")
                        .timestamp("ts")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testMoveOrderByFlat() throws Exception {
        assertQuery(
                "select-choose transaction_id from (select-virtual [cast(cast(transactionid,varchar),bigint) transaction_id, pg_catalog.age(transactionid1) age] cast(cast(transactionid,varchar),bigint) transaction_id, pg_catalog.age(transactionid1) age from (select-choose [transactionid, transactionid transactionid1] transactionid, transactionid transactionid1 from (select [transactionid] from pg_catalog.pg_locks() L where transactionid != null) L) L order by age desc limit 1)",
                "select L.transactionid::varchar::bigint as transaction_id\n" +
                        "from pg_catalog.pg_locks L\n" +
                        "where L.transactionid is not null\n" +
                        "order by pg_catalog.age(L.transactionid) desc\n" +
                        "limit 1"
        );
    }

    @Test
    public void testMoveOrderByFlatInUnion() throws Exception {
        assertQuery(
                // "age" column should not be included in the final selection list
                // we also expect limit to be moved to the outer query
                "select-choose locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart from (select-virtual [locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart, pg_catalog.age(transactionid) age] locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart, pg_catalog.age(transactionid) age from (select-choose [locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart] locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart from (select [locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart] from pg_catalog.pg_locks() l1) l1 union all select-choose [locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart] locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart from (select [locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart] from pg_catalog.pg_locks() L where transactionid != null) L) order by age desc limit 1)",
                "select * from pg_catalog.pg_locks l1 " +
                        "union all " +
                        "select *\n" +
                        "from pg_catalog.pg_locks L\n" +
                        "where L.transactionid is not null\n" +
                        "order by pg_catalog.age(transactionid) desc\n" +
                        "limit 1"
        );
    }

    @Test
    public void testMoveOrderByFlatWildcard() throws Exception {
        assertQuery(
                // "age" column should not be included in the final selection list
                "select-choose" +
                        " locktype," +
                        " database," +
                        " relation," +
                        " page," +
                        " tuple," +
                        " virtualxid," +
                        " transactionid," +
                        " classid," +
                        " objid," +
                        " objsubid," +
                        " virtualtransaction," +
                        " pid," +
                        " mode," +
                        " granted," +
                        " fastpath," +
                        " waitstart " +
                        "from (" +
                        "select-virtual" +
                        " [locktype," +
                        " database," +
                        " relation," +
                        " page," +
                        " tuple," +
                        " virtualxid," +
                        " transactionid," +
                        " classid," +
                        " objid," +
                        " objsubid," +
                        " virtualtransaction," +
                        " pid," +
                        " mode," +
                        " granted," +
                        " fastpath," +
                        " waitstart," +
                        " pg_catalog.age(transactionid1) age]" +
                        " locktype," +
                        " database," +
                        " relation," +
                        " page," +
                        " tuple," +
                        " virtualxid," +
                        " transactionid," +
                        " classid," +
                        " objid," +
                        " objsubid," +
                        " virtualtransaction," +
                        " pid," +
                        " mode," +
                        " granted," +
                        " fastpath," +
                        " waitstart," +
                        " pg_catalog.age(transactionid1) age " +
                        "from (select-choose [locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart, transactionid transactionid1] locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart, transactionid transactionid1 from (select [locktype, database, relation, page, tuple, virtualxid, transactionid, classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath, waitstart] from pg_catalog.pg_locks() L where transactionid != null) L) L" +
                        " order by age desc limit 1" +
                        ")",
                "select *\n" +
                        "from pg_catalog.pg_locks L\n" +
                        "where L.transactionid is not null\n" +
                        "order by pg_catalog.age(L.transactionid) desc\n" +
                        "limit 1"
        );
    }

    @Test
    public void testMoveOrderBySubQuery() throws Exception {
        assertQuery(
                "select-virtual transaction_id + 1 column from (select-choose [transaction_id] transaction_id from (select-virtual [cast(cast(transactionid,varchar),bigint) transaction_id, pg_catalog.age(transactionid1) age] cast(cast(transactionid,varchar),bigint) transaction_id, pg_catalog.age(transactionid1) age from (select-choose [transactionid, transactionid transactionid1] transactionid, transactionid transactionid1 from (select [transactionid] from pg_catalog.pg_locks() L where transactionid != null) L) L order by age desc limit 1))",
                "select transaction_id + 1 from (select L.transactionid::varchar::bigint as transaction_id\n" +
                        "from pg_catalog.pg_locks L\n" +
                        "where L.transactionid is not null\n" +
                        "order by pg_catalog.age(L.transactionid) desc\n" +
                        "limit 1)"
        );
    }

    @Test
    public void testMultipleExpressions() throws Exception {
        assertQuery(
                "select-virtual x, sum + 25 ohoh from (select-group-by [a + b * c x, sum(z) sum] a + b * c x, sum(z) sum from (select [a, c, b, z] from zyzy))",
                "select a+b*c x, sum(z)+25 ohoh from zyzy",
                modelOf("zyzy")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testNestedCast() throws SqlException {
        assertQuery(
                "select-virtual cast(cast(1 + x / 2,int),timestamp) ts from (select [x] from long_sequence(1000))",
                "select cast(cast((1 + x) / 2 as int) as timestamp) ts from long_sequence(1000)",
                modelOf("ts").col("ts", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testNestedJoinReorder() throws Exception {
        assertQuery(
                "select-choose x.orderId orderId, x.productId productId, y.orderId orderId1, y.customerId customerId, y.customerId1 customerId1, y.orderId1 orderId11, y.productId productId1, y.supplier supplier, y.productId1 productId11, y.supplier1 supplier1 from (select [orderId, productId] from (select-choose [orders.orderId orderId, products.productId productId] orders.orderId orderId, products.productId productId from (select [orderId, customerId] from orders join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.customerId join select [customerId] from customers on customers.customerId = orders.customerId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier where orderId = customerId)) x cross join select [orderId, customerId, customerId1, orderId1, productId, supplier, productId1, supplier1] from (select-choose [orders.orderId orderId, orders.customerId customerId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1] orders.orderId orderId, orders.customerId customerId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1 from (select [orderId, customerId] from orders join select [customerId] from customers on customers.customerId = orders.customerId join (select [orderId, productId] from orderDetails d where orderId = productId) d on d.productId = orders.orderId join select [supplier] from suppliers on suppliers.supplier = orders.orderId join select [productId, supplier] from products on products.productId = orders.orderId and products.supplier = suppliers.supplier)) y) x",
                "with x as (select orders.orderId, products.productId from " +
                        "orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId), " +
                        " y as (" +
                        "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and orders.orderId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " where orders.orderId = suppliers.supplier)" +
                        " select * from x cross join y",
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testNonAggFunctionWithAggFunctionSampleBy() throws SqlException {
        assertQuery(
                "select-group-by day, isin, last(start_price) last from (select-virtual [day(ts) day, isin, start_price] day(ts) day, isin, start_price, ts from (select [ts, isin, start_price] from xetra timestamp (ts) where isin = 'DE000A0KRJS4')) sample by 1d",
                "select day(ts), isin, last(start_price) from xetra where isin='DE000A0KRJS4' sample by 1d",
                modelOf("xetra")
                        .timestamp("ts")
                        .col("isin", ColumnType.SYMBOL)
                        .col("start_price", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testNonWindowFunctionInWindowContext() throws Exception {
        assertException(
                "select max(price) over (partition by symbol) from trades",
                "create table trades " +
                        "(" +
                        " price double," +
                        " symbol symbol," +
                        " ts timestamp" +
                        ") timestamp(ts) partition by day",
                7,
                "non-window function called in window context"
        );
    }

    @Test
    public void testNoopGroupBy() throws SqlException {
        assertQuery(
                "select-group-by sym, avg(bid) avgBid from (select [sym, bid] from x timestamp (ts) where sym in ('AA','BB'))",
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by sym",
                modelOf("x")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.INT)
                        .col("ask", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testNoopGroupByAfterFrom() throws Exception {
        assertQuery(
                "select-group-by sym, avg(bid) avgBid from (select [sym, bid] from x timestamp (ts))",
                "select sym, avg(bid) avgBid from x group by sym",
                modelOf("x")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.INT)
                        .col("ask", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testNoopGroupByFailureWhenMissingColumn() throws Exception {
        assertException(
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by ",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                71,
                "literal expected"
        );
    }

    @Test
    public void testNotMoveWhereIntoDistinct() throws SqlException {
        assertQuery(
                "select-choose a from (select-distinct [a] a from (select-choose [a] a from (select [a] from tab)) where a = 10)",
                "(select distinct a from tab) where a = 10",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testNullChecks() throws SqlException {
        assertQuery(
                "select-choose a from (select [a, time] from x timestamp (time) where time in ('2020-08-01T17:00:00.305314Z','2020-09-20T17:00:00.312334Z'))",
                "SELECT \n" +
                        "a\n" +
                        "FROM x WHERE b = 'H' AND time in('2020-08-01T17:00:00.305314Z' , '2020-09-20T17:00:00.312334Z')\n" +
                        "select * from long_sequence(1)",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.SYMBOL)
                        .timestamp("time")
        );
    }

    @Test
    public void testOfOrderByOnMultipleColumnsWhenColumnIsMissing() throws Exception {
        assertQuery(
                "select-choose z from (select-choose [y z, x] y z, x from (select [y, x] from tab) order by z desc, x)",
                "select y z  from tab order by z desc, x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testOneWindowColumn() throws Exception {
        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts) from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testOneWindowColumnAndLimit() throws Exception {
        assertQuery(
                "select-window a, b, f(c) f over (partition by b order by ts) " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts))) limit 200",
                "select a,b, f(c) over (partition by b order by ts) from xyz limit 200",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testOneWindowColumnPrefixed() throws Exception {
        // extra model in the middle is because we reference "b" as both "b" and "z.b"
        assertQuery(
                "select-window a, b, row_number() row_number over (partition by b1 order by ts) from (select-choose [a, b, b b1, ts] a, b, b b1, ts from (select [a, b, ts] from xyz z timestamp (ts)) z) z",
                "select a,b, row_number() over (partition by z.b order by z.ts) from xyz z",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testOptimiseNotAnd() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b or b != a)",
                "select a, b from tab where not (a = b and b = a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b)",
                "select a, b from tab where not (a = b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotGreater() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a <= b)",
                "select a, b from tab where not (a > b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotGreaterOrEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a < b)",
                "select a, b from tab where not (a >= b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotLess() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a >= b)",
                "select a, b from tab where not (a < b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotLessOrEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a > b)",
                "select a, b from tab where not (a <= b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotLiteral() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where not(a))",
                "select a, b from tab where not (a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotLiteralOr() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where not(a) and b != a)",
                "select a, b from tab where not (a or b = a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotNotEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a = b)",
                "select a, b from tab where not (a != b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotNotNotEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b)",
                "select a, b from tab where not(not (a != b))",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotOr() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b and b != a)",
                "select a, b from tab where not (a = b or b = a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptimiseNotOrLiterals() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where not(a) and not(b))",
                "select a, b from tab where not (a or b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOptionalSelect() throws Exception {
        assertQuery(
                "select-choose ts, x from (select [ts, x] from tab t2 where x > 100 latest on ts partition by x) t2",
                "tab t2 where x > 100 latest on ts partition by x",
                modelOf("tab").timestamp("ts").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testOrderBy1() throws Exception {
        assertQuery(
                "select-choose x, y from (select-choose [x, y, z] x, y, z from (select [x, y, z] from tab) order by x, y, z)",
                "select x,y from tab order by x,y,z",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByAmbiguousColumn() throws Exception {
        assertSyntaxError(
                "select tab1.x from tab1 join tab2 on (x) order by y",
                50,
                "Ambiguous",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByGroupByCol() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab) order by b",
                "select a, sum(b) b from tab order by b",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByGroupByCol2() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab) order by a",
                "select a, sum(b) b from tab order by a",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByGroupByColPrefixed() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab) order by a",
                "select a, sum(b) b from tab order by tab.a",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByGroupByColWithAliasDifferentToColumnName() throws SqlException {
        assertQuery(
                "select-group-by a, max(b) x from (select [a, b] from tab) order by x",
                "select a, max(b) x from tab order by x",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByIssue1() throws SqlException {
        assertQuery(
                "select-virtual to_date(timestamp) t from (select [timestamp] from blocks.csv) order by t",
                "select to_date(timestamp) t from 'blocks.csv' order by t",
                modelOf("blocks.csv").col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testOrderByOnAliasedColumn() throws SqlException {
        assertQuery(
                "select-choose y from (select [y] from tab) order by y",
                "select y from tab order by tab.y",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnExpression() throws SqlException {
        assertQuery(
                "select-virtual y + x z from (select [x, y] from tab) order by z",
                "select y+x z from tab order by z",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinSubQuery() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y, b.s s from (select [x, z] from (select-choose [x, z] x, z from (select [x, z] from tab1 where x = 'Z')) a join select [y, s, z] from (select-choose [y, s, z] x, y, z, s from (select [y, s, z] from tab2 where s ~ 'K')) b on b.z = a.z) a order by s",
                "select a.x, b.y, b.s from (select x,z from tab1 where x = 'Z' order by x) a join (tab2 where s ~ 'K') b on a.z=b.z order by b.s",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinSubQuery2() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y from (select [x, z] from (select-choose [x, z] x, z from (select-choose [x, z, p] x, z, p from (select [x, z, p] from tab1 where x = 'Z') order by p)) a join select [y, z] from (select-choose [y, z] x, y, z, s from (select [y, z, s] from tab2 where s ~ 'K')) b on b.z = a.z) a",
                "select a.x, b.y from (select x,z from tab1 where x = 'Z' order by p) a join (tab2 where s ~ 'K') b on a.z=b.z",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("p", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinSubQuery3() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y from " +
                        "(select [x] from " +
                        "(select-choose [x, z] x, z from " +
                        "(select [x, z] from tab1 where x = 'Z') order by z) a " +
                        "asof join select [y, z] from " +
                        "(select-choose [y, z, s] y, z, s from (select [y, z, s] from tab2 where s ~ 'K') order by s) b " +
                        "post-join-where a.x = b.z) a",
                //select-choose a.x x, b.y y from (select [x] from (select-choose [x, z] x, z from (select [x, z] from tab1 where x = 'Z') order by z) a asof join select [y, z] from (select-choose [y, s] y, z, s from (select [y, s] from tab2 where s ~ 'K') order by s) b post-join-where a.x = b.z) a
                "select a.x, b.y from (select x,z from tab1 where x = 'Z' order by z) a " +
                        "asof join (select y,z,s from tab2 where s ~ 'K' order by s) b where a.x = b.z",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinTableReference() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y, b.s s from (select [x, z] from tab1 a join select [y, s, z] from tab2 b on b.z = a.z) a order by s",
                "select a.x, b.y, b.s from tab1 a join tab2 b on a.z = b.z order by b.s",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnMultipleColumns() throws SqlException {
        assertQuery(
                "select-choose y z, x from (select [y, x] from tab) order by z desc, x",
                "select y z, x from tab order by z desc, x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnNonSelectedColumn() throws Exception {
        assertQuery(
                "select-choose y from (select-choose [y, x] y, x from (select [y, x] from tab) order by x)",
                "select y from tab order by x",
                modelOf("tab")
                        .col("y", ColumnType.DOUBLE)
                        .col("x", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testOrderByOnNonSelectedColumn2() throws Exception {
        assertQuery(
                "select-virtual 2 * y + x column, 3 / x xx from (select [x, y] from tab) order by xx",
                "select 2*y+x, 3/x xx from tab order by xx",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnOuterResult() throws SqlException {
        assertQuery(
                "select-virtual x, sum1 + sum z from (select-group-by [x, sum(3 / x) sum, sum(2 * y + x) sum1] x, sum(3 / x) sum, sum(2 * y + x) sum1 from (select [x, y] from tab)) order by z",
                "select x, sum(2*y+x) + sum(3/x) z from tab order by z asc",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnSelectedAlias() throws SqlException {
        assertQuery(
                "select-choose y z from (select [y] from tab) order by z",
                "select y z from tab order by z",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testOrderByPosition() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y] from tab) order by y, x",
                "select x,y from tab order by 2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionColumnAliases() throws Exception {
        assertQuery(
                "select-choose x c1, y c2 from (select [x, y] from tab) order by c2, c1",
                "select x c1, y c2 from tab order by 2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionCorrupt() throws Exception {
        assertSyntaxError(
                "tab order by 3a, 1",
                13,
                "Invalid column: 3a",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionNoSelect() throws Exception {
        assertQuery(
                "select-choose x, y, z from (select [x, y, z] from tab) order by z desc, x",
                "tab order by 3 desc,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionOutOfRange1() throws Exception {
        assertSyntaxError(
                "tab order by 0, 1",
                13,
                "order column position is out of range",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)

        );
    }

    @Test
    public void testOrderByPositionOutOfRange2() throws Exception {
        assertSyntaxError(
                "tab order by 2, 4",
                16,
                "order column position is out of range",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)

        );
    }

    @Test
    public void testOrderByPositionWithAggregateColumn() throws Exception {
        assertQuery(
                "select-group-by x, y, count() count from (select [x, y] from tab) order by count, y, x",
                "select x, y, count() from tab order by 3,2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionWithColumnAliasesAndAggregateColumn() throws Exception {
        assertQuery(
                "select-group-by c1, c2, count() c3 from (select-choose [x c1, y c2] x c1, y c2 from (select [x, y] from tab)) order by c3, c2, c1",
                "select x c1, y c2, count() c3 from tab order by 3,2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionWithColumnAliasesAndVirtualColumn() throws Exception {
        assertQuery(
                "select-virtual c1, c2, c1 + c2 c3 from (select-choose [x c1, y c2] x c1, y c2 from (select [x, y] from tab)) order by c3, c2, c1",
                "select x c1, y c2, x+y c3 from tab order by 3,2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionWithVirtualColumn() throws Exception {
        assertQuery(
                "select-virtual x, y, x + y column from (select [x, y] from tab) order by column, y, x",
                "select x, y, x+y from tab order by 3,2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPropagation() throws SqlException {
        assertQuery(
                "select-choose id, customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType from " +
                        "(select-choose [C.contactId id, contactlist.customName customName, contactlist.name name, contactlist.email email, contactlist.country_name country_name, contactlist.country_code country_code, contactlist.city city, contactlist.region region, contactlist.emoji_flag emoji_flag, contactlist.latitude latitude, contactlist.longitude longitude, contactlist.isNotReal isNotReal, contactlist.notRealType notRealType, timestamp] C.contactId id, contactlist.customName customName, contactlist.name name, contactlist.email email, contactlist.country_name country_name, contactlist.country_code country_code, contactlist.city city, contactlist.region region, contactlist.emoji_flag emoji_flag, contactlist.latitude latitude, contactlist.longitude longitude, contactlist.isNotReal isNotReal, contactlist.notRealType notRealType, timestamp from " +
                        "(select [contactId] from (select-distinct [contactId] contactId from (select-choose [contactId] contactId from " +
                        "(select-choose [contactId, groupId] contactId, groupId, timestamp from " +
                        "(select [groupId, contactId] from contact_events latest on timestamp partition by contactId) where groupId = 'qIqlX6qESMtTQXikQA46') eventlist) " +
                        "except " +
                        "select-choose [_id contactId] _id contactId from " +
                        "(select-choose [_id, notRealType] _id, customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp from " +
                        "(select [notRealType, _id] from contacts latest on timestamp partition by _id) where notRealType = 'bot') contactlist) C " +
                        "join " +
                        "select [customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp, _id] from " +
                        "(select-choose [customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp, _id] _id, customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp from " +
                        "(select [customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp, _id] from contacts latest on timestamp partition by _id)) contactlist on contactlist._id = C.contactId) C order by timestamp desc)",
                "WITH \n" +
                        "contactlist AS (SELECT * FROM contacts LATEST ON timestamp PARTITION BY _id ORDER BY timestamp),\n" +
                        "eventlist AS (SELECT * FROM contact_events LATEST ON timestamp PARTITION BY contactId ORDER BY timestamp),\n" +
                        "C AS (\n" +
                        "    SELECT DISTINCT contactId FROM eventlist WHERE groupId = 'qIqlX6qESMtTQXikQA46'\n" +
                        "    EXCEPT\n" +
                        "    SELECT _id as contactId FROM contactlist WHERE notRealType = 'bot'\n" +
                        ")\n" +
                        "  SELECT \n" +
                        "    C.contactId as id, \n" +
                        "    contactlist.customName,\n" +
                        "    contactlist.name,\n" +
                        "    contactlist.email,\n" +
                        "    contactlist.country_name,\n" +
                        "    contactlist.country_code,\n" +
                        "    contactlist.city,\n" +
                        "    contactlist.region,\n" +
                        "    contactlist.emoji_flag,\n" +
                        "    contactlist.latitude,\n" +
                        "    contactlist.longitude,\n" +
                        "    contactlist.isNotReal,\n" +
                        "    contactlist.notRealType\n" +
                        "  FROM C \n" +
                        "  JOIN contactlist ON contactlist._id = C.contactId\n" +
                        "  ORDER BY timestamp DESC\n",
                modelOf("contacts")
                        .col("_id", ColumnType.SYMBOL)
                        .col("customName", ColumnType.STRING)
                        .col("name", ColumnType.SYMBOL)
                        .col("email", ColumnType.STRING)
                        .col("country_name", ColumnType.SYMBOL)
                        .col("country_code", ColumnType.SYMBOL)
                        .col("city", ColumnType.SYMBOL)
                        .col("region", ColumnType.SYMBOL)
                        .col("emoji_flag", ColumnType.STRING)
                        .col("latitude", ColumnType.DOUBLE)
                        .col("longitude", ColumnType.DOUBLE)
                        .col("isNotReal", ColumnType.SYMBOL)
                        .col("notRealType", ColumnType.SYMBOL)
                        .timestamp(),
                modelOf("contact_events")
                        .col("contactId", ColumnType.SYMBOL)
                        .col("groupId", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testOrderByWithColumnAliasesAndAggregateColumn() throws Exception {
        assertQuery(
                "select-group-by c1, c2, count() c3 from (select-choose [x c1, y c2] x c1, y c2 from (select [x, y] from tab)) order by c3, c2, c1",
                "select x c1, y c2, count() c3 from tab order by c3,c2,c1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByWithColumnAliasesAndVirtualColumn() throws Exception {
        assertQuery(
                "select-virtual c1, c2, c1 + c2 c3 from (select-choose [x c1, y c2] x c1, y c2 from (select [x, y] from tab)) order by c3, c2, c1",
                "select x c1, y c2, x+y c3 from tab order by c3,c2,c1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByWithLatestBy() throws Exception {
        assertQuery(
                "select-choose id, vendor, pickup_datetime from (select [id, vendor, pickup_datetime] from trips where pickup_datetime < '2009-01-01T00:02:19.000000Z' latest on pickup_datetime partition by vendor_id) order by pickup_datetime",
                "SELECT * FROM trips\n" +
                        "WHERE pickup_datetime < '2009-01-01T00:02:19.000000Z'\n" +
                        "latest on pickup_datetime partition by vendor_id\n" +
                        "ORDER BY pickup_datetime",
                modelOf("trips")
                        .col("id", ColumnType.INT)
                        .col("vendor", ColumnType.SYMBOL)
                        .timestamp("pickup_datetime")
        );
    }

    @Test
    public void testOrderByWithLatestByDeprecated() throws Exception {
        assertQuery(
                "select-choose id, vendor, pickup_datetime from (select [id, vendor, pickup_datetime] from trips timestamp (pickup_datetime) latest by vendor_id where pickup_datetime < '2009-01-01T00:02:19.000000Z') order by pickup_datetime",
                "SELECT * FROM trips\n" +
                        "latest by vendor_id\n" +
                        "WHERE pickup_datetime < '2009-01-01T00:02:19.000000Z'\n" +
                        "ORDER BY pickup_datetime",
                modelOf("trips")
                        .col("id", ColumnType.INT)
                        .col("vendor", ColumnType.SYMBOL)
                        .timestamp("pickup_datetime")
        );
    }

    @Test
    public void testOrderByWithSampleBy() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-choose [t, a, b] a, b, t from (select [t, a, b] from tab) order by t) timestamp (t) sample by 2m order by a",
                "select a, sum(b) from (tab order by t) timestamp(t) sample by 2m order by a",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testOrderByWithSampleBy2() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-group-by [a, sum(b) b] a, sum(b) b from (select-choose [t, a, b] a, b, t from (select [t, a, b] from tab) order by t) timestamp (t) sample by 10m) order by a",
                "select a, sum(b) from (select a,sum(b) b from (tab order by t) timestamp(t) sample by 10m order by b) order by a",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testOrderByWithSampleBy3() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-group-by [a, sum(b) b] a, sum(b) b from (select-choose [a, b, t] a, b, t from (select [a, b, t] from tab timestamp (t)) order by t) sample by 10m) order by a",
                "select a, sum(b) from (select a,sum(b) b from (tab order by t) sample by 10m order by b) order by a",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .timestamp("t")
        );
    }

    @Test
    public void testOrderByWithWildcardAndVirtualColumn() throws Exception {
        assertQuery(
                "select-virtual x, y, x + y c3 from (select [x, y] from tab) order by c3, y, x",
                "select *, x+y c3 from tab order by 3,2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoin() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a left join select [x] from b on b.x = a.x) a",
                "select a.x from a a left join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinColumnAlias() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c left join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = NaN) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " left join orders o on c.customerId = o.customerId) " +
                        " where kk = NaN limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinColumnAliasConst() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count " +
                        "from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk " +
                        "from (select [customerId] " +
                        "from customers c " +
                        "left join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = 10) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() " +
                        "from customers c " +
                        "left join orders o on c.customerId = o.customerId) " +
                        "where kk = 10 " +
                        "limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinColumnAliasNull() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c left join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = null) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " left join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinRightPredicate() throws SqlException {
        assertQuery(
                "select-choose x, y from (select [x] from l left join select [y] from r on r.y = l.x post-join-where y > 0)",
                "select x, y\n" +
                        "from l left join r on l.x = r.y\n" +
                        "where y > 0",
                modelOf("l").col("x", ColumnType.INT),
                modelOf("r").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinRightPredicate1() throws SqlException {
        assertQuery(
                "select-choose x, y from (select [x] from l left join select [y] from r on r.y = l.x post-join-where y > 0 or y > 10)",
                "select x, y\n" +
                        "from l left join r on l.x = r.y\n" +
                        "where y > 0 or y > 10",
                modelOf("l").col("x", ColumnType.INT),
                modelOf("r").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testPGCastToDate() throws SqlException {
        // '2021-01-26'::date
        assertQuery(
                "select-virtual to_pg_date('2021-01-26') to_pg_date from (long_sequence(1))",
                "select '2021-01-26'::date"
        );
    }

    @Test
    public void testPGCastToFloat4() throws SqlException {
        assertQuery(
                "select-virtual cast(123,float) x from (long_sequence(1))",
                "select 123::float4 x"
        );
    }

    @Test
    public void testPGCastToFloat8() throws SqlException {
        assertQuery(
                "select-virtual cast(123,double) x from (long_sequence(1))",
                "select 123::float8 x"
        );
    }

    @Test
    @Ignore
    public void testPGColumnListQuery() throws SqlException {
        assertQuery(
                "",
                "SELECT c.oid,\n" +
                        "  n.nspname,\n" +
                        "  c.relname\n" +
                        "FROM pg_catalog.pg_class c\n" +
                        "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                        "WHERE c.relname OPERATOR(pg_catalog.~) E'^(movies\\\\.csv)$'\n" +
                        "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                        "ORDER BY 2, 3;"
        );
    }

    @Test
    public void testPGTableListQuery() throws SqlException {
        assertQuery(
                "select-virtual Schema, Name, switch(relkind,'r','table','v','view','m','materialized view','i','index','S','sequence','s','special','f','foreign table','p','table','I','index') Type, pg_catalog.pg_get_userbyid(relowner) Owner from (select-choose [n.nspname Schema, c.relname Name, c.relkind relkind, c.relowner relowner] n.nspname Schema, c.relname Name, c.relkind relkind, c.relowner relowner from (select [relname, relkind, relowner, relnamespace, oid] from pg_catalog.pg_class() c left join select [nspname, oid] from pg_catalog.pg_namespace() n on n.oid = c.relnamespace post-join-where n.nspname != 'pg_catalog' and n.nspname != 'information_schema' and n.nspname !~ '^pg_toast' where relkind in ('r','p','v','m','S','f','') and pg_catalog.pg_table_is_visible(oid)) c) c order by Schema, Name",
                "SELECT n.nspname                              as \"Schema\",\n" +
                        "       c.relname                              as \"Name\",\n" +
                        "       CASE c.relkind\n" +
                        "           WHEN 'r' THEN 'table'\n" +
                        "           WHEN 'v' THEN 'view'\n" +
                        "           WHEN 'm' THEN 'materialized view'\n" +
                        "           WHEN 'i' THEN 'index'\n" +
                        "           WHEN 'S' THEN 'sequence'\n" +
                        "           WHEN 's' THEN 'special'\n" +
                        "           WHEN 'f' THEN 'foreign table'\n" +
                        "           WHEN 'p' THEN 'table'\n" +
                        "           WHEN 'I' THEN 'index' END          as \"Type\",\n" +
                        "       pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n" +
                        "FROM pg_catalog.pg_class c\n" +
                        "         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                        "WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', '')\n" +
                        "  AND n.nspname != 'pg_catalog'\n" +
                        "  AND n.nspname != 'information_schema'\n" +
                        "  AND n.nspname !~ '^pg_toast'\n" +
                        "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                        "ORDER BY 1, 2"
        );
    }

    @Test
    public void testPartitionByOrderByAcceptsAsc() throws SqlException {
        assertPartitionByOverOrderByAcceptsDirection("asc", "");
    }

    @Test
    public void testPartitionByOrderByAcceptsDefault() throws SqlException {
        assertPartitionByOverOrderByAcceptsDirection("", "");
    }

    @Test
    public void testPartitionByOrderByAcceptsDesc() throws SqlException {
        assertPartitionByOverOrderByAcceptsDirection("desc", " desc");
    }

    @Test
    public void testPgCastRewrite() throws Exception {
        assertQuery(
                "select-virtual cast(t,varchar) cast from (select [t] from x)",
                "select t::varchar from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testPipeConcatInJoin() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x from (select [x] from tab cross join select [y] from bap post-join-where concat(tab.x,'1') = concat('2',substring(bap.y,0,5)))",
                "select 1, x from tab join bap on tab.x || '1' = '2' || substring(bap.y, 0, 5)",
                modelOf("tab").col("x", ColumnType.STRING),
                modelOf("bap").col("y", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatInWhere() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x from (select [x, z, y] from tab where concat(x,'-',y) = z)",
                "select 1, x from tab where x || '-' || y = z",
                modelOf("tab").col("x", ColumnType.STRING)
                        .col("y", ColumnType.STRING)
                        .col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatNested() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x,'3') concat from (select [x] from tab)",
                "select 1, x, '2' || x || '3' from tab",
                modelOf("tab").col("x", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatNested4() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x,'3',y) concat from (select [x, y] from tab)",
                "select 1, x, '2' || x || '3' || y from tab",
                modelOf("tab").col("x", ColumnType.STRING).col("y", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatWithFunctionConcatOnLeft() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',cast(x + 1,string),'3') concat from (select [x] from tab)",
                "select 1, x, concat('2', cast(x + 1 as string)) || '3' from tab",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testPipeConcatWithFunctionConcatOnRight() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',cast(x + 1,string),'3') concat from (select [x] from tab)",
                "select 1, x, '2' || concat(cast(x + 1 as string), '3') from tab",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testPipeConcatWithNestedCast() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',cast(x + 1,string),'3') concat from (select [x] from tab)",
                "select 1, x, '2' || cast(x + 1 as string) || '3' from tab",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testPushWhereThroughUnionAll() throws SqlException {
        assertQuery(
                "select-choose sm from (select-group-by [sum(x) sm] sum(x) sm from (select-choose [x] x from (select [x] from t1) union all select-choose [x] x from (select [x] from t2)) where sm = 1)",
                "select * from ( select sum(x) as sm from (select * from t1 union all select * from t2 ) ) where sm = 1",
                modelOf("t1").col("x", ColumnType.INT),
                modelOf("t2").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testQueryExceptQuery() throws SqlException {
        assertQuery(
                "select-choose [a, b, c, x, y, z] a, b, c, x, y, z from (select [a, b, c, x, y, z] from x) except select-choose [a, b, c, x, y, z] a, b, c, x, y, z from (select [a, b, c, x, y, z] from y)",
                "select * from x except select* from y",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("y")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testQueryIntersectQuery() throws SqlException {
        assertQuery(
                "select-choose [a, b, c, x, y, z] a, b, c, x, y, z from (select [a, b, c, x, y, z] from x) intersect select-choose [a, b, c, x, y, z] a, b, c, x, y, z from (select [a, b, c, x, y, z] from y)",
                "select * from x intersect select* from y",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("y")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testQuotedTableAliasFollowedByStar() throws Exception {
        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select foo.* from long_sequence(1) as foo"
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select 'foo'.* from long_sequence(1) as foo"
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select foo.* from long_sequence(1) as 'foo'"
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select 'foo'.* from long_sequence(1) as 'foo'"
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select \"foo\".* from long_sequence(1) as \"foo\""
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select \"foo\".* from long_sequence(1) as 'foo'"
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select 'foo'.* from long_sequence(1) as \"foo\""
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select 'foo'.\"*\" from long_sequence(1) as \"foo\""
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select 'foo'.'*' from long_sequence(1) as \"foo\""
        );

        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) foo) foo",
                "select \"foo.*\" from long_sequence(1) as \"foo\""
        );

        // the alias itself contains double quotes. should this be allowed at all?
        assertQuery(
                "select-choose x from (select [x] from long_sequence(1) \\\"foo\\\") \\\"foo\\\"",
                "select \\\"foo\\\".* from long_sequence(1) as \\\"foo\\\""
        );
    }

    @Test
    public void testRedundantSelect() throws Exception {
        assertSyntaxError(
                "select x from select (select x from a) timestamp(x)",
                22,
                "query is not expected, did you mean column?",
                modelOf("a").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testRegexOnFunction() throws SqlException {
        assertQuery(
                "select-choose a from (select-virtual [rnd_str() a] rnd_str() a from (long_sequence(100)) where a ~ '^W')",
                "(select rnd_str() a from long_sequence(100)) where a ~ '^W'"
        );
    }

    @Test
    public void testRightJoinAfterSubqueryClauseGivesError() throws Exception {
        assertSyntaxError(
                "select * from (select * from t1 ) right join t1 on x",
                34,
                "unsupported join type",
                modelOf("t1").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testRightJoinAsFirstJoinGivesError() throws Exception {
        assertSyntaxError(
                "select * from t1 right join t1 on x",
                17,
                "unsupported join type",
                modelOf("t1").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testRightJoinDeepInFromClauseGivesError() throws Exception {
        assertSyntaxError(
                "select * from t1 join t1 on x right join t1 on x",
                30,
                "unsupported join type",
                modelOf("t1").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSampleBy() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select [x, y] from tab timestamp (timestamp)) sample by 2m",
                "select x,sum(y) from tab sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleBy2() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select [x, y] from tab timestamp (timestamp)) sample by 2U",
                "select x,sum(y) from tab sample by 2U",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleByAliasedColumn() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k, k k1 from (select [b, a, k] from x y timestamp (timestamp)) y sample by 3h",
                "select b, sum(a), k, k from x y sample by 3h",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testSampleByAlreadySelected() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select [x, y] from tab timestamp (x)) sample by 2m",
                "select x,sum(y) from tab timestamp(x) sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.TIMESTAMP)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByAltTimestamp() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select [x, y] from tab timestamp (t)) sample by 2m",
                "select x,sum(y) from tab timestamp(t) sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByEndingWithSemicolon() throws SqlException {
        assertQuery(
                "select-group-by first(ts) first from (select [ts] from t1) sample by 15m align to calendar with offset '00:00'",
                "SELECT first(ts) FROM t1 SAMPLE BY 15m ALIGN TO CALENDAR;",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByEndingWithWhitespace() throws SqlException {
        assertQuery(
                "select-group-by first(ts) first from (select [ts] from t1) sample by 15m align to calendar with offset '00:00'",
                "SELECT first(ts) FROM t1 SAMPLE BY 15m ALIGN TO CALENDAR",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByFillList() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(21.1,22,null,98)",
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill(21.1,22,null,98)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMin() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(mid)",
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill(mid)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMinAsSubQuery() throws SqlException {
        assertQuery(
                "select-choose a, b from (select-group-by [a, sum(b) b] a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(mid))",
                "select * from (select a,sum(b) b from tab timestamp(t) sample by 10m fill(mid))",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMissingCloseBrace() throws Exception {
        assertSyntaxError(
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill (21231.2344",
                70,
                "')' expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMissingOpenBrace() throws Exception {
        assertSyntaxError(
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill 21231.2344",
                59,
                "'(' expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMissingValue() throws Exception {
        assertSyntaxError(
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill ()",
                60,
                "'none', 'prev', 'mid', 'null' or number expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillValue() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(21231.2344)",
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill(21231.2344)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByIncorrectPlacement() throws Exception {
        assertSyntaxError(
                "select a, sum(b) from ((tab order by t) timestamp(t) sample by 10m order by t) order by a",
                63,
                "at least one aggregation function must be present",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByInvalidColumn() throws Exception {
        assertSyntaxError(
                "select x,sum(y) from tab timestamp(z) sample by 2m",
                35,
                "Invalid column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleByInvalidType() throws Exception {
        assertSyntaxError(
                "select x,sum(y) from tab timestamp(x) sample by 2m",
                35,
                "not a TIMESTAMP",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleByNoAggregate() throws Exception {
        assertSyntaxError("select x,y from tab sample by 2m", 30, "at least one",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleBySansSelect() throws Exception {
        assertSyntaxError(
                "(t1 sample by 1m)",
                14,
                "at least one aggregation function must be present in 'select' clause",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByTimestampAscOrder() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select-choose [x, y, ts] x, y, ts from (select [x, y, ts] from tab timestamp (ts)) order by ts) sample by 2m",
                "select x,sum(y) from (tab order by ts asc) sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testSampleByTimestampAscOrderWithJoin() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select-choose [tab.x x, tab.y y] tab.x x, tab.y y from (select [x, y] from (select-choose [x, y, ts] x, y, ts from (select [x, y, ts] from tab timestamp (ts)) order by ts) tab join select [x] from tab2 on tab2.x = tab.x) tab) tab sample by 2m",
                "select tab.x,sum(y) from (tab order by ts asc) tab join tab2 on (x) sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp("ts"),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testSampleByTimestampDescOrder() throws Exception {
        assertSyntaxError(
                "select x,sum(y) from (tab order by ts desc) sample by 2m",
                0,
                "base query does not provide ASC order over dedicated TIMESTAMP column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testSampleByTimestampDescOrderVirtualColumn() throws Exception {
        assertSyntaxError(
                "select sum(x) from (select x+1 as x, ts from (tab order by ts desc)) sample by 2m",
                0,
                "base query does not provide ASC order over dedicated TIMESTAMP column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testSampleByTimestampDescOrderWithJoin() throws Exception {
        assertSyntaxError(
                "select tab.x,sum(y) from (tab order by ts desc) tab join tab2 on (x) sample by 2m",
                0,
                "ASC order over TIMESTAMP column is required but not provided",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp("ts"),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testSampleByUndefinedTimestamp() throws Exception {
        assertSyntaxError(
                "select x,sum(y) from tab sample by 2m",
                0,
                "base query does not provide ASC order over dedicated TIMESTAMP column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByUndefinedTimestampWithDistinct() throws Exception {
        assertSyntaxError(
                "select x,sum(y) from (select distinct x, y from tab) sample by 2m",
                0,
                "base query does not provide ASC order over dedicated TIMESTAMP column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByUndefinedTimestampWithJoin() throws Exception {
        assertSyntaxError(
                "select tab.x,sum(y) from tab join tab2 on (x) sample by 2m",
                0,
                "ASC order over TIMESTAMP column is required but not provided",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testSelectAfterOrderBy() throws SqlException {
        assertQuery(
                "select-distinct [Schema] Schema from (select-choose [Schema] Schema from (select-virtual [Schema, Name] Schema, Name, switch(relkind,'r','table','v','view','m','materialized view','i','index','S','sequence','s','special','f','foreign table','p','table','I','index') Type, pg_catalog.pg_get_userbyid(relowner) Owner from (select-choose [n.nspname Schema, c.relname Name] n.nspname Schema, c.relname Name, c.relkind relkind, c.relowner relowner from (select [relname, relnamespace, relkind, oid] from pg_catalog.pg_class() c left join select [nspname, oid] from pg_catalog.pg_namespace() n on n.oid = c.relnamespace post-join-where n.nspname != 'pg_catalog' and n.nspname != 'information_schema' and n.nspname !~ '^pg_toast' where relkind in ('r','p','v','m','S','f','') and pg_catalog.pg_table_is_visible(oid)) c) c order by Schema, Name))",
                "select distinct Schema from \n" +
                        "(SELECT n.nspname                              as \"Schema\",\n" +
                        "       c.relname                              as \"Name\",\n" +
                        "       CASE c.relkind\n" +
                        "           WHEN 'r' THEN 'table'\n" +
                        "           WHEN 'v' THEN 'view'\n" +
                        "           WHEN 'm' THEN 'materialized view'\n" +
                        "           WHEN 'i' THEN 'index'\n" +
                        "           WHEN 'S' THEN 'sequence'\n" +
                        "           WHEN 's' THEN 'special'\n" +
                        "           WHEN 'f' THEN 'foreign table'\n" +
                        "           WHEN 'p' THEN 'table'\n" +
                        "           WHEN 'I' THEN 'index' END          as \"Type\",\n" +
                        "       pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n" +
                        "FROM pg_catalog.pg_class c\n" +
                        "         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                        "WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', '')\n" +
                        "  AND n.nspname != 'pg_catalog'\n" +
                        "  AND n.nspname != 'information_schema'\n" +
                        "  AND n.nspname !~ '^pg_toast'\n" +
                        "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                        "ORDER BY 1, 2);\n"
        );
    }

    @Test
    public void testSelectAliasAsFunction() throws Exception {
        assertSyntaxError(
                "select sum(x) x() from tab",
                15,
                "',', 'from' or 'over' expected",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSelectAliasIgnoreColumn() throws SqlException {
        assertQuery(
                "select-virtual 3 x from (long_sequence(2))",
                "select 3 x from long_sequence(2)"
        );
    }

    @Test
    public void testSelectAliasNoWhere() throws SqlException {
        assertQuery(
                "select-virtual rnd_int(1,2,0) a from (long_sequence(1))",
                "select rnd_int(1, 2, 0) a"
        );
    }

    @Test
    public void testSelectAsAliasQuoted() throws SqlException {
        assertQuery(
                "select-choose a 'y y' from (select [a] from tab)",
                "select a as \"y y\" from tab",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testSelectColumnWithAlias() throws SqlException {
        assertQuery(
                "select-virtual a, rnd_int() c from (select-choose [x a] x a from (select [x] from long_sequence(5)))",
                "select x a, rnd_int() c from long_sequence(5)"
        );
    }

    @Test
    public void testSelectColumnsFromJoinSubQueries() throws SqlException {
        assertQuery(
                "select-virtual addr, sum_out - sum_in total from (select-choose [a.addr addr, b.sum_in sum_in, a.sum_out sum_out] a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in from (select [addr, sum_out] from (select-group-by [addr, sum(value) sum_out] addr, count() count, sum(value) sum_out from (select-choose [fromAddress addr, value] fromAddress addr, value from (select [fromAddress, value] from transactions.csv))) a join select [sum_in, toAddress] from (select-group-by [sum(value) sum_in, toAddress] toAddress, count() count, sum(value) sum_in from (select [value, toAddress] from transactions.csv)) b on b.toAddress = a.addr) a)",
                "select addr, sum_out - sum_in total from (\n" +
                        "(select fromAddress addr, count(), sum(value) sum_out from 'transactions.csv') a join\n" +
                        "(select toAddress, count(), sum(value) sum_in from 'transactions.csv') b on a.addr = b.toAddress\n" +
                        ")",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.LONG)
                        .col("toAddress", ColumnType.LONG)
                        .col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectColumnsFromJoinSubQueries2() throws SqlException {
        assertQuery(
                "select-choose addr, count, sum_out, toAddress, count1, sum_in from (select-choose [a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in] a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in from (select [addr, count, sum_out] from (select-group-by [addr, count() count, sum(value) sum_out] addr, count() count, sum(value) sum_out from (select-choose [fromAddress addr, value] fromAddress addr, value from (select [fromAddress, value] from transactions.csv))) a join select [toAddress, count, sum_in] from (select-group-by [toAddress, count() count, sum(value) sum_in] toAddress, count() count, sum(value) sum_in from (select [toAddress, value] from transactions.csv)) b on b.toAddress = a.addr) a)",
                "(\n" +
                        "(select fromAddress addr, count(), sum(value) sum_out from 'transactions.csv') a join\n" +
                        "(select toAddress, count(), sum(value) sum_in from 'transactions.csv') b on a.addr = b.toAddress\n" +
                        ")",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.LONG)
                        .col("toAddress", ColumnType.LONG)
                        .col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectColumnsFromJoinSubQueries3() throws SqlException {
        assertQuery(
                "select-choose a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in from (select [addr, count, sum_out] from (select-group-by [addr, count() count, sum(value) sum_out] addr, count() count, sum(value) sum_out from (select-choose [fromAddress addr, value] fromAddress addr, value from (select [fromAddress, value] from transactions.csv))) a join select [toAddress, count, sum_in] from (select-group-by [toAddress, count() count, sum(value) sum_in] toAddress, count() count, sum(value) sum_in from (select [toAddress, value] from transactions.csv)) b on b.toAddress = a.addr) a",
                "(select fromAddress addr, count(), sum(value) sum_out from 'transactions.csv') a join\n" +
                        "(select toAddress, count(), sum(value) sum_in from 'transactions.csv') b on a.addr = b.toAddress\n",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.LONG)
                        .col("toAddress", ColumnType.LONG)
                        .col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectContainsDuplicateColumnAliases() throws Exception {
        ddl(
                "CREATE TABLE t1 (" +
                        "  ts TIMESTAMP, " +
                        "  x INT" +
                        ") TIMESTAMP(ts) PARTITION BY DAY"
        );
        ddl(
                "CREATE TABLE t2 (" +
                        "  ts TIMESTAMP, " +
                        "  x INT" +
                        ") TIMESTAMP(ts) PARTITION BY DAY"
        );
        insert("INSERT INTO t1(ts, x) VALUES (1, 1)");
        insert("INSERT INTO t2(ts, x) VALUES (1, 2)");
        engine.releaseInactive();

        assertSql("TS\tts1\tx\tts2\n" +
                "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\n", "select t2.ts as \"TS\", t1.*, t2.ts \"ts1\" from t1 asof join (select * from t2) t2;");
        assertSql("ts\tx\tts1\tx1\tts2\n" +
                "1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t2\t1970-01-01T00:00:00.000001Z\n", "select *, t2.ts as \"TS1\" from t1 asof join (select * from t2) t2;");
        assertSql("ts\tx\tts1\n" +
                "1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\n", "select t1.*, t2.ts from t1 asof join (select * from t2) t2;");

        assertSyntaxError(
                "SELECT " +
                        "   a.a aa, " +
                        "   a.b ab, " +
                        "   b.a ba, " +
                        "   b.b bb " +
                        "FROM DB a lt join DB a",
                71,
                "Duplicate table or alias: a",
                modelOf("DB").col("a", ColumnType.SYMBOL).timestamp("b")
        );

        assertSyntaxError(
                "SELECT " +
                        "   a.a aa, " +
                        "   a.b ab, " +
                        "   b.a ab, " +
                        "   b.b bb " +
                        "FROM DB a lt join DB b",
                36,
                "Duplicate column [name=ab]",
                modelOf("DB").col("a", ColumnType.SYMBOL).timestamp("b")
        );
    }

    @Test
    public void testSelectDistinct() throws SqlException {
        assertQuery(
                "select-distinct [a, b] a, b from (select-choose [a, b] a, b from (select [a, b] from tab))",
                "select distinct a, b from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctArithmetic() throws SqlException {
        assertQuery(
                "select-distinct [column] column from (select-virtual [a + b column] a + b column from (select [b, a] from tab))",
                "select distinct a + b from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctGroupByFunction() throws SqlException {
        assertQuery(
                "select-distinct [a, bb] a, bb from (select-group-by [a, sum(b) bb] a, sum(b) bb from (select [a, b] from tab))",
                "select distinct a, sum(b) bb from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctGroupByFunctionArithmetic() throws SqlException {
        assertQuery(
                "select-distinct [a, bb] a, bb from (select-virtual [a, sum1 + sum bb] a, sum1 + sum bb from (select-group-by [a, sum(c) sum, sum(b) sum1] a, sum(c) sum, sum(b) sum1 from (select [a, c, b] from tab)))",
                "select distinct a, sum(b)+sum(c) bb from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctGroupByFunctionArithmeticLimit() throws SqlException {
        assertQuery(
                "select-distinct [a, bb] a, bb from (select-virtual [a, sum1 + sum bb] a, sum1 + sum bb from (select-group-by [a, sum(c) sum, sum(b) sum1] a, sum(c) sum, sum(b) sum1 from (select [a, c, b] from tab))) limit 10",
                "select distinct a, sum(b)+sum(c) bb from tab limit 10",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctGroupByFunctionArithmeticOrderByLimit() throws SqlException {
        assertQuery(
                "select-distinct [a, bb] a, bb from (select-virtual [a, sum1 + sum bb] a, sum1 + sum bb from " +
                        "(select-group-by [a, sum(c) sum, sum(b) sum1] a, sum(c) sum, sum(b) sum1 " +
                        "from (select [a, c, b] from tab))) order by a limit 10",
                "select distinct a, sum(b)+sum(c) bb from tab order by a limit 10",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctUnion() throws SqlException {
        assertQuery(
                "select-choose c from (" +
                        "select-distinct [c, b] c, b from (select-choose [a c, b] a c, b from (select [a, b] from trips)) " +
                        "union all " +
                        "select-distinct [c, b] c, b from (select-choose [c, d b] c, d b from (select [c, d] from trips)))",
                "select c from (select distinct a c, b from trips union all select distinct c, d b from trips)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectDuplicateAlias() throws Exception {
        assertSyntaxError(
                "select x x, x x from long_sequence(1)",
                14,
                "Duplicate column [name=x]"
        );

        assertSyntaxError(
                "select x x, y x from tabula",
                14,
                "Duplicate column [name=x]",
                modelOf("tabula")
                        .col("x", ColumnType.LONG)
                        .col("y", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectEndsWithSemicolon() throws Exception {
        assertQuery(
                "select-choose x from (select [x] from x)",
                "select * from x;",
                modelOf("x").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSelectEscapedQuotedIdentifier() throws SqlException {
        assertQuery(
                "select-virtual 'test' quoted\"\"id from (long_sequence(1))",
                "select 'test' as \"quoted\"\"id\""
        );
    }

    @Test
    public void testSelectEscapedStringLiteral() throws SqlException {
        assertQuery(
                "select-virtual 'test''quotes' test''quotes from (long_sequence(1))",
                "select 'test''quotes'"
        );
    }

    @Test
    public void testSelectFromNoColumns() throws Exception {
        assertSyntaxError(
                "select from",
                7,
                "column expression expected"
        );
    }

    @Test
    public void testSelectFromNonCursorFunction() throws Exception {
        assertSyntaxError("select * from length('hello')", 14, "function must return CURSOR");
    }

    @Test
    public void testSelectFromSelectWildcardAndExpr() throws SqlException {
        assertQuery(
                "select-virtual column + x column from (select-virtual [x, x + y column] x, y, x1, z, x + y column from (select-choose [tab1.x x, tab1.y y] tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x] from tab2 on tab2.x = tab1.x)))",
                "select \"column\" + x from (select *, tab1.x + y from tab1 join tab2 on (x))",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectFromSubQuery() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x, y from (select [x, y] from tab where y > 10)) a",
                "select a.x from (tab where y > 10) a",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByAndWindow() throws Exception {
        assertSyntaxError(
                "select sum(x), count() over() from tab",
                0,
                "Window function is not allowed",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByArithmetic() throws SqlException {
        assertQuery(
                "select-virtual sum + 10 column, sum1 from (select-group-by [sum(x) sum, sum(y) sum1] sum(x) sum, sum(y) sum1 from (select [x, y] from tab))",
                "select sum(x)+10, sum(y) from tab",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByArithmeticAndLimit() throws SqlException {
        assertQuery(
                "select-virtual sum + 10 column, sum1 from (select-group-by [sum(x) sum, sum(y) sum1] sum(x) sum, sum(y) sum1 from (select [x, y] from tab)) limit 200",
                "select sum(x)+10, sum(y) from tab limit 200",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByUnion() throws SqlException {
        assertQuery(
                "select-choose b from (select-group-by [sum(b) b, a] a, sum(b) b from (select [b, a] from trips) union all select-group-by [avg(d) b, c] c, avg(d) b from (select [d, c] from trips))",
                "select b from (select a, sum(b) b from trips union all select c, avg(d) b from trips)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectLatestByDeprecatedUnion() throws SqlException {
        assertQuery(
                "select-choose b from (select-choose [b] a, b from (select [b, c] from trips latest by c) union all select-choose [d b] c, d b from (select [d, a] from trips latest by a))",
                "select b from (select a, b b from trips latest by c union all select c, d b from trips latest by a)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectLatestByUnion() throws SqlException {
        assertQuery(
                "select-choose b from (select-choose [b] a, b from (select [b, c] from trips latest on ts partition by c) union all select-choose [d b] c, d b from (select [d, a] from trips latest on ts partition by a))",
                "select b from (select a, b b from trips latest on ts partition by c union all select c, d b from trips latest on ts partition by a)",
                modelOf("trips")
                        .timestamp("ts")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectMissing() throws Exception {
        assertSyntaxError("from x 'a b' where x > 1", 0, "Did you mean 'select * from'?");
    }

    @Test
    public void testSelectMissingExpression() throws Exception {
        assertSyntaxError(
                "select ,a from tab",
                7,
                "missing expression"
        );
    }

    @Test
    public void testSelectNoFromUnion() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-virtual [1 a, 1 b] 1 a, 1 b from (long_sequence(1)) union all select-virtual [333 333, 1 1] 333 333, 1 1 from (long_sequence(1))) x",
                "select a, sum(b) from (select 1 a, 1 b union all select 333, 1) x"
        );
    }

    @Test
    public void testSelectNoWhere() throws SqlException {
        assertQuery(
                "select-virtual rnd_int(1,2,0) rnd_int from (long_sequence(1))",
                "select rnd_int(1, 2, 0)"
        );
    }

    @Test
    public void testSelectOnItsOwn() throws Exception {
        assertSyntaxError("select ", 7, "column expected");
    }

    @Test
    public void testSelectOrderByWhereOnCount() throws SqlException {
        assertQuery(
                "select-choose a, c from (select-group-by [a, count() c] a, count() c from (select [a] from tab) where c > 0 order by a)",
                "(select a, count() c from tab order by 1) where c > 0",
                modelOf("tab").col("a", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSelectPlainColumns() throws Exception {
        assertQuery(
                "select-choose a, b, c from (select [a, b, c] from t)",
                "select a,b,c from t",
                modelOf("t").col("a", ColumnType.INT).col("b", ColumnType.INT).col("c", ColumnType.INT)
        );
    }

    @Test
    public void testSelectSelectColumn() throws Exception {
        assertSyntaxError(
                "select a, select from tab",
                17,
                "reserved name"
        );
    }

    @Test
    public void testSelectSingleExpression() throws Exception {
        assertQuery(
                "select-virtual a + b * c x from (select [a, c, b] from t)",
                "select a+b*c x from t",
                modelOf("t").col("a", ColumnType.INT).col("b", ColumnType.INT).col("c", ColumnType.INT)
        );
    }

    @Test
    public void testSelectSingleTimestampColumn() throws SqlException {
        assertQuery(
                "select-choose t3 from (select-choose [t3] t, tt, t3 from (select [t3] from x timestamp (t3)) order by t3 desc limit 1)",
                "select t3 from x limit -1",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP).timestamp("t3")
        );
    }

    @Test
    public void testSelectSumFromSubQueryLimit() throws SqlException {
        assertQuery(
                "select-group-by sum(tip_amount) sum from (select-choose [tip_amount] a, b, c, d, tip_amount, e from (select [tip_amount] from trips) limit 100000000)",
                "select sum(tip_amount) from (trips limit 100000000)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectSumSquared() throws Exception {
        assertSql("x1\tx\n" +
                "1\t1\n" +
                "2\t4\n", "select x, sum(x)*sum(x) x from long_sequence(2)");
    }

    @Test
    public void testSelectTrailingComma() throws SqlException {
        assertQuery(
                "select-choose a, b, c, d from (select [a, b, c, d] from tab)",
                "select " +
                        "a," +
                        "b," +
                        "c," +
                        "d," +
                        "from tab",
                modelOf("tab")
                        .col("a", ColumnType.SYMBOL)
                        .col("b", ColumnType.SYMBOL)
                        .col("c", ColumnType.SYMBOL)
                        .col("d", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSelectVirtualAliasClash() throws SqlException {
        assertQuery(
                "select-virtual x + x x, x x1 from (select [x] from long_sequence(1) z) z",
                "select x+x x, x from long_sequence(1) z"
        );
    }

    @Test
    public void testSelectWhereOnCount() throws SqlException {
        assertQuery(
                "select-choose a, c from (select-group-by [a, count() c] a, count() c from (select [a] from tab) where c > 0)",
                "(select a, count() c from tab) where c > 0",
                modelOf("tab").col("a", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSelectWildcard() throws SqlException {
        assertQuery(
                "select-choose tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x)",
                "select * from tab1 join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardAlias() throws Exception {
        assertSyntaxError(
                "select tab2.* x, b.* from tab1 a join tab2 on (x)",
                14,
                "wildcard cannot have alias",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardAndExpr() throws SqlException {
        assertQuery(
                "select-virtual x, y, x1, z, x + y column from (select-choose [tab1.x x, tab1.y y, tab2.x x1, tab2.z z] tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x))",
                "select *, tab1.x + y from tab1 join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardAndNotTimestamp() throws Exception {
        assertSyntaxError(
                "select * from (select x from tab1) timestamp(y)",
                45,
                "Invalid column",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSelectWildcardAndTimestamp() throws SqlException {
        assertQuery(
                "select-choose x, y from (select-choose [y, x] x, y from (select [y, x] from tab1)) timestamp (y)",
                "select * from (select x, y from tab1) timestamp(y)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSelectWildcardDetachedStar() throws Exception {
        assertQuery(
                "select-choose tab2.x x, tab2.z z, a.x x1, a.y y from (select [x, y] from tab1 a join select [x, z] from tab2 on tab2.x = a.x) a",
                "select tab2.*, a.  * from tab1 a join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardInterpretation() throws Exception {
        // multiplication * must not be confused with wildcard
        assertQuery(
                "select-virtual 2 * 1 x, x1, y from (select-choose [x x1, y] x x1, y from (select [x, y] from tab1 b) b) b",
                "select 2*1 x, b.* from tab1 b",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardInvalidTableAlias() throws Exception {
        assertSyntaxError(
                "select tab2.*, b.* from tab1 a join tab2 on (x)",
                15,
                "invalid table alias",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardMissingStar() throws Exception {
        assertSyntaxError(
                "select tab2.*, a. from tab1 a join tab2 on (x)",
                17,
                "'*' or column name expected",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardOnly() throws Exception {
        assertException(
                "select *",
                "create table tab (seq long)",
                7,
                "'from' expected"
        );
    }

    @Test
    public void testSelectWildcardPrefixed() throws SqlException {
        assertQuery(
                "select-choose tab2.x x, tab2.z z, tab1.x x1, tab1.y y from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x)",
                "select tab2.*, tab1.* from tab1 join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardPrefixed2() throws SqlException {
        assertQuery(
                "select-choose tab2.x x, tab2.z z, a.x x1, a.y y from (select [x, y] from tab1 a join select [x, z] from tab2 on tab2.x = a.x) a",
                "select tab2.*, a.* from tab1 a join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardTabNoFrom() throws Exception {
        assertException(
                "select * tab",
                "create table tab (seq long)",
                9,
                "wildcard cannot have alias"
        );
    }

    @Test
    public void testSelectWindowOperator() throws Exception {
        assertSyntaxError(
                "select sum(x), 2*x over() from tab",
                16,
                "Window function expected",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSimpleCaseExpression() throws SqlException {
        assertQuery(
                "select-virtual switch(a,1,'A',2,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case a when 1 then 'A' when 2 then 'B' else 'C' end + 1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testSimpleCaseExpressionAsConstant() throws SqlException {
        assertQuery(
                "select-virtual switch(1,1,'A',2,'B','C') + 1 column, b from (select [b] from tab)",
                "select case 1 when 1 then 'A' when 2 then 'B' else 'C' end + 1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testSimpleSubQuery() throws Exception {
        assertQuery(
                "select-choose y from (select [y] from x where y > 1)",
                "(x) where y > 1",
                modelOf("x").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSingleTableLimit() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y, z] from tab where x > z) limit 100",
                "select x x, y y from tab where x > z limit 100",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSingleTableLimitLoHi() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y, z] from tab where x > z) limit 100,200",
                "select x x, y y from tab where x > z limit 100,200",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSingleTableLimitLoHiExtraToken() throws Exception {
        assertSyntaxError("select x x, y y from tab where x > z limit 100,200 b", 51, "unexpected");
    }

    @Test
    public void testSingleTableNoWhereLimit() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y] from tab) limit 100",
                "select x x, y y from tab limit 100",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSpliceJoin() throws SqlException {
        assertQuery(
                "select-choose t.timestamp timestamp, t.tag tag, q.timestamp timestamp1 from (select [timestamp, tag] from trades t timestamp (timestamp) splice join select [timestamp] from quotes q timestamp (timestamp) where tag = null) t",
                "trades t splice join quotes q where tag = null",
                modelOf("trades").timestamp().col("tag", ColumnType.SYMBOL),
                modelOf("quotes").timestamp()
        );
    }

    @Test
    public void testSpliceJoinColumnAliasNull() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c splice join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = null) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " splice join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testSpliceJoinNullFilter() throws SqlException {
        assertQuery(
                "select-choose t.timestamp timestamp, t.tag tag, q.x x, q.timestamp timestamp1 from (select [timestamp, tag] from trades t timestamp (timestamp) splice join select [x, timestamp] from quotes q timestamp (timestamp) post-join-where x = null) t",
                "trades t splice join quotes q where x = null",
                modelOf("trades").timestamp().col("tag", ColumnType.SYMBOL),
                modelOf("quotes").col("x", ColumnType.SYMBOL).timestamp()
        );
    }

    @Test
    public void testSpliceJoinOrder() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, e.employeeId employeeId, o.customerId customerId1 from (select [customerId] from customers c splice join select [employeeId] from employees e on e.employeeId = c.customerId join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " splice join employees e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees").col("employeeId", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSpliceJoinSubQuery() throws Exception {
        // execution order must be (src: SQL Server)
        //        1. FROM
        //        2. ON
        //        3. JOIN
        //        4. WHERE
        //        5. GROUP BY
        //        6. WITH CUBE or WITH ROLLUP
        //        7. HAVING
        //        8. SELECT
        //        9. DISTINCT
        //        10. ORDER BY
        //        11. TOP
        //
        // which means "where" clause for "e" table has to be explicitly as post-join-where
        assertQuery(
                "select-choose c.customerId customerId, e.blah blah, e.lastName lastName, e.employeeId employeeId, e.timestamp timestamp, o.customerId customerId1 from (select [customerId] from customers c splice join select [blah, lastName, employeeId, timestamp] from (select-virtual ['1' blah, lastName, employeeId, timestamp] '1' blah, lastName, employeeId, timestamp from (select [lastName, employeeId, timestamp] from employees) order by lastName) e on e.employeeId = c.customerId post-join-where e.lastName = 'x' and e.blah = 'y' join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " splice join (select '1' blah, lastName, employeeId, timestamp from employees order by lastName) e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId where e.lastName = 'x' and e.blah = 'y'",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP),
                modelOf("orders")
                        .col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSpliceJoinSubQuerySimpleAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, a.blah blah, a.lastName lastName, a.customerId customerId1, a.timestamp timestamp from (select [customerId] from customers c splice join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) a on a.customerId = c.customerId) c",
                "customers c" +
                        " splice join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) a on (customerId)",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSpliceJoinSubQuerySimpleNoAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, _xQdbA0.blah blah, _xQdbA0.lastName lastName, _xQdbA0.customerId customerId1, _xQdbA0.timestamp timestamp from (select [customerId] from customers c splice join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) _xQdbA0 on _xQdbA0.customerId = c.customerId) c",
                "customers c" +
                        " splice join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) on (customerId)",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSubQuery() throws Exception {
        assertQuery(
                "select-choose x, y from (select-choose [x, y] x, y from (select [y, x] from tab t2 where x > 100 latest on ts partition by x) t2 where y > 0) t1",
                "select x, y from (select x, y from tab t2 where x > 100 latest on ts partition by x) t1 where y > 0",
                modelOf("tab").timestamp("ts").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSubQueryAliasWithSpace() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) 'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT)
        );
    }

    @Test
    public void testSubQueryAsArg() throws Exception {
        assertQuery(
                "select-choose customerId from (select [customerId] from customers where (select-choose orderId from (select [orderId] from orders)) > 1)",
                "select * from customers where (select * from orders) > 1",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testSubQueryKeepOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select [x] from a) order by x)",
                "select x from (select * from a order by x)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSubQueryLimitLoHi() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y] from (select-choose [y, x] x, y from (select [y, x, z] from tab where x > z) limit 100,200) _xQdbA1 where x = y) limit 150",
                "(select x x, y y from tab where x > z limit 100,200) where x = y limit 150",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSubQuerySyntaxError() throws Exception {
        assertSyntaxError("select x from (select tab. tab where x > 10 t1)", 26, "'*' or column name expected");
    }

    @Test
    public void testTableListToCrossJoin() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a join select [x] from c on c.x = a.x) a",
                "select a.x from a a, c where a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testTableNameAsArithmetic() throws Exception {
        assertSyntaxError(
                "select x from 'tab' + 1",
                20,
                "function, literal or constant is expected",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testTableNameCannotOpen() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public long getSpinLockTimeout() {
                return 1000;
            }
        };

        assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration, Metrics.disabled());
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                TableModel[] tableModels = new TableModel[]{modelOf("tab").col("x", ColumnType.INT)};
                try {
                    try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, sqlExecutionContext.getWorkerCount(), sqlExecutionContext.getSharedWorkerCount())) {
                        for (int i = 0, n = tableModels.length; i < n; i++) {
                            TestUtils.create(tableModels[i], engine);
                        }
                        compiler.compile("select * from tab", ctx);
                        Assert.fail("Exception expected");
                    } catch (SqlException e) {
                        Assert.assertEquals(14, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                    }
                } finally {
                    for (int i = 0, n = tableModels.length; i < n; i++) {
                        TableModel tableModel = tableModels[i];
                        TableToken tableToken = engine.verifyTableName(tableModel.getName());
                        Path path = tableModel.getPath().of(tableModel.getConfiguration().getRoot()).concat(tableToken).slash$();
                        Assert.assertTrue(configuration.getFilesFacade().rmdir(path));
                        tableModel.close();
                    }
                }
            }
        });
    }

    @Test
    public void testTableNameJustNoRowidMarker() throws Exception {
        assertSyntaxError(
                "select * from '*!*'",
                14,
                "come on"
        );
    }

    @Test
    public void testTableNameLocked() throws Exception {
        assertMemoryLeak(() -> {
            String dirName = "tab" + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
            TableToken tableToken = new TableToken("tab", dirName, 1 + getSystemTablesCount(), false, false, false);
            CharSequence lockedReason = engine.lockAll(tableToken, "testing", true);
            Assert.assertNull(lockedReason);
            try {
                TableModel[] tableModels = new TableModel[]{modelOf("tab").col("x", ColumnType.INT)};
                try {
                    try {
                        for (int i = 0, n = tableModels.length; i < n; i++) {
                            CreateTableTestUtils.create(tableModels[i]);
                        }
                        select("select * from tab");
                        Assert.fail("Exception expected");
                    } catch (SqlException e) {
                        Assert.assertEquals(14, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "table is locked");
                    }
                } finally {
                    for (int i = 0, n = tableModels.length; i < n; i++) {
                        TableModel tableModel = tableModels[i];
                        TableToken tableToken1 = engine.verifyTableName(tableModel.getName());
                        Path path = tableModel.getPath().of(tableModel.getConfiguration().getRoot()).concat(tableToken1).slash$();
                        configuration.getFilesFacade().rmdir(path);
                        tableModel.close();
                    }
                }
            } finally {
                engine.unlock(securityContext, tableToken, null, false);
            }
        });
    }

    @Test
    public void testTableNameReserved() throws Exception {
        try (Path path = new Path()) {
            String dirName = "tab" + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
            configuration.getFilesFacade().touch(path.of(root).concat(dirName).$());
        }

        assertSyntaxError(
                "select * from tab",
                14,
                "table does not exist [table=tab]" // creating folder does not reserve table name anymore
        );
    }

    @Test
    public void testTableNameWithNoRowidMarker() throws SqlException {
        assertQuery(
                "select-choose x from (select [x] from *!*tab)",
                "select * from '*!*tab'",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testTimestampOnSubQuery() throws Exception {
        assertQuery(
                "select-choose x from (select-choose [x] x, y from (select [x, y] from a b where x > y) b) timestamp (x)",
                "select x from (a b) timestamp(x) where x > y",
                modelOf("a").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testTimestampOnTable() throws Exception {
        assertQuery(
                "select-choose x from (select [x, y] from a b timestamp (x) where x > y) b",
                "select x from a b timestamp(x) where x > y",
                modelOf("a")
                        .col("x", ColumnType.TIMESTAMP)
                        .col("y", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testTimestampWithTimezoneCast() throws Exception {
        assertQuery(
                "select-virtual cast(t,timestamp) cast from (select [t] from x)",
                "select cast(t as timestamp with time zone) from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testTimestampWithTimezoneCastInSelect() throws Exception {
        assertQuery(
                "select-virtual cast('2005-04-02 12:00:00-07',timestamp) col from (x)",
                "select cast('2005-04-02 12:00:00-07' as timestamp with time zone) col from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testTimestampWithTimezoneConstPrefix() throws Exception {
        assertQuery(
                "select-choose t, tt from (select [t, tt] from x where t > cast('2005-04-02 12:00:00-07',timestamp))",
                "select * from x where t > timestamp with time zone '2005-04-02 12:00:00-07'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testTimestampWithTimezoneConstPrefixInInsert() throws Exception {
        assertInsertQuery(
                modelOf("test").col("test_timestamp", ColumnType.TIMESTAMP).col("test_value", ColumnType.STRING));
    }

    @Test
    public void testTimestampWithTimezoneConstPrefixInSelect() throws Exception {
        assertQuery(
                "select-virtual cast('2005-04-02 12:00:00-07',timestamp) alias from (x)",
                "select timestamp with time zone '2005-04-02 12:00:00-07' \"alias\" from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testTimestampWithTimezoneConstPrefixInsideCast() throws Exception {
        assertQuery(
                "select-choose t, tt from (select [t, tt] from x where t > cast(cast('2005-04-02 12:00:00-07',timestamp),DATE))",
                "select * from x where t > CAST(timestamp with time zone '2005-04-02 12:00:00-07' as DATE)",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testTooManyArgumentsInWindowFunction() throws Exception {
        assertException(
                "select row_number(1,2,3) over (partition by symbol) from trades",
                "create table trades " +
                        "(" +
                        " price double," +
                        " symbol symbol," +
                        " ts timestamp" +
                        ") timestamp(ts) partition by day",
                7,
                "too many arguments"
        );
    }

    @Test
    public void testTooManyColumnsEdgeInOrderBy() throws Exception {
        try (TableModel model = modelOf("x")) {
            for (int i = 0; i < SqlParser.MAX_ORDER_BY_COLUMNS - 1; i++) {
                model.col("f" + i, ColumnType.INT);
            }
            CreateTableTestUtils.create(model);
        }

        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < SqlParser.MAX_ORDER_BY_COLUMNS - 1; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('f').append(i);
        }
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            QueryModel st = (QueryModel) compiler.testCompileModel(b, sqlExecutionContext);
            Assert.assertEquals(SqlParser.MAX_ORDER_BY_COLUMNS - 1, st.getOrderBy().size());
        }
    }

    @Test
    public void testTooManyColumnsInOrderBy() {
        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < SqlParser.MAX_ORDER_BY_COLUMNS; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('f').append(i);
        }
        try {
            select(b);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertEquals("Too many columns", e.getFlyweightMessage());
        }
    }

    @Test
    public void testTwoWindowColumns() throws Exception {
        assertQuery(
                "select-window a, b, f(c) my over (partition by b order by ts), d(c) d over () " +
                        "from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts) my, d(c) over() from xyz",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testUnbalancedBracketInSubQuery() throws Exception {
        assertSyntaxError("select x from (tab where x > 10 t1", 32, "expected");
    }

    @Test
    public void testUndefinedBindVariables() throws SqlException {
        assertQuery(
                "select-virtual $1 + 1 column, $2 $2, $3 $3 from (long_sequence(10))",
                "select $1+1, $2, $3 from long_sequence(10)"
        );
    }

    @Test
    public void testUnderTerminatedOver() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ts from xyz", 50, "expected");
    }

    @Test
    public void testUnderTerminatedOver2() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ts", 49, "'asc' or 'desc' expected");
    }

    @Test
    public void testUnexpectedDotInCaseStatement() throws Exception {
        assertSyntaxError("SELECT CASE WHEN . THEN 1 ELSE 1 END", 17, "unexpected dot");
        assertSyntaxError("SELECT CASE WHEN True THEN . ELSE 1 END", 27, "unexpected dot");
        assertSyntaxError("SELECT CASE WHEN True THEN 1 ELSE . END", 34, "unexpected dot");

        assertQuery("select-virtual case(True,1.0f,1) case from (long_sequence(1))", "SELECT CASE WHEN True THEN 1.0f ELSE 1 END");
        assertQuery("select-virtual case(True,'a.b','1') case from (long_sequence(1))", "SELECT CASE WHEN True THEN 'a.b' ELSE '1' END");
        assertQuery("select-virtual case(True,x,'1') case from (select [x] from long_sequence(1) a) a", "SELECT CASE WHEN True THEN \"a.x\" ELSE '1' END from long_sequence(1) a");
        assertQuery("select-virtual case(True,x,1) case from (select [x] from long_sequence(1) a) a", "SELECT CASE WHEN True THEN a.x ELSE 1 END from long_sequence(1) a");
    }

    @Test
    public void testUnexpectedTokenInWindowFunction() throws Exception {
        assertSyntaxError("select a,b, f(c) over (by b order by ts) from xyz", 23, "expected");
    }

    @Test
    public void testUnion() throws SqlException {
        assertQuery(
                "select-choose [x] x from (select [x] from a) union select-choose [y] y from (select [y] from b) union select-choose [z] z from (select [z] from c)",
                "select * from a union select * from b union select * from c",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionAllInSubQuery() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select [x] from a) union select-choose [y] y from (select [y] from b) union all select-choose [z] z from (select [z] from c))",
                "select x from (select * from a union select * from b union all select * from c)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionColumnMisSelection() throws SqlException {
        assertQuery(
                "select-virtual ts, futures_price, 0.0 spot_price from (select-group-by [ts, avg(bid_price) futures_price] ts, avg(bid_price) futures_price from (select [ts, bid_price, market_type] from market_updates timestamp (ts) where market_type = 'futures') sample by 1m) union all select-virtual ts, 0.0 futures_price, spot_price from (select-group-by [ts, avg(bid_price) spot_price] ts, avg(bid_price) spot_price from (select [ts, bid_price, market_type] from market_updates timestamp (ts) where market_type = 'spot') sample by 1m)",
                "select \n" +
                        "    ts, \n" +
                        "    avg(bid_price) AS futures_price, \n" +
                        "    0.0 AS spot_price \n" +
                        "FROM market_updates \n" +
                        "WHERE market_type = 'futures' \n" +
                        "SAMPLE BY 1m \n" +
                        "UNION ALL\n" +
                        "SELECT \n" +
                        "    ts, \n" +
                        "    0.0 AS futures_price, \n" +
                        "    avg(bid_price) AS spot_price\n" +
                        "FROM market_updates\n" +
                        "WHERE market_type = 'spot' \n" +
                        "SAMPLE BY 1m",
                modelOf("market_updates")
                        .col("bid_price", ColumnType.DOUBLE)
                        .col("market_type", ColumnType.SYMBOL)
                        .timestamp("ts")
        );
    }

    @Test
    public void testUnionDifferentColumnCount() throws Exception {
        assertSyntaxError(
                "select x from (select * from a union select * from b union all select sum(z) from (c order by t) timestamp(t) sample by 6h) order by x",
                63,
                "queries have different number of columns",
                modelOf("a").col("x", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("b").col("y", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("c").col("z", ColumnType.INT).col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testUnionJoinReorder3() throws Exception {
        assertQuery(
                "select-virtual [1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8] 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8 from (long_sequence(1)) union " +
                        "select-choose [orders.orderId orderId, customers.customerId customerId, shippers.shipper shipper, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1] " +
                        "orders.orderId orderId, customers.customerId customerId, shippers.shipper shipper, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1 " +
                        "from (select [orderId] from orders " +
                        "join select [shipper] from shippers on shippers.shipper = orders.orderId " +
                        "join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = shippers.shipper and d.orderId = orders.orderId " +
                        "join select [productId, supplier] from products on products.productId = d.productId " +
                        "join select [supplier] from suppliers on suppliers.supplier = products.supplier " +
                        "join select [customerId] from customers outer-join-expression 1 = 1)",
                "select 1, 2, 3, 4, 5, 6, 7, 8 from long_sequence(1)" +
                        " union " +
                        "orders" +
                        " left join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testUnionKeepOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x, z] x, z from (select-choose [x, z] x, z from (select [x, z] from a) union select-choose [x, z] x, z from (select [x, z] from b) union all select-choose [x, z] x, z from (select [x, z] from c)) order by z)",
                "select x from (select * from a union select * from b union all select * from c order by z)",
                modelOf("a").col("x", ColumnType.INT).col("z", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT).col("z", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionKeepOrderByIndex() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select-choose [x] x from (select [x] from a) union select-choose [y] y from (select [y] from b) union all select-choose [z] z from (select [z] from c)) order by x)",
                "select x from (select * from a union select * from b union all select * from c order by 1)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionKeepOrderByWhenSampleByPresent() throws SqlException {
        assertQuery(
                "select-choose x from " +
                        "(select-choose [x, t] x, t from (select [x, t] from a) " +
                        "union " +
                        "select-choose [y, t] y, t from (select [y, t] from b) " +
                        "union all " +
                        "select-virtual ['a' k, sum] 'a' k, sum from " +
                        "(select-group-by [sum(z) sum] sum(z) sum from " +
                        "(select-choose [t, z] z, t from (select [t, z] from c) order by t) " +
                        "timestamp (t) sample by 6h)" +
                        ") order by x",
                "select x from " +
                        "(select * from a " +
                        "union " +
                        "select * from b " +
                        "union all " +
                        "select 'a' k, sum(z) from (c order by t) timestamp(t) sample by 6h" +
                        ") order by x",
                modelOf("a").col("x", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("b").col("y", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("c").col("z", ColumnType.INT).col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testUnionMoveWhereIntoSubQuery() throws Exception {
        assertQuery(
                "select-virtual [1 1, 2 2, 3 3] 1 1, 2 2, 3 3 from (long_sequence(1)) " +
                        "union " +
                        "select-choose [c.customerId customerId, o.customerId customerId1, o.x x] c.customerId customerId, o.customerId customerId1, o.x x from " +
                        "(select [customerId] from customers c left join " +
                        "select [customerId, x] from (select-choose [customerId, x] customerId, x from " +
                        "(select [customerId, x] from orders o where x = 10 and customerId = 100) o) o on customerId = c.customerId where customerId = 100) c",
                "select 1, 2, 3 from long_sequence(1)" +
                        " union " +
                        "customers c" +
                        " left join (orders o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.customerId = 100",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testUnionRemoveOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x, z from (select-choose [x, z] x, z from (select [x, z] from a) union select-choose [x, z] x, z from (select [x, z] from b) union all select-choose [x, z] x, z from (select [x, z] from c))) order by x",
                "select x from (select * from a union select * from b union all select * from c order by z) order by x",
                modelOf("a").col("x", ColumnType.INT).col("z", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT).col("z", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionRemoveRedundantOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from " +
                        "(select-choose [x, t] x, t from (select [x, t] from a) " +
                        "union select-choose [y, t] y, t from (select [y, t] from b) " +
                        "union all select-virtual [1 1, sum] 1 1, sum from (select-group-by [sum(z) sum] sum(z) sum " +
                        "from (select-choose [t, z] z, t from (select [t, z] from c) order by t) timestamp (t) sample by 6h)) " +
                        "order by x",
                "select x from (select * from a union select * from b union all select 1, sum(z) from (c order by t, t) timestamp(t) sample by 6h) order by x",
                modelOf("a").col("x", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("b").col("y", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("c").col("z", ColumnType.INT).col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testUnionSansSelect() throws Exception {
        assertQuery(
                "select-choose ts, x from (select-choose [ts, x] ts, x from (select [ts, x] from t1) union all select-choose [ts, x] ts, x from (select [ts, x] from t2))",
                "(t1 union all t2)",
                modelOf("t1").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                modelOf("t2").col("ts", ColumnType.TIMESTAMP).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testUtfStringConstants() throws SqlException {
        assertQuery(
                "select-virtual rnd_str('Raphaël','Léo') rnd_str from (long_sequence(200))",
                "SELECT rnd_str('Raphaël','Léo') FROM long_sequence(200);"
        );
    }

    @Test
    public void testWhereClause() throws Exception {
        assertQuery(
                "select-virtual x, sum + 25 ohoh from (select-group-by [a + b * c x, sum(z) sum] a + b * c x, sum(z) sum from (select [a, c, b, z] from zyzy where a in (0,10) and b = 10))",
                "select a+b*c x, sum(z)+25 ohoh from zyzy where a in (0,10) AND b = 10",
                modelOf("zyzy")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testWhereClauseInsideInSubQuery() throws SqlException {
        assertQuery(
                "select-choose ts, sym, bid, ask from (select [ts, sym, bid, ask] from trades timestamp (ts) where ts = '2010-01-04' and sym in (select-choose sym from (select [sym, isNewSymbol] from symbols where not(isNewSymbol))))",
                "select * from trades where ts='2010-01-04' and sym in (select sym from symbols where NOT isNewSymbol);",
                modelOf("trades")
                        .timestamp("ts")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.DOUBLE)
                        .col("ask", ColumnType.DOUBLE),
                modelOf("symbols")
                        .col("sym", ColumnType.SYMBOL)
                        .col("isNewSymbol", ColumnType.BOOLEAN)
        );
    }

    @Test
    public void testWhereClauseWithInStatement() throws SqlException {
        assertQuery(
                "select-choose sym, bid, ask, ts from (select [sym, bid, ask, ts] from x timestamp (ts) where sym in ('AA','BB')) order by ts desc",
                "select * from x where sym in ('AA', 'BB' ) order by ts desc",
                modelOf("x")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.INT)
                        .col("ask", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testWhereNotSelectedColumn() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, c.weight weight, o.customerId customerId1, o.x x from (select [customerId, weight] from customers c left join select [customerId, x] from (select-choose [customerId, x] customerId, x from (select [customerId, x] from orders o where x = 10) o) o on o.customerId = c.customerId where weight = 100) c",
                "customers c" +
                        " left join (orders o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.weight = 100",
                modelOf("customers").col("customerId", ColumnType.INT).col("weight", ColumnType.DOUBLE),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testWindowFunctionReferencesSameColumnAsVirtual() throws Exception {
        assertQuery(
                "select-window a, b1, f(c) f over (partition by b11 order by ts) from (select-virtual [a, concat(b,'abc') b1, c, b1 b11, ts] a, concat(b,'abc') b1, c, b1 b11, ts from (select-choose [a, b, c, b b1, ts] a, b, c, b b1, ts from (select [a, b, c, ts] from xyz k timestamp (ts)) k) k) k",
                "select a, concat(k.b, 'abc') b1, f(c) over (partition by k.b order by k.ts) from xyz k",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testWindowLiteralAfterFunction() throws Exception {
        assertQuery(
                "select-window a, b1, f(c) f over (partition by b11 order by ts), b from (select-virtual [a, concat(b,'abc') b1, c, b1 b11, ts, b] a, concat(b,'abc') b1, c, b, b1 b11, ts from (select-choose [a, b, c, b b1, ts] a, b, c, b b1, ts from (select [a, b, c, ts] from xyz k timestamp (ts)) k) k) k",
                "select a, concat(k.b, 'abc') b1, f(c) over (partition by k.b order by k.ts), b from xyz k",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testWindowOrderDirection() throws Exception {
        assertQuery(
                "select-window a, b, f(c) my over (partition by b order by ts desc, x, y) from (select-choose [a, b, c, ts, x, y] a, b, c, ts, x, y from (select [a, b, c, ts, x, y] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b order by ts desc, x asc, y) my from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testWindowPartitionByMultiple() throws Exception {
        assertQuery(
                "select-window a, b, f(c) my over (partition by b, a order by ts), d(c) d over () from (select-choose [a, b, c, ts] a, b, c, ts from (select [a, b, c, ts] from xyz timestamp (ts)))",
                "select a,b, f(c) over (partition by b, a order by ts) my, d(c) over() from xyz",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testWithDuplicateName() throws Exception {
        assertSyntaxError(
                "with x as (tab), x as (tab2) x",
                17,
                "duplicate name",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testWithFollowedByInvalidToken() throws Exception {
        assertException("with x as (select * from long_sequence(1)) create", 43, "'select' | 'update' | 'insert' expected");
    }

    @Test
    public void testWithRecursive() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select-choose [a] a from (select [a] from tab)) x) y",
                "with x as (select * from tab)," +
                        " y as (select * from x)" +
                        " select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSelectFrom() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x",
                "with x as (" +
                        " select a from tab" +
                        ") select a from x",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSelectFrom2() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x",
                "with x as (" +
                        " select a from tab" +
                        ") select * from x",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSubquery() throws SqlException {
        assertQuery(
                "select-choose 1 from (select-virtual [1 1] 1 1 from ((select-choose a from (select [a] from tab)) x cross join (select-choose a from (select-choose [a] a from (select [a] from tab)) x) y) x)",
                "with x as (select * from tab)," +
                        " y as (select * from x)" +
                        " select * from (select 1 from x cross join y)",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSyntaxError() throws Exception {
        assertSyntaxError(
                "with x as (" +
                        " select ,a from tab" +
                        ") x",
                19,
                "missing expression",
                modelOf("tab").col("a", ColumnType.INT)

        );
    }

    @Test
    public void testWithTwoAliasesExcept() throws SqlException {
        assertQuery(
                "select-choose [a] a from (select-choose [a] a from (select [a] from tab)) x except select-choose [a] a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab)," +
                        " y as (select * from tab)" +
                        " select * from x except select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithTwoAliasesIntersect() throws SqlException {
        assertQuery(
                "select-choose [a] a from (select-choose [a] a from (select [a] from tab)) x intersect select-choose [a] a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab)," +
                        " y as (select * from tab)" +
                        " select * from x intersect select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithTwoAliasesUnion() throws SqlException {
        assertQuery(
                "select-choose [a] a from (select-choose [a] a from (select [a] from tab)) x union select-choose [a] a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab)," +
                        " y as (select * from tab)" +
                        " select * from x union select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithUnionWith() throws SqlException {
        assertQuery(
                "select-choose [a] a from (select-choose [a] a from (select [a] from tab)) x union select-choose [a] a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab) select * from x union with " +
                        " y as (select * from tab) select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    private void assertCreateTable(String expected, String ddl, TableModel... tableModels) throws SqlException {
        assertModel(expected, ddl, ExecutionModel.CREATE_TABLE, tableModels);
    }

    private void assertIdentifierError(String query) throws Exception {
        assertSyntaxError(query, 11, "identifier");
    }

    private void assertPartitionByOverOrderByAcceptsDirection(String orderInQuery, String orderInModel) throws SqlException {
        assertQuery(
                "select-choose ts, temperature from " +
                        "(select-window [ts, temperature, row_number() rid over (partition by timestamp_floor('y',ts) order by temperature" + orderInModel + ")] ts, temperature, row_number() rid over (partition by timestamp_floor('y',ts) order by temperature" + orderInModel + ") " +
                        "from (select [ts, temperature] from weather) where rid = 0) inq order by ts",
                "select ts, temperature from \n" +
                        "( \n" +
                        "  select ts, temperature,  \n" +
                        "         row_number() over (partition by timestamp_floor('y', ts) order by temperature " + orderInQuery + ")  rid \n" +
                        "  from weather \n" +
                        ") inq \n" +
                        "where rid = 0 \n" +
                        "order by ts\n",
                modelOf("weather").col("ts", ColumnType.TIMESTAMP).col("temperature", ColumnType.FLOAT)
        );
    }

    private void assertWindowSyntaxError(String query, int position, String contains, TableModel... tableModels) throws Exception {
        try {
            refreshTablesInBaseEngine();
            assertMemoryLeak(() -> {
                for (int i = 0, n = tableModels.length; i < n; i++) {
                    CreateTableTestUtils.create(tableModels[i]);
                }
                for (String frameType : frameTypes) {
                    assertException(query.replace("#FRAME", frameType), position, contains, false);
                }
            });
        } finally {
            for (int i = 0, n = tableModels.length; i < n; i++) {
                TableModel tableModel = tableModels[i];
                TableToken tableToken = engine.verifyTableName(tableModel.getName());
                Path path = tableModel.getPath().of(tableModel.getConfiguration().getRoot()).concat(tableToken).slash$();
                configuration.getFilesFacade().rmdir(path);
                tableModel.close();
            }
        }
    }

    protected void assertWindowQuery(String expectedTemplate, String query, TableModel... tableModels) throws SqlException {
        createModelsAndRun(
                () -> {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        for (String frameType : frameTypes) {
                            ExecutionModel model = compiler.testCompileModel(query.replace("#FRAME", frameType), sqlExecutionContext);
                            Assert.assertEquals(model.getModelType(), ExecutionModel.QUERY);
                            sink.clear();
                            ((Sinkable) model).toSink(sink);
                            String expected = expectedTemplate.replace("#FRAME", frameType.trim())
                                    .replace("#UNIT ", "range ".equals(frameType) ? "microsecond " : "");
                            TestUtils.assertEquals(expected, sink);
                            if (model instanceof QueryModel && model.getModelType() == ExecutionModel.QUERY) {
                                validateTopDownColumns((QueryModel) model);
                            }
                        }
                    }
                },
                tableModels
        );
    }

    protected int getSystemTablesCount() {
        return 0;
    }

    @FunctionalInterface
    public interface CairoAware {
        void run() throws SqlException;
    }
}
