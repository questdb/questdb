/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.lexer;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.CairoTestUtils;
import com.questdb.cairo.Engine;
import com.questdb.cairo.TableModel;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.griffin.lexer.model.CreateTableModel;
import com.questdb.griffin.lexer.model.ParsedModel;
import com.questdb.griffin.lexer.model.QueryModel;
import com.questdb.std.Files;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;

public class SqlLexerOptimiserTest extends AbstractCairoTest {
    private final static CairoEngine engine = new Engine(configuration);
    private final static SqlLexerOptimiser parser = new SqlLexerOptimiser(engine, configuration);

    @AfterClass
    public static void tearDown() throws IOException {
        engine.close();
    }

    @Test
    public void testAliasWithSpace() throws Exception {
        assertModel("x 'b a' where x > 1",
                "x 'b a' where x > 1",
                modelOf("x").col("x", ColumnType.INT));
    }

    @Test
    public void testAliasWithSpaceX() {
        assertSyntaxError("from x 'a b' where x > 1", 7, "Unexpected");
    }

    @Test
    public void testAnalyticOrderDirection() throws Exception {
        assertModel(
                "select a, b, f(c) my over (partition by b order by ts desc, x, y) from (xyz)",
                "select a,b, f(c) my over (partition by b order by ts desc, x asc, y) from xyz",
                modelOf("xyz").col("x", ColumnType.INT));
    }

    @Test
    public void testCreateTable() throws ParserException {
        final String sql = "create table x (" +
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
                "partition by MONTH " +
                "record hint 100";

        String expectedNames[] = {"a", "b", "c", "d", "e", "f", "g", "h", "t", "x", "z", "y"};
        int expectedTypes[] = {
                ColumnType.INT,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.DATE,
                ColumnType.BINARY,
                ColumnType.TIMESTAMP,
                ColumnType.SYMBOL,
                ColumnType.STRING,
                ColumnType.BOOLEAN
        };

        String expectedIndexColumns[] = {};
        int expectedBlockSizes[] = {};

        assertTableModel(sql, expectedNames, expectedTypes, expectedIndexColumns, expectedBlockSizes, 8, "MONTH");
    }

    @Test
    public void testCreateTableInPlaceIndex() throws ParserException {
        final String sql = "create table x (" +
                "a INT, " +
                "b BYTE, " +
                "c SHORT, " +
                "d LONG index, " + // <-- index here
                "e FLOAT, " +
                "f DOUBLE, " +
                "g DATE, " +
                "h BINARY, " +
                "t TIMESTAMP, " +
                "x SYMBOL index, " + // <-- index here
                "z STRING, " +
                "y BOOLEAN) " +
                "timestamp(t) " +
                "partition by YEAR " +
                "record hint 100";

        String expectedNames[] = {"a", "b", "c", "d", "e", "f", "g", "h", "t", "x", "z", "y"};
        int expectedTypes[] = {
                ColumnType.INT,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.DATE,
                ColumnType.BINARY,
                ColumnType.TIMESTAMP,
                ColumnType.SYMBOL,
                ColumnType.STRING,
                ColumnType.BOOLEAN
        };

        String expectedIndexColumns[] = {"d", "x"};
        int expectedBlockSizes[] = {configuration.getIndexValueBlockSize(), configuration.getIndexValueBlockSize()};
        assertTableModel(sql, expectedNames, expectedTypes, expectedIndexColumns, expectedBlockSizes, 8, "YEAR");
    }

    @Test
    public void testCreateTableInPlaceIndexAndBlockSize() throws ParserException {
        final String sql = "create table x (" +
                "a INT index block size 16, " +
                "b BYTE, " +
                "c SHORT, " +
                "t TIMESTAMP, " +
                "d LONG, " +
                "e FLOAT, " +
                "f DOUBLE, " +
                "g DATE, " +
                "h BINARY, " +
                "x SYMBOL index block size 128, " +
                "z STRING, " +
                "y BOOLEAN) " +
                "timestamp(t) " +
                "partition by MONTH " +
                "record hint 100";

        String expectedNames[] = {"a", "b", "c", "t", "d", "e", "f", "g", "h", "x", "z", "y"};
        int expectedTypes[] = {
                ColumnType.INT,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.TIMESTAMP,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.DATE,
                ColumnType.BINARY,
                ColumnType.SYMBOL,
                ColumnType.STRING,
                ColumnType.BOOLEAN
        };
        String expectedIndexColumns[] = {"a", "x"};
        int expectedBlockSizes[] = {16, 128};
        assertTableModel(sql, expectedNames, expectedTypes, expectedIndexColumns, expectedBlockSizes, 3, "MONTH");
    }

    @Test
    public void testCreateTableMissingDef() {
        assertSyntaxError("create table xyx", 13, "Unexpected");
    }

    @Test
    public void testCreateTableMissingName() {
        assertSyntaxError("create table ", 12, "Unexpected");
    }

    @Test
    public void testCreateTableWithIndex() throws ParserException {

        final String sql = "create table x (" +
                "a INT index block size 16, " +
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
                "y BOOLEAN)" +
                ", index(x) " +
                "timestamp(t) " +
                "partition by MONTH " +
                "record hint 100";

        String expectedNames[] = {"a", "b", "c", "d", "e", "f", "g", "h", "t", "x", "z", "y"};
        int expectedTypes[] = {
                ColumnType.INT,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.DATE,
                ColumnType.BINARY,
                ColumnType.TIMESTAMP,
                ColumnType.SYMBOL,
                ColumnType.STRING,
                ColumnType.BOOLEAN
        };
        String expectedIndexColumns[] = {"a", "x"};
        int expectedBlockSizes[] = {16, configuration.getIndexValueBlockSize()};
        assertTableModel(sql, expectedNames, expectedTypes, expectedIndexColumns, expectedBlockSizes, 8, "MONTH");
    }

    @Test
    public void testCreateUnsupported() {
        assertSyntaxError("create object x", 7, "table");
    }

    @Test
    public void testCrossJoin() {
        try {
            parser.parse("select x from a a cross join b on b.x = a.x");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(31, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "cannot");
        }
    }

    @Test
    public void testCrossJoin2() throws Exception {
        assertModel(
                "select x from (a a cross join b z)",
                "select x from a a cross join b z",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT));
    }

    @Test
    public void testCrossJoin3() throws Exception {
        assertModel(
                "select x from (a a join c on a.x = c.x cross join b z)",
                "select x from a a " +
                        "cross join b z " +
                        "join c on a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testCrossJoinNoAlias() throws Exception {
        assertModel("select x from (a a join c on a.x = c.x cross join b)",
                "select x from a a " +
                        "cross join b " +
                        "join c on a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT));
    }

    @Test
    public void testDisallowDotInColumnAlias() {
        assertSyntaxError("select x x.y, y from tab order by x", 9, "not allowed");
    }

    @Test
    public void testEmptyGroupBy() {
        assertSyntaxError("select x, y from tab sample by", 28, "end of input");
    }

    @Test
    public void testEmptyOrderBy() {
        assertSyntaxError("select x, y from tab order by", 27, "end of input");
    }

    @Test
    public void testExtraCommaOrderByInAnalyticFunction() {
        assertSyntaxError("select a,b, f(c) my over (partition by b order by ,ts) from xyz", 50, "literal");
    }

    @Test
    public void testExtraComma2OrderByInAnalyticFunction() {
        assertSyntaxError("select a,b, f(c) my over (partition by b order by ts,) from xyz", 53, "literal expected");
    }

    @Test
    public void testExtraCommaPartitionByInAnalyticFunction() {
        assertSyntaxError("select a,b, f(c) my over (partition by b, order by ts) from xyz", 48, ") expected");
    }

    @Test
    public void testInnerJoin() throws Exception {
        assertModel(
                "select x from (a a join b on b.x = a.x)",
                "select x from a a inner join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidGroupBy1() {
        assertSyntaxError("select x, y from tab sample by x,", 32, "Unexpected");
    }

    @Test
    public void testInvalidGroupBy2() {
        assertSyntaxError("select x, y from (tab sample by x,)", 33, "')' expected");
    }

    @Test
    public void testInvalidGroupBy3() {
        assertSyntaxError("select x, y from tab sample by x, order by y", 32, "Unexpected token: ,");
    }

    @Test
    public void testInvalidInnerJoin1() {
        assertSyntaxError("select x from a a inner join b z", 31, "'on'");
    }

    @Test
    public void testInvalidInnerJoin2() {
        assertSyntaxError("select x from a a inner join b z on", 33, "Expression");
    }

    @Test
    public void testInvalidOrderBy1() {
        assertSyntaxError("select x, y from tab order by x,", 31, "end of input");
    }

    @Test
    public void testInvalidOrderBy2() {
        assertSyntaxError("select x, y from (tab order by x,)", 33, "Expression expected");
    }

    @Test
    public void testInvalidOuterJoin1() {
        assertSyntaxError("select x from a a outer join b z", 31, "'on'");
    }

    @Test
    public void testInvalidOuterJoin2() {
        assertSyntaxError("select x from a a outer join b z on", 33, "Expression");
    }

    @Test
    public void testInvalidSubQuery() {
        assertSyntaxError("select x,y from (tab where x = 100) latest by x", 36, "latest");
    }

    @Test
    public void testJoin1() throws Exception {
        assertModel(
                "select x, y from ((select x from (tab t2 latest by x where x > 100)) t1 join tab2 xx2 on xx2.x = t1.x join (select x, y from (tab4 latest by z where a > b and y > 0)) x4 on x4.x = t1.x cross join tab3 on xx2.x > tab3.b)",
                "select x, y from (select x from tab t2 latest by x where x > 100) t1 " +
                        "join tab2 xx2 on xx2.x = t1.x " +
                        "join tab3 on xx2.x > tab3.b " +
                        "join (select x,y from tab4 latest by z where a > b) x4 on x4.x = t1.x " +
                        "where y > 0",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT),
                modelOf("tab3").col("b", ColumnType.INT),
                modelOf("tab4").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.INT).col("a", ColumnType.INT));
    }

    @Test
    public void testJoin2() throws Exception {
        assertModel(
                "select x from (((select tab2.x x from (tab join tab2 on tab.x = tab2.x)) t join tab3 on tab3.x = t.x))",
                "select x from ((select tab2.x from tab join tab2 on tab.x=tab2.x) t join tab3 on tab3.x = t.x)",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT),
                modelOf("tab3").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testLexerReset() {

        for (int i = 0; i < 10; i++) {
            try {
                parser.parse("select \n" +
                        "-- ltod(Date)\n" +
                        "count() \n" +
                        "-- from acc\n" +
                        "from acc(Date) sample by 1d\n" +
                        "-- where x = 10\n");
                Assert.fail();
            } catch (ParserException e) {
                TestUtils.assertEquals("Unexpected token: Date", e.getFlyweightMessage());
            }
        }
    }

    @Test
    public void testMissingWhere() {
        try {
            parser.parse("select id, x + 10, x from tab id ~ 'HBRO'");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(33, e.getPosition());
        }
    }

    @Test
    public void testMixedFieldsSubQuery() throws Exception {
        assertModel(
                "select x, y from ((select z from (tab t2 latest by x where x > 100)) t1 where y > 0)",
                "select x, y from (select z from tab t2 latest by x where x > 100) t1 where y > 0",
                modelOf("tab").col("x", ColumnType.INT));
    }

    @Test
    public void testMostRecentWhereClause() throws Exception {
        assertModel(
                "select a + b * c x, sum(z) + 25 ohoh from (zyzy latest by x where in(y,x,a) and b = 10)",
                "select a+b*c x, sum(z)+25 ohoh from zyzy latest by x where a in (x,y) and b = 10",
                modelOf("zyzy").col("a", ColumnType.INT).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testMultipleExpressions() throws Exception {
        assertModel(
                "select a + b * c x, sum(z) + 25 ohoh from (zyzy)",
                "select a+b*c x, sum(z)+25 ohoh from zyzy",
                modelOf("zyzy").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testOneAnalyticColumn() throws Exception {
        assertModel(
                "select a, b, f(c) over (partition by b order by ts) from (xyz)",
                "select a,b, f(c) over (partition by b order by ts) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
        );
    }

    @Test
    public void testOptionalSelect() throws Exception {
        assertModel(
                "tab t2 latest by x where x > 100",
                "tab t2 latest by x where x > 100",
                modelOf("tab").col("x", ColumnType.INT));
    }

    @Test
    public void testOrderBy1() throws Exception {
        assertModel(
                "select x, y from (tab order by x, y, z)",
                "select x,y from tab order by x,y,z",
                modelOf("tab").col("x", ColumnType.INT)
        );

    }

    @Test
    public void testOrderByExpression() {
        assertSyntaxError("select x, y from tab order by x+y", 31, "Unexpected");
    }

    @Test
    public void testOuterJoin() throws Exception {
        assertModel(
                "select x from (a a outer join b on b.x = a.x)",
                "select x from a a outer join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSampleBy1() throws Exception {
        assertModel(
                "select x, y from (tab sample by 2m)",
                "select x,y from tab sample by 2m",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectPlainColumns() throws Exception {
        assertModel(
                "select a, b, c from (t)",
                "select a,b,c from t",
                modelOf("t").col("a", ColumnType.INT).col("b", ColumnType.INT).col("c", ColumnType.INT)
        );
    }

    @Test
    public void testSelectSingleExpression() throws Exception {
        assertModel(
                "select a + b * c x from (t)",
                "select a+b*c x from t",
                modelOf("t").col("a", ColumnType.INT).col("b", ColumnType.INT).col("c", ColumnType.INT));
    }

    @Test
    public void testSimpleSubQuery() throws Exception {
        assertModel("(x where y > 1)", "(x) where y > 1", modelOf("x").col("y", ColumnType.INT));
    }

    @Test
    public void testSingleJournalLimit() throws Exception {
        assertModel(
                "select x x, y y from (tab where x > z limit 100)",
                "select x x, y y from tab where x > z limit 100",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSingleJournalLimitLoHi() throws Exception {
        assertModel(
                "select x x, y y from (tab where x > z limit 100,200)",
                "select x x, y y from tab where x > z limit 100,200",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSingleJournalLimitLoHiExtraToken() {
        assertSyntaxError("select x x, y y from tab where x > z limit 100,200 b", 51, "Unexpected");
    }

    @Test
    public void testSingleJournalNoWhereLimit() throws Exception {
        assertModel(
                "select x x, y y from (tab limit 100)",
                "select x x, y y from tab limit 100",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT));
    }

    @Test
    public void testSubQuery() throws Exception {
        assertModel(
                "select x, y from ((select x from (tab t2 latest by x where x > 100)) t1 where y > 0)",
                "select x, y from (select x from tab t2 latest by x where x > 100) t1 where y > 0",
                modelOf("tab").col("x", ColumnType.INT)
        );

    }

    @Test
    public void testSubQueryAliasWithSpace() throws Exception {
        assertModel(
                "(x where a > 1 and x > 1) 'b a'",
                "(x where a > 1) 'b a' where x > 1",
                modelOf("x").col("x", ColumnType.INT));
    }

    @Test
    public void testSubqueryLimitLoHi() throws Exception {
        assertModel(
                "(select x x, y y from (tab where x > z and x = y limit 100,200)) limit 150",
                "(select x x, y y from tab where x > z limit 100,200) where x = y limit 150",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testTimestampOnJournal() throws Exception {
        assertModel(
                "select x from (a b timestamp (x) where x > y)",
                "select x from a b timestamp(x) where x > y",
                modelOf("a").col("x", ColumnType.TIMESTAMP));
    }

    @Test
    public void testTimestampOnSubQuery() throws Exception {
        assertModel("select x from ((a b where x > y) timestamp (x))",
                "select x from (a b) timestamp(x) where x > y",
                modelOf("a").col("x", ColumnType.INT).col("y", ColumnType.INT));
    }

    @Test
    public void testTooManyColumnsEdgeInOrderBy() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            model.col("f0", ColumnType.INT);
            CairoTestUtils.create(model);
        }

        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < SqlLexerOptimiser.MAX_ORDER_BY_COLUMNS - 1; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('f').append(i);
        }
        QueryModel st = (QueryModel) parser.parse(b);
        Assert.assertEquals(SqlLexerOptimiser.MAX_ORDER_BY_COLUMNS - 1, st.getOrderBy().size());
    }

    @Test
    public void testTooManyColumnsInOrderBy() {
        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < SqlLexerOptimiser.MAX_ORDER_BY_COLUMNS; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('f').append(i);
        }
        try {
            parser.parse(b);
        } catch (ParserException e) {
            TestUtils.assertEquals("Too many columns", e.getFlyweightMessage());
        }
    }

    @Test
    public void testTwoAnalyticColumns() throws Exception {
        assertModel(
                "select a, b, f(c) my over (partition by b order by ts), d(c) over () from (xyz)",
                "select a,b, f(c) my over (partition by b order by ts), d(c) over() from xyz",
                modelOf("xyz").col("c", ColumnType.INT)
        );
    }

    @Test
    public void testUnbalancedBracketInSubQuery() {
        assertSyntaxError("select x from (tab where x > 10 t1", 32, "expected");
    }

    @Test
    public void testUnderTerminatedOver() {
        assertSyntaxError("select a,b, f(c) my over (partition by b order by ts from xyz", 53, "expected");
    }

    @Test
    public void testUnderTerminatedOver2() {
        assertSyntaxError("select a,b, f(c) my over (partition by b order by ts", 50, "Unexpected");
    }

    @Test
    public void testUnexpectedTokenInAnalyticFunction() {
        assertSyntaxError("select a,b, f(c) my over (by b order by ts) from xyz", 26, "expected");
    }

    @Test
    public void testWhereClause() throws Exception {
        assertModel(
                "select a + b * c x, sum(z) + 25 ohoh from (zyzy where in(y,x,a) and b = 10)",
                "select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 10",
                modelOf("zyzy").col("a", ColumnType.INT).col("b", ColumnType.INT));
    }

    private void assertModel(String expected, String query, TableModel... tableModels) throws ParserException {
        try {
            for (int i = 0, n = tableModels.length; i < n; i++) {
                CairoTestUtils.create(tableModels[i]);
            }
            sink.clear();
            ((QueryModel) parser.parse(query)).toSink(sink);
            TestUtils.assertEquals(expected, sink);
        } finally {
            Assert.assertTrue(engine.releaseAllReaders());
            for (int i = 0, n = tableModels.length; i < n; i++) {
                TableModel tableModel = tableModels[i];
                Path path = tableModel.getPath().of(tableModel.getCairoCfg().getRoot()).concat(tableModel.getName()).put(Files.SEPARATOR).$();
                Assert.assertTrue(configuration.getFilesFacade().rmdir(path));
                tableModel.close();
            }
        }
    }

    private void assertSyntaxError(String query, int position, String contains) {
        try {
            parser.parse(query);
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertContains(e.getMessage(), contains);
        }
    }

    private void assertTableModel(
            String sql,
            String[] expectedNames,
            int[] expectedTypes,
            String[] columnsWithIndexes,
            int[] expectedBlockSizes,
            int expectedTimestampIndex,
            String expectedPartitionBy) throws ParserException {

        ParsedModel model = parser.parse(sql);
        Assert.assertTrue(model instanceof CreateTableModel);

        final CreateTableModel m = (CreateTableModel) model;

        Assert.assertEquals(expectedPartitionBy, m.getPartitionBy().token);
        Assert.assertEquals(expectedNames.length, m.getColumnCount());

        // check indexes
        HashSet<String> indexed = new HashSet<>();
        for (int i = 0, n = columnsWithIndexes.length; i < n; i++) {
            int index = m.getColumnIndex(columnsWithIndexes[i]);

            Assert.assertNotEquals(-1, index);
            Assert.assertTrue(m.getIndexedFlag(index));
            Assert.assertEquals(expectedBlockSizes[i], m.getIndexBlockCapacity(index));
            indexed.add(columnsWithIndexes[i]);
        }

        for (int i = 0, n = m.getColumnCount(); i < n; i++) {
            Assert.assertEquals(expectedNames[i], m.getColumnName(i));
            Assert.assertEquals(expectedTypes[i], m.getColumnType(i));
            // assert that non-indexed columns are correctly reflected in model
            if (!indexed.contains(expectedNames[i])) {
                Assert.assertFalse(m.getIndexedFlag(i));
                Assert.assertEquals(0, m.getIndexBlockCapacity(i));
            }
        }
        Assert.assertEquals(expectedTimestampIndex, m.getColumnIndex(m.getTimestamp().token));
    }

    private TableModel modelOf(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.NONE);
    }
}
