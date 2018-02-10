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

package com.questdb.griffin.parser;

import com.questdb.griffin.common.ExprNode;
import com.questdb.griffin.parser.model.AnalyticColumn;
import com.questdb.griffin.parser.model.QueryModel;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static com.questdb.griffin.parser.GriffinParserTestUtils.toRpn;

public class QueryParserTest extends AbstractTest {
    private final QueryParser parser = new QueryParser();

    @Test
    public void testAliasWithSpace() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("x 'b a' where x > 1");
        Assert.assertEquals("b a", statement.getAlias().token);
    }

    @Test
    public void testAliasWithSpace2() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("(x where a > 1) 'b a' where x > 1");
        Assert.assertEquals("b a", statement.getAlias().token);
    }

    @Test
    public void testAliasWithSpacex() {
        try {
            parser.parse("from x 'a b' where x > 1");
        } catch (ParserException e) {
            Assert.assertEquals(7, e.getPosition());
        }
    }

    @Test
    public void testAliasedAnalyticColumn() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a,b, f(c) my over (partition by b order by ts) from xyz");
        Assert.assertEquals(3, statement.getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getColumns().get(2);
        Assert.assertEquals("my", col.getAlias());
        Assert.assertEquals(ExprNode.FUNCTION, col.getAst().type);
        Assert.assertEquals(1, col.getPartitionBy().size());
        Assert.assertEquals("b", col.getPartitionBy().get(0).token);

        Assert.assertEquals(1, col.getOrderBy().size());
        Assert.assertEquals("ts", col.getOrderBy().get(0).token);
    }

    @Test
    public void testAnalyticOrderDirection() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a,b, f(c) my over (partition by b order by ts desc, x asc, y) from xyz");
        Assert.assertEquals(3, statement.getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getColumns().get(2);
        Assert.assertEquals("my", col.getAlias());
        Assert.assertEquals(ExprNode.FUNCTION, col.getAst().type);
        Assert.assertEquals(1, col.getPartitionBy().size());
        Assert.assertEquals("b", col.getPartitionBy().get(0).token);

        Assert.assertEquals(3, col.getOrderBy().size());
        Assert.assertEquals("ts", col.getOrderBy().get(0).token);
        Assert.assertEquals(QueryModel.ORDER_DIRECTION_DESCENDING, col.getOrderByDirection().get(0));
        Assert.assertEquals("x", col.getOrderBy().get(1).token);
        Assert.assertEquals(QueryModel.ORDER_DIRECTION_ASCENDING, col.getOrderByDirection().get(1));
        Assert.assertEquals("y", col.getOrderBy().get(2).token);
        Assert.assertEquals(QueryModel.ORDER_DIRECTION_ASCENDING, col.getOrderByDirection().get(2));
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
        QueryModel statement = (QueryModel) parser.parse("select x from a a cross join b z");
        Assert.assertNotNull(statement);
        Assert.assertEquals("a", statement.getAlias().token);
        Assert.assertEquals(2, statement.getJoinModels().size());
        Assert.assertEquals(QueryModel.JOIN_CROSS, statement.getJoinModels().getQuick(1).getJoinType());
        Assert.assertNull(statement.getJoinModels().getQuick(1).getJoinCriteria());
    }

    @Test
    public void testCrossJoin3() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x from a a " +
                "cross join b z " +
                "join c on a.x = c.x");
        Assert.assertNotNull(statement);
        Assert.assertEquals("a", statement.getAlias().token);
        Assert.assertEquals(3, statement.getJoinModels().size());
        Assert.assertEquals(QueryModel.JOIN_CROSS, statement.getJoinModels().getQuick(1).getJoinType());
        Assert.assertNull(statement.getJoinModels().getQuick(1).getJoinCriteria());
        Assert.assertEquals(QueryModel.JOIN_INNER, statement.getJoinModels().getQuick(2).getJoinType());
        Assert.assertNotNull(statement.getJoinModels().getQuick(2).getJoinCriteria());
    }

    @Test
    public void testCrossJoinNoAlias() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x from a a " +
                "cross join b " +
                "join c on a.x = c.x");
        Assert.assertNotNull(statement);
        Assert.assertEquals("a", statement.getAlias().token);
        Assert.assertEquals(3, statement.getJoinModels().size());
        Assert.assertEquals(QueryModel.JOIN_CROSS, statement.getJoinModels().getQuick(1).getJoinType());
        Assert.assertNull(statement.getJoinModels().getQuick(1).getJoinCriteria());
        Assert.assertEquals(QueryModel.JOIN_INNER, statement.getJoinModels().getQuick(2).getJoinType());
        Assert.assertNotNull(statement.getJoinModels().getQuick(2).getJoinCriteria());
    }

    @Test
    public void testEmptyGroupBy() {
        try {
            parser.parse("select x, y from tab sample by");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(28, e.getPosition());
        }
    }

    @Test
    public void testEmptyOrderBy() {
        try {
            parser.parse("select x, y from tab order by");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(27, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "end of input");
        }
    }

    @Test
    public void testExtraComma2OrderByInAnalyticFunction() {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ts,) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(53, e.getPosition());
        }
    }

    @Test
    public void testExtraCommaOrderByInAnalyticFunction() {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ,ts) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(50, e.getPosition());
        }
    }

    @Test
    public void testExtraCommaPartitionByInAnalyticFunction() {
        try {
            parser.parse("select a,b, f(c) my over (partition by b, order by ts) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(48, e.getPosition());
        }
    }

    @Test
    public void testInnerJoin() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x from a a inner join b on b.x = a.x");
        Assert.assertNotNull(statement);
        Assert.assertEquals("a", statement.getAlias().token);
        Assert.assertEquals(2, statement.getJoinModels().size());
        Assert.assertEquals(QueryModel.JOIN_INNER, statement.getJoinModels().getQuick(1).getJoinType());
        Assert.assertEquals("b.xa.x=", toRpn(statement.getJoinModels().getQuick(1).getJoinCriteria()));
    }

    @Test
    public void testInvalidGroupBy1() {
        try {
            parser.parse("select x, y from tab sample by x,");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(32, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Unexpected");
        }
    }

    @Test
    public void testInvalidGroupBy2() {
        try {
            parser.parse("select x, y from (tab sample by x,)");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(33, e.getPosition());
        }
    }

    @Test
    public void testInvalidGroupBy3() {
        try {
            parser.parse("select x, y from tab sample by x, order by y");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(32, e.getPosition());
        }
    }

    @Test
    public void testInvalidInnerJoin1() {
        try {
            parser.parse("select x from a a inner join b z");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(31, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "'on'");
        }
    }

    @Test
    public void testInvalidInnerJoin2() {
        try {
            parser.parse("select x from a a inner join b z on");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(33, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Expression");
        }
    }

    @Test
    public void testInvalidOrderBy1() {
        try {
            parser.parse("select x, y from tab order by x,");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(31, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "end of input");
        }
    }

    @Test
    public void testInvalidOrderBy2() {
        try {
            parser.parse("select x, y from (tab order by x,)");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(33, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Expression expected");
        }
    }

    @Test
    public void testInvalidOuterJoin1() {
        try {
            parser.parse("select x from a a outer join b z");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(31, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "'on'");
        }
    }

    @Test
    public void testInvalidOuterJoin2() {
        try {
            parser.parse("select x from a a outer join b z on");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(33, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Expression");
        }
    }

    @Test
    public void testInvalidSubQuery() {
        try {
            parser.parse("select x,y from (tab where x = 100) latest by x");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(36, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "latest");
        }

    }

    @Test
    public void testJoin1() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x, y from (select x from tab t2 latest by x where x > 100) t1 " +
                "join tab2 xx2 on tab2.x = t1.x " +
                "join tab3 on xx2.x > tab3.b " +
                "join (select x,y from tab4 latest by z where a > b) x4 on x4.x = t1.y " +
                "where y > 0");

        Assert.assertEquals("t1", statement.getAlias().token);
        Assert.assertEquals(4, statement.getJoinModels().size());
        Assert.assertNotNull(statement.getNestedModel());
        Assert.assertNull(statement.getJournalName());
        Assert.assertEquals("y0>", toRpn(statement.getWhereClause()));
        Assert.assertEquals("tab", toRpn(statement.getNestedModel().getJournalName()));
        Assert.assertEquals("t2", statement.getNestedModel().getAlias().token);
        Assert.assertEquals(1, statement.getNestedModel().getJoinModels().size());

        Assert.assertEquals("xx2", statement.getJoinModels().getQuick(1).getAlias().token);
        Assert.assertNull(statement.getJoinModels().getQuick(2).getAlias());
        Assert.assertEquals("x4", statement.getJoinModels().getQuick(3).getAlias().token);
        Assert.assertNotNull(statement.getJoinModels().getQuick(3).getNestedModel());

        Assert.assertEquals("tab2", toRpn(statement.getJoinModels().getQuick(1).getJournalName()));
        Assert.assertEquals("tab3", toRpn(statement.getJoinModels().getQuick(2).getJournalName()));
        Assert.assertNull(statement.getJoinModels().getQuick(3).getJournalName());

        Assert.assertEquals("tab2.xt1.x=", toRpn(statement.getJoinModels().getQuick(1).getJoinCriteria()));
        Assert.assertEquals("xx2.xtab3.b>", toRpn(statement.getJoinModels().getQuick(2).getJoinCriteria()));
        Assert.assertEquals("x4.xt1.y=", toRpn(statement.getJoinModels().getQuick(3).getJoinCriteria()));

        Assert.assertEquals("ab>", toRpn(statement.getJoinModels().getQuick(3).getNestedModel().getWhereClause()));
        Assert.assertEquals("z", toRpn(statement.getJoinModels().getQuick(3).getNestedModel().getLatestBy()));
    }

    @Test
    public void testJoin2() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x from ((tab join tab2 on tab.x=tab2.x) join tab3 on tab3.x = tab2.x)");
        Assert.assertNotNull(statement);
        Assert.assertEquals(1, statement.getJoinModels().size());
        Assert.assertNotNull(statement.getNestedModel());
        Assert.assertEquals(2, statement.getNestedModel().getJoinModels().size());
        Assert.assertEquals("tab3", toRpn(statement.getNestedModel().getJoinModels().getQuick(1).getJournalName()));
        Assert.assertEquals("tab3.xtab2.x=", toRpn(statement.getNestedModel().getJoinModels().getQuick(1).getJoinCriteria()));
        Assert.assertEquals(0, statement.getNestedModel().getColumns().size());
        Assert.assertNotNull(statement.getNestedModel().getNestedModel());
        Assert.assertEquals("tab", toRpn(statement.getNestedModel().getNestedModel().getJournalName()));
        Assert.assertEquals(2, statement.getNestedModel().getNestedModel().getJoinModels().size());
        Assert.assertEquals("tab2", toRpn(statement.getNestedModel().getNestedModel().getJoinModels().getQuick(1).getJournalName()));
        Assert.assertEquals("tab.xtab2.x=", toRpn(statement.getNestedModel().getNestedModel().getJoinModels().getQuick(1).getJoinCriteria()));
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
        QueryModel statement = (QueryModel) parser.parse("select x, y from (select z from tab t2 latest by x where x > 100) t1 " +
                "where y > 0");
        Assert.assertNotNull(statement);
        Assert.assertNotNull(statement.getNestedModel());
        Assert.assertNull(statement.getJournalName());
        Assert.assertEquals("t1", statement.getAlias().token);

        Assert.assertEquals("tab", toRpn(statement.getNestedModel().getJournalName()));
        Assert.assertEquals("t2", statement.getNestedModel().getAlias().token);
        Assert.assertEquals("x100>", toRpn(statement.getNestedModel().getWhereClause()));
        Assert.assertEquals("x", toRpn(statement.getNestedModel().getLatestBy()));
        Assert.assertEquals(2, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAst().token);
        Assert.assertEquals("y", statement.getColumns().get(1).getAst().token);
        Assert.assertEquals(1, statement.getNestedModel().getColumns().size());
        Assert.assertEquals("z", statement.getNestedModel().getColumns().get(0).getAst().token);
    }

    @Test
    public void testMostRecentWhereClause() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a+b*c x, sum(z)+25 ohoh from zyzy latest by x where a in (x,y) and b = 10");

        // journal name
        Assert.assertEquals("zyzy", statement.getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAlias());
        Assert.assertEquals("ohoh", statement.getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("axyinb10=and", toRpn(statement.getWhereClause()));
        // latest by
        Assert.assertEquals("x", toRpn(statement.getLatestBy()));
    }

    @Test
    public void testMultipleExpressions() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a+b*c x, sum(z)+25 ohoh from zyzy");
        Assert.assertNotNull(statement);
        Assert.assertEquals("zyzy", statement.getJournalName().token);
        Assert.assertEquals(2, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAlias());
        Assert.assertEquals("ohoh", statement.getColumns().get(1).getAlias());
    }

    @Test
    public void testOneAnalyticColumn() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a,b, f(c) over (partition by b order by ts) from xyz");
        Assert.assertEquals(3, statement.getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getColumns().get(2);

        Assert.assertEquals(ExprNode.FUNCTION, col.getAst().type);
        Assert.assertEquals(1, col.getPartitionBy().size());
        Assert.assertEquals("b", col.getPartitionBy().get(0).token);

        Assert.assertEquals(1, col.getOrderBy().size());
        Assert.assertEquals("ts", col.getOrderBy().get(0).token);
    }

    @Test
    public void testOptionalSelect() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("tab t2 latest by x where x > 100");
        Assert.assertNotNull(statement);
        Assert.assertEquals("tab", toRpn(statement.getJournalName()));
        Assert.assertEquals("t2", statement.getAlias().token);
        Assert.assertEquals("x100>", toRpn(statement.getWhereClause()));
        Assert.assertEquals(0, statement.getColumns().size());
        Assert.assertEquals("x", toRpn(statement.getLatestBy()));
    }

    @Test
    public void testOrderBy1() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x,y from tab order by x,y,z");
        Assert.assertNotNull(statement);
        Assert.assertEquals(3, statement.getOrderBy().size());
        Assert.assertEquals("x", toRpn(statement.getOrderBy().getQuick(0)));
        Assert.assertEquals("y", toRpn(statement.getOrderBy().getQuick(1)));
        Assert.assertEquals("z", toRpn(statement.getOrderBy().getQuick(2)));
    }

    @Test
    public void testOrderByExpression() {
        try {
            parser.parse("select x, y from tab order by x+y");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(31, e.getPosition());
        }
    }

    @Test
    public void testOuterJoin() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x from a a outer join b on b.x = a.x");
        Assert.assertNotNull(statement);
        Assert.assertEquals("a", statement.getAlias().token);
        Assert.assertEquals(2, statement.getJoinModels().size());
        Assert.assertEquals(QueryModel.JOIN_OUTER, statement.getJoinModels().getQuick(1).getJoinType());
        Assert.assertEquals("b.xa.x=", toRpn(statement.getJoinModels().getQuick(1).getJoinCriteria()));
    }

    @Test
    public void testSampleBy1() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x,y from tab sample by 2m");
        Assert.assertNotNull(statement);
        Assert.assertEquals("2m", statement.getSampleBy().token);
    }

    @Test
    public void testSelectPlainColumns() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a,b,c from t");

        Assert.assertNotNull(statement);
        Assert.assertEquals("t", statement.getJournalName().token);
        Assert.assertEquals(3, statement.getColumns().size());
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(ExprNode.LITERAL, statement.getColumns().get(i).getAst().type);
        }
    }

    @Test
    public void testSelectSingleExpression() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a+b*c x from t");
        Assert.assertNotNull(statement);
        Assert.assertEquals(1, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAlias());
        Assert.assertEquals("+", statement.getColumns().get(0).getAst().token);
        Assert.assertEquals("t", statement.getJournalName().token);
    }

    @Test
    public void testSimpleSubquery() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("(x) where x > 1");
        Assert.assertNotNull(statement.getNestedModel());
        Assert.assertEquals("x", statement.getNestedModel().getJournalName().token);
    }

    @Test
    public void testSingleJournalLimit() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x x, y y from tab where x > z limit 100");
        // journal name
        Assert.assertEquals("tab", statement.getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("xz>", toRpn(statement.getWhereClause()));
        // limit
        Assert.assertEquals("100", toRpn(statement.getLimitLo()));
    }

    @Test
    public void testSingleJournalLimitLoHi() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x x, y y from tab where x > z limit 100,200");
        // journal name
        Assert.assertEquals("tab", statement.getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("xz>", toRpn(statement.getWhereClause()));
        // limit
        Assert.assertEquals("100", toRpn(statement.getLimitLo()));
        Assert.assertEquals("200", toRpn(statement.getLimitHi()));
    }

    @Test
    public void testSingleJournalLimitLoHiExtraToken() {
        try {
            parser.parse("select x x, y y from tab where x > z limit 100,200 b");
        } catch (ParserException e) {
            Assert.assertEquals(51, e.getPosition());
        }
    }

    @Test
    public void testSingleJournalNoWhereLimit() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x x, y y from tab limit 100");
        // journal name
        Assert.assertEquals("tab", statement.getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getColumns().get(1).getAlias());
        // limit
        Assert.assertEquals("100", toRpn(statement.getLimitLo()));
    }

    @Test
    public void testSubQuery() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x, y from (select x from tab t2 latest by x where x > 100) t1 " +
                "where y > 0");
        Assert.assertNotNull(statement);
        Assert.assertNotNull(statement.getNestedModel());
        Assert.assertNull(statement.getJournalName());
        Assert.assertEquals("t1", statement.getAlias().token);

        Assert.assertEquals("tab", toRpn(statement.getNestedModel().getJournalName()));
        Assert.assertEquals("t2", statement.getNestedModel().getAlias().token);
        Assert.assertEquals("x100>", toRpn(statement.getNestedModel().getWhereClause()));
        Assert.assertEquals("x", toRpn(statement.getNestedModel().getLatestBy()));
    }

    @Test
    public void testSubqueryLimitLoHi() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("(select x x, y y from tab where x > z limit 100,200) where x = y limit 150");
        // journal name
        Assert.assertEquals("tab", statement.getNestedModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getNestedModel().getColumns().size());
        Assert.assertEquals("x", statement.getNestedModel().getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getNestedModel().getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("xz>", toRpn(statement.getNestedModel().getWhereClause()));
        // limit
        Assert.assertEquals("100", toRpn(statement.getNestedModel().getLimitLo()));
        Assert.assertEquals("200", toRpn(statement.getNestedModel().getLimitHi()));

        Assert.assertEquals("150", toRpn(statement.getLimitLo()));
        Assert.assertNull(statement.getLimitHi());
    }

    @Test
    public void testTimestampOnJournal() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x from a b timestamp(x) where x > y");
        Assert.assertEquals("x", statement.getTimestamp().token);
        Assert.assertEquals("b", statement.getAlias().token);
        Assert.assertNotNull(statement.getWhereClause());
    }

    @Test
    public void testTimestampOnSubquery() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select x from (a b) timestamp(x) where x > y");
        Assert.assertEquals("x", statement.getTimestamp().token);
        Assert.assertNotNull(statement.getNestedModel());
        Assert.assertNotNull(statement.getWhereClause());
    }

    @Test
    public void testTooManyColumnsEdgeInOrderBy() throws Exception {
        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < QueryParser.MAX_ORDER_BY_COLUMNS - 1; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('f').append(i);
        }
        QueryModel st = (QueryModel) parser.parse(b);
        Assert.assertEquals(QueryParser.MAX_ORDER_BY_COLUMNS - 1, st.getOrderBy().size());
    }

    @Test
    public void testTooManyColumnsInOrderBy() {
        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < QueryParser.MAX_ORDER_BY_COLUMNS; i++) {
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
        QueryModel statement = (QueryModel) parser.parse("select a,b, f(c) my over (partition by b order by ts), d(c) over() from xyz");
        Assert.assertEquals(4, statement.getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getColumns().get(2);
        Assert.assertEquals("my", col.getAlias());
        Assert.assertEquals(ExprNode.FUNCTION, col.getAst().type);
        Assert.assertEquals(1, col.getPartitionBy().size());
        Assert.assertEquals("b", col.getPartitionBy().get(0).token);
        Assert.assertEquals(1, col.getOrderBy().size());
        Assert.assertEquals("ts", col.getOrderBy().get(0).token);

        col = (AnalyticColumn) statement.getColumns().get(3);
        Assert.assertEquals("d", col.getAst().token);
        Assert.assertNull(col.getAlias());
        Assert.assertEquals(0, col.getPartitionBy().size());
        Assert.assertEquals(0, col.getOrderBy().size());
    }

    @Test
    public void testUnbalancedBracketInSubQuery() {
        try {
            parser.parse("select x from (tab where x > 10 t1");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(32, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "expected");
        }
    }

    @Test
    public void testUnderTerminatedOver() {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ts from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(53, e.getPosition());
        }
    }

    @Test
    public void testUnderTerminatedOver2() {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ts");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(50, e.getPosition());
        }
    }

    @Test
    public void testUnexpectedTokenInAnalyticFunction() {
        try {
            parser.parse("select a,b, f(c) my over (by b order by ts) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(26, e.getPosition());
        }
    }

    @Test
    public void testWhereClause() throws Exception {
        QueryModel statement = (QueryModel) parser.parse("select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 10");
        // journal name
        Assert.assertEquals("zyzy", statement.getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getColumns().size());
        Assert.assertEquals("x", statement.getColumns().get(0).getAlias());
        Assert.assertEquals("ohoh", statement.getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("axyinb10=and", toRpn(statement.getWhereClause()));
    }
}
