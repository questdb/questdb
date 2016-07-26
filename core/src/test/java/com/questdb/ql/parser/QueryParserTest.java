/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.parser;

import com.questdb.ex.ParserException;
import com.questdb.misc.Chars;
import com.questdb.ql.model.*;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class QueryParserTest extends AbstractTest {
    private final QueryParser parser = new QueryParser();

    @Test
    public void testAliasWithSpace() throws Exception {
        Statement statement = parser.parse("x 'b a' where x > 1");
        Assert.assertEquals("b a", statement.getQueryModel().getAlias().token);
    }

    @Test
    public void testAliasWithSpace2() throws Exception {
        Statement statement = parser.parse("(x where a > 1) 'b a' where x > 1");
        Assert.assertEquals("b a", statement.getQueryModel().getAlias().token);
    }

    @Test
    public void testAliasWithSpacex() throws Exception {
        try {
            parser.parse("from x 'a b' where x > 1");
        } catch (ParserException e) {
            Assert.assertEquals(7, QueryError.getPosition());
        }
    }

    @Test
    public void testAliasedAnalyticColumn() throws Exception {
        Statement statement = parser.parse("select a,b, f(c) my over (partition by b order by ts) from xyz");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertEquals(3, statement.getQueryModel().getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getQueryModel().getColumns().get(2);
        Assert.assertEquals("my", col.getAlias());
        Assert.assertEquals(ExprNode.FUNCTION, col.getAst().type);
        Assert.assertEquals(1, col.getPartitionBy().size());
        Assert.assertEquals("b", col.getPartitionBy().get(0).token);

        Assert.assertEquals(1, col.getOrderBy().size());
        Assert.assertEquals("ts", col.getOrderBy().get(0).token);
    }

    @Test
    public void testAnalyticOrderDirection() throws Exception {
        Statement statement = parser.parse("select a,b, f(c) my over (partition by b order by ts desc, x asc, y) from xyz");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertEquals(3, statement.getQueryModel().getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getQueryModel().getColumns().get(2);
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
    public void testCrossJoin() throws Exception {
        try {
            parser.parse("select x from a a cross join b on b.x = a.x");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(31, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "cannot"));
        }
    }

    @Test
    public void testCrossJoin2() throws Exception {
        Statement statement = parser.parse("select x from a a cross join b z");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("a", statement.getQueryModel().getAlias().token);
        Assert.assertEquals(2, statement.getQueryModel().getJoinModels().size());
        Assert.assertEquals(QueryModel.JoinType.CROSS, statement.getQueryModel().getJoinModels().getQuick(1).getJoinType());
        Assert.assertNull(statement.getQueryModel().getJoinModels().getQuick(1).getJoinCriteria());
    }

    @Test
    public void testCrossJoin3() throws Exception {
        Statement statement = parser.parse("select x from a a " +
                "cross join b z " +
                "join c on a.x = c.x");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("a", statement.getQueryModel().getAlias().token);
        Assert.assertEquals(3, statement.getQueryModel().getJoinModels().size());
        Assert.assertEquals(QueryModel.JoinType.CROSS, statement.getQueryModel().getJoinModels().getQuick(1).getJoinType());
        Assert.assertNull(statement.getQueryModel().getJoinModels().getQuick(1).getJoinCriteria());
        Assert.assertEquals(QueryModel.JoinType.INNER, statement.getQueryModel().getJoinModels().getQuick(2).getJoinType());
        Assert.assertNotNull(statement.getQueryModel().getJoinModels().getQuick(2).getJoinCriteria());
    }

    @Test
    public void testCrossJoinNoAlias() throws Exception {
        Statement statement = parser.parse("select x from a a " +
                "cross join b " +
                "join c on a.x = c.x");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("a", statement.getQueryModel().getAlias().token);
        Assert.assertEquals(3, statement.getQueryModel().getJoinModels().size());
        Assert.assertEquals(QueryModel.JoinType.CROSS, statement.getQueryModel().getJoinModels().getQuick(1).getJoinType());
        Assert.assertNull(statement.getQueryModel().getJoinModels().getQuick(1).getJoinCriteria());
        Assert.assertEquals(QueryModel.JoinType.INNER, statement.getQueryModel().getJoinModels().getQuick(2).getJoinType());
        Assert.assertNotNull(statement.getQueryModel().getJoinModels().getQuick(2).getJoinCriteria());
    }

    @Test
    public void testEmptyGroupBy() throws Exception {
        try {
            parser.parse("select x, y from tab sample by");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(28, QueryError.getPosition());
        }
    }

    @Test
    public void testEmptyOrderBy() throws Exception {
        try {
            parser.parse("select x, y from tab order by");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(27, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "end of input"));
        }
    }

    @Test
    public void testExtraComma2OrderByInAnalyticFunction() throws Exception {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ts,) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(53, QueryError.getPosition());
        }
    }

    @Test
    public void testExtraCommaOrderByInAnalyticFunction() throws Exception {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ,ts) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(50, QueryError.getPosition());
        }
    }

    @Test
    public void testExtraCommaPartitionByInAnalyticFunction() throws Exception {
        try {
            parser.parse("select a,b, f(c) my over (partition by b, order by ts) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(48, QueryError.getPosition());
        }
    }

    @Test
    public void testInnerJoin() throws Exception {
        Statement statement = parser.parse("select x from a a inner join b on b.x = a.x");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("a", statement.getQueryModel().getAlias().token);
        Assert.assertEquals(2, statement.getQueryModel().getJoinModels().size());
        Assert.assertEquals(QueryModel.JoinType.INNER, statement.getQueryModel().getJoinModels().getQuick(1).getJoinType());
        Assert.assertEquals("b.xa.x=", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(1).getJoinCriteria()));
    }

    @Test
    public void testInvalidGroupBy1() throws Exception {
        try {
            parser.parse("select x, y from tab sample by x,");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(33, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Unexpected"));
        }
    }

    @Test
    public void testInvalidGroupBy2() throws Exception {
        try {
            parser.parse("select x, y from (tab sample by x,)");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(34, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidGroupBy3() throws Exception {
        try {
            parser.parse("select x, y from tab sample by x, order by y");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(33, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidInnerJoin1() throws Exception {
        try {
            parser.parse("select x from a a inner join b z");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(31, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "'on'"));
        }
    }

    @Test
    public void testInvalidInnerJoin2() throws Exception {
        try {
            parser.parse("select x from a a inner join b z on");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(33, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Expression"));
        }
    }

    @Test
    public void testInvalidOrderBy1() throws Exception {
        try {
            parser.parse("select x, y from tab order by x,");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(32, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "end of input"));
        }
    }

    @Test
    public void testInvalidOrderBy2() throws Exception {
        try {
            parser.parse("select x, y from (tab order by x,)");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(33, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Expression expected"));
        }
    }

    @Test
    public void testInvalidOuterJoin1() throws Exception {
        try {
            parser.parse("select x from a a outer join b z");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(31, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "'on'"));
        }
    }

    @Test
    public void testInvalidOuterJoin2() throws Exception {
        try {
            parser.parse("select x from a a outer join b z on");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(33, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Expression"));
        }
    }

    @Test
    public void testInvalidSubQuery() throws Exception {
        try {
            parser.parse("select x,y from (tab where x = 100) latest by x");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(36, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "latest"));
        }

    }

    @Test
    public void testJoin1() throws Exception {
        Statement statement = parser.parse("select x, y from (select x from tab t2 latest by x where x > 100) t1 " +
                "join tab2 xx2 on tab2.x = t1.x " +
                "join tab3 on xx2.x > tab3.b " +
                "join (select x,y from tab4 latest by z where a > b) x4 on x4.x = t1.y " +
                "where y > 0");

        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertEquals("t1", statement.getQueryModel().getAlias().token);
        Assert.assertEquals(4, statement.getQueryModel().getJoinModels().size());
        Assert.assertNotNull(statement.getQueryModel().getNestedModel());
        Assert.assertNull(statement.getQueryModel().getJournalName());
        Assert.assertEquals("y0>", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
        Assert.assertEquals("tab", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getJournalName()));
        Assert.assertEquals("t2", statement.getQueryModel().getNestedModel().getAlias().token);
        Assert.assertEquals(1, statement.getQueryModel().getNestedModel().getJoinModels().size());

        Assert.assertEquals("xx2", statement.getQueryModel().getJoinModels().getQuick(1).getAlias().token);
        Assert.assertNull(statement.getQueryModel().getJoinModels().getQuick(2).getAlias());
        Assert.assertEquals("x4", statement.getQueryModel().getJoinModels().getQuick(3).getAlias().token);
        Assert.assertNotNull(statement.getQueryModel().getJoinModels().getQuick(3).getNestedModel());

        Assert.assertEquals("tab2", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(1).getJournalName()));
        Assert.assertEquals("tab3", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(2).getJournalName()));
        Assert.assertNull(statement.getQueryModel().getJoinModels().getQuick(3).getJournalName());

        Assert.assertEquals("tab2.xt1.x=", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(1).getJoinCriteria()));
        Assert.assertEquals("xx2.xtab3.b>", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(2).getJoinCriteria()));
        Assert.assertEquals("x4.xt1.y=", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(3).getJoinCriteria()));

        Assert.assertEquals("ab>", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(3).getNestedModel().getWhereClause()));
        Assert.assertEquals("z", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(3).getNestedModel().getLatestBy()));
    }

    @Test
    public void testJoin2() throws Exception {
        Statement statement = parser.parse("select x from ((tab join tab2 on tab.x=tab2.x) join tab3 on tab3.x = tab2.x)");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals(1, statement.getQueryModel().getJoinModels().size());
        Assert.assertNotNull(statement.getQueryModel().getNestedModel());
        Assert.assertEquals(2, statement.getQueryModel().getNestedModel().getJoinModels().size());
        Assert.assertEquals("tab3", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getJoinModels().getQuick(1).getJournalName()));
        Assert.assertEquals("tab3.xtab2.x=", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getJoinModels().getQuick(1).getJoinCriteria()));
        Assert.assertEquals(0, statement.getQueryModel().getNestedModel().getColumns().size());
        Assert.assertNotNull(statement.getQueryModel().getNestedModel().getNestedModel());
        Assert.assertEquals("tab", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getNestedModel().getJournalName()));
        Assert.assertEquals(2, statement.getQueryModel().getNestedModel().getNestedModel().getJoinModels().size());
        Assert.assertEquals("tab2", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getNestedModel().getJoinModels().getQuick(1).getJournalName()));
        Assert.assertEquals("tab.xtab2.x=", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getNestedModel().getJoinModels().getQuick(1).getJoinCriteria()));
    }

    @Test
    public void testLexerReset() throws Exception {

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
                TestUtils.assertEquals("Unexpected token: Date", QueryError.getMessage());
            }
        }
    }

    @Test
    public void testMissingWhere() throws Exception {
        try {
            parser.parse("select id, x + 10, x from tab id ~ 'HBRO'");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(33, QueryError.getPosition());
        }
    }

    @Test
    public void testMixedFieldsSubQuery() throws Exception {
        Statement statement = parser.parse("select x, y from (select z from tab t2 latest by x where x > 100) t1 " +
                "where y > 0");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertNotNull(statement.getQueryModel().getNestedModel());
        Assert.assertNull(statement.getQueryModel().getJournalName());
        Assert.assertEquals("t1", statement.getQueryModel().getAlias().token);

        Assert.assertEquals("tab", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getJournalName()));
        Assert.assertEquals("t2", statement.getQueryModel().getNestedModel().getAlias().token);
        Assert.assertEquals("x100>", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getWhereClause()));
        Assert.assertEquals("x", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getLatestBy()));
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAst().token);
        Assert.assertEquals("y", statement.getQueryModel().getColumns().get(1).getAst().token);
        Assert.assertEquals(1, statement.getQueryModel().getNestedModel().getColumns().size());
        Assert.assertEquals("z", statement.getQueryModel().getNestedModel().getColumns().get(0).getAst().token);
    }

    @Test
    public void testMostRecentWhereClause() throws Exception {
        Statement statement = parser.parse("select a+b*c x, sum(z)+25 ohoh from zyzy latest by x where a in (x,y) and b = 10");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAlias());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("axyinb10=and", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
        // latest by
        Assert.assertEquals("x", TestUtils.toRpn(statement.getQueryModel().getLatestBy()));
    }

    @Test
    public void testMultipleExpressions() throws Exception {
        Statement statement = parser.parse("select a+b*c x, sum(z)+25 ohoh from zyzy");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName().token);
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAlias());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getAlias());
    }

    @Test
    public void testOneAnalyticColumn() throws Exception {
        Statement statement = parser.parse("select a,b, f(c) over (partition by b order by ts) from xyz");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertEquals(3, statement.getQueryModel().getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getQueryModel().getColumns().get(2);

        Assert.assertEquals(ExprNode.FUNCTION, col.getAst().type);
        Assert.assertEquals(1, col.getPartitionBy().size());
        Assert.assertEquals("b", col.getPartitionBy().get(0).token);

        Assert.assertEquals(1, col.getOrderBy().size());
        Assert.assertEquals("ts", col.getOrderBy().get(0).token);
    }

    @Test
    public void testOptionalSelect() throws Exception {
        Statement statement = parser.parse("tab t2 latest by x where x > 100");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("tab", TestUtils.toRpn(statement.getQueryModel().getJournalName()));
        Assert.assertEquals("t2", statement.getQueryModel().getAlias().token);
        Assert.assertEquals("x100>", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
        Assert.assertEquals(0, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", TestUtils.toRpn(statement.getQueryModel().getLatestBy()));
    }

    @Test
    public void testOrderBy1() throws Exception {
        Statement statement = parser.parse("select x,y from tab order by x,y,z");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals(3, statement.getQueryModel().getOrderBy().size());
        Assert.assertEquals("x", TestUtils.toRpn(statement.getQueryModel().getOrderBy().getQuick(0)));
        Assert.assertEquals("y", TestUtils.toRpn(statement.getQueryModel().getOrderBy().getQuick(1)));
        Assert.assertEquals("z", TestUtils.toRpn(statement.getQueryModel().getOrderBy().getQuick(2)));
    }

    @Test
    public void testOrderByExpression() throws Exception {
        try {
            parser.parse("select x, y from tab order by x+y");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(32, QueryError.getPosition());
        }
    }

    @Test
    public void testOuterJoin() throws Exception {
        Statement statement = parser.parse("select x from a a outer join b on b.x = a.x");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("a", statement.getQueryModel().getAlias().token);
        Assert.assertEquals(2, statement.getQueryModel().getJoinModels().size());
        Assert.assertEquals(QueryModel.JoinType.OUTER, statement.getQueryModel().getJoinModels().getQuick(1).getJoinType());
        Assert.assertEquals("b.xa.x=", TestUtils.toRpn(statement.getQueryModel().getJoinModels().getQuick(1).getJoinCriteria()));
    }

    @Test
    public void testSampleBy1() throws Exception {
        Statement statement = parser.parse("select x,y from tab sample by 2m");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("2m", statement.getQueryModel().getSampleBy().token);
    }

    @Test
    public void testSelectPlainColumns() throws Exception {
        Statement statement = parser.parse("select a,b,c from t");

        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("t", statement.getQueryModel().getJournalName().token);
        Assert.assertEquals(3, statement.getQueryModel().getColumns().size());
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(ExprNode.LITERAL, statement.getQueryModel().getColumns().get(i).getAst().type);
        }
    }

    @Test
    public void testSelectSingleExpression() throws Exception {
        Statement statement = parser.parse("select a+b*c x from t");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals(1, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAlias());
        Assert.assertEquals("+", statement.getQueryModel().getColumns().get(0).getAst().token);
        Assert.assertEquals("t", statement.getQueryModel().getJournalName().token);
    }

    @Test
    public void testSimpleSubquery() throws Exception {
        Statement statement = parser.parse("(x) where x > 1");
        Assert.assertNotNull(statement.getQueryModel().getNestedModel());
        Assert.assertEquals("x", statement.getQueryModel().getNestedModel().getJournalName().token);
    }

    @Test
    public void testSingleJournalLimit() throws Exception {
        Statement statement = parser.parse("select x x, y y from tab where x > z limit 100");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("tab", statement.getQueryModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getQueryModel().getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("xz>", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
        // limit
        Assert.assertEquals("100", TestUtils.toRpn(statement.getQueryModel().getLimitLo()));
    }

    @Test
    public void testSingleJournalLimitLoHi() throws Exception {
        Statement statement = parser.parse("select x x, y y from tab where x > z limit 100,200");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("tab", statement.getQueryModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getQueryModel().getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("xz>", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
        // limit
        Assert.assertEquals("100", TestUtils.toRpn(statement.getQueryModel().getLimitLo()));
        Assert.assertEquals("200", TestUtils.toRpn(statement.getQueryModel().getLimitHi()));
    }

    @Test
    public void testSingleJournalLimitLoHiExtraToken() throws Exception {
        try {
            parser.parse("select x x, y y from tab where x > z limit 100,200 b");
        } catch (ParserException e) {
            Assert.assertEquals(51, QueryError.getPosition());
        }
    }

    @Test
    public void testSingleJournalNoWhereLimit() throws Exception {
        Statement statement = parser.parse("select x x, y y from tab limit 100");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("tab", statement.getQueryModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getQueryModel().getColumns().get(1).getAlias());
        // limit
        Assert.assertEquals("100", TestUtils.toRpn(statement.getQueryModel().getLimitLo()));
    }

    @Test
    public void testSubQuery() throws Exception {
        Statement statement = parser.parse("select x, y from (select x from tab t2 latest by x where x > 100) t1 " +
                "where y > 0");
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertNotNull(statement.getQueryModel().getNestedModel());
        Assert.assertNull(statement.getQueryModel().getJournalName());
        Assert.assertEquals("t1", statement.getQueryModel().getAlias().token);

        Assert.assertEquals("tab", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getJournalName()));
        Assert.assertEquals("t2", statement.getQueryModel().getNestedModel().getAlias().token);
        Assert.assertEquals("x100>", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getWhereClause()));
        Assert.assertEquals("x", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getLatestBy()));
    }

    @Test
    public void testSubqueryLimitLoHi() throws Exception {
        Statement statement = parser.parse("(select x x, y y from tab where x > z limit 100,200) where x = y limit 150");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("tab", statement.getQueryModel().getNestedModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getNestedModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getNestedModel().getColumns().get(0).getAlias());
        Assert.assertEquals("y", statement.getQueryModel().getNestedModel().getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("xz>", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getWhereClause()));
        // limit
        Assert.assertEquals("100", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getLimitLo()));
        Assert.assertEquals("200", TestUtils.toRpn(statement.getQueryModel().getNestedModel().getLimitHi()));

        Assert.assertEquals("150", TestUtils.toRpn(statement.getQueryModel().getLimitLo()));
        Assert.assertNull(statement.getQueryModel().getLimitHi());
    }

    @Test
    public void testTimestampOnJournal() throws Exception {
        Statement statement = parser.parse("select x from a b timestamp(x) where x > y");
        Assert.assertEquals("x", statement.getQueryModel().getTimestamp().token);
        Assert.assertEquals("b", statement.getQueryModel().getAlias().token);
        Assert.assertNotNull(statement.getQueryModel().getWhereClause());
    }

    @Test
    public void testTimestampOnSubquery() throws Exception {
        Statement statement = parser.parse("select x from (a b) timestamp(x) where x > y");
        Assert.assertEquals("x", statement.getQueryModel().getTimestamp().token);
        Assert.assertNotNull(statement.getQueryModel().getNestedModel());
        Assert.assertNotNull(statement.getQueryModel().getWhereClause());
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
        Statement st = parser.parse(b);
        Assert.assertEquals(QueryParser.MAX_ORDER_BY_COLUMNS - 1, st.getQueryModel().getOrderBy().size());
    }

    @Test
    public void testTooManyColumnsInOrderBy() throws Exception {
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
            TestUtils.assertEquals("Too many columns", QueryError.getMessage());
        }
    }

    @Test
    public void testTwoAnalyticColumns() throws Exception {
        Statement statement = parser.parse("select a,b, f(c) my over (partition by b order by ts), d(c) over() from xyz");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertEquals(4, statement.getQueryModel().getColumns().size());

        AnalyticColumn col = (AnalyticColumn) statement.getQueryModel().getColumns().get(2);
        Assert.assertEquals("my", col.getAlias());
        Assert.assertEquals(ExprNode.FUNCTION, col.getAst().type);
        Assert.assertEquals(1, col.getPartitionBy().size());
        Assert.assertEquals("b", col.getPartitionBy().get(0).token);
        Assert.assertEquals(1, col.getOrderBy().size());
        Assert.assertEquals("ts", col.getOrderBy().get(0).token);

        col = (AnalyticColumn) statement.getQueryModel().getColumns().get(3);
        Assert.assertEquals("d", col.getAst().token);
        Assert.assertNull(col.getAlias());
        Assert.assertEquals(0, col.getPartitionBy().size());
        Assert.assertEquals(0, col.getOrderBy().size());
    }

    @Test
    public void testUnbalancedBracketInSubQuery() throws Exception {
        try {
            parser.parse("select x from (tab where x > 10 t1");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(32, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "expected"));
        }
    }

    @Test
    public void testUnderTerminatedOver() throws Exception {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ts from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(53, QueryError.getPosition());
        }
    }

    @Test
    public void testUnderTerminatedOver2() throws Exception {
        try {
            parser.parse("select a,b, f(c) my over (partition by b order by ts");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(50, QueryError.getPosition());
        }
    }

    @Test
    public void testUnexpectedTokenInAnalyticFunction() throws Exception {
        try {
            parser.parse("select a,b, f(c) my over (by b order by ts) from xyz");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(26, QueryError.getPosition());
        }
    }

    @Test
    public void testWhereClause() throws Exception {
        Statement statement = parser.parse("select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 10");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getAlias());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getAlias());
        // where
        Assert.assertEquals("axyinb10=and", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
    }
}
