/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.ql.model.Statement;
import com.nfsdb.ql.model.StatementType;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class QueryParserTest extends AbstractTest {
    private final QueryParser parser = new QueryParser();

    @Test
    public void testJoin1() throws Exception {
        Statement statement = parse("select x, y from (select x from tab t2 latest by x where x > 100) t1 " +
                "join tab2 xx2 on tab2.x = t1.x " +
                "join tab3 on xx2.x > tab3.b " +
                "join (select x,y from tab4 latest by z where a > b) x4 on x4.x = t1.y " +
                "where y > 0");
        System.out.println(statement);
    }

    @Test
    public void testMostRecentWhereClause() throws Exception {
        QueryParser parser = new QueryParser();
        parser.setContent("select a+b*c x, sum(z)+25 ohoh from zyzy latest by x where a in (x,y) and b = 10");
        Statement statement = parser.parse();
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getName());
        // where
        Assert.assertEquals("axyinb10=and", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
        // latest by
        Assert.assertEquals("x", TestUtils.toRpn(statement.getQueryModel().getLatestBy()));
    }

    @Test
    public void testMultipleExpressions() throws Exception {
        QueryParser parser = new QueryParser();
        parser.setContent("select a+b*c x, sum(z)+25 ohoh from zyzy");
        Statement statement = parser.parse();
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName().token);
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getName());
    }

    @Test
    public void testSelectPlainColumns() throws Exception {
        Statement statement = parse("select a,b,c from t");

        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("t", statement.getQueryModel().getJournalName().token);
        Assert.assertEquals(3, statement.getQueryModel().getColumns().size());
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(ExprNode.NodeType.LITERAL, statement.getQueryModel().getColumns().get(i).getAst().type);
        }
    }

    @Test
    public void testSelectSingleExpression() throws Exception {
        Statement statement = parse("select a+b*c x from t");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals(1, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("+", statement.getQueryModel().getColumns().get(0).getAst().token);
        Assert.assertEquals("t", statement.getQueryModel().getJournalName().token);
    }

    @Test
    public void testSubQuery() throws Exception {
        Statement statement = parse("select x, y from (select x from tab t2 latest by x where x > 100) t1 " +
                "where y > 0");
        System.out.println(statement);
    }

    @Test
    public void testWhereClause() throws Exception {
        Statement statement = parse("select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 10");
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName().token);
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getName());
        // where
        Assert.assertEquals("axyinb10=and", TestUtils.toRpn(statement.getQueryModel().getWhereClause()));
    }

    private Statement parse(CharSequence query) throws ParserException {
        parser.setContent(query);
        return parser.parse();
    }
}
