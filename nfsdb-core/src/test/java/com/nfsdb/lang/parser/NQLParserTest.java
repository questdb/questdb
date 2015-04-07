/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.parser;

import com.nfsdb.lang.ast.ExprNode;
import com.nfsdb.lang.ast.Statement;
import com.nfsdb.lang.ast.StatementType;
import org.junit.Assert;
import org.junit.Test;

public class NQLParserTest {
    @Test
    public void testMostRecentWhereClause() throws Exception {
        NQLParser parser = new NQLParser();
        parser.setContent("select a+b*c x, sum(z)+25 ohoh from zyzy most recent by x where a in (x,y), b = 10");
        Statement statement = parser.parse();
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName());
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getName());
        // where
        Assert.assertEquals(2, statement.getQueryModel().getWhereClauses().size());
        Assert.assertEquals("in", statement.getQueryModel().getWhereClauses().get(0).token);
        Assert.assertEquals("=", statement.getQueryModel().getWhereClauses().get(1).token);
        // most recent by
        Assert.assertNotNull(statement.getQueryModel().getMostRecentBy());
        Assert.assertEquals("x", statement.getQueryModel().getMostRecentBy().token);
        Assert.assertEquals(ExprNode.NodeType.LITERAL, statement.getQueryModel().getMostRecentBy().type);
        Assert.assertFalse(statement.getQueryModel().isMostRecentByPostfix());
    }

    @Test
    public void testMultipleExpressions() throws Exception {
        NQLParser parser = new NQLParser();
        parser.setContent("select a+b*c x, sum(z)+25 ohoh from zyzy");
        Statement statement = parser.parse();
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName());
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getName());
    }

    @Test
    public void testSelectPlainColumns() throws Exception {
        NQLParser parser = new NQLParser();
        parser.setContent("select a,b,c from t");
        Statement statement = parser.parse();

        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals("t", statement.getQueryModel().getJournalName());
        Assert.assertEquals(3, statement.getQueryModel().getColumns().size());
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(ExprNode.NodeType.LITERAL, statement.getQueryModel().getColumns().get(i).getAst().type);
        }
    }

    @Test
    public void testSelectSingleExpression() throws Exception {
        NQLParser parser = new NQLParser();
        parser.setContent("select a+b*c x from t");
        Statement statement = parser.parse();

        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        Assert.assertNotNull(statement.getQueryModel());
        Assert.assertEquals(1, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("+", statement.getQueryModel().getColumns().get(0).getAst().token);
        Assert.assertEquals("t", statement.getQueryModel().getJournalName());
    }

    @Test
    public void testWhereClause() throws Exception {
        NQLParser parser = new NQLParser();
        parser.setContent("select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y), b = 10");
        Statement statement = parser.parse();
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName());
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getName());
        // where
        Assert.assertEquals(2, statement.getQueryModel().getWhereClauses().size());
        Assert.assertEquals("in", statement.getQueryModel().getWhereClauses().get(0).token);
        Assert.assertEquals("=", statement.getQueryModel().getWhereClauses().get(1).token);
    }

    @Test
    public void testWhereClauseMostRecentBy() throws Exception {
        NQLParser parser = new NQLParser();
        parser.setContent("select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y), b = 10 most recent by x");
        Statement statement = parser.parse();
        Assert.assertEquals(StatementType.QUERY_JOURNAL, statement.getType());
        // journal name
        Assert.assertEquals("zyzy", statement.getQueryModel().getJournalName());
        // columns
        Assert.assertEquals(2, statement.getQueryModel().getColumns().size());
        Assert.assertEquals("x", statement.getQueryModel().getColumns().get(0).getName());
        Assert.assertEquals("ohoh", statement.getQueryModel().getColumns().get(1).getName());
        // where
        Assert.assertEquals(2, statement.getQueryModel().getWhereClauses().size());
        Assert.assertEquals("in", statement.getQueryModel().getWhereClauses().get(0).token);
        Assert.assertEquals("=", statement.getQueryModel().getWhereClauses().get(1).token);
        // most recent by
        Assert.assertNotNull(statement.getQueryModel().getMostRecentBy());
        Assert.assertEquals("x", statement.getQueryModel().getMostRecentBy().token);
        Assert.assertEquals(ExprNode.NodeType.LITERAL, statement.getQueryModel().getMostRecentBy().type);
        Assert.assertTrue(statement.getQueryModel().isMostRecentByPostfix());
    }
}
