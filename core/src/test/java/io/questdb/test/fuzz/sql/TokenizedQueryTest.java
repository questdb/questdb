/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.fuzz.sql;

import org.junit.Assert;
import org.junit.Test;

public class TokenizedQueryTest {

    @Test
    public void testEmptyQuery() {
        TokenizedQuery query = new TokenizedQuery();
        Assert.assertTrue(query.isEmpty());
        Assert.assertEquals(0, query.size());
        Assert.assertEquals("", query.serialize());
    }

    @Test
    public void testSimpleSelect() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addKeyword("FROM")
                .addIdentifier("t");

        Assert.assertEquals(4, query.size());
        Assert.assertEquals("SELECT a FROM t", query.serialize());
    }

    @Test
    public void testSelectWithColumns() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addPunctuation(",")
                .addIdentifier("b")
                .addPunctuation(",")
                .addIdentifier("c")
                .addKeyword("FROM")
                .addIdentifier("t");

        Assert.assertEquals("SELECT a, b, c FROM t", query.serialize());
    }

    @Test
    public void testSelectWithExpression() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addOperator("+")
                .addLiteral("1")
                .addKeyword("FROM")
                .addIdentifier("t");

        Assert.assertEquals("SELECT a + 1 FROM t", query.serialize());
    }

    @Test
    public void testSelectWithParens() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addPunctuation("(")
                .addIdentifier("a")
                .addOperator("+")
                .addIdentifier("b")
                .addPunctuation(")")
                .addOperator("*")
                .addLiteral("2")
                .addKeyword("FROM")
                .addIdentifier("t");

        Assert.assertEquals("SELECT (a + b) * 2 FROM t", query.serialize());
    }

    @Test
    public void testQualifiedColumn() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("t")
                .addPunctuation(".")
                .addIdentifier("a")
                .addKeyword("FROM")
                .addIdentifier("t");

        Assert.assertEquals("SELECT t.a FROM t", query.serialize());
    }

    @Test
    public void testRemoveToken() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addPunctuation(",")
                .addIdentifier("b")
                .addKeyword("FROM")
                .addIdentifier("t");

        // Remove comma and second column
        TokenizedQuery modified = query.removeToken(2).removeToken(2);
        Assert.assertEquals("SELECT a FROM t", modified.serialize());
    }

    @Test
    public void testRemoveRange() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addPunctuation(",")
                .addIdentifier("b")
                .addPunctuation(",")
                .addIdentifier("c")
                .addKeyword("FROM")
                .addIdentifier("t");

        // Remove ", b"
        TokenizedQuery modified = query.removeRange(2, 4);
        Assert.assertEquals("SELECT a, c FROM t", modified.serialize());
    }

    @Test
    public void testSwapTokens() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addKeyword("FROM")
                .addIdentifier("t");

        // Swap SELECT and FROM
        TokenizedQuery modified = query.swapTokens(0, 2);
        Assert.assertEquals("FROM a SELECT t", modified.serialize());
    }

    @Test
    public void testInsertToken() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addKeyword("FROM")
                .addIdentifier("t");

        // Insert DISTINCT after SELECT
        TokenizedQuery modified = query.insertToken(1, SqlToken.keyword("DISTINCT"));
        Assert.assertEquals("SELECT DISTINCT a FROM t", modified.serialize());
    }

    @Test
    public void testReplaceToken() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addKeyword("FROM")
                .addIdentifier("t");

        // Replace column name
        TokenizedQuery modified = query.replaceToken(1, SqlToken.identifier("b"));
        Assert.assertEquals("SELECT b FROM t", modified.serialize());
    }

    @Test
    public void testDuplicateToken() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addKeyword("FROM")
                .addIdentifier("t");

        // Duplicate SELECT
        TokenizedQuery modified = query.duplicateToken(0);
        Assert.assertEquals("SELECT SELECT a FROM t", modified.serialize());
    }

    @Test
    public void testTruncateAt() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a")
                .addKeyword("FROM")
                .addIdentifier("t")
                .addKeyword("WHERE")
                .addIdentifier("x")
                .addOperator(">")
                .addLiteral("1");

        // Truncate after FROM t
        TokenizedQuery modified = query.truncateAt(4);
        Assert.assertEquals("SELECT a FROM t", modified.serialize());
    }

    @Test
    public void testClear() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a");

        Assert.assertEquals(2, query.size());
        query.clear();
        Assert.assertEquals(0, query.size());
        Assert.assertTrue(query.isEmpty());
    }

    @Test
    public void testAddAll() {
        TokenizedQuery q1 = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a");

        TokenizedQuery q2 = new TokenizedQuery()
                .addKeyword("FROM")
                .addIdentifier("t");

        q1.addAll(q2);
        Assert.assertEquals("SELECT a FROM t", q1.serialize());
    }

    @Test
    public void testGetTokens() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a");

        var tokens = query.getTokens();
        Assert.assertEquals(2, tokens.size());
        Assert.assertEquals(SqlToken.keyword("SELECT"), tokens.get(0));
        Assert.assertEquals(SqlToken.identifier("a"), tokens.get(1));
    }

    @Test
    public void testRemoveTokenOutOfBounds() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a");

        // Out of bounds removal returns copy unchanged
        TokenizedQuery modified = query.removeToken(5);
        Assert.assertEquals("SELECT a", modified.serialize());

        modified = query.removeToken(-1);
        Assert.assertEquals("SELECT a", modified.serialize());
    }

    @Test
    public void testSwapTokensOutOfBounds() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("a");

        // Out of bounds swap returns copy unchanged
        TokenizedQuery modified = query.swapTokens(0, 5);
        Assert.assertEquals("SELECT a", modified.serialize());
    }

    @Test
    public void testToString() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addLiteral("1");

        Assert.assertEquals("SELECT 1", query.toString());
    }

    @Test
    public void testFunctionCall() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("sum")
                .addPunctuation("(")
                .addIdentifier("a")
                .addPunctuation(")")
                .addKeyword("FROM")
                .addIdentifier("t");

        Assert.assertEquals("SELECT sum(a) FROM t", query.serialize());
    }

    @Test
    public void testFunctionCallWithMultipleArgs() {
        TokenizedQuery query = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("coalesce")
                .addPunctuation("(")
                .addIdentifier("a")
                .addPunctuation(",")
                .addIdentifier("b")
                .addPunctuation(",")
                .addLiteral("0")
                .addPunctuation(")")
                .addKeyword("FROM")
                .addIdentifier("t");

        Assert.assertEquals("SELECT coalesce(a, b, 0) FROM t", query.serialize());
    }
}
