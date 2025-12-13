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

public class SqlTokenTest {

    @Test
    public void testKeywordToken() {
        SqlToken token = SqlToken.keyword("SELECT");
        Assert.assertEquals("SELECT", token.value());
        Assert.assertEquals(SqlToken.Type.KEYWORD, token.type());
        Assert.assertTrue(token.isKeyword());
        Assert.assertFalse(token.isIdentifier());
        Assert.assertFalse(token.isLiteral());
        Assert.assertFalse(token.isOperator());
        Assert.assertFalse(token.isPunctuation());
    }

    @Test
    public void testIdentifierToken() {
        SqlToken token = SqlToken.identifier("my_table");
        Assert.assertEquals("my_table", token.value());
        Assert.assertEquals(SqlToken.Type.IDENTIFIER, token.type());
        Assert.assertFalse(token.isKeyword());
        Assert.assertTrue(token.isIdentifier());
    }

    @Test
    public void testLiteralToken() {
        SqlToken token = SqlToken.literal("42");
        Assert.assertEquals("42", token.value());
        Assert.assertEquals(SqlToken.Type.LITERAL, token.type());
        Assert.assertTrue(token.isLiteral());
    }

    @Test
    public void testOperatorToken() {
        SqlToken token = SqlToken.operator("+");
        Assert.assertEquals("+", token.value());
        Assert.assertEquals(SqlToken.Type.OPERATOR, token.type());
        Assert.assertTrue(token.isOperator());
    }

    @Test
    public void testPunctuationToken() {
        SqlToken token = SqlToken.punctuation("(");
        Assert.assertEquals("(", token.value());
        Assert.assertEquals(SqlToken.Type.PUNCTUATION, token.type());
        Assert.assertTrue(token.isPunctuation());
    }

    @Test
    public void testSpaceBeforeRules() {
        // Punctuation doesn't need space before
        Assert.assertFalse(SqlToken.punctuation("(").needsSpaceBefore());
        Assert.assertFalse(SqlToken.punctuation(")").needsSpaceBefore());
        Assert.assertFalse(SqlToken.punctuation(",").needsSpaceBefore());

        // Other types need space before
        Assert.assertTrue(SqlToken.keyword("SELECT").needsSpaceBefore());
        Assert.assertTrue(SqlToken.identifier("table").needsSpaceBefore());
        Assert.assertTrue(SqlToken.literal("42").needsSpaceBefore());
        Assert.assertTrue(SqlToken.operator("+").needsSpaceBefore());
    }

    @Test
    public void testSpaceAfterRules() {
        // Open paren and dot don't need space after
        Assert.assertFalse(SqlToken.punctuation("(").needsSpaceAfter());
        Assert.assertFalse(SqlToken.punctuation(".").needsSpaceAfter());

        // Close paren, comma, semicolon need space after (for readability)
        Assert.assertTrue(SqlToken.punctuation(")").needsSpaceAfter());
        Assert.assertTrue(SqlToken.punctuation(",").needsSpaceAfter());
        Assert.assertTrue(SqlToken.punctuation(";").needsSpaceAfter());

        // Other types need space after
        Assert.assertTrue(SqlToken.keyword("SELECT").needsSpaceAfter());
        Assert.assertTrue(SqlToken.identifier("table").needsSpaceAfter());
    }

    @Test
    public void testEqualsAndHashCode() {
        SqlToken token1 = SqlToken.keyword("SELECT");
        SqlToken token2 = SqlToken.keyword("SELECT");
        SqlToken token3 = SqlToken.identifier("SELECT");
        SqlToken token4 = SqlToken.keyword("FROM");

        Assert.assertEquals(token1, token2);
        Assert.assertEquals(token1.hashCode(), token2.hashCode());

        Assert.assertNotEquals(token1, token3);  // Different type
        Assert.assertNotEquals(token1, token4);  // Different value
    }

    @Test
    public void testToString() {
        SqlToken token = SqlToken.keyword("SELECT");
        Assert.assertEquals("KEYWORD:SELECT", token.toString());
    }

    @Test(expected = NullPointerException.class)
    public void testNullValue() {
        new SqlToken(null, SqlToken.Type.KEYWORD);
    }

    @Test(expected = NullPointerException.class)
    public void testNullType() {
        new SqlToken("SELECT", null);
    }
}
