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

package io.questdb.test.fuzz.sql.generators;

import io.questdb.std.Rnd;
import io.questdb.test.fuzz.sql.GeneratorConfig;
import io.questdb.test.fuzz.sql.GeneratorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class ExpressionGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private GeneratorContext ctx;

    @Before
    public void setUp() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        ctx = new GeneratorContext(rnd, GeneratorConfig.defaults());
    }

    @Test
    public void testGenerateProducesTokens() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generate(ctx);
            Assert.assertFalse("Should produce at least one token", ctx.tokens().isEmpty());
        }
    }

    @Test
    public void testGenerateTerminalProducesTokens() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateTerminal(ctx);
            Assert.assertFalse("Should produce at least one token", ctx.tokens().isEmpty());
        }
    }

    @Test
    public void testGenerateTerminalIsSimple() {
        // Terminal expressions should be simple (literals or column refs)
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateTerminal(ctx);

            String sql = ctx.toSql();
            // Should not contain operators that indicate complex expressions
            Assert.assertFalse("Terminal should not contain AND: " + sql,
                    sql.contains(" AND "));
            Assert.assertFalse("Terminal should not contain OR: " + sql,
                    sql.contains(" OR "));
        }
    }

    @Test
    public void testGenerateColumnReference() {
        Set<String> refs = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateColumnReference(ctx);
            refs.add(ctx.toSql());
        }

        // Should generate various column names
        Assert.assertTrue("Should generate multiple different column refs", refs.size() > 1);

        // All should be valid identifiers (start with letter)
        for (String ref : refs) {
            Assert.assertTrue("Column ref should start with letter: " + ref,
                    Character.isLetter(ref.charAt(0)));
        }
    }

    @Test
    public void testGenerateColumnReferenceWithAlias() {
        // Set up context with table aliases
        ctx.tableAliases().add("t1");
        ctx.tableAliases().add("t2");

        boolean foundQualified = false;
        boolean foundUnqualified = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            // Re-add aliases after reset
            ctx.tableAliases().add("t1");
            ctx.tableAliases().add("t2");

            ExpressionGenerator.generateColumnReference(ctx);
            String sql = ctx.toSql();

            if (sql.contains(".")) {
                foundQualified = true;
                // Qualified reference should start with alias
                Assert.assertTrue("Qualified ref should start with alias: " + sql,
                        sql.startsWith("t1.") || sql.startsWith("t2."));
            } else {
                foundUnqualified = true;
            }
        }

        Assert.assertTrue("Should generate some qualified references", foundQualified);
        Assert.assertTrue("Should generate some unqualified references", foundUnqualified);
    }

    @Test
    public void testGenerateBinaryExpression() {
        Set<String> operators = new HashSet<>();

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            ExpressionGenerator.generateBinaryExpression(ctx);
            String sql = ctx.toSql();

            // Extract operator (look for common operators)
            if (sql.contains(" + ")) operators.add("+");
            if (sql.contains(" - ")) operators.add("-");
            if (sql.contains(" * ")) operators.add("*");
            if (sql.contains(" / ")) operators.add("/");
            if (sql.contains(" = ")) operators.add("=");
            if (sql.contains(" != ")) operators.add("!=");
            if (sql.contains(" <> ")) operators.add("<>");
            if (sql.contains(" < ")) operators.add("<");
            if (sql.contains(" > ")) operators.add(">");
            if (sql.contains(" <= ")) operators.add("<=");
            if (sql.contains(" >= ")) operators.add(">=");
            if (sql.contains(" AND ")) operators.add("AND");
            if (sql.contains(" OR ")) operators.add("OR");
        }

        // Should use various operators
        Assert.assertTrue("Should generate arithmetic operators",
                operators.contains("+") || operators.contains("-") ||
                operators.contains("*") || operators.contains("/"));
        Assert.assertTrue("Should generate comparison operators",
                operators.contains("=") || operators.contains("!=") ||
                operators.contains("<") || operators.contains(">"));
        Assert.assertTrue("Should generate logical operators",
                operators.contains("AND") || operators.contains("OR"));
    }

    @Test
    public void testGenerateUnaryExpression() {
        boolean foundNegation = false;
        boolean foundNot = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateUnaryExpression(ctx);
            String sql = ctx.toSql();

            if (sql.startsWith("-")) {
                foundNegation = true;
            }
            if (sql.startsWith("NOT ")) {
                foundNot = true;
            }
        }

        Assert.assertTrue("Should generate negation", foundNegation);
        Assert.assertTrue("Should generate NOT", foundNot);
    }

    @Test
    public void testGenerateParenthesizedExpression() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            ExpressionGenerator.generateParenthesizedExpression(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with open paren: " + sql, sql.startsWith("("));
            Assert.assertTrue("Should end with close paren: " + sql, sql.endsWith(")"));
        }
    }

    @Test
    public void testGenerateFunctionCall() {
        Set<String> functions = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateFunctionCall(ctx);
            String sql = ctx.toSql();

            // Extract function name (before the first paren)
            int parenIndex = sql.indexOf('(');
            Assert.assertTrue("Should contain open paren: " + sql, parenIndex > 0);

            String funcName = sql.substring(0, parenIndex);
            functions.add(funcName);

            // Should have matching parens
            Assert.assertTrue("Should end with close paren: " + sql, sql.endsWith(")"));
        }

        // Should use various functions
        Assert.assertTrue("Should generate multiple function names", functions.size() > 1);
    }

    @Test
    public void testGenerateBooleanExpression() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateBooleanExpression(ctx);
            String sql = ctx.toSql();

            // Boolean expressions should contain comparison or logical operators
            boolean hasComparison = sql.contains("=") || sql.contains("<") ||
                    sql.contains(">") || sql.contains("!=") || sql.contains("<>");
            boolean hasLogical = sql.contains("AND") || sql.contains("OR") || sql.contains("NOT");
            boolean hasParens = sql.contains("(");

            Assert.assertTrue("Boolean expression should have comparison, logical op, or parens: " + sql,
                    hasComparison || hasLogical || hasParens);
        }
    }

    @Test
    public void testGenerateComparisonExpression() {
        Set<String> operators = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateComparisonExpression(ctx);
            String sql = ctx.toSql();

            // Extract comparison operator
            if (sql.contains(" = ")) operators.add("=");
            if (sql.contains(" != ")) operators.add("!=");
            if (sql.contains(" <> ")) operators.add("<>");
            if (sql.contains(" < ")) operators.add("<");
            if (sql.contains(" > ")) operators.add(">");
            if (sql.contains(" <= ")) operators.add("<=");
            if (sql.contains(" >= ")) operators.add(">=");
        }

        // Should use various comparison operators
        Assert.assertTrue("Should generate comparison operators", operators.size() > 2);
    }

    @Test
    public void testGenerateArithmeticExpression() {
        Set<String> operators = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateArithmeticExpression(ctx);
            String sql = ctx.toSql();

            // Extract arithmetic operator
            if (sql.contains(" + ")) operators.add("+");
            if (sql.contains(" - ")) operators.add("-");
            if (sql.contains(" * ")) operators.add("*");
            if (sql.contains(" / ")) operators.add("/");
        }

        // Should use various arithmetic operators
        Assert.assertTrue("Should generate arithmetic operators", operators.size() >= 2);
    }

    @Test
    public void testGenerateSelectExpression() {
        boolean foundWithAlias = false;
        boolean foundWithoutAlias = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateSelectExpression(ctx, true);
            String sql = ctx.toSql();

            if (sql.contains(" AS ")) {
                foundWithAlias = true;
            } else {
                foundWithoutAlias = true;
            }
        }

        Assert.assertTrue("Should sometimes generate alias", foundWithAlias);
        Assert.assertTrue("Should sometimes omit alias", foundWithoutAlias);
    }

    @Test
    public void testDepthLimiting() {
        // Configure with low max depth
        GeneratorConfig config = GeneratorConfig.builder()
                .maxExpressionDepth(2)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext lowDepthCtx = new GeneratorContext(rnd, config);

        // Even with complex expression generation, should not stack overflow
        for (int i = 0; i < 100; i++) {
            lowDepthCtx.reset();
            ExpressionGenerator.generate(lowDepthCtx);

            String sql = lowDepthCtx.toSql();
            Assert.assertNotNull(sql);
            Assert.assertFalse(sql.isEmpty());
        }
    }

    @Test
    public void testDistribution() {
        int literals = 0;
        int columns = 0;
        int binary = 0;
        int unary = 0;
        int parens = 0;
        int functions = 0;

        // Generate many expressions and check distribution
        for (int i = 0; i < 1000; i++) {
            ctx.reset();
            ExpressionGenerator.generate(ctx);
            String sql = ctx.toSql();

            // Simple heuristics to classify
            if (sql.startsWith("(") && sql.endsWith(")") && sql.indexOf("(", 1) == -1) {
                parens++;
            } else if (sql.contains("(") && !sql.startsWith("-") && !sql.startsWith("NOT")) {
                functions++;
            } else if (sql.startsWith("-") || sql.startsWith("NOT ")) {
                unary++;
            } else if (sql.contains(" + ") || sql.contains(" - ") || sql.contains(" * ") ||
                    sql.contains(" / ") || sql.contains(" = ") || sql.contains(" != ") ||
                    sql.contains(" < ") || sql.contains(" > ") || sql.contains(" AND ") ||
                    sql.contains(" OR ") || sql.contains(" <= ") || sql.contains(" >= ") ||
                    sql.contains(" <> ")) {
                binary++;
            } else if (sql.matches("[a-zA-Z].*") && !sql.contains("'")) {
                columns++;
            } else {
                literals++;
            }
        }

        // Verify reasonable distribution
        Assert.assertTrue("Should have some literals: " + literals, literals > 50);
        Assert.assertTrue("Should have some columns: " + columns, columns > 50);
        Assert.assertTrue("Should have some binary expressions: " + binary, binary > 50);
    }

    @Test
    public void testReproducibility() {
        // Generate with specific seed
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx1 = new GeneratorContext(rnd1, GeneratorConfig.defaults());

        String[] expressions1 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx1.reset();
            ExpressionGenerator.generate(ctx1);
            expressions1[i] = ctx1.toSql();
        }

        // Generate with same seed
        Rnd rnd2 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx2 = new GeneratorContext(rnd2, GeneratorConfig.defaults());

        String[] expressions2 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx2.reset();
            ExpressionGenerator.generate(ctx2);
            expressions2[i] = ctx2.toSql();
        }

        // Should be identical
        Assert.assertArrayEquals(expressions1, expressions2);
    }

    @Test
    public void testNoNullTokens() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generate(ctx);

            // Verify no null tokens
            for (int j = 0; j < ctx.tokens().size(); j++) {
                Assert.assertNotNull("Token should not be null", ctx.tokens().get(j));
                Assert.assertNotNull("Token value should not be null", ctx.tokens().get(j).value());
            }
        }
    }

    @Test
    public void testValidSqlSyntax() {
        // Basic syntax validation - balanced parentheses
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generate(ctx);
            String sql = ctx.toSql();

            int openCount = 0;
            int closeCount = 0;
            for (char c : sql.toCharArray()) {
                if (c == '(') openCount++;
                if (c == ')') closeCount++;
            }

            Assert.assertEquals("Parentheses should be balanced in: " + sql,
                    openCount, closeCount);
        }
    }

    // --- Advanced expression tests ---

    @Test
    public void testGenerateCaseExpression() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            ExpressionGenerator.generateCaseExpression(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with CASE: " + sql, sql.startsWith("CASE "));
            Assert.assertTrue("Should contain WHEN: " + sql, sql.contains(" WHEN "));
            Assert.assertTrue("Should contain THEN: " + sql, sql.contains(" THEN "));
            Assert.assertTrue("Should end with END: " + sql, sql.endsWith(" END"));
        }
    }

    @Test
    public void testGenerateCastExpression() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            ExpressionGenerator.generateCastExpression(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with CAST: " + sql, sql.startsWith("CAST"));
            Assert.assertTrue("Should contain AS: " + sql, sql.contains(" AS "));
            Assert.assertTrue("Should have parentheses: " + sql,
                    sql.contains("(") && sql.contains(")"));
        }
    }

    @Test
    public void testGenerateInExpression() {
        boolean foundIn = false;
        boolean foundNotIn = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateInExpression(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain IN: " + sql, sql.contains(" IN "));
            Assert.assertTrue("Should have parentheses: " + sql,
                    sql.contains("(") && sql.contains(")"));

            if (sql.contains(" NOT IN ")) {
                foundNotIn = true;
            } else {
                foundIn = true;
            }
        }

        Assert.assertTrue("Should generate IN", foundIn);
        Assert.assertTrue("Should generate NOT IN", foundNotIn);
    }

    @Test
    public void testGenerateBetweenExpression() {
        boolean foundBetween = false;
        boolean foundNotBetween = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateBetweenExpression(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain BETWEEN: " + sql, sql.contains(" BETWEEN "));
            Assert.assertTrue("Should contain AND: " + sql, sql.contains(" AND "));

            if (sql.contains(" NOT BETWEEN ")) {
                foundNotBetween = true;
            } else {
                foundBetween = true;
            }
        }

        Assert.assertTrue("Should generate BETWEEN", foundBetween);
        Assert.assertTrue("Should generate NOT BETWEEN", foundNotBetween);
    }

    @Test
    public void testGenerateLikeExpression() {
        boolean foundLike = false;
        boolean foundNotLike = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateLikeExpression(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain LIKE: " + sql, sql.contains(" LIKE "));

            if (sql.contains(" NOT LIKE ")) {
                foundNotLike = true;
            } else {
                foundLike = true;
            }
        }

        Assert.assertTrue("Should generate LIKE", foundLike);
        Assert.assertTrue("Should generate NOT LIKE", foundNotLike);
    }

    @Test
    public void testGenerateIsNullExpression() {
        boolean foundIsNull = false;
        boolean foundIsNotNull = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ExpressionGenerator.generateIsNullExpression(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain IS: " + sql, sql.contains(" IS "));
            Assert.assertTrue("Should end with NULL: " + sql, sql.endsWith(" NULL"));

            if (sql.contains(" IS NOT NULL")) {
                foundIsNotNull = true;
            } else {
                foundIsNull = true;
            }
        }

        Assert.assertTrue("Should generate IS NULL", foundIsNull);
        Assert.assertTrue("Should generate IS NOT NULL", foundIsNotNull);
    }

    @Test
    public void testGenerateVariety() {
        // Generate many expressions and check we get various types
        boolean foundCase = false;
        boolean foundCast = false;
        boolean foundIn = false;
        boolean foundBetween = false;
        boolean foundLike = false;
        boolean foundIsNull = false;

        for (int i = 0; i < 500; i++) {
            ctx.reset();
            ExpressionGenerator.generate(ctx);
            String sql = ctx.toSql();

            if (sql.contains("CASE ")) foundCase = true;
            if (sql.contains("CAST")) foundCast = true;
            if (sql.contains(" IN ")) foundIn = true;
            if (sql.contains(" BETWEEN ")) foundBetween = true;
            if (sql.contains(" LIKE ")) foundLike = true;
            if (sql.contains(" IS ") && sql.contains(" NULL")) foundIsNull = true;
        }

        // With 500 iterations and reasonable weights, we should see most expression types
        Assert.assertTrue("Should generate CASE expressions", foundCase);
        Assert.assertTrue("Should generate CAST expressions", foundCast);
        Assert.assertTrue("Should generate IN expressions", foundIn);
        Assert.assertTrue("Should generate BETWEEN expressions", foundBetween);
        // LIKE and IS NULL have lower weights, may not always appear in 500 iterations
    }
}
