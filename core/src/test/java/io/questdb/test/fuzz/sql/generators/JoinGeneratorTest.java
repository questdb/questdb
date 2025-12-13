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

public class JoinGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private GeneratorContext ctx;

    @Before
    public void setUp() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        ctx = new GeneratorContext(rnd, GeneratorConfig.defaults());
    }

    @Test
    public void testGenerateProducesJoinClause() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            JoinGenerator.generate(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain JOIN: " + sql, sql.contains("JOIN"));
        }
    }

    @Test
    public void testGenerateVariousJoinTypes() {
        Set<String> joinTypes = new HashSet<>();

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            JoinGenerator.generate(ctx);
            String sql = ctx.toSql();

            if (sql.contains("INNER JOIN")) joinTypes.add("INNER");
            if (sql.contains("LEFT OUTER JOIN")) joinTypes.add("LEFT OUTER");
            if (sql.contains("LEFT JOIN") && !sql.contains("OUTER")) joinTypes.add("LEFT");
            if (sql.contains("RIGHT OUTER JOIN")) joinTypes.add("RIGHT OUTER");
            if (sql.contains("RIGHT JOIN") && !sql.contains("OUTER")) joinTypes.add("RIGHT");
            if (sql.contains("CROSS JOIN")) joinTypes.add("CROSS");
        }

        Assert.assertTrue("Should generate multiple join types: " + joinTypes.size(),
                joinTypes.size() >= 3);
    }

    @Test
    public void testGenerateInnerJoin() {
        for (int i = 0; i < 20; i++) {
            ctx.reset();
            JoinGenerator.generateInnerJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be INNER JOIN: " + sql, sql.startsWith("INNER JOIN"));
            Assert.assertTrue("Should have ON or USING: " + sql,
                    sql.contains(" ON ") || sql.contains(" USING "));
        }
    }

    @Test
    public void testGenerateLeftJoin() {
        boolean foundLeft = false;
        boolean foundLeftOuter = false;

        for (int i = 0; i < 50; i++) {
            ctx.reset();
            JoinGenerator.generateLeftJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be LEFT JOIN variant: " + sql,
                    sql.startsWith("LEFT JOIN") || sql.startsWith("LEFT OUTER JOIN"));

            if (sql.startsWith("LEFT OUTER JOIN")) foundLeftOuter = true;
            if (sql.startsWith("LEFT JOIN") && !sql.startsWith("LEFT OUTER")) foundLeft = true;
        }

        Assert.assertTrue("Should generate LEFT JOIN", foundLeft);
        Assert.assertTrue("Should generate LEFT OUTER JOIN", foundLeftOuter);
    }

    @Test
    public void testGenerateRightJoin() {
        boolean foundRight = false;
        boolean foundRightOuter = false;

        for (int i = 0; i < 50; i++) {
            ctx.reset();
            JoinGenerator.generateRightJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be RIGHT JOIN variant: " + sql,
                    sql.startsWith("RIGHT JOIN") || sql.startsWith("RIGHT OUTER JOIN"));

            if (sql.startsWith("RIGHT OUTER JOIN")) foundRightOuter = true;
            if (sql.startsWith("RIGHT JOIN") && !sql.startsWith("RIGHT OUTER")) foundRight = true;
        }

        Assert.assertTrue("Should generate RIGHT JOIN", foundRight);
        Assert.assertTrue("Should generate RIGHT OUTER JOIN", foundRightOuter);
    }

    @Test
    public void testGenerateCrossJoin() {
        for (int i = 0; i < 20; i++) {
            ctx.reset();
            JoinGenerator.generateCrossJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be CROSS JOIN: " + sql, sql.startsWith("CROSS JOIN"));
            // CROSS JOIN should not have ON or USING
            Assert.assertFalse("CROSS JOIN should not have ON: " + sql, sql.contains(" ON "));
            Assert.assertFalse("CROSS JOIN should not have USING: " + sql, sql.contains(" USING "));
        }
    }

    @Test
    public void testGenerateOnCondition() {
        ctx.tableAliases().add("t1");

        for (int i = 0; i < 50; i++) {
            ctx.reset();
            ctx.tableAliases().add("t1");
            JoinGenerator.generateOnCondition(ctx, "t2");
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with ON: " + sql, sql.startsWith("ON "));
            Assert.assertTrue("Should contain =: " + sql, sql.contains("="));
        }
    }

    @Test
    public void testGenerateUsingClause() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            JoinGenerator.generateUsingClause(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with USING: " + sql, sql.startsWith("USING"));
            Assert.assertTrue("Should have open paren: " + sql, sql.contains("("));
            Assert.assertTrue("Should have close paren: " + sql, sql.contains(")"));
        }
    }

    @Test
    public void testGenerateMultipleJoins() {
        ctx.reset();
        JoinGenerator.generateMultipleJoins(ctx, 3);
        String sql = ctx.toSql();

        // Count JOIN occurrences
        int joinCount = 0;
        int index = 0;
        while ((index = sql.indexOf("JOIN", index)) != -1) {
            joinCount++;
            index++;
        }

        Assert.assertTrue("Should have at least 3 JOINs: " + joinCount, joinCount >= 3);
    }

    @Test
    public void testGenerateConditionalJoin() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            JoinGenerator.generateConditionalJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain JOIN: " + sql, sql.contains("JOIN"));
            Assert.assertFalse("Should not be CROSS JOIN: " + sql, sql.contains("CROSS"));
            Assert.assertTrue("Should have ON or USING: " + sql,
                    sql.contains(" ON ") || sql.contains(" USING "));
        }
    }

    @Test
    public void testJoinWithAlias() {
        boolean foundWithAlias = false;
        boolean foundWithoutAlias = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            int aliasCountBefore = ctx.tableAliases().size();
            JoinGenerator.generateInnerJoin(ctx);
            int aliasCountAfter = ctx.tableAliases().size();

            if (aliasCountAfter > aliasCountBefore) {
                foundWithAlias = true;
            } else {
                foundWithoutAlias = true;
            }
        }

        Assert.assertTrue("Should sometimes add alias", foundWithAlias);
        // Without alias is less common but should occur
    }

    @Test
    public void testOnConditionVariety() {
        boolean foundSingleCondition = false;
        boolean foundMultipleConditions = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            ctx.tableAliases().add("t1");
            JoinGenerator.generateOnCondition(ctx, "t2");
            String sql = ctx.toSql();

            if (sql.contains(" AND ")) {
                foundMultipleConditions = true;
            } else {
                foundSingleCondition = true;
            }
        }

        Assert.assertTrue("Should generate single condition", foundSingleCondition);
        Assert.assertTrue("Should generate multiple conditions", foundMultipleConditions);
    }

    @Test
    public void testBalancedParentheses() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            JoinGenerator.generate(ctx);
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

    @Test
    public void testReproducibility() {
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx1 = new GeneratorContext(rnd1, GeneratorConfig.defaults());

        String[] joins1 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx1.reset();
            JoinGenerator.generate(ctx1);
            joins1[i] = ctx1.toSql();
        }

        Rnd rnd2 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx2 = new GeneratorContext(rnd2, GeneratorConfig.defaults());

        String[] joins2 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx2.reset();
            JoinGenerator.generate(ctx2);
            joins2[i] = ctx2.toSql();
        }

        Assert.assertArrayEquals(joins1, joins2);
    }

    // --- QuestDB-specific join tests ---

    @Test
    public void testGenerateAsofJoin() {
        for (int i = 0; i < 20; i++) {
            ctx.reset();
            JoinGenerator.generateAsofJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be ASOF JOIN: " + sql, sql.startsWith("ASOF JOIN"));
            Assert.assertTrue("Should have ON clause: " + sql, sql.contains(" ON "));
            // ASOF JOIN should not use USING
            Assert.assertFalse("ASOF JOIN should not have USING: " + sql, sql.contains(" USING "));
        }
    }

    @Test
    public void testGenerateLtJoin() {
        for (int i = 0; i < 20; i++) {
            ctx.reset();
            JoinGenerator.generateLtJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be LT JOIN: " + sql, sql.startsWith("LT JOIN"));
            Assert.assertTrue("Should have ON clause: " + sql, sql.contains(" ON "));
            // LT JOIN should not use USING
            Assert.assertFalse("LT JOIN should not have USING: " + sql, sql.contains(" USING "));
        }
    }

    @Test
    public void testGenerateSpliceJoin() {
        for (int i = 0; i < 20; i++) {
            ctx.reset();
            JoinGenerator.generateSpliceJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be SPLICE JOIN: " + sql, sql.startsWith("SPLICE JOIN"));
            Assert.assertTrue("Should have ON clause: " + sql, sql.contains(" ON "));
            // SPLICE JOIN should not use USING
            Assert.assertFalse("SPLICE JOIN should not have USING: " + sql, sql.contains(" USING "));
            // SPLICE JOIN should not have TOLERANCE
            Assert.assertFalse("SPLICE JOIN should not have TOLERANCE: " + sql, sql.contains("TOLERANCE"));
        }
    }

    @Test
    public void testAsofJoinWithTolerance() {
        boolean foundWithTolerance = false;
        boolean foundWithoutTolerance = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            JoinGenerator.generateAsofJoin(ctx);
            String sql = ctx.toSql();

            if (sql.contains("TOLERANCE")) {
                foundWithTolerance = true;
            } else {
                foundWithoutTolerance = true;
            }
        }

        Assert.assertTrue("Should sometimes generate ASOF JOIN with TOLERANCE", foundWithTolerance);
        Assert.assertTrue("Should sometimes generate ASOF JOIN without TOLERANCE", foundWithoutTolerance);
    }

    @Test
    public void testLtJoinWithTolerance() {
        boolean foundWithTolerance = false;
        boolean foundWithoutTolerance = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            JoinGenerator.generateLtJoin(ctx);
            String sql = ctx.toSql();

            if (sql.contains("TOLERANCE")) {
                foundWithTolerance = true;
            } else {
                foundWithoutTolerance = true;
            }
        }

        Assert.assertTrue("Should sometimes generate LT JOIN with TOLERANCE", foundWithTolerance);
        Assert.assertTrue("Should sometimes generate LT JOIN without TOLERANCE", foundWithoutTolerance);
    }

    @Test
    public void testGenerateToleranceClause() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            JoinGenerator.generateToleranceClause(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with TOLERANCE: " + sql, sql.startsWith("TOLERANCE "));
            // Should have a time interval
            Assert.assertTrue("Should have time interval: " + sql,
                    sql.matches("TOLERANCE \\d+[smhd]"));
        }
    }

    @Test
    public void testGenerateTimestampOnCondition() {
        ctx.tableAliases().add("t1");

        for (int i = 0; i < 50; i++) {
            ctx.reset();
            ctx.tableAliases().add("t1");
            JoinGenerator.generateTimestampOnCondition(ctx, "t2");
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with ON: " + sql, sql.startsWith("ON "));
            Assert.assertTrue("Should contain =: " + sql, sql.contains("="));
            // Should reference timestamp columns
            Assert.assertTrue("Should reference timestamp column: " + sql,
                    sql.contains("ts") || sql.contains("timestamp"));
        }
    }

    @Test
    public void testQuestdbJoinWithHighProbability() {
        // Configure with 100% QuestDB join probability
        GeneratorConfig config = GeneratorConfig.builder()
                .asofJoinProb(0.34)
                .ltJoinProb(0.33)
                .spliceJoinProb(0.33)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext questdbCtx = new GeneratorContext(rnd, config);

        int asofCount = 0;
        int ltCount = 0;
        int spliceCount = 0;

        for (int i = 0; i < 300; i++) {
            questdbCtx.reset();
            JoinGenerator.generate(questdbCtx);
            String sql = questdbCtx.toSql();

            if (sql.contains("ASOF JOIN")) asofCount++;
            if (sql.contains("LT JOIN")) ltCount++;
            if (sql.contains("SPLICE JOIN")) spliceCount++;
        }

        // With high QuestDB join probability, we should get a good mix
        Assert.assertTrue("Should generate ASOF joins: " + asofCount, asofCount > 0);
        Assert.assertTrue("Should generate LT joins: " + ltCount, ltCount > 0);
        Assert.assertTrue("Should generate SPLICE joins: " + spliceCount, spliceCount > 0);
    }

    @Test
    public void testGenerateStandardJoinOnly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            JoinGenerator.generateStandardJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain JOIN: " + sql, sql.contains("JOIN"));
            Assert.assertFalse("Should not be ASOF: " + sql, sql.contains("ASOF"));
            Assert.assertFalse("Should not be LT: " + sql, sql.contains("LT JOIN"));
            Assert.assertFalse("Should not be SPLICE: " + sql, sql.contains("SPLICE"));
        }
    }

    @Test
    public void testGenerateQuestdbJoinOnly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            JoinGenerator.generateQuestdbJoin(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain JOIN: " + sql, sql.contains("JOIN"));
            Assert.assertTrue("Should be QuestDB join: " + sql,
                    sql.contains("ASOF") || sql.contains("LT JOIN") || sql.contains("SPLICE"));
        }
    }
}
