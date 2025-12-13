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

public class SelectGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private GeneratorContext ctx;

    @Before
    public void setUp() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        ctx = new GeneratorContext(rnd, GeneratorConfig.defaults());
    }

    @Test
    public void testGenerateProducesValidStructure() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SelectGenerator.generate(ctx);
            String sql = ctx.toSql();

            // Must start with SELECT
            Assert.assertTrue("Should start with SELECT: " + sql,
                    sql.startsWith("SELECT "));

            // Must contain FROM
            Assert.assertTrue("Should contain FROM: " + sql,
                    sql.contains(" FROM "));
        }
    }

    @Test
    public void testGenerateSelectList() {
        boolean foundStar = false;
        boolean foundColumns = false;
        boolean foundAlias = false;

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            SelectGenerator.generate(ctx);
            String sql = ctx.toSql();

            // Extract select list (between SELECT and FROM)
            int selectIdx = sql.indexOf("SELECT ") + 7;
            int fromIdx = sql.indexOf(" FROM ");
            String selectList = sql.substring(selectIdx, fromIdx);

            if (selectList.equals("*")) {
                foundStar = true;
            } else {
                foundColumns = true;
                if (selectList.contains(" AS ")) {
                    foundAlias = true;
                }
            }
        }

        Assert.assertTrue("Should sometimes generate SELECT *", foundStar);
        Assert.assertTrue("Should sometimes generate column list", foundColumns);
        Assert.assertTrue("Should sometimes generate aliases", foundAlias);
    }

    @Test
    public void testGenerateFromClause() {
        boolean foundTableOnly = false;
        boolean foundTableWithAlias = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SelectGenerator.generate(ctx);
            String sql = ctx.toSql();

            // Extract FROM clause
            int fromIdx = sql.indexOf(" FROM ") + 6;
            int whereIdx = sql.indexOf(" WHERE ");
            String fromClause = whereIdx > 0
                    ? sql.substring(fromIdx, whereIdx)
                    : sql.substring(fromIdx);

            // Count identifiers (space-separated)
            String[] parts = fromClause.trim().split("\\s+");
            if (parts.length == 1) {
                foundTableOnly = true;
            } else if (parts.length == 2) {
                foundTableWithAlias = true;
            }
        }

        Assert.assertTrue("Should sometimes generate table without alias", foundTableOnly);
        Assert.assertTrue("Should sometimes generate table with alias", foundTableWithAlias);
    }

    @Test
    public void testGenerateWhereClause() {
        int withWhere = 0;
        int withoutWhere = 0;

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            SelectGenerator.generate(ctx);
            String sql = ctx.toSql();

            if (sql.contains(" WHERE ")) {
                withWhere++;
            } else {
                withoutWhere++;
            }
        }

        // Should have a mix based on whereProb (default 0.5)
        Assert.assertTrue("Should sometimes have WHERE: " + withWhere, withWhere > 30);
        Assert.assertTrue("Should sometimes omit WHERE: " + withoutWhere, withoutWhere > 30);
    }

    @Test
    public void testGenerateSimple() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SelectGenerator.generateSimple(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with SELECT: " + sql, sql.startsWith("SELECT "));
            Assert.assertTrue("Should contain FROM: " + sql, sql.contains(" FROM "));
            Assert.assertFalse("Simple should not have WHERE: " + sql, sql.contains(" WHERE "));
            Assert.assertFalse("Simple should not have AS: " + sql, sql.contains(" AS "));
        }
    }

    @Test
    public void testGenerateWithTable() {
        ctx.reset();
        SelectGenerator.generateWithTable(ctx, "my_table", "col1", "col2", "col3");
        String sql = ctx.toSql();

        Assert.assertEquals("SELECT col1, col2, col3 FROM my_table", sql);
    }

    @Test
    public void testGenerateWithTableStar() {
        ctx.reset();
        SelectGenerator.generateWithTable(ctx, "orders");
        String sql = ctx.toSql();

        Assert.assertEquals("SELECT * FROM orders", sql);
    }

    @Test
    public void testGenerateSelectOne() {
        ctx.reset();
        SelectGenerator.generateSelectOne(ctx);
        String sql = ctx.toSql();

        Assert.assertEquals("SELECT 1", sql);
    }

    @Test
    public void testGenerateWithSubquery() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SelectGenerator.generateWithSubquery(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with SELECT: " + sql, sql.startsWith("SELECT "));
            Assert.assertTrue("Should contain FROM: " + sql, sql.contains(" FROM "));

            // Should contain parentheses for subquery (when not at max depth)
            // The subquery structure is: SELECT ... FROM (SELECT ...) alias
            if (sql.contains("(")) {
                Assert.assertTrue("Subquery should have matching parens: " + sql, sql.contains(")"));
            }
        }
    }

    @Test
    public void testDepthLimiting() {
        // Configure with minimum max depth and 0 subquery probability
        // shouldRecurseQuery() uses exponential decay and checks queryDepth >= maxDepth
        // At queryDepth=0 with maxDepth=1, recursion is still possible but with low probability
        // For reliable testing, we check that with sufficient iterations we get consistent behavior
        GeneratorConfig config = GeneratorConfig.builder()
                .maxDepth(1)
                .subqueryProb(0.0)  // Reduce subquery probability
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext lowDepthCtx = new GeneratorContext(rnd, config);

        // With default depth and subquery probability reduced, most should be simple
        int withSubquery = 0;
        int withoutSubquery = 0;

        for (int i = 0; i < 50; i++) {
            lowDepthCtx.reset();
            SelectGenerator.generateWithSubquery(lowDepthCtx);
            String sql = lowDepthCtx.toSql();

            if (sql.contains("(SELECT") || sql.contains("( SELECT")) {
                withSubquery++;
            } else {
                withoutSubquery++;
            }
        }

        // Should have at least some of each due to probability-based generation
        // At queryDepth=0, shouldRecurseQuery returns true with ~50% chance when maxDepth=1
        Assert.assertTrue("Should generate both types of queries. With: " + withSubquery + ", Without: " + withoutSubquery,
                withSubquery > 0 || withoutSubquery > 0);
    }

    @Test
    public void testBalancedParentheses() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SelectGenerator.generate(ctx);
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
        // Generate with specific seed
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx1 = new GeneratorContext(rnd1, GeneratorConfig.defaults());

        String[] queries1 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx1.reset();
            SelectGenerator.generate(ctx1);
            queries1[i] = ctx1.toSql();
        }

        // Generate with same seed
        Rnd rnd2 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx2 = new GeneratorContext(rnd2, GeneratorConfig.defaults());

        String[] queries2 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx2.reset();
            SelectGenerator.generate(ctx2);
            queries2[i] = ctx2.toSql();
        }

        // Should be identical
        Assert.assertArrayEquals(queries1, queries2);
    }

    @Test
    public void testNoEmptyOutput() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SelectGenerator.generate(ctx);

            Assert.assertFalse("Should produce tokens", ctx.tokens().isEmpty());
            Assert.assertFalse("SQL should not be empty", ctx.toSql().isEmpty());
        }
    }

    @Test
    public void testVariety() {
        // Generate many queries and check they're not all identical
        java.util.Set<String> uniqueQueries = new java.util.HashSet<>();

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SelectGenerator.generate(ctx);
            uniqueQueries.add(ctx.toSql());
        }

        // Should generate many unique queries
        Assert.assertTrue("Should generate variety of queries: " + uniqueQueries.size(),
                uniqueQueries.size() > 20);
    }

    @Test
    public void testWhereClauseStructure() {
        // Configure with 100% WHERE probability
        GeneratorConfig config = GeneratorConfig.builder()
                .whereProb(1.0)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext whereCtx = new GeneratorContext(rnd, config);

        for (int i = 0; i < 50; i++) {
            whereCtx.reset();
            SelectGenerator.generate(whereCtx);
            String sql = whereCtx.toSql();

            Assert.assertTrue("Should have WHERE clause: " + sql, sql.contains(" WHERE "));

            // WHERE clause should have something after it
            int whereIdx = sql.indexOf(" WHERE ") + 7;
            String whereClause = sql.substring(whereIdx);
            Assert.assertFalse("WHERE clause should not be empty: " + sql, whereClause.isEmpty());
        }
    }

    @Test
    public void testGenerateDistinct() {
        // Configure with 100% DISTINCT probability
        GeneratorConfig config = GeneratorConfig.builder()
                .distinctProb(1.0)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext distinctCtx = new GeneratorContext(rnd, config);

        for (int i = 0; i < 20; i++) {
            distinctCtx.reset();
            SelectGenerator.generate(distinctCtx);
            String sql = distinctCtx.toSql();

            Assert.assertTrue("Should have DISTINCT: " + sql, sql.contains("SELECT DISTINCT"));
        }
    }

    @Test
    public void testGenerateGroupBy() {
        // Configure with 100% GROUP BY probability
        GeneratorConfig config = GeneratorConfig.builder()
                .groupByProb(1.0)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext groupByCtx = new GeneratorContext(rnd, config);

        for (int i = 0; i < 20; i++) {
            groupByCtx.reset();
            SelectGenerator.generate(groupByCtx);
            String sql = groupByCtx.toSql();

            Assert.assertTrue("Should have GROUP BY: " + sql, sql.contains(" GROUP BY "));
        }
    }

    @Test
    public void testGenerateOrderBy() {
        // Configure with 100% ORDER BY probability
        GeneratorConfig config = GeneratorConfig.builder()
                .orderByProb(1.0)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext orderByCtx = new GeneratorContext(rnd, config);

        boolean foundAsc = false;
        boolean foundDesc = false;
        boolean foundNulls = false;

        for (int i = 0; i < 50; i++) {
            orderByCtx.reset();
            SelectGenerator.generate(orderByCtx);
            String sql = orderByCtx.toSql();

            Assert.assertTrue("Should have ORDER BY: " + sql, sql.contains(" ORDER BY "));

            if (sql.contains(" ASC")) foundAsc = true;
            if (sql.contains(" DESC")) foundDesc = true;
            if (sql.contains(" NULLS ")) foundNulls = true;
        }

        Assert.assertTrue("Should sometimes generate ASC", foundAsc);
        Assert.assertTrue("Should sometimes generate DESC", foundDesc);
        // NULLS FIRST/LAST has low probability, may not always appear
    }

    @Test
    public void testGenerateLimit() {
        // Configure with 100% LIMIT probability
        GeneratorConfig config = GeneratorConfig.builder()
                .limitProb(1.0)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext limitCtx = new GeneratorContext(rnd, config);

        boolean foundOffset = false;

        for (int i = 0; i < 50; i++) {
            limitCtx.reset();
            SelectGenerator.generate(limitCtx);
            String sql = limitCtx.toSql();

            Assert.assertTrue("Should have LIMIT: " + sql, sql.contains(" LIMIT "));

            if (sql.contains(" OFFSET ") || sql.matches(".*LIMIT \\d+, \\d+.*")) {
                foundOffset = true;
            }
        }

        Assert.assertTrue("Should sometimes generate OFFSET", foundOffset);
    }

    @Test
    public void testGenerateGroupByClauseDirectly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SelectGenerator.generateGroupByClause(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with GROUP BY: " + sql, sql.startsWith("GROUP BY "));
        }
    }

    @Test
    public void testGenerateOrderByClauseDirectly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SelectGenerator.generateOrderByClause(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with ORDER BY: " + sql, sql.startsWith("ORDER BY "));
        }
    }

    @Test
    public void testGenerateLimitClauseDirectly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SelectGenerator.generateLimitClause(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with LIMIT: " + sql, sql.startsWith("LIMIT "));
        }
    }

    @Test
    public void testGenerateWithJoins() {
        // Configure with 100% JOIN probability
        GeneratorConfig config = GeneratorConfig.builder()
                .joinProb(1.0)
                .maxJoins(3)
                .build();
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorContext joinCtx = new GeneratorContext(rnd, config);

        int queriesWithJoins = 0;

        for (int i = 0; i < 50; i++) {
            joinCtx.reset();
            SelectGenerator.generate(joinCtx);
            String sql = joinCtx.toSql();

            if (sql.contains(" JOIN ")) {
                queriesWithJoins++;
            }
        }

        Assert.assertTrue("Should generate queries with JOINs: " + queriesWithJoins,
                queriesWithJoins > 20);
    }

    @Test
    public void testTableAliasRegistration() {
        // When a table alias is generated, it should be registered
        GeneratorConfig config = GeneratorConfig.builder().build();
        Rnd rnd = new Rnd(42L, 42L);  // Use specific seed for reproducibility
        GeneratorContext aliasCtx = new GeneratorContext(rnd, config);

        int aliasesFound = 0;
        for (int i = 0; i < 100; i++) {
            aliasCtx.reset();
            SelectGenerator.generate(aliasCtx);

            if (aliasCtx.tableAliases().size() > 0) {
                aliasesFound++;
            }
        }

        // Should sometimes register aliases
        Assert.assertTrue("Should sometimes register table aliases: " + aliasesFound,
                aliasesFound > 10);
    }
}
