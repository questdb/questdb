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

import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GeneratorContextTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private Rnd rnd;
    private GeneratorConfig config;
    private GeneratorContext ctx;

    @Before
    public void setUp() {
        rnd = new Rnd(SEED0, SEED1);
        config = GeneratorConfig.defaults();
        ctx = new GeneratorContext(rnd, config);
    }

    @Test
    public void testInitialState() {
        Assert.assertSame(rnd, ctx.rnd());
        Assert.assertSame(config, ctx.config());
        Assert.assertEquals(0, ctx.queryDepth());
        Assert.assertEquals(0, ctx.expressionDepth());
        Assert.assertEquals(0, ctx.cteNames().size());
        Assert.assertEquals(0, ctx.tableAliases().size());
        Assert.assertEquals(0, ctx.columnNames().size());
        Assert.assertFalse(ctx.inAggregateContext());
        Assert.assertFalse(ctx.hasTimestampColumn());
        Assert.assertFalse(ctx.inWindowFunction());
    }

    @Test
    public void testReset() {
        // Modify state
        ctx.registerCte("cte1");
        ctx.registerTableAlias("t1");
        ctx.registerColumn("col1");
        ctx.setInAggregateContext(true);
        ctx.setHasTimestampColumn(true);
        ctx.setInWindowFunction(true);
        ctx.incrementExpressionDepth();

        // Reset
        ctx.reset();

        // Verify state is cleared
        Assert.assertEquals(0, ctx.queryDepth());
        Assert.assertEquals(0, ctx.expressionDepth());
        Assert.assertEquals(0, ctx.cteNames().size());
        Assert.assertEquals(0, ctx.tableAliases().size());
        Assert.assertEquals(0, ctx.columnNames().size());
        Assert.assertFalse(ctx.inAggregateContext());
        Assert.assertFalse(ctx.hasTimestampColumn());
        Assert.assertFalse(ctx.inWindowFunction());
    }

    @Test
    public void testScopeRegistration() {
        ctx.registerCte("cte1");
        ctx.registerCte("cte2");
        Assert.assertEquals(2, ctx.cteNames().size());
        Assert.assertEquals("cte1", ctx.cteNames().get(0));
        Assert.assertEquals("cte2", ctx.cteNames().get(1));

        ctx.registerTableAlias("t1");
        ctx.registerTableAlias("t2");
        Assert.assertEquals(2, ctx.tableAliases().size());

        ctx.registerColumn("col1");
        Assert.assertEquals(1, ctx.columnNames().size());
    }

    @Test
    public void testClearScope() {
        ctx.registerCte("cte1");
        ctx.registerTableAlias("t1");
        ctx.registerColumn("col1");

        ctx.clearTableAliases();
        Assert.assertEquals(0, ctx.tableAliases().size());
        Assert.assertEquals(1, ctx.cteNames().size());  // CTEs unchanged

        ctx.clearColumns();
        Assert.assertEquals(0, ctx.columnNames().size());
    }

    @Test
    public void testNameGeneration() {
        Assert.assertEquals("t1", ctx.newTableName());
        Assert.assertEquals("t2", ctx.newTableName());
        Assert.assertEquals("t3", ctx.newTableName());

        Assert.assertEquals("cte1", ctx.newCteName());
        Assert.assertEquals("cte2", ctx.newCteName());

        Assert.assertEquals("col1", ctx.newColumnName());
        Assert.assertEquals("col2", ctx.newColumnName());

        Assert.assertEquals("a1", ctx.newAlias());
        Assert.assertEquals("a2", ctx.newAlias());
    }

    @Test
    public void testRandomNames() {
        // Random table name is t1-t5
        for (int i = 0; i < 100; i++) {
            String name = ctx.randomTableName();
            Assert.assertTrue(name.matches("t[1-5]"));
        }

        // Random column name is col1-col10 or ts/sym
        for (int i = 0; i < 100; i++) {
            String name = ctx.randomColumnName();
            Assert.assertTrue(
                    name.equals("ts") || name.equals("sym") || name.matches("col[1-9]|col10")
            );
        }

        // Random function name is func1-func20
        for (int i = 0; i < 100; i++) {
            String name = ctx.randomFunctionName();
            Assert.assertTrue(name.matches("func[1-9]|func1[0-9]|func20"));
        }
    }

    @Test
    public void testRandomColumnReference() {
        // Without table aliases, just column name
        String ref = ctx.randomColumnReference();
        Assert.assertFalse(ref.contains("."));

        // With table aliases, may be qualified
        ctx.registerTableAlias("t1");
        ctx.registerTableAlias("t2");

        int qualified = 0;
        for (int i = 0; i < 100; i++) {
            ref = ctx.randomColumnReference();
            if (ref.contains(".")) {
                qualified++;
                Assert.assertTrue(ref.startsWith("t1.") || ref.startsWith("t2."));
            }
        }
        // Should have some qualified and some unqualified
        Assert.assertTrue(qualified > 0);
        Assert.assertTrue(qualified < 100);
    }

    @Test
    public void testRandomCteAndAliasFromScope() {
        Assert.assertNull(ctx.randomCteName());
        Assert.assertNull(ctx.randomTableAlias());

        ctx.registerCte("cte1");
        ctx.registerCte("cte2");
        ctx.registerTableAlias("t1");

        for (int i = 0; i < 100; i++) {
            String cte = ctx.randomCteName();
            Assert.assertTrue(cte.equals("cte1") || cte.equals("cte2"));
        }

        Assert.assertEquals("t1", ctx.randomTableAlias());
    }

    @Test
    public void testExpressionDepthTracking() {
        Assert.assertEquals(0, ctx.expressionDepth());
        ctx.incrementExpressionDepth();
        Assert.assertEquals(1, ctx.expressionDepth());
        ctx.incrementExpressionDepth();
        Assert.assertEquals(2, ctx.expressionDepth());
        ctx.decrementExpressionDepth();
        Assert.assertEquals(1, ctx.expressionDepth());
    }

    @Test
    public void testShouldRecurseQuery() {
        GeneratorConfig simpleConfig = GeneratorConfig.builder().maxDepth(2).build();
        GeneratorContext simpleCtx = new GeneratorContext(new Rnd(SEED0, SEED1), simpleConfig);

        // At depth 0, should usually recurse
        int recursions = 0;
        for (int i = 0; i < 100; i++) {
            simpleCtx.reset();
            if (simpleCtx.shouldRecurseQuery()) {
                recursions++;
            }
        }
        Assert.assertTrue(recursions > 30);  // Should recurse often at depth 0

        // Create child contexts to increase depth
        GeneratorContext child = simpleCtx.childContext();  // depth 1
        GeneratorContext grandchild = child.childContext();  // depth 2

        // At max depth, should never recurse
        Assert.assertFalse(grandchild.shouldRecurseQuery());
    }

    @Test
    public void testShouldRecurseExpression() {
        GeneratorConfig simpleConfig = GeneratorConfig.builder().maxExpressionDepth(2).build();
        GeneratorContext simpleCtx = new GeneratorContext(new Rnd(SEED0, SEED1), simpleConfig);

        // At max expression depth, should never recurse
        simpleCtx.incrementExpressionDepth();
        simpleCtx.incrementExpressionDepth();
        Assert.assertFalse(simpleCtx.shouldRecurseExpression());
    }

    @Test
    public void testChildContext() {
        ctx.registerCte("cte1");
        ctx.registerTableAlias("t1");
        ctx.setHasTimestampColumn(true);

        GeneratorContext child = ctx.childContext();

        // Child inherits scope
        Assert.assertEquals(1, child.cteNames().size());
        Assert.assertEquals("cte1", child.cteNames().get(0));
        Assert.assertEquals(1, child.tableAliases().size());
        Assert.assertTrue(child.hasTimestampColumn());

        // Child has incremented depth
        Assert.assertEquals(1, child.queryDepth());
        Assert.assertEquals(0, child.expressionDepth());

        // Child scope is independent
        child.registerCte("cte2");
        Assert.assertEquals(2, child.cteNames().size());
        Assert.assertEquals(1, ctx.cteNames().size());  // Parent unchanged

        // Child counters continue from parent
        ctx.newTableName();  // t1
        child = ctx.childContext();
        Assert.assertEquals("t2", child.newTableName());  // Continues from t1
    }

    @Test
    public void testTokenBuilding() {
        ctx.keyword("SELECT")
                .identifier("a")
                .punctuation(",")
                .identifier("b")
                .keyword("FROM")
                .identifier("t");

        Assert.assertEquals("SELECT a, b FROM t", ctx.toSql());
    }

    @Test
    public void testTokenBuildingWithOperators() {
        ctx.keyword("SELECT")
                .identifier("a")
                .operator("+")
                .literal("1")
                .keyword("FROM")
                .identifier("t");

        Assert.assertEquals("SELECT a + 1 FROM t", ctx.toSql());
    }

    @Test
    public void testTokenBuildingWithParens() {
        ctx.keyword("SELECT")
                .identifier("func")
                .openParen()
                .identifier("a")
                .comma()
                .identifier("b")
                .closeParen()
                .keyword("FROM")
                .identifier("t");

        Assert.assertEquals("SELECT func(a, b) FROM t", ctx.toSql());
    }

    @Test
    public void testCanAddCte() {
        GeneratorConfig config = GeneratorConfig.builder().maxCteCount(2).build();
        GeneratorContext ctx = new GeneratorContext(new Rnd(SEED0, SEED1), config);

        Assert.assertTrue(ctx.canAddCte());
        ctx.registerCte("cte1");
        Assert.assertTrue(ctx.canAddCte());
        ctx.registerCte("cte2");
        Assert.assertFalse(ctx.canAddCte());
    }

    @Test
    public void testCanAddJoin() {
        GeneratorConfig config = GeneratorConfig.builder().maxJoins(2).build();
        GeneratorContext ctx = new GeneratorContext(new Rnd(SEED0, SEED1), config);

        Assert.assertTrue(ctx.canAddJoin());
        ctx.registerTableAlias("t1");
        Assert.assertTrue(ctx.canAddJoin());
        ctx.registerTableAlias("t2");
        Assert.assertTrue(ctx.canAddJoin());  // <= maxJoins
        ctx.registerTableAlias("t3");
        Assert.assertFalse(ctx.canAddJoin());  // > maxJoins
    }

    @Test
    public void testWithProbability() {
        int trueCount = 0;
        for (int i = 0; i < 1000; i++) {
            if (ctx.withProbability(0.5)) {
                trueCount++;
            }
        }
        // With p=0.5, should be roughly 500 (allow +-100 for randomness)
        Assert.assertTrue(trueCount > 400 && trueCount < 600);
    }

    @Test
    public void testNextIntAndBoolean() {
        // Just verify they work without exceptions
        for (int i = 0; i < 100; i++) {
            int n = ctx.nextInt(10);
            Assert.assertTrue(n >= 0 && n < 10);
            ctx.nextBoolean();  // Just verify no exception
        }
    }
}
