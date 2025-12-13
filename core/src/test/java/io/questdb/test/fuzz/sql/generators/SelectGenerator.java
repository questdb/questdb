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
import io.questdb.test.fuzz.sql.GeneratorContext;

/**
 * Generates SELECT statements.
 * <p>
 * Supports:
 * <ul>
 *   <li>SELECT column list (expressions with optional aliases)</li>
 *   <li>SELECT DISTINCT</li>
 *   <li>SELECT * (star)</li>
 *   <li>FROM single table (with optional alias)</li>
 *   <li>Optional WHERE clause</li>
 *   <li>Optional GROUP BY clause</li>
 *   <li>Optional ORDER BY clause (ASC/DESC, NULLS FIRST/LAST)</li>
 *   <li>Optional LIMIT/OFFSET clause</li>
 * </ul>
 * <p>
 * QuestDB-specific features:
 * <ul>
 *   <li>LATEST ON timestamp PARTITION BY columns</li>
 *   <li>SAMPLE BY interval [FILL] [ALIGN TO]</li>
 * </ul>
 * <p>
 * Note: QuestDB does not support HAVING clause.
 */
public final class SelectGenerator {

    private SelectGenerator() {
        // Static utility class
    }

    /**
     * Generates a complete SELECT statement.
     */
    public static void generate(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // SELECT keyword
        ctx.keyword("SELECT");

        // Optional DISTINCT
        if (rnd.nextDouble() < ctx.config().distinctProb()) {
            ctx.keyword("DISTINCT");
        }

        // Select list
        generateSelectList(ctx);

        // FROM clause
        generateFromClause(ctx);

        // Optional JOIN clauses
        if (rnd.nextDouble() < ctx.config().joinProb() && ctx.canAddJoin()) {
            int joinCount = 1 + rnd.nextInt(Math.min(3, ctx.config().maxJoins()));
            for (int i = 0; i < joinCount && ctx.canAddJoin(); i++) {
                JoinGenerator.generate(ctx);
            }
        }

        // Optional LATEST ON clause (QuestDB-specific)
        // Note: LATEST ON should come after JOINs but before WHERE
        if (rnd.nextDouble() < ctx.config().latestOnProb()) {
            LatestOnGenerator.generate(ctx);
        }

        // Optional WHERE clause
        if (rnd.nextDouble() < ctx.config().whereProb()) {
            generateWhereClause(ctx);
        }

        // Optional SAMPLE BY clause (QuestDB-specific)
        // SAMPLE BY should come after WHERE but before GROUP BY
        if (rnd.nextDouble() < ctx.config().sampleByProb()) {
            SampleByGenerator.generate(ctx);
        }

        // Optional GROUP BY clause
        if (rnd.nextDouble() < ctx.config().groupByProb()) {
            generateGroupByClause(ctx);
        }

        // Optional ORDER BY clause
        if (rnd.nextDouble() < ctx.config().orderByProb()) {
            generateOrderByClause(ctx);
        }

        // Optional LIMIT clause
        if (rnd.nextDouble() < ctx.config().limitProb()) {
            generateLimitClause(ctx);
        }
    }

    /**
     * Generates the SELECT list (columns/expressions).
     */
    public static void generateSelectList(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Decide between star and explicit columns
        if (rnd.nextDouble() < 0.15) {
            // SELECT *
            ctx.operator("*");
            return;
        }

        // Generate 1 to maxColumns expressions
        int columnCount = 1 + rnd.nextInt(ctx.config().maxColumns());

        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }

            // Generate expression with optional alias
            generateSelectItem(ctx);
        }
    }

    /**
     * Generates a single select item (expression with optional alias).
     */
    public static void generateSelectItem(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Decide what kind of select item to generate
        int variant = rnd.nextInt(10);

        if (variant < 4) {
            // Simple column reference (most common)
            ExpressionGenerator.generateColumnReference(ctx);
        } else if (variant < 7) {
            // Expression
            ExpressionGenerator.generateTerminal(ctx);
        } else {
            // More complex expression
            ExpressionGenerator.generate(ctx);
        }

        // Optional alias
        if (rnd.nextDouble() < 0.3) {
            ctx.keyword("AS");
            ctx.identifier(ctx.newAlias());
        }
    }

    /**
     * Generates the FROM clause.
     */
    public static void generateFromClause(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("FROM");

        // Table name
        String tableName = ctx.newTableName();
        ctx.identifier(tableName);

        // Optional table alias
        if (rnd.nextDouble() < 0.4) {
            String alias = ctx.newAlias();
            ctx.identifier(alias);
            ctx.tableAliases().add(alias);
        }
    }

    /**
     * Generates a WHERE clause.
     */
    public static void generateWhereClause(GeneratorContext ctx) {
        ctx.keyword("WHERE");
        ExpressionGenerator.generateBooleanExpression(ctx);
    }

    /**
     * Generates a GROUP BY clause.
     */
    public static void generateGroupByClause(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("GROUP");
        ctx.keyword("BY");

        // Generate 1 to maxGroupByColumns columns
        int columnCount = 1 + rnd.nextInt(ctx.config().maxGroupByColumns());

        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            // GROUP BY typically uses column references or positions
            if (rnd.nextDouble() < 0.8) {
                ExpressionGenerator.generateColumnReference(ctx);
            } else {
                // Column position (1-based)
                ctx.literal(String.valueOf(1 + rnd.nextInt(5)));
            }
        }
    }

    /**
     * Generates an ORDER BY clause.
     */
    public static void generateOrderByClause(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("ORDER");
        ctx.keyword("BY");

        // Generate 1 to maxOrderByColumns columns
        int columnCount = 1 + rnd.nextInt(ctx.config().maxOrderByColumns());

        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }

            // Column reference or position
            if (rnd.nextDouble() < 0.7) {
                ExpressionGenerator.generateColumnReference(ctx);
            } else {
                ctx.literal(String.valueOf(1 + rnd.nextInt(5)));
            }

            // Optional ASC/DESC
            if (rnd.nextDouble() < 0.5) {
                ctx.keyword(rnd.nextBoolean() ? "ASC" : "DESC");
            }

            // Optional NULLS FIRST/LAST
            if (rnd.nextDouble() < 0.2) {
                ctx.keyword("NULLS");
                ctx.keyword(rnd.nextBoolean() ? "FIRST" : "LAST");
            }
        }
    }

    /**
     * Generates a LIMIT clause with optional OFFSET.
     */
    public static void generateLimitClause(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("LIMIT");

        // LIMIT value (1-1000)
        ctx.literal(String.valueOf(1 + rnd.nextInt(1000)));

        // Optional OFFSET
        if (rnd.nextDouble() < 0.3) {
            // Two syntaxes: LIMIT n OFFSET m  or  LIMIT n, m
            if (rnd.nextBoolean()) {
                ctx.keyword("OFFSET");
                ctx.literal(String.valueOf(rnd.nextInt(100)));
            } else {
                ctx.comma();
                ctx.literal(String.valueOf(rnd.nextInt(100)));
            }
        }
    }

    /**
     * Generates a simple SELECT statement (minimal complexity).
     * Useful for testing or as subquery.
     */
    public static void generateSimple(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("SELECT");

        // Simple select list: 1-3 columns
        int columnCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            ExpressionGenerator.generateColumnReference(ctx);
        }

        ctx.keyword("FROM");
        ctx.identifier(ctx.newTableName());
    }

    /**
     * Generates a SELECT with explicit table and columns.
     * Useful when you need control over the generated identifiers.
     */
    public static void generateWithTable(GeneratorContext ctx, String tableName, String... columns) {
        ctx.keyword("SELECT");

        if (columns.length == 0) {
            ctx.operator("*");
        } else {
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) {
                    ctx.comma();
                }
                ctx.identifier(columns[i]);
            }
        }

        ctx.keyword("FROM");
        ctx.identifier(tableName);
    }

    /**
     * Generates a SELECT 1 statement (useful for testing).
     */
    public static void generateSelectOne(GeneratorContext ctx) {
        ctx.keyword("SELECT");
        ctx.literal("1");
    }

    /**
     * Generates a SELECT with a subquery in the FROM clause.
     * Note: For Phase 2 (basic), this uses simple recursion control.
     * More sophisticated depth tracking will be added in Phase 3.
     */
    public static void generateWithSubquery(GeneratorContext ctx) {
        if (!ctx.shouldRecurseQuery()) {
            // Fall back to simple select if we shouldn't recurse
            generateSimple(ctx);
            return;
        }

        Rnd rnd = ctx.rnd();

        ctx.keyword("SELECT");

        // Simple select list for outer query
        int columnCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            ctx.identifier(ctx.randomColumnName());
        }

        ctx.keyword("FROM");
        ctx.openParen();

        // Generate inner query (simple to avoid deep recursion)
        generateSimple(ctx);

        ctx.closeParen();

        // Subquery alias (required)
        String alias = ctx.newAlias();
        ctx.identifier(alias);
        ctx.tableAliases().add(alias);
    }
}
