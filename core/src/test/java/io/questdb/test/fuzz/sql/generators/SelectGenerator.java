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
 * Generates basic SELECT statements.
 * <p>
 * Supports:
 * <ul>
 *   <li>SELECT column list (expressions with optional aliases)</li>
 *   <li>SELECT * (star)</li>
 *   <li>FROM single table (with optional alias)</li>
 *   <li>Optional WHERE clause</li>
 * </ul>
 * <p>
 * This is a basic implementation for Phase 2. More advanced features
 * (GROUP BY, HAVING, ORDER BY, joins, subqueries) will be added in Phase 3.
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

        // Select list
        generateSelectList(ctx);

        // FROM clause
        generateFromClause(ctx);

        // Optional WHERE clause
        if (rnd.nextDouble() < ctx.config().whereProb()) {
            generateWhereClause(ctx);
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
