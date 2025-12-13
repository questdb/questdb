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
 * Generates SQL expressions of various types.
 * <p>
 * Supports:
 * <ul>
 *   <li>Literal expressions (via LiteralGenerator)</li>
 *   <li>Column references (qualified and unqualified)</li>
 *   <li>Parenthesized expressions</li>
 *   <li>Binary operators: arithmetic (+, -, *, /, %), comparison (=, !=, <, >, <=, >=), logical (AND, OR)</li>
 *   <li>Unary operators: -, NOT</li>
 *   <li>Function calls (basic)</li>
 *   <li>CASE WHEN ... THEN ... ELSE ... END</li>
 *   <li>CAST(expr AS type)</li>
 *   <li>IN (list) and NOT IN</li>
 *   <li>BETWEEN ... AND ... and NOT BETWEEN</li>
 *   <li>LIKE and NOT LIKE</li>
 *   <li>IS NULL and IS NOT NULL</li>
 * </ul>
 * <p>
 * Uses depth tracking to prevent infinite recursion.
 */
public final class ExpressionGenerator {

    // Expression type weights
    private static final int WEIGHT_LITERAL = 20;
    private static final int WEIGHT_COLUMN = 25;
    private static final int WEIGHT_BINARY = 20;
    private static final int WEIGHT_UNARY = 5;
    private static final int WEIGHT_PARENS = 8;
    private static final int WEIGHT_FUNCTION = 5;
    private static final int WEIGHT_CASE = 4;
    private static final int WEIGHT_CAST = 3;
    private static final int WEIGHT_IN = 3;
    private static final int WEIGHT_BETWEEN = 3;
    private static final int WEIGHT_LIKE = 2;
    private static final int WEIGHT_IS_NULL = 2;
    private static final int TOTAL_WEIGHT = WEIGHT_LITERAL + WEIGHT_COLUMN + WEIGHT_BINARY
            + WEIGHT_UNARY + WEIGHT_PARENS + WEIGHT_FUNCTION + WEIGHT_CASE + WEIGHT_CAST
            + WEIGHT_IN + WEIGHT_BETWEEN + WEIGHT_LIKE + WEIGHT_IS_NULL;

    // At max depth, only generate terminals
    private static final int WEIGHT_TERMINAL_LITERAL = 50;
    private static final int WEIGHT_TERMINAL_COLUMN = 50;
    private static final int TOTAL_TERMINAL_WEIGHT = WEIGHT_TERMINAL_LITERAL + WEIGHT_TERMINAL_COLUMN;

    // Operators
    private static final String[] ARITHMETIC_OPS = {"+", "-", "*", "/"};
    private static final String[] COMPARISON_OPS = {"=", "!=", "<>", "<", ">", "<=", ">="};
    private static final String[] LOGICAL_OPS = {"AND", "OR"};
    private static final String[] UNARY_OPS = {"-", "NOT"};

    // Simple aggregate/scalar functions for basic testing
    private static final String[] FUNCTIONS = {
            "abs", "round", "floor", "ceil", "length", "lower", "upper", "trim"
    };

    // SQL types for CAST
    private static final String[] CAST_TYPES = {
            "INT", "LONG", "DOUBLE", "FLOAT", "STRING", "SYMBOL",
            "BOOLEAN", "TIMESTAMP", "DATE", "SHORT", "BYTE"
    };

    // LIKE patterns
    private static final String[] LIKE_PATTERNS = {
            "'%'", "'%a%'", "'a%'", "'%b'", "'_a%'", "'%_'", "'test%'", "'%test'"
    };

    private ExpressionGenerator() {
        // Static utility class
    }

    /**
     * Generates a random expression.
     */
    public static void generate(GeneratorContext ctx) {
        if (!ctx.shouldRecurseExpression()) {
            // At max depth, only generate terminals
            generateTerminal(ctx);
            return;
        }

        ctx.incrementExpressionDepth();
        try {
            Rnd rnd = ctx.rnd();
            int roll = rnd.nextInt(TOTAL_WEIGHT);
            int cumulative = 0;

            cumulative += WEIGHT_LITERAL;
            if (roll < cumulative) {
                LiteralGenerator.generate(ctx);
                return;
            }

            cumulative += WEIGHT_COLUMN;
            if (roll < cumulative) {
                generateColumnReference(ctx);
                return;
            }

            cumulative += WEIGHT_BINARY;
            if (roll < cumulative) {
                generateBinaryExpression(ctx);
                return;
            }

            cumulative += WEIGHT_UNARY;
            if (roll < cumulative) {
                generateUnaryExpression(ctx);
                return;
            }

            cumulative += WEIGHT_PARENS;
            if (roll < cumulative) {
                generateParenthesizedExpression(ctx);
                return;
            }

            cumulative += WEIGHT_FUNCTION;
            if (roll < cumulative) {
                generateFunctionCall(ctx);
                return;
            }

            cumulative += WEIGHT_CASE;
            if (roll < cumulative) {
                generateCaseExpression(ctx);
                return;
            }

            cumulative += WEIGHT_CAST;
            if (roll < cumulative) {
                generateCastExpression(ctx);
                return;
            }

            cumulative += WEIGHT_IN;
            if (roll < cumulative) {
                generateInExpression(ctx);
                return;
            }

            cumulative += WEIGHT_BETWEEN;
            if (roll < cumulative) {
                generateBetweenExpression(ctx);
                return;
            }

            cumulative += WEIGHT_LIKE;
            if (roll < cumulative) {
                generateLikeExpression(ctx);
                return;
            }

            generateIsNullExpression(ctx);
        } finally {
            ctx.decrementExpressionDepth();
        }
    }

    /**
     * Generates a terminal expression (no recursion).
     */
    public static void generateTerminal(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        int roll = rnd.nextInt(TOTAL_TERMINAL_WEIGHT);

        if (roll < WEIGHT_TERMINAL_LITERAL) {
            LiteralGenerator.generate(ctx);
        } else {
            generateColumnReference(ctx);
        }
    }

    /**
     * Generates a column reference.
     */
    public static void generateColumnReference(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Decide if qualified (table.column) or unqualified (column)
        if (ctx.tableAliases().size() > 0 && rnd.nextDouble() < 0.5) {
            // Qualified reference
            String alias = ctx.tableAliases().get(rnd.nextInt(ctx.tableAliases().size()));
            ctx.identifier(alias);
            ctx.dot();
            ctx.identifier(ctx.randomColumnName());
        } else {
            // Unqualified reference
            ctx.identifier(ctx.randomColumnName());
        }
    }

    /**
     * Generates a binary expression (left op right).
     */
    public static void generateBinaryExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Choose operator type
        int opType = rnd.nextInt(3);
        String op;

        switch (opType) {
            case 0:
                op = ARITHMETIC_OPS[rnd.nextInt(ARITHMETIC_OPS.length)];
                break;
            case 1:
                op = COMPARISON_OPS[rnd.nextInt(COMPARISON_OPS.length)];
                break;
            default:
                op = LOGICAL_OPS[rnd.nextInt(LOGICAL_OPS.length)];
                break;
        }

        // Generate left operand
        generate(ctx);

        // Operator
        ctx.operator(op);

        // Generate right operand
        generate(ctx);
    }

    /**
     * Generates a unary expression (op expr).
     */
    public static void generateUnaryExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String op = UNARY_OPS[rnd.nextInt(UNARY_OPS.length)];

        if (op.equals("-")) {
            // Numeric negation
            ctx.operator("-");
            // Prefer numeric operands
            if (rnd.nextBoolean()) {
                LiteralGenerator.generateNumeric(ctx);
            } else {
                generate(ctx);
            }
        } else {
            // NOT operator
            ctx.keyword("NOT");
            generate(ctx);
        }
    }

    /**
     * Generates a parenthesized expression.
     */
    public static void generateParenthesizedExpression(GeneratorContext ctx) {
        ctx.openParen();
        generate(ctx);
        ctx.closeParen();
    }

    /**
     * Generates a function call.
     */
    public static void generateFunctionCall(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String funcName = FUNCTIONS[rnd.nextInt(FUNCTIONS.length)];

        ctx.identifier(funcName);
        ctx.openParen();

        // Generate 1-3 arguments
        int argCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < argCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            generate(ctx);
        }

        ctx.closeParen();
    }

    // --- Specialized generators ---

    /**
     * Generates a boolean expression (for WHERE/HAVING clauses).
     */
    public static void generateBooleanExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        int variant = rnd.nextInt(5);

        ctx.incrementExpressionDepth();
        try {
            switch (variant) {
                case 0:
                    // Simple comparison
                    generateComparisonExpression(ctx);
                    break;
                case 1:
                    // AND expression
                    generateComparisonExpression(ctx);
                    ctx.keyword("AND");
                    generateComparisonExpression(ctx);
                    break;
                case 2:
                    // OR expression
                    generateComparisonExpression(ctx);
                    ctx.keyword("OR");
                    generateComparisonExpression(ctx);
                    break;
                case 3:
                    // NOT expression
                    ctx.keyword("NOT");
                    ctx.openParen();
                    generateComparisonExpression(ctx);
                    ctx.closeParen();
                    break;
                default:
                    // Nested boolean
                    ctx.openParen();
                    generateComparisonExpression(ctx);
                    ctx.closeParen();
                    ctx.keyword(rnd.nextBoolean() ? "AND" : "OR");
                    ctx.openParen();
                    generateComparisonExpression(ctx);
                    ctx.closeParen();
                    break;
            }
        } finally {
            ctx.decrementExpressionDepth();
        }
    }

    /**
     * Generates a comparison expression.
     */
    public static void generateComparisonExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String op = COMPARISON_OPS[rnd.nextInt(COMPARISON_OPS.length)];

        // Left side (prefer column)
        if (rnd.nextDouble() < 0.7) {
            generateColumnReference(ctx);
        } else {
            generateTerminal(ctx);
        }

        ctx.operator(op);

        // Right side (prefer literal)
        if (rnd.nextDouble() < 0.7) {
            LiteralGenerator.generate(ctx);
        } else {
            generateTerminal(ctx);
        }
    }

    /**
     * Generates an arithmetic expression.
     */
    public static void generateArithmeticExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String op = ARITHMETIC_OPS[rnd.nextInt(ARITHMETIC_OPS.length)];

        ctx.incrementExpressionDepth();
        try {
            // Left operand
            if (ctx.shouldRecurseExpression() && rnd.nextDouble() < 0.3) {
                generateArithmeticExpression(ctx);
            } else {
                if (rnd.nextBoolean()) {
                    generateColumnReference(ctx);
                } else {
                    LiteralGenerator.generateNumeric(ctx);
                }
            }

            ctx.operator(op);

            // Right operand
            if (ctx.shouldRecurseExpression() && rnd.nextDouble() < 0.3) {
                generateArithmeticExpression(ctx);
            } else {
                if (rnd.nextBoolean()) {
                    generateColumnReference(ctx);
                } else {
                    LiteralGenerator.generateNumeric(ctx);
                }
            }
        } finally {
            ctx.decrementExpressionDepth();
        }
    }

    /**
     * Generates an expression suitable for SELECT list (with optional alias).
     */
    public static void generateSelectExpression(GeneratorContext ctx, boolean withAlias) {
        generate(ctx);

        if (withAlias && ctx.rnd().nextDouble() < 0.3) {
            ctx.keyword("AS");
            ctx.identifier(ctx.newAlias());
        }
    }

    // --- Advanced expression generators ---

    /**
     * Generates a CASE WHEN ... THEN ... ELSE ... END expression.
     */
    public static void generateCaseExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("CASE");

        // Generate 1-3 WHEN branches
        int branchCount = 1 + rnd.nextInt(3);

        for (int i = 0; i < branchCount; i++) {
            ctx.keyword("WHEN");
            generateComparisonExpression(ctx);
            ctx.keyword("THEN");
            generateTerminal(ctx);
        }

        // Optional ELSE
        if (rnd.nextDouble() < 0.7) {
            ctx.keyword("ELSE");
            generateTerminal(ctx);
        }

        ctx.keyword("END");
    }

    /**
     * Generates a CAST(expr AS type) expression.
     */
    public static void generateCastExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("CAST");
        ctx.openParen();
        generateTerminal(ctx);
        ctx.keyword("AS");
        ctx.keyword(CAST_TYPES[rnd.nextInt(CAST_TYPES.length)]);
        ctx.closeParen();
    }

    /**
     * Generates an IN or NOT IN expression.
     */
    public static void generateInExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Left side
        generateColumnReference(ctx);

        // Optional NOT
        if (rnd.nextDouble() < 0.3) {
            ctx.keyword("NOT");
        }

        ctx.keyword("IN");
        ctx.openParen();

        // Generate 2-5 values
        int valueCount = 2 + rnd.nextInt(4);

        for (int i = 0; i < valueCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            LiteralGenerator.generate(ctx);
        }

        ctx.closeParen();
    }

    /**
     * Generates a BETWEEN ... AND ... expression.
     */
    public static void generateBetweenExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Value to test
        generateColumnReference(ctx);

        // Optional NOT
        if (rnd.nextDouble() < 0.3) {
            ctx.keyword("NOT");
        }

        ctx.keyword("BETWEEN");

        // Lower bound
        LiteralGenerator.generateNumeric(ctx);

        ctx.keyword("AND");

        // Upper bound
        LiteralGenerator.generateNumeric(ctx);
    }

    /**
     * Generates a LIKE or NOT LIKE expression.
     */
    public static void generateLikeExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Column reference
        generateColumnReference(ctx);

        // Optional NOT
        if (rnd.nextDouble() < 0.3) {
            ctx.keyword("NOT");
        }

        ctx.keyword("LIKE");

        // Pattern
        ctx.literal(LIKE_PATTERNS[rnd.nextInt(LIKE_PATTERNS.length)]);
    }

    /**
     * Generates an IS NULL or IS NOT NULL expression.
     */
    public static void generateIsNullExpression(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Column reference
        generateColumnReference(ctx);

        ctx.keyword("IS");

        // Optional NOT
        if (rnd.nextDouble() < 0.5) {
            ctx.keyword("NOT");
        }

        ctx.keyword("NULL");
    }
}
