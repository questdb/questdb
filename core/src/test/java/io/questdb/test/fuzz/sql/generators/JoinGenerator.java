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
 * Generates SQL JOIN clauses.
 * <p>
 * Supports standard SQL joins:
 * <ul>
 *   <li>INNER JOIN</li>
 *   <li>LEFT [OUTER] JOIN</li>
 *   <li>RIGHT [OUTER] JOIN</li>
 *   <li>CROSS JOIN</li>
 *   <li>JOIN ... ON condition</li>
 *   <li>JOIN ... USING (columns)</li>
 * </ul>
 * <p>
 * And QuestDB-specific time-series joins:
 * <ul>
 *   <li>ASOF JOIN ... ON timestamp_condition [TOLERANCE interval]</li>
 *   <li>LT JOIN ... ON timestamp_condition [TOLERANCE interval]</li>
 *   <li>SPLICE JOIN ... ON timestamp_condition</li>
 * </ul>
 */
public final class JoinGenerator {

    // Standard join types
    private static final String[] STANDARD_JOIN_TYPES = {
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
            "RIGHT OUTER JOIN",
            "CROSS JOIN"
    };

    // QuestDB time-series join types
    private static final String[] QUESTDB_JOIN_TYPES = {
            "ASOF JOIN",
            "LT JOIN",
            "SPLICE JOIN"
    };

    // All join types combined
    private static final String[] ALL_JOIN_TYPES;
    static {
        ALL_JOIN_TYPES = new String[STANDARD_JOIN_TYPES.length + QUESTDB_JOIN_TYPES.length];
        System.arraycopy(STANDARD_JOIN_TYPES, 0, ALL_JOIN_TYPES, 0, STANDARD_JOIN_TYPES.length);
        System.arraycopy(QUESTDB_JOIN_TYPES, 0, ALL_JOIN_TYPES, STANDARD_JOIN_TYPES.length, QUESTDB_JOIN_TYPES.length);
    }

    // Join types that require ON/USING clause (CROSS JOIN doesn't)
    private static final String[] CONDITIONAL_JOIN_TYPES = {
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
            "RIGHT OUTER JOIN"
    };

    // Time intervals for TOLERANCE clause
    private static final String[] TOLERANCE_INTERVALS = {
            "1s", "5s", "10s", "30s",
            "1m", "5m", "10m", "30m",
            "1h", "2h", "6h", "12h",
            "1d", "7d"
    };

    private JoinGenerator() {
        // Static utility class
    }

    /**
     * Generates a random JOIN clause.
     * Includes standard SQL joins and QuestDB-specific time-series joins.
     */
    public static void generate(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Decide between standard and QuestDB joins based on config
        if (rnd.nextDouble() < ctx.config().questdbJoinProb()) {
            generateQuestdbJoin(ctx);
        } else {
            generateStandardJoin(ctx);
        }
    }

    /**
     * Generates a standard SQL JOIN clause.
     */
    public static void generateStandardJoin(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String joinType = STANDARD_JOIN_TYPES[rnd.nextInt(STANDARD_JOIN_TYPES.length)];
        generateJoinWithType(ctx, joinType);
    }

    /**
     * Generates a QuestDB-specific time-series JOIN clause (ASOF, LT, or SPLICE).
     */
    public static void generateQuestdbJoin(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String joinType = QUESTDB_JOIN_TYPES[rnd.nextInt(QUESTDB_JOIN_TYPES.length)];
        generateJoinWithType(ctx, joinType);
    }

    /**
     * Generates a JOIN clause with a specific join type.
     */
    public static void generateJoinWithType(GeneratorContext ctx, String joinType) {
        Rnd rnd = ctx.rnd();

        // Output join type keywords
        for (String keyword : joinType.split(" ")) {
            ctx.keyword(keyword);
        }

        // Table reference
        String tableName = ctx.newTableName();
        ctx.identifier(tableName);

        // Optional table alias
        String alias = null;
        if (rnd.nextDouble() < 0.6) {
            alias = ctx.newAlias();
            ctx.identifier(alias);
            ctx.tableAliases().add(alias);
        }

        String tableRef = alias != null ? alias : tableName;

        // Handle different join types
        if (joinType.contains("CROSS")) {
            // CROSS JOIN doesn't need ON/USING
            return;
        }

        if (isQuestdbJoin(joinType)) {
            // QuestDB time-series joins require ON clause with timestamp
            generateTimestampOnCondition(ctx, tableRef);

            // ASOF and LT can have optional TOLERANCE
            if (!joinType.contains("SPLICE") && rnd.nextDouble() < 0.3) {
                generateToleranceClause(ctx);
            }
        } else {
            // Standard joins use ON or USING
            generateJoinCondition(ctx, tableRef);
        }
    }

    /**
     * Checks if this is a QuestDB time-series join type.
     */
    private static boolean isQuestdbJoin(String joinType) {
        return joinType.contains("ASOF") || joinType.contains("LT") || joinType.contains("SPLICE");
    }

    /**
     * Generates an INNER JOIN clause.
     */
    public static void generateInnerJoin(GeneratorContext ctx) {
        generateJoinWithType(ctx, "INNER JOIN");
    }

    /**
     * Generates a LEFT JOIN clause.
     */
    public static void generateLeftJoin(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String joinType = rnd.nextBoolean() ? "LEFT JOIN" : "LEFT OUTER JOIN";
        generateJoinWithType(ctx, joinType);
    }

    /**
     * Generates a RIGHT JOIN clause.
     */
    public static void generateRightJoin(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String joinType = rnd.nextBoolean() ? "RIGHT JOIN" : "RIGHT OUTER JOIN";
        generateJoinWithType(ctx, joinType);
    }

    /**
     * Generates a CROSS JOIN clause.
     */
    public static void generateCrossJoin(GeneratorContext ctx) {
        generateJoinWithType(ctx, "CROSS JOIN");
    }

    /**
     * Generates the join condition (ON or USING clause).
     */
    public static void generateJoinCondition(GeneratorContext ctx, String rightTableOrAlias) {
        Rnd rnd = ctx.rnd();

        // Choose between ON and USING
        if (rnd.nextDouble() < 0.7) {
            generateOnCondition(ctx, rightTableOrAlias);
        } else {
            generateUsingClause(ctx);
        }
    }

    /**
     * Generates an ON condition for a join.
     */
    public static void generateOnCondition(GeneratorContext ctx, String rightTableOrAlias) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("ON");

        // Generate join condition(s)
        int conditionCount = 1 + rnd.nextInt(2);  // 1-2 conditions

        for (int i = 0; i < conditionCount; i++) {
            if (i > 0) {
                ctx.keyword("AND");
            }

            // Left side: existing table column
            if (ctx.tableAliases().size() > 0 && rnd.nextDouble() < 0.7) {
                String leftAlias = ctx.tableAliases().get(rnd.nextInt(ctx.tableAliases().size()));
                ctx.identifier(leftAlias);
                ctx.dot();
            }
            ctx.identifier(ctx.randomColumnName());

            // Operator (usually =)
            ctx.operator("=");

            // Right side: new table column
            ctx.identifier(rightTableOrAlias);
            ctx.dot();
            ctx.identifier(ctx.randomColumnName());
        }
    }

    /**
     * Generates a USING clause for a join.
     */
    public static void generateUsingClause(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("USING");
        ctx.openParen();

        // Generate 1-3 column names
        int columnCount = 1 + rnd.nextInt(3);

        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            ctx.identifier(ctx.randomColumnName());
        }

        ctx.closeParen();
    }

    /**
     * Generates multiple JOINs for a query.
     */
    public static void generateMultipleJoins(GeneratorContext ctx, int joinCount) {
        for (int i = 0; i < joinCount; i++) {
            generate(ctx);
        }
    }

    /**
     * Generates a conditional join (non-CROSS).
     */
    public static void generateConditionalJoin(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        String joinType = CONDITIONAL_JOIN_TYPES[rnd.nextInt(CONDITIONAL_JOIN_TYPES.length)];
        generateJoinWithType(ctx, joinType);
    }

    // --- QuestDB-specific join methods ---

    /**
     * Generates an ASOF JOIN clause.
     * ASOF JOIN matches the closest timestamp that is less than or equal to the lookup timestamp.
     */
    public static void generateAsofJoin(GeneratorContext ctx) {
        generateJoinWithType(ctx, "ASOF JOIN");
    }

    /**
     * Generates an LT JOIN clause.
     * LT JOIN matches the closest timestamp that is strictly less than the lookup timestamp.
     */
    public static void generateLtJoin(GeneratorContext ctx) {
        generateJoinWithType(ctx, "LT JOIN");
    }

    /**
     * Generates a SPLICE JOIN clause.
     * SPLICE JOIN combines rows from two tables ordered by timestamp.
     */
    public static void generateSpliceJoin(GeneratorContext ctx) {
        generateJoinWithType(ctx, "SPLICE JOIN");
    }

    /**
     * Generates an ON condition for timestamp-based joins.
     * Used by ASOF, LT, and SPLICE joins.
     */
    public static void generateTimestampOnCondition(GeneratorContext ctx, String rightTableOrAlias) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("ON");

        // Left side: timestamp column from existing table
        if (ctx.tableAliases().size() > 0 && rnd.nextDouble() < 0.7) {
            String leftAlias = ctx.tableAliases().get(rnd.nextInt(ctx.tableAliases().size()));
            ctx.identifier(leftAlias);
            ctx.dot();
        }
        // Use "ts" or "timestamp" as common timestamp column names
        ctx.identifier(rnd.nextBoolean() ? "ts" : "timestamp");

        ctx.operator("=");

        // Right side: timestamp column from joined table
        ctx.identifier(rightTableOrAlias);
        ctx.dot();
        ctx.identifier(rnd.nextBoolean() ? "ts" : "timestamp");
    }

    /**
     * Generates a TOLERANCE clause for ASOF and LT joins.
     * Example: TOLERANCE 1h, TOLERANCE 30m
     */
    public static void generateToleranceClause(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("TOLERANCE");
        ctx.literal(TOLERANCE_INTERVALS[rnd.nextInt(TOLERANCE_INTERVALS.length)]);
    }
}
