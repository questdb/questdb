/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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


package io.questdb.test.griffin.fuzz.clauses;

import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.FuzzColumn;
import io.questdb.test.griffin.fuzz.FuzzNames;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ColumnRefExpr;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * Equi-join GROUP BY across two WAL tables: the shape that the fused parallel hash join GROUP BY
 * ({@code Async Hash Join Group By}) replaces. QueryRunner runs every query of this shape with the
 * fused plan on and off and compares the results, so the generator aims most of its queries at
 * what {@code HashJoinGroupByCandidate.analyse()} accepts and sends the rest through the features
 * that must keep the ordinary plan.
 * <p>
 * Shape:
 * <pre>
 * SELECT [key AS e0[, key AS e1],] agg AS a0[, ...], count() AS aN, count(x.k) AS aN+1
 * FROM input l (JOIN | LEFT JOIN | RIGHT JOIN) input r ON l.key = r.key [AND (one-side predicate)]
 * [WHERE conjunct [AND ...]]
 * [GROUP BY e0[, e1] | SAMPLE BY interval [FILL(...)] [ALIGN TO CALENDAR | ALIGN TO FIRST OBSERVATION]]
 * [ORDER BY ...]
 * [LIMIT N]
 * </pre>
 * The join key is one of the four shared key columns, or two of them together: the INT column
 * {@code k} and the SYMBOL column {@code sym}, which the fused plan reads through its narrow INT
 * layout, and the LONG column {@code lk} and the VARCHAR column {@code vk}, which it stages
 * through a {@link io.questdb.cairo.RecordSink} into a map, as it stages every composite key and
 * the SYMBOL-against-VARCHAR pair. Grouping keys, aggregate arguments and single-side predicates
 * come from either input, so every join type sees them on the probe, on the build and on the
 * null-extended side. Each input is the table itself, a projection over it that renames some
 * columns and may filter (the fused planner resolves columns through it), a {@code LATEST ON}
 * sub-query, or a {@code LIMIT} sub-query (a barrier the fused planner must refuse).
 * <p>
 * Most queries reference only the column types and aggregates the fused plan reads. The rest
 * reach the features the fused planner has to turn down, each of which keeps the ordinary plan
 * in both arms of the fused axis: a constant WHERE conjunct, SAMPLE BY with FILL, a
 * {@code LIMIT} sub-query, {@code LATEST ON} on a table input, a cross-input WHERE predicate, an ON predicate over the preserved side
 * of an outer join, columns of other types, and aggregates outside the fused
 * allowlist. A planner that accepts one of them by mistake returns different rows than the
 * ordinary plan, and the fused axis reports it.
 * <p>
 * Two exact counts close every projection: {@code count()} counts the joined pairs of the group,
 * and {@code count(x.k)} counts the null-extended side's key, so a match that turns into a
 * null-extended row changes it. FLOAT and DOUBLE aggregates compare within the runner's
 * floating-point tolerance, and the counts cannot absorb a dropped or duplicated pair the way
 * that tolerance can. The tolerance is relative to the result, so the generator leaves out the
 * aggregates whose result can cancel to near zero while their inputs stay large (covariance,
 * correlation, regression, skewness, kurtosis): their reduction-order noise would exceed it.
 * HashJoinGroupByAggregatesTest covers them. The fuzz integers are small, so integer sums,
 * averages, variances and weighted standard deviations stay exact or nearly so.
 */
public final class HashJoinGroupByClause {
    private static final String[] BOOLEAN_DDLS = {"BOOLEAN"};
    // count() arguments whose count class the fused plan accepts: CountInt, CountLong,
    // CountFloat, CountDouble and CountSymbol, each for its own argument type only.
    private static final String[] COUNT_ARGUMENT_DDLS = {"INT", "LONG", "FLOAT", "DOUBLE", "SYMBOL"};
    // The optimiser moves a constant conjunct into the join model's constant WHERE clause,
    // which the fused planner must refuse. Only a false one makes a planner that accepts it
    // visible, so three in four are false.
    private static final String[] CONSTANT_PREDICATES = {"1 = 0", "false", "2 < 1", "1 = 1"};
    private static final String[] CROSS_INPUT_OPS = {"<", "<=", ">", ">=", "!="};
    // Filled SAMPLE BY keeps the ordinary plan; FILL(NONE) does not fill and fuses. FILL(0)
    // needs one value per aggregate, so appendFill() spells it out.
    private static final String[] FILLS = {"FILL(NULL)", "FILL(PREV)", null, "FILL(NONE)"};
    private static final String[] FLOATING_DDLS = {"FLOAT", "DOUBLE"};
    // Column types the fused plan reads, HashJoinGroupByCandidate.supportsValueType().
    private static final String[] FUSABLE_DDLS = {
            "BOOLEAN", "BYTE", "SHORT", "CHAR", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE", "SYMBOL"
    };
    // Expression grouping-key kinds whose results the fused plan reads.
    private static final ColumnKind[] FUSABLE_KEY_KINDS = {ColumnKind.NUMERIC, ColumnKind.TEMPORAL, ColumnKind.BOOLEAN, ColumnKind.CHAR};
    private static final String[] INTEGER_DDLS = {"BYTE", "SHORT", "INT", "LONG"};
    private static final String INT_KEY = "k";
    private static final String[] JOIN_KINDS = {"JOIN", "LEFT JOIN", "RIGHT JOIN"};
    private static final String LEFT_ALIAS = "l";
    private static final String LONG_KEY = "lk";
    // min() and max() arguments. The fused plan accepts every type but BOOLEAN and BYTE, which the
    // parser passes to classes registered for other argument types, so those two keep the ordinary plan.
    private static final String[] MIN_MAX_DDLS = {
            "BOOLEAN", "BYTE", "SHORT", "CHAR", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"
    };
    private static final String RIGHT_ALIAS = "r";
    // The fuzz tables span 30 to 75 hours, so every interval yields several buckets.
    private static final String[] SAMPLE_BY_INTERVALS = {"1h", "6h", "1d"};
    private static final String SYMBOL_KEY = "sym";
    private static final String VARCHAR_KEY = "vk";
    private static final String[] VARIANCE_FUNCTIONS = {"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop"};
    // The last three are the weighted stddev functions, which appendAggregate() also emits with an abs() weight.
    private static final String[] WEIGHTED_FUNCTIONS = {
            "weighted_avg", "vwap", "weighted_stddev_rel", "weighted_stddev", "weighted_stddev_freq"
    };

    private HashJoinGroupByClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzTable left, FuzzTable right, BindContext ctx, boolean injectFaultFn) {
        // The draws below depend neither on ctx nor on injectFaultFn, so the bind variant
        // regenerates the same tree.
        final String joinKind = JOIN_KINDS[rnd.nextInt(JOIN_KINDS.length)];
        final boolean isRightJoin = joinKind.startsWith("RIGHT");
        final boolean isLeftJoin = joinKind.startsWith("LEFT");
        // One query in twenty may reference any column, which exercises the fallback for the
        // column types the fused plan does not read.
        final boolean isFusableOnly = rnd.nextInt(20) != 0;
        final Input l = pickInput(rnd, left, isFusableOnly, ctx);
        final Input r = pickInput(rnd, right, isFusableOnly, ctx);

        StringSink sql = new StringSink();
        sql.put("SELECT ");
        final int keyCount = rnd.nextBoolean() ? 0 : 1 + rnd.nextInt(2);
        for (int i = 0; i < keyCount; i++) {
            final boolean isLeft = rnd.nextBoolean();
            appendGroupingKey(sql, rnd, isLeft ? l : r, isLeft ? LEFT_ALIAS : RIGHT_ALIAS, ctx);
            sql.put(" AS e").put(i).put(", ");
        }
        int aggCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < aggCount; i++) {
            final boolean isLeft = rnd.nextBoolean();
            appendAggregate(sql, rnd, isLeft ? l : r, isLeft ? LEFT_ALIAS : RIGHT_ALIAS);
            sql.put(" AS a").put(i).put(", ");
        }
        // The row-set guards; see the class comment.
        sql.put("count() AS a").put(aggCount++);
        sql.put(", count(").put(isRightJoin ? LEFT_ALIAS : RIGHT_ALIAS).put('.')
                .put((isRightJoin ? l : r).name(INT_KEY)).put(") AS a").put(aggCount++);

        sql.put(" FROM ").put(l.fromSql).put(' ').put(LEFT_ALIAS);
        sql.put(' ').put(joinKind).put(' ').put(r.fromSql).put(' ').put(RIGHT_ALIAS);
        // 0-11: INT key and 12-23: SYMBOL key, the two the narrow INT layout reads.
        // 24-29: LONG key, 30-35: VARCHAR key, 36-37: SYMBOL against VARCHAR and
        // 38-39: INT and SYMBOL together, the four that stage their key into a map.
        final int keyPick = rnd.nextInt(40);
        sql.put(" ON ");
        if (keyPick < 12 || keyPick >= 38) {
            appendKeyEquality(sql, rnd, l.name(INT_KEY), r.name(INT_KEY));
        } else if (keyPick < 24) {
            appendKeyEquality(sql, rnd, l.name(SYMBOL_KEY), r.name(SYMBOL_KEY));
        } else if (keyPick < 30) {
            appendKeyEquality(sql, rnd, l.name(LONG_KEY), r.name(LONG_KEY));
        } else if (keyPick < 36) {
            appendKeyEquality(sql, rnd, l.name(VARCHAR_KEY), r.name(VARCHAR_KEY));
        } else {
            // vk holds sym's texts, so the pair matches rows; it reconciles to VARCHAR and
            // both sides stage their text rather than translating symbol keys.
            appendKeyEquality(sql, rnd, l.name(SYMBOL_KEY), r.name(VARCHAR_KEY));
        }
        if (keyPick >= 38) {
            sql.put(" AND ");
            appendKeyEquality(sql, rnd, l.name(SYMBOL_KEY), r.name(SYMBOL_KEY));
        }
        if (rnd.nextInt(4) == 0) {
            // An ON predicate over the build keeps the fused plan; one over the preserved side
            // of an outer join does not, so an outer join puts it on the build three times in
            // four. An INNER join moves it into the WHERE clause either way.
            final boolean isOnBuild = rnd.nextInt(4) != 0;
            final boolean isLeft = isRightJoin ? isOnBuild : isLeftJoin ? !isOnBuild : rnd.nextBoolean();
            sql.put(" AND (").put(predicate(rnd, isLeft ? l : r, isLeft ? LEFT_ALIAS : RIGHT_ALIAS, ctx)).put(')');
        }

        final ObjList<String> conjuncts = new ObjList<>();
        if (rnd.nextBoolean()) {
            conjuncts.add(predicate(rnd, l, LEFT_ALIAS, ctx));
        }
        if (rnd.nextBoolean()) {
            conjuncts.add(predicate(rnd, r, RIGHT_ALIAS, ctx));
        }
        if (rnd.nextInt(8) == 0) {
            conjuncts.add(CONSTANT_PREDICATES[rnd.nextInt(CONSTANT_PREDICATES.length)]);
        }
        if (rnd.nextInt(40) == 0) {
            // A residual over both inputs keeps the ordinary plan. The operator is never an
            // equality, which an INNER join would turn into a second join key.
            conjuncts.add(LEFT_ALIAS + ".ts " + CROSS_INPUT_OPS[rnd.nextInt(CROSS_INPUT_OPS.length)] + ' ' + RIGHT_ALIAS + ".ts");
        }
        if (injectFaultFn || conjuncts.size() > 0) {
            sql.put(" WHERE ");
            if (injectFaultFn) {
                sql.put(TestFaultFunctionFactory.CALL);
            }
            for (int i = 0, n = conjuncts.size(); i < n; i++) {
                if (i > 0 || injectFaultFn) {
                    sql.put(" AND ");
                }
                sql.put('(').put(conjuncts.getQuick(i)).put(')');
            }
        }

        // SAMPLE BY buckets on the master's designated timestamp, l.ts. The optimiser learns a
        // sub-query's designated timestamp only at code generation, so over a sub-query master
        // SAMPLE BY skips the GROUP BY rewrite, and the path without it fails with "base query
        // does not provide designated TIMESTAMP column" unless the query selects l.ts. Only a
        // table master gets a SAMPLE BY.
        if (l.isTable && rnd.nextInt(7) == 0) {
            // The optimiser rewrites SAMPLE BY aligned to calendar into a GROUP BY, which fuses
            // unless it fills. A RIGHT JOIN null-extends l.ts; the NULL-timestamp group comes
            // out ahead of the filled grid.
            sql.put(" SAMPLE BY ").put(SAMPLE_BY_INTERVALS[rnd.nextInt(SAMPLE_BY_INTERVALS.length)]);
            if (rnd.nextInt(3) == 0) {
                appendFill(sql, rnd, aggCount);
            }
            // ALIGN TO FIRST OBSERVATION skips the rewrite and needs l.ts in order, which a
            // RIGHT JOIN does not keep ("TIMESTAMP column is required but not provided").
            final int align = rnd.nextInt(3);
            if (align == 1) {
                sql.put(" ALIGN TO CALENDAR");
            } else if (align == 2 && !isRightJoin) {
                sql.put(" ALIGN TO FIRST OBSERVATION");
            }
        } else if (keyCount > 0 && rnd.nextBoolean()) {
            sql.put(" GROUP BY ");
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                // The alias or the 1-based position: the keys lead the projection.
                if (rnd.nextBoolean()) {
                    sql.put('e').put(i);
                } else {
                    sql.put(i + 1);
                }
            }
        }

        if (rnd.nextBoolean()) {
            JoinClauseSupport.appendOrderBy(sql, rnd, keyCount, aggCount);
        }
        // LIMIT without an ORDER BY over every key picks a different valid subset of groups
        // in each plan, so the runner compares row counts only for these queries.
        final boolean hasLimit = rnd.nextInt(5) == 0;
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(20));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    /**
     * Emits one aggregate over {@code input}. Most draws land in the fused allowlist
     * ({@code HashJoinGroupByAggregates}); sum and avg over BYTE and min and max over BOOLEAN or
     * BYTE pass their argument to a class registered for another type, so the query keeps the
     * ordinary plan. Every table carries the INT key {@code k} and the SYMBOL key {@code sym}, so
     * the integer and count arguments always find a column.
     */
    private static void appendAggregate(StringSink sql, Rnd rnd, Input input, String alias) {
        final int pick = rnd.nextInt(40);
        if (pick < 5) {
            sql.put("count(*)");
        } else if (pick < 13) {
            appendColumnAggregate(sql, rnd, "count(", input, alias, COUNT_ARGUMENT_DDLS, ")");
        } else if (pick < 17) {
            appendColumnAggregate(sql, rnd, rnd.nextBoolean() ? "sum(" : "avg(", input, alias, INTEGER_DDLS, "::DOUBLE)");
        } else if (pick < 21) {
            // FLOAT and DOUBLE partial sums round differently with the merge order.
            appendColumnAggregate(sql, rnd, rnd.nextBoolean() ? "sum(" : "avg(", input, alias, FLOATING_DDLS, "::DOUBLE)");
        } else if (pick < 26) {
            appendColumnAggregate(sql, rnd, rnd.nextBoolean() ? "sum(" : "avg(", input, alias, INTEGER_DDLS, ")");
        } else if (pick < 31) {
            appendColumnAggregate(sql, rnd, rnd.nextBoolean() ? "min(" : "max(", input, alias, MIN_MAX_DDLS, ")");
        } else if (pick < 34) {
            final int fn = rnd.nextInt(5);
            if (fn < 3) {
                appendColumnAggregate(sql, rnd, fn == 0 ? "bit_and(" : fn == 1 ? "bit_or(" : "bit_xor(", input, alias, INTEGER_DDLS, ")");
            } else {
                appendColumnAggregate(sql, rnd, fn == 3 ? "bool_and(" : "bool_or(", input, alias, BOOLEAN_DDLS, ")");
            }
        } else if (pick < 36) {
            appendColumnAggregate(sql, rnd, rnd.nextBoolean() ? "ksum(" : "nsum(", input, alias, FLOATING_DDLS, "::DOUBLE)");
        } else if (pick < 39) {
            appendColumnAggregate(sql, rnd, VARIANCE_FUNCTIONS[rnd.nextInt(VARIANCE_FUNCTIONS.length)] + "(", input, alias, INTEGER_DDLS, "::DOUBLE)");
        } else {
            // Two-argument aggregates take any numeric argument types. Integer sums stay exact.
            final FuzzColumn value = pickColumn(rnd, input.columns, INTEGER_DDLS);
            final FuzzColumn weight = pickColumn(rnd, input.columns, INTEGER_DDLS);
            if (value == null || weight == null) {
                sql.put("count(*)");
                return;
            }
            // A negative weight makes a weighted stddev group NULL, so an abs() weight keeps groups with
            // values. One draw picks both the function and the abs().
            final int fn = rnd.nextInt(WEIGHTED_FUNCTIONS.length + 3);
            final boolean isAbsWeight = fn >= WEIGHTED_FUNCTIONS.length;
            sql.put(WEIGHTED_FUNCTIONS[isAbsWeight ? fn - 3 : fn]).put('(')
                    .put(alias).put('.').put(FuzzNames.column(rnd, value.getName())).put(", ")
                    .put(isAbsWeight ? "abs(" : "")
                    .put(alias).put('.').put(FuzzNames.column(rnd, weight.getName()))
                    .put(isAbsWeight ? "))" : ")");
        }
    }

    private static void appendColumnAggregate(StringSink sql, Rnd rnd, String prefix, Input input, String alias, String[] ddls, String suffix) {
        final FuzzColumn column = pickColumn(rnd, input.columns, ddls);
        if (column == null) {
            sql.put("count(*)");
            return;
        }
        sql.put(prefix).put(alias).put('.').put(FuzzNames.column(rnd, column.getName())).put(suffix);
    }

    private static void appendFill(StringSink sql, Rnd rnd, int aggCount) {
        final String fill = FILLS[rnd.nextInt(FILLS.length)];
        if (fill != null) {
            sql.put(' ').put(fill);
            return;
        }
        sql.put(" FILL(");
        for (int i = 0; i < aggCount; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            sql.put('0');
        }
        sql.put(')');
    }

    /**
     * Emits a grouping key over {@code input}: a column reference on three draws in four, a
     * generated expression otherwise. The expression's kind is one the fused plan reads unless
     * the query may reference any column; a key of another type keeps the ordinary plan.
     */
    private static void appendGroupingKey(StringSink sql, Rnd rnd, Input input, String alias, BindContext ctx) {
        if (rnd.nextInt(4) != 0) {
            final FuzzColumn column = pickGroupableColumn(rnd, input.columns);
            if (column != null) {
                new ColumnRefExpr(rnd, column, alias).appendSql(sql, ctx);
                return;
            }
        }
        final ExpressionGenerator gen = new ExpressionGenerator(rnd, input.columns, alias, 1);
        final ColumnKind kind = input.isFusableOnly
                ? FUSABLE_KEY_KINDS[rnd.nextInt(FUSABLE_KEY_KINDS.length)]
                : GroupByClause.pickGroupableKind(rnd, gen);
        gen.generateOfKind(kind).appendSql(sql, ctx);
    }

    // Both operand orders: the join context records the key per model either way.
    private static void appendKeyEquality(StringSink sql, Rnd rnd, String leftColumn, String rightColumn) {
        if (rnd.nextBoolean()) {
            sql.put(LEFT_ALIAS).put('.').put(leftColumn).put(" = ").put(RIGHT_ALIAS).put('.').put(rightColumn);
        } else {
            sql.put(RIGHT_ALIAS).put('.').put(rightColumn).put(" = ").put(LEFT_ALIAS).put('.').put(leftColumn);
        }
    }

    private static boolean contains(String[] values, String value) {
        for (String v : values) {
            if (v.equals(value)) {
                return true;
            }
        }
        return false;
    }

    private static ObjList<FuzzColumn> fusableColumns(ObjList<FuzzColumn> columns) {
        final ObjList<FuzzColumn> out = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn c = columns.getQuick(i);
            if (contains(FUSABLE_DDLS, c.getType().getDdl())) {
                out.add(c);
            }
        }
        return out;
    }

    private static FuzzColumn pickColumn(Rnd rnd, ObjList<FuzzColumn> columns, String[] ddls) {
        final ObjList<FuzzColumn> matching = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn c = columns.getQuick(i);
            if (contains(ddls, c.getType().getDdl())) {
                matching.add(c);
            }
        }
        return matching.size() > 0 ? matching.getQuick(rnd.nextInt(matching.size())) : null;
    }

    private static FuzzColumn pickGroupableColumn(Rnd rnd, ObjList<FuzzColumn> columns) {
        final ObjList<FuzzColumn> matching = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn c = columns.getQuick(i);
            if (c.getType().getKind().isGroupable()) {
                matching.add(c);
            }
        }
        return matching.size() > 0 ? matching.getQuick(rnd.nextInt(matching.size())) : null;
    }

    /**
     * Draws one input: the table itself on 32 draws in 40, a projection over it on six, a
     * {@code LATEST ON} sub-query on one and a {@code LIMIT} sub-query on one. The projection
     * keeps every column and the designated timestamp's name, renames each other column with
     * probability 1/4 and filters on half the draws, so the fused planner has to resolve the outer
     * names through it. The {@code LATEST ON} sub-query keeps the latest row per join key. Written
     * on the table, it is a feature the fused planner refuses; written around a projection of the
     * table, child compilation applies it to the input, and the input may fuse as a build. The
     * {@code LIMIT} sub-query takes the first rows in timestamp order, which is the same set in
     * every storage layout, and is a barrier that keeps the ordinary plan.
     */
    private static Input pickInput(Rnd rnd, FuzzTable table, boolean isFusableOnly, BindContext ctx) {
        final String tableName = FuzzNames.table(rnd, table.getName());
        final int pick = rnd.nextInt(40);
        if (pick < 32) {
            return new Input(tableName, table.getColumns(), isFusableOnly, true);
        }
        if (pick == 39) {
            return new Input("(SELECT * FROM " + tableName + " LIMIT " + (10 + rnd.nextInt(50)) + ')', table.getColumns(), isFusableOnly, false);
        }
        if (pick == 38) {
            final String latestOn = " LATEST ON " + table.getTsColumnName() + " PARTITION BY " + (rnd.nextBoolean() ? INT_KEY : SYMBOL_KEY);
            final String sql = rnd.nextBoolean()
                    ? "(SELECT * FROM " + tableName + latestOn + ')'
                    : "((SELECT * FROM " + tableName + ')' + latestOn + ')';
            return new Input(sql, table.getColumns(), isFusableOnly, false);
        }
        final ObjList<FuzzColumn> exposed = new ObjList<>();
        final StringSink sql = new StringSink();
        sql.put("(SELECT ");
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            final FuzzColumn c = table.getColumn(i);
            if (i > 0) {
                sql.put(", ");
            }
            sql.put(c.getName());
            if (!c.getName().equals(table.getTsColumnName()) && rnd.nextInt(4) == 0) {
                final String alias = c.getName() + "_v";
                sql.put(" AS ").put(alias);
                exposed.add(new FuzzColumn(alias, c.getType()));
            } else {
                exposed.add(c);
            }
        }
        sql.put(" FROM ").put(tableName);
        if (rnd.nextBoolean()) {
            final ObjList<FuzzColumn> columns = isFusableOnly ? fusableColumns(table.getColumns()) : table.getColumns();
            sql.put(" WHERE ").put(new PredicateGenerator(rnd, 1).generate(columns, null, ctx));
        }
        sql.put(')');
        return new Input(sql.toString(), exposed, isFusableOnly, false);
    }

    private static String predicate(Rnd rnd, Input input, String alias, BindContext ctx) {
        return new PredicateGenerator(rnd, 1).generate(input.columns, alias, ctx);
    }

    /**
     * One join input: the SQL after {@code FROM} (or after the join keyword), the columns the
     * query may reference under the names the input exposes, and every column it exposes, for
     * the key lookups.
     */
    private static final class Input {
        final ObjList<FuzzColumn> allColumns;
        final ObjList<FuzzColumn> columns;
        final String fromSql;
        final boolean isFusableOnly;
        final boolean isTable;

        Input(String fromSql, ObjList<FuzzColumn> allColumns, boolean isFusableOnly, boolean isTable) {
            this.fromSql = fromSql;
            this.allColumns = allColumns;
            this.isFusableOnly = isFusableOnly;
            this.isTable = isTable;
            this.columns = isFusableOnly ? fusableColumns(allColumns) : allColumns;
        }

        // The exposed name of a base column: a projection input may have renamed it.
        String name(String baseName) {
            for (int i = 0, n = allColumns.size(); i < n; i++) {
                final String name = allColumns.getQuick(i).getName();
                if (name.equals(baseName) || name.equals(baseName + "_v")) {
                    return name;
                }
            }
            throw new AssertionError("every fuzz table carries the column " + baseName);
        }
    }
}
