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
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * Row-producing equi-joins across two WAL tables: the INNER and LEFT OUTER hash joins that the
 * parallel hash join ({@code Async Hash Join}) runs in place of {@code Hash Join Light} and
 * {@code Hash Left Outer Join Light}. QueryRunner runs every query of this shape with the operator
 * on and off and compares the two results in order, since the operator returns the ordinary plan's
 * rows in the ordinary plan's order, so the generator aims most of its queries at what the
 * operator accepts and sends the rest through the features that keep the ordinary plan.
 * <p>
 * Shape:
 * <pre>
 * SELECT (* | side.column AS c0[, ...])
 * FROM input l (JOIN | LEFT JOIN) input r ON l.key = r.key [AND l.key2 = r.key2] [AND (residual)]
 * [WHERE conjunct [AND ...]]
 * [ORDER BY ...]
 * [LIMIT N | LIMIT -N]
 * </pre>
 * The join key is one of the four shared key columns, two of them together, or the INT key
 * against the LONG one or the designated timestamps against each other: the INT column
 * {@code k} and the SYMBOL column {@code sym}, which the operator reads straight off the frames,
 * the SYMBOL key translated into the build's dictionary, and the others, which it stages into a
 * map. The key domains are small, so most keys repeat in the build, and their chains reach the
 * consumer or, under a residual or a post-join filter, the workers.
 * <p>
 * An input is the table itself, a projection over it that renames some columns and may filter
 * (a probe or build filter that the operator steals), the table scanned backward under
 * {@code ORDER BY ts DESC} (a backward probe on the left, a sort that keeps the ordinary plan on
 * the right), or a {@code LIMIT} sub-query (a barrier that keeps the ordinary plan). A LEFT join's
 * ON condition may carry a residual over the left input or over both inputs, which the operator
 * runs per pair, or over the right one, which the optimiser moves into the build's filter. The
 * WHERE clause may filter either input, compare the two inputs (a post-join filter), keep the
 * LEFT join's misses alone ({@code r.k IS NULL}, an anti-join), narrow a designated timestamp to an
 * interval, or hold a constant. The inputs never pass through an operator whose row order varies
 * between executions, so the two plans' results compare row for row; a {@code LIMIT} without a
 * total order still marks the query non-deterministic for the multiset axes, whose storage
 * shadow can scan an indexed input in another order.
 * <p>
 * A {@code LIMIT} without an {@code ORDER BY} never comes with a filter on the left input: such a
 * query orders by the left input's designated timestamp before its {@code LIMIT}. The optimiser
 * pushes a bare {@code LIMIT} into the left input's filter, below the join, so a join that drops
 * left rows returns fewer rows than the {@code LIMIT} asks for (questdb/questdb#7982, also on the
 * PR base). Every plan of such a query hits the bug alike, the parallel hash join's among them,
 * since the pushed limit keeps the ordinary plan, but the fuzz axes see a difference wherever one
 * form of the query keeps the filter and the other does not: a literal filter that folds to a
 * constant against its bind form, or an indexed shadow table that reads the filter through its
 * index.
 */
public final class EquiJoinClause {
    public static final String LEFT_ALIAS = "l";
    public static final String RIGHT_ALIAS = "r";
    // Operators that never turn a comparison of the two inputs into a join key.
    private static final String[] CROSS_INPUT_OPS = {"<", "<=", ">", ">=", "!="};
    // The optimiser moves a constant conjunct into the join model's constant WHERE clause; only a
    // false one shows an operator that ignores it, so three in four are false.
    private static final String[] CONSTANT_PREDICATES = {"1 = 0", "false", "2 < 1", "1 = 1"};
    private static final String INT_KEY = "k";
    // Interval widths in hours. The fuzz tables start at FuzzConfig's 2024-01-01 and span 30 to
    // 75 hours, so an interval selects anything from no row to every row of a table.
    private static final int[] INTERVAL_HOURS = {1, 3, 12, 48};
    private static final String LONG_KEY = "lk";
    private static final String SYMBOL_KEY = "sym";
    private static final String TS = "ts";
    private static final String VARCHAR_KEY = "vk";

    private EquiJoinClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzTable left, FuzzTable right, BindContext ctx, boolean injectFaultFn) {
        // The draws below depend neither on ctx nor on injectFaultFn, so the bind variant
        // regenerates the same tree.
        final boolean isLeftJoin = rnd.nextBoolean();
        final Input l = pickInput(rnd, left, ctx);
        final Input r = pickInput(rnd, right, ctx);
        // True when the left input may run a filter, which a bare LIMIT would be pushed into.
        boolean isLeftFiltered = l.isFiltered;

        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        final boolean isStar = rnd.nextInt(6) == 0;
        int projected = 0;
        if (isStar) {
            sql.put('*');
        } else {
            projected = 1 + rnd.nextInt(5);
            for (int i = 0; i < projected; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                final boolean isLeft = rnd.nextBoolean();
                final Input input = isLeft ? l : r;
                final FuzzColumn column = input.columns.getQuick(rnd.nextInt(input.columns.size()));
                sql.put(isLeft ? LEFT_ALIAS : RIGHT_ALIAS).put('.').put(column.getName()).put(" AS c").put(i);
            }
        }

        sql.put(" FROM ").put(l.fromSql).put(' ').put(LEFT_ALIAS);
        sql.put(isLeftJoin ? " LEFT JOIN " : " JOIN ").put(r.fromSql).put(' ').put(RIGHT_ALIAS);
        sql.put(" ON ");
        // 0-11: INT key and 12-23: SYMBOL key, which the operator reads off the frames.
        // 24-29: LONG key, 30-35: VARCHAR key, 36-37: SYMBOL against VARCHAR, and the composite
        // keys 38-45: INT and SYMBOL, 46-50: LONG and VARCHAR, 51-55: INT and LONG, which stage
        // their key into a map, as do 56-57: INT against LONG and 58-59: the designated timestamps.
        final int keyPick = rnd.nextInt(60);
        if (keyPick >= 46 && keyPick < 51) {
            appendKeyEquality(sql, rnd, l.name(LONG_KEY), r.name(LONG_KEY));
            sql.put(" AND ");
            appendKeyEquality(sql, rnd, l.name(VARCHAR_KEY), r.name(VARCHAR_KEY));
        } else if (keyPick >= 51 && keyPick < 56) {
            appendKeyEquality(sql, rnd, l.name(INT_KEY), r.name(INT_KEY));
            sql.put(" AND ");
            appendKeyEquality(sql, rnd, l.name(LONG_KEY), r.name(LONG_KEY));
        } else if (keyPick >= 56 && keyPick < 58) {
            appendKeyEquality(sql, rnd, l.name(INT_KEY), r.name(LONG_KEY));
        } else if (keyPick >= 58) {
            appendKeyEquality(sql, rnd, TS, TS);
        } else if (keyPick < 12 || keyPick >= 38) {
            appendKeyEquality(sql, rnd, l.name(INT_KEY), r.name(INT_KEY));
        } else if (keyPick < 24) {
            appendKeyEquality(sql, rnd, l.name(SYMBOL_KEY), r.name(SYMBOL_KEY));
        } else if (keyPick < 30) {
            appendKeyEquality(sql, rnd, l.name(LONG_KEY), r.name(LONG_KEY));
        } else if (keyPick < 36) {
            appendKeyEquality(sql, rnd, l.name(VARCHAR_KEY), r.name(VARCHAR_KEY));
        } else {
            // vk holds sym's texts, so the pair matches rows; both sides stage their text.
            appendKeyEquality(sql, rnd, l.name(SYMBOL_KEY), r.name(VARCHAR_KEY));
        }
        if (keyPick >= 38 && keyPick < 46) {
            sql.put(" AND ");
            appendKeyEquality(sql, rnd, l.name(SYMBOL_KEY), r.name(SYMBOL_KEY));
        }
        if (rnd.nextInt(3) == 0) {
            // A residual over the left input or over both inputs runs per pair on a LEFT join; an
            // INNER join moves the condition into its WHERE clause. One over the right input
            // filters the build either way.
            sql.put(" AND (");
            switch (rnd.nextInt(3)) {
                case 0 -> {
                    sql.put(predicate(rnd, l, LEFT_ALIAS, ctx));
                    // The INNER join's WHERE clause hands the condition to the left input's filter.
                    isLeftFiltered |= !isLeftJoin;
                }
                case 1 -> sql.put(predicate(rnd, r, RIGHT_ALIAS, ctx));
                default -> sql.put(crossPredicate(rnd, l, r));
            }
            sql.put(')');
        }

        final ObjList<String> conjuncts = new ObjList<>();
        if (rnd.nextBoolean()) {
            // The probe filter, or, on a swapped INNER execution, the build's.
            conjuncts.add(predicate(rnd, l, LEFT_ALIAS, ctx));
            isLeftFiltered = true;
        }
        if (rnd.nextInt(3) == 0) {
            // The build's filter on an INNER join; a post-join filter on a LEFT one.
            conjuncts.add(predicate(rnd, r, RIGHT_ALIAS, ctx));
        }
        if (rnd.nextInt(4) == 0) {
            conjuncts.add(crossPredicate(rnd, l, r));
        }
        if (isLeftJoin && rnd.nextInt(8) == 0) {
            // The LEFT join's misses alone.
            conjuncts.add(RIGHT_ALIAS + '.' + r.name(INT_KEY) + " IS NULL");
        }
        if (l.isTable && rnd.nextInt(4) == 0) {
            conjuncts.add(interval(rnd, LEFT_ALIAS));
        }
        if (r.isTable && rnd.nextInt(6) == 0) {
            conjuncts.add(interval(rnd, RIGHT_ALIAS));
        }
        if (rnd.nextInt(16) == 0) {
            conjuncts.add(CONSTANT_PREDICATES[rnd.nextInt(CONSTANT_PREDICATES.length)]);
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

        boolean hasOrderBy = true;
        switch (rnd.nextInt(6)) {
            // The left input's designated timestamp descending: a backward probe scan.
            case 0 -> sql.put(" ORDER BY ").put(LEFT_ALIAS).put('.').put(TS).put(" DESC");
            // Ascending, which the left input already scans in, and which fixes the join's order.
            case 1 -> sql.put(" ORDER BY ").put(LEFT_ALIAS).put('.').put(TS);
            case 2 -> {
                if (!isStar) {
                    sql.put(" ORDER BY c").put(rnd.nextInt(projected)).put(rnd.nextBoolean() ? " DESC" : "");
                } else {
                    hasOrderBy = false;
                }
            }
            default -> hasOrderBy = false;
        }
        final boolean hasLimit = rnd.nextInt(6) == 0;
        if (hasLimit) {
            if (!hasOrderBy && isLeftFiltered) {
                // A bare LIMIT would be pushed into the left input's filter (questdb/questdb#7982);
                // the order adds no draw, so the bind form keeps the literal form's tree.
                sql.put(" ORDER BY ").put(LEFT_ALIAS).put('.').put(TS);
            }
            sql.put(" LIMIT ").put(rnd.nextBoolean() ? "" : "-").put(1 + rnd.nextInt(30));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    // Both operand orders: the join context records the key per model either way.
    private static void appendKeyEquality(StringSink sql, Rnd rnd, String leftColumn, String rightColumn) {
        if (rnd.nextBoolean()) {
            sql.put(LEFT_ALIAS).put('.').put(leftColumn).put(" = ").put(RIGHT_ALIAS).put('.').put(rightColumn);
        } else {
            sql.put(RIGHT_ALIAS).put('.').put(rightColumn).put(" = ").put(LEFT_ALIAS).put('.').put(leftColumn);
        }
    }

    /**
     * A comparison of the two inputs that is never an equality, so that it stays a filter over
     * the joined pairs: the designated timestamps, the INT key against the LONG one, or two
     * numeric columns, one of each input.
     */
    private static String crossPredicate(Rnd rnd, Input l, Input r) {
        final String op = CROSS_INPUT_OPS[rnd.nextInt(CROSS_INPUT_OPS.length)];
        final int pick = rnd.nextInt(3);
        if (pick == 0) {
            return LEFT_ALIAS + '.' + TS + ' ' + op + ' ' + RIGHT_ALIAS + '.' + TS;
        }
        if (pick == 1) {
            return LEFT_ALIAS + '.' + l.name(INT_KEY) + ' ' + op + ' ' + RIGHT_ALIAS + '.' + r.name(LONG_KEY);
        }
        final FuzzColumn leftColumn = pickNumeric(rnd, l.columns);
        final FuzzColumn rightColumn = pickNumeric(rnd, r.columns);
        final String leftName = leftColumn != null ? leftColumn.getName() : l.name(LONG_KEY);
        final String rightName = rightColumn != null ? rightColumn.getName() : r.name(LONG_KEY);
        return LEFT_ALIAS + '.' + leftName + ' ' + op + ' ' + RIGHT_ALIAS + '.' + rightName;
    }

    // The literal of the given hour after 2024-01-01T00:00, within January.
    private static String hourOf2024(int hours) {
        final int day = 1 + hours / 24;
        final int hour = hours % 24;
        return "'2024-01-" + (day < 10 ? "0" : "") + day + 'T' + (hour < 10 ? "0" : "") + hour + ":00:00.000000Z'";
    }

    private static String interval(Rnd rnd, String alias) {
        final int lo = rnd.nextInt(60);
        final int hi = lo + INTERVAL_HOURS[rnd.nextInt(INTERVAL_HOURS.length)];
        return alias + '.' + TS + " >= " + hourOf2024(lo) + " AND " + alias + '.' + TS + " < " + hourOf2024(hi);
    }

    /**
     * Draws one input: the table itself on 30 draws in 40, a projection over it on six, the table
     * scanned backward on two and a {@code LIMIT} sub-query on two. The projection keeps every
     * column and the designated timestamp's name, renames each other column with probability
     * 1/4 and filters on half the draws, so the planner resolves the outer names through it. The
     * backward scan sorts the rows by the designated timestamp, which the planner turns into a
     * backward scan of the table, and the {@code LIMIT} sub-query takes the first rows in
     * timestamp order, the same set in every storage layout.
     */
    private static Input pickInput(Rnd rnd, FuzzTable table, BindContext ctx) {
        final String tableName = FuzzNames.table(rnd, table.getName());
        final int pick = rnd.nextInt(40);
        if (pick < 30) {
            return new Input(tableName, table.getColumns(), true, false);
        }
        if (pick >= 38) {
            return new Input("(SELECT * FROM " + tableName + " LIMIT " + (10 + rnd.nextInt(50)) + ')', table.getColumns(), false, false);
        }
        if (pick >= 36) {
            return new Input("(SELECT * FROM " + tableName + " ORDER BY " + TS + " DESC)", table.getColumns(), false, false);
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
        final boolean isFiltered = rnd.nextBoolean();
        if (isFiltered) {
            sql.put(" WHERE ").put(new PredicateGenerator(rnd, 1).generate(table.getColumns(), null, ctx));
        }
        sql.put(')');
        return new Input(sql.toString(), exposed, false, isFiltered);
    }

    private static FuzzColumn pickNumeric(Rnd rnd, ObjList<FuzzColumn> columns) {
        final ObjList<FuzzColumn> matching = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn c = columns.getQuick(i);
            if (c.getType().getKind() == ColumnKind.NUMERIC) {
                matching.add(c);
            }
        }
        return matching.size() > 0 ? matching.getQuick(rnd.nextInt(matching.size())) : null;
    }

    private static String predicate(Rnd rnd, Input input, String alias, BindContext ctx) {
        return new PredicateGenerator(rnd, 1).generate(input.columns, alias, ctx);
    }

    /**
     * One join input: the SQL after {@code FROM} (or after the join keyword), and the columns it
     * exposes, under the names it exposes them by.
     */
    private static final class Input {
        final ObjList<FuzzColumn> columns;
        final String fromSql;
        // A projection with a WHERE clause, which the input's filter runs.
        final boolean isFiltered;
        final boolean isTable;

        Input(String fromSql, ObjList<FuzzColumn> columns, boolean isTable, boolean isFiltered) {
            this.fromSql = fromSql;
            this.columns = columns;
            this.isTable = isTable;
            this.isFiltered = isFiltered;
        }

        // The exposed name of a base column: a projection input may have renamed it.
        String name(String baseName) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final String name = columns.getQuick(i).getName();
                if (name.equals(baseName) || name.equals(baseName + "_v")) {
                    return name;
                }
            }
            throw new AssertionError("every fuzz table carries the column " + baseName);
        }
    }
}
