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
import org.jetbrains.annotations.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Row-producing equi-joins across WAL tables: the INNER and LEFT OUTER hash joins that the
 * parallel hash join ({@code Async Hash Join}) runs in place of {@code Hash Join Light} and
 * {@code Hash Left Outer Join Light}, alone or stacked into a star. QueryRunner runs every query of
 * this shape with the operator on and off and compares the two results in order, since the
 * operator returns the ordinary plan's rows in the ordinary plan's order, so the generator aims
 * most of its queries at what the operator accepts and sends the rest through the features that
 * keep the ordinary plan.
 * <p>
 * Shape:
 * <pre>
 * SELECT (* | input.column AS cN, ... [, window AS cN, ...] [, l.k + N AS cN])
 * FROM input l
 *   (JOIN | LEFT JOIN) input r ON key [AND (residual)]
 *   [(JOIN | LEFT JOIN) input r2 ON key [AND (residual)] ...]
 * [WHERE conjunct [AND ...]]
 * [ORDER BY ...]
 * [LIMIT [lo, ]N | LIMIT [lo, ]-N]
 * </pre>
 * The first join is the one {@link #generate(Rnd, FuzzTable, FuzzTable, BindContext, boolean)}
 * draws alone. {@link #generate(Rnd, FuzzTable, FuzzTable, ObjList, BindContext, boolean)}, which
 * QueryFuzzTest runs, follows it with up to {@link #MAX_JOINS} - 1 later joins over any of the
 * run's tables, aliased {@code r2}, {@code r3}, ..., a star whose joins the operator stacks into
 * stages of one operator, and adds window functions over the joined rows.
 * <p>
 * A join's key is one of the four shared key columns, two of them together, or the INT key
 * against the LONG one or the designated timestamps against each other: the INT column
 * {@code k} and the SYMBOL column {@code sym}, which the operator reads straight off the frames,
 * the SYMBOL key translated into the build's dictionary, and the others, which it stages into a
 * map. A later join's key reads the left input, the fact table of the star, or, now and then, an
 * earlier join's input: a snowflake key, which leaves that join to an ordinary hash join over the
 * operator. The key domains are small, so most keys repeat in every build, and their chains reach
 * the consumer or, under a residual or a post-join filter, the workers, each stage's matches
 * nested in the previous stage's. The joins' duplicates multiply, so a later join that would take
 * the estimated output past {@link #FAN_OUT_BUDGET} rows per left row takes a selective key (a
 * composite or the timestamps) or a dimension narrowed to the first hours of its table, whose few
 * rows still repeat keys.
 * <p>
 * An input is the table itself, a projection over it that renames some columns and may filter
 * (a probe or build filter that the operator steals), the table scanned backward under
 * {@code ORDER BY ts DESC} (a backward probe on the left, a sort that keeps the ordinary plan on
 * the right), or a {@code LIMIT} sub-query (a barrier that keeps the ordinary plan). A third of a
 * star's INNER first joins read their two tables unfiltered, which lets the join swap its inputs
 * unless a parent of the join asks for its order, and the first join of a star joins the left table
 * with itself now and then, so its two inputs have the same size; the fuzz tables' sizes otherwise
 * differ by a few rows, so such a join swaps its inputs in some queries and not in others.
 * <p>
 * A LEFT join's ON condition may carry a residual over the left input, over its own input, over
 * both or over its own input and an earlier join's, which the operator runs per pair; one over its
 * own input alone filters the build instead. An INNER join moves its residual into the WHERE
 * clause. The WHERE
 * clause may filter any input, compare two inputs (a post-join filter on the later of their
 * joins), keep a LEFT join's misses alone ({@code r.k IS NULL}, an anti-join), narrow a designated
 * timestamp to an interval, or hold a constant. The inputs never pass through an operator whose
 * row order varies between executions, so the two plans' results compare row for row.
 * <p>
 * A window function numbers, ranks, counts, sums or reads neighbouring values of the joined rows,
 * partitioned or not, in the order of no column, the left input's timestamp either way, or a later
 * input's timestamp. Asking the joined rows for their order is what decides whether the first join
 * may still swap its inputs, and a post-join filter answers that question in its own way (see
 * {@code AsyncHashJoinRecordCursorFactory.followedOrderByAdvice()}), so window functions and orders
 * come over post-filtered stars as well. Rows that tie on a window's order key get their
 * values in the order the join returns them, which the parallel join axis checks but a storage
 * shadow that reads an index may change, so a window function marks the query non-deterministic for
 * the multiset axes, as a {@code LIMIT} without a total order does.
 * <p>
 * A third of the {@code LIMIT}s are ranges, whose literal lower bound comes before the value: a
 * positive range reads at most its larger bound's rows, which the parallel hash join narrows its
 * first window to, and one whose upper bound is negative counts rows from the end. A query with a
 * {@code LIMIT} and a projection list projects the left input's INT key plus the {@code LIMIT}'s
 * value last. The bind form reads both through the positional {@code $1}, see
 * {@link BindContext#bindLimit(String)}, which the runner leaves undefined at compile: the SELECT
 * list types it INT before the {@code LIMIT} reads it, whichever plan runs, as a PostgreSQL client
 * that sends untyped parameters relies on.
 * <p>
 * A {@code LIMIT} without an {@code ORDER BY} never comes with a filter on the left input: such a
 * query orders by the left input's designated timestamp before its {@code LIMIT}. The optimiser
 * pushes a bare {@code LIMIT} into the left input's filter, below the joins, so a join that drops
 * left rows returns fewer rows than the {@code LIMIT} asks for (questdb/questdb#7982, also on the
 * PR base). Every plan of such a query hits the bug alike, the parallel hash join's among them,
 * since the pushed limit keeps the ordinary plan, but the fuzz axes see a difference wherever one
 * form of the query keeps the filter and the other does not: a literal filter that folds to a
 * constant against its bind form, or an indexed shadow table that reads the filter through its
 * index.
 * <p>
 * Half of the queries with a {@code LIMIT}, but those that inject a fault, state it so that it reaches
 * the joins from outside their own query, or through an operator over them, see
 * {@link #applyLimitForm}: the {@code LIMIT} of a sub-query or a CTE over the query, of a UNION ALL of
 * the query with itself, or of the query under an ASOF or LT join of which the joins are the master,
 * or under a WHERE that holds no column. The forms draw from a generator of their own, seeded from
 * the query's, so that every query keeps the draws it made before they existed. For the tests that
 * read the generated queries, {@link #limitForm(String)} names the form of a query and
 * {@link #unwrapLimitForm(String)} takes the query back out of it.
 */
public final class EquiJoinClause {
    public static final String LEFT_ALIAS = "l";
    // The LIMIT forms that applyLimitForm() draws, see limitForm().
    public static final int LIMIT_FORM_AS_OF = 3;
    public static final int LIMIT_FORM_CTE = 1;
    public static final int LIMIT_FORM_LT = 4;
    // A query that states its LIMIT, if it has one, as it is.
    public static final int LIMIT_FORM_NONE = -1;
    public static final int LIMIT_FORM_RUNTIME_CONSTANT_FILTER = 5;
    public static final int LIMIT_FORM_SUB_QUERY = 0;
    public static final int LIMIT_FORM_UNION_ALL = 2;
    // The most joins of one query: the first join and up to five later ones.
    public static final int MAX_JOINS = 6;
    public static final String RIGHT_ALIAS = "r";
    // The head of the ASOF join of a LIMIT form, see applyLimitForm().
    private static final String AS_OF_LIMIT_FORM_HEAD = " ASOF JOIN ";
    // Operators that never turn a comparison of two inputs into a join key.
    private static final String[] CROSS_INPUT_OPS = {"<", "<=", ">", ">=", "!="};
    // The optimiser moves a constant conjunct into the join model's constant WHERE clause; only a
    // false one shows an operator that ignores it, so three in four are false.
    private static final String[] CONSTANT_PREDICATES = {"1 = 0", "false", "2 < 1", "1 = 1"};
    // The texts of the CTE form before and after the query, see applyLimitForm().
    private static final String CTE_LIMIT_FORM_HEAD = "WITH q AS (";
    private static final String CTE_LIMIT_FORM_TAIL = ") SELECT * FROM q";
    // The estimated rows that the joins produce per left row, past which a later join takes a
    // selective key or a narrowed dimension. The fuzz tables hold up to about 150 rows, so a query
    // returns a few thousand rows at most, which every axis materializes several times. The
    // estimate leaves out NULL keys, which match each other: a star over keys that are NULL in
    // many rows of its inputs may return far more, and QueryRunner skips it past its row cap.
    private static final double FAN_OUT_BUDGET = 24;
    // A fault query's test_fault() goes into the WHERE clause of a join's input, into the ON residual
    // of a join, or at the head of the query's WHERE clause; see generate().
    private static final int FAULT_SITE_BUILD = 1;
    private static final int FAULT_SITE_RESIDUAL = 2;
    private static final int FAULT_SITE_WHERE = 0;
    private static final int FAULT_SITES = 3;
    private static final String INT_KEY = "k";
    // Interval widths in hours. The fuzz tables start at FuzzConfig's 2024-01-01 and span 30 to
    // 75 hours, so an interval selects anything from no row to every row of a table.
    private static final int[] INTERVAL_HOURS = {1, 3, 12, 48};
    // The key picks: 0-11 INT and 12-23 SYMBOL, which the operator reads off the frames; 24-29
    // LONG, 30-35 VARCHAR and 36-37 SYMBOL against VARCHAR; the composite keys 38-45 INT and
    // SYMBOL, 46-50 LONG and VARCHAR and 51-55 INT and LONG; 56-57 INT against LONG and 58-59 the
    // designated timestamps. All but the INT and SYMBOL keys stage their key into a map.
    private static final int KEY_PICKS = 60;
    // A query's LIMIT, literal or bound, which comes last.
    private static final Pattern LIMIT_CLAUSE = Pattern.compile(" LIMIT (?:\\d+, )?-?(?:\\d+|\\$1)$");
    // The draws of applyLimitForm(): the LIMIT_FORM_* values and, past them, the draws that keep the
    // query as it is.
    private static final int LIMIT_FORMS = 12;
    private static final String LONG_KEY = "lk";
    // The head of the LT join of a LIMIT form, see applyLimitForm().
    private static final String LT_LIMIT_FORM_HEAD = " LT JOIN ";
    // The share of its table that a narrowed dimension keeps: the rows of its first one to six
    // hours, twelve at most of at least sixty.
    private static final double NARROWED_SHARE = 0.1;
    // The WHERE of the LIMIT form that holds no column, see applyLimitForm().
    private static final String RUNTIME_CONSTANT_FILTER_LIMIT_FORM = " WHERE now() > '2000-01-01'";
    // One later join in this many reads its key off an earlier join's input instead of the left
    // input's: a snowflake join, which the operator leaves to an ordinary hash join over it.
    private static final int SNOWFLAKE_ONE_IN_N = 12;
    // The text of the sub-query form before the query, see applyLimitForm().
    private static final String SUB_QUERY_LIMIT_FORM_HEAD = "SELECT * FROM (";
    private static final String SYMBOL_KEY = "sym";
    // The alias of the slave of an ASOF or LT join that a LIMIT form adds, see applyLimitForm().
    private static final String TIME_SERIES_ALIAS = "a";
    // The ASOF or LT join of a LIMIT form as applyLimitForm() writes it: over a table, keyed on the
    // left input's SYMBOL key, which a projection may rename, or not, and an ASOF join under a
    // TOLERANCE or not.
    private static final Pattern TIME_SERIES_JOIN_LIMIT_FORM = Pattern.compile(
            " (?:ASOF JOIN \\S+ a(?: ON a\\.sym = l\\.sym(?:_v)?)?(?: TOLERANCE 1h)?|LT JOIN \\S+ a(?: ON a\\.sym = l\\.sym(?:_v)?)?)"
    );
    private static final String TS = "ts";
    // The text of the UNION ALL form between the query and its copy, see applyLimitForm().
    private static final String UNION_ALL_LIMIT_FORM = " UNION ALL ";
    private static final String VARCHAR_KEY = "vk";

    private EquiJoinClause() {
    }

    /**
     * Draws one join of two inputs, without window functions: the shape of the first join of a
     * star.
     */
    public static GeneratedQuery generate(Rnd rnd, FuzzTable left, FuzzTable right, BindContext ctx, boolean injectFaultFn) {
        return generate(rnd, left, right, null, ctx, injectFaultFn);
    }

    /**
     * Draws a star: the join of the two tables, then, unless {@code tables} is null, up to
     * {@link #MAX_JOINS} - 1 later joins over the given tables, and window functions over the
     * joined rows.
     */
    public static GeneratedQuery generate(
            Rnd rnd,
            FuzzTable left,
            FuzzTable right,
            @Nullable ObjList<FuzzTable> tables,
            BindContext ctx,
            boolean injectFaultFn
    ) {
        // The draws below depend neither on ctx nor on injectFaultFn, but for the fault's site, so
        // the bind variant, which a fault query never runs, regenerates the same tree.
        final Rnd formRnd = new Rnd(rnd.getSeed0() ^ 0x9E37_79B9_7F4A_7C15L, rnd.getSeed1() ^ 0xC2B2_AE3D_27D4_EB4FL);
        final boolean isStar = tables != null;
        final boolean isFirstLeftJoin = rnd.nextBoolean();
        // A third of a star's INNER first joins read two unfiltered tables, which the joins' order
        // decides whether to swap, and leave every filter of the two tables out of the query, since
        // a filtered input never swaps. Other queries filter their inputs too often to swap much.
        final boolean isSwapProne = isStar && !isFirstLeftJoin && rnd.nextInt(3) == 0;
        final Input l = isSwapProne ? tableInput(rnd, left) : pickInput(rnd, left, ctx);
        final ObjList<Join> joins = new ObjList<>();
        // The first join's right table is the left one now and then, so that the two inputs of a
        // join that may swap them have the same size.
        final FuzzTable firstRight = isStar && rnd.nextInt(8) == 0 ? left : right;
        final Input r = isSwapProne ? tableInput(rnd, firstRight) : pickInput(rnd, firstRight, ctx);
        final int firstKeyPick = rnd.nextInt(KEY_PICKS);
        joins.add(new Join(RIGHT_ALIAS, isFirstLeftJoin, r, firstKeyPick, -1));
        double fanOut = Math.max(1, keyFanOut(firstKeyPick) * r.rowShare);
        final int laterJoins = isStar ? pickLaterJoinCount(rnd) : 0;
        for (int j = 1; j <= laterJoins; j++) {
            final boolean isLeftJoin = rnd.nextBoolean();
            final FuzzTable table = tables.getQuick(rnd.nextInt(tables.size()));
            final int keySource = rnd.nextInt(SNOWFLAKE_ONE_IN_N) == 0 ? rnd.nextInt(j) : -1;
            int keyPick = rnd.nextInt(KEY_PICKS);
            boolean isNarrowed = false;
            if (fanOut * keyFanOut(keyPick) > FAN_OUT_BUDGET) {
                // Too many duplicates for the joins before it: narrow the dimension, which keeps its
                // duplicate keys, or take a key that matches about one row.
                isNarrowed = rnd.nextBoolean();
                if (!isNarrowed || fanOut * keyFanOut(keyPick) * NARROWED_SHARE > FAN_OUT_BUDGET) {
                    keyPick = pickSelectiveKey(rnd);
                }
            }
            final Input input = isNarrowed ? narrowedInput(rnd, table) : pickInput(rnd, table, ctx);
            joins.add(new Join("r" + (j + 1), isLeftJoin, input, keyPick, keySource));
            fanOut *= Math.max(1, keyFanOut(keyPick) * input.rowShare);
        }
        // True when the left input may run a filter, which a bare LIMIT would be pushed into.
        boolean isLeftFiltered = l.isFiltered;
        // Where a fault query's test_fault() goes: at the head of the WHERE clause, into the ON
        // residual of a join, which a LEFT join runs for each pair, or into the WHERE clause of a
        // join's input, which the build runs for each of its rows. Draws nothing without a fault.
        final int faultSite = injectFaultFn ? rnd.nextInt(FAULT_SITES) : FAULT_SITE_WHERE;
        final int faultJoin = faultSite != FAULT_SITE_WHERE ? rnd.nextInt(joins.size()) : -1;

        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        final boolean isSelectStar = rnd.nextInt(6) == 0;
        int projected = 0;
        int windows = 0;
        if (isSelectStar) {
            sql.put('*');
        } else {
            projected = 1 + rnd.nextInt(5);
            for (int i = 0; i < projected; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                final int source = rnd.nextInt(1 + joins.size());
                final Input input = source == 0 ? l : joins.getQuick(source - 1).input;
                final FuzzColumn column = input.columns.getQuick(rnd.nextInt(input.columns.size()));
                sql.put(source == 0 ? LEFT_ALIAS : joins.getQuick(source - 1).alias).put('.').put(column.getName()).put(" AS c").put(i);
            }
            windows = isStar && rnd.nextInt(4) == 0 ? 1 + rnd.nextInt(2) : 0;
            for (int i = 0; i < windows; i++) {
                sql.put(", ");
                appendWindow(sql, rnd, l, joins);
                sql.put(" AS c").put(projected + i);
            }
        }
        // A LIMIT's value goes into the projection list here, once the LIMIT is drawn.
        final int projectionEnd = sql.length();

        sql.put(" FROM ").put(l.fromSql).put(' ').put(LEFT_ALIAS);
        for (int j = 0, n = joins.size(); j < n; j++) {
            final Join join = joins.getQuick(j);
            sql.put(join.isLeftJoin ? " LEFT JOIN " : " JOIN ");
            if (j == faultJoin && faultSite == FAULT_SITE_BUILD) {
                sql.put("(SELECT * FROM ").put(join.input.fromSql).put(" WHERE ").put(TestFaultFunctionFactory.CALL).put(')');
            } else {
                sql.put(join.input.fromSql);
            }
            sql.put(' ').put(join.alias);
            sql.put(" ON ");
            final boolean isSnowflake = join.keySource >= 0;
            appendKey(
                    sql,
                    rnd,
                    join.keyPick,
                    isSnowflake ? joins.getQuick(join.keySource).alias : LEFT_ALIAS,
                    isSnowflake ? joins.getQuick(join.keySource).input : l,
                    join.alias,
                    join.input
            );
            if (rnd.nextInt(3) == 0) {
                // A residual over the left input, over both inputs or over the join's input and an
                // earlier join's runs per pair on a LEFT join; an INNER join moves the condition
                // into its WHERE clause. One over the join's input alone filters the build either
                // way.
                sql.put(" AND (");
                int kind = rnd.nextInt(j == 0 ? 3 : 4);
                if (isSwapProne && !join.isLeftJoin && (kind == 0 || (j == 0 && kind == 1))) {
                    // The INNER join hands the condition to the left input's filter or to the first
                    // join's build filter, either of which keeps the first join from swapping.
                    kind = 2;
                }
                switch (kind) {
                    case 0 -> {
                        sql.put(predicate(rnd, l, LEFT_ALIAS, ctx));
                        // The INNER join's WHERE clause hands the condition to the left input's filter.
                        isLeftFiltered |= !join.isLeftJoin;
                    }
                    case 1 -> sql.put(predicate(rnd, join.input, join.alias, ctx));
                    case 2 -> sql.put(crossPredicate(rnd, l, LEFT_ALIAS, join.input, join.alias));
                    default -> {
                        final Join earlier = joins.getQuick(rnd.nextInt(j));
                        sql.put(crossPredicate(rnd, earlier.input, earlier.alias, join.input, join.alias));
                    }
                }
                sql.put(')');
            }
            if (j == faultJoin && faultSite == FAULT_SITE_RESIDUAL) {
                // test_fault() comes first and returns true until it throws; the other operand
                // reads both inputs, so the conjunct stays a residual over the pair.
                sql.put(" AND (").put(TestFaultFunctionFactory.CALL).put(" OR ")
                        .put(crossPredicate(rnd, l, LEFT_ALIAS, join.input, join.alias)).put(')');
            }
        }
        // An ASOF or LT join, or a WHERE, of a LIMIT form goes here, see applyLimitForm().
        final int fromEnd = sql.length();

        final ObjList<String> conjuncts = new ObjList<>();
        if (rnd.nextBoolean() && !isSwapProne) {
            // The left input's filter, which the operator steals as its probe filter.
            conjuncts.add(predicate(rnd, l, LEFT_ALIAS, ctx));
            isLeftFiltered = true;
        }
        for (int j = 0, n = joins.size(); j < n; j++) {
            final Join join = joins.getQuick(j);
            // A swap-prone query filters neither the left input nor the first join's.
            final boolean isFilterable = !isSwapProne || j > 0;
            if (rnd.nextInt(j == 0 ? 3 : 4) == 0 && isFilterable) {
                // The build's filter on an INNER join; a post-join filter on a LEFT one.
                conjuncts.add(predicate(rnd, join.input, join.alias, ctx));
            }
            if (rnd.nextInt(4) == 0) {
                // A post-join filter on this join; on a later join, it may compare its input with an
                // earlier join's.
                final int other = j == 0 ? 0 : rnd.nextInt(j + 1);
                if (other == 0) {
                    conjuncts.add(crossPredicate(rnd, l, LEFT_ALIAS, join.input, join.alias));
                } else {
                    final Join earlier = joins.getQuick(other - 1);
                    conjuncts.add(crossPredicate(rnd, earlier.input, earlier.alias, join.input, join.alias));
                }
            }
            if (join.isLeftJoin && rnd.nextInt(8) == 0) {
                // The LEFT join's misses alone.
                conjuncts.add(join.alias + '.' + join.input.name(INT_KEY) + " IS NULL");
            }
            if (j == 0 && l.isTable && rnd.nextInt(4) == 0 && isFilterable) {
                conjuncts.add(interval(rnd, LEFT_ALIAS));
            }
            if (join.input.isTable && rnd.nextInt(j == 0 ? 6 : 8) == 0 && isFilterable) {
                conjuncts.add(interval(rnd, join.alias));
            }
        }
        if (rnd.nextInt(16) == 0) {
            conjuncts.add(CONSTANT_PREDICATES[rnd.nextInt(CONSTANT_PREDICATES.length)]);
        }
        final boolean isFaultInWhere = injectFaultFn && faultSite == FAULT_SITE_WHERE;
        if (isFaultInWhere || conjuncts.size() > 0) {
            sql.put(" WHERE ");
            if (isFaultInWhere) {
                sql.put(TestFaultFunctionFactory.CALL);
            }
            for (int i = 0, n = conjuncts.size(); i < n; i++) {
                if (i > 0 || isFaultInWhere) {
                    sql.put(" AND ");
                }
                sql.put('(').put(conjuncts.getQuick(i)).put(')');
            }
        }

        boolean hasOrderBy = true;
        final int orderPick = rnd.nextInt(6);
        switch (orderPick) {
            // The left input's designated timestamp descending: a backward probe scan.
            case 0 -> sql.put(" ORDER BY ").put(LEFT_ALIAS).put('.').put(TS).put(" DESC");
            // Ascending, which the left input already scans in, and which fixes the join's order.
            case 1 -> sql.put(" ORDER BY ").put(LEFT_ALIAS).put('.').put(TS);
            case 2 -> {
                if (!isSelectStar) {
                    sql.put(" ORDER BY c").put(rnd.nextInt(projected + windows)).put(rnd.nextBoolean() ? " DESC" : "");
                } else {
                    hasOrderBy = false;
                }
            }
            default -> hasOrderBy = false;
        }
        final boolean hasLimit = rnd.nextInt(6) == 0;
        String limitReuse = null;
        boolean isOrdered = hasOrderBy;
        int limitStart = -1;
        if (hasLimit) {
            if (!hasOrderBy && isLeftFiltered) {
                // A bare LIMIT would be pushed into the left input's filter (questdb/questdb#7982);
                // the order adds no draw, so the bind form keeps the literal form's tree.
                sql.put(" ORDER BY ").put(LEFT_ALIAS).put('.').put(TS);
                isOrdered = true;
            }
            limitStart = sql.length();
            final String sign = rnd.nextBoolean() ? "" : "-";
            final String value = Integer.toString(1 + rnd.nextInt(30));
            final String lowerBound = rnd.nextInt(3) == 0 ? rnd.nextInt(10) + ", " : "";
            if (isSelectStar) {
                sql.put(" LIMIT ").put(lowerBound).put(sign).put(value);
            } else {
                // The projection reuses the LIMIT's value, which the bind form reads as $1.
                final String limit = ctx != null ? ctx.bindLimit(value) : value;
                sql.put(" LIMIT ").put(lowerBound).put(sign).put(limit);
                limitReuse = ", " + LEFT_ALIAS + '.' + l.name(INT_KEY) + " + " + limit + " AS c" + (projected + windows);
            }
        }
        final String text = sql.toString();
        String query = limitReuse == null ? text : text.substring(0, projectionEnd) + limitReuse + text.substring(projectionEnd);
        if (hasLimit && !injectFaultFn) {
            // The LIMIT's value in the projection moved the clauses after it.
            final int shift = limitReuse == null ? 0 : limitReuse.length();
            // An ASOF or LT join needs the joins' rows in ascending timestamp order: neither a
            // backward left input nor an ORDER BY other than the left input's timestamp, ascending,
            // nor a window function, whose order the scan may follow.
            final boolean isAscending = !l.isBackward && windows == 0 && (!hasOrderBy || orderPick == 1);
            query = applyLimitForm(formRnd, query, fromEnd + shift, limitStart + shift, isOrdered, isAscending, conjuncts.size() > 0, l, right);
        }
        return new GeneratedQuery(query, !hasLimit && windows == 0);
    }

    /**
     * The LIMIT form in which a query of this generator states its LIMIT, see
     * {@link #applyLimitForm}: one of the {@code LIMIT_FORM_*} values, {@link #LIMIT_FORM_NONE} for a
     * query that states its LIMIT, if it has one, as it is. Reads the queries of this generator
     * only, whose names and literals hold no space: only a form writes a UNION ALL, an ASOF or LT
     * join or the WHERE that holds no column, and the parenthesis of the sub-query form closes right
     * before the LIMIT, unlike the one of a query whose left input is a sub-query.
     */
    public static int limitForm(String sql) {
        final int limitStart = limitStart(sql);
        if (limitStart < 0) {
            return LIMIT_FORM_NONE;
        }
        if (sql.startsWith(SUB_QUERY_LIMIT_FORM_HEAD) && closing(sql, SUB_QUERY_LIMIT_FORM_HEAD.length() - 1) == limitStart - 1) {
            return LIMIT_FORM_SUB_QUERY;
        }
        if (sql.startsWith(CTE_LIMIT_FORM_HEAD)) {
            return LIMIT_FORM_CTE;
        }
        if (sql.contains(UNION_ALL_LIMIT_FORM)) {
            return LIMIT_FORM_UNION_ALL;
        }
        if (sql.contains(AS_OF_LIMIT_FORM_HEAD)) {
            return LIMIT_FORM_AS_OF;
        }
        if (sql.contains(LT_LIMIT_FORM_HEAD)) {
            return LIMIT_FORM_LT;
        }
        return sql.contains(RUNTIME_CONSTANT_FILTER_LIMIT_FORM) ? LIMIT_FORM_RUNTIME_CONSTANT_FILTER : LIMIT_FORM_NONE;
    }

    /**
     * The text that the LIMIT form of a query of this generator adds to the query it wraps, which
     * {@link #unwrapLimitForm(String)} takes out: the texts of the sub-query or of the CTE around the
     * query, joined; the UNION ALL, without its second branch, a copy of the query that a bind form
     * writes with its own bind variables; the ASOF or LT join; or the WHERE that holds no column. An
     * empty string for a query without a form. Throws an AssertionError as unwrapLimitForm() does.
     */
    public static String limitFormText(String sql) {
        return splitLimitForm(sql).text();
    }

    /**
     * The query whose LIMIT a query of this generator states in a LIMIT form, with that LIMIT: the
     * query of the sub-query or of the CTE, the first branch of the UNION ALL, or the query without
     * the ASOF or LT join or without the WHERE that holds no column; a query without a form as it
     * is. Reads the queries of this generator only, see {@link #limitForm(String)}, and throws an
     * AssertionError on a form that {@link #applyLimitForm} does not write, so that a form and its
     * inverse cannot drift apart unnoticed.
     */
    public static String unwrapLimitForm(String sql) {
        return splitLimitForm(sql).query();
    }

    /**
     * The key of a join: the source input's key columns against the join's input's, as the key pick
     * says (see {@link #KEY_PICKS}).
     */
    private static void appendKey(StringSink sql, Rnd rnd, int keyPick, String sourceAlias, Input source, String alias, Input input) {
        if (keyPick >= 46 && keyPick < 51) {
            appendKeyEquality(sql, rnd, sourceAlias, source.name(LONG_KEY), alias, input.name(LONG_KEY));
            sql.put(" AND ");
            appendKeyEquality(sql, rnd, sourceAlias, source.name(VARCHAR_KEY), alias, input.name(VARCHAR_KEY));
        } else if (keyPick >= 51 && keyPick < 56) {
            appendKeyEquality(sql, rnd, sourceAlias, source.name(INT_KEY), alias, input.name(INT_KEY));
            sql.put(" AND ");
            appendKeyEquality(sql, rnd, sourceAlias, source.name(LONG_KEY), alias, input.name(LONG_KEY));
        } else if (keyPick >= 56 && keyPick < 58) {
            appendKeyEquality(sql, rnd, sourceAlias, source.name(INT_KEY), alias, input.name(LONG_KEY));
        } else if (keyPick >= 58) {
            appendKeyEquality(sql, rnd, sourceAlias, TS, alias, TS);
        } else if (keyPick < 12 || keyPick >= 38) {
            appendKeyEquality(sql, rnd, sourceAlias, source.name(INT_KEY), alias, input.name(INT_KEY));
        } else if (keyPick < 24) {
            appendKeyEquality(sql, rnd, sourceAlias, source.name(SYMBOL_KEY), alias, input.name(SYMBOL_KEY));
        } else if (keyPick < 30) {
            appendKeyEquality(sql, rnd, sourceAlias, source.name(LONG_KEY), alias, input.name(LONG_KEY));
        } else if (keyPick < 36) {
            appendKeyEquality(sql, rnd, sourceAlias, source.name(VARCHAR_KEY), alias, input.name(VARCHAR_KEY));
        } else {
            // vk holds sym's texts, so the pair matches rows; both sides stage their text.
            appendKeyEquality(sql, rnd, sourceAlias, source.name(SYMBOL_KEY), alias, input.name(VARCHAR_KEY));
        }
        if (keyPick >= 38 && keyPick < 46) {
            sql.put(" AND ");
            appendKeyEquality(sql, rnd, sourceAlias, source.name(SYMBOL_KEY), alias, input.name(SYMBOL_KEY));
        }
    }

    // Both operand orders: the join context records the key per model either way.
    private static void appendKeyEquality(StringSink sql, Rnd rnd, String sourceAlias, String sourceColumn, String alias, String column) {
        if (rnd.nextBoolean()) {
            sql.put(sourceAlias).put('.').put(sourceColumn).put(" = ").put(alias).put('.').put(column);
        } else {
            sql.put(alias).put('.').put(column).put(" = ").put(sourceAlias).put('.').put(sourceColumn);
        }
    }

    /**
     * A window function over the joined rows: a ranking, a count, a sum or a neighbouring value of
     * a key column of any input, over no partition or one key column's, in the order of no column,
     * the left input's timestamp either way or a joined input's timestamp.
     */
    private static void appendWindow(StringSink sql, Rnd rnd, Input l, ObjList<Join> joins) {
        switch (rnd.nextInt(7)) {
            case 0 -> sql.put("row_number()");
            case 1 -> sql.put("rank()");
            case 2 -> sql.put("dense_rank()");
            case 3 -> sql.put("count(*)");
            default -> {
                sql.put(rnd.nextBoolean() ? "sum(" : rnd.nextBoolean() ? "lag(" : "first_value(");
                appendInputColumn(sql, rnd, l, joins, rnd.nextBoolean() ? INT_KEY : LONG_KEY);
                sql.put(')');
            }
        }
        sql.put(" OVER (");
        final boolean isPartitioned = rnd.nextBoolean();
        if (isPartitioned) {
            sql.put("PARTITION BY ");
            appendInputColumn(sql, rnd, l, joins, rnd.nextBoolean() ? SYMBOL_KEY : INT_KEY);
        }
        switch (rnd.nextInt(4)) {
            case 0 -> {
            }
            case 1 -> sql.put(isPartitioned ? " " : "").put("ORDER BY ").put(LEFT_ALIAS).put('.').put(TS);
            case 2 -> sql.put(isPartitioned ? " " : "").put("ORDER BY ").put(LEFT_ALIAS).put('.').put(TS).put(" DESC");
            default -> {
                final Join join = joins.getQuick(rnd.nextInt(joins.size()));
                sql.put(isPartitioned ? " " : "").put("ORDER BY ").put(join.alias).put('.').put(TS);
                if (rnd.nextBoolean()) {
                    sql.put(" DESC");
                }
            }
        }
        sql.put(')');
    }

    // A shared column of the left input or of a joined one, under the name the input exposes.
    private static void appendInputColumn(StringSink sql, Rnd rnd, Input l, ObjList<Join> joins, String baseName) {
        final int source = rnd.nextInt(1 + joins.size());
        if (source == 0) {
            sql.put(LEFT_ALIAS).put('.').put(l.name(baseName));
        } else {
            final Join join = joins.getQuick(source - 1);
            sql.put(join.alias).put('.').put(join.input.name(baseName));
        }
    }

    /**
     * On half of the draws, states the query's LIMIT in a form that reaches the parallel hash joins
     * from outside their own query, or through an operator over them, if the query allows the form,
     * and otherwise leaves the query as it is: the LIMIT of a sub-query over the query, or of a CTE
     * of it; of a UNION ALL of the query with itself, unless it has an ORDER BY, which a branch
     * would have to keep; of the query whose joins are the master of an ASOF join, keyed on the left
     * input's SYMBOL key or not, under a TOLERANCE or not, or of an LT join, both of which need the
     * joins' rows in ascending timestamp order; or of the query under a WHERE that reads no column,
     * which runs once per execution, unless the query has a WHERE.
     *
     * @param fromEnd    the end of the query's FROM clause, the joins included
     * @param limitStart the start of the query's LIMIT, its last clause
     */
    private static String applyLimitForm(
            Rnd formRnd,
            String query,
            int fromEnd,
            int limitStart,
            boolean isOrdered,
            boolean isAscending,
            boolean hasWhere,
            Input l,
            FuzzTable slave
    ) {
        final String head = query.substring(0, limitStart);
        final String limit = query.substring(limitStart);
        final int form = formRnd.nextInt(LIMIT_FORMS);
        switch (form) {
            case LIMIT_FORM_SUB_QUERY:
                return SUB_QUERY_LIMIT_FORM_HEAD + head + ')' + limit;
            case LIMIT_FORM_CTE:
                return CTE_LIMIT_FORM_HEAD + head + CTE_LIMIT_FORM_TAIL + limit;
            case LIMIT_FORM_UNION_ALL:
                return isOrdered ? query : head + UNION_ALL_LIMIT_FORM + head + limit;
            case LIMIT_FORM_AS_OF:
            case LIMIT_FORM_LT: {
                if (!isAscending) {
                    return query;
                }
                final StringSink join = new StringSink();
                join.put(form == LIMIT_FORM_AS_OF ? AS_OF_LIMIT_FORM_HEAD : LT_LIMIT_FORM_HEAD).put(slave.getName()).put(' ').put(TIME_SERIES_ALIAS);
                if (formRnd.nextBoolean()) {
                    join.put(" ON ").put(TIME_SERIES_ALIAS).put('.').put(SYMBOL_KEY)
                            .put(" = ").put(LEFT_ALIAS).put('.').put(l.name(SYMBOL_KEY));
                }
                if (form == LIMIT_FORM_AS_OF && formRnd.nextInt(4) == 0) {
                    join.put(" TOLERANCE 1h");
                }
                return query.substring(0, fromEnd) + join + query.substring(fromEnd);
            }
            case LIMIT_FORM_RUNTIME_CONSTANT_FILTER:
                return hasWhere ? query : query.substring(0, fromEnd) + RUNTIME_CONSTANT_FILTER_LIMIT_FORM + query.substring(fromEnd);
            default:
                return query;
        }
    }

    // The index of the parenthesis that closes the one at the given index, past quoted literals.
    private static int closing(String sql, int open) {
        int depth = 0;
        for (int i = open; i < sql.length(); i++) {
            final char c = sql.charAt(i);
            if (c == '\'') {
                i = sql.indexOf('\'', i + 1);
            } else if (c == '(') {
                depth++;
            } else if (c == ')' && --depth == 0) {
                return i;
            }
        }
        throw new AssertionError("unbalanced parentheses: " + sql);
    }

    /**
     * A comparison of two inputs that is never an equality, so that it stays a filter over the
     * joined pairs: the designated timestamps, the INT key against the LONG one, or two numeric
     * columns, one of each input.
     */
    private static String crossPredicate(Rnd rnd, Input a, String aliasA, Input b, String aliasB) {
        final String op = CROSS_INPUT_OPS[rnd.nextInt(CROSS_INPUT_OPS.length)];
        final int pick = rnd.nextInt(3);
        if (pick == 0) {
            return aliasA + '.' + TS + ' ' + op + ' ' + aliasB + '.' + TS;
        }
        if (pick == 1) {
            return aliasA + '.' + a.name(INT_KEY) + ' ' + op + ' ' + aliasB + '.' + b.name(LONG_KEY);
        }
        final FuzzColumn columnA = pickNumeric(rnd, a.columns);
        final FuzzColumn columnB = pickNumeric(rnd, b.columns);
        final String nameA = columnA != null ? columnA.getName() : a.name(LONG_KEY);
        final String nameB = columnB != null ? columnB.getName() : b.name(LONG_KEY);
        return aliasA + '.' + nameA + ' ' + op + ' ' + aliasB + '.' + nameB;
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
     * The estimated rows of a fuzz table of 150 rows that one left row matches on the key the pick
     * says. Every key domain holds NULL, which matches NULL: the INT and LONG keys take one of 16
     * values or NULL (one row in eight), so a value repeats in about 10 rows; the SYMBOL and VARCHAR
     * keys take one of 8 values or NULL, each in a ninth of the rows; a composite key matches about
     * one row, and the designated timestamps, which the fuzz tables share, at most one.
     */
    private static double keyFanOut(int keyPick) {
        if (keyPick < 12 || (keyPick >= 24 && keyPick < 30) || (keyPick >= 56 && keyPick < 58)) {
            return 10;
        }
        if (keyPick < 38) {
            return 17;
        }
        return 1;
    }

    // The start of the query's LIMIT, literal or bound, which comes last, or -1 without a LIMIT.
    private static int limitStart(String sql) {
        final Matcher limit = LIMIT_CLAUSE.matcher(sql);
        return limit.find() ? limit.start() : -1;
    }

    /**
     * A dimension narrowed to its table's first one to six hours by an interval, which keeps the
     * input a page frame scan the operator takes as a build, and keeps between two and twelve of its
     * rows, so that a later join's duplicate keys stay within the fan-out budget.
     */
    private static Input narrowedInput(Rnd rnd, FuzzTable table) {
        final String fromSql = "(SELECT * FROM " + FuzzNames.table(rnd, table.getName())
                + " WHERE " + TS + " < " + hourOf2024(1 + rnd.nextInt(6)) + ')';
        return new Input(fromSql, table.getColumns(), false, false, false, NARROWED_SHARE);
    }

    // The table itself, unfiltered.
    private static Input tableInput(Rnd rnd, FuzzTable table) {
        return new Input(FuzzNames.table(rnd, table.getName()), table.getColumns(), true, false, false, 1);
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
            return new Input(tableName, table.getColumns(), true, false, false, 1);
        }
        if (pick >= 38) {
            final int limit = 10 + rnd.nextInt(50);
            return new Input("(SELECT * FROM " + tableName + " LIMIT " + limit + ')', table.getColumns(), false, false, false, limit / 150.0);
        }
        if (pick >= 36) {
            return new Input("(SELECT * FROM " + tableName + " ORDER BY " + TS + " DESC)", table.getColumns(), false, false, true, 1);
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
        return new Input(sql.toString(), exposed, false, isFiltered, false, 1);
    }

    /**
     * How many joins follow the first one: none on half the draws, one on a quarter, two on an
     * eighth, and three to five on the rest.
     */
    private static int pickLaterJoinCount(Rnd rnd) {
        final int pick = rnd.nextInt(8);
        if (pick < 4) {
            return 0;
        }
        if (pick < 6) {
            return 1;
        }
        if (pick < 7) {
            return 2;
        }
        return 3 + rnd.nextInt(MAX_JOINS - 3);
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

    // A key that matches about one row: one of the composite keys, picks 38 to 55, or the
    // designated timestamps, picks 58 and 59.
    private static int pickSelectiveKey(Rnd rnd) {
        final int pick = 38 + rnd.nextInt(20);
        return pick < 56 ? pick : pick + 2;
    }

    private static String predicate(Rnd rnd, Input input, String alias, BindContext ctx) {
        return new PredicateGenerator(rnd, 1).generate(input.columns, alias, ctx);
    }

    /**
     * Takes a query of this generator apart at its LIMIT form, see {@link #unwrapLimitForm(String)}
     * and {@link #limitFormText(String)}: the inverse of {@link #applyLimitForm}, which checks that
     * the form is one that applyLimitForm() writes.
     */
    private static LimitFormSplit splitLimitForm(String sql) {
        final int form = limitForm(sql);
        if (form == LIMIT_FORM_NONE) {
            return new LimitFormSplit(sql, "");
        }
        final int limitStart = limitStart(sql);
        final String limit = sql.substring(limitStart);
        return switch (form) {
            case LIMIT_FORM_SUB_QUERY -> new LimitFormSplit(
                    sql.substring(SUB_QUERY_LIMIT_FORM_HEAD.length(), limitStart - 1) + limit,
                    SUB_QUERY_LIMIT_FORM_HEAD + ')'
            );
            case LIMIT_FORM_CTE -> {
                final int tailStart = limitStart - CTE_LIMIT_FORM_TAIL.length();
                if (!sql.startsWith(CTE_LIMIT_FORM_TAIL, tailStart)) {
                    throw new AssertionError("a CTE form whose LIMIT does not follow the CTE: " + sql);
                }
                yield new LimitFormSplit(sql.substring(CTE_LIMIT_FORM_HEAD.length(), tailStart) + limit, CTE_LIMIT_FORM_HEAD + CTE_LIMIT_FORM_TAIL);
            }
            case LIMIT_FORM_UNION_ALL -> {
                final int union = sql.indexOf(UNION_ALL_LIMIT_FORM);
                final String query = sql.substring(0, union);
                if (!query.equals(sql.substring(union + UNION_ALL_LIMIT_FORM.length(), limitStart))) {
                    throw new AssertionError("a UNION ALL form of two different queries: " + sql);
                }
                yield new LimitFormSplit(query + limit, UNION_ALL_LIMIT_FORM);
            }
            case LIMIT_FORM_AS_OF, LIMIT_FORM_LT -> {
                // The join ends where the query's WHERE, ORDER BY or LIMIT starts, whichever comes
                // first.
                final int start = sql.indexOf(form == LIMIT_FORM_AS_OF ? AS_OF_LIMIT_FORM_HEAD : LT_LIMIT_FORM_HEAD);
                int end = limitStart;
                for (String clause : new String[]{" WHERE ", " ORDER BY "}) {
                    final int at = sql.indexOf(clause, start);
                    if (at > -1 && at < end) {
                        end = at;
                    }
                }
                final String join = sql.substring(start, end);
                if (!TIME_SERIES_JOIN_LIMIT_FORM.matcher(join).matches()) {
                    throw new AssertionError("an ASOF or LT join that no LIMIT form writes: " + sql);
                }
                yield new LimitFormSplit(sql.substring(0, start) + sql.substring(end), join);
            }
            // LIMIT_FORM_RUNTIME_CONSTANT_FILTER
            default -> {
                final int filter = sql.indexOf(RUNTIME_CONSTANT_FILTER_LIMIT_FORM);
                yield new LimitFormSplit(
                        sql.substring(0, filter) + sql.substring(filter + RUNTIME_CONSTANT_FILTER_LIMIT_FORM.length()),
                        RUNTIME_CONSTANT_FILTER_LIMIT_FORM
                );
            }
        };
    }

    /**
     * One join input: the SQL after {@code FROM} (or after the join keyword), the columns it
     * exposes, under the names it exposes them by, and the share of its table's rows it reads at
     * most.
     */
    private static final class Input {
        final ObjList<FuzzColumn> columns;
        final String fromSql;
        // The table scanned in descending timestamp order.
        final boolean isBackward;
        // A projection with a WHERE clause, which the input's filter runs.
        final boolean isFiltered;
        final boolean isTable;
        final double rowShare;

        Input(String fromSql, ObjList<FuzzColumn> columns, boolean isTable, boolean isFiltered, boolean isBackward, double rowShare) {
            this.fromSql = fromSql;
            this.columns = columns;
            this.isTable = isTable;
            this.isFiltered = isFiltered;
            this.isBackward = isBackward;
            this.rowShare = rowShare;
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

    /**
     * One join of the query: its alias, its type, its input, its key pick and the earlier join
     * whose input the key reads instead of the left input's, or -1.
     */
    private static final class Join {
        final String alias;
        final Input input;
        final boolean isLeftJoin;
        final int keyPick;
        final int keySource;

        Join(String alias, boolean isLeftJoin, Input input, int keyPick, int keySource) {
            this.alias = alias;
            this.isLeftJoin = isLeftJoin;
            this.input = input;
            this.keyPick = keyPick;
            this.keySource = keySource;
        }
    }

    /**
     * A query of this generator taken apart at its LIMIT form: the query that the form wraps, with
     * its LIMIT, and the text that the form adds to it, see {@link #limitFormText(String)}.
     */
    private record LimitFormSplit(String query, String text) {
    }
}
