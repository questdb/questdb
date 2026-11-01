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

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.FuzzColumn;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ConstantExpr;
import io.questdb.test.griffin.fuzz.expr.FuzzConstant;
import io.questdb.test.griffin.fuzz.types.ColumnKind;
import io.questdb.test.griffin.fuzz.types.IntKeyType;
import io.questdb.test.griffin.fuzz.types.SymbolType;

/**
 * A set operation whose branches give one output column different types, filtered over that
 * column:
 * <pre>
 * [WITH u AS (set)] SELECT v, id FROM (set) u WHERE filter(v)
 * [WITH u AS (set)] SELECT u.v, u.id, x.ts FROM (set) u [LEFT] JOIN t x ON on(u.v)
 * [WITH u AS (set)] SELECT x.ts, u.v, u.id FROM t x [LEFT] JOIN (set) u ON on(u.v)
 * </pre>
 * The set chains two or three branches with UNION, EXCEPT or INTERSECT, each with or without ALL.
 * A branch is {@code SELECT term vN, k idN FROM t [WHERE p]} over a table of its own draw, and
 * every term of one query comes from one family that {@code SetOperationCasts} unifies:
 * <ul>
 *     <li>TEXT: a SYMBOL, STRING or VARCHAR term in one of the first two branches, beside a term of
 *     any other type but DECIMAL; the set unifies to STRING or VARCHAR, or keeps SYMBOL when every
 *     branch reads one;</li>
 *     <li>TEMPORAL: TIMESTAMP, TIMESTAMP_NS and DATE, unifying to the finest precision;</li>
 *     <li>NUMERIC: BYTE, SHORT, INT, LONG, FLOAT and DOUBLE, unifying to the widest.</li>
 * </ul>
 * The families leave out what the engine rejects: a number or CHAR beside a TIMESTAMP or DATE, and
 * a CHAR beside a FLOAT or DOUBLE. They also leave out DECIMAL, which a STRING or VARCHAR branch
 * unifies to DECIMAL and fails on its first text that is not a number. TEXT keeps its text term
 * among the first two branches because the set unifies those two before it reads the third. Only
 * later branches may select NULL, so the first branch always types the column.
 * <p>
 * Two UNION chains are drawn on purpose. One TEXT query in four leads with two {@code sym}
 * branches and takes the type a third branch gives a SYMBOL. One TEMPORAL query in two leads with
 * the designated timestamp beside its TIMESTAMP_NS or DATE cast, in either order, or beside NULL.
 * The binder binds a filter over that column in each branch only when the branch's precision does
 * not change what the filter means, the optimiser pushes false into the NULL branch only when the
 * filter rejects NULL, and a UNION returns every row either branch keeps.
 * <p>
 * The filter reads v the way its family unifies: text against timestamp texts and SYMBOL values,
 * with IN and LIKE; time against timestamps, with IN, BETWEEN, {@code timestamp_floor} and
 * {@code hour}, and through {@code ::LONG} and {@code ::STRING}, whose values depend on the
 * precision; numbers against numbers. Text constants bind as STRING; time constants, quoted
 * texts in the literal form, bind as TIMESTAMP. A join keys on the branches' {@code k}
 * with the filter over {@code u.v} in its ON clause, or, when v is text, on v itself against
 * {@code sym} or {@code vk}; a time or number key would have to match v's unified type exactly.
 * <p>
 * Each query carries a fenced variant, see {@link #fence}, which keeps the filter above the set.
 * The planner may push the filter into the branches only where it means there what it means over
 * the column the set returns, so QueryRunner holds the two to the same rows.
 */
public final class SetOperationClause {
    // 2024-01-01T00:00:00Z, where the fuzz tables start, in microseconds.
    private static final long FIRST_MICROS = 1_704_067_200_000_000L;
    private static final String[] FLOOR_UNITS = {"h", "2h", "15m", "d"};
    private static final long HALF_HOUR_MICROS = 1_800_000_000L;
    private static final String[] JOIN_KINDS = {"JOIN", "LEFT JOIN"};
    private static final String[] LIKE_PATTERNS = {"2024-01-02%", "2024-01-0_T1%", "%:30:00%", "%.000Z", "%000000000Z", "s%", "%5"};
    private static final String[] MINUTES = {"00", "15", "30"};
    private static final String[] NUMERIC_CASTS = {"k::BYTE", "k::SHORT", "k::FLOAT", "lk::DOUBLE"};
    private static final String[] OPERATORS = {"UNION", "UNION ALL", "EXCEPT", "EXCEPT ALL", "INTERSECT", "INTERSECT ALL"};
    private static final String[] ORDERING_OPS = {"=", "!=", "<", "<=", ">", ">="};
    private static final String[][] PRECISION_LEADS = {
            {"ts", "ts::TIMESTAMP_NS"}, {"ts", "ts::DATE"}, {"ts::TIMESTAMP_NS", "ts"}, {"ts::DATE", "ts"}, {"ts", "null"}
    };
    private static final String[] SYMBOL_LEAD = {"sym", "sym"};
    private static final String[] TEMPORAL_CASTS = {"ts::TIMESTAMP_NS", "ts::DATE"};
    private static final String[] TEXT_CASTS = {"sym::STRING"};
    private static final String[] TEXT_JOIN_KEYS = {"sym", "vk"};
    private static final String[] UNION_OPERATORS = {"UNION", "UNION ALL"};

    private SetOperationClause() {
    }

    /**
     * The set with a LIMIT no fuzz table reaches before its closing parenthesis. The LIMIT applies to the whole set,
     * and filter pushdown crosses no LIMIT, so a filter over the set's column stays above it and compares the value
     * the set returns.
     */
    public static String fence(String set) {
        return set.substring(0, set.length() - 1) + " LIMIT 1_000_000)";
    }

    public static GeneratedQuery generate(Rnd rnd, ObjList<FuzzTable> tables, BindContext ctx, boolean injectFaultFn) {
        final int pick = rnd.nextInt(4);
        final ColumnKind family = pick < 2 ? ColumnKind.STRING_LIKE : pick == 2 ? ColumnKind.TEMPORAL : ColumnKind.NUMERIC;
        final String[] lead = family == ColumnKind.STRING_LIKE && rnd.nextInt(4) == 0 ? SYMBOL_LEAD
                : family == ColumnKind.TEMPORAL && rnd.nextBoolean() ? PRECISION_LEADS[rnd.nextInt(PRECISION_LEADS.length)]
                : null;
        final StringSink set = new StringSink();
        set.put('(');
        final int branches = lead == SYMBOL_LEAD ? 3 : 2 + rnd.nextInt(2);
        final int textBranch = rnd.nextInt(2);
        final String[] operators = lead != null ? UNION_OPERATORS : OPERATORS;
        for (int i = 0; i < branches; i++) {
            if (i > 0) {
                set.put(' ').put(operators[rnd.nextInt(operators.length)]).put(' ');
            }
            final FuzzTable table = tables.getQuick(rnd.nextInt(tables.size()));
            final String suffix = i > 0 ? Integer.toString(i) : "";
            set.put("SELECT ").put(lead != null && i < 2 ? lead[i] : pickTerm(rnd, table, family, i == textBranch, i > 0))
                    .put(" v").put(suffix).put(", k id").put(suffix).put(" FROM ").put(table.getName());
            PredicateGenerator.appendWhere(set, rnd, table.getColumns(), null, 1, ctx, injectFaultFn && i == 0);
        }
        set.put(')');
        final String setSql = set.toString();

        final StringSink sql = new StringSink();
        final String setRef;
        if (rnd.nextInt(4) == 0) {
            sql.put("WITH u AS ").put(setSql).put(' ');
            setRef = "u";
        } else {
            setRef = setSql + " u";
        }
        final int form = rnd.nextInt(4);
        if (form < 2) {
            sql.put("SELECT v, id FROM ").put(setRef).put(" WHERE ");
            appendFilter(sql, rnd, family, "", ctx);
            return fenced(sql, setSql);
        }
        final FuzzTable joined = tables.getQuick(rnd.nextInt(tables.size()));
        final String join = JOIN_KINDS[rnd.nextInt(JOIN_KINDS.length)];
        if (form == 2) {
            sql.put("SELECT u.v, u.id, x.ts FROM ").put(setRef).put(' ').put(join).put(' ').put(joined.getName()).put(" x ON ");
        } else {
            sql.put("SELECT x.ts, u.v, u.id FROM ").put(joined.getName()).put(" x ").put(join).put(' ').put(setRef).put(" ON ");
        }
        if (family == ColumnKind.STRING_LIKE && rnd.nextBoolean()) {
            sql.put("u.v = x.").put(TEXT_JOIN_KEYS[rnd.nextInt(TEXT_JOIN_KEYS.length)]);
            if (rnd.nextBoolean()) {
                return fenced(sql, setSql);
            }
        } else {
            sql.put("u.id = x.k");
        }
        sql.put(" AND ");
        appendFilter(sql, rnd, family, "u.", ctx);
        return fenced(sql, setSql);
    }

    private static void addColumns(ObjList<String> terms, FuzzTable table, ColumnKind kind) {
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            final FuzzColumn column = table.getColumn(i);
            final ColumnKind columnKind = column.getType().getKind();
            if (kind == null ? columnKind != ColumnKind.DECIMAL : columnKind == kind) {
                terms.add(column.getName());
            }
        }
    }

    private static void addTerms(ObjList<String> terms, String[] casts) {
        for (String cast : casts) {
            terms.add(cast);
        }
    }

    private static void appendFilter(StringSink sql, Rnd rnd, ColumnKind family, String qualifier, BindContext ctx) {
        final String column = qualifier + 'v';
        switch (rnd.nextInt(4)) {
            case 0 -> {
                sql.put("NOT (");
                appendLeaf(sql, rnd, family, column, ctx);
                sql.put(')');
            }
            case 1 -> {
                sql.put('(');
                appendLeaf(sql, rnd, family, column, ctx);
                sql.put(rnd.nextBoolean() ? " AND " : " OR ");
                if (rnd.nextBoolean()) {
                    appendLeaf(sql, rnd, family, column, ctx);
                } else {
                    sql.put(qualifier).put("id ").put(ORDERING_OPS[rnd.nextInt(ORDERING_OPS.length)]).put(' ');
                    new ConstantExpr(ColumnKind.NUMERIC, IntKeyType.INSTANCE.generateConstant(rnd)).appendSql(sql, ctx);
                }
                sql.put(')');
            }
            default -> appendLeaf(sql, rnd, family, column, ctx);
        }
    }

    private static void appendLeaf(StringSink sql, Rnd rnd, ColumnKind family, String column, BindContext ctx) {
        final int pick = rnd.nextInt(10);
        if (pick == 0) {
            sql.put(column).put(rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            return;
        }
        if (pick == 1) {
            sql.put(column).put(rnd.nextInt(3) == 0 ? " NOT IN (" : " IN (");
            for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                constant(rnd, family).appendSql(sql, ctx);
            }
            sql.put(')');
            return;
        }
        if (family == ColumnKind.STRING_LIKE && pick == 2) {
            appendLike(sql, rnd, column);
            return;
        }
        if (family == ColumnKind.TEMPORAL && pick == 2) {
            sql.put(column).put(" BETWEEN ");
            constant(rnd, family).appendSql(sql, ctx);
            sql.put(" AND ");
            constant(rnd, family).appendSql(sql, ctx);
            return;
        }
        if (family == ColumnKind.TEMPORAL && pick == 3) {
            final String unit = FLOOR_UNITS[rnd.nextInt(FLOOR_UNITS.length)];
            final int day = 1 + rnd.nextInt(4);
            final int hour = unit.equals("d") ? 0 : 2 * rnd.nextInt(12);
            final String floor = "2024-01-0" + day + 'T' + (hour < 10 ? "0" : "") + hour + ":00:00.000000Z";
            sql.put("timestamp_floor('").put(unit).put("', ").put(column).put(") = ");
            new ConstantExpr(ColumnKind.TEMPORAL, new FuzzConstant('\'' + floor + '\'', "TIMESTAMP", floor)).appendSql(sql, ctx);
            return;
        }
        if (family == ColumnKind.TEMPORAL && pick == 4) {
            sql.put("hour(").put(column).put(") ").put(ORDERING_OPS[rnd.nextInt(ORDERING_OPS.length)]).put(' ');
            final String hour = Integer.toString(rnd.nextInt(24));
            new ConstantExpr(ColumnKind.NUMERIC, new FuzzConstant(hour, "INT", hour)).appendSql(sql, ctx);
            return;
        }
        if (family == ColumnKind.TEMPORAL && (pick == 5 || pick == 6)) {
            sql.put(column).put("::LONG ").put(ORDERING_OPS[rnd.nextInt(ORDERING_OPS.length)]).put(' ');
            final String micros = Long.toString(FIRST_MICROS + rnd.nextInt(192) * HALF_HOUR_MICROS);
            new ConstantExpr(ColumnKind.NUMERIC, new FuzzConstant(micros, "LONG", micros)).appendSql(sql, ctx);
            return;
        }
        if (family == ColumnKind.TEMPORAL && pick == 7) {
            appendLike(sql, rnd, column + "::STRING");
            return;
        }
        sql.put(column).put(' ').put(ORDERING_OPS[rnd.nextInt(ORDERING_OPS.length)]).put(' ');
        constant(rnd, family).appendSql(sql, ctx);
    }

    private static void appendLike(StringSink sql, Rnd rnd, String operand) {
        sql.put(operand).put(rnd.nextInt(3) == 0 ? " NOT LIKE '" : " LIKE '").put(LIKE_PATTERNS[rnd.nextInt(LIKE_PATTERNS.length)]).put('\'');
    }

    private static ConstantExpr constant(Rnd rnd, ColumnKind family) {
        if (family == ColumnKind.NUMERIC) {
            final String value = Integer.toString(rnd.nextInt(20) - 2);
            return new ConstantExpr(family, new FuzzConstant(value, "INT", value));
        }
        if (family == ColumnKind.TEMPORAL) {
            final String value = timestampText(rnd, 2);
            return new ConstantExpr(family, new FuzzConstant('\'' + value + '\'', "TIMESTAMP", value));
        }
        final String value = rnd.nextBoolean()
                ? SymbolType.DOMAIN[rnd.nextInt(SymbolType.DOMAIN.length)]
                : timestampText(rnd, rnd.nextInt(3));
        return new ConstantExpr(family, new FuzzConstant('\'' + value + '\'', "STRING", value));
    }

    private static GeneratedQuery fenced(StringSink sql, String set) {
        final String query = sql.toString();
        return new GeneratedQuery(query, true).withFencedSql(query.replace(set, fence(set)));
    }

    private static String pickTerm(Rnd rnd, FuzzTable table, ColumnKind family, boolean isTextBranch, boolean isNullable) {
        final ObjList<String> terms = new ObjList<>();
        if (family == ColumnKind.STRING_LIKE && isTextBranch) {
            addTerms(terms, TEXT_CASTS);
            addColumns(terms, table, ColumnKind.STRING_LIKE);
        } else if (family == ColumnKind.STRING_LIKE && rnd.nextBoolean()) {
            addTerms(terms, TEXT_CASTS);
            addTerms(terms, NUMERIC_CASTS);
            addColumns(terms, table, null);
        } else if (family == ColumnKind.NUMERIC) {
            addTerms(terms, NUMERIC_CASTS);
            addColumns(terms, table, ColumnKind.NUMERIC);
        } else {
            addTerms(terms, TEMPORAL_CASTS);
            addColumns(terms, table, ColumnKind.TEMPORAL);
        }
        if (isNullable && !isTextBranch) {
            terms.add("null");
        }
        return terms.getQuick(rnd.nextInt(terms.size()));
    }

    // Day, hour or microsecond precision; the fuzz tables hold 2024-01-01 to 2024-01-04 at 30-minute steps.
    private static String timestampText(Rnd rnd, int precision) {
        final StringSink text = new StringSink();
        text.put("2024-01-0").put(1 + rnd.nextInt(4));
        if (precision > 0) {
            final int hour = rnd.nextInt(24);
            text.put('T').put(hour < 10 ? "0" : "").put(hour);
        }
        if (precision > 1) {
            text.put(':').put(MINUTES[rnd.nextInt(MINUTES.length)]).put(":00.000000Z");
        }
        return text.toString();
    }
}
