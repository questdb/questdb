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
import io.questdb.test.griffin.fuzz.FuzzSource;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * {@code SELECT [col AS e0, ...] winFn(arg) OVER (
 * [PARTITION BY col[, col]] ORDER BY ts [ASC|DESC] [frame]) AS w0 [, ...]
 * FROM t [WHERE p] [ORDER BY ...] [LIMIT N]}.
 * <p>
 * The clause always orders the window by the designated timestamp, which
 * the fuzz tables generate with a strictly increasing {@code
 * timestamp_sequence}. A total order inside each partition makes every
 * window value (including order-dependent ones such as {@code
 * first_value} / {@code rank} / {@code lag}) reproducible across runs, so
 * the differential oracle can compare the materialized result sets rather
 * than just row counts; floating-point reduction-order drift in {@code
 * avg} / {@code sum} is absorbed by the runner's FP tolerance. The caller
 * routes here only when the source carries a designated timestamp.
 * <p>
 * Function catalogue spans the ranking family ({@code row_number},
 * {@code rank}, {@code dense_rank}, {@code cume_dist}, {@code
 * percent_rank}, {@code ntile}), the windowed aggregates ({@code avg},
 * {@code sum}, {@code min}, {@code max}, {@code count}) and the navigation
 * functions ({@code first_value}, {@code last_value}, {@code lag},
 * {@code lead}, {@code nth_value}). Ranking and lag/lead reject an
 * explicit frame, so a frame clause is only emitted for the aggregates and
 * the value-at-position navigators. The argument kind is drawn from the
 * set each function supports so the typed window variants (LONG, DOUBLE,
 * TIMESTAMP, DECIMAL) all get exercised; unsupported type/frame
 * combinations are rejected by the compiler as {@code SqlException} and
 * swallowed by the oracle.
 * <p>
 * As with the other clauses, every main-{@code rnd} draw is independent of
 * the {@link BindContext}, so the literal and bind passes build the same
 * tree shape; only the constant leaves rendered through {@code ctx} differ.
 */
public final class WindowClause {

    private static final String[] RANGE_UNITS = {"MINUTE", "HOUR"};

    private WindowClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource source, BindContext ctx, boolean injectFaultFn) {
        FuzzTable table = source.getTable();
        boolean useAlias = rnd.nextBoolean();
        String alias = useAlias ? "t0" : null;
        String qualifier = alias;

        ExpressionGenerator exprGen = new ExpressionGenerator(rnd, table.getColumns(), qualifier, 2);
        ObjList<FuzzColumn> partCandidates = partitionCandidates(table);

        StringSink sql = new StringSink();
        sql.put(source.getPrefixSql());
        sql.put("SELECT ");

        // Optional leading bare-column projections so the output carries some
        // context columns next to the window results. Aliased e0..eN-1.
        int leadCols = rnd.nextInt(3);
        int aliasIdx = 0;
        boolean first = true;
        for (int i = 0; i < leadCols && partCandidates.size() > 0; i++) {
            if (!first) {
                sql.put(", ");
            }
            first = false;
            FuzzColumn c = partCandidates.getQuick(rnd.nextInt(partCandidates.size()));
            appendColRef(sql, qualifier, c.getName());
            sql.put(" AS e").put(aliasIdx++);
        }

        int nWin = 1 + rnd.nextInt(2);
        for (int i = 0; i < nWin; i++) {
            if (!first) {
                sql.put(", ");
            }
            first = false;
            WinFn fn = WinFn.VALUES[rnd.nextInt(WinFn.VALUES.length)];
            appendWindowCall(sql, rnd, fn, exprGen, ctx);
            sql.put(" OVER (");
            appendOver(sql, rnd, table, qualifier, partCandidates, fn);
            sql.put(") AS w").put(i);
        }

        sql.put(" FROM ").put(source.getFromSqlWithGarble(rnd));
        if (useAlias) {
            sql.put(' ').put(alias);
        }

        PredicateGenerator.appendWhere(sql, rnd, table.getColumns(), qualifier, 2, ctx, injectFaultFn);

        // Optional outer ORDER BY over the projection aliases (e0..eN-1, w0..wM-1).
        int totalAliases = aliasIdx + nWin;
        if (rnd.nextBoolean()) {
            appendOrderBy(sql, rnd, aliasIdx, nWin, totalAliases);
        }

        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        // Window values are deterministic (total order on ts); only a LIMIT
        // without a fully-disambiguating outer ORDER BY can pick a different
        // valid subset across runs, so it downgrades to a row-count compare.
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    private static void appendColRef(StringSink sink, String qualifier, String name) {
        if (qualifier != null) {
            sink.put(qualifier).put('.');
        }
        sink.put(name);
    }

    private static void appendFrame(StringSink sink, Rnd rnd, boolean orderAsc) {
        // Time-based RANGE is only well-defined over an ascending timestamp
        // order, so it is offered as an extra option only in that case.
        int pick = rnd.nextInt(orderAsc ? 6 : 5);
        switch (pick) {
            case 0 -> sink.put(" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
            case 1 -> sink.put(" ROWS BETWEEN ").put(1 + rnd.nextInt(10)).put(" PRECEDING AND CURRENT ROW");
            case 2 -> sink.put(" ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
            case 3 -> sink.put(" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");
            case 4 -> sink.put(" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
            default -> sink.put(" RANGE BETWEEN ").put(1 + rnd.nextInt(6)).put(' ')
                    .put(RANGE_UNITS[rnd.nextInt(RANGE_UNITS.length)]).put(" PRECEDING AND CURRENT ROW");
        }
    }

    private static void appendOrderBy(StringSink sink, Rnd rnd, int numLeadAliases, int numWin, int totalAliases) {
        int picks = 1 + rnd.nextInt(Math.min(2, totalAliases));
        sink.put(" ORDER BY ");
        for (int i = 0; i < picks; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            int idx = rnd.nextInt(totalAliases);
            if (idx < numLeadAliases) {
                sink.put('e').put(idx);
            } else {
                sink.put('w').put(idx - numLeadAliases);
            }
            if (rnd.nextBoolean()) {
                sink.put(rnd.nextBoolean() ? " ASC" : " DESC");
            }
        }
    }

    private static void appendOver(
            StringSink sink,
            Rnd rnd,
            FuzzTable table,
            String qualifier,
            ObjList<FuzzColumn> partCandidates,
            WinFn fn
    ) {
        int nPart = rnd.nextInt(3);
        if (nPart > 0 && partCandidates.size() > 0) {
            sink.put("PARTITION BY ");
            for (int i = 0; i < nPart; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                FuzzColumn c = partCandidates.getQuick(rnd.nextInt(partCandidates.size()));
                appendColRef(sink, qualifier, c.getName());
            }
            sink.put(' ');
        }
        sink.put("ORDER BY ");
        appendColRef(sink, qualifier, table.getTsColumnName());
        boolean asc = rnd.nextBoolean();
        boolean explicitDir = rnd.nextBoolean();
        if (explicitDir) {
            sink.put(asc ? " ASC" : " DESC");
        }
        boolean orderAsc = !explicitDir || asc;
        if (fn.allowsFrame && rnd.nextInt(100) < 70) {
            appendFrame(sink, rnd, orderAsc);
        }
    }

    private static void appendWindowCall(StringSink sink, Rnd rnd, WinFn fn, ExpressionGenerator exprGen, BindContext ctx) {
        sink.put(fn.name).put('(');
        switch (fn.arg) {
            case NONE -> {
            }
            case BUCKETS -> sink.put(1 + rnd.nextInt(8));
            case STAR_OR_NUMERIC -> {
                if (rnd.nextBoolean()) {
                    sink.put('*');
                } else {
                    exprGen.generateOfKind(pickArgKind(rnd, fn)).appendSql(sink, ctx);
                }
            }
            case NUMERIC -> exprGen.generateOfKind(pickArgKind(rnd, fn)).appendSql(sink, ctx);
            case NUMERIC_OFFSET -> {
                exprGen.generateOfKind(pickArgKind(rnd, fn)).appendSql(sink, ctx);
                // Optional offset, and optional default value when an offset is present.
                if (rnd.nextBoolean()) {
                    sink.put(", ").put(1 + rnd.nextInt(5));
                    if (rnd.nextBoolean()) {
                        sink.put(", ").put(rnd.nextInt(100));
                    }
                }
            }
            case NUMERIC_NTH -> {
                exprGen.generateOfKind(pickArgKind(rnd, fn)).appendSql(sink, ctx);
                sink.put(", ").put(1 + rnd.nextInt(5));
            }
        }
        sink.put(')');
    }

    private static ObjList<FuzzColumn> partitionCandidates(FuzzTable table) {
        // Non-array, non-timestamp columns make meaningful partition keys; the
        // timestamp is the window order key and partitioning by it would make
        // every row its own partition.
        ObjList<FuzzColumn> out = new ObjList<>();
        String tsCol = table.getTsColumnName();
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            FuzzColumn c = table.getColumn(i);
            if (c.getType().getKind() == ColumnKind.ARRAY) {
                continue;
            }
            if (c.getName().equals(tsCol)) {
                continue;
            }
            out.add(c);
        }
        return out;
    }

    private static ColumnKind pickArgKind(Rnd rnd, WinFn fn) {
        // Bias to NUMERIC (the most broadly supported window argument); the
        // navigation/extremum functions also have TIMESTAMP and DECIMAL
        // variants worth exercising.
        int roll = rnd.nextInt(100);
        if (fn.allowsTemporalArg && roll < 15) {
            return ColumnKind.TEMPORAL;
        }
        if (roll < 30) {
            return ColumnKind.DECIMAL;
        }
        return ColumnKind.NUMERIC;
    }

    // Window function catalogue. allowsFrame is false for the ranking family
    // and for lag/lead, which reject an explicit frame clause.
    private enum WinFn {
        ROW_NUMBER("row_number", Arg.NONE, false, false),
        RANK("rank", Arg.NONE, false, false),
        DENSE_RANK("dense_rank", Arg.NONE, false, false),
        CUME_DIST("cume_dist", Arg.NONE, false, false),
        PERCENT_RANK("percent_rank", Arg.NONE, false, false),
        NTILE("ntile", Arg.BUCKETS, false, false),
        AVG("avg", Arg.NUMERIC, true, false),
        SUM("sum", Arg.NUMERIC, true, false),
        MIN("min", Arg.NUMERIC, true, true),
        MAX("max", Arg.NUMERIC, true, true),
        COUNT("count", Arg.STAR_OR_NUMERIC, true, false),
        FIRST_VALUE("first_value", Arg.NUMERIC, true, true),
        LAST_VALUE("last_value", Arg.NUMERIC, true, true),
        LAG("lag", Arg.NUMERIC_OFFSET, false, true),
        LEAD("lead", Arg.NUMERIC_OFFSET, false, true),
        NTH_VALUE("nth_value", Arg.NUMERIC_NTH, true, true);

        static final WinFn[] VALUES = values();
        final boolean allowsFrame;
        final boolean allowsTemporalArg;
        final Arg arg;
        final String name;

        WinFn(String name, Arg arg, boolean allowsFrame, boolean allowsTemporalArg) {
            this.name = name;
            this.arg = arg;
            this.allowsFrame = allowsFrame;
            this.allowsTemporalArg = allowsTemporalArg;
        }
    }

    private enum Arg {
        NONE, BUCKETS, NUMERIC, STAR_OR_NUMERIC, NUMERIC_OFFSET, NUMERIC_NTH
    }
}
