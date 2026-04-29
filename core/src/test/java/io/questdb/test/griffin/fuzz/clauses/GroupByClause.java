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
import io.questdb.test.griffin.fuzz.FuzzSource;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * {@code SELECT keyExpr [, keyExpr2], aggExpr FROM t [WHERE p]
 *   [GROUP BY ...] [LIMIT N]}.
 * <p>
 * Key slots are {@link FuzzExpr}s of a groupable kind; the aggregate
 * operates on an expression of a kind compatible with the chosen
 * aggregate (numeric for sum/avg, any-non-array for count/first/last/
 * min/max/count_distinct/approx_count_distinct). An explicit
 * {@code GROUP BY} clause is emitted on roughly half the queries and
 * either references projection aliases or re-emits the expression,
 * which exercises different parser paths.
 */
public final class GroupByClause {

    private GroupByClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource source) {
        FuzzTable table = source.getTable();
        boolean useAlias = rnd.nextBoolean();
        String alias = useAlias ? "t0" : null;
        String qualifier = alias;

        ExpressionGenerator exprGen = new ExpressionGenerator(rnd, table.getColumns(), qualifier, 2);

        // Build the list of key expressions and one aggregate.
        ObjList<FuzzExpr> keys = new ObjList<>();
        if (rnd.nextBoolean()) {
            int nKeys = 1 + rnd.nextInt(2);
            for (int i = 0; i < nKeys; i++) {
                keys.add(exprGen.generateOfKind(pickGroupableKind(rnd)));
            }
        }
        Aggregate agg = pickAggregate(rnd, exprGen);

        StringSink sql = new StringSink();
        sql.put(source.getPrefixSql());
        sql.put("SELECT ");
        for (int i = 0, n = keys.size(); i < n; i++) {
            keys.getQuick(i).appendSql(sql);
            sql.put(" AS e").put(i);
            sql.put(", ");
        }
        agg.appendSql(sql);
        sql.put(" AS a0");

        sql.put(" FROM ").put(source.getFromSqlWithGarble(rnd));
        if (useAlias) {
            sql.put(' ').put(alias);
        }

        if (rnd.nextBoolean()) {
            String pred = new PredicateGenerator(rnd, 2).generate(table.getColumns(), qualifier);
            sql.put(" WHERE ").put(pred);
        }

        // Explicit GROUP BY for roughly half of the queries. Each key can
        // be written as the projection alias, the re-emitted expression,
        // or a 1-based positional index, so all three grammar paths get
        // exercised.
        if (keys.size() > 0 && rnd.nextBoolean()) {
            sql.put(" GROUP BY ");
            for (int i = 0, n = keys.size(); i < n; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                int mode = rnd.nextInt(3);
                if (mode == 0) {
                    sql.put('e').put(i);
                } else if (mode == 1) {
                    // positional: keys occupy positions 1..n; aggregate is n+1.
                    sql.put(i + 1);
                } else {
                    keys.getQuick(i).appendSql(sql);
                }
            }
        }

        // ORDER BY over the aggregate result set. Every projection slot
        // (keys and aggregate) has a stable alias, so we just pick from
        // {e0..eN-1, a0} and attach ASC/DESC.
        if (rnd.nextBoolean()) {
            appendOrderBy(sql, rnd, keys.size());
        }

        // LIMIT over a parallel GROUP BY can pick a different valid subset on
        // each run when ORDER BY does not fully disambiguate; emit it half the
        // time and tag the query so the runner can choose the right oracle.
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        // first/last over a hash-grouped CTE/subquery is undefined: the inner
        // iterates a hash map, so which row counts as "first" can change
        // between runs and across JIT modes.
        boolean deterministic = !hasLimit && (!agg.isOrderDependent() || source.isOrderStable());
        return new GeneratedQuery(sql.toString(), deterministic);
    }

    private static void appendOrderBy(StringSink sink, Rnd rnd, int numKeys) {
        int total = numKeys + 1; // eN keys + a0
        int picks = 1 + rnd.nextInt(Math.min(2, total));
        sink.put(" ORDER BY ");
        for (int i = 0; i < picks; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            int idx = rnd.nextInt(total);
            // ORDER BY can name the alias or use a 1-based position.
            if (rnd.nextBoolean()) {
                sink.put(idx + 1);
            } else if (idx < numKeys) {
                sink.put('e').put(idx);
            } else {
                sink.put("a0");
            }
            if (rnd.nextBoolean()) {
                sink.put(rnd.nextBoolean() ? " ASC" : " DESC");
            }
        }
    }

    private static Aggregate pickAggregate(Rnd rnd, ExpressionGenerator gen) {
        int pick = rnd.nextInt(7);
        switch (pick) {
            case 0:
                return Aggregate.noArg("count");
            case 1:
                return Aggregate.withArg("count", gen.generateAnyKind());
            case 2:
                return Aggregate.withArg(rnd.nextBoolean() ? "sum" : "avg",
                        gen.generateOfKind(ColumnKind.NUMERIC));
            case 3:
                return Aggregate.withArg(rnd.nextBoolean() ? "min" : "max", gen.generateAnyKind());
            case 4:
                return Aggregate.withArg(rnd.nextBoolean() ? "first" : "last", gen.generateAnyKind());
            case 5:
                return Aggregate.withArg("count_distinct", gen.generateAnyKind());
            default:
                return Aggregate.withArg("approx_count_distinct", gen.generateAnyKind());
        }
    }

    private static ColumnKind pickGroupableKind(Rnd rnd) {
        // Everything except ARRAY is groupable; bias towards the common
        // keys seen in real workloads.
        ColumnKind[] options = {
                ColumnKind.STRING_LIKE, ColumnKind.NUMERIC, ColumnKind.TEMPORAL,
                ColumnKind.BOOLEAN, ColumnKind.CHAR, ColumnKind.IDENTIFIER,
                ColumnKind.DECIMAL
        };
        return options[rnd.nextInt(options.length)];
    }

    private static final class Aggregate {
        private final FuzzExpr arg;
        private final String name;

        private Aggregate(String name, FuzzExpr arg) {
            this.name = name;
            this.arg = arg;
        }

        static Aggregate noArg(String name) {
            return new Aggregate(name, null);
        }

        static Aggregate withArg(String name, FuzzExpr arg) {
            return new Aggregate(name, arg);
        }

        void appendSql(StringSink sink) {
            sink.put(name).put('(');
            if (arg != null) {
                arg.appendSql(sink);
            }
            sink.put(')');
        }

        boolean isOrderDependent() {
            return "first".equals(name) || "last".equals(name);
        }
    }
}
