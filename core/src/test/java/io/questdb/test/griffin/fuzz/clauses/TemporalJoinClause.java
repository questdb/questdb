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

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.FuzzNames;
import io.questdb.test.griffin.fuzz.FuzzSource;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;

/**
 * ASOF / LT / SPLICE join across two WAL tables. Both tables are aliased
 * (table aliases are mandatory when columns collide, and our factory
 * always produces a shared {@code sym} and {@code ts}). ASOF and LT
 * accept an optional {@code TOLERANCE}; SPLICE does not. Projection is
 * a mix of left-side and right-side expressions so the join path is
 * exercised beyond a single "ts+sym" shape.
 */
public final class TemporalJoinClause {

    private static final String[] JOIN_KINDS = {"ASOF JOIN", "LT JOIN", "SPLICE JOIN"};
    private static final String[] TOLERANCES = {"1s", "30s", "5m", "1h"};

    private TemporalJoinClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource leftSrc, FuzzSource rightSrc) {
        FuzzTable left = leftSrc.getTable();
        FuzzTable right = rightSrc.getTable();
        String joinKind = JOIN_KINDS[rnd.nextInt(JOIN_KINDS.length)];
        boolean splice = "SPLICE JOIN".equals(joinKind);

        String leftAlias = "a";
        String rightAlias = "b";

        ExpressionGenerator leftGen = new ExpressionGenerator(rnd, left.getColumns(), leftAlias, 2);
        ExpressionGenerator rightGen = new ExpressionGenerator(rnd, right.getColumns(), rightAlias, 2);

        StringSink sql = new StringSink();
        sql.put(leftSrc.getPrefixSql());
        sql.put(rightSrc.getPrefixSql());
        sql.put("SELECT ");
        int n = 2 + rnd.nextInt(4);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            FuzzExpr e = rnd.nextBoolean() ? leftGen.generateAnyKind() : rightGen.generateAnyKind();
            e.appendSql(sql);
            sql.put(" AS e").put(i);
        }

        sql.put(" FROM ").put(leftSrc.getFromSqlWithGarble(rnd)).put(' ').put(leftAlias);
        sql.put(' ').put(joinKind).put(' ').put(rightSrc.getFromSqlWithGarble(rnd)).put(' ').put(rightAlias);

        // ON (col) is optional on ASOF/LT; SPLICE takes no ON clause.
        if (!splice && rnd.nextBoolean()) {
            sql.put(" ON (").put(FuzzNames.column(rnd, "sym")).put(')');
        }

        // TOLERANCE applies to ASOF/LT only.
        if (!splice && rnd.nextInt(3) == 0) {
            sql.put(" TOLERANCE ").put(TOLERANCES[rnd.nextInt(TOLERANCES.length)]);
        }

        if (rnd.nextBoolean()) {
            // WHERE can touch either side; use left.
            String pred = new PredicateGenerator(rnd, 1).generate(left.getColumns(), leftAlias);
            sql.put(" WHERE ").put(pred);
        }

        if (rnd.nextBoolean()) {
            int picks = 1 + rnd.nextInt(Math.min(2, n));
            sql.put(" ORDER BY ");
            for (int i = 0; i < picks; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                int idx = rnd.nextInt(n);
                if (rnd.nextBoolean()) {
                    sql.put('e').put(idx);
                } else {
                    sql.put(idx + 1);
                }
                if (rnd.nextBoolean()) {
                    sql.put(rnd.nextBoolean() ? " ASC" : " DESC");
                }
            }
        }

        // LIMIT over a parallel ASOF / LT / SPLICE join can pick a different
        // valid subset across runs. Emit it half the time and tell the runner
        // which queries are safe to compare row-for-row.
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }
}
