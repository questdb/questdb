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
 * Single-table {@code SELECT expr[, expr]* FROM t [WHERE p]
 *   [ORDER BY key[, key]* [ASC|DESC]] [LIMIT N]}. Projection entries are
 * typed expressions produced by {@link ExpressionGenerator}. A
 * {@code SELECT *} shape is still emitted occasionally for wildcard
 * coverage. ORDER BY prefers projection aliases when they exist; when
 * aliases are off (or {@code SELECT *} was chosen), it falls back to
 * the designated timestamp column &mdash; or is skipped if the source
 * has none (virtual tables).
 */
public final class SimpleClause {

    private SimpleClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource source) {
        FuzzTable table = source.getTable();
        boolean useTableAlias = rnd.nextBoolean();
        boolean useColAliases = rnd.nextBoolean();
        String alias = useTableAlias ? "t0" : null;
        String qualifier = useTableAlias ? alias : null;

        StringSink sql = new StringSink();
        sql.put(source.getPrefixSql());
        sql.put("SELECT ");
        int numAliases = appendProjection(sql, rnd, table, qualifier, useColAliases);
        sql.put(" FROM ").put(source.getFromSqlWithGarble(rnd));
        if (useTableAlias) {
            sql.put(' ').put(alias);
        }

        if (rnd.nextBoolean()) {
            String pred = new PredicateGenerator(rnd, 2).generate(table.getColumns(), qualifier);
            sql.put(" WHERE ").put(pred);
        }

        if (rnd.nextBoolean()) {
            appendOrderBy(sql, rnd, table, qualifier, numAliases);
        }

        // LIMIT without a fully-disambiguating ORDER BY can pick a different
        // subset on each run when the source has parallel non-determinism. We
        // emit it half the time and let the runner know which queries are
        // safe to compare row-for-row vs. row-count-only.
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    private static void appendOrderBy(
            StringSink sink,
            Rnd rnd,
            FuzzTable table,
            String qualifier,
            int numAliases
    ) {
        if (numAliases == 0 && table.getTsColumnName() == null) {
            return;
        }
        sink.put(" ORDER BY ");
        if (numAliases > 0) {
            int picks = 1 + rnd.nextInt(Math.min(2, numAliases));
            for (int i = 0; i < picks; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                int idx = rnd.nextInt(numAliases);
                // alias-style "eN" or 1-based positional reference.
                if (rnd.nextBoolean()) {
                    sink.put('e').put(idx);
                } else {
                    sink.put(idx + 1);
                }
                if (rnd.nextBoolean()) {
                    sink.put(rnd.nextBoolean() ? " ASC" : " DESC");
                }
            }
            return;
        }
        // Fallback: ORDER BY ts, the one guaranteed-sortable column.
        if (qualifier != null) {
            sink.put(qualifier).put('.');
        }
        sink.put(FuzzNames.column(rnd, table.getTsColumnName()));
        sink.put(rnd.nextBoolean() ? " ASC" : " DESC");
    }

    /**
     * @return the number of {@code eN} aliases emitted into the projection
     * (0 when {@code SELECT *} or column aliases are off).
     */
    private static int appendProjection(
            StringSink sink,
            Rnd rnd,
            FuzzTable table,
            String qualifier,
            boolean useColAliases
    ) {
        if (rnd.nextInt(5) == 0) {
            if (qualifier != null) {
                sink.put(qualifier).put(".*");
            } else {
                sink.put('*');
            }
            return 0;
        }

        ExpressionGenerator gen = new ExpressionGenerator(rnd, table.getColumns(), qualifier, 2);
        int n = 1 + rnd.nextInt(5);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            FuzzExpr e = gen.generateAnyKind();
            e.appendSql(sink);
            if (useColAliases) {
                sink.put(" AS e").put(i);
            }
        }
        return useColAliases ? n : 0;
    }
}
