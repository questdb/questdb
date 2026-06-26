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
import io.questdb.test.griffin.fuzz.FuzzNames;
import io.questdb.test.griffin.fuzz.FuzzSource;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * {@code SELECT [* | expr AS e0, ...] FROM t [WHERE p]
 * LATEST ON ts PARTITION BY col[, col]* [ORDER BY ...] [LIMIT N]}.
 * <p>
 * For every distinct combination of the {@code PARTITION BY} keys the query
 * returns the single row with the greatest designated-timestamp value among
 * the rows the {@code WHERE} keeps. The fuzz tables generate strictly
 * increasing, unique timestamps, so the latest row in each partition is
 * uniquely determined and the result is a stable multiset regardless of the
 * access path: the differential oracle can compare the materialized result
 * sets row for row across JIT-on/off and across the indexed-symbol vs
 * full-scan storage shadow (only a {@code LIMIT} without a fully
 * disambiguating {@code ORDER BY} downgrades to a row-count compare).
 * <p>
 * {@code LATEST ON} always names the source's designated timestamp -- the
 * engine rejects any other column with "latest by over a table requires
 * designated TIMESTAMP" -- so the caller routes here only when the source
 * carries one. The clause skips the table alias (like {@link SampleByClause})
 * so the timestamp literal and the bare key list stay unqualified. Partition
 * keys are drawn from the non-array columns other than the timestamp, biased
 * to none in particular so every key type (SYMBOL, STRING, the numeric and
 * temporal families, BOOLEAN, CHAR, the identifier family, DECIMAL) gets
 * exercised; when the table happens to expose no such column the timestamp
 * itself is used, which is a valid if degenerate single-row-per-partition key.
 * The {@code WHERE} is woven the usual way, so the FUNCTION fault rides it; the
 * latest-by cursors filter serially (no parallel reduce), so a fired fault
 * always surfaces rather than being discarded on a worker.
 * <p>
 * As with the other clauses, every main-{@code rnd} draw is independent of the
 * {@link BindContext}, so the literal and bind passes build the same tree
 * shape; only the constant leaves rendered through {@code ctx} differ.
 */
public final class LatestOnClause {

    private LatestOnClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource source, BindContext ctx, boolean injectFaultFn) {
        FuzzTable table = source.getTable();
        boolean useColAliases = rnd.nextBoolean();

        StringSink sql = new StringSink();
        sql.put(source.getPrefixSql());
        sql.put("SELECT ");
        int numAliases = appendProjection(sql, rnd, table, useColAliases, ctx);
        sql.put(" FROM ").put(source.getFromSqlWithGarble(rnd));

        PredicateGenerator.appendWhere(sql, rnd, table.getColumns(), null, 2, ctx, injectFaultFn);

        sql.put(" LATEST ON ").put(FuzzNames.column(rnd, table.getTsColumnName()));
        appendPartitionBy(sql, rnd, table);

        if (rnd.nextBoolean()) {
            appendOrderBy(sql, rnd, table, numAliases);
        }

        // LATEST ON yields a stable multiset, so only a LIMIT without a fully
        // disambiguating ORDER BY can pick a different valid subset across runs;
        // emit it half the time and downgrade those to a row-count compare.
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    private static void appendOrderBy(StringSink sink, Rnd rnd, FuzzTable table, int numAliases) {
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
        // No projection aliases (SELECT * or aliases off): order by the
        // designated timestamp, the one column guaranteed to be in the output.
        sink.put(FuzzNames.column(rnd, table.getTsColumnName()));
        sink.put(rnd.nextBoolean() ? " ASC" : " DESC");
    }

    private static void appendPartitionBy(StringSink sink, Rnd rnd, FuzzTable table) {
        ObjList<FuzzColumn> candidates = partitionCandidates(table);
        sink.put(" PARTITION BY ");
        if (candidates.size() == 0) {
            // Table exposes only the designated timestamp and arrays; the
            // timestamp is itself a valid (every-row-its-own-partition) key.
            sink.put(FuzzNames.column(rnd, table.getTsColumnName()));
            return;
        }
        int size = candidates.size();
        int nKeys = 1 + rnd.nextInt(Math.min(3, size));
        // Partial Fisher-Yates over the candidate indices so the keys are
        // distinct: never repeat a column in one PARTITION BY list.
        int[] order = new int[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        for (int i = 0; i < nKeys; i++) {
            int j = i + rnd.nextInt(size - i);
            int tmp = order[i];
            order[i] = order[j];
            order[j] = tmp;
            if (i > 0) {
                sink.put(", ");
            }
            sink.put(FuzzNames.column(rnd, candidates.getQuick(order[i]).getName()));
        }
    }

    /**
     * @return the number of {@code eN} aliases emitted into the projection
     * (0 when {@code SELECT *} or column aliases are off).
     */
    private static int appendProjection(StringSink sink, Rnd rnd, FuzzTable table, boolean useColAliases, BindContext ctx) {
        if (rnd.nextInt(5) == 0) {
            sink.put('*');
            return 0;
        }
        ExpressionGenerator gen = new ExpressionGenerator(rnd, table.getColumns(), null, 2);
        int n = 1 + rnd.nextInt(4);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            gen.generateAnyKind().appendSql(sink, ctx);
            if (useColAliases) {
                sink.put(" AS e").put(i);
            }
        }
        return useColAliases ? n : 0;
    }

    private static ObjList<FuzzColumn> partitionCandidates(FuzzTable table) {
        // Any non-array column other than the designated timestamp is a
        // meaningful partition key; arrays are not comparable as keys and the
        // timestamp would make every row its own partition (kept only as the
        // last-resort fallback in appendPartitionBy).
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
}
