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
import io.questdb.test.griffin.fuzz.FuzzIndex;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ConstantExpr;
import io.questdb.test.griffin.fuzz.expr.FuzzConstant;
import io.questdb.test.griffin.fuzz.types.ColumnKind;
import io.questdb.test.griffin.fuzz.types.SymbolType;

/**
 * Generates the two query shapes that make the planner pick a posting-index
 * read operator, so those paths get exercised rather than merely built:
 * <ul>
 *   <li><b>distinct key enumeration</b> ({@code PostingIndex}):
 *       {@code SELECT DISTINCT <postingSym> FROM t [WHERE <ts interval>]}.
 *       The projection must be exactly one bare reference to a posting-indexed
 *       SYMBOL column, and the WHERE -- if present -- must be timestamp/interval
 *       only; a non-timestamp predicate downgrades it to a full scan.</li>
 *   <li><b>covering read</b> ({@code CoveringIndex}):
 *       {@code SELECT <subset of {postingSym, its INCLUDE cols, ts}> FROM t
 *       WHERE <postingSym> = 'v' | IN (...)}. The projection must stay within
 *       the covered set, and the key predicate must be an equality or IN on the
 *       indexed symbol; anything outside the covered set falls back to a normal
 *       scan.</li>
 * </ul>
 * Both shapes need a direct real table whose schema actually carries a
 * posting-indexed symbol (covering additionally needs a non-empty INCLUDE
 * list). When the primary's sibling shadow holds a different index shape, the
 * storage diff compares the posting/covering access path against bitmap or a
 * full scan over identical data, so any divergence is a real bug.
 */
public final class PostingClause {

    // In-range timestamp literals for the optional interval-only WHERE on the
    // distinct shape; the fuzzer's tables start at 2024-01-01 and span 30..75h.
    private static final String[] TS_LITERALS = {
            "2024-01-01T06:00:00.000000Z", "2024-01-01T12:00:00.000000Z", "2024-01-02"
    };

    private PostingClause() {
    }

    /**
     * Returns a posting-index read query for {@code table}, or {@code null}
     * (without drawing from {@code rnd}) when the table has no posting-indexed
     * symbol column, so the caller's fall-through stays deterministic.
     */
    public static GeneratedQuery tryGenerate(Rnd rnd, FuzzTable table, BindContext ctx) {
        ObjList<FuzzColumn> posting = new ObjList<>();
        ObjList<FuzzColumn> covering = new ObjList<>();
        ObjList<FuzzColumn> columns = table.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            FuzzColumn c = columns.getQuick(i);
            FuzzIndex idx = c.getIndex();
            if (idx != null && idx.isPosting()) {
                posting.add(c);
                if (idx.coveringColumns() != null && idx.coveringColumns().size() > 0) {
                    covering.add(c);
                }
            }
        }
        if (posting.size() == 0) {
            return null;
        }
        if (covering.size() > 0 && rnd.nextBoolean()) {
            return generateCovering(rnd, table, covering.getQuick(rnd.nextInt(covering.size())), ctx);
        }
        return generateDistinct(rnd, table, posting.getQuick(rnd.nextInt(posting.size())));
    }

    private static void appendDistinctSubset(StringSink sql, Rnd rnd, ObjList<String> candidates) {
        int size = candidates.size();
        int count = 1 + rnd.nextInt(size);
        // Partial Fisher-Yates over candidate indices: 'count' distinct picks.
        int[] order = new int[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        for (int i = 0; i < count; i++) {
            int j = i + rnd.nextInt(size - i);
            int tmp = order[i];
            order[i] = order[j];
            order[j] = tmp;
            if (i > 0) {
                sql.put(", ");
            }
            sql.put(candidates.getQuick(order[i]));
        }
    }

    private static void appendSymbolValue(StringSink sql, Rnd rnd, BindContext ctx) {
        // Always a non-null in-domain value so the predicate stays a clean key
        // equality that resolves to an actual stored symbol.
        String v = SymbolType.DOMAIN[rnd.nextInt(SymbolType.DOMAIN.length)];
        new ConstantExpr(ColumnKind.STRING_LIKE, new FuzzConstant("'" + v + "'", "SYMBOL", v)).appendSql(sql, ctx);
    }

    private static GeneratedQuery generateCovering(Rnd rnd, FuzzTable table, FuzzColumn col, BindContext ctx) {
        // The projection must be a subset of {indexed column, covering columns,
        // designated timestamp} for the covering read path to be chosen.
        ObjList<String> candidates = new ObjList<>();
        candidates.add(col.getName());
        ObjList<String> coveringColumns = col.getIndex().coveringColumns();
        for (int i = 0, n = coveringColumns.size(); i < n; i++) {
            candidates.add(coveringColumns.getQuick(i));
        }
        candidates.add(table.getTsColumnName());

        StringSink sql = new StringSink();
        sql.put("SELECT ");
        appendDistinctSubset(sql, rnd, candidates);
        sql.put(" FROM ").put(table.getName());
        sql.put(" WHERE ").put(col.getName());
        if (rnd.nextBoolean()) {
            sql.put(" = ");
            appendSymbolValue(sql, rnd, ctx);
        } else {
            sql.put(" IN (");
            int k = 1 + rnd.nextInt(3);
            for (int i = 0; i < k; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                appendSymbolValue(sql, rnd, ctx);
            }
            sql.put(')');
        }
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(8));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    private static GeneratedQuery generateDistinct(Rnd rnd, FuzzTable table, FuzzColumn col) {
        StringSink sql = new StringSink();
        sql.put("SELECT DISTINCT ").put(col.getName()).put(" FROM ").put(table.getName());
        // Optional interval-only WHERE. A timestamp predicate is pushed into
        // the partition frame and keeps the posting distinct path; a predicate
        // on any other column would turn it into a full scan.
        if (rnd.nextBoolean()) {
            String ts = table.getTsColumnName();
            String lit = TS_LITERALS[rnd.nextInt(TS_LITERALS.length)];
            switch (rnd.nextInt(3)) {
                case 0 -> sql.put(" WHERE ").put(ts).put(" >= '").put(lit).put('\'');
                case 1 -> sql.put(" WHERE ").put(ts).put(" < '").put(lit).put('\'');
                default -> sql.put(" WHERE ").put(ts).put(" in '2024-01-01'");
            }
        }
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(8));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }
}
