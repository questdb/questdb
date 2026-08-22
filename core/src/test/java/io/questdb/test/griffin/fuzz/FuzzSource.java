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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.types.ColumnKind;
import io.questdb.test.griffin.fuzz.types.LongType;

/**
 * A FROM-clause source: what the clause plugs in after {@code FROM }.
 * Covers real tables, virtual tables (via {@code long_sequence()}) and
 * two wrappers on either of those &mdash; inline subqueries and CTEs.
 * Wrappers are not nested (no CTE-over-CTE); the factories below are the
 * full grammar.
 * <p>
 * Inner queries inside subqueries and CTEs take one of four shapes,
 * drawn at random:
 * <ul>
 *   <li>{@code SELECT * FROM base} (default);</li>
 *   <li>{@code SELECT * FROM base WHERE p [LIMIT N]} (filtered);</li>
 *   <li>{@code SELECT col AS r0, col AS r1, ts AS rts FROM base}
 *       (renaming projection &mdash; outer sees the aliases);</li>
 *   <li>{@code SELECT keyCol AS k, count() AS cnt FROM base}
 *       (aggregated &mdash; outer sees a small 2-column schema).</li>
 * </ul>
 * The logical {@link FuzzTable} held by the source reflects the inner's
 * output schema so outer clauses can project, group by and compare
 * against it using the right names and kinds.
 * <p>
 * To keep the grammar bounded, a subquery or CTE always takes a
 * {@code direct} or {@code longSequence} base; no wrap-of-wrap.
 */
public final class FuzzSource {
    private final String fromSql;
    private final boolean isBareIdentifier;
    private final boolean orderStable;
    private final String prefixSql;
    private final FuzzTable table;

    private FuzzSource(String prefixSql, String fromSql, FuzzTable table, boolean isBareIdentifier, boolean orderStable) {
        this.prefixSql = prefixSql;
        this.fromSql = fromSql;
        this.table = table;
        this.isBareIdentifier = isBareIdentifier;
        this.orderStable = orderStable;
    }

    /**
     * CTE that wraps the given base source. The CTE name is a bare
     * identifier and may be garbled, so occasionally the outer query
     * references a non-existent CTE.
     */
    public static FuzzSource cte(Rnd rnd, FuzzSource base, int cteIndex) {
        InnerQuery inner = buildInnerQuery(rnd, base);
        String cteName = "cte" + cteIndex;
        String prefix = "WITH " + cteName + " AS (" + inner.sql + ") ";
        FuzzTable cteTable = new FuzzTable(cteName, inner.table.getColumns(), inner.table.getTsColumnName());
        return new FuzzSource(prefix, cteName, cteTable, true, inner.orderStable);
    }

    /**
     * Plain reference to a real table. The name may be garbled by the
     * clause to exercise "table does not exist" paths.
     */
    public static FuzzSource direct(FuzzTable table) {
        return new FuzzSource("", table.getName(), table, true, true);
    }

    /**
     * Virtual table backed by {@code long_sequence(N)} &mdash; a single
     * LONG column named {@code x} and no designated timestamp. Suitable
     * for SIMPLE queries and as the base of a subquery or CTE; not for
     * SAMPLE BY / ASOF JOIN (no ts).
     */
    public static FuzzSource longSequence(Rnd rnd) {
        int len = 10 + rnd.nextInt(91); // 10..100 rows
        ObjList<FuzzColumn> cols = new ObjList<>();
        cols.add(new FuzzColumn("x", LongType.INSTANCE));
        FuzzTable virt = new FuzzTable("ls", cols, null);
        return new FuzzSource("", "long_sequence(" + len + ")", virt, false, true);
    }

    /**
     * Inline subquery wrapping the given base. The clause must append
     * an alias after it; SQL grammar typically requires one for a
     * subquery in FROM, and the clause already offers an alias knob.
     */
    public static FuzzSource subquery(Rnd rnd, FuzzSource base) {
        InnerQuery inner = buildInnerQuery(rnd, base);
        return new FuzzSource("", "(" + inner.sql + ")", inner.table, false, inner.orderStable);
    }

    public String getFromSql() {
        return fromSql;
    }

    /**
     * Same as {@link #getFromSql} but garbles the table/CTE name with
     * the usual {@link FuzzNames#table} probability when {@code fromSql}
     * is a bare identifier. Expression-bodied sources (long_sequence,
     * subquery) are returned unchanged because substituting their body
     * would be meaningless.
     */
    public String getFromSqlWithGarble(Rnd rnd) {
        if (!isBareIdentifier) {
            return fromSql;
        }
        return FuzzNames.table(rnd, fromSql);
    }

    public String getPrefixSql() {
        return prefixSql;
    }

    public FuzzTable getTable() {
        return table;
    }

    /**
     * Returns true when iterating the source emits rows in a deterministic
     * order across runs. Real tables (timestamp-ordered) and {@code
     * long_sequence()} are stable; an aggregated CTE/subquery iterates a
     * hash map, so {@code first}/{@code last} over it is undefined.
     */
    public boolean isOrderStable() {
        return orderStable;
    }

    private static InnerQuery aggregated(Rnd rnd, FuzzSource base) {
        ObjList<FuzzColumn> baseCols = base.getTable().getColumns();
        // Search for a groupable (non-array) column; retry a few times so
        // we don't miss one behind an unlucky first draw.
        FuzzColumn key = null;
        for (int i = 0; i < baseCols.size() * 2 && key == null; i++) {
            FuzzColumn c = baseCols.getQuick(rnd.nextInt(baseCols.size()));
            ColumnKind kind = c.getType().getKind();
            if (kind.isGroupable() && kind != ColumnKind.ARRAY) {
                key = c;
            }
        }
        if (key == null) {
            return star(base);
        }
        String sql = "SELECT " + key.getName() + " AS k, count() AS cnt FROM " + base.getFromSql();
        ObjList<FuzzColumn> cols = new ObjList<>();
        cols.add(new FuzzColumn("k", key.getType()));
        cols.add(new FuzzColumn("cnt", LongType.INSTANCE));
        // No ts: aggregated output doesn't carry the bucket timestamp.
        FuzzTable newTable = new FuzzTable(base.getTable().getName(), cols, null);
        // GROUP BY iterates a hash map, so the row order is implementation-defined.
        return new InnerQuery(sql, newTable, false);
    }

    // Draw an inner-query shape. Distribution is tuned so the simple
    // star shape still dominates but filtered, renaming and aggregated
    // inner queries each appear a measurable fraction of the time.
    private static InnerQuery buildInnerQuery(Rnd rnd, FuzzSource base) {
        int shape = rnd.nextInt(10);
        if (shape < 2) {
            return aggregated(rnd, base);
        }
        if (shape < 4) {
            return projected(rnd, base);
        }
        if (shape < 6) {
            return filtered(rnd, base);
        }
        return star(base);
    }

    private static InnerQuery filtered(Rnd rnd, FuzzSource base) {
        // Use a cheap built-in predicate; going through PredicateGenerator
        // from here would introduce a circular dependency on the clauses.
        ObjList<FuzzColumn> cols = base.getTable().getColumns();
        FuzzColumn col = cols.getQuick(rnd.nextInt(cols.size()));
        StringSink sb = new StringSink();
        sb.put("SELECT * FROM ").put(base.getFromSql());
        sb.put(" WHERE ").put(col.getName()).put(rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
        if (rnd.nextBoolean()) {
            sb.put(" LIMIT ").put(10 + rnd.nextInt(91));
        }
        return new InnerQuery(sb.toString(), base.getTable(), base.isOrderStable());
    }

    private static InnerQuery projected(Rnd rnd, FuzzSource base) {
        ObjList<FuzzColumn> baseCols = base.getTable().getColumns();
        String origTsColName = base.getTable().getTsColumnName();

        ObjList<FuzzColumn> renamedCols = new ObjList<>();
        StringSink sb = new StringSink();
        sb.put("SELECT ");
        String newTsColName = null;
        int aliasIdx = 0;
        boolean first = true;
        for (int i = 0, n = baseCols.size(); i < n; i++) {
            FuzzColumn c = baseCols.getQuick(i);
            boolean isTs = c.getName().equals(origTsColName);
            // Drop non-ts columns with 1/3 probability; the ts column is
            // always kept so outer SAMPLE BY stays possible.
            if (!isTs && rnd.nextInt(3) == 0) {
                continue;
            }
            if (!first) {
                sb.put(", ");
            }
            first = false;
            String alias = "r" + (aliasIdx++);
            sb.put(c.getName()).put(" AS ").put(alias);
            renamedCols.add(new FuzzColumn(alias, c.getType()));
            if (isTs) {
                newTsColName = alias;
            }
        }
        if (first) {
            // Shouldn't happen (ts is always kept) but fall back safely.
            return star(base);
        }
        sb.put(" FROM ").put(base.getFromSql());
        FuzzTable newTable = new FuzzTable(base.getTable().getName(), renamedCols, newTsColName);
        return new InnerQuery(sb.toString(), newTable, base.isOrderStable());
    }

    private static InnerQuery star(FuzzSource base) {
        return new InnerQuery("SELECT * FROM " + base.getFromSql(), base.getTable(), base.isOrderStable());
    }

    private record InnerQuery(String sql, FuzzTable table, boolean orderStable) {
    }
}
