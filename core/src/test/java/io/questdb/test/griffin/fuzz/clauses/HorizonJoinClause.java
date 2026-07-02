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
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * HORIZON JOIN across two WAL tables. A HORIZON JOIN runs as a keyed GROUP
 * BY: for each offset drawn from the {@code RANGE FROM..TO..STEP} or
 * {@code LIST (...)} horizon spec it ASOF-matches the slave at
 * {@code master.ts + offset} and aggregates the matched slave (and/or
 * master) rows, grouped by the projected non-aggregate columns.
 * <p>
 * Shape:
 * <pre>
 * SELECT [h.offset AS e0,] [t.sym AS e1,] agg(p.col) AS a0 [, agg(...) AS a1 ...]
 * FROM master t
 * HORIZON JOIN slave p [ON (t.sym = p.sym)]
 * (RANGE FROM f TO t STEP s | LIST (o0, o1, ...)) AS h
 * [WHERE master-only-predicate]
 * [GROUP BY e0[, e1]]
 * [ORDER BY ...]
 * [LIMIT N]
 * </pre>
 * The horizon offset and the join-key master column are the only grouping
 * keys the generator emits, both proven valid by {@code HorizonJoinTest}; a
 * projection with neither is a non-keyed single-row aggregate. At least one
 * aggregate is always emitted and the first is always over the slave so the
 * join actually contributes. The {@code RANGE}/{@code LIST} spec is bounded
 * to at most six offsets (RANGE; LIST emits two to four), each offset being a
 * separate ASOF pass.
 * <p>
 * The WHERE references master columns only ({@code "WHERE clause of HORIZON
 * JOIN can only reference left-hand side columns"}). The two tables share a
 * {@code sym SYMBOL} and {@code ts TIMESTAMP} by construction, so the keyed
 * {@code ON (t.sym = p.sym)} form is always emittable; an indexed {@code sym}
 * routes the join through an index-driven slave scan, which the shadow
 * differential exercises against the non-indexed sibling.
 */
public final class HorizonJoinClause {

    // Interval-literal unit chars for the horizon offsets, biased to minutes
    // since the fuzz tables step rows 30 minutes apart over a few DAY partitions.
    private static final char[] HORIZON_UNITS = {'m', 'm', 'h', 's'};
    private static final String MASTER_ALIAS = "t";
    private static final String SLAVE_ALIAS = "p";

    private HorizonJoinClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzTable master, FuzzTable slave, BindContext ctx, boolean injectFaultFn) {
        // The shared sym lets us always offer a keyed join; ~1/3 of queries go
        // non-keyed (no ON) to exercise the cross-matching path.
        boolean keyed = rnd.nextInt(3) != 0;
        boolean offsetKey = rnd.nextInt(3) != 0;
        boolean symKey = keyed && rnd.nextBoolean();
        boolean useList = rnd.nextBoolean();

        // Build the projection: optional grouping keys, then 1..3 aggregates.
        StringSink sql = new StringSink();
        sql.put("SELECT ");
        int keyCount = 0;
        if (offsetKey) {
            if (rnd.nextBoolean()) {
                sql.put("h.offset");
            } else {
                sql.put("(h.offset / 1000000)");
            }
            sql.put(" AS e").put(keyCount++);
            sql.put(", ");
        }
        if (symKey) {
            sql.put(MASTER_ALIAS).put(".sym AS e").put(keyCount++);
            sql.put(", ");
        }
        int aggCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < aggCount; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            // The first aggregate is always over the slave so the join
            // contributes; later ones may aggregate the master instead.
            boolean overSlave = i == 0 || rnd.nextInt(3) != 0;
            appendAggregate(sql, rnd, overSlave ? slave : master, overSlave ? SLAVE_ALIAS : MASTER_ALIAS);
            sql.put(" AS a").put(i);
        }

        sql.put(" FROM ").put(master.getName()).put(' ').put(MASTER_ALIAS);
        sql.put(" HORIZON JOIN ").put(slave.getName()).put(' ').put(SLAVE_ALIAS);
        if (keyed) {
            sql.put(" ON (").put(MASTER_ALIAS).put(".sym = ").put(SLAVE_ALIAS).put(".sym)");
        }
        appendHorizonSpec(sql, rnd, useList);

        // WHERE may reference master columns only.
        PredicateGenerator.appendWhere(sql, rnd, master.getColumns(), MASTER_ALIAS, 1, ctx, injectFaultFn);

        // Explicit GROUP BY over the key aliases on roughly half the keyed
        // queries; the rest rely on implicit grouping from the projection.
        if (keyCount > 0 && rnd.nextBoolean()) {
            sql.put(" GROUP BY ");
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                sql.put('e').put(i);
            }
        }

        if (rnd.nextBoolean()) {
            appendOrderBy(sql, rnd, keyCount, aggCount);
        }

        // LIMIT over the keyed GROUP BY can pick a different valid subset when
        // ORDER BY does not fully disambiguate; tag the query so the runner
        // compares row counts rather than content.
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    private static void appendAggregate(StringSink sql, Rnd rnd, FuzzTable table, String alias) {
        int pick = rnd.nextInt(7);
        switch (pick) {
            case 0 -> sql.put("count(*)");
            case 1 -> {
                String c = pickColumn(rnd, table, null);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put("count(").put(alias).put('.').put(c).put(')');
                }
            }
            case 2, 3 -> {
                String c = pickColumn(rnd, table, ColumnKind.NUMERIC);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put(rnd.nextBoolean() ? "sum(" : "avg(").put(alias).put('.').put(c).put(')');
                }
            }
            case 4 -> {
                String c = pickOrderableColumn(rnd, table);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put(rnd.nextBoolean() ? "min(" : "max(").put(alias).put('.').put(c).put(')');
                }
            }
            default -> {
                String c = pickColumn(rnd, table, null);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put(rnd.nextBoolean() ? "first(" : "last(").put(alias).put('.').put(c).put(')');
                }
            }
        }
    }

    private static void appendHorizonSpec(StringSink sql, Rnd rnd, boolean useList) {
        char unit = HORIZON_UNITS[rnd.nextInt(HORIZON_UNITS.length)];
        if (useList) {
            int n = 2 + rnd.nextInt(3); // 2..4 offsets
            int step = 1 + rnd.nextInt(3);
            int center = rnd.nextInt(n);
            // A single step keeps the offsets strictly increasing and distinct
            // while straddling zero, so each one ASOF-matches a different row.
            sql.put(" LIST (");
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                sql.put((i - center) * step).put(unit);
            }
            sql.put(") AS h");
        } else {
            int step = 1 + rnd.nextInt(3);
            int before = rnd.nextInt(3); // 0..2 steps before zero
            int after = 1 + rnd.nextInt(3); // 1..3 steps after zero (zero + before + after = up to 6 offsets)
            sql.put(" RANGE FROM ").put(-step * before).put(unit)
                    .put(" TO ").put(step * after).put(unit)
                    .put(" STEP ").put(step).put(unit).put(" AS h");
        }
    }

    private static void appendOrderBy(StringSink sql, Rnd rnd, int keyCount, int aggCount) {
        int total = keyCount + aggCount;
        int picks = 1 + rnd.nextInt(Math.min(2, total));
        sql.put(" ORDER BY ");
        for (int i = 0; i < picks; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            int idx = rnd.nextInt(total);
            if (idx < keyCount) {
                sql.put('e').put(idx);
            } else {
                sql.put('a').put(idx - keyCount);
            }
            if (rnd.nextBoolean()) {
                sql.put(rnd.nextBoolean() ? " ASC" : " DESC");
            }
        }
    }

    private static String pickColumn(Rnd rnd, FuzzTable table, ColumnKind kind) {
        ObjList<String> matching = new ObjList<>();
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            FuzzColumn c = table.getColumn(i);
            ColumnKind k = c.getType().getKind();
            // ARRAY columns do not aggregate cleanly; exclude them everywhere.
            if (k == ColumnKind.ARRAY) {
                continue;
            }
            if (kind == null || k == kind) {
                matching.add(c.getName());
            }
        }
        if (matching.size() == 0) {
            return null;
        }
        return matching.getQuick(rnd.nextInt(matching.size()));
    }

    private static String pickOrderableColumn(Rnd rnd, FuzzTable table) {
        ObjList<String> matching = new ObjList<>();
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            FuzzColumn c = table.getColumn(i);
            if (c.getType().getKind().isOrderable()) {
                matching.add(c.getName());
            }
        }
        if (matching.size() == 0) {
            return null;
        }
        return matching.getQuick(rnd.nextInt(matching.size()));
    }
}
