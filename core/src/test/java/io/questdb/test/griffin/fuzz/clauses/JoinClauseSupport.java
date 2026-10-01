/*******************************************************************************
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
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * Aggregate / order-by / column-picking helpers shared by {@link HorizonJoinClause}
 * and {@link WindowJoinClause}. Both emit an alias-qualified aggregate over a
 * projected table ({@code agg(alias.col)}) and an ORDER BY over the same
 * {@code e<n>} / {@code a<n>} projection aliases, so the generation logic is
 * identical bar the table and alias each clause feeds in.
 */
final class JoinClauseSupport {

    private JoinClauseSupport() {
    }

    // Emits one aggregate over {@code table}, qualified by {@code alias}: count(*),
    // count(col), sum/avg(numeric), min/max(orderable), or first/last(col). Falls
    // back to count(*) when the table has no column of the required kind.
    static void appendAggregate(StringSink sql, Rnd rnd, FuzzTable table, String alias) {
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
                // Bias to NUMERIC but exercise DECIMAL on a minority of draws, the same split
                // WindowClause.pickArgKind uses. DECIMAL sum / avg over a join is part of the bug
                // class this PR fixes, and NUMERIC covers BYTE..DOUBLE only, so DECIMAL never
                // reached a join aggregate at all. The one legitimate failure a DECIMAL aggregate
                // has is already an accepted skip in QueryRunner.isDecimalAggregationOverflow.
                ColumnKind kind = rnd.nextInt(100) < 25 ? ColumnKind.DECIMAL : ColumnKind.NUMERIC;
                String c = pickColumn(rnd, table, kind);
                if (c == null && kind == ColumnKind.DECIMAL) {
                    c = pickColumn(rnd, table, ColumnKind.NUMERIC);
                }
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

    // ORDER BY over 1..2 of the projected key ({@code e<n>}) and aggregate
    // ({@code a<n>}) aliases, each with an optional ASC / DESC direction.
    static void appendOrderBy(StringSink sql, Rnd rnd, int keyCount, int aggCount) {
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

    /**
     * Emits the row-set guard aggregate: {@code count(<slaveAlias>.ts)}, the number of SLAVE rows
     * the join matched for the output row it sits on. The slave timestamp is the designated one and
     * is never null, so the count is exactly that cardinality.
     * <p>
     * It counts the slave timestamp rather than {@code count(*)} because the two join shapes differ:
     * a HORIZON JOIN updates its aggregates once per (master row, offset) pair whether or not the
     * ASOF probe matched, so {@code count(*)} there is a purely master-side quantity that a dropped
     * or duplicated slave match leaves untouched.
     * <p>
     * It is mandatory rather than one of the random picks because the FLOAT / DOUBLE cell
     * comparison in {@code QueryRunner.rowEqualsWithFpTolerance} tolerates reduction-order drift
     * relative to the MAGNITUDE of the result, and a term much smaller than the sum fits inside
     * that envelope: for {@code {1_000_000, 1_000_000, 1_000_000, 9}} the tolerance at 3e6 is
     * roughly 23, so losing the 9 costs nothing. The envelope cannot be tightened away - a
     * {@code sum} over a FLOAT column is typed DOUBLE, so the metadata cannot tell a
     * float-precision accumulation from a double-precision one - but an exact integer count cannot
     * absorb a dropped or duplicated row. It is also the only thing that sees a dropped row under a
     * {@code min} / {@code max} / {@code first} / {@code last} projection, where a non-extremal row
     * leaves the aggregate itself unchanged.
     * <p>
     * What it does NOT catch: a window or horizon bound shifted by one interval drops a row at one
     * end and admits one at the other, so the cardinality is unchanged and only the FP-tolerant sum
     * moves. That shape needs an absolute oracle, not a count.
     */
    static void appendRowSetGuard(StringSink sql, String slaveAlias, int aggIndex) {
        sql.put(", count(").put(slaveAlias).put(".ts) AS a").put(aggIndex);
    }

    // Picks a random column name of the given kind (any kind when {@code kind}
    // is null), skipping ARRAY columns which do not aggregate cleanly; null when
    // the table has none.
    static String pickColumn(Rnd rnd, FuzzTable table, ColumnKind kind) {
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

    // Picks a random orderable column name (for min/max); null when the table
    // has none.
    static String pickOrderableColumn(Rnd rnd, FuzzTable table) {
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
