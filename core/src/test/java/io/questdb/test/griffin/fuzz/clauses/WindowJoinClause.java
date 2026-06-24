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
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * WINDOW JOIN across two WAL tables. A WINDOW JOIN preserves the master
 * row count -- one output row per master row -- and for each master row
 * aggregates the slave rows whose timestamp falls in
 * {@code [master.ts - lo, master.ts + hi]} (a {@code RANGE BETWEEN ... AND
 * ...} frame). Master columns project bare; slave columns must be wrapped
 * in an aggregate.
 * <p>
 * Shape:
 * <pre>
 * SELECT masterExpr AS e0 [, masterExpr AS e1 ...], agg(p.col) AS a0 [, agg(...) AS a1 ...]
 * FROM master t
 * WINDOW JOIN slave p [ON (t.sym = p.sym)]
 * RANGE BETWEEN &lt;lo&gt; AND &lt;hi&gt; [INCLUDE PREVAILING | EXCLUDE PREVAILING]
 * [WHERE master-only-predicate]
 * [ORDER BY ...]
 * [LIMIT N]
 * </pre>
 * Bounds use static forms only -- {@code UNBOUNDED PRECEDING}, {@code
 * CURRENT ROW}, and {@code N <unit> PRECEDING/FOLLOWING} -- with units
 * biased to minutes/hours so a frame actually spans several 30-minute-spaced
 * slave rows. The {@code hi} bound is chosen to respect the parser's
 * ordering rules ({@code start row is CURRENT/FOLLOWING, end row must not be
 * PRECEDING}), so the generator never emits a frame the parser rejects.
 * {@code PREVAILING} (whether the last slave row before the frame is folded
 * in) is emitted on ~2/3 of queries.
 * <p>
 * The WHERE references master columns only. The shared {@code sym} makes the
 * keyed {@code ON (t.sym = p.sym)} form always emittable.
 */
public final class WindowJoinClause {

    private static final String MASTER_ALIAS = "t";
    private static final String SLAVE_ALIAS = "p";
    // Bound unit words accepted by SqlParser.parseTimeUnit, biased to
    // minutes/hours to match the fuzz tables' 30-minute row spacing.
    private static final String[] UNITS = {"minutes", "minutes", "hours", "seconds"};

    private WindowJoinClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzTable master, FuzzTable slave, BindContext ctx, boolean injectFaultFn) {
        ExpressionGenerator masterGen = new ExpressionGenerator(rnd, master.getColumns(), MASTER_ALIAS, 2);
        boolean keyed = rnd.nextInt(3) != 0;

        StringSink sql = new StringSink();
        sql.put("SELECT ");
        int masterSlots = 1 + rnd.nextInt(3); // 1..3 bare-ish master expressions
        for (int i = 0; i < masterSlots; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            FuzzExpr e = masterGen.generateAnyKind();
            e.appendSql(sql, ctx);
            sql.put(" AS e").put(i);
        }
        int aggCount = 1 + rnd.nextInt(3); // 1..3 slave aggregates
        for (int i = 0; i < aggCount; i++) {
            sql.put(", ");
            appendAggregate(sql, rnd, slave);
            sql.put(" AS a").put(i);
        }

        sql.put(" FROM ").put(master.getName()).put(' ').put(MASTER_ALIAS);
        sql.put(" WINDOW JOIN ").put(slave.getName()).put(' ').put(SLAVE_ALIAS);
        if (keyed) {
            sql.put(" ON (").put(MASTER_ALIAS).put(".sym = ").put(SLAVE_ALIAS).put(".sym)");
        }
        appendFrame(sql, rnd);

        // WHERE may reference master columns only.
        PredicateGenerator.appendWhere(sql, rnd, master.getColumns(), MASTER_ALIAS, 1, ctx, injectFaultFn);

        if (rnd.nextBoolean()) {
            appendOrderBy(sql, rnd, masterSlots, aggCount);
        }

        // LIMIT over a parallel WINDOW JOIN can pick a different valid subset
        // when ORDER BY does not fully disambiguate; tag the query so the
        // runner compares row counts rather than content.
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    private static void appendAggregate(StringSink sql, Rnd rnd, FuzzTable slave) {
        int pick = rnd.nextInt(7);
        switch (pick) {
            case 0 -> sql.put("count(*)");
            case 1 -> {
                String c = pickColumn(rnd, slave, null);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put("count(").put(SLAVE_ALIAS).put('.').put(c).put(')');
                }
            }
            case 2, 3 -> {
                String c = pickColumn(rnd, slave, ColumnKind.NUMERIC);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put(rnd.nextBoolean() ? "sum(" : "avg(").put(SLAVE_ALIAS).put('.').put(c).put(')');
                }
            }
            case 4 -> {
                String c = pickOrderableColumn(rnd, slave);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put(rnd.nextBoolean() ? "min(" : "max(").put(SLAVE_ALIAS).put('.').put(c).put(')');
                }
            }
            default -> {
                String c = pickColumn(rnd, slave, null);
                if (c == null) {
                    sql.put("count(*)");
                } else {
                    sql.put(rnd.nextBoolean() ? "first(" : "last(").put(SLAVE_ALIAS).put('.').put(c).put(')');
                }
            }
        }
    }

    /**
     * Emits {@code RANGE BETWEEN <lo> AND <hi> [INCLUDE|EXCLUDE PREVAILING]}.
     * WINDOW joins reject {@code UNBOUNDED} bounds and require {@code hi >= lo}
     * on the timeline ({@code "WINDOW join hi value cannot be less than lo
     * value"}). Drawing two signed offsets in one unit and ordering them keeps
     * both rules: a PRECEDING offset is negative, CURRENT ROW is zero, a
     * FOLLOWING offset is positive, so {@code lo <= hi} also satisfies the
     * parser's "start CURRENT/FOLLOWING, end not PRECEDING" rule for free.
     */
    private static void appendFrame(StringSink sql, Rnd rnd) {
        sql.put(" RANGE BETWEEN ");
        String unit = UNITS[rnd.nextInt(UNITS.length)];
        int a = signedOffset(rnd, unit);
        int b = signedOffset(rnd, unit);
        appendBoundAt(sql, Math.min(a, b), unit);
        sql.put(" AND ");
        appendBoundAt(sql, Math.max(a, b), unit);
        int prevailing = rnd.nextInt(3);
        if (prevailing == 0) {
            sql.put(" INCLUDE PREVAILING");
        } else if (prevailing == 1) {
            sql.put(" EXCLUDE PREVAILING");
        }
    }

    private static void appendBoundAt(StringSink sql, int offset, String unit) {
        if (offset == 0) {
            sql.put("CURRENT ROW");
        } else if (offset < 0) {
            sql.put(-offset).put(' ').put(unit).put(" PRECEDING");
        } else {
            sql.put(offset).put(' ').put(unit).put(" FOLLOWING");
        }
    }

    // A signed frame offset in the given unit: 0 (CURRENT ROW) on 1/5 of draws,
    // otherwise a magnitude scaled to the unit with a random sign.
    private static int signedOffset(Rnd rnd, String unit) {
        if (rnd.nextInt(5) == 0) {
            return 0;
        }
        int max = switch (unit) {
            case "hours" -> 4;
            case "seconds" -> 3600;
            default -> 180; // minutes
        };
        int mag = 1 + rnd.nextInt(max);
        return rnd.nextBoolean() ? -mag : mag;
    }

    private static void appendOrderBy(StringSink sql, Rnd rnd, int masterSlots, int aggCount) {
        int total = masterSlots + aggCount;
        int picks = 1 + rnd.nextInt(Math.min(2, total));
        sql.put(" ORDER BY ");
        for (int i = 0; i < picks; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            int idx = rnd.nextInt(total);
            if (idx < masterSlots) {
                sql.put('e').put(idx);
            } else {
                sql.put('a').put(idx - masterSlots);
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
