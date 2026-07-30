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
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.BindContext;

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
 * join actually contributes, and a trailing {@code count(p.ts)} is always projected - see
 * {@link JoinClauseSupport#appendRowSetGuard}. The {@code RANGE}/{@code LIST} spec is bounded
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

    // Interval-literal unit chars for the horizon offsets. The fuzz tables step rows 30 minutes
    // apart, so hours and whole-30-minute steps (see stepForUnit) give every offset its own ASOF
    // match; seconds deliberately stay below the spacing, where several offsets resolve to the same
    // slave row, and are kept to a minority of draws rather than a quarter of them.
    private static final char[] HORIZON_UNITS = {'m', 'm', 'h', 'h', 's'};
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
                sql.put("(h.offset / 1_000_000)");
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
            JoinClauseSupport.appendAggregate(sql, rnd, overSlave ? slave : master, overSlave ? SLAVE_ALIAS : MASTER_ALIAS);
            sql.put(" AS a").put(i);
        }
        // Pins how many slave rows the join matched, so a dropped or duplicated one cannot hide
        // inside the FLOAT tolerance of a sum sitting next to it. It counts p.ts rather than the
        // output rows: this shape aggregates once per (master row, offset) pair whether the ASOF
        // probe matched or not, so count(*) here says nothing about the slave side.
        JoinClauseSupport.appendRowSetGuard(sql, SLAVE_ALIAS, aggCount);
        aggCount++;

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
            JoinClauseSupport.appendOrderBy(sql, rnd, keyCount, aggCount);
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

    private static void appendHorizonSpec(StringSink sql, Rnd rnd, boolean useList) {
        char unit = HORIZON_UNITS[rnd.nextInt(HORIZON_UNITS.length)];
        if (useList) {
            int n = 2 + rnd.nextInt(3); // 2..4 offsets
            int step = stepForUnit(rnd, unit);
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
            int step = stepForUnit(rnd, unit);
            int before = rnd.nextInt(3); // 0..2 steps before zero
            int after = 1 + rnd.nextInt(3); // 1..3 steps after zero (zero + before + after = up to 6 offsets)
            sql.put(" RANGE FROM ").put(-step * before).put(unit)
                    .put(" TO ").put(step * after).put(unit)
                    .put(" STEP ").put(step).put(unit).put(" AS h");
        }
    }

    /**
     * The offset step for a unit, sized against the fuzz tables' 30-minute row spacing.
     * <p>
     * A step below that spacing leaves consecutive offsets ASOF-matching the SAME slave row - the
     * horizon degenerates into a repeated ASOF join and stops exercising multi-row windows. Minutes
     * therefore step in whole 30-minute multiples. Seconds keep a sub-spacing step on purpose: a
     * horizon whose offsets collapse onto one row is a legitimate shape and worth generating, just
     * not worth most of the draws.
     */
    private static int stepForUnit(Rnd rnd, char unit) {
        return switch (unit) {
            case 'm' -> 30 * (1 + rnd.nextInt(3));
            default -> 1 + rnd.nextInt(3);
        };
    }

}
