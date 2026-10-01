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
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;

/**
 * WINDOW JOIN across two WAL tables. A WINDOW JOIN preserves the master
 * row count - one output row per master row - and for each master row
 * aggregates the slave rows whose timestamp falls in
 * {@code [master.ts - lo, master.ts + hi]} (a {@code RANGE BETWEEN ... AND
 * ...} frame). Master columns project bare; slave columns must be wrapped
 * in an aggregate.
 * <p>
 * Shape:
 * <pre>
 * SELECT masterExpr AS e0 [, masterExpr AS e1 ...][, t.ts AS e&lt;n&gt;]
 *        , agg(p.col) AS a0 [, agg(...) AS a1 ...], count(p.ts) AS a&lt;n&gt;
 * FROM master t
 * WINDOW JOIN slave p [ON (t.sym = p.sym)]
 * RANGE BETWEEN &lt;lo&gt; AND &lt;hi&gt; [INCLUDE PREVAILING | EXCLUDE PREVAILING]
 * [WHERE master-only-predicate]
 * [ORDER BY ...]
 * [ORDER BY t.ts alias LIMIT N]
 * </pre>
 * The trailing {@code count(p.ts)} is always projected - see
 * {@link JoinClauseSupport#appendRowSetGuard} - and an ordered {@code LIMIT} brings the master
 * timestamp into the projection so the limited row set is fully determined. One limited query in
 * four stays unordered, and only those are tagged non-deterministic.
 * Bounds use static forms only - {@code UNBOUNDED PRECEDING}, {@code
 * CURRENT ROW}, and {@code N <unit> PRECEDING/FOLLOWING} - with units
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
        // Decided up front because an ordered limited query projects the master timestamp; see the
        // ORDER BY below. One limited query in four stays UNORDERED and is tagged non-deterministic:
        // a bare LIMIT is what exercises the parallel path's early cancellation, and ordering every
        // limited query would take that shape out of the generator entirely.
        boolean hasLimit = rnd.nextBoolean();
        boolean isLimitOrdered = hasLimit && rnd.nextInt(4) != 0;

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
        // The ordering key a LIMIT needs - see the ORDER BY below. Projected, not just ordered on,
        // because ORDER BY resolves against the projection here.
        if (isLimitOrdered) {
            sql.put(", ").put(MASTER_ALIAS).put(".ts AS e").put(masterSlots);
            masterSlots++;
        }
        int aggCount = 1 + rnd.nextInt(3); // 1..3 slave aggregates
        for (int i = 0; i < aggCount; i++) {
            sql.put(", ");
            JoinClauseSupport.appendAggregate(sql, rnd, slave, SLAVE_ALIAS);
            sql.put(" AS a").put(i);
        }
        // Pins how many slave rows the join matched, so a dropped or duplicated one cannot hide
        // inside the FLOAT tolerance of a sum sitting next to it.
        JoinClauseSupport.appendRowSetGuard(sql, SLAVE_ALIAS, aggCount);
        aggCount++;

        sql.put(" FROM ").put(master.getName()).put(' ').put(MASTER_ALIAS);
        sql.put(" WINDOW JOIN ").put(slave.getName()).put(' ').put(SLAVE_ALIAS);
        if (keyed) {
            sql.put(" ON (").put(MASTER_ALIAS).put(".sym = ").put(SLAVE_ALIAS).put(".sym)");
        }
        appendFrame(sql, rnd);

        // WHERE may reference master columns only.
        PredicateGenerator.appendWhere(sql, rnd, master.getColumns(), MASTER_ALIAS, 1, ctx, injectFaultFn);

        // A LIMIT needs an ordering that fully determines WHICH rows survive, or the two sides of a
        // differential can each return a different valid subset and only their row counts can be
        // compared. A WINDOW JOIN emits exactly one row per master row, and the fuzz tables step ts
        // by a fixed interval from a fixed start, so the master timestamp is unique per output row:
        // ordering by it alone is a total order, the LIMIT window is determined, and the runner can
        // compare cells. Roughly half the generated queries carry a LIMIT, so this is a large part
        // of the shape's coverage.
        //
        // Ordering by an aggregate alias would NOT do: a FLOAT sum drifts with reduction order, so
        // two rows close in that column can swap between the two sides, and the row-by-row compare
        // would then read a real divergence off a legitimate tie.
        if (isLimitOrdered) {
            sql.put(" ORDER BY e").put(masterSlots - 1);
        } else if (!hasLimit && rnd.nextBoolean()) {
            JoinClauseSupport.appendOrderBy(sql, rnd, masterSlots, aggCount);
        }
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        return new GeneratedQuery(sql.toString(), isLimitOrdered || !hasLimit);
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

}
