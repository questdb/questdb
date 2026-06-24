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
import io.questdb.test.griffin.fuzz.clauses.GroupByClause;
import io.questdb.test.griffin.fuzz.clauses.HorizonJoinClause;
import io.questdb.test.griffin.fuzz.clauses.PostingClause;
import io.questdb.test.griffin.fuzz.clauses.SampleByClause;
import io.questdb.test.griffin.fuzz.clauses.SimpleClause;
import io.questdb.test.griffin.fuzz.clauses.TemporalJoinClause;
import io.questdb.test.griffin.fuzz.clauses.WindowClause;
import io.questdb.test.griffin.fuzz.clauses.WindowJoinClause;
import io.questdb.test.griffin.fuzz.expr.BindContext;

/**
 * Picks a query shape and delegates to the matching clause generator.
 * <p>
 * Shape distribution is roughly: temporal JOIN 15% (when two or more
 * tables exist), posting-index read 15% (when the table has a
 * posting-indexed symbol), SAMPLE BY 20%, GROUP BY 20%, and the remaining
 * ~30% split between SIMPLE and WINDOW. The join and posting bands fall
 * through to the generic single-table shapes when their precondition is
 * not met; SAMPLE BY and WINDOW downgrade to SIMPLE on sources without a
 * designated timestamp.
 * <p>
 * When window fuzzing is enabled ({@code windowEnabled}, on by default), a
 * WINDOW band is carved out of the upper half of the SIMPLE range; the
 * SAMPLE BY and GROUP BY bands are unchanged, so disabling window restores
 * the exact pre-window shape distribution (SIMPLE ~30%).
 * <p>
 * For non-join shapes the {@link FuzzSource} is usually a direct
 * reference to a real table, but occasionally wrapped in a subquery or
 * CTE, or replaced by a {@code long_sequence()} virtual table. Virtual
 * tables have no designated timestamp, so SAMPLE BY queries on them
 * fall back to SIMPLE. Posting-index read shapes always use a direct
 * real table since the planner needs the base table to pick the index.
 */
public final class QueryGenerator {

    // Out of 100.
    private static final int VIRTUAL_BASE_PCT = 10;  // real vs long_sequence
    private static final int WRAP_CTE_PCT = 10;      // of the remaining 100, wrap in CTE
    private static final int WRAP_SUBQUERY_PCT = 10; // of the remaining 100, wrap in subquery

    private QueryGenerator() {
    }

    public static GeneratedQuery generate(Rnd rnd, ObjList<FuzzTable> tables, BindContext ctx, boolean injectFaultFn, boolean windowEnabled, boolean horizonJoinEnabled, boolean windowJoinEnabled) {
        int pick = rnd.nextInt(100);
        FuzzTable t = tables.getQuick(rnd.nextInt(tables.size()));

        if (pick < 15 && tables.size() >= 2) {
            FuzzTable other = tables.getQuick(rnd.nextInt(tables.size()));
            if (other == t) {
                other = tables.getQuick((tables.indexOf(t) + 1) % tables.size());
            }
            // Joins stay on direct, real tables: ASOF/LT/SPLICE, HORIZON and
            // WINDOW joins all need a designated timestamp on both sides which
            // virtual tables don't carry. The 15% join band is split across the
            // enabled join kinds; with both new kinds off it stays temporal-only
            // and draws no extra rnd op, preserving the original distribution.
            return generateJoin(rnd, t, other, ctx, injectFaultFn, horizonJoinEnabled, windowJoinEnabled);
        }

        // Posting-index read shapes (distinct key enumeration / covering read)
        // on a direct real table. tryGenerate returns null without touching rnd
        // when the table has no posting-indexed symbol, so the generic shape
        // distribution below stays unchanged in that case. Function faults skip
        // this path so test_fault() always lands in a clause that emits it;
        // PostingClause builds its own narrow SQL without a generic WHERE.
        if (pick < 30 && !injectFaultFn) {
            GeneratedQuery posting = PostingClause.tryGenerate(rnd, t, ctx);
            if (posting != null) {
                return posting;
            }
        }

        FuzzSource source = pickSource(rnd, t);
        if (pick < 50) {
            // SAMPLE BY requires a designated ts; downgrade to SIMPLE on
            // virtual-only sources.
            if (source.getTable().getTsColumnName() == null) {
                return SimpleClause.generate(rnd, source, ctx, injectFaultFn);
            }
            return SampleByClause.generate(rnd, source, ctx, injectFaultFn);
        }
        if (pick < 70) {
            return GroupByClause.generate(rnd, source, ctx, injectFaultFn);
        }
        // pick in [70, 100) is SIMPLE. When window fuzzing is enabled, carve the
        // upper half [85, 100) into a WINDOW band; this leaves the SAMPLE BY and
        // GROUP BY bands untouched, so disabling window restores the exact
        // pre-window distribution. Window functions order by the designated
        // timestamp, so a source without one falls through to SIMPLE.
        if (windowEnabled && pick >= 85 && source.getTable().getTsColumnName() != null) {
            return WindowClause.generate(rnd, source, ctx, injectFaultFn);
        }
        return SimpleClause.generate(rnd, source, ctx, injectFaultFn);
    }

    /**
     * Picks one of the enabled join kinds for the join band. Temporal
     * (ASOF/LT/SPLICE) is always available; HORIZON and WINDOW joins are added
     * when their config flags are on. The sub-pick is drawn only when more than
     * one kind is enabled, so the both-off case reproduces the pre-existing
     * temporal-only rnd stream exactly. All three kinds run on direct real
     * tables and thread {@code injectFaultFn} into their master-only WHERE.
     */
    private static GeneratedQuery generateJoin(
            Rnd rnd,
            FuzzTable t,
            FuzzTable other,
            BindContext ctx,
            boolean injectFaultFn,
            boolean horizonJoinEnabled,
            boolean windowJoinEnabled
    ) {
        int kinds = 1 + (horizonJoinEnabled ? 1 : 0) + (windowJoinEnabled ? 1 : 0);
        int joinPick = kinds > 1 ? rnd.nextInt(kinds) : 0;
        if (joinPick == 0) {
            return TemporalJoinClause.generate(rnd, FuzzSource.direct(t), FuzzSource.direct(other), ctx, injectFaultFn);
        }
        if (horizonJoinEnabled && joinPick == 1) {
            return HorizonJoinClause.generate(rnd, t, other, ctx, injectFaultFn);
        }
        // The remaining slot is WINDOW: either joinPick == 2 (both kinds on) or
        // joinPick == 1 with only WINDOW enabled.
        return WindowJoinClause.generate(rnd, t, other, ctx, injectFaultFn);
    }

    private static FuzzSource pickSource(Rnd rnd, FuzzTable realTable) {
        FuzzSource base = rnd.nextInt(100) < VIRTUAL_BASE_PCT
                ? FuzzSource.longSequence(rnd)
                : FuzzSource.direct(realTable);

        int wrap = rnd.nextInt(100);
        if (wrap < WRAP_CTE_PCT) {
            return FuzzSource.cte(rnd, base, 0);
        }
        if (wrap < WRAP_CTE_PCT + WRAP_SUBQUERY_PCT) {
            return FuzzSource.subquery(rnd, base);
        }
        return base;
    }
}
