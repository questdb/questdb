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
import io.questdb.test.griffin.fuzz.clauses.PostingClause;
import io.questdb.test.griffin.fuzz.clauses.SampleByClause;
import io.questdb.test.griffin.fuzz.clauses.SimpleClause;
import io.questdb.test.griffin.fuzz.clauses.TemporalJoinClause;
import io.questdb.test.griffin.fuzz.expr.BindContext;

/**
 * Picks a query shape and delegates to the matching clause generator.
 * <p>
 * Shape distribution is roughly: temporal JOIN 15% (when two or more
 * tables exist), posting-index read 15% (when the table has a
 * posting-indexed symbol), SAMPLE BY 20%, GROUP BY 20%, SIMPLE 30%. The
 * join and posting bands fall through to the generic single-table shapes
 * when their precondition is not met.
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

    public static GeneratedQuery generate(Rnd rnd, ObjList<FuzzTable> tables, BindContext ctx) {
        int pick = rnd.nextInt(100);
        FuzzTable t = tables.getQuick(rnd.nextInt(tables.size()));

        if (pick < 15 && tables.size() >= 2) {
            FuzzTable other = tables.getQuick(rnd.nextInt(tables.size()));
            if (other == t) {
                other = tables.getQuick((tables.indexOf(t) + 1) % tables.size());
            }
            // Joins stay on direct, real tables: ASOF/LT/SPLICE need a
            // designated timestamp on both sides which virtual tables
            // don't carry.
            return TemporalJoinClause.generate(rnd, FuzzSource.direct(t), FuzzSource.direct(other), ctx);
        }

        // Posting-index read shapes (distinct key enumeration / covering read)
        // on a direct real table. tryGenerate returns null without touching rnd
        // when the table has no posting-indexed symbol, so the generic shape
        // distribution below stays unchanged in that case.
        if (pick < 30) {
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
                return SimpleClause.generate(rnd, source, ctx);
            }
            return SampleByClause.generate(rnd, source, ctx);
        }
        if (pick < 70) {
            return GroupByClause.generate(rnd, source, ctx);
        }
        return SimpleClause.generate(rnd, source, ctx);
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
