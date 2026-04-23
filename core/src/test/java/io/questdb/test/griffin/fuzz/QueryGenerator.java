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
import io.questdb.test.griffin.fuzz.clauses.SampleByClause;
import io.questdb.test.griffin.fuzz.clauses.SimpleClause;
import io.questdb.test.griffin.fuzz.clauses.TemporalJoinClause;

/**
 * Picks a query shape and delegates to the matching clause generator.
 * <p>
 * Shape distribution is roughly: SIMPLE 40%, GROUP BY 25%, SAMPLE BY
 * 20%, temporal JOIN 15% (when two or more tables exist, else replaced
 * with SIMPLE).
 * <p>
 * For non-join shapes the {@link FuzzSource} is usually a direct
 * reference to a real table, but occasionally wrapped in a subquery or
 * CTE, or replaced by a {@code long_sequence()} virtual table. Virtual
 * tables have no designated timestamp, so SAMPLE BY queries on them
 * fall back to SIMPLE.
 */
public final class QueryGenerator {

    // Out of 100.
    private static final int VIRTUAL_BASE_PCT = 10;  // real vs long_sequence
    private static final int WRAP_CTE_PCT = 10;      // of the remaining 100, wrap in CTE
    private static final int WRAP_SUBQUERY_PCT = 10; // of the remaining 100, wrap in subquery

    private QueryGenerator() {
    }

    public static String generate(Rnd rnd, ObjList<FuzzTable> tables) {
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
            return TemporalJoinClause.generate(rnd, FuzzSource.direct(t), FuzzSource.direct(other));
        }

        FuzzSource source = pickSource(rnd, t);
        if (pick < 35) {
            // SAMPLE BY requires a designated ts; downgrade to SIMPLE on
            // virtual-only sources.
            if (source.getTable().getTsColumnName() == null) {
                return SimpleClause.generate(rnd, source);
            }
            return SampleByClause.generate(rnd, source);
        }
        if (pick < 60) {
            return GroupByClause.generate(rnd, source);
        }
        return SimpleClause.generate(rnd, source);
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
