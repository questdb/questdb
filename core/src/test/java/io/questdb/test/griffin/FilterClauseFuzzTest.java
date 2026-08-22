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

package io.questdb.test.griffin;

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Differential fuzzing for the FILTER (WHERE ...) clause.
 * <p>
 * The clause has an exact oracle: for every aggregate that supports it, the filtered form must
 * equal the same aggregate evaluated over a pre-filtered subquery, for any data and any condition.
 * The seed is printed on failure so a mismatch reproduces.
 * <p>
 * d2 is deliberately not a linear function of d. Scale-invariant statistics such as corr and
 * regr_slope are constant on collinear data, so those aggregates would agree between the two forms
 * no matter what the lowering did.
 */
public class FilterClauseFuzzTest extends AbstractCairoTest {

    private static final String[] AGGREGATES = {
            "sum(d)", "avg(d)", "min(d)", "max(d)", "count(*)", "count(d)", "count(l)",
            "count_distinct(s)", "stddev_samp(d)", "var_samp(d)", "geomean(d)", "ksum(d)",
            "nsum(d)", "skewness(d)", "kurtosis(d)", "string_agg(s, ',')", "first_not_null(d)",
            "last_not_null(d)", "corr(d, d2)", "covar_samp(d, d2)", "weighted_avg(d, d2)",
            "vwap(d, d2)", "arg_max(d, d2)", "arg_min(d, d2)", "approx_percentile(d, 0.5)",
            "regr_slope(d, d2)", "bit_and(i)", "bit_or(i)", "bit_xor(i)", "sum(l)", "avg(l)"
    };

    private static final String[] PREDICATES = {
            "id %% %d != 0",
            "id > %d",
            "d > %d",
            "d is null",
            "d is not null",
            "s is null or id %% %d = 0",
            "not (id %% %d = 0)",
            "id between %d and 40",
            "l %% %d = 1 and d is not null"
    };

    @Test
    public void testFilterMatchesPreFilteredSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final StringSink filtered = new StringSink();
            final StringSink reference = new StringSink();

            for (int iteration = 0; iteration < 150; iteration++) {
                final int rows = 1 + rnd.nextInt(200);
                final int nullEvery = 2 + rnd.nextInt(6);
                final int nullEvery2 = 2 + rnd.nextInt(7);
                execute("drop table if exists fz");
                execute(
                        "create table fz as (select" +
                                " x id," +
                                " case when x % " + nullEvery + " = 0 then null else x::double end d," +
                                " case when x % " + nullEvery2 + " = 0 then null else ((x * 7) % 13)::double end d2," +
                                " case when x % " + nullEvery + " = 1 then null else x end l," +
                                " case when x % " + nullEvery2 + " = 1 then null else x::int end i," +
                                " case when x % " + nullEvery + " = 2 then null else ('v' || (x % 13)) end s," +
                                " timestamp_sequence(0, 1_000_000) ts" +
                                " from long_sequence(" + rows + ")) timestamp(ts) partition by day"
                );

                final String aggregate = AGGREGATES[rnd.nextInt(AGGREGATES.length)];
                final String predicate = String.format(
                        PREDICATES[rnd.nextInt(PREDICATES.length)],
                        1 + rnd.nextInt(20),
                        1 + rnd.nextInt(20)
                );

                filtered.clear();
                filtered.put("select ").put(aggregate).put(" filter (where ").put(predicate).put(") r from fz");
                reference.clear();
                reference.put("select ").put(aggregate).put(" r from (select * from fz where ")
                        .put(predicate).put(") timestamp(ts)");

                // assertSqlCursors compares metadata as well as rows and logs both result sets on a
                // mismatch; the wrapper adds back the iteration and SQL that reproduce it.
                try {
                    assertSqlCursors(reference.toString(), filtered.toString());
                } catch (AssertionError e) {
                    throw new AssertionError(
                            "iteration " + iteration + " rows=" + rows + " sql=" + filtered, e
                    );
                }
            }
        });
    }
}
