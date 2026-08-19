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

import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Sweeps the FILTER (WHERE ...) clause across every registered aggregate.
 * <p>
 * The clause lowers to CASE WHEN condition THEN arg END, which nulls a value rather than dropping
 * a row. That is only equivalent to filtering for aggregates that skip NULL inputs, so every
 * aggregate has to be classified here as either LOWERABLE or REJECTED. An aggregate that nobody
 * classifies fails {@link #testEveryAggregateIsClassified}, which stops a newly added
 * null-preserving aggregate from silently returning a different answer under FILTER.
 * <p>
 * The dataset deliberately carries NULLs in every value column: on dense data, nulling a value and
 * dropping a row are indistinguishable, so NULL-free tests cannot detect the difference at all. For
 * the same reason d2 is not a linear function of d - see {@link #createData()}.
 */
public class FilterClauseSweepTest extends AbstractCairoTest {

    // Several conditions, not one. A single condition can coincide with the data so that a broken
    // lowering still produces the right answer - an unwrapped arg_min key went undetected because
    // the first matching row happened to be one whose value was NULL anyway.
    private static final String[] CONDITIONS = {
            "id > 5 and id % 3 != 0",
            "id % 2 = 0",
            "d is not null and id > 20",
            "id < 50"
    };
    // aggregate name -> invocation over the columns created by DDL
    private static final Map<String, String> LOWERABLE = new LinkedHashMap<>();
    private static final Map<String, String> REJECTED = new LinkedHashMap<>();

    static {
        LOWERABLE.put("approx_count_distinct", "approx_count_distinct(i)");
        LOWERABLE.put("approx_median", "approx_median(d)");
        LOWERABLE.put("approx_percentile", "approx_percentile(d, 0.5)");
        LOWERABLE.put("arg_max", "arg_max(d, d2)");
        LOWERABLE.put("arg_min", "arg_min(d, d2)");
        LOWERABLE.put("avg", "avg(d)");
        LOWERABLE.put("bit_and", "bit_and(bitv)");
        LOWERABLE.put("bit_or", "bit_or(bitv)");
        LOWERABLE.put("bit_xor", "bit_xor(i)");
        LOWERABLE.put("corr", "corr(d, d2)");
        LOWERABLE.put("count", "count(d)");
        LOWERABLE.put("count_distinct", "count_distinct(s)");
        LOWERABLE.put("covar_pop", "covar_pop(d, d2)");
        LOWERABLE.put("covar_samp", "covar_samp(d, d2)");
        LOWERABLE.put("first_not_null", "first_not_null(d)");
        LOWERABLE.put("geomean", "geomean(d)");
        LOWERABLE.put("ksum", "ksum(d)");
        LOWERABLE.put("kurtosis", "kurtosis(d)");
        LOWERABLE.put("kurtosis_pop", "kurtosis_pop(d)");
        LOWERABLE.put("kurtosis_samp", "kurtosis_samp(d)");
        LOWERABLE.put("last_not_null", "last_not_null(d)");
        LOWERABLE.put("max", "max(d)");
        LOWERABLE.put("min", "min(d)");
        LOWERABLE.put("nsum", "nsum(d)");
        LOWERABLE.put("regr_intercept", "regr_intercept(d, d2)");
        LOWERABLE.put("regr_r2", "regr_r2(d, d2)");
        LOWERABLE.put("regr_slope", "regr_slope(d, d2)");
        LOWERABLE.put("skewness", "skewness(d)");
        LOWERABLE.put("skewness_pop", "skewness_pop(d)");
        LOWERABLE.put("skewness_samp", "skewness_samp(d)");
        LOWERABLE.put("sparkline", "sparkline(d)");
        LOWERABLE.put("stddev", "stddev(d)");
        LOWERABLE.put("stddev_pop", "stddev_pop(d)");
        LOWERABLE.put("stddev_samp", "stddev_samp(d)");
        LOWERABLE.put("string_agg", "string_agg(s, ',')");
        LOWERABLE.put("string_distinct_agg", "string_distinct_agg(s, ',')");
        LOWERABLE.put("sum", "sum(d)");
        LOWERABLE.put("var_pop", "var_pop(d)");
        LOWERABLE.put("var_samp", "var_samp(d)");
        LOWERABLE.put("variance", "variance(d)");
        LOWERABLE.put("vwap", "vwap(d, d2)");
        LOWERABLE.put("weighted_avg", "weighted_avg(d, d2)");
        LOWERABLE.put("weighted_stddev", "weighted_stddev(d, d2)");
        LOWERABLE.put("weighted_stddev_freq", "weighted_stddev_freq(d, d2)");
        LOWERABLE.put("weighted_stddev_rel", "weighted_stddev_rel(d, d2)");
        LOWERABLE.put("haversine_dist_deg", "haversine_dist_deg(d, d2, ts)");
        LOWERABLE.put("array_elem_avg", "array_elem_avg(arr)");
        LOWERABLE.put("array_elem_max", "array_elem_max(arr)");
        LOWERABLE.put("array_elem_min", "array_elem_min(arr)");
        LOWERABLE.put("array_elem_sum", "array_elem_sum(arr)");

        // NULL-preserving: the CASE lowering would change the result rather than drop the row
        REJECTED.put("array_agg", "array_agg(d)");
        REJECTED.put("bool_and", "bool_and(b)");
        REJECTED.put("bool_or", "bool_or(b)");
        REJECTED.put("first", "first(d)");
        REJECTED.put("isOrdered", "isOrdered(l)");
        REJECTED.put("last", "last(d)");
        REJECTED.put("mode", "mode(b)");
        // not null-preservation: twap requires the designated timestamp column as its second
        // argument, which the CASE wrapper replaces with an expression
        REJECTED.put("twap", "twap(d, ts)");
    }

    @Test
    public void testEveryAggregateIsClassified() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionFactoryCache cache = engine.getFunctionFactoryCache();
            final TreeSet<String> unclassified = new TreeSet<>();
            cache.getFactories().forEach((key, value) -> {
                final String name = key.toString();
                if (!cache.isGroupBy(name) || isTestOnly(value)) {
                    return;
                }
                if (!LOWERABLE.containsKey(name) && !REJECTED.containsKey(name)) {
                    unclassified.add(name);
                }
            });
            Assert.assertTrue(
                    "aggregate(s) not classified for FILTER support: " + unclassified
                            + " - add them to LOWERABLE (and prove equivalence) or to REJECTED",
                    unclassified.isEmpty()
            );
        });
    }

    @Test
    public void testLowerableAggregatesMatchPreFilteredSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createData();
            for (Map.Entry<String, String> e : LOWERABLE.entrySet()) {
                final String call = e.getValue();
                for (String condition : CONDITIONS) {
                    // assertSqlCursors compares metadata as well as rows, and on a mismatch logs both
                    // result sets side by side, which a plain string compare of two printed cursors
                    // cannot do
                    assertSqlCursors(
                            "select " + call + " r from (select * from t where " + condition + ") timestamp(ts)",
                            "select " + call + " filter (where " + condition + ") r from t"
                    );
                }
            }
        });
    }

    @Test
    public void testRejectedAggregatesAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            createData();
            for (Map.Entry<String, String> e : REJECTED.entrySet()) {
                assertException(
                        "select " + e.getValue() + " filter (where " + CONDITIONS[0] + ") r from t",
                        7,
                        "FILTER is not supported for '" + e.getKey() + "'"
                );
            }
        });
    }

    private static boolean isTestOnly(ObjList<FunctionFactoryDescriptor> descriptors) {
        for (int i = 0, n = descriptors.size(); i < n; i++) {
            if (!descriptors.getQuick(i).getFactory().getClass().getName()
                    .startsWith("io.questdb.griffin.engine.functions.test.")) {
                return false;
            }
        }
        return true;
    }

    private void createData() throws Exception {
        // Two columns exist purely so that certain aggregates can tell a working lowering from a
        // broken one.
        //
        // d2 must not be a linear function of d. Every scale-invariant statistic - corr, regr_slope,
        // regr_r2, regr_intercept - is constant on collinear data, so those four aggregates would
        // return the same value for every condition and could never fail.
        //
        // bitv takes three values chosen so that an AND reduction is not swallowed. bit_and over a
        // column of consecutive integers collapses to 0 for every subset; here the three values share
        // high bits, and dropping the id > 55 group raises bit_and from 8 to 12 while dropping the
        // id < 6 group lowers bit_or from 14 to 12.
        execute("""
                CREATE TABLE t AS (SELECT
                  x id,
                  CASE WHEN x % 5 = 0 THEN null ELSE x::int END i,
                  CASE WHEN x % 13 = 0 THEN null WHEN x > 55 THEN 8 WHEN x < 6 THEN 14 ELSE 12 END::int bitv,
                  CASE WHEN x % 9 = 0 THEN null ELSE x END l,
                  CASE WHEN x % 7 = 0 THEN null ELSE x::double END d,
                  CASE WHEN x % 6 = 0 THEN null ELSE ((x * 7) % 13)::double END d2,
                  CASE WHEN x % 4 = 0 THEN null ELSE ('s' || x) END s,
                  x % 2 = 0 b,
                  CASE WHEN x % 11 = 0 THEN null ELSE ARRAY[x::double, (x + 1)::double] END arr,
                  timestamp_sequence(0, 100_000) ts
                FROM long_sequence(60)) TIMESTAMP(ts) PARTITION BY DAY""");
    }
}
