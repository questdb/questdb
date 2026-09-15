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

package io.questdb.test.cairo.covering;

import org.junit.Test;

/**
 * End-to-end coverage of the reported flight-telemetry query shape: a four-key
 * {@code IN} list, {@code SAMPLE BY 10s} with {@code avg()}, and time bounds
 * supplied by scalar subqueries.
 * <p>
 * Two things must be true and both are asserted: the query takes the per-key
 * (unordered) scan because {@code avg()} is order-insensitive, AND the
 * subquery-derived bounds still reach the partition frame cursor as a resolved
 * interval -- the original report's plan showed no interval line at all, which
 * is what prompted printing the covering node's partition frame cursor child.
 */
public class CoveringIndexTelemetryShapeTest extends AbstractCoveringIndexQueryTest {

    // createTelemetryWithNulls() and createFlights() come from the shared fixture.
    private static final String FLIGHT_QUERY =
            "SELECT ts, avg(value), param_id FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC','KCAS','CALT')" +
                    " AND ts >= (SELECT start_time FROM flights WHERE flight_ground = 100)" +
                    " AND ts <  (SELECT end_time FROM flights WHERE flight_ground = 100)" +
                    " SAMPLE BY 10s";

    @Test
    public void testFlightShapeTakesPerKeyAndMatchesFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            // The plan contains BOTH:
            //   frames: per-key (unordered)      -- avg() is order-insensitive
            //   Interval forward scan on: telemetry + a resolved intervals: [...] line
            // The second is the point: bounds from a scalar subquery still prune
            // partitions, and RuntimeIntervalModel resolves them at plan time. The
            // upper bound is "< end_time" (08:00) and the printed interval correctly
            // shows an inclusive end one microsecond below it (07.999999).
            assertQuery(FLIGHT_QUERY)
                    .noLeakCheck()
                    .assertsPlan("Encode sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,param_id]\n" +
                            "      keyFunctions: [timestamp_floor_utc('10s',ts)]\n" +
                            "      values: [avg(value)]\n" +
                            "      filter: null\n" +
                            "        CoveringIndex on: param_id with: ts, value\n" +
                            "          frames: per-key (unordered)\n" +
                            "          filter: param_id IN ['SFID','HOTMIC','KCAS','CALT']\n" +
                            "            Interval forward scan on: telemetry\n" +
                            "              intervals: [(\"1970-01-01T00:00:02.000000Z\",\"1970-01-01T00:00:07.999999Z\")]\n");
            // The 6 s flight window falls inside a single 10 s bucket, so all four keys'
            // rows land in the SAME group-by bucket. Row order across keys within one
            // bucket is implementation-defined (per-key iterates the IN-list key order;
            // the no_index arm's serial group-by iterates its own hash-map order), so an
            // ORDER BY is required to make the raw-text comparison meaningful -- without
            // it the two arms can (and do) disagree on row order while agreeing on rows.
            final String orderedSql = FLIGHT_QUERY + " ORDER BY param_id";
            assertSameResult(orderedSql,
                    orderedSql.replace("SELECT ts,", "SELECT /*+ no_index */ ts,"));
        });
    }

    /**
     * {@code first()} is order-sensitive, but this query's grouping key set is
     * {@code (ts bucket, param_id)}, not the time bucket alone: {@code param_id} is
     * a plain (non-aggregated) SELECT column, so
     * {@link io.questdb.griffin.engine.groupby.GroupByUtils#assembleGroupByFunctions} folds it
     * into the GROUP BY key list the covering factory receives. The SAMPLE BY
     * timestamp bucket itself is excluded from that list (it is derived from the base
     * table's designated timestamp, not tracked as an ordinary key column), so the
     * covering factory sees exactly one grouping column -- param_id, its own index key --
     * and {@code groupsByIndexKeyOnly()} legitimately accepts the per-key offer, same as
     * {@link #testFlightShapeTakesPerKeyAndMatchesFullScan}. Swapping the aggregate alone
     * does NOT force the merge here; see
     * {@link #testFirstGroupedOnlyByTimeBucketKeepsTheMerge} for the shape that does.
     */
    @Test
    public void testFlightShapeWithFirstAlsoTakesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            final String sql = FLIGHT_QUERY.replace("avg(value)", "first(value)");
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("Encode sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,param_id]\n" +
                            "      keyFunctions: [timestamp_floor_utc('10s',ts)]\n" +
                            "      values: [first(value)]\n" +
                            "      filter: null\n" +
                            "        CoveringIndex on: param_id with: ts, value\n" +
                            "          frames: per-key (unordered)\n" +
                            "          filter: param_id IN ['SFID','HOTMIC','KCAS','CALT']\n" +
                            "            Interval forward scan on: telemetry\n" +
                            "              intervals: [(\"1970-01-01T00:00:02.000000Z\",\"1970-01-01T00:00:07.999999Z\")]\n");
            // Same single-bucket, multi-key ordering caveat as
            // testFlightShapeTakesPerKeyAndMatchesFullScan: order by param_id to make the
            // comparison deterministic.
            final String orderedSql = sql + " ORDER BY param_id";
            assertSameResult(orderedSql, orderedSql.replace("SELECT ts,", "SELECT /*+ no_index */ ts,"));
        });
    }

    /**
     * The shape {@link #testFlightShapeWithFirstAlsoTakesPerKey} was meant to pin: drop
     * {@code param_id} from the SELECT list so the ONLY grouping key left is the SAMPLE BY
     * time bucket. A bucket now spans rows from all four keys, interleaved, so first()
     * over a per-key (unordered) stream would silently return "whichever key's posting
     * list was drained first" instead of the earliest row -- {@code groupsByIndexKeyOnly()}
     * sees zero grouping columns (not one matching its own index column) and declines the
     * offer, so the merge is kept.
     */
    @Test
    public void testFirstGroupedOnlyByTimeBucketKeepsTheMerge() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            final String sql = "SELECT ts, first(value) FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC','KCAS','CALT')" +
                    " AND ts >= (SELECT start_time FROM flights WHERE flight_ground = 100)" +
                    " AND ts <  (SELECT end_time FROM flights WHERE flight_ground = 100)" +
                    " SAMPLE BY 10s";
            // No "frames: per-key" line, and the group-by key list is [ts] alone (not
            // [ts,param_id]): groupsByIndexKeyOnly() sees zero real key columns and
            // declines the offer, so ordering is left to the k-way merge.
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("Encode sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      keyFunctions: [timestamp_floor_utc('10s',ts)]\n" +
                            "      values: [first(value)]\n" +
                            "      filter: null\n" +
                            "        CoveringIndex on: param_id with: ts, value\n" +
                            "          filter: param_id IN ['SFID','HOTMIC','KCAS','CALT']\n" +
                            "            Interval forward scan on: telemetry\n" +
                            "              intervals: [(\"1970-01-01T00:00:02.000000Z\",\"1970-01-01T00:00:07.999999Z\")]\n");
            // A single bucket here yields exactly one output row, so no ordering
            // ambiguity -- unlike the two tests above where param_id is also selected.
            assertSameResult(sql, sql.replace("SELECT ts,", "SELECT /*+ no_index */ ts,"));
        });
    }

    @Test
    public void testOrderByTsLimitKeepsTheMerge() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            // Per-key here is O(n log n) against the merge's O(limit) -- measured at 298x
            // worse. This is a plain projection with a LIMIT, so it never reaches group-by
            // codegen and never sees the offer; the plan below pins that it stays that way
            // (no "frames: per-key" line).
            assertQuery("SELECT ts, value FROM telemetry WHERE param_id IN ('SFID','HOTMIC')" +
                    " ORDER BY ts LIMIT 10")
                    .noLeakCheck()
                    .assertsPlan("Limit value: 10 skip-rows-max: 0 take-rows-max: 10\n" +
                            "    SelectedRecord\n" +
                            "        CoveringIndex on: param_id with: ts, value\n" +
                            "          filter: param_id IN ['SFID','HOTMIC']\n" +
                            "            Frame forward scan on: telemetry\n");
        });
    }

    private void createSchema() throws Exception {
        createTelemetryWithNulls();
        createFlights();
    }
}
