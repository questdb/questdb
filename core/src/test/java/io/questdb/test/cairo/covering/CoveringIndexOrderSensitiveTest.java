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

public class CoveringIndexOrderSensitiveTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testFirstGroupedByIndexKeyIsNotRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();
            // first() is order-sensitive and the grouping is exactly the index column, so the
            // base accepts the offer and drops to per-key frames. The guard must NOT fire: the
            // base owns the claim that this arrangement is legal, and the values must still be
            // each key's earliest row, not "whichever key was scanned first".
            assertQuery("SELECT param_id, first(value) FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns(
                            "param_id\tfirst\n" +
                                    "SFID\t4.0\n" +
                                    "HOTMIC\t1.0\n"
                    );
        });
    }

    @Test
    public void testFirstGroupedByIndexKeyTakesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryWithNulls();
            // Grouping is exactly the index column, so the scan drops the k-way merge:
            // "frames: per-key (unordered)" is the line that proves it.
            assertQuery("SELECT param_id, first(value) FROM telemetry WHERE param_id IN ('SFID','HOTMIC')")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              keys: [param_id]
                              values: [first(value)]
                              filter: null
                                CoveringIndex on: param_id with: value
                                  frames: per-key (unordered)
                                  filter: param_id IN ['SFID','HOTMIC']
                                    Frame forward scan on: telemetry
                            """);
        });
    }

    @Test
    public void testFirstGroupedByTimeBucketKeepsTheMerge() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryWithNulls();
            // SAMPLE BY groups by time bucket, so a bucket draws from many keys. Per-key
            // would return "whichever key was scanned first" -- 373/389 buckets wrong when
            // this was measured. The plan must NOT say per-key.
            assertQuery("SELECT ts, first(value) FROM telemetry WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('10s',ts)]
                                  values: [first(value)]
                                  filter: null
                                    CoveringIndex on: param_id with: ts, value
                                      filter: param_id IN ['SFID','HOTMIC']
                                        Frame forward scan on: telemetry
                            """);
            assertSameResult(
                    "SELECT ts, first(value) FROM telemetry" +
                            " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s",
                    "SELECT /*+ no_index */ ts, first(value) FROM telemetry" +
                            " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s"
            );
        });
    }

    @Test
    public void testFirstLastFamilyGroupedByIndexKeyMatchesFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryWithNulls();
            final String[] aggs = {"first", "last", "first_not_null", "last_not_null"};
            for (String agg : aggs) {
                final String indexed = "SELECT param_id, " + agg + "(value) FROM telemetry" +
                        " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id";
                // Pin every aggregate to the path it is meant to exercise. Without this the
                // value comparison below would also pass if an aggregate quietly fell back to
                // the k-way merge, and only first() would have direct per-key evidence.
                assertQuery(indexed).noLeakCheck().assertsPlanContaining("frames: per-key");
                assertSameResult(
                        indexed,
                        "SELECT /*+ no_index */ param_id, " + agg + "(value) FROM telemetry" +
                                " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id"
                );
            }
        });
    }

    @Test
    public void testFirstLastFamilyOverManyPartitionsMatchesFullScan() throws Exception {
        assertMemoryLeak(() -> {
            // ~70 daily partitions, so the per-key frame SEQUENCE for one key spans many
            // partitions. That is the invariant acceptance rests on: per-key iterates
            // partitions OUTER, so a single key's frames still arrive in ascending partition
            // order and frame order within a key IS timestamp order. Over the single-partition
            // fixtures this is vacuous -- those tests would pass even if a key's partitions
            // came back shuffled. first()/last() here can only be right if the order holds.
            createTelemetryMultiPartition();
            final String[] aggs = {"first", "last", "first_not_null", "last_not_null"};
            for (String agg : aggs) {
                final String indexed = "SELECT param_id, " + agg + "(value) FROM telemetry" +
                        " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id";
                assertQuery(indexed).noLeakCheck().assertsPlanContaining("frames: per-key");
                assertSameResult(
                        indexed,
                        "SELECT /*+ no_index */ param_id, " + agg + "(value) FROM telemetry" +
                                " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id"
                );
            }
        });
    }

    @Test
    public void testParallelOrderSensitiveAggregatesGroupedByIndexKeyTakePerKey() throws Exception {
        assertMemoryLeak(() -> {
            // The acceptance half of the twap() case. Grouping is exactly the index column, so
            // per-key IS legal: a single key's frames still arrive in ascending partition order,
            // hence in timestamp order, which is what twap()'s step-function integration needs.
            // ~70 partitions, so one key's frame sequence really does span many of them.
            createTelemetryMultiPartition();
            final String indexed = "SELECT param_id, twap(value, ts) FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id";
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("frames: per-key");
            assertSameResult(
                    indexed,
                    "SELECT /*+ no_index */ param_id, twap(value, ts) FROM telemetry" +
                            " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id"
            );
        });
    }

    @Test
    public void testParallelOrderSensitiveAggregatesMatchFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryWithNulls();
            // array_agg(), sparkline() and twap() are parallel-capable, so they reach the
            // already-wired async group-by site, and none of them carried isOrderSensitive()
            // before this audit. Each query below groups by a time bucket, not by the index
            // key, so the covering scan must DECLINE the ordering opt-out and keep the k-way
            // merge. Unflagged, the offer carried orderSensitive == false, the scan dropped to
            // one frame per key, and every bucket then saw its rows key-major rather than in
            // timestamp order: array_agg() and sparkline() render that order directly, and
            // twap() bridges the gap between two keys' observations as if it were elapsed time.
            final String[] projections = {"array_agg(value)", "sparkline(value)", "twap(value, ts)"};
            for (String projection : projections) {
                final String indexed = "SELECT ts, " + projection + " FROM telemetry" +
                        " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s";
                assertQuery(indexed).noLeakCheck().assertsPlanContaining("CoveringIndex on: param_id");
                assertQuery(indexed).noLeakCheck().assertsPlanNotContaining("frames: per-key");
                assertSameResult(
                        indexed,
                        "SELECT /*+ no_index */ ts, " + projection + " FROM telemetry" +
                                " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s"
                );
            }
        });
    }

    @Test
    public void testSampleByOverManyPartitionsKeepsTheMerge() throws Exception {
        assertMemoryLeak(() -> {
            // The negative case over the same multi-partition data: a time bucket draws from
            // several keys, so per-key frames can never satisfy it. The offer must be declined
            // and the k-way merge kept, across partitions as well as within one.
            createTelemetryMultiPartition();
            final String indexed = "SELECT ts, first(value) FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 1h";
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("CoveringIndex on: param_id");
            assertQuery(indexed).noLeakCheck().assertsPlanNotContaining("frames: per-key");
            assertSameResult(
                    indexed,
                    "SELECT /*+ no_index */ ts, first(value) FROM telemetry" +
                            " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 1h"
            );
        });
    }
}
