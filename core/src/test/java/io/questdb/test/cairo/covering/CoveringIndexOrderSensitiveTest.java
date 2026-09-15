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

    private static final String ORDER_SENSITIVE_REJECTION =
            "base query does not provide ASC order over designated TIMESTAMP column, required by an order-sensitive aggregate";

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

    /**
     * The guard at the async KEYED group-by site must throw BEFORE the ownership transfer.
     * <p>
     * The transfer nulls {@code innerProjectionFunctions} and {@code outerProjectionFunctions},
     * and those two variables are the only handles through which the {@code catch} at the end of
     * {@code generateGroupBy} can reach the assembled functions: with both null,
     * {@code GroupByUtils.freeAssembledProjectionFunctions} returns at its very first branch
     * without closing anything. A throw placed between the transfer and the constructor therefore
     * closes nothing that the constructor would have adopted.
     * <p>
     * The {@code ARRAY[...]} literal in the projection is load-bearing, not decoration. Most
     * projection functions hold only heap state, so failing to close them is invisible to
     * {@code assertMemoryLeak} and a test built on them would pass with the defect present. An
     * array literal owns a {@code DirectArray}, whose backing store is native and tagged
     * {@code NATIVE_ND_ARRAY}. Verified by mutation: with the offer and the guard moved back below
     * this site's transfer block, this test fails with
     * {@code Memory usage by tag: NATIVE_ND_ARRAY, difference: 24 expected:<0> but was:<24>} --
     * eight bytes per element of the three-element literal, and on the leak check rather than on
     * the exception, which still throws exactly as asserted.
     */
    @Test
    public void testAsyncKeyedGuardRejectionFreesAssembledFunctions() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            assertExceptionNoLeakCheck(
                    "SELECT sym, array_agg(price) x, ARRAY[1.0,2.0,3.0] z FROM pattern_tel WHERE sym LIKE 'A%'",
                    0,
                    ORDER_SENSITIVE_REJECTION
            );
        });
    }

    /**
     * The async NOT-KEYED twin of
     * {@link #testAsyncKeyedGuardRejectionFreesAssembledFunctions()}: same transfer/guard ordering
     * hazard, same native-array detector, same code site, different position for the array: here
     * it is the grouping key rather than an extra projection column, so the key-rewrite loop
     * replaces the outer entry and the parsed original becomes reachable only through its paired
     * inner slot. That is the one branch of
     * {@code GroupByUtils.freeAssembledProjectionFunctions} that the sibling test does not walk.
     */
    @Test
    public void testAsyncKeyedGuardRejectionFreesAssembledFunctionsWithArrayKey() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            assertExceptionNoLeakCheck(
                    "SELECT ARRAY[1.0,2.0] k, array_agg(price) FROM pattern_tel WHERE sym LIKE 'A%'",
                    0,
                    ORDER_SENSITIVE_REJECTION
            );
        });
    }

    /**
     * The async NOT-KEYED site: no grouping column at all, so codegen takes the
     * {@code keyTypesCopy.getColumnCount() == 0} branch, which has its own transfer block with the
     * same hazard and received the same fix.
     * <p>
     * This test pins REACHABILITY only, not the transfer ordering, and the distinction is
     * deliberate. A native-memory detector needs a projection entry that owns native memory at
     * compile time, and at this site every projection entry is an aggregate: any constant
     * subexpression that could own a {@code DirectArray} is folded away before a Function is
     * built. Four shapes were tried against a mutation of this site alone --
     * {@code array_agg(price * ARRAY[2.0,3.0][1])}, {@code array_agg(price)[1]},
     * {@code sum(ARRAY[1.0,5.0][2]), array_agg(price)} and
     * {@code array_agg(price + ARRAY[1.0,2.0][2]), array_agg(price)} -- and none leaked, because
     * constant folding removes the array before codegen. So this site's ordering is currently
     * unobservable, and would become observable the moment a not-keyed projection function owns
     * native memory.
     */
    @Test
    public void testAsyncNotKeyedGuardRejectionIsReachableWithoutAKey() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(price) FROM pattern_tel WHERE sym LIKE 'A%'",
                    0,
                    ORDER_SENSITIVE_REJECTION
            );
        });
    }

    /**
     * A POSTING-indexed symbol whose pattern filter admits under 2% of rows, which is what puts
     * {@code AdaptiveSymbolPatternRecordCursorFactory} into its wrapped mode. Wrapped mode is the
     * one configuration that both supplies page frames -- so the group by is generated at an
     * ASYNC site -- and advertises {@code SCAN_DIRECTION_OTHER}, because one of the delegates it
     * may open is the cursor-order symbol-pattern index scan. That combination is what makes the
     * guard's throw reachable at all. A covering {@code latestBy} base cannot do it: {@code
     * latestBy} leaves both page-frame cursors null, so it is always generated serially, and the
     * serial sites carry no guard.
     */
    private void createSymbolPatternTable() throws Exception {
        execute("CREATE TABLE pattern_tel (" +
                "  sym SYMBOL INDEX TYPE POSTING INCLUDE (price)," +
                "  price DOUBLE," +
                "  ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO pattern_tel VALUES ('AA', 1.0, 0), ('AB', 2.0, 1)");
        execute("INSERT INTO pattern_tel SELECT 'BA', x::DOUBLE, timestamp_sequence(2, 1) FROM long_sequence(1000)");
    }
}
