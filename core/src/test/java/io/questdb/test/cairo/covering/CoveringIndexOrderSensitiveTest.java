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

    /**
     * mode() is classified order-sensitive because the winner among values tied on count falls
     * out of the count map's slot order, which linear probing makes a function of insertion
     * order. This pins the consequence: over a time bucket, which draws from many keys, the scan
     * must keep the merge. sum() is the control -- same query shape, order-invariant aggregate,
     * and it DOES get per-key -- so a regression that stopped consulting the flag at this site
     * would show as this test going per-key while the control stayed unchanged.
     */
    @Test
    public void testModeGroupedByTimeBucketKeepsTheMerge() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryWithNulls();
            assertQuery("SELECT ts, mode(value) FROM telemetry WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('10s',ts)]
                                  values: [mode(value)]
                                  filter: null
                                    CoveringIndex on: param_id with: ts, value
                                      filter: param_id IN ['SFID','HOTMIC']
                                        Frame forward scan on: telemetry
                            """);
            assertQuery("SELECT ts, sum(value) FROM telemetry WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('10s',ts)]
                                  values: [sum(value)]
                                  filter: null
                                    CoveringIndex on: param_id with: ts, value
                                      frames: per-key (unordered)
                                      filter: param_id IN ['SFID','HOTMIC']
                                        Frame forward scan on: telemetry
                            """);
            assertSameResult(
                    "SELECT ts, mode(value) FROM telemetry" +
                            " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 10s",
                    "SELECT /*+ no_index */ ts, mode(value) FROM telemetry" +
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

    /**
     * The shape the refusal actually cost users, and the one no test covered while the refusal was
     * in place: {@code first()}/{@code last()} over a {@code LIKE} filter on a POSTING-indexed
     * symbol. Every {@code First*}/{@code Last*} function is order-sensitive and
     * {@code supportsParallelism()}, so they reach the async group-by sites and hit the guard --
     * which made a mainstream time-series query throw on this branch while stock master answered
     * it. See {@link #testAsyncKeyedOrderSensitiveOverPatternFilterMatchesFullScan()} for why the
     * guard was wrong.
     * <p>
     * The NOT-KEYED arm is the load-bearing one. Its single group draws from BOTH matching keys, so
     * it is the only arm where key-major arrival and timestamp arrival can disagree -- and the
     * fixture is built so they do, with {@code 'AB'} owning the earliest row and {@code 'AA'} the
     * latest. Timestamp order gives {@code first = 20.0, last = 11.0}; one-key-at-a-time order
     * would give {@code 10.0} and {@code 21.0}. The keyed arm cannot discriminate -- a group
     * confined to one key is ascending either way -- but is kept because it is the query users
     * write.
     */
    @Test
    public void testFirstLastFamilyOverPatternFilterMatchesFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSymbolPatternTable();
            final String[] aggs = {"first", "last", "first_not_null", "last_not_null"};
            for (String agg : aggs) {
                final String notKeyed = "SELECT " + agg + "(price) FROM pattern_tel_x WHERE sym LIKE 'A%'";
                assertQuery(notKeyed).noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");
                assertQuery(notKeyed).noLeakCheck().assertsPlanContaining("Async Group By");
                assertSameResult(
                        notKeyed,
                        "SELECT /*+ no_index */ " + agg + "(price) FROM pattern_tel_x WHERE sym LIKE 'A%'"
                );

                final String keyed = "SELECT sym, " + agg + "(price) FROM pattern_tel_x" +
                        " WHERE sym LIKE 'A%' ORDER BY sym";
                assertQuery(keyed).noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");
                assertQuery(keyed).noLeakCheck().assertsPlanContaining("Async Group By");
                assertSameResult(
                        keyed,
                        "SELECT /*+ no_index */ sym, " + agg + "(price) FROM pattern_tel_x" +
                                " WHERE sym LIKE 'A%' ORDER BY sym"
                );
            }
            // Pin the discriminating values outright, so a future change that makes BOTH arms
            // key-major still fails here rather than agreeing on a wrong answer.
            assertQuery("SELECT first(price) f, last(price) l FROM pattern_tel_x WHERE sym LIKE 'A%'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("f\tl\n20.0\t11.0\n");
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
     * {@code array_agg()} over a pattern filter on a POSTING-indexed symbol, at the async KEYED
     * group-by site. This pinned a REFUSAL until the final whole-branch review; it now pins the
     * answer, which is also what stock master returns.
     * <p>
     * <b>Why the refusal was wrong.</b> It was a false positive, not a conservative trade. The
     * order-sensitivity guard used to read
     * {@link io.questdb.cairo.sql.RecordCursorFactory#getScanDirection()}, and
     * {@code AdaptiveSymbolPatternRecordCursorFactory} has to answer THAT conservatively across
     * every delegate it may open -- including the bitmap-index delegate, whose key-by-key drain
     * genuinely is unordered. But that delegate has no page frames to give, so
     * {@code getPageFrameCursor()} never returns it, and {@code getCursor()} opens it only when
     * there is no covering delegate at all. The fixture below declares {@code INCLUDE (price)}, so
     * a covering delegate exists and NO route this factory can take opens the delegate whose
     * direction was making the guard fire. The guard was asking the wrong factory the wrong
     * question: it protects a page-frame consumer, so it now asks
     * {@code getPageFrameScanDirection()}, which describes only the delegates such a consumer can
     * be served by.
     * <p>
     * The guard is NOT weakened. Where the base is genuinely unordered it still fires; the tests in
     * this class that pin the k-way merge being kept for a time-bucket grouping are untouched and
     * still pass.
     * <p>
     * <b>What the {@code ARRAY[...]} literal is still doing here.</b> It was added as a
     * native-memory detector for a separate fix -- the guard must run ABOVE the ownership transfer
     * at this site, because the transfer nulls {@code innerProjectionFunctions} and
     * {@code outerProjectionFunctions}, the only handles {@code generateGroupBy}'s {@code catch}
     * can free assembled functions through. A {@code DirectArray} is tagged
     * {@code NATIVE_ND_ARRAY}, so a leak there is visible to {@code assertMemoryLeak}, which
     * heap-only projection functions are not; the ordering fix was verified by mutation at the
     * time ({@code NATIVE_ND_ARRAY, difference: 24}). That fix stays in the code, but with the
     * refusal gone there is no longer a reachable throw at this site, so this test can no longer
     * cover it -- it now asserts only that the SUCCESS path leaks nothing.
     */
    @Test
    public void testAsyncKeyedOrderSensitiveOverPatternFilterMatchesFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            final String indexed = "SELECT sym, array_agg(price) x, ARRAY[1.0,2.0,3.0] z FROM pattern_tel" +
                    " WHERE sym LIKE 'A%' ORDER BY sym";
            // Both arms must not be the same plan, or the comparison below proves nothing.
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("Async Group By");
            assertSameResult(
                    indexed,
                    "SELECT /*+ no_index */ sym, array_agg(price) x, ARRAY[1.0,2.0,3.0] z FROM pattern_tel" +
                            " WHERE sym LIKE 'A%' ORDER BY sym"
            );
        });
    }

    /**
     * The async KEYED twin of
     * {@link #testAsyncKeyedOrderSensitiveOverPatternFilterMatchesFullScan()}: same code site, same
     * reasoning for why the refusal was a false positive, different position for the array literal.
     * Here it is the grouping key rather than an extra projection column, so the key-rewrite loop
     * replaces the outer entry and the parsed original becomes reachable only through its paired
     * inner slot -- the one branch of
     * {@code GroupByUtils.freeAssembledProjectionFunctions} that the sibling test does not walk.
     * <p>
     * The key is constant, so every matching row lands in one group and {@code array_agg()} renders
     * the arrival order directly: this asserts the rows reach the aggregate in timestamp order.
     */
    @Test
    public void testAsyncKeyedOrderSensitiveWithArrayKeyMatchesFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            final String indexed = "SELECT ARRAY[1.0,2.0] k, array_agg(price) FROM pattern_tel WHERE sym LIKE 'A%'";
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("Async Group By");
            assertSameResult(
                    indexed,
                    "SELECT /*+ no_index */ ARRAY[1.0,2.0] k, array_agg(price) FROM pattern_tel WHERE sym LIKE 'A%'"
            );
        });
    }

    /**
     * The async NOT-KEYED site: no grouping column at all, so codegen takes the
     * {@code keyTypesCopy.getColumnCount() == 0} branch. Same false-positive refusal until the
     * final review, same reasoning -- see
     * {@link #testAsyncKeyedOrderSensitiveOverPatternFilterMatchesFullScan()}.
     * <p>
     * This site has no native-memory detector and never had one: every projection entry here is an
     * aggregate, and any constant subexpression that could own a {@code DirectArray} is folded away
     * before a Function is built. Four shapes were tried against a mutation of this site alone --
     * {@code array_agg(price * ARRAY[2.0,3.0][1])}, {@code array_agg(price)[1]},
     * {@code sum(ARRAY[1.0,5.0][2]), array_agg(price)} and
     * {@code array_agg(price + ARRAY[1.0,2.0][2]), array_agg(price)} -- and none leaked. Recorded
     * so the gap is not rediscovered as a finding.
     */
    @Test
    public void testAsyncNotKeyedOrderSensitiveOverPatternFilterMatchesFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            final String indexed = "SELECT array_agg(price) FROM pattern_tel WHERE sym LIKE 'A%'";
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");
            assertQuery(indexed).noLeakCheck().assertsPlanContaining("Async Group By");
            assertSameResult(
                    indexed,
                    "SELECT /*+ no_index */ array_agg(price) FROM pattern_tel WHERE sym LIKE 'A%'"
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
    /**
     * sum()/avg() over DECIMAL256 carry a FIXED 256-bit running sum that THROWS
     * {@code Overflow in addition} on a partial sum outside 2^255, and DECIMAL256 is the widest
     * type there is, so unlike every other decimal sum/avg they have nothing to widen to. That
     * makes the ERROR a function of arrival order even though the VALUE is not: the fixture
     * alternates {@code +10^76-1} and {@code -10^76-1} between two keys, so in timestamp order no
     * partial sum ever exceeds one operand and the answer is exactly 0, while key-major arrival
     * delivers sixty same-sign additions and the sixth leaves the range.
     * <p>
     * Declared order-insensitive this threw where stock master answered {@code 0}. The assertions
     * are the VALUE, not merely the absence of an exception -- a throw is the failure that was
     * reported, but an accumulator silently wrapping instead would be worse.
     * <p>
     * The {@code sum(value)} control is what stops this being vacuous. It is the same query over
     * the same fixture with a DOUBLE column, and it DOES take per-key, so the merge below is kept
     * because of the AGGREGATE and not because the fixture is too small or too sparse to qualify.
     */
    @Test
    public void testSumAvgOverDecimal256KeepTheMerge() throws Exception {
        assertMemoryLeak(() -> {
            createAlternatingDecimal256Table();
            assertQuery("SELECT sum(value) FROM dec_tel WHERE param_id IN ('A','B')")
                    .noLeakCheck()
                    .assertsPlanContaining("frames: per-key (unordered)");
            for (String agg : new String[]{"sum(v)", "avg(v)", "avg(v, 0)"}) {
                final String indexed = "SELECT " + agg + " FROM dec_tel WHERE param_id IN ('A','B')";
                assertQuery(indexed).noLeakCheck().assertsPlanNotContaining("frames: per-key");
                assertSameResult(
                        indexed,
                        "SELECT /*+ no_index */ " + agg + " FROM dec_tel WHERE param_id IN ('A','B')"
                );
            }
            assertQuery("SELECT sum(v) s, avg(v) a, avg(v, 0) r FROM dec_tel WHERE param_id IN ('A','B')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("s\ta\tr\n0\t0\t0\n");
        });
    }

    /**
     * Two keys whose rows INTERLEAVE in time and whose values cancel pairwise: {@code 'A'} holds
     * the largest DECIMAL(76,0) there is and {@code 'B'} its negation. Timestamp order therefore
     * keeps every partial sum inside one operand; key-major order does not.
     * <p>
     * Sixty rows per key, not six, because per-key mode declines below ~32 rows per
     * (key, partition) pair. A six-row fixture would fall back to the merge on density and prove
     * nothing about the aggregate. The DOUBLE {@code value} column carries the control query that
     * pins the fixture really is per-key-eligible.
     */
    private void createAlternatingDecimal256Table() throws Exception {
        final String max = "9999999999999999999999999999999999999999999999999999999999999999999999999999";
        execute("CREATE TABLE dec_tel (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (v, value)," +
                "  v DECIMAL(76,0)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO dec_tel SELECT" +
                " x::timestamp," +
                " CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END," +
                " CASE WHEN x % 2 = 0 THEN '" + max + "'::DECIMAL(76,0)" +
                "      ELSE '-" + max + "'::DECIMAL(76,0) END," +
                " x::double" +
                " FROM long_sequence(120)");
    }

    /**
     * {@link #createSymbolPatternTable()}'s shape, but with the two matching keys INTERLEAVED so
     * that key-major arrival and timestamp arrival disagree: {@code 'AB'} owns the earliest row and
     * {@code 'AA'} the latest. Without that the two orders coincide and an order-sensitivity
     * assertion over this fixture would be vacuous.
     * <p>
     * Four matching rows in 1004 is 0.4%, comfortably inside the 2% share that admits the covering
     * route and so puts the factory in wrapped mode -- the one mode that supplies page frames, and
     * therefore the only one that reaches the guarded async group-by sites.
     */
    private void createInterleavedSymbolPatternTable() throws Exception {
        execute("CREATE TABLE pattern_tel_x (" +
                "  sym SYMBOL INDEX TYPE POSTING INCLUDE (price)," +
                "  price DOUBLE," +
                "  ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO pattern_tel_x VALUES" +
                " ('AB', 20.0, 0), ('AA', 10.0, 1), ('AB', 21.0, 2), ('AA', 11.0, 3)");
        execute("INSERT INTO pattern_tel_x SELECT 'BA', x::DOUBLE, timestamp_sequence(4, 1) FROM long_sequence(1000)");
    }

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
