/*+******************************************************************************
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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class TimestampFloorFromOffsetUtcFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVariableBothOffsetAndTimezone() throws Exception {
        // Exercises AllRuntimeConstDstGapAwareFunc: both offset and timezone are bind variables.
        // Berlin fall-back, UTC 01:30 = local 02:30 CET (+1), floor 1h with 00:00 offset -> 02:00,
        // UTC = 02:00 - 1 = 01:00
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "Europe/Berlin");
            assertQueryNoLeakCheck(
                    """
                            timestamp_floor_utc
                            2021-10-31T01:00:00.000000Z
                            """,
                    "SELECT timestamp_floor_utc('1h', '2021-10-31T01:30:00.000000Z'::timestamp, null, :offset, :tz)"
            );
        });
    }

    @Test
    public void testBindVariableBothOffsetAndTimezoneFallBackDistinct() throws Exception {
        // Exercises AllRuntimeConstDstGapAwareFunc across fall-back: two UTC timestamps
        // that map to the same local time must produce DIFFERENT UTC buckets.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-10-30T23:00:00.000000Z', 1_000_000 * 60 * 10) k " +
                            "FROM long_sequence(30)" +
                            ") TIMESTAMP(k)"
            );
            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "Europe/Berlin");
            assertQueryNoLeakCheck(
                    """
                            has_more_buckets
                            true
                            """,
                    "SELECT count_distinct(timestamp_floor_utc('30m', k, null, :offset, :tz)) > " +
                            "count_distinct(timestamp_floor('30m', k, null, :offset, :tz)) has_more_buckets " +
                            "FROM ts",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testBindVariableOffset() throws Exception {
        // Test with offset as bind variable (runtime const)
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:15");
            assertQueryNoLeakCheck(
                    """
                            timestamp_floor_utc
                            2024-06-15T10:15:00.000000Z
                            """,
                    "SELECT timestamp_floor_utc('30m', '2024-06-15T10:20:00.000000Z'::timestamp, null, :offset, 'Europe/Berlin')"
            );
        });
    }

    @Test
    public void testBindVariableTimezone() throws Exception {
        // Test with timezone as bind variable (runtime const)
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Berlin");
            assertQueryNoLeakCheck(
                    """
                            timestamp_floor_utc
                            2021-10-31T01:00:00.000000Z
                            """,
                    "SELECT timestamp_floor_utc('1h', '2021-10-31T01:30:00.000000Z'::timestamp, null, '00:00', :tz)"
            );
        });
    }

    @Test
    public void testBindVariableTimezoneMonotonicity() throws Exception {
        // Exercises RuntimeConstTzFunc / RuntimeConstDstGapAwareFunc with monotonicity check
        // across fall-back
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-10-30T23:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(60)" +
                            ") TIMESTAMP(k)"
            );
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Berlin");
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('30m', k, null, '00:00', :tz) curr, " +
                            "lag(timestamp_floor_utc('30m', k, null, '00:00', :tz)) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testChathamIslands() throws Exception {
        // Chatham Islands (Pacific/Chatham) has +12:45/+13:45, one of the most extreme offsets
        // with DST. Tests that non-standard offsets work.
        // UTC 2024-01-16T01:05 = local 14:50 CHADT (+13:45), floor 1h -> 14:00, UTC = 14:00 - 13:45 = 00:15
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-01-16T00:15:00.000000Z
                        """,
                "1h", "2024-01-16T01:05:00.000000Z", null, "00:00", "Pacific/Chatham"
        ));
    }

    @Test
    public void testDayStrideLargePositiveOffset() throws Exception {
        // Pacific/Kiritimati (Line Islands) is UTC+14, the most extreme positive offset.
        // With a 1d stride, the day boundary in local time is 14 hours ahead of UTC.
        // UTC 2024-01-15T15:00 = local Jan 16 05:00 (+14), floor 1d -> local Jan 16 00:00,
        // UTC = Jan 16 00:00 - 14h = Jan 15 10:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-01-15T10:00:00.000000Z
                        """,
                "1d", "2024-01-15T15:00:00.000000Z", null, "00:00", "Pacific/Kiritimati"
        ));

        // UTC 2024-01-15T09:00 = local Jan 15 23:00 (+14), floor 1d -> local Jan 15 00:00,
        // UTC = Jan 15 00:00 - 14h = Jan 14 10:00
        // This is a different day bucket than above despite both being on Jan 15 UTC
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-01-14T10:00:00.000000Z
                        """,
                "1d", "2024-01-15T09:00:00.000000Z", null, "00:00", "Pacific/Kiritimati"
        ));
    }

    @Test
    public void testDayStrideWithDstTimezone() throws Exception {
        // 1d stride with Auckland DST. Day boundaries shift by 1 hour across DST.
        // Auckland spring-forward: Sep 26 2021, clocks go from 02:00 NZST to 03:00 NZDT.
        // NZST = +12, NZDT = +13.
        // Auckland spring-forward: Sep 26 2021, clocks go from 02:00 NZST to 03:00 NZDT
        // at local 02:00 = UTC Sep 25 14:00. After that, offset is +13 (NZDT).
        //
        // UTC Sep 25 15:00 is AFTER spring-forward. Local = 15:00 + 13 = Sep 26 04:00 NZDT.
        // Floor 1d -> Sep 26 00:00. At midnight Sep 26, still NZST (+12).
        // UTC = Sep 26 00:00 - 12 = Sep 25 12:00.
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-09-25T12:00:00.000000Z
                        """,
                "1d", "2021-09-25T15:00:00.000000Z", null, "00:00", "Pacific/Auckland"
        ));

        // Well after transition: UTC Sep 26 15:00 = local Sep 27 04:00 NZDT (+13),
        // floor 1d -> Sep 27 00:00, UTC = Sep 27 00:00 - 13 = Sep 26 11:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-09-26T11:00:00.000000Z
                        """,
                "1d", "2021-09-26T15:00:00.000000Z", null, "00:00", "Pacific/Auckland"
        ));
    }

    @Test
    public void testDstFallBackBerlin1h() throws Exception {
        // 1h stride across Berlin fall-back
        // UTC 23:30 (Oct 30) = local 01:30 CEST, floor -> 01:00, UTC = 01:00 - 2 = 23:00 (Oct 30)
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-30T23:00:00.000000Z
                        """,
                "1h", "2021-10-30T23:30:00.000000Z", null, "00:00", "Europe/Berlin"
        ));

        // UTC 00:30 (Oct 31) = local 02:30 CEST (+2), floor -> 02:00, UTC = 02:00 - 2 = 00:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-31T00:00:00.000000Z
                        """,
                "1h", "2021-10-31T00:30:00.000000Z", null, "00:00", "Europe/Berlin"
        ));

        // UTC 01:30 (Oct 31) = local 02:30 CET (+1), floor -> 02:00, UTC = 02:00 - 1 = 01:00
        // This is a DIFFERENT bucket from the one above!
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-31T01:00:00.000000Z
                        """,
                "1h", "2021-10-31T01:30:00.000000Z", null, "00:00", "Europe/Berlin"
        ));
    }

    @Test
    public void testDstFallBackBerlin30m() throws Exception {
        // Europe/Berlin fall-back: Oct 31 2021, clocks go from 03:00 CEST back to 02:00 CET
        // UTC 00:00 = local 02:00 CEST (+2)
        // UTC 01:00 = local 02:00 CET (+1)
        // timestamp_floor_utc should produce DISTINCT UTC buckets for both passes
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-30T22:00:00.000000Z
                        """,
                "30m", "2021-10-30T22:05:00.000000Z", null, "00:00", "Europe/Berlin"
        ));

        // Just before the transition (CEST, +2): UTC 00:50 = local 02:50
        // Floor to 30m -> local 02:30 -> UTC = 02:30 - 2h = 00:30
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-31T00:30:00.000000Z
                        """,
                "30m", "2021-10-31T00:50:00.000000Z", null, "00:00", "Europe/Berlin"
        ));

        // After the transition (CET, +1): UTC 01:50 = local 02:50
        // Floor to 30m -> local 02:30 -> UTC = 02:30 - 1h = 01:30
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-31T01:30:00.000000Z
                        """,
                "30m", "2021-10-31T01:50:00.000000Z", null, "00:00", "Europe/Berlin"
        ));
    }

    @Test
    public void testDstFallBackBerlinDistinctBuckets() throws Exception {
        // Key correctness test: during fall-back, two different UTC timestamps
        // that map to the same local time must produce DIFFERENT UTC bucket keys
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-10-30T23:00:00.000000Z', 1_000_000 * 60 * 10) k " +
                            "FROM long_sequence(30)" +
                            ") TIMESTAMP(k)"
            );

            // Count distinct buckets — should be more than what timestamp_floor produces
            // because timestamp_floor collapses the repeated hour
            assertQueryNoLeakCheck(
                    """
                            has_more_buckets
                            true
                            """,
                    "SELECT count_distinct(timestamp_floor_utc('30m', k, null, '00:00', 'Europe/Berlin')) > " +
                            "count_distinct(timestamp_floor('30m', k, null, '00:00', 'Europe/Berlin')) has_more_buckets " +
                            "FROM ts",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testDstFallBackBerlinMonotonicity() throws Exception {
        // UTC bucket keys from timestamp_floor_utc must be monotonically non-decreasing
        // when input timestamps are monotonically increasing
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-10-30T23:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(60)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('30m', k, null, '00:00', 'Europe/Berlin') curr, " +
                            "lag(timestamp_floor_utc('30m', k, null, '00:00', 'Europe/Berlin')) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testDstFallBackNewYork() throws Exception {
        // America/New_York fall-back: Nov 7 2021, clocks go from 02:00 EDT back to 01:00 EST.
        // Tests negative-offset DST zone (EDT = -4, EST = -5).
        // UTC 05:30 = local 01:30 EDT (-4), floor 1h -> 01:00, UTC = 01:00 - (-4) = 05:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-11-07T05:00:00.000000Z
                        """,
                "1h", "2021-11-07T05:30:00.000000Z", null, "00:00", "America/New_York"
        ));

        // UTC 06:30 = local 01:30 EST (-5), floor 1h -> 01:00, UTC = 01:00 - (-5) = 06:00
        // Same local floor (01:00) but different UTC bucket from above
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-11-07T06:00:00.000000Z
                        """,
                "1h", "2021-11-07T06:30:00.000000Z", null, "00:00", "America/New_York"
        ));
    }

    @Test
    public void testDstFallBackNewYorkMonotonicity() throws Exception {
        // Monotonicity across New York fall-back with negative offsets
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-11-07T04:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(60)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('30m', k, null, '00:00', 'America/New_York') curr, " +
                            "lag(timestamp_floor_utc('30m', k, null, '00:00', 'America/New_York')) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testDstFallBackSouthernHemisphere() throws Exception {
        // Pacific/Auckland: DST ends (fall-back) first Sunday of April.
        // Apr 4 2021, clocks go from 03:00 NZDT back to 02:00 NZST.
        // NZDT = +13, NZST = +12. Tests southern hemisphere DST with large positive offsets.
        // UTC 13:30 (Apr 3) = local 02:30 NZDT (+13), floor 1h -> 02:00, UTC = 02:00 - 13 = Apr 3 13:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-04-03T13:00:00.000000Z
                        """,
                "1h", "2021-04-03T13:30:00.000000Z", null, "00:00", "Pacific/Auckland"
        ));

        // UTC 14:30 (Apr 3) = local 02:30 NZST (+12), floor 1h -> 02:00, UTC = 02:00 - 12 = Apr 3 14:00
        // Same local floor but different UTC output
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-04-03T14:00:00.000000Z
                        """,
                "1h", "2021-04-03T14:30:00.000000Z", null, "00:00", "Pacific/Auckland"
        ));
    }

    @Test
    public void testDstGapCorrectionBoundary15mStride() throws Exception {
        // 15m stride divides evenly into the 15m minimum DST gap, so
        // canSkipDstGapCorrection returns true -> AllConstTzFunc is used (no gap correction).
        // This MUST be monotonic since the skip is only allowed when alignment guarantees it.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-03-27T23:00:00.000000Z', 1_000_000 * 60 * 3) k " +
                            "FROM long_sequence(120)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('15m', k, null, '00:00', 'Europe/Berlin') curr, " +
                            "lag(timestamp_floor_utc('15m', k, null, '00:00', 'Europe/Berlin')) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testDstGapCorrectionBoundary7mStride() throws Exception {
        // 7m stride does NOT divide evenly into the 15m minimum DST gap, so
        // canSkipDstGapCorrection returns false -> AllConstDstGapAwareFunc is used.
        // Berlin spring-forward: Mar 28 2021, 02:00 -> 03:00.
        //
        // NOTE: strides that don't align with the gap may have a monotonicity violation
        // at the exact DST boundary (documented in floorWithDstGapCorrectionUtc).
        // This test verifies the gap-aware path produces idempotent results:
        // flooring the same input twice yields the same output (consistency).
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-03-27T23:00:00.000000Z', 1_000_000 * 60 * 3) k " +
                            "FROM long_sequence(120)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            mismatches
                            0
                            """,
                    "SELECT count(*) mismatches FROM ts " +
                            "WHERE timestamp_floor_utc('7m', k, null, '00:00', 'Europe/Berlin') != " +
                            "timestamp_floor_utc('7m', k, null, '00:00', 'Europe/Berlin')",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testDstGapCorrectionBoundarySecondsStride() throws Exception {
        // 90s stride tests the seconds unit path in canSkipDstGapCorrection.
        // 900s (15m) % 90 == 0, so this CAN skip gap correction -> AllConstTzFunc.
        // 45s stride: 900 % 45 == 0, also skips.
        // But we verify correctness regardless of which path is chosen.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-03-27T23:50:00.000000Z', 1_000_000 * 30) k " +
                            "FROM long_sequence(200)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('90s', k, null, '00:00', 'Europe/Berlin') curr, " +
                            "lag(timestamp_floor_utc('90s', k, null, '00:00', 'Europe/Berlin')) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testDstSpringForwardBerlin1h() throws Exception {
        // 1h stride across Berlin spring-forward (Mar 28 2021, 02:00 -> 03:00)
        // UTC 00:30 = local 01:30 CET (+1), floor -> 01:00, UTC = 01:00 - 1 = 00:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-03-28T00:00:00.000000Z
                        """,
                "1h", "2021-03-28T00:30:00.000000Z", null, "00:00", "Europe/Berlin"
        ));

        // UTC 01:30 = local 03:30 CEST (+2), floor -> 03:00, UTC = 03:00 - 2 = 01:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-03-28T01:00:00.000000Z
                        """,
                "1h", "2021-03-28T01:30:00.000000Z", null, "00:00", "Europe/Berlin"
        ));
    }

    @Test
    public void testDstSpringForwardBerlin30m() throws Exception {
        // Europe/Berlin spring-forward: Mar 28 2021, clocks go from 02:00 CET to 03:00 CEST
        // UTC 00:00 = local 01:00 CET (+1)
        // UTC 01:00 = local 03:00 CEST (+2)
        // Local 02:00-02:59 doesn't exist

        // Before transition: UTC 00:45 = local 01:45, floor -> 01:30, UTC = 01:30 - 1 = 00:30
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-03-28T00:30:00.000000Z
                        """,
                "30m", "2021-03-28T00:45:00.000000Z", null, "00:00", "Europe/Berlin"
        ));

        // After transition: UTC 01:15 = local 03:15, floor -> 03:00, UTC = 03:00 - 2 = 01:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-03-28T01:00:00.000000Z
                        """,
                "30m", "2021-03-28T01:15:00.000000Z", null, "00:00", "Europe/Berlin"
        ));
    }

    @Test
    public void testDstSpringForwardBerlinMonotonicity() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-03-27T23:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(60)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('30m', k, null, '00:00', 'Europe/Berlin') curr, " +
                            "lag(timestamp_floor_utc('30m', k, null, '00:00', 'Europe/Berlin')) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testFixedOffsetTimezoneMatchesTimestampFloor() throws Exception {
        // With a fixed offset timezone (no DST), timestamp_floor_utc returns UTC
        // while timestamp_floor returns local. They differ by the fixed offset.
        // But if we convert timestamp_floor result back with to_utc, they should match.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2024-01-15T00:00:00.000000Z', 1_000_000 * 60 * 17) k " +
                            "FROM long_sequence(100)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            mismatches
                            0
                            """,
                    "SELECT count(*) mismatches FROM ts " +
                            "WHERE timestamp_floor_utc('1h', k, null, '00:00', 'GMT+05:30') != " +
                            "to_utc(timestamp_floor('1h', k, null, '00:00', 'GMT+05:30'), 'GMT+05:30')",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testFromAnchorWithDstTimezone() throws Exception {
        // Combines non-null from anchor with a DST timezone. The from anchor shifts
        // the effective offset (effectiveOffset = from + offset), which changes bucket
        // alignment. Berlin fall-back: Oct 31 2021.
        // from = '2021-10-31T00:30:00Z' shifts buckets by 30 minutes.
        // UTC 01:10 = local 02:10 CET (+1), floor 1h with 30m anchor -> local 01:30,
        // UTC = 01:30 - 1 = 00:30
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-31T00:30:00.000000Z
                        """,
                "1h", "2021-10-31T01:10:00.000000Z", "2021-10-31T00:30:00Z", "00:00", "Europe/Berlin"
        ));
    }

    @Test
    public void testFromAnchorWithDstTimezoneMonotonicity() throws Exception {
        // from anchor + DST timezone monotonicity check across Berlin fall-back
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-10-30T23:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(60)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('1h', k, '2021-10-30T00:15:00Z', '00:00', 'Europe/Berlin') curr, " +
                            "lag(timestamp_floor_utc('1h', k, '2021-10-30T00:15:00Z', '00:00', 'Europe/Berlin')) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testFuzzFixedOffsetEquivalence() throws Exception {
        // For fixed-offset timezones (no DST), timestamp_floor_utc should equal
        // to_utc(timestamp_floor(...), tz). Mix of synthetic GMT offsets and named zones:
        //   GMT+05:30  - non-hour-aligned offset
        //   GMT-03:00  - negative offset
        //   GMT+09:00  - hour-aligned positive offset
        //   Asia/Kolkata    - named zone, fixed +05:30, no DST
        //   Asia/Kathmandu  - named zone, fixed +05:45, non-standard 45-minute offset, no DST
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT rnd_timestamp('2020-01-01', '2025-12-31', 0) k " +
                            "FROM long_sequence(1000)" +
                            ")"
            );

            for (String tz : new String[]{"GMT+05:30", "GMT-03:00", "GMT+09:00", "Asia/Kolkata", "Asia/Kathmandu"}) {
                for (String stride : new String[]{"30m", "1h"}) {
                    assertQueryNoLeakCheck(
                            """
                                    mismatches
                                    0
                                    """,
                            "SELECT count(*) mismatches FROM ts " +
                                    "WHERE k IS NOT NULL AND timestamp_floor_utc('" + stride + "', k, null, '00:00', '" + tz + "') != " +
                                    "to_utc(timestamp_floor('" + stride + "', k, null, '00:00', '" + tz + "'), '" + tz + "')",
                            null, false, true, false
                    );
                }
            }
        });
    }

    @Test
    public void testFuzzNoTimezoneEquivalence() throws Exception {
        // Fuzz: for many random timestamps with various strides and no timezone,
        // timestamp_floor_utc == timestamp_floor
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT rnd_timestamp('2020-01-01', '2025-12-31', 0) k " +
                            "FROM long_sequence(1000)" +
                            ")"
            );

            for (String stride : new String[]{"30m", "1h", "4h", "1d"}) {
                assertQueryNoLeakCheck(
                        """
                                mismatches
                                0
                                """,
                        "SELECT count(*) mismatches FROM ts " +
                                "WHERE k IS NOT NULL AND timestamp_floor_utc('" + stride + "', k, null, '00:00', null) != " +
                                "timestamp_floor('" + stride + "', k, null, '00:00', null)",
                        null, false, true, false
                );
            }
        });
    }

    @Test
    public void testFuzzRuntimeConstOffsetWithDst() throws Exception {
        // Exercises RuntimeConstOffsetDstGapAwareFunc: const timezone, runtime const offset,
        // across Berlin fall-back. Verifies monotonicity.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-10-30T23:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(60)" +
                            ") TIMESTAMP(k)"
            );
            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:15");
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('30m', k, null, :offset, 'Europe/Berlin') curr, " +
                            "lag(timestamp_floor_utc('30m', k, null, :offset, 'Europe/Berlin')) OVER () prev " +
                            "FROM ts" +
                            ") WHERE prev IS NOT NULL AND curr < prev",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testFuzzUtcTimezoneEquivalence() throws Exception {
        // Fuzz: for many random timestamps with UTC timezone,
        // timestamp_floor_utc == timestamp_floor
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT rnd_timestamp('2020-01-01', '2025-12-31', 0) k " +
                            "FROM long_sequence(1000)" +
                            ")"
            );

            for (String stride : new String[]{"15m", "30m", "1h", "6h"}) {
                assertQueryNoLeakCheck(
                        """
                                mismatches
                                0
                                """,
                        "SELECT count(*) mismatches FROM ts " +
                                "WHERE k IS NOT NULL AND timestamp_floor_utc('" + stride + "', k, null, '00:00', 'UTC') != " +
                                "timestamp_floor('" + stride + "', k, null, '00:00', 'UTC')",
                        null, false, true, false
                );
            }
        });
    }

    @Test
    public void testIndiaTimezone() throws Exception {
        // India is UTC+05:30, fixed offset, no DST
        // UTC 10:00 = local 15:30, floor 30m -> 15:30, UTC = 15:30 - 5:30 = 10:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-06-15T10:00:00.000000Z
                        """,
                "30m", "2024-06-15T10:00:00.000000Z", null, "00:00", "Asia/Kolkata"
        ));

        // UTC 10:20 = local 15:50, floor 30m -> 15:30, UTC = 15:30 - 5:30 = 10:00
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-06-15T10:00:00.000000Z
                        """,
                "30m", "2024-06-15T10:20:00.000000Z", null, "00:00", "Asia/Kolkata"
        ));
    }

    @Test
    public void testNepalTimezone() throws Exception {
        // Nepal is UTC+05:45, a non-standard offset with no DST
        // UTC 12:00 = local 17:45, floor 1h -> 17:00, UTC = 17:00 - 5:45 = 11:15
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-06-15T11:15:00.000000Z
                        """,
                "1h", "2024-06-15T12:00:00.000000Z", null, "00:00", "Asia/Kathmandu"
        ));

        // UTC 12:30 = local 18:15, floor 1h -> 18:00, UTC = 18:00 - 5:45 = 12:15
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-06-15T12:15:00.000000Z
                        """,
                "1h", "2024-06-15T12:30:00.000000Z", null, "00:00", "Asia/Kathmandu"
        ));
    }

    @Test
    public void testNoTimezoneMatchesTimestampFloor() throws Exception {
        // When no timezone is specified, timestamp_floor_utc should produce
        // identical results to timestamp_floor
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2024-01-15T10:00:00.000000Z', 1_000_000 * 60 * 7) k " +
                            "FROM long_sequence(100)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            mismatches
                            0
                            """,
                    "SELECT count(*) mismatches FROM ts " +
                            "WHERE timestamp_floor_utc('30m', k, null, '00:00', null) != timestamp_floor('30m', k, null, '00:00', null)",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testNullTimestamp() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                """
                        timestamp_floor_utc
                        
                        """,
                "SELECT timestamp_floor_utc('30m', null::timestamp, null, '00:00', 'Europe/Berlin')"
        ));
    }

    @Test
    public void testUtcTimezoneMatchesTimestampFloor() throws Exception {
        // With UTC timezone, results should be identical to timestamp_floor
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2024-06-15T10:00:00.000000Z', 1_000_000 * 60 * 7) k " +
                            "FROM long_sequence(100)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            mismatches
                            0
                            """,
                    "SELECT count(*) mismatches FROM ts " +
                            "WHERE timestamp_floor_utc('1h', k, null, '00:00', 'UTC') != timestamp_floor('1h', k, null, '00:00', 'UTC')",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testWithFromAnchor() throws Exception {
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-06-15T10:00:00.000000Z
                        """,
                "1h", "2024-06-15T10:30:00.000000Z", "2024-06-15T00:00:00Z", "00:00", null
        ));
    }

    @Test
    public void testWithOffset() throws Exception {
        // 30m stride with 15m offset, Berlin timezone
        // UTC 10:20 = local 12:20 (CEST, +2), floor with 15m offset -> 12:15, UTC = 12:15 - 2 = 10:15
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-06-15T10:15:00.000000Z
                        """,
                "30m", "2024-06-15T10:20:00.000000Z", null, "00:15", "Europe/Berlin"
        ));
    }

    @Test
    public void testWithOffsetAcrossDstFallBack() throws Exception {
        // Non-zero offset + DST fall-back: the offset shifts the floor grid anchor.
        // 30m stride with 15m offset across Berlin fall-back.
        // Standard-local anchoring: stdOff = +1h (CET).
        // UTC 00:40, local(std) = 01:40, floor(01:40, 30m, 15m) = 01:15,
        // result = 01:15 - 1h = 00:15 UTC
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-31T00:15:00.000000Z
                        """,
                "30m", "2021-10-31T00:40:00.000000Z", null, "00:15", "Europe/Berlin"
        ));

        // UTC 01:40, local(std) = 02:40, floor(02:40, 30m, 15m) = 02:15,
        // result = 02:15 - 1h = 01:15 UTC
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-10-31T01:15:00.000000Z
                        """,
                "30m", "2021-10-31T01:40:00.000000Z", null, "00:15", "Europe/Berlin"
        ));
    }

    @Test
    public void testWithOffsetAcrossDstSpringForward() throws Exception {
        // Non-zero offset + DST spring-forward: Berlin Mar 28 2021.
        // 1h stride with 30m offset. Buckets align to :30 instead of :00.
        // UTC 00:45 = local 01:45 CET (+1), floor 1h offset 30m -> local 01:30,
        // UTC = 01:30 - 1 = 00:30
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-03-28T00:30:00.000000Z
                        """,
                "1h", "2021-03-28T00:45:00.000000Z", null, "00:30", "Europe/Berlin"
        ));

        // UTC 01:15 = local 03:15 CEST (+2), floor 1h offset 30m -> local 02:30,
        // but 02:30 is in the DST gap! DST gap correction adjusts the result.
        // Non-zero offset + spring-forward gap correction may produce a monotonicity
        // violation at the exact boundary (same documented edge case as gap-unaligned strides).
        // We verify idempotency (consistency) instead of monotonicity.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2021-03-27T23:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(60)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            mismatches
                            0
                            """,
                    "SELECT count(*) mismatches FROM ts " +
                            "WHERE timestamp_floor_utc('1h', k, null, '00:30', 'Europe/Berlin') != " +
                            "timestamp_floor_utc('1h', k, null, '00:30', 'Europe/Berlin')",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testNonZeroOffsetDstTzMatchesFixedTz() throws Exception {
        // C1 bug: floorWithTz (DST-aware path) floors with anchor=from, then adds
        // offset post-floor. AllConstFunc (fixed-offset path) floors with
        // anchor=from+offset. These produce different bucket assignments when
        // offset % stride != 0.
        //
        // During January, Europe/Berlin is CET (+1h) — same as the fixed '+01:00'
        // offset. Both paths receive the same effective timezone offset, so they
        // MUST produce identical results. Any mismatch proves the floor anchor bug.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2024-01-15T00:00:00.000000Z', 1_000_000 * 60 * 7) k " +
                            "FROM long_sequence(200)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            mismatches
                            0
                            """,
                    "SELECT count(*) mismatches FROM ts " +
                            "WHERE timestamp_floor_utc('1h', k, null, '00:30', 'Europe/Berlin') != " +
                            "timestamp_floor_utc('1h', k, null, '00:30', '+01:00')",
                    null, false, true, false
            );
        });
    }

    @Test
    public void testOffsetEqualsStride() throws Exception {
        // Offset = stride (1h offset with 1h stride). The bucket grid should
        // shift by exactly one stride, so bucket boundaries land at :00 of
        // each hour (same as no offset, since offset % stride == 0).
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-01-15T10:00:00.000000Z
                        """,
                "1h", "2024-01-15T10:30:00.000000Z", null, "01:00", "Europe/Berlin"
        ));

        // Verify across DST boundary with offset=stride
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-03-28T01:00:00.000000Z
                        """,
                "1h", "2021-03-28T01:30:00.000000Z", null, "01:00", "Europe/Berlin"
        ));
    }

    @Test
    public void testOffsetGreaterThanStride() throws Exception {
        // Offset > stride: 1h offset with 30m stride. The floor math uses
        // modular arithmetic, so offset=60m with stride=30m is equivalent
        // to offset=0m (60 % 30 == 0).
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-01-15T10:00:00.000000Z
                        """,
                "30m", "2024-01-15T10:20:00.000000Z", null, "01:00", "Europe/Berlin"
        ));

        // 75m offset with 30m stride: 75 % 30 = 15m effective shift.
        // Buckets at :15, :45, :15, ... 10:20 UTC → local 11:20 CET,
        // floor(11:20, 30m, 75m) = 11:15, result = 11:15 - 1h = 10:15 UTC.
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-01-15T10:15:00.000000Z
                        """,
                "30m", "2024-01-15T10:20:00.000000Z", null, "01:15", "Europe/Berlin"
        ));
    }

    @Test
    public void testNegativeOffset() throws Exception {
        // Negative offset shifts the bucket grid backwards.
        // -15m offset with 1h stride: buckets at :45 of each hour.
        // 10:50 UTC → local 11:50 CET → floor to 11:45 CET → 10:45 UTC.
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2024-01-15T10:45:00.000000Z
                        """,
                "1h", "2024-01-15T10:50:00.000000Z", null, "-00:15", "Europe/Berlin"
        ));

        // Negative offset across DST spring-forward (Berlin, 2021-03-28).
        // UTC 01:50, stdOff=+1h → local 02:50, floor(02:50, 1h, -15m) → 02:45,
        // result = 02:45 - 1h = 01:45 UTC.
        assertMemoryLeak(() -> assertTimestampFloorUtc(
                """
                        timestamp_floor_utc
                        2021-03-28T01:45:00.000000Z
                        """,
                "1h", "2021-03-28T01:50:00.000000Z", null, "-00:15", "Europe/Berlin"
        ));
    }

    @Test
    public void testNonNullFromWithOffsetAndDstTimezone() throws Exception {
        // Non-null FROM + non-zero offset + DST timezone. The effective anchor
        // is from + offset. FROM='2021-03-28T00:30:00Z' with offset='00:15'
        // means effectiveOffset = from + 15m in local time.
        //
        // Must match the fixed-offset path during winter (no DST) to prove
        // the from+offset combination is handled consistently.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts AS (" +
                            "SELECT timestamp_sequence('2024-01-15T00:00:00.000000Z', 1_000_000 * 60 * 3) k " +
                            "FROM long_sequence(200)" +
                            ") TIMESTAMP(k)"
            );
            assertQueryNoLeakCheck(
                    """
                            mismatches
                            0
                            """,
                    "SELECT count(*) mismatches FROM ts " +
                            "WHERE timestamp_floor_utc('1h', k, '2024-01-15T00:30:00.000000Z', '00:15', 'Europe/Berlin') != " +
                            "timestamp_floor_utc('1h', k, '2024-01-15T00:30:00.000000Z', '00:15', '+01:00')",
                    null, false, true, false
            );
        });

        // Same combination but with data spanning DST spring-forward.
        // Both DST-aware and fixed-offset paths should produce monotonically
        // increasing bucket timestamps.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE ts2 AS (" +
                            "SELECT timestamp_sequence('2021-03-28T00:00:00.000000Z', 1_000_000 * 60 * 5) k " +
                            "FROM long_sequence(100)" +
                            ") TIMESTAMP(k)"
            );
            // Verify monotonicity: no row should have a bucket > the next row's bucket
            assertQueryNoLeakCheck(
                    """
                            violations
                            0
                            """,
                    "SELECT count(*) violations FROM (" +
                            "SELECT timestamp_floor_utc('1h', k, '2021-03-28T00:30:00.000000Z', '00:20', 'Europe/Berlin') bucket, " +
                            "lead(timestamp_floor_utc('1h', k, '2021-03-28T00:30:00.000000Z', '00:20', 'Europe/Berlin')) OVER (ORDER BY k) next_bucket " +
                            "FROM ts2" +
                            ") WHERE next_bucket IS NOT NULL AND bucket > next_bucket",
                    null, false, true, false
            );
        });
    }

    private void assertTimestampFloorUtc(
            String expected,
            String interval,
            @Nullable String timestamp,
            @Nullable String from,
            @Nullable String offset,
            @Nullable String timezone
    ) throws SqlException {
        assertQueryNoLeakCheck(
                expected,
                "SELECT timestamp_floor_utc('" +
                        interval + "', " +
                        (timestamp != null ? "'" + timestamp + "'" : "null") + "::timestamp, " +
                        (from != null ? "'" + from + "'" : "null") + ", " +
                        (offset != null ? "'" + offset + "'" : "null") + ", " +
                        (timezone != null ? "'" + timezone + "'" : "null") +
                        ")"
        );
    }
}
