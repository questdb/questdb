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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.SampleByIntervalIterator;
import io.questdb.cairo.mv.TimeZoneIntervalIterator;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TimeZoneIntervalIteratorTest extends AbstractIntervalIteratorTest {
    // Windows of about four days, from about 38 hours before a DST gap or fold of three zones, each [zone, start, end]:
    // Europe/Berlin moves its clocks at local 02:00, Africa/Cairo at local midnight, Australia/Lord_Howe by 30 minutes.
    private static final String[][] DST_SHIFT_WINDOWS = {
            {"Europe/Berlin", "2023-03-24T10:03:00.000000Z", "2023-03-28T12:01:00.000000Z"},
            {"Europe/Berlin", "2023-10-27T10:03:00.000000Z", "2023-10-31T12:01:00.000000Z"},
            {"Africa/Cairo", "2023-04-26T08:03:00.000000Z", "2023-04-30T10:01:00.000000Z"},
            {"Africa/Cairo", "2023-10-25T07:03:00.000000Z", "2023-10-29T09:01:00.000000Z"},
            {"Australia/Lord_Howe", "2023-09-29T00:03:00.000000Z", "2023-10-03T02:01:00.000000Z"},
            {"Australia/Lord_Howe", "2023-03-30T23:03:00.000000Z", "2023-04-04T01:01:00.000000Z"}
    };
    private static final Log LOG = LogFactory.getLog(TimeZoneIntervalIteratorTest.class);

    public TimeZoneIntervalIteratorTest(TimestampDriver timestampDriver) {
        super(timestampDriver);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {MicrosTimestampDriver.INSTANCE}, {NanosTimestampDriver.INSTANCE}
        });
    }

    @Test
    public void testBigStep() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-03-03T01:01:00.000000Z"),
                timestampDriver.parseFloorLiteral("2024-03-04T01:01:00.000000Z"),
                14
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-03T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-05T00:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-03T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-05T00:00:00.000000Z"), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testBucketFloor() throws Exception {
        // Daily buckets WITH OFFSET '02:30' in Europe/Berlin, which moves its clocks forward at 2023-03-26T01:00:00Z, local
        // 02:00 CET (+1h) to 03:00 CEST (+2h). The bucket of 2023-03-25 starts at local 02:30 CET, 01:30Z. The bucket of
        // 2023-03-26 would start at local 02:30, inside the gap, so it starts at the transition; the transition itself
        // still lies in the iterator's shift interval, which the bucket before the gap takes. The bucket of 2023-03-27
        // starts at local 02:30 CEST, 00:30Z.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                timestampDriver.fromMinutes(150),
                null,
                timestampDriver.parseFloorLiteral("2023-03-25T12:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2023-03-28T12:00:00.000000Z"),
                1
        );
        final String[][] floors = {
                {"2023-03-25T12:00:00.000000Z", "2023-03-25T01:30:00.000000Z"},
                {"2023-03-26T00:59:59.999999Z", "2023-03-25T01:30:00.000000Z"},
                {"2023-03-26T01:00:00.000000Z", "2023-03-25T01:30:00.000000Z"},
                {"2023-03-26T01:10:00.000000Z", "2023-03-26T01:00:00.000000Z"},
                {"2023-03-26T12:00:00.000000Z", "2023-03-26T01:00:00.000000Z"},
                {"2023-03-27T00:29:59.999999Z", "2023-03-26T01:00:00.000000Z"},
                {"2023-03-27T00:30:00.000000Z", "2023-03-27T00:30:00.000000Z"},
                {"2023-03-28T12:00:00.000000Z", "2023-03-28T00:30:00.000000Z"}
        };
        for (String[] floor : floors) {
            Assert.assertEquals(
                    floor[0],
                    timestampDriver.parseFloorLiteral(floor[1]),
                    iterator.bucketFloor(timestampDriver.parseFloorLiteral(floor[0]))
            );
        }
    }

    @Test
    public void testFixedOffset() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final long offset = timestampDriver.fromHours(1);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                offset,
                null,
                0,
                timestampDriver.fromDays(7) - 1,
                1
        );

        Assert.assertEquals(offset, iterator.getMinTimestamp());
        Assert.assertEquals(offset + timestampDriver.fromDays(7), iterator.getMaxTimestamp());

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(offset + timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(offset + timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testFuzzPerClusterDayBucketsAcrossDstGaps() throws Exception {
        // SAMPLE BY 1d and 1M with offsets anywhere in the day put bucket floors inside DST gaps and folds. Every
        // step-group must also hold whole buckets of the SQL, whose keys come from java.time.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (String[] window : DST_SHIFT_WINDOWS) {
            final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, window[0]);
            final ZoneId zone = ZoneId.of(window[0]);
            final long start = timestampDriver.parseFloorLiteral(window[1]);
            final long end = timestampDriver.parseFloorLiteral(window[2]);
            for (int i = 0; i < 200; i++) {
                testFuzzPerClusterDayBuckets(rnd, tzRules, zone, start, end);
            }
        }
    }

    @Test
    public void testFuzzPerClusterDayBucketsDrawFloorsInDstGaps() {
        // testFuzzPerClusterDayBucketsAcrossDstGaps must keep drawing intervals whose day bucket floor lies inside the
        // DST gap of each zone, where no local time exists, and inside its fold, where the local times repeat. The
        // shift and the floors come from java.time. The seed is fixed, so a failure reproduces.
        final Rnd rnd = new Rnd(42, 42);
        final StringBuilder report = new StringBuilder();
        int minFloorsInShift = Integer.MAX_VALUE;
        for (String[] window : DST_SHIFT_WINDOWS) {
            final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, window[0]);
            final ZoneId zone = ZoneId.of(window[0]);
            final long start = timestampDriver.parseFloorLiteral(window[1]);
            final long end = timestampDriver.parseFloorLiteral(window[2]);
            final ZoneOffsetTransition shift = zone.getRules().nextTransition(toInstant(start));
            // a gap spans the local times [before, after), a fold [after, before)
            final LocalDateTime shiftLo = shift.isGap() ? shift.getDateTimeBefore() : shift.getDateTimeAfter();
            final LocalDateTime shiftHi = shift.isGap() ? shift.getDateTimeAfter() : shift.getDateTimeBefore();
            int floorsInShift = 0;
            for (int i = 0; i < 500; i++) {
                final PerClusterFuzzCase fuzzCase = drawDayBucketFuzzCase(rnd, tzRules, start, end);
                if (fuzzCase.timeUnit != 'd') {
                    continue;
                }
                for (int j = 0, n = fuzzCase.intervals.size(); j < n; j += 2) {
                    final LocalDateTime floor = localBucketFloor(zone, 'd', fuzzCase.offset, fuzzCase.intervals.getQuick(j));
                    if (floor.isAfter(shiftLo) && floor.isBefore(shiftHi)) {
                        floorsInShift++;
                    }
                }
            }
            report.append(window[0]).append(shift.isGap() ? " gap: " : " fold: ").append(floorsInShift).append("; ");
            minFloorsInShift = Math.min(minFloorsInShift, floorsInShift);
        }
        Assert.assertTrue("too few day floors inside a DST shift: " + report, minFloorsInShift >= 50);
    }

    @Test
    public void testFuzzTimeZoneWithDst() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2021-03-26T10:03:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-29T12:01:00.000000Z")
        );
    }

    @Test
    public void testFuzzTimeZoneWithDstLargeInterval() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2020-01-01T00:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2030-01-01T00:00:00.000000Z")
        );
    }

    @Test
    public void testFuzzTimeZoneWithFixedOffset() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "GMT+2"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2000-01-01T23:10:00.000000Z"),
                timestampDriver.parseFloorLiteral("2000-01-02T20:59:59.000000Z")
        );
    }

    @Test
    public void testFuzzUTC() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2024-03-03T12:01:01.000000Z"),
                timestampDriver.parseFloorLiteral("2024-03-07T12:01:01.000000Z")
        );
    }

    @Test
    public void testIntervalsEmpty() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                new LongList(),
                0,
                timestampDriver.fromDays(7) - 1,
                1
        );

        Assert.assertEquals(0, iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.fromDays(7), iterator.getMaxTimestamp());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testIntervalsSingle() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC");
        final LongList intervals = new LongList();

        final long minTs = 0;
        final long maxTs = timestampDriver.fromDays(3) - 1;

        // match
        intervals.add(minTs, maxTs);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());

        // to the left
        intervals.clear();
        intervals.add(-2L, -1L);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the left with intersection
        intervals.clear();
        intervals.add(-2L, 0L);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // to the right
        intervals.clear();
        intervals.add(maxTs + 1, maxTs + 2);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the right with intersection
        intervals.clear();
        intervals.add(maxTs, maxTs + 2);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.fromDays(2), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(3), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // middle
        intervals.clear();
        intervals.add(timestampDriver.fromDays(1) + 1, timestampDriver.fromDays(2) - 1);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(2), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testIntervalsToTop() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC");
        final LongList intervals = new LongList();

        intervals.add(0, timestampDriver.fromDays(1) - 1);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, 0, timestampDriver.fromDays(3) - 1, 1);

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        iterator.toTop(1);

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testMonthNegativeOffset() throws Exception {
        // a 1M view with a negative offset starts each bucket 1h before its local month;
        // the local bucket starts stay at 23:00 while the UTC offset moves across the DST gap
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'M', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                -timestampDriver.fromHours(1),
                null,
                timestampDriver.parseFloorLiteral("2024-01-15T00:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2024-05-20T00:00:00.000000Z"),
                1
        );

        final LongList expectedBounds = new LongList();
        expectedBounds.add(timestampDriver.parseFloorLiteral("2023-12-31T22:00:00.000000Z"));
        expectedBounds.add(timestampDriver.parseFloorLiteral("2024-01-31T22:00:00.000000Z"));
        expectedBounds.add(timestampDriver.parseFloorLiteral("2024-02-29T22:00:00.000000Z"));
        expectedBounds.add(timestampDriver.parseFloorLiteral("2024-03-31T21:00:00.000000Z"));
        expectedBounds.add(timestampDriver.parseFloorLiteral("2024-04-30T21:00:00.000000Z"));
        expectedBounds.add(timestampDriver.parseFloorLiteral("2024-05-31T21:00:00.000000Z"));
        assertSteps(iterator, expectedBounds);
    }

    @Test
    public void testPerClusterStepSnapsAcrossDstFold() throws Exception {
        // Europe/Berlin fall-back: 2021-10-31T01:00:00Z (local clocks roll 03:00 -> 02:00).
        // Two clusters straddle the fold:
        //   cluster 0: UTC [23:30, 23:31] on Oct 30 (local 01:30 before fold, CEST/+2)
        //   cluster 1: UTC [03:30, 03:31] on Oct 31 (local 04:30 after fold, CET/+1)
        // The local fold interval [02:00, 03:00] sits between them, so on the
        // cluster-1 boundary snapToCluster() must walk past it, reset
        // shiftLoIndex / shiftOffset, and re-anchor utcTimestampHi to the
        // post-fold offset (+1h). 1h sampler.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'h', 0);

        final long minTs = timestampDriver.parseFloorLiteral("2021-10-30T22:00:00.000000Z");
        final long maxTs = timestampDriver.parseFloorLiteral("2021-10-31T05:30:00.000000Z");

        final LongList intervals = new LongList();
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-10-30T22:30:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-10-30T22:31:00.000000Z")
        );
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-10-31T03:30:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-10-31T03:31:00.000000Z")
        );

        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(1L);
        stepPerCluster.add(1L);

        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                intervals,
                minTs,
                maxTs,
                stepPerCluster
        );

        // Pre-fold cluster: the bucket containing 22:30 UTC -> [22:00, 23:00) UTC.
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T22:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:00:00.000000Z"), iterator.getTimestampHi());

        // Post-fold cluster after snapToCluster(). The bucket containing
        // 03:30 UTC on Oct 31 must come out as [03:00, 04:00) UTC -- a +1h
        // bucket, not a +2h one. If snapToCluster() failed to advance
        // shiftOffset to the post-fold offset, the UTC bucket boundaries
        // would shift by one hour.
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T03:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T04:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testPerClusterStepSnapsAcrossDstGap() throws Exception {
        // Europe/Berlin spring-forward: 2021-03-28T01:00:00Z (local clocks
        // jump 02:00 -> 03:00). Two clusters straddle the gap:
        //   cluster 0: UTC [23:30, 23:31] on Mar 27 (local 00:30 before gap, CET/+1)
        //   cluster 1: UTC [03:30, 03:31] on Mar 28 (local 05:30 after gap, CEST/+2)
        // The local gap interval [02:00, 03:00] sits between them. On the
        // cluster-1 boundary snapToCluster() must walk past it, advance
        // shiftOffset from +1h to +2h, and re-derive utcTimestampHi
        // accordingly. 1h sampler.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'h', 0);

        final long minTs = timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z");
        final long maxTs = timestampDriver.parseFloorLiteral("2021-03-28T05:30:00.000000Z");

        final LongList intervals = new LongList();
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-03-27T23:30:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-27T23:31:00.000000Z")
        );
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-03-28T03:30:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-28T03:31:00.000000Z")
        );

        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(1L);
        stepPerCluster.add(1L);

        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                intervals,
                minTs,
                maxTs,
                stepPerCluster
        );

        // Pre-gap cluster: [23:00, 00:00) UTC -- entirely in CET (+1h).
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:00:00.000000Z"), iterator.getTimestampHi());

        // Post-gap cluster after snapToCluster(). The bucket containing
        // 03:30 UTC on Mar 28 must come out as [03:00, 04:00) UTC -- a +2h
        // bucket. A regression where snapToCluster() failed to advance
        // shiftOffset past the gap would emit [02:00, 03:00) UTC instead.
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T04:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testFuzzPerClusterAcrossDstShifts() throws Exception {
        // Europe/Berlin moves its clocks forward on 2021-03-28 and back on 2021-10-31.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin");
        for (int i = 0; i < 200; i++) {
            testFuzzPerCluster(
                    rnd,
                    tzRules,
                    timestampDriver.parseFloorLiteral("2021-03-26T10:03:00.000000Z"),
                    timestampDriver.parseFloorLiteral("2021-03-30T12:01:00.000000Z")
            );
            testFuzzPerCluster(
                    rnd,
                    tzRules,
                    timestampDriver.parseFloorLiteral("2021-10-29T10:03:00.000000Z"),
                    timestampDriver.parseFloorLiteral("2021-11-02T12:01:00.000000Z")
            );
        }
    }

    @Test
    public void testFuzzPerClusterUtc() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC");
        for (int i = 0; i < 200; i++) {
            testFuzzPerCluster(
                    rnd,
                    tzRules,
                    timestampDriver.parseFloorLiteral("2024-03-03T12:01:01.000000Z"),
                    timestampDriver.parseFloorLiteral("2024-03-20T12:01:01.000000Z")
            );
        }
    }

    @Test
    public void testSecondClusterStartsAtTransitionWhenBucketFloorIsInDstGap() throws Exception {
        // The two-cluster form of testStepGroupStartsAtTransitionWhenBucketFloorIsInDstGap: the snap from the first
        // cluster's bucket to the second cluster, which starts in the bucket after the gap, lands on the transition.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final long firstTs = timestampDriver.parseFloorLiteral("2023-03-20T12:00:00.000000Z");
        final long secondTs = timestampDriver.parseFloorLiteral("2023-03-26T01:10:00.000000Z");
        final LongList intervals = new LongList();
        intervals.add(firstTs, firstTs);
        intervals.add(secondTs, secondTs);
        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(1L);
        stepPerCluster.add(1L);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                timestampDriver.fromMinutes(150),
                intervals,
                firstTs,
                secondTs,
                stepPerCluster
        );

        final LongList expected = new LongList();
        // local [2023-03-20T02:30, 2023-03-21T02:30) CET
        expected.add(timestampDriver.parseFloorLiteral("2023-03-20T01:30:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-21T01:30:00.000000Z"));
        // from the transition to local 2023-03-27T02:30 CEST
        expected.add(timestampDriver.parseFloorLiteral("2023-03-26T01:00:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-27T00:30:00.000000Z"));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testSecondClusterSnapsToDstGapEdgesAndFold() throws Exception {
        // Floors that exist next to a DST shift keep their snaps: a floor at the gap's start or end snaps to the
        // transition, and a floor inside the fold of 2023-10-29, local 02:00-03:00 twice, to its second pass, where the
        // rows of its bucket start. The transitions and floors come from java.time.
        final LongList expected = new LongList();
        expected.add(timestampDriver.parseFloorLiteral("2023-03-20T01:00:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-21T01:00:00.000000Z"));
        expected.add(timestampDriver.parseFloorLiteral("2023-03-26T01:00:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-27T00:00:00.000000Z"));
        assertStepGroupsOfTwoClusters(120, "2023-03-20T12:00", "2023-03-26T01:10", expected);

        expected.clear();
        expected.add(timestampDriver.parseFloorLiteral("2023-03-20T02:00:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-21T02:00:00.000000Z"));
        expected.add(timestampDriver.parseFloorLiteral("2023-03-26T01:00:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-27T01:00:00.000000Z"));
        assertStepGroupsOfTwoClusters(180, "2023-03-20T12:00", "2023-03-26T01:10", expected);

        expected.clear();
        expected.add(timestampDriver.parseFloorLiteral("2023-10-20T00:30:00.000000Z"), timestampDriver.parseFloorLiteral("2023-10-21T00:30:00.000000Z"));
        expected.add(timestampDriver.parseFloorLiteral("2023-10-29T01:30:00.000000Z"), timestampDriver.parseFloorLiteral("2023-10-30T01:30:00.000000Z"));
        assertStepGroupsOfTwoClusters(150, "2023-10-20T12:00", "2023-10-29T05:00", expected);
    }

    @Test
    public void testStepGroupCapsAtIntervalCeilAcrossDstGap() throws Exception {
        // The same shape as testStepGroupCapsAtIntervalCeilUtc, but anchored at real local days that
        // straddle Europe/Berlin's spring-forward (2021-03-28T01:00:00Z, local 02:00 -> 03:00).
        // Interval 1 is local [2021-03-27T00:00, 2021-03-28T00:00) (CET, +1h); the gap holds the
        // DST transition; interval 2 is local [2021-03-29T00:00, 2021-03-30T00:00) (CEST, +2h).
        // The cap must still end the first interval's step-group at its local-day ceiling, and the
        // snap across the gap must re-anchor to the post-transition +2h offset.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'h', 0);

        final LongList intervals = new LongList();
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-03-26T23:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z") - 1
        );
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-03-28T22:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-29T22:00:00.000000Z") - 1
        );

        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(10L);
        stepPerCluster.add(10L);

        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                intervals,
                timestampDriver.parseFloorLiteral("2021-03-26T23:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-29T22:00:00.000000Z") - 1,
                stepPerCluster
        );

        final LongList expected = new LongList();
        expected.add(timestampDriver.parseFloorLiteral("2021-03-26T23:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-27T09:00:00.000000Z"));
        expected.add(timestampDriver.parseFloorLiteral("2021-03-27T09:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-27T19:00:00.000000Z"));
        // capped at the first interval's local-day ceiling (local 2021-03-28T00:00 = UTC 23:00)
        expected.add(timestampDriver.parseFloorLiteral("2021-03-27T19:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z"));
        // snapped across the gap to the +2h post-transition interval
        expected.add(timestampDriver.parseFloorLiteral("2021-03-28T22:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-29T08:00:00.000000Z"));
        expected.add(timestampDriver.parseFloorLiteral("2021-03-29T08:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-29T18:00:00.000000Z"));
        expected.add(timestampDriver.parseFloorLiteral("2021-03-29T18:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-29T22:00:00.000000Z"));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testStepGroupCapsAtIntervalCeilUtc() throws Exception {
        // UTC analogue of FixedOffsetIntervalIteratorTest.testStepGroupCapsAtIntervalCeilAcrossGap:
        // two day-wide intervals [0h, 24h) and [48h, 72h) with a 10 one-hour-bucket step cap the
        // third step-group at 24h instead of overrunning to 30h across the gap.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'h', 0);

        final LongList intervals = new LongList();
        intervals.add(timestampDriver.fromHours(0), timestampDriver.fromHours(24) - 1);
        intervals.add(timestampDriver.fromHours(48), timestampDriver.fromHours(72) - 1);

        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(10L);
        stepPerCluster.add(10L);

        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                intervals,
                timestampDriver.fromHours(0),
                timestampDriver.fromHours(72) - 1,
                stepPerCluster
        );

        final LongList expected = new LongList();
        expected.add(timestampDriver.fromHours(0), timestampDriver.fromHours(10));
        expected.add(timestampDriver.fromHours(10), timestampDriver.fromHours(20));
        expected.add(timestampDriver.fromHours(20), timestampDriver.fromHours(24)); // capped, not 30h
        expected.add(timestampDriver.fromHours(48), timestampDriver.fromHours(58));
        expected.add(timestampDriver.fromHours(58), timestampDriver.fromHours(68));
        expected.add(timestampDriver.fromHours(68), timestampDriver.fromHours(72));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testStepGroupCrossingFallBackCapsAtIntervalCeil() throws Exception {
        // Europe/Berlin moves its clocks back at 2021-10-31T01:00:00Z: local 03:00 CEST (+2h) becomes
        // 02:00 CET (+1h), so local 02:00-03:00 happens twice. Interval 1 is UTC
        // [2021-10-30T20:00, 2021-10-31T06:00), local [22:00 CEST, 07:00 CET). Its first step-group
        // of 10 local one-hour buckets, local [22:00, 08:00), spans 11 real hours across the shift and
        // would end at 07:00Z; the cap ends it at the local ceiling of the interval's last bucket,
        // 07:00 CET = 06:00Z. Interval 2, UTC [2021-11-01T00:00, 05:00), lies after a gap.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'h', 0);

        final LongList intervals = new LongList();
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-10-30T20:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-10-31T06:00:00.000000Z") - 1
        );
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-11-01T00:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-11-01T05:00:00.000000Z") - 1
        );

        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(10L);
        stepPerCluster.add(10L);

        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                intervals,
                intervals.getQuick(0),
                intervals.getQuick(3),
                stepPerCluster
        );

        final LongList expected = new LongList();
        // capped at local 07:00 CET, not 08:00 CET = 07:00Z
        expected.add(timestampDriver.parseFloorLiteral("2021-10-30T20:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-10-31T06:00:00.000000Z"));
        // snapped across the gap
        expected.add(timestampDriver.parseFloorLiteral("2021-11-01T00:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-11-01T05:00:00.000000Z"));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testStepGroupCrossingSpringForwardCapsAtIntervalCeil() throws Exception {
        // Europe/Berlin moves its clocks forward at 2021-03-28T01:00:00Z: local 02:00 CET (+1h) becomes
        // 03:00 CEST (+2h), so local 02:00-03:00 never happens. Interval 1 is UTC
        // [2021-03-27T12:00, 2021-03-28T05:00), local [13:00 CET, 07:00 CEST). Its second step-group
        // of 10 local one-hour buckets, local [23:00, 09:00), spans 9 real hours across the shift and
        // would end at 07:00Z; the cap ends it at the local ceiling of the interval's last bucket,
        // 07:00 CEST = 05:00Z. Interval 2, UTC [2021-03-29T00:00, 10:00), lies after a gap.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'h', 0);

        final LongList intervals = new LongList();
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-03-27T12:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-28T05:00:00.000000Z") - 1
        );
        intervals.add(
                timestampDriver.parseFloorLiteral("2021-03-29T00:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-29T10:00:00.000000Z") - 1
        );

        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(10L);
        stepPerCluster.add(10L);

        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                intervals,
                intervals.getQuick(0),
                intervals.getQuick(3),
                stepPerCluster
        );

        final LongList expected = new LongList();
        expected.add(timestampDriver.parseFloorLiteral("2021-03-27T12:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-27T22:00:00.000000Z"));
        // capped at local 07:00 CEST, not 09:00 CEST = 07:00Z
        expected.add(timestampDriver.parseFloorLiteral("2021-03-27T22:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-28T05:00:00.000000Z"));
        // snapped across the gap
        expected.add(timestampDriver.parseFloorLiteral("2021-03-29T00:00:00.000000Z"), timestampDriver.parseFloorLiteral("2021-03-29T10:00:00.000000Z"));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testStepGroupDoesNotRecomputeSharedBucketTwice() throws Exception {
        // Two intervals meet inside the same 1-day bucket (day 0); a far third interval (day 5)
        // keeps the iteration going. With an overshooting 3-day step the first step-group ends at
        // day 0's bucket ceiling (day 1), so the shared day-0 bucket is emitted once.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);

        final LongList intervals = new LongList();
        intervals.add(timestampDriver.fromDays(0) + timestampDriver.fromHours(1), timestampDriver.fromDays(0) + timestampDriver.fromHours(5));
        intervals.add(timestampDriver.fromDays(0) + timestampDriver.fromHours(14), timestampDriver.fromDays(0) + timestampDriver.fromHours(20));
        intervals.add(timestampDriver.fromDays(5), timestampDriver.fromDays(5) + timestampDriver.fromHours(5));

        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(3L);
        stepPerCluster.add(3L);
        stepPerCluster.add(1L);

        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                intervals,
                timestampDriver.fromDays(0) + timestampDriver.fromHours(1),
                timestampDriver.fromDays(5) + timestampDriver.fromHours(5),
                stepPerCluster
        );

        final LongList expected = new LongList();
        expected.add(timestampDriver.fromDays(0), timestampDriver.fromDays(1)); // both day-0 intervals, once
        expected.add(timestampDriver.fromDays(5), timestampDriver.fromDays(6));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testStepGroupStartsAtTransitionAcrossDstGapZones() throws Exception {
        // An interval ten minutes after a DST gap's transition, in the bucket whose local floor lies inside the gap:
        // its step-group starts at the transition and ends at the next floor. The transitions and floors come from
        // java.time. A 30-minute gap, a gap at local midnight, a month bucket, a transition before 1970, which the
        // zone's history holds, and one in 2100, which its rules give.
        assertStepGroupOfInterval("Europe/Berlin", 'd', 121, "2023-03-26T01:10", "2023-03-26T01:00", "2023-03-27T00:01");
        assertStepGroupOfInterval("Europe/Berlin", 'd', 179, "2023-03-26T01:10", "2023-03-26T01:00", "2023-03-27T00:59");
        assertStepGroupOfInterval("America/New_York", 'd', 150, "2023-03-12T07:10", "2023-03-12T07:00", "2023-03-13T06:30");
        assertStepGroupOfInterval("Africa/Cairo", 'd', 30, "2023-04-27T22:10", "2023-04-27T22:00", "2023-04-28T21:30");
        assertStepGroupOfInterval("Europe/London", 'd', 90, "2023-03-26T01:10", "2023-03-26T01:00", "2023-03-27T00:30");
        assertStepGroupOfInterval("Europe/Helsinki", 'd', 210, "2023-03-26T01:10", "2023-03-26T01:00", "2023-03-27T00:30");
        assertStepGroupOfInterval("Australia/Lord_Howe", 'd', 135, "2023-09-30T15:40", "2023-09-30T15:30", "2023-10-01T15:15");
        assertStepGroupOfInterval("Australia/Sydney", 'M', 150, "2023-09-30T16:10", "2023-09-30T16:00", "2023-10-31T15:30");
        assertStepGroupOfInterval("America/New_York", 'd', 150, "1969-04-27T07:10", "1969-04-27T07:00", "1969-04-28T06:30");
        assertStepGroupOfInterval("Europe/Berlin", 'd', 150, "2100-03-28T01:10", "2100-03-28T01:00", "2100-03-29T00:30");
        // Floors that lie at the gap's start, at its end and inside a fold, where the local floor exists, keep
        // their step-groups: the transition at the gap's ends, and the second pass of the floor in the fold.
        assertStepGroupOfInterval("Europe/Berlin", 'd', 120, "2023-03-26T01:10", "2023-03-26T01:00", "2023-03-27T00:00");
        assertStepGroupOfInterval("Europe/Berlin", 'd', 180, "2023-03-26T01:10", "2023-03-26T01:00", "2023-03-27T01:00");
        assertStepGroupOfInterval("Europe/Berlin", 'd', 150, "2023-10-29T05:00", "2023-10-29T01:30", "2023-10-30T01:30");
    }

    @Test
    public void testStepGroupStartsAtTransitionWhenBucketFloorIsInDstGap() throws Exception {
        // Europe/Berlin moves its clocks forward at 2023-03-26T01:00:00Z: local 02:00 CET (+1h) becomes 03:00 CEST
        // (+2h). Under SAMPLE BY 1d WITH OFFSET '02:30', the bucket of 2023-03-26 starts at local 02:30, inside the
        // gap, so its first row is the one at the transition, while the rows before it, local 01:30-02:00 CET, belong
        // to the bucket of 2023-03-25. The step-group of an interval in the bucket after the gap starts at the
        // transition, not at 00:30Z, local 02:30 less the offset after the gap.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final long intervalTs = timestampDriver.parseFloorLiteral("2023-03-26T01:10:00.000000Z");
        final LongList intervals = new LongList();
        intervals.add(intervalTs, intervalTs);
        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(1L);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                timestampDriver.fromMinutes(150),
                intervals,
                intervalTs,
                intervalTs,
                stepPerCluster
        );

        // the walk starts at the bucket before the gap, local 2023-03-25T02:30 CET
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2023-03-25T01:30:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2023-03-27T00:30:00.000000Z"), iterator.getMaxTimestamp());
        final LongList expected = new LongList();
        expected.add(timestampDriver.parseFloorLiteral("2023-03-26T01:00:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-27T00:30:00.000000Z"));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testStepGroupStartsAtTransitionWhenBucketFloorIsInDstGapWithSingleStep() throws Exception {
        // testStepGroupStartsAtTransitionWhenBucketFloorIsInDstGap through the single-step mode, with a step of ten
        // buckets, which the snap shortens to the interval's bucket.
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final long intervalTs = timestampDriver.parseFloorLiteral("2023-03-26T01:10:00.000000Z");
        final LongList intervals = new LongList();
        intervals.add(intervalTs, intervalTs);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                timestampDriver.fromMinutes(150),
                intervals,
                intervalTs,
                intervalTs,
                10
        );

        final LongList expected = new LongList();
        expected.add(timestampDriver.parseFloorLiteral("2023-03-26T01:00:00.000000Z"), timestampDriver.parseFloorLiteral("2023-03-27T00:30:00.000000Z"));
        assertStepGroups(iterator, expected);
    }

    @Test
    public void testSmoke() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                null,
                0,
                timestampDriver.fromDays(7) - 1,
                1
        );

        Assert.assertEquals(0, iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.fromDays(7), iterator.getMaxTimestamp());

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDst() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 2, 'h', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-03-28T00:59:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-28T08:01:00.000000Z"),
                2
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T10:00:00.000000Z"), iterator.getMaxTimestamp());

        // DST edge is here
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T06:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T06:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T10:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstForwardShiftWithIntervalLargerThanShift() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 75, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-03-28T00:01:00.000000Z"), // 01:01 local time (GMT+1)
                timestampDriver.parseFloorLiteral("2021-03-28T02:52:00.000000Z"), // 04:52 local time (GMT+2)
                1
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:15:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:15:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:15:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampHi());

        // DST edge is here (02:00 -> 03:00 local time)
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:15:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstShiftBackwardWithSmallInterval1() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 15, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-10-31T00:51:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-10-31T01:41:00.000000Z"),
                1
        );

        // both min and max timestamps must be aligned at the backward shift, not at 00:45-01:45
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstShiftBackwardWithSmallInterval2() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 15, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-10-30T23:30:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-10-31T02:40:00.000000Z"),
                1
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:30:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:45:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:45:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:45:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:15:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:15:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:45:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstShiftForwardWithSmallInterval() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 30, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-03-28T00:01:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-28T02:52:00.000000Z"),
                1
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:00:00.000000Z"), iterator.getTimestampHi());

        // DST edge is here
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstSingleRow1() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 30, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/London"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-10-27T01:00:00.600000Z"),
                timestampDriver.parseFloorLiteral("2024-10-27T01:00:00.600000Z"),
                2
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T02:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstSingleRow2() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 30, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/London"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-03-31T00:30:06.000000Z"),
                timestampDriver.parseFloorLiteral("2024-03-31T00:30:06.000000Z"),
                2
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T00:30:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T01:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T00:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T01:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithFixedOffset() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "GMT+00:30"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-01-01T01:01:00.000000Z"),
                timestampDriver.parseFloorLiteral("2024-01-03T07:00:01.000000Z"),
                1
        );

        final long minExpectedTs = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        final long maxExpectedTs = timestampDriver.parseFloorLiteral("2024-01-04T00:00:00.000000Z");

        final long tzOffset = timestampDriver.fromMinutes(30);

        Assert.assertEquals(minExpectedTs - tzOffset, iterator.getMinTimestamp());
        Assert.assertEquals(maxExpectedTs - tzOffset, iterator.getMaxTimestamp());

        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(minExpectedTs - tzOffset + timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(minExpectedTs - tzOffset + timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    @Override
    protected SampleByIntervalIterator createIterator(
            TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long offset,
            LongList intervals,
            long minTs,
            long maxTs,
            LongList stepPerInterval
    ) {
        Assert.assertNotNull(tzRules);
        return new TimeZoneIntervalIterator().of(
                timestampDriver,
                sampler,
                tzRules,
                offset,
                intervals,
                minTs,
                maxTs,
                stepPerInterval
        );
    }

    @Override
    protected SampleByIntervalIterator createIterator(
            TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long offset,
            @Nullable LongList intervals,
            long minTs,
            long maxTs,
            int step
    ) {
        Assert.assertNotNull(tzRules);
        return new TimeZoneIntervalIterator().of(
                timestampDriver,
                sampler,
                tzRules,
                offset,
                intervals,
                minTs,
                maxTs,
                step
        );
    }

    // Asserts the one step-group of an interval at the given UTC minute under SAMPLE BY 1<unit> ALIGN TO CALENDAR TIME
    // ZONE zone WITH OFFSET of the given minutes, in the per-cluster mode with a step of one bucket.
    private void assertStepGroupOfInterval(
            String zone,
            char unit,
            int offsetMinutes,
            String intervalMinute,
            String expectedLoMinute,
            String expectedHiMinute
    ) throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, unit, 0);
        final long intervalTs = timestampDriver.parseFloorLiteral(intervalMinute + ":00.000000Z");
        final LongList intervals = new LongList();
        intervals.add(intervalTs, intervalTs);
        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(1L);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, zone),
                timestampDriver.fromMinutes(offsetMinutes),
                intervals,
                intervalTs,
                intervalTs,
                stepPerCluster
        );
        final String label = zone + ", 1" + unit + ", offset " + offsetMinutes + "m, interval " + intervalMinute;
        Assert.assertTrue(label, iterator.next());
        Assert.assertEquals(label, toInstant(timestampDriver.parseFloorLiteral(expectedLoMinute + ":00.000000Z")), toInstant(iterator.getTimestampLo()));
        Assert.assertEquals(label, toInstant(timestampDriver.parseFloorLiteral(expectedHiMinute + ":00.000000Z")), toInstant(iterator.getTimestampHi()));
        Assert.assertFalse(label, iterator.next());
    }

    // Asserts the step-groups of two one-row clusters under SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'
    // WITH OFFSET of the given minutes, in the per-cluster mode with a step of one bucket.
    private void assertStepGroupsOfTwoClusters(int offsetMinutes, String firstMinute, String secondMinute, LongList expected) throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final long firstTs = timestampDriver.parseFloorLiteral(firstMinute + ":00.000000Z");
        final long secondTs = timestampDriver.parseFloorLiteral(secondMinute + ":00.000000Z");
        final LongList intervals = new LongList();
        intervals.add(firstTs, firstTs);
        intervals.add(secondTs, secondTs);
        final LongList stepPerCluster = new LongList();
        stepPerCluster.add(1L);
        stepPerCluster.add(1L);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                timestampDriver.fromMinutes(offsetMinutes),
                intervals,
                firstTs,
                secondTs,
                stepPerCluster
        );
        assertStepGroups(iterator, expected);
    }
}
