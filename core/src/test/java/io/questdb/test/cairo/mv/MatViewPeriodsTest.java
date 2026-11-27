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
import io.questdb.cairo.mv.MatViewPeriods;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.TreeSet;

public class MatViewPeriodsTest {
    private static final String[] LENGTHS = {"1s", "7m", "15m", "30m", "45m", "1h", "90m", "2h", "3h", "5h", "1d"};
    private static final int[] YEARS = {2005, 2022, 2060};
    // Zones behind and ahead of UTC, with half-hour offsets, with a half-hour change (Lord_Howe), with changes at
    // midnight (Gaza, Havana, Santiago), with changes around Ramadan (Casablanca) and with a negative DST (Dublin).
    // Across the zones, the years take the changes from the zone history and from the zone rules, within and beyond
    // the years that TimeZoneRules caches.
    private static final String[] ZONES = {
            "Africa/Casablanca",
            "America/Havana",
            "America/New_York",
            "America/Santiago",
            "America/St_Johns",
            "Asia/Gaza",
            "Australia/Adelaide",
            "Australia/Lord_Howe",
            "Europe/Berlin",
            "Europe/Dublin"
    };

    @Test
    public void testAroundEveryChange() throws Exception {
        int changeCount = 0;
        for (String zone : ZONES) {
            final ZoneRules zoneRules = ZoneId.of(zone).getRules();
            final TimeZoneRules rules = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, zone);
            for (int year : YEARS) {
                ZoneOffsetTransition change = zoneRules.nextTransition(Instant.parse(year + "-01-01T00:00:00Z"));
                while (change != null && change.getInstant().isBefore(Instant.parse((year + 1) + "-01-01T00:00:00Z"))) {
                    for (String length : LENGTHS) {
                        assertAroundChange(zone, zoneRules, rules, length, change);
                    }
                    changeCount++;
                    change = zoneRules.nextTransition(change.getInstant());
                }
            }
        }
        // Each zone changes its clock at least once a year in each of the years.
        Assert.assertTrue("changes: " + changeCount, changeCount >= ZONES.length * YEARS.length);
    }

    @Test
    public void testBeforeSamplerStart() throws Exception {
        // The grid goes on below the sampler's start, so a period starts at the grid point at or before a time there,
        // as it does above the start.
        final TimestampSampler sampler = sampler("1h");
        final TimeZoneRules rules = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "America/New_York");
        // The grid starts at 03:00 local on 2022-03-13, which is 07:00Z, right after DST starts.
        sampler.setStart(MicrosTimestampDriver.floor("2022-03-13T03:00"));
        // 06:35Z is 01:35 EST, in the period that starts at 01:00 local, 06:00Z. The clock skips the grid point at
        // 02:00 local, so the next period starts at the change.
        assertPeriod(sampler, rules, "2022-03-13T06:35", "2022-03-13T06:00", "2022-03-13T07:00");
        // On a grid point, and a microsecond before it.
        assertPeriod(sampler, rules, "2022-03-13T06:00", "2022-03-13T06:00", "2022-03-13T07:00");
        Assert.assertEquals(
                MicrosTimestampDriver.floor("2022-03-13T05:00"),
                MatViewPeriods.periodStart(sampler, rules, MicrosTimestampDriver.floor("2022-03-13T06:00") - 1)
        );
        Assert.assertEquals(
                MicrosTimestampDriver.floor("2022-03-13T06:00"),
                MatViewPeriods.nextPeriodStart(sampler, rules, MicrosTimestampDriver.floor("2022-03-13T06:00") - 1)
        );
        // Without rules, local time is UTC.
        assertPeriod(sampler, null, "2022-03-13T01:35", "2022-03-13T01:00", "2022-03-13T02:00");
        assertPeriod(sampler, null, "2022-03-13T01:00", "2022-03-13T01:00", "2022-03-13T02:00");

        // Across 1970, the grid goes on the same way, whether it starts before or after 1970.
        final long minute = Micros.MINUTE_MICROS;
        for (long start : new long[]{2 * Micros.HOUR_MICROS, -2 * Micros.HOUR_MICROS}) {
            sampler.setStart(start);
            Assert.assertEquals(-3 * Micros.HOUR_MICROS, MatViewPeriods.periodStart(sampler, null, -150 * minute));
            Assert.assertEquals(-2 * Micros.HOUR_MICROS, MatViewPeriods.nextPeriodStart(sampler, null, -150 * minute));
            Assert.assertEquals(-Micros.HOUR_MICROS, MatViewPeriods.periodStart(sampler, null, -30 * minute));
            Assert.assertEquals(0, MatViewPeriods.nextPeriodStart(sampler, null, -30 * minute));
            Assert.assertEquals(0, MatViewPeriods.periodStart(sampler, null, 30 * minute));
            Assert.assertEquals(Micros.HOUR_MICROS, MatViewPeriods.nextPeriodStart(sampler, null, 30 * minute));
        }
    }

    @Test
    public void testBeforeSamplerStartWhereClockMovesBack() throws Exception {
        // DST ends in America/New_York on 2022-11-06 at 06:00Z (02:00 EDT -> 01:00 EST), and the clock passes
        // 01:00-02:00 local twice. A view created at 05:45Z, 01:45 EDT, starts its grid at the grid point before that,
        // and from 06:00Z the local clock is behind that start until it reaches it again.
        final TimeZoneRules rules = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "America/New_York");

        // The 30m grid starts at 01:30 local. At 06:10Z, 01:10 EST, the period [06:00Z, 06:30Z) has not ended.
        final TimestampSampler sampler30m = sampler("30m");
        sampler30m.setStart(MicrosTimestampDriver.floor("2022-11-06T01:30"));
        assertPeriod(sampler30m, rules, "2022-11-06T06:10", "2022-11-06T06:00", "2022-11-06T06:30");
        assertPeriod(sampler30m, rules, "2022-11-06T06:00", "2022-11-06T06:00", "2022-11-06T06:30");
        assertPeriod(sampler30m, rules, "2022-11-06T05:50", "2022-11-06T05:30", "2022-11-06T06:00");

        // The 45m grid points in local time are 00:45, 01:30 and 02:15, and the grid starts at 01:30. The clock moves
        // back at 02:00 EDT, before it reaches 02:15, and reaches 01:30 again at 06:30Z, so the period that starts at
        // 01:30 EDT, 05:30Z, lasts an hour.
        final TimestampSampler sampler45m = sampler("45m");
        sampler45m.setStart(MicrosTimestampDriver.floor("2022-11-06T01:30"));
        assertPeriod(sampler45m, rules, "2022-11-06T06:10", "2022-11-06T05:30", "2022-11-06T06:30");
        assertPeriod(sampler45m, rules, "2022-11-06T06:00", "2022-11-06T05:30", "2022-11-06T06:30");
        assertPeriod(sampler45m, rules, "2022-11-06T06:30", "2022-11-06T06:30", "2022-11-06T07:15");

        // DST ends in Europe/Berlin on 2022-10-30 at 01:00Z (03:00 CEST -> 02:00 CET). A view created at 00:50Z, 02:50
        // CEST, starts its 15m grid at 02:45 local. At 01:10Z, 02:10 CET, the period [01:00Z, 01:15Z) has not ended.
        final TimeZoneRules berlin = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin");
        final TimestampSampler sampler15m = sampler("15m");
        sampler15m.setStart(MicrosTimestampDriver.floor("2022-10-30T02:45"));
        assertPeriod(sampler15m, berlin, "2022-10-30T01:10", "2022-10-30T01:00", "2022-10-30T01:15");
        assertPeriod(sampler15m, berlin, "2022-10-30T01:44", "2022-10-30T01:30", "2022-10-30T01:45");
    }

    @Test
    public void testWithoutChanges() throws Exception {
        // Asia/Kolkata has no DST, UTC has a fixed offset, and without rules local time is UTC.
        final long from = MicrosTimestampDriver.floor("2022-03-10T00:00");
        final long to = MicrosTimestampDriver.floor("2022-03-12T00:00");
        for (String length : LENGTHS) {
            for (String zone : new String[]{"Asia/Kolkata", "UTC", null}) {
                final TimestampSampler sampler = sampler(length);
                final long lengthUs = sampler.getBucketSize();
                final ZoneOffset offset = zone != null ? ZoneId.of(zone).getRules().getOffset(Instant.EPOCH) : ZoneOffset.UTC;
                final long offsetUs = offset.getTotalSeconds() * Micros.SECOND_MICROS;
                final TimeZoneRules rules = zone != null ? MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, zone) : null;
                // The grid starts before the times checked, or after them, where the clock is behind the start.
                final long[] samplerStarts = {
                        Math.floorDiv(from + offsetUs, lengthUs) * lengthUs - lengthUs,
                        Math.floorDiv(to + offsetUs, lengthUs) * lengthUs + lengthUs
                };
                for (long samplerStart : samplerStarts) {
                    sampler.setStart(samplerStart);
                    for (long utc = from; utc < to; utc += Math.max(lengthUs / 3, Micros.MINUTE_MICROS) + 1) {
                        final long expectedStart = Math.floorDiv(utc + offsetUs, lengthUs) * lengthUs - offsetUs;
                        final String message = zone + ", " + length + ", start " + Micros.toUSecString(samplerStart) + ", " + Micros.toUSecString(utc);
                        Assert.assertEquals(message, expectedStart, MatViewPeriods.periodStart(sampler, rules, utc));
                        Assert.assertEquals(message, expectedStart + lengthUs, MatViewPeriods.nextPeriodStart(sampler, rules, utc));
                        Assert.assertEquals(message, expectedStart, MatViewPeriods.periodStart(sampler, rules, expectedStart));
                        Assert.assertEquals(message, expectedStart + lengthUs, MatViewPeriods.nextPeriodStart(sampler, rules, expectedStart));
                    }
                }
            }
        }
    }

    // Lists the UTC times at which a period starts around the change with java.time, and checks both methods at each
    // of them, a microsecond around each of them, and at times in between.
    private static void assertAroundChange(
            String zone,
            ZoneRules zoneRules,
            TimeZoneRules rules,
            String length,
            ZoneOffsetTransition transition
    ) throws SqlException {
        final long change = toMicros(transition.getInstant());
        final TimestampSampler sampler = sampler(length);
        final long lengthUs = sampler.getBucketSize();
        final long window = Math.min(Math.max(200 * lengthUs, 2 * Micros.HOUR_MICROS), 5 * Micros.DAY_MICROS);
        // A day covers any UTC offset, so the grid points in [localLo, localHi] cover the period starts that the
        // checks below look for, from the one before lo to the one after hi.
        final long localLo = Math.floorDiv(change - window - lengthUs - Micros.DAY_MICROS, lengthUs) * lengthUs;
        final long localHi = change + window + lengthUs + Micros.DAY_MICROS;

        final TreeSet<Long> starts = new TreeSet<>();
        for (long gridPoint = localLo; gridPoint <= localHi; gridPoint += lengthUs) {
            final LocalDateTime local = LocalDateTime.ofEpochSecond(
                    Math.floorDiv(gridPoint, Micros.SECOND_MICROS),
                    (int) Math.floorMod(gridPoint, Micros.SECOND_MICROS) * 1000,
                    ZoneOffset.UTC
            );
            final List<ZoneOffset> offsets = zoneRules.getValidOffsets(local);
            if (offsets.isEmpty()) {
                // The clock skips the grid point, so the period starts at the change.
                starts.add(toMicros(zoneRules.getTransition(local).getInstant()));
            } else {
                // The clock reaches the grid point once, or twice where it moves back.
                for (int i = 0, n = offsets.size(); i < n; i++) {
                    starts.add(gridPoint - offsets.get(i).getTotalSeconds() * Micros.SECOND_MICROS);
                }
            }
        }

        // The production grid starts at a multiple of the length since the epoch, in local time: at the grid point
        // before the time of the CREATE or ALTER that set the period. The clock can fall behind that start, such as
        // when it moves back, so the checks run with the grid starting before the times they check, at the last grid
        // point that the clock reaches before the change, and after the times they check.
        final long localChange = change + transition.getOffsetBefore().getTotalSeconds() * Micros.SECOND_MICROS;
        final long[] samplerStarts = {
                localLo,
                localLo + Math.floorDiv(localChange - 1 - localLo, lengthUs) * lengthUs,
                localLo + (Math.floorDiv(localHi - localLo, lengthUs) + 1) * lengthUs
        };
        final long lo = change - window;
        final long hi = change + window;
        final long step = Math.max(lengthUs / 4, Micros.SECOND_MICROS) + 7;
        for (long samplerStart : samplerStarts) {
            sampler.setStart(samplerStart);
            for (long utc = lo; utc <= hi; utc += step) {
                assertAt(zone, length, sampler, samplerStart, rules, starts, utc);
            }
            for (Long start : starts.subSet(lo, true, hi, true)) {
                assertAt(zone, length, sampler, samplerStart, rules, starts, start - 1);
                assertAt(zone, length, sampler, samplerStart, rules, starts, start);
                assertAt(zone, length, sampler, samplerStart, rules, starts, start + 1);
            }
        }
    }

    private static void assertAt(
            String zone,
            String length,
            TimestampSampler sampler,
            long samplerStart,
            TimeZoneRules rules,
            TreeSet<Long> starts,
            long utc
    ) {
        final String message = zone + ", " + length + ", start " + Micros.toUSecString(samplerStart) + ", " + Micros.toUSecString(utc);
        Assert.assertEquals(message, starts.floor(utc), (Long) MatViewPeriods.periodStart(sampler, rules, utc));
        Assert.assertEquals(message, starts.higher(utc), (Long) MatViewPeriods.nextPeriodStart(sampler, rules, utc));
    }

    // Checks the period that holds the given UTC time: the time at which it starts, and the time at which the next one
    // starts.
    private static void assertPeriod(
            TimestampSampler sampler,
            @Nullable TimeZoneRules rules,
            String utc,
            String expectedStart,
            String expectedNextStart
    ) throws NumericException {
        final long utcUs = MicrosTimestampDriver.floor(utc);
        Assert.assertEquals(utc, MicrosTimestampDriver.floor(expectedStart), MatViewPeriods.periodStart(sampler, rules, utcUs));
        Assert.assertEquals(utc, MicrosTimestampDriver.floor(expectedNextStart), MatViewPeriods.nextPeriodStart(sampler, rules, utcUs));
    }

    private static TimestampSampler sampler(String length) throws SqlException {
        return TimestampSamplerFactory.getInstance(MicrosTimestampDriver.INSTANCE, length, 0);
    }

    private static long toMicros(Instant instant) {
        return instant.getEpochSecond() * Micros.SECOND_MICROS + instant.getNano() / 1000;
    }
}
