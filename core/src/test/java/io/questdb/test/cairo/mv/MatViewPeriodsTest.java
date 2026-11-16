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
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
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
                        assertAroundChange(zone, zoneRules, rules, length, toMicros(change.getInstant()));
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
        final TimestampSampler sampler = sampler("1h");
        final TimeZoneRules rules = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "America/New_York");
        // The grid starts at 03:00 local on 2022-03-13, which is 07:00Z, right after DST starts.
        sampler.setStart(MicrosTimestampDriver.floor("2022-03-13T03:00"));
        // The sampler rounds 01:35 local up to the grid point at 02:00, which the clock skips, so the period starts at
        // the change.
        Assert.assertEquals(
                MicrosTimestampDriver.floor("2022-03-13T07:00"),
                MatViewPeriods.periodStart(sampler, rules, MicrosTimestampDriver.floor("2022-03-13T06:35"))
        );
        // Without a change in between, the result is the time of the grid point that the sampler rounds up to.
        Assert.assertEquals(
                MicrosTimestampDriver.floor("2022-03-13T06:00"),
                MatViewPeriods.periodStart(sampler, rules, MicrosTimestampDriver.floor("2022-03-13T05:35"))
        );
        Assert.assertEquals(
                MicrosTimestampDriver.floor("2022-03-13T02:00"),
                MatViewPeriods.periodStart(sampler, null, MicrosTimestampDriver.floor("2022-03-13T01:35"))
        );
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
                sampler.setStart(Math.floorDiv(from + offsetUs, lengthUs) * lengthUs - lengthUs);
                final TimeZoneRules rules = zone != null ? MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, zone) : null;
                for (long utc = from; utc < to; utc += Math.max(lengthUs / 3, Micros.MINUTE_MICROS) + 1) {
                    final long expectedStart = Math.floorDiv(utc + offsetUs, lengthUs) * lengthUs - offsetUs;
                    Assert.assertEquals(expectedStart, MatViewPeriods.periodStart(sampler, rules, utc));
                    Assert.assertEquals(expectedStart + lengthUs, MatViewPeriods.nextPeriodStart(sampler, rules, utc));
                    Assert.assertEquals(expectedStart, MatViewPeriods.periodStart(sampler, rules, expectedStart));
                    Assert.assertEquals(expectedStart + lengthUs, MatViewPeriods.nextPeriodStart(sampler, rules, expectedStart));
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
            long change
    ) throws SqlException {
        final TimestampSampler sampler = sampler(length);
        final long lengthUs = sampler.getBucketSize();
        final long window = Math.min(Math.max(200 * lengthUs, 2 * Micros.HOUR_MICROS), 5 * Micros.DAY_MICROS);
        // A day covers any UTC offset, so the grid points in [localLo, localHi] cover the period starts that the
        // checks below look for, from the one before lo to the one after hi.
        final long localLo = Math.floorDiv(change - window - lengthUs - Micros.DAY_MICROS, lengthUs) * lengthUs;
        final long localHi = change + window + lengthUs + Micros.DAY_MICROS;
        // The production grid starts at a multiple of the length since the epoch, in local time.
        sampler.setStart(localLo);

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

        final long lo = change - window;
        final long hi = change + window;
        final long step = Math.max(lengthUs / 4, Micros.SECOND_MICROS) + 7;
        for (long utc = lo; utc <= hi; utc += step) {
            assertAt(zone, length, sampler, rules, starts, utc);
        }
        for (Long start : starts.subSet(lo, true, hi, true)) {
            assertAt(zone, length, sampler, rules, starts, start - 1);
            assertAt(zone, length, sampler, rules, starts, start);
            assertAt(zone, length, sampler, rules, starts, start + 1);
        }
    }

    private static void assertAt(
            String zone,
            String length,
            TimestampSampler sampler,
            TimeZoneRules rules,
            TreeSet<Long> starts,
            long utc
    ) {
        final String message = zone + ", " + length + ", " + Micros.toUSecString(utc);
        Assert.assertEquals(message, starts.floor(utc), (Long) MatViewPeriods.periodStart(sampler, rules, utc));
        Assert.assertEquals(message, starts.higher(utc), (Long) MatViewPeriods.nextPeriodStart(sampler, rules, utc));
    }

    private static TimestampSampler sampler(String length) throws SqlException {
        return TimestampSamplerFactory.getInstance(MicrosTimestampDriver.INSTANCE, length, 0);
    }

    private static long toMicros(Instant instant) {
        return instant.getEpochSecond() * Micros.SECOND_MICROS + instant.getNano() / 1000;
    }
}
