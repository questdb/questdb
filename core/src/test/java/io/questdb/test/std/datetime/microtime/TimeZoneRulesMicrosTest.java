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

package io.questdb.test.std.datetime.microtime;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.FixedTimeZoneRule;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.TimeZoneRulesMicros;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TimeZoneRulesMicrosTest {
    private static final Log LOG = LogFactory.getLog(TimeZoneRulesMicrosTest.class);

    private static void assertFirstInstant(String zone, ZoneRules zoneRules, TimeZoneRulesMicros rules, long local) {
        final LocalDateTime ldt = LocalDateTime.ofEpochSecond(Math.floorDiv(local, Micros.SECOND_MICROS), (int) Math.floorMod(local, Micros.SECOND_MICROS) * 1000, ZoneOffset.UTC);
        final List<ZoneOffset> offsets = zoneRules.getValidOffsets(ldt);
        final long expected = offsets.isEmpty()
                ? zoneRules.getTransition(ldt).getInstant().getEpochSecond() * Micros.SECOND_MICROS
                : local - Math.max(offsets.get(0).getTotalSeconds(), offsets.get(offsets.size() - 1).getTotalSeconds()) * Micros.SECOND_MICROS;
        Assert.assertEquals(zone + " " + ldt, expected, rules.getFirstInstantAtOrAfter(local));
    }

    private static void assertHighWater(String zone, ZoneRules zoneRules, TimeZoneRulesMicros rules, long utc) {
        final Instant instant = Instant.ofEpochSecond(Math.floorDiv(utc, Micros.SECOND_MICROS), Math.floorMod(utc, Micros.SECOND_MICROS) * 1000);
        long expected = utc + zoneRules.getOffset(instant).getTotalSeconds() * Micros.SECOND_MICROS;
        final ZoneOffsetTransition previous = zoneRules.previousTransition(instant.plusNanos(1));
        if (previous != null && previous.isOverlap()) {
            final long localBefore = previous.getDateTimeBefore().toEpochSecond(ZoneOffset.UTC) * Micros.SECOND_MICROS;
            expected = Math.max(expected, localBefore - 1);
        }
        Assert.assertEquals(zone + " " + instant, expected, rules.getWallClockHighWater(utc));
    }

    @Test
    public void testCompatibility() {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesMicros> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesMicros(zone.getRules()));
        }

        long micros = Micros.toMicros(1900, 1, 1, 0, 0);
        long deadline = Micros.toMicros(2115, 12, 31, 0, 0);

        while (micros < deadline) {
            final int y = Micros.getYear(micros);

            Instant dt = Instant.ofEpochMilli(micros / 1000);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesMicros rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.getOffset().getTotalSeconds();
                // find out how much algo added to datetime itself
                long offset = rules.getOffset(micros, y);

                try {
                    Assert.assertEquals(expected, offset / Micros.SECOND_MICROS);
                } catch (Throwable e) {
                    System.out.println(zone.getId() + "; " + zdt + "; " + Micros.toString(micros + offset));
                    System.out.println("e: " + expected + "; a: " + offset);
                    System.out.println(dt);
                    System.out.println(Micros.toString(micros));
                    throw e;
                }
            }
            micros += Micros.DAY_MICROS;
        }
    }

    @Test
    public void testGapDuration() throws NumericException {
        final ZoneId zone = ZoneId.of("Europe/Berlin");
        final TimeZoneRulesMicros rules = new TimeZoneRulesMicros(zone.getRules());

        Assert.assertEquals(0, rules.getDstGapOffset(0));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1888-05-12T23:45:51.045Z")));

        // DST
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-03-28T01:00:00.000Z")));
        Assert.assertEquals(0L, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-03-28T02:00:00.000Z")));
        Assert.assertEquals(60001000L, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-03-28T02:01:00.001Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-03-28T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-03-28T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-10-31T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-10-31T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("2021-10-31T03:01:00.000Z")));

        // historical
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-03-30T01:01:00.000Z")));
        Assert.assertEquals(0L, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-03-30T02:00:00.000Z")));
        Assert.assertEquals(60000001L, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-03-30T02:01:00.000001Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-03-30T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-03-30T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-10-26T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-10-26T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MicrosFormatUtils.parseTimestamp("1997-10-26T03:01:00.000Z")));
    }

    @Test
    public void testFirstInstantAndWallClockHighWaterAroundTransitions() {
        final String[] zones = {
                "Europe/Berlin", "America/New_York", "Australia/Sydney", "Australia/Lord_Howe", "Europe/Dublin",
                "America/Havana", "Pacific/Apia", "America/Sao_Paulo", "Asia/Tehran", "UTC"
        };
        final long minute = Micros.MINUTE_MICROS;
        for (String id : zones) {
            final ZoneRules zoneRules = ZoneId.of(id).getRules();
            final TimeZoneRulesMicros rules = new TimeZoneRulesMicros(zoneRules);
            ZoneOffsetTransition tr = zoneRules.nextTransition(Instant.parse("1970-01-01T00:00:00Z"));
            while (tr != null && tr.getInstant().isBefore(Instant.parse("2045-01-01T00:00:00Z"))) {
                final long transition = tr.getInstant().getEpochSecond() * Micros.SECOND_MICROS;
                final long localBefore = tr.getDateTimeBefore().toEpochSecond(ZoneOffset.UTC) * Micros.SECOND_MICROS;
                final long localAfter = tr.getDateTimeAfter().toEpochSecond(ZoneOffset.UTC) * Micros.SECOND_MICROS;
                for (long d = -26 * 60; d <= 26 * 60; d += 7) {
                    assertHighWater(id, zoneRules, rules, transition + d * minute);
                    assertFirstInstant(id, zoneRules, rules, localBefore + d * minute);
                }
                for (long edge : new long[]{transition - 1, transition, transition + 1, transition + localBefore - localAfter - 1, transition + localBefore - localAfter}) {
                    assertHighWater(id, zoneRules, rules, edge);
                }
                for (long edge : new long[]{localBefore - 1, localBefore, localBefore + 1, localAfter - 1, localAfter, localAfter + 1}) {
                    assertFirstInstant(id, zoneRules, rules, edge);
                }
                tr = zoneRules.nextTransition(tr.getInstant());
            }
        }
    }

    @Test
    public void testGetNextDSTBeforeFirstTransition() {
        // Antarctica/Troll's first-ever DST transition is after 1970 (2005); for an instant before
        // it, getNextDST must return that transition, not Long.MAX_VALUE ("no more transitions").
        final ZoneId zone = ZoneId.of("Antarctica/Troll");
        final TimeZoneRulesMicros rules = new TimeZoneRulesMicros(zone.getRules());
        final long beforeFirst = Micros.toMicros(2000, 1, 1, 0, 0);
        final long next = rules.getNextDST(beforeFirst);
        Assert.assertNotEquals(Long.MAX_VALUE, next);
        Assert.assertTrue(next > beforeFirst);
        // the offset genuinely changes at that transition
        Assert.assertNotEquals(rules.getOffset(beforeFirst), rules.getOffset(next));
    }

    @Test
    public void testGetLocalTimeOffsetGcd() {
        final long[] localTimes = {0, 30 * 60, 2 * 3_600 + 30 * 60, 23 * 3_600 + 45 * 60, -3_600};
        for (String id : ZoneId.getAvailableZoneIds()) {
            final ZoneRules zoneRules = ZoneId.of(id).getRules();
            final TimeZoneRulesMicros rules = new TimeZoneRulesMicros(zoneRules);
            for (long localTime : localTimes) {
                final long base = zoneRules.getOffset(Instant.EPOCH).getTotalSeconds();
                long expected = 0;
                for (ZoneOffsetTransition tr = zoneRules.nextTransition(Instant.EPOCH);
                     tr != null && tr.getInstant().getEpochSecond() < 7_258_118_400L;
                     tr = zoneRules.nextTransition(tr.getInstant())) {
                    final long before = tr.getOffsetBefore().getTotalSeconds();
                    expected = BigInteger.valueOf(expected).gcd(BigInteger.valueOf(tr.getOffsetAfter().getTotalSeconds() - base)).longValue();
                    if (tr.isGap()) {
                        final long skipped = Math.floorMod(localTime - tr.getDateTimeBefore().toLocalTime().toSecondOfDay(), 86_400L);
                        if (skipped > 0 && skipped < tr.getDuration().getSeconds()) {
                            expected = BigInteger.valueOf(expected).gcd(BigInteger.valueOf(before + skipped - base)).longValue();
                        }
                    }
                }
                Assert.assertEquals(id + " at " + localTime, expected * Micros.SECOND_MICROS, rules.getLocalTimeOffsetGcd(0, localTime * Micros.SECOND_MICROS));
            }
        }
        final TimeZoneRulesMicros berlin = new TimeZoneRulesMicros(ZoneId.of("Europe/Berlin").getRules());
        Assert.assertEquals(Micros.HOUR_MICROS, berlin.getLocalTimeOffsetGcd(0, 0));
        // 02:30 falls in the spring-forward gap, which starts at 02:00 and maps the local time to the shift instant.
        Assert.assertEquals(30 * Micros.MINUTE_MICROS, berlin.getLocalTimeOffsetGcd(0, 150 * Micros.MINUTE_MICROS));
        // Berlin kept local mean time, +00:53:28, until 1893.
        Assert.assertEquals(8 * Micros.SECOND_MICROS, berlin.getLocalTimeOffsetGcd(Micros.toMicros(1890, 1, 1, 0, 0), 0));
        Assert.assertEquals(0, new TimeZoneRulesMicros(ZoneId.of("Asia/Kolkata").getRules()).getLocalTimeOffsetGcd(0, 0));
        Assert.assertEquals(0, new TimeZoneRulesMicros(ZoneId.of("UTC").getRules()).getLocalTimeOffsetGcd(0, 0));
        Assert.assertEquals(0, new FixedTimeZoneRule(330 * Micros.MINUTE_MICROS).getLocalTimeOffsetGcd(0, 0));
    }

    @Test
    public void testPerformance() {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesMicros> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesMicros(zone.getRules()));
        }

        long millis = Micros.toMicros(1900, 1, 1, 0, 0);
        long deadline = Micros.toMicros(2615, 12, 31, 0, 0);

        while (millis < deadline) {
            for (int i = 0, n = zones.size(); i < n; i++) {
                zoneRules.get(i).getOffset(millis);
            }
            millis += Micros.DAY_MICROS;
        }
    }

    @Test
    public void testSingle() {
        ZoneId zone = ZoneId.of("GMT");
        TimeZoneRulesMicros rules = new TimeZoneRulesMicros(zone.getRules());

        int y = 2017;
        int m = 3;
        int d = 29;

        LocalDateTime dt = LocalDateTime.of(y, m, d, 0, 0);
        long millis = Micros.toMicros(y, m, d, 0, 0);

        ZonedDateTime zdt = dt.atZone(zone);
        long expected = zdt.getOffset().getTotalSeconds();

        // find out how much algo added to datetime itself
        long changed = Micros.toMicros(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * 1000L;
        // add any extra time
        expected += (changed - millis) / 1000;
        long offset = rules.getOffset(millis, y);

        try {
            Assert.assertEquals(expected, offset / 1000);
        } catch (Throwable e) {
            System.out.println(zone.getId() + "; " + zdt + "; " + Micros.toString(millis + offset));
            throw e;
        }
    }

    @Test
    public void testToUtcCompatibility() {
        final Rnd rnd = TestUtils.generateRandom(LOG);

        final Set<String> allZones = ZoneId.getAvailableZoneIds();
        final List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        final List<ZoneId> zones = new ArrayList<>(zoneList.size());
        final List<TimeZoneRulesMicros> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesMicros(zone.getRules()));
        }

        final DateTimeFormatter localDateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
        long micros = Micros.toMicros(1900, 1, 1, 0, 0);
        final long deadline = Micros.toMicros(2115, 12, 31, 0, 0);
        final long step = Math.max(1, rnd.nextLong(30)) * Micros.DAY_MICROS;
        LocalDateTime dt;
        while (micros < deadline) {
            try {
                dt = LocalDateTime.parse(Micros.toString(micros), localDateTimeFormat);
            } catch (Throwable e) {
                System.out.println(micros);
                throw e;
            }

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesMicros rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.toInstant().toEpochMilli() * Micros.MILLI_MICROS;
                // find out how much algo added to datetime itself
                long offset = rules.getLocalOffset(dt.toInstant(ZoneOffset.UTC).toEpochMilli() * Micros.MILLI_MICROS);
                long actual = micros - offset;

                try {
                    Assert.assertEquals(expected, actual);
                } catch (Throwable e) {
                    System.out.println(micros);
                    System.out.println(zone.getId() + "; " + zdt + "; " + Micros.toString(actual));
                    System.out.println("e: " + expected + "; a: " + actual);
                    System.out.println(dt);
                    System.out.println(Micros.toString(micros));
                    throw e;
                }
            }
            micros += step;
        }
    }
}
