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

package io.questdb.test.std.datetime.nanotime;

import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.datetime.nanotime.TimeZoneRulesNanos;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TimeZoneRulesNanosTest {
    private static final Log LOG = LogFactory.getLog(TimeZoneRulesNanosTest.class);

    @Test
    public void testCompatibility() {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesNanos> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesNanos(zone.getRules()));
        }

        long nanos = Nanos.toNanos(1900, 1, 1, 0, 0);
        long deadline = Nanos.toNanos(2115, 12, 31, 0, 0);

        while (nanos < deadline) {
            final int y = Nanos.getYear(nanos);

            Instant dt = Instant.ofEpochMilli(nanos / 1000_000);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesNanos rules = zoneRules.get(i);
                ZonedDateTime zdt = dt.atZone(zone);
                long expected = zdt.getOffset().getTotalSeconds();
                // find out how much algo added to datetime itself
                long offset = rules.getOffset(nanos, y);

                try {
                    Assert.assertEquals(expected, offset / Nanos.SECOND_NANOS);
                } catch (Throwable e) {
                    System.out.println(zone.getId() + "; " + zdt + "; " + Nanos.toString(nanos + offset));
                    System.out.println("e: " + expected + "; a: " + offset);
                    System.out.println(dt);
                    System.out.println(Nanos.toString(nanos));
                    throw e;
                }
            }
            nanos += Nanos.DAY_NANOS;
        }
    }

    @Test
    public void testGapDuration() throws NumericException {
        final ZoneId zone = ZoneId.of("Europe/Berlin");
        final TimeZoneRulesNanos rules = new TimeZoneRulesNanos(zone.getRules());

        Assert.assertEquals(0, rules.getDstGapOffset(0));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1888-05-12T23:45:51.045Z")));

        // DST
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-03-28T01:00:00.000Z")));
        Assert.assertEquals(0L, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-03-28T02:00:00.000Z")));
        Assert.assertEquals(60001000000L, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-03-28T02:01:00.001Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-03-28T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-03-28T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-10-31T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-10-31T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-10-31T03:01:00.000Z")));

        // historical
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-03-30T01:01:00.000Z")));
        Assert.assertEquals(0L, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-03-30T02:00:00.000Z")));
        Assert.assertEquals(60000001000L, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-03-30T02:01:00.000001Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-03-30T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-03-30T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-10-26T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-10-26T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(NanosTimestampDriver.INSTANCE.parseFloorLiteral("1997-10-26T03:01:00.000Z")));
    }

    @Test
    public void testGapNanosecondPrecision() throws NumericException {
        assertGapNanosecondPrecision("Europe/Berlin", "1893-04-01T00:00:00Z", 392 * Nanos.SECOND_NANOS);
        assertGapNanosecondPrecision("Europe/Berlin", "2021-03-28T02:00:00Z", Nanos.HOUR_NANOS);
        final ZoneId zone = ZoneId.of("America/New_York");
        final LocalDateTime gapStart = zone.getRules().nextTransition(Instant.parse("2262-01-01T00:00:00Z")).getDateTimeBefore();
        assertGapNanosecondPrecision(zone.getId(), gapStart.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z", Nanos.HOUR_NANOS);
    }

    @Test
    public void testNegativeTransitionNanosecondPrecision() throws NumericException {
        final TimeZoneRulesNanos rules = new TimeZoneRulesNanos(ZoneId.of("Europe/Berlin").getRules());
        final long transition = NanosTimestampDriver.INSTANCE.parseFloorLiteral("1893-03-31T23:06:32Z");
        Assert.assertEquals(3208 * Nanos.SECOND_NANOS, rules.getOffset(transition - 1));
        Assert.assertEquals(Nanos.HOUR_NANOS, rules.getOffset(transition));
        Assert.assertEquals(transition, rules.getNextDST(transition - 1));
        final long gapEnd = NanosTimestampDriver.INSTANCE.parseFloorLiteral("1893-04-01T00:06:32Z");
        Assert.assertEquals(3208 * Nanos.SECOND_NANOS, rules.getLocalOffset(gapEnd - 1));
        Assert.assertEquals(Nanos.HOUR_NANOS, rules.getLocalOffset(gapEnd));
    }

    @Test
    public void testRangeBoundaryCompatibility() {
        final List<String> zones = new ArrayList<>(ZoneId.getAvailableZoneIds());
        Collections.sort(zones);
        for (String zoneName : zones) {
            final ZoneId zone = ZoneId.of(zoneName);
            final TimeZoneRulesNanos rules = new TimeZoneRulesNanos(zone.getRules());
            for (long epoch : new long[]{Long.MIN_VALUE + 1, Long.MAX_VALUE}) {
                final Instant instant = Instant.ofEpochSecond(Math.floorDiv(epoch, Nanos.SECOND_NANOS), Math.floorMod(epoch, Nanos.SECOND_NANOS));
                final LocalDateTime local = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                final long offset = zone.getRules().getOffset(instant).getTotalSeconds() * Nanos.SECOND_NANOS;
                Assert.assertEquals(zoneName, offset, rules.getOffset(epoch));
                Assert.assertEquals(zoneName, offset, rules.getOffset(epoch, local.getYear()));
                final List<ZoneOffset> validOffsets = zone.getRules().getValidOffsets(local);
                final ZoneOffset localOffset = validOffsets.isEmpty() ? zone.getRules().getTransition(local).getOffsetBefore() : validOffsets.get(0);
                Assert.assertEquals(zoneName, localOffset.getTotalSeconds() * Nanos.SECOND_NANOS, rules.getLocalOffset(epoch));
                Assert.assertEquals(zoneName, localOffset.getTotalSeconds() * Nanos.SECOND_NANOS, rules.getLocalOffset(epoch, local.getYear()));
            }
        }
    }

    @Test
    public void testPerformance() {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesNanos> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesNanos(zone.getRules()));
        }

        long nanos = Nanos.toNanos(1900, 1, 1, 0, 0);
        long deadline = Nanos.toNanos(2615, 12, 31, 0, 0);

        while (nanos < deadline) {
            for (int i = 0, n = zones.size(); i < n; i++) {
                zoneRules.get(i).getOffset(nanos);
            }
            nanos += Nanos.DAY_NANOS;
        }
    }

    @Test
    public void testSingle() {
        ZoneId zone = ZoneId.of("GMT");
        TimeZoneRulesNanos rules = new TimeZoneRulesNanos(zone.getRules());

        int y = 2017;
        int m = 3;
        int d = 29;

        LocalDateTime dt = LocalDateTime.of(y, m, d, 0, 0);
        long nanos = Nanos.toNanos(y, m, d, 0, 0);

        ZonedDateTime zdt = dt.atZone(zone);
        long expected = zdt.getOffset().getTotalSeconds();

        // find out how much algo added to datetime itself
        long changed = Nanos.toNanos(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * 1000L;
        // add any extra time
        expected += (changed - nanos) / 1000;
        long offset = rules.getOffset(nanos, y);

        try {
            Assert.assertEquals(expected, offset / 1000);
        } catch (Throwable e) {
            System.out.println(zone.getId() + "; " + zdt + "; " + Nanos.toString(nanos + offset));
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
        final List<TimeZoneRulesNanos> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesNanos(zone.getRules()));
        }

        final DateTimeFormatter localDateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
        long nanos = Nanos.toNanos(1900, 1, 1, 0, 0);
        final long deadline = Nanos.toNanos(2115, 12, 31, 0, 0);
        final long step = Math.max(1, rnd.nextLong(30)) * Nanos.DAY_NANOS;
        LocalDateTime dt;
        while (nanos < deadline) {
            try {
                dt = LocalDateTime.parse(Nanos.toString(nanos), localDateTimeFormat);
            } catch (Throwable e) {
                System.out.println(nanos);
                throw e;
            }

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesNanos rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.toInstant().toEpochMilli() * Nanos.MILLI_NANOS;
                // find out how much algo added to datetime itself
                long offset = rules.getLocalOffset(dt.toInstant(ZoneOffset.UTC).toEpochMilli() * Nanos.MILLI_NANOS);
                long actual = nanos - offset;

                try {
                    Assert.assertEquals(expected, actual);
                } catch (Throwable e) {
                    System.out.println(nanos);
                    System.out.println(zone.getId() + "; " + zdt + "; " + Nanos.toString(actual));
                    System.out.println("e: " + expected + "; a: " + actual);
                    System.out.println(dt);
                    System.out.println(Nanos.toString(nanos));
                    throw e;
                }
            }
            nanos += step;
        }
    }

    @Test
    public void testUpperRangeLocalOffset() {
        final TimeZoneRulesNanos rules = new TimeZoneRulesNanos(ZoneId.of("America/New_York").getRules());
        final long local = Nanos.toNanos(2262, 4, 9, 0, 0);
        Assert.assertEquals(-4 * Nanos.HOUR_NANOS, rules.getLocalOffset(local));
        Assert.assertEquals(-4 * Nanos.HOUR_NANOS, rules.getLocalOffset(local, 2262));
    }

    @Test
    public void testUpperRangeNextDST() {
        final ZoneId zone = ZoneId.of("America/New_York");
        final TimeZoneRulesNanos rules = new TimeZoneRulesNanos(zone.getRules());
        final long transition = zone.getRules().nextTransition(Instant.parse("2262-01-01T00:00:00Z")).toEpochSecond() * Nanos.SECOND_NANOS;
        Assert.assertEquals(transition, rules.getNextDST(transition - 1));
        Assert.assertEquals(transition, rules.getNextDST(transition - 1, 2262));
        Assert.assertEquals(Long.MAX_VALUE, rules.getNextDST(transition));
        Assert.assertEquals(Long.MAX_VALUE, rules.getNextDST(transition, 2262));
        Assert.assertEquals(Long.MAX_VALUE, rules.getNextDST(Long.MAX_VALUE));
        Assert.assertEquals(Long.MAX_VALUE, new TimeZoneRulesNanos(ZoneId.of("GMT").getRules()).getNextDST(0));
    }

    @Test
    public void testUpperRangeOffset() {
        final TimeZoneRulesNanos rules = new TimeZoneRulesNanos(ZoneId.of("America/New_York").getRules());
        final long utc = Nanos.toNanos(2262, 4, 9, 6, 0);
        Assert.assertEquals(-4 * Nanos.HOUR_NANOS, rules.getOffset(utc));
        Assert.assertEquals(-4 * Nanos.HOUR_NANOS, rules.getOffset(utc, 2262));
    }

    private static void assertGapNanosecondPrecision(String zoneName, String localStart, long duration) throws NumericException {
        final TimeZoneRulesNanos rules = new TimeZoneRulesNanos(ZoneId.of(zoneName).getRules());
        final long start = NanosTimestampDriver.INSTANCE.parseFloorLiteral(localStart);
        Assert.assertEquals(0, rules.getDstGapOffset(start - 1));
        Assert.assertEquals(0, rules.getDstGapOffset(start));
        Assert.assertEquals(1, rules.getDstGapOffset(start + 1));
        Assert.assertEquals(999, rules.getDstGapOffset(start + 999));
        Assert.assertEquals(1000, rules.getDstGapOffset(start + 1000));
        Assert.assertEquals(123456789, rules.getDstGapOffset(start + 123456789));
        Assert.assertEquals(duration - 1, rules.getDstGapOffset(start + duration - 1));
        Assert.assertEquals(0, rules.getDstGapOffset(start + duration));
        Assert.assertEquals(0, rules.getDstGapOffset(start + duration + 1));
    }
}
