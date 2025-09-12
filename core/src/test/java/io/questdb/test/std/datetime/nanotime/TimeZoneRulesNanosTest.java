/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
}
