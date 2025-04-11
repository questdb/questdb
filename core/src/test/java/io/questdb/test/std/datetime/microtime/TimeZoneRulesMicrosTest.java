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

package io.questdb.test.std.datetime.microtime;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimeZoneRulesMicros;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
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

public class TimeZoneRulesMicrosTest {
    private static final Log LOG = LogFactory.getLog(TimeZoneRulesMicrosTest.class);

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

        long micros = Timestamps.toMicros(1900, 1, 1, 0, 0);
        long deadline = Timestamps.toMicros(2115, 12, 31, 0, 0);

        while (micros < deadline) {
            final int y = Timestamps.getYear(micros);

            Instant dt = Instant.ofEpochMilli(micros / 1000);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesMicros rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.getOffset().getTotalSeconds();
                // find out how much algo added to datetime itself
                long offset = rules.getOffset(micros, y);

                try {
                    Assert.assertEquals(expected, offset / Timestamps.SECOND_MICROS);
                } catch (Throwable e) {
                    System.out.println(zone.getId() + "; " + zdt + "; " + Timestamps.toString(micros + offset));
                    System.out.println("e: " + expected + "; a: " + offset);
                    System.out.println(dt);
                    System.out.println(Timestamps.toString(micros));
                    throw e;
                }
            }
            micros += Timestamps.DAY_MICROS;
        }
    }

    @Test
    public void testGapDuration() throws NumericException {
        final ZoneId zone = ZoneId.of("Europe/Berlin");
        final TimeZoneRulesMicros rules = new TimeZoneRulesMicros(zone.getRules());

        Assert.assertEquals(0, rules.getGapDuration(0));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1888-05-12T23:45:51.045Z")));

        // DST
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-03-28T01:00:00.000Z")));
        Assert.assertEquals(3600000000L, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-03-28T02:00:00.000Z")));
        Assert.assertEquals(3600000000L, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-03-28T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-03-28T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-03-28T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-10-31T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-10-31T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("2021-10-31T03:01:00.000Z")));

        // historical
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-03-30T01:01:00.000Z")));
        Assert.assertEquals(3600000000L, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-03-30T02:00:00.000Z")));
        Assert.assertEquals(3600000000L, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-03-30T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-03-30T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-03-30T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-10-26T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-10-26T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getGapDuration(TimestampFormatUtils.parseTimestamp("1997-10-26T03:01:00.000Z")));
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

        long millis = Timestamps.toMicros(1900, 1, 1, 0, 0);
        long deadline = Timestamps.toMicros(2615, 12, 31, 0, 0);

        while (millis < deadline) {
            for (int i = 0, n = zones.size(); i < n; i++) {
                zoneRules.get(i).getOffset(millis);
            }
            millis += Timestamps.DAY_MICROS;
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
        long millis = Timestamps.toMicros(y, m, d, 0, 0);

        ZonedDateTime zdt = dt.atZone(zone);
        long expected = zdt.getOffset().getTotalSeconds();

        // find out how much algo added to datetime itself
        long changed = Timestamps.toMicros(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * 1000L;
        // add any extra time
        expected += (changed - millis) / 1000;
        long offset = rules.getOffset(millis, y);

        try {
            Assert.assertEquals(expected, offset / 1000);
        } catch (Throwable e) {
            System.out.println(zone.getId() + "; " + zdt + "; " + Timestamps.toString(millis + offset));
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
        long micros = Timestamps.toMicros(1900, 1, 1, 0, 0);
        final long deadline = Timestamps.toMicros(2115, 12, 31, 0, 0);
        final long step = Math.max(1, rnd.nextLong(30)) * Timestamps.DAY_MICROS;

        while (micros < deadline) {
            LocalDateTime dt = LocalDateTime.parse(Timestamps.toString(micros), localDateTimeFormat);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesMicros rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.toInstant().toEpochMilli() * Timestamps.MILLI_MICROS;
                // find out how much algo added to datetime itself
                long offset = rules.getLocalOffset(dt.toInstant(ZoneOffset.UTC).toEpochMilli() * Timestamps.MILLI_MICROS);
                long actual = micros - offset;

                try {
                    Assert.assertEquals(expected, actual);
                } catch (Throwable e) {
                    System.out.println(zone.getId() + "; " + zdt + "; " + Timestamps.toString(actual));
                    System.out.println("e: " + expected + "; a: " + actual);
                    System.out.println(dt);
                    System.out.println(Timestamps.toString(micros));
                    throw e;
                }
            }
            micros += step;
        }
    }
}