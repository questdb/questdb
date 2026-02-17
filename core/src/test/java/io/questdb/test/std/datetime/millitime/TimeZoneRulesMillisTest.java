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

package io.questdb.test.std.datetime.millitime;

import io.questdb.cairo.MillsTimestampDriver;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.datetime.millitime.TimeZoneRulesMillis;
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

public class TimeZoneRulesMillisTest {
    private static final Log LOG = LogFactory.getLog(TimeZoneRulesMillisTest.class);

    @Test
    public void testCompatibility() {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesMillis> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesMillis(zone.getRules()));
        }

        long epoch = Dates.toMillis(1900, 1, 1, 0, 0);
        long epochDeadline = Dates.toMillis(2115, 12, 31, 0, 0);

        while (epoch < epochDeadline) {
            int y = Dates.getYear(epoch);
            Instant dt = Instant.ofEpochMilli(epoch);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesMillis rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.getOffset().getTotalSeconds();
                // find out how much algo added to datetime itself
                long offset = rules.getOffset(epoch, y);

                try {
                    Assert.assertEquals(expected, offset / Dates.SECOND_MILLIS);
                } catch (Throwable e) {
                    System.out.println(zone.getId() + "; " + zdt + "; " + Dates.toString(epoch + offset));
                    System.out.println("e: " + expected + "; a: " + offset);
                    System.out.println(dt);
                    System.out.println(Dates.toString(epoch));
                    throw e;
                }
            }
            epoch += Dates.DAY_MILLIS;
        }
    }

    @Test
    public void testGapDuration() throws NumericException {
        final ZoneId zone = ZoneId.of("Europe/Berlin");
        final TimeZoneRulesMillis rules = new TimeZoneRulesMillis(zone.getRules());

        Assert.assertEquals(0, rules.getDstGapOffset(0));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1888-05-12T23:45:51.045Z")));

        // DST
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-03-28T01:00:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-03-28T02:00:00.000Z")));
        Assert.assertEquals(60000, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-03-28T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-03-28T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-03-28T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-10-31T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-10-31T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("2021-10-31T03:01:00.000Z")));

        // historical
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-03-30T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-03-30T02:00:00.000Z")));
        Assert.assertEquals(60000, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-03-30T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-03-30T03:00:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-03-30T03:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-10-26T01:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-10-26T02:01:00.000Z")));
        Assert.assertEquals(0, rules.getDstGapOffset(MillsTimestampDriver.floor("1997-10-26T03:01:00.000Z")));
    }

    @Test
    public void testPerformance() {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesMillis> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesMillis(zone.getRules()));
        }

        long millis = Dates.toMillis(1900, 1, 1, 0, 0);
        long deadline = Dates.toMillis(2615, 12, 31, 0, 0);

        while (millis < deadline) {
            for (int i = 0, n = zones.size(); i < n; i++) {
                try {
                    zoneRules.get(i).getOffset(millis);
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw e;
                }
            }
            millis += Dates.DAY_MILLIS;
        }
    }

    @Test
    public void testSingle() {
        ZoneId zone = ZoneId.of("GMT");
        TimeZoneRulesMillis rules = new TimeZoneRulesMillis(zone.getRules());

        int y = 2017;
        int m = 3;
        int d = 29;

        LocalDateTime dt = LocalDateTime.of(y, m, d, 0, 0);
        long millis = Dates.toMillis(y, m, d, 0, 0);

        ZonedDateTime zdt = dt.atZone(zone);
        long expected = zdt.getOffset().getTotalSeconds();

        // find out how much algo added to datetime itself
        long changed = Dates.toMillis(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * 1000L;
        // add any extra time
        expected += (changed - millis) / 1000;
        long offset = rules.getOffset(millis, y);

        try {
            Assert.assertEquals(expected, offset / 1000);
        } catch (Throwable e) {
            System.out.println(zone.getId() + "; " + zdt + "; " + Dates.toString(millis + offset));
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
        final List<TimeZoneRulesMillis> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesMillis(zone.getRules()));
        }

        final DateTimeFormatter localDateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
        long millis = Dates.toMillis(1900, 1, 1, 0, 0);
        final long deadline = Dates.toMillis(2115, 12, 31, 0, 0);
        final long step = Math.max(1, rnd.nextLong(30)) * Dates.DAY_MILLIS;

        while (millis < deadline) {
            LocalDateTime dt = LocalDateTime.parse(Dates.toString(millis), localDateTimeFormat);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesMillis rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.toInstant().toEpochMilli();
                // find out how much algo added to datetime itself
                long offset = rules.getLocalOffset(dt.toInstant(ZoneOffset.UTC).toEpochMilli());
                long actual = millis - offset;

                try {
                    Assert.assertEquals(expected, actual);
                } catch (Throwable e) {
                    System.out.println(zone.getId() + "; " + zdt + "; " + Dates.toString(actual));
                    System.out.println("e: " + expected + "; a: " + actual);
                    System.out.println(dt);
                    System.out.println(Dates.toString(millis));
                    throw e;
                }
            }
            millis += step;
        }
    }
}