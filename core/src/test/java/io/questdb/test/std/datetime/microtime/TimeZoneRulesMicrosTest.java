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

import io.questdb.std.datetime.microtime.TimeZoneRulesMicros;
import io.questdb.std.datetime.microtime.Timestamps;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TimeZoneRulesMicrosTest {

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
            final boolean leap = Timestamps.isLeapYear(y);

            Instant dt = Instant.ofEpochMilli(micros / 1000);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesMicros rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.getOffset().getTotalSeconds();
                // find out how much algo added to datetime itself
                long offset = rules.getOffset(micros, y, leap);

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
        long offset = rules.getOffset(millis, y, Timestamps.isLeapYear(y));

        try {
            Assert.assertEquals(expected, offset / 1000);
        } catch (Throwable e) {
            System.out.println(zone.getId() + "; " + zdt + "; " + Timestamps.toString(millis + offset));
            throw e;
        }
    }
}