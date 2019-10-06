/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.std.microtime;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TimeZoneRulesImplTest {

    @Test
    public void testCompatibility() {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesImpl> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesImpl(z, zone.getRules()));
        }

        long micros = Timestamps.toMicros(1900, 1, 1, 0, 0);
        long deadline = Timestamps.toMicros(2115, 12, 31, 0, 0);

        while (micros < deadline) {
            int y = Timestamps.getYear(micros);
            boolean leap = Timestamps.isLeapYear(y);
            int m = Timestamps.getMonthOfYear(micros, y, leap);
            int d = Timestamps.getDayOfMonth(micros, y, m, leap);

            LocalDateTime dt = LocalDateTime.of(y, m, d, 0, 0);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesImpl rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.getOffset().getTotalSeconds();
                // find out how much algo added to datetime itself
                long changed = Timestamps.toMicros(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * Timestamps.SECOND_MICROS;
                // add any extra time
                expected += (changed - micros) / Timestamps.SECOND_MICROS;

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
        List<TimeZoneRulesImpl> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesImpl(z, zone.getRules()));
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
        TimeZoneRulesImpl rules = new TimeZoneRulesImpl("GMT", zone.getRules());

        int y = 2017;
        int m = 3;
        int d = 29;

        LocalDateTime dt = LocalDateTime.of(y, m, d, 0, 0);
        long millis = Timestamps.toMicros(y, m, d, 0, 0);

        ZonedDateTime zdt = dt.atZone(zone);
        long expected = zdt.getOffset().getTotalSeconds();

        // find out how much algo added to datetime itself
        long changed = Timestamps.toMicros(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * 1000;
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