package com.questdb.std.time;

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
    public void testCompatibility() throws Exception {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesImpl> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesImpl(zone.getRules()));
        }

        long millis = Dates.toMillis(1900, 1, 1, 0, 0);
        long deadline = Dates.toMillis(2115, 12, 31, 0, 0);

        while (millis < deadline) {
            int y = Dates.getYear(millis);
            boolean leap = Dates.isLeapYear(y);
            int m = Dates.getMonthOfYear(millis, y, leap);
            int d = Dates.getDayOfMonth(millis, y, m, leap);

            LocalDateTime dt = LocalDateTime.of(y, m, d, 0, 0);

            for (int i = 0, n = zones.size(); i < n; i++) {
                ZoneId zone = zones.get(i);
                TimeZoneRulesImpl rules = zoneRules.get(i);

                ZonedDateTime zdt = dt.atZone(zone);

                long expected = zdt.getOffset().getTotalSeconds();
                // find out how much algo added to datetime itself
                long changed = Dates.toMillis(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * 1000;
                // add any extra time
                expected += (changed - millis) / 1000;

                long offset = rules.getOffset(millis);

                try {
                    Assert.assertEquals(expected, offset / 1000);
                } catch (Throwable e) {
                    System.out.println(zone.getId() + "; " + zdt + "; " + Dates.toString(millis + offset));
                    System.out.println("e: " + expected + "; a: " + offset);
                    System.out.println(dt);
                    System.out.println(Dates.toString(millis));
                    throw e;
                }
            }
            millis += Dates.DAY_MILLIS;
        }
    }

    @Test
    public void testPerformance() throws Exception {
        Set<String> allZones = ZoneId.getAvailableZoneIds();
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);
        List<ZoneId> zones = new ArrayList<>(zoneList.size());
        List<TimeZoneRulesImpl> zoneRules = new ArrayList<>(zoneList.size());

        for (String z : zoneList) {
            ZoneId zone = ZoneId.of(z);
            zones.add(zone);
            zoneRules.add(new TimeZoneRulesImpl(zone.getRules()));
        }

        long millis = Dates.toMillis(1900, 1, 1, 0, 0);
        long deadline = Dates.toMillis(2615, 12, 31, 0, 0);

        while (millis < deadline) {
            for (int i = 0, n = zones.size(); i < n; i++) {
                zoneRules.get(i).getOffset(millis);
            }
            millis += Dates.DAY_MILLIS;
        }
    }

    @Test
    public void testSingle() throws Exception {
        ZoneId zone = ZoneId.of("GMT");
        TimeZoneRulesImpl rules = new TimeZoneRulesImpl(zone.getRules());

        int y = 2017;
        int m = 3;
        int d = 29;

        LocalDateTime dt = LocalDateTime.of(y, m, d, 0, 0);
        long millis = Dates.toMillis(y, m, d, 0, 0);

        ZonedDateTime zdt = dt.atZone(zone);
        long expected = zdt.getOffset().getTotalSeconds();

        // find out how much algo added to datetime itself
        long changed = Dates.toMillis(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute()) + zdt.getSecond() * 1000;
        // add any extra time
        expected += (changed - millis) / 1000;
        long offset = rules.getOffset(millis);

        try {
            Assert.assertEquals(expected, offset / 1000);
        } catch (Throwable e) {
            System.out.println(zone.getId() + "; " + zdt + "; " + Dates.toString(millis + offset));
            throw e;
        }
    }
}