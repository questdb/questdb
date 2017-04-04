/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.std;

import com.questdb.misc.Dates;
import com.questdb.misc.Unsafe;

import java.time.*;
import java.time.temporal.TemporalAdjusters;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;

public class TimeZoneRules {
    public static final long SAVING_INSTANT_TRANSITION;
    public static final long STANDARD_OFFSETS;
    public static final long LAST_RULES;
    public static final long SAVINGS_LOCAL_TRANSITION;
    public static final long WALL_OFFSETS;
    private final long cutoffTransition;
    private final LongList historicTransitions = new LongList();
    private final ZoneOffsetTransitionRule[] lastRules;
    private final int[] wallOffsets;
    private long standardOffset;

    public TimeZoneRules(ZoneRules rules) {
        final long[] savingsInstantTransition = (long[]) Unsafe.getUnsafe().getObject(rules, SAVING_INSTANT_TRANSITION);

        if (savingsInstantTransition.length == 0) {
            ZoneOffset[] standardOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, STANDARD_OFFSETS);
            standardOffset = standardOffsets[0].getTotalSeconds() * 1000;
        } else {
            standardOffset = Long.MIN_VALUE;
        }

        LocalDateTime[] savingsLocalTransitions = (LocalDateTime[]) Unsafe.getUnsafe().getObject(rules, SAVINGS_LOCAL_TRANSITION);
        for (int i = 0, n = savingsLocalTransitions.length; i < n; i++) {
            LocalDateTime dt = savingsLocalTransitions[i];

            historicTransitions.add(Dates.toMillis(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute()) +
                    dt.getSecond() * 1000 +
                    dt.getNano() / 1000);
        }
        cutoffTransition = historicTransitions.getLast();

        this.lastRules = (ZoneOffsetTransitionRule[]) Unsafe.getUnsafe().getObject(rules, LAST_RULES);
        ZoneOffset[] wallOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, WALL_OFFSETS);
        this.wallOffsets = new int[wallOffsets.length];
        for (int i = 0, n = wallOffsets.length; i < n; i++) {
            this.wallOffsets[i] = wallOffsets[i].getTotalSeconds();
        }
    }

    public static void main(String[] args) {
        LocalDate date = LocalDate.of(2017, 4, 3);
        System.out.println(date);
        System.out.println(date.with(TemporalAdjusters.previousOrSame(DayOfWeek.TUESDAY)));
    }

    public long getOffset(long millis) {
        if (standardOffset != Long.MIN_VALUE) {
            return standardOffset;
        }

        int n = lastRules.length;
        if (n > 0 && millis > cutoffTransition) {
            int year = Dates.getYear(millis);
            boolean leap = Dates.isLeapYear(year);

            int offset = 0;

            for (int i = 0; i < n; i++) {
                ZoneOffsetTransitionRule zr = Unsafe.arrayGet(lastRules, i);
                offset = zr.getOffsetBefore().getTotalSeconds();
                ZoneOffset offsetAfter = zr.getOffsetAfter();
                LocalTime time = zr.getLocalTime();

                int dom = zr.getDayOfMonthIndicator();
                int month = zr.getMonth().getValue();

                DayOfWeek dow = zr.getDayOfWeek();
                long date;
                if (dom < 0) {
                    date = Dates.toMillis(year, month, Dates.getDaysPerMonth(month, leap) + 1 + dom, time.getHour(), time.getMinute()) + time.getSecond() * Dates.SECOND_MILLIS;
                    if (dow != null) {
                        date = Dates.previousOrSameDayOfWeek(date, dow.getValue());
                    }
                } else {
                    date = Dates.toMillis(year, month, dom, time.getHour(), time.getMinute()) + time.getSecond() * Dates.SECOND_MILLIS;
                    if (dow != null) {
                        date = Dates.nextOrSameDayOfWeek(date, dow.getValue());
                    }
                }

                if (zr.isMidnightEndOfDay()) {
                    date = Dates.addDays(date, 1);
                }

                switch (zr.getTimeDefinition()) {
                    case UTC:
                        date += (offset - ZoneOffset.UTC.getTotalSeconds()) * Dates.SECOND_MILLIS;
                        break;
                    case STANDARD:
                        date += (offset - zr.getStandardOffset().getTotalSeconds()) * Dates.SECOND_MILLIS;
                        break;
                    default:  // WALL
                        break;
                }

                long delta = offsetAfter.getTotalSeconds() - offset;

                if (delta > 0) {
                    if (millis < date) {
                        return offset * Dates.SECOND_MILLIS;
                    }

                    if (millis < date + delta) {
                        return (offsetAfter.getTotalSeconds() + delta) * Dates.SECOND_MILLIS;
                    } else {
                        offset = offsetAfter.getTotalSeconds();
                    }
                } else {
                    if (millis < date) {
                        return offset * Dates.SECOND_MILLIS;
                    } else {
                        offset = offsetAfter.getTotalSeconds();
                    }
                }
            }

            return offset * Dates.SECOND_MILLIS;

        }


        int index = historicTransitions.binarySearch(millis);
        if (index == -1) {
            return Unsafe.arrayGet(wallOffsets, 0) * Dates.SECOND_MILLIS;
        }

        if (index < 0) {
            index = -index - 2;
        } else if (index < historicTransitions.size() - 1 && historicTransitions.getQuick(index) == historicTransitions.getQuick(index + 1)) {
            index++;
        }

        if ((index & 1) == 0) {
            int offsetBefore = Unsafe.arrayGet(wallOffsets, index / 2);
            int offsetAfter = Unsafe.arrayGet(wallOffsets, index / 2 + 1);

            int delta = offsetAfter - offsetBefore;
            if (delta > 0) {
                // engage 0 transition logic
                return (delta + offsetAfter) * Dates.SECOND_MILLIS;

            } else {
                return offsetBefore * Dates.SECOND_MILLIS;
            }
        } else {
            return Unsafe.arrayGet(wallOffsets, index / 2 + 1) * Dates.SECOND_MILLIS;
        }
    }

    static {
        try {
            SAVING_INSTANT_TRANSITION = Unsafe.getFieldOffset(ZoneRules.class, "savingsInstantTransitions");
            STANDARD_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "standardOffsets");
            LAST_RULES = Unsafe.getFieldOffset(ZoneRules.class, "lastRules");
            SAVINGS_LOCAL_TRANSITION = Unsafe.getFieldOffset(ZoneRules.class, "savingsLocalTransitions");
            WALL_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "wallOffsets");
        } catch (NoSuchFieldException e) {
            throw new Error(e);
        }
    }
}
