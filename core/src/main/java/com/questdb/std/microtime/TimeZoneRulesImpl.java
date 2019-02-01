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

package com.questdb.std.microtime;

import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.Unsafe;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;

public class TimeZoneRulesImpl implements TimeZoneRules {
    public static final long SAVING_INSTANT_TRANSITION = Unsafe.getFieldOffset(ZoneRules.class, "savingsInstantTransitions");
    public static final long STANDARD_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "standardOffsets");
    public static final long LAST_RULES = Unsafe.getFieldOffset(ZoneRules.class, "lastRules");
    public static final long SAVINGS_LOCAL_TRANSITION = Unsafe.getFieldOffset(ZoneRules.class, "savingsLocalTransitions");
    public static final long WALL_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "wallOffsets");
    private final long cutoffTransition;
    private final LongList historicTransitions = new LongList();
    private final ObjList<TransitionRule> rules;
    private final int ruleCount;
    private final int[] wallOffsets;
    private final long firstWall;
    private final long lastWall;
    private final int historyOverlapCheckCutoff;
    private final String id;
    private final long standardOffset;

    public TimeZoneRulesImpl(String id, ZoneRules rules) {
        this.id = id;
        final long[] savingsInstantTransition = (long[]) Unsafe.getUnsafe().getObject(rules, SAVING_INSTANT_TRANSITION);

        if (savingsInstantTransition.length == 0) {
            ZoneOffset[] standardOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, STANDARD_OFFSETS);
            standardOffset = standardOffsets[0].getTotalSeconds() * Dates.SECOND_MICROS;
        } else {
            standardOffset = Long.MIN_VALUE;
        }

        LocalDateTime[] savingsLocalTransitions = (LocalDateTime[]) Unsafe.getUnsafe().getObject(rules, SAVINGS_LOCAL_TRANSITION);
        for (int i = 0, n = savingsLocalTransitions.length; i < n; i++) {
            LocalDateTime dt = savingsLocalTransitions[i];

            historicTransitions.add(Dates.toMicros(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute()) +
                    dt.getSecond() * Dates.SECOND_MICROS + dt.getNano() / 1000);
        }
        cutoffTransition = historicTransitions.getLast();
        historyOverlapCheckCutoff = historicTransitions.size() - 1;


        ZoneOffsetTransitionRule[] lastRules = (ZoneOffsetTransitionRule[]) Unsafe.getUnsafe().getObject(rules, LAST_RULES);
        this.rules = new ObjList<>(lastRules.length);
        for (int i = 0, n = lastRules.length; i < n; i++) {
            ZoneOffsetTransitionRule zr = lastRules[i];
            TransitionRule tr = new TransitionRule();
            tr.offsetBefore = zr.getOffsetBefore().getTotalSeconds();
            tr.offsetAfter = zr.getOffsetAfter().getTotalSeconds();
            tr.standardOffset = zr.getStandardOffset().getTotalSeconds();
            tr.dow = zr.getDayOfWeek() == null ? -1 : zr.getDayOfWeek().getValue();
            tr.dom = zr.getDayOfMonthIndicator();
            tr.month = zr.getMonth().getValue();
            tr.midnightEOD = zr.isMidnightEndOfDay();
            tr.hour = zr.getLocalTime().getHour();
            tr.minute = zr.getLocalTime().getMinute();
            tr.second = zr.getLocalTime().getSecond();
            switch (zr.getTimeDefinition()) {
                case UTC:
                    tr.timeDef = TransitionRule.UTC;
                    break;
                case STANDARD:
                    tr.timeDef = TransitionRule.STANDARD;
                    break;
                default:
                    tr.timeDef = TransitionRule.WALL;
                    break;
            }
            this.rules.add(tr);
        }

        this.ruleCount = lastRules.length;

        ZoneOffset[] wallOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, WALL_OFFSETS);
        this.wallOffsets = new int[wallOffsets.length];
        for (int i = 0, n = wallOffsets.length; i < n; i++) {
            this.wallOffsets[i] = wallOffsets[i].getTotalSeconds();
        }
        this.firstWall = this.wallOffsets[0] * Dates.SECOND_MICROS;
        this.lastWall = this.wallOffsets[wallOffsets.length - 1] * Dates.SECOND_MICROS;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public long getOffset(long micros, int year, boolean leap) {
        if (standardOffset != Long.MIN_VALUE) {
            return standardOffset;
        }

        if (ruleCount > 0 && micros > cutoffTransition) {
            return fromRules(micros, year, leap);
        }

        if (micros > cutoffTransition) {
            return lastWall;
        }

        return fromHistory(micros);
    }

    @Override
    public long getOffset(long micros) {
        int y = Dates.getYear(micros);
        return getOffset(micros, y, Dates.isLeapYear(y));
    }

    private long fromHistory(long micros) {
        int index = historicTransitions.binarySearch(micros);
        if (index == -1) {
            return firstWall;
        }

        if (index < 0) {
            index = -index - 2;
        } else if (index < historyOverlapCheckCutoff && historicTransitions.getQuick(index) == historicTransitions.getQuick(index + 1)) {
            index++;
        }

        if ((index & 1) == 0) {
            int offsetBefore = Unsafe.arrayGet(wallOffsets, index / 2);
            int offsetAfter = Unsafe.arrayGet(wallOffsets, index / 2 + 1);

            int delta = offsetAfter - offsetBefore;
            if (delta > 0) {
                // engage 0 transition logic
                return (delta + offsetAfter) * Dates.SECOND_MICROS;
            } else {
                return offsetBefore * Dates.SECOND_MICROS;
            }
        } else {
            return Unsafe.arrayGet(wallOffsets, index / 2 + 1) * Dates.SECOND_MICROS;
        }
    }

    private long fromRules(long micros, int year, boolean leap) {

        int offset = 0;

        for (int i = 0; i < ruleCount; i++) {
            TransitionRule zr = rules.getQuick(i);
            offset = zr.offsetBefore;
            int offsetAfter = zr.offsetAfter;

            int dom = zr.dom;
            int month = zr.month;

            int dow = zr.dow;
            long date;
            if (dom < 0) {
                date = Dates.toMicros(year, leap, month, Dates.getDaysPerMonth(month, leap) + 1 + dom, zr.hour, zr.minute) + zr.second * Dates.SECOND_MICROS;
                if (dow > -1) {
                    date = Dates.previousOrSameDayOfWeek(date, dow);
                }
            } else {
                date = Dates.toMicros(year, leap, month, dom, zr.hour, zr.minute) + zr.second * Dates.SECOND_MICROS;
                if (dow > -1) {
                    date = Dates.nextOrSameDayOfWeek(date, dow);
                }
            }

            if (zr.midnightEOD) {
                date = Dates.addDays(date, 1);
            }

            switch (zr.timeDef) {
                case TransitionRule.UTC:
                    date += (offset - ZoneOffset.UTC.getTotalSeconds()) * Dates.SECOND_MICROS;
                    break;
                case TransitionRule.STANDARD:
                    date += (offset - zr.standardOffset) * Dates.SECOND_MICROS;
                    break;
                default:  // WALL
                    break;
            }

            long delta = offsetAfter - offset;

            if (delta > 0) {
                if (micros < date) {
                    return offset * Dates.SECOND_MICROS;
                }

                if (micros < date + delta) {
                    return (offsetAfter + delta) * Dates.SECOND_MICROS;
                } else {
                    offset = offsetAfter;
                }
            } else {
                if (micros < date) {
                    return offset * Dates.SECOND_MICROS;
                } else {
                    offset = offsetAfter;
                }
            }
        }

        return offset * Dates.SECOND_MICROS;
    }

    private static class TransitionRule {
        public static final int UTC = 0;
        public static final int STANDARD = 1;
        public static final int WALL = 2;
        int offsetBefore;
        int offsetAfter;
        int standardOffset;
        int dow;
        int dom;
        int month;
        boolean midnightEOD;
        int hour;
        int minute;
        int second;
        int timeDef;
    }
}
