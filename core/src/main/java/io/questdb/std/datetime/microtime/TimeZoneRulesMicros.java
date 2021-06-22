/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std.datetime.microtime;

import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.TransitionRule;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;

public class TimeZoneRulesMicros implements TimeZoneRules {
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
    private final long standardOffset;

    public TimeZoneRulesMicros(ZoneRules rules) {
        final long[] savingsInstantTransition = (long[]) Unsafe.getUnsafe().getObject(rules, SAVING_INSTANT_TRANSITION);

        if (savingsInstantTransition.length == 0) {
            ZoneOffset[] standardOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, STANDARD_OFFSETS);
            standardOffset = standardOffsets[0].getTotalSeconds() * Timestamps.SECOND_MICROS;
        } else {
            standardOffset = Long.MIN_VALUE;
        }

        LocalDateTime[] savingsLocalTransitions = (LocalDateTime[]) Unsafe.getUnsafe().getObject(rules, SAVINGS_LOCAL_TRANSITION);
        for (int i = 0, n = savingsLocalTransitions.length; i < n; i++) {
            LocalDateTime dt = savingsLocalTransitions[i];

            historicTransitions.add(Timestamps.toMicros(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute()) +
                    dt.getSecond() * Timestamps.SECOND_MICROS + dt.getNano() / 1000);
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
        this.firstWall = this.wallOffsets[0] * Timestamps.SECOND_MICROS;
        this.lastWall = this.wallOffsets[wallOffsets.length - 1] * Timestamps.SECOND_MICROS;
    }

    @Override
    public long getOffset(long utc, int year, boolean leap) {
        if (standardOffset != Long.MIN_VALUE) {
            return standardOffset;
        }

        if (ruleCount > 0 && utc > cutoffTransition) {
            return fromRules(utc, year, leap);
        }

        if (utc > cutoffTransition) {
            return lastWall;
        }

        return fromHistory(utc);
    }

    @Override
    public long getOffset(long utcOffset) {
        int y = Timestamps.getYear(utcOffset);
        return getOffset(utcOffset, y, Timestamps.isLeapYear(y));
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
            int offsetBefore = wallOffsets[index / 2];
            int offsetAfter = wallOffsets[index / 2 + 1];

            int delta = offsetAfter - offsetBefore;
            if (delta > 0) {
                // engage 0 transition logic
                return (delta + offsetAfter) * Timestamps.SECOND_MICROS;
            } else {
                return offsetBefore * Timestamps.SECOND_MICROS;
            }
        } else {
            return wallOffsets[index / 2 + 1] * Timestamps.SECOND_MICROS;
        }
    }

    private long fromRules(long utcMicros, int year, boolean leap) {

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
                date = Timestamps.toMicros(year, leap, month, Timestamps.getDaysPerMonth(month, leap) + 1 + dom, zr.hour, zr.minute) + zr.second * Timestamps.SECOND_MICROS;
                if (dow > -1) {
                    date = Timestamps.previousOrSameDayOfWeek(date, dow);
                }
            } else {
                date = Timestamps.toMicros(year, leap, month, dom, zr.hour, zr.minute) + zr.second * Timestamps.SECOND_MICROS;
                if (dow > -1) {
                    date = Timestamps.nextOrSameDayOfWeek(date, dow);
                }
            }

            if (zr.midnightEOD) {
                date = Timestamps.addDays(date, 1);
            }

            switch (zr.timeDef) {
                case TransitionRule.UTC:
                    date += (offset - ZoneOffset.UTC.getTotalSeconds()) * Timestamps.SECOND_MICROS;
                    break;
                case TransitionRule.STANDARD:
                    date += (offset - zr.standardOffset) * Timestamps.SECOND_MICROS;
                    break;
                default:  // WALL
                    break;
            }

            long delta = offsetAfter - offset;

            if (delta > 0) {
                if (utcMicros < date) {
                    return offset * Timestamps.SECOND_MICROS;
                }

                if (utcMicros < date + delta) {
                    return (offsetAfter + delta) * Timestamps.SECOND_MICROS;
                } else {
                    offset = offsetAfter;
                }
            } else {
                if (utcMicros < date) {
                    return offset * Timestamps.SECOND_MICROS;
                } else {
                    offset = offsetAfter;
                }
            }
        }

        return offset * Timestamps.SECOND_MICROS;
    }
}
