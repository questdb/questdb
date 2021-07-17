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

package io.questdb.std.datetime;

import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;

public abstract class AbstractTimeZoneRules implements TimeZoneRules {
    public static final long SAVING_INSTANT_TRANSITION = Unsafe.getFieldOffset(ZoneRules.class, "savingsInstantTransitions");
    public static final long STANDARD_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "standardOffsets");
    public static final long LAST_RULES = Unsafe.getFieldOffset(ZoneRules.class, "lastRules");
    public static final long WALL_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "wallOffsets");
    private final long cutoffTransition;
    private final LongList historicTransitions = new LongList();
    private final ObjList<TransitionRule> rules;
    private final int ruleCount;
    private final int[] wallOffsets;
    private final long firstWall;
    private final long lastWall;
    private final long standardOffset;
    private final long multiplier;

    public AbstractTimeZoneRules(ZoneRules rules, long multiplier) {
        this.multiplier = multiplier;
        final long[] savingsInstantTransition = (long[]) Unsafe.getUnsafe().getObject(rules, SAVING_INSTANT_TRANSITION);

        if (savingsInstantTransition.length == 0) {
            ZoneOffset[] standardOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, STANDARD_OFFSETS);
            standardOffset = standardOffsets[0].getTotalSeconds() * multiplier;
        } else {
            standardOffset = Long.MIN_VALUE;
            for (int i = 0, n = savingsInstantTransition.length; i < n; i++) {
                historicTransitions.add(savingsInstantTransition[i] * multiplier);
            }
        }

        cutoffTransition = historicTransitions.getLast();

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
        this.firstWall = this.wallOffsets[0] * multiplier;
        this.lastWall = this.wallOffsets[wallOffsets.length - 1] * multiplier;
    }

    @Override
    public long getOffset(long utcEpoch, int year, boolean leap) {
        if (standardOffset != Long.MIN_VALUE) {
            return standardOffset;
        }

        if (ruleCount > 0 && utcEpoch > cutoffTransition) {
            return offsetFromRules(utcEpoch, year, leap);
        }

        if (utcEpoch > cutoffTransition) {
            return lastWall;
        }
        return offsetFromHistory(utcEpoch);
    }

    @Override
    public long getOffset(long utcEpoch) {
        final int y = getYear(utcEpoch);
        return getOffset(utcEpoch, y, isLeapYear(y));
    }

    @Override
    public long getNextDST(long utcEpoch, int year, boolean leap) {
        if (standardOffset != Long.MIN_VALUE) {
            // when we have standard offset the next DST does not exist
            // since we are trying to avoid unnecessary offset lookup the max long
            // should ensure all timestamps stay below next DST
            return Long.MAX_VALUE;
        }

        if (ruleCount > 0 && utcEpoch >= cutoffTransition) {
            return dstFromRules(utcEpoch, year, leap);
        }

        if (utcEpoch < cutoffTransition) {
            return dstFromHistory(utcEpoch);
        }

        return Long.MAX_VALUE;
    }

    @Override
    public long getNextDST(long utcEpoch) {
        final int y = getYear(utcEpoch);
        return getNextDST(utcEpoch, y, isLeapYear(y));
    }

    abstract protected long addDays(long epoch, int days);

    private long dstFromHistory(long epoch) {
        int index = historicTransitions.binarySearch(epoch);
        if (index == -1) {
            return Long.MAX_VALUE;
        }

        if (index < 0) {
            index = -index - 2;
        }
        return historicTransitions.getQuick(index + 1);
    }

    private long dstFromRules(long epoch, int year, boolean leap) {

        for (int i = 0; i < ruleCount; i++) {
            long date = getDSTFromRule(year, leap, i);
            if (epoch < date) {
                return date;
            }
        }

        if (ruleCount > 0) {
            return getDSTFromRule(year + 1, isLeapYear(year + 1), 0);
        }

        return Long.MAX_VALUE;
    }

    private long getDSTFromRule(int year, boolean leap, int i) {
        int offsetBefore;
        TransitionRule zr = rules.getQuick(i);
        offsetBefore = zr.offsetBefore;

        int dom = zr.dom;
        int month = zr.month;

        int dow = zr.dow;
        long date;
        if (dom < 0) {
            date = toEpoch(
                    year,
                    leap,
                    month,
                    getDaysPerMonth(month, leap) + 1 + dom,
                    zr.hour,
                    zr.minute
            ) + zr.second * multiplier;
            if (dow > -1) {
                date = previousOrSameDayOfWeek(date, dow);
            }
        } else {
            assert month > 0;
            date = toEpoch(year, leap, month, dom, zr.hour, zr.minute) + zr.second * multiplier;
            if (dow > -1) {
                date = nextOrSameDayOfWeek(date, dow);
            }
        }

        if (zr.midnightEOD) {
            date = addDays(date, 1);
        }

        switch (zr.timeDef) {
            case TransitionRule.UTC:
                date += (offsetBefore - ZoneOffset.UTC.getTotalSeconds()) * multiplier;
                break;
            case TransitionRule.STANDARD:
                date += (offsetBefore - zr.standardOffset) * multiplier;
                break;
            default:  // WALL
                break;
        }

        // go back to epoch epoch
        date -= offsetBefore * multiplier;
        return date;
    }

    abstract protected int getDaysPerMonth(int month, boolean leapYear);

    abstract protected int getYear(long epoch);

    abstract protected boolean isLeapYear(int year);

    abstract protected long nextOrSameDayOfWeek(long epoch, int dow);

    private long offsetFromHistory(long epoch) {
        int index = historicTransitions.binarySearch(epoch);
        if (index == -1) {
            return firstWall;
        }

        if (index < 0) {
            index = -index - 2;
        }
        return wallOffsets[index + 1] * multiplier;
    }

    private long offsetFromRules(long epoch, int year, boolean leap) {

        int offsetBefore;
        int offsetAfter = 0;

        for (int i = 0; i < ruleCount; i++) {
            TransitionRule zr = rules.getQuick(i);
            offsetBefore = zr.offsetBefore;
            offsetAfter = zr.offsetAfter;

            int dom = zr.dom;
            int month = zr.month;

            int dow = zr.dow;
            long date;
            if (dom < 0) {
                date = toEpoch(
                        year,
                        leap,
                        month,
                        getDaysPerMonth(month, leap) + 1 + dom,
                        zr.hour,
                        zr.minute
                ) + zr.second * multiplier;
                if (dow > -1) {
                    date = previousOrSameDayOfWeek(date, dow);
                }
            } else {
                assert month > 0;
                date = toEpoch(year, leap, month, dom, zr.hour, zr.minute) + zr.second * multiplier;
                if (dow > -1) {
                    date = nextOrSameDayOfWeek(date, dow);
                }
            }

            if (zr.midnightEOD) {
                date = addDays(date, 1);
            }

            switch (zr.timeDef) {
                case TransitionRule.UTC:
                    date += (offsetBefore - ZoneOffset.UTC.getTotalSeconds()) * multiplier;
                    break;
                case TransitionRule.STANDARD:
                    date += (offsetBefore - zr.standardOffset) * multiplier;
                    break;
                default:  // WALL
                    break;
            }

            // go back to epoch epoch
            date -= offsetBefore * multiplier;

            if (epoch < date) {
                return offsetBefore * multiplier;
            }
        }

        return offsetAfter * multiplier;
    }

    abstract protected long previousOrSameDayOfWeek(long epoch, int dow);

    abstract protected long toEpoch(int year, boolean leapYear, int month, int day, int hour, int min);
}
