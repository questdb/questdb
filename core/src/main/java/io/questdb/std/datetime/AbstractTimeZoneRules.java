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

package io.questdb.std.datetime;

import io.questdb.std.ConcurrentIntHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.util.function.IntFunction;

public abstract class AbstractTimeZoneRules implements TimeZoneRules {
    public static final long LAST_RULES = Unsafe.getFieldOffset(ZoneRules.class, "lastRules");
    public static final long SAVING_INSTANT_TRANSITION = Unsafe.getFieldOffset(ZoneRules.class, "savingsInstantTransitions");
    public static final long STANDARD_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "standardOffsets");
    public static final long WALL_OFFSETS = Unsafe.getFieldOffset(ZoneRules.class, "wallOffsets");
    private final IntFunction<Transition[]> computeTransitionsRef;
    private final long cutoffTransition;
    private final long firstWall;
    private final LongList historicTransitions = new LongList();
    private final long lastWall;
    private final long multiplier;
    private final int ruleCount;
    private final TransitionRule[] rules;
    private final long standardOffset;
    // year to transition list cache
    private final ConcurrentIntHashMap<Transition[]> transitionsCache = new ConcurrentIntHashMap<>();
    private final int[] wallOffsets;

    public AbstractTimeZoneRules(ZoneRules rules, long multiplier) {
        this.multiplier = multiplier;
        this.computeTransitionsRef = this::computeTransitions;
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

        final ZoneOffsetTransitionRule[] lastRules = (ZoneOffsetTransitionRule[]) Unsafe.getUnsafe().getObject(rules, LAST_RULES);
        this.rules = new TransitionRule[lastRules.length];
        for (int i = 0, n = lastRules.length; i < n; i++) {
            final ZoneOffsetTransitionRule zr = lastRules[i];
            final int timeDef;
            switch (zr.getTimeDefinition()) {
                case UTC:
                    timeDef = TransitionRule.UTC;
                    break;
                case STANDARD:
                    timeDef = TransitionRule.STANDARD;
                    break;
                default:
                    timeDef = TransitionRule.WALL;
                    break;
            }
            TransitionRule tr = new TransitionRule(
                    zr.getOffsetBefore().getTotalSeconds(),
                    zr.getOffsetAfter().getTotalSeconds(),
                    zr.getStandardOffset().getTotalSeconds(),
                    zr.getDayOfWeek() == null ? -1 : zr.getDayOfWeek().getValue(),
                    zr.getDayOfMonthIndicator(),
                    zr.getMonth().getValue(),
                    zr.isMidnightEndOfDay(),
                    zr.getLocalTime().getHour(),
                    zr.getLocalTime().getMinute(),
                    zr.getLocalTime().getSecond(),
                    timeDef
            );
            this.rules[i] = tr;
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
    public long getNextDST(long utcEpoch, int year) {
        if (standardOffset != Long.MIN_VALUE) {
            // when we have standard offset the next DST does not exist
            // since we are trying to avoid unnecessary offset lookup the max long
            // should ensure all timestamps stay below next DST
            return Long.MAX_VALUE;
        }

        if (ruleCount > 0 && utcEpoch >= cutoffTransition) {
            return dstFromRules(utcEpoch, year);
        }

        if (utcEpoch < cutoffTransition) {
            return dstFromHistory(utcEpoch);
        }

        return Long.MAX_VALUE;
    }

    @Override
    public long getNextDST(long utcEpoch) {
        final int y = getYear(utcEpoch);
        return getNextDST(utcEpoch, y);
    }

    @Override
    public long getOffset(long utcEpoch, int year) {
        if (standardOffset != Long.MIN_VALUE) {
            return standardOffset;
        }

        if (ruleCount > 0 && utcEpoch > cutoffTransition) {
            return offsetFromRules(utcEpoch, year);
        }

        if (utcEpoch > cutoffTransition) {
            return lastWall;
        }
        return offsetFromHistory(utcEpoch);
    }

    @Override
    public long getOffset(long utcEpoch) {
        final int y = getYear(utcEpoch);
        return getOffset(utcEpoch, y);
    }

    private Transition[] computeTransitions(int year) {
        final Transition[] transitions = new Transition[ruleCount];
        final boolean leap = isLeapYear(year);

        int offsetBefore;
        for (int i = 0; i < ruleCount; i++) {
            TransitionRule zr = rules[i];
            offsetBefore = zr.offsetBefore;

            int dom = zr.dom;
            int month = zr.month;

            int dow = zr.dow;
            long timestamp;
            if (dom < 0) {
                timestamp = toEpoch(
                        year,
                        leap,
                        month,
                        getDaysPerMonth(month, leap) + 1 + dom,
                        zr.hour,
                        zr.minute
                ) + zr.second * multiplier;
                if (dow > -1) {
                    timestamp = previousOrSameDayOfWeek(timestamp, dow);
                }
            } else {
                assert month > 0;
                timestamp = toEpoch(year, leap, month, dom, zr.hour, zr.minute) + zr.second * multiplier;
                if (dow > -1) {
                    timestamp = nextOrSameDayOfWeek(timestamp, dow);
                }
            }

            if (zr.midnightEOD) {
                timestamp = addDays(timestamp, 1);
            }

            switch (zr.timeDef) {
                case TransitionRule.UTC:
                    timestamp += (offsetBefore - ZoneOffset.UTC.getTotalSeconds()) * multiplier;
                    break;
                case TransitionRule.STANDARD:
                    timestamp += (offsetBefore - zr.standardOffset) * multiplier;
                    break;
                default: // WALL
                    break;
            }

            // go back to epoch
            timestamp -= offsetBefore * multiplier;

            transitions[i] = new Transition(zr.offsetBefore, zr.offsetAfter, timestamp);
        }
        return transitions;
    }

    private long dstFromHistory(long epoch) {
        int index = historicTransitions.binarySearch(epoch, Vect.BIN_SEARCH_SCAN_UP);
        if (index == -1) {
            return Long.MAX_VALUE;
        }

        if (index < 0) {
            index = -index - 2;
        }
        return historicTransitions.getQuick(index + 1);
    }

    private long dstFromRules(long epoch, int year) {
        for (int i = 0; i < ruleCount; i++) {
            long date = getDSTFromRule(year, i);
            if (epoch < date) {
                return date;
            }
        }

        if (ruleCount > 0) {
            return getDSTFromRule(year + 1, 0);
        }

        return Long.MAX_VALUE;
    }

    private long getDSTFromRule(int year, int i) {
        final Transition[] transitions = getTransitions(year);
        return transitions[i].dstTimestamp;
    }

    private Transition[] getTransitions(int year) {
        Transition[] transitions = transitionsCache.get(year);
        if (transitions != null) {
            return transitions;
        }
        return transitionsCache.computeIfAbsent(year, computeTransitionsRef);
    }

    private long offsetFromHistory(long epoch) {
        int index = historicTransitions.binarySearch(epoch, Vect.BIN_SEARCH_SCAN_UP);
        if (index == -1) {
            return firstWall;
        }

        if (index < 0) {
            index = -index - 2;
        }
        return wallOffsets[index + 1] * multiplier;
    }

    private long offsetFromRules(long epoch, int year) {
        int offsetBefore;
        int offsetAfter = 0;

        final Transition[] transitions = getTransitions(year);
        for (int i = 0, n = transitions.length; i < n; i++) {
            Transition tr = transitions[i];
            offsetBefore = tr.offsetBefore;
            offsetAfter = tr.offsetAfter;
            if (epoch < tr.dstTimestamp) {
                return offsetBefore * multiplier;
            }
        }

        return offsetAfter * multiplier;
    }

    abstract protected long addDays(long epoch, int days);

    abstract protected int getDaysPerMonth(int month, boolean leapYear);

    abstract protected int getYear(long epoch);

    abstract protected boolean isLeapYear(int year);

    abstract protected long nextOrSameDayOfWeek(long epoch, int dow);

    abstract protected long previousOrSameDayOfWeek(long epoch, int dow);

    abstract protected long toEpoch(int year, boolean leapYear, int month, int day, int hour, int min);
}
