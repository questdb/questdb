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

package io.questdb.std.datetime;

import io.questdb.std.ConcurrentIntHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.util.function.IntFunction;

public abstract class AbstractTimeZoneRules implements TimeZoneRules {
    private static final int L1_CACHE_YEAR_HI = 2051; // exclusive
    private static final int L1_CACHE_YEAR_LO = 2000; // inclusive
    private static final long LAST_RULES_OFFSET = Unsafe.getFieldOffset(ZoneRules.class, "lastRules");
    private static final long SAVING_INSTANT_TRANSITIONS_OFFSET = Unsafe.getFieldOffset(ZoneRules.class, "savingsInstantTransitions");
    private static final long SAVING_LOCAL_TRANSITIONS_OFFSET = Unsafe.getFieldOffset(ZoneRules.class, "savingsLocalTransitions");
    private static final long STANDARD_OFFSETS_OFFSET = Unsafe.getFieldOffset(ZoneRules.class, "standardOffsets");
    private static final long WALL_OFFSETS_OFFSET = Unsafe.getFieldOffset(ZoneRules.class, "wallOffsets");
    private final IntFunction<Transition[]> computeTransitionsRef;
    private final long cutoffTransition;
    private final long firstWall;
    private final LongList historicTransitions = new LongList();
    private final long lastWall;
    private final long localCutoffTransition;
    private final LongList localHistoricTransitions = new LongList();
    private final long multiplier;
    private final int ruleCount;
    private final TransitionRule[] rules;
    private final long standardOffset;
    // year to array of transitions level 1 cache: [L1_CACHE_YEAR_LO, L1_CACHE_YEAR_HI) years only
    private final Transition[][] transitionsL1Cache;
    // year to array of transitions level 2 cache (all other years)
    private final ConcurrentIntHashMap<Transition[]> transitionsL2Cache;
    private final int[] wallOffsets;

    public AbstractTimeZoneRules(ZoneRules rules, long multiplier) {
        this.multiplier = multiplier;
        this.computeTransitionsRef = this::computeTransitions;

        final long[] savingsInstantTransitions = (long[]) Unsafe.getUnsafe().getObject(rules, SAVING_INSTANT_TRANSITIONS_OFFSET);
        if (savingsInstantTransitions.length == 0) {
            ZoneOffset[] standardOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, STANDARD_OFFSETS_OFFSET);
            standardOffset = standardOffsets[0].getTotalSeconds() * multiplier;
        } else {
            standardOffset = Long.MIN_VALUE;
            for (int i = 0, n = savingsInstantTransitions.length; i < n; i++) {
                historicTransitions.add(savingsInstantTransitions[i] * multiplier);
            }
        }
        cutoffTransition = historicTransitions.getLast();

        final ZoneOffsetTransitionRule[] lastRules = (ZoneOffsetTransitionRule[]) Unsafe.getUnsafe().getObject(rules, LAST_RULES_OFFSET);
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
            final TransitionRule tr = new TransitionRule(
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

        final ZoneOffset[] wallOffsets = (ZoneOffset[]) Unsafe.getUnsafe().getObject(rules, WALL_OFFSETS_OFFSET);
        this.wallOffsets = new int[wallOffsets.length];
        for (int i = 0, n = wallOffsets.length; i < n; i++) {
            this.wallOffsets[i] = wallOffsets[i].getTotalSeconds();
        }
        this.firstWall = this.wallOffsets[0] * multiplier;
        this.lastWall = this.wallOffsets[wallOffsets.length - 1] * multiplier;

        final LocalDateTime[] savingsLocalTransitions = (LocalDateTime[]) Unsafe.getUnsafe().getObject(rules, SAVING_LOCAL_TRANSITIONS_OFFSET);
        for (int i = 0, n = savingsLocalTransitions.length; i < n; i++) {
            localHistoricTransitions.add(savingsLocalTransitions[i].toInstant(ZoneOffset.UTC).getEpochSecond() * multiplier);
        }
        localCutoffTransition = localHistoricTransitions.getLast();

        // warm-up L1 transitions cache
        final int cutoffYear = getYear(cutoffTransition);
        if (ruleCount > 0 && cutoffYear < L1_CACHE_YEAR_HI) {
            transitionsL1Cache = new Transition[L1_CACHE_YEAR_HI - L1_CACHE_YEAR_LO][];
            for (int i = Math.max(0, cutoffYear - L1_CACHE_YEAR_LO), n = transitionsL1Cache.length; i < n; i++) {
                transitionsL1Cache[i] = computeTransitions(i + L1_CACHE_YEAR_LO);
            }
        } else {
            transitionsL1Cache = null;
        }

        transitionsL2Cache = ruleCount > 0 ? new ConcurrentIntHashMap<>() : null;
    }

    @Override
    public long getDstGapOffset(long localEpoch) {
        if (standardOffset != Long.MIN_VALUE) {
            return 0; // no offset switches, no gaps
        }

        if (ruleCount > 0 && localEpoch > localCutoffTransition) {
            return gapOffsetFromRules(localEpoch);
        }

        if (localEpoch > localCutoffTransition) {
            return 0; // no offset switches, no gaps
        }
        return gapOffsetFromHistory(localEpoch);
    }

    @Override
    public long getLocalOffset(long localEpoch, int year) {
        if (standardOffset != Long.MIN_VALUE) {
            return standardOffset;
        }

        if (ruleCount > 0 && localEpoch > localCutoffTransition) {
            return localOffsetFromRules(localEpoch, year);
        }

        if (localEpoch > localCutoffTransition) {
            return lastWall;
        }
        return localOffsetFromHistory(localEpoch);
    }

    @Override
    public long getLocalOffset(long localEpoch) {
        final int y = getYear(localEpoch);
        return getLocalOffset(localEpoch, y);
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

    @Override
    public boolean hasFixedOffset() {
        return standardOffset != Long.MIN_VALUE;
    }

    private Transition[] computeTransitions(int year) {
        final Transition[] transitions = new Transition[ruleCount];
        final boolean leap = isLeapYear(year);

        for (int i = 0; i < ruleCount; i++) {
            final TransitionRule zr = rules[i];
            final int offsetBefore = zr.offsetBefore;

            final int dom = zr.dom;
            final int month = zr.month;

            final int dow = zr.dow;
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

            transitions[i] = new Transition(
                    zr.offsetBefore * multiplier,
                    zr.offsetAfter * multiplier,
                    timestamp - offsetBefore * multiplier // go back to epoch
            );
        }
        return transitions;
    }

    private long dstFromHistory(long utcEpoch) {
        int index = historicTransitions.binarySearch(utcEpoch, Vect.BIN_SEARCH_SCAN_UP);
        if (index == -1) {
            return Long.MAX_VALUE;
        }

        if (index < 0) {
            index = -index - 2;
        }
        return historicTransitions.getQuick(index + 1);
    }

    private long dstFromRules(long utcEpoch, int year) {
        for (int i = 0; i < ruleCount; i++) {
            long date = getDSTFromRule(year, i);
            if (utcEpoch < date) {
                return date;
            }
        }

        if (ruleCount > 0) {
            return getDSTFromRule(year + 1, 0);
        }

        return Long.MAX_VALUE;
    }

    private long gapOffsetFromHistory(long localEpoch) {
        int index = localHistoricTransitions.binarySearch(localEpoch, Vect.BIN_SEARCH_SCAN_UP);
        if (index == -1) {
            return 0; // no offset switches, no gaps
        }

        if (index < 0) {
            index = -index - 2;
        }

        // check if the timestamp is within transition boundaries
        if ((index & 1) == 0) {
            // check if it's a gap transition
            int offsetBefore = wallOffsets[index / 2];
            int offsetAfter = wallOffsets[(index / 2) + 1];
            if (offsetBefore < offsetAfter) {
                return localEpoch - localHistoricTransitions.get(index);
            }
        }
        return 0;
    }

    private long gapOffsetFromRules(long localEpoch) {
        final int year = getYear(localEpoch);
        final Transition[] transitions = getTransitions(year);
        for (int i = 0, n = transitions.length; i < n; i++) {
            final Transition tr = transitions[i];
            // check for gap transitions
            final long localBefore = tr.transition + tr.offsetBefore;
            final long localAfter = tr.transition + tr.offsetAfter;
            // check if the timestamp is within transition boundaries
            if (localEpoch >= localBefore && localEpoch < localAfter) {
                return localEpoch - localBefore;
            }
        }
        return 0;
    }

    private long getDSTFromRule(int year, int i) {
        final Transition[] transitions = getTransitions(year);
        return transitions[i].transition;
    }

    private Transition[] getTransitions(int year) {
        if (year >= L1_CACHE_YEAR_LO && year < L1_CACHE_YEAR_HI) {
            return transitionsL1Cache[year - L1_CACHE_YEAR_LO];
        }

        final Transition[] transitions = transitionsL2Cache.get(year);
        if (transitions != null) {
            return transitions;
        }
        return transitionsL2Cache.computeIfAbsent(year, computeTransitionsRef);
    }

    private long localOffsetFromHistory(long localEpoch) {
        int index = localHistoricTransitions.binarySearch(localEpoch, Vect.BIN_SEARCH_SCAN_UP);
        if (index == -1) {
            return firstWall;
        }

        if (index < 0) {
            index = -index - 2;
        }

        // (index + 1) & ~0x1 aligns the index to 2, similar to how it's done in Bytes#align2b()
        index = (index + 1) & ~0x1;
        return wallOffsets[index / 2] * multiplier;
    }

    private long localOffsetFromRules(long localEpoch, int year) {
        final Transition[] transitions = getTransitions(year);
        long offsetAfterUs = 0;
        for (int i = 0, n = transitions.length; i < n; i++) {
            final Transition tr = transitions[i];
            final long localTransition = tr.transition + tr.offsetBefore;
            // in case of gap transition, e.g. +01:00 to +02:00, use the "before" offset
            // for non-existing local timestamps
            final long gapDuration = Math.max(tr.offsetAfter - tr.offsetBefore, 0);
            if (localEpoch < localTransition || localEpoch < localTransition + gapDuration) {
                return tr.offsetBefore;
            }
            offsetAfterUs = tr.offsetAfter;
        }
        return offsetAfterUs;
    }

    private long offsetFromHistory(long utcEpoch) {
        int index = historicTransitions.binarySearch(utcEpoch, Vect.BIN_SEARCH_SCAN_UP);
        if (index == -1) {
            return firstWall;
        }

        if (index < 0) {
            index = -index - 2;
        }
        return wallOffsets[index + 1] * multiplier;
    }

    private long offsetFromRules(long utcEpoch, int year) {
        final Transition[] transitions = getTransitions(year);
        long offsetAfterUs = 0;
        for (int i = 0, n = transitions.length; i < n; i++) {
            final Transition tr = transitions[i];
            if (utcEpoch < tr.transition) {
                return tr.offsetBefore;
            }
            offsetAfterUs = tr.offsetAfter;
        }
        return offsetAfterUs;
    }

    protected abstract long addDays(long epoch, int days);

    protected abstract int getDaysPerMonth(int month, boolean leapYear);

    protected abstract int getYear(long epoch);

    protected abstract boolean isLeapYear(int year);

    protected abstract long nextOrSameDayOfWeek(long epoch, int dow);

    protected abstract long previousOrSameDayOfWeek(long epoch, int dow);

    protected abstract long toEpoch(int year, boolean leapYear, int month, int day, int hour, int min);
}
