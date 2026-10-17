/*+*****************************************************************************
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

package io.questdb.std.datetime.nanotime;

import io.questdb.std.datetime.AbstractTimeZoneRules;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimeZoneRulesMicros;

import java.time.zone.ZoneRules;

public class TimeZoneRulesNanos extends AbstractTimeZoneRules {
    // Transitions of the last years of the nanosecond range overflow, so instants and local times
    // from this bound on convert through the microsecond rules.
    private static final long UPPER_RANGE = Nanos.toNanos(2261, 1, 1);
    private final TimeZoneRulesMicros upperRangeRules;

    public TimeZoneRulesNanos(ZoneRules rules) {
        super(rules, Nanos.SECOND_NANOS);
        this.upperRangeRules = new TimeZoneRulesMicros(rules);
    }

    @Override
    public long getDstGapOffset(long localEpoch) {
        if (localEpoch < UPPER_RANGE) {
            return super.getDstGapOffset(localEpoch);
        }
        final long micros = Math.floorDiv(localEpoch, Nanos.MICRO_NANOS);
        final long remainder = Math.floorMod(localEpoch, Nanos.MICRO_NANOS);
        final long gap = upperRangeRules.getDstGapOffset(micros);
        if (gap == 0 && (remainder == 0 || upperRangeRules.getDstGapOffset(micros + 1) == 0)) {
            return 0;
        }
        return gap * Nanos.MICRO_NANOS + remainder;
    }

    @Override
    public long getFirstInstantAtOrAfter(long localEpoch) {
        return localEpoch < UPPER_RANGE ? super.getFirstInstantAtOrAfter(localEpoch) : TimeZoneRules.firstInstantAtOrAfter(this, localEpoch);
    }

    @Override
    public long getLocalOffset(long localEpoch) {
        return localEpoch < UPPER_RANGE ? super.getLocalOffset(localEpoch) : upperRangeRules.getLocalOffset(Math.floorDiv(localEpoch, Nanos.MICRO_NANOS)) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getLocalOffset(long localEpoch, int year) {
        return localEpoch < UPPER_RANGE ? super.getLocalOffset(localEpoch, year) : upperRangeRules.getLocalOffset(Math.floorDiv(localEpoch, Nanos.MICRO_NANOS), year) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getNextDST(long utcEpoch) {
        return utcEpoch < UPPER_RANGE ? super.getNextDST(utcEpoch) : toNanos(upperRangeRules.getNextDST(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS)));
    }

    @Override
    public long getNextDST(long utcEpoch, int year) {
        return utcEpoch < UPPER_RANGE ? super.getNextDST(utcEpoch, year) : toNanos(upperRangeRules.getNextDST(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS), year));
    }

    @Override
    public long getOffset(long utcEpoch) {
        return utcEpoch < UPPER_RANGE ? super.getOffset(utcEpoch) : upperRangeRules.getOffset(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS)) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getOffset(long utcEpoch, int year) {
        return utcEpoch < UPPER_RANGE ? super.getOffset(utcEpoch, year) : upperRangeRules.getOffset(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS), year) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getWallClockHighWater(long utcEpoch) {
        return utcEpoch < UPPER_RANGE ? super.getWallClockHighWater(utcEpoch) : TimeZoneRules.wallClockHighWater(this, utcEpoch);
    }

    private static long toNanos(long micros) {
        return micros > Long.MAX_VALUE / Nanos.MICRO_NANOS ? Long.MAX_VALUE : micros * Nanos.MICRO_NANOS;
    }

    @Override
    protected long addDays(long epoch, int days) {
        return Nanos.addDays(epoch, days);
    }

    @Override
    protected int getDaysPerMonth(int month, boolean leapYear) {
        return CommonUtils.getDaysPerMonth(month, leapYear);
    }

    @Override
    protected int getYear(long epoch) {
        return Nanos.getYear(epoch);
    }

    @Override
    protected boolean isLeapYear(int year) {
        return Nanos.isLeapYear(year);
    }

    @Override
    protected long nextOrSameDayOfWeek(long epoch, int dow) {
        return Nanos.nextOrSameDayOfWeek(epoch, dow);
    }

    @Override
    protected long previousOrSameDayOfWeek(long epoch, int dow) {
        return Nanos.previousOrSameDayOfWeek(epoch, dow);
    }

    @Override
    protected long toEpoch(int year, boolean leapYear, int month, int day, int hour, int min) {
        return Nanos.toNanos(year, leapYear, month, day, hour, min);
    }
}
