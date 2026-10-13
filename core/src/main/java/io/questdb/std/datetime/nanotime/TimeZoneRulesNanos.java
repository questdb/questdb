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

import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimeZoneRulesMicros;

import java.time.zone.ZoneRules;

public class TimeZoneRulesNanos implements TimeZoneRules {
    private final TimeZoneRulesMicros rules;

    public TimeZoneRulesNanos(ZoneRules rules) {
        this.rules = new TimeZoneRulesMicros(rules);
    }

    @Override
    public long getDstGapOffset(long localEpoch) {
        final long micros = Math.floorDiv(localEpoch, Nanos.MICRO_NANOS);
        final long remainder = Math.floorMod(localEpoch, Nanos.MICRO_NANOS);
        final long gap = rules.getDstGapOffset(micros);
        if (gap == 0 && (remainder == 0 || rules.getDstGapOffset(micros + 1) == 0)) {
            return 0;
        }
        return gap * Nanos.MICRO_NANOS + remainder;
    }

    @Override
    public long getLocalOffset(long localEpoch) {
        return rules.getLocalOffset(Math.floorDiv(localEpoch, Nanos.MICRO_NANOS)) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getLocalOffset(long localEpoch, int year) {
        return rules.getLocalOffset(Math.floorDiv(localEpoch, Nanos.MICRO_NANOS), year) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getNextDST(long utcEpoch) {
        return toNanos(rules.getNextDST(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS)));
    }

    @Override
    public long getNextDST(long utcEpoch, int year) {
        return toNanos(rules.getNextDST(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS), year));
    }

    @Override
    public long getOffset(long utcEpoch) {
        return rules.getOffset(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS)) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getOffset(long utcEpoch, int year) {
        return rules.getOffset(Math.floorDiv(utcEpoch, Nanos.MICRO_NANOS), year) * Nanos.MICRO_NANOS;
    }

    @Override
    public long getStandardOffset() {
        return rules.getStandardOffset() * Nanos.MICRO_NANOS;
    }

    @Override
    public boolean hasFixedOffset() {
        return rules.hasFixedOffset();
    }

    private static long toNanos(long micros) {
        return micros > Long.MAX_VALUE / Nanos.MICRO_NANOS ? Long.MAX_VALUE : micros * Nanos.MICRO_NANOS;
    }
}
