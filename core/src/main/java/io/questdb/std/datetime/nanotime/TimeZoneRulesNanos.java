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

package io.questdb.std.datetime.nanotime;

import io.questdb.std.datetime.AbstractTimeZoneRules;
import io.questdb.std.datetime.CommonUtils;

import java.time.zone.ZoneRules;

public class TimeZoneRulesNanos extends AbstractTimeZoneRules {

    public TimeZoneRulesNanos(ZoneRules rules) {
        super(rules, Nanos.SECOND_NANOS);
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
