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

package io.questdb.std.datetime.millitime;

import io.questdb.std.datetime.AbstractTimeZoneRules;

import java.time.zone.ZoneRules;

public class TimeZoneRulesMillis extends AbstractTimeZoneRules {
    public TimeZoneRulesMillis(ZoneRules rules) {
        super(rules, Dates.SECOND_MILLIS);
    }

    @Override
    public long getNextDST(long utcEpoch) {
        return 0;
    }

    @Override
    protected long addDays(long epoch, int days) {
        return Dates.addDays(epoch, days);
    }

    @Override
    protected int getDaysPerMonth(int month, boolean leapYear) {
        return Dates.getDaysPerMonth(month, leapYear);
    }

    @Override
    protected int getYear(long epoch) {
        return Dates.getYear(epoch);
    }

    @Override
    protected boolean isLeapYear(int year) {
        return Dates.isLeapYear(year);
    }

    @Override
    protected long nextOrSameDayOfWeek(long epoch, int dow) {
        return Dates.nextOrSameDayOfWeek(epoch, dow);
    }

    @Override
    protected long previousOrSameDayOfWeek(long epoch, int dow) {
        return Dates.previousOrSameDayOfWeek(epoch, dow);
    }

    @Override
    protected long toEpoch(int year, boolean leapYear, int month, int day, int hour, int min) {
        return Dates.toMillis(year, leapYear, month, day, hour, min);
    }
}
