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

package io.questdb.log;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRules;

public final class LogLevel {
    public static int ADVISORY = 16;
    public static String ADVISORY_HEADER = " A ";
    public static int CRITICAL = 8;
    public static String CRITICAL_HEADER = " C ";
    public static int DEBUG = 1;
    public static String DEBUG_HEADER = " D ";
    public static int ERROR = 4;
    public static String ERROR_HEADER = " E ";
    public static int INFO = 2;
    public static int ALL = DEBUG | INFO | ERROR | CRITICAL | ADVISORY;
    public static String INFO_HEADER = " I ";
    public static int MAX = Numbers.msb(LogLevel.ADVISORY) + 1;
    public static int MASK = ~(-1 << (MAX));
    public static DateFormat TIMESTAMP_FORMAT;
    public static String TIMESTAMP_TIMEZONE;
    public static DateLocale TIMESTAMP_TIMEZONE_LOCALE;
    public static TimeZoneRules TIMESTAMP_TIMEZONE_RULES;

    private LogLevel() {
    }

    public static void init(CairoConfiguration config) {
        if (config.getLogLevelVerbose()) {
            ADVISORY_HEADER = " ADVISORY ";
            CRITICAL_HEADER = " CRITICAL ";
            DEBUG_HEADER = " DEBUG ";
            ERROR_HEADER = " ERROR ";
            INFO_HEADER = " INFO ";
        }

        if (config.getLogTimestampTimezone() != null) {
            TIMESTAMP_TIMEZONE = config.getLogTimestampTimezone();
            TIMESTAMP_TIMEZONE_RULES = config.getLogTimestampTimezoneRules();
            TIMESTAMP_TIMEZONE_LOCALE = config.getLogTimestampTimezoneLocale();
            TIMESTAMP_FORMAT = config.getLogTimestampFormat();
        }
    }
}
