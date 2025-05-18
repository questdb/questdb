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

public class CommonFormatUtils {
    public static final int DAY_HOURS = 24;
    public static final String DAY_PATTERN = "yyyy-MM-dd";
    public static final DateLocale EN_LOCALE = DateLocaleFactory.INSTANCE.getLocale("en");
    public static final int HOUR_24 = 2;
    public static final int HOUR_AM = 0;
    public static final int HOUR_MINUTES = 60;
    public static final String HOUR_PATTERN = "yyyy-MM-ddTHH";
    public static final int HOUR_PM = 1;
    public static final long MINUTE_SECONDS = 60;
    public static final String MONTH_PATTERN = "yyyy-MM";
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    public static final String WEEK_PATTERN = "YYYY-Www";
    public static final String YEAR_PATTERN = "yyyy";
    public static final String GREEDY_MILLIS1_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.Sz";
    public static final String GREEDY_MILLIS2_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSz";
    public static final String PG_TIMESTAMP_MILLI_TIME_Z_PATTERN = "y-MM-dd HH:mm:ss.SSSz";
    public static final String SEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ssz";
    public static final String USEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUz";
    public static final String NSEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUNNNz";
    public static final int YEAR_MONTHS = 12;
}
