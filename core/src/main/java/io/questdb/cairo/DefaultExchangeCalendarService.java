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

package io.questdb.cairo;

import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.datetime.microtime.Micros;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation with mock schedule for XNYS (NYSE) for January 2026.
 * Custom deployments can provide their own implementation with actual calendar data.
 */
public class DefaultExchangeCalendarService implements ExchangeCalendarService {
    public static final ExchangeCalendarService INSTANCE = new DefaultExchangeCalendarService();

    // NYSE trading hours: 9:30 AM - 4:00 PM Eastern Time
    // In January, ET = EST = UTC-5
    // 9:30 AM EST = 14:30 UTC, 4:00 PM EST = 21:00 UTC
    private static final int OPEN_HOUR_UTC = 14;
    private static final int OPEN_MINUTE_UTC = 30;
    private static final int CLOSE_HOUR_UTC = 21;
    private static final int CLOSE_MINUTE_UTC = 0;

    // Trading days in January 2026 (excludes weekends and holidays: Jan 1 New Year's, Jan 19 MLK Day)
    private static final int[] JANUARY_2026_TRADING_DAYS = {
            2, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 20, 21, 22, 23, 26, 27, 28, 29, 30
    };

    private static final LongList XNYS_SCHEDULE = buildXnysSchedule();

    private DefaultExchangeCalendarService() {
    }

    private static LongList buildXnysSchedule() {
        LongList schedule = new LongList();
        for (int day : JANUARY_2026_TRADING_DAYS) {
            long openMicros = Micros.toMicros(2026, 1, day, OPEN_HOUR_UTC, OPEN_MINUTE_UTC);
            long closeMicros = Micros.toMicros(2026, 1, day, CLOSE_HOUR_UTC, CLOSE_MINUTE_UTC);
            schedule.add(openMicros);
            schedule.add(closeMicros);
        }
        return schedule;
    }

    @Override
    @Nullable
    public LongList getSchedule(CharSequence exchange) {
        if (Chars.equalsIgnoreCase(exchange, "XNYS")) {
            return XNYS_SCHEDULE;
        }
        return null;
    }
}
