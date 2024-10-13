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

package io.questdb.cairo.ptt;

import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.microtime.TimestampFormatUtils.WEEK_FORMAT;

public class IsoWeekPartitionFormat implements DateFormat {

    @Override
    public void format(long timestamp, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
        long weekTime = timestamp - Timestamps.floorWW(timestamp);
        WEEK_FORMAT.format(timestamp, locale, timeZoneName, sink);

        if (weekTime > 0) {
            int dayOfWeek = (int) (weekTime / Timestamps.DAY_MICROS) + 1;
            int hour = (int) ((weekTime % Timestamps.DAY_MICROS) / Timestamps.HOUR_MICROS);
            int minute = (int) ((weekTime % Timestamps.HOUR_MICROS) / Timestamps.MINUTE_MICROS);
            int second = (int) ((weekTime % Timestamps.MINUTE_MICROS) / Timestamps.SECOND_MICROS);
            int milliMicros = (int) (weekTime % Timestamps.SECOND_MICROS);

            sink.putAscii('-');
            sink.put(dayOfWeek);

            if (hour > 0 || minute > 0 || second > 0 || milliMicros > 0) {
                sink.putAscii('T');
                TimestampFormatUtils.append0(sink, hour);

                if (minute > 0 || second > 0 || milliMicros > 0) {
                    TimestampFormatUtils.append0(sink, minute);
                    TimestampFormatUtils.append0(sink, second);

                    if (milliMicros > 0) {
                        sink.putAscii('-');
                        TimestampFormatUtils.append00000(sink, milliMicros);
                    }
                }
            }
        }
    }

    @Override
    public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) throws NumericException {
        return parse(in, 0, in.length(), locale);
    }

    @Override
    public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException {
        long baseTs = WEEK_FORMAT.parse(in, lo, lo + 8, locale);
        lo += 8;
        if (lo < hi) {
            return PartitionDateParseUtil.parseDayTime(in, hi, lo, baseTs, 7, 1);
        }
        return baseTs;
    }
}