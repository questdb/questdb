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

import io.questdb.cairo.PartitionBy;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.microtime.TimestampFormatUtils.DAY_FORMAT;

public class IsoDatePartitionFormat implements DateFormat {
    private final DateFormat baseFormat;
    private final PartitionBy.PartitionFloorMethod floorMethod;

    public IsoDatePartitionFormat(PartitionBy.PartitionFloorMethod floorMethod, DateFormat baseFormat) {
        this.floorMethod = floorMethod;
        this.baseFormat = baseFormat;
    }

    @Override
    public void format(long timestamp, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
        long overspill = timestamp - floorMethod.floor(timestamp);

        if (overspill > 0) {
            DAY_FORMAT.format(timestamp, locale, timeZoneName, sink);
            long time = timestamp - (timestamp / Timestamps.DAY_MICROS) * Timestamps.DAY_MICROS;

            if (time > 0) {
                int hour = (int) (time / Timestamps.HOUR_MICROS);
                int minute = (int) ((time % Timestamps.HOUR_MICROS) / Timestamps.MINUTE_MICROS);
                int second = (int) ((time % Timestamps.MINUTE_MICROS) / Timestamps.SECOND_MICROS);
                int milliMicros = (int) (time % Timestamps.SECOND_MICROS);

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
        } else {
            baseFormat.format(timestamp, locale, timeZoneName, sink);
        }
    }

    @Override
    public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) throws NumericException {
        return PartitionDateParseUtil.parseFloorPartialTimestamp(in, 0, in.length());
    }

    @Override
    public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException {
        return PartitionDateParseUtil.parseFloorPartialTimestamp(in, lo, hi);
    }
}