/*+******************************************************************************
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.TimestampDriver;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.TimeZoneRules;


/**
 * Floors timestamps with timezone-aware alignment but returns UTC results.
 * <p>
 * For each input UTC timestamp:
 * 1. Convert to local time using the timezone offset at that UTC moment
 * 2. Floor to the stride boundary in local time
 * 3. Convert back to UTC using the SAME offset (from step 1)
 * <p>
 * This produces unambiguous UTC bucket keys even across DST transitions,
 * unlike {@link TimestampFloorFromOffsetFunctionFactory} which returns local time
 * and loses information during DST fall-back (clock-back) transitions.
 * <p>
 * When no timezone is specified, this function behaves identically to
 * {@link TimestampFloorFromOffsetFunctionFactory}.
 */
public class TimestampFloorFromOffsetUtcFunctionFactory extends AbstractTimestampFloorFromOffsetFunctionFactory {
    public static final String NAME = "timestamp_floor_utc";

    public static long value(TimestampDriver.TimestampFloorWithOffsetMethod floor, long timestamp, int stride, long effectiveOffset, long tzOffset) {
        return floor.floor(timestamp + tzOffset, stride, effectiveOffset) - tzOffset;
    }

    public static long value(TimestampDriver.TimestampFloorWithOffsetMethod floor, long timestamp, int stride, long effectiveOffset, TimeZoneRules tzRules, char unit) {
        // Use the shared conversion strategy from CommonUtils so that
        // the mat view refresh iterator produces matching bucket boundaries.
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit);
        final long localTimestamp = timestamp + tzOff;
        final long result = floor.floor(localTimestamp, stride, effectiveOffset);
        return CommonUtils.offsetFlooredUtcResult(result, tzOff, 0, tzRules, unit);
    }

    @Override
    public String getSignature() {
        // s = stride string constant (e.g. '1h', '15m')
        // N = input timestamp to floor (variable)
        // n = epoch reference timestamp constant (origin for stride alignment)
        // S = offset string (e.g. '+02:00')
        // S = timezone string (e.g. 'Europe/Berlin')
        return NAME + "(sNnSS)";
    }

    @Override
    String getName() {
        return NAME;
    }

    @Override
    boolean isReturnUtc() {
        return true;
    }
}
