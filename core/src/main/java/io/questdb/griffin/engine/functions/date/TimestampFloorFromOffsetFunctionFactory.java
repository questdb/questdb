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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.TimestampDriver;
import io.questdb.std.datetime.TimeZoneRules;


/**
 * Floors timestamps with modulo relative to a timestamp from 1970-01-01, as
 * well as an offset from the epoch start.
 * <p>
 * Fused variant of timestamp_floor() and to_timezone() functions meant
 * to be used in SAMPLE BY to parallel GROUP BY SQL rewrite.
 * <p>
 * When timezone is specified, the returned timestamps are in local time.
 */
public class TimestampFloorFromOffsetFunctionFactory extends AbstractTimestampFloorFromOffsetFunctionFactory {
    private static final String NAME = TimestampFloorFunctionFactory.NAME;

    public static long value(TimestampDriver.TimestampFloorWithOffsetMethod floor, long timestamp, int stride, long effectiveOffset, long tzOffset) {
        return floor.floor(timestamp + tzOffset, stride, effectiveOffset);
    }

    public static long value(TimestampDriver.TimestampFloorWithOffsetMethod floor, long timestamp, int stride, long effectiveOffset, TimeZoneRules tzRules) {
        final long tzOff = tzRules.getOffset(timestamp);
        final long localTimestamp = timestamp + tzOff;
        long result = floor.floor(localTimestamp, stride, effectiveOffset);
        // Move the timestamp to the bucket if it belongs to a DST gap, i.e. non-existing
        // time interval that occur due to a forward clock shift.
        // This is required to avoid duplicate timestamps returned by SAMPLE BY + DST time zone + offset
        // queries that get rewritten to a parallel GROUP BY.
        long gapDuration = tzRules.getDstGapOffset(result);
        if (gapDuration != 0) {
            // The floored local time landed in a DST gap (spring-forward). Back up by the gap
            // duration to reach a real local time, then re-floor to find the correct bucket.
            result = floor.floor(result - gapDuration, stride, effectiveOffset);
        }
        return result;
    }

    @Override
    public String getSignature() {
        return NAME + "(sNnSS)";
    }

    @Override
    String getName() {
        return NAME;
    }

    @Override
    boolean isReturnUtc() {
        return false;
    }
}
