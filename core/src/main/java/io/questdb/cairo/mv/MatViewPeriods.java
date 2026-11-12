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

package io.questdb.cairo.mv;

import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.std.datetime.TimeZoneRules;
import org.jetbrains.annotations.Nullable;

/**
 * Finds the UTC times at which the periods of a period materialized view start. The periods follow a grid of local
 * times in the time zone of the view, the sampler's grid, so a daylight saving time change shifts them:
 * <ul>
 * <li>where the clock moves forward, it skips the grid points in between, and the periods that would start at them
 * start at the change;</li>
 * <li>where the clock moves back, it passes the grid points in between twice, and a period starts at each pass.</li>
 * </ul>
 * The sampler and all times are in microseconds, and the sampler's start is a grid point in local time. Without time
 * zone rules, local time is UTC.
 */
public final class MatViewPeriods {

    private MatViewPeriods() {
    }

    /**
     * Returns the earliest UTC time after the given one at which a period starts.
     */
    public static long nextPeriodStart(TimestampSampler sampler, @Nullable TimeZoneRules rules, long utc) {
        if (rules == null) {
            return sampler.nextTimestamp(sampler.round(utc));
        }
        final long offset = rules.getOffset(utc);
        final long localStart = sampler.nextTimestamp(sampler.round(utc + offset));
        final long start = localStart - offset;
        if (rules.getOffset(start) == offset) {
            return start;
        }
        // The clock changes after utc, and before it reaches localStart.
        final long change = rules.getNextDST(utc);
        final long changeOffset = rules.getOffset(change);
        if (changeOffset > offset) {
            // The clock moves forward, from change + offset to change + changeOffset.
            return localStart < change + changeOffset ? change : localStart - changeOffset;
        }
        // The clock moves back, from change + offset to change + changeOffset, and passes the grid points from there
        // once more.
        final long localRestart = change + changeOffset;
        final long gridPoint = sampler.round(localRestart);
        return (gridPoint < localRestart ? sampler.nextTimestamp(gridPoint) : gridPoint) - changeOffset;
    }

    /**
     * Returns the latest UTC time at or before the given one at which a period starts. The last period that is
     * complete at the given time ends there. Before the sampler's start, the sampler rounds the time up, and this
     * method returns a later time.
     */
    public static long periodStart(TimestampSampler sampler, @Nullable TimeZoneRules rules, long utc) {
        if (rules == null) {
            return sampler.round(utc);
        }
        final long offset = rules.getOffset(utc);
        final long localStart = sampler.round(utc + offset);
        final long start = localStart - offset;
        final long startOffset = rules.getOffset(start);
        if (startOffset == offset || start > utc) {
            return start;
        }
        // The clock changed after the period start, and at or before utc.
        final long change = rules.getNextDST(start);
        if (offset > startOffset) {
            // The clock moved forward, from change + startOffset to change + offset.
            return localStart < change + startOffset ? localStart - startOffset : change;
        }
        // The clock moved back, from change + startOffset to change + offset, and has not reached a grid point since.
        // Before the change, it last reached the last grid point before change + startOffset.
        return sampler.round(change + startOffset - 1) - startOffset;
    }
}
