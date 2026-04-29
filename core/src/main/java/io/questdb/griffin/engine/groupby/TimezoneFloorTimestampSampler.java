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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * Wraps a calendar-based {@link TimestampSampler} (day, week, month, year) so its
 * bucket grid follows local-time rules of a given {@link TimeZoneRules}. Each
 * UTC timestamp is converted to local time via {@link CommonUtils#getFloorUtcTzOffset},
 * the wrapped sampler advances or floors in local space, and the result is
 * converted back via {@link CommonUtils#offsetFlooredUtcResult}. Because both
 * helpers are the ones {@code timestamp_floor_utc} uses, the sampler's grid
 * matches the GROUP BY bucket keys produced by the inner aggregation, so the
 * fast-path fill cursor's grid-drift guard never trips on DST transitions
 * (Europe/Berlin "1d" spans 23 h on spring-forward and 25 h on fall-back).
 * <p>
 * The wrapped sampler is invoked exclusively in local time; this class never
 * mutates its state directly. Sub-day strides do not need this wrapping
 * because {@link CommonUtils#getFloorUtcTzOffset} returns the standard
 * (non-DST) offset for them and the standing fast-path FROM/TO wrap already
 * pre-shifts the anchor via {@code to_utc(FROM, tz)}, leaving bucket widths
 * uniform in UTC.
 */
public class TimezoneFloorTimestampSampler implements TimestampSampler {
    private final TimestampSampler localSampler;
    private final TimeZoneRules tzRules;
    private final char unit;

    public TimezoneFloorTimestampSampler(TimestampSampler localSampler, TimeZoneRules tzRules, char unit) {
        this.localSampler = localSampler;
        this.tzRules = tzRules;
        this.unit = unit;
    }

    @Override
    public long getApproxBucketSize() {
        return localSampler.getApproxBucketSize();
    }

    @Override
    public long getBucketSize() {
        return localSampler.getBucketSize();
    }

    @Override
    public int getTimestampType() {
        return localSampler.getTimestampType();
    }

    @Override
    public long nextTimestamp(long utcTimestamp) {
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, utcTimestamp, unit);
        final long localNext = localSampler.nextTimestamp(utcTimestamp + tzOff);
        return CommonUtils.offsetFlooredUtcResult(localNext, tzOff, 0, tzRules, unit);
    }

    @Override
    public long nextTimestamp(long utcTimestamp, long numSteps) {
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, utcTimestamp, unit);
        final long localNext = localSampler.nextTimestamp(utcTimestamp + tzOff, numSteps);
        return CommonUtils.offsetFlooredUtcResult(localNext, tzOff, 0, tzRules, unit);
    }

    @Override
    public long previousTimestamp(long utcTimestamp) {
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, utcTimestamp, unit);
        final long localPrev = localSampler.previousTimestamp(utcTimestamp + tzOff);
        return CommonUtils.offsetFlooredUtcResult(localPrev, tzOff, 0, tzRules, unit);
    }

    @Override
    public long round(long utcTimestamp) {
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, utcTimestamp, unit);
        final long localFloored = localSampler.round(utcTimestamp + tzOff);
        return CommonUtils.offsetFlooredUtcResult(localFloored, tzOff, 0, tzRules, unit);
    }

    @Override
    public void setOffset(long offset) {
        // The fast path forces calendarOffset = 0 for day-or-larger + TZ + FILL
        // (see SqlOptimiser.rewriteSampleBy fillOffset propagation gate); this
        // branch nevertheless forwards the value so a future relaxation of that
        // gate can pass an in-local-time offset straight through.
        localSampler.setOffset(offset);
    }

    @Override
    public void setStart(long utcTimestamp) {
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, utcTimestamp, unit);
        localSampler.setStart(utcTimestamp + tzOff);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("TzAware(");
        localSampler.toSink(sink);
        sink.putAscii(')');
    }
}
