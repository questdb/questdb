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
 * <p>
 * Anchor methods follow three input conventions:
 * <ul>
 *     <li>{@link #setStart(long)} takes a UTC instant; the wrap converts to
 *     local before forwarding. Used when the caller holds a GROUP BY bucket
 *     label (already on the local grid in UTC space).</li>
 *     <li>{@link #setOffset(long)} takes a local-time offset; forwarded as-is
 *     to the wrapped sampler.</li>
 *     <li>{@link #setLocalAnchor(long)} takes a value already in local-grid
 *     space (e.g. {@code from + offset} as treated by
 *     {@code timestamp_floor_utc}); forwarded directly without conversion.</li>
 * </ul>
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
    public long localAnchorAsUtc(long localAnchor) {
        // Convert a local-grid value to its UTC representation, mirroring
        // CommonUtils.offsetFlooredUtcResult's super-day branch:
        // resultTzOff = tzRules.getOffset(local - tzOff(local-as-utc)). The
        // first lookup approximates the UTC instant of the anchor; the second
        // resolves the actual offset at that UTC. For super-day units this
        // matches what timestamp_floor_utc emits when the floor lands at
        // localAnchor.
        final long approxTzOff = CommonUtils.getFloorUtcTzOffset(tzRules, localAnchor, unit);
        final long resultTzOff = tzRules.getOffset(localAnchor - approxTzOff);
        return localAnchor - resultTzOff;
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
    public void setLocalAnchor(long localTimestamp) {
        // Treat the input as already lying on the local-time grid (no
        // UTC->local conversion). Used by callers that hold a value computed
        // in the same space {@code timestamp_floor_utc} treats {@code from +
        // offset}: a raw modulus seed for the local bucket grid, not a UTC
        // instant.
        localSampler.setStart(localTimestamp);
    }

    @Override
    public void setOffset(long offset) {
        // Forwarded as-is: the wrapped sampler operates entirely in local-time
        // space (round/nextTimestamp/setStart all hand it values that have
        // already had tzOff applied), so the offset reaches the local grid
        // without further translation.
        localSampler.setOffset(offset);
    }

    @Override
    public void setStart(long utcTimestamp) {
        // Input is a UTC instant. Convert to local before forwarding so the
        // wrapped sampler's grid anchor sits at the local position of that
        // UTC instant. Used by the no-offset fast path where the cursor hands
        // in firstTs (a GROUP BY bucket label whose local conversion lands
        // exactly on a bucket boundary). Callers that already hold a
        // local-grid value should use {@link #setLocalAnchor(long)}.
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
