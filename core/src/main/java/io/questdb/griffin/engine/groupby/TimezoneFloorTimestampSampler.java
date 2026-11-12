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
 * Wraps a calendar-based {@link TimestampSampler} (day, week, month, year) so that it steps over
 * the bucket grid {@code timestamp_floor_utc} floors to under a time zone: the wrapped sampler
 * works on local grid lines, and {@link CommonUtils#floorUtcToLocal} and
 * {@link CommonUtils#floorLocalToUtc} convert between UTC and that grid.
 * <p>
 * Anchor methods follow three input conventions:
 * <ul>
 *     <li>{@link #setStart(long)} takes a UTC instant and anchors at its local reading.</li>
 *     <li>{@link #setOffset(long)} takes a local-time offset, forwarded as-is.</li>
 *     <li>{@link #setLocalAnchor(long)} takes a local grid line, e.g. {@code from + offset} as
 *     {@code timestamp_floor_utc} treats it, forwarded as-is.</li>
 * </ul>
 */
public class TimezoneFloorTimestampSampler implements TimestampSampler {
    private final TimestampSampler localSampler;
    private final char unit;
    private TimeZoneRules tzRules;

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
        return CommonUtils.floorLocalToUtc(tzRules, localAnchor, unit);
    }

    @Override
    public long nextTimestamp(long utcTimestamp) {
        // Lines in one spring-forward gap share their start, so step over the empty buckets.
        long line = localSampler.round(CommonUtils.floorUtcToLocal(tzRules, utcTimestamp, unit));
        while (true) {
            final long nextLine = localSampler.nextTimestamp(line);
            if (nextLine == Long.MAX_VALUE || nextLine <= line) {
                return Long.MAX_VALUE;
            }
            final long next = CommonUtils.floorLocalToUtc(tzRules, nextLine, unit);
            if (next > utcTimestamp) {
                return next;
            }
            line = nextLine;
        }
    }

    @Override
    public long nextTimestamp(long utcTimestamp, long numSteps) {
        long timestamp = utcTimestamp;
        for (long i = 0; i < numSteps; i++) {
            timestamp = nextTimestamp(timestamp);
        }
        return timestamp;
    }

    @Override
    public long previousTimestamp(long utcTimestamp) {
        long line = localSampler.round(CommonUtils.floorUtcToLocal(tzRules, utcTimestamp, unit));
        while (true) {
            final long previousLine = localSampler.previousTimestamp(line);
            if (previousLine >= line) {
                return Long.MIN_VALUE;
            }
            final long previous = CommonUtils.floorLocalToUtc(tzRules, previousLine, unit);
            if (previous < utcTimestamp) {
                return previous;
            }
            line = previousLine;
        }
    }

    @Override
    public long round(long utcTimestamp) {
        return CommonUtils.floorLocalToUtc(tzRules, localSampler.round(CommonUtils.floorUtcToLocal(tzRules, utcTimestamp, unit)), unit);
    }

    @Override
    public void setLocalAnchor(long localTimestamp) {
        localSampler.setStart(localTimestamp);
    }

    @Override
    public void setOffset(long offset) {
        localSampler.setOffset(offset);
    }

    @Override
    public void setStart(long utcTimestamp) {
        localSampler.setStart(CommonUtils.floorUtcToLocal(tzRules, utcTimestamp, unit));
    }

    /**
     * Rebinds the time zone; a bind-variable TIME ZONE can change between executions of a cached factory.
     */
    public void setTzRules(@NotNull TimeZoneRules tzRules) {
        this.tzRules = tzRules;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("TzAware(");
        localSampler.toSink(sink);
        sink.putAscii(')');
    }
}
