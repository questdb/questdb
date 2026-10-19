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

import io.questdb.std.str.Sinkable;

public interface TimestampSampler extends Sinkable {

    long getApproxBucketSize();

    default long getBucketSize() {
        throw new UnsupportedOperationException();
    }

    int getTimestampType();

    long nextTimestamp(long timestamp, long numSteps);

    default long nextTimestamp(long timestamp) {
        return nextTimestamp(timestamp, 1);
    }

    /**
     * Converts a value from the sampler's grid space (where
     * {@link #setLocalAnchor(long)} accepts inputs) to UTC space (where
     * {@link #round(long)} returns outputs). Non-wrapping samplers operate
     * entirely in the same space, so the default returns the input unchanged.
     * {@link TimezoneFloorTimestampSampler} overrides to apply the
     * local-to-UTC conversion that mirrors {@code timestamp_floor_utc}'s
     * back-conversion of a flooredLocal value.
     */
    default long localAnchorAsUtc(long localAnchor) {
        return localAnchor;
    }

    long previousTimestamp(long timestamp);

    long round(long timestamp);

    /**
     * Anchors the bucket grid at a value that is already in the sampler's
     * grid space. Non-wrapping samplers operate in the same space their
     * inputs come from, so the default delegates to {@link #setStart(long)}.
     * {@link TimezoneFloorTimestampSampler} overrides to bypass the UTC->local
     * conversion {@link #setStart(long)} performs, since callers using this
     * method already hold a local-grid anchor (e.g. {@code from + offset}
     * computed by {@code timestamp_floor_utc}).
     */
    default void setLocalAnchor(long timestamp) {
        setStart(timestamp);
    }

    void setOffset(long timestamp);

    void setStart(long timestamp);
}
