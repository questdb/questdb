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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * Fixed-size bucket timestamp sampler.
 */
public class SimpleTimestampSampler implements TimestampSampler {
    private final long bucket;
    private final int timestampType;
    private long start;

    public SimpleTimestampSampler(long bucket, int timestampType) {
        this.bucket = bucket;
        this.timestampType = timestampType;
    }

    @Override
    public long getApproxBucketSize() {
        return bucket;
    }

    @Override
    public long getBucketSize() {
        return bucket;
    }

    @Override
    public int getTimestampType() {
        return timestampType;
    }

    @Override
    public long nextTimestamp(long timestamp) {
        try {
            return Math.addExact(timestamp, bucket);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long nextTimestamp(long timestamp, int numSteps) {
        try {
            return Math.addExact(timestamp, Math.multiplyExact(numSteps, bucket));
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long previousTimestamp(long timestamp) {
        return timestamp - bucket;
    }

    @Override
    public long round(long value) {
        long q = (value - start) / bucket;
        if (value < 0 && q * bucket != value) {
            q = q - 1;
        }
        return start + q * bucket;
    }

    @Override
    public void setStart(long timestamp) {
        this.start = timestamp;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("BaseTsSampler");
    }
}
