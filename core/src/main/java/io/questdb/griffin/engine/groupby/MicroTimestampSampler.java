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

public class MicroTimestampSampler implements TimestampSampler {
    private final long bucketSize;
    private long start;

    public MicroTimestampSampler(long bucketSize) {
        this.bucketSize = bucketSize;
    }

    @Override
    public long getApproxBucketSize() {
        return bucketSize;
    }

    @Override
    public long getBucketSize() {
        return bucketSize;
    }

    @Override
    public long nextTimestamp(long timestamp) {
        return timestamp + bucketSize;
    }

    @Override
    public long nextTimestamp(long timestamp, int numSteps, long maxTimestamp) {
        return Math.min(timestamp + numSteps * bucketSize, maxTimestamp);
    }

    @Override
    public long previousTimestamp(long timestamp) {
        return timestamp - bucketSize;
    }

    @Override
    public long round(long value) {
        long q = (value - start) / bucketSize;
        if (value < 0 && q * bucketSize != value) {
            q = q - 1;
        }
        return start + q * bucketSize;
    }

    @Override
    public void setStart(long timestamp) {
        this.start = timestamp;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("MicroTsSampler");
    }
}
