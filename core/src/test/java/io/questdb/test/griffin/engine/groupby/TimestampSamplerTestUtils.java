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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.TimestampSampler;
import org.junit.Assert;

final class TimestampSamplerTestUtils {

    private TimestampSamplerTestUtils() {
    }

    // Walks the grid from gridPoint, a bucket start, back to lo and then forward to hi, and checks that round()
    // returns the bucket start for values every step apart and for the instants around each bucket start.
    static void assertRoundMatchesGridWalk(TimestampSampler sampler, long gridPoint, long lo, long hi, long step) {
        long bucketStart = gridPoint;
        while (bucketStart > lo) {
            final long previousBucketStart = sampler.previousTimestamp(bucketStart);
            // the walk fails instead of looping forever when previousTimestamp() doesn't move back
            Assert.assertTrue(previousBucketStart < bucketStart);
            bucketStart = previousBucketStart;
        }
        long value = lo;
        while (bucketStart < hi) {
            final long nextBucketStart = sampler.nextTimestamp(bucketStart);
            // the walk fails instead of looping forever when nextTimestamp() doesn't move forward
            Assert.assertTrue(nextBucketStart > bucketStart);
            for (; value < nextBucketStart; value += step) {
                Assert.assertEquals(bucketStart, sampler.round(value));
            }
            Assert.assertEquals(bucketStart, sampler.round(nextBucketStart - 1));
            Assert.assertEquals(nextBucketStart, sampler.round(nextBucketStart));
            Assert.assertEquals(nextBucketStart, sampler.round(nextBucketStart + 1));
            bucketStart = nextBucketStart;
        }
    }

    static void assertRoundOfLargestTimestamps(TimestampSampler sampler, long expected) {
        Assert.assertEquals(expected, sampler.round(Long.MAX_VALUE));
        Assert.assertEquals(expected, sampler.round(Long.MAX_VALUE - 1));
    }
}
