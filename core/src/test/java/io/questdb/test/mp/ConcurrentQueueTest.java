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

package io.questdb.test.mp;

import io.questdb.mp.ConcurrentQueue;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentQueueTest {

    @Test
    public void testAvailabilityAcrossSequenceWrap() {
        final ConcurrentQueue<Object> queue = ConcurrentQueue.createConcurrentObjectQueue(32);
        final long freezeOffset = queue.capacity() * 2L;

        queue.setCurrentSegmentSequenceForTesting(Long.MAX_VALUE, Long.MAX_VALUE, false);
        Assert.assertEquals(0, queue.getApproximateCount());
        Assert.assertFalse(queue.hasAvailable());

        queue.setCurrentSegmentSequenceForTesting(Long.MAX_VALUE, Long.MIN_VALUE, false);
        Assert.assertEquals(1, queue.getApproximateCount());
        Assert.assertTrue(queue.hasAvailable());

        final long fullHead = Long.MAX_VALUE - queue.capacity() + 1L;
        queue.setCurrentSegmentSequenceForTesting(fullHead, Long.MIN_VALUE, true);
        Assert.assertEquals(queue.capacity(), queue.getApproximateCount());
        Assert.assertTrue(queue.hasAvailable());

        queue.setCurrentSegmentSequenceForTesting(Long.MAX_VALUE, Long.MIN_VALUE + freezeOffset, true);
        Assert.assertEquals(1, queue.getApproximateCount());
        Assert.assertTrue(queue.hasAvailable());

        queue.setCurrentSegmentSequenceForTesting(Long.MIN_VALUE, Long.MIN_VALUE + freezeOffset, true);
        Assert.assertEquals(0, queue.getApproximateCount());
        Assert.assertFalse(queue.hasAvailable());
    }
}
