/*******************************************************************************
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

import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class SOLatchTest {

    @Test
    public void testUnparkLatch() throws BrokenBarrierException, InterruptedException {
        int threads = TestUtils.generateRandom(null).nextInt(30) + 1;
        SOCountDownLatch latch = new SOCountDownLatch(threads);
        triggerLatchAsync(threads, latch);
        latch.await();
    }

    @Test
    public void testUnparkUnboundedLatch() throws BrokenBarrierException, InterruptedException {
        int threads = TestUtils.generateRandom(null).nextInt(30) + 1;
        final SOUnboundedCountDownLatch latch = new SOUnboundedCountDownLatch();
        triggerLatchAsync(threads, latch);
        latch.await(threads);
    }

    private static void triggerLatchAsync(int threads, CountDownLatchSPI latch) throws InterruptedException, BrokenBarrierException {
        CyclicBarrier startLatch = new CyclicBarrier(threads + 1);

        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                try {
                    startLatch.await();
                    latch.countDown();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            });
            th.start();
        }
        startLatch.await();
    }
}
