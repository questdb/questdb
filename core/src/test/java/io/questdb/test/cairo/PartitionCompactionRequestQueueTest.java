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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionCompactionRequestQueue;
import io.questdb.cairo.TableToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PartitionCompactionRequestQueueTest {
    private static final TableToken TOKEN_A = new TableToken("a", "a~1", null, 1, true, false, false);
    private static final TableToken TOKEN_B = new TableToken("b", "b~2", null, 2, true, false, false);

    @Test
    public void testClearDropsEveryRequest() {
        final PartitionCompactionRequestQueue queue = new PartitionCompactionRequestQueue(4);
        Assert.assertTrue(queue.tryRequest(TOKEN_A, 1));
        Assert.assertTrue(queue.tryRequest(TOKEN_B, 2));
        queue.clear();
        Assert.assertFalse(queue.poll(new PartitionCompactionRequestQueue.Request()));
        for (int i = 0; i < 4; i++) {
            Assert.assertTrue("a cleared queue takes its full capacity again", queue.tryRequest(TOKEN_A, i));
        }
    }

    @Test
    public void testConcurrentRequestersLoseNothingTheQueueTook() throws Exception {
        final int requesterCount = 4;
        final int requestsPerRequester = 10_000;
        final PartitionCompactionRequestQueue queue = new PartitionCompactionRequestQueue(64);
        final CyclicBarrier start = new CyclicBarrier(requesterCount + 1);
        final AtomicInteger taken = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] requesters = new Thread[requesterCount];
        for (int t = 0; t < requesterCount; t++) {
            final TableToken token = t % 2 == 0 ? TOKEN_A : TOKEN_B;
            requesters[t] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < requestsPerRequester; i++) {
                        if (queue.tryRequest(token, i)) {
                            taken.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            requesters[t].start();
        }

        final PartitionCompactionRequestQueue.Request request = new PartitionCompactionRequestQueue.Request();
        int polled = 0;
        start.await();
        boolean isRequesterAlive = true;
        while (isRequesterAlive) {
            isRequesterAlive = false;
            for (Thread requester : requesters) {
                isRequesterAlive |= requester.isAlive();
            }
            while (queue.poll(request)) {
                Assert.assertNotNull(request.getTableToken());
                polled++;
            }
        }
        for (Thread requester : requesters) {
            requester.join();
        }
        while (queue.poll(request)) {
            polled++;
        }
        Assert.assertNull(failure.get());
        Assert.assertEquals("every request the queue took must come out exactly once", taken.get(), polled);
    }

    @Test
    public void testFullQueueRefusesUntilPolled() {
        final PartitionCompactionRequestQueue queue = new PartitionCompactionRequestQueue(4);
        for (int i = 0; i < 4; i++) {
            Assert.assertTrue(queue.tryRequest(TOKEN_A, i));
        }
        Assert.assertFalse("a full queue must refuse rather than block", queue.tryRequest(TOKEN_B, 100));

        final PartitionCompactionRequestQueue.Request request = new PartitionCompactionRequestQueue.Request();
        Assert.assertTrue(queue.poll(request));
        Assert.assertTrue("a polled slot must be free again", queue.tryRequest(TOKEN_B, 100));
    }

    @Test
    public void testPollReturnsRequestsInOrder() {
        final PartitionCompactionRequestQueue queue = new PartitionCompactionRequestQueue(4);
        Assert.assertTrue(queue.tryRequest(TOKEN_A, 10));
        Assert.assertTrue(queue.tryRequest(TOKEN_B, 20));
        Assert.assertTrue(queue.tryRequest(TOKEN_A, 30));

        final PartitionCompactionRequestQueue.Request request = new PartitionCompactionRequestQueue.Request();
        Assert.assertTrue(queue.poll(request));
        Assert.assertSame(TOKEN_A, request.getTableToken());
        Assert.assertEquals(10, request.getLogicalPartitionTimestamp());
        Assert.assertTrue(queue.poll(request));
        Assert.assertSame(TOKEN_B, request.getTableToken());
        Assert.assertEquals(20, request.getLogicalPartitionTimestamp());
        Assert.assertTrue(queue.poll(request));
        Assert.assertSame(TOKEN_A, request.getTableToken());
        Assert.assertEquals(30, request.getLogicalPartitionTimestamp());
        Assert.assertFalse("an empty queue must report nothing", queue.poll(request));
    }
}
