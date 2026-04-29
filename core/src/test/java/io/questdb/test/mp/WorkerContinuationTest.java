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

import io.questdb.mp.WorkerContinuation;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerContinuationTest {

    @Test
    public void testIsMountedFalseOutsideRun() {
        Assert.assertFalse(WorkerContinuation.isMounted());
    }

    @Test
    public void testIsMountedTrueInsideRun() {
        AtomicReference<Boolean> mountedInsideBody = new AtomicReference<>();
        WorkerContinuation cont = new WorkerContinuation(
                () -> mountedInsideBody.set(WorkerContinuation.isMounted()),
                c -> {
                }
        );
        cont.run();
        Assert.assertTrue(cont.isDone());
        Assert.assertEquals(Boolean.TRUE, mountedInsideBody.get());
        Assert.assertFalse(WorkerContinuation.isMounted());
    }

    @Test
    public void testMultipleYieldsInOneContinuation() {
        AtomicReference<Integer> step = new AtomicReference<>(0);
        WorkerContinuation cont = new WorkerContinuation(() -> {
            step.set(1);
            WorkerContinuation.suspend();
            step.set(2);
            WorkerContinuation.suspend();
            step.set(3);
        }, c -> {
        });
        cont.run();
        Assert.assertFalse(cont.isDone());
        Assert.assertEquals(Integer.valueOf(1), step.get());

        cont.run();
        Assert.assertFalse(cont.isDone());
        Assert.assertEquals(Integer.valueOf(2), step.get());

        cont.run();
        Assert.assertTrue(cont.isDone());
        Assert.assertEquals(Integer.valueOf(3), step.get());
    }

    @Test
    public void testSuspendResumeOnDifferentThread() throws InterruptedException {
        AtomicReference<Thread> threadAtStart = new AtomicReference<>();
        AtomicReference<Thread> threadAfterResume = new AtomicReference<>();
        CountDownLatch doneLatch = new CountDownLatch(1);

        TestWorkerPool pool = new TestWorkerPool("sql-continuation-test", 1);
        // Cont's sink is the pool's resume queue; the pool's worker outer driver
        // drains the queue between mounts, so cont.scheduleResume() lands on a
        // worker of this pool.
        WorkerContinuation cont = new WorkerContinuation(() -> {
            threadAtStart.set(Thread.currentThread());
            WorkerContinuation.suspend();
            threadAfterResume.set(Thread.currentThread());
            doneLatch.countDown();
        }, pool.getContinuationSink());

        try {
            Thread runner = Thread.currentThread();
            cont.run();
            Assert.assertFalse("continuation must be parked, not done", cont.isDone());
            Assert.assertSame("body start ran on the caller thread", runner, threadAtStart.get());
            Assert.assertNull("body has not yet passed suspend()", threadAfterResume.get());

            pool.start();
            cont.scheduleResume();

            Assert.assertTrue("continuation resume timed out", doneLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(cont.isDone());
            Assert.assertNotNull(threadAfterResume.get());
            Assert.assertNotSame(
                    "resumed body must run on a worker thread, not the caller",
                    runner,
                    threadAfterResume.get()
            );
            Assert.assertTrue(
                    "resumed body must run on a pool worker",
                    threadAfterResume.get().getName().contains("sql-continuation-test")
            );
        } finally {
            pool.halt();
        }
    }
}
