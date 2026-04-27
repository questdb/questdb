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

import io.questdb.mp.ContinuationResumeJob;
import io.questdb.mp.SqlContinuation;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SqlContinuationTest {

    @Test
    public void testIsMountedFalseOutsideRun() {
        Assert.assertFalse(SqlContinuation.isMounted());
    }

    @Test
    public void testIsMountedTrueInsideRun() {
        AtomicReference<Boolean> mountedInsideBody = new AtomicReference<>();
        SqlContinuation cont = new SqlContinuation(() -> mountedInsideBody.set(SqlContinuation.isMounted()));
        cont.run();
        Assert.assertTrue(cont.isDone());
        Assert.assertEquals(Boolean.TRUE, mountedInsideBody.get());
        Assert.assertFalse(SqlContinuation.isMounted());
    }

    @Test
    public void testMultipleYieldsInOneContinuation() throws InterruptedException {
        AtomicReference<Integer> step = new AtomicReference<>(0);
        SqlContinuation cont = new SqlContinuation(() -> {
            step.set(1);
            SqlContinuation.suspend();
            step.set(2);
            SqlContinuation.suspend();
            step.set(3);
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

        SqlContinuation cont = new SqlContinuation(() -> {
            threadAtStart.set(Thread.currentThread());
            SqlContinuation.suspend();
            threadAfterResume.set(Thread.currentThread());
            doneLatch.countDown();
        });

        ContinuationResumeJob resumeJob = new ContinuationResumeJob();
        TestWorkerPool pool = new TestWorkerPool("sql-continuation-test", 1);
        pool.assign(resumeJob);

        try {
            Thread runner = Thread.currentThread();
            cont.run();
            Assert.assertFalse("continuation must be parked, not done", cont.isDone());
            Assert.assertSame("body start ran on the caller thread", runner, threadAtStart.get());
            Assert.assertNull("body has not yet passed suspend()", threadAfterResume.get());

            pool.start();
            resumeJob.enqueue(cont);

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
