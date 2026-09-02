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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.griffin.engine.groupby.GroupByLongTopKJob;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.SPSequence;
import io.questdb.std.NumericException;
import io.questdb.tasks.GroupByLongTopKTask;
import io.questdb.tasks.GroupByMergeShardTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the error slot the detached merge shard and long top K workers use to hand
 * their failure to the owner thread.
 */
public class PostAggregationCircuitBreakerTest extends AbstractCairoTest {

    @Test
    public void testCancelWithoutErrorTripsWithNoErrorRecorded() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel();
        Assert.assertTrue(breaker.checkIfTripped());
        Assert.assertFalse(breaker.hasError());
    }

    @Test
    public void testCollateralErrorAfterTripIsDropped() {
        // A worker spinning in PerWorkerLocks.acquireSlot throws "query aborted" purely because
        // it observed the trip; that must not become the reported reason.
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel();
        breaker.cancel(CairoException.nonCritical().put("query aborted").setInterruption(true));
        Assert.assertFalse(breaker.hasError());
    }

    @Test
    public void testFirstErrorWins() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel(CairoException.nonCritical().put("first"));
        breaker.cancel(CairoException.nonCritical().put("second"));
        TestUtils.assertContains(((CairoException) breaker.buildError()).getFlyweightMessage(), "first");
    }

    @Test
    public void testPreservesCairoExceptionFlags() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel(
                CairoException.nonCritical()
                        .put("query memory limit exceeded [workload=QUERY]")
                        .position(42)
                        .setOutOfMemory(true)
        );

        Assert.assertTrue(breaker.checkIfTripped());
        Assert.assertTrue(breaker.hasError());

        final RuntimeException error = breaker.buildError();
        Assert.assertTrue(error instanceof CairoException);
        final CairoException e = (CairoException) error;
        Assert.assertTrue(e.isOutOfMemory());
        Assert.assertFalse(e.isCritical());
        Assert.assertEquals(42, e.getPosition());
        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded [workload=QUERY]");
    }

    @Test
    public void testPreservesCancellationAndErrno() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel(CairoException.critical(13).put("boom").setCancellation(true).setInterruption(true));

        final CairoException e = (CairoException) breaker.buildError();
        Assert.assertEquals(13, e.getErrno());
        Assert.assertTrue(e.isCritical());
        Assert.assertTrue(e.isCancellation());
        Assert.assertTrue(e.isInterruption());
        TestUtils.assertContains(e.getFlyweightMessage(), "boom");
    }

    @Test
    public void testRebuildsImplicitCastException() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel(ImplicitCastException.instance().put("inconvertible value").position(7));

        final RuntimeException error = breaker.buildError();
        Assert.assertTrue(error instanceof ImplicitCastException);
        final ImplicitCastException e = (ImplicitCastException) error;
        Assert.assertEquals(7, e.getPosition());
        TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
    }

    @Test
    public void testRebuildsNumericException() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel(NumericException.instance().put("not a number").position(3));

        final RuntimeException error = breaker.buildError();
        Assert.assertTrue(error instanceof NumericException);
        final NumericException e = (NumericException) error;
        Assert.assertEquals(3, e.getPosition());
        TestUtils.assertContains(e.getFlyweightMessage(), "not a number");
    }

    @Test
    public void testResetClearsErrorAndFlag() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel(CairoException.nonCritical().put("stale").setOutOfMemory(true));
        Assert.assertTrue(breaker.hasError());

        breaker.reset();
        Assert.assertFalse(breaker.checkIfTripped());
        Assert.assertFalse(breaker.hasError());

        breaker.cancel(CairoException.nonCritical().put("fresh"));
        final CairoException e = (CairoException) breaker.buildError();
        Assert.assertFalse(e.isOutOfMemory());
        TestUtils.assertContains(e.getFlyweightMessage(), "fresh");
    }

    @Test
    public void testWrapsUnexpectedThrowable() {
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        breaker.cancel(new IllegalStateException("slot already released"));

        final RuntimeException error = breaker.buildError();
        Assert.assertTrue(error instanceof CairoException);
        final CairoException e = (CairoException) error;
        TestUtils.assertContains(e.getFlyweightMessage(), "unexpected post-aggregation error: slot already released");
    }

    @Test
    public void testLongTopKJobHandsWorkerFailureToTheOwner() {
        // A null atom stands in for any failure inside the top-K body.
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
        final AtomicInteger startedCounter = new AtomicInteger();
        final RingQueue<GroupByLongTopKTask> queue = new RingQueue<>(GroupByLongTopKTask::new, 4);
        final SPSequence pubSeq = new SPSequence(4);
        final SCSequence subSeq = new SCSequence();
        pubSeq.then(subSeq).then(pubSeq);

        final long pubCursor = pubSeq.next();
        queue.get(pubCursor).of(breaker, startedCounter, doneLatch, null, null, 0, 0, 1);
        pubSeq.done(pubCursor);

        final long subCursor = subSeq.next();
        GroupByLongTopKJob.run(-1, queue.get(subCursor), subSeq, subCursor, null);

        Assert.assertTrue("the worker must count down even on failure", doneLatch.done(1));
        Assert.assertTrue(breaker.checkIfTripped());
        Assert.assertTrue("the worker's error must reach the owner", breaker.hasError());
        TestUtils.assertContains(((CairoException) breaker.buildError()).getFlyweightMessage(), "unexpected post-aggregation error");
    }

    @Test
    public void testMergeShardJobHandsWorkerFailureToTheOwner() {
        // A null sharding context stands in for any failure inside the merge body.
        final PostAggregationCircuitBreaker breaker = new PostAggregationCircuitBreaker(engine);
        final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
        final AtomicInteger startedCounter = new AtomicInteger();
        final RingQueue<GroupByMergeShardTask> queue = new RingQueue<>(GroupByMergeShardTask::new, 4);
        final SPSequence pubSeq = new SPSequence(4);
        final SCSequence subSeq = new SCSequence();
        pubSeq.then(subSeq).then(pubSeq);

        final long pubCursor = pubSeq.next();
        queue.get(pubCursor).of(breaker, startedCounter, doneLatch, null, 0);
        pubSeq.done(pubCursor);

        final long subCursor = subSeq.next();
        GroupByMergeShardJob.run(-1, queue.get(subCursor), subSeq, subCursor, null);

        Assert.assertEquals(1, startedCounter.get());
        Assert.assertTrue("the worker must count down even on failure", doneLatch.done(1));
        Assert.assertTrue(breaker.checkIfTripped());
        Assert.assertTrue("the worker's error must reach the owner", breaker.hasError());
        TestUtils.assertContains(((CairoException) breaker.buildError()).getFlyweightMessage(), "unexpected post-aggregation error");
    }
}
