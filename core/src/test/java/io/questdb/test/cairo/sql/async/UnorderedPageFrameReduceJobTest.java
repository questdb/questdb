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

package io.questdb.test.cairo.sql.async;

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceJob;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceTask;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.continuation.FiberDispatchContext;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class UnorderedPageFrameReduceJobTest extends AbstractCairoTest {
    @Test
    public void testContendedClaimThenCancellation() throws Exception {
        for (int contendedClaimCount = 0; contendedClaimCount <= 2; contendedClaimCount++) {
            assertContendedClaim(contendedClaimCount, true, true, false);
            assertContendedClaim(contendedClaimCount, true, true, true);
        }
    }

    @Test
    public void testContendedClaimThenEmptyQueue() throws Exception {
        for (int contendedClaimCount = 0; contendedClaimCount <= 2; contendedClaimCount++) {
            assertContendedClaim(contendedClaimCount, false, false, false);
            assertContendedClaim(contendedClaimCount, false, false, true);
        }
    }

    @Test
    public void testContendedClaimThenTask() throws Exception {
        for (int contendedClaimCount = 0; contendedClaimCount <= 2; contendedClaimCount++) {
            assertContendedClaim(contendedClaimCount, true, false, false);
            assertContendedClaim(contendedClaimCount, true, false, true);
        }
    }

    private void assertContendedClaim(
            int contendedClaimCount,
            boolean hasTask,
            boolean isCancelled,
            boolean isManaged
    ) throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger claimCalls = new AtomicInteger();
            final AtomicInteger doneCalls = new AtomicInteger();
            final AtomicInteger reduceCalls = new AtomicInteger();
            final FiberDispatchContext dispatchContext = isManaged ? new FiberDispatchContext() {
            } : null;
            try (
                    RingQueue<UnorderedPageFrameReduceTask> queue = new RingQueue<>(UnorderedPageFrameReduceTask::new, 1);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                            engine,
                            configuration.getCircuitBreakerConfiguration()
                    );
                    UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            configuration,
                            engine.getMessageBus(),
                            new StatefulAtom() {
                            },
                            (workerId, _, frameIndex, _, reducedSequence, stealingSequence) -> {
                                Assert.assertEquals(-1, workerId);
                                Assert.assertEquals(0, frameIndex);
                                Assert.assertSame(reducedSequence, stealingSequence);
                                reduceCalls.incrementAndGet();
                            },
                            1
                    ) {
                        @Override
                        public SqlExecutionCircuitBreaker getCircuitBreaker() {
                            return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                        }

                        @Override
                        public FiberDispatchContext getDispatchContext() {
                            return dispatchContext;
                        }
                    }
            ) {
                final MPSequence pubSeq = new MPSequence(queue.getCycle());
                final MCSequence subSeq = new MCSequence(queue.getCycle()) {
                    @Override
                    public void done(long cursor) {
                        doneCalls.incrementAndGet();
                        super.done(cursor);
                    }

                    @Override
                    public long next() {
                        final int call = claimCalls.getAndIncrement();
                        if (call < contendedClaimCount) {
                            if (isCancelled && call == 0) {
                                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                            }
                            return -2;
                        }
                        return super.next();
                    }
                };
                pubSeq.then(subSeq).then(pubSeq);
                if (hasTask) {
                    final long cursor = pubSeq.next();
                    Assert.assertEquals(0, cursor);
                    queue.get(cursor).of(frameSequence, 0);
                    pubSeq.done(cursor);
                }
                if (isCancelled && contendedClaimCount == 0) {
                    frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                }
                Assert.assertEquals(!hasTask, UnorderedPageFrameReduceJob.consumeQueue(
                        queue, subSeq, record, circuitBreaker, frameSequence
                ));
                Assert.assertEquals(contendedClaimCount + 1, claimCalls.get());
                Assert.assertEquals(hasTask ? 1 : 0, doneCalls.get());
                Assert.assertEquals(hasTask && !isCancelled ? 1 : 0, reduceCalls.get());
                Assert.assertEquals(hasTask ? -1 : 0, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(!isCancelled, frameSequence.isActive());
                Assert.assertEquals(
                        isCancelled ? SqlExecutionCircuitBreaker.STATE_CANCELLED : SqlExecutionCircuitBreaker.STATE_OK,
                        frameSequence.getCancelReason()
                );
                Assert.assertTrue(UnorderedPageFrameReduceJob.consumeQueue(
                        queue, subSeq, record, circuitBreaker, frameSequence
                ));
                Assert.assertEquals(hasTask ? 1 : 0, doneCalls.get());
                Assert.assertEquals(hasTask && !isCancelled ? 1 : 0, reduceCalls.get());
                Assert.assertEquals(contendedClaimCount + 2, claimCalls.get());
            }
        });
    }
}
