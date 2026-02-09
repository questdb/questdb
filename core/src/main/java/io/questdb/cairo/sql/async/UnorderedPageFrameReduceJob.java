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

package io.questdb.cairo.sql.async;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Consumes tasks from the single unordered page frame reduce queue.
 * Unlike {@link PageFrameReduceJob}, tasks are released immediately after reading
 * the frame index and sequence reference. Completion is signaled via the sequence's
 * {@link io.questdb.mp.SOUnboundedCountDownLatch}.
 */
public class UnorderedPageFrameReduceJob implements Job, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(UnorderedPageFrameReduceJob.class);
    private final MessageBus messageBus;
    private SqlExecutionCircuitBreakerWrapper circuitBreaker;
    private PageFrameMemoryRecord record;

    public UnorderedPageFrameReduceJob(CairoEngine engine, MessageBus messageBus) {
        this.messageBus = messageBus;
        this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        this.circuitBreaker = new SqlExecutionCircuitBreakerWrapper(engine, engine.getConfiguration().getCircuitBreakerConfiguration());
    }

    /**
     * Tries to consume a single task from the unordered queue.
     */
    public static void consumeQueue(
            RingQueue<UnorderedPageFrameReduceTask> queue,
            MCSequence subSeq,
            PageFrameMemoryRecord record,
            SqlExecutionCircuitBreakerWrapper circuitBreaker,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        consumeQueue(-1, queue, subSeq, record, circuitBreaker, stealingFrameSequence);
    }

    @Override
    public void close() {
        circuitBreaker = Misc.free(circuitBreaker);
        record = Misc.free(record);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        return !consumeQueue(
                workerId,
                messageBus.getUnorderedPageFrameReduceQueue(),
                messageBus.getUnorderedPageFrameReduceSubSeq(),
                record,
                circuitBreaker,
                null
        );
    }

    private static boolean consumeQueue(
            int workerId,
            RingQueue<UnorderedPageFrameReduceTask> queue,
            MCSequence subSeq,
            PageFrameMemoryRecord record,
            SqlExecutionCircuitBreakerWrapper circuitBreaker,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        do {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final UnorderedPageFrameReduceTask task = queue.get(cursor);
                // Read task fields before releasing the slot.
                final UnorderedPageFrameSequence<?> frameSequence = task.getFrameSequence();
                final int frameIndex = task.getFrameIndex();
                final long taskSequenceId = task.getFrameSequenceId();
                // Release the queue slot immediately.
                task.clear();
                subSeq.done(cursor);

                if (taskSequenceId != frameSequence.getId()) {
                    // Stale task from a previous dispatch cycle; discard without
                    // touching the (already reset) latch or error state.
                    LOG.error()
                            .$("skipping stale task [expected=").$(frameSequence.getId())
                            .$(", got=").$(taskSequenceId)
                            .I$();
                } else {
                    try {
                        if (frameSequence.isActive()) {
                            // Always initialize the circuit breaker before checking state.
                            circuitBreaker.init(frameSequence.getCircuitBreaker());
                            final int cbState = frameSequence.isUninterruptible()
                                    ? SqlExecutionCircuitBreaker.STATE_OK
                                    : circuitBreaker.getState(frameSequence.getStartTime(), frameSequence.getCircuitBreaker().getFd());

                            if (cbState == SqlExecutionCircuitBreaker.STATE_OK) {
                                record.of(frameSequence.getSymbolTableSource());
                                frameSequence.getReduceStartedCounter().incrementAndGet();
                                frameSequence.getReducer().reduce(
                                        workerId,
                                        record,
                                        frameIndex,
                                        circuitBreaker,
                                        frameSequence,
                                        stealingFrameSequence
                                );
                            } else {
                                frameSequence.cancel(cbState);
                            }
                        }
                    } catch (Throwable th) {
                        LOG.error()
                                .$("reduce error [error=").$(th)
                                .$(", id=").$(frameSequence.getId())
                                .$(", frameIndex=").$(frameIndex)
                                .$(", frameCount=").$(frameSequence.getFrameCount())
                                .I$();
                        int interruptReason = SqlExecutionCircuitBreaker.STATE_OK;
                        if (th instanceof CairoException e) {
                            interruptReason = e.getInterruptionReason();
                        }
                        frameSequence.setError(th);
                        frameSequence.cancel(interruptReason);
                    } finally {
                        frameSequence.getDoneLatch().countDown();
                    }
                }
                return false;
            } else if (cursor == -1) {
                break;
            }
            Os.pause();
        } while (true);
        return true;
    }
}
