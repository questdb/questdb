/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageAddressCacheRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class PageFrameReduceJob implements Job, Closeable {

    private final static Log LOG = LogFactory.getLog(PageFrameReduceJob.class);
    private final MessageBus messageBus;
    private final int shardCount;
    private final int[] shards;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private PageAddressCacheRecord record;

    // Each thread should be assigned own instance of this job, making the code effectively
    // single threaded. Such assignment is necessary for threads to have their own shard walk sequence.
    public PageFrameReduceJob(
            MessageBus bus,
            Rnd rnd,
            @Nullable SqlExecutionCircuitBreakerConfiguration sqlExecutionCircuitBreakerConfiguration
    ) {
        this.messageBus = bus;
        this.shardCount = messageBus.getPageFrameReduceShardCount();
        this.shards = new int[shardCount];
        // fill shards[] with shard indexes
        for (int i = 0; i < shardCount; i++) {
            shards[i] = i;
        }

        // shuffle shard indexes such that each job has its own
        // pass order over the shared queues
        int currentIndex = shardCount;
        int randomIndex;
        while (currentIndex != 0) {
            randomIndex = (int) Math.floor(rnd.nextDouble() * currentIndex);
            currentIndex--;

            final int tmp = shards[currentIndex];
            shards[currentIndex] = shards[randomIndex];
            shards[randomIndex] = tmp;
        }

        this.record = new PageAddressCacheRecord();
        if (sqlExecutionCircuitBreakerConfiguration != null) {
            this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(sqlExecutionCircuitBreakerConfiguration, MemoryTag.NATIVE_CB1);
        } else {
            this.circuitBreaker = NetworkSqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
        }
    }

    /**
     * Reduces single queue item when item is available. Return value is inverted as in
     * true when queue item is not available, false otherwise. Item is reduced using the
     * reducer method provided with each queue item.
     *
     * @param queue                 page frame queue instance
     * @param subSeq                subscriber sequence
     * @param record                instance of record that can be positioned on the frame and each row in that frame
     * @param circuitBreaker        circuit breaker instance
     * @param stealingFrameSequence page frame sequence that is stealing work, it is used to identify if
     *                              page frame sequence is stealing its own work or someone else's
     * @return inverted value of queue processing status; true if nothing was processed.
     */
    public static boolean consumeQueue(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence subSeq,
            PageAddressCacheRecord record,
            SqlExecutionCircuitBreaker circuitBreaker,
            PageFrameSequence<?> stealingFrameSequence
    ) {
        return consumeQueue(
                -1,
                queue,
                subSeq,
                record,
                circuitBreaker,
                stealingFrameSequence
        );
    }

    public static void reduce(
            PageAddressCacheRecord record,
            SqlExecutionCircuitBreaker circuitBreaker,
            PageFrameReduceTask task,
            PageFrameSequence<?> frameSequence,
            PageFrameSequence<?> stealingFrameSequence
    ) {
        reduce(
                -1,
                record,
                circuitBreaker,
                task,
                frameSequence,
                stealingFrameSequence
        );
    }

    @Override
    public void close() {
        circuitBreaker = Misc.freeIfCloseable(circuitBreaker);
        record = Misc.free(record);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        // there is job instance per thread, the worker id must never change
        // for this job
        boolean useful = false;
        for (int i = 0; i < shardCount; i++) {
            final int shard = shards[i];
            useful = !consumeQueue(
                    workerId,
                    messageBus.getPageFrameReduceQueue(shard),
                    messageBus.getPageFrameReduceSubSeq(shard),
                    record,
                    circuitBreaker,
                    null // this is correct worker processing tasks rather than PageFrameSequence
                    // helping to steal work
            ) || useful;
        }
        return useful;
    }

    private static boolean consumeQueue(
            int workerId,
            RingQueue<PageFrameReduceTask> queue,
            MCSequence subSeq,
            PageAddressCacheRecord record,
            SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        // loop is required to deal with CAS errors, cursor == -2
        do {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final PageFrameReduceTask task = queue.get(cursor);
                final PageFrameSequence<?> frameSequence = task.getFrameSequence();
                try {
                    LOG.debug()
                            .$("reducing [shard=").$(frameSequence.getShard())
                            .$(", id=").$(frameSequence.getId())
                            .$(", taskType=").$(task.getType())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();
                    if (frameSequence.isActive()) {
                        reduce(workerId, record, circuitBreaker, task, frameSequence, stealingFrameSequence);
                    }
                } catch (Throwable th) {
                    LOG.error().$("reduce error [ex=").$(th).I$();
                    task.setErrorMsg(th);
                    frameSequence.cancel();
                } finally {
                    subSeq.done(cursor);
                    // Reduce counter has to be incremented only when we make
                    // sure that the task is available for consumers.
                    frameSequence.getReduceCounter().incrementAndGet();
                }
                return false;
            } else if (cursor == -1) {
                // queue is empty, we should yield or help
                break;
            }
            Os.pause();
        } while (true);
        return true;
    }

    private static void reduce(
            int workerId,
            PageAddressCacheRecord record,
            SqlExecutionCircuitBreaker circuitBreaker,
            PageFrameReduceTask task,
            PageFrameSequence<?> frameSequence,
            PageFrameSequence<?> stealingFrameSequence
    ) {
        // we deliberately hold the queue item because
        // processing is daisy-chained. If we were to release item before
        // finishing reduction, next step (job) will be processing an incomplete task
        if (frameSequence.isUninterruptible() || !circuitBreaker.checkIfTripped(frameSequence.getStartTime(), frameSequence.getCircuitBreakerFd())) {
            record.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
            record.setFrameIndex(task.getFrameIndex());
            assert !frameSequence.done;
            frameSequence.getReducer().reduce(workerId, record, task, circuitBreaker, stealingFrameSequence);
        } else {
            frameSequence.cancel();
        }
    }
}
