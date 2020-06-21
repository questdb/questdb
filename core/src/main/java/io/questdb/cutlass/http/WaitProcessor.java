/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.cairo.pool.ex.EntryUnavailableException;
import io.questdb.mp.*;
import io.questdb.std.time.MillisecondClock;
import org.jetbrains.annotations.Nullable;

import java.util.PriorityQueue;
import java.util.concurrent.locks.LockSupport;

public class WaitProcessor implements RescheduleContext {

    private static final int retryQueueLength = 4096;
    private final RingQueue<RetryHolder> inQueue = new RingQueue<>(RetryHolder::new, retryQueueLength);
    private final Sequence inPubSequence = new MPSequence(retryQueueLength);
    private final Sequence inSubSequence = new SCSequence();
    private final PriorityQueue<RetryHolder> nextRerun = new PriorityQueue<>(64);
    private final RingQueue<RetryHolder> outQueue = new RingQueue<>(RetryHolder::new, retryQueueLength);
    private final Sequence outPubSequence = new SPSequence(retryQueueLength);
    private final Sequence outSubSequence = new MCSequence(retryQueueLength);
    private final MillisecondClock clock;
    private final long maxWaitCapMs;
    private final double exponentialWaitMultiplier;

    public WaitProcessor(JobRunner pool, WaitProcessorConfiguration configuration) {
        this.clock = configuration.getClock();
        this.maxWaitCapMs = configuration.getMaxWaitCapMs();
        this.exponentialWaitMultiplier = configuration.getExponentialWaitMultiplier();

        inPubSequence.then(inSubSequence).then(inPubSequence);
        outPubSequence.then(outSubSequence).then(outPubSequence);

        if (pool.getWorkerCount() > 0) {
            int workerId = pool.getWorkerCount() - 1; // Last one lucky.
            pool.assign(workerId, workerId1 -> processInQueue() || sendToOutQueue());
        }
    }

    @Override
    // This supposed to run in http execution thread / job
    public void reschedule(Retry retry) {
        reschedule(retry, 0, 0);
    }

    public void reschedule(Retry retry, int attempt, long waitStartMs) {
        long now = clock.getTicks();
        while (true) {
            long cursor = inPubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                LockSupport.parkNanos(1);
                continue;
            }

            // -1 = queue is empty. It means there are already too many retries waiting
            // Send error to client.
            if (cursor < 0) {
                throw EntryUnavailableException.INSTANCE;
            }

            try {
                RetryHolder retryHolder = inQueue.get(cursor);
                retryHolder.retry = retry;
                retryHolder.attempt = attempt;
                retryHolder.lastRunTimestamp = now;
                retryHolder.waitStartTimestamp = attempt == 0 ? now : waitStartMs;

                return;
            } finally {
                inPubSequence.done(cursor);
            }
        }
    }

    // This hijacks http execution thread / job and runs retries in it.
    public boolean runReruns(HttpRequestProcessorSelector selector) {
        boolean useful = false;
        long now = 0;

        while (true) {
            RetryHolder retryHolder = getNextRerun();
            if (retryHolder != null) {
                useful = true;
                if (!retryHolder.retry.tryRerun(selector)) {
                    // Need more attempts
                    reschedule(retryHolder.retry, retryHolder.attempt + 1, retryHolder.waitStartTimestamp);
                }
            } else {
                return useful;
            }
        }
    }

    private @Nullable RetryHolder getNextRerun() {
        long cursor = outSubSequence.next();
        // -2 = there was a contest for queue index and this thread has lost
        if (cursor < 0) {
            return null;
        }

        try {
            return outQueue.get(cursor);
        } finally {
            outSubSequence.done(cursor);
        }
    }

    // Process incoming queue and put it on priority queue with next timestamp to rerun
    private boolean processInQueue() {
        while (true) {
            long cursor = inSubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                LockSupport.parkNanos(1);
                continue;
            }

            // -1 = queue is empty. All done.
            if (cursor < 0) {
                return false;
            }

            RetryHolder toRun;
            try {
                toRun = inQueue.get(cursor);
            } finally {
                inSubSequence.done(cursor);
            }

            RetryHolder copy = new RetryHolder();
            copy.retry = toRun.retry;
            copy.waitStartTimestamp = toRun.waitStartTimestamp;
            copy.attempt = toRun.attempt;
            copy.lastRunTimestamp = toRun.lastRunTimestamp;
            copy.nextRunTimestamp = calculateNextTimestamp(toRun.attempt, toRun.waitStartTimestamp, toRun.lastRunTimestamp);

            nextRerun.add(copy);
        }
    }

    private long calculateNextTimestamp(int attempt, long startTimestamp, long lastRunTimestamp) {
        if (attempt == 0) {
            // First retry after fixed time of 2ms
            return lastRunTimestamp + 2;
        }

        // 'exponentialWaitMultiplier' times wait time starting until it is 'maxWaitCapMs' sec
        long totalWait = lastRunTimestamp - startTimestamp;
        return Math.min(maxWaitCapMs, Math.max(4L, (long)(totalWait * exponentialWaitMultiplier))) + lastRunTimestamp;
    }

    private boolean sendToOutQueue() {
        boolean useful = false;
        final long now = clock.getTicks();
        while (nextRerun.size() > 0) {
            RetryHolder next = nextRerun.peek();
            if (next.nextRunTimestamp <= now) {
                useful = true;
                if (!sendToOutQueue(nextRerun.poll())) {
                    return true;
                }
            }
            else {
                // All reruns are in the future.
                return useful;
            }
        }
        return useful;
    }

    private boolean sendToOutQueue(RetryHolder retryHolder) {
        while (true) {
            long cursor = outPubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                LockSupport.parkNanos(1);
                continue;
            }

            if (cursor < 0) {
                // Cannot put to out queue. It is full. Release job and retry next run.
                return false;
            }

            try {
                RetryHolder retryHolderOut = outQueue.get(cursor);
                retryHolderOut.retry = retryHolder.retry;
                retryHolderOut.lastRunTimestamp = retryHolder.lastRunTimestamp;
                retryHolderOut.attempt = retryHolder.attempt;
                retryHolderOut.waitStartTimestamp = retryHolder.waitStartTimestamp;
                return true;
            } finally {
                outPubSequence.done(cursor);
            }
        }
    }
}
