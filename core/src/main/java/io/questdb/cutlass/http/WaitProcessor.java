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

import io.questdb.cutlass.http.ex.RetryFailedOperationException;
import io.questdb.mp.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;

import java.util.PriorityQueue;
import java.util.concurrent.locks.LockSupport;

public class WaitProcessor extends  SynchronizedJob implements RescheduleContext {

    private final RingQueue<RetryHolder> inQueue;
    private final Sequence inPubSequence;
    private final Sequence inSubSequence;
    private final PriorityQueue<Retry> nextRerun;
    private final RingQueue<RetryHolder> outQueue;
    private final Sequence outPubSequence;
    private final Sequence outSubSequence;
    private final MillisecondClock clock;
    private final long maxWaitCapMs;
    private final double exponentialWaitMultiplier;

    public WaitProcessor(WaitProcessorConfiguration configuration) {
        this.clock = configuration.getClock();
        this.maxWaitCapMs = configuration.getMaxWaitCapMs();
        this.exponentialWaitMultiplier = configuration.getExponentialWaitMultiplier();
        nextRerun = new PriorityQueue<>(configuration.getInitialWaitQueueSize(), WaitProcessor::compareRetiesInQueue);

        int retryQueueLength = configuration.getMaxProcessingQueueSize();
        inQueue = new RingQueue<>(RetryHolder::new, retryQueueLength);
        inPubSequence = new MPSequence(retryQueueLength);
        inSubSequence = new SCSequence();
        outQueue = new RingQueue<>(RetryHolder::new, retryQueueLength);
        outPubSequence = new SPSequence(retryQueueLength);
        outSubSequence = new MCSequence(retryQueueLength);

        inPubSequence.then(inSubSequence).then(inPubSequence);
        outPubSequence.then(outSubSequence).then(outPubSequence);
    }

    @Override
    protected boolean runSerially() {
        return processInQueue() || sendToOutQueue();
    }

    @Override
    // This supposed to run in http execution thread / job
    public void reschedule(Retry retry) {
        reschedule(retry, 0, 0);
    }

    private void reschedule(Retry retry, int attempt, long waitStartMs) {
        long now = clock.getTicks();
        retry.getAttemptDetails().attempt = attempt;
        retry.getAttemptDetails().lastRunTimestamp = now;
        retry.getAttemptDetails().waitStartTimestamp = attempt == 0 ? now : waitStartMs;

        while (true) {
            long cursor = inPubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                continue;
            }

            // -1 = queue is empty. It means there are already too many retries waiting
            // Send error to client.
            if (cursor < 0) {
                throw RetryFailedOperationException.INSTANCE;
            }

            RetryHolder retryHolder = inQueue.get(cursor);
            retryHolder.retry = retry;
            inPubSequence.done(cursor);
            return;
        }
    }

    // This hijacks http execution thread / job and runs retries in it.
    public boolean runReruns(HttpRequestProcessorSelector selector) {
        boolean useful = false;

        while (true) {
            Retry retry = getNextRerun();
            if (retry != null) {
                useful = true;
                if (!retry.tryRerun(selector, this)) {
                    try {
                        reschedule(retry, retry.getAttemptDetails().attempt + 1, retry.getAttemptDetails().waitStartTimestamp);
                    } catch (RetryFailedOperationException e) {
                        retry.fail(selector, e);
                    }
                }
            } else {
                return useful;
            }
        }
    }

    private @Nullable Retry getNextRerun() {
        long cursor = outSubSequence.next();
        // -2 = there was a contest for queue index and this thread has lost
        if (cursor < 0) {
            return null;
        }

        RetryHolder retryHolder = outQueue.get(cursor);
        Retry r = retryHolder.retry;
        retryHolder.retry = null;
        outSubSequence.done(cursor);
        return r;
    }

    // Process incoming queue and put it on priority queue with next timestamp to rerun
    private boolean processInQueue() {
        boolean any = false;
        while (true) {
            long cursor = inSubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                continue;
            }

            // -1 = queue is empty. All done.
            if (cursor < 0) {
                return any;
            }

            Retry retry;
            try {
                RetryHolder toRun = inQueue.get(cursor);
                retry = toRun.retry;
                toRun.retry = null;
            } finally {
                inSubSequence.done(cursor);
            }
            retry.getAttemptDetails().nextRunTimestamp = calculateNextTimestamp(retry.getAttemptDetails());

            nextRerun.add(retry);
            any = true;
        }
    }

    private long calculateNextTimestamp(RetryAttemptAttributes attemptAttributes) {
        if (attemptAttributes.attempt == 0) {
            // First retry after fixed time of 2ms
            return attemptAttributes.lastRunTimestamp + 2;
        }

        // 'exponentialWaitMultiplier' times wait time starting until it is 'maxWaitCapMs' sec
        long totalWait = attemptAttributes.lastRunTimestamp - attemptAttributes.waitStartTimestamp;
        return Math.min(maxWaitCapMs, Math.max(4L, (long)(totalWait * exponentialWaitMultiplier))) + attemptAttributes.lastRunTimestamp;
    }

    private boolean sendToOutQueue() {
        boolean useful = false;
        final long now = clock.getTicks();
        while (nextRerun.size() > 0) {
            Retry next = nextRerun.peek();
            if (next.getAttemptDetails().nextRunTimestamp <= now) {
                useful = true;
                Retry retry = nextRerun.poll();
                if (!sendToOutQueue(retry)) {
                    nextRerun.add(retry);
                    return true;
                }
            } else {
                // All reruns are in the future.
                return useful;
            }
        }
        return useful;
    }

    private boolean sendToOutQueue(Retry retry) {
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

            RetryHolder retryHolderOut = outQueue.get(cursor);
            retryHolderOut.retry = retry;
            outPubSequence.done(cursor);
            return true;
        }
    }

    private static int compareRetiesInQueue(Retry r1, Retry r2) {
        // r1, r2 are always not null, null retries are not queued
        RetryAttemptAttributes a1 = r1.getAttemptDetails();
        RetryAttemptAttributes a2 = r2.getAttemptDetails();
        return Long.compare(a1.nextRunTimestamp, a2.nextRunTimestamp);
    }
}
