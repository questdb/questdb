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

package io.questdb.cutlass.http;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.http.ex.RetryFailedOperationException;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;

import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.PriorityQueue;

public class WaitProcessor extends SynchronizedJob implements RescheduleContext, Closeable {
    private final MillisecondClock clock;
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final double exponentialWaitMultiplier;
    private RetryHolder freeRetryHolders;
    private final Sequence inPubSequence;
    private final RingQueue<RetryHolder> inQueue;
    private final Sequence inSubSequence;
    private final long maxWaitCapMs;
    private final PriorityQueue<RetryHolder> nextRerun;
    private final Sequence outPubSequence;
    private final RingQueue<RetryHolder> outQueue;
    private final Sequence outSubSequence;

    public WaitProcessor(WaitProcessorConfiguration configuration, IODispatcher<HttpConnectionContext> dispatcher) {
        clock = configuration.getClock();
        maxWaitCapMs = configuration.getMaxWaitCapMs();
        exponentialWaitMultiplier = configuration.getExponentialWaitMultiplier();
        final int initialWaitQueueSize = configuration.getInitialWaitQueueSize();
        nextRerun = new PriorityQueue<>(initialWaitQueueSize, WaitProcessor::compareRetriesInQueue);
        for (int i = 0; i < initialWaitQueueSize; i++) {
            releaseRetryHolder(new RetryHolder());
        }
        this.dispatcher = dispatcher;

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
    public void close() {
        acquireRunLock();
        try {
            Throwable failure = null;
            try {
                processInQueue();
            } catch (Throwable th) {
                failure = th;
            }
            while (true) {
                long cursor = outSubSequence.next();
                if (cursor < -1) {
                    Os.pause();
                    continue;
                }
                if (cursor < 0) {
                    break;
                }
                RetryHolder retryHolder = outQueue.get(cursor);
                try {
                    freeRetry(retryHolder);
                } catch (Throwable th) {
                    failure = aggregate(failure, th);
                } finally {
                    outSubSequence.done(cursor);
                }
            }
            for (int i = 0, n = nextRerun.size(); i < n; i++) {
                final RetryHolder retryHolder = nextRerun.poll();
                try {
                    freeRetry(retryHolder);
                } catch (Throwable th) {
                    failure = aggregate(failure, th);
                }
                releaseRetryHolder(retryHolder);
            }
            CairoException.rethrowCleanupFailure(failure);
        } finally {
            releaseRunLock();
        }
    }

    @TestOnly
    public long getRescheduleCount() {
        return inPubSequence.current() + 1;
    }

    public boolean launchReruns(FiberRuntime runtime, RetryLauncher launcher) {
        boolean useful = false;

        while (hasPendingReruns()) {
            final Fiber fiber = runtime.tryReserveFiber();
            if (fiber == null) {
                return useful;
            }
            final long reservationEpoch = fiber.getReservationEpoch();
            final long cursor = outSubSequence.next();
            if (cursor < 0) {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
                return useful;
            }
            final RetryHolder retryHolder = outQueue.get(cursor);
            final Retry retry = retryHolder.retry;
            final long taskIncarnation = retryHolder.taskIncarnation;
            retryHolder.clear();
            outSubSequence.done(cursor);
            if (retry != null) {
                useful = true;
                if (retry.isRetryCurrent(taskIncarnation)) {
                    try {
                        launcher.launch(fiber, reservationEpoch, retry, taskIncarnation);
                    } catch (Throwable th) {
                        try {
                            runtime.releaseReservedFiber(fiber, reservationEpoch);
                        } catch (Throwable cleanupError) {
                            if (cleanupError != th) {
                                th.addSuppressed(cleanupError);
                            }
                        }
                        try {
                            if (retry.claimRetryClose(taskIncarnation)) {
                                Misc.free(retry);
                            }
                        } catch (Throwable cleanupError) {
                            if (cleanupError != th) {
                                th.addSuppressed(cleanupError);
                            }
                        }
                        throw th;
                    }
                    continue;
                }
            }
            runtime.releaseReservedFiber(fiber, reservationEpoch);
        }
        return useful;
    }

    @Override
    // This supposed to run in http execution thread / job
    public void reschedule(Retry retry) {
        publishReschedule(prepareReschedule(retry, 0, 0, 0));
    }

    public void reschedule(Retry retry, long taskIncarnation) {
        publishReschedule(prepareReschedule(retry, taskIncarnation, 0, 0));
    }

    // This hijacks http execution thread / job and runs retries in it.
    public boolean runReruns(HttpRequestProcessorSelector selector) {
        boolean useful = false;

        while (true) {
            final long cursor = outSubSequence.next();
            if (cursor < 0) {
                return useful;
            }
            final RetryHolder retryHolder = outQueue.get(cursor);
            final Retry retry = retryHolder.retry;
            final long taskIncarnation = retryHolder.taskIncarnation;
            retryHolder.clear();
            outSubSequence.done(cursor);
            if (retry != null) {
                useful = true;
                if (retry.isRetryCurrent(taskIncarnation)) {
                    run(selector, retry);
                }
            }
        }
    }

    @Override
    public boolean runSerially() {
        return processInQueue() || sendToOutQueue();
    }

    void abortPreparedReschedule(long cursor) {
        inQueue.get(cursor).clear();
        inPubSequence.done(cursor);
    }

    long prepareReschedule(Retry retry) throws RetryFailedOperationException {
        return prepareReschedule(retry, 0, 0, 0);
    }

    long prepareReschedule(Retry retry, long taskIncarnation) throws RetryFailedOperationException {
        return prepareReschedule(retry, taskIncarnation, 0, 0);
    }

    long prepareRescheduleNextAttempt(Retry retry) throws RetryFailedOperationException {
        return prepareReschedule(
                retry,
                0,
                retry.getAttemptDetails().attempt + 1,
                retry.getAttemptDetails().waitStartTimestamp
        );
    }

    long prepareRescheduleNextAttempt(Retry retry, long taskIncarnation) throws RetryFailedOperationException {
        return prepareReschedule(
                retry,
                taskIncarnation,
                retry.getAttemptDetails().attempt + 1,
                retry.getAttemptDetails().waitStartTimestamp
        );
    }

    void publishReschedule(long cursor) {
        inPubSequence.done(cursor);
    }

    private static Throwable aggregate(Throwable primary, Throwable th) {
        if (primary == null) {
            return th;
        }
        if (primary != th) {
            primary.addSuppressed(th);
        }
        return primary;
    }

    private static int compareRetriesInQueue(RetryHolder r1, RetryHolder r2) {
        return Long.compare(r1.nextRunTimestamp, r2.nextRunTimestamp);
    }

    private static void freeRetry(RetryHolder retryHolder) {
        final Retry retry = retryHolder.retry;
        if (retry != null && retry.claimRetryClose(retryHolder.taskIncarnation)) {
            Misc.free(retry);
        }
        retryHolder.clear();
    }

    private long calculateNextTimestamp(int attempt, long lastRunTimestamp, long waitStartTimestamp) {
        if (attempt == 0) {
            // First retry after fixed time of 2ms
            return lastRunTimestamp + 2;
        }

        // 'exponentialWaitMultiplier' times wait time starting until it is 'maxWaitCapMs' sec
        final long totalWait = lastRunTimestamp - waitStartTimestamp;
        return Math.min(maxWaitCapMs, Math.max(4L, (long) (totalWait * exponentialWaitMultiplier))) + lastRunTimestamp;
    }

    private boolean hasPendingReruns() {
        final long next = outSubSequence.current() + 1;
        return outSubSequence.getBarrier().availableIndex(next) >= next;
    }

    private RetryHolder nextRetryHolder() {
        final RetryHolder holder = freeRetryHolders;
        if (holder == null) {
            return new RetryHolder();
        }
        freeRetryHolders = holder.nextFree;
        holder.nextFree = null;
        return holder;
    }

    private long prepareReschedule(Retry retry, long taskIncarnation, int attempt, long waitStartMs) {
        final long now = clock.getTicks();
        final long waitStartTimestamp = attempt == 0 ? now : waitStartMs;
        final RetryAttemptAttributes attemptAttributes = retry.getAttemptDetails();
        attemptAttributes.attempt = attempt;
        attemptAttributes.lastRunTimestamp = now;
        attemptAttributes.nextRunTimestamp = calculateNextTimestamp(attempt, now, waitStartTimestamp);
        attemptAttributes.waitStartTimestamp = waitStartTimestamp;

        while (true) {
            long cursor = inPubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                Os.pause();
                continue;
            }

            // -1 = queue is full. It means there are already too many retries waiting
            // Send error to client.
            if (cursor < 0) {
                throw RetryFailedOperationException.INSTANCE;
            }

            RetryHolder retryHolder = inQueue.get(cursor);
            retryHolder.of(retry, taskIncarnation, attemptAttributes.nextRunTimestamp);
            return cursor;
        }
    }

    // Process incoming queue and put it on priority queue with next timestamp to rerun
    private boolean processInQueue() {
        boolean any = false;
        while (true) {
            long cursor = inSubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                Os.pause();
                continue;
            }

            // -1 = queue is empty. All done.
            if (cursor < 0) {
                return any;
            }

            RetryHolder scheduledRetry = null;
            try {
                final RetryHolder incomingRetry = inQueue.get(cursor);
                if (incomingRetry.retry != null) {
                    any = true;
                    if (incomingRetry.retry.isRetryCurrent(incomingRetry.taskIncarnation)) {
                        scheduledRetry = nextRetryHolder();
                        scheduledRetry.of(
                                incomingRetry.retry,
                                incomingRetry.taskIncarnation,
                                incomingRetry.nextRunTimestamp
                        );
                    }
                }
                incomingRetry.clear();
            } finally {
                inSubSequence.done(cursor);
            }
            if (scheduledRetry != null) {
                nextRerun.add(scheduledRetry);
            }
        }
    }

    private void releaseRetryHolder(RetryHolder holder) {
        holder.clear();
        holder.nextFree = freeRetryHolders;
        freeRetryHolders = holder;
    }

    private void run(HttpRequestProcessorSelector selector, Retry retry) {
        try {
            if (!retry.tryRerun(selector, this)) {
                try {
                    publishReschedule(prepareRescheduleNextAttempt(retry));
                } catch (RetryFailedOperationException e) {
                    retry.fail(selector, e);
                }
            }
        } catch (PeerIsSlowToReadException e) {
            HttpConnectionContext context = (HttpConnectionContext) retry;
            dispatcher.registerChannel(context, IOOperation.WRITE);
        } catch (PeerIsSlowToWriteException e) {
            HttpConnectionContext context = (HttpConnectionContext) retry;
            dispatcher.registerChannel(context, IOOperation.READ);
        } catch (ServerDisconnectException e) {
            HttpConnectionContext context = (HttpConnectionContext) retry;
            dispatcher.disconnect((HttpConnectionContext) retry, context.getDisconnectReason());
        }
    }

    private boolean sendToOutQueue() {
        boolean useful = false;
        final long now = clock.getTicks();
        while (!nextRerun.isEmpty()) {
            final RetryHolder next = nextRerun.peek();
            if (!next.retry.isRetryCurrent(next.taskIncarnation)) {
                nextRerun.poll();
                next.clear();
                releaseRetryHolder(next);
                useful = true;
                continue;
            }
            if (next.nextRunTimestamp <= now) {
                useful = true;
                final RetryHolder retryHolder = nextRerun.poll();
                if (!sendToOutQueue(retryHolder)) {
                    nextRerun.add(retryHolder);
                    return true;
                }
                retryHolder.clear();
                releaseRetryHolder(retryHolder);
            } else {
                // All reruns are in the future.
                return useful;
            }
        }
        return useful;
    }

    private boolean sendToOutQueue(RetryHolder retry) {
        while (true) {
            long cursor = outPubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                Os.pause();
                continue;
            }

            if (cursor < 0) {
                // Cannot put to out queue. It is full. Release job and retry next run.
                return false;
            }

            RetryHolder retryHolderOut = outQueue.get(cursor);
            retryHolderOut.of(retry.retry, retry.taskIncarnation, retry.nextRunTimestamp);
            outPubSequence.done(cursor);
            return true;
        }
    }

    /**
     * Schedules a due retry for execution; the fiber-mode dispatch job passes a
     * launcher that stages the retry on the connection's fiber task.
     */
    @FunctionalInterface
    public interface RetryLauncher {
        void launch(Fiber fiber, long reservationEpoch, Retry retry, long taskIncarnation);
    }
}
