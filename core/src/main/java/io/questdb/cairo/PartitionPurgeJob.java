/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.PartitionPurgeTask;

import java.io.Closeable;
import java.util.Comparator;
import java.util.PriorityQueue;

import static io.questdb.cairo.TableUtils.setPathForPartition;
import static io.questdb.cairo.TableUtils.txnPartitionConditionally;

public class PartitionPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(PartitionPurgeJob.class);
    private static final int MAX_ATTEMPTS = 2;

    private final MicrosecondClock clock;
    private final FilesFacade ff;
    private final RingQueue<PartitionPurgeTask> inQueue;
    private final Sequence inSubSequence;
    private final WeakMutableObjectPool<PartitionPurgeRetryTask> taskPool;
    private final PriorityQueue<PartitionPurgeRetryTask> retryQueue;
    private TxnScoreboard txnScoreboard;
    private final int txnScoreboardEntryCount;
    private final Path path;
    private final int rootLen;
    private int attemptCount;


    public PartitionPurgeJob(CairoEngine engine) {
        CairoConfiguration config = engine.getConfiguration();
        MessageBus messageBus = engine.getMessageBus();
        clock = config.getMicrosecondClock();
        ff = config.getFilesFacade();
        txnScoreboardEntryCount = config.getTxnScoreboardEntryCount();
        int capacity = config.getPartitionPurgeQueueCapacity();
        retryQueue = new PriorityQueue<>(capacity, Comparator.comparingLong(task -> task.nextRunTimestamp));
        inQueue = messageBus.getPartitionPurgeQueue();
        inSubSequence = messageBus.getColumnPurgeSubSeq();
        taskPool = new WeakMutableObjectPool<>(PartitionPurgeRetryTask::new, capacity);
        path = new Path();
        path.of(config.getRoot());
        rootLen = path.length();
    }

    @Override
    public void close() {
        Misc.free(taskPool);
        Misc.free(txnScoreboard);
        Misc.free(path);
    }

    @Override
    protected boolean runSerially() {
        if (attemptCount >= MAX_ATTEMPTS) {
            return false;
        }

        boolean useful = processInQueue();
        boolean cleanupUseful = purgePartition();
        attemptCount = 0;
        return cleanupUseful || useful;
    }

    private boolean processInQueue() {
        boolean useful = false;
        long ticks = clock.getTicks(); // micro
        while (true) {
            final long cursor = inSubSequence.next();
            if (cursor < -1) {
                Os.pause(); // (-2) yield, another thread took this cursor
                continue;
            }
            if (cursor < 0) {
                break; // (-1) empty queue
            }

            // copy task to high priority queue
            PartitionPurgeRetryTask task = taskPool.pop().copyFrom(inQueue.get(cursor), ticks);
            inSubSequence.done(cursor);
            retryQueue.add(task);
            useful = true;
            ticks++;
        }
        return useful;
    }

    private boolean purgePartition() {
        boolean useful = false;
        final long now = clock.getTicks() + 1L;
        while (retryQueue.size() > 0) {
            PartitionPurgeRetryTask task = retryQueue.peek();
            if (task.isDue(now)) {
                retryQueue.poll();
                useful = true;

                final long txn = task.getPartitionNameTxn();
                path.trimTo(rootLen).concat(task.getTableName());
                final int rootTableNameLen = path.length();
                if (txnScoreboard == null) {
                    txnScoreboard = new TxnScoreboard(ff, txnScoreboardEntryCount);
                }
                txnScoreboard.ofRW(path);
                try {
                    if (txnScoreboard.acquireTxn(txn)) {
                        txnScoreboard.releaseTxn(txn);
                    }
                    if (txnScoreboard.getMin() > txn - 1) {
                        try {
                            setPathForPartition(
                                    path.trimTo(rootTableNameLen),
                                    task.getPartitionBy(),
                                    task.getTimestamp(),
                                    false
                            );
                            txnPartitionConditionally(path, txn);
                            long errno = ff.rmdir(path.$());
                            if (errno == 0 || errno == -1) {
                                taskPool.push(task);
                            } else {
                                task.updateNextRunTimestamp(now);
                                retryQueue.add(task);
                            }
                        } finally {
                            path.trimTo(rootLen);
                        }
                    }

                } catch (CairoException ignore) {
                    // Scoreboard can be over allocated
                    // retry
                } finally {
                    path.trimTo(rootLen);
                }
            } else {
                return useful;
            }
        }
        return useful;
    }

    static class PartitionPurgeRetryTask extends PartitionPurgeTask {
        private static final long START_RETRY_DELAY = 10_000L;

        private long retryDelay = START_RETRY_DELAY;
        private long nextRunTimestamp;

        PartitionPurgeRetryTask copyFrom(PartitionPurgeTask inTask, long ticks) {
            super.copyFrom(inTask);
            retryDelay = START_RETRY_DELAY;
            nextRunTimestamp = ticks + retryDelay;
            return this;
        }

        boolean isDue(long now) {
            return nextRunTimestamp < now;
        }

        void updateNextRunTimestamp(long now) {
            retryDelay = Math.min(60_000_000L, retryDelay * 2L);
            nextRunTimestamp = now + retryDelay;
        }
    }
}
