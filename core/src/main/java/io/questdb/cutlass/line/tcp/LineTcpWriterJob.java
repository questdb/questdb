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

package io.questdb.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

class LineTcpWriterJob implements Job, Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpWriterJob.class);
    private final ObjList<TableUpdateDetails> assignedTables;
    private final long commitInterval;
    private final Metrics metrics;
    private final MillisecondClock millisecondClock;
    private long nextCommitTime;
    private final RingQueue<LineTcpMeasurementEvent> queue;
    private final LineTcpMeasurementScheduler scheduler;
    private final Sequence sequence;
    private final int workerId;

    LineTcpWriterJob(
            int workerId,
            RingQueue<LineTcpMeasurementEvent> queue,
            Sequence sequence,
            MillisecondClock millisecondClock,
            long commitInterval,
            LineTcpMeasurementScheduler scheduler,
            Metrics metrics,
            ObjList<TableUpdateDetails> assignedTables
    ) {
        this.workerId = workerId;
        this.queue = queue;
        this.sequence = sequence;
        this.millisecondClock = millisecondClock;
        this.commitInterval = commitInterval;
        this.nextCommitTime = millisecondClock.getTicks();
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.assignedTables = assignedTables;
    }

    @Override
    public void close() {
        LOG.debug().$("line protocol writer closing [workerId=").$(workerId).I$();
        for (int n = 0; n < queue.getCycle(); n++) {
            if (!run(Job.TERMINATING_STATUS)) {
                break;
            }
        }
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        final boolean isBusy = drainQueue();
        if (!isBusy) {
            commitTables();
            tickWriters();
        }
        return isBusy;
    }

    private void commitTables() {
        long wallClockMillis = millisecondClock.getTicks();
        if (wallClockMillis > nextCommitTime) {
            long minTableNextCommitTime = Long.MAX_VALUE;
            for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
                final TableUpdateDetails tud = assignedTables.getQuick(n);
                try {
                    long tableNextCommitTime = tud.commitIfIntervalElapsed(wallClockMillis);
                    wallClockMillis = millisecondClock.getTicks();
                    if (tableNextCommitTime < minTableNextCommitTime) {
                        minTableNextCommitTime = tableNextCommitTime;
                    }
                } catch (Throwable ex) {
                    LOG.critical()
                            .$("commit failed [table=").$(tud.getTableToken())
                            .$(",ex=").$(ex)
                            .I$();
                    metrics.healthMetrics().incrementUnhandledErrors();
                }
            }
            nextCommitTime = minTableNextCommitTime != Long.MAX_VALUE
                    ? minTableNextCommitTime
                    : wallClockMillis + commitInterval;
        }
    }

    private boolean drainQueue() {
        final int drainBudget = queue.getCycle();
        boolean isBusy = false;
        for (int drained = 0; drained < drainBudget; drained++) {
            long cursor;
            while ((cursor = sequence.next()) < 0) {
                if (cursor == -1) {
                    return isBusy;
                }
                Os.pause();
            }
            isBusy = true;
            final LineTcpMeasurementEvent event = queue.get(cursor);

            try {
                final TableUpdateDetails tud = event.getTableUpdateDetails();
                boolean isCloseWriter = false;
                if (event.getWriterWorkerId() == workerId) {
                    try {
                        if (tud.isWriterInError() || tud.getWriter() == null) {
                            isCloseWriter = true;
                        } else {
                            if (!tud.isAssignedToJob()) {
                                assignedTables.add(tud);
                                tud.setAssignedToJob(true);
                                nextCommitTime = millisecondClock.getTicks();
                                LOG.info()
                                        .$("assigned table to writer thread [tableName=").$(tud.getTableToken())
                                        .$(", workerId=").$(workerId)
                                        .I$();
                            }
                            event.append();
                        }
                    } catch (Throwable ex) {
                        tud.setWriterInError();
                        LOG.critical()
                                .$("closing writer because of error [table=").$(tud.getTableToken())
                                .$(", ex=").$(ex)
                                .I$();
                        metrics.healthMetrics().incrementUnhandledErrors();
                        isCloseWriter = true;
                        event.createWriterReleaseEvent(tud, false);
                    }
                } else if (event.getWriterWorkerId() == LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER) {
                    isCloseWriter = true;
                }

                if (isCloseWriter && tud.getWriter() != null) {
                    scheduler.processWriterReleaseEvent(event, workerId);
                    assignedTables.remove(tud);
                    tud.setAssignedToJob(false);
                    nextCommitTime = millisecondClock.getTicks();
                }
            } catch (Throwable ex) {
                LOG.error().$("failed to process ILP event because of exception [ex=").$(ex).I$();
            } finally {
                sequence.done(cursor);
            }
        }
        return isBusy;
    }

    private void tickWriters() {
        for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
            assignedTables.getQuick(n).tick();
        }
    }
}
