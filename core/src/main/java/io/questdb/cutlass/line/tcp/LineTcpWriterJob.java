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

package io.questdb.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

import java.io.Closeable;

class LineTcpWriterJob implements Job, Closeable {
    private final static Log LOG = LogFactory.getLog(LineTcpWriterJob.class);
    private final int workerId;
    private final RingQueue<LineTcpMeasurementEvent> queue;
    private final Sequence sequence;
    private final Path path = new Path();
    private final ObjList<TableUpdateDetails> assignedTables = new ObjList<>();
    private final MillisecondClock millisecondClock;
    private final long commitIntervalDefault;
    private final LineTcpMeasurementScheduler scheduler;
    private long nextCommitTime;
    private final Metrics metrics;

    LineTcpWriterJob(
            int workerId,
            RingQueue<LineTcpMeasurementEvent> queue,
            Sequence sequence,
            MillisecondClock millisecondClock,
            long commitIntervalDefault,
            LineTcpMeasurementScheduler scheduler,
            Metrics metrics
    ) {
        this.workerId = workerId;
        this.queue = queue;
        this.sequence = sequence;
        this.millisecondClock = millisecondClock;
        this.commitIntervalDefault = commitIntervalDefault;
        this.nextCommitTime = millisecondClock.getTicks();
        this.scheduler = scheduler;
        this.metrics = metrics;
    }

    @Override
    public void close() {
        LOG.info().$("line protocol writer closing [threadId=").$(workerId).$(']').$();
        // Finish all jobs in the queue before stopping
        for (int n = 0; n < queue.getCycle(); n++) {
            if (!run(workerId)) {
                break;
            }
        }

        Misc.free(path);
        Misc.freeObjList(assignedTables);
        assignedTables.clear();
    }

    @Override
    public boolean run(int workerId) {
        assert this.workerId == workerId;
        boolean busy = drainQueue();
        // while ILP is hammering the database via multiple connections the writer
        // is likely to be very busy so commitTables() will run infrequently
        // commit should run regardless the busy flag but has to finish quickly
        // idea is to store the tables in a heap data structure being the tables most
        // desperately need a commit on the top
        if (!busy) {
            commitTables();
            tickWriters();
        }
        return busy;
    }

    private void commitTables() {
        long wallClockMillis = millisecondClock.getTicks();
        if (wallClockMillis > nextCommitTime) {
            long minTableNextCommitTime = Long.MAX_VALUE;
            for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
                // the heap based solution mentioned above will eliminate the minimum search
                // we could just process the min element of the heap until we hit the first commit
                // time greater than millis and that will be our nextCommitTime
                try {
                    long tableNextCommitTime = assignedTables.getQuick(n).commitIfIntervalElapsed(wallClockMillis);
                    // get current time again, commit is not instant and take quite some time.
                    wallClockMillis = millisecondClock.getTicks();
                    if (tableNextCommitTime < minTableNextCommitTime) {
                        // taking the earliest commit time
                        minTableNextCommitTime = tableNextCommitTime;
                    }
                } catch (Throwable ex) {
                    LOG.critical()
                            .$("commit failed [table=").$(assignedTables.getQuick(n).getTableNameUtf16())
                            .$(",ex=").$(ex)
                            .I$();
                    metrics.health().incrementUnhandledErrors();
                }
            }
            // if no tables, just use the default commit interval
            nextCommitTime = minTableNextCommitTime != Long.MAX_VALUE ? minTableNextCommitTime : wallClockMillis + commitIntervalDefault;
        }
    }

    private boolean drainQueue() {
        boolean busy = false;
        while (true) {
            long cursor;
            while ((cursor = sequence.next()) < 0) {
                if (cursor == -1) {
                    return busy;
                }
                Os.pause();
            }
            busy = true;
            final LineTcpMeasurementEvent event = queue.get(cursor);

            try {
                // we check the event's writer thread ID to avoid consuming
                // incomplete events

                final TableUpdateDetails tab = event.getTableUpdateDetails();
                boolean closeWriter = false;
                if (event.getWriterWorkerId() == workerId) {
                    try {
                        if (tab.isWriterInError()) {
                            closeWriter = true;
                        } else {
                            if (!tab.isAssignedToJob()) {
                                assignedTables.add(tab);
                                tab.setAssignedToJob(true);
                                nextCommitTime = millisecondClock.getTicks();
                                LOG.info()
                                        .$("assigned table to writer thread [tableName=").$(tab.getTableNameUtf16())
                                        .$(", threadId=").$(workerId)
                                        .I$();
                            }
                            event.append();
                        }
                    } catch (Throwable ex) {
                        tab.setWriterInError();
                        LOG.critical()
                                .$("closing writer because of error [table=").$(tab.getTableNameUtf16())
                                .$(",ex=").$(ex)
                                .I$();
                        metrics.health().incrementUnhandledErrors();
                        closeWriter = true;
                        event.createWriterReleaseEvent(tab, false);
                        // This is a critical error, so we treat it as an unhandled one.
                    }
                } else {
                    if (event.getWriterWorkerId() == LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER) {
                        closeWriter = true;
                    }
                }

                if (closeWriter && tab.getWriter() != null) {
                    scheduler.processWriterReleaseEvent(event, workerId);
                    assignedTables.remove(tab);
                    tab.setAssignedToJob(false);
                    nextCommitTime = millisecondClock.getTicks();
                }
            } catch (Throwable ex) {
                LOG.error().$("failed to process ILP event because of exception [ex=").$(ex).I$();
            }

            sequence.done(cursor);
        }
    }

    private void tickWriters() {
        for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
            assignedTables.getQuick(n).tick();
        }
    }
}
