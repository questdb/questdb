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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.FloatingDirectCharSink;
import io.questdb.std.str.Path;

import java.io.Closeable;

class LineTcpWriterJob implements Job, Closeable {
    private final static Log LOG = LogFactory.getLog(LineTcpWriterJob.class);
    private final int workerId;
    private final RingQueue<LineTcpMeasurementEvent> queue;
    private final Sequence sequence;
    private final Path path = new Path();
    private final FloatingDirectCharSink floatingCharSink = new FloatingDirectCharSink();
    private final ObjList<TableUpdateDetails> assignedTables = new ObjList<>();
    private final MillisecondClock millisecondClock;
    private final long commitIntervalDefault;
    private final LineTcpMeasurementScheduler scheduler;
    private long nextCommitTime;

    LineTcpWriterJob(
            int workerId,
            RingQueue<LineTcpMeasurementEvent> queue,
            Sequence sequence,
            MillisecondClock millisecondClock,
            long commitIntervalDefault,
            LineTcpMeasurementScheduler scheduler
    ) {
        this.workerId = workerId;
        this.queue = queue;
        this.sequence = sequence;
        this.millisecondClock = millisecondClock;
        this.commitIntervalDefault = commitIntervalDefault;
        this.nextCommitTime = millisecondClock.getTicks();
        this.scheduler = scheduler;
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
        // is likely to be very busy so checkIfTablesNeedCommit() will run infrequently
        // commit should run regardless the busy flag but has to finish quickly
        // idea is to store the tables in a heap data structure being the tables most
        // desperately need a commit on the top
        if (!busy) {
            checkIfTablesNeedCommit();
            tickWriters();
        }
        return busy;
    }

    private void checkIfTablesNeedCommit() {
        final long millis = millisecondClock.getTicks();
        if (millis > nextCommitTime) {
            final int sz = assignedTables.size();
            if (sz > 0) {
                for (int n = 0; n < sz; n++) {
                    long tableNextCommitTime = assignedTables.getQuick(n).checkIfTableNeedsCommit(millis);
                    if (tableNextCommitTime < nextCommitTime) {
                        // taking the earliest commit time
                        nextCommitTime = tableNextCommitTime;
                    }
                }
            } else {
                // no tables, just use the default commit interval
                nextCommitTime = millis + commitIntervalDefault;
            }
        }
    }

    private void tickWriters() {
        for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
            assignedTables.getQuick(n).tick();
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
            }
            busy = true;
            final LineTcpMeasurementEvent event = queue.get(cursor);
            boolean eventProcessed;

            try {
                // we check the event's writer thread ID to avoid consuming
                // incomplete events

                final TableUpdateDetails tab = event.getTableUpdateDetails();
                if (event.getWriterWorkerId() == workerId) {
                    try {
                        if (!tab.isAssignedToJob()) {
                            assignedTables.add(tab);
                            tab.setAssignedToJob(true);
                            nextCommitTime = millisecondClock.getTicks();
                            LOG.info()
                                    .$("assigned table to writer thread [tableName=").$(tab.getTableNameUtf16())
                                    .$(", threadId=").$(workerId)
                                    .I$();
                        }
                        event.append(floatingCharSink);
                        eventProcessed = true;
                    } catch (Throwable ex) {
                        LOG.error()
                                .$("closing writer because of error [table=").$(tab.getTableNameUtf16())
                                .$(",ex=").$(ex)
                                .I$();
                        event.createWriterReleaseEvent(tab, false);
                        eventProcessed = false;
                    }
                } else {
                    if (event.getWriterWorkerId() == LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER) {
                        eventProcessed = scheduler.processWriterReleaseEvent(event, workerId);
                        assignedTables.remove(tab);
                        tab.setAssignedToJob(false);
                        nextCommitTime = millisecondClock.getTicks();
                    } else {
                        eventProcessed = true;
                    }
                }
            } catch (Throwable ex) {
                eventProcessed = true;
                LOG.error().$("failed to process ILP event because of exception [ex=").$(ex).I$();
            }

            // by not releasing cursor we force the sequence to return us the same value over and over
            // until cursor value is released
            if (eventProcessed) {
                sequence.done(cursor);
            } else {
                return false;
            }
        }
    }
}
