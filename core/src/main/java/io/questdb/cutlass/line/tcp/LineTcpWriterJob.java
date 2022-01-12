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
    private final long maintenanceInterval;
    private final LineTcpMeasurementScheduler scheduler;
    private long lastMaintenanceMillis = 0;

    LineTcpWriterJob(
            int workerId,
            RingQueue<LineTcpMeasurementEvent> queue,
            Sequence sequence,
            MillisecondClock millisecondClock,
            long maintenanceInterval,
            LineTcpMeasurementScheduler scheduler
    ) {
        this.workerId = workerId;
        this.queue = queue;
        this.sequence = sequence;
        this.millisecondClock = millisecondClock;
        this.maintenanceInterval = maintenanceInterval;
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
        if (!busy && !doMaintenance()) {
            tickWriters();
        }
        return busy;
    }

    private boolean doMaintenance() {
        final long ticks = millisecondClock.getTicks();
        if (ticks - lastMaintenanceMillis < maintenanceInterval) {
            return false;
        }

        lastMaintenanceMillis = ticks;
        for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
            assignedTables.getQuick(n).handleWriterThreadMaintenance(ticks, maintenanceInterval);
        }
        return true;
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

                if (event.getWriterWorkerId() == workerId) {
                    final TableUpdateDetails tab = event.getTableUpdateDetails();
                    try {
                        if (!tab.isAssignedToJob()) {
                            assignedTables.add(tab);
                            tab.setAssignedToJob(true);
                            LOG.info()
                                    .$("assigned table to writer thread [tableName=").$(tab.getTableNameUtf16())
                                    .$(", threadId=").$(workerId)
                                    .I$();
                        }
                        event.append(floatingCharSink);
                        eventProcessed = true;
                    } catch (Throwable ex) {
                        LOG.error()
                                .$("closing writer for because of error [table=").$(tab.getTableNameUtf16())
                                .$(",ex=").$(ex)
                                .I$();
                        event.createWriterReleaseEvent(tab, false);
                        eventProcessed = false;
                    }
                } else {
                    if (event.getWriterWorkerId() == LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER) {
                        eventProcessed = scheduler.processWriterReleaseEvent(event, workerId);
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
