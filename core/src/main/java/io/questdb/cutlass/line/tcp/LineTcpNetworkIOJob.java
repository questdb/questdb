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
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;

import static io.questdb.network.IODispatcher.DISCONNECT_REASON_UNKNOWN_OPERATION;

class LineTcpNetworkIOJob implements NetworkIOJob, Job {
    private final static Log LOG = LogFactory.getLog(LineTcpNetworkIOJob.class);
    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private final int workerId;
    private final CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8 = new CharSequenceObjHashMap<>();
    private final ObjList<SymbolCache> unusedSymbolCaches = new ObjList<>();
    private final MillisecondClock millisecondClock;
    private final long maintenanceInterval;
    private final LineTcpMeasurementScheduler scheduler;
    // Context blocked on LineTcpMeasurementScheduler queue
    private LineTcpConnectionContext busyContext = null;
    private final IORequestProcessor<LineTcpConnectionContext> onRequest = this::onRequest;
    private long maintenanceJobDeadline;

    LineTcpNetworkIOJob(
            LineTcpReceiverConfiguration configuration,
            LineTcpMeasurementScheduler scheduler,
            IODispatcher<LineTcpConnectionContext> dispatcher,
            int workerId
    ) {
        this.millisecondClock = configuration.getMillisecondClock();
        this.maintenanceInterval = configuration.getMaintenanceInterval();
        this.scheduler = scheduler;
        this.maintenanceJobDeadline = millisecondClock.getTicks() + maintenanceInterval;
        this.dispatcher = dispatcher;
        this.workerId = workerId;
    }

    @Override
    public void addTableUpdateDetails(String tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
        tableUpdateDetailsUtf8.put(tableNameUtf8, tableUpdateDetails);
        tableUpdateDetails.addReference(workerId);
    }

    @Override
    public void close() {
        Misc.freeObjList(unusedSymbolCaches);
    }

    @Override
    public TableUpdateDetails getLocalTableDetails(CharSequence tableName) {
        return tableUpdateDetailsUtf8.get(tableName);
    }

    @Override
    public ObjList<SymbolCache> getUnusedSymbolCaches() {
        return unusedSymbolCaches;
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public boolean run(int workerId) {
        assert this.workerId == workerId;
        boolean busy = false;
        if (busyContext != null) {
            if (handleIO(busyContext)) {
                return true;
            }
            LOG.debug().$("context is no longer waiting on a full queue [fd=").$(busyContext.getFd()).$(']').$();
            busyContext = null;
            busy = true;
        }

        if (dispatcher.processIOQueue(onRequest)) {
            busy = true;
        }

        final long millis = millisecondClock.getTicks();
        if (millis > maintenanceJobDeadline) {
            busy = scheduler.doMaintenance(tableUpdateDetailsUtf8, workerId, millis);
            if (!busy) {
                maintenanceJobDeadline = millis + maintenanceInterval;
            }
        }

        return busy;
    }

    private boolean handleIO(LineTcpConnectionContext context) {
        if (!context.invalid()) {
            switch (context.handleIO(this)) {
                case NEEDS_READ:
                    context.getDispatcher().registerChannel(context, IOOperation.READ);
                    return false;
                case NEEDS_WRITE:
                    context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                    return false;
                case QUEUE_FULL:
                    return true;
                case NEEDS_DISCONNECT:
                    context.getDispatcher().disconnect(context, DISCONNECT_REASON_UNKNOWN_OPERATION);
                    return false;
            }
        }
        return false;
    }

    private void onRequest(int operation, LineTcpConnectionContext context) {
        if (handleIO(context)) {
            busyContext = context;
            LOG.debug().$("context is waiting on a full queue [fd=").$(context.getFd()).$(']').$();
        }
    }

}
