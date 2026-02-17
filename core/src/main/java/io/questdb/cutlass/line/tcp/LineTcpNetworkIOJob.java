/*******************************************************************************
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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.Misc;
import io.questdb.std.Pool;
import io.questdb.std.Utf8StringObjHashMap;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;

import static io.questdb.network.IODispatcher.DISCONNECT_REASON_RETRY_FAILED;
import static io.questdb.network.IODispatcher.DISCONNECT_REASON_UNKNOWN_OPERATION;

class LineTcpNetworkIOJob implements NetworkIOJob {
    private final static Log LOG = LogFactory.getLog(LineTcpNetworkIOJob.class);
    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private final long maintenanceInterval;
    private final MillisecondClock millisecondClock;
    private final LineTcpMeasurementScheduler scheduler;
    private final Utf8StringObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8 = new Utf8StringObjHashMap<>();
    private final WeakClosableObjectPool<SymbolCache> unusedSymbolCaches;
    private final int workerId;
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
        try {
            this.millisecondClock = configuration.getMillisecondClock();
            this.maintenanceInterval = configuration.getMaintenanceInterval();
            this.scheduler = scheduler;
            this.maintenanceJobDeadline = millisecondClock.getTicks() + maintenanceInterval;
            this.dispatcher = dispatcher;
            this.workerId = workerId;
            this.unusedSymbolCaches = new WeakClosableObjectPool<>(() -> new SymbolCache(configuration), 10, true);
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void addTableUpdateDetails(Utf8String tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
        tableUpdateDetailsUtf8.put(tableNameUtf8, tableUpdateDetails);
        tableUpdateDetails.addReference(workerId);
    }

    @Override
    public void close() {
        if (busyContext != null) {
            dispatcher.disconnect(busyContext, DISCONNECT_REASON_RETRY_FAILED);
            busyContext = null;
        }
        Misc.free(unusedSymbolCaches);
    }

    @Override
    public TableUpdateDetails getLocalTableDetails(DirectUtf8Sequence tableNameUtf8) {
        return tableUpdateDetailsUtf8.get(tableNameUtf8);
    }

    @Override
    public Pool<SymbolCache> getSymbolCachePool() {
        return unusedSymbolCaches;
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public void releaseWalTableDetails() {
        scheduler.releaseWalTableDetails(tableUpdateDetailsUtf8);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        assert this.workerId == workerId;
        boolean busy = false;
        if (busyContext != null) {
            if (handleIO(busyContext, dispatcher)) {
                // queue is still full
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

    private boolean handleIO(LineTcpConnectionContext context, IODispatcher<LineTcpConnectionContext> dispatcher) {
        if (!context.invalid()) {
            switch (context.handleIO(this)) {
                case NEEDS_READ:
                    dispatcher.registerChannel(context, IOOperation.READ);
                    return false;
                case NEEDS_WRITE:
                    dispatcher.registerChannel(context, IOOperation.WRITE);
                    return false;
                case QUEUE_FULL:
                    return true;
                case NEEDS_DISCONNECT:
                    dispatcher.disconnect(context, DISCONNECT_REASON_UNKNOWN_OPERATION);
                    return false;
            }
        }
        return false;
    }

    private boolean onRequest(int operation, LineTcpConnectionContext context, IODispatcher<LineTcpConnectionContext> dispatcher) {
        if (operation == IOOperation.HEARTBEAT) {
            context.doMaintenance(millisecondClock.getTicks());
            dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
            return false;
        }
        if (handleIO(context, dispatcher)) {
            busyContext = context;
            LOG.debug().$("context is waiting on a full queue [fd=").$(context.getFd()).$(']').$();
            return false;
        }
        return true;
    }
}
