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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cutlass.AcceptGatedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactoryImpl;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;


public class LineTcpReceiver implements Closeable {
    private final AtomicBoolean acceptOpen;
    // one per worker group, each with its own listener on the receiver's port
    private final ObjList<IODispatcher<LineTcpConnectionContext>> dispatchers = new ObjList<>();
    private LineTcpMeasurementScheduler scheduler;

    public LineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool sharedPoolNetwork,
            WorkerPool sharedPoolWrite
    ) {
        this(configuration, engine, sharedPoolNetwork, sharedPoolWrite, new AtomicBoolean(true));
    }

    public LineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool sharedPoolNetwork,
            WorkerPool sharedPoolWrite,
            AtomicBoolean acceptOpen
    ) {
        this.acceptOpen = acceptOpen;
        try {
            this.scheduler = null;
            ObjectFactory<LineTcpConnectionContext> factory;
            factory = () -> new LineTcpConnectionContext(configuration, scheduler);

            IOContextFactoryImpl<LineTcpConnectionContext> contextFactory = new IOContextFactoryImpl<>(
                    factory,
                    configuration.getConnectionPoolInitialCapacity()
            );
            final int groupCount = IODispatchers.getGroupCount(configuration, sharedPoolNetwork);
            IODispatchers.createGroups(configuration, contextFactory, groupCount, dispatchers);
            // AcceptGatedJob is stateless and the receiver frees the dispatchers, so one instance per
            // group, assigned to each of the group's workers, runs like assign(Job) on every worker
            final ObjList<AcceptGatedJob> dispatcherJobs = new ObjList<>(groupCount);
            for (int g = 0; g < groupCount; g++) {
                dispatcherJobs.add(new AcceptGatedJob(dispatchers.getQuick(g), acceptOpen));
            }
            final int workerCount = sharedPoolNetwork.getWorkerCount();
            for (int i = 0; i < workerCount; i++) {
                sharedPoolNetwork.assign(i, dispatcherJobs.getQuick(groupCount == 1 ? 0 : sharedPoolNetwork.getGroup(i)));
            }
            this.scheduler = new LineTcpMeasurementScheduler(configuration, engine, sharedPoolNetwork, dispatchers, sharedPoolWrite);

            for (int i = 0; i < workerCount; i++) {
                // line tcp context factory has thread local pools
                // therefore we need each thread to clean their thread locals individually
                sharedPoolNetwork.assignThreadLocalCleaner(i, contextFactory::freeThreadLocal);
            }
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeBestEffort(null, scheduler);
        for (int g = 0, n = dispatchers.size(); g < n; g++) {
            failure = Misc.freeBestEffort(failure, dispatchers.getQuick(g));
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The connections that group {@code group}'s dispatcher holds.
     */
    @TestOnly
    public int getGroupConnectionCount(int group) {
        return dispatchers.getQuick(group).getConnectionCount();
    }

    @TestOnly
    public int getGroupCount() {
        return dispatchers.size();
    }

}
