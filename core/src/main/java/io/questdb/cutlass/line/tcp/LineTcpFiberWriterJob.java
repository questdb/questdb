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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

final class LineTcpFiberWriterJob implements Job, Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpFiberWriterJob.class);
    private boolean isFailureLogged;
    private final FiberRuntime runtime;
    private final LineTcpFiberWriterTask task;
    private final LineTcpWriterJob writerJob;

    LineTcpFiberWriterJob(FiberRuntime runtime, LineTcpWriterJob writerJob) {
        this.runtime = runtime;
        this.writerJob = writerJob;
        this.task = new LineTcpFiberWriterTask(runtime, writerJob);
    }

    @Override
    public void close() {
        writerJob.close();
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        if (runtime.state() != FiberRuntimeState.OPEN) {
            return runBlocking(workerContext);
        }

        final int taskState = task.getScheduleState();
        if (taskState != FiberTask.STATE_IDLE) {
            return false;
        }
        if (!writerJob.hasWork()) {
            return false;
        }

        task.prepareLaunch();
        final LaunchResult result = runtime.launch(task, task.getIncarnation());
        if (result == LaunchResult.LAUNCHED) {
            isFailureLogged = false;
            return true;
        }
        if (result != LaunchResult.SATURATED
                && result != LaunchResult.ALREADY_OWNED
                && result != LaunchResult.QUIESCING
                && !isFailureLogged) {
            isFailureLogged = true;
            LOG.critical().$("could not launch ILP writer fiber [result=").$(result).I$();
        }
        return false;
    }

    private boolean runBlocking(WorkerContext workerContext) {
        final int taskState = task.getScheduleState();
        if (taskState != FiberTask.STATE_IDLE
                && taskState != FiberTask.STATE_DONE
                && taskState != FiberTask.STATE_CANCELLED) {
            return false;
        }
        return writerJob.run(workerContext);
    }
}
