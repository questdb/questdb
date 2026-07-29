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
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;

final class LineTcpFiberWriterTask extends FiberTask {
    private static final Log LOG = LogFactory.getLog(LineTcpFiberWriterTask.class);
    private boolean isReusable = true;
    private final FiberRuntime runtime;
    private final LineTcpWriterJob writerJob;

    LineTcpFiberWriterTask(FiberRuntime runtime, LineTcpWriterJob writerJob) {
        this.runtime = runtime;
        this.writerJob = writerJob;
    }

    void prepareLaunch() {
        if (getScheduleState() != STATE_IDLE) {
            throw new IllegalStateException(
                    "ILP writer task is not idle [state=" + getScheduleState() + ']'
            );
        }
        isReusable = true;
    }

    @Override
    protected void onAbandoned() {
        isReusable = false;
    }

    @Override
    protected void onDone() {
        if (isReusable && runtime.state() == FiberRuntimeState.OPEN) {
            reopen();
        }
    }

    @Override
    protected void onError(Throwable th) {
        LOG.critical().$("ILP writer fiber failed [error=").$(th).I$();
    }

    @Override
    protected boolean runStep() {
        writerJob.runFiber();
        return true;
    }
}
