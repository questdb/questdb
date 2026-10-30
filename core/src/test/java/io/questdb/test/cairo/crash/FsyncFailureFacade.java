/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoException;
import io.questdb.test.std.TestFilesFacadeImpl;

/**
 * Once armed, fails one {@code fsync} or {@code fsyncAndClose} called from {@code caller}, or one synchronous
 * or asynchronous {@code msync} when {@code isMsync}, selected by {@code isAsyncMsync}. Fails the first after
 * {@code skipCount} such calls with a genuine data-sync failure, then behaves normally.
 */
public final class FsyncFailureFacade extends TestFilesFacadeImpl {
    public int failureCount;
    public boolean isArmed;
    private final String caller;
    private final boolean isAsyncMsync;
    private final boolean isMsync;
    private int skipCount;

    public FsyncFailureFacade(String caller, int skipCount, boolean isMsync) {
        this(caller, skipCount, isMsync, false);
    }

    public FsyncFailureFacade(String caller, int skipCount, boolean isMsync, boolean isAsyncMsync) {
        this.caller = caller;
        this.skipCount = skipCount;
        this.isMsync = isMsync;
        this.isAsyncMsync = isAsyncMsync;
    }

    /**
     * True when a method of the given name is on the current thread's stack.
     */
    public static boolean isCalledFrom(String methodName) {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (methodName.equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void fsync(long fd) {
        if (!isMsync && isFailureDue()) {
            throw CairoException.dataSyncFailure(this, 5, "fsync", "injected fsync failure");
        }
        super.fsync(fd);
    }

    @Override
    public void fsyncAndClose(long fd) {
        if (!isMsync && isFailureDue()) {
            // FilesFacadeImpl closes the fd before it throws. So must this fake, or the leak is its own.
            super.close(fd);
            throw CairoException.dataSyncFailure(this, 5, "fsyncAndClose", "injected fsync failure");
        }
        super.fsyncAndClose(fd);
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        if (isMsync && async == isAsyncMsync && isFailureDue()) {
            throw CairoException.dataSyncFailure(this, 5, "msync", "injected msync failure");
        }
        super.msync(addr, len, async);
    }

    private boolean isFailureDue() {
        if (!isArmed || !isCalledFrom(caller)) {
            return false;
        }
        if (skipCount > 0) {
            skipCount--;
            return false;
        }
        isArmed = false;
        failureCount++;
        return true;
    }
}
