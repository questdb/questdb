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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

final class WalApplyExecutorPool implements Closeable {
    private int createdCount;
    private final CairoEngine engine;
    private int freeCount;
    private ApplyWal2TableJob freeList;
    private boolean isClosed;
    private final int maxLiveCount;
    private final int sharedQueryWorkerCount;

    WalApplyExecutorPool(CairoEngine engine, int sharedQueryWorkerCount, int maxLiveCount) {
        if (maxLiveCount < 1) {
            throw new IllegalArgumentException("WAL apply executor limit must be positive");
        }
        this.engine = engine;
        this.maxLiveCount = maxLiveCount;
        this.sharedQueryWorkerCount = sharedQueryWorkerCount;
    }

    @Override
    public void close() {
        final Throwable initialFailure;
        ApplyWal2TableJob executor;
        synchronized (this) {
            if (isClosed) {
                return;
            }
            isClosed = true;
            initialFailure = freeCount == createdCount
                    ? null
                    : new IllegalStateException(
                    "WAL apply executor pool closed with leased executors [created="
                    + createdCount
                    + ", free=" + freeCount
                    + ']'
            );
            executor = freeList;
            freeList = null;
            createdCount -= freeCount;
            freeCount = 0;
        }
        Throwable failure = initialFailure;
        while (executor != null) {
            final ApplyWal2TableJob next = executor.nextFree;
            executor.isPooled = false;
            executor.nextFree = null;
            failure = Misc.freeBestEffort(failure, executor);
            executor = next;
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    @TestOnly
    public synchronized int getCreatedCount() {
        return createdCount;
    }

    @TestOnly
    public synchronized int getFreeCount() {
        return freeCount;
    }

    public void release(ApplyWal2TableJob executor) {
        boolean isFree = false;
        synchronized (this) {
            if (executor.isPooled || createdCount <= freeCount) {
                throw new IllegalStateException("WAL apply executor pool overflow");
            }
            if (isClosed) {
                createdCount--;
                isFree = true;
            } else {
                executor.isPooled = true;
                executor.nextFree = freeList;
                freeList = executor;
                freeCount++;
            }
        }
        if (isFree) {
            Misc.free(executor);
        }
    }

    public synchronized @Nullable ApplyWal2TableJob tryAcquire() {
        if (isClosed) {
            return null;
        }
        if (freeList != null) {
            final ApplyWal2TableJob executor = freeList;
            freeList = executor.nextFree;
            executor.isPooled = false;
            executor.nextFree = null;
            freeCount--;
            return executor;
        }
        if (createdCount >= maxLiveCount) {
            return null;
        }
        final ApplyWal2TableJob executor = new ApplyWal2TableJob(engine, sharedQueryWorkerCount);
        createdCount++;
        return executor;
    }
}
