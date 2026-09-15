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

package io.questdb.mp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.FiberWakeSink;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Fixed WorkerPool idle registry. A ready bit is a single-use wake claim: either the Worker
 * unregisters it before leaving the idle path or a publisher claims it and owns the unpark.
 */
final class WorkerWakeController implements FiberWakeSink {
    private static final Log LOG = LogFactory.getLog(WorkerWakeController.class);
    private final AtomicInteger readyCount = new AtomicInteger();
    private final long[] readyWords;
    private final ObjList<Thread> targets;
    private final AtomicInteger wakeCursor = new AtomicInteger();
    private final int workerCount;
    private volatile boolean isActive = true;

    WorkerWakeController(int workerCount) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("wake controller requires at least one Worker");
        }
        this.workerCount = workerCount;
        this.readyWords = new long[(int) (((long) workerCount + Long.SIZE - 1) / Long.SIZE)];
        this.targets = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            targets.add(null);
        }
    }

    void deactivate() {
        isActive = false;
        claimAllReadyBits();
        for (int i = 0; i < workerCount; i++) {
            targets.setQuick(i, null);
        }
    }

    boolean isReady(int workerId) {
        if (!isValidWorkerId(workerId)) {
            return false;
        }
        final int wordIndex = workerId >>> 6;
        final long bit = 1L << (workerId & 63);
        return (Unsafe.arrayGetVolatile(readyWords, wordIndex) & bit) != 0;
    }

    boolean registerReady(int workerId) {
        if (!isActive || !isValidWorkerId(workerId)) {
            return false;
        }
        if (targets.getQuick(workerId) == null) {
            LOG.critical().$("Worker registered before publishing its wake target [value=").$(workerId).I$();
            assert false : "Worker registered before publishing its wake target";
            return false;
        }
        readyCount.incrementAndGet();
        final int wordIndex = workerId >>> 6;
        final long bit = 1L << (workerId & 63);
        while (true) {
            final long current = Unsafe.arrayGetVolatile(readyWords, wordIndex);
            if ((current & bit) != 0) {
                decrementReadyCount();
                LOG.critical().$("Worker registered an existing ready bit [value=").$(workerId).I$();
                assert false : "Worker registered an existing ready bit";
                return false;
            }
            if (Unsafe.cas(readyWords, wordIndex, current, current | bit)) {
                if (!isActive) {
                    unregisterReady(workerId);
                    return false;
                }
                return true;
            }
        }
    }

    void registerTarget(int workerId, Thread target) {
        if (!isValidWorkerId(workerId)) {
            throw new IllegalArgumentException("wake target Worker id is out of range [workerId="
                    + workerId + ", workerCount=" + workerCount + ']');
        }
        if (target == null) {
            throw new IllegalArgumentException("wake target must not be null");
        }
        if (targets.getQuick(workerId) != null) {
            throw new IllegalStateException("wake target is already registered [workerId=" + workerId + ']');
        }
        targets.setQuick(workerId, target);
    }

    void unregisterReady(int workerId) {
        if (!isValidWorkerId(workerId)) {
            return;
        }
        final int wordIndex = workerId >>> 6;
        final long bit = 1L << (workerId & 63);
        while (true) {
            final long current = Unsafe.arrayGetVolatile(readyWords, wordIndex);
            if ((current & bit) == 0) {
                return;
            }
            if (Unsafe.cas(readyWords, wordIndex, current, current & ~bit)) {
                decrementReadyCount();
                return;
            }
        }
    }

    @Override
    public void wakeAll() {
        if (isActive) {
            claimAllReadyBits();
        }
    }

    @Override
    public boolean wakeOne(int preferredWorkerId) {
        if (!isActive) {
            return false;
        }
        final int count = readyCount.get();
        if (count == 0) {
            return false;
        }
        if (count < 0) {
            LOG.critical().$("ready Worker count is negative [value=").$(preferredWorkerId).I$();
            assert false : "ready Worker count is negative";
        }
        int claimedWorkerId = tryClaimPreferred(preferredWorkerId);
        if (claimedWorkerId < 0) {
            claimedWorkerId = tryClaimFromCursor();
        }
        if (claimedWorkerId < 0) {
            return false;
        }
        decrementReadyCount();
        unpark(claimedWorkerId);
        return true;
    }

    @TestOnly
    int getReadyCount() {
        return readyCount.get();
    }

    @TestOnly
    void setWakeCursorForTesting(int wakeCursor) {
        this.wakeCursor.set(wakeCursor);
    }

    private static long rangeMask(int offset, int length) {
        if (length == Long.SIZE) {
            return -1L;
        }
        return (-1L >>> (Long.SIZE - length)) << offset;
    }

    private void claimAllReadyBits() {
        for (int wordIndex = 0, wordCount = readyWords.length; wordIndex < wordCount; wordIndex++) {
            long claimed;
            do {
                claimed = Unsafe.arrayGetVolatile(readyWords, wordIndex);
            } while (claimed != 0 && !Unsafe.cas(readyWords, wordIndex, claimed, 0));
            if (claimed == 0) {
                continue;
            }
            final int claimedCount = Long.bitCount(claimed);
            decrementReadyCount(claimedCount);
            while (claimed != 0) {
                final int bitIndex = Long.numberOfTrailingZeros(claimed);
                final int workerId = (wordIndex << 6) + bitIndex;
                if (workerId < workerCount) {
                    unpark(workerId);
                }
                claimed &= claimed - 1;
            }
        }
    }

    private void decrementReadyCount() {
        decrementReadyCount(1);
    }

    private void decrementReadyCount(int delta) {
        final int count = readyCount.addAndGet(-delta);
        if (count < 0) {
            LOG.critical().$("ready Worker count underflow [value=").$(count).I$();
            assert false : "ready Worker count underflow";
        }
    }

    private boolean isValidWorkerId(int workerId) {
        return workerId >= 0 && workerId < workerCount;
    }

    private int tryClaimFromCursor() {
        final int startWorkerId = Math.floorMod(wakeCursor.getAndIncrement(), workerCount);
        final int claimedWorkerId = tryClaimRange(startWorkerId, workerCount);
        if (claimedWorkerId >= 0 || startWorkerId == 0) {
            return claimedWorkerId;
        }
        return tryClaimRange(0, startWorkerId);
    }

    private int tryClaimPreferred(int workerId) {
        if (!isValidWorkerId(workerId)) {
            return -1;
        }
        final int wordIndex = workerId >>> 6;
        final long bit = 1L << (workerId & 63);
        final long current = Unsafe.arrayGetVolatile(readyWords, wordIndex);
        return (current & bit) != 0 && Unsafe.cas(readyWords, wordIndex, current, current & ~bit)
                ? workerId
                : -1;
    }

    private int tryClaimRange(int fromWorkerId, int toWorkerId) {
        int currentWorkerId = fromWorkerId;
        while (currentWorkerId < toWorkerId) {
            final int wordIndex = currentWorkerId >>> 6;
            final int bitOffset = currentWorkerId & 63;
            final int wordEnd = (int) Math.min(toWorkerId, ((long) wordIndex + 1) << 6);
            final int claimedWorkerId = tryClaimWord(
                    wordIndex,
                    rangeMask(bitOffset, wordEnd - currentWorkerId)
            );
            if (claimedWorkerId >= 0) {
                return claimedWorkerId;
            }
            currentWorkerId = wordEnd;
        }
        return -1;
    }

    private int tryClaimWord(int wordIndex, long mask) {
        for (int attempt = 0; attempt < Long.SIZE; attempt++) {
            final long current = Unsafe.arrayGetVolatile(readyWords, wordIndex);
            final long available = current & mask;
            if (available == 0) {
                return -1;
            }
            final long bit = Long.lowestOneBit(available);
            if (Unsafe.cas(readyWords, wordIndex, current, current & ~bit)) {
                return (wordIndex << 6) + Long.numberOfTrailingZeros(bit);
            }
        }
        return -1;
    }

    private void unpark(int workerId) {
        final Thread target = targets.getQuick(workerId);
        if (target == null) {
            LOG.critical().$("claimed ready Worker has no wake target [value=").$(workerId).I$();
            assert false : "claimed ready Worker has no wake target";
            return;
        }
        LockSupport.unpark(target);
    }
}
