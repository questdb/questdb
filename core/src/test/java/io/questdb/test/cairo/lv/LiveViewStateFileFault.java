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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link LiveViewBaseAmountOpenFault}, which fails the base's amount column file, and faults on the state file of
 * the view {@link #watchStateFile} names. Ahead of the next open of that file for writing it runs the action
 * {@link #runAtNextStateFileOpen} names; then it counts the open when the view is marked dropped by then, and
 * fails it, reporting a missing file, while {@link #failStateFileOpens} has failures left. It keeps the
 * descriptors the opens return until they are closed, fails an allocation that grows the file through one of them
 * while {@link #failStateFileGrowths} has failures left, and counts each close by whether the closing thread holds
 * the view's monitor.
 */
class LiveViewStateFileFault extends LiveViewBaseAmountOpenFault {
    private final AtomicBoolean isMissingStateFileErrnoPending = new AtomicBoolean();
    // The descriptors the opens of the watched state file returned and nothing closed yet.
    private final ConcurrentHashMap<Long, Boolean> openStateFileFds = new ConcurrentHashMap<>();
    private final AtomicInteger stateFileClosesHoldingTheMonitor = new AtomicInteger();
    private final AtomicInteger stateFileClosesWithoutTheMonitor = new AtomicInteger();
    private final AtomicInteger stateFileGrowthFailures = new AtomicInteger();
    private final AtomicReference<Runnable> stateFileOpenAction = new AtomicReference<>();
    private final AtomicInteger stateFileOpenFailures = new AtomicInteger();
    private final AtomicInteger stateFileOpensAfterDrop = new AtomicInteger();
    // The file name, with the view's directory and the separators ahead of it, of the watched state file.
    private volatile String watchedFileName;
    private volatile LiveViewInstance watchedInstance;

    @Override
    public boolean allocate(long fd, long size) {
        // TestFilesFacadeImpl routes the ranged allocation, which a mapped file's growth makes, through here.
        if (openStateFileFds.containsKey(fd) && stateFileGrowthFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            return false;
        }
        return super.allocate(fd, size);
    }

    @Override
    public boolean close(long fd) {
        // Ahead of the close, so no other open can take the descriptor's number while it is still listed.
        if (openStateFileFds.remove(fd) != null) {
            if (Thread.holdsLock(watchedInstance)) {
                stateFileClosesHoldingTheMonitor.incrementAndGet();
            } else {
                stateFileClosesWithoutTheMonitor.incrementAndGet();
            }
        }
        return super.close(fd);
    }

    @Override
    public int errno() {
        return isMissingStateFileErrnoPending.compareAndSet(true, false) ? CairoException.ERRNO_FILE_DOES_NOT_EXIST : super.errno();
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        final String fileName = watchedFileName;
        final boolean isWatched = fileName != null && Utf8s.endsWithAscii(name, fileName);
        if (isWatched) {
            final Runnable action = stateFileOpenAction.getAndSet(null);
            if (action != null) {
                action.run();
            }
            // After the action, which can wait for the view to be marked dropped: the open comes after it.
            if (watchedInstance.isDropped()) {
                stateFileOpensAfterDrop.incrementAndGet();
            }
            if (stateFileOpenFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                isMissingStateFileErrnoPending.set(true);
                return -1;
            }
        }
        final long fd = super.openRW(name, opts);
        if (isWatched && fd > -1) {
            openStateFileFds.put(fd, Boolean.TRUE);
        }
        return fd;
    }

    // Fails the next count allocations that grow the watched state file through a descriptor its opens returned. The
    // open of a file that has content maps its length and allocates nothing, so the first one comes with a write that
    // runs past the file's end.
    void failStateFileGrowths(int count) {
        stateFileGrowthFailures.set(count);
    }

    void failStateFileOpens(int count) {
        stateFileOpenFailures.set(count);
    }

    int getOpenStateFileFdCount() {
        return openStateFileFds.size();
    }

    int getStateFileClosesHoldingTheMonitor() {
        return stateFileClosesHoldingTheMonitor.get();
    }

    int getStateFileClosesWithoutTheMonitor() {
        return stateFileClosesWithoutTheMonitor.get();
    }

    int getStateFileOpensAfterDrop() {
        return stateFileOpensAfterDrop.get();
    }

    // Runs the action once, at the next open of the watched state file, ahead of the open.
    void runAtNextStateFileOpen(Runnable action) {
        stateFileOpenAction.set(action);
    }

    // Sets the instance before the name the open checks first.
    void watchStateFile(LiveViewInstance instance) {
        watchedInstance = instance;
        watchedFileName = Files.SEPARATOR + instance.getLiveViewToken().getDirName() + Files.SEPARATOR + LiveViewState.LIVE_VIEW_STATE_FILE_NAME;
    }
}
