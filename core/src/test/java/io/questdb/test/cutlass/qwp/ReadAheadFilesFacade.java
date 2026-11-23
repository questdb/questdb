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

package io.questdb.test.cutlass.qwp;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Files facade that picks how a backward QWP stream reads its frames ahead, whatever the
 * platform supports: through MADV_WILLNEED windows, which it still passes on to the OS
 * where the OS takes them, or by reading the windows itself. It counts the bytes the
 * stream asks the OS for, and the bytes the stream reads itself. A cold facade reports every
 * range out of memory, so the stream asks for every window, as it does over data it has not
 * read yet; otherwise the OS tells. A test can make the next range the stream reads ahead
 * take a while, as slow storage makes it.
 */
public class ReadAheadFilesFacade extends FilesFacadeImpl {
    private final AtomicLong advisedBytes = new AtomicLong();
    private final boolean isCold;
    private final boolean isWillNeedSupported;
    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong residencyCheckCount = new AtomicLong();
    // how long the next range the stream reads ahead takes, in millis
    private final AtomicLong stallMillis = new AtomicLong();
    private final AtomicLong touchedBytes = new AtomicLong();

    public ReadAheadFilesFacade(boolean isWillNeedSupported) {
        this(isWillNeedSupported, false);
    }

    public ReadAheadFilesFacade(boolean isWillNeedSupported, boolean isCold) {
        this.isWillNeedSupported = isWillNeedSupported;
        this.isCold = isCold;
    }

    public long advisedBytes() {
        return advisedBytes.get();
    }

    @Override
    public boolean isMadviseWillNeedSupported() {
        return isWillNeedSupported;
    }

    @Override
    public boolean isPageCacheResident(long address, long len) {
        residencyCheckCount.incrementAndGet();
        return !isCold && super.isPageCacheResident(address, len);
    }

    @Override
    public int madviseWillNeed(long address, long len) {
        advisedBytes.addAndGet(len);
        requestCount.incrementAndGet();
        stall();
        return super.madviseWillNeed(address, len);
    }

    public long requestCount() {
        return requestCount.get();
    }

    public long residencyCheckCount() {
        return residencyCheckCount.get();
    }

    /**
     * Makes the next range the stream reads ahead, whether it asks the OS for it or reads it
     * itself, take at least {@code millis}.
     */
    public void stallNextRange(long millis) {
        stallMillis.set(millis);
    }

    @Override
    public String toString() {
        return (isWillNeedSupported ? "MADV_WILLNEED windows" : "windows the stream reads itself") + (isCold ? ", cold" : "");
    }

    @Override
    public long touchPages(long address, long len) {
        touchedBytes.addAndGet(len);
        stall();
        return super.touchPages(address, len);
    }

    public long touchedBytes() {
        return touchedBytes.get();
    }

    private void stall() {
        final long millis = stallMillis.getAndSet(0);
        if (millis > 0) {
            Os.sleep(millis);
        }
    }
}
