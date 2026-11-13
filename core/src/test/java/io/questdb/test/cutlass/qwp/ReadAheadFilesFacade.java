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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Files facade that picks how a backward QWP stream reads its frames ahead, whatever the
 * platform supports: through MADV_WILLNEED windows, which it still passes on to the OS
 * where the OS takes them, or by reading each frame at once. It counts the bytes the
 * stream asks the OS for. A cold facade reports every range out of memory, so the stream
 * asks for every window, as it does over data it has not read yet; otherwise the OS tells.
 */
public class ReadAheadFilesFacade extends FilesFacadeImpl {
    private final AtomicLong advisedBytes = new AtomicLong();
    private final boolean isCold;
    private final boolean isWillNeedSupported;
    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong residencyCheckCount = new AtomicLong();

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
        return super.madviseWillNeed(address, len);
    }

    public long requestCount() {
        return requestCount.get();
    }

    public long residencyCheckCount() {
        return residencyCheckCount.get();
    }

    @Override
    public String toString() {
        return (isWillNeedSupported ? "MADV_WILLNEED windows" : "whole-frame reads") + (isCold ? ", cold" : "");
    }
}
