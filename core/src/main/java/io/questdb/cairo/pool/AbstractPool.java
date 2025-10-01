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

package io.questdb.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public abstract class AbstractPool implements Closeable {
    public static final long CLOSED = Unsafe.getFieldOffset(AbstractPool.class, "closed");
    public static final long UNALLOCATED = -1L;
    private static final int FALSE = 0;
    private static final int TRUE = 1;
    protected final MicrosecondClock clock;
    protected final FilesFacade ff;
    private final CairoConfiguration configuration;
    private final long inactiveTtlUs;
    @SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
    private volatile int closed = FALSE;
    private PoolListener eventListener;

    public AbstractPool(CairoConfiguration configuration, long inactiveTtlMillis) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.clock = configuration.getMicrosecondClock();
        this.inactiveTtlUs = inactiveTtlMillis * 1000;
    }

    @Override
    public void close() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, CLOSED, FALSE, TRUE)) {
            closePool();
        }
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public PoolListener getPoolListener() {
        return eventListener;
    }

    public boolean releaseAll() {
        return releaseAll(Long.MAX_VALUE);
    }

    public boolean releaseInactive() {
        return releaseAll(clock.getTicks() - inactiveTtlUs);
    }

    @TestOnly
    public boolean reopen() {
        return Unsafe.getUnsafe().compareAndSwapInt(this, CLOSED, TRUE, FALSE);
    }

    public void setPoolListener(PoolListener eventListener) {
        this.eventListener = eventListener;
    }

    protected void closePool() {
        releaseAll(Long.MAX_VALUE);
        notifyListener(Thread.currentThread().getId(), null, PoolListener.EV_POOL_CLOSED);
    }

    protected boolean isClosed() {
        return closed == TRUE;
    }

    protected void notifyListener(long thread, TableToken tableToken, short event) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(PoolListener.SRC_WRITER, thread, tableToken, event, (short) 0, (short) 0);
        }
    }

    protected abstract boolean releaseAll(long deadline);
}
