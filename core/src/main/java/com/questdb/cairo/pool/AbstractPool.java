/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.pool;

import com.questdb.std.FilesFacade;
import com.questdb.std.Unsafe;
import com.questdb.std.microtime.MicrosecondClock;
import com.questdb.std.str.ImmutableCharSequence;

import java.io.Closeable;

abstract class AbstractPool implements Closeable {
    public static final long CLOSED = Unsafe.getFieldOffset(AbstractPool.class, "closed");
    protected static final long UNALLOCATED = -1L;
    private static final int TRUE = 1;
    private static final int FALSE = 0;
    protected final CharSequence root;
    protected final FilesFacade ff;
    private final long inactiveTtlUs;
    private final MicrosecondClock clock;
    private PoolListener eventListener;
    @SuppressWarnings("FieldCanBeLocal")
    private volatile int closed = FALSE;

    public AbstractPool(FilesFacade ff, MicrosecondClock clock, CharSequence root, long inactiveTtlMs) {
        this.ff = ff;
        this.clock = clock;
        this.root = ImmutableCharSequence.of(root);
        this.inactiveTtlUs = inactiveTtlMs * 1000;
    }

    @Override
    public final void close() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, CLOSED, FALSE, TRUE)) {
            closePool();
        }
    }

    public PoolListener getPoolListener() {
        return eventListener;
    }

    public boolean releaseInactive() {
        return releaseAll(clock.getTicks() - inactiveTtlUs);
    }

    public void setPoolListner(PoolListener eventListener) {
        this.eventListener = eventListener;
    }

    protected void closePool() {
        releaseAll(Long.MAX_VALUE);
        notifyListener(Thread.currentThread().getId(), null, PoolListener.EV_POOL_CLOSED);
    }

    protected boolean isClosed() {
        return closed == TRUE;
    }

    protected void notifyListener(long thread, CharSequence name, short event) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(PoolListener.SRC_WRITER, thread, name, event, (short) 0, (short) 0);
        }
    }

    protected abstract boolean releaseAll(long deadline);
}
