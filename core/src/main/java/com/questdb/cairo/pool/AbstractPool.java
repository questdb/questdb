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

import com.questdb.cairo.FilesFacade;
import com.questdb.std.str.ImmutableCharSequence;

import java.io.Closeable;

abstract class AbstractPool implements Closeable {
    protected static final long UNALLOCATED = -1L;
    protected final CharSequence root;
    protected final FilesFacade ff;
    private final long inactiveTtlMs;
    protected PoolListener eventListener;

    public AbstractPool(FilesFacade ff, CharSequence root, long inactiveTtlMs) {
        this.ff = ff;
        this.root = ImmutableCharSequence.of(root);
        this.inactiveTtlMs = inactiveTtlMs;
    }

    public PoolListener getPoolListener() {
        return eventListener;
    }

    public void setPoolListner(PoolListener eventListener) {
        this.eventListener = eventListener;
    }

    public boolean releaseInactive() {
        return releaseAll(System.currentTimeMillis() - inactiveTtlMs);
    }

    protected abstract boolean releaseAll(long deadline);
}
