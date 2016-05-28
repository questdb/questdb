/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.mp;

import com.questdb.ex.FatalError;
import com.questdb.misc.Unsafe;

public abstract class SynchronizedJob implements Job {
    private static final long LOCKED_OFFSET;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private volatile int locked = 0;

    @Override
    public boolean run() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, LOCKED_OFFSET, 0, 1)) {
            try {
                return runSerially();
            } finally {
                locked = 0;
            }
        }
        return false;
    }

    @Override
    public void setupThread() {
    }

    protected abstract boolean runSerially();

    static {
        try {
            LOCKED_OFFSET = Unsafe.getUnsafe().objectFieldOffset(SynchronizedJob.class.getDeclaredField("locked"));
        } catch (NoSuchFieldException e) {
            throw new FatalError(e);
        }
    }

}
