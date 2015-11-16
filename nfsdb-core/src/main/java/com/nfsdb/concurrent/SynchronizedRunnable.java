/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.concurrent;

import com.nfsdb.utils.Unsafe;

public abstract class SynchronizedRunnable implements Runnable {
    private static final long LOCKED_OFFSET;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private volatile int locked = 0;

    @Override
    public void run() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, LOCKED_OFFSET, 0, 1)) {
            try {
                _run();
            } finally {
                locked = 0;
            }
        }
    }

    protected abstract void _run();

    static {
        try {
            LOCKED_OFFSET = Unsafe.getUnsafe().objectFieldOffset(SynchronizedRunnable.class.getDeclaredField("locked"));
        } catch (NoSuchFieldException e) {
            throw new Error(e);
        }
    }
}
