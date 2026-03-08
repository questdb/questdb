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

package io.questdb.network;

import io.questdb.cairo.CairoException;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.std.Misc;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakMutableObjectPool;

import java.io.Closeable;

public class IOContextFactoryImpl<C extends IOContext<C>> implements IOContextFactory<C>, Closeable, EagerThreadSetup {

    private final ThreadLocal<WeakMutableObjectPool<C>> contextPool;
    private volatile boolean closed = false;

    public IOContextFactoryImpl(ObjectFactory<C> factory, int poolSize) {
        // todo: this is very slow, refactor
        this.contextPool = new ThreadLocal<>(() -> new WeakMutableObjectPool<>(factory, poolSize));
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void done(C context) {
        if (closed) {
            Misc.free(context);
        } else {
            contextPool.get().push(context);
        }
    }

    public void freeThreadLocal() {
        // helper call, it will free only thread-local instance and not others
        Misc.free(contextPool);
    }

    public C newInstance(long fd) {
        WeakMutableObjectPool<C> pool = contextPool.get();
        C context = pool.pop();
        try {
            return context.of(fd);
        } catch (CairoException e) {
            if (e.isCritical()) {
                context.close();
            } else {
                context.clear();
                pool.push(context);
            }
            throw e;
        } catch (Throwable t) {
            context.close();
            throw t;
        }
    }

    @Override
    public void setup() {
        contextPool.get();
    }
}
