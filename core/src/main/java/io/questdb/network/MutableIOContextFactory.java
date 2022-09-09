/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.mp.EagerThreadSetup;
import io.questdb.std.Misc;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakMutableObjectPool;

import java.io.Closeable;

public class MutableIOContextFactory<C extends MutableIOContext<C>>
        implements IOContextFactory<C>, Closeable, EagerThreadSetup {

    private final ThreadLocal<WeakMutableObjectPool<C>> contextPool;
    private volatile boolean closed = false;

    public MutableIOContextFactory(ObjectFactory<C> factory, int poolSize) {
        this.contextPool = new ThreadLocal<>(() -> new WeakMutableObjectPool<>(factory, poolSize));
    }

    @Override
    public void close() {
        closed = true;
        Misc.free(this.contextPool);
    }

    @Override
    public C newInstance(long fd, IODispatcher<C> dispatcher) {
        return contextPool.get().pop().of(fd, dispatcher);
    }

    @Override
    public void done(C context) {
        if (closed) {
            Misc.free(context);
        } else {
            context.of(-1, null);
            contextPool.get().push(context);
        }
    }

    @Override
    public void setup() {
        contextPool.get();
    }
}
