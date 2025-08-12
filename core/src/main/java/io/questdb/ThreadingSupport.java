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

package io.questdb;


import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadingSupport {
    private final static java.lang.ThreadLocal<ThreadState> threadState = new java.lang.ThreadLocal<>();
    private final static AtomicInteger threadLocalKeyCounter = new AtomicInteger(0);


    public static int getNextThreadLocalKey() {
        int nextKey = threadLocalKeyCounter.getAndIncrement();
        return nextKey;
    }

    public static void detachThreadLocals() {
        threadState.set(null);
    }

    public static Object getVirtualThreadLocal(int key) {
        ThreadState state = threadState.get();
        if (state == null || state.threadStatics.size() <= key) {
            return null;
        }
        return state.threadStatics.get(key);
    }

    public static void putVirtualThreadLocal(int key, Object value) {
        ThreadState state = threadState.get();
        if (state == null) {
            state = new ThreadState();
            threadState.set(state);
        }
        state.threadStatics.extendAndSet(key, value);
    }

    public static void useThreadLocalState(ThreadState container) {
        threadState.set(container);
    }

    public static class ThreadState {
        private final ObjList<Object> threadStatics = new ObjList<>(1024);
    }
}
