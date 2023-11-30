/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;

import java.util.concurrent.atomic.AtomicLong;

public class QueryRegistry {
    private final NanosecondClock clock;
    private final AtomicLong idSeq = new AtomicLong();
    private final ConcurrentLongHashMap<Entry> registry = new ConcurrentLongHashMap<>();
    private final ThreadLocal<WeakMutableObjectPool<Entry>> tlQueryPool;

    public QueryRegistry(CairoConfiguration configuration) {
        this.clock = configuration.getNanosecondClock();
        tlQueryPool = new ThreadLocal<>(() -> new WeakMutableObjectPool<>(Entry::new, configuration.getQueryRegistryPoolSize()));
    }

    // TODO we shouldn't copy text in case of sensitive queries
    public long register(CharSequence query) {
        final long qid = idSeq.getAndIncrement();
        final Entry e = tlQueryPool.get().pop();
        e.registeredAtNs = clock.getTicks();
        e.sink.put(query);
        registry.put(qid, e);
        return qid;
    }

    public void unregister(long qid) {
        final Entry e = registry.remove(qid);
        tlQueryPool.get().push(e);
    }

    private static class Entry implements Mutable {
        // TODO add cancelled flag, owner id
        final StringSink sink = new StringSink();
        long registeredAtNs;

        @Override
        public void clear() {
            sink.clear();
            registeredAtNs = 0;
        }
    }
}
