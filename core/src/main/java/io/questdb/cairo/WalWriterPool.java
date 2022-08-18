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

package io.questdb.cairo;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.concurrent.locks.ReentrantLock;

public class WalWriterPool implements Closeable {
    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<WalWriter> cache = new ArrayDeque<>();
    private final int maxSize;
    private final String tableName;
    private final TableRegistry tableRegistry;
    private final CairoConfiguration configuration;

    public WalWriterPool(int maxSize, String tableName, TableRegistry tableRegistry, CairoConfiguration configuration) {
        this.maxSize = maxSize;
        this.tableName = tableName;
        this.tableRegistry = tableRegistry;
        this.configuration = configuration;
    }

    public WalWriter pop() {
        lock.lock();
        try {
            final WalWriter obj = cache.poll();
            return obj == null ? new WalWriter(tableName, tableRegistry, configuration, this) : obj;
        } finally {
            lock.unlock();
        }
    }

    public boolean push(WalWriter obj) {
        assert obj != null;
        lock.lock();
        try {
            if (cache.size() < maxSize) {
                obj.clear();
                cache.push(obj);
                return true;
            } else {
                obj.doClose(true);
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        while (cache.size() > 0) {
            WalWriter w = cache.pop();
            w.doClose(true);
        }
    }
}
