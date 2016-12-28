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

package com.questdb.factory;

import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.ObjList;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ReaderFactoryPool implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReaderFactoryPool.class);
    private final static Object NULL = new Object();
    private final ConcurrentLinkedDeque<CachingReaderFactory> pool;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final JournalConfiguration configuration;
    private final AtomicInteger openCount = new AtomicInteger();
    private final int capacity;
    private final ConcurrentMap<String, Object> stopList = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public ReaderFactoryPool(JournalConfiguration configuration, int capacity) {
        this.configuration = configuration;
        this.capacity = capacity;
        this.pool = new ConcurrentLinkedDeque<>();
    }

    public void blockName(String name) {
        stopList.putIfAbsent(name, NULL);
        CachingReaderFactory factory;
        ObjList<CachingReaderFactory> list = new ObjList<>();
        while ((factory = pool.poll()) != null) {
            list.add(factory);
            factory.closeJournal(name);
        }

        for (int i = 0, n = list.size(); i < n; i++) {
            pool.push(list.getQuick(i));
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            CachingReaderFactory factory;
            while ((factory = pool.poll()) != null) {
                try {
                    factory.clearPool();
                    factory.close();
                } catch (Throwable ex) {
                    LOG.info().$("Error closing CachingReaderFactory. Continuing.").$(ex).$();
                }
            }
        }
    }

    public CachingReaderFactory get() throws InterruptedException {
        if (running.get()) {
            CachingReaderFactory factory = pool.poll();
            if (factory == null) {
                openCount.incrementAndGet();
                factory = new CachingReaderFactory(configuration, this);
            } else {
                factory.setInUse();
            }
            return factory;
        } else {
            throw new InterruptedException("Journal pool has been closed");
        }
    }

    public int getAvailableCount() {
        return pool.size();
    }

    public int getCapacity() {
        return capacity;
    }

    public int getOpenCount() {
        return openCount.get();
    }

    public boolean isBlocked(String name) {
        return stopList.containsKey(name);
    }

    public void unblockName(String name) {
        stopList.remove(name);
    }

    void release(final CachingReaderFactory factory) {
        if (running.get() && openCount.get() <= capacity) {
            factory.expireOpenFiles();
            pool.push(factory);
            return;
        }
        openCount.decrementAndGet();
        factory.clearPool();
        factory.close();
    }
}