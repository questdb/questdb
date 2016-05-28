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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY"})
public class JournalFactoryPool implements Closeable {
    private static final Log LOG = LogFactory.getLog(JournalFactoryPool.class);
    private final ConcurrentLinkedDeque<JournalCachingFactory> pool;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final JournalConfiguration configuration;
    private final AtomicInteger openCount = new AtomicInteger();
    private final int capacity;

    @SuppressWarnings("unchecked")
    public JournalFactoryPool(JournalConfiguration configuration, int capacity) {
        this.configuration = configuration;
        this.capacity = capacity;
        this.pool = new ConcurrentLinkedDeque<>();
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            JournalCachingFactory factory;
            while ((factory = pool.poll()) != null) {
                try {
                    factory.clearPool();
                    factory.close();
                } catch (Throwable ex) {
                    LOG.info().$("Error closing JournalCachingFactory. Continuing.").$(ex).$();
                }
            }
        }
    }

    public JournalCachingFactory get() throws InterruptedException {
        if (running.get()) {
            JournalCachingFactory factory = pool.poll();
            if (factory == null) {
                openCount.incrementAndGet();
                factory = new JournalCachingFactory(configuration, this);
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

    void release(final JournalCachingFactory factory) {
        if (running.get() && openCount.get() < capacity) {
            factory.expireOpenFiles();
            pool.addFirst(factory);
            return;
        }
        openCount.decrementAndGet();
        factory.clearPool();
        factory.close();
    }
}