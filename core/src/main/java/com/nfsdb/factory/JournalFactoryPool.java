/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.factory;

import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
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