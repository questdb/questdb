/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.factory;

import com.nfsdb.ex.JournalException;
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
    private final AtomicInteger allLength = new AtomicInteger();
    private final int capacity;

    @SuppressWarnings("unchecked")
    public JournalFactoryPool(JournalConfiguration configuration, int capacity) throws InterruptedException {
        this.configuration = configuration;
        this.capacity = capacity;
        this.pool = new ConcurrentLinkedDeque<>();
    }

    @Override
    public void close() {
        if (running.get()) {
            running.set(false);

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

    public JournalCachingFactory get() throws InterruptedException, JournalException {
        if (running.get()) {
            JournalCachingFactory factory = pool.poll();
            if (factory == null) {
                int index = allLength.incrementAndGet() - 1;
                if (index < capacity) {
                    factory = new JournalCachingFactory(configuration, this);
                }
            }
            else {
                factory.setInUse();
            }
            return factory;
        } else {
            throw new InterruptedException("Journal pool has been closed");
        }
    }

    void release(final JournalCachingFactory factory) {
        if (running.get()) {
            factory.expireOpenFiles();
            pool.addFirst(factory);
        } else {
            factory.clearPool();
            factory.close();
        }
    }
}