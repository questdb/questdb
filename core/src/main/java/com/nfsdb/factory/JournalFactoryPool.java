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

package com.nfsdb.factory;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.logging.Log;
import com.nfsdb.logging.LogFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY"})
public class JournalFactoryPool implements Closeable {
    private static final Log LOG = LogFactory.getLog(JournalFactoryPool.class);

    private final ArrayBlockingQueue<JournalCachingFactory> pool;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public JournalFactoryPool(JournalConfiguration configuration, int capacity) throws InterruptedException {
        this.pool = new ArrayBlockingQueue<>(capacity, true);
        for (int i = 0; i < capacity; i++) {
            pool.put(new JournalCachingFactory(configuration, this));
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            for (JournalCachingFactory factory : pool) {
                factory.clearPool();
                factory.close();
            }
        }
    }

    public JournalCachingFactory get() throws InterruptedException, JournalException {
        if (running.get()) {
            JournalCachingFactory factory = pool.take();
            factory.refresh();
            return factory;
        } else {
            throw new InterruptedException("Journal pool has been closed");
        }
    }

    void release(final JournalCachingFactory factory) {
        factory.expireOpenFiles();
        try {
            pool.put(factory);
        } catch (InterruptedException e) {
            LOG.error().$("Cannot return factory to pool").$(e).$();
        }
    }
}