/*+*****************************************************************************
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

package io.questdb.test.cairo.view;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.std.str.Path;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread that borrows a table's pooled reader, as a running query does, until {@link #close} is
 * called. The thread is the only borrower of the table, so its reader sits in the pool's first
 * slot, where the lock attempt of a DDL stops before it takes any slot of the pool.
 */
public class ReaderHoldingThread {
    private final CountDownLatch close = new CountDownLatch(1);
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final CountDownLatch holding = new CountDownLatch(1);
    private final Thread thread;

    private ReaderHoldingThread(CairoEngine engine, String tableName) {
        thread = new Thread(() -> {
            try (TableReader ignore = engine.getReader(tableName)) {
                holding.countDown();
                close.await();
            } catch (Throwable th) {
                error.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "reader-holder");
    }

    public static ReaderHoldingThread start(CairoEngine engine, String tableName) throws InterruptedException {
        final ReaderHoldingThread holder = new ReaderHoldingThread(engine, tableName);
        holder.thread.start();
        Assert.assertTrue("could not borrow the reader of " + tableName, holder.holding.await(30, TimeUnit.SECONDS));
        return holder;
    }

    /**
     * Returns the reader to the pool and waits for the thread. Fails with the error the thread met.
     */
    public void close() throws InterruptedException {
        close.countDown();
        thread.join();
        final Throwable th = error.get();
        if (th != null) {
            throw new AssertionError("reader holder failed", th);
        }
    }
}
