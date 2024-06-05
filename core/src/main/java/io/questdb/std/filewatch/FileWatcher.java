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

package io.questdb.std.filewatch;

import io.questdb.FileEventCallback;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.DebouncingRunnable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileWatcher implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(FileWatcher.class);
    private static final Duration debouncePeriod = Duration.ofMillis(100);
    protected final DebouncingRunnable runnable;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final SOCountDownLatch haltedLatch = new SOCountDownLatch(1);
    private final Thread reloadThread;
    private final AtomicBoolean started = new AtomicBoolean();
    private final SOCountDownLatch startedLatch = new SOCountDownLatch(1);

    public FileWatcher(@NotNull FileEventCallback callback) {
        this.runnable = new DebouncingRunnable(callback::onFileEvent, debouncePeriod);

        reloadThread = new Thread(() -> {
            try {
                do {
                    if (closed.get()) {
                        return;
                    }
                    waitForChange();
                } while (true);
            } catch (Exception exc) {
                LOG.error().$(exc).$();
            } finally {
                haltedLatch.countDown();
                LOG.info().$("filewatcher poller thread closed").$();
            }
        });
    }

    @Override
    public void close() {
        halt();
    }

    public void halt() {
        if (closed.compareAndSet(false, true)) {
            if (started.compareAndSet(true, false)) {
                startedLatch.await();
                releaseWait();
                haltedLatch.await();
                _close();
            }
        }
    }

    public void start() {
        if (!closed.get() && started.compareAndSet(false, true)) {
            reloadThread.start();
            startedLatch.countDown();
        }
    }

    protected abstract void _close();

    protected abstract void releaseWait();

    protected abstract void waitForChange();
}
