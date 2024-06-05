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
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileWatcher implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(FileWatcher.class);
    private static final Duration debouncePeriod = Duration.ofMillis(100);
    protected final DebouncingCallback callback;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final AtomicBoolean started = new AtomicBoolean();
    private final SOCountDownLatch latch = new SOCountDownLatch(2);
    private final Thread reloadThread;

    public FileWatcher(@NotNull FileEventCallback callback) {
        this.callback = new DebouncingCallback(callback, debouncePeriod.toNanos());
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
                latch.countDown();
                LOG.info().$("filewatcher poller thread closed").$();
            }
        });
    }

    @Override
    public void close() {
        // todo: close on non-started watcher should be a no-op
        callback.close();
        latch.await();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            callback.start();
            reloadThread.start();
        }
    }

    protected abstract void waitForChange();

    public class DebouncingCallback implements FileEventCallback, QuietCloseable {
        private final long debouncePeriodNanos;
        private final FileEventCallback delegate;
        private final Object mutex = new Object();
        private boolean closed = false;
        private long lastEventNanos = Long.MAX_VALUE;

        public DebouncingCallback(FileEventCallback delegate, long debouncePeriodNanos) {
            this.delegate = delegate;
            this.debouncePeriodNanos = debouncePeriodNanos;
        }

        @Override
        public void close() {
            synchronized (mutex) {
                closed = true;
                mutex.notifyAll();
            }
        }

        @Override
        public void onFileEvent() {
            synchronized (mutex) {
                lastEventNanos = System.nanoTime();
                mutex.notifyAll();
            }
        }

        private void start() {
            Thread thread = new Thread(() -> {
                synchronized (mutex) {
                    for (; ; ) {
                        if (closed) {
                            latch.countDown();
                            return;
                        }
                        long now = System.nanoTime();
                        // todo: check for overflow
                        long waitNanos = lastEventNanos - now + debouncePeriodNanos;
                        long waitMillis = waitNanos / 1_000_000;
                        try {
                            if (waitMillis > 0) {
                                mutex.wait(waitMillis);
                            } else {
                                delegate.onFileEvent();
                                lastEventNanos = Long.MAX_VALUE;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            LOG.critical().$("error while reloading server configuration").$(e).$();
                        }
                    }
                }
            });
            thread.setName("config hot-reload debouncing thread");
            thread.start();
        }
    }
}
