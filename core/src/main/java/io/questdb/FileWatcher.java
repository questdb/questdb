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

package io.questdb;

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
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final CharSequence filePath;
    protected final DebouncingRunnable runnable;
    protected final AtomicBoolean started = new AtomicBoolean();
    private final SOCountDownLatch latch = new SOCountDownLatch(1);
    private final Thread reloadThread;

    public FileWatcher(
            CharSequence filePath,
            @NotNull FileEventCallback callback

    ) {
        this.runnable = new DebouncingRunnable(callback::onFileEvent, debouncePeriod);

        this.filePath = filePath;
        if (this.filePath == null || this.filePath.isEmpty()) {
            throw new IllegalArgumentException("filePath is null or empty");
        }

        reloadThread = new Thread(() -> {
            try {
                do {
                    if (closed.get()) {
                        return;
                    }
                    this.waitForChange();
                } while (true);
            } catch (FileWatcherNativeException exc) {
                LOG.error().$(exc).$();
            } finally {
                latch.countDown();
                LOG.info().$("filewatcher thread closed").$();
            }
        });
    }

    @Override
    public void close() {
        latch.await();
    }

    public void watch() {
        if (started.compareAndSet(false, true)) {
            reloadThread.start();
        }
    }

    protected abstract void waitForChange() throws FileWatcherNativeException;
}
