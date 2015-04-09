/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb;

import com.nfsdb.utils.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TimerCache {
    private final long updateFrequency;
    private final ExecutorService service = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("nfsdb-timer-cache", true));
    private long millis = System.currentTimeMillis();

    public TimerCache() {
        this.updateFrequency = TimeUnit.SECONDS.toNanos(1);
    }

    public long getCachedMillis() {
        return millis;
    }

    public void halt() {
        service.shutdownNow();
    }

    public TimerCache start() {
        service.submit(new Runnable() {
            @Override
            public void run() {
                while (!service.isShutdown()) {
                    millis = System.currentTimeMillis();
                    LockSupport.parkNanos(updateFrequency);
                }
            }
        });
        return this;
    }
}
