/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;

import java.io.Closeable;
import java.io.IOException;

public class ReloadingServerMain implements Closeable {
    ServerMain server;
    final WorkerPoolManager workerPoolManager;
    FreeOnExit freeOnExit = new FreeOnExit();

    final Bootstrap bootstrap;


    public ReloadingServerMain(String... args) {
        this(new Bootstrap(args), true);
    }
    public ReloadingServerMain(final Bootstrap bootstrap, final boolean addShutdownHook) throws IllegalArgumentException {
        this.bootstrap = bootstrap;
        this.server = new ServerMain(bootstrap);
        this.workerPoolManager = new WorkerPoolManager(bootstrap.getConfiguration(), bootstrap.getMetrics()) {
            @Override
            protected void configureSharedPool(WorkerPool sharedPool) {
                ServerConfigurationChangeWatcherJob watcherJob = new ServerConfigurationChangeWatcherJob(bootstrap, server, addShutdownHook);
                sharedPool.assign(watcherJob);
                freeOnExit.register(watcherJob);
            }
        };

    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            server.close();
        }
        freeOnExit.close();
    }

    public void start() {
        this.workerPoolManager.start(this.bootstrap.getLog());
        this.server.start();
    }

    public static void main(String[] args) {
        try {
            new ReloadingServerMain(args).start();
        } catch (Throwable thr) {
            thr.printStackTrace();
            LogFactory.closeInstance();
            System.exit(55);
        }
    }
}
