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

import io.questdb.cairo.O3OpenColumnJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Filewatcher;
import io.questdb.std.str.Path;
import io.questdb.mp.SynchronizedJob;
import java.io.Closeable;
import java.nio.file.Paths;

public class ServerConfigurationChangeWatcherJob extends SynchronizedJob implements Closeable {
    private final long watcherAddress;
    private final Bootstrap bootstrap;
    private ServerMain serverMain;
    private final static Log LOG = LogFactory.getLog(ServerConfigurationChangeWatcherJob.class);
    private final boolean addShutdownhook;
    private ReloadingPropServerConfiguration configuration;

    public ServerConfigurationChangeWatcherJob(Bootstrap bootstrap, ServerMain serverMain, boolean addShutdownHook) throws IllegalArgumentException {
        if (!(bootstrap.getConfiguration() instanceof ReloadingPropServerConfiguration)) {
            throw new IllegalArgumentException("bootstrap.getConfiguration() must return a ReloadingPropServerConfiguration");
        }

        this.bootstrap = bootstrap;
        this.configuration = (ReloadingPropServerConfiguration) bootstrap.getConfiguration();
        this.serverMain = serverMain;
        this.addShutdownhook = addShutdownHook;

        final String configFilePath = this.bootstrap.getConfiguration().getCairoConfiguration().getConfRoot() + Bootstrap.CONFIG_FILE;

        try (Path path = new Path()) {
            path.of(configFilePath).$();
            this.watcherAddress = Filewatcher.setup(path.ptr());
        }
    }

    public void close() {
        Filewatcher.teardown(this.watcherAddress);
    }

    @Override
    protected boolean runSerially() {
        if (serverMain == null) {
            return true;
        }

        if (Filewatcher.changed(this.watcherAddress)) {
            if (this.configuration.reload()) {
                LOG.info().$("config successfully reloaded").$();
                serverMain.close();
                serverMain = new ServerMain(this.bootstrap);
                serverMain.start(this.addShutdownhook);
            }
        }
        return true;
    }
}