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

import io.questdb.std.Filewatcher;
import io.questdb.std.str.Path;
import io.questdb.mp.SynchronizedJob;
import java.io.Closeable;
import java.nio.file.Paths;

public class ServerConfigurationChangeWatcherJob extends SynchronizedJob implements Closeable {
    private final long watcherAddress;
    private final ReloadingPropServerConfiguration config;

    public ServerConfigurationChangeWatcherJob(ReloadingPropServerConfiguration config) {
        this.config = config;

        try (Path path = new Path()) {
            path.of(config.getCairoConfiguration().getConfRoot() + Bootstrap.CONFIG_FILE).$();
            this.watcherAddress = Filewatcher.setup(path.ptr());
        }
    }

    public void close() {
        Filewatcher.teardown(this.watcherAddress);
    }

    @Override
    protected boolean runSerially() {
        if (Filewatcher.changed(this.watcherAddress)) {
            this.config.reload();
        }
        return true;
    }
}