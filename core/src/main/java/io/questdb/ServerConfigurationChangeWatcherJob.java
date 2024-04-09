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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Path;
import io.questdb.mp.SynchronizedJob;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

public class ServerConfigurationChangeWatcherJob extends SynchronizedJob implements Closeable {
    //private final long watcherAddress;
    //private final DynamicServerConfiguration serverConfiguration;
    private final static Log LOG = LogFactory.getLog(ServerConfigurationChangeWatcherJob.class);
    //private Properties properties;
    //private final java.nio.file.Path confPath;

    public ServerConfigurationChangeWatcherJob(DynamicServerConfiguration serverConfiguration) throws IOException {
    /*
        this.serverConfiguration = serverConfiguration;
        this.confPath = Paths.get(serverConfiguration.getConfRoot().toString(), Bootstrap.CONFIG_FILE);

        this.properties = new Properties();
        try (InputStream is = java.nio.file.Files.newInputStream(this.confPath)) {
            this.properties.load(is);
        }

        try (Path path = new Path()) {
            path.of(confPath.toString()).$();
            this.watcherAddress = InotifyDirWatcher.setup(path.ptr());
        }

     */
    }

    public void close() {
        /*
        InotifyDirWatcher.teardown(this.watcherAddress);

         */
    }

    @Override
    protected boolean runSerially() {
    /*
        if (InotifyDirWatcher.changed(this.watcherAddress)) {
            LOG.info().$("config file changed").$();
            Properties newProperties = new Properties();

            try (InputStream is = java.nio.file.Files.newInputStream(this.confPath)) {
                newProperties.load(is);
            }
            catch (IOException exc) {
                LOG.error().$("error loading properties").$();
                return false;
            }

            if (!newProperties.equals(this.properties)) {
                serverConfiguration.reload(newProperties);
                this.properties = newProperties;
                LOG.info().$("config successfully reloaded").$();
            } else {
                LOG.info().$("skipping config reload").$();
            }
        }
        return true;
     */
        return false;
    }
}