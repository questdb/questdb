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

package io.questdb.test;

import io.questdb.BuildInformationHolder;
import io.questdb.ReloadingPropServerConfiguration;
import io.questdb.ServerConfigurationChangeWatcherJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Properties;

public class ServerConfigurationChangeWatcherJobTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(ReloadingPropServerConfigurationTest.class);
    @Test
    public void testConfigReload() throws Exception {

        temp.newFolder("conf");
        try (PrintWriter writer = new PrintWriter(temp.newFile("conf/mime.types"), Charset.defaultCharset())) {
            writer.println("");
        }

        ReloadingPropServerConfiguration config = new ReloadingPropServerConfiguration(
            temp.getRoot().getAbsolutePath(), new Properties(), null, ServerConfigurationChangeWatcherJobTest.LOG, new BuildInformationHolder()
        );




    }

}