/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import org.hamcrest.MatcherAssert;

import static org.hamcrest.CoreMatchers.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;


public class ServerMainTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testExtractSiteExtractsDefaultLogConfFileIfItsMissing() throws IOException {
        Log log = LogFactory.getLog("server-main");
        File logConf = Paths.get(temp.getRoot().getPath(), "conf", LogFactory.DEFAULT_CONFIG_NAME).toFile();

        MatcherAssert.assertThat(logConf.exists(), is(false));

        ServerMain.extractSite(BuildInformationHolder.INSTANCE, temp.getRoot().getPath(), log);

        MatcherAssert.assertThat(logConf.exists(), is(true));
    }

    @Test
    public void testExtractSiteExtractsDefaultConfDirIfItsMissing() throws IOException {
        Log log = LogFactory.getLog("server-main");

        File conf = Paths.get(temp.getRoot().getPath(), "conf").toFile();
        File logConf = Paths.get(conf.getPath(), LogFactory.DEFAULT_CONFIG_NAME).toFile();
        File serverConf = Paths.get(conf.getPath(), "server.conf").toFile();
        File mimeTypes = Paths.get(conf.getPath(), "mime.types").toFile();
        //File dateFormats = Paths.get(conf.getPath(), "date.formats").toFile();

        ServerMain.extractSite(BuildInformationHolder.INSTANCE, temp.getRoot().getPath(), log);

        assertExists(logConf);
        assertExists(serverConf);
        assertExists(mimeTypes);
        //assertExists(dateFormats); date.formats is referenced in method but doesn't exist in SCM/jar
    }

    private static void assertExists(File f) {
        MatcherAssert.assertThat(f.getPath(), f.exists(), is(true));
    }
}
