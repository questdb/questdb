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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.CoreMatchers.is;


public class ServerMainTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    boolean publicZipStubCreated = false;

    @Before
    public void setUp() throws IOException {
        //fake public.zip if it's missing to avoid forcing use of build-web-console profile just to run tests 
        URL resource = ServerMain.class.getResource("/io/questdb/site/public.zip");
        if (resource == null) {
            File siteDir = new File(ServerMain.class.getResource("/io/questdb/site/").getFile());
            File publicZip = new File(siteDir, "public.zip");

            try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(publicZip))) {
                ZipEntry entry = new ZipEntry("test.txt");
                zip.putNextEntry(entry);
                zip.write("test".getBytes());
                zip.closeEntry();
            }

            publicZipStubCreated = true;
        }
    }

    @After
    public void tearDown() {
        if (publicZipStubCreated) {
            File siteDir = new File(ServerMain.class.getResource("/io/questdb/site/").getFile());
            File publicZip = new File(siteDir, "public.zip");

            if (publicZip.exists()) {
                publicZip.delete();
            }
        }
    }

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
