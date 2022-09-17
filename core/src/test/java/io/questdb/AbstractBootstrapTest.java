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

import io.questdb.std.*;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public abstract class AbstractBootstrapTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;

    private static final File siteDir = new File(ServerMain.class.getResource("/io/questdb/site/").getFile());
    private static boolean publicZipStubCreated = false;


    @BeforeClass
    public static void setUpStatic() throws Exception {
        //fake public.zip if it's missing to avoid forcing use of build-web-console profile just to run tests 
        URL resource = ServerMain.class.getResource("/io/questdb/site/public.zip");
        if (resource == null) {
            File publicZip = new File(siteDir, "public.zip");
            try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(publicZip))) {
                ZipEntry entry = new ZipEntry("test.txt");
                zip.putNextEntry(entry);
                zip.write("test".getBytes());
                zip.closeEntry();
            }
            publicZipStubCreated = true;
        }
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
            TestUtils.createTestPath(root);
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }
    }

    @AfterClass
    public static void tearDownStatic() {
        if (publicZipStubCreated) {
            File publicZip = new File(siteDir, "public.zip");
            if (publicZip.exists()) {
                publicZip.delete();
            }
        }
        TestUtils.removeTestPath(root);
        temp.delete();
    }

    static void createDummyConfiguration() throws Exception {
        String config = root.toString() + Files.SEPARATOR + "conf";
        TestUtils.createTestPath(config);
        String file = config + Files.SEPARATOR + "server.conf";
        try (PrintWriter writer = new PrintWriter(file, "UTF-8")) {
            writer.println("");
        }
        file = config + Files.SEPARATOR + "mime.types";
        try (PrintWriter writer = new PrintWriter(file, "UTF-8")) {
            writer.println("");
        }
    }

    void assertFail(String message, String... args) throws IOException {
        try {
            Bootstrap.withArgs(args);
            Assert.fail();
        } catch (Bootstrap.BootstrapException thr) {
            TestUtils.assertContains(thr.getMessage(), message);
        }
    }
}
