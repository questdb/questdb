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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.log.*;
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class BootstrapTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static CharSequence root;

    private static final File siteDir = new File(Server.class.getResource("/io/questdb/site/").getFile());
    private static boolean publicZipStubCreated = false;


    @BeforeClass
    public static void setUpStatic() throws Exception {
        //fake public.zip if it's missing to avoid forcing use of build-web-console profile just to run tests 
        URL resource = Server.class.getResource("/io/questdb/site/public.zip");
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

    @Test
    public void testBadArgs() throws IOException {
        assertFail("Root directory name expected (-d <root-path>)");
        assertFail("Root directory name expected (-d <root-path>)", "-d");
        assertFail("Root directory name expected (-d <root-path>)", "does not exist");
    }

    @Test
    public void testServerConfFileDoesNotExist() throws IOException {
        String configFolder = root.toString() + Files.SEPARATOR + "conf";
        TestUtils.createTestPath(configFolder);
        File f = new File(configFolder);
        Assert.assertTrue(f.exists());
        Assert.assertTrue(f.isDirectory());
        assertFail("java.nio.file.NoSuchFileException:", "-d", root.toString());
    }

    @Test
    public void testExtractSite() throws Exception {
        createDummyConfiguration();
        try (Path path = new Path().of(root)) {
            int plen = path.length();
            Assert.assertFalse(Files.exists(path.concat("conf").concat(LogFactory.DEFAULT_CONFIG_NAME).$()));
            Bootstrap bootstrap = Bootstrap.withArgs("-d", root.toString());
            Assert.assertNotNull(bootstrap.getLog());
            Assert.assertNotNull(bootstrap.getConfig());
            Assert.assertNotNull(bootstrap.getMetrics());
            bootstrap.extractSite();
            Assert.assertTrue(Files.exists(path.trimTo(plen).concat("public").concat("version.txt").$()));
            Assert.assertTrue(Files.exists(path.trimTo(plen).concat("conf").concat(LogFactory.DEFAULT_CONFIG_NAME).$()));
        }
    }

    @Test
    public void testReportCrashFiles() throws IOException {
        final File x = temp.newFile();
        final String logFileName = x.getAbsolutePath();
        final CairoConfiguration configuration = new DefaultCairoConfiguration(temp.getRoot().getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.CRITICAL, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.bind();
            factory.startThread();
            try {
                Log logger = factory.create("x");

                // create crash files
                try (Path path = new Path().of(temp.getRoot().getAbsolutePath())) {
                    int plen = path.length();
                    Files.touch(path.trimTo(plen).concat(configuration.getOGCrashFilePrefix()).put(1).put(".log").$());
                    Files.touch(path.trimTo(plen).concat(configuration.getOGCrashFilePrefix()).put(2).put(".log").$());
                    Files.mkdirs(path.trimTo(plen).concat(configuration.getOGCrashFilePrefix()).put(3).slash$(), configuration.getMkDirMode());
                }

                Bootstrap.reportCrashFiles(configuration, logger);

                // wait until sequence is consumed and written to file
                while (logger.getCriticalSequence().getBarrier().current() < 1) {
                    Os.pause();
                }
            } finally {
                factory.haltThread();
            }
        }

        // make sure we check disk contents after factory is closed
        try (Path path = new Path().of(logFileName).$()) {
            int bufSize = 4096;
            long buf = Unsafe.calloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            // we should read sub-4k bytes from the file
            long fd = Files.openRO(path);
            Assert.assertTrue(fd > -1);
            try {
                while (true) {
                    int len = (int) Files.read(fd, buf, bufSize, 0);
                    if (len > 0) {
                        NativeLPSZ str = new NativeLPSZ().of(buf);
                        int index1 = Chars.indexOf(str, 0, len, configuration.getArchivedCrashFilePrefix() + "0.log");
                        Assert.assertTrue(index1 > -1);
                        // make sure max files (1) limit is not exceeded
                        int index2 = Chars.indexOf(str, index1 + 1, len, configuration.getArchivedCrashFilePrefix() + "1.log");
                        Assert.assertEquals(-1, index2);
                        index2 = Chars.indexOf(str, index1 + 1, len, configuration.getOGCrashFilePrefix() + "2.log");
                        Assert.assertTrue(index2 > -1 && index2 > index1);
                        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat(configuration.getOGCrashFilePrefix() + "2.log").$()));
                        int index3 = Chars.indexOf(str, index2 + 1, len, configuration.getOGCrashFilePrefix() + "3");
                        Assert.assertEquals(-1, index3);
                        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat(configuration.getOGCrashFilePrefix() + "3").$()));
                        break;
                    } else {
                        Os.pause();
                    }
                }
            } finally {
                Files.close(fd);
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testProcessArgsNoArgs() {
        try {
            Bootstrap.processArgs();
            Assert.fail();
        } catch (Bootstrap.BootstrapException thr) {
            TestUtils.assertContains(thr.getMessage(), "Arguments expected, non provided");
        }
    }

    @Test
    public void testProcessArgs() {
        CharSequenceObjHashMap<String> optHash = Bootstrap.processArgs("-d", "folder", "-n", "-f");
        Assert.assertEquals("folder", optHash.get("-d"));
        Assert.assertEquals("", optHash.get("-n"));
        Assert.assertEquals("", optHash.get("-f"));
        Assert.assertNull(optHash.get("-a"));
        Assert.assertNull(optHash.get("a"));
    }

    @Test
    public void testProcessArgsMissingKey() {
        CharSequenceObjHashMap<String> optHash = Bootstrap.processArgs("d", "folder", "-f", "-t", "n", "m");
        Assert.assertNull(optHash.get("d"));
        Assert.assertNull(optHash.get("-d"));
        Assert.assertEquals("d", optHash.get("$0"));
        Assert.assertEquals("folder", optHash.get("$1"));
        Assert.assertEquals("", optHash.get("-f"));
        Assert.assertEquals("n", optHash.get("-t"));
        Assert.assertEquals("m", optHash.get("$5"));
        Assert.assertNull(optHash.get("-a"));
        Assert.assertNull(optHash.get("a"));
    }

    @Test
    public void testServerMain() throws Exception {
        createDummyConfiguration();
        try (Server server = new Server("-d", root.toString())) {
            server.start();
        }
    }

    private static void createDummyConfiguration() throws Exception {
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

    private void assertFail(String message, String... args) throws IOException {
        try {
            Bootstrap.withArgs(args);
            Assert.fail();
        } catch (Bootstrap.BootstrapException thr) {
            TestUtils.assertContains(thr.getMessage(), message);
        }
    }
}
