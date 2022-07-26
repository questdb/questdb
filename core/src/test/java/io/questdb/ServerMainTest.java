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
import org.hamcrest.MatcherAssert;
import org.junit.*;
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

    @Test
    public void testExtractSiteExtractsDefaultLogConfFileIfItsMissing() throws IOException {
        Log log = LogFactory.getLog("server-main");
        File logConf = Paths.get(temp.getRoot().getPath(), "conf", LogFactory.DEFAULT_CONFIG_NAME).toFile();

        MatcherAssert.assertThat(logConf.exists(), is(false));

        ServerMain.extractSite(BuildInformationHolder.INSTANCE, temp.getRoot().getPath(), log);

        MatcherAssert.assertThat(logConf.exists(), is(true));
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

                ServerMain.reportCrashFiles(configuration, logger);

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
                        index2 = Chars.indexOf(str, index1 + 1, len, configuration.getOGCrashFilePrefix()+"2.log");
                        Assert.assertTrue(index2 > -1 && index2 > index1);
                        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat(configuration.getOGCrashFilePrefix()+"2.log").$()));
                        int index3 = Chars.indexOf(str, index2 + 1, len, configuration.getOGCrashFilePrefix()+"3");
                        Assert.assertEquals(-1, index3);
                        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat(configuration.getOGCrashFilePrefix()+"3").$()));
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

    private static void assertExists(File f) {
        MatcherAssert.assertThat(f.getPath(), f.exists(), is(true));
    }
}
