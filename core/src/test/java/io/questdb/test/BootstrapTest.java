/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.Bootstrap;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogFileWriter;
import io.questdb.log.LogLevel;
import io.questdb.log.LogWriterConfig;
import io.questdb.log.Logger;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class BootstrapTest extends AbstractBootstrapTest {

    @Test
    public void testBadArgs() {
        assertFail("Root directory name expected (-d <root-path>)");
        assertFail("Root directory name expected (-d <root-path>)", "-d");
        assertFail("Root directory name expected (-d <root-path>)", "does not exist");
        assertFail("Root directory does not exist: nope", "-d", "nope");
    }


    @Test
    public void testDirectoryWithSpaces() throws Exception {
        auxPath.of(root + "\\spaced path").$();
        java.nio.file.Files.createDirectories(java.nio.file.Path.of(auxPath.toString()));
        CairoConfiguration configuration = new DefaultCairoConfiguration(auxPath.toString());
        CairoEngine engine = new CairoEngine(configuration);
        engine.close();
    }

    @Test
    public void testExtractSite() throws Exception {
        createDummyConfiguration();
        auxPath.of(root).$();
        int pathLen = auxPath.size();
        Bootstrap bootstrap = new Bootstrap(getServerMainArgs());
        Assert.assertNotNull(bootstrap.getLog());
        Assert.assertNotNull(bootstrap.getConfiguration());
        Assert.assertNotNull(bootstrap.getConfiguration().getMetrics());
        bootstrap.extractSite();
        Assert.assertTrue(Files.exists(auxPath.trimTo(pathLen).concat("conf").concat(LogFactory.DEFAULT_CONFIG_NAME).$()));
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
    public void testProcessArgsNoArgs() {
        try {
            Bootstrap.processArgs();
            Assert.fail();
        } catch (Bootstrap.BootstrapException thr) {
            TestUtils.assertContains(thr.getMessage(), "Arguments expected, non provided");
        }
    }

    @Test
    public void testReportCrashFiles() throws IOException {
        final File x = temp.newFile();
        final String logFileName = x.getAbsolutePath();
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(temp.getRoot().getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.CRITICAL, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.bind();
            factory.startThread();
            Log logger = factory.create("x");

            // create crash files
            auxPath.of(temp.getRoot().getAbsolutePath()).$();
            int plen = auxPath.size();
            Files.touch(auxPath.concat(configuration.getOGCrashFilePrefix()).put(1).put(".log").$());
            Files.touch(auxPath.trimTo(plen).concat(configuration.getOGCrashFilePrefix()).put(2).put(".log").$());
            Files.mkdirs(auxPath.trimTo(plen).concat(configuration.getOGCrashFilePrefix()).put(3).slash(), configuration.getMkDirMode());

            Bootstrap.reportCrashFiles(configuration, logger);

            // wait until sequence is consumed and written to file
            while (((Logger) logger).getCriticalSequence().getBarrier().current() < 1) {
                Os.pause();
            }
        }

        // make sure we check disk contents after factory is closed
        try (Path path = new Path().of(logFileName)) {
            int bufSize = 4096;
            long buf = Unsafe.calloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            // we should read sub-4k bytes from the file
            long fd = TestFilesFacadeImpl.INSTANCE.openRO(path.$());
            Assert.assertTrue(fd > -1);
            try {
                while (true) {
                    int len = (int) Files.read(fd, buf, bufSize, 0);
                    if (len > 0) {
                        DirectUtf8StringZ str = new DirectUtf8StringZ().of(buf);
                        int index1 = Utf8s.indexOfAscii(str, 0, len, configuration.getArchivedCrashFilePrefix() + "0.log");
                        Assert.assertTrue(index1 > -1);
                        // make sure max files (1) limit is not exceeded
                        int index2 = Utf8s.indexOfAscii(str, index1 + 1, len, configuration.getArchivedCrashFilePrefix() + "1.log");
                        Assert.assertEquals(-1, index2);

                        // at this point we could have renamed file with either index '1' or '2'. This is random and
                        // depends on the order OS directory listing returns names.
                        String fileIndexThatRemains = "2.log";
                        index2 = Utf8s.indexOfAscii(str, index1 + 1, len, configuration.getOGCrashFilePrefix() + fileIndexThatRemains);
                        if (index2 == -1) {
                            // we could have renamed 2 and left 1 behind
                            fileIndexThatRemains = "1.log";
                            index2 = Utf8s.indexOfAscii(str, index1 + 1, len, configuration.getOGCrashFilePrefix() + fileIndexThatRemains);
                        }

                        Assert.assertTrue(index2 > -1 && index2 > index1);

                        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat(configuration.getOGCrashFilePrefix() + fileIndexThatRemains).$()));

                        int index3 = Utf8s.indexOfAscii(str, index2 + 1, len, configuration.getOGCrashFilePrefix() + "3");
                        Assert.assertEquals(-1, index3);
                        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat(configuration.getOGCrashFilePrefix() + "3").$()));
                        break;
                    } else {
                        Os.pause();
                    }
                }
            } finally {
                TestFilesFacadeImpl.INSTANCE.close(fd);
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
