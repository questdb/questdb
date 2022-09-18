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

import io.questdb.network.Net;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Properties;
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
    }

    @AfterClass
    public static void tearDownStatic() {
        if (publicZipStubCreated) {
            File publicZip = new File(siteDir, "public.zip");
            if (publicZip.exists()) {
                publicZip.delete();
            }
        }
    }

    @Before
    public void setUp() {
        try {
            root = temp.newFolder("QDB_DATA").getAbsolutePath();
            TestUtils.createTestPath(root);
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }
    }

    @After
    public void tearDown() {
        TestUtils.removeTestPath(root);
        temp.delete();
    }

    static final Properties PG_CONNECTION_PROPERTIES = new Properties();

    static {
        PG_CONNECTION_PROPERTIES.setProperty("user", "admin");
        PG_CONNECTION_PROPERTIES.setProperty("password", "quest");
        PG_CONNECTION_PROPERTIES.setProperty("sslmode", "disable");
        PG_CONNECTION_PROPERTIES.setProperty("binaryTransfer", "true");
    }

    final String PG_CONNECTION_URI = "jdbc:postgresql://127.0.0.1:8822/qdb";

    static void createDummyConfiguration() throws Exception {
        final String confPath = root.toString() + Files.SEPARATOR + "conf";
        TestUtils.createTestPath(confPath);
        String file = confPath + Files.SEPARATOR + "server.conf";
        try (PrintWriter writer = new PrintWriter(file, "UTF-8")) {

            // enable services
            writer.println("http.enabled=true");
            writer.println("http.min.enabled=true");
            writer.println("pg.enabled=true");
            writer.println("line.tcp.enabled=true");
            writer.println("line.udp.enabled=true");

            // disable services
            writer.println("http.query.cache.enabled=false");
            writer.println("pg.select.cache.enabled=false");
            writer.println("pg.insert.cache.enabled=false");
            writer.println("pg.update.cache.enabled=false");
            writer.println("cairo.wal.enabled.default=false");
            writer.println("metrics.enabled=false");
            writer.println("telemetry.enabled=false");

            // configure end points
            writer.println("http.bind.to=0.0.0.0:9010");
            writer.println("http.min.net.bind.to=0.0.0.0:9011");
            writer.println("pg.net.bind.to=0.0.0.0:8822");
            writer.println("line.tcp.net.bind.to=0.0.0.0:9019");
            writer.println("line.udp.bind.to=0.0.0.0:9019");
            writer.println("line.udp.receive.buffer.size=" + 1024 * 4);

            // configure worker pools
            writer.println("shared.worker.count=1");
            writer.println("http.worker.count=0");
            writer.println("http.min.worker.count=0");
            writer.println("http.connection.pool.initial.capacity=4");
            writer.println("pg.worker.count=0");
            writer.println("line.tcp.writer.worker.count=0");
            writer.println("line.tcp.io.worker.count=0");

        }
        file = confPath + Files.SEPARATOR + "mime.types";
        try (PrintWriter writer = new PrintWriter(file, "UTF-8")) {
            writer.println("");
        }
    }

    void assertFail(String message, String... args) {
        try {
            Bootstrap.withArgs(args);
            Assert.fail();
        } catch (Bootstrap.BootstrapException thr) {
            TestUtils.assertContains(thr.getMessage(), message);
        }
    }

    static void assertMemoryLeak(TestUtils.LeakProneCode runnable) throws Exception {
        Path.clearThreadLocals();
        long mem = Unsafe.getMemUsed();
        long[] memoryUsageByTag = new long[MemoryTag.SIZE];
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            memoryUsageByTag[i] = Unsafe.getMemUsedByTag(i);
        }

        Assert.assertTrue("Initial file unsafe mem should be >= 0", mem >= 0);
        long fileCount = Files.getOpenFileCount();
        Assert.assertTrue("Initial file count should be >= 0", fileCount >= 0);

        int addrInfoCount = Net.getAllocatedAddrInfoCount();
        Assert.assertTrue("Initial allocated addrinfo count should be >= 0", addrInfoCount >= 0);

        int sockAddrCount = Net.getAllocatedSockAddrCount();
        Assert.assertTrue("Initial allocated sockaddr count should be >= 0", sockAddrCount >= 0);

        runnable.run();
        Path.clearThreadLocals();
        if (fileCount != Files.getOpenFileCount()) {
            Assert.assertEquals("file descriptors " + Files.getOpenFdDebugInfo(), fileCount, Files.getOpenFileCount());
        }

        // Checks that the same tag used for allocation and freeing native memory
        long memAfter = Unsafe.getMemUsed();
        Assert.assertTrue(memAfter == 0);

        int addrInfoCountAfter = Net.getAllocatedAddrInfoCount();
        Assert.assertTrue(addrInfoCountAfter > -1);
        if (addrInfoCount != addrInfoCountAfter) {
            Assert.fail("AddrInfo allocation count before the test: " + addrInfoCount + ", after the test: " + addrInfoCountAfter);
        }

        int sockAddrCountAfter = Net.getAllocatedSockAddrCount();
        Assert.assertTrue(sockAddrCountAfter > -1);
        if (sockAddrCount != sockAddrCountAfter) {
            Assert.fail("SockAddr allocation count before the test: " + sockAddrCount + ", after the test: " + sockAddrCountAfter);
        }
    }
}
