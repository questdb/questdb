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

package io.questdb.test.recovery;

import io.questdb.RecoveryMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RecoveryMainTest extends AbstractTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testRejectsActiveLock() throws Exception {
        String installRoot = root + "/recovery-lock-test";
        java.nio.file.Path dbRootPath = Paths.get(installRoot, "db");
        Files.createDirectories(dbRootPath);

        try (Path lockPath = new Path().of(dbRootPath.toString()).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).put(".lock")) {
            long fd = FF.openRWNoCache(lockPath.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            Assert.assertEquals(0, FF.lock(fd));
            try {
                ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
                ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
                try (
                        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
                        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)
                ) {
                    int code = RecoveryMain.run(
                            new String[]{"-d", installRoot},
                            new ByteArrayInputStream("quit\n".getBytes(StandardCharsets.UTF_8)),
                            out,
                            err,
                            FF
                    );
                    Assert.assertNotEquals(0, code);
                    Assert.assertTrue(errBytes.toString(StandardCharsets.UTF_8).contains("database appears to be running"));
                }
            } finally {
                FF.close(fd);
            }
        }
    }

    @Test
    public void testResolvesRelativeCairoRootFromServerConf() throws Exception {
        String installRoot = root + "/recovery-conf-test";
        java.nio.file.Path confDir = Paths.get(installRoot, "conf");
        java.nio.file.Path dbRootPath = Paths.get(installRoot, "customdb");
        Files.createDirectories(confDir);
        Files.createDirectories(dbRootPath);
        Files.writeString(confDir.resolve("server.conf"), "cairo.root=customdb\n");

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        try (
                PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
                PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)
        ) {
            int code = RecoveryMain.run(
                    new String[]{"-d", installRoot},
                    new ByteArrayInputStream("quit\n".getBytes(StandardCharsets.UTF_8)),
                    out,
                    err,
                    FF
            );
            Assert.assertEquals(0, code);
            Assert.assertTrue(outBytes.toString(StandardCharsets.UTF_8).contains("dbRoot=" + dbRootPath));
            Assert.assertEquals("", errBytes.toString(StandardCharsets.UTF_8));
        }
    }
}

