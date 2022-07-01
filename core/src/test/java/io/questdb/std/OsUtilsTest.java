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

package io.questdb.std;

import io.questdb.DefaultServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.LPSZ;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class OsUtilsTest {
    static final Log LOG = LogFactory.getLog(OsUtils.class);

    @Test
    public void testFileLimitNonLinux() {
        Assume.assumeFalse(Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);

        long fileLimit = FilesFacadeImpl.INSTANCE.getOsFileLimit();
        Assert.assertEquals(-1L, fileLimit);

        System.out.println(fileLimit);
    }


    @Test
    public void testFileLimitLinux() {
        Assume.assumeTrue(Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);

        long fileLimit = FilesFacadeImpl.INSTANCE.getOsFileLimit();
        Assert.assertTrue(fileLimit > 0);

        System.out.println(fileLimit);
    }

    @Test
    public void testMapLimitNonLinux() {
        Assume.assumeFalse(Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);

        long mapCount = OsUtils.getMaxMapCount(LOG, FilesFacadeImpl.INSTANCE);
        Assert.assertEquals(-1L, mapCount);

        System.out.println(mapCount);
    }

    @Test
    public void testMapLimitLinux() {
        Assume.assumeTrue(Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);

        long mapCount = OsUtils.getMaxMapCount(LOG, FilesFacadeImpl.INSTANCE);
        Assert.assertTrue(mapCount > 0);

        System.out.println(mapCount);
    }

    @Test
    public void testMapLimitLinuxFails() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);

            FilesFacade ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    return -1L;
                }
            };

            long mapCount = OsUtils.getMaxMapCount(LOG, ff);
            Assert.assertEquals(-1, mapCount);
        });
    }

    @Test
    public void testServerMainFailsConfiguration() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);

            DefaultServerConfiguration configuraton = new DefaultServerConfiguration("");
            FilesFacade ff = configuraton.getCairoConfiguration().getFilesFacade();
            boolean success = ServerMain.checkOsProcessLimits(LOG, configuraton.getCairoConfiguration(), Integer.MAX_VALUE, Integer.MAX_VALUE);

            Assert.assertFalse(success);
            Assert.assertEquals(ff.getOsFileLimit(), ff.getOpenFileLimit());
            Assert.assertEquals(OsUtils.getMaxMapCount(LOG, ff), ff.getMapLimit());
        });
    }

    @Test
    public void testServerMainSetsConfigurations() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);

            DefaultServerConfiguration configuraton = new DefaultServerConfiguration("");

            boolean success = ServerMain.checkOsProcessLimits(LOG, configuraton.getCairoConfiguration(), 10, 10);
            Assert.assertTrue(success);

            FilesFacade ff = configuraton.getCairoConfiguration().getFilesFacade();
            Assert.assertEquals(ff.getOsFileLimit(), ff.getOpenFileLimit());
            Assert.assertEquals(OsUtils.getMaxMapCount(LOG, ff), ff.getMapLimit());
        });
    }
}
