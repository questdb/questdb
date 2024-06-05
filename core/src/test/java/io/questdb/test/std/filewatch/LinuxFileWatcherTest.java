/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.std.filewatch;

import io.questdb.FileEventCallback;
import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.filewatch.LinuxAccessorFacade;
import io.questdb.std.filewatch.LinuxAccessorFacadeImpl;
import io.questdb.std.filewatch.LinuxFileWatcher;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static io.questdb.PropertyKey.*;

public class LinuxFileWatcherTest extends AbstractTest {

    private Utf8Sequence configPath;
    private static final FileEventCallback DUMMY_CALLBACK = () -> {};

    @Before
    public void createDummyFile() throws Exception {
        final String confPath = root + Files.SEPARATOR + "conf";
        TestUtils.createTestPath(confPath);
        String file = confPath + Files.SEPARATOR + "server.conf";
        try (PrintWriter writer = new PrintWriter(file, StandardCharsets.UTF_8)) {
            writer.println(HTTP_ENABLED + "=true");
        }
        configPath = new Utf8String(file);
    }

    @Test
    public void testInotifyInitFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingLinuxAccessorFacade faultInjector = new FaultInjectingLinuxAccessorFacade(LinuxAccessorFacadeImpl.INSTANCE);
            faultInjector.injectFaultInotifyInit(0);
            assertFailure(faultInjector, "inotify_init");
        });
    }

    @Test
    public void testPipeFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingLinuxAccessorFacade faultInjector = new FaultInjectingLinuxAccessorFacade(LinuxAccessorFacadeImpl.INSTANCE);
            faultInjector.injectPipeFailure(0);
            assertFailure(faultInjector, "pipe error");
        });
    }

    private void assertFailure(FaultInjectingLinuxAccessorFacade faultInjector, String expectedError) {
        try {
            new LinuxFileWatcher(faultInjector, configPath, DUMMY_CALLBACK);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
    }

    private static class FaultInjectingLinuxAccessorFacade implements LinuxAccessorFacade {
        private final LinuxAccessorFacade delegate;
        private int inotifyInitFaultAt = -1;
        private int inotifyInitCounter = 0;
        private int pipeFaultAt = -1;
        private int pipeCounter = 0;

        private FaultInjectingLinuxAccessorFacade(LinuxAccessorFacade delegate) {
            this.delegate = delegate;
        }

        public void injectFaultInotifyInit(int invocationNo) {
            inotifyInitFaultAt = invocationNo;
        }

        public void injectPipeFailure(int invocationNo) {
            pipeFaultAt = invocationNo;
        }

        @Override
        public int inotifyAddWatch(int fd, long pathPtr, int flags) {
            return delegate.inotifyAddWatch(fd, pathPtr, flags);
        }

        @Override
        public int inotifyInit() {
            if (inotifyInitCounter++ == inotifyInitFaultAt) {
                return -1;
            }
            return delegate.inotifyInit();
        }

        @Override
        public short inotifyRmWatch(int fd, int wd) {
            return delegate.inotifyRmWatch(fd, wd);
        }

        @Override
        public long pipe() {
            if (pipeCounter++ == pipeFaultAt) {
                return -1;
            }
            return delegate.pipe();
        }

        @Override
        public int readEvent(int fd, long buf, int bufSize) {
            return delegate.readEvent(fd, buf, bufSize);
        }

        @Override
        public int readPipe(int fd) {
            return delegate.readPipe(fd);
        }

        @Override
        public int writePipe(int fd) {
            return delegate.writePipe(fd);
        }
    }
}
