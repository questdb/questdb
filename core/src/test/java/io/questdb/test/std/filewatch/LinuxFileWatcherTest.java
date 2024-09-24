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
import io.questdb.network.EpollFacade;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.filewatch.LinuxAccessorFacade;
import io.questdb.std.filewatch.LinuxAccessorFacadeImpl;
import io.questdb.std.filewatch.LinuxFileWatcher;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;

import static io.questdb.PropertyKey.HTTP_ENABLED;

public class LinuxFileWatcherTest extends AbstractTest {

    private static final FileEventCallback DUMMY_CALLBACK = () -> {
    };
    private Utf8Sequence configPath;

    @Before
    public void createDummyFile() throws Exception {
        final String confPath = root + Files.SEPARATOR + "conf";
        TestUtils.createTestPath(confPath);
        String file = confPath + Files.SEPARATOR + "server.conf";
        try (PrintWriter writer = new PrintWriter(file)) {
            writer.println(HTTP_ENABLED + "=true");
        }
        configPath = new Utf8String(file);
    }

    @Before
    public void ensureLinux() {
        Assume.assumeTrue(Os.isLinux());
    }

    @Test
    public void testCloseAfterEpollWaitFailureDoesNotDeadlock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingEpollFacade faultInjector = new FaultInjectingEpollFacade(EpollFacadeImpl.INSTANCE);
            faultInjector.epollWaitFailure(0);
            try (LinuxFileWatcher watcher = new LinuxFileWatcher(LinuxAccessorFacadeImpl.INSTANCE, faultInjector, configPath, DUMMY_CALLBACK)) {
                watcher.start();
            }
        });
    }

    @Test
    public void testCloseAfterReadEventFailureDoesNotDeadlock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingEpollFacade faultInjector = new FaultInjectingEpollFacade(EpollFacadeImpl.INSTANCE);
            faultInjector.epollWaitFailure(0);
            try (LinuxFileWatcher watcher = new LinuxFileWatcher(LinuxAccessorFacadeImpl.INSTANCE, faultInjector, configPath, DUMMY_CALLBACK)) {
                watcher.start();
            }
        });
    }

    @Test
    public void testEpollCtlFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingEpollFacade faultInjector = new FaultInjectingEpollFacade(EpollFacadeImpl.INSTANCE);
            faultInjector.injectEpollCtlFailure(0);
            assertFailure(LinuxAccessorFacadeImpl.INSTANCE, faultInjector, "epoll_ctl error");
        });

        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingEpollFacade faultInjector = new FaultInjectingEpollFacade(EpollFacadeImpl.INSTANCE);
            faultInjector.injectEpollCtlFailure(1);
            assertFailure(LinuxAccessorFacadeImpl.INSTANCE, faultInjector, "epoll_ctl error");
        });
    }

    @Test
    public void testInotifyInitFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingLinuxAccessorFacade faultInjector = new FaultInjectingLinuxAccessorFacade(LinuxAccessorFacadeImpl.INSTANCE);
            faultInjector.injectFaultInotifyInit(0);
            assertFailure(faultInjector, EpollFacadeImpl.INSTANCE, "inotify_init");
        });
    }

    @Test
    public void testPipeFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FaultInjectingLinuxAccessorFacade faultInjector = new FaultInjectingLinuxAccessorFacade(LinuxAccessorFacadeImpl.INSTANCE);
            faultInjector.injectPipeFailure(0);
            assertFailure(faultInjector, EpollFacadeImpl.INSTANCE, "pipe error");
        });
    }

    private void assertFailure(LinuxAccessorFacade accessorFacade, EpollFacade epollFacade, String expectedError) {
        try {
            new LinuxFileWatcher(accessorFacade, epollFacade, configPath, DUMMY_CALLBACK);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
    }

    private static class FaultInjectingEpollFacade implements EpollFacade {
        private final EpollFacade delegate;
        private int epollCtlCounter = 0;
        private int epollCtlFaultAt = -1;
        private int epollWaitCounter = 0;
        private int epollWaitFaultAt = -1;

        private FaultInjectingEpollFacade(EpollFacade delegate) {
            this.delegate = delegate;
        }

        @Override
        public int epollCreate() {
            return delegate.epollCreate();
        }

        @Override
        public int epollCtl(long epfd, int op, long fd, long eventPtr) {
            if (epollCtlCounter++ == epollCtlFaultAt) {
                return -1;
            }
            return delegate.epollCtl(epfd, op, fd, eventPtr);
        }

        @Override
        public int epollWait(long epfd, long eventPtr, int eventCount, int timeout) {
            if (epollWaitCounter++ == epollWaitFaultAt) {
                return -1;
            }
            return delegate.epollWait(epfd, eventPtr, eventCount, timeout);
        }

        public void epollWaitFailure(int invocationNo) {
            this.epollWaitFaultAt = invocationNo;
        }

        @Override
        public int errno() {
            return delegate.errno();
        }

        @Override
        public int eventFd() {
            return delegate.eventFd();
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return delegate.getNetworkFacade();
        }

        public void injectEpollCtlFailure(int invocationNo) {
            this.epollCtlFaultAt = invocationNo;
        }

        @Override
        public long readEventFd(long fd) {
            return delegate.readEventFd(fd);
        }

        @Override
        public int writeEventFd(long fd) {
            return delegate.writeEventFd(fd);
        }
    }


    private static class FaultInjectingLinuxAccessorFacade implements LinuxAccessorFacade {
        private final LinuxAccessorFacade delegate;
        private int inotifyInitCounter = 0;
        private int inotifyInitFaultAt = -1;
        private int pipeCounter = 0;
        private int pipeFaultAt = -1;

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
        public int inotifyAddWatch(long fd, long pathPtr, int flags) {
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
        public short inotifyRmWatch(long fd, int wd) {
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
        public int readEvent(long fd, long buf, int bufSize) {
            return delegate.readEvent(fd, buf, bufSize);
        }

        @Override
        public int readPipe(long fd) {
            return delegate.readPipe(fd);
        }

        @Override
        public int writePipe(long fd) {
            return delegate.writePipe(fd);
        }
    }
}
