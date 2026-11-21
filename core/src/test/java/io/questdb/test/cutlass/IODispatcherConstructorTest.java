/*+*****************************************************************************
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

package io.questdb.test.cutlass;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.EpollFacade;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.IODispatcherLinux;
import io.questdb.network.IODispatcherOsx;
import io.questdb.network.IODispatcherWindows;
import io.questdb.network.KqueueFacade;
import io.questdb.network.SelectFacade;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class IODispatcherConstructorTest {

    @Test
    public void testLinuxConstructorFailureClosesListener() throws Exception {
        assertSubclassConstructorRollback(new TestConfiguration() {
            @Override
            public EpollFacade getEpollFacade() {
                throw new InjectedFailure();
            }
        }, IODispatcherLinux::new);
    }

    @Test
    public void testOsxConstructorFailureClosesListener() throws Exception {
        assertSubclassConstructorRollback(new TestConfiguration() {
            @Override
            public KqueueFacade getKqueueFacade() {
                throw new InjectedFailure();
            }
        }, IODispatcherOsx::new);
    }

    @Test
    public void testWindowsConstructorFailureClosesListener() throws Exception {
        assertSubclassConstructorRollback(new TestConfiguration() {
            @Override
            public SelectFacade getSelectFacade() {
                throw new InjectedFailure();
            }
        }, IODispatcherWindows::new);
    }

    // The facade getter throws before the platform poller exists, so each subclass runs on any
    // host, while super() has already bound the listening socket the rollback has to close.
    private static void assertSubclassConstructorRollback(
            IODispatcherConfiguration configuration,
            DispatcherFactory dispatcherFactory
    ) throws Exception {
        assertMemoryLeak(() -> {
            try {
                dispatcherFactory.newInstance(configuration, _ -> null).close();
                Assert.fail("expected InjectedFailure");
            } catch (InjectedFailure ignore) {
            }
        });
    }

    @FunctionalInterface
    private interface DispatcherFactory {
        IODispatcher<HttpConnectionContext> newInstance(
                IODispatcherConfiguration configuration,
                IOContextFactory<HttpConnectionContext> ioContextFactory
        );
    }

    private static class InjectedFailure extends RuntimeException {
        InjectedFailure() {
            super("injected configuration failure", null, false, false);
        }
    }

    private static class TestConfiguration extends DefaultIODispatcherConfiguration {
        @Override
        public int getBindPort() {
            return 0;
        }
    }
}
