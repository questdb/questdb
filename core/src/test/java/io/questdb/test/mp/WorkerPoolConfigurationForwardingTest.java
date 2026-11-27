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

package io.questdb.test.mp;

import io.questdb.Metrics;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfigurationWrapper;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGConfigurationWrapper;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolConfigurationWrapper;
import io.questdb.mp.continuation.FiberDispatchController;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Checks that the configuration wrappers a worker pool's configuration can arrive in forward
 * every {@link WorkerPoolConfiguration} method to their delegate. A method left to the interface
 * default silently replaces the delegate's value: a missing group forward, for example, leaves a
 * pool configured for worker groups running as one group.
 * <p>
 * This test leaves out {@code HttpMinServerConfigurationWrapper} and
 * {@code ServerMain.MinHttpPoolConfiguration} on purpose. The min HTTP server's pool never splits
 * into groups and never hosts fibers, so its configuration forwards only the methods that pool uses.
 */
public class WorkerPoolConfigurationForwardingTest extends AbstractTest {
    private static final FiberDispatchController DISPATCH_CONTROLLER = runtime -> null;
    private static final int[][] GROUP_CPUS = {{0, 1}, {2, 3}};

    @Test
    public void testAssertionAcceptsMethodsInheritedFromSuperclass() {
        TestUtils.assertOverridesWorkerPoolConfiguration(InheritingWrapper.class);
    }

    @Test
    public void testAssertionReportsMethodsLeftToDefaults() {
        try {
            TestUtils.assertOverridesWorkerPoolConfiguration(PartialWrapper.class);
        } catch (AssertionError e) {
            final String message = e.getMessage();
            TestUtils.assertContains(message, PartialWrapper.class.getName());
            TestUtils.assertContains(message, "getGroupCount, getGroupCpus");
            TestUtils.assertNotContains(message, "getPoolName");
            TestUtils.assertNotContains(message, "getWorkerCount");
            TestUtils.assertNotContains(message, "isLegacy");
            return;
        }
        Assert.fail("expected an AssertionError for a wrapper that leaves methods to interface defaults");
    }

    @Test
    public void testHttpServerConfigurationWrapperForwardsGroupsAndDispatchController() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final HttpServerConfigurationWrapper wrapper = new HttpServerConfigurationWrapper(Metrics.DISABLED);
            wrapper.setDelegate(new DefaultHttpServerConfiguration(new DefaultCairoConfiguration(root)) {
                @Override
                public FiberDispatchController getFiberDispatchController() {
                    return DISPATCH_CONTROLLER;
                }

                @Override
                public int getGroupCount() {
                    return GROUP_CPUS.length;
                }

                @Override
                public int[][] getGroupCpus() {
                    return GROUP_CPUS;
                }
            });
            Assert.assertSame(DISPATCH_CONTROLLER, wrapper.getFiberDispatchController());
            Assert.assertEquals(GROUP_CPUS.length, wrapper.getGroupCount());
            Assert.assertSame(GROUP_CPUS, wrapper.getGroupCpus());
        });
    }

    @Test
    public void testHttpServerConfigurationWrapperOverridesAllMethods() {
        TestUtils.assertOverridesWorkerPoolConfiguration(HttpServerConfigurationWrapper.class);
    }

    @Test
    public void testPGConfigurationWrapperForwardsGroupsAndDispatchController() {
        final PGConfigurationWrapper wrapper = new PGConfigurationWrapper(Metrics.DISABLED);
        wrapper.setDelegate(new DefaultPGConfiguration() {
            @Override
            public FiberDispatchController getFiberDispatchController() {
                return DISPATCH_CONTROLLER;
            }

            @Override
            public int getGroupCount() {
                return GROUP_CPUS.length;
            }

            @Override
            public int[][] getGroupCpus() {
                return GROUP_CPUS;
            }
        });
        Assert.assertSame(DISPATCH_CONTROLLER, wrapper.getFiberDispatchController());
        Assert.assertEquals(GROUP_CPUS.length, wrapper.getGroupCount());
        Assert.assertSame(GROUP_CPUS, wrapper.getGroupCpus());
    }

    @Test
    public void testPGConfigurationWrapperOverridesAllMethods() {
        TestUtils.assertOverridesWorkerPoolConfiguration(PGConfigurationWrapper.class);
    }

    @Test
    public void testWorkerPoolConfigurationWrapperOverridesAllMethods() {
        TestUtils.assertOverridesWorkerPoolConfiguration(WorkerPoolConfigurationWrapper.class);
    }

    private static class InheritingWrapper extends WorkerPoolConfigurationWrapper {
    }

    private static class PartialWrapper implements WorkerPoolConfiguration {
        private final WorkerPoolConfiguration delegate;

        PartialWrapper(WorkerPoolConfiguration delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getPoolName() {
            return delegate.getPoolName();
        }

        @Override
        public int getWorkerCount() {
            return delegate.getWorkerCount();
        }
    }
}
