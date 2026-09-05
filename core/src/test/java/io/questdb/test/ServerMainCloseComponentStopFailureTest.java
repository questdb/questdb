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

package io.questdb.test;

import io.questdb.ServerMain;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies ServerMain cleanup when a component fails to stop.
 */
public class ServerMainCloseComponentStopFailureTest extends AbstractBootstrapTest {

    private static final ObjList<String> EMPTY_DEPS = new ObjList<>();

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(120, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testUnboundedCloseFreesGraphWhenComponentStopThrows() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger stopInvocations = new AtomicInteger();
            final Component stopFailer = new Component() {
                @Override
                public ObjList<String> hardRequiredDependencies() {
                    return EMPTY_DEPS;
                }

                @Override
                public String name() {
                    return "test-stop-failer";
                }

                @Override
                public ObjList<String> softDependencies() {
                    return EMPTY_DEPS;
                }

                @Override
                public void start(LifecycleContext ctx) {
                }

                @Override
                public void stop() {
                    stopInvocations.incrementAndGet();
                    throw new RuntimeException("forced stop failure [unbounded close free test]");
                }
            };

            final ServerMain serverMain = new ServerMain(getServerMainArgs()) {
                @Override
                protected void registerComponents(LifecycleOrchestrator orch) {
                    orch.register(stopFailer);
                }
            };
            serverMain.start(false);
            serverMain.close();

            Assert.assertTrue("the failing component's stop() must have been attempted", stopInvocations.get() > 0);
            Assert.assertTrue(
                    "unbounded close() must free the graph and complete even when a component stop() throws",
                    serverMain.isCloseComplete()
            );
        });
    }
}
