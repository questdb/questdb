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
 * Witnesses {@code ServerMain.start(boolean)} resetting its {@code running} CAS flag after a
 * startup failure, so a retry actually re-runs the orchestrator instead of being a silent no-op.
 *
 * <p>{@code start()} latches a {@code running.compareAndSet(false, true)} gate before running the
 * boot DAG. Without the failure-path reset, a first {@code start()} that throws would leave the
 * gate latched at {@code true}, and every subsequent {@code start()} would short-circuit the CAS
 * and return without ever re-running the orchestrator -- a retry that silently does nothing.
 *
 * <p>The test drives a {@code ServerMain} whose component DAG is a single always-throwing
 * component (the heavy production envelopes are deliberately not registered, so no writer-pool,
 * query-tracing, or page-frame circuit-breaker native state is allocated to mask the signal). It
 * asserts:
 * <ul>
 *   <li>the first {@code start()} propagates the boot failure and leaves {@code hasStarted() == false}
 *       (the reset);</li>
 *   <li>the second {@code start()} actually re-runs the DAG -- the failing component's
 *       {@code start()} is invoked a second time and the second {@code start()} throws again. Were
 *       the running flag NOT reset, the second {@code start()} would short-circuit the CAS and
 *       return silently without re-running the DAG or throwing; observing a second throw (and a
 *       second component invocation) is the proof the retry was not a silent no-op.</li>
 * </ul>
 *
 * <p>Runs under {@code assertMemoryLeak}: each failed boot tears its minimal partially-started DAG
 * down and the server closes cleanly, so native and fd counts must balance.
 */
public class ServerMainStartRetryAfterFailureTest extends AbstractBootstrapTest {

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
    public void startResetsRunningFlagSoRetryReRunsTheDag() throws Exception {
        assertMemoryLeak(() -> {
            // A failing component shared across both start() calls. It always throws, so neither
            // boot succeeds. startInvocations counts how often the DAG actually reached its
            // start(); the second start() must increment it again (proving the retry re-ran the
            // DAG) rather than short-circuiting the CAS gate.
            final AtomicInteger startInvocations = new AtomicInteger();
            final Component failer = new Component() {
                @Override
                public ObjList<String> hardRequiredDependencies() {
                    return EMPTY_DEPS;
                }

                @Override
                public String name() {
                    return "test-boot-failer";
                }

                @Override
                public ObjList<String> softDependencies() {
                    return EMPTY_DEPS;
                }

                @Override
                public void start(LifecycleContext ctx) {
                    startInvocations.incrementAndGet();
                    throw new RuntimeException("forced boot failure [start-retry test]");
                }

                @Override
                public void stop() {
                }
            };

            try (ServerMain serverMain = new ServerMain(getServerMainArgs()) {
                @Override
                protected void registerComponents(LifecycleOrchestrator orch) {
                    // Register ONLY the failer: the start()/running-CAS reset under test lives in
                    // ServerMain.start(boolean) and is exercised regardless of the DAG contents.
                    // Keeping the DAG minimal avoids allocating heavy native boot state (writer
                    // pools, query-tracing, page-frame circuit breakers) whose rollback timing
                    // could mask the running-flag-reset signal across an in-process double boot.
                    orch.register(failer);
                }
            }) {
                // First start: the failer throws, failing the boot.
                boolean firstThrew = false;
                try {
                    serverMain.start(false);
                } catch (Throwable t) {
                    firstThrew = true;
                }
                Assert.assertTrue("first start() must propagate the forced boot failure", firstThrew);
                Assert.assertEquals("the failing component must have been started once on the first boot",
                        1, startInvocations.get());
                Assert.assertFalse(
                        "start() must reset running to false after a boot failure so a retry is not a no-op",
                        serverMain.hasStarted());

                // Second start: the running flag was reset, so the CAS gate opens and the DAG runs
                // again. The failer throws again -- the second throw, plus the second start()
                // invocation, proves the retry re-ran the DAG rather than silently no-op'ing.
                boolean secondThrew = false;
                try {
                    serverMain.start(false);
                } catch (Throwable t) {
                    secondThrew = true;
                }
                Assert.assertTrue(
                        "the retry must actually re-run the DAG and propagate the failure again, not silently no-op",
                        secondThrew);
                Assert.assertEquals(
                        "the failing component's start() must have been invoked a second time by the retry",
                        2, startInvocations.get());
                Assert.assertFalse(
                        "start() must reset running to false after the second boot failure too",
                        serverMain.hasStarted());
            }
        });
    }

    private static final ObjList<String> EMPTY_DEPS = new ObjList<>();
}
