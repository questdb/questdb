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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Witnesses the PGServer constructor self-clean path: when the post-field-init body throws
 * (the documented common case is the bind failure from {@code IODispatchers.create}), the
 * partially constructed PGServer never enters a try-with-resources at its call site, so
 * {@code close()} never runs. The constructor instead wraps the body in a catch that frees the
 * native handles already allocated above the throw (typesAndSelectCache + contextFactory,
 * and dispatcher if it got built) and rethrows, mirroring the LineTcpReceiver close-and-rethrow
 * shape.
 *
 * <p>This is the pg-wire counterpart of the protocol-envelope partial-init self-clean tests
 * ({@code ServerMainProtocolEnvelopePartialInitTest}). It forces a deterministic ctor throw by
 * a bind collision: a first PGServer binds an ephemeral port and a second PGServer is asked to
 * bind that exact port, so the second's dispatcher bind fails.
 *
 * <p>The whole test runs under {@code assertMemoryLeak}, so the native caches/factory that the
 * failed constructor allocated and then freed must leave the native allocator and file-descriptor
 * counts byte-balanced; a missing free in the catch path would fail the wrap. The test also
 * asserts the constructor propagated a throw rather than returning a half-built server.
 */
public class PGServerCtorThrowSelfCleanTest extends AbstractCairoTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void ctorThrowOnBindFailureFreesPartialConstruction() throws Exception {
        assertMemoryLeak(() -> {
            // First server binds an ephemeral port; read back the concrete port it resolved.
            final DefaultPGConfiguration ephemeralCfg = new DefaultPGConfiguration() {
                @Override
                public int getBindPort() {
                    return 0;
                }
            };
            try (WorkerPool holderPool = new TestWorkerPool(1);
                 PGServer holder = newServer(ephemeralCfg, holderPool)) {
                final int boundPort = holder.getPort();
                Assert.assertTrue("holder must resolve a concrete ephemeral port; got " + boundPort,
                        boundPort > 0);

                // Second server is asked to bind the SAME concrete port -> its dispatcher bind
                // fails inside IODispatchers.create, so the PGServer ctor body throws AFTER it has
                // already allocated typesAndSelectCache + contextFactory.
                final DefaultPGConfiguration collidingCfg = new DefaultPGConfiguration() {
                    @Override
                    public int getBindPort() {
                        return boundPort;
                    }
                };

                boolean threw = false;
                try (WorkerPool collidingPool = new TestWorkerPool(1)) {
                    final PGServer colliding = newServer(collidingCfg, collidingPool);
                    // Reaching here means no throw -- close the unexpected server so the wrap stays
                    // honest, then fail.
                    colliding.close();
                } catch (Throwable t) {
                    threw = true;
                }
                Assert.assertTrue(
                        "PGServer ctor must throw when its dispatcher cannot bind the already-bound port "
                                + boundPort + " (the partial-construction self-clean path)",
                        threw);

                // The holder is still healthy: its port is unchanged and it can be closed cleanly
                // by the try-with-resources, proving the failed second construction did not corrupt
                // shared native state.
                Assert.assertEquals("holder port must be unchanged after the colliding ctor failed",
                        boundPort, holder.getPort());
            }
        });
    }

    private static PGServer newServer(DefaultPGConfiguration configuration, WorkerPool workerPool) {
        final PGCircuitBreakerRegistry registry =
                new DefaultPGCircuitBreakerRegistry(configuration, engine.getConfiguration());
        return new PGServer(
                configuration,
                engine,
                workerPool,
                registry,
                () -> new SqlExecutionContextImpl(engine, workerPool.getWorkerCount())
        );
    }
}
