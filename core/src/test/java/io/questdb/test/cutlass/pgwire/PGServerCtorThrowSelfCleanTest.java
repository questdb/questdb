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
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
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
 * ({@code ServerMainProtocolEnvelopePartialInitTest}). It forces a deterministic ctor throw with
 * a network facade that rejects the dispatcher bind.
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
            final DefaultPGConfiguration failingBindCfg = new DefaultPGConfiguration() {
                private final NetworkFacade networkFacade = new BindFailingNetworkFacade();

                @Override
                public NetworkFacade getNetworkFacade() {
                    return networkFacade;
                }
            };

            boolean threw = false;
            try (WorkerPool workerPool = new TestWorkerPool(1)) {
                final PGServer server = newServer(failingBindCfg, workerPool);
                // Reaching here means no throw -- close the unexpected server so the wrap stays
                // honest, then fail.
                server.close();
            } catch (Throwable t) {
                threw = true;
            }
            Assert.assertTrue(
                    "PGServer ctor must throw when its dispatcher cannot bind (the partial-construction self-clean path)",
                    threw);
        });
    }

    private static class BindFailingNetworkFacade extends NetworkFacadeImpl {
        @Override
        public boolean bindTcp(long fd, int address, int port) {
            return false;
        }

        @Override
        public int errno() {
            return -1;
        }
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
