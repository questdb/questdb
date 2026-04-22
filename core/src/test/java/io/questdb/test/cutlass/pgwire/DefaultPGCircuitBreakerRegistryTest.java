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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DefaultPGCircuitBreakerRegistryTest extends AbstractCairoTest {

    private static final PGConfiguration PG_CONFIG = new DefaultPGConfiguration() {
        @Override
        public int getLimit() {
            return 4;
        }
    };
    private static final PGConfiguration PG_CONFIG_LIMIT_1 = new DefaultPGConfiguration() {
        @Override
        public int getLimit() {
            return 1;
        }
    };

    @Test
    public void testCancelHappyPath() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(PG_CONFIG, configuration);
                    NetworkSqlExecutionCircuitBreaker cb = newCircuitBreaker()
            ) {
                int idx = registry.add(cb);
                cb.setSecret(123_456);
                Assert.assertFalse("circuit breaker must not be tripped before cancel()", cb.checkIfTripped());
                // happy path: correct idx and secret causes cancel() to trip the breaker
                registry.cancel(idx, 123_456);
                Assert.assertTrue("cancel() must trip the circuit breaker", cb.checkIfTripped());
            }
        });
    }

    @Test
    public void testCancelRejectsEmptySlot() throws Exception {
        assertMemoryLeak(() -> {
            try (DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(PG_CONFIG, configuration)) {
                // slot 0 has never been assigned; must not crash and must reject cleanly
                expectCairoFailure(() -> registry.cancel(0, 0), "empty circuit breaker slot");
            }
        });
    }

    @Test
    public void testCancelRejectsIdxEqualToSize() throws Exception {
        // off-by-one regression: previously `if (size() < idx)` allowed idx == size() through,
        // leading to ObjList.getQuick(size()) reading past the logical end of the list.
        assertMemoryLeak(() -> {
            try (
                    DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(PG_CONFIG, configuration);
                    NetworkSqlExecutionCircuitBreaker cb = newCircuitBreaker()
            ) {
                int idx = registry.add(cb);
                cb.setSecret(42);
                // the registry is pre-sized with `limit` null slots; idx + 1 up to `limit` are
                // also valid indices but empty. Anything at or beyond `limit` must be rejected.
                expectCairoFailure(() -> registry.cancel(PG_CONFIG.getLimit(), 42), "wrong circuit breaker idx");
                expectCairoFailure(() -> registry.cancel(PG_CONFIG.getLimit() + 1, 42), "wrong circuit breaker idx");
                // sanity: the legitimate idx still works
                registry.cancel(idx, 42);
            }
        });
    }

    @Test
    public void testCancelRejectsIdxEqualToSizeAfterDynamicGrowth() throws Exception {
        // Same off-by-one as testCancelRejectsIdxEqualToSize, but pinned at the dynamic-growth
        // branch of add(): once all `limit` pre-allocated null slots are occupied, add() grows
        // circuitBreakers via circuitBreakers.add(cb), extending its logical size past `limit`.
        // Under the old `size() < idx` check, cancel(size(), ...) passed and getQuick(size())
        // read past the logical end of the ObjList.
        assertMemoryLeak(() -> {
            try (
                    DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(PG_CONFIG_LIMIT_1, configuration);
                    NetworkSqlExecutionCircuitBreaker cb0 = newCircuitBreaker();
                    NetworkSqlExecutionCircuitBreaker cb1 = newCircuitBreaker()
            ) {
                int idx0 = registry.add(cb0); // fills the one pre-allocated slot
                int idx1 = registry.add(cb1); // forces dynamic growth; circuitBreakers.size() becomes 2
                Assert.assertEquals(0, idx0);
                Assert.assertEquals(1, idx1);
                registry.remove(idx0);        // slot 0 becomes null; slot 1 still holds cb1; size stays 2
                cb1.setSecret(0xCAFEBABE);
                // idx == size() must be rejected even after the list has grown past `limit`.
                expectCairoFailure(() -> registry.cancel(2, 0xCAFEBABE), "wrong circuit breaker idx");
                // sanity: the legitimate occupied idx still works
                registry.cancel(idx1, 0xCAFEBABE);
            }
        });
    }

    @Test
    public void testCancelRejectsNegativeIdx() throws Exception {
        assertMemoryLeak(() -> {
            try (DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(PG_CONFIG, configuration)) {
                expectCairoFailure(() -> registry.cancel(-1, 0), "wrong circuit breaker idx");
                expectCairoFailure(() -> registry.cancel(Integer.MIN_VALUE, 0), "wrong circuit breaker idx");
            }
        });
    }

    @Test
    public void testCancelRejectsSecretMinusOneSentinel() throws Exception {
        // after clear(), the breaker's secret is -1 until init() assigns a new random. A cancel
        // arriving in that window must not be able to succeed by guessing secret = -1.
        assertMemoryLeak(() -> {
            try (
                    DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(PG_CONFIG, configuration);
                    NetworkSqlExecutionCircuitBreaker cb = newCircuitBreaker()
            ) {
                int idx = registry.add(cb);
                cb.clear(); // sets secret = -1
                Assert.assertEquals(-1, cb.getSecret());
                expectCairoFailure(() -> registry.cancel(idx, -1), "wrong circuit breaker secret");
            }
        });
    }

    @Test
    public void testCancelRejectsWrongSecret() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(PG_CONFIG, configuration);
                    NetworkSqlExecutionCircuitBreaker cb = newCircuitBreaker()
            ) {
                int idx = registry.add(cb);
                cb.setSecret(0xDEADBEEF);
                expectCairoFailure(() -> registry.cancel(idx, 0xC0FFEE), "wrong circuit breaker secret");
            }
        });
    }

    private static void expectCairoFailure(Runnable op, String expectedMessage) {
        try {
            op.run();
            Assert.fail("expected CairoException with message containing '" + expectedMessage + "'");
        } catch (CairoException e) {
            Assert.assertTrue(
                    "unexpected message: " + e.getFlyweightMessage(),
                    e.getFlyweightMessage().toString().contains(expectedMessage)
            );
        }
    }

    private NetworkSqlExecutionCircuitBreaker newCircuitBreaker() {
        return new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB5);
    }
}
