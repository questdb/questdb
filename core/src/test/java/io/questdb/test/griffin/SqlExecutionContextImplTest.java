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

package io.questdb.test.griffin;

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlExecutionContextImplTest extends AbstractCairoTest {

    @Test
    public void testRejectedWithDoesNotPartiallyMutateMountedContext() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            context.with(
                    AllowAllSecurityContext.INSTANCE,
                    null,
                    null,
                    17,
                    SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
            );
            context.setQueryRegistryOwnerId(42);
            try {
                Assert.assertThrows(
                        IllegalStateException.class,
                        () -> context.with(
                                DenyAllSecurityContext.INSTANCE,
                                null,
                                null,
                                23,
                                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
                        )
                );
                Assert.assertSame(AllowAllSecurityContext.INSTANCE, context.getSecurityContext());
                Assert.assertEquals(17, context.getRequestFd());

                Assert.assertThrows(IllegalStateException.class, () -> context.with(DenyAllSecurityContext.INSTANCE, null, null));
                Assert.assertSame(AllowAllSecurityContext.INSTANCE, context.getSecurityContext());

                Assert.assertThrows(IllegalStateException.class, () -> context.with(29));
                Assert.assertEquals(17, context.getRequestFd());

                Assert.assertThrows(IllegalStateException.class, () -> context.with((BindVariableService) null));
                Assert.assertThrows(
                        IllegalStateException.class,
                        () -> context.with(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER)
                );
            } finally {
                context.setQueryRegistryOwnerId(-1);
            }
            context.reset();
        });
    }

    @Test
    public void testResetRejectsMountedQueryOwner() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            context.setQueryRegistryOwnerId(42);
            try {
                final IllegalStateException exception = Assert.assertThrows(
                        IllegalStateException.class,
                        context::reset
                );
                Assert.assertTrue(exception.getMessage().contains("ownerId=42"));
                Assert.assertEquals(42, context.getQueryRegistryOwnerId());
            } finally {
                context.setQueryRegistryOwnerId(-1);
            }
            context.reset();
        });
    }

    @Test
    public void testSecurityContextSwapPreservesMountedQueryOwner() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            context.with(AllowAllSecurityContext.INSTANCE);
            context.setQueryRegistryOwnerId(42);
            try {
                Assert.assertSame(
                        AllowAllSecurityContext.INSTANCE,
                        context.swapSecurityContext(DenyAllSecurityContext.INSTANCE)
                );
                Assert.assertSame(DenyAllSecurityContext.INSTANCE, context.getSecurityContext());
                Assert.assertEquals(42, context.getQueryRegistryOwnerId());
                Assert.assertSame(
                        DenyAllSecurityContext.INSTANCE,
                        context.swapSecurityContext(AllowAllSecurityContext.INSTANCE)
                );
                Assert.assertEquals(42, context.getQueryRegistryOwnerId());
            } finally {
                context.setQueryRegistryOwnerId(-1);
            }
            context.reset();
        });
    }
}
