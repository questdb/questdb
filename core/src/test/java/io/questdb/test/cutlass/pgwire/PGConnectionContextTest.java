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

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGConnectionContext;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.cutlass.pgwire.TypesAndInsert;
import io.questdb.cutlass.pgwire.TypesAndSelect;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.std.SimpleAssociativeCache;
import io.questdb.std.WeakSelfReturningObjectPool;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PGConnectionContextTest extends AbstractCairoTest {

    @Test
    public void testClearFreesCachedInserts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select x v, timestamp_sequence(0, 1_000_000) ts from long_sequence(3)) timestamp(ts) partition by day");
            execute("create table dst (v long, ts timestamp) timestamp(ts) partition by day");

            final String sql = "insert into dst select v, ts from src";
            final PGConfiguration configuration = new Port0PGConfiguration(-1);
            final WeakSelfReturningObjectPool<TypesAndInsert> taiPool = new WeakSelfReturningObjectPool<>(TypesAndInsert::new, 1);
            try (
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                            engine,
                            configuration.getCircuitBreakerConfiguration()
                    );
                    SqlExecutionContextImpl pgExecutionContext = new SqlExecutionContextImpl(engine, 1);
                    NoOpAssociativeCache<TypesAndSelect> tasCache = new NoOpAssociativeCache<>();
                    PGConnectionContext context = new PGConnectionContext(
                            engine,
                            configuration,
                            pgExecutionContext,
                            circuitBreaker,
                            tasCache
                    )
            ) {
                // cache the insert the same way an abandoned pipeline entry does during a session
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    entry.compileNewSQL(sql, engine, sqlExecutionContext, taiPool, false);
                    entry.cacheIfPossible(tasCache, context.getInsertCache());
                }
                final SimpleAssociativeCache<TypesAndInsert> taiCache = (SimpleAssociativeCache<TypesAndInsert>) context.getInsertCache();
                final TypesAndInsert tai = taiCache.peek(sql);
                Assert.assertNotNull(tai);
                Assert.assertNotNull(tai.getInsert());

                // the IO context pool calls clear() when the connection is closed and the context goes idle
                context.clear();

                Assert.assertNull(taiCache.peek(sql));
                Assert.assertNull(tai.getInsert());
            }
        });
    }

    @Test
    public void testInsertCacheDisabledDoesNotRetainInserts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select x v, timestamp_sequence(0, 1_000_000) ts from long_sequence(3)) timestamp(ts) partition by day");
            execute("create table dst (v long, ts timestamp) timestamp(ts) partition by day");

            final String sql = "insert into dst select v, ts from src";
            final PGConfiguration configuration = new Port0PGConfiguration(-1) {
                @Override
                public boolean isInsertCacheEnabled() {
                    return false;
                }
            };
            final WeakSelfReturningObjectPool<TypesAndInsert> taiPool = new WeakSelfReturningObjectPool<>(TypesAndInsert::new, 1);
            // the pool holds a single instance, so this is the instance the compiled insert will use
            final TypesAndInsert tai = taiPool.pop();
            tai.close();
            try (
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                            engine,
                            configuration.getCircuitBreakerConfiguration()
                    );
                    SqlExecutionContextImpl pgExecutionContext = new SqlExecutionContextImpl(engine, 1);
                    NoOpAssociativeCache<TypesAndSelect> tasCache = new NoOpAssociativeCache<>();
                    PGConnectionContext context = new PGConnectionContext(
                            engine,
                            configuration,
                            pgExecutionContext,
                            circuitBreaker,
                            tasCache
                    )
            ) {
                // offer the insert to the cache the same way an abandoned pipeline entry does during a session
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    entry.compileNewSQL(sql, engine, sqlExecutionContext, taiPool, false);
                    Assert.assertNotNull(tai.getInsert());
                    entry.cacheIfPossible(tasCache, context.getInsertCache());
                }

                Assert.assertNull(tai.getInsert());
                Assert.assertNull(context.getInsertCache().poll(sql));
            }
        });
    }
}
