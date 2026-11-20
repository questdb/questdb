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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// QWP auto-creates a table only once SecurityContext.authorizeAutoCreateOwner() accepts the owner, see QwpTudCache.
public class QwpTudCacheAutoCreateOwnerTest extends AbstractCairoTest {

    @Test
    public void testOwnerRefusalStandsWhenNoOtherConnectionCreatesTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTudCache cache = newCache()) {
                try {
                    cache.getTableUpdateDetails(new OwnerRefusingSecurityContext(null), new Utf8String("tab"), schema(), null, 10);
                    Assert.fail("the owner refusal must stand");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isAuthorizationError());
                    TestUtils.assertContains(e.getFlyweightMessage(), "owner refused");
                }
            }
            Assert.assertNull(engine.getTableTokenIfExists("tab"));
        });
    }

    @Test
    public void testOwnerRefusalWaitsForTableAnotherConnectionCreates() throws Exception {
        // The table another connection is creating needs no owner from this one: the refused lookup waits for it, as
        // createTable(ifNotExists) would, and returns it.
        assertMemoryLeak(() -> {
            final SOCountDownLatch parked = new SOCountDownLatch(1);
            final SOCountDownLatch release = new SOCountDownLatch(1);
            final AtomicReference<Throwable> creatorError = new AtomicReference<>();
            final Thread creator = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute("CREATE TABLE tab (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL", executionContext);
                } catch (Throwable th) {
                    creatorError.set(th);
                } finally {
                    // release the native path memory the creation used
                    Path.clearThreadLocals();
                }
            });
            final Thread releaser = new Thread(() -> {
                Os.sleep(100);
                release.countDown();
            });
            // parks the creator with the table name taken, but the table not visible yet
            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    parked.countDown();
                    release.await();
                }
            });
            try (QwpTudCache cache = newCache()) {
                // the owner is refused while the creator holds the name, and the creator is released after that
                final OwnerRefusingSecurityContext securityContext = new OwnerRefusingSecurityContext(() -> {
                    creator.start();
                    Assert.assertTrue("creator did not reach its DDL listener", parked.await(TimeUnit.SECONDS.toNanos(30)));
                    releaser.start();
                });
                final WalTableUpdateDetails tud = cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                Assert.assertNotNull(tud);
                Assert.assertEquals(engine.verifyTableName("tab"), tud.getTableToken());
            } finally {
                release.countDown();
                creator.join();
                releaser.join();
                engine.setDdlListener(DefaultDdlListener.INSTANCE);
            }
            Assert.assertNull(creatorError.get());
        });
    }

    private static QwpTudCache newCache() {
        final DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(
                new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration)
        );
        return new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY);
    }

    private static ObjList<QwpColumnDef> schema() {
        final ObjList<QwpColumnDef> schema = new ObjList<>();
        schema.add(new QwpColumnDef("v", QwpConstants.TYPE_LONG));
        return schema;
    }

    private static final class OwnerRefusingSecurityContext extends AllowAllSecurityContext {
        private final Runnable onAuthorize;

        private OwnerRefusingSecurityContext(@Nullable Runnable onAuthorize) {
            this.onAuthorize = onAuthorize;
        }

        @Override
        public void authorizeAutoCreateOwner() {
            if (onAuthorize != null) {
                onAuthorize.run();
            }
            throw CairoException.authorization().put("owner refused");
        }
    }
}
