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
    public void testOwnerRefusalIsRetriableWhileAnotherConnectionCreatesTable() throws Exception {
        // The refused lookup does not wait for a table another connection is still creating, as nothing bounds how long
        // that connection holds the name: it fails at once with an error the client retries, and the retry finds the table.
        assertMemoryLeak(() -> {
            final SOCountDownLatch parked = new SOCountDownLatch(1);
            final SOCountDownLatch release = new SOCountDownLatch(1);
            final SOCountDownLatch created = new SOCountDownLatch(1);
            final AtomicReference<Throwable> creatorError = new AtomicReference<>();
            final Thread creator = newCreatorThread(creatorError, created);
            // Parks the creator with the table name taken, but the table not visible yet. A lookup that waited for the
            // creator would hang, so the creator stops waiting after a while, and the lookup then fails the test.
            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    parked.countDown();
                    release.await(TimeUnit.SECONDS.toNanos(30));
                }
            });
            try (QwpTudCache cache = newCache()) {
                // the owner is refused while the creator holds the name
                final OwnerRefusingSecurityContext securityContext = new OwnerRefusingSecurityContext(() -> {
                    creator.start();
                    Assert.assertTrue("creator did not reach its DDL listener", parked.await(TimeUnit.SECONDS.toNanos(30)));
                });
                try {
                    cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                    Assert.fail("the refused lookup must not wait for the table the creator holds");
                } catch (CairoException e) {
                    // neither an authorization error nor a schema mismatch, so the client retries it
                    Assert.assertFalse(e.isAuthorizationError());
                    Assert.assertFalse(e.isSchemaMismatch());
                    Assert.assertFalse(e.isCritical());
                    TestUtils.assertEquals("table is being created, cannot ingest [table=tab]", e.getFlyweightMessage());
                }
                release.countDown();
                created.await();
                Assert.assertNull(creatorError.get());
                // the retry writes into the creator's table, which needs no owner from this connection
                final WalTableUpdateDetails tud = cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                Assert.assertNotNull(tud);
                Assert.assertEquals(engine.verifyTableName("tab"), tud.getTableToken());
            } finally {
                release.countDown();
                creator.join();
                engine.setDdlListener(DefaultDdlListener.INSTANCE);
            }
        });
    }

    @Test
    public void testOwnerRefusalServesTableAnotherConnectionCreated() throws Exception {
        // A table another connection creates between this connection's lookup and its owner check needs no owner from
        // this connection: the refused lookup returns it.
        assertMemoryLeak(() -> {
            final SOCountDownLatch created = new SOCountDownLatch(1);
            final AtomicReference<Throwable> creatorError = new AtomicReference<>();
            final Thread creator = newCreatorThread(creatorError, created);
            try (QwpTudCache cache = newCache()) {
                // the owner is refused once the creator has created the table
                final OwnerRefusingSecurityContext securityContext = new OwnerRefusingSecurityContext(() -> {
                    creator.start();
                    created.await();
                });
                final WalTableUpdateDetails tud = cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                Assert.assertNotNull(tud);
                Assert.assertEquals(engine.verifyTableName("tab"), tud.getTableToken());
            } finally {
                creator.join();
            }
            Assert.assertNull(creatorError.get());
        });
    }

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

    private static QwpTudCache newCache() {
        final DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(
                new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration)
        );
        return new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY);
    }

    // A thread that creates the table "tab", as another connection would, and then counts down created.
    private static Thread newCreatorThread(AtomicReference<Throwable> creatorError, SOCountDownLatch created) {
        return new Thread(() -> {
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                engine.execute("CREATE TABLE tab (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL", executionContext);
            } catch (Throwable th) {
                creatorError.set(th);
            } finally {
                // release the native path memory the creation used
                Path.clearThreadLocals();
                created.countDown();
            }
        });
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
