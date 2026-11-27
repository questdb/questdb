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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableNameRegistry;
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
    public void testOwnerRefusalIsRetriableWhileAnotherConnectionCreatesTable() throws Exception {
        // The refused lookup does not wait for a table another connection is still creating, as nothing bounds how long
        // that connection holds the name: it fails at once with an error the client retries, and the retry finds the table.
        assertRefusalWhileAnotherConnectionCreatesTable(false);
    }

    @Test
    public void testOwnerRefusalIsRetriableWhileRebaseSwapsTable() throws Exception {
        // REBASE WAL holds the name of the table while it swaps in the rebased directory, and then hands the name to
        // the rebased table. A refused lookup in that window must not stand, as the table exists before and after it:
        // it fails with an error the client retries, and the retry writes into the rebased table, which needs no
        // owner from this connection.
        assertRefusalWhileRebaseSwapsTable(false);
    }

    @Test
    public void testOwnerRefusalServesTableAnotherConnectionCreated() throws Exception {
        // A table another connection creates between this connection's lookup and its owner check needs no owner from
        // this connection: the refused lookup returns it.
        assertRefusalAfterAnotherConnectionCreatedTable(false);
    }

    @Test
    public void testOwnerRefusalStandsWhenNoOtherConnectionCreatesTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTudCache cache = newCache()) {
                try {
                    cache.getTableUpdateDetails(new OwnerRefusingSecurityContext(null, false), new Utf8String("tab"), schema(), null, 10);
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
    public void testReadOnlyRefusalStandsAfterAnotherConnectionCreatedTable() throws Exception {
        // A primary demoted in place refuses the owner as a replica does, with a read-only refusal. That refusal stands
        // whatever another operation does with the table's name: it carries the marker that gets the connection the
        // role-change close, which sends the client on to the new primary.
        assertRefusalAfterAnotherConnectionCreatedTable(true);
    }

    @Test
    public void testReadOnlyRefusalStandsWhileAnotherConnectionCreatesTable() throws Exception {
        assertRefusalWhileAnotherConnectionCreatesTable(true);
    }

    @Test
    public void testReadOnlyRefusalStandsWhileRebaseSwapsTable() throws Exception {
        assertRefusalWhileRebaseSwapsTable(true);
    }

    // Asserts the refusal of a lookup while another operation holds the table's name: a read-only refusal stands as
    // the security context raised it, an owner refusal turns into an error the client retries.
    private static void assertNameHeldRefusal(CairoException e, boolean isReadOnlyRefusal, String retriableMessage) {
        if (isReadOnlyRefusal) {
            assertReadOnlyRefusal(e);
        } else {
            // neither an authorization error nor a schema mismatch, so the client retries it
            Assert.assertFalse(e.isAuthorizationError());
            Assert.assertFalse(e.isSchemaMismatch());
            Assert.assertFalse(e.isCritical());
            TestUtils.assertEquals(retriableMessage, e.getFlyweightMessage());
        }
    }

    private static void assertReadOnlyRefusal(CairoException e) {
        Assert.assertTrue(e.isReadOnlyAccessRefusal());
        TestUtils.assertEquals(CairoException.READ_ONLY_ACCESS_MESSAGE, e.getFlyweightMessage());
    }

    // Refuses the owner after another connection has created the table: an owner refusal serves that table, a
    // read-only refusal stands.
    private static void assertRefusalAfterAnotherConnectionCreatedTable(boolean isReadOnlyRefusal) throws Exception {
        assertMemoryLeak(() -> {
            final SOCountDownLatch created = new SOCountDownLatch(1);
            final AtomicReference<Throwable> creatorError = new AtomicReference<>();
            final Thread creator = newCreatorThread(creatorError, created);
            try (QwpTudCache cache = newCache()) {
                // the owner is refused once the creator has created the table
                final OwnerRefusingSecurityContext securityContext = new OwnerRefusingSecurityContext(() -> {
                    creator.start();
                    created.await();
                }, isReadOnlyRefusal);
                if (isReadOnlyRefusal) {
                    try {
                        cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                        Assert.fail("the read-only refusal must stand");
                    } catch (CairoException e) {
                        assertReadOnlyRefusal(e);
                    }
                    Assert.assertEquals(0, cache.size());
                } else {
                    final WalTableUpdateDetails tud = cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                    Assert.assertNotNull(tud);
                    Assert.assertEquals(engine.verifyTableName("tab"), tud.getTableToken());
                }
            } finally {
                creator.join();
            }
            Assert.assertNull(creatorError.get());
        });
    }

    // Refuses the owner while another connection holds the table's name to create it.
    private static void assertRefusalWhileAnotherConnectionCreatesTable(boolean isReadOnlyRefusal) throws Exception {
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
                }, isReadOnlyRefusal);
                try {
                    cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                    Assert.fail("the refused lookup must not wait for the table the creator holds");
                } catch (CairoException e) {
                    assertNameHeldRefusal(e, isReadOnlyRefusal, "table is being created, cannot ingest [table=tab]");
                }
                release.countDown();
                created.await();
                Assert.assertNull(creatorError.get());
                if (!isReadOnlyRefusal) {
                    // the retry writes into the creator's table, which needs no owner from this connection
                    final WalTableUpdateDetails tud = cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                    Assert.assertNotNull(tud);
                    Assert.assertEquals(engine.verifyTableName("tab"), tud.getTableToken());
                }
            } finally {
                release.countDown();
                creator.join();
                engine.setDdlListener(DefaultDdlListener.INSTANCE);
            }
        });
    }

    // Refuses the owner while REBASE WAL holds the table's name to swap in the rebased table.
    private static void assertRefusalWhileRebaseSwapsTable(boolean isReadOnlyRefusal) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            execute("INSERT INTO tab VALUES (1, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            // REBASE WAL takes a hard-suspended table
            execute("ALTER TABLE tab SUSPEND WAL");
            final TableToken suspendedToken = engine.verifyTableName("tab");
            final SOCountDownLatch locked = new SOCountDownLatch(1);
            final SOCountDownLatch release = new SOCountDownLatch(1);
            // TableNameRegistryRW.swapTable() takes the metadata cache's write lock while it holds the name, so
            // holding the lock parks the rebase right there. A lookup that waited for the name would wait for the
            // rebase, and so for this lock: the holder lets go after a while, and the lookup then fails the test
            // instead of hanging it.
            final Thread lockHolder = new Thread(() -> {
                try (MetadataCacheWriter ignore = engine.getMetadataCache().writeLock()) {
                    locked.countDown();
                    release.await(TimeUnit.SECONDS.toNanos(30));
                }
            });
            final AtomicReference<Throwable> rebaseError = new AtomicReference<>();
            final Thread rebase = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute("ALTER TABLE tab REBASE WAL", executionContext);
                } catch (Throwable th) {
                    rebaseError.set(th);
                } finally {
                    // release the native path memory the rebase used
                    Path.clearThreadLocals();
                }
            });
            try (QwpTudCache cache = newCache()) {
                final OwnerRefusingSecurityContext securityContext = new OwnerRefusingSecurityContext(null, isReadOnlyRefusal);
                lockHolder.start();
                locked.await();
                rebase.start();
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (engine.getTableNameRegistry().getTableToken("tab") != TableNameRegistry.LOCKED_DROP_TOKEN) {
                    Assert.assertTrue("the rebase did not reach the swap of the table's name", System.nanoTime() < deadline);
                    Assert.assertNull(rebaseError.get());
                    Os.pause();
                }
                try {
                    cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                    Assert.fail("the lookup must not go on while the rebase holds the name");
                } catch (CairoException e) {
                    assertNameHeldRefusal(e, isReadOnlyRefusal, "table is being dropped, renamed or rebased, cannot ingest [table=tab]");
                }
                release.countDown();
                rebase.join();
                Assert.assertNull(rebaseError.get());
                final TableToken rebasedToken = engine.verifyTableName("tab");
                Assert.assertNotEquals(suspendedToken.getDirName(), rebasedToken.getDirName());
                if (!isReadOnlyRefusal) {
                    // the retry writes into the rebased table
                    final WalTableUpdateDetails tud = cache.getTableUpdateDetails(securityContext, new Utf8String("tab"), schema(), null, 10);
                    Assert.assertNotNull(tud);
                    Assert.assertEquals(rebasedToken, tud.getTableToken());
                }
            } finally {
                release.countDown();
                lockHolder.join();
                rebase.join();
            }
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

    // Refuses the owner of what the connection auto-creates, after it runs onAuthorize. With isReadOnlyRefusal it
    // refuses as a replica does, and so as a primary that was demoted in place does, with a read-only refusal.
    private static final class OwnerRefusingSecurityContext extends AllowAllSecurityContext {
        private final boolean isReadOnlyRefusal;
        private final Runnable onAuthorize;

        private OwnerRefusingSecurityContext(@Nullable Runnable onAuthorize, boolean isReadOnlyRefusal) {
            this.onAuthorize = onAuthorize;
            this.isReadOnlyRefusal = isReadOnlyRefusal;
        }

        @Override
        public void authorizeAutoCreateOwner() {
            if (onAuthorize != null) {
                onAuthorize.run();
            }
            throw isReadOnlyRefusal ? CairoException.readOnlyAccess() : CairoException.authorization().put("owner refused");
        }
    }
}
