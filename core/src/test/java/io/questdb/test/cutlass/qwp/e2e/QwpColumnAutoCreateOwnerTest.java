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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.DefaultFactoryProvider;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

// QWP auto-creates a column only once SecurityContext.authorizeAutoCreateOwner() accepts the owner, see QwpWalAppender.
public class QwpColumnAutoCreateOwnerTest extends AbstractQwpWebSocketTest {

    @Test
    public void testOwnerRefusalDoesNotServeColumnOfRenamedTable() throws Exception {
        // The table was renamed after this connection cached its writer, and the renamed table has the column a block
        // names. Catching the writer up to find that column would replay the RENAME into it, and the rows for the old
        // name would commit into the renamed table. The block is refused instead, and its replay on a new connection
        // finds the old name free, where the owner refusal of the table it would create stands.
        final AtomicInteger ownerChecks = useOwnerRefusingSecurityContext(null);
        runInContext(port -> {
            execute("CREATE TABLE t (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            final List<SenderError> errors = new CopyOnWriteArrayList<>();
            LineSenderServerException refusal = null;
            try (QwpWebSocketSender sender = connectWs(port, errors::add)) {
                sender.table("t").longColumn("v", 1).atNow();
                Assert.assertTrue(sender.drain(30_000));
                execute("RENAME TABLE t TO u");
                execute("ALTER TABLE u ADD COLUMN c LONG");
                // A deferred frame takes the per-frame path: a commit batch refuses a renamed table as it joins.
                sender.setDeferCommit(true);
                sender.table("t").longColumn("v", 2).longColumn("c", 2).atNow();
                sender.flush();
                sender.setDeferCommit(false);
                sender.table("t").longColumn("v", 3).atNow();
                sender.flush();
            } catch (LineSenderServerException e) {
                refusal = e;
            }
            Assert.assertNotNull("the rows for t must not commit into u", refusal);
            Assert.assertSame(SenderError.Category.SECURITY_ERROR, refusal.getServerError().getCategory());
            TestUtils.assertContains(refusal.getMessage(), "owner refused");
            // the replay's table creation checked the owner, the column add did not
            Assert.assertEquals(1, ownerChecks.get());
            assertRenameRefusal(errors, "t");
            drainWalQueue();
            assertQuery("SELECT v, c FROM u").noLeakCheck().expectSize().returns("""
                    v\tc
                    1\tnull
                    """);
            Assert.assertNull(engine.getTableTokenIfExists("t"));
        });
    }

    @Test
    public void testOwnerRefusalDoesNotServeColumnOfTableRenamedDuringAdd() throws Exception {
        assertRefusedAddOfTableRenamedDuringAdd(true);
    }

    @Test
    public void testOwnerRefusalOfTableRenamedDuringAddIsRetried() throws Exception {
        assertRefusedAddOfTableRenamedDuringAdd(false);
    }

    @Test
    public void testOwnerRefusalServesColumnAnotherConnectionAdded() throws Exception {
        // Another connection adds the column after this connection's table lookup caught its writer up with the table's
        // structure, and before this connection's own add. The refused add finds that column instead of standing.
        final AtomicReference<Throwable> addError = new AtomicReference<>();
        final AtomicInteger ownerChecks = useOwnerRefusingSecurityContext(() -> {
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                engine.execute("ALTER TABLE t ADD COLUMN c LONG", executionContext);
            } catch (Throwable th) {
                addError.set(th);
            }
        });
        runInContext(port -> {
            execute("CREATE TABLE t (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("t").longColumn("v", 1).longColumn("c", 2).atNow();
                sender.flush();
            }
            Assert.assertNull(addError.get());
            Assert.assertEquals("the owner must have been refused", 1, ownerChecks.get());
            drainWalQueue();
            assertQuery("SELECT v, c FROM t").noLeakCheck().expectSize().returns("""
                    v\tc
                    1\t2
                    """);
        });
    }

    @Test
    public void testOwnerRefusalStandsAfterRenameRoundTripDuringAdd() throws Exception {
        // Another session renames the table away and back between this connection's table lookup and its owner check.
        // Catching the writer up after the refusal replays both renames, which leave the table under its name, so the
        // refusal stands at once, as when nobody renames the table, without a retry on a new connection.
        final AtomicReference<Throwable> renameError = new AtomicReference<>();
        assertOwnerRefusalStands(() -> {
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                engine.execute("RENAME TABLE t TO u", executionContext);
                engine.execute("RENAME TABLE u TO t", executionContext);
            } catch (Throwable th) {
                renameError.set(th);
            }
        });
        Assert.assertNull(renameError.get());
    }

    @Test
    public void testOwnerRefusalStandsWhenNoOtherConnectionAddsColumn() throws Exception {
        assertOwnerRefusalStands(null);
    }

    // Refuses the owner of the column a frame adds, after running onAddColumn as the connection is about to add it, and
    // asserts that the refusal stands at once: one owner check, so no retry on a new connection, and no column.
    private void assertOwnerRefusalStands(@Nullable Runnable onAddColumn) throws Exception {
        final AtomicInteger ownerChecks = useOwnerRefusingSecurityContext(onAddColumn);
        runInContext(port -> {
            execute("CREATE TABLE t (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            LineSenderServerException refusal = null;
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("t").longColumn("v", 1).longColumn("c", 2).atNow();
                sender.flush();
            } catch (LineSenderServerException e) {
                refusal = e;
            }
            Assert.assertNotNull("the owner refusal must stand", refusal);
            Assert.assertSame(SenderError.Category.SECURITY_ERROR, refusal.getServerError().getCategory());
            TestUtils.assertContains(refusal.getMessage(), "owner refused");
            Assert.assertEquals(1, ownerChecks.get());
            // apply whatever was sequenced, so that a column added through the WAL would show
            drainWalQueue();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t"))) {
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("c"));
            }
            assertQuery("SELECT v FROM t").noLeakCheck().expectSize().returns("v\n");
        });
    }

    // Another session renames the table between this connection's table lookup and its owner check, as it is about to
    // add a column, and adds that column to the renamed table when isColumnAdded. Catching the writer up after the
    // refusal replays the RENAME into it: the block is refused, whether the column turns up or not, and its replay
    // on a new connection finds the old name free, where the owner refusal of the table it would create stands.
    private void assertRefusedAddOfTableRenamedDuringAdd(boolean isColumnAdded) throws Exception {
        final AtomicReference<Throwable> renameError = new AtomicReference<>();
        final AtomicInteger ownerChecks = useOwnerRefusingSecurityContext(() -> {
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                engine.execute("RENAME TABLE t TO u", executionContext);
                if (isColumnAdded) {
                    engine.execute("ALTER TABLE u ADD COLUMN c LONG", executionContext);
                }
            } catch (Throwable th) {
                renameError.set(th);
            }
        });
        runInContext(port -> {
            execute("CREATE TABLE t (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            final List<SenderError> errors = new CopyOnWriteArrayList<>();
            LineSenderServerException refusal = null;
            try (QwpWebSocketSender sender = connectWs(port, errors::add)) {
                sender.table("t").longColumn("v", 1).longColumn("c", 2).atNow();
                sender.flush();
            } catch (LineSenderServerException e) {
                refusal = e;
            }
            Assert.assertNull(renameError.get());
            Assert.assertNotNull("the rows for t must not commit into u", refusal);
            Assert.assertSame(SenderError.Category.SECURITY_ERROR, refusal.getServerError().getCategory());
            TestUtils.assertContains(refusal.getMessage(), "owner refused");
            // the column add checked the owner, then the replay's table creation
            Assert.assertEquals(2, ownerChecks.get());
            assertRenameRefusal(errors, "t");
            drainWalQueue();
            assertQuery("SELECT * FROM u")
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .expectSize()
                    .returns(isColumnAdded ? "v\ttimestamp\tc\n" : "v\ttimestamp\n");
            Assert.assertNull(engine.getTableTokenIfExists("t"));
        });
    }

    // Serves every connection a security context that refuses the owner of what it auto-creates, as Enterprise does
    // for an external user that named none, and runs onAddColumn once, the first time a connection is about to add a
    // column. Returns the count of refused owner checks.
    private AtomicInteger useOwnerRefusingSecurityContext(@Nullable Runnable onAddColumn) {
        final AtomicInteger ownerChecks = new AtomicInteger();
        final AtomicReference<Runnable> pendingOnAddColumn = new AtomicReference<>(onAddColumn);
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull SecurityContextFactory getSecurityContextFactory() {
                return (principalContext, interfaceId) -> new AllowAllSecurityContext() {
                    @Override
                    public void authorizeAlterTableAddColumn(TableToken tableToken) {
                        final Runnable action = pendingOnAddColumn.getAndSet(null);
                        if (action != null) {
                            action.run();
                        }
                    }

                    @Override
                    public void authorizeAutoCreateOwner() {
                        ownerChecks.incrementAndGet();
                        throw CairoException.authorization().put("owner refused");
                    }
                };
            }
        };
        return ownerChecks;
    }
}
