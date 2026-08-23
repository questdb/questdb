/*******************************************************************************
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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshSqlExecutionContext;
import io.questdb.cairo.security.AbstractPrincipalAwareSecurityContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A live view is only well-defined if its result is a pure function of the base table and
 * its definition, so every refresh cycle - and, under symmetric replica refresh, every
 * node - converges on the same rows. {@code CREATE LIVE VIEW} rejects non-deterministic
 * functions in the body, but the refresh job recompiles the view's SELECT on its own
 * ({@code LiveViewRefreshJob.ensureCompiledFactory}, reached on a restart that rebuilds the
 * factory straight from the persisted definition), a path the CREATE gate never runs.
 * {@link LiveViewRefreshSqlExecutionContext} therefore forces
 * {@link LiveViewRefreshSqlExecutionContext#allowNonDeterministicFunctions()} off so that
 * recompile rejects a non-deterministic function too, mirroring
 * {@code MatViewRefreshSqlExecutionContext}. These tests pin that defense-in-depth guard,
 * and the write-authorization guard beside it: the context's security context permits
 * insertion only into the live view whose refresh cycle is currently bound.
 */
public class LiveViewRefreshSqlExecutionContextTest extends AbstractCairoTest {

    @Test
    public void testRefreshContextAuthorizesInsertOnlyIntoRefreshingView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be registered at CREATE", instance);
            final TableToken viewToken = instance.getLiveViewToken();
            final TableToken baseToken = engine.verifyTableName("base");

            final LiveViewRefreshSqlExecutionContext ctx = new LiveViewRefreshSqlExecutionContext(engine, 0);
            final SecurityContext securityContext = ctx.getSecurityContext();

            // With no refresh cycle bound the context authorizes no write at all -
            // not even into the view's own table.
            assertInsertDenied(securityContext, viewToken);

            ctx.ofRefreshingInstance(instance);
            try {
                // The view being refreshed is the one table the context may write into.
                securityContext.authorizeInsert(viewToken);
                // Every other table stays read-only, the base table included.
                assertInsertDenied(securityContext, baseToken);
            } finally {
                ctx.ofRefreshingInstance(null);
            }

            // Clearing the binding closes the write window again.
            assertInsertDenied(securityContext, viewToken);
        });
    }

    @Test
    public void testRefreshContextDoesNotOverRejectDeterministicSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final LiveViewRefreshSqlExecutionContext ctx = new LiveViewRefreshSqlExecutionContext(engine, 0);
            // The guard must not disturb an ordinary deterministic body: the refresh recompile
            // of a legitimate view compiles exactly as it does today.
            ctx.setLiveViewCompile(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn FROM base",
                        ctx
                );
                Misc.free(cq.getRecordCursorFactory());
            } finally {
                ctx.setLiveViewCompile(false);
            }
        });
    }

    @Test
    public void testRefreshContextForPrincipalKeepsViewScopedInsert() throws Exception {
        // forPrincipal must NOT downgrade the refresh context to a plain read-only context: its
        // newPrincipalContext override returns this, so the view-scoped authorizeInsert (which lets writes
        // through to the refreshing view's own table) survives the per-principal derivation. A plain
        // ReadOnlySecurityContext would deny the view insert too. Mirrors the mat view guard in MatViewTest.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be registered at CREATE", instance);
            final TableToken viewToken = instance.getLiveViewToken();
            final TableToken baseToken = engine.verifyTableName("base");

            final LiveViewRefreshSqlExecutionContext ctx = new LiveViewRefreshSqlExecutionContext(engine, 0);
            final SecurityContext securityContext = ctx.getSecurityContext();

            ctx.ofRefreshingInstance(instance);
            try {
                // forPrincipal returns the very same instance rather than deriving a plain read-only context
                final SecurityContext derived = ((AbstractPrincipalAwareSecurityContext) securityContext).forPrincipal("alice");
                Assert.assertSame(securityContext, derived);

                // the view-scoped allowance survives the derivation: the refreshing view's own table stays
                // writable, while every other table stays denied (a plain read-only downgrade would deny the
                // view insert too, so the permitted case is what proves the override was not dropped)
                derived.authorizeInsert(viewToken);
                assertInsertDenied(derived, baseToken);
            } finally {
                ctx.ofRefreshingInstance(null);
            }
        });
    }

    @Test
    public void testRefreshContextForbidsNonDeterministicFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final LiveViewRefreshSqlExecutionContext ctx = new LiveViewRefreshSqlExecutionContext(engine, 0);
            Assert.assertFalse(
                    "live view refresh must forbid non-deterministic functions",
                    ctx.allowNonDeterministicFunctions()
            );

            // Compile the SELECT straight through the refresh context, the way the refresh
            // job's ensureCompiledFactory does, so the CREATE-time gate is out of the picture.
            // With setLiveViewCompile armed the reject names the "live view" kind, not the mat
            // view, and identifies the offending function - here now(), in the projection.
            ctx.setLiveViewCompile(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = null;
                try {
                    cq = compiler.compile("SELECT ts, x, now() AS r FROM base", ctx);
                    Misc.free(cq.getRecordCursorFactory());
                    Assert.fail("refresh recompile must reject a non-deterministic function");
                } catch (SqlException e) {
                    Assert.assertTrue(
                            "wrong message [msg=" + e.getFlyweightMessage() + ']',
                            Chars.contains(e.getFlyweightMessage(), "non-deterministic function cannot be used in live view: now")
                    );
                }
            } finally {
                ctx.setLiveViewCompile(false);
            }
        });
    }

    private static void assertInsertDenied(SecurityContext securityContext, TableToken tableToken) {
        try {
            securityContext.authorizeInsert(tableToken);
            Assert.fail("insert into " + tableToken.getTableName() + " must be denied");
        } catch (CairoException e) {
            Assert.assertTrue("expected an authorization error", e.isAuthorizationError());
            TestUtils.assertContains(e.getFlyweightMessage(), "Write permission denied");
        }
    }
}
