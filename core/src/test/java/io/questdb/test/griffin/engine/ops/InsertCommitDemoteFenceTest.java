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

package io.questdb.test.griffin.engine.ops;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.InsertAsSelectOperationImpl;
import io.questdb.griffin.engine.ops.InsertOperationImpl;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that the HTTP /exec write executors -- InsertOperationImpl (plain INSERT) and
 * InsertAsSelectOperationImpl (INSERT ... SELECT) -- hold the role-switch lock around the read-only
 * re-check at commit() so a PRIMARY-to-REPLICA flip that lands during the SELECT pump cannot commit
 * an unreplicated txn and return HTTP 200.
 * <p>
 * The /exec path checks ReadOnlyStatementGate before compiling, but that gate read is check-then-act:
 * the writer is acquired while the node is still PRIMARY, the rows are appended into its in-memory
 * buffer (the SELECT pump for INSERT ... SELECT can run arbitrarily long), and only InsertMethod.commit()
 * externalizes them.
 * <p>
 * RED state (before the fix): commit() called writer.commit() unconditionally; a flip between the gate
 * read and commit() acknowledged an unreplicated write. The tests assert writer.commit() is NOT called
 * and an authorization error is thrown -- both fail before the fix.
 * <p>
 * GREEN state (after the fix): the early-out, the in-lock re-check, and the rollback-on-refusal refuse
 * the commit; writer.commit() is never called and the buffered rows are rolled back.
 */
public class InsertCommitDemoteFenceTest extends AbstractCairoTest {

    /**
     * INSERT ... SELECT: the in-lock re-check catches a flip that lands after the early-out passes but
     * before the commit -- modelling a demote that flips mid-pump. writer.commit() must not be called.
     */
    @Test
    public void testInsertAsSelectCommitInLockReCheckCatchesFlip() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            AtomicInteger rollbackCalled = new AtomicInteger(0);
            AtomicInteger readOnlyCallCount = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine(readOnlyCallCount)) {
                TableToken token = new TableToken("ias_flip", "ias_flip~1", null, 1, false, false, false);
                TableWriterAPI writer = fakeWriter(token, commitCalled, rollbackCalled);
                InsertAsSelectOperationImpl op = new InsertAsSelectOperationImpl(
                        flipEngine, token, fakeFactory(), null, 7L, -1, 0, 0
                );
                try {
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(flipEngine);
                    try (InsertMethod method = op.createMethod(ctx, fixedWriterSource(writer))) {
                        try {
                            method.commit();
                            Assert.fail("commit() must throw authorization when the in-lock re-check sees read-only");
                        } catch (CairoException e) {
                            assertReadOnlyRefusal(e);
                        }
                    }
                } finally {
                    Misc.free(op);
                }
                Assert.assertEquals("writer.commit() must not be called on the flipped node", 0, commitCalled.get());
                Assert.assertTrue("buffered rows must be rolled back", rollbackCalled.get() >= 1);
                Assert.assertTrue(
                        "isReadOnlyMode() must be called at least twice to reach the in-lock re-check",
                        readOnlyCallCount.get() >= 2
                );
            }
        });
    }

    /**
     * INSERT ... SELECT on a PRIMARY node must commit exactly once -- the fence must not refuse a
     * legitimate write.
     */
    @Test
    public void testInsertAsSelectCommitOnPrimaryCommits() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            AtomicInteger rollbackCalled = new AtomicInteger(0);
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                TableToken token = new TableToken("ias_ok", "ias_ok~1", null, 1, false, false, false);
                TableWriterAPI writer = fakeWriter(token, commitCalled, rollbackCalled);
                InsertAsSelectOperationImpl op = new InsertAsSelectOperationImpl(
                        primaryEngine, token, fakeFactory(), null, 7L, -1, 0, 0
                );
                try {
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(primaryEngine);
                    try (InsertMethod method = op.createMethod(ctx, fixedWriterSource(writer))) {
                        method.commit();
                    }
                } finally {
                    Misc.free(op);
                }
                Assert.assertEquals("writer.commit() must be called once on a primary node", 1, commitCalled.get());
            }
        });
    }

    /**
     * Plain INSERT: the in-lock re-check catches a flip that lands after the early-out passes but
     * before the commit. writer.commit() must not be called and the rows must be rolled back.
     */
    @Test
    public void testInsertCommitInLockReCheckCatchesFlip() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            AtomicInteger rollbackCalled = new AtomicInteger(0);
            AtomicInteger readOnlyCallCount = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine(readOnlyCallCount)) {
                TableToken token = new TableToken("ins_flip", "ins_flip~1", null, 1, false, false, false);
                TableWriterAPI writer = fakeWriter(token, commitCalled, rollbackCalled);
                InsertOperationImpl op = new InsertOperationImpl(flipEngine, token, 7L);
                try {
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(flipEngine);
                    try (InsertMethod method = op.createMethod(ctx, fixedWriterSource(writer))) {
                        try {
                            method.commit();
                            Assert.fail("commit() must throw authorization when the in-lock re-check sees read-only");
                        } catch (CairoException e) {
                            assertReadOnlyRefusal(e);
                        }
                    }
                } finally {
                    Misc.free(op);
                }
                Assert.assertEquals("writer.commit() must not be called on the flipped node", 0, commitCalled.get());
                Assert.assertTrue("buffered rows must be rolled back", rollbackCalled.get() >= 1);
                Assert.assertTrue(
                        "isReadOnlyMode() must be called at least twice to reach the in-lock re-check",
                        readOnlyCallCount.get() >= 2
                );
            }
        });
    }

    /**
     * The headline INSERT bypass: a read-only engine must refuse the commit with the standard
     * authorization error, roll back, and never call writer.commit().
     */
    @Test
    public void testInsertCommitRefusesAndRollsBackOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            AtomicInteger rollbackCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                TableToken token = new TableToken("ins_ro", "ins_ro~1", null, 1, false, false, false);
                TableWriterAPI writer = fakeWriter(token, commitCalled, rollbackCalled);
                InsertOperationImpl op = new InsertOperationImpl(readOnlyEngine, token, 7L);
                try {
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(readOnlyEngine);
                    try (InsertMethod method = op.createMethod(ctx, fixedWriterSource(writer))) {
                        try {
                            method.commit();
                            Assert.fail("commit() must throw CairoException.authorization on a read-only node");
                        } catch (CairoException e) {
                            assertReadOnlyRefusal(e);
                        }
                    }
                } finally {
                    Misc.free(op);
                }
                Assert.assertEquals("writer.commit() must not be called on a read-only node", 0, commitCalled.get());
                Assert.assertTrue("buffered rows must be rolled back", rollbackCalled.get() >= 1);
            }
        });
    }

    // --- helpers ---

    private static void assertReadOnlyRefusal(CairoException e) {
        Assert.assertTrue("exception must be an authorization error", e.isAuthorizationError());
        Assert.assertTrue(
                "message must be 'replica access is read-only'",
                e.getMessage().contains("replica access is read-only")
        );
    }

    /**
     * A RecordCursorFactory proxy that supports the InsertAsSelectOperationImpl close() path
     * (Misc.free(factory)). The fence under test sits in commit(), which never touches the factory,
     * so getCursor()/getMetadata() are never reached.
     */
    private static RecordCursorFactory fakeFactory() {
        return (RecordCursorFactory) Proxy.newProxyInstance(
                RecordCursorFactory.class.getClassLoader(),
                new Class[]{RecordCursorFactory.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );
    }

    /**
     * A TableWriterAPI proxy that records commit()/rollback() calls and satisfies createMethod's
     * metadata-version and table-name match checks (so the method is built without recompilation).
     */
    private static TableWriterAPI fakeWriter(TableToken token, AtomicInteger commitCalled, AtomicInteger rollbackCalled) {
        return (TableWriterAPI) Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "commit" -> {
                        commitCalled.incrementAndGet();
                        yield null;
                    }
                    case "rollback" -> {
                        rollbackCalled.incrementAndGet();
                        yield null;
                    }
                    case "getMetadataVersion" -> 7L;
                    case "getTableToken" -> token;
                    case "getMetadata" -> (TableRecordMetadata) Proxy.newProxyInstance(
                            TableRecordMetadata.class.getClassLoader(),
                            new Class[]{TableRecordMetadata.class},
                            (p2, m2, a2) -> switch (m2.getName()) {
                                case "getColumnCount" -> (int) 0;
                                case "close" -> null;
                                default -> throw new UnsupportedOperationException(m2.getName());
                            }
                    );
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );
    }

    private static WriterSource fixedWriterSource(TableWriterAPI writer) {
        return new WriterSource() {
            @Override
            public TableWriterAPI getTableWriterAPI(TableToken tableToken, String lockReason) {
                return writer;
            }

            @Override
            public TableWriterAPI getTableWriterAPI(CharSequence tableName, String lockReason) {
                return writer;
            }
        };
    }

    /**
     * isReadOnlyMode() returns false on the first call (early-out passes) and true on every
     * subsequent call (the flip happened inside the lock window).
     */
    private CairoEngine buildFlipAfterFirstCallEngine(AtomicInteger callCount) throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                int n = callCount.incrementAndGet();
                return n >= 2;
            }
        };
    }

    private CairoEngine buildPrimaryEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                return false;
            }
        };
    }

    private CairoEngine buildReadOnlyEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                return true;
            }
        };
    }
}
