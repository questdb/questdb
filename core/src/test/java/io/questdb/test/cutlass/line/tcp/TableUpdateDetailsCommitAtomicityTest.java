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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.std.Misc;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that TableUpdateDetails commit, closeNoLock, and releaseWriter paths
 * hold the role-switch lock around the read-only re-check so that a PRIMARY-to-REPLICA
 * flip cannot race a client commit onto the replica.
 *
 * RED state (before the fix):
 *   - closeNoLock() and releaseWriter(true) had no read-only check: writerAPI.commit()
 *     was called unconditionally on a read-only node.
 *   - commit() had only a single volatile read with no lock: a flip between the gate-read
 *     and writerAPI.commit() was undetected.
 *
 * GREEN state (after the fix):
 *   - All three paths acquire the role-switch lock and re-check isReadOnlyMode() inside
 *     the lock before committing; a read-only node never calls writerAPI.commit().
 */
public class TableUpdateDetailsCommitAtomicityTest extends AbstractCairoTest {

    /**
     * commit() must throw CairoException.authorization("replica access is read-only") when
     * the engine is read-only, and must NOT call writerAPI.commit().
     *
     * This exercises the early-out path (engine already read-only before lock acquire) AND
     * the in-lock re-check via the AlwaysReadOnlyEngine: both paths must refuse.
     */
    @Test
    public void testCommitRefusesOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                WalTableUpdateDetails tud = buildTud(readOnlyEngine, 1L, commitCalled);
                try {
                    tud.commit(false);
                    Assert.fail("commit() must throw CairoException.authorization on read-only engine");
                } catch (CairoException e) {
                    Assert.assertTrue("exception must be an authorization error", e.isAuthorizationError());
                    Assert.assertTrue(
                            "message must be 'replica access is read-only'",
                            e.getMessage().contains("replica access is read-only")
                    );
                } finally {
                    Misc.free(tud);
                }
            }
            Assert.assertEquals("writerAPI.commit() must not be called on read-only engine", 0, commitCalled.get());
        });
    }

    /**
     * commit() must call writerAPI.commit() exactly once when the engine stays PRIMARY.
     */
    @Test
    public void testCommitHappyPathCallsWriterCommit() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                WalTableUpdateDetails tud = buildTud(primaryEngine, 1L, commitCalled);
                try {
                    tud.commit(false);
                } finally {
                    Misc.free(tud);
                }
            }
            Assert.assertEquals("writerAPI.commit() must be called once on primary engine", 1, commitCalled.get());
        });
    }

    /**
     * closeNoLock() with commitOnClose=true must NOT call writerAPI.commit() when the engine
     * is read-only.
     *
     * RED state (before fix): no read-only check in closeNoLock -> commit is called anyway.
     * GREEN state (after fix): in-lock check sees read-only -> commit is skipped.
     */
    @Test
    public void testCloseNoLockSkipsCommitOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            // commitOnClose=true: the close path tries to commit.
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                WalTableUpdateDetails tud = buildTudWithCommitOnClose(readOnlyEngine, 0L, commitCalled, true);
                Misc.free(tud);  // triggers closeNoLock
            }
            Assert.assertEquals(
                    "closeNoLock() must not call writerAPI.commit() on read-only engine",
                    0, commitCalled.get()
            );
        });
    }

    /**
     * releaseWriter(true) must NOT call writerAPI.commit() when the engine is read-only.
     *
     * RED state (before fix): no read-only check in releaseWriter -> commit is called anyway.
     * GREEN state (after fix): in-lock check sees read-only -> commit is skipped.
     */
    @Test
    public void testReleaseWriterSkipsCommitOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                WalTableUpdateDetails tud = buildTud(readOnlyEngine, 0L, commitCalled);
                try {
                    callReleaseWriter(tud, true);
                } finally {
                    // writerAPI is already null after releaseWriter; set engine to non-null before free
                    Misc.free(tud);
                }
            }
            Assert.assertEquals(
                    "releaseWriter(true) must not call writerAPI.commit() on read-only engine",
                    0, commitCalled.get()
            );
        });
    }

    /**
     * The in-lock re-check in commit() catches the TOCTOU scenario: engine is PRIMARY on the
     * first isReadOnlyMode() call (early-out passes) but REPLICA on the second call (in-lock
     * re-check). The fix prevents writerAPI.commit() from being invoked in this case.
     *
     * RED state (before fix): only one check exists (the early-out), so the flip between the
     * two reads is undetected and writerAPI.commit() is called.
     * GREEN state (after fix): the in-lock re-check catches the flip and throws authorization.
     */
    @Test
    public void testCommitInLockReCheckCatchesFlipAfterEarlyOut() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            // Engine that reports read-only only on the SECOND isReadOnlyMode() call,
            // simulating a flip that occurs after the early-out but before commit.
            AtomicInteger readOnlyCallCount = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine(readOnlyCallCount)) {
                WalTableUpdateDetails tud = buildTud(flipEngine, 1L, commitCalled);
                try {
                    tud.commit(false);
                    Assert.fail("commit() must throw authorization when in-lock re-check sees read-only");
                } catch (CairoException e) {
                    Assert.assertTrue("exception must be authorization error", e.isAuthorizationError());
                    Assert.assertTrue(
                            "message must be 'replica access is read-only'",
                            e.getMessage().contains("replica access is read-only")
                    );
                } finally {
                    Misc.free(tud);
                }
            }
            Assert.assertEquals("writerAPI.commit() must not be called", 0, commitCalled.get());
            Assert.assertTrue(
                    "isReadOnlyMode() must be called at least twice to reach the in-lock re-check",
                    readOnlyCallCount.get() >= 2
            );
        });
    }

    // --- helpers ---

    /**
     * Builds a minimal WalTableUpdateDetails backed by a proxy writer that reports
     * the given uncommitted row count and records commit() calls.
     * The TUD uses commitOnClose=false (default for WAL TUDs).
     */
    private WalTableUpdateDetails buildTud(
            CairoEngine eng, long uncommittedRowCount, AtomicInteger commitCalled
    ) throws Exception {
        return buildTudWithCommitOnClose(eng, uncommittedRowCount, commitCalled, false);
    }

    private WalTableUpdateDetails buildTudWithCommitOnClose(
            CairoEngine eng, long uncommittedRowCount, AtomicInteger commitCalled, boolean commitOnClose
    ) throws Exception {
        LineHttpProcessorConfiguration lineConfig =
                new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(eng.getConfiguration());
        DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
        Utf8String tableName = new Utf8String("fake_table");

        TableWriterAPI fakeWriter = (TableWriterAPI) Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getUncommittedRowCount" -> uncommittedRowCount;
                    case "getMetadata" -> (TableRecordMetadata) Proxy.newProxyInstance(
                            TableRecordMetadata.class.getClassLoader(),
                            new Class[]{TableRecordMetadata.class},
                            (p2, m2, a2) -> switch (m2.getName()) {
                                case "getTimestampIndex" -> (int) -1;
                                case "getTimestampType" -> (int) io.questdb.cairo.ColumnType.NULL;
                                case "getColumnCount" -> (int) 0;
                                case "close" -> null;
                                default -> throw new UnsupportedOperationException(m2.getName());
                            }
                    );
                    // isWal=false: avoids the post-commit WAL token staleness check in commit()
                    // which would try to look up the token via engine.getTableTokenIfExists().
                    case "getTableToken" -> new TableToken("fake_table", "fake_table~1", null, 1, false, false, false);
                    case "supportsMultipleWriters" -> true;
                    case "getLastSeqTxn" -> -1L;
                    case "getWalId" -> 1;
                    case "getSegmentId" -> 0;
                    case "commit" -> {
                        commitCalled.incrementAndGet();
                        yield null;
                    }
                    case "ic" -> {
                        commitCalled.incrementAndGet();
                        yield null;
                    }
                    case "close", "rollback" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );

        return new WalTableUpdateDetails(
                eng,
                null,           // no SecurityContext needed for these paths
                fakeWriter,
                defaultColumnTypes,
                tableName,
                null,           // symbolCachePool -- not used in commit/close/release paths
                Long.MAX_VALUE, // commitInterval -- never auto-commit
                commitOnClose,
                Long.MAX_VALUE  // maxUncommittedRows
        );
    }

    /** Builds a minimal CairoEngine that always returns read-only mode. */
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

    /** Builds a minimal CairoEngine that always returns primary (read-write) mode. */
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

    /**
     * Builds a minimal CairoEngine whose isReadOnlyMode() returns false on the first call
     * (early-out passes) and true on all subsequent calls (flip happened inside the lock window).
     */
    private CairoEngine buildFlipAfterFirstCallEngine(AtomicInteger callCount) throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                int n = callCount.incrementAndGet();
                return n >= 2;  // false on first call; true from second call onward
            }
        };
    }

    /** Calls the package-private releaseWriter(boolean) via reflection. */
    private static void callReleaseWriter(WalTableUpdateDetails tud, boolean commit) throws Exception {
        Method m = io.questdb.cutlass.line.tcp.TableUpdateDetails.class.getDeclaredMethod("releaseWriter", boolean.class);
        m.setAccessible(true);
        m.invoke(tud, commit);
    }
}
