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
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.InsertAsSelectOperationImpl;
import io.questdb.griffin.engine.ops.InsertOperationImpl;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

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
     * On a PRIMARY node the async-enqueue branch must remain reachable: the read-only re-check passes and
     * the branch proceeds to enqueue (it does not refuse a legitimate writer-busy fallback). The enqueue
     * itself is exercised by the protocol/integration tests; here we assert the fence does not turn the
     * non-WAL writer-busy fallback into a refusal on a primary node.
     */
    @Test
    public void testAsyncEnqueueBranchProceedsOnPrimary() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPoolExhaustedWriterEngine(false)) {
                OperationDispatcher<AbstractOperation> dispatcher = new OperationDispatcher<>(primaryEngine, "test update") {
                    @Override
                    protected long apply(AbstractOperation operation, TableWriterAPI writerFronted) {
                        return 0;
                    }
                };
                try {
                    dispatcher.execute(fakeOperation(), TestUtils.createSqlExecutionCtx(primaryEngine), new SCSequence(), false);
                } catch (Throwable e) {
                    // The enqueue itself does not complete against a fake table on a bare engine (the
                    // table-name registry is not built), so the branch fails after the read-only re-check
                    // passes. The failure must NOT be the read-only refusal -- the point is the primary
                    // node reached the enqueue rather than refusing it.
                    final boolean readOnlyRefusal = e instanceof CairoException ce
                            && ce.isAuthorizationError()
                            && ce.getMessage() != null
                            && ce.getMessage().contains("replica access is read-only");
                    Assert.assertFalse(
                            "a primary node must not refuse the async-enqueue branch with the read-only error",
                            readOnlyRefusal
                    );
                }
            }
        });
    }

    /**
     * The async-enqueue fallback fence: when the WAL writer acquire throws EntryUnavailableException (the
     * pool is exhausted), the catch branch must re-check read-only BEFORE enqueuing the operation. On a
     * read-only (demoting) node the branch must refuse with the standard authorization error rather than
     * route the WAL UPDATE through the legacy non-WAL writer pool, which would physical-commit it without
     * minting a replicated sequencer txn (a silent acked-loss). A non-null event sub-sequence is supplied
     * so the branch is the async-enqueue path, not the re-throw.
     */
    @Test
    public void testAsyncEnqueueBranchRefusesOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine readOnlyEngine = buildPoolExhaustedWriterEngine(true)) {
                OperationDispatcher<AbstractOperation> dispatcher = new OperationDispatcher<>(readOnlyEngine, "test update") {
                    @Override
                    protected long apply(AbstractOperation operation, TableWriterAPI writerFronted) {
                        return 0;
                    }
                };
                try {
                    dispatcher.execute(fakeOperation(), TestUtils.createSqlExecutionCtx(readOnlyEngine), new SCSequence(), false);
                    Assert.fail("the async-enqueue branch must refuse on a read-only node before enqueuing");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
            }
        });
    }

    /**
     * On a PRIMARY node the dispatcher fence must let the operation through: apply() runs exactly once.
     */
    @Test
    public void testDispatcherFenceAppliesOnPrimary() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger applyCalled = new AtomicInteger(0);
            try (CairoEngine primaryEngine = buildPrimaryWriterEngine()) {
                OperationDispatcher<AbstractOperation> dispatcher = new OperationDispatcher<>(primaryEngine, "test update") {
                    @Override
                    protected long apply(AbstractOperation operation, TableWriterAPI writerFronted) {
                        applyCalled.incrementAndGet();
                        return 0;
                    }
                };
                dispatcher.execute(fakeOperation(), TestUtils.createSqlExecutionCtx(primaryEngine), null, false);
                Assert.assertEquals("apply() must run once on a primary node", 1, applyCalled.get());
            }
        });
    }

    /**
     * The OperationDispatcher fence (WAL UPDATE/ALTER over pg-wire and /exec): a flip that lands after
     * the eager getTableWriterAPI gate passes but before the inline apply() must be caught by the
     * in-lock re-check, so the operation is NOT externalized (apply() never runs). The eager gate
     * consumes the first isReadOnlyMode() read (returns false, the writer is acquired as PRIMARY); the
     * in-lock re-check consumes the second (returns true, refuse). Behavioral assertion: apply() is
     * never reached and the refusal is the standard read-only authorization error.
     */
    @Test
    public void testDispatcherFenceInLockReCheckCatchesFlip() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger applyCalled = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallWriterEngine()) {
                OperationDispatcher<AbstractOperation> dispatcher = new OperationDispatcher<>(flipEngine, "test update") {
                    @Override
                    protected long apply(AbstractOperation operation, TableWriterAPI writerFronted) {
                        applyCalled.incrementAndGet();
                        return 0;
                    }
                };
                try {
                    dispatcher.execute(fakeOperation(), TestUtils.createSqlExecutionCtx(flipEngine), null, false);
                    Assert.fail("dispatcher.execute() must throw authorization when the in-lock re-check sees read-only");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
                Assert.assertEquals(
                        "apply() must not externalize the operation on the flipped node", 0, applyCalled.get());
            }
        });
    }

    /**
     * A read-only engine must refuse the WAL UPDATE/ALTER dispatch at the unlocked fast-refuse, before
     * acquiring a writer or reaching apply().
     */
    @Test
    public void testDispatcherFenceRefusesOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger applyCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyWriterEngine()) {
                OperationDispatcher<AbstractOperation> dispatcher = new OperationDispatcher<>(readOnlyEngine, "test update") {
                    @Override
                    protected long apply(AbstractOperation operation, TableWriterAPI writerFronted) {
                        applyCalled.incrementAndGet();
                        return 0;
                    }
                };
                try {
                    dispatcher.execute(fakeOperation(), TestUtils.createSqlExecutionCtx(readOnlyEngine), null, false);
                    Assert.fail("dispatcher.execute() must throw authorization on a read-only node");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
                Assert.assertEquals("apply() must not run on a read-only node", 0, applyCalled.get());
            }
        });
    }

    /**
     * INSERT ... SELECT: the in-lock re-check catches a flip that lands after the early-out passes but
     * before the commit -- modelling a demote that flips mid-pump. writer.commit() must not be called.
     */
    @Test
    public void testInsertAsSelectCommitInLockReCheckCatchesFlip() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            AtomicInteger rollbackCalled = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine()) {
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
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine()) {
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

    @Test
    public void testRoleSwitchLocksRejectConditions() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                Assert.assertThrows(
                        UnsupportedOperationException.class,
                        primaryEngine.getRoleSwitchReadLock()::newCondition
                );
                Assert.assertThrows(
                        UnsupportedOperationException.class,
                        primaryEngine.getRoleSwitchWriteLock()::newCondition
                );
            }
        });
    }

    @Test
    public void testRoleSwitchReadLockRestoresModeAfterTaskFailure() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicReference<Throwable> taskError = new AtomicReference<>();
                final FiberRuntime runtime = new FiberRuntime(1);
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final FiberTask task = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        taskError.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        readLock.lock();
                        try {
                            Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                            failFiberTask();
                            return true;
                        } finally {
                            readLock.unlock();
                        }
                    }
                };
                try {
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertTrue(task.isDone());
                    Assert.assertNotNull(taskError.get());
                    TestUtils.assertContains(taskError.get().getMessage(), "role-switch read lock blocks suspension");
                    Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
                    Assert.assertTrue(writeLock.tryLock());
                    writeLock.unlock();
                } finally {
                    close(runtime);
                }
            }
        });
    }

    @Test
    public void testRoleSwitchReadLockReentersWhileWriterQueued() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final CountDownLatch writerDone = new CountDownLatch(1);
                final Thread writer = new Thread(() -> {
                    writeLock.lock();
                    writeLock.unlock();
                    writerDone.countDown();
                }, "role-switch-writer");
                readLock.lock();
                try {
                    writer.start();
                    TestUtils.assertEventually(() -> Assert.assertEquals(Thread.State.WAITING, writer.getState()));
                    Assert.assertTrue(readLock.tryLock());
                    readLock.unlock();
                } finally {
                    readLock.unlock();
                }
                Assert.assertTrue(writerDone.await(5, TimeUnit.SECONDS));
                writer.join();
                Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
            }
        });
    }

    @Test
    public void testRoleSwitchReadLockCleanupAfterTaskLeak() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicReference<Throwable> taskError = new AtomicReference<>();
                final FiberRuntime runtime = new FiberRuntime(1);
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final FiberTask leakingTask = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        taskError.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        readLock.lock();
                        return true;
                    }
                };
                final AtomicReference<Throwable> replacementTaskError = new AtomicReference<>();
                final FiberTask replacementTask = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        replacementTaskError.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        readLock.lock();
                        try {
                            Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                            return true;
                        } finally {
                            readLock.unlock();
                        }
                    }
                };
                try {
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(leakingTask));
                    Assert.assertEquals(1, runtime.drain(1));

                    Assert.assertTrue(leakingTask.isDone());
                    Assert.assertNotNull(taskError.get());
                    TestUtils.assertContains(taskError.get().getMessage(), "leaked role-switch read lock");
                    Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
                    Assert.assertTrue(writeLock.tryLock());
                    writeLock.unlock();

                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(replacementTask));
                    Assert.assertEquals(1, runtime.drain(1));
                    if (replacementTaskError.get() != null) {
                        throw new AssertionError(replacementTaskError.get());
                    }
                    Assert.assertTrue(replacementTask.isDone());
                    Assert.assertEquals(1, runtime.getCreatedFiberCount());
                } finally {
                    close(runtime);
                }
            }
        });
    }

    @Test
    public void testRoleSwitchReadLockInterruptibleWaitDoesNotLeak() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicBoolean isInterrupted = new AtomicBoolean();
                final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final Thread reader = new Thread(() -> {
                    try {
                        readLock.lockInterruptibly();
                        try {
                            readerFailure.set(new AssertionError("reader acquired a held role-switch write lock"));
                        } finally {
                            readLock.unlock();
                        }
                    } catch (InterruptedException e) {
                        isInterrupted.set(true);
                    } catch (Throwable th) {
                        readerFailure.set(th);
                    }
                });
                reader.setDaemon(true);

                writeLock.lock();
                try {
                    reader.start();
                    awaitThreadWaiting(reader);
                    reader.interrupt();
                    reader.join(TimeUnit.SECONDS.toMillis(10));
                    Assert.assertFalse(reader.isAlive());
                    Assert.assertTrue(isInterrupted.get());
                    Assert.assertNull(readerFailure.get());
                    Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
                } finally {
                    reader.interrupt();
                    writeLock.unlock();
                }

                Assert.assertTrue(readLock.tryLock());
                readLock.unlock();
            }
        });
    }

    @Test
    public void testRoleSwitchReadLockIsReentrantInLegacyExecution() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final AtomicBoolean isWriterAcquired = new AtomicBoolean();
                final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
                final AtomicReference<Throwable> wrongOwnerFailure = new AtomicReference<>();
                final CountDownLatch writerStarted = new CountDownLatch(1);
                final Thread writer = new Thread(() -> {
                    writerStarted.countDown();
                    try {
                        if (!writeLock.tryLock(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out acquiring role-switch write lock");
                        }
                        try {
                            isWriterAcquired.set(true);
                        } finally {
                            writeLock.unlock();
                        }
                    } catch (Throwable th) {
                        writerFailure.set(th);
                    }
                });
                writer.setDaemon(true);
                readLock.lock();
                try {
                    Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                    writer.start();
                    Assert.assertTrue(writerStarted.await(10, TimeUnit.SECONDS));
                    awaitThreadWaiting(writer);
                    Assert.assertFalse(isWriterAcquired.get());

                    Assert.assertTrue(readLock.tryLock(1, TimeUnit.SECONDS));
                    try {
                        Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                    } finally {
                        readLock.unlock();
                    }

                    final Thread wrongOwner = new Thread(() -> {
                        try {
                            readLock.unlock();
                            Assert.fail("wrong owner unlocked role-switch read lock");
                        } catch (Throwable th) {
                            wrongOwnerFailure.set(th);
                        }
                    });
                    wrongOwner.setDaemon(true);
                    wrongOwner.start();
                    wrongOwner.join(TimeUnit.SECONDS.toMillis(10));
                    Assert.assertFalse(wrongOwner.isAlive());
                    Assert.assertTrue(wrongOwnerFailure.get() instanceof IllegalMonitorStateException);
                    Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                } finally {
                    readLock.unlock();
                }

                writer.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse(writer.isAlive());
                if (writerFailure.get() != null) {
                    throw new AssertionError(writerFailure.get());
                }
                Assert.assertTrue(isWriterAcquired.get());
                Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
                Assert.assertTrue(writeLock.tryLock());
                writeLock.unlock();
            }
        });
    }

    @Test
    public void testRoleSwitchReadLockRestoresFiberMode() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicReference<Throwable> taskError = new AtomicReference<>();
                final FiberRuntime runtime = new FiberRuntime(1);
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final FiberTask task = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        taskError.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        Assert.assertEquals(SuspensionScope.Mode.FIBER, SuspensionScope.getMode());
                        readLock.lock();
                        try {
                            Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                            Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                            readLock.lock();
                            try {
                                Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                                Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                            } finally {
                                readLock.unlock();
                            }
                            Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                        } finally {
                            readLock.unlock();
                        }
                        Assert.assertEquals(SuspensionScope.Mode.FIBER, SuspensionScope.getMode());
                        return true;
                    }
                };
                try {
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    if (taskError.get() != null) {
                        throw new AssertionError(taskError.get());
                    }
                    Assert.assertTrue(task.isDone());
                } finally {
                    close(runtime);
                }
                Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
            }
        });
    }

    @Test
    public void testRoleSwitchReadLockTryLockTimesOutBehindWriter() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicBoolean isImmediateLockAcquired = new AtomicBoolean();
                final AtomicBoolean isTimedLockAcquired = new AtomicBoolean();
                final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final Thread reader = new Thread(() -> {
                    try {
                        isImmediateLockAcquired.set(readLock.tryLock());
                        if (isImmediateLockAcquired.get()) {
                            readLock.unlock();
                        }
                        isTimedLockAcquired.set(readLock.tryLock(1, TimeUnit.MILLISECONDS));
                        if (isTimedLockAcquired.get()) {
                            readLock.unlock();
                        }
                    } catch (Throwable th) {
                        readerFailure.set(th);
                    }
                });
                reader.setDaemon(true);

                writeLock.lock();
                try {
                    reader.start();
                    reader.join(TimeUnit.SECONDS.toMillis(10));
                    Assert.assertFalse(reader.isAlive());
                    Assert.assertFalse(isImmediateLockAcquired.get());
                    Assert.assertFalse(isTimedLockAcquired.get());
                    Assert.assertNull(readerFailure.get());
                } finally {
                    writeLock.unlock();
                }
            }
        });
    }

    @Test
    public void testRoleSwitchReadLocksLeakCleanupAcrossEnginesInFiber() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    CairoEngine firstEngine = buildPrimaryEngine();
                    CairoEngine secondEngine = buildPrimaryEngine()
            ) {
                final AtomicReference<Throwable> taskError = new AtomicReference<>();
                final FiberRuntime runtime = new FiberRuntime(1);
                final Lock firstReadLock = firstEngine.getRoleSwitchReadLock();
                final Lock secondReadLock = secondEngine.getRoleSwitchReadLock();
                final FiberTask task = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        taskError.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        firstReadLock.lock();
                        secondReadLock.lock();
                        firstReadLock.unlock();
                        Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                        return true;
                    }
                };
                try {
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertTrue(task.isDone());
                    Assert.assertNotNull(taskError.get());
                    TestUtils.assertContains(taskError.get().getMessage(), "leaked role-switch read lock");
                    Assert.assertEquals(0, firstEngine.getRoleSwitchReadLockCount());
                    Assert.assertEquals(0, secondEngine.getRoleSwitchReadLockCount());
                } finally {
                    close(runtime);
                }
            }
        });
    }

    @Test
    public void testRoleSwitchReadLocksNestAcrossEngines() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    CairoEngine firstEngine = buildPrimaryEngine();
                    CairoEngine secondEngine = buildPrimaryEngine()
            ) {
                final Lock firstReadLock = firstEngine.getRoleSwitchReadLock();
                final Lock secondReadLock = secondEngine.getRoleSwitchReadLock();
                final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.FIBER);
                boolean isFirstReadLockHeld = false;
                boolean isSecondReadLockHeld = false;
                try {
                    firstReadLock.lock();
                    isFirstReadLockHeld = true;
                    secondReadLock.lock();
                    isSecondReadLockHeld = true;
                    Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                    Assert.assertEquals(1, firstEngine.getRoleSwitchReadLockCount());
                    Assert.assertEquals(1, secondEngine.getRoleSwitchReadLockCount());

                    firstReadLock.unlock();
                    isFirstReadLockHeld = false;
                    Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                    secondReadLock.lock();
                    Assert.assertEquals(1, secondEngine.getRoleSwitchReadLockCount());
                    secondReadLock.unlock();
                    Assert.assertEquals(1, secondEngine.getRoleSwitchReadLockCount());
                    secondReadLock.unlock();
                    isSecondReadLockHeld = false;
                    Assert.assertEquals(SuspensionScope.Mode.FIBER, SuspensionScope.getMode());
                } finally {
                    if (isSecondReadLockHeld) {
                        secondReadLock.unlock();
                    }
                    if (isFirstReadLockHeld) {
                        firstReadLock.unlock();
                    }
                    SuspensionScope.restore(previousMode);
                }
            }
        });
    }

    @Test
    public void testRoleSwitchWriteLockInterruptibleWaitDoesNotLeak() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicBoolean isInterrupted = new AtomicBoolean();
                final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final Thread writer = new Thread(() -> {
                    try {
                        writeLock.lockInterruptibly();
                        try {
                            writerFailure.set(new AssertionError("writer acquired a held role-switch read lock"));
                        } finally {
                            writeLock.unlock();
                        }
                    } catch (InterruptedException e) {
                        isInterrupted.set(true);
                    } catch (Throwable th) {
                        writerFailure.set(th);
                    }
                });
                writer.setDaemon(true);

                readLock.lock();
                try {
                    writer.start();
                    awaitThreadWaiting(writer);
                    writer.interrupt();
                    writer.join(TimeUnit.SECONDS.toMillis(10));
                    Assert.assertFalse(writer.isAlive());
                    Assert.assertTrue(isInterrupted.get());
                    Assert.assertNull(writerFailure.get());
                    Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());
                } finally {
                    writer.interrupt();
                    readLock.unlock();
                }

                Assert.assertTrue(writeLock.tryLock());
                writeLock.unlock();
            }
        });
    }

    @Test
    public void testRoleSwitchWriteLockReentrancyAndDowngrade() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                writeLock.lock();
                writeLock.lock();
                readLock.lock();
                Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());

                writeLock.unlock();
                Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
                writeLock.unlock();
                Assert.assertEquals(1, primaryEngine.getRoleSwitchReadLockCount());

                readLock.unlock();
                Assert.assertEquals(0, primaryEngine.getRoleSwitchReadLockCount());
                Assert.assertTrue(writeLock.tryLock());
                writeLock.unlock();
            }
        });
    }

    @Test
    public void testRoleSwitchWriteLockRejectsFiberOwner() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicReference<Throwable> taskError = new AtomicReference<>();
                final FiberRuntime runtime = new FiberRuntime(1);
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final FiberTask task = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        taskError.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        writeLock.lock();
                        try {
                            return true;
                        } finally {
                            writeLock.unlock();
                        }
                    }
                };
                try {
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertTrue(task.isDone());
                    Assert.assertNotNull(taskError.get());
                    TestUtils.assertContains(taskError.get().getMessage(), "role-switch write lock");
                    Assert.assertTrue(writeLock.tryLock());
                    writeLock.unlock();
                } finally {
                    close(runtime);
                }
            }
        });
    }

    @Test
    public void testRoleSwitchWriteLockTryLockWithMinimumTimeout() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicBoolean isWriteLocked = new AtomicBoolean();
                final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final Thread writer = new Thread(() -> {
                    try {
                        isWriteLocked.set(writeLock.tryLock(Long.MIN_VALUE, TimeUnit.NANOSECONDS));
                    } catch (Throwable th) {
                        writerFailure.set(th);
                    }
                });
                writer.setDaemon(true);

                readLock.lock();
                try {
                    writer.start();
                    writer.join(TimeUnit.SECONDS.toMillis(10));
                    Assert.assertFalse(writer.isAlive());
                    Assert.assertFalse(isWriteLocked.get());
                    Assert.assertNull(writerFailure.get());
                } finally {
                    readLock.unlock();
                }
            }
        });
    }

    @Test
    public void testRoleSwitchWriterPreventsReaderBarging() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                final AtomicInteger acquireOrder = new AtomicInteger();
                final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
                final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
                final CountDownLatch readerStarted = new CountDownLatch(1);
                final Lock readLock = primaryEngine.getRoleSwitchReadLock();
                final Lock writeLock = primaryEngine.getRoleSwitchWriteLock();
                final Thread writer = new Thread(() -> {
                    try {
                        writeLock.lock();
                        try {
                            if (!acquireOrder.compareAndSet(0, 1)) {
                                throw new AssertionError("reader barged ahead of role-switch writer");
                            }
                        } finally {
                            writeLock.unlock();
                        }
                    } catch (Throwable th) {
                        writerFailure.set(th);
                    }
                });
                final Thread reader = new Thread(() -> {
                    readerStarted.countDown();
                    try {
                        readLock.lock();
                        try {
                            if (!acquireOrder.compareAndSet(1, 2)) {
                                throw new AssertionError("role-switch reader acquired out of order");
                            }
                        } finally {
                            readLock.unlock();
                        }
                    } catch (Throwable th) {
                        readerFailure.set(th);
                    }
                });
                writer.setDaemon(true);
                reader.setDaemon(true);

                readLock.lock();
                try {
                    writer.start();
                    awaitThreadWaiting(writer);
                    reader.start();
                    Assert.assertTrue(readerStarted.await(10, TimeUnit.SECONDS));
                    awaitThreadWaiting(reader);
                } finally {
                    readLock.unlock();
                }

                writer.join(TimeUnit.SECONDS.toMillis(10));
                reader.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse(writer.isAlive());
                Assert.assertFalse(reader.isAlive());
                Assert.assertNull(writerFailure.get());
                Assert.assertNull(readerFailure.get());
                Assert.assertEquals(2, acquireOrder.get());
            }
        });
    }

    private static void assertReadOnlyRefusal(CairoException e) {
        Assert.assertTrue("exception must be an authorization error", e.isAuthorizationError());
        Assert.assertTrue(
                "message must be 'replica access is read-only'",
                e.getMessage().contains("replica access is read-only")
        );
    }

    private static void awaitThreadWaiting(Thread thread) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            final Thread.State state = thread.getState();
            if (state == Thread.State.BLOCKED
                    || state == Thread.State.TIMED_WAITING
                    || state == Thread.State.WAITING) {
                return;
            }
            if (state == Thread.State.TERMINATED) {
                Assert.fail("thread terminated before waiting [name=" + thread.getName() + ']');
            }
            LockSupport.parkNanos(100_000);
        }
        Assert.fail("thread did not wait [name=" + thread.getName() + ", state=" + thread.getState() + ']');
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(8);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
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
     * A real UpdateOperation used only to carry a TableToken into OperationDispatcher.execute. The
     * dispatcher's apply() is overridden per test to count invocations, so this operation's own apply()
     * never runs -- the fence either refuses before apply() or the test counts the call.
     */
    private static UpdateOperation fakeOperation() {
        final TableToken token = new TableToken("disp_fence", "disp_fence~1", null, 1, true, false, false);
        final ObjList<CharSequence> columns = new ObjList<>();
        columns.add("val");
        return new UpdateOperation(token, 1, 0, 0, columns);
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
     * A TableWriterAPI proxy that only needs to satisfy the try-with-resources close() in
     * OperationDispatcher.execute. The fence refuses before apply() in the tests that use it, so no
     * write method is reached; close() is the only call the dispatcher makes on a refused path.
     */
    private static TableWriterAPI noOpWriter() {
        return (TableWriterAPI) Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );
    }

    private static void failFiberTask() {
        throw new IllegalStateException("role-switch read lock blocks suspension");
    }

    /**
     * isReadOnlyMode() returns false on the first call (early-out passes) and true on every
     * subsequent call (the flip happened inside the lock window).
     */
    private CairoEngine buildFlipAfterFirstCallEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        final AtomicInteger callCount = new AtomicInteger(0);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                int n = callCount.incrementAndGet();
                return n >= 2;
            }
        };
    }

    /**
     * Engine for the dispatcher fence: isReadOnlyMode() returns false on the first call (the eager
     * getTableWriterAPI gate passes, the writer is acquired as PRIMARY) and true on every subsequent
     * call (the flip happened inside the lock window). getTableWriterAPI returns a fake writer so no
     * real table is needed.
     */
    private CairoEngine buildFlipAfterFirstCallWriterEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        final AtomicInteger callCount = new AtomicInteger(0);
        return new CairoEngine(cfg, false) {
            @Override
            public TableWriterAPI getTableWriterAPI(TableToken tableToken, String lockReason) {
                return noOpWriter();
            }

            @Override
            public boolean isReadOnlyMode() {
                return callCount.incrementAndGet() >= 2;
            }
        };
    }

    /**
     * Engine whose WAL writer acquire always throws EntryUnavailableException (the pool-exhausted
     * condition that routes OperationDispatcher.execute into the async-enqueue catch branch). The
     * isReadOnlyMode() flag is fixed to {@code readOnly} so the catch-branch re-check either refuses
     * (read-only) or proceeds (primary).
     */
    private CairoEngine buildPoolExhaustedWriterEngine(boolean readOnly) throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public TableWriterAPI getTableWriterAPI(TableToken tableToken, String lockReason) {
                throw EntryUnavailableException.instance("pool size exceeded");
            }

            @Override
            public boolean isReadOnlyMode() {
                return readOnly;
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

    private CairoEngine buildPrimaryWriterEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public TableWriterAPI getTableWriterAPI(TableToken tableToken, String lockReason) {
                return noOpWriter();
            }

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

    private CairoEngine buildReadOnlyWriterEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public TableWriterAPI getTableWriterAPI(TableToken tableToken, String lockReason) {
                return noOpWriter();
            }

            @Override
            public boolean isReadOnlyMode() {
                return true;
            }
        };
    }
}
