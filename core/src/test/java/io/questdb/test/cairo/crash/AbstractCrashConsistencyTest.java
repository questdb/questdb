package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCrashConsistencyTest extends AbstractCairoTest {

    protected CrashFaultFilesFacade crashFf;

    /**
     * Skips the test on Windows. The harness simulates power loss by reading and rewriting files under
     * a live engine that still holds them open, mapped and locked, and both halves of that are POSIX-only:
     * <ul>
     *   <li>{@code markDurableBaseline()} reads every file; QuestDB's Windows lock is
     *       {@code LockFile(h, 0, 0, 1, 0)}, an exclusive lock on byte 0, so ReadFile fails with
     *       ERROR_LOCK_VIOLATION.</li>
     *   <li>{@code crash()} truncates files back to their durable prefix; Windows refuses to resize a
     *       file with a user-mapped section open.</li>
     * </ul>
     * Neither is a property of the code under test, and closing the mappings first would remove the
     * state the harness models.
     * <p>
     * Every test that calls {@code markDurableBaseline()} or {@code crash()} must pass this gate, including
     * tests that install their own {@link CrashFaultFilesFacade} subclass instead of using
     * {@link #runWithCrashFacade}.
     */
    protected static void assumeCrashHarnessSupported() {
        Assume.assumeFalse(
                "crash-consistency harness rewrites files under a live engine, which Windows forbids",
                Os.isWindows()
        );
    }

    /**
     * Run {@code body} with the crash facade installed as the engine's FilesFacade.
     */
    protected void runWithCrashFacade(TestUtils.LeakProneCode body) throws Exception {
        assumeCrashHarnessSupported();
        crashFf = new CrashFaultFilesFacade();
        // Scope process-death fd reclamation to per-table files; engine-root files (tables.d, the table-id
        // generator, config) are owned by long-lived singletons that outlive releaseEngineHandles().
        crashFf.setDbRoot(root);
        try {
            assertMemoryLeak(crashFf, body);
        } finally {
            // A test that trips the facade's stale-handle fence fails loudly with the writer's close half done,
            // which leaves the writer owned in its pool; the next test's CREATE TABLE would then spin on its lock
            // for the whole timeout. Drop the fence (the facade is no longer the engine's) and reclaim what the
            // failed close left behind, as after a simulated crash. No-op on a healthy engine.
            crashFf.reset();
            engine.releaseCrashOrphanedWalWriters();
            engine.releaseCrashOrphanedWriters();
        }
    }

    /**
     * Mark everything committed so far as durable (prior, log-journaled state).
     */
    protected void markDurableBaseline() {
        if (crashFf == null) throw new IllegalStateException("call runWithCrashFacade(...) first");
        crashFf.markDurableBaseline(engine.getConfiguration().getDbRoot());
    }

    /**
     * Simulate power loss: mark it, release handles (their close-time flushes come after the power loss and do not
     * count), then roll files back.
     */
    protected void crashAndReopen() {
        if (crashFf == null) throw new IllegalStateException("call runWithCrashFacade(...) first");
        crashFf.powerLoss();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        // A freshly booted engine's WAL-writer pool is empty: a pooled WAL writer released after the rollback would
        // truncate its segment files against the rolled-back state.
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        crashFf.crash(engine.getConfiguration().getDbRoot());
    }

    /**
     * The power loss at the instant the writers' batches are still pending: marks it, then drops every writer's
     * pending without a flush. The writers are closed after this and before {@link CrashFaultFilesFacade#crash}
     * rolls the files back: the mark keeps their close-time flushes out of the durable state, and the rollback's
     * stale-handle fence refuses a mutation through a handle that survived it.
     */
    protected void powerLossDroppingPending(WalWriter... writers) {
        if (crashFf == null) throw new IllegalStateException("call runWithCrashFacade(...) first");
        crashFf.powerLoss();
        for (WalWriter writer : writers) {
            writer.simulatePowerLossDropPending();
        }
    }

    /**
     * Asserts containment: the rows that read back are correct, or a CairoException or CairoError is thrown.
     * Fewer rows (a truncated tail or a rolled-back commit) is an acceptable crash outcome; full durability
     * is {@link #assertSyncDurable}. A silently wrong row value is never acceptable.
     */
    protected void assertNoSilentCorruption(String tableName, String column, List<String> expected) {
        try {
            List<String> actual = readColumn(tableName, column);
            // fewer rows is OK here (rollback); the surviving rows must not be silently wrong
            int n = Math.min(actual.size(), expected.size());
            for (int i = 0; i < n; i++) {
                Assert.assertEquals("row " + i + " silently wrong", expected.get(i), actual.get(i));
            }
        } catch (CairoException | CairoError e) {
            // acceptable: corruption detected loudly
        } catch (InternalError e) {
            // JVM converts SIGBUS (mmap access past truncated file end) to InternalError; acceptable loud detection
        } catch (RuntimeException e) {
            // readColumn wraps SqlException in RuntimeException; allow if the cause is a loud Cairo error
            if (!(e.getCause() instanceof CairoException) && !(e.getCause() instanceof CairoError)) {
                throw e;
            }
        }
    }

    /**
     * Asserts full durability: every committed row is present and correct after the crash.
     */
    protected void assertSyncDurable(String tableName, String column, List<String> expected) {
        List<String> actual = readColumn(tableName, column);
        Assert.assertEquals("row count after crash", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals("row " + i, expected.get(i), actual.get(i));
        }
    }

    protected List<String> readColumn(String tableName, String column) {
        List<String> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select " + column + " from " + tableName)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Record r = c.getRecord();
                while (c.hasNext()) {
                    CharSequence v = r.getStrA(0);
                    out.add(v == null ? null : v.toString());
                }
            }
        } catch (SqlException e) {
            throw new RuntimeException("readColumn failed for: select " + column + " from " + tableName, e);
        }
        return out;
    }

    protected List<String> readVarcharColumn(String tableName, String column) {
        List<String> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select " + column + " from " + tableName)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Record r = c.getRecord();
                while (c.hasNext()) {
                    io.questdb.std.str.Utf8Sequence v = r.getVarcharA(0);
                    out.add(v == null ? null : v.toString());
                }
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException("readVarcharColumn failed for: select " + column + " from " + tableName, e);
        }
        return out;
    }

}
