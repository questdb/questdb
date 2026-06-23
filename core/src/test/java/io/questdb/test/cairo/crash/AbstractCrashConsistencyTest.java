package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCrashConsistencyTest extends AbstractCairoTest {

    protected CrashFaultFilesFacade crashFf;

    /** Run {@code body} with the crash facade installed as the engine's FilesFacade. */
    protected void runWithCrashFacade(TestUtils.LeakProneCode body) throws Exception {
        crashFf = new CrashFaultFilesFacade();
        assertMemoryLeak(crashFf, body);
    }

    /** Mark everything committed so far as durable (prior, log-journaled state). */
    protected void markDurableBaseline() {
        if (crashFf == null) throw new IllegalStateException("call runWithCrashFacade(...) first");
        crashFf.markDurableBaseline(engine.getConfiguration().getDbRoot());
    }

    /** Simulate power loss: release handles (clean close never fsyncs) then roll files back. */
    protected void crashAndReopen() {
        if (crashFf == null) throw new IllegalStateException("call runWithCrashFacade(...) first");
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        crashFf.crash(engine.getConfiguration().getDbRoot());
    }

    /**
     * Bar 1 (containment): the rows that read back are correct, OR a CairoException/CairoError is thrown.
     * Returning FEWER rows (tail truncated / commit rolled back) is an acceptable crash outcome —
     * full durability is Bar 2 (assertSyncDurable). What is never acceptable is a silently WRONG row value.
     */
    protected void assertNoSilentCorruption(String tableName, String column, List<String> expected) {
        try {
            List<String> actual = readColumn(tableName, column);
            // fewer rows is OK here (rollback); we only assert the surviving rows are not silently wrong
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

    /** Bar 2: every committed row present and correct after crash. */
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

    /**
     * Re-run {@code workload} crashing after each successive durability op (1..maxPoints), reopening
     * and running {@code assertion} each time, until the workload completes untripped. The proof tool
     * for ordering fixes: finds the window where a pointer is durable but its data is not. Releases
     * writer mappings before truncating so no mapping is held over crash().
     */
    protected int forEachCrashPoint(
            TestUtils.LeakProneCode seed,
            TestUtils.LeakProneCode workload,
            TestUtils.LeakProneCode assertion,
            int maxPoints
    ) throws Exception {
        int tripCount = 0;
        for (int k = 1; k <= maxPoints; k++) {
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.clear();
            crashFf.reset();
            seed.run();
            markDurableBaseline();
            boolean tripped = false;
            crashFf.armCrashAt(k);
            try {
                workload.run();
            } catch (CrashSimulationError e) {
                tripped = true;
            } catch (CairoException | CairoError e) {
                // The engine's commit path may wrap CrashSimulationError in CairoException/CairoError;
                // detect this by walking the cause chain.
                if (hasCrashCause(e)) {
                    tripped = true;
                } else {
                    throw e;
                }
            } finally {
                crashFf.armCrashAt(-1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            crashFf.crash(engine.getConfiguration().getDbRoot());
            assertion.run();
            if (!tripped) {
                LOG.info().$("forEachCrashPoint completed untripped at k=").$(k).$(" trips=").$(tripCount).$();
                return tripCount;
            }
            tripCount++;
        }
        Assert.fail("forEachCrashPoint exhausted " + maxPoints + " points without completing");
        return tripCount; // unreachable, but required for compilation
    }

    /**
     * Detect whether a Throwable was caused by a CrashSimulationError, either directly (cause chain)
     * or indirectly (the engine may copy only the message into a fresh CairoException without chaining
     * the original Error as the cause).
     */
    private static boolean hasCrashCause(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof CrashSimulationError) {
                return true;
            }
            String msg = cause.getMessage();
            if (msg != null && msg.contains("simulated crash at durability op")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
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
