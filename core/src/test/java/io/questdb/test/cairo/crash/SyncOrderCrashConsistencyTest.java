package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Checks that on the O3 merge path (O3CopyJob.syncColumns) the varchar data file (.d) is fsynced
 * before the aux file (.i) that holds offsets into it.
 *
 * <p>Data must be durable before the pointer to it. Otherwise a crash can leave a durable .i whose
 * offsets point past the durable extent of .d, and the reopened table silently returns corrupt data.
 *
 * <p>BitmapIndexWriter.sync orders msync calls, not fsync calls. msync takes an address rather than
 * an fd, so the facade cannot attribute it to a file and this test does not cover it.
 */
public class SyncOrderCrashConsistencyTest extends AbstractCrashConsistencyTest {

    @Test
    public void testO3SyncsDataBeforeAuxForVarcharColumn() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        try {
            Assert.assertEquals("test requires SYNC commit mode",
                    CommitMode.SYNC, engine.getConfiguration().getCommitMode());

            runWithCrashFacade(() -> {
                // Create a partitioned varchar table so an out-of-order insert triggers an O3 merge
                // into an existing partition, running O3CopyJob.syncColumns which fsyncs .d and .i.
                execute("create table o (ts timestamp, v varchar) timestamp(ts) partition by day");

                // Insert several in-order rows across one day to establish the partition.
                long base = 1_000_000_000_000L; // 2001-09-08 in micros
                for (int i = 0; i < 5; i++) {
                    execute("insert into o values (" + (base + i * 10_000_000L) + ", 'inorder-row-" + i + "')");
                }

                // Clear the sync order recorded during the in-order inserts; the checks below look at the
                // O3 merge only.
                crashFf.reset();

                // Insert an out-of-order row: a timestamp before the last in-order row, same partition.
                // This forces the O3 merge path -> O3CopyJob.syncColumns -> fsync of .d then .i.
                execute("insert into o values (" + (base - 5_000_000L) + ", 'ooo-row')");

                List<String> order = crashFf.getSyncOrder();

                // Find the column name 'v' as it appears in file paths (QuestDB uses column name directly).
                // Data file ends with .d; aux/index file ends with .i.
                int firstDataIdx = -1;
                int firstAuxIdx = -1;
                for (int i = 0; i < order.size(); i++) {
                    String path = order.get(i);
                    // Match paths that contain the column name segment and end with the extension.
                    // QuestDB partition paths look like: .../o/2001-09-08/v.d  and  .../o/2001-09-08/v.i
                    boolean isVarcharData = (path.endsWith("/v.d") || path.endsWith("\\v.d")
                            || path.contains("/v.d.") || path.contains("\\v.d."));
                    boolean isVarcharAux = (path.endsWith("/v.i") || path.endsWith("\\v.i")
                            || path.contains("/v.i.") || path.contains("\\v.i."));
                    if (isVarcharData && firstDataIdx < 0) firstDataIdx = i;
                    if (isVarcharAux && firstAuxIdx < 0) firstAuxIdx = i;
                }

                if (firstDataIdx < 0 || firstAuxIdx < 0) {
                    // The O3 path may have both paths under temporary names, or no fsync fired.
                    // Report the recorded sync order as a diagnostic failure rather than a false pass.
                    StringBuilder sb = new StringBuilder("O3 fsync order did not contain both v.d and v.i. Recorded sync order:\n");
                    for (int i = 0; i < order.size(); i++) {
                        sb.append("  [").append(i).append("] ").append(order.get(i)).append('\n');
                    }
                    sb.append("firstDataIdx=").append(firstDataIdx).append(" firstAuxIdx=").append(firstAuxIdx);
                    Assert.fail(sb.toString());
                }

                Assert.assertTrue(
                        "O3 must sync data (.d) before aux (.i) for varchar column v "
                                + "(firstDataIdx=" + firstDataIdx + " firstAuxIdx=" + firstAuxIdx + ")",
                        firstDataIdx < firstAuxIdx
                );
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }
}
