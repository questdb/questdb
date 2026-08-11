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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * DATA BEFORE POINTER for WAL segment structural changes.
 * <p>
 * A segment's {@code _meta} names its columns, and WAL apply resolves those names to files. So the files
 * must be in place BEFORE the metadata that points at them is published — otherwise a crash on the
 * {@code _meta.swp} barrier leaves durable metadata naming a file that does not exist, the segment becomes
 * permanently unappliable, and recovery suspends the table with
 * {@code "WAL segment column too short for committed row range [... actual=-1]"} (actual=-1 = MISSING file).
 * <p>
 * These tests assert the ORDER directly rather than crashing: by the time {@code _meta.swp} is opened, the
 * column file the new metadata will name must already exist on disk. That makes them deterministic and,
 * unlike a crash sweep over this workload, actually discriminating — reverting either ordering fix turns
 * them red.
 */
public class WalWriterStructuralPublishOrderTest extends AbstractCairoTest {

    /**
     * Records the order in which the WAL segment's files are created/renamed, so a test can assert that a
     * column file was in place before the segment metadata naming it was published.
     */
    private static final class OrderRecordingFacade extends TestFilesFacadeImpl {
        final List<String> ops = new ArrayList<>();

        /**
         * {@code mustContain} disambiguates the SEGMENT metadata ({@code .../walN/<seg>/_meta}) from the
         * table-level one at the table root — they share a suffix, and only the segment's names the columns
         * that WAL apply resolves.
         */
        int indexOfFirst(String kind, String mustContain, String suffix) {
            for (int i = 0; i < ops.size(); i++) {
                String op = ops.get(i);
                if (op.startsWith(kind + " ") && op.contains(mustContain) && op.endsWith(suffix)) {
                    return i;
                }
            }
            return -1;
        }

        // LPSZ has no usable toString(), so decode it the way CrashFaultFilesFacade does; matching on the
        // identity hash would silently never match and make every assertion here vacuous.
        private static String str(LPSZ name) {
            String path = Utf8String.newInstance(name).toString();
            final int nul = path.indexOf('\0');
            return nul > -1 ? path.substring(0, nul) : path;
        }

        final java.util.Map<Long, String> fdPaths = new java.util.HashMap<>();

        @Override
        public long openRW(LPSZ name, int opts) {
            final String path = str(name);
            ops.add("openRW " + path);
            final long fd = super.openRW(name, opts);
            fdPaths.put(fd, path);
            return fd;
        }

        @Override
        public void fdatasync(long fd) {
            final String path = fdPaths.get(fd);
            ops.add("fdatasync " + (path == null ? "fd:" + fd : path));
            super.fdatasync(fd);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int res = super.rename(from, to);
            ops.add("rename " + str(to));
            return res;
        }
    }

    private static void assertBefore(OrderRecordingFacade ff, String whatKind, String what, String thenKind, String then) {
        // Both the column file and the segment _meta live under the wal segment directory.
        final int first = ff.indexOfFirst(whatKind, "wal", what);
        final int second = ff.indexOfFirst(thenKind, "wal", then);
        Assert.assertTrue("expected a '" + whatKind + " ..." + what + "' op, but the structural path never "
                + "performed one - the test is not exercising the branch under test. Ops:\n"
                + String.join("\n", ff.ops), first >= 0);
        Assert.assertTrue("expected a '" + thenKind + " ..." + then + "' op (the SEGMENT metadata publish), "
                + "but none happened. Ops:\n" + String.join("\n", ff.ops), second >= 0);
        Assert.assertTrue("DATA BEFORE POINTER violated: the segment metadata (" + then + ") was published at "
                + "op " + second + ", BEFORE the column file (" + what + ") was put in place at op " + first
                + ". A crash on the _meta.swp barrier would leave durable metadata naming a file that does "
                + "not exist.\nOps:\n" + String.join("\n", ff.ops), first < second);
    }

    private TableToken seedTable(String name) throws Exception {
        execute("create table " + name + " (ts timestamp, s string, v long) timestamp(ts) partition by day wal");
        execute("insert into " + name + " values ('2024-01-01T00:00:00.000000Z', 'abc', 1)");
        drainWalQueue();
        return engine.verifyTableName(name);
    }

    @Test
    public void testAddColumnCreatesColumnFileBeforePublishingSegmentMeta() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final OrderRecordingFacade ff = new OrderRecordingFacade();
        assertMemoryLeak(ff, () -> {
            final TableToken tt = seedTable("wsp_add");
            try (WalWriter w = engine.getWalWriter(tt)) {
                w.newRow(1_704_067_201_000_000L).append();  // uncommitted rows -> in-segment structural path
                ff.ops.clear();
                final AlterOperationBuilder b = new AlterOperationBuilder()
                        .ofAddColumn(0, tt, w.getMetadata().getTableId());
                b.addColumnToList("added_col", 0, ColumnType.LONG, 0, false, (byte) 0, 0, false);
                final AlterOperation op = b.build();
                try (SqlExecutionContextImpl ctx =
                             new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    op.withContext(ctx);
                    w.apply(op, true);
                }
                assertBefore(ff, "openRW", "added_col.d", "openRW", "_meta");
            } finally {
                engine.releaseAllWalWriters();
                engine.releaseAllWriters();
            }
        });
    }

    @Test
    public void testRenameMovesColumnFileBeforePublishingSegmentMeta() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final OrderRecordingFacade ff = new OrderRecordingFacade();
        assertMemoryLeak(ff, () -> {
            final TableToken tt = seedTable("wsp_ren");
            try (WalWriter w = engine.getWalWriter(tt)) {
                w.newRow(1_704_067_201_000_000L).append();  // uncommitted rows -> in-segment structural path
                ff.ops.clear();
                final AlterOperationBuilder b = new AlterOperationBuilder()
                        .ofRenameColumn(0, tt, w.getMetadata().getTableId());
                b.ofRenameColumn("s", "renamed_s");
                final AlterOperation op = b.build();
                try (SqlExecutionContextImpl ctx =
                             new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    op.withContext(ctx);
                    w.apply(op, true);
                }
                assertBefore(ff, "rename", "renamed_s.d", "openRW", "_meta");
            } finally {
                engine.releaseAllWalWriters();
                engine.releaseAllWriters();
            }
        });
    }

    /**
     * ADAPTIVE device-barrier handshake for the structural null-backfill.
     * <p>
     * Adding a column to a segment that already holds uncommitted rows backfills nulls into the new
     * column's files. That write happens on the STRUCTURAL path, which sequences via events-only barriers
     * ({@code syncAdaptiveEventsBeforeSequencing}) and never runs {@code syncIfRequired0}'s per-column
     * fdatasync loop — so the backfill has to carry its own device barrier.
     * <p>
     * The var-size helpers used to rely on {@code msync(MS_SYNC)} implicitly being a range-fsync. That
     * makes the durability grade depend on kernel behaviour rather than on an explicit barrier, and under
     * a group-commit window (W&gt;0) it forces a synchronous device flush the design deliberately defers.
     * Every other ADAPTIVE path in this class pairs an ordering msync with an explicit fdatasync; this
     * pins that the backfill does too.
     */
    @Test
    public void testAdaptiveVarColumnBackfillGetsExplicitDeviceBarrier() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final OrderRecordingFacade ff = new OrderRecordingFacade();
        assertMemoryLeak(ff, () -> {
            final TableToken tt = seedTable("wsp_bf");
            try (WalWriter w = engine.getWalWriter(tt)) {
                w.newRow(1_704_067_201_000_000L).append();  // uncommitted rows -> backfill runs
                ff.ops.clear();
                final AlterOperationBuilder b = new AlterOperationBuilder()
                        .ofAddColumn(0, tt, w.getMetadata().getTableId());
                b.addColumnToList("bf_col", 0, ColumnType.VARCHAR, 0, false, (byte) 0, 0, false);
                final AlterOperation op = b.build();
                try (SqlExecutionContextImpl ctx =
                             new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    op.withContext(ctx);
                    w.apply(op, true);
                }
                // VARCHAR backfills the AUX vector; its data vector has a zero min entry size, so
                // setVarColumnDataFileNull legitimately writes and flushes nothing. The aux file is the
                // one carrying bytes, so it is the one that must carry the barrier.
                Assert.assertTrue("the backfill never created the new column's aux file - this test is "
                                + "not exercising the path it claims to. Ops:\n" + String.join("\n", ff.ops),
                        ff.indexOfFirst("openRW", "wal", "bf_col.i") >= 0);
                Assert.assertTrue("ADAPTIVE structural null-backfill left the new column with NO explicit "
                                + "device barrier: an msync alone leans on the kernel treating MS_SYNC as a "
                                + "range-fsync, and under a group-commit window defers nothing. Every other "
                                + "ADAPTIVE path pairs the ordering msync with an fdatasync. Ops:\n"
                                + String.join("\n", ff.ops),
                        ff.indexOfFirst("fdatasync", "wal", "bf_col.i") >= 0);
            } finally {
                engine.releaseAllWalWriters();
                engine.releaseAllWriters();
            }
        });
    }
}
